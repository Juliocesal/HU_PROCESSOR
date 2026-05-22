import logging
import time

from celery import shared_task
from django.conf import settings
from django.utils import timezone

log = logging.getLogger(__name__)

QUEUE_LOCK_KEY = 'nexhus:queue_processing_lock'
QUEUE_STOP_KEY = 'nexhus:queue_stop_requested'
QUEUE_LOCK_TTL_SECONDS = 60 * 60 * 6


def _get_redis_lock_client():
    import redis

    return redis.Redis.from_url(
        settings.CELERY_BROKER_URL,
        socket_connect_timeout=2,
        socket_timeout=2,
    )


def _acquire_queue_lock(owner: str) -> bool:
    try:
        client = _get_redis_lock_client()
        acquired = bool(client.set(QUEUE_LOCK_KEY, owner, nx=True, ex=QUEUE_LOCK_TTL_SECONDS))
        if acquired:
            client.delete(QUEUE_STOP_KEY)
        return acquired
    except Exception as e:
        log.error("queue_lock_acquire_failed owner=%s error=%s", owner, e)
        return False


def _release_queue_lock(owner: str) -> None:
    try:
        client = _get_redis_lock_client()
        current = client.get(QUEUE_LOCK_KEY)
        if current and current.decode('utf-8', errors='replace') == owner:
            client.delete(QUEUE_LOCK_KEY)
            client.delete(QUEUE_STOP_KEY)
    except Exception as e:
        log.warning("queue_lock_release_failed owner=%s error=%s", owner, e)


def is_queue_locked() -> bool:
    """Devuelve True cuando un worker posee el lock de procesamiento en Redis."""
    try:
        return bool(_get_redis_lock_client().exists(QUEUE_LOCK_KEY))
    except Exception as e:
        log.warning("queue_lock_status_failed error=%s", e)
        return False


def request_queue_stop() -> bool:
    """
    Solicita al worker activo detenerse en el siguiente punto seguro.

    El trabajo en SAP GUI no debe cortarse a mitad de HU. La tarea revisa esta
    marca entre HUs y antes de ZE16/PDF para detener la cola sin corromper SAP.
    """
    try:
        client = _get_redis_lock_client()
        if not client.exists(QUEUE_LOCK_KEY):
            return False
        client.set(QUEUE_STOP_KEY, '1', ex=QUEUE_LOCK_TTL_SECONDS)
        return True
    except Exception as e:
        log.error("queue_stop_request_failed error=%s", e)
        return False


def _stop_requested() -> bool:
    try:
        return bool(_get_redis_lock_client().exists(QUEUE_STOP_KEY))
    except Exception as e:
        log.warning("queue_stop_check_failed error=%s", e)
        return False


def _stopped_result(pallets_processed: int, hus_processed: int, errors: int) -> dict:
    return {
        'status': 'stopped',
        'message': 'Proceso detenido por el usuario.',
        'pallets_processed': pallets_processed,
        'hus_processed': hus_processed,
        'errors': errors,
    }


@shared_task(bind=True, max_retries=0)
def process_queue_task(self, run_f1=True, run_f2=True, run_pdf=True):
    """
    Procesa toda la cola en orden deterministico:
    pallet -> todas sus HUs -> ZE16/PDF/impresion -> siguiente pallet.
    """
    from queue_app.models import HUItem, Pallet
    from queue_app.utils import (
        emit_error,
        emit_queue_done,
        emit_receipt_done,
        pallets_ready_for_pdf_queryset,
    )

    owner = f"process_queue_task:{self.request.id}"
    if not _acquire_queue_lock(owner):
        message = 'Queue processing already active; start request ignored.'
        log.warning(message)
        emit_error(message)
        return {'ok': False, 'error': message}

    pallets_processed = 0
    hus_processed = 0
    errors = 0

    try:
        pending_pallet_ids = list(
            Pallet.objects.filter(items__status=HUItem.STATUS_PENDING)
            .distinct()
            .order_by('id')
            .values_list('id', flat=True)
        )
        pdf_pallet_ids = list(
            pallets_ready_for_pdf_queryset()
            .order_by('id')
            .values_list('id', flat=True)
        ) if run_pdf else []
        pallet_ids = sorted(set(pending_pallet_ids) | set(pdf_pallet_ids))

        if not pallet_ids:
            result = {
                'status': 'error',
                'message': 'No pending HUs or PDF receipts to process.',
                'pallets_processed': 0,
                'hus_processed': 0,
                'errors': 0,
            }
            emit_queue_done(result)
            return result

        log.info("process_queue_task start pallets=%s", pallet_ids)

        for pallet_id in pallet_ids:
            if _stop_requested():
                final = _stopped_result(pallets_processed, hus_processed, errors)
                emit_queue_done(final)
                return final

            pallet = Pallet.objects.get(pk=pallet_id)
            item_ids = list(
                pallet.items.filter(status=HUItem.STATUS_PENDING)
                .order_by('added_at', 'id')
                .values_list('id', flat=True)
            )

            if not item_ids and pallet_id not in pdf_pallet_ids:
                continue

            log.info(
                "process_queue_task pallet_start pallet=%s hu_count=%s",
                pallet_id,
                len(item_ids),
            )

            for item_id in item_ids:
                if _stop_requested():
                    final = _stopped_result(pallets_processed, hus_processed, errors)
                    emit_queue_done(final)
                    return final

                item = _process_hu_item(
                    item_id,
                    run_f1=run_f1,
                    run_f2=run_f2,
                    run_pallet_boundary=False,
                    emit_pallet_completion=False,
                )
                hus_processed += 1
                if item and item.status in (HUItem.STATUS_ERROR, HUItem.STATUS_HU_NOT_FOUND):
                    errors += 1

            pallet_errors = pallet.items.filter(
                status__in=[HUItem.STATUS_ERROR, HUItem.STATUS_HU_NOT_FOUND]
            ).count()
            if pallet_errors:
                if run_pdf:
                    result = {
                        'status': 'error',
                        'message': f'PDF omitido: {pallet_errors} HU(s) con error en pallet {pallet_id}',
                        'marked': 0,
                    }
                    _save_pdf_result(pallet, result, None)
                    emit_receipt_done(pallet_id, result)

                log.warning(
                    "process_queue_task pallet_has_errors pallet=%s errors=%s",
                    pallet_id,
                    pallet_errors,
                )
                pallets_processed += 1
                continue

            if run_pdf:
                if _stop_requested():
                    final = _stopped_result(pallets_processed, hus_processed, errors)
                    emit_queue_done(final)
                    return final

                result = _run_pallet_boundary(pallet_id, emit_completion=True)
                if result and result.get('status') != 'ok':
                    errors += 1
                    log.error(
                        "process_queue_task pallet_receipt_failed pallet=%s result=%s",
                        pallet_id,
                        result,
                    )
                    pallets_processed += 1
                    continue

            pallets_processed += 1
            log.info("process_queue_task pallet_done pallet=%s", pallet_id)

        final_status = 'ok' if errors == 0 else 'error'
        final_message = (
            'All pallets were processed and printed successfully'
            if errors == 0
            else f'Queue finished with {errors} error(s)'
        )
        final = {
            'status': final_status,
            'message': final_message,
            'pallets_processed': pallets_processed,
            'hus_processed': hus_processed,
            'errors': errors,
        }
        emit_queue_done(final)
        log.info("process_queue_task done result=%s", final)
        return final

    except Exception as e:
        log.exception("process_queue_task fatal_error")
        final = {
            'status': 'error',
            'message': str(e),
            'pallets_processed': pallets_processed,
            'hus_processed': hus_processed,
            'errors': errors + 1,
        }
        emit_queue_done(final)
        return final
    finally:
        _release_queue_lock(owner)


@shared_task(bind=True, max_retries=0)
def process_hu_task(self, hu_item_id: int, run_f1=True, run_f2=True):
    """
    Tarea de compatibilidad para una HU. La UI actual inicia process_queue_task.
    """
    owner = f"process_hu_task:{self.request.id}"
    if not _acquire_queue_lock(owner):
        log.warning("process_hu_task skipped because queue lock is active hu_id=%s", hu_item_id)
        return {'ok': False, 'error': 'Queue processing already active'}

    try:
        return _process_hu_item(
            hu_item_id,
            run_f1=run_f1,
            run_f2=run_f2,
            run_pallet_boundary=True,
            emit_pallet_completion=True,
        )
    finally:
        _release_queue_lock(owner)


def _process_hu_item(
    hu_item_id: int,
    run_f1=True,
    run_f2=True,
    run_pallet_boundary=True,
    emit_pallet_completion=True,
):
    import pythoncom
    from core.hu_origins import detect_origin
    from core.sap_client import SAPClient, SAPConnectionError
    from queue_app.models import HUItem
    from queue_app.utils import emit_item_update, emit_stats_update

    pythoncom.CoInitialize()

    try:
        try:
            item = HUItem.objects.select_related('pallet').get(pk=hu_item_id)
        except HUItem.DoesNotExist:
            log.error("process_hu_item missing hu_id=%s", hu_item_id)
            return None

        if item.status != HUItem.STATUS_PENDING:
            log.warning("process_hu_item skip hu=%s status=%s", item.hu_code, item.status)
            return item

        origin = detect_origin(item.hu_code)

        item.status = HUItem.STATUS_PROCESSING
        item.save(update_fields=['status'])
        emit_item_update(item)
        emit_stats_update(item.pallet)

        sap = SAPClient()
        sap.connect()
        log.info("process_hu_item sap_connected hu=%s", item.hu_code)

        res1 = None

        if run_f1:
            sap.setup_phase1(origin=origin)
            res1 = sap.process_hu_phase1(item.hu_code, origin=origin)
            item.phase1_msg = res1['message']
            item.f1_done_at = timezone.now()

            if res1['status'] in ('error', 'hu_not_found'):
                item.status = (
                    HUItem.STATUS_HU_NOT_FOUND
                    if res1['status'] == 'hu_not_found'
                    else HUItem.STATUS_ERROR
                )
                item.processed_at = timezone.now()
                item.save(update_fields=['status', 'phase1_msg', 'f1_done_at', 'processed_at'])
                emit_item_update(item)
                emit_stats_update(item.pallet)
                log.warning(
                    "process_hu_item f1_failed hu=%s reason=%s",
                    item.hu_code,
                    res1['message'],
                )
                if run_pallet_boundary:
                    _run_pallet_boundary(item.pallet_id, emit_completion=emit_pallet_completion)
                return item

            if res1['status'] == 'duplicate':
                item.phase1_msg = 'Ya se hizo el Acknowledge'

        if run_f2:
            sap.setup_phase2(origin=origin)
            res2 = sap.process_hu_phase2(
                item.hu_code,
                phase2_wait=origin.phase2_wait,
                origin=origin,
            )
            item.phase2_msg = res2['message']
            item.phase2_ms = res2['duration_ms']

            if res2['status'] == 'ok':
                pallet = item.pallet
                pallet.f2_done_at = res2['phase2_ts']
                pallet.save(update_fields=['f2_done_at'])
                item.status = (
                    HUItem.STATUS_OK
                    if (not run_f1 or (res1 and res1['status'] == 'ok'))
                    else HUItem.STATUS_DUPLICATE
                )
            else:
                item.status = HUItem.STATUS_ERROR
        else:
            item.status = HUItem.STATUS_OK

        item.processed_at = timezone.now()
        item.save(update_fields=[
            'status',
            'phase1_msg',
            'phase2_msg',
            'phase2_ms',
            'f1_done_at',
            'processed_at',
        ])
        emit_item_update(item)
        emit_stats_update(item.pallet)

        if run_pallet_boundary:
            _run_pallet_boundary(item.pallet_id, emit_completion=emit_pallet_completion)

        return item

    except SAPConnectionError as e:
        log.error("process_hu_item sap_error hu_id=%s error=%s", hu_item_id, e)
        return _mark_item_error(hu_item_id, str(e))
    except Exception as e:
        log.exception("process_hu_item fatal_error hu_id=%s", hu_item_id)
        return _mark_item_error(hu_item_id, str(e))
    finally:
        pythoncom.CoUninitialize()


def _run_pallet_boundary(pallet_id: int, emit_completion=True):
    """
    Ejecuta el recibo del pallet solo cuando todas sus HUs terminaron.
    """
    from queue_app.models import HUItem, Pallet

    try:
        pallet = Pallet.objects.get(pk=pallet_id)
    except Pallet.DoesNotExist:
        return {'status': 'error', 'message': f'Pallet {pallet_id} no existe', 'marked': 0}

    items = pallet.items.all()
    still_running = items.filter(
        status__in=[HUItem.STATUS_PENDING, HUItem.STATUS_PROCESSING]
    ).exists()

    if still_running:
        log.debug("_run_pallet_boundary pallet=%s still active, skip", pallet_id)
        return {'status': 'pending', 'message': 'Pallet still processing', 'marked': 0}

    if pallet.status == Pallet.STATUS_DONE and pallet.receipt_done_at:
        log.debug("_run_pallet_boundary pallet=%s already done, skip", pallet_id)
        return {'status': 'ok', 'message': 'Pallet already printed', 'marked': 0}

    valid_hus = list(
        items.filter(status__in=[HUItem.STATUS_OK, HUItem.STATUS_DUPLICATE])
        .values_list('hu_code', flat=True)
    )

    if not valid_hus:
        log.warning("_run_pallet_boundary pallet=%s without valid HUs", pallet_id)
        return {'status': 'error', 'message': 'Sin HUs validos para ZE16/PDF', 'marked': 0}

    log.info(
        "_run_pallet_boundary pallet=%s hu_count=%s -> ze16_pdf_task_sync",
        pallet_id,
        len(valid_hus),
    )
    result = ze16_pdf_task_sync(pallet_id, emit_completion=emit_completion)

    if result.get('status') == 'ok':
        pallet.status = Pallet.STATUS_DONE
        pallet.save(update_fields=['status'])

    return result


def _build_hu_display_map(ze16_client, hu_codes: list[str]) -> dict[str, str]:
    """
    Mantiene las llaves HU normalizadas de SAP e imprime ITA sin ceros iniciales.

    ZE16 requiere HUs ITA de 20 digitos, pero el recibo debe mostrar la HU tal
    como la escanearon los operadores.
    """
    display_map = {}
    for hu_code in hu_codes:
        normalized_hu = ze16_client._normalize_hu(hu_code)
        display_hu = (
            normalized_hu.lstrip('0')
            if normalized_hu.lstrip('0').startswith('29')
            else hu_code.strip()
        )
        display_map[normalized_hu] = display_hu
    return display_map


def ze16_pdf_task_sync(pallet_id: int, emit_completion=True):
    """
    Paso sincronico ZE16 + PDF + impresion. Retorna solo despues de print_pdf.
    """
    import pythoncom
    from core.hu_origins import detect_origin, resolve_effective_origin
    from core.pdf_receipt import PalletReceiptPDF
    from core.sap_client import SAPClient
    from core.ze16_client import ZE16Client, ZE16Error
    from queue_app.models import HUItem, Pallet
    from queue_app.utils import emit_receipt_done

    pythoncom.CoInitialize()
    result = {'status': 'error', 'message': 'Unknown ZE16/PDF error', 'marked': 0}
    pdf_started = None

    try:
        pallet = Pallet.objects.get(pk=pallet_id)
        hu_codes = list(
            pallet.items.filter(status__in=[HUItem.STATUS_OK, HUItem.STATUS_DUPLICATE])
            .values_list('hu_code', flat=True)
        )
        hu_count = len(hu_codes)

        if not hu_codes:
            result = {'status': 'error', 'message': 'Sin HUs validos para ZE16', 'marked': 0}
            _save_pdf_result(pallet, result, pdf_started)
            return result

        origin = detect_origin(hu_codes[0])
        effective_origin = resolve_effective_origin(origin, hu_count)

        sap = SAPClient()
        sap.connect()
        printed_by = getattr(settings, 'SAP_LOGIN_USER', '') or sap.get_user()

        ze16 = ZE16Client(sap.session)
        receipts = ze16.get_receipts_for_pallet(hu_codes)
        hu_display_map = _build_hu_display_map(ze16, hu_codes)

        if not receipts:
            result = {
                'status': 'error',
                'message': f'ZE16: sin Receipt IDs para pallet {pallet_id}',
                'marked': 0,
            }
            _save_pdf_result(pallet, result, pdf_started)
            return result

        pdf_started = time.perf_counter()
        pdf_path = PalletReceiptPDF.generate(
            pallet_id=pallet_id,
            origin_label=effective_origin.label,
            receipts=receipts,
            hu_display_map=hu_display_map,
            printed_by=printed_by,
        )
        printed = PalletReceiptPDF.print_pdf(pdf_path)

        if not printed:
            result = {
                'status': 'error',
                'message': f'PDF generado pero no confirmado por impresora: {pdf_path}',
                'marked': 0,
            }
            _save_pdf_result(pallet, result, pdf_started)
            return result

        found = len(receipts)
        suffix = f" ({found}/{hu_count} con receipt)" if found < hu_count else ""
        result = {
            'status': 'ok',
            'message': f'ZE16+PDF OK - {found} recibos{suffix}',
            'marked': found,
        }

        _save_pdf_result(pallet, result, pdf_started)

        log.info("ze16_pdf_task_sync done pallet=%s result=%s", pallet_id, result)
        return result

    except ZE16Error as e:
        log.error("ze16_pdf_task_sync ZE16Error pallet=%s error=%s", pallet_id, e)
        result = {'status': 'error', 'message': f'ZE16 Error: {e}', 'marked': 0}
        if 'pallet' in locals():
            _save_pdf_result(pallet, result, pdf_started)
        return result
    except Exception as e:
        log.exception("ze16_pdf_task_sync error pallet=%s", pallet_id)
        result = {'status': 'error', 'message': str(e), 'marked': 0}
        if 'pallet' in locals():
            _save_pdf_result(pallet, result, pdf_started)
        return result
    finally:
        if emit_completion:
            emit_receipt_done(pallet_id, result)
        pythoncom.CoUninitialize()


def _save_pdf_result(pallet, result: dict, started_at: float | None) -> dict:
    """Guarda resultado y duracion del tramo PDF: generar archivo + imprimir."""
    from queue_app.models import HUItem, Pallet

    pdf_ms = max(0, int((time.perf_counter() - started_at) * 1000)) if started_at else 0
    result['pdf_ms'] = pdf_ms

    pallet.pdf_status = (
        Pallet.PDF_STATUS_OK
        if result.get('status') == 'ok'
        else Pallet.PDF_STATUS_ERROR
    )
    pallet.pdf_msg = (result.get('message') or '')[:255]
    pallet.pdf_ms = pdf_ms

    update_fields = ['pdf_status', 'pdf_msg', 'pdf_ms']
    if result.get('status') == 'ok':
        completed_at = timezone.now()
        pallet.receipt_done_at = completed_at
        update_fields.append('receipt_done_at')
        pallet.items.filter(
            status__in=[HUItem.STATUS_OK, HUItem.STATUS_DUPLICATE]
        ).update(receipt_done_at=completed_at)

    pallet.save(update_fields=update_fields)
    result['pdf_status'] = pallet.pdf_status
    result['pdf_display'] = pallet.pdf_display
    result['pdf_msg'] = pallet.pdf_msg
    return result


def _mark_item_error(hu_item_id: int, message: str):
    from queue_app.models import HUItem
    from queue_app.utils import emit_item_update, emit_stats_update

    try:
        item = HUItem.objects.select_related('pallet').get(pk=hu_item_id)
        item.status = HUItem.STATUS_ERROR
        item.phase1_msg = message
        item.processed_at = timezone.now()
        item.save(update_fields=['status', 'phase1_msg', 'processed_at'])
        emit_item_update(item)
        emit_stats_update(item.pallet)
        return item
    except Exception:
        log.exception("_mark_item_error failed hu_id=%s", hu_item_id)
        return None
