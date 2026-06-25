import logging
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError

from django.conf import settings
from django.utils import timezone

from queue_app.services.queue_runtime import (
    QueueOwnershipLost,
    _ensure_queue_ownership,
    _queue_lock_owned_by,
)
from queue_app.services.performance_service import log_performance
from queue_app.services.sap_com_guard import SAPCOMCircuitBreaker

log = logging.getLogger(__name__)


SAP_COM_ZE16_TIMEOUT_SECONDS = max(float(getattr(settings, 'SAP_COM_ZE16_TIMEOUT_SECONDS', 180)), 1.0)
SAP_COM_CALL_WARN_SECONDS = max(float(getattr(settings, 'SAP_COM_CALL_WARN_SECONDS', 5)), 0.0)


def _run_pallet_boundary(
    pallet_id: int,
    emit_completion=True,
    owner: str | None = None,
    sap_com_breaker: SAPCOMCircuitBreaker | None = None,
):
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
    _ensure_queue_ownership(owner, 'run_pallet_boundary')
    result = ze16_pdf_task_sync(
        pallet_id,
        emit_completion=emit_completion,
        owner=owner,
        sap_com_breaker=sap_com_breaker,
    )

    if result.get('status') == 'ok':
        _ensure_queue_ownership(owner, 'mark_pallet_done')
        pallet.status = Pallet.STATUS_DONE
        pallet.save(update_fields=['status'])
        from queue_app.services.stats_service import invalidate_queue_stats_cache

        invalidate_queue_stats_cache()

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


def _ze16_receipt_retry_config() -> tuple[int, float]:
    try:
        attempts = int(getattr(settings, 'SAP_ZE16_RECEIPT_MAX_ATTEMPTS', 4))
    except (TypeError, ValueError):
        attempts = 4

    try:
        delay_seconds = float(getattr(settings, 'SAP_ZE16_RECEIPT_RETRY_DELAY_SECONDS', 5))
    except (TypeError, ValueError):
        delay_seconds = 5

    return max(1, attempts), max(0.0, delay_seconds)


def _get_ze16_receipts_with_retries(
    ze16,
    hu_codes: list[str],
    *,
    pallet_id: int,
    hu_count: int,
    owner: str | None = None,
    status_callback=None,
    sleep_func=time.sleep,
) -> tuple[dict[str, str], int]:
    attempts, delay_seconds = _ze16_receipt_retry_config()

    for attempt in range(1, attempts + 1):
        _ensure_queue_ownership(owner, f'ze16_receipt_attempt_{attempt}')
        if status_callback:
            status_callback(
                f'Pallet P{pallet_id:02d}: consultando receipts en ZE16 ({attempt}/{attempts}).',
                badge='ZE16',
                mode='running',
                footer=f'Buscando receipts para {hu_count} HU(s).',
                active_pallet_id=pallet_id,
                active_hu_count=hu_count,
            )

        ze16_started_at = time.perf_counter()
        receipts = ze16.get_receipts_for_pallet(hu_codes)
        log_performance(
            log,
            'sap.ze16.receipt_query',
            ze16_started_at,
            pallet=pallet_id,
            attempt=attempt,
            receipts=len(receipts),
        )
        _ensure_queue_ownership(owner, f'ze16_receipts_received_{attempt}')
        if receipts:
            return receipts, attempt

        log.warning(
            "ze16_receipts_empty pallet=%s attempt=%s/%s",
            pallet_id,
            attempt,
            attempts,
        )
        if attempt < attempts and delay_seconds:
            if status_callback:
                status_callback(
                    f'Pallet P{pallet_id:02d}: ZE16 no devolvio receipts. Reintentando.',
                    badge='ZE16',
                    mode='running',
                    footer=f'Siguiente intento en {delay_seconds:g}s.',
                    active_pallet_id=pallet_id,
                    active_hu_count=hu_count,
                )
            sleep_func(delay_seconds)

    return {}, attempts


def _query_ze16_receipts_with_timeout(
    *,
    pallet_id: int,
    hu_codes: list[str],
    hu_count: int,
    owner: str | None,
    status_callback,
    sap_com_breaker: SAPCOMCircuitBreaker | None = None,
) -> tuple[dict[str, str], int, str, dict[str, str]]:
    from core.sap_client import SAPClient, SAPCOMBlockedError, SAPCOMFatalBlockedError
    from core.ze16_client import ZE16Client, ZE16Error

    breaker = sap_com_breaker or SAPCOMCircuitBreaker(status_callback=status_callback)

    def query_in_com_thread():
        import pythoncom

        pythoncom.CoInitialize()
        try:
            sap = SAPClient()
            sap_started_at = time.perf_counter()
            sap.connect()
            log_performance(log, 'sap.connect_ze16', sap_started_at, pallet=pallet_id)
            _ensure_queue_ownership(owner, 'ze16_sap_connected')

            printed_by = getattr(settings, 'SAP_LOGIN_USER', '') or sap.get_user()
            ze16 = ZE16Client(sap.session)
            ze16_started_at = time.perf_counter()
            receipts, receipt_attempts = _get_ze16_receipts_with_retries(
                ze16,
                hu_codes,
                pallet_id=pallet_id,
                hu_count=hu_count,
                owner=owner,
                status_callback=status_callback,
            )
            log_performance(
                log,
                'sap.ze16.receipt_retries',
                ze16_started_at,
                pallet=pallet_id,
                attempts=receipt_attempts,
                receipts=len(receipts),
            )
            hu_display_map = _build_hu_display_map(ze16, hu_codes)
            return receipts, receipt_attempts, printed_by, hu_display_map
        finally:
            pythoncom.CoUninitialize()

    started_at = time.perf_counter()
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix='sap-ze16-worker')
    try:
        future = executor.submit(query_in_com_thread)
        try:
            result = future.result(timeout=SAP_COM_ZE16_TIMEOUT_SECONDS)
            breaker.record_success('sap.ze16.receipt_query', log)
            return result
        except FutureTimeoutError as exc:
            elapsed_ms = int((time.perf_counter() - started_at) * 1000)
            timeout_ms = int(SAP_COM_ZE16_TIMEOUT_SECONDS * 1000)
            future.cancel()
            future.add_done_callback(
                lambda _future: breaker.record_late_completion('sap.ze16.receipt_query', log)
            )
            executor.shutdown(wait=False, cancel_futures=True)
            fatal = breaker.handle_timeout(
                operation='sap.ze16.receipt_query',
                elapsed_ms=elapsed_ms,
                timeout_ms=timeout_ms,
                logger=log,
            )
            if fatal:
                raise SAPCOMFatalBlockedError(
                    "SAP COM no respondio en ZE16. Corrida detenida para evitar "
                    "acumulacion de hilos COM."
                ) from exc
            blocked = SAPCOMBlockedError("ZE16 COM timeout")
            blocked.__cause__ = exc
            raise ZE16Error(
                f"SAP COM bloqueado en ZE16 tras {SAP_COM_ZE16_TIMEOUT_SECONDS:g}s. "
                "Puede haber otra automatizacion SAP ocupando SAP GUI."
            ) from blocked
        finally:
            elapsed_ms = int((time.perf_counter() - started_at) * 1000)
            warn_ms = int(SAP_COM_CALL_WARN_SECONDS * 1000)
            if warn_ms and elapsed_ms >= warn_ms:
                log.warning(
                    "sap_com_call_slow operation=sap.ze16.receipt_query pallet=%s duration_ms=%s warn_ms=%s",
                    pallet_id,
                    elapsed_ms,
                    warn_ms,
                )
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


def ze16_pdf_task_sync(
    pallet_id: int,
    emit_completion=True,
    owner: str | None = None,
    sap_com_breaker: SAPCOMCircuitBreaker | None = None,
):
    """
    Paso sincronico ZE16 + PDF + impresion. Retorna solo despues de print_pdf.
    """
    from core.hu_origins import detect_origin, resolve_effective_origin
    from core.pdf_receipt import PalletReceiptPDF
    from core.sap_client import SAPCOMFatalBlockedError
    from core.ze16_client import ZE16Error
    from queue_app.models import HUItem, Pallet
    from queue_app.utils import emit_queue_status, emit_receipt_done

    workflow_started_at = time.perf_counter()
    result = {'status': 'error', 'message': 'Unknown ZE16/PDF error', 'marked': 0}
    pdf_started = None

    try:
        _ensure_queue_ownership(owner, 'ze16_pdf_start')
        db_started_at = time.perf_counter()
        pallet = Pallet.objects.get(pk=pallet_id)
        hu_codes = list(
            pallet.items.filter(status__in=[HUItem.STATUS_OK, HUItem.STATUS_DUPLICATE])
            .values_list('hu_code', flat=True)
        )
        log_performance(
            log,
            'db.fetch_pallet_for_receipt',
            db_started_at,
            pallet=pallet_id,
            hus=len(hu_codes),
        )
        hu_count = len(hu_codes)

        if not hu_codes:
            result = {'status': 'error', 'message': 'Sin HUs validos para ZE16', 'marked': 0}
            _save_pdf_result(pallet, result, pdf_started, owner=owner)
            return result

        origin = detect_origin(hu_codes[0])
        effective_origin = resolve_effective_origin(origin, hu_count)

        emit_queue_status(
            f'Pallet P{pallet_id:02d}: conectando SAP para ZE16.',
            badge='ZE16',
            mode='running',
            footer='Preparando consulta de receipts.',
            active_pallet_id=pallet_id,
            active_hu_count=hu_count,
        )
        receipts, receipt_attempts, printed_by, hu_display_map = _query_ze16_receipts_with_timeout(
            pallet_id=pallet_id,
            hu_codes=hu_codes,
            hu_count=hu_count,
            owner=owner,
            status_callback=emit_queue_status,
            sap_com_breaker=sap_com_breaker,
        )

        if not receipts:
            result = {
                'status': 'error',
                'message': (
                    f'ZE16: sin Receipt IDs para pallet {pallet_id} '
                    f'tras {receipt_attempts} intento(s)'
                ),
                'marked': 0,
            }
            _save_pdf_result(pallet, result, pdf_started, owner=owner)
            return result

        pdf_started = time.perf_counter()
        emit_queue_status(
            f'Pallet P{pallet_id:02d}: generando PDF de recibo.',
            badge='PDF',
            mode='running',
            footer=f'Creando recibo con {len(receipts)} receipt(s).',
            active_pallet_id=pallet_id,
            active_hu_count=hu_count,
        )
        pdf_generation_started_at = time.perf_counter()
        pdf_path = PalletReceiptPDF.generate(
            pallet_id=pallet_id,
            origin_label=effective_origin.label,
            receipts=receipts,
            hu_display_map=hu_display_map,
            printed_by=printed_by,
        )
        log_performance(
            log,
            'pdf.generate',
            pdf_generation_started_at,
            pallet=pallet_id,
            receipts=len(receipts),
        )
        _ensure_queue_ownership(owner, 'pdf_generated')
        emit_queue_status(
            f'Pallet P{pallet_id:02d}: enviando PDF a impresora.',
            badge='PRINT',
            mode='running',
            footer='Esperando confirmacion del sistema de impresion.',
            active_pallet_id=pallet_id,
            active_hu_count=hu_count,
        )
        print_started_at = time.perf_counter()
        printed = PalletReceiptPDF.print_pdf(pdf_path)
        log_performance(log, 'pdf.print_and_spool', print_started_at, pallet=pallet_id, printed=printed)
        _ensure_queue_ownership(owner, 'pdf_printed')

        if not printed:
            result = {
                'status': 'error',
                'message': f'PDF generado pero no confirmado por impresora: {pdf_path}',
                'marked': 0,
            }
            _save_pdf_result(pallet, result, pdf_started, owner=owner)
            return result

        found = len(receipts)
        suffix = f" ({found}/{hu_count} con receipt)" if found < hu_count else ""
        result = {
            'status': 'ok',
            'message': f'ZE16+PDF OK - {found} recibos{suffix}',
            'marked': found,
        }

        _save_pdf_result(pallet, result, pdf_started, owner=owner)

        log.info("ze16_pdf_task_sync done pallet=%s result=%s", pallet_id, result)
        return result

    except QueueOwnershipLost:
        log.warning("ze16_pdf_task_sync stopped_lost_ownership pallet=%s owner=%s", pallet_id, owner)
        raise
    except SAPCOMFatalBlockedError as e:
        log.critical("ze16_pdf_task_sync SAP_COM_BLOCKED_FATAL pallet=%s error=%s", pallet_id, e)
        result = {'status': 'error', 'message': str(e), 'marked': 0}
        if 'pallet' in locals():
            _save_pdf_result(pallet, result, pdf_started, owner=owner)
        raise
    except ZE16Error as e:
        log.error("ze16_pdf_task_sync ZE16Error pallet=%s error=%s", pallet_id, e)
        result = {'status': 'error', 'message': f'ZE16 Error: {e}', 'marked': 0}
        if 'pallet' in locals():
            _save_pdf_result(pallet, result, pdf_started, owner=owner)
        return result
    except Exception as e:
        log.exception("ze16_pdf_task_sync error pallet=%s", pallet_id)
        result = {'status': 'error', 'message': str(e), 'marked': 0}
        if 'pallet' in locals():
            _save_pdf_result(pallet, result, pdf_started, owner=owner)
        return result
    finally:
        if emit_completion and (not owner or _queue_lock_owned_by(owner)):
            emit_receipt_done(pallet_id, result)
        log_performance(
            log,
            'queue.ze16_pdf_total',
            workflow_started_at,
            pallet=pallet_id,
            status=result.get('status'),
        )


def _save_pdf_result(pallet, result: dict, started_at: float | None, owner: str | None = None) -> dict:
    """Guarda resultado y duracion del tramo PDF: generar archivo + imprimir."""
    from queue_app.models import Pallet

    _ensure_queue_ownership(owner, 'save_pdf_result')
    pdf_ms = max(0, int((time.perf_counter() - started_at) * 1000)) if started_at else 0
    result['pdf_ms'] = pdf_ms

    pallet.pdf_status = (
        Pallet.PDF_STATUS_OK
        if result.get('status') == 'ok'
        else Pallet.PDF_STATUS_ERROR
    )
    pallet.pdf_msg = (result.get('message') or '')[:255]
    pallet.pdf_ms = pdf_ms
    completed_at = timezone.now()
    pallet.processing_finished_at = completed_at

    update_fields = ['pdf_status', 'pdf_msg', 'pdf_ms', 'processing_finished_at']
    if result.get('status') == 'ok':
        pallet.receipt_done_at = completed_at
        update_fields.append('receipt_done_at')

    item_updates = {
        'pdf_status': pallet.pdf_status,
        'pdf_msg': pallet.pdf_msg,
        'pdf_ms': pdf_ms,
    }
    if completed_at:
        item_updates['receipt_done_at'] = completed_at
    db_started_at = time.perf_counter()
    pallet.items.all().update(**item_updates)
    pallet.save(update_fields=update_fields)
    from queue_app.services.stats_service import invalidate_queue_stats_cache

    invalidate_queue_stats_cache()
    log_performance(
        log,
        'db.save_pdf_result',
        db_started_at,
        pallet=pallet.pk,
        status=pallet.pdf_status,
    )
    result['pdf_status'] = pallet.pdf_status
    result['pdf_display'] = pallet.pdf_display
    result['pdf_msg'] = pallet.pdf_msg
    result['processing_time_display'] = pallet.processing_time_display
    result['processing_started_at'] = (
        pallet.processing_started_at.isoformat()
        if pallet.processing_started_at
        else ''
    )
    result['processing_finished_at'] = (
        pallet.processing_finished_at.isoformat()
        if pallet.processing_finished_at
        else ''
    )
    result['receipt_done_at'] = pallet.receipt_done_at.isoformat() if pallet.receipt_done_at else ''
    return result
