import logging
import time

from celery import shared_task
from django.conf import settings
from django.utils import timezone

log = logging.getLogger(__name__)

QUEUE_LOCK_KEY = 'nexhus:queue_processing_lock'
QUEUE_STOP_KEY = 'nexhus:queue_stop_requested'
QUEUE_ACTIVE_PALLETS_KEY = 'nexhus:queue_active_pallets'
QUEUE_CONTINUOUS_ARMED_KEY = 'nexhus:queue_continuous_armed'
QUEUE_IDLE_DEADLINE_KEY = 'nexhus:queue_idle_deadline'
QUEUE_LOCK_TTL_SECONDS = 60 * 60 * 6
QUEUE_DEFAULT_IDLE_TIMEOUT_SECONDS = 3
QUEUE_IDLE_POLL_SECONDS = 2


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
            client.delete(QUEUE_ACTIVE_PALLETS_KEY)
            client.delete(QUEUE_IDLE_DEADLINE_KEY)
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
            client.delete(QUEUE_ACTIVE_PALLETS_KEY)
            client.delete(QUEUE_IDLE_DEADLINE_KEY)
    except Exception as e:
        log.warning("queue_lock_release_failed owner=%s error=%s", owner, e)


def is_queue_locked() -> bool:
    """Devuelve True cuando un worker posee el lock de procesamiento en Redis."""
    try:
        return bool(_get_redis_lock_client().exists(QUEUE_LOCK_KEY))
    except Exception as e:
        log.warning("queue_lock_status_failed error=%s", e)
        return False


def arm_continuous_queue() -> None:
    """Mantiene armado el modo continuo despues de un inicio manual."""
    try:
        _get_redis_lock_client().set(
            QUEUE_CONTINUOUS_ARMED_KEY,
            '1',
            ex=QUEUE_LOCK_TTL_SECONDS,
        )
    except Exception as e:
        log.warning("queue_continuous_arm_failed error=%s", e)


def disarm_continuous_queue() -> None:
    """Desactiva la reactivacion automatica al detener o limpiar la cola."""
    try:
        _get_redis_lock_client().delete(QUEUE_CONTINUOUS_ARMED_KEY)
        _get_redis_lock_client().delete(QUEUE_IDLE_DEADLINE_KEY)
    except Exception as e:
        log.warning("queue_continuous_disarm_failed error=%s", e)


def is_continuous_queue_armed() -> bool:
    """Indica si un cierre de pallet puede reactivar Celery automaticamente."""
    try:
        return bool(_get_redis_lock_client().exists(QUEUE_CONTINUOUS_ARMED_KEY))
    except Exception as e:
        log.warning("queue_continuous_arm_check_failed error=%s", e)
        return False


def _set_processing_pallet_ids(pallet_ids: list[int]) -> None:
    """Guarda en Redis los pallets que pertenecen a la corrida Celery actual."""
    try:
        client = _get_redis_lock_client()
        client.delete(QUEUE_ACTIVE_PALLETS_KEY)
        if pallet_ids:
            client.sadd(QUEUE_ACTIVE_PALLETS_KEY, *[str(pallet_id) for pallet_id in pallet_ids])
            client.expire(QUEUE_ACTIVE_PALLETS_KEY, QUEUE_LOCK_TTL_SECONDS)
    except Exception as e:
        log.warning("queue_active_pallets_set_failed error=%s", e)


def get_processing_pallet_ids() -> set[int]:
    """Devuelve los IDs de pallets que el worker ya tomo para la corrida actual."""
    try:
        raw_ids = _get_redis_lock_client().smembers(QUEUE_ACTIVE_PALLETS_KEY)
        return {
            int(raw_id.decode('utf-8', errors='replace'))
            for raw_id in raw_ids
            if raw_id
        }
    except Exception as e:
        log.warning("queue_active_pallets_read_failed error=%s", e)
        return set()


def _set_queue_idle_deadline(timeout_seconds: float) -> None:
    """Guarda hasta cuando el worker esperara otro pallet antes de cerrar SAP."""
    try:
        deadline = time.time() + max(0, float(timeout_seconds))
        client = _get_redis_lock_client()
        client.set(
            QUEUE_IDLE_DEADLINE_KEY,
            str(deadline),
            ex=QUEUE_LOCK_TTL_SECONDS,
        )
    except Exception as e:
        log.warning("queue_idle_deadline_set_failed error=%s", e)


def _clear_queue_idle_deadline() -> None:
    """Limpia la marca de espera cuando el worker vuelve a trabajar o termina."""
    try:
        _get_redis_lock_client().delete(QUEUE_IDLE_DEADLINE_KEY)
    except Exception as e:
        log.warning("queue_idle_deadline_clear_failed error=%s", e)


def get_queue_idle_remaining_seconds() -> int | None:
    """Devuelve segundos restantes de espera continua, si existe esa ventana."""
    try:
        raw_deadline = _get_redis_lock_client().get(QUEUE_IDLE_DEADLINE_KEY)
        if not raw_deadline:
            return None

        deadline = float(raw_deadline.decode('utf-8', errors='replace'))
        return max(0, int(deadline - time.time()))
    except Exception as e:
        log.warning("queue_idle_deadline_read_failed error=%s", e)
        return None


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


def _collect_processable_pallet_ids(run_pdf: bool) -> tuple[list[int], set[int]]:
    """Devuelve pallets cerrados/listos y separa cuáles solo esperan PDF."""
    from queue_app.models import HUItem, Pallet
    from queue_app.utils import pallets_ready_for_pdf_queryset

    pending_pallet_ids = list(
        Pallet.objects.filter(
            status=Pallet.STATUS_READY,
            items__status=HUItem.STATUS_PENDING,
        )
        .distinct()
        .order_by('id')
        .values_list('id', flat=True)
    )
    pdf_pallet_ids = set(
        pallets_ready_for_pdf_queryset()
        .order_by('id')
        .values_list('id', flat=True)
    ) if run_pdf else set()
    return sorted(set(pending_pallet_ids) | pdf_pallet_ids), pdf_pallet_ids


def _active_pallet_wait_context() -> dict:
    """Describe el pallet abierto cuando el worker continuo espera accion humana."""
    from queue_app.models import HUItem, Pallet

    active = (
        Pallet.objects
        .filter(status=Pallet.STATUS_ACTIVE)
        .order_by('-id')
        .first()
    )
    if not active:
        return {
            'message': 'Modo continuo activo. Escanea HUs para preparar el siguiente pallet.',
            'footer': 'Esperando nuevos HUs.',
            'active_pallet_id': None,
            'active_hu_count': 0,
        }

    hu_count = active.items.count()
    pending_count = active.items.filter(status=HUItem.STATUS_PENDING).count()
    pallet_label = f'P{active.pk:02d}'
    if hu_count:
        return {
            'message': (
                f'Pallet {pallet_label} abierto con {hu_count} HU(s). '
                'Cierra el pallet para continuar el proceso.'
            ),
            'footer': (
                f'Esperando cierre de {pallet_label}. '
                'Usa Nuevo pallet o escanea PALLET.'
            ),
            'active_pallet_id': active.pk,
            'active_hu_count': pending_count,
        }

    return {
        'message': f'Pallet {pallet_label} abierto sin HUs. Escanea para continuar.',
        'footer': f'Esperando HUs en {pallet_label}.',
        'active_pallet_id': active.pk,
        'active_hu_count': 0,
    }


def _mark_pallet_processing_started(pallet) -> None:
    """Marca el inicio del ciclo F1->PDF la primera vez que Celery toma el pallet."""
    if pallet.processing_started_at:
        return

    pallet.processing_started_at = timezone.now()
    pallet.processing_finished_at = None
    pallet.save(update_fields=['processing_started_at', 'processing_finished_at'])


def _mark_pallet_processing_finished(pallet) -> None:
    """Marca fin de ciclo cuando el pallet termina sin pasar por PDF."""
    if not pallet.processing_started_at or pallet.processing_finished_at:
        return

    pallet.processing_finished_at = timezone.now()
    pallet.save(update_fields=['processing_finished_at'])


def _calculate_elapsed_ms(started_at) -> int:
    """Convierte un timestamp de inicio en milisegundos transcurridos."""
    if not started_at:
        return 0
    return max(0, int((timezone.now() - started_at).total_seconds() * 1000))


def _close_sap_after_queue_idle() -> None:
    """Cierra SAP al finalizar la corrida para evitar sesiones zombie por inactividad."""
    if not getattr(settings, 'SAP_CLOSE_WHEN_QUEUE_IDLE', True):
        return

    try:
        from core.sap_client import SAPClient

        closed = SAPClient.close_sessions()
        log.info("process_queue_task sap_idle_close closed=%s", closed)
    except Exception as e:
        log.warning("process_queue_task sap_idle_close_failed error=%s", e)


@shared_task(bind=True, max_retries=0)
def process_queue_task(
    self,
    run_f1=True,
    run_f2=True,
    run_pdf=True,
    continuous=False,
    idle_timeout=None,
):
    """
    Procesa toda la cola en orden deterministico:
    pallet -> todas sus HUs -> ZE16/PDF/impresion -> siguiente pallet.
    """
    from queue_app.models import HUItem, Pallet
    from queue_app.utils import (
        emit_error,
        emit_queue_done,
        emit_queue_status,
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
    idle_deadline = None
    idle_status_sent = False
    idle_timeout = (
        QUEUE_DEFAULT_IDLE_TIMEOUT_SECONDS
        if continuous and idle_timeout is None
        else float(idle_timeout or 0)
    )

    try:
        while True:
            if _stop_requested():
                final = _stopped_result(pallets_processed, hus_processed, errors)
                emit_queue_done(final)
                return final

            pallet_ids, pdf_ready_pallet_ids = _collect_processable_pallet_ids(run_pdf)
            if not pallet_ids:
                _set_processing_pallet_ids([])
                if not continuous or idle_timeout <= 0 or (
                    pallets_processed == 0 and hus_processed == 0 and errors == 0
                ):
                    final_status = 'ok' if errors == 0 else 'error'
                    final_message = (
                        'All pallets were processed and printed successfully'
                        if errors == 0 and (pallets_processed or hus_processed)
                        else (
                            f'Queue finished with {errors} error(s)'
                            if errors
                            else 'No closed pallets ready to process.'
                        )
                    )
                    final = {
                        'status': final_status if (pallets_processed or hus_processed or errors) else 'error',
                        'message': final_message,
                        'pallets_processed': pallets_processed,
                        'hus_processed': hus_processed,
                        'errors': errors,
                    }
                    emit_queue_done(final)
                    log.info("process_queue_task done result=%s", final)
                    return final

                now = time.monotonic()
                if idle_deadline is None:
                    idle_deadline = now + idle_timeout
                    _set_queue_idle_deadline(idle_timeout)
                    idle_status_sent = False
                    log.info("process_queue_task idle_wait timeout=%ss", idle_timeout)

                if not idle_status_sent:
                    context = _active_pallet_wait_context()
                    emit_queue_status(
                        context['message'],
                        badge='EN ESPERA',
                        mode='waiting',
                        footer=context['footer'],
                        active_pallet_id=context['active_pallet_id'],
                        active_hu_count=context['active_hu_count'],
                        remaining_seconds=max(0, int(idle_deadline - now)),
                    )
                    idle_status_sent = True

                if now >= idle_deadline:
                    emit_queue_status(
                        'Tiempo de espera agotado. Cerrando sesion SAP hasta el siguiente inicio.',
                        badge='SAP',
                        mode='waiting',
                        footer='El worker no encontro otro pallet cerrado dentro de la ventana de espera.',
                        remaining_seconds=0,
                    )
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

                time.sleep(QUEUE_IDLE_POLL_SECONDS)
                continue

            idle_deadline = None
            idle_status_sent = False
            _clear_queue_idle_deadline()
            _set_processing_pallet_ids(pallet_ids)
            log.info("process_queue_task batch_start pallets=%s", pallet_ids)

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

                if not item_ids:
                    if pallet_id not in pdf_ready_pallet_ids:
                        continue
                else:
                    _mark_pallet_processing_started(pallet)

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
                    else:
                        _mark_pallet_processing_finished(pallet)

                    emit_queue_status(
                        f'Pallet P{pallet_id:02d} requiere revision: {pallet_errors} HU(s) con error.',
                        badge='REVISION',
                        mode='error',
                        footer=f'Revisa los errores de P{pallet_id:02d} antes de imprimir.',
                    )
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

                    emit_queue_status(
                        f'Pallet P{pallet_id:02d} listo. Generando ZE16/PDF e impresion.',
                        badge='PDF',
                        mode='running',
                        footer=f'Generando e imprimiendo recibo de P{pallet_id:02d}.',
                    )
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
                else:
                    _mark_pallet_processing_finished(pallet)

                pallets_processed += 1
                log.info("process_queue_task pallet_done pallet=%s", pallet_id)

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
        _close_sap_after_queue_idle()
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
    from queue_app.utils import emit_item_update, emit_queue_status, emit_stats_update

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
        item.processing_started_at = timezone.now()
        item.error_msg = ''
        item.pdf_status = ''
        item.pdf_msg = ''
        item.pdf_ms = 0
        item.save(update_fields=[
            'status',
            'processing_started_at',
            'error_msg',
            'pdf_status',
            'pdf_msg',
            'pdf_ms',
        ])
        emit_item_update(item)
        emit_stats_update(item.pallet)

        sap = SAPClient()
        emit_queue_status(
            f'Conectando con SAP para HU {item.hu_code}.',
            badge='SAP',
            mode='running',
            footer=f'Preparando sesion SAP para Pallet P{item.pallet_id:02d}.',
            active_pallet_id=item.pallet_id,
            active_hu_count=1,
        )
        sap.connect()
        log.info("process_hu_item sap_connected hu=%s", item.hu_code)

        res1 = None

        if run_f1:
            emit_queue_status(
                f'HU {item.hu_code}: iniciando F1 Acknowledge.',
                badge='F1',
                mode='running',
                footer='Capturando HU en ZMOVEINBHU.',
                active_pallet_id=item.pallet_id,
                active_hu_count=1,
            )
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
                item.processing_ms = _calculate_elapsed_ms(item.processing_started_at)
                item.error_msg = res1['message']
                item.save(update_fields=[
                    'status',
                    'phase1_msg',
                    'f1_done_at',
                    'processed_at',
                    'processing_ms',
                    'error_msg',
                ])
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
            emit_queue_status(
                f'HU {item.hu_code}: {"F1 listo. " if run_f1 else ""}Iniciando F2 Separazione.',
                badge='F2',
                mode='running',
                footer='Procesando separazione en ZMMTIJSEP.',
                active_pallet_id=item.pallet_id,
                active_hu_count=1,
            )
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
                item.error_msg = res2['message']
        else:
            item.status = HUItem.STATUS_OK

        item.processed_at = timezone.now()
        item.processing_ms = _calculate_elapsed_ms(item.processing_started_at)
        if item.status in (HUItem.STATUS_OK, HUItem.STATUS_DUPLICATE):
            item.error_msg = ''
        elif not item.error_msg:
            item.error_msg = item.phase2_msg or item.phase1_msg or 'Error de procesamiento'
        item.save(update_fields=[
            'status',
            'phase1_msg',
            'phase2_msg',
            'phase2_ms',
            'error_msg',
            'f1_done_at',
            'processed_at',
            'processing_ms',
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
    from queue_app.utils import emit_queue_status, emit_receipt_done

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

        emit_queue_status(
            f'Pallet P{pallet_id:02d}: conectando SAP para ZE16.',
            badge='ZE16',
            mode='running',
            footer='Preparando consulta de receipts.',
            active_pallet_id=pallet_id,
            active_hu_count=hu_count,
        )
        sap = SAPClient()
        sap.connect()
        printed_by = getattr(settings, 'SAP_LOGIN_USER', '') or sap.get_user()

        emit_queue_status(
            f'Pallet P{pallet_id:02d}: consultando receipts en ZE16.',
            badge='ZE16',
            mode='running',
            footer=f'Buscando receipts para {hu_count} HU(s).',
            active_pallet_id=pallet_id,
            active_hu_count=hu_count,
        )
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
        emit_queue_status(
            f'Pallet P{pallet_id:02d}: generando PDF de recibo.',
            badge='PDF',
            mode='running',
            footer=f'Creando recibo con {len(receipts)} receipt(s).',
            active_pallet_id=pallet_id,
            active_hu_count=hu_count,
        )
        pdf_path = PalletReceiptPDF.generate(
            pallet_id=pallet_id,
            origin_label=effective_origin.label,
            receipts=receipts,
            hu_display_map=hu_display_map,
            printed_by=printed_by,
        )
        emit_queue_status(
            f'Pallet P{pallet_id:02d}: enviando PDF a impresora.',
            badge='PRINT',
            mode='running',
            footer='Esperando confirmacion del sistema de impresion.',
            active_pallet_id=pallet_id,
            active_hu_count=hu_count,
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
    pallet.items.all().update(**item_updates)

    pallet.save(update_fields=update_fields)
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


def _mark_item_error(hu_item_id: int, message: str):
    from queue_app.models import HUItem
    from queue_app.utils import emit_item_update, emit_stats_update

    try:
        item = HUItem.objects.select_related('pallet').get(pk=hu_item_id)
        if not item.processing_started_at:
            item.processing_started_at = timezone.now()
        item.status = HUItem.STATUS_ERROR
        item.phase1_msg = message
        item.error_msg = message
        item.processed_at = timezone.now()
        item.processing_ms = _calculate_elapsed_ms(item.processing_started_at)
        item.save(update_fields=[
            'status',
            'phase1_msg',
            'error_msg',
            'processing_started_at',
            'processed_at',
            'processing_ms',
        ])
        emit_item_update(item)
        emit_stats_update(item.pallet)
        return item
    except Exception:
        log.exception("_mark_item_error failed hu_id=%s", hu_item_id)
        return None
