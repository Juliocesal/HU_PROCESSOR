import logging
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from dataclasses import dataclass
from typing import Callable

from django.conf import settings
from django.utils import timezone

from queue_app.services.queue_runtime import (
    QUEUE_DEFAULT_IDLE_TIMEOUT_SECONDS,
    QUEUE_IDLE_POLL_SECONDS,
    QueueOwnershipLost,
    _ensure_queue_ownership,
)
from queue_app.services.performance_service import log_performance
from queue_app.services.sap_com_guard import SAPCOMCircuitBreaker

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class QueueWorkerHooks:
    new_queue_owner_token: Callable
    acquire_queue_lock: Callable
    touch_queue_worker_heartbeat: Callable
    start_queue_worker_heartbeat: Callable
    stop_requested: Callable
    stopped_result: Callable
    collect_processable_pallet_ids: Callable
    set_processing_pallet_ids: Callable
    set_queue_idle_deadline: Callable
    clear_queue_idle_deadline: Callable
    active_pallet_wait_context: Callable
    ensure_queue_ownership: Callable
    mark_pallet_processing_started: Callable
    mark_pallet_processing_finished: Callable
    process_hu_item: Callable
    save_pdf_result: Callable
    run_pallet_boundary: Callable
    queue_lock_owned_by: Callable
    close_sap_after_queue_idle: Callable
    release_queue_lock: Callable
    emit_queue_done: Callable
    emit_pallet_done: Callable
    emit_queue_status: Callable
    emit_receipt_done: Callable


def stopped_result(pallets_processed: int, hus_processed: int, errors: int) -> dict:
    return {
        'status': 'stopped',
        'message': 'Proceso detenido por el usuario.',
        'pallets_processed': pallets_processed,
        'hus_processed': hus_processed,
        'errors': errors,
    }


def collect_processable_pallet_ids(run_pdf: bool) -> tuple[list[int], set[int]]:
    """Devuelve pallets cerrados/listos y separa cuales solo esperan PDF."""
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


def active_pallet_wait_context() -> dict:
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


def mark_pallet_processing_started(
    pallet,
    owner: str | None = None,
    *,
    ensure_queue_ownership_func: Callable | None = None,
) -> None:
    """Marca el inicio del ciclo F1->PDF la primera vez que Celery toma el pallet."""
    if pallet.processing_started_at:
        return

    ensure_queue_ownership_func = ensure_queue_ownership_func or _ensure_queue_ownership
    ensure_queue_ownership_func(owner, 'mark_pallet_processing_started')
    pallet.processing_started_at = timezone.now()
    pallet.processing_finished_at = None
    pallet.save(update_fields=['processing_started_at', 'processing_finished_at'])
    from queue_app.services.stats_service import invalidate_queue_stats_cache

    invalidate_queue_stats_cache()


def mark_pallet_processing_finished(
    pallet,
    owner: str | None = None,
    *,
    ensure_queue_ownership_func: Callable | None = None,
) -> None:
    """Marca fin de ciclo cuando el pallet termina sin pasar por PDF."""
    if not pallet.processing_started_at or pallet.processing_finished_at:
        return

    ensure_queue_ownership_func = ensure_queue_ownership_func or _ensure_queue_ownership
    ensure_queue_ownership_func(owner, 'mark_pallet_processing_finished')
    pallet.processing_finished_at = timezone.now()
    pallet.save(update_fields=['processing_finished_at'])
    from queue_app.services.stats_service import invalidate_queue_stats_cache

    invalidate_queue_stats_cache()


def close_sap_after_queue_idle(
    *,
    logger: logging.Logger | None = None,
    sap_com_breaker: SAPCOMCircuitBreaker | None = None,
    sap_worker_session=None,
) -> None:
    """Cierra SAP al finalizar la corrida para evitar sesiones zombie por inactividad."""
    logger = logger or log
    if not getattr(settings, 'SAP_CLOSE_WHEN_QUEUE_IDLE', True):
        return

    timeout_seconds = max(float(getattr(settings, 'SAP_COM_CONNECT_TIMEOUT_SECONDS', 30)), 1.0)
    started_at = time.perf_counter()
    if sap_worker_session is not None:
        try:
            closed = sap_worker_session.close_current_session(timeout=timeout_seconds)
            log_performance(logger, 'sap.close_worker_session', started_at, closed=closed)
            logger.info("process_queue_task sap_worker_session_close closed=%s", closed)
            return
        except Exception as exc:
            logger.warning("process_queue_task sap_worker_session_close_failed error=%s", exc)
            return

    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix='sap-close-worker')
    try:
        from core.sap_client import SAPClient

        future = executor.submit(SAPClient.close_sessions)
        closed = future.result(timeout=timeout_seconds)
        log_performance(logger, 'sap.close_idle_sessions', started_at, closed=closed)
        logger.info("process_queue_task sap_idle_close closed=%s", closed)
    except FutureTimeoutError:
        elapsed_ms = int((time.perf_counter() - started_at) * 1000)
        timeout_ms = int(timeout_seconds * 1000)
        logger.error(
            "SAP_COM_BLOCKED operation=sap.close_idle_sessions duration_ms=%s timeout_ms=%s",
            elapsed_ms,
            timeout_ms,
        )
        if sap_com_breaker is not None:
            sap_com_breaker.handle_timeout(
                operation='sap.close_idle_sessions',
                elapsed_ms=elapsed_ms,
                timeout_ms=timeout_ms,
                logger=logger,
                cooldown=False,
                emit_status=False,
            )
    except Exception as e:
        logger.warning("process_queue_task sap_idle_close_failed error=%s", e)
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


def run_queue_worker(
    *,
    request_id: str | None,
    run_f1=True,
    run_f2=True,
    run_pdf=True,
    continuous=False,
    idle_timeout=None,
    force_new_sap_login=False,
    hooks: QueueWorkerHooks,
    logger: logging.Logger | None = None,
):
    """
    Orquesta una corrida completa de cola.

    La tarea Celery conserva los hooks para mantener compatibilidad con tests y
    con imports legacy en queue_app.tasks.
    """
    from queue_app.models import HUItem, Pallet

    logger = logger or log
    worker_started_at = time.perf_counter()
    owner = hooks.new_queue_owner_token('process_queue_task', request_id)
    lock_started_at = time.perf_counter()
    if not hooks.acquire_queue_lock(owner):
        log_performance(logger, 'redis.acquire_queue_lock', lock_started_at, acquired=False)
        message = 'Queue processing already active; start request ignored.'
        logger.warning(message)
        hooks.emit_queue_status(
            message,
            badge='EN CURSO',
            mode='running',
            footer='Se ignoro una solicitud duplicada porque otro worker controla la cola.',
            is_running=True,
        )
        return {'ok': False, 'error': message}
    log_performance(logger, 'redis.acquire_queue_lock', lock_started_at, acquired=True)

    hooks.touch_queue_worker_heartbeat(owner)
    heartbeat_stop, heartbeat_thread = hooks.start_queue_worker_heartbeat(owner)
    pallets_processed = 0
    hus_processed = 0
    errors = 0
    idle_deadline = None
    idle_status_sent = False
    last_empty_poll_metric_at = 0.0
    sap_session = None
    sap_com_fatal = False
    pythoncom = None
    com_initialized = False
    idle_timeout = (
        QUEUE_DEFAULT_IDLE_TIMEOUT_SECONDS
        if continuous and idle_timeout is None
        else float(idle_timeout or 0)
    )

    try:
        import pythoncom as pythoncom_module
        from queue_app.services.hu_processing_service import SAPWorkerSession

        pythoncom = pythoncom_module
        pythoncom.CoInitialize()
        com_initialized = True
        sap_session = SAPWorkerSession(
            status_callback=hooks.emit_queue_status,
            force_new_login_on_first_connect=force_new_sap_login,
        )

        while True:
            hooks.ensure_queue_ownership(owner, 'queue_loop')
            hooks.touch_queue_worker_heartbeat(owner)
            if hooks.stop_requested(owner):
                final = hooks.stopped_result(pallets_processed, hus_processed, errors)
                hooks.ensure_queue_ownership(owner, 'emit_queue_done_stop')
                hooks.emit_queue_done(final)
                return final

            db_started_at = time.perf_counter()
            pallet_ids, pdf_ready_pallet_ids = hooks.collect_processable_pallet_ids(run_pdf)
            poll_now = time.monotonic()
            if pallet_ids or poll_now - last_empty_poll_metric_at >= 30:
                log_performance(
                    logger,
                    'db.collect_processable_pallets',
                    db_started_at,
                    pallets=len(pallet_ids),
                    pdf_ready=len(pdf_ready_pallet_ids),
                )
                if not pallet_ids:
                    last_empty_poll_metric_at = poll_now
            if not pallet_ids:
                hooks.set_processing_pallet_ids([], owner=owner)
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
                    hooks.ensure_queue_ownership(owner, 'emit_queue_done_empty')
                    hooks.emit_queue_done(final)
                    logger.info("process_queue_task done result=%s", final)
                    return final

                now = poll_now
                if idle_deadline is None:
                    idle_deadline = now + idle_timeout
                    hooks.set_queue_idle_deadline(idle_timeout, owner=owner)
                    idle_status_sent = False
                    logger.info("process_queue_task idle_wait timeout=%ss", idle_timeout)

                if not idle_status_sent:
                    hooks.touch_queue_worker_heartbeat(owner)
                    context = hooks.active_pallet_wait_context()
                    hooks.emit_queue_status(
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
                    hooks.ensure_queue_ownership(owner, 'emit_idle_timeout_status')
                    hooks.emit_queue_status(
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
                    hooks.ensure_queue_ownership(owner, 'emit_queue_done_idle_timeout')
                    hooks.emit_queue_done(final)
                    logger.info("process_queue_task done result=%s", final)
                    return final

                time.sleep(QUEUE_IDLE_POLL_SECONDS)
                continue

            idle_deadline = None
            idle_status_sent = False
            hooks.clear_queue_idle_deadline(owner=owner)
            hooks.set_processing_pallet_ids(pallet_ids, owner=owner)
            logger.info("process_queue_task batch_start pallets=%s", pallet_ids)

            for pallet_id in pallet_ids:
                hooks.ensure_queue_ownership(owner, 'pallet_loop')
                hooks.touch_queue_worker_heartbeat(owner)
                if hooks.stop_requested(owner):
                    final = hooks.stopped_result(pallets_processed, hus_processed, errors)
                    hooks.ensure_queue_ownership(owner, 'emit_queue_done_stop_pallet_loop')
                    hooks.emit_queue_done(final)
                    return final

                db_started_at = time.perf_counter()
                pallet = Pallet.objects.get(pk=pallet_id)
                item_ids = list(
                    pallet.items.filter(status=HUItem.STATUS_PENDING)
                    .order_by('added_at', 'id')
                    .values_list('id', flat=True)
                )
                log_performance(
                    logger,
                    'db.fetch_pallet_hus',
                    db_started_at,
                    pallet=pallet_id,
                    hus=len(item_ids),
                )

                if not item_ids:
                    if pallet_id not in pdf_ready_pallet_ids:
                        continue
                else:
                    hooks.mark_pallet_processing_started(pallet, owner=owner)

                logger.info(
                    "process_queue_task pallet_start pallet=%s hu_count=%s",
                    pallet_id,
                    len(item_ids),
                )

                for item_id in item_ids:
                    hooks.ensure_queue_ownership(owner, 'hu_loop')
                    hooks.touch_queue_worker_heartbeat(owner)
                    if hooks.stop_requested(owner):
                        final = hooks.stopped_result(pallets_processed, hus_processed, errors)
                        hooks.ensure_queue_ownership(owner, 'emit_queue_done_stop_hu_loop')
                        hooks.emit_queue_done(final)
                        return final

                    hu_started_at = time.perf_counter()
                    item = hooks.process_hu_item(
                        item_id,
                        run_f1=run_f1,
                        run_f2=run_f2,
                        run_pallet_boundary=False,
                        emit_pallet_completion=False,
                        owner=owner,
                        sap_session=sap_session,
                    )
                    log_performance(
                        logger,
                        'queue.process_hu',
                        hu_started_at,
                        pallet=pallet_id,
                        hu_id=item_id,
                        status=item.status if item else 'missing',
                    )
                    hooks.ensure_queue_ownership(owner, 'hu_processed')
                    hooks.touch_queue_worker_heartbeat(owner)
                    hus_processed += 1
                    if item and item.status in (HUItem.STATUS_ERROR, HUItem.STATUS_HU_NOT_FOUND):
                        errors += 1

                db_started_at = time.perf_counter()
                pallet_errors = pallet.items.filter(
                    status__in=[HUItem.STATUS_ERROR, HUItem.STATUS_HU_NOT_FOUND]
                ).count()
                log_performance(
                    logger,
                    'db.count_pallet_errors',
                    db_started_at,
                    pallet=pallet_id,
                    errors=pallet_errors,
                )
                if pallet_errors:
                    if run_pdf:
                        result = {
                            'status': 'error',
                            'message': f'PDF omitido: {pallet_errors} HU(s) con error en pallet {pallet_id}',
                            'marked': 0,
                        }
                        hooks.save_pdf_result(pallet, result, None, owner=owner)
                        hooks.ensure_queue_ownership(owner, 'emit_receipt_done_error')
                        hooks.emit_receipt_done(pallet_id, result)
                    else:
                        hooks.mark_pallet_processing_finished(pallet, owner=owner)

                    hooks.ensure_queue_ownership(owner, 'emit_queue_status_pallet_errors')
                    hooks.emit_queue_status(
                        f'Pallet P{pallet_id:02d} requiere revision: {pallet_errors} HU(s) con error.',
                        badge='REVISION',
                        mode='error',
                        footer=f'Revisa los errores de P{pallet_id:02d} antes de imprimir.',
                    )
                    logger.warning(
                        "process_queue_task pallet_has_errors pallet=%s errors=%s",
                        pallet_id,
                        pallet_errors,
                    )
                    pallets_processed += 1
                    hooks.ensure_queue_ownership(owner, 'emit_pallet_done_errors')
                    hooks.emit_pallet_done(pallet_id, pallet.items.count())
                    continue

                if run_pdf:
                    if hooks.stop_requested(owner):
                        final = hooks.stopped_result(pallets_processed, hus_processed, errors)
                        hooks.ensure_queue_ownership(owner, 'emit_queue_done_stop_before_pdf')
                        hooks.emit_queue_done(final)
                        return final

                    hooks.ensure_queue_ownership(owner, 'emit_queue_status_pdf')
                    hooks.emit_queue_status(
                        f'Pallet P{pallet_id:02d} listo. Generando ZE16/PDF e impresion.',
                        badge='PDF',
                        mode='running',
                        footer=f'Generando e imprimiendo recibo de P{pallet_id:02d}.',
                    )
                    hooks.touch_queue_worker_heartbeat(owner)
                    boundary_started_at = time.perf_counter()
                    result = hooks.run_pallet_boundary(
                        pallet_id,
                        emit_completion=True,
                        owner=owner,
                        sap_com_breaker=sap_session.com_breaker,
                        sap_worker_session=sap_session,
                    )
                    log_performance(
                        logger,
                        'queue.pallet_boundary',
                        boundary_started_at,
                        pallet=pallet_id,
                        result=result.get('status') if result else 'missing',
                    )
                    hooks.ensure_queue_ownership(owner, 'pallet_boundary_complete')
                    hooks.touch_queue_worker_heartbeat(owner)
                    if result and result.get('status') != 'ok':
                        errors += 1
                        logger.error(
                            "process_queue_task pallet_receipt_failed pallet=%s result=%s",
                            pallet_id,
                            result,
                        )
                        pallets_processed += 1
                        hooks.ensure_queue_ownership(owner, 'emit_pallet_done_receipt_failed')
                        hooks.emit_pallet_done(pallet_id, pallet.items.count())
                        continue
                else:
                    hooks.mark_pallet_processing_finished(pallet, owner=owner)

                pallets_processed += 1
                hooks.ensure_queue_ownership(owner, 'emit_pallet_done')
                hooks.emit_pallet_done(pallet_id, pallet.items.count())
                logger.info("process_queue_task pallet_done pallet=%s", pallet_id)

    except QueueOwnershipLost as e:
        logger.warning("process_queue_task stopped_lost_ownership owner=%s error=%s", owner, e)
        return {'ok': False, 'stopped': True, 'error': str(e)}
    except Exception as e:
        from core.sap_client import SAPCOMFatalBlockedError

        if isinstance(e, SAPCOMFatalBlockedError):
            sap_com_fatal = True
            metrics = sap_session.metrics() if sap_session is not None else {}
            message = (
                'SAP_COM_BLOCKED_FATAL: SAP no respondió después de varios intentos. '
                'Corrida detenida para evitar acumulación de hilos COM.'
            )
            logger.critical(
                "process_queue_task SAP_COM_BLOCKED_FATAL owner=%s error=%s metrics=%s",
                owner,
                e,
                metrics,
            )
            final = {
                'status': 'error',
                'message': message,
                'pallets_processed': pallets_processed,
                'hus_processed': hus_processed,
                'errors': errors + 1,
                'sap_com_metrics': metrics,
            }
            if hooks.queue_lock_owned_by(owner):
                hooks.emit_queue_status(
                    'Corrida detenida porque SAP no respondió.',
                    badge='SAP',
                    mode='error',
                    footer=(
                        'Libera SAP o espera a que termine la otra automatización. '
                        'Luego puedes iniciar una nueva corrida.'
                    ),
                )
                hooks.emit_queue_done(final)
            else:
                logger.warning("process_queue_task fatal_com_done_skipped_lost_ownership owner=%s", owner)
            return final

        logger.exception("process_queue_task fatal_error")
        final = {
            'status': 'error',
            'message': str(e),
            'pallets_processed': pallets_processed,
            'hus_processed': hus_processed,
            'errors': errors + 1,
        }
        if hooks.queue_lock_owned_by(owner):
            hooks.emit_queue_done(final)
        else:
            logger.warning("process_queue_task fatal_done_skipped_lost_ownership owner=%s", owner)
        return final
    finally:
        try:
            if hooks.queue_lock_owned_by(owner) and not sap_com_fatal:
                hooks.close_sap_after_queue_idle(
                    sap_com_breaker=sap_session.com_breaker if sap_session is not None else None,
                    sap_worker_session=sap_session,
                )
            elif sap_com_fatal:
                logger.warning("process_queue_task skip_sap_close_after_com_fatal owner=%s", owner)
            else:
                logger.warning("process_queue_task skip_sap_close_lost_ownership owner=%s", owner)
            heartbeat_stop.set()
            heartbeat_thread.join(timeout=1)
            release_started_at = time.perf_counter()
            hooks.release_queue_lock(owner)
            log_performance(logger, 'redis.release_queue_lock', release_started_at)
            log_performance(
                logger,
                'celery.queue_task_total',
                worker_started_at,
                request_id=request_id,
                pallets=pallets_processed,
                hus=hus_processed,
                errors=errors,
            )
        finally:
            if sap_session is not None:
                sap_session.close()
            if com_initialized and pythoncom is not None:
                pythoncom.CoUninitialize()
