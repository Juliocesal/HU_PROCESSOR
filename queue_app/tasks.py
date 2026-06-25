import logging

from celery import shared_task

from queue_app.services.hu_processing_service import (
    _calculate_elapsed_ms as _service_calculate_elapsed_ms,
    _mark_item_error as _service_mark_item_error,
    _process_hu_item as _service_process_hu_item,
)
from queue_app.services.queue_runtime import (
    QueueOwnershipLost,
    _acquire_queue_lock,
    _clear_queue_idle_deadline,
    _ensure_queue_ownership,
    _new_queue_owner_token,
    _queue_lock_owned_by,
    _release_queue_lock,
    _set_processing_pallet_ids,
    _set_queue_idle_deadline,
    _start_queue_worker_heartbeat,
    _stop_requested,
    _touch_queue_worker_heartbeat,
    arm_continuous_queue,
    clear_queue_last_status,
    clear_queue_runtime_state,
    disarm_continuous_queue,
    get_processing_pallet_ids,
    get_queue_idle_remaining_seconds,
    get_queue_last_status,
    get_queue_lock_owner,
    get_queue_stop_request_age_seconds,
    get_queue_worker_heartbeat_age_seconds,
    is_continuous_queue_armed,
    is_queue_locked,
    is_queue_stop_requested,
    request_queue_stop,
    set_queue_last_status,
)
from queue_app.services.queue_worker_service import (
    QueueWorkerHooks,
    active_pallet_wait_context as _service_active_pallet_wait_context,
    close_sap_after_queue_idle as _service_close_sap_after_queue_idle,
    collect_processable_pallet_ids as _service_collect_processable_pallet_ids,
    mark_pallet_processing_finished as _service_mark_pallet_processing_finished,
    mark_pallet_processing_started as _service_mark_pallet_processing_started,
    run_queue_worker,
    stopped_result as _service_stopped_result,
)
from queue_app.services.receipt_service import (
    _build_hu_display_map,
    _get_ze16_receipts_with_retries,
    _run_pallet_boundary,
    _save_pdf_result,
    _ze16_receipt_retry_config,
    ze16_pdf_task_sync,
)

log = logging.getLogger(__name__)


def _stopped_result(pallets_processed: int, hus_processed: int, errors: int) -> dict:
    return _service_stopped_result(pallets_processed, hus_processed, errors)


def _collect_processable_pallet_ids(run_pdf: bool) -> tuple[list[int], set[int]]:
    return _service_collect_processable_pallet_ids(run_pdf)


def _active_pallet_wait_context() -> dict:
    return _service_active_pallet_wait_context()


def _mark_pallet_processing_started(pallet, owner: str | None = None) -> None:
    _service_mark_pallet_processing_started(
        pallet,
        owner=owner,
        ensure_queue_ownership_func=_ensure_queue_ownership,
    )


def _mark_pallet_processing_finished(pallet, owner: str | None = None) -> None:
    _service_mark_pallet_processing_finished(
        pallet,
        owner=owner,
        ensure_queue_ownership_func=_ensure_queue_ownership,
    )


def _calculate_elapsed_ms(started_at) -> int:
    """Compatibilidad para tests/imports legacy; la implementacion vive en servicios."""
    return _service_calculate_elapsed_ms(started_at)


def _close_sap_after_queue_idle(sap_com_breaker=None) -> None:
    _service_close_sap_after_queue_idle(logger=log, sap_com_breaker=sap_com_breaker)


def _queue_worker_hooks() -> QueueWorkerHooks:
    from queue_app.utils import (
        emit_pallet_done,
        emit_queue_done,
        emit_queue_status,
        emit_receipt_done,
    )

    return QueueWorkerHooks(
        new_queue_owner_token=_new_queue_owner_token,
        acquire_queue_lock=_acquire_queue_lock,
        touch_queue_worker_heartbeat=_touch_queue_worker_heartbeat,
        start_queue_worker_heartbeat=_start_queue_worker_heartbeat,
        stop_requested=_stop_requested,
        stopped_result=_stopped_result,
        collect_processable_pallet_ids=_collect_processable_pallet_ids,
        set_processing_pallet_ids=_set_processing_pallet_ids,
        set_queue_idle_deadline=_set_queue_idle_deadline,
        clear_queue_idle_deadline=_clear_queue_idle_deadline,
        active_pallet_wait_context=_active_pallet_wait_context,
        ensure_queue_ownership=_ensure_queue_ownership,
        mark_pallet_processing_started=_mark_pallet_processing_started,
        mark_pallet_processing_finished=_mark_pallet_processing_finished,
        process_hu_item=_process_hu_item,
        save_pdf_result=_save_pdf_result,
        run_pallet_boundary=_run_pallet_boundary,
        queue_lock_owned_by=_queue_lock_owned_by,
        close_sap_after_queue_idle=_close_sap_after_queue_idle,
        release_queue_lock=_release_queue_lock,
        emit_queue_done=emit_queue_done,
        emit_pallet_done=emit_pallet_done,
        emit_queue_status=emit_queue_status,
        emit_receipt_done=emit_receipt_done,
    )


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
    return run_queue_worker(
        request_id=self.request.id,
        run_f1=run_f1,
        run_f2=run_f2,
        run_pdf=run_pdf,
        continuous=continuous,
        idle_timeout=idle_timeout,
        hooks=_queue_worker_hooks(),
        logger=log,
    )


@shared_task(bind=True, max_retries=0)
def process_hu_task(self, hu_item_id: int, run_f1=True, run_f2=True):
    """
    Tarea de compatibilidad para una HU. La UI actual inicia process_queue_task.
    """
    owner = _new_queue_owner_token('process_hu_task', self.request.id)
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
            owner=owner,
        )
    except QueueOwnershipLost as e:
        log.warning("process_hu_task stopped_lost_ownership owner=%s hu_id=%s error=%s", owner, hu_item_id, e)
        return {'ok': False, 'stopped': True, 'error': str(e)}
    finally:
        _release_queue_lock(owner)


def _process_hu_item(
    hu_item_id: int,
    run_f1=True,
    run_f2=True,
    run_pallet_boundary=True,
    emit_pallet_completion=True,
    owner: str | None = None,
    sap_session=None,
):
    return _service_process_hu_item(
        hu_item_id,
        run_f1=run_f1,
        run_f2=run_f2,
        run_pallet_boundary=run_pallet_boundary,
        emit_pallet_completion=emit_pallet_completion,
        owner=owner,
        run_pallet_boundary_func=_run_pallet_boundary,
        sap_session=sap_session,
    )


def _mark_item_error(hu_item_id: int, message: str, owner: str | None = None):
    """Compatibilidad para imports legacy; la implementacion vive en servicios."""
    return _service_mark_item_error(hu_item_id, message, owner=owner)
