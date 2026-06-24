import logging

from django.utils import timezone

from queue_app.services.queue_runtime import QueueOwnershipLost, _ensure_queue_ownership
from queue_app.services.receipt_service import _run_pallet_boundary

log = logging.getLogger(__name__)


def _calculate_elapsed_ms(started_at) -> int:
    """Convierte un timestamp de inicio en milisegundos transcurridos."""
    if not started_at:
        return 0
    return max(0, int((timezone.now() - started_at).total_seconds() * 1000))


def _process_hu_item(
    hu_item_id: int,
    run_f1=True,
    run_f2=True,
    run_pallet_boundary=True,
    emit_pallet_completion=True,
    owner: str | None = None,
    run_pallet_boundary_func=_run_pallet_boundary,
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

        _ensure_queue_ownership(owner, 'mark_hu_processing')
        item.status = HUItem.STATUS_PROCESSING
        item.processing_started_at = timezone.now()
        item.error_msg = ''
        item.pdf_status = ''
        item.pdf_msg = ''
        item.pdf_ms = 0
        _ensure_queue_ownership(owner, 'save_hu_processing')
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
        _ensure_queue_ownership(owner, 'emit_sap_connect_status')
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
            _ensure_queue_ownership(owner, 'save_f1_result')
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
                _ensure_queue_ownership(owner, 'save_hu_f1_error')
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
                    run_pallet_boundary_func(
                        item.pallet_id,
                        emit_completion=emit_pallet_completion,
                        owner=owner,
                    )
                return item

            if res1['status'] == 'already_separated':
                item.phase1_msg = res1['message']
                log.info("process_hu_item phase1_destination_notice hu=%s", item.hu_code)
            elif res1['status'] == 'duplicate':
                item.phase1_msg = res1['message'] or 'Ya se hizo el Acknowledge'

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
            _ensure_queue_ownership(owner, 'save_f2_result')
            item.phase2_msg = res2['message']
            item.phase2_ms = res2['duration_ms']

            if res2['status'] == 'ok':
                pallet = item.pallet
                pallet.f2_done_at = res2['phase2_ts']
                _ensure_queue_ownership(owner, 'save_pallet_f2_done')
                pallet.save(update_fields=['f2_done_at'])
                item.status = (
                    HUItem.STATUS_OK
                    if (
                        not run_f1
                        or (res1 and res1['status'] in ('ok', 'already_separated'))
                    )
                    else HUItem.STATUS_DUPLICATE
                )
            else:
                item.status = HUItem.STATUS_ERROR
                item.error_msg = res2['message']
        else:
            item.status = (
                HUItem.STATUS_DUPLICATE
                if res1 and res1['status'] == 'duplicate'
                else HUItem.STATUS_OK
            )

        item.processed_at = timezone.now()
        item.processing_ms = _calculate_elapsed_ms(item.processing_started_at)
        if item.status in (HUItem.STATUS_OK, HUItem.STATUS_DUPLICATE):
            item.error_msg = ''
        elif not item.error_msg:
            item.error_msg = item.phase2_msg or item.phase1_msg or 'Error de procesamiento'
        _ensure_queue_ownership(owner, 'save_hu_final_result')
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
            run_pallet_boundary_func(
                item.pallet_id,
                emit_completion=emit_pallet_completion,
                owner=owner,
            )

        return item

    except QueueOwnershipLost:
        log.warning("process_hu_item stopped_lost_ownership hu_id=%s owner=%s", hu_item_id, owner)
        raise
    except SAPConnectionError as e:
        log.error("process_hu_item sap_error hu_id=%s error=%s", hu_item_id, e)
        return _mark_item_error(hu_item_id, str(e), owner=owner)
    except Exception as e:
        log.exception("process_hu_item fatal_error hu_id=%s", hu_item_id)
        return _mark_item_error(hu_item_id, str(e), owner=owner)
    finally:
        pythoncom.CoUninitialize()


def _mark_item_error(hu_item_id: int, message: str, owner: str | None = None):
    from queue_app.models import HUItem
    from queue_app.utils import emit_item_update, emit_stats_update

    try:
        _ensure_queue_ownership(owner, 'mark_item_error')
        item = HUItem.objects.select_related('pallet').get(pk=hu_item_id)
        if not item.processing_started_at:
            item.processing_started_at = timezone.now()
        item.status = HUItem.STATUS_ERROR
        item.phase1_msg = message
        item.error_msg = message
        item.processed_at = timezone.now()
        item.processing_ms = _calculate_elapsed_ms(item.processing_started_at)
        _ensure_queue_ownership(owner, 'save_mark_item_error')
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
