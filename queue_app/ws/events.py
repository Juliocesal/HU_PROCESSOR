import logging
import time

from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer

from queue_app.services.stats_service import (
    calculate_queue_operational_status,
    calculate_queue_stats,
)
from queue_app.services.performance_service import log_performance

log = logging.getLogger(__name__)

QUEUE_UPDATES_GROUP = 'queue_updates'


def _stats(stats_func=None) -> dict:
    return (stats_func or calculate_queue_stats)()


def _operational_status(status_func=None) -> dict | None:
    return (status_func or calculate_queue_operational_status)()


def _send(event_type, data, send_func=None):
    (send_func or _send_group)(event_type, data)


def emit_item_update(item, *, send_func=None):
    """Emite la representacion actual de una fila HU hacia la UI."""
    from core.hu_origins import detect_origin

    origin = detect_origin(item.hu_code)
    origin_code = item.origin_code or origin.code
    pallet = item.pallet

    _send('item_update', {
        'type': 'item_update',
        'hu_code': item.hu_code,
        'status': item.status,
        'f1_display': item.f1_display,
        'f2_display': item.f2_display,
        'phase1_msg': item.phase1_msg,
        'phase2_msg': item.phase2_msg,
        'phase2_ms': item.phase2_ms,
        'pallet_id': item.pallet_id,
        'origin_code': origin_code,
        'pdf_status': pallet.pdf_status,
        'pdf_display': pallet.pdf_display,
        'pdf_msg': pallet.pdf_msg,
        'pdf_ms': pallet.pdf_ms,
        'processing_time_display': pallet.processing_time_display,
        'processing_started_at': pallet.processing_started_at.isoformat() if pallet.processing_started_at else '',
        'processing_finished_at': pallet.processing_finished_at.isoformat() if pallet.processing_finished_at else '',
        'receipt_done_at': pallet.receipt_done_at.isoformat() if pallet.receipt_done_at else '',
    }, send_func)


def emit_stats_update(_pallet=None, *, stats=None, stats_func=None, send_func=None):
    """Emite los KPIs agregados recalculados."""
    _send('stats_update', {
        'type': 'stats_update',
        **(stats if stats is not None else _stats(stats_func)),
    }, send_func)


def emit_pallet_created(pallet, *, stats_func=None, send_func=None):
    """Emite un pallet vacio recien creado a todos los navegadores conectados."""
    _send('pallet_created', {
        'type': 'pallet_created',
        'pallet_id': pallet.pk,
        'origin_code': pallet.origin_code or '',
        'stats': _stats(stats_func),
    }, send_func)


def emit_hu_deleted(
    hu_code,
    pallet_id,
    pallet_deleted=False,
    stats=None,
    *,
    stats_func=None,
    send_func=None,
):
    """Notifica que una HU desaparecio de la cola para sincronizar otras UIs."""
    _send('hu_deleted', {
        'type': 'hu_deleted',
        'hu_code': hu_code,
        'pallet_id': pallet_id,
        'pallet_deleted': pallet_deleted,
        'stats': stats or _stats(stats_func),
    }, send_func)


def emit_pallet_deleted(pallet_id, stats=None, *, stats_func=None, send_func=None):
    """Notifica que un pallet completo fue eliminado de la cola."""
    _send('pallet_deleted', {
        'type': 'pallet_deleted',
        'pallet_id': pallet_id,
        'stats': stats or _stats(stats_func),
    }, send_func)


def emit_queue_cleared(pallet_id, stats=None, *, stats_func=None, send_func=None):
    """Notifica limpieza total de cola y pallet inicial creado."""
    from queue_app.services.queue_runtime import clear_queue_last_status

    clear_queue_last_status()
    _send('queue_cleared', {
        'type': 'queue_cleared',
        'pallet_id': pallet_id,
        'stats': stats or _stats(stats_func),
    }, send_func)


def emit_receipt_done(pallet_id, result, *, stats_func=None, send_func=None):
    """Emite el resultado ZE16/PDF de un pallet."""
    _send('receipt_done', {
        'type': 'receipt_done',
        'pallet_id': pallet_id,
        'status': result.get('status', 'error'),
        'message': result.get('message', ''),
        'marked': result.get('marked', 0),
        'pdf_status': result.get('pdf_status', result.get('status', 'error')),
        'pdf_display': result.get('pdf_display', ''),
        'pdf_msg': result.get('pdf_msg', result.get('message', '')),
        'pdf_ms': result.get('pdf_ms', 0),
        'processing_time_display': result.get('processing_time_display', ''),
        'processing_started_at': result.get('processing_started_at', ''),
        'processing_finished_at': result.get('processing_finished_at', ''),
        'receipt_done_at': result.get('receipt_done_at', ''),
        'stats': _stats(stats_func),
    }, send_func)


def emit_queue_status(
    message,
    *,
    badge='INFO',
    mode='running',
    footer='',
    active_pallet_id=None,
    active_hu_count=0,
    remaining_seconds=None,
    is_running=None,
    stats=None,
    stats_func=None,
    send_func=None,
):
    """Emite un mensaje operativo no invasivo para la barra de progreso."""
    stats = dict(stats) if stats is not None else _stats(stats_func)
    if is_running is not None:
        stats['is_running'] = bool(is_running)

    payload = {
        'type': 'queue_status',
        'message': message,
        'badge': badge,
        'mode': mode,
        'footer': footer or message,
        'active_pallet_id': active_pallet_id,
        'active_hu_count': active_hu_count,
        'remaining_seconds': remaining_seconds,
        'stats': stats,
    }

    try:
        from queue_app.services.queue_runtime import set_queue_last_status

        set_queue_last_status({
            key: value
            for key, value in payload.items()
            if key != 'stats'
        })
    except Exception as exc:
        log.warning("queue_last_status_emit_failed: %s", exc)

    _send('queue_status', payload, send_func)


def emit_current_queue_status_if_available(
    *,
    status_func=None,
    emit_status_func=None,
):
    """Reemite el estado operativo actual, por ejemplo tras escanear otra HU."""
    payload = _operational_status(status_func)
    if not payload:
        return

    target_emit = emit_status_func or emit_queue_status
    target_emit(
        payload['message'],
        badge=payload['badge'],
        mode=payload['mode'],
        footer=payload['footer'],
        active_pallet_id=payload['active_pallet_id'],
        active_hu_count=payload['active_hu_count'],
        remaining_seconds=payload['remaining_seconds'],
    )


def emit_queue_done(result, *, stats_func=None, send_func=None):
    """Emite el unico evento final de una corrida de cola."""
    stats = _stats(stats_func)
    # El evento final se emite justo antes de liberar el lock de Celery; para la
    # UI ya no debe considerarse una corrida activa.
    stats['is_running'] = False
    _send('queue_done', {
        'type': 'queue_done',
        'status': result.get('status', 'error'),
        'message': result.get('message', ''),
        'pallets_processed': result.get('pallets_processed', 0),
        'hus_processed': result.get('hus_processed', 0),
        'errors': result.get('errors', 0),
        'stats': stats,
    }, send_func)


def emit_pallet_done(pallet_id, hu_count, *, stats_func=None, send_func=None):
    """Emite avance visual por pallet sin disparar alertas finales."""
    from queue_app.models import Pallet

    pallet = Pallet.objects.filter(pk=pallet_id).first()
    _send('pallet_done', {
        'type': 'pallet_done',
        'pallet_id': pallet_id,
        'hu_count': hu_count,
        'processing_time_display': pallet.processing_time_display if pallet else '',
        'processing_started_at': (
            pallet.processing_started_at.isoformat()
            if pallet and pallet.processing_started_at
            else ''
        ),
        'processing_finished_at': (
            pallet.processing_finished_at.isoformat()
            if pallet and pallet.processing_finished_at
            else ''
        ),
        'receipt_done_at': (
            pallet.receipt_done_at.isoformat()
            if pallet and pallet.receipt_done_at
            else ''
        ),
        'stats': _stats(stats_func),
    }, send_func)


def _send_group(event_type, data):
    """Envia eventos a Channels desde codigo sincrono, como tareas Celery."""
    started_at = time.perf_counter()
    try:
        channel_layer = get_channel_layer()
        async_to_sync(channel_layer.group_send)(QUEUE_UPDATES_GROUP, data)
        log_performance(
            log,
            'websocket.group_send',
            started_at,
            event=event_type,
            hu=data.get('hu_code'),
            pallet=data.get('pallet_id'),
        )
    except Exception as exc:
        log_performance(log, 'websocket.group_send', started_at, event=event_type, failed=True)
        log.warning("emit_%s failed: %s", event_type, exc)
