"""
Fachada de compatibilidad para utilidades historicas de la cola.

La logica real vive ahora por funcionalidad:
- queue_app.services.stats_service: KPIs, estados operativos y selectores.
- queue_app.ws.events: eventos WebSocket enviados a la UI.
"""

from queue_app.services.stats_service import (
    calculate_queue_operational_status,
    calculate_queue_stats,
    pallets_ready_for_pdf_queryset,
)
from queue_app.ws import events as _events
from queue_app.ws.events import QUEUE_UPDATES_GROUP


def _send_group(event_type, data):
    return _events._send_group(event_type, data)


def emit_item_update(item):
    return _events.emit_item_update(item, send_func=_send_group)


def emit_stats_update(_pallet=None, stats=None):
    return _events.emit_stats_update(
        _pallet,
        stats=stats,
        stats_func=calculate_queue_stats,
        send_func=_send_group,
    )


def emit_pallet_created(pallet):
    return _events.emit_pallet_created(
        pallet,
        stats_func=calculate_queue_stats,
        send_func=_send_group,
    )


def emit_hu_deleted(hu_code, pallet_id, pallet_deleted=False, stats=None):
    return _events.emit_hu_deleted(
        hu_code,
        pallet_id,
        pallet_deleted=pallet_deleted,
        stats=stats,
        stats_func=calculate_queue_stats,
        send_func=_send_group,
    )


def emit_pallet_deleted(pallet_id, stats=None):
    return _events.emit_pallet_deleted(
        pallet_id,
        stats=stats,
        stats_func=calculate_queue_stats,
        send_func=_send_group,
    )


def emit_queue_cleared(pallet_id, stats=None):
    return _events.emit_queue_cleared(
        pallet_id,
        stats=stats,
        stats_func=calculate_queue_stats,
        send_func=_send_group,
    )


def emit_receipt_done(pallet_id, result):
    return _events.emit_receipt_done(
        pallet_id,
        result,
        stats_func=calculate_queue_stats,
        send_func=_send_group,
    )


def emit_queue_status(
    message,
    *,
    badge='INFO',
    mode='running',
    footer='',
    active_pallet_id=None,
    active_hu_code=None,
    active_hu_count=0,
    remaining_seconds=None,
    is_running=None,
    stats=None,
):
    return _events.emit_queue_status(
        message,
        badge=badge,
        mode=mode,
        footer=footer,
        active_pallet_id=active_pallet_id,
        active_hu_code=active_hu_code,
        active_hu_count=active_hu_count,
        remaining_seconds=remaining_seconds,
        is_running=is_running,
        stats=stats,
        stats_func=calculate_queue_stats,
        send_func=_send_group,
    )


def emit_current_queue_status_if_available(stats=None) -> None:
    def emit_status_with_stats(message, **kwargs):
        return emit_queue_status(message, stats=stats, **kwargs)

    return _events.emit_current_queue_status_if_available(
        status_func=calculate_queue_operational_status,
        emit_status_func=emit_status_with_stats,
    )


def emit_queue_done(result):
    return _events.emit_queue_done(
        result,
        stats_func=calculate_queue_stats,
        send_func=_send_group,
    )


def emit_pallet_done(pallet_id, hu_count):
    return _events.emit_pallet_done(
        pallet_id,
        hu_count,
        stats_func=calculate_queue_stats,
        send_func=_send_group,
    )
