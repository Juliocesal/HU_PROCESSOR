import logging

from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer

log = logging.getLogger(__name__)

QUEUE_UPDATES_GROUP = 'queue_updates'


def calculate_queue_stats() -> dict:
    """Devuelve los KPIs compartidos por vistas HTTP y eventos WebSocket."""
    from queue_app.models import HUItem, Pallet

    items = HUItem.objects.all()
    return {
        'total': items.count(),
        'ok': items.filter(status__in=[HUItem.STATUS_OK, HUItem.STATUS_DUPLICATE]).count(),
        'errors': items.filter(
            status__in=[HUItem.STATUS_ERROR, HUItem.STATUS_HU_NOT_FOUND]
        ).count(),
        'pending': items.filter(status=HUItem.STATUS_PENDING).count(),
        'pallets': Pallet.objects.filter(status=Pallet.STATUS_ACTIVE).count(),
    }


def emit_item_update(item):
    """Emite la representacion actual de una fila HU hacia la UI."""
    from core.hu_origins import detect_origin

    origin = detect_origin(item.hu_code)
    origin_code = item.origin_code or origin.code

    _send_group('item_update', {
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
    })


def emit_stats_update(_pallet=None):
    """Emite los KPIs agregados recalculados."""
    _send_group('stats_update', {
        'type': 'stats_update',
        **calculate_queue_stats(),
    })


def emit_pallet_created(pallet):
    """Emite un pallet vacio recien creado a todos los navegadores conectados."""
    _send_group('pallet_created', {
        'type': 'pallet_created',
        'pallet_id': pallet.pk,
        'origin_code': pallet.origin_code or '',
        'stats': calculate_queue_stats(),
    })


def emit_receipt_done(pallet_id, result):
    """Emite el resultado ZE16/PDF de un pallet."""
    _send_group('receipt_done', {
        'type': 'receipt_done',
        'pallet_id': pallet_id,
        'status': result.get('status', 'error'),
        'message': result.get('message', ''),
        'marked': result.get('marked', 0),
    })


def emit_queue_done(result):
    """Emite el unico evento final de una corrida de cola."""
    _send_group('queue_done', {
        'type': 'queue_done',
        'status': result.get('status', 'error'),
        'message': result.get('message', ''),
        'pallets_processed': result.get('pallets_processed', 0),
        'hus_processed': result.get('hus_processed', 0),
        'errors': result.get('errors', 0),
    })


def emit_pallet_done(pallet_id, hu_count):
    """Emite avance visual por pallet sin disparar alertas finales."""
    _send_group('pallet_done', {
        'type': 'pallet_done',
        'pallet_id': pallet_id,
        'hu_count': hu_count,
    })


def emit_error(message):
    """Emite un error general de cola."""
    _send_group('error_message', {
        'type': 'error_message',
        'message': message,
    })


def _send_group(event_type, data):
    """Envia eventos a Channels desde codigo sincrono, como tareas Celery."""
    try:
        channel_layer = get_channel_layer()
        async_to_sync(channel_layer.group_send)(QUEUE_UPDATES_GROUP, data)
    except Exception as exc:
        log.warning("emit_%s failed: %s", event_type, exc)
