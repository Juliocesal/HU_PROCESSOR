import logging
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer

log = logging.getLogger(__name__)


def emit_item_update(item):
    """
    Llamado desde Celery task después de cada cambio en un HUItem.
    Equivale a: self._bridge.item_update.emit(item)
    """
    _send_group('item_update', {
        'type':       'item_update',
        'hu_code':    item.hu_code,
        'status':     item.status,
        'f1_display': item.f1_display,
        'f2_display': item.f2_display,
        'phase1_msg': item.phase1_msg,
        'phase2_msg': item.phase2_msg,
        'phase2_ms':  item.phase2_ms,
        'pallet_id':  item.pallet_id,
    })


def emit_stats_update(pallet):
    """
    Recalcula stats del pallet y los manda al browser.
    Equivale a: self._bridge.stats_update.emit(self._queue.stats)
    """
    from queue_app.models import HUItem
    items = HUItem.objects.all()

    _send_group('stats_update', {
        'type':    'stats_update',
        'total':   items.count(),
        'ok':      items.filter(status__in=['ok', 'duplicate']).count(),
        'errors':  items.filter(status='error').count(),
        'pending': items.filter(status='pending').count(),
        'pallets': items.values('pallet_id').distinct().count(),
    })


def emit_sp01_done(pallet_id, result):
    """
    Equivale a: self._bridge.sp01_done.emit(result)
    """
    _send_group('sp01_done', {
        'type':      'sp01_done',
        'pallet_id': pallet_id,
        'status':    result.get('status', 'error'),
        'message':   result.get('message', ''),
        'marked':    result.get('marked', 0),
    })


def emit_pallet_done(pallet_id, hu_count):
    """
    Equivale a: self._bridge.pallet_done.emit(pid)
    """
    _send_group('pallet_done', {
        'type':      'pallet_done',
        'pallet_id': pallet_id,
        'hu_count':  hu_count,
    })


def emit_error(message):
    """
    Equivale a: self._bridge.error.emit(msg)
    """
    _send_group('error_message', {
        'type':    'error_message',
        'message': message,
    })


def _send_group(event_type, data):
    """Helper interno — manda al grupo de Channels desde código sync (Celery)."""
    try:
        channel_layer = get_channel_layer()
        async_to_sync(channel_layer.group_send)(
            'queue_updates',
            data,
        )
    except Exception as e:
        log.warning(f"emit_{event_type} failed: {e}")