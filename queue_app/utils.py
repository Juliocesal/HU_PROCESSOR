import logging
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer

log = logging.getLogger(__name__)


def calculate_queue_stats() -> dict:
    """Centraliza los KPIs para que HTTP, WebSocket y estado inicial coincidan."""
    from queue_app.models import HUItem, Pallet

    items = HUItem.objects.all()
    return {
        'total': items.count(),
        'ok': items.filter(status__in=['ok', 'duplicate']).count(),
        'errors': items.filter(status__in=['error', 'hu_not_found']).count(),
        'pending': items.filter(status='pending').count(),
        'pallets': Pallet.objects.filter(status=Pallet.STATUS_ACTIVE).count(),
    }


def emit_item_update(item):
    """
    Llamado desde Celery task después de cada cambio en un HUItem.
    Equivale a: self._bridge.item_update.emit(item)
    """
    from core.hu_origins import detect_origin

    # Detectar origin desde el hu_code si no está guardado
    origin = detect_origin(item.hu_code)
    origin_code = item.origin_code if item.origin_code else origin.code

    _send_group('item_update', {
        'type':        'item_update',
        'hu_code':     item.hu_code,
        'status':      item.status,
        'f1_display':  item.f1_display,
        'f2_display':  item.f2_display,
        'phase1_msg':  item.phase1_msg,
        'phase2_msg':  item.phase2_msg,
        'phase2_ms':   item.phase2_ms,
        'pallet_id':   item.pallet_id,
        'origin_code': origin_code,
    })


def emit_stats_update(pallet):
    """
    Recalcula stats del pallet y los manda al browser.
    Equivale a: self._bridge.stats_update.emit(self._queue.stats)
    """
    _send_group('stats_update', {'type': 'stats_update', **calculate_queue_stats()})


def emit_pallet_created(pallet):
    """Notifica a todos los browsers que existe un nuevo pallet vacio."""
    _send_group('pallet_created', {
        'type': 'pallet_created',
        'pallet_id': pallet.pk,
        'origin_code': pallet.origin_code or '',
        'stats': calculate_queue_stats(),
    })


def emit_receipt_done(pallet_id, result):
    """Notifica el resultado de ZE16/PDF para un pallet."""
    _send_group('receipt_done', {
        'type':      'receipt_done',
        'pallet_id': pallet_id,
        'status':    result.get('status', 'error'),
        'message':   result.get('message', ''),
        'marked':    result.get('marked', 0),
    })


def emit_queue_done(result):
    """Notifica una sola vez que toda la corrida de cola terminó."""
    _send_group('queue_done', {
        'type':              'queue_done',
        'status':            result.get('status', 'error'),
        'message':           result.get('message', ''),
        'pallets_processed': result.get('pallets_processed', 0),
        'hus_processed':     result.get('hus_processed', 0),
        'errors':            result.get('errors', 0),
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
