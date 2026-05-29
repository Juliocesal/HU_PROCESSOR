import logging

from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer

log = logging.getLogger(__name__)

QUEUE_UPDATES_GROUP = 'queue_updates'


def calculate_queue_stats() -> dict:
    """Devuelve los KPIs compartidos por vistas HTTP y eventos WebSocket."""
    from queue_app.models import HUItem, Pallet
    from queue_app.tasks import is_queue_locked

    items = HUItem.objects.all()
    pdf_pending = pallets_ready_for_pdf_queryset().count()
    is_running = is_queue_locked() or items.filter(status=HUItem.STATUS_PROCESSING).exists()
    # FUTURA BD DE CONSULTA:
    # Estos filtros ya representan WHERE por estado para KPIs/reportes:
    # status OK/duplicate = procesados, status error/hu_not_found = problemas,
    # status pending = pendientes, Pallet.status active = pallets abiertos.
    return {
        'total': items.count(),
        'ok': items.filter(status__in=[HUItem.STATUS_OK, HUItem.STATUS_DUPLICATE]).count(),
        'errors': items.filter(
            status__in=[HUItem.STATUS_ERROR, HUItem.STATUS_HU_NOT_FOUND]
        ).count(),
        'pending': items.filter(status=HUItem.STATUS_PENDING).count(),
        'pallets': Pallet.objects.exclude(status=Pallet.STATUS_DONE).count(),
        'pdf_pending': pdf_pending,
        'is_running': is_running,
    }


def pallets_ready_for_pdf_queryset():
    """
    Pallets sin HUs pendientes ni fallidas, con HUs OK y recibo PDF pendiente.

    Esta regla permite imprimir un pallet que antes tuvo error si el HU fallido
    fue borrado/corregido y los HUs restantes ya estan OK.
    """
    from queue_app.models import HUItem, Pallet

    blocked_statuses = [
        HUItem.STATUS_PENDING,
        HUItem.STATUS_PROCESSING,
        HUItem.STATUS_ERROR,
        HUItem.STATUS_HU_NOT_FOUND,
    ]

    return (
        Pallet.objects
        .filter(
            status=Pallet.STATUS_READY,
            items__status__in=[HUItem.STATUS_OK, HUItem.STATUS_DUPLICATE],
            receipt_done_at__isnull=True,
        )
        .exclude(items__status__in=blocked_statuses)
        .exclude(pdf_status=Pallet.PDF_STATUS_OK)
        .distinct()
    )


def emit_item_update(item):
    """Emite la representacion actual de una fila HU hacia la UI."""
    from core.hu_origins import detect_origin

    origin = detect_origin(item.hu_code)
    origin_code = item.origin_code or origin.code
    pallet = item.pallet

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
        'pdf_status': pallet.pdf_status,
        'pdf_display': pallet.pdf_display,
        'pdf_msg': pallet.pdf_msg,
        'pdf_ms': pallet.pdf_ms,
        'processing_time_display': pallet.processing_time_display,
        'processing_started_at': pallet.processing_started_at.isoformat() if pallet.processing_started_at else '',
        'processing_finished_at': pallet.processing_finished_at.isoformat() if pallet.processing_finished_at else '',
        'receipt_done_at': pallet.receipt_done_at.isoformat() if pallet.receipt_done_at else '',
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


def emit_hu_deleted(hu_code, pallet_id, pallet_deleted=False, stats=None):
    """Notifica que una HU desaparecio de la cola para sincronizar otras UIs."""
    _send_group('hu_deleted', {
        'type': 'hu_deleted',
        'hu_code': hu_code,
        'pallet_id': pallet_id,
        'pallet_deleted': pallet_deleted,
        'stats': stats or calculate_queue_stats(),
    })


def emit_pallet_deleted(pallet_id, stats=None):
    """Notifica que un pallet completo fue eliminado de la cola."""
    _send_group('pallet_deleted', {
        'type': 'pallet_deleted',
        'pallet_id': pallet_id,
        'stats': stats or calculate_queue_stats(),
    })


def emit_queue_cleared(pallet_id, stats=None):
    """Notifica limpieza total de cola y pallet inicial creado."""
    _send_group('queue_cleared', {
        'type': 'queue_cleared',
        'pallet_id': pallet_id,
        'stats': stats or calculate_queue_stats(),
    })


def emit_receipt_done(pallet_id, result):
    """Emite el resultado ZE16/PDF de un pallet."""
    _send_group('receipt_done', {
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
    })


def emit_queue_done(result):
    """Emite el unico evento final de una corrida de cola."""
    stats = calculate_queue_stats()
    # El evento final se emite justo antes de liberar el lock de Celery; para la
    # UI ya no debe considerarse una corrida activa.
    stats['is_running'] = False
    _send_group('queue_done', {
        'type': 'queue_done',
        'status': result.get('status', 'error'),
        'message': result.get('message', ''),
        'pallets_processed': result.get('pallets_processed', 0),
        'hus_processed': result.get('hus_processed', 0),
        'errors': result.get('errors', 0),
        'stats': stats,
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
