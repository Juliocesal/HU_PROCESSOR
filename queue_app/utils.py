import logging

from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from django.utils import timezone

log = logging.getLogger(__name__)

QUEUE_UPDATES_GROUP = 'queue_updates'


def _format_duration(seconds: int) -> str:
    """Convierte segundos acumulados a texto compacto para la UI."""
    safe_seconds = max(0, int(seconds or 0))
    if safe_seconds < 60:
        return f'{safe_seconds}s'

    minutes = safe_seconds // 60
    rest = safe_seconds % 60
    return f'{minutes}min' if rest == 0 else f'{minutes}min {rest}s'


def _calculate_pallets_total_processing_seconds(pallets) -> int:
    """Suma duraciones reales de pallets iniciados; incluye el pallet activo."""
    now = timezone.now()
    total_seconds = 0
    for pallet in pallets:
        if not pallet.processing_started_at:
            continue

        end = pallet.processing_finished_at or pallet.receipt_done_at or now
        total_seconds += max(0, int((end - pallet.processing_started_at).total_seconds()))

    return total_seconds


def calculate_queue_stats() -> dict:
    """Devuelve los KPIs compartidos por vistas HTTP y eventos WebSocket."""
    from queue_app.models import HUItem, Pallet
    from queue_app.tasks import is_queue_locked

    items = HUItem.objects.all()
    pallets_with_hus = Pallet.objects.filter(items__isnull=False).distinct()
    pallets_with_hus_list = list(pallets_with_hus)
    total_processing_seconds = _calculate_pallets_total_processing_seconds(pallets_with_hus_list)
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
        'pallets_total': len(pallets_with_hus_list),
        'pallets_done': sum(1 for pallet in pallets_with_hus_list if pallet.processing_finished_at),
        'pallets_processing_seconds': total_processing_seconds,
        'pallets_processing_display': _format_duration(total_processing_seconds),
        'pdf_pending': pdf_pending,
        'is_running': is_running,
    }


def calculate_queue_operational_status() -> dict | None:
    """Describe que esta haciendo el worker para reconstruir feedback visual."""
    from queue_app.models import HUItem, Pallet
    from queue_app.tasks import (
        get_queue_last_status,
        get_processing_pallet_ids,
        get_queue_idle_remaining_seconds,
        is_queue_locked,
    )

    queue_locked = is_queue_locked()
    if not queue_locked:
        return None

    last_status = get_queue_last_status()
    if last_status:
        if last_status.get('remaining_seconds') is not None:
            last_status['remaining_seconds'] = get_queue_idle_remaining_seconds()
        return last_status

    processing = (
        HUItem.objects
        .filter(status=HUItem.STATUS_PROCESSING)
        .select_related('pallet')
        .order_by('processing_started_at', 'id')
        .first()
    )
    if processing:
        pallet_id = processing.pallet_id
        return {
            'type': 'queue_status',
            'badge': 'PROCESANDO',
            'mode': 'running',
            'message': f'Procesando HU {processing.hu_code} en Pallet P{pallet_id:02d}.',
            'footer': f'SAP esta trabajando con HU {processing.hu_code}.',
            'active_pallet_id': pallet_id,
            'active_hu_count': 1,
            'remaining_seconds': None,
        }

    remaining_seconds = get_queue_idle_remaining_seconds()
    if remaining_seconds is not None:
        active = (
            Pallet.objects
            .filter(status=Pallet.STATUS_ACTIVE)
            .order_by('-id')
            .first()
        )
        if not active:
            return {
                'type': 'queue_status',
                'badge': 'EN ESPERA',
                'mode': 'waiting',
                'message': 'Modo continuo activo. Escanea HUs para preparar el siguiente pallet.',
                'footer': 'Esperando nuevos HUs.',
                'active_pallet_id': None,
                'active_hu_count': 0,
                'remaining_seconds': remaining_seconds,
            }

        hu_count = active.items.count()
        pallet_label = f'P{active.pk:02d}'
        if hu_count:
            return {
                'type': 'queue_status',
                'badge': 'EN ESPERA',
                'mode': 'waiting',
                'message': (
                    f'Pallet {pallet_label} abierto con {hu_count} HU(s). '
                    'Cierra el pallet para continuar el proceso.'
                ),
                'footer': (
                    f'Esperando cierre de {pallet_label}. '
                    'Usa Nuevo pallet o escanea PALLET.'
                ),
                'active_pallet_id': active.pk,
                'active_hu_count': hu_count,
                'remaining_seconds': remaining_seconds,
            }

        return {
            'type': 'queue_status',
            'badge': 'EN ESPERA',
            'mode': 'waiting',
            'message': f'Pallet {pallet_label} abierto sin HUs. Escanea para continuar.',
            'footer': f'Esperando HUs en {pallet_label}.',
            'active_pallet_id': active.pk,
            'active_hu_count': 0,
            'remaining_seconds': remaining_seconds,
        }

    active_pallet_ids = sorted(get_processing_pallet_ids())
    if active_pallet_ids:
        labels = ', '.join(f'P{pallet_id:02d}' for pallet_id in active_pallet_ids)
        return {
            'type': 'queue_status',
            'badge': 'PROCESANDO',
            'mode': 'running',
            'message': f'Worker activo con pallet(s) {labels}.',
            'footer': 'Esperando siguiente actualizacion de SAP/Celery.',
            'active_pallet_id': active_pallet_ids[0],
            'active_hu_count': 0,
            'remaining_seconds': None,
        }

    return {
        'type': 'queue_status',
        'badge': 'PROCESANDO',
        'mode': 'running',
        'message': 'Worker activo. Sincronizando estado de cola.',
        'footer': 'Esperando siguiente actualizacion de SAP/Celery.',
        'active_pallet_id': None,
        'active_hu_count': 0,
        'remaining_seconds': None,
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
            pdf_status='',
        )
        .exclude(items__status__in=blocked_statuses)
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
    from queue_app.tasks import clear_queue_last_status

    clear_queue_last_status()
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
        'stats': calculate_queue_stats(),
    })


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
):
    """Emite un mensaje operativo no invasivo para la barra de progreso."""
    stats = calculate_queue_stats()
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
        from queue_app.tasks import set_queue_last_status

        set_queue_last_status({
            key: value
            for key, value in payload.items()
            if key != 'stats'
        })
    except Exception as exc:
        log.warning("queue_last_status_emit_failed: %s", exc)

    _send_group('queue_status', payload)


def emit_current_queue_status_if_available() -> None:
    """Reemite el estado operativo actual, por ejemplo tras escanear otra HU."""
    payload = calculate_queue_operational_status()
    if not payload:
        return

    emit_queue_status(
        payload['message'],
        badge=payload['badge'],
        mode=payload['mode'],
        footer=payload['footer'],
        active_pallet_id=payload['active_pallet_id'],
        active_hu_count=payload['active_hu_count'],
        remaining_seconds=payload['remaining_seconds'],
    )


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
    from queue_app.models import Pallet

    pallet = Pallet.objects.filter(pk=pallet_id).first()
    _send_group('pallet_done', {
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
        'stats': calculate_queue_stats(),
    })


def _send_group(event_type, data):
    """Envia eventos a Channels desde codigo sincrono, como tareas Celery."""
    try:
        channel_layer = get_channel_layer()
        async_to_sync(channel_layer.group_send)(QUEUE_UPDATES_GROUP, data)
    except Exception as exc:
        log.warning("emit_%s failed: %s", event_type, exc)
