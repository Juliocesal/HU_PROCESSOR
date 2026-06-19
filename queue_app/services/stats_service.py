import time

from django.db.models import Count, Q
from django.utils import timezone

_QUEUE_LOCK_STATS_CACHE_SECONDS = 1.0
_QUEUE_LOCK_STATS_CACHE = {
    'expires_at': 0.0,
    'value': False,
}


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


def _is_queue_locked_for_stats(is_queue_locked_func) -> bool:
    """Cachea brevemente el lock solo para KPIs visuales; no decide procesos."""
    now = time.monotonic()
    if now < _QUEUE_LOCK_STATS_CACHE['expires_at']:
        return bool(_QUEUE_LOCK_STATS_CACHE['value'])

    locked = bool(is_queue_locked_func())
    _QUEUE_LOCK_STATS_CACHE['value'] = locked
    _QUEUE_LOCK_STATS_CACHE['expires_at'] = (
        time.monotonic() + _QUEUE_LOCK_STATS_CACHE_SECONDS
    )
    return locked


def calculate_queue_stats() -> dict:
    """Devuelve los KPIs compartidos por vistas HTTP y eventos WebSocket."""
    from queue_app.models import HUItem, Pallet
    from queue_app.services.queue_runtime import is_queue_locked_quick

    item_counts = HUItem.objects.aggregate(
        total=Count('id'),
        ok=Count('id', filter=Q(status__in=[
            HUItem.STATUS_OK,
            HUItem.STATUS_DUPLICATE,
        ])),
        errors=Count('id', filter=Q(status__in=[
            HUItem.STATUS_ERROR,
            HUItem.STATUS_HU_NOT_FOUND,
        ])),
        pending=Count('id', filter=Q(status=HUItem.STATUS_PENDING)),
        processing=Count('id', filter=Q(status=HUItem.STATUS_PROCESSING)),
    )
    pallets_with_hus = Pallet.objects.filter(items__isnull=False).distinct()
    pallets_total = pallets_with_hus.count()
    pallets_done = pallets_with_hus.filter(processing_finished_at__isnull=False).count()
    visible_pallets = Pallet.objects.exclude(status=Pallet.STATUS_DONE).count()
    timed_pallets = (
        Pallet.objects
        .filter(items__isnull=False, processing_started_at__isnull=False)
        .only('processing_started_at', 'processing_finished_at', 'receipt_done_at')
        .distinct()
    )
    total_processing_seconds = _calculate_pallets_total_processing_seconds(timed_pallets)
    pdf_pending = pallets_ready_for_pdf_queryset().count()
    has_processing_hus = bool(item_counts['processing'])
    is_running = has_processing_hus or _is_queue_locked_for_stats(is_queue_locked_quick)
    # FUTURA BD DE CONSULTA:
    # Estos filtros ya representan WHERE por estado para KPIs/reportes:
    # status OK/duplicate = procesados, status error/hu_not_found = problemas,
    # status pending = pendientes, Pallet.status active = pallets abiertos.
    return {
        'total': item_counts['total'],
        'ok': item_counts['ok'],
        'errors': item_counts['errors'],
        'pending': item_counts['pending'],
        'pallets': visible_pallets,
        'pallets_total': pallets_total,
        'pallets_done': pallets_done,
        'pallets_processing_seconds': total_processing_seconds,
        'pallets_processing_display': _format_duration(total_processing_seconds),
        'pdf_pending': pdf_pending,
        'is_running': is_running,
    }


def calculate_queue_operational_status() -> dict | None:
    """Describe que esta haciendo el worker para reconstruir feedback visual."""
    from queue_app.models import HUItem, Pallet
    from queue_app.services.queue_runtime import (
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
