import logging
import time

from django.utils import timezone

from queue_app.models import HUItem
from queue_app.services.stats_service import (
    calculate_queue_operational_status,
    calculate_queue_stats,
)
from queue_app.services.performance_service import log_performance

log = logging.getLogger(__name__)

SNAPSHOT_HU_FIELDS = [
    'id',
    'hu_code',
    'status',
    'phase1_msg',
    'phase2_msg',
    'phase2_ms',
    'pallet_id',
    'origin_code',
    'added_at',
]
SNAPSHOT_PALLET_FIELDS = [
    'pallet__id',
    'pallet__pdf_status',
    'pallet__pdf_msg',
    'pallet__pdf_ms',
    'pallet__processing_started_at',
    'pallet__processing_finished_at',
    'pallet__receipt_done_at',
]


def build_queue_snapshot_payload() -> dict:
    """Arma el mismo estado base que consume la UI al conectar o resincronizar."""
    started_at = time.perf_counter()
    items = (
        HUItem.objects
        .select_related('pallet')
        .only(*(SNAPSHOT_HU_FIELDS + SNAPSHOT_PALLET_FIELDS))
        .order_by('added_at', 'id')
    )
    stats = calculate_queue_stats()
    pallet_cache = {}
    payload = {
        'items': [
            serialize_queue_snapshot_item(item, pallet_cache=pallet_cache)
            for item in items.iterator(chunk_size=500)
        ],
        'stats': stats,
        'queue_status': (
            calculate_queue_operational_status()
            if stats.get('is_running')
            else None
        ),
        'snapshot_at': timezone.now().isoformat(),
    }
    log_performance(
        log,
        'db.build_queue_snapshot',
        started_at,
        items=len(payload['items']),
    )
    return payload


def _snapshot_pallet_payload(item: HUItem, pallet_cache: dict[int, dict]) -> dict:
    """Cachea campos compartidos por todas las HUs del mismo pallet."""
    cached = pallet_cache.get(item.pallet_id)
    if cached is not None:
        return cached

    pallet = item.pallet
    cached = {
        'pdf_status': pallet.pdf_status,
        'pdf_display': pallet.pdf_display,
        'pdf_msg': pallet.pdf_msg,
        'pdf_ms': pallet.pdf_ms,
        'processing_time_display': pallet.processing_time_display,
        'processing_started_at': (
            pallet.processing_started_at.isoformat()
            if pallet.processing_started_at
            else ''
        ),
        'processing_finished_at': (
            pallet.processing_finished_at.isoformat()
            if pallet.processing_finished_at
            else ''
        ),
        'receipt_done_at': (
            pallet.receipt_done_at.isoformat()
            if pallet.receipt_done_at
            else ''
        ),
    }
    pallet_cache[item.pallet_id] = cached
    return cached


def serialize_queue_snapshot_item(item: HUItem, pallet_cache=None) -> dict:
    """Serializa una HU con los campos que ya consume queue.js."""
    if pallet_cache is None:
        pallet_cache = {}

    pallet_payload = _snapshot_pallet_payload(item, pallet_cache)
    return {
        'hu_code': item.hu_code,
        'status': item.status,
        'f1_display': item.f1_display,
        'f2_display': item.f2_display,
        'phase1_msg': item.phase1_msg,
        'phase2_msg': item.phase2_msg,
        'phase2_ms': item.phase2_ms,
        'pallet_id': item.pallet_id,
        'origin_code': item.origin_code,
        **pallet_payload,
    }
