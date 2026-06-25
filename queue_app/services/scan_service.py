import logging
import time
from dataclasses import dataclass
from typing import Callable

from django.db import transaction

from queue_app.models import HUItem, Pallet, ScanLog
from queue_app.services.performance_service import log_performance

log = logging.getLogger(__name__)


class DuplicateHUScan(Exception):
    """Senal controlada para duplicados detectados antes de crear el HU."""


@dataclass(frozen=True)
class QueuedHUScan:
    """Resultado persistido de un escaneo valido."""

    item: HUItem
    pallet: Pallet


def record_duplicate_scan(hu_code: str, message: str) -> None:
    """Audita duplicados sin convertir una carrera de escaneo en error 500."""
    try:
        ScanLog.objects.create(
            hu_code=hu_code,
            result='duplicate',
            message=message[:255],
        )
    except Exception as exc:
        log.warning("scan_hu duplicate_log_failed hu=%s error=%s", hu_code, exc)


def retryable_scan_error_payload(
    hu_code: str,
    exc: Exception,
    *,
    status=503,
    error_type='scan_error',
    detail_formatter: Callable[[Exception], str] | None = None,
) -> dict:
    """Payload recuperable para conservar lecturas cuando DB/Django falla."""
    log.exception("scan_hu retryable_error hu=%s type=%s", hu_code, error_type)
    message = (
        'No se pudo guardar el HU en este momento. '
        'El navegador lo conservara y lo reintentara automaticamente.'
    )
    detail = detail_formatter(exc) if detail_formatter else str(exc)[:300]
    return {
        'ok': False,
        'error': message,
        'message': message,
        'type': error_type,
        'retryable': True,
        'status': status,
        'detail': detail,
    }


def queue_hu_scan(
    raw: str,
    origin,
    *,
    get_or_create_active_pallet,
    mark_pallet_ready,
    log_label='scan_hu',
) -> QueuedHUScan:
    """Valida duplicado y guarda un HU pendiente dentro del pallet correspondiente."""
    started_at = time.perf_counter()
    with transaction.atomic():
        if HUItem.objects.select_for_update().filter(hu_code=raw).exists():
            ScanLog.objects.create(
                hu_code=raw,
                result='duplicate',
                message='HU ya registrado.',
            )
            log.info("%s duplicate hu=%s", log_label, raw)
            raise DuplicateHUScan('HU ya registrado.')

        pallet = get_or_create_active_pallet(raw, origin)
        item = HUItem.objects.create(
            hu_code=raw,
            pallet=pallet,
            origin_code=origin.code,
            status=HUItem.STATUS_PENDING,
        )
        ScanLog.objects.create(
            hu_code=raw,
            result='queued',
            message=f'Pallet {pallet.pk}',
        )
        if origin.auto_pallet:
            mark_pallet_ready(pallet)

    from queue_app.services.stats_service import invalidate_queue_stats_cache

    invalidate_queue_stats_cache()
    log_performance(
        log,
        'db.queue_hu_scan',
        started_at,
        hu=raw,
        pallet=pallet.pk,
        origin=origin.code,
    )
    return QueuedHUScan(item=item, pallet=pallet)
