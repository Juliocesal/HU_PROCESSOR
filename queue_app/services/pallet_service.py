import logging

from django.db import connection, transaction

from queue_app.models import HUItem, Pallet
from queue_app.services.queue_runtime import get_processing_pallet_ids, is_queue_locked

log = logging.getLogger(__name__)


def locked_active_pallet() -> Pallet | None:
    """Obtiene el pallet activo bajo bloqueo transaccional y corrige duplicados vacios."""
    active_pallets = list(
        Pallet.objects
        .select_for_update()
        .filter(status=Pallet.STATUS_ACTIVE)
        .order_by('-id')
    )
    if not active_pallets:
        return None

    active = active_pallets[0]
    for stale in active_pallets[1:]:
        if stale.items.exists():
            stale.status = Pallet.STATUS_READY
            stale.save(update_fields=['status'])
            log.warning(
                "active_pallet_duplicate_closed kept=%s closed=%s",
                active.pk,
                stale.pk,
            )
        else:
            stale_id = stale.pk
            stale.delete()
            log.warning(
                "active_pallet_duplicate_empty_deleted kept=%s deleted=%s",
                active.pk,
                stale_id,
            )

    return active


def get_or_create_active_pallet(
    hu_code: str,
    origin,
    *,
    is_queue_locked_func=is_queue_locked,
    get_processing_pallet_ids_func=get_processing_pallet_ids,
) -> Pallet:
    """
    Decide si el HU va al pallet activo o crea uno nuevo segun las reglas actuales.
    """
    if not connection.in_atomic_block:
        with transaction.atomic():
            return get_or_create_active_pallet(
                hu_code,
                origin,
                is_queue_locked_func=is_queue_locked_func,
                get_processing_pallet_ids_func=get_processing_pallet_ids_func,
            )

    active = locked_active_pallet()
    queue_locked = is_queue_locked_func()
    processing_pallet_ids = get_processing_pallet_ids_func() if queue_locked else set()

    if not active:
        return Pallet.objects.create(
            status=Pallet.STATUS_ACTIVE,
            origin_code=origin.code,
        )

    if active.pk in processing_pallet_ids:
        return Pallet.objects.create(
            status=Pallet.STATUS_ACTIVE,
            origin_code=origin.code,
        )

    if not active.items.exists():
        active.origin_code = origin.code
        active.save(update_fields=['origin_code'])
        return active

    if active.origin_code != origin.code or origin.auto_pallet:
        mark_pallet_ready(active)
        return Pallet.objects.create(
            status=Pallet.STATUS_ACTIVE,
            origin_code=origin.code,
        )

    return active


def mark_pallet_ready(pallet: Pallet) -> None:
    """Cierra un pallet con HUs para que Celery pueda tomarlo."""
    if not connection.in_atomic_block:
        with transaction.atomic():
            mark_pallet_ready(pallet)
        return

    pallet = Pallet.objects.select_for_update().get(pk=pallet.pk)
    if pallet.status == Pallet.STATUS_ACTIVE and pallet.items.exists():
        pallet.status = Pallet.STATUS_READY
        pallet.save(update_fields=['status'])


def close_active_pallet_for_processing() -> None:
    """Cierra el pallet abierto actual cuando el usuario inicia el proceso."""
    with transaction.atomic():
        active = locked_active_pallet()
        if active and active.items.exists():
            mark_pallet_ready(active)


def create_next_active_pallet() -> tuple[Pallet | None, Pallet | None]:
    """
    Cierra el pallet activo actual y crea el siguiente.

    Retorna (nuevo_pallet, pallet_vacio) para que la vista mantenga los mismos
    mensajes actuales sin mezclar reglas de dominio con JSON HTTP.
    """
    with transaction.atomic():
        active = locked_active_pallet()

        if active and not active.items.exists():
            return None, active

        if active:
            mark_pallet_ready(active)

        return Pallet.objects.create(status=Pallet.STATUS_ACTIVE), None


def recalculate_pallet_after_hu_delete(pallet: Pallet) -> bool:
    """Limpia el estado PDF si el pallet vuelve a ser imprimible tras borrar una HU."""
    pallet.refresh_from_db()
    if not pallet.items.exists():
        return False

    has_errors = pallet.items.filter(
        status__in=[HUItem.STATUS_ERROR, HUItem.STATUS_HU_NOT_FOUND]
    ).exists()
    if has_errors or pallet.pdf_status == Pallet.PDF_STATUS_OK:
        return False

    if pallet.pdf_status or pallet.pdf_msg or pallet.pdf_ms:
        pallet.pdf_status = ''
        pallet.pdf_msg = ''
        pallet.pdf_ms = 0
        pallet.receipt_done_at = None
        pallet.processing_finished_at = None
        pallet.status = Pallet.STATUS_READY
        pallet.save(update_fields=[
            'pdf_status',
            'pdf_msg',
            'pdf_ms',
            'receipt_done_at',
            'processing_finished_at',
            'status',
        ])
        return True

    return False
