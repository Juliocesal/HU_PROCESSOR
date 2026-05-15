import logging
from datetime import datetime
from celery import shared_task
from django.utils import timezone

log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
#  TAREA PRINCIPAL — procesar un HU completo (F1 → F2 → boundary check)
# ══════════════════════════════════════════════════════════════════════════════

@shared_task(bind=True, max_retries=0)
def process_hu_task(self, hu_item_id: int, run_f1=True, run_f2=True):
    """
    Equivalente al loop _run() del QueueWorker, pero para un solo HUItem.
    Celery lo ejecuta en el worker Windows que tiene SAP GUI abierto.
    """
    import pythoncom
    from core.sap_client import SAPClient, SAPConnectionError
    from core.hu_origins import detect_origin
    from queue_app.models import HUItem, Pallet
    from queue_app.utils import emit_item_update, emit_stats_update

    pythoncom.CoInitialize()
    sap = None

    try:
        # ── Cargar el HUItem desde la DB ──────────────────────────────────────
        try:
            item = HUItem.objects.get(pk=hu_item_id)
        except HUItem.DoesNotExist:
            log.error(f"process_hu_task — HUItem id={hu_item_id} no existe")
            return

        if item.status != HUItem.STATUS_PENDING:
            log.warning(f"process_hu_task — hu={item.hu_code} ya tiene status={item.status}, skip")
            return

        origin = detect_origin(item.hu_code)

        # ── Marcar como procesando ────────────────────────────────────────────
        item.status = HUItem.STATUS_PROCESSING
        item.save(update_fields=['status'])
        emit_item_update(item)
        emit_stats_update(item.pallet)

        # ── Conectar a SAP ────────────────────────────────────────────────────
        sap = SAPClient()
        sap.connect()
        log.info(f"process_hu_task sap_connected hu={item.hu_code}")

        res1 = None

        # ── Fase 1: ZMOVEINBHU ────────────────────────────────────────────────
        if run_f1:
            sap.setup_phase1(origin=origin)
            res1 = sap.process_hu_phase1(item.hu_code, origin=origin)
            item.phase1_msg = res1['message']
            item.f1_done_at = timezone.now()

            if res1['status'] in ('error', 'hu_not_found'):
                item.status = res1['status'] if res1['status'] == 'hu_not_found' else HUItem.STATUS_ERROR
                item.processed_at = timezone.now()
                item.save(update_fields=['status', 'phase1_msg', 'f1_done_at', 'processed_at'])
                emit_item_update(item)
                emit_stats_update(item.pallet)
                log.warning(f"process_hu_task f1_failed hu={item.hu_code} reason={res1['message']}")
                # Revisar si el pallet terminó
                _check_pallet_boundary.delay(item.pallet_id)
                return

            if res1['status'] == 'duplicate':
                item.phase1_msg = 'Ya se hizo el Acknowledge'

        # ── Fase 2: ZMMTIJSEP ────────────────────────────────────────────────
        if run_f2:
            sap.setup_phase2(origin=origin)
            res2 = sap.process_hu_phase2(
                item.hu_code,
                phase2_wait=origin.phase2_wait,
                origin=origin,
            )
            item.phase2_msg = res2['message']
            item.phase2_ms  = res2['duration_ms']

            if res2['status'] == 'ok':
                # Guardar timestamp F2 en el pallet para SP01
                pallet = item.pallet
                pallet.f2_done_at = res2['phase2_ts']
                pallet.save(update_fields=['f2_done_at'])
                item.status = HUItem.STATUS_OK if (not run_f1 or (res1 and res1['status'] == 'ok')) else HUItem.STATUS_DUPLICATE
            else:
                item.status = HUItem.STATUS_ERROR

        else:
            item.status = HUItem.STATUS_OK

        item.processed_at = timezone.now()
        item.save(update_fields=[
            'status', 'phase1_msg', 'phase2_msg',
            'phase2_ms', 'f1_done_at', 'processed_at'
        ])
        emit_item_update(item)
        emit_stats_update(item.pallet)

        # Revisar si el pallet terminó para lanzar SP01/ZE16
        _check_pallet_boundary.delay(item.pallet_id)

    except SAPConnectionError as e:
        log.error(f"process_hu_task sap_error hu_id={hu_item_id} error={e}")
        _mark_item_error(hu_item_id, str(e))
    except Exception as e:
        log.exception(f"process_hu_task fatal_error hu_id={hu_item_id}")
        _mark_item_error(hu_item_id, str(e))
    finally:
        pythoncom.CoUninitialize()


# ══════════════════════════════════════════════════════════════════════════════
#  BOUNDARY — revisar si el pallet terminó y lanzar SP01 o ZE16+PDF
# ══════════════════════════════════════════════════════════════════════════════

@shared_task(bind=True, max_retries=0)
def _check_pallet_boundary(self, pallet_id: int):
    """
    Equivalente a _handle_pallet_boundary() del QueueWorker.
    Se lanza después de cada HU procesado.
    Si todos los HUs del pallet terminaron → lanza sp01_task o ze16_pdf_task.
    """
    from queue_app.models import Pallet, HUItem

    try:
        pallet = Pallet.objects.get(pk=pallet_id)
    except Pallet.DoesNotExist:
        return

    items = pallet.items.all()

    # Si algún HU sigue pendiente o procesando, esperar
    still_running = items.filter(
        status__in=[HUItem.STATUS_PENDING, HUItem.STATUS_PROCESSING]
    ).exists()

    if still_running:
        log.debug(f"_check_pallet_boundary pallet={pallet_id} — aún hay HUs activos")
        return

    # Ya terminaron todos — marcar pallet done
    if pallet.status == Pallet.STATUS_DONE:
        log.debug(f"_check_pallet_boundary pallet={pallet_id} — ya estaba done")
        return

    pallet.status = Pallet.STATUS_DONE
    pallet.save(update_fields=['status'])

    hu_count   = items.count()
    valid_hus  = list(
        items.filter(status__in=[HUItem.STATUS_OK, HUItem.STATUS_DUPLICATE])
              .values_list('hu_code', flat=True)
    )

    if not valid_hus:
        log.warning(f"_check_pallet_boundary pallet={pallet_id} — sin HUs válidos, skip")
        return

    if hu_count >= 2:
        # Pallet multi-HU → ZE16 + PDF
        log.info(f"_check_pallet_boundary pallet={pallet_id} multi_hu={hu_count} → ze16_pdf_task")
        ze16_pdf_task.delay(pallet_id)
    else:
        # Pallet single-HU → SP01
        log.info(f"_check_pallet_boundary pallet={pallet_id} single_hu → sp01_task")
        phase2_ts_str = pallet.f2_done_at.isoformat() if pallet.f2_done_at else None
        sp01_task.delay(pallet_id, phase2_ts_str)


# ══════════════════════════════════════════════════════════════════════════════
#  SP01 — impresión para pallet single-HU
# ══════════════════════════════════════════════════════════════════════════════

@shared_task(bind=True, max_retries=0)
def sp01_task(self, pallet_id: int, phase2_ts_str: str | None):
    """
    Equivalente al bloque single-HU de _handle_pallet_boundary().
    """
    import pythoncom
    from core.sap_client import SAPClient, SAPConnectionError
    from core.hu_origins import detect_origin
    from core.print_watcher import PrintDialogWatcher
    from queue_app.models import Pallet, HUItem
    from queue_app.utils import emit_sp01_done

    pythoncom.CoInitialize()
    try:
        pallet    = Pallet.objects.get(pk=pallet_id)
        hu_codes  = list(
            pallet.items.filter(status__in=[HUItem.STATUS_OK, HUItem.STATUS_DUPLICATE])
                        .values_list('hu_code', flat=True)
        )

        if not hu_codes:
            emit_sp01_done(pallet_id, {'status': 'error', 'message': 'Sin HUs válidos para SP01', 'marked': 0})
            return

        # Reconstruir timestamp de F2
        phase2_ts = datetime.fromisoformat(phase2_ts_str) if phase2_ts_str else datetime.now()

        origin = detect_origin(hu_codes[0])
        watcher = PrintDialogWatcher(idle_timeout=8.0, poll_interval=0.15)

        sap = SAPClient()
        sap.connect()

        result = sap.execute_sp01_for_pallet(
            hu_codes=hu_codes,
            phase2_ts=phase2_ts,
            expected_count=1,
            print_watcher_start=lambda: watcher.start(expected_count=1),
            origin=origin,
        )

        pallet.sp01_done_at = timezone.now()
        pallet.save(update_fields=['sp01_done_at'])

        log.info(f"sp01_task done pallet={pallet_id} result={result}")
        emit_sp01_done(pallet_id, result)

    except Exception as e:
        log.exception(f"sp01_task error pallet={pallet_id}")
        emit_sp01_done(pallet_id, {'status': 'error', 'message': str(e), 'marked': 0})
    finally:
        pythoncom.CoUninitialize()


# ══════════════════════════════════════════════════════════════════════════════
#  ZE16 + PDF — impresión para pallet multi-HU
# ══════════════════════════════════════════════════════════════════════════════

@shared_task(bind=True, max_retries=0)
def ze16_pdf_task(self, pallet_id: int):
    """
    Equivalente al bloque multi-HU de _handle_pallet_boundary().
    """
    import pythoncom
    from core.sap_client import SAPClient
    from core.ze16_client import ZE16Client, ZE16Error
    from core.pdf_receipt import PalletReceiptPDF
    from core.hu_origins import detect_origin, resolve_effective_origin
    from queue_app.models import Pallet, HUItem
    from queue_app.utils import emit_sp01_done

    pythoncom.CoInitialize()
    try:
        pallet    = Pallet.objects.get(pk=pallet_id)
        hu_codes  = list(
            pallet.items.filter(status__in=[HUItem.STATUS_OK, HUItem.STATUS_DUPLICATE])
                        .values_list('hu_code', flat=True)
        )
        hu_count  = len(hu_codes)

        if not hu_codes:
            emit_sp01_done(pallet_id, {'status': 'error', 'message': 'Sin HUs válidos para ZE16', 'marked': 0})
            return

        origin           = detect_origin(hu_codes[0])
        effective_origin = resolve_effective_origin(origin, hu_count)

        sap = SAPClient()
        sap.connect()

        ze16     = ZE16Client(sap.session)
        receipts = ze16.get_receipts_for_pallet(hu_codes)

        if not receipts:
            result = {'status': 'error', 'message': f'ZE16: sin Receipt IDs para pallet {pallet_id}', 'marked': 0}
            emit_sp01_done(pallet_id, result)
            return

        pdf_path = PalletReceiptPDF.generate(
            pallet_id=pallet_id,
            origin_label=effective_origin.label,
            receipts=receipts,
        )
        PalletReceiptPDF.print_pdf(pdf_path)

        found  = len(receipts)
        suffix = f" ({found}/{hu_count} con receipt)" if found < hu_count else ""
        result = {'status': 'ok', 'message': f'ZE16+PDF OK — {found} recibos{suffix}', 'marked': found}

        pallet.sp01_done_at = timezone.now()
        pallet.save(update_fields=['sp01_done_at'])

        log.info(f"ze16_pdf_task done pallet={pallet_id} result={result}")
        emit_sp01_done(pallet_id, result)

    except ZE16Error as e:
        log.error(f"ze16_pdf_task ZE16Error pallet={pallet_id} error={e}")
        emit_sp01_done(pallet_id, {'status': 'error', 'message': f'ZE16 Error: {e}', 'marked': 0})
    except Exception as e:
        log.exception(f"ze16_pdf_task error pallet={pallet_id}")
        emit_sp01_done(pallet_id, {'status': 'error', 'message': str(e), 'marked': 0})
    finally:
        pythoncom.CoUninitialize()


# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS INTERNOS
# ══════════════════════════════════════════════════════════════════════════════

def _mark_item_error(hu_item_id: int, message: str):
    from queue_app.models import HUItem
    from queue_app.utils import emit_item_update, emit_stats_update
    try:
        item = HUItem.objects.get(pk=hu_item_id)
        item.status       = HUItem.STATUS_ERROR
        item.phase1_msg   = message
        item.processed_at = timezone.now()
        item.save(update_fields=['status', 'phase1_msg', 'processed_at'])
        emit_item_update(item)
        emit_stats_update(item.pallet)
    except Exception:
        log.exception(f"_mark_item_error failed hu_id={hu_item_id}")