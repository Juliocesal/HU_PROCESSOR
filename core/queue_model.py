import time
import queue
import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Callable

from core.hu_origins import detect_origin, Origin, resolve_effective_origin
from core.print_watcher import PrintDialogWatcher

log = logging.getLogger(__name__)


@dataclass
class HUItem:
    hu_code:    str
    pallet_id:  int
    origin:     Origin
    added_at:   datetime = field(default_factory=datetime.now)

    status:       str = "pending"
    phase1_msg:   str = ""
    phase2_msg:   str = ""
    phase2_ms:    int = 0
    processed_at: datetime | None = None
    f1_done_at:   datetime | None = None
    receipt_done_at: datetime | None = None

    @property
    def status_display(self) -> str:
        labels = {
            "pending":      "Pendiente",
            "processing":   "Procesando...",
            "ok":           "OK",
            "duplicate":    "Ya procesado",
            "error":        "Error",
            "hu_not_found": "HU no existe",
        }
        return labels.get(self.status, self.status)

    @property
    def f1_display(self) -> str:
        if self.status == "pending":
            return ""
        if self.status == "processing":
            return "..."
        if self.status in ("ok", "duplicate"):
            return self.phase1_msg or "OK"
        return self.phase1_msg or "Error"

    @property
    def f2_display(self) -> str:
        if self.status in ("pending", "processing"):
            return ""
        if self.status == "ok":
            return f"OK ({self.phase2_ms}ms)"
        if self.status == "duplicate":
            return self.phase2_msg or ""
        return self.phase2_msg or ""


class HUQueue:
    """Cola thread-safe de HUs."""

    def __init__(self):
        self._items: list[HUItem] = []
        self._lock  = threading.Lock()
        self._q: queue.Queue[HUItem] = queue.Queue()
        self._cancelled_hus: set[str] = set()
        self._pallet_counter = 0
        self._last_pallet_id = 0
        self._recycled_ids: list[int] = []
        self._pallet_start_times: dict[int, datetime] = {}
        self._pallet_f2_end_times: dict[int, datetime] = {}
        self._pallet_receipt_end_times: dict[int, datetime] = {}

    # ── Gestión de pallets ────────────────────────────────────────────────────

    def new_pallet(self) -> int:
        with self._lock:
            if self._last_pallet_id > 0:
                current_has_items = any(
                    i.pallet_id == self._last_pallet_id for i in self._items
                )
                if not current_has_items:
                    log.info(f"new_pallet — pallet {self._last_pallet_id} ya está vacío, reutilizando")
                    return self._last_pallet_id

            if self._recycled_ids:
                pallet_id = self._recycled_ids.pop(0)
                self._last_pallet_id = pallet_id
                log.info(f"new_pallet id={pallet_id} (reciclado)")
                return pallet_id

            self._pallet_counter += 1
            self._last_pallet_id = self._pallet_counter
            log.info(f"new_pallet id={self._pallet_counter}")
            return self._pallet_counter

    @property
    def current_pallet(self) -> int:
        with self._lock:
            return self._last_pallet_id

    def release_pallet(self, pallet_id: int):
        with self._lock:
            if pallet_id <= 0:
                return
            still_has_items = any(i.pallet_id == pallet_id for i in self._items)
            if still_has_items:
                log.debug(f"release_pallet id={pallet_id} — aún tiene ítems, ignorado")
                return
            if pallet_id not in self._recycled_ids:
                self._recycled_ids.append(pallet_id)
                self._recycled_ids.sort()
                log.info(f"release_pallet id={pallet_id} reciclado={self._recycled_ids}")

    def reprocess_all(self) -> int:
        items_to_requeue = []
        with self._lock:
            self._cancelled_hus.clear()
            count = 0
            for item in self._items:
                if item.status in ("ok", "duplicate", "error"):
                    item.status = "pending"
                    item.phase1_msg = ""
                    item.phase2_msg = ""
                    item.phase2_ms = 0
                    item.processed_at = None
                    item.f1_done_at = None
                    item.receipt_done_at = None
                    count += 1
                    items_to_requeue.append(item)
            self._pallet_start_times.clear()
            self._pallet_f2_end_times.clear()
            self._pallet_receipt_end_times.clear()

        while not self._q.empty():
            try:
                self._q.get_nowait()
            except queue.Empty:
                break

        for item in items_to_requeue:
            self._q.put(item)

        log.info(f"reprocess_all — reiniciados {count} HUs")
        return count

    def set_active_pallet(self, pallet_id: int):
        with self._lock:
            self._last_pallet_id = pallet_id

    # ── Agregar HUs ──────────────────────────────────────────────────────────

    def add_hu(self, hu_code: str) -> "HUItem | None":
        origin = detect_origin(hu_code)
        with self._lock:
            for item in self._items:
                if item.hu_code == hu_code:
                    log.warning(f"hu_duplicate_scan hu={hu_code}")
                    return None

            self._cancelled_hus.discard(hu_code)

            if self._pallet_counter == 0 and not self._recycled_ids:
                self._pallet_counter += 1
                self._last_pallet_id = self._pallet_counter
            else:
                current_has_items = any(
                    i.pallet_id == self._last_pallet_id for i in self._items
                )
                if current_has_items:
                    pallet_origin = None
                    for item in self._items:
                        if item.pallet_id == self._last_pallet_id:
                            pallet_origin = item.origin
                            break

                    if (pallet_origin and origin.code != pallet_origin.code) or origin.auto_pallet:
                        if self._recycled_ids:
                            new_id = self._recycled_ids.pop(0)
                        else:
                            self._pallet_counter += 1
                            new_id = self._pallet_counter
                        self._last_pallet_id = new_id

            pallet_id = self._last_pallet_id

        item = HUItem(hu_code=hu_code, pallet_id=pallet_id, origin=origin)
        with self._lock:
            self._items.append(item)

        self._q.put(item)
        log.info(f"hu_added hu={hu_code} pallet={pallet_id} origin={origin.code}")
        return item

    # ── Acceso a items ────────────────────────────────────────────────────────

    def get_all(self) -> list[HUItem]:
        with self._lock:
            return list(self._items)

    def get_next_pending(self) -> "HUItem | None":
        try:
            item = self._q.get(timeout=0.5)
            with self._lock:
                if item.hu_code in self._cancelled_hus:
                    log.info(f"get_next_pending — skipping cancelled hu={item.hu_code}")
                    self._q.task_done()
                    return None
            return item
        except queue.Empty:
            return None

    def task_done(self):
        self._q.task_done()

    def peek_next_pallet_id(self) -> "int | None":
        try:
            items_snapshot = list(self._q.queue)
            if items_snapshot:
                return items_snapshot[0].pallet_id
            return None
        except Exception:
            return None

    def remove_hu(self, hu_code: str) -> "int | None":
        with self._lock:
            for i, item in enumerate(self._items):
                if item.hu_code == hu_code:
                    pallet_id = item.pallet_id
                    self._items.pop(i)
                    self._cancelled_hus.add(hu_code)
                    log.info(f"remove_hu hu={hu_code} pallet={pallet_id}")
                    return pallet_id
        return None

    # ── Stats ─────────────────────────────────────────────────────────────────

    @property
    def stats(self) -> dict:
        with self._lock:
            total   = len(self._items)
            pending = sum(1 for i in self._items if i.status == "pending")
            ok      = sum(1 for i in self._items if i.status in ("ok", "duplicate"))
            errors  = sum(1 for i in self._items if i.status == "error")
            active_pallets = len({i.pallet_id for i in self._items})
        return {
            "total": total, "pending": pending,
            "ok": ok, "errors": errors, "pallets": active_pallets
        }

    def get_hu_codes_for_pallet(self, pallet_id: int) -> list[str]:
        with self._lock:
            return [i.hu_code for i in self._items if i.pallet_id == pallet_id]

    def get_origin_for_pallet(self, pallet_id: int) -> "Origin | None":
        with self._lock:
            for item in self._items:
                if item.pallet_id == pallet_id:
                    return item.origin
            return None

    def get_all_hu_codes(self) -> list[str]:
        with self._lock:
            return [i.hu_code for i in self._items]

    def get_all_items(self) -> list["HUItem"]:
        with self._lock:
            return list(self._items)

    def get_all_pallet_ids(self) -> list[int]:
        with self._lock:
            return sorted({i.pallet_id for i in self._items})

    def pallet_is_empty(self, pallet_id: int) -> bool:
        with self._lock:
            return not any(i.pallet_id == pallet_id for i in self._items)

    def get_hu_status(self, hu_code: str) -> str | None:
        with self._lock:
            for item in self._items:
                if item.hu_code == hu_code:
                    return item.status
        return None

    def pallet_is_safe_to_delete(self, pallet_id: int) -> bool:
        with self._lock:
            for item in self._items:
                if item.pallet_id == pallet_id:
                    if item.status in ("processing", "ok", "duplicate"):
                        return False
        return True

    def mark_pallet_started(self, pallet_id: int):
        with self._lock:
            if pallet_id not in self._pallet_start_times:
                self._pallet_start_times[pallet_id] = datetime.now()

    def mark_pallet_f2_done(self, pallet_id: int):
        with self._lock:
            if pallet_id not in self._pallet_f2_end_times:
                self._pallet_f2_end_times[pallet_id] = datetime.now()

    def mark_pallet_receipt_done(self, pallet_id: int):
        with self._lock:
            self._pallet_receipt_end_times[pallet_id] = datetime.now()
            now = datetime.now()
            for item in self._items:
                if item.pallet_id == pallet_id:
                    item.receipt_done_at = now

    def get_pallet_processing_time(self, pallet_id: int) -> str | None:
        with self._lock:
            start = self._pallet_start_times.get(pallet_id)
            if not start:
                return None

            hu_codes = [i for i in self._items if i.pallet_id == pallet_id]
            f2_end   = self._pallet_f2_end_times.get(pallet_id)
            receipt_end = self._pallet_receipt_end_times.get(pallet_id)

            if len(hu_codes) == 1 and f2_end:
                end = f2_end
            elif receipt_end:
                end = receipt_end
            elif f2_end:
                end = datetime.now()
            else:
                end = datetime.now()

            delta = (end - start).total_seconds()

            if delta < 60:
                return f"{int(delta)}s"
            else:
                mins = int(delta // 60)
                secs = int(delta % 60)
                return f"{mins}min" if secs == 0 else f"{mins}min {secs}s"

    def clear(self):
        with self._lock:
            self._items.clear()
            self._pallet_counter = 0
            self._last_pallet_id = 0
            self._recycled_ids.clear()
            self._cancelled_hus.clear()
            self._pallet_start_times.clear()
            self._pallet_f2_end_times.clear()
            self._pallet_receipt_end_times.clear()
        while not self._q.empty():
            try:
                self._q.get_nowait()
                self._q.task_done()
            except queue.Empty:
                break


# ══════════════════════════════════════════════════════════════════════════════
#  WORKER
# ══════════════════════════════════════════════════════════════════════════════

class QueueWorker:
    """Worker que consume la cola y ejecuta F1 → F2 → ZE16/ZE16/PDF."""

    def __init__(self, hu_queue: HUQueue):
        self._queue   = hu_queue
        self._running = False
        self._thread: threading.Thread | None = None
        self._sap     = None

        self._resume_event = threading.Event()
        self._resume_event.set()

        self.on_item_update:  Callable | None = None
        self.on_stats_update: Callable | None = None
        self.on_receipt_done:    Callable | None = None
        self.on_pallet_done:  Callable | None = None
        self.on_error:        Callable | None = None

        self.run_f1   = True
        self.run_f2   = True
        self.run_receipt = True
        self._print_watcher = PrintDialogWatcher(idle_timeout=8.0, poll_interval=0.15)

        # ── Timestamps de F2 por pallet ───────────────────────────────────────
        # Clave: pallet_id → datetime del último F2 exitoso de ese pallet.
        # Solo se escribe cuando Phase2Result["status"] == "ok".
        # Se limpia al consumirlo en _handle_pallet_boundary o al reprocesar.
        self._pallet_phase2_ts: dict[int, datetime] = {}

    # ── Ciclo de vida ─────────────────────────────────────────────────────────

    def start(self, run_f1=True, run_f2=True, run_receipt=True, **kwargs):
        if self._running:
            return
        self.run_f1   = run_f1
        self.run_f2   = run_f2
        self.run_receipt = run_receipt
        self._running = True
        self._resume_event.set()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        log.info("queue_worker_started")

    def stop(self):
        self._running = False
        self._resume_event.set()
        log.info("queue_worker_stop_requested")

    def resume(self):
        log.info("queue_worker_resumed_by_ui")
        self._resume_event.set()

    def is_running(self) -> bool:
        return self._running

    def is_paused(self) -> bool:
        return self._running and not self._resume_event.is_set()

    def reset_pallet_timestamps(self) -> None:
        """
        Limpia todos los timestamps de F2 almacenados.
        Llamar siempre antes de reprocess_all() para evitar que ZE16/PDF
        use timestamps de una ejecución anterior.
        """
        self._pallet_phase2_ts.clear()
        log.info("pallet_phase2_timestamps_cleared")

    # ── Emisores ──────────────────────────────────────────────────────────────

    def _emit_item(self, item: HUItem):
        if self.on_item_update:
            self.on_item_update(item)

    def _emit_stats(self):
        if self.on_stats_update:
            self.on_stats_update(self._queue.stats)

    def _pallet_hu_count(self, pallet_id: int) -> int:
        return len(self._queue.get_hu_codes_for_pallet(pallet_id))

    # ── Resolución del timestamp de F2 para ZE16/PDF ──────────────────────────────

    def _get_phase2_ts_for_receipt(self, pallet_id: int) -> datetime:
        """
        Retorna el timestamp de F2 para el pallet dado.

        Si no hay timestamp registrado (todos los HUs fallaron en F2),
        usa datetime.now() - 1min como fallback con warning explícito.
        El timestamp se elimina del dict al consumirse para evitar reúsos.
        """
        ts = self._pallet_phase2_ts.pop(pallet_id, None)
        if ts is not None:
            log.info(
                "phase2_ts_resolved pallet=%d ts=%s",
                pallet_id, ts.strftime("%H:%M:%S"),
            )
            return ts

        fallback = datetime.now() - timedelta(minutes=1)
        log.warning(
            "phase2_ts_missing pallet=%d — usando fallback ts=%s. "
            "Causa probable: todos los HUs del pallet fallaron en F2.",
            pallet_id, fallback.strftime("%H:%M:%S"),
        )
        return fallback

    # ── ZE16 + PDF — pallet multi-HU ─────────────────────────────────────────

    def _ze16_and_print_pallet(
        self,
        pallet_id: int,
        valid_codes: list[str],
        origin: Origin | None,
    ) -> dict:
        from core.ze16_client import ZE16Client, ZE16Error
        from core.pdf_receipt import PalletReceiptPDF

        origin_label = origin.label if origin else "Desconocido"
        log.info(
            "ze16_pdf_pallet start pallet=%d hu_count=%d",
            pallet_id, len(valid_codes),
        )

        try:
            ze16 = ZE16Client(self._sap.session)
            receipts = ze16.get_receipts_for_pallet(valid_codes)
            hu_display_map = {}
            for hu in valid_codes:
                hu_norm = ze16._normalize_hu(hu)
                hu_display = hu_norm.lstrip("0") if hu_norm.lstrip("0").startswith("29") else hu.strip()
                hu_display_map[hu_norm] = hu_display
        except ZE16Error as e:
            log.error("ze16_pdf_pallet ZE16Error pallet=%d error=%s", pallet_id, e)
            return {"status": "error", "message": f"ZE16 Error: {e}", "marked": 0}
        except Exception as e:
            log.error("ze16_pdf_pallet unexpected pallet=%d error=%s", pallet_id, e)
            return {"status": "error", "message": f"ZE16 Error inesperado: {e}", "marked": 0}

        if not receipts:
            log.warning(
                "ze16_pdf_pallet no_receipts pallet=%d codes=%s",
                pallet_id, valid_codes,
            )
            return {
                "status":  "error",
                "message": f"ZE16: sin Receipt IDs para pallet {pallet_id}",
                "marked":  0,
            }

        try:
            pdf_path = PalletReceiptPDF.generate(
                pallet_id=pallet_id,
                origin_label=origin_label,
                receipts=receipts,
                hu_display_map=hu_display_map,
            )
        except Exception as e:
            log.error("ze16_pdf_pallet pdf_error pallet=%d error=%s", pallet_id, e)
            return {"status": "error", "message": f"PDF Error: {e}", "marked": 0}

        printed = PalletReceiptPDF.print_pdf(pdf_path)
        found  = len(receipts)
        total  = len(valid_codes)
        suffix = (
            f" ({found}/{total} con receipt)"
            if found < total
            else ""
        )

        log.info(
            "ze16_pdf_pallet done pallet=%d receipts=%d printed=%s path=%s",
            pallet_id, found, printed, pdf_path,
        )
        return {
            "status":  "ok",
            "message": f"ZE16+PDF OK — {found} recibos{suffix}",
            "marked":  found,
        }

    # ── Fin de pallet ─────────────────────────────────────────────────────────

    def _handle_pallet_boundary(
        self,
        finished_pallet: int,
        next_pallet: int | None,
    ) -> bool:
        """
        Llamado cuando se detecta cambio de pallet o fin de cola.
        Siempre ejecuta ZE16+PDF independientemente del número de HUs.
        """
        if not self.run_f2 or not self.run_receipt:
            return True

        hu_count = self._pallet_hu_count(finished_pallet)
        hu_codes = self._queue.get_hu_codes_for_pallet(finished_pallet)

        log.info(
            "pallet_boundary pallet=%d hu_count=%d → ze16_pdf",
            finished_pallet, hu_count,
        )

        valid_codes = [
            c for c in hu_codes
            if self._queue.get_hu_status(c) in ("ok", "duplicate")
        ]

        if not valid_codes:
            log.warning(
                "pallet_boundary pallet=%d — no valid HUs, skip ZE16",
                finished_pallet,
            )
            self._pallet_phase2_ts.pop(finished_pallet, None)

            if next_pallet is None:
                if self.on_receipt_done:
                    self.on_receipt_done({
                        "status":  "error",
                        "message": "No hay HUs válidos para generar recibos",
                        "marked":  0,
                    })
                return False
            if self.on_pallet_done:
                self.on_pallet_done(finished_pallet)
            return True

        pallet_origin = self._queue.get_origin_for_pallet(finished_pallet)
        effective_origin = (
            resolve_effective_origin(pallet_origin, hu_count)
            if pallet_origin else pallet_origin
        )

        # ZE16+PDF no usa phase2_ts — consumirlo para no acumular
        self._pallet_phase2_ts.pop(finished_pallet, None)

        res = self._ze16_and_print_pallet(
            pallet_id=finished_pallet,
            valid_codes=valid_codes,
            origin=effective_origin,
        )

        self._queue.mark_pallet_receipt_done(finished_pallet)

        if next_pallet is None:
            if self.on_receipt_done:
                self.on_receipt_done(res)
            return False

        if self.on_pallet_done:
            self.on_pallet_done(finished_pallet)
        return True

    # ── Loop principal ────────────────────────────────────────────────────────

    def _run(self):
        import pythoncom
        from core.sap_client import SAPClient, SAPConnectionError

        pythoncom.CoInitialize()
        try:
            self._sap = SAPClient()
            self._sap.connect()
            log.info("queue_worker_sap_connected")

            phase1_setup_done = False
            phase2_setup_done = False
            current_pallet_id: int | None = None

            while self._running:

                item = self._queue.get_next_pending()
                if item is None:
                    stats = self._queue.stats
                    if stats["pending"] == 0 and (stats["ok"] > 0 or stats["errors"] > 0):
                        self._running = False
                    continue

                self._queue.mark_pallet_started(item.pallet_id)

                if current_pallet_id is None:
                    current_pallet_id = item.pallet_id
                elif current_pallet_id != item.pallet_id:
                    current_pallet_id = item.pallet_id

                res1: dict | None = None

                # ── Fase 1: ZMOVEINBHU ────────────────────────────────────────
                if self.run_f1:
                    if not phase1_setup_done:
                        self._sap.setup_phase1(origin=item.origin)
                        phase1_setup_done = True

                    item.status = "processing"
                    self._emit_item(item)
                    self._emit_stats()

                    res1 = self._sap.process_hu_phase1(item.hu_code, origin=item.origin)
                    item.phase1_msg = res1["message"]

                    if res1["status"] in ("error", "hu_not_found"):
                        item.status = "error"
                        item.processed_at = datetime.now()
                        self._emit_item(item)
                        self._emit_stats()
                        self._queue.task_done()
                        log.warning(
                            "hu_skipped_f2 hu=%s reason=%s",
                            item.hu_code, res1["message"],
                        )
                        self._queue.mark_pallet_f2_done(item.pallet_id)

                        next_pallet = self._queue.peek_next_pallet_id()
                        if next_pallet is None or next_pallet != item.pallet_id:
                            should_continue = self._handle_pallet_boundary(
                                item.pallet_id, next_pallet
                            )
                            if not should_continue:
                                self._running = False
                                break
                            current_pallet_id = next_pallet
                        continue

                    if res1["status"] == "duplicate":
                        item.phase1_msg = "Ya se hizo el Acknowledge"
                    item.f1_done_at = datetime.now()

                # ── Fase 2: ZMMTIJSEP ────────────────────────────────────────
                if self.run_f2:
                    if not phase2_setup_done:
                        self._sap.setup_phase2(origin=item.origin)
                        phase2_setup_done = True
                        phase1_setup_done = False

                    res2 = self._sap.process_hu_phase2(
                        item.hu_code,
                        phase2_wait=item.origin.phase2_wait,
                        origin=item.origin,
                    )
                    item.phase2_msg = res2["message"]
                    item.phase2_ms  = res2["duration_ms"]

                    # Guardar timestamp solo si F2 fue exitoso.
                    # El último F2 exitoso del pallet es el que se usa en ZE16/PDF
                    # (sobrescribir está bien — queremos el más reciente).
                    if res2["status"] == "ok":
                        self._pallet_phase2_ts[item.pallet_id] = res2["phase2_ts"]
                        log.debug(
                            "phase2_ts_stored pallet=%d hu=%s ts=%s",
                            item.pallet_id,
                            item.hu_code,
                            res2["phase2_ts"].strftime("%H:%M:%S"),
                        )

                    if res2["status"] == "error":
                        item.status = "error"
                    else:
                        item.status = (
                            "ok"
                            if (not self.run_f1 or (res1 is not None and res1["status"] == "ok"))
                            else "duplicate"
                        )
                else:
                    item.status = "ok"

                item.processed_at = datetime.now()
                if self.run_f2:
                    self._queue.mark_pallet_f2_done(item.pallet_id)

                self._emit_item(item)
                self._emit_stats()
                self._queue.task_done()

                next_pallet     = self._queue.peek_next_pallet_id()
                pallet_boundary = (
                    next_pallet is None or next_pallet != current_pallet_id
                )

                if pallet_boundary:
                    should_continue = self._handle_pallet_boundary(
                        current_pallet_id, next_pallet
                    )
                    if not should_continue:
                        self._running = False
                        break
                    current_pallet_id = next_pallet

        except Exception as e:
            log.exception("queue_worker_fatal_error")
            if self.on_error:
                self.on_error(str(e))
        finally:
            self._running = False
            pythoncom.CoUninitialize()
            log.info("queue_worker_stopped")
