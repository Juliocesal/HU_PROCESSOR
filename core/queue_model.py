"""
core/queue_model.py
Cola de HUs con estados, worker SAP y lógica de pallet.
"""

import time
import queue
import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable

from core.hu_origins import detect_origin, Origin
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
    f1_done_at:   datetime | None = None  # Cuando se completa F1
    sp01_done_at: datetime | None = None  # Cuando se completa SP01 (para el pallet)

    @property
    def status_display(self) -> str:
        labels = {
            "pending":    "Pendiente",
            "processing": "Procesando...",
            "ok":         "OK",
            "duplicate":  "Ya procesado",
            "error":      "Error",
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
        self._pallet_counter = 0       # máximo ID creado hasta ahora
        self._last_pallet_id = 0       # pallet activo actual
        self._recycled_ids: list[int] = []  # IDs liberados, ordenados ascendente
        self._pallet_start_times: dict[int, datetime] = {}      # Cuándo inicia F1 del pallet
        self._pallet_f2_end_times: dict[int, datetime] = {}     # Cuándo termina F2 del pallet
        self._pallet_sp01_end_times: dict[int, datetime] = {}   # Cuándo termina SP01 del pallet

    # ── Gestión de pallets ────────────────────────────────────────────────────

    def new_pallet(self) -> int:
        """
        Crea un nuevo pallet.
        - Si el pallet actual existe y está vacío, lo devuelve sin crear uno nuevo.
        - Si hay IDs reciclados disponibles, reutiliza el menor.
        - Si no, incrementa el contador.
        """
        with self._lock:
            # Si ya hay un pallet activo y está vacío, no crear otro
            if self._last_pallet_id > 0:
                current_has_items = any(
                    i.pallet_id == self._last_pallet_id for i in self._items
                )
                if not current_has_items:
                    log.info(f"new_pallet — pallet {self._last_pallet_id} ya está vacío, reutilizando")
                    return self._last_pallet_id

            # Reutilizar ID reciclado si hay
            if self._recycled_ids:
                pallet_id = self._recycled_ids.pop(0)   # tomar el menor
                self._last_pallet_id = pallet_id
                log.info(f"new_pallet id={pallet_id} (reciclado)")
                return pallet_id

            # Nuevo ID incremental
            self._pallet_counter += 1
            self._last_pallet_id = self._pallet_counter
            log.info(f"new_pallet id={self._pallet_counter}")
            return self._pallet_counter

    @property
    def current_pallet(self) -> int:
        with self._lock:
            return self._last_pallet_id

    def release_pallet(self, pallet_id: int):
        """
        Libera un ID de pallet para reciclarlo.
        Solo libera si el pallet realmente está vacío.
        """
        with self._lock:
            if pallet_id <= 0:
                return
            # No liberar si todavía tiene ítems
            still_has_items = any(i.pallet_id == pallet_id for i in self._items)
            if still_has_items:
                log.debug(f"release_pallet id={pallet_id} — aún tiene ítems, ignorado")
                return
            # Evitar duplicados en la lista de reciclados
            if pallet_id not in self._recycled_ids:
                self._recycled_ids.append(pallet_id)
                self._recycled_ids.sort()
                log.info(f"release_pallet id={pallet_id} reciclado={self._recycled_ids}")

    def set_active_pallet(self, pallet_id: int):
        """Establece el pallet activo sin crear uno nuevo."""
        with self._lock:
            self._last_pallet_id = pallet_id

    # ── Agregar HUs ──────────────────────────────────────────────────────────

    def add_hu(self, hu_code: str) -> "HUItem | None":
        """
        Agrega una HU a la cola.
        Retorna el HUItem creado, o None si el código es duplicado.
        """
        origin = detect_origin(hu_code)

        with self._lock:
            # Verificar duplicado
            for item in self._items:
                if item.hu_code == hu_code:
                    log.warning(f"hu_duplicate_scan hu={hu_code}")
                    return None

            if self._pallet_counter == 0 and not self._recycled_ids:
                # Primer uso absoluto — crear pallet 1
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

                    # Crear nuevo pallet si origen distinto o auto_pallet
                    if (pallet_origin and origin.code != pallet_origin.code) or origin.auto_pallet:
                        # Reutilizar reciclado o incrementar
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
            return self._q.get(timeout=0.5)
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
        """
        Elimina un HU de la lista interna.
        Retorna el pallet_id al que pertenecía, o None si no existía.
        Solo debe llamarse cuando el worker no está activo.
        """
        with self._lock:
            for i, item in enumerate(self._items):
                if item.hu_code == hu_code:
                    pallet_id = item.pallet_id
                    self._items.pop(i)
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
            # Pallets activos = los que tienen al menos 1 ítem
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

    def get_all_pallet_ids(self) -> list[int]:
        """Retorna todos los pallet IDs que tienen al menos 1 HU, ordenados."""
        with self._lock:
            return sorted({i.pallet_id for i in self._items})

    def pallet_is_empty(self, pallet_id: int) -> bool:
        with self._lock:
            return not any(i.pallet_id == pallet_id for i in self._items)

    def mark_pallet_started(self, pallet_id: int):
        """Marca cuándo inicia F1 del pallet."""
        with self._lock:
            if pallet_id not in self._pallet_start_times:
                self._pallet_start_times[pallet_id] = datetime.now()

    def mark_pallet_f2_done(self, pallet_id: int):
        """Marca cuándo termina F2 del pallet (solo la primera vez)."""
        with self._lock:
            # Solo guardar la hora si no existe (PRIMERA vez que F2 termina para este pallet)
            if pallet_id not in self._pallet_f2_end_times:
                self._pallet_f2_end_times[pallet_id] = datetime.now()

    def mark_pallet_sp01_done(self, pallet_id: int):
        """Marca cuándo termina SP01 del pallet."""
        with self._lock:
            self._pallet_sp01_end_times[pallet_id] = datetime.now()
            now = datetime.now()
            for item in self._items:
                if item.pallet_id == pallet_id:
                    item.sp01_done_at = now

    def get_pallet_processing_time(self, pallet_id: int) -> str | None:
        """
        Calcula el tiempo de procesamiento del pallet.
        - Si está en proceso: muestra tiempo transcurrido desde F1 hasta ahora
        - Si terminó F2: muestra tiempo total desde F1 hasta F2
        Retorna: "2min 30s", "45s", o None si no ha iniciado.
        """
        with self._lock:
            start = self._pallet_start_times.get(pallet_id)
            if not start:
                return None

            # Para 1 HU se usa fin de F2; para varios HUs se usa fin de SP01 (si existe), si no, vivo
            hu_codes = [i for i in self._items if i.pallet_id == pallet_id]
            f2_end   = self._pallet_f2_end_times.get(pallet_id)
            sp01_end = self._pallet_sp01_end_times.get(pallet_id)

            if len(hu_codes) == 1 and f2_end:
                end = f2_end
            elif sp01_end:
                end = sp01_end
            elif f2_end:
                end = datetime.now()
            else:
                end = datetime.now()

            delta = (end - start).total_seconds()

            # Formatear: "2min 30s" o "45s"
            if delta < 60:
                return f"{int(delta)}s"
            else:
                mins = int(delta // 60)
                secs = int(delta % 60)
                if secs == 0:
                    return f"{mins}min"
                else:
                    return f"{mins}min {secs}s"

    def clear(self):
        """Limpia la cola completamente (solo cuando no hay worker activo)."""
        with self._lock:
            self._items.clear()
            self._pallet_counter = 0
            self._last_pallet_id = 0
            self._recycled_ids.clear()
            self._pallet_start_times.clear()
            self._pallet_f2_end_times.clear()
            self._pallet_sp01_end_times.clear()
        while not self._q.empty():
            try:
                self._q.get_nowait()
                self._q.task_done()
            except queue.Empty:
                break


class QueueWorker:
    """Worker que consume la cola y ejecuta las fases SAP."""

    def __init__(self, hu_queue: HUQueue):
        self._queue   = hu_queue
        self._running = False
        self._thread: threading.Thread | None = None
        self._sap     = None

        self._resume_event = threading.Event()
        self._resume_event.set()

        self._single_hu_pallets: list[int] = []

        # ── Print dialog watcher ──────────────────────────────────────────────
        # idle_timeout=12.0 → da tiempo suficiente para que SAP lance todos los
        # diálogos de impresión antes de que el watcher se detenga por inactividad
        self._print_watcher = PrintDialogWatcher(idle_timeout=12.0)

        self.on_item_update:  Callable | None = None
        self.on_stats_update: Callable | None = None
        self.on_sp01_done:    Callable | None = None
        self.on_pallet_done:  Callable | None = None
        self.on_error:        Callable | None = None

        self.run_f1   = True
        self.run_f2   = True
        self.run_sp01 = True

    def start(self, run_f1=True, run_f2=True, run_sp01=True, **kwargs):
        if self._running:
            return
        self.run_f1   = run_f1
        self.run_f2   = run_f2
        self.run_sp01 = run_sp01
        self._running = True
        self._single_hu_pallets.clear()
        self._resume_event.set()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        log.info("queue_worker_started dynamic_print_mode")

    def stop(self):
        self._running = False
        self._resume_event.set()
        # NO detener el watcher aquí - dejarlo que se ejecute hasta idle_timeout
        # (los diálogos de SP01 pueden aparecer milisegundos después)
        log.info("queue_worker_stop_requested")

    def resume(self):
        log.info("queue_worker_resumed_by_ui")
        self._resume_event.set()

    def is_running(self) -> bool:
        return self._running

    def is_paused(self) -> bool:
        return self._running and not self._resume_event.is_set()

    def _emit_item(self, item: HUItem):
        if self.on_item_update:
            self.on_item_update(item)

    def _emit_stats(self):
        if self.on_stats_update:
            self.on_stats_update(self._queue.stats)

    def _pallet_hu_count(self, pallet_id: int) -> int:
        return len(self._queue.get_hu_codes_for_pallet(pallet_id))

    def _wait_for_watcher(self, timeout: float = 60.0):
        """
        Espera a que el print_watcher termine de confirmar todos los diálogos.
        Máximo `timeout` segundos para no bloquearse indefinidamente.
        
        El watcher se detiene automáticamente cuando no detecta nuevos diálogos
        durante su idle_timeout (12 segundos).
        
        IMPORTANTE: Este método NO toca el objeto COM de SAP — evita la race
        condition que causaba congelamiento cuando _wait_idle() bloqueaba el
        thread COM mientras el watcher intentaba enviar clicks Win32.
        """
        log.info("waiting_for_print_watcher...")
        deadline = time.time() + timeout
        while self._print_watcher.is_running() and time.time() < deadline:
            time.sleep(0.2)
        log.info(
            f"print_watcher_finished confirmed={self._print_watcher.confirmed_count}"
        )

    def _handle_pallet_boundary(self, finished_pallet: int,
                                 next_pallet: int | None) -> bool:
        if not self.run_f2 or not self.run_sp01:
            return True

        hu_count = self._pallet_hu_count(finished_pallet)
        hu_codes = self._queue.get_hu_codes_for_pallet(finished_pallet)

        if hu_count >= 2:
            log.info(f"pallet_boundary multi_hu pallet={finished_pallet} "
                     f"count={hu_count} hu_codes={hu_codes}")

            # CAMBIO: El watcher se inicia ahora DENTRO de sap_client, justo antes
            # de presionar el botón de impresión, no aquí al principio del SP01.
            # Esto evita que se agote el timeout mientras esperamos spools.
            
            res_sp01 = self._sap.execute_sp01_for_pallet(
                hu_codes,
                expected_count=len(hu_codes),
                print_watcher_start=self._print_watcher.start,
            )
            
            # Esperar a que el watcher confirme todos los diálogos
            self._wait_for_watcher()

            log.info(f"sp01_pallet_result={res_sp01} "
                     f"print_dialogs_confirmed={self._print_watcher.confirmed_count}")

            # Registrar fin de SP01 para pallet multi-HU
            self._queue.mark_pallet_sp01_done(finished_pallet)

            if next_pallet is None:
                self._flush_single_hu_pallets(extra_result=res_sp01)
                return False

            self._resume_event.clear()
            if self.on_pallet_done:
                self.on_pallet_done(finished_pallet)
            return True

        else:
            self._single_hu_pallets.append(finished_pallet)
            log.info(f"pallet_boundary single_hu pallet={finished_pallet} "
                     f"accumulated={self._single_hu_pallets}")

            if next_pallet is None:
                self._flush_single_hu_pallets(extra_result=None)
                return False

            return True

    def _flush_single_hu_pallets(self, extra_result: dict | None):
        if not self._single_hu_pallets:
            result = extra_result or {"status": "ok", "message": "Cola completada", "marked": 0}
            if self.on_sp01_done:
                self.on_sp01_done(result)
            return

        all_codes: list[str] = []
        for pid in self._single_hu_pallets:
            all_codes.extend(self._queue.get_hu_codes_for_pallet(pid))

        log.info(f"flush_single_hu_pallets pallets={self._single_hu_pallets} hu_codes={all_codes}")

        # CAMBIO: El watcher se inicia ahora DENTRO de sap_client, justo antes del botón
        res = self._sap.execute_sp01(all_codes, print_watcher_start=self._print_watcher.start)
        
        # Esperar al watcher (sin race condition COM)
        self._wait_for_watcher()

        log.info(f"sp01_flush_result={res} "
                 f"print_dialogs_confirmed={self._print_watcher.confirmed_count}")

        # Los pallets single-HU ya usan F2 para tiempo; no cambiar fin aquí.
        # (SP01 ya se hizo como lote, pero cuando el usuario pide stop en F2.)
        for pid in self._single_hu_pallets:
            self._queue.mark_pallet_sp01_done(pid)

        if extra_result and extra_result.get("status") == "ok":
            combined_marked = extra_result.get("marked", 0) + res.get("marked", 0)
            combined = {
                "status":  "ok" if res.get("status") == "ok" else "error",
                "message": f"{extra_result['message']} · {res['message']}",
                "marked":  combined_marked,
            }
        else:
            combined = res

        if self.on_sp01_done:
            self.on_sp01_done(combined)

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

                if not self._resume_event.is_set():
                    self._resume_event.wait()
                    if not self._running:
                        break
                    phase1_setup_done = False
                    phase2_setup_done = False
                    log.info("queue_worker_resumed_processing")

                item = self._queue.get_next_pending()
                if item is None:
                    stats = self._queue.stats
                    if stats["pending"] == 0 and (stats["ok"] > 0 or stats["errors"] > 0):
                        if self._single_hu_pallets and self.run_sp01 and self.run_f2:
                            log.info("queue_empty_flush_accumulated_single_hu_pallets")
                            self._flush_single_hu_pallets(extra_result=None)
                        self._running = False
                    continue

                # Marcar tiempo de inicio del pallet la primera vez que se procesa cualquier HU
                self._queue.mark_pallet_started(item.pallet_id)

                if current_pallet_id is None:
                    current_pallet_id = item.pallet_id
                elif current_pallet_id != item.pallet_id:
                    current_pallet_id = item.pallet_id

                if self.run_f1:
                    if not phase1_setup_done:
                        self._sap.setup_phase1()
                        phase1_setup_done = True

                    item.status = "processing"
                    self._emit_item(item)
                    self._emit_stats()

                    res1 = self._sap.process_hu_phase1(item.hu_code)
                    item.phase1_msg = res1["message"]

                    if res1["status"] == "error":
                        item.status = "error"
                        item.processed_at = datetime.now()
                        self._emit_item(item)
                        self._emit_stats()
                        self._queue.task_done()
                        log.warning(f"hu_skipped_f2 hu={item.hu_code} reason={res1['message']}")

                        next_pallet = self._queue.peek_next_pallet_id()
                        if next_pallet is None or next_pallet != item.pallet_id:
                            should_continue = self._handle_pallet_boundary(item.pallet_id, next_pallet)
                            if not should_continue:
                                self._running = False
                                break
                            current_pallet_id = next_pallet
                        continue

                    if res1["status"] == "duplicate":
                        item.phase1_msg = "Ya se hizo el Acknowledge"

                    # Marcar cuando se completa F1
                    item.f1_done_at = datetime.now()

                if self.run_f2:
                    if not phase2_setup_done:
                        self._sap.setup_phase2()
                        phase2_setup_done = True
                        phase1_setup_done = False

                    res2 = self._sap.process_hu_phase2(item.hu_code, phase2_wait=item.origin.phase2_wait)
                    item.phase2_msg = res2["message"]
                    item.phase2_ms  = res2.get("duration_ms", 0)

                    if res2["status"] == "error":
                        item.status = "error"
                    else:
                        item.status = (
                            "ok"
                            if (not self.run_f1 or res1["status"] == "ok")
                            else "duplicate"
                        )
                else:
                    item.status = "ok"

                item.processed_at = datetime.now()

                # F2 completado para este HU
                if self.run_f2:
                    self._queue.mark_pallet_f2_done(item.pallet_id)

                self._emit_item(item)
                self._emit_stats()
                self._queue.task_done()

                next_pallet     = self._queue.peek_next_pallet_id()
                pallet_boundary = (next_pallet is None or next_pallet != current_pallet_id)

                if pallet_boundary:
                    should_continue = self._handle_pallet_boundary(current_pallet_id, next_pallet)
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
            # Permitir que el print_watcher se ejecute hasta idle_timeout natural
            # (así captura todos los diálogos de SP01)
            pythoncom.CoUninitialize()
            log.info("queue_worker_stopped")