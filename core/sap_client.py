"""
core/sap_client.py — flujo idéntico al VBA original, con tiempos de espera mayores.
"""

import time
import logging
import pythoncom
import win32com.client
from typing import Callable

log = logging.getLogger(__name__)

SISTEMA_SAP    = "LUP"
TX_MOVEINBHU   = "/nZMOVEINBHU"
TX_TIJSEP      = "/nZMMTIJSEP"
TX_SP01        = "/nSP01"
NODE_MOVEINBHU = "F00098"
NODE_TIJSEP    = "F00097"

# Tiempos — equivalentes al VBA (WAIT_LONG=2s, WAIT_SHORT=1s)
WAIT_LONG      = 1.0
WAIT_SHORT     = 1.0
WAIT_TREE      = 3.0   # espera extra tras navegar, para que cargue el árbol interno
TIMEOUT_SAP    = 15.0
POLL_INTERVAL  = 0.05
WAIT_SP01_REFRESH = 3.0   # segundos entre recargas mientras espera spools

# IDs exactos del VBA
TREE_PATH    = "wnd[0]/usr/cntlIMAGE_CONTAINER/shellcont/shell/shellcont[0]/shell"
FIELD_F1_HU  = "wnd[0]/usr/ctxtP_HU"
FIELD_F2_HU  = "wnd[0]/usr/txtGV_HU"
SP01_REFRESH_BTN = "wnd[0]/tbar[1]/btn[45]"

# ALV grid de ZMOVEINBHU (donde aparecen los mensajes de resultado)
ALV_GRID_PATH = "wnd[0]/usr/cntlCC_ALV/shellcont/shell"

# Columnas conocidas del ALV de ZMOVEINBHU (en orden de prioridad)
ALV_MSG_COLUMNS = ["MESSAGE", "MSG_TEXT", "TEXT", "MSGTEXT", "DESCRIPTION", "MAKTX"]

# Icono SAP: @5B@ = éxito (verde), @5C@ = error (rojo)
SAP_ICON_OK    = "@5B@"
SAP_ICON_ERROR = "@5C@"

# Keywords que indican error en el mensaje del ALV
ALV_ERROR_KEYWORDS = [
    "deficit", "error", "not found", "no existe", "bloqueado",
    "locked", "incorrect", "invalid", "no se encontró",
    "quantity", "cantidad", "warehouse", "stock",
]


class SAPConnectionError(Exception):
    pass


class SAPClient:

    def __init__(self):
        self._session = None
        self._usuario = ""

    # ── Conexión ──────────────────────────────────────────────────────────────

    def connect(self, sistema: str = SISTEMA_SAP) -> bool:
        pythoncom.CoInitialize()
        try:
            sap_gui = win32com.client.GetObject("SAPGUI")
            app = sap_gui.GetScriptingEngine
        except Exception as e:
            raise SAPConnectionError(f"No se pudo obtener SAPGUI: {e}")

        if int(app.Children.Count) == 0:
            raise SAPConnectionError("No hay conexiones activas en SAP GUI")

        for i_conn in range(int(app.Children.Count)):
            conn = app.Children(i_conn)
            for i_sess in range(int(conn.Children.Count)):
                sess = conn.Children(i_sess)
                try:
                    if sess.Info.SystemName.upper().strip() == sistema.upper():
                        self._session = sess
                        self._usuario = sess.Info.User
                        log.info(f"sap_connected system={sistema} user={self._usuario}")
                        return True
                except Exception:
                    continue

        raise SAPConnectionError(f"No se encontró sesión {sistema} activa")

    @property
    def session(self):
        if self._session is None:
            raise SAPConnectionError("Sin sesión SAP.")
        return self._session

    def get_user(self) -> str:
        return self._usuario

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _find(self, element_id: str):
        """findById seguro — devuelve None en lugar de lanzar excepción."""
        try:
            return self.session.findById(element_id)
        except Exception:
            return None

    def _wait_idle(self, timeout: float = TIMEOUT_SAP) -> bool:
        deadline = time.time() + timeout
        while True:
            try:
                if not self._session.Busy:
                    return True
            except Exception:
                return True
            if time.time() > deadline:
                return False
            time.sleep(POLL_INTERVAL)

    def _get_sbar_text(self) -> str:
        obj = self._find("wnd[0]/sbar")
        try:
            return obj.Text if obj else ""
        except Exception:
            return ""

    def _get_popup(self):
        return self._find("wnd[1]")

    def _close_all_popups(self) -> None:
        for _ in range(3):
            popup = self._get_popup()
            if popup is None:
                break
            try:
                popup.sendVKey(12)
                time.sleep(0.3)
                self._wait_idle(timeout=3.0)
            except Exception:
                break

    # ── Lectura del ALV grid de ZMOVEINBHU ───────────────────────────────────

    def _get_alv_message(self) -> str:
        """
        Lee el mensaje de resultado de la primera fila del ALV grid de ZMOVEINBHU.

        El grid muestra:
          - Columna ICON : @5B@ (éxito verde) o @5C@ (error rojo)
          - Columna de texto: descripción completa del resultado
            p.ej. "Packaging data was saved. Material document 7570516200 was created"
            o     "Deficit of BA Unrestricted-use 10.00 NR : 0RX3582V MX03 Z322 0107155709"

        Replica la lógica del VBS:
            shell.currentCellRow = -1
            shell.selectColumn "ICON"
        """
        try:
            shell = self._find(ALV_GRID_PATH)
            if shell is None:
                log.debug("alv_grid_not_found — usando sbar como fallback")
                return ""

            # Seleccionar primera fila (equivalente al VBS currentCellRow = -1)
            try:
                shell.currentCellRow = -1
            except Exception:
                pass

            # Intentar leer el icono primero para incluirlo en el mensaje
            icon_val = ""
            try:
                icon_val = str(shell.getCellValue(0, "ICON")).strip()
            except Exception:
                pass

            # Leer columna de descripción en orden de prioridad
            msg_text = ""
            for col in ALV_MSG_COLUMNS:
                try:
                    val = shell.getCellValue(0, col)
                    if val and str(val).strip():
                        msg_text = str(val).strip()
                        break
                except Exception:
                    continue

            # Si no se encontró texto descriptivo, devolver solo el icono (si hay)
            if not msg_text:
                log.debug(f"alv_no_text_col_found icon='{icon_val}'")
                return ""

            # Devolver solo el texto (sin iconos SAP)
            return msg_text.strip()

        except Exception as e:
            log.debug(f"alv_read_exception: {e}")
            return ""

    def _is_alv_error(self, alv_message: str) -> bool:
        """
        Determina si el mensaje del ALV indica un error.

        Criterios (en orden):
          1. Contiene icono rojo SAP @5C@  → error
          2. Contiene icono verde SAP @5B@ → éxito
          3. Contiene keywords de error conocidos → error
          4. Sin coincidencia → asumir éxito (comportamiento conservador)
        """
        if SAP_ICON_ERROR in alv_message:
            return True
        if SAP_ICON_OK in alv_message:
            return False
        msg_lower = alv_message.lower()
        return any(kw in msg_lower for kw in ALV_ERROR_KEYWORDS)

    # ── Navegación — replica EXACTA del VBA ──────────────────────────────────

    def _abrir_transaccion(self, tx_code: str) -> None:
        """Equivalente a AbrirTransaccion() del VBA."""
        okcd = self._find("wnd[0]/tbar[0]/okcd")
        if okcd:
            okcd.Text = tx_code
        wnd = self._find("wnd[0]")
        if wnd:
            wnd.sendVKey(0)
        # Esperar que SAP cargue completamente — VBA usaba Wait 2s
        time.sleep(WAIT_LONG)
        self._wait_idle()
        # Espera adicional para que cargue el árbol interno de la TX
        time.sleep(WAIT_TREE)
        self._wait_idle()
        log.info(f"tx_opened code={tx_code} current_tx={self._session.Info.Transaction}")

    def _navegar_nodo(self, node_id: str) -> bool:
        """
        Equivalente a NavegerNodoZMOVEINBHU / NavegerNodoTIJSEP del VBA.
        Busca el árbol interno de la TX y hace doubleClickNode.
        """
        tree = self._find(TREE_PATH)
        if tree is None:
            log.warning(f"tree_not_found path={TREE_PATH}")
            return False

        try:
            tree.selectedNode = node_id
            tree.doubleClickNode(node_id)
            self._wait_idle()
            time.sleep(WAIT_LONG)
            log.info(f"node_clicked node={node_id}")
            return True
        except Exception as e:
            log.warning(f"node_click_failed node={node_id} error={e}")
            return False

    # ── Setup de fases ────────────────────────────────────────────────────────

    def setup_phase1(self) -> None:
        """
        Navega a ZMOVEINBHU.
        El diagnóstico confirmó que /nZMOVEINBHU va directo a la pantalla
        con ctxtP_HU — no hay árbol de menú en esta instalación de SAP.
        """
        log.info("setup_phase1_start")
        self._close_all_popups()
        try:
            wnd = self._find("wnd[0]")
            if wnd:
                wnd.maximize()
        except Exception:
            pass

        self._abrir_transaccion(TX_MOVEINBHU)

        # Intentar árbol solo si ctxtP_HU no está todavía visible
        if self._find(FIELD_F1_HU) is None:
            self._navegar_nodo(NODE_MOVEINBHU)

        if self._find(FIELD_F1_HU) is not None:
            log.info("phase1_ready — ctxtP_HU confirmed")
        else:
            log.warning(f"phase1_field_not_visible sbar='{self._get_sbar_text()}'")

    def setup_phase2(self) -> None:
        """Navega a ZMMTIJSEP con la misma lógica adaptativa que setup_phase1."""
        log.info("setup_phase2_start")
        self._close_all_popups()

        self._abrir_transaccion(TX_TIJSEP)

        if self._find(FIELD_F2_HU) is None:
            self._navegar_nodo(NODE_TIJSEP)

        if self._find(FIELD_F2_HU) is not None:
            log.info("phase2_ready — txtGV_HU confirmed")
        else:
            log.warning(f"phase2_field_not_visible sbar='{self._get_sbar_text()}'")

    # ── Fase 1: ZMOVEINBHU ────────────────────────────────────────────────────

    def process_hu_phase1(self, hu_code: str) -> dict:
        """
        Replica exacta de ProcesarHU_ZMOVEINBHU del VBA, con lectura del ALV.

        Flujo:
          1. Ingresar HU → F8
          2. Si aparece popup → duplicate (ya procesado)
          3. Sin popup → leer ALV grid para obtener mensaje completo
             - @5C@ o keyword de error → status "error" (se salta F2)
             - @5B@ o sin keyword      → status "ok"
          4. F3 para limpiar pantalla y dejarla lista para el siguiente HU
        """
        try:
            campo_hu = self._find(FIELD_F1_HU)
            if campo_hu is None:
                log.warning(f"phase1_wrong_screen hu={hu_code} — re-navigating")
                self.setup_phase1()
                campo_hu = self._find(FIELD_F1_HU)
                if campo_hu is None:
                    sbar = self._get_sbar_text()
                    return {
                        "status":  "error",
                        "message": f"No se encontró campo HU. SAP: '{sbar}'",
                        "sbar":    sbar,
                    }

            # Ingresar HU → F8
            campo_hu.Text = hu_code
            campo_hu.caretPosition = len(hu_code)
            self._find("wnd[0]").sendVKey(8)
            self._wait_idle()
            time.sleep(WAIT_SHORT)

            sbar  = self._get_sbar_text()
            popup = self._get_popup()

            if popup is not None:
                # Popup = ya procesado → Enter para cerrar
                popup_title = ""
                try:
                    popup_title = popup.Text
                except Exception:
                    pass
                self._find("wnd[1]").sendVKey(0)
                self._wait_idle()
                time.sleep(WAIT_SHORT)
                log.info(f"hu_phase1_duplicate hu={hu_code}")
                return {
                    "status":  "duplicate",
                    "message": f"Ya se hizo el Acknowledge ({popup_title})",
                    "sbar":    sbar,
                }

            else:
                # Sin popup → leer ALV grid para obtener el mensaje completo
                alv_message = self._get_alv_message()

                if alv_message and self._is_alv_error(alv_message):
                    # ── ERROR SAP ───────────────────────────────────────────
                    # Mensaje completo del ALV (p.ej. "Deficit of BA Unrestricted-use...")
                    log.warning(
                        f"hu_phase1_error_alv hu={hu_code} msg='{alv_message}'"
                    )
                    # F3 para limpiar pantalla y preparar el siguiente HU
                    self._find("wnd[0]").sendVKey(3)
                    self._wait_idle()
                    time.sleep(WAIT_SHORT)
                    return {
                        "status":  "error",
                        "message": alv_message,
                        "sbar":    sbar,
                    }

                else:
                    # ── OK ──────────────────────────────────────────────────
                    # Usar mensaje descriptivo del ALV si está disponible
                    ok_msg = alv_message if alv_message else "OK"
                    self._find("wnd[0]").sendVKey(3)
                    self._wait_idle()
                    time.sleep(WAIT_SHORT)
                    log.info(f"hu_phase1_ok hu={hu_code} msg='{ok_msg}'")
                    return {
                        "status":  "ok",
                        "message": ok_msg,
                        "sbar":    sbar,
                    }

        except Exception as e:
            log.error(f"hu_phase1_error hu={hu_code} error={e}")
            return {"status": "error", "message": f"ERROR: {e}", "sbar": ""}

    # ── Fase 2: ZMMTIJSEP ────────────────────────────────────────────────────

    def process_hu_phase2(self, hu_code: str, phase2_wait: float = WAIT_SHORT) -> dict:
        """
        Replica exacta de ProcesarHU_ZMMTIJSEP del VBA.
        
        Parámetro adicional:
        ----------
        phase2_wait : float — espera después de Enter, según origen del HU.
                             Orígenes rápidos (TH, T100): ~1.0s
                             Orígenes lentos (C10, 29, ELPS): ~2.5-3.0s
        """
        t_start = time.time()
        try:
            campo_hu = self._find(FIELD_F2_HU)
            if campo_hu is None:
                log.warning(f"phase2_wrong_screen hu={hu_code} — re-navigating")
                self.setup_phase2()
                campo_hu = self._find(FIELD_F2_HU)
                if campo_hu is None:
                    sbar = self._get_sbar_text()
                    return {
                        "status":      "error",
                        "message":     f"No se encontró campo HU. SAP: '{sbar}'",
                        "duration_ms": int((time.time() - t_start) * 1000),
                    }

            # Ingresar HU → Enter
            campo_hu.Text = hu_code
            campo_hu.caretPosition = len(hu_code)
            self._find("wnd[0]").sendVKey(0)

            # Espera dinámica según origen
            if phase2_wait > 0:
                time.sleep(phase2_wait)

            # Polling hasta idle — timeout 15s (igual que VBA)
            if not self._wait_idle(timeout=TIMEOUT_SAP):
                return {
                    "status":      "error",
                    "message":     "TIMEOUT: SAP no respondió en 15s",
                    "duration_ms": int((time.time() - t_start) * 1000),
                }

            elapsed = time.time() - t_start
            if elapsed < WAIT_SHORT:
                time.sleep(WAIT_SHORT - elapsed)

            self._wait_idle()

            duration = int((time.time() - t_start) * 1000)
            log.info(f"hu_phase2_ok hu={hu_code} duration_ms={duration} phase2_wait={phase2_wait}")
            return {"status": "ok", "message": "OK", "duration_ms": duration}

        except Exception as e:
            duration = int((time.time() - t_start) * 1000)
            log.error(f"hu_phase2_error hu={hu_code} error={e}")
            return {
                "status":      "error",
                "message":     f"ERROR: {e}",
                "duration_ms": duration,
            }

    # ── Fase 3: SP01 ─────────────────────────────────────────────────────────

    def _sp01_open_spool_list(self) -> None:
        """Abre SP01 y navega hasta la lista de spools."""
        self._abrir_transaccion(TX_SP01)
        try:
            self._find("wnd[0]").maximize()
        except Exception:
            pass
        self._wait_idle()
        time.sleep(WAIT_LONG)
        # F8 ejecuta la pantalla de filtros y muestra la lista de spools
        self._find("wnd[0]").sendVKey(8)
        self._wait_idle()
        time.sleep(WAIT_LONG)

    def _sp01_refresh(self) -> None:
        """Recarga la lista de spools (equivalente al btn[45] del VBA)."""
        btn = self._find(SP01_REFRESH_BTN)
        if btn:
            btn.press()
            self._wait_idle()
            time.sleep(WAIT_SHORT)
        else:
            log.warning("sp01_refresh_btn_not_found")

    def _sp01_mark_and_print(
        self,
        hu_filter: list[str] | None,
        expected_count: int = 0,
        max_retries: int = 40,
        print_watcher_start: Callable | None = None,
    ) -> dict:
        """
        Recorre la tabla de spools, marca los que coincidan con hu_filter
        (o todos si hu_filter es None) y los imprime.

        Si expected_count > 0 y los spools marcados son menos que ese número,
        recarga la lista (btn[45]) cada WAIT_SP01_REFRESH segundos y reintenta
        hasta alcanzar expected_count o agotar max_retries.

        Parámetros
        ----------
        hu_filter            : list[str] | None  — HU codes a buscar en el título.
        expected_count       : int               — cuántos spools se esperan encontrar.
                                                   0 = no esperar, imprimir lo que haya.
        max_retries          : int               — límite de recargas antes de rendirse.
        print_watcher_start  : Callable | None   — Callback invocado justo antes de presionar botón.
        """
        hu_upper = [h.strip().upper() for h in hu_filter] if hu_filter else None

        attempt = 0
        marked  = 0
        last_row = None
        previous_marked = -1  # Rastrear cambios en el recuento
        unchanged_attempts = 0  # Contar intentos sin cambio

        while True:
            marked   = 0
            last_row = None
            rows_to_check: list[tuple[int, object, str]] = []

            # ── Escanear tabla ────────────────────────────────────────────────
            row = 4
            while True:
                chk_obj = self._find(f"wnd[0]/usr/chk[1,{row}]")
                if chk_obj is None:
                    break

                # Saltar filas ya completadas
                status_text = ""
                status_obj  = self._find(f"wnd[0]/usr/lbl[36,{row}]")
                if status_obj:
                    try:
                        status_text = str(status_obj.Text).strip()
                    except Exception:
                        pass

                if "compl" in status_text.lower():
                    log.debug(f"sp01_skip_compl row={row}")
                    row += 2
                    continue

                # Leer título
                title_text = ""
                title_obj  = self._find(f"wnd[0]/usr/lbl[49,{row}]")
                if title_obj:
                    try:
                        title_text = str(title_obj.Text).strip()
                    except Exception:
                        pass

                title_upper = title_text.upper()
                match = (hu_upper is None) or any(hu in title_upper for hu in hu_upper)

                if match:
                    rows_to_check.append((row, chk_obj, title_text))

                row += 2

            marked = len(rows_to_check)

            # ── Detectar si el recuento se estabilizó ──────────────────────────
            # Si marked == previous_marked y hay marcados válidos, asumir que es todo
            if marked == previous_marked and marked > 0:
                unchanged_attempts += 1
                log.info(
                    f"sp01_marked_stable marked={marked} unchanged_attempts={unchanged_attempts}"
                )
                if unchanged_attempts >= 2:
                    # El recuento no cambió en 2+ intentos → asumir que esto es todo
                    log.info(
                        f"sp01_stopping_wait marked={marked} expected={expected_count} "
                        f"(estabilizado tras {attempt} intentos)"
                    )
                    break
            else:
                # El recuento cambió → resetear contador
                unchanged_attempts = 0
                previous_marked = marked

            # ── ¿Tenemos todos los spools esperados? ──────────────────────────
            if expected_count > 0 and marked < expected_count and attempt < max_retries:
                attempt += 1
                log.info(
                    f"sp01_waiting_spools attempt={attempt}/{max_retries} "
                    f"found={marked} expected={expected_count} — refreshing in {WAIT_SP01_REFRESH}s"
                )
                time.sleep(WAIT_SP01_REFRESH)
                self._sp01_refresh()
                continue

            break

        # ── Diagnóstico si no se encontró nada ────────────────────────────────
        if marked == 0:
            diag_rows = []
            for diag_row in range(4, 4 + 20, 2):
                title_lbl  = self._find(f"wnd[0]/usr/lbl[49,{diag_row}]")
                status_lbl = self._find(f"wnd[0]/usr/lbl[36,{diag_row}]")
                if title_lbl is None:
                    break
                try:
                    t = title_lbl.Text
                    s = status_lbl.Text if status_lbl else "?"
                    diag_rows.append(f"row={diag_row} status='{s}' title='{t}'")
                except Exception:
                    diag_rows.append(f"row={diag_row}→[err]")
            log.warning(
                f"sp01_no_rows_found | filter={hu_upper} | "
                f"filas_leidas=[{' | '.join(diag_rows) or 'ninguna'}]"
            )
            return {
                "status":  "error",
                "message": "No se encontraron spools pendientes",
                "marked":  0,
            }

        # ── Marcar checkboxes de las filas encontradas ────────────────────────
        for (row, chk_obj, title_text) in rows_to_check:
            try:
                chk_obj.selected = True
                last_row = row
                log.info(f"sp01_row_marked row={row} title={title_text[:60]}")
            except Exception as e:
                log.warning(f"sp01_check_failed row={row} error={e}")

        # setFocus al último checkbox marcado
        if last_row:
            last_chk = self._find(f"wnd[0]/usr/chk[1,{last_row}]")
            if last_chk:
                try:
                    last_chk.setFocus()
                except Exception:
                    pass

        # ── Menú → rango páginas → imprimir ──────────────────────────────────
        menu = self._find("wnd[0]/mbar/menu[0]/menu[0]/menu[1]")
        if menu:
            menu.select()
            self._wait_idle()
            time.sleep(WAIT_SHORT)
        else:
            log.warning("sp01_menu_not_found")

        start_field = self._find("wnd[0]/usr/txtSTART_300")
        end_field   = self._find("wnd[0]/usr/txtEND_300")
        if start_field:
            start_field.Text = "1"
        if end_field:
            end_field.Text = "1"
            try:
                end_field.setFocus()
                end_field.caretPosition = 1
            except Exception:
                pass

        btn_id   = "wnd[0]/tbar[1]/btn[13]" if marked == 1 else "wnd[0]/tbar[1]/btn[25]"
        btn_name = "btn[13]"                if marked == 1 else "btn[25]"
        btn      = self._find(btn_id)

        if btn:
            # START THE WATCHER RIGHT BEFORE PRESSING THE BUTTON
            # This ensures watcher is ready EXACTLY when the print dialog appears
            if print_watcher_start:
                print_watcher_start()
            btn.press()
            log.info(f"sp01_print_confirmed via={btn_name} marked={marked}")
        else:
            log.warning(f"sp01_{btn_name}_not_found — fallback sendVKey(8)")
            if print_watcher_start:
                print_watcher_start()
            self._find("wnd[0]").sendVKey(8)

        # ── CAMBIO CRÍTICO: NO llamar _wait_idle() aquí ──
        # El problema era una race condition COM: _wait_idle() bloquea el thread
        # interrogando session.Busy, mientras el watcher (otro thread) intenta
        # enviar clicks Win32. SAP GUI se congela intentando procesar ambos.
        # 
        # Solución: retornar inmediatamente sin _wait_idle(). El watcher trabaja
        # en su propio thread con Win32 puro (no COM), así que no interfiere.
        # El caller (queue_model) espera explícitamente al watcher si necesita.
        
        # Solo esperar un momento fijo para que SAP lance los primeros diálogos
        time.sleep(1.0)

        # Cerrar popup SAP si aparece (distinto de los diálogos Win32 Print)
        popup = self._get_popup()
        if popup:
            try:
                popup.sendVKey(0)
            except Exception:
                pass

        # Retornar sin esperar idle — el watcher continúa trabajando libremente
        return {
            "status":  "ok",
            "message": f"SP01 OK — {marked} spool(s) impreso(s)",
            "marked":  marked,
        }

    def execute_sp01(self, active_hu_codes: list[str], print_watcher_start: Callable | None = None) -> dict:
        """
        SP01 global — marca e imprime todos los spools pendientes que
        correspondan a los HU activos en cola en ese momento.

        Parámetros
        ----------
        active_hu_codes : list[str]
            Todos los HU codes actualmente en proceso (de todos los pallets).
        """
        try:
            self._sp01_open_spool_list()
            result = self._sp01_mark_and_print(
                hu_filter=active_hu_codes,
                print_watcher_start=print_watcher_start,
            )
            log.info(f"sp01_global_done result={result}")
            return result
        except Exception as e:
            log.error(f"sp01_global_error error={e}")
            return {"status": "error", "message": f"SP01 ERROR: {e}", "marked": 0}

    def execute_sp01_for_pallet(self, hu_codes: list[str], expected_count: int = 0, print_watcher_start: Callable | None = None) -> dict:
        """
        SP01 filtrado por pallet — marca e imprime solo los spools cuyo
        título contenga alguno de los HU codes del pallet recién terminado.

        Parámetros
        ----------
        hu_codes             : list[str]  — Códigos HU del pallet recién terminado.
        expected_count       : int        — Spools esperados; si > 0 reintenta hasta
                                            encontrarlos o agotar max_retries.
        print_watcher_start  : Callable   — Callback para iniciar watcher en el momento justo.
        """
        try:
            self._sp01_open_spool_list()
            result = self._sp01_mark_and_print(
                hu_filter=hu_codes,
                expected_count=expected_count,
                print_watcher_start=print_watcher_start,
            )
            log.info(f"sp01_pallet_done result={result} hu_codes={hu_codes}")
            return result
        except Exception as e:
            log.error(f"sp01_pallet_error error={e} hu_codes={hu_codes}")
            return {"status": "error", "message": f"SP01 ERROR: {e}", "marked": 0}

    # ── Verificación estática ─────────────────────────────────────────────────

    @staticmethod
    def check_session(sistema: str = SISTEMA_SAP) -> tuple[bool, str]:
        try:
            pythoncom.CoInitialize()
            sap_gui = win32com.client.GetObject("SAPGUI")
            app = sap_gui.GetScriptingEngine
            for i_conn in range(int(app.Children.Count)):
                conn = app.Children(i_conn)
                for i_sess in range(int(conn.Children.Count)):
                    sess = conn.Children(i_sess)
                    try:
                        if sess.Info.SystemName.upper().strip() == sistema.upper():
                            return True, sess.Info.User
                    except Exception:
                        continue
        except Exception:
            pass
        return False, ""