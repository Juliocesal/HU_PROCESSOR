import time
import logging
import pythoncom
import win32com.client
from datetime import datetime
from datetime import timedelta
from typing import Callable, TypedDict
from .hu_origins import Origin, detect_origin, UNKNOWN_ORIGIN

log = logging.getLogger(__name__)

SISTEMA_SAP    = "LUP"
TX_MOVEINBHU   = "/nZMOVEINBHU"
TX_TIJSEP      = "/nZMMTIJSEP"
TX_SP01        = "/nSP01"
NODE_MOVEINBHU = "F00098"
NODE_TIJSEP    = "F00097"

# Tiempos por defecto (fallback cuando no hay Origin disponible)
WAIT_LONG         = 1.0
WAIT_SHORT        = 1.0
WAIT_TREE         = 3.0
TIMEOUT_SAP       = 15.0
POLL_INTERVAL     = 0.05
WAIT_SP01_REFRESH = 3.0

# IDs exactos del VBA
TREE_PATH        = "wnd[0]/usr/cntlIMAGE_CONTAINER/shellcont/shell/shellcont[0]/shell"
FIELD_F1_HU      = "wnd[0]/usr/ctxtP_HU"
FIELD_F2_HU      = "wnd[0]/usr/txtGV_HU"
SP01_REFRESH_BTN = "wnd[0]/tbar[1]/btn[45]"

# Campo de filtro de hora en SP01
FIELD_RQCRTS = (
    "wnd[0]/usr/tabsTABSTRIP_BL1/tabpSCR1"
    "/ssub%_SUBSCREEN_BL1:RSPOSP01NR:0100/ctxtP_RQCRTS"
)

# ALV grid de ZMOVEINBHU
ALV_GRID_PATH   = "wnd[0]/usr/cntlCC_ALV/shellcont/shell"
ALV_MSG_COLUMNS = ["MESSAGE", "MSG_TEXT", "TEXT", "MSGTEXT", "DESCRIPTION", "MAKTX"]

SAP_ICON_OK    = "@5B@"
SAP_ICON_ERROR = "@5C@"

ALV_ERROR_KEYWORDS = [
    "deficit", "error", "not found", "no existe", "bloqueado",
    "locked", "incorrect", "invalid", "no se encontró",
    "quantity", "cantidad", "warehouse", "stock",
    "wrong",
    "not allowed",
    "no permitido",
    "exceeded",
    "missing",
]


# ── TypedDicts para retornos estructurados ────────────────────────────────────

class ScanResult(TypedDict):
    pending:         list[tuple[int, int, str]]
    completed_count: int


class Phase1Result(TypedDict):
    status:  str   # "ok" | "duplicate" | "hu_not_found" | "error"
    message: str
    sbar:    str


class Phase2Result(TypedDict):
    status:      str       # "ok" | "error"
    message:     str
    duration_ms: int
    phase2_ts:   datetime  # timestamp del inicio de F2 ajustado para SP01.
                           # datetime.min si status == "error" — no usar en ese caso.


class SP01Result(TypedDict):
    status:  str   # "ok" | "already_done" | "error"
    message: str
    marked:  int


class SAPConnectionError(Exception):
    pass


# ── Centinela de timestamp inválido ───────────────────────────────────────────
# Usar datetime.min como valor explícito en Phase2Result cuando F2 falla.
# Si alguien intenta usarlo en SP01, el filtro de hora será "00:00:00" del año 1,
# lo que fallará ruidosamente en SAP en lugar de filtrar spools incorrectos.
_INVALID_TS = datetime.min


def _make_phase2_ts() -> datetime:
    """
    Retorna el timestamp de inicio de F2 ajustado para el filtro SP01.
    Se resta 1 minuto para asegurar que el spool ya generado quede
    dentro del rango de búsqueda de SP01.
    """
    return datetime.now() - timedelta(minutes=1)


class SAPClient:

    def __init__(self):
        self._session  = None
        self._usuario  = ""
        # NOTA: _phase2_ts eliminado — el timestamp ahora fluye
        # explícitamente como parámetro en Phase2Result → execute_sp01*.

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

    def _get_origin_timings(
        self, origin: Origin | None = None, hu_code: str = ""
    ) -> tuple[float, float, float, float]:
        if origin is None:
            origin = detect_origin(hu_code) if hu_code else UNKNOWN_ORIGIN
        return (origin.wait_long, origin.wait_short, origin.wait_tree, origin.wait_sp01_refresh)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _find(self, element_id: str):
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
        for _ in range(5):
            popup = self._get_popup()
            if popup is None:
                break
            try:
                popup.sendVKey(12)
                time.sleep(0.2)
                self._wait_idle(timeout=3.0)
            except Exception:
                break

    # ── ALV grid de ZMOVEINBHU ────────────────────────────────────────────────

    def _get_alv_message(self) -> str:
        try:
            shell = self._find(ALV_GRID_PATH)
            if shell is None:
                log.debug("alv_grid_not_found — usando sbar como fallback")
                return ""

            try:
                shell.currentCellRow = -1
            except Exception:
                pass

            icon_val = ""
            try:
                icon_val = str(shell.getCellValue(0, "ICON")).strip()
            except Exception:
                pass

            if icon_val == SAP_ICON_ERROR:
                msg_text = ""
                for col in ALV_MSG_COLUMNS:
                    try:
                        val = shell.getCellValue(0, col)
                        if val and str(val).strip():
                            msg_text = str(val).strip()
                            break
                    except Exception:
                        continue
                error_msg = f"{SAP_ICON_ERROR} {msg_text}" if msg_text else f"{SAP_ICON_ERROR} Error en SAP (hexágono rojo)"
                log.warning(f"alv_error_detected icon={SAP_ICON_ERROR} msg='{error_msg}'")
                return error_msg

            msg_text = ""
            for col in ALV_MSG_COLUMNS:
                try:
                    val = shell.getCellValue(0, col)
                    if val and str(val).strip():
                        msg_text = str(val).strip()
                        break
                except Exception:
                    continue

            if not msg_text:
                log.debug(f"alv_no_text_col_found icon='{icon_val}'")
                return ""

            return msg_text.strip()

        except Exception as e:
            log.debug(f"alv_read_exception: {e}")
            return ""

    def _is_alv_error(self, alv_message: str) -> bool:
        if SAP_ICON_ERROR in alv_message:
            return True
        if SAP_ICON_OK in alv_message:
            return False
        msg_lower = alv_message.lower()
        return any(kw in msg_lower for kw in ALV_ERROR_KEYWORDS)

    # ── Navegación ────────────────────────────────────────────────────────────

    def _abrir_transaccion(self, tx_code: str, origin: Origin | None = None) -> None:
        wait_long, _, wait_tree, _ = (
            self._get_origin_timings(origin)
            if origin
            else (WAIT_LONG, WAIT_SHORT, WAIT_TREE, WAIT_SP01_REFRESH)
        )

        okcd = self._find("wnd[0]/tbar[0]/okcd")
        if okcd:
            okcd.Text = tx_code
        wnd = self._find("wnd[0]")
        if wnd:
            wnd.sendVKey(0)
        time.sleep(wait_long)
        self._wait_idle()
        time.sleep(wait_tree)
        self._wait_idle()
        log.info(f"tx_opened code={tx_code} current_tx={self._session.Info.Transaction}")

    def _navegar_nodo(self, node_id: str, origin: Origin | None = None) -> bool:
        wait_long = origin.wait_long if origin else WAIT_LONG

        tree = self._find(TREE_PATH)
        if tree is None:
            log.warning(f"tree_not_found path={TREE_PATH}")
            return False

        try:
            tree.selectedNode = node_id
            tree.doubleClickNode(node_id)
            self._wait_idle()
            time.sleep(wait_long)
            log.info(f"node_clicked node={node_id}")
            return True
        except Exception as e:
            log.warning(f"node_click_failed node={node_id} error={e}")
            return False

    # ── Setup de fases ────────────────────────────────────────────────────────

    def setup_phase1(self, origin: Origin | None = None) -> None:
        log.info("setup_phase1_start")
        self._close_all_popups()
        try:
            wnd = self._find("wnd[0]")
            if wnd:
                wnd.maximize()
        except Exception:
            pass

        self._abrir_transaccion(TX_MOVEINBHU, origin=origin)

        if self._find(FIELD_F1_HU) is None:
            self._navegar_nodo(NODE_MOVEINBHU, origin=origin)

        if self._find(FIELD_F1_HU) is not None:
            log.info("phase1_ready — ctxtP_HU confirmed")
        else:
            log.warning(f"phase1_field_not_visible sbar='{self._get_sbar_text()}'")

    def setup_phase2(self, origin: Origin | None = None) -> None:
        log.info("setup_phase2_start")
        self._close_all_popups()

        self._abrir_transaccion(TX_TIJSEP, origin=origin)

        if self._find(FIELD_F2_HU) is None:
            self._navegar_nodo(NODE_TIJSEP, origin=origin)

        if self._find(FIELD_F2_HU) is not None:
            log.info("phase2_ready — txtGV_HU confirmed")
        else:
            log.warning(f"phase2_field_not_visible sbar='{self._get_sbar_text()}'")

    # ── Fase 1: ZMOVEINBHU ────────────────────────────────────────────────────

    def process_hu_phase1(self, hu_code: str, origin: Origin | None = None) -> Phase1Result:
        wait_long, wait_short, _, _ = self._get_origin_timings(origin, hu_code)

        try:
            campo_hu = self._find(FIELD_F1_HU)
            if campo_hu is None:
                log.warning(f"phase1_wrong_screen hu={hu_code} — re-navigating")
                self.setup_phase1(origin=origin)
                campo_hu = self._find(FIELD_F1_HU)
                if campo_hu is None:
                    sbar = self._get_sbar_text()
                    return Phase1Result(
                        status="error",
                        message=f"No se encontró campo HU. SAP: '{sbar}'",
                        sbar=sbar,
                    )

            campo_hu.Text = hu_code
            campo_hu.caretPosition = len(hu_code)

            wnd_submit = self._find("wnd[0]")
            if wnd_submit is None:
                return Phase1Result(status="error", message="wnd[0] no disponible", sbar="")
            wnd_submit.sendVKey(8)
            self._wait_idle()
            time.sleep(wait_short)

            sbar  = self._get_sbar_text()
            popup = self._get_popup()

            if popup is not None:
                popup_title = ""
                popup_text  = ""
                try:
                    popup_title = popup.Text
                except Exception:
                    pass

                msg_lbl = self._find("wnd[1]/usr/txtMESSTXT1")
                if msg_lbl is None:
                    msg_lbl = self._find("wnd[1]/usr/lbl[1,2]")
                if msg_lbl:
                    try:
                        popup_text = str(msg_lbl.Text).strip()
                    except Exception:
                        pass

                wnd1 = self._find("wnd[1]")
                if wnd1:
                    wnd1.sendVKey(0)
                    self._wait_idle()
                    time.sleep(wait_short)

                combined = (popup_text + " " + popup_title).lower()
                HU_NOT_EXIST_KEYWORDS = [
                    "does not exist", "no existe", "not found", "no encontrado",
                ]
                is_not_found = any(kw in combined for kw in HU_NOT_EXIST_KEYWORDS)

                if is_not_found:
                    log.warning(
                        f"hu_phase1_not_found hu={hu_code} "
                        f"popup_title='{popup_title}' popup_text='{popup_text}'"
                    )
                    return Phase1Result(
                        status="hu_not_found",
                        message=f"HU no existe en SAP: '{popup_text or popup_title}'",
                        sbar=sbar,
                    )
                else:
                    log.info(
                        f"hu_phase1_duplicate hu={hu_code} "
                        f"popup_title='{popup_title}' popup_text='{popup_text}'"
                    )
                    return Phase1Result(
                        status="duplicate",
                        message=f"Ya se hizo el Acknowledge ({popup_text or popup_title})",
                        sbar=sbar,
                    )

            else:
                alv_message = self._get_alv_message()

                if alv_message and self._is_alv_error(alv_message):
                    error_type = "SAP_AUTHORIZATION_DENIED" if SAP_ICON_ERROR in alv_message else "ALV_ERROR"
                    log.warning(f"hu_phase1_error_alv hu={hu_code} type={error_type} msg='{alv_message}'")
                    wnd_back = self._find("wnd[0]")
                    if wnd_back:
                        wnd_back.sendVKey(3)
                    self._wait_idle()
                    time.sleep(wait_short)
                    return Phase1Result(status="error", message=alv_message, sbar=sbar)
                else:
                    ok_msg = alv_message if alv_message else "OK"
                    wnd_back = self._find("wnd[0]")
                    if wnd_back:
                        wnd_back.sendVKey(3)
                    self._wait_idle()
                    time.sleep(wait_short)
                    log.info(f"hu_phase1_ok hu={hu_code} msg='{ok_msg}'")
                    return Phase1Result(status="ok", message=ok_msg, sbar=sbar)

        except Exception as e:
            log.error(f"hu_phase1_error hu={hu_code} error={e}")
            return Phase1Result(status="error", message=f"ERROR: {e}", sbar="")

    # ── Fase 2: ZMMTIJSEP ─────────────────────────────────────────────────────
    #
    # CAMBIO CLAVE: el timestamp de F2 ya no se guarda en self._phase2_ts.
    # Se captura localmente y se retorna en Phase2Result["phase2_ts"].
    # Si F2 falla, phase2_ts = datetime.min (centinela explícito).
    # El worker lo almacena por pallet_id y lo pasa a execute_sp01*.
    # ──────────────────────────────────────────────────────────────────────────

    def process_hu_phase2(
        self,
        hu_code: str,
        phase2_wait: float | None = None,
        origin: Origin | None = None,
    ) -> Phase2Result:
        _, wait_short, _, _ = self._get_origin_timings(origin, hu_code)

        if phase2_wait is None:
            phase2_wait = origin.phase2_wait if origin else WAIT_SHORT

        # Capturar timestamp localmente — no se almacena en self
        phase2_ts = _make_phase2_ts()
        log.info("phase2_timestamp_captured ts=%s", phase2_ts.strftime("%d.%m.%Y %H:%M:%S"))

        t_start = time.time()
        try:
            campo_hu = self._find(FIELD_F2_HU)
            if campo_hu is None:
                log.warning(f"phase2_wrong_screen hu={hu_code} — re-navigating")
                self.setup_phase2(origin=origin)
                campo_hu = self._find(FIELD_F2_HU)
                if campo_hu is None:
                    sbar = self._get_sbar_text()
                    return Phase2Result(
                        status="error",
                        message=f"No se encontró campo HU. SAP: '{sbar}'",
                        duration_ms=int((time.time() - t_start) * 1000),
                        phase2_ts=_INVALID_TS,
                    )

            campo_hu.Text = hu_code
            campo_hu.caretPosition = len(hu_code)

            wnd_submit = self._find("wnd[0]")
            if wnd_submit is None:
                return Phase2Result(
                    status="error",
                    message="wnd[0] no disponible",
                    duration_ms=int((time.time() - t_start) * 1000),
                    phase2_ts=_INVALID_TS,
                )
            wnd_submit.sendVKey(0)

            if phase2_wait > 0:
                time.sleep(phase2_wait)

            if not self._wait_idle(timeout=TIMEOUT_SAP):
                return Phase2Result(
                    status="error",
                    message="TIMEOUT: SAP no respondió en 15s",
                    duration_ms=int((time.time() - t_start) * 1000),
                    phase2_ts=_INVALID_TS,
                )

            elapsed = time.time() - t_start
            if elapsed < wait_short:
                time.sleep(wait_short - elapsed)

            self._wait_idle()

            duration = int((time.time() - t_start) * 1000)
            log.info(f"hu_phase2_ok hu={hu_code} duration_ms={duration} phase2_wait={phase2_wait}")
            return Phase2Result(
                status="ok",
                message="OK",
                duration_ms=duration,
                phase2_ts=phase2_ts,   # timestamp válido solo en éxito
            )

        except Exception as e:
            duration = int((time.time() - t_start) * 1000)
            log.error(f"hu_phase2_error hu={hu_code} error={e}")
            return Phase2Result(
                status="error",
                message=f"ERROR: {e}",
                duration_ms=duration,
                phase2_ts=_INVALID_TS,
            )

    # ── Fase 3: SP01 ─────────────────────────────────────────────────────────
    #
    # CAMBIO CLAVE: phase2_ts es parámetro obligatorio en todos los métodos
    # de SP01. _sp01_open_spool_list ya no lee self._phase2_ts.
    # ──────────────────────────────────────────────────────────────────────────

    def _sp01_open_spool_list(
        self,
        phase2_ts: datetime,
        origin: Origin | None = None,
    ) -> None:
        """
        Abre SP01, escribe la hora de Fase 2 directamente en P_RQCRTS y
        ejecuta la búsqueda con F8.

        Args:
            phase2_ts: timestamp capturado en process_hu_phase2 para el
                       pallet que se está imprimiendo. Nunca debe ser
                       datetime.min — el caller es responsable de validarlo.
        """
        wait_long = origin.wait_long if origin else WAIT_LONG

        # Guardia defensiva: loguear si recibimos el centinela inválido
        if phase2_ts == _INVALID_TS:
            log.error(
                "sp01_open_spool_list recibió datetime.min (centinela inválido). "
                "Usando datetime.now() como fallback de emergencia."
            )
            phase2_ts = datetime.now() - timedelta(minutes=1)

        self._abrir_transaccion(TX_SP01, origin=origin)

        wnd = self._find("wnd[0]")
        if wnd:
            try:
                wnd.maximize()
            except Exception:
                pass
        self._wait_idle()
        time.sleep(wait_long)

        # Formato de hora para el campo SP01
        hh = str(phase2_ts.hour).zfill(2)
        mm = str(phase2_ts.minute).zfill(2)
        ss = str(phase2_ts.second).zfill(2)
        time_str = f"{hh}:{mm}:{ss}"
        log.info("sp01_time_filter ts=%s", time_str)

        field_rqcrts = self._find(FIELD_RQCRTS)
        if field_rqcrts is not None:
            try:
                field_rqcrts.text = time_str
                field_rqcrts.setFocus()
                field_rqcrts.caretPosition = 8
                log.info("sp01_rqcrts_set time='%s'", time_str)
            except Exception as e:
                log.warning("sp01_rqcrts_set_failed error=%s", e)
        else:
            log.warning("sp01_rqcrts_field_not_found — time filter skipped")

        wnd = self._find("wnd[0]")
        if wnd:
            wnd.sendVKey(8)
        self._wait_idle()
        time.sleep(wait_long)

    def _sp01_refresh(self) -> None:
        btn = self._find(SP01_REFRESH_BTN)
        if btn:
            btn.press()
            self._wait_idle()
            time.sleep(WAIT_SHORT)
        else:
            log.warning("sp01_refresh_btn_not_found")

    def _hu_matches_title(self, hu: str, title_upper: str) -> bool:
        import re
        pattern = r'(?<![A-Z0-9])' + re.escape(hu) + r'(?![A-Z0-9])'
        if re.search(pattern, title_upper):
            return True
        zero_pattern = r'0+' + re.escape(hu) + r'(?![A-Z0-9])'
        return bool(re.search(zero_pattern, title_upper))

    def _sp01_scan_all_rows(self, hu_upper: list[str] | None) -> ScanResult:
        usr_area = self._find("wnd[0]/usr")
        if usr_area is None:
            log.warning("sp01_scan — no usr area")
            return ScanResult(pending=[], completed_count=0)

        scroll_max = 0
        try:
            vscroll = usr_area.verticalScrollbar
            scroll_max = int(vscroll.maximum) if vscroll else 0
        except Exception:
            scroll_max = 30

        log.info(f"sp01_scan start scroll_max={scroll_max}")

        seen_titles: set[str] = set()
        pending: list[tuple[int, int, str]] = []
        completed_count  = 0
        total_expected   = len(hu_upper) if hu_upper else None

        for pos in range(0, scroll_max + 1):
            try:
                usr_area = self._find("wnd[0]/usr")
                usr_area.verticalScrollbar.position = pos
                time.sleep(0.15)
            except Exception as e:
                log.debug(f"sp01_scroll pos={pos} error={e} — stopping scroll")
                break

            found_any_checkbox = False
            row = 4
            while True:
                chk_obj = self._find(f"wnd[0]/usr/chk[1,{row}]")
                if chk_obj is None:
                    break

                found_any_checkbox = True

                title_text = ""
                title_obj  = self._find(f"wnd[0]/usr/lbl[49,{row}]")
                if title_obj:
                    try:
                        title_text = str(title_obj.Text).strip()
                    except Exception:
                        pass

                title_upper = title_text.upper()

                if title_upper in seen_titles:
                    row += 2
                    continue
                seen_titles.add(title_upper)

                match = (hu_upper is None) or any(
                    self._hu_matches_title(hu, title_upper) for hu in hu_upper
                )
                if not match:
                    row += 2
                    continue

                status_text = ""
                status_obj  = self._find(f"wnd[0]/usr/lbl[36,{row}]")
                if status_obj:
                    try:
                        status_text = str(status_obj.Text).strip().lower()
                    except Exception:
                        pass

                if "compl" in status_text:
                    completed_count += 1
                    log.debug(f"sp01_scan_completed scroll={pos} row={row} title={title_text[:60]}")
                else:
                    pending.append((pos, row, title_text))
                    log.debug(f"sp01_scan_hit scroll={pos} row={row} title={title_text[:60]}")

                row += 2

            if not found_any_checkbox:
                log.debug(f"sp01_scan — no checkboxes at scroll={pos}, done")
                break

            if total_expected is not None:
                found_so_far = len(pending) + completed_count
                if found_so_far >= total_expected:
                    log.debug(
                        f"sp01_scan early_exit pos={pos} "
                        f"pending={len(pending)} completed={completed_count} "
                        f"total_expected={total_expected}"
                    )
                    break

        try:
            usr_area = self._find("wnd[0]/usr")
            usr_area.verticalScrollbar.position = 0
            time.sleep(0.1)
        except Exception:
            pass

        log.info(
            f"sp01_scan_done pending={len(pending)} completed={completed_count} "
            f"hu_filter={hu_upper}"
        )
        return ScanResult(pending=pending, completed_count=completed_count)

    def _sp01_mark_rows(self, rows: list[tuple[int, int, str]]) -> int:
        marked   = 0
        last_chk = None

        for (scroll_pos, screen_row, title_text) in rows:
            try:
                usr_area = self._find("wnd[0]/usr")
                usr_area.verticalScrollbar.position = scroll_pos
                time.sleep(0.15)
            except Exception as e:
                log.warning(f"sp01_mark_scroll pos={scroll_pos} error={e}")
                continue

            current_title = ""
            title_obj = self._find(f"wnd[0]/usr/lbl[49,{screen_row}]")
            if title_obj:
                try:
                    current_title = str(title_obj.Text).strip()
                except Exception:
                    pass

            if current_title.upper() != title_text.upper():
                log.warning(
                    f"sp01_mark_mismatch expected='{title_text[:40]}' "
                    f"got='{current_title[:40]}' — scanning visible rows"
                )
                found = False
                row   = 4
                while True:
                    t_obj = self._find(f"wnd[0]/usr/lbl[49,{row}]")
                    if t_obj is None:
                        break
                    try:
                        t = str(t_obj.Text).strip()
                    except Exception:
                        row += 2
                        continue
                    if t.upper() == title_text.upper():
                        screen_row = row
                        found = True
                        break
                    row += 2
                if not found:
                    log.warning(f"sp01_mark_not_found title='{title_text[:60]}'")
                    continue

            chk_obj = self._find(f"wnd[0]/usr/chk[1,{screen_row}]")
            if chk_obj:
                try:
                    chk_obj.selected = True
                    last_chk = chk_obj
                    marked  += 1
                    log.info(
                        f"sp01_marked scroll={scroll_pos} row={screen_row} "
                        f"title={title_text[:60]}"
                    )
                except Exception as e:
                    log.warning(f"sp01_mark_failed row={screen_row} error={e}")

        if last_chk:
            try:
                last_chk.setFocus()
            except Exception:
                pass

        try:
            usr_area = self._find("wnd[0]/usr")
            usr_area.verticalScrollbar.position = 0
            time.sleep(0.1)
        except Exception:
            pass

        log.info(f"sp01_mark_done total_marked={marked}")
        return marked

    def _sp01_mark_and_print(
        self,
        hu_filter: list[str] | None,
        expected_count: int = 0,
        max_retries: int = 40,
        print_watcher_start: Callable | None = None,
        origin: Origin | None = None,
    ) -> SP01Result:
        _, _, _, wait_sp01_refresh = self._get_origin_timings(
            origin, hu_filter[0] if hu_filter else ""
        )

        hu_upper = [h.strip().upper() for h in hu_filter] if hu_filter else None
        attempt  = 0
        scan: ScanResult = ScanResult(pending=[], completed_count=0)

        while True:
            scan            = self._sp01_scan_all_rows(hu_upper)
            pending_count   = len(scan["pending"])
            completed_count = scan["completed_count"]
            total_found     = pending_count + completed_count

            log.info(
                f"sp01_attempt={attempt} pending={pending_count} "
                f"completed={completed_count} expected={expected_count}"
            )

            if expected_count > 0 and completed_count >= expected_count and pending_count == 0:
                log.info(
                    f"sp01_all_already_completed completed={completed_count} "
                    f"expected={expected_count} — skipping print"
                )
                return SP01Result(
                    status="already_done",
                    message=f"Spools ya impresos anteriormente ({completed_count} Completed)",
                    marked=0,
                )

            if expected_count > 0 and total_found >= expected_count:
                break

            if expected_count == 0:
                break

            if attempt < max_retries:
                attempt += 1
                log.warning(
                    f"sp01_waiting_spools attempt={attempt}/{max_retries} "
                    f"pending={pending_count} completed={completed_count} "
                    f"expected={expected_count} — refresh in {wait_sp01_refresh}s"
                )
                time.sleep(wait_sp01_refresh)
                self._sp01_refresh()
                continue
            else:
                log.warning(
                    f"sp01_max_retries_exhausted pending={pending_count} "
                    f"completed={completed_count} expected={expected_count} "
                    f"— proceeding with {pending_count} pending"
                )
                break

        rows = scan["pending"]

        if not rows:
            diag: list[str] = []
            for diag_row in range(4, 4 + 20, 2):
                title_lbl  = self._find(f"wnd[0]/usr/lbl[49,{diag_row}]")
                status_lbl = self._find(f"wnd[0]/usr/lbl[36,{diag_row}]")
                if title_lbl is None:
                    break
                try:
                    t = title_lbl.Text
                    s = status_lbl.Text if status_lbl else "?"
                    diag.append(f"row={diag_row} status='{s}' title='{t}'")
                except Exception:
                    diag.append(f"row={diag_row}→[err]")
            log.warning(
                f"sp01_no_rows_found filter={hu_upper} "
                f"filas=[{' | '.join(diag) or 'ninguna'}]"
            )
            return SP01Result(status="error", message="No se encontraron spools pendientes", marked=0)

        marked = self._sp01_mark_rows(rows)

        if marked == 0:
            log.error("sp01_mark_rows returned 0 — nothing was marked")
            return SP01Result(status="error", message="No se pudo marcar ningún spool", marked=0)

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
            if print_watcher_start:
                print_watcher_start()
            btn.press()
            log.info(f"sp01_print_confirmed via={btn_name} marked={marked}")
        else:
            log.warning(f"sp01_{btn_name}_not_found — fallback sendVKey(8)")
            if print_watcher_start:
                print_watcher_start()
            wnd = self._find("wnd[0]")
            if wnd:
                wnd.sendVKey(8)

        time.sleep(1.0)

        popup = self._get_popup()
        if popup:
            try:
                popup.sendVKey(0)
            except Exception:
                pass

        return SP01Result(
            status="ok",
            message=f"SP01 OK — {marked} spool(s) impreso(s)",
            marked=marked,
        )

    def execute_sp01(
        self,
        active_hu_codes: list[str],
        phase2_ts: datetime,
        expected_count: int = 0,
        print_watcher_start: Callable | None = None,
        origin: Origin | None = None,
    ) -> SP01Result:
        """
        SP01 global — imprime todos los spools de los HU activos.

        Args:
            active_hu_codes: lista de HU codes del lote actual.
            phase2_ts:       timestamp del último F2 exitoso del lote.
                             Usar el valor de Phase2Result["phase2_ts"].
                             NUNCA pasar datetime.min.
        """
        try:
            self._sp01_open_spool_list(phase2_ts=phase2_ts, origin=origin)
            result = self._sp01_mark_and_print(
                hu_filter=active_hu_codes,
                expected_count=expected_count,
                print_watcher_start=print_watcher_start,
                origin=origin,
            )
            log.info(f"sp01_global_done result={result}")
            return result
        except Exception as e:
            log.error(f"sp01_global_error error={e}")
            return SP01Result(status="error", message=f"SP01 ERROR: {e}", marked=0)

    def execute_sp01_for_pallet(
        self,
        hu_codes: list[str],
        phase2_ts: datetime,
        expected_count: int = 0,
        print_watcher_start: Callable | None = None,
        origin: Origin | None = None,
    ) -> SP01Result:
        """
        SP01 filtrado por pallet — imprime solo los spools de este pallet.

        Args:
            hu_codes:  lista de HU codes del pallet.
            phase2_ts: timestamp del último F2 exitoso del pallet.
                       Usar el valor de Phase2Result["phase2_ts"].
                       NUNCA pasar datetime.min.
        """
        try:
            self._sp01_open_spool_list(phase2_ts=phase2_ts, origin=origin)
            result = self._sp01_mark_and_print(
                hu_filter=hu_codes,
                expected_count=expected_count,
                print_watcher_start=print_watcher_start,
                origin=origin,
            )
            log.info(f"sp01_pallet_done result={result} hu_codes={hu_codes}")
            return result
        except Exception as e:
            log.error(f"sp01_pallet_error error={e} hu_codes={hu_codes}")
            return SP01Result(status="error", message=f"SP01 ERROR: {e}", marked=0)

    # ── Verificación estática ─────────────────────────────────────────────────

    @staticmethod
    def check_session(sistema: str = SISTEMA_SAP) -> tuple[bool, str]:
        try:
            pythoncom.CoInitialize()
            try:
                sap_gui = win32com.client.GetObject("SAPGUI")
                app     = sap_gui.GetScriptingEngine
                for i_conn in range(int(app.Children.Count)):
                    conn = app.Children(i_conn)
                    for i_sess in range(int(conn.Children.Count)):
                        sess = conn.Children(i_sess)
                        try:
                            if sess.Info.SystemName.upper().strip() == sistema.upper():
                                return True, sess.Info.User
                        except Exception:
                            continue
            finally:
                pythoncom.CoUninitialize()
        except Exception:
            pass
        return False, ""