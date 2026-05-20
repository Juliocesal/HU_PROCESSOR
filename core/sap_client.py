import time
import logging
import pythoncom
import win32com.client
from datetime import datetime
from typing import TypedDict
from .hu_origins import Origin, detect_origin, UNKNOWN_ORIGIN
from django.conf import settings

log = logging.getLogger(__name__)

SISTEMA_SAP    = getattr(settings, 'SAP_SISTEMA',      'LUP')
TX_MOVEINBHU   = getattr(settings, 'SAP_TX_MOVEINBHU', '/nZMOVEINBHU')
TX_TIJSEP      = getattr(settings, 'SAP_TX_TIJSEP',    '/nZMMTIJSEP')
NODE_MOVEINBHU = "F00098"
NODE_TIJSEP    = "F00097"

# Tiempos por defecto (fallback cuando no hay Origin disponible)
WAIT_LONG         = 1.0
WAIT_SHORT        = 1.0
WAIT_TREE         = 3.0
TIMEOUT_SAP       = 15.0
POLL_INTERVAL     = 0.05

# IDs exactos del VBA
TREE_PATH        = "wnd[0]/usr/cntlIMAGE_CONTAINER/shellcont/shell/shellcont[0]/shell"
FIELD_F1_HU      = "wnd[0]/usr/ctxtP_HU"
FIELD_F2_HU      = "wnd[0]/usr/txtGV_HU"

# ALV grid de ZMOVEINBHU
ALV_GRID_PATH   = "wnd[0]/usr/cntlCC_ALV/shellcont/shell"
ALV_MSG_COLUMNS = ["MESSAGE", "MSG_TEXT", "TEXT", "MSGTEXT", "DESCRIPTION", "MAKTX"]

SAP_ICON_OK    = "@5B@"
SAP_ICON_ERROR = "@5C@"

ALV_ERROR_KEYWORDS = [
    "deficit", "error", "not found", "no existe", "bloqueado",
    "locked", "incorrect", "invalid", "no se encontro",
    "quantity", "cantidad", "warehouse", "stock",
    "wrong",
    "not allowed",
    "no permitido",
    "exceeded",
    "missing",
]


# -- TypedDicts para retornos estructurados ------------------------------------

class Phase1Result(TypedDict):
    status:  str   # "ok" | "duplicate" | "hu_not_found" | "error"
    message: str
    sbar:    str


class Phase2Result(TypedDict):
    status:      str       # "ok" | "error"
    message:     str
    duration_ms: int
    phase2_ts:   datetime  # timestamp del inicio de F2.
                           # datetime.min si status == "error".



class SAPConnectionError(Exception):
    pass


# -- Centinela de timestamp invalido -------------------------------------------
# Usar datetime.min como valor explicito en Phase2Result cuando F2 falla.
_INVALID_TS = datetime.min


def _make_phase2_ts() -> datetime:
    """Retorna el timestamp de inicio de F2 para auditoria del pallet."""
    return datetime.now()


class SAPClient:

    def __init__(self):
        self._session  = None
        self._usuario  = ""
        # El timestamp de F2 viaja en Phase2Result para auditoria del pallet.

    # -- Conexion --------------------------------------------------------------

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
                        log.info("sap_connected system=%s user=%s", sistema, self._usuario)
                        return True
                except Exception:
                    continue

        raise SAPConnectionError(f"No se encontro sesion {sistema} activa")

    @property
    def session(self):
        if self._session is None:
            raise SAPConnectionError("Sin sesion SAP.")
        return self._session

    def get_user(self) -> str:
        return self._usuario

    def _get_origin_timings(
        self, origin: Origin | None = None, hu_code: str = ""
    ) -> tuple[float, float, float, float]:
        if origin is None:
            origin = detect_origin(hu_code) if hu_code else UNKNOWN_ORIGIN
        return (origin.wait_long, origin.wait_short, origin.wait_tree, origin.wait_receipt_refresh)

    # -- Helpers ---------------------------------------------------------------

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

    # -- ALV grid de ZMOVEINBHU ------------------------------------------------

    def _get_alv_message(self) -> str:
        try:
            shell = self._find(ALV_GRID_PATH)
            if shell is None:
                log.debug("alv_grid_not_found - usando sbar como fallback")
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
                error_msg = f"{SAP_ICON_ERROR} {msg_text}" if msg_text else f"{SAP_ICON_ERROR} Error en SAP (hexagono rojo)"
                log.warning("alv_error_detected icon=%s msg=%r", SAP_ICON_ERROR, error_msg)
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
                log.debug("alv_no_text_col_found icon=%r", icon_val)
                return ""

            return msg_text.strip()

        except Exception as e:
            log.debug("alv_read_exception: %s", e)
            return ""

    def _is_alv_error(self, alv_message: str) -> bool:
        if SAP_ICON_ERROR in alv_message:
            return True
        if SAP_ICON_OK in alv_message:
            return False
        msg_lower = alv_message.lower()
        return any(kw in msg_lower for kw in ALV_ERROR_KEYWORDS)

    # -- Navegacion ------------------------------------------------------------

    def _abrir_transaccion(self, tx_code: str, origin: Origin | None = None) -> None:
        wait_long, _, wait_tree, _ = (
            self._get_origin_timings(origin)
            if origin
            else (WAIT_LONG, WAIT_SHORT, WAIT_TREE, 0)
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
        log.info("tx_opened code=%s current_tx=%s", tx_code, self._session.Info.Transaction)

    def _navegar_nodo(self, node_id: str, origin: Origin | None = None) -> bool:
        wait_long = origin.wait_long if origin else WAIT_LONG

        tree = self._find(TREE_PATH)
        if tree is None:
            log.warning("tree_not_found path=%s", TREE_PATH)
            return False

        try:
            tree.selectedNode = node_id
            tree.doubleClickNode(node_id)
            self._wait_idle()
            time.sleep(wait_long)
            log.info("node_clicked node=%s", node_id)
            return True
        except Exception as e:
            log.warning("node_click_failed node=%s error=%s", node_id, e)
            return False

    # -- Setup de fases --------------------------------------------------------

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
            log.info("phase1_ready - ctxtP_HU confirmed")
        else:
            log.warning("phase1_field_not_visible sbar=%r", self._get_sbar_text())

    def setup_phase2(self, origin: Origin | None = None) -> None:
        log.info("setup_phase2_start")
        self._close_all_popups()

        self._abrir_transaccion(TX_TIJSEP, origin=origin)

        if self._find(FIELD_F2_HU) is None:
            self._navegar_nodo(NODE_TIJSEP, origin=origin)

        if self._find(FIELD_F2_HU) is not None:
            log.info("phase2_ready - txtGV_HU confirmed")
        else:
            log.warning("phase2_field_not_visible sbar=%r", self._get_sbar_text())

    # -- Fase 1: ZMOVEINBHU ----------------------------------------------------

    def process_hu_phase1(self, hu_code: str, origin: Origin | None = None) -> Phase1Result:
        wait_long, wait_short, _, _ = self._get_origin_timings(origin, hu_code)

        try:
            campo_hu = self._find(FIELD_F1_HU)
            if campo_hu is None:
                log.warning("phase1_wrong_screen hu=%s - re-navigating", hu_code)
                self.setup_phase1(origin=origin)
                campo_hu = self._find(FIELD_F1_HU)
                if campo_hu is None:
                    sbar = self._get_sbar_text()
                    return Phase1Result(
                        status="error",
                        message=f"No se encontro campo HU. SAP: '{sbar}'",
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
                        "hu_phase1_not_found hu=%s popup_title=%r popup_text=%r",
                        hu_code,
                        popup_title,
                        popup_text,
                    )
                    return Phase1Result(
                        status="hu_not_found",
                        message=f"HU no existe en SAP: '{popup_text or popup_title}'",
                        sbar=sbar,
                    )
                else:
                    log.info(
                        "hu_phase1_duplicate hu=%s popup_title=%r popup_text=%r",
                        hu_code,
                        popup_title,
                        popup_text,
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
                    log.warning(
                        "hu_phase1_error_alv hu=%s type=%s msg=%r",
                        hu_code,
                        error_type,
                        alv_message,
                    )
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
                    log.info("hu_phase1_ok hu=%s msg=%r", hu_code, ok_msg)
                    return Phase1Result(status="ok", message=ok_msg, sbar=sbar)

        except Exception as e:
            log.error("hu_phase1_error hu=%s error=%s", hu_code, e)
            return Phase1Result(status="error", message=f"ERROR: {e}", sbar="")

    # -- Fase 2: ZMMTIJSEP -----------------------------------------------------
    #
    # CAMBIO CLAVE: el timestamp de F2 ya no se guarda en self._phase2_ts.
    # Se captura localmente y se retorna en Phase2Result["phase2_ts"].
    # Si F2 falla, phase2_ts = datetime.min (centinela explicito).
    # El worker lo almacena por pallet_id para auditoria del proceso.
    # --------------------------------------------------------------------------

    def process_hu_phase2(
        self,
        hu_code: str,
        phase2_wait: float | None = None,
        origin: Origin | None = None,
    ) -> Phase2Result:
        _, wait_short, _, _ = self._get_origin_timings(origin, hu_code)

        if phase2_wait is None:
            phase2_wait = origin.phase2_wait if origin else WAIT_SHORT

        # Capturar timestamp localmente - no se almacena en self
        phase2_ts = _make_phase2_ts()
        log.info("phase2_timestamp_captured ts=%s", phase2_ts.strftime("%d.%m.%Y %H:%M:%S"))

        t_start = time.time()
        try:
            campo_hu = self._find(FIELD_F2_HU)
            if campo_hu is None:
                log.warning("phase2_wrong_screen hu=%s - re-navigating", hu_code)
                self.setup_phase2(origin=origin)
                campo_hu = self._find(FIELD_F2_HU)
                if campo_hu is None:
                    sbar = self._get_sbar_text()
                    return Phase2Result(
                        status="error",
                        message=f"No se encontro campo HU. SAP: '{sbar}'",
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
                    message="TIMEOUT: SAP no respondio en 15s",
                    duration_ms=int((time.time() - t_start) * 1000),
                    phase2_ts=_INVALID_TS,
                )

            elapsed = time.time() - t_start
            if elapsed < wait_short:
                time.sleep(wait_short - elapsed)

            self._wait_idle()

            duration = int((time.time() - t_start) * 1000)
            log.info(
                "hu_phase2_ok hu=%s duration_ms=%s phase2_wait=%s",
                hu_code,
                duration,
                phase2_wait,
            )
            return Phase2Result(
                status="ok",
                message="OK",
                duration_ms=duration,
                phase2_ts=phase2_ts,   # timestamp valido solo en exito
            )

        except Exception as e:
            duration = int((time.time() - t_start) * 1000)
            log.error("hu_phase2_error hu=%s error=%s", hu_code, e)
            return Phase2Result(
                status="error",
                message=f"ERROR: {e}",
                duration_ms=duration,
                phase2_ts=_INVALID_TS,
            )

    # -- Verificacion estatica -------------------------------------------------

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
