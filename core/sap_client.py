import os
import time
import logging
import pythoncom
import win32com.client
from datetime import datetime
from typing import TypedDict
from django.utils import timezone
from .hu_origins import Origin, detect_origin, UNKNOWN_ORIGIN
from django.conf import settings


log = logging.getLogger(__name__)


SISTEMA_SAP    = getattr(settings, 'SAP_SISTEMA',      'LUP')
TX_MOVEINBHU   = getattr(settings, 'SAP_TX_MOVEINBHU', '/nZMOVEINBHU')
TX_TIJSEP      = getattr(settings, 'SAP_TX_TIJSEP',    '/nZMMTIJSEP')
SAP_CONNECTION_NAME = getattr(settings, 'SAP_CONNECTION_NAME', 'LUP Production [Public]')
SAP_LOGON_EXE = getattr(
    settings,
    'SAP_LOGON_EXE',
    r'C:\Program Files (x86)\SAP\FrontEnd\SAPgui\saplogon.exe',
)
SAP_LOGIN_USER = getattr(settings, 'SAP_LOGIN_USER', '')
SAP_LOGIN_PASSWORD = getattr(settings, 'SAP_LOGIN_PASSWORD', '')
SAP_LOGIN_CLIENT = getattr(settings, 'SAP_LOGIN_CLIENT', '')
SAP_LOGIN_LANGUAGE = getattr(settings, 'SAP_LOGIN_LANGUAGE', 'EN')
SAP_PROBE_BEFORE_WORK = getattr(settings, 'SAP_PROBE_BEFORE_WORK', True)
SAP_STARTUP_TIMEOUT_SECONDS = float(getattr(settings, 'SAP_STARTUP_TIMEOUT_SECONDS', 30))
SAP_STARTUP_POLL_SECONDS = max(float(getattr(settings, 'SAP_STARTUP_POLL_SECONDS', 1)), 0.1)
SAP_SESSION_RESPONSIVE_SECONDS = max(float(getattr(settings, 'SAP_SESSION_RESPONSIVE_SECONDS', 2)), 0.1)
NODE_MOVEINBHU = "F00098"
NODE_TIJSEP    = "F00097"


# Tiempos por defecto (fallback cuando no hay Origin disponible)
WAIT_LONG         = 1.0
WAIT_SHORT        = 1.0
WAIT_TREE         = 3.0
TIMEOUT_SAP       = 15.0
POLL_INTERVAL     = 0.05
SAP_TRANSACTION_MIN_WAIT_SECONDS = max(
    float(getattr(settings, 'SAP_TRANSACTION_MIN_WAIT_SECONDS', 0.05)), 0.0,
)
SAP_TRANSACTION_READY_TIMEOUT_SECONDS = max(
    float(getattr(settings, 'SAP_TRANSACTION_READY_TIMEOUT_SECONDS', TIMEOUT_SAP)),
    SAP_TRANSACTION_MIN_WAIT_SECONDS,
)


# IDs exactos del VBA
TREE_PATH        = "wnd[0]/usr/cntlIMAGE_CONTAINER/shellcont/shell/shellcont[0]/shell"
FIELD_F1_HU      = "wnd[0]/usr/ctxtP_HU"
FIELD_F2_HU      = "wnd[0]/usr/txtGV_HU"
FIELD_LOGIN_CLIENT = "wnd[0]/usr/txtRSYST-MANDT"
FIELD_LOGIN_USER = "wnd[0]/usr/txtRSYST-BNAME"
FIELD_LOGIN_PASSWORD = "wnd[0]/usr/pwdRSYST-BCODE"
FIELD_LOGIN_LANGUAGE = "wnd[0]/usr/txtRSYST-LANGU"
MULTI_LOGON_TITLE = "License Information for Multiple Logons"


# ALV grid de ZMOVEINBHU
ALV_GRID_PATH   = "wnd[0]/usr/cntlCC_ALV/shellcont/shell"
ALV_MSG_COLUMNS = ["MESSAGE", "MSG_TEXT", "TEXT", "MSGTEXT", "DESCRIPTION", "MAKTX"]


SAP_ICON_OK    = "@5B@"
SAP_ICON_ERROR = "@5C@"
SAP_HU_ALREADY_IN_DESTINATION_MESSAGE = "hu already in the destination storage location"
SAP_PHASE2_MATERIAL_TYPES_MESSAGE = "hu contains different material types"
SAP_PHASE2_IMPOSSIBLE_TO_CONTINUE_MESSAGE = "impossible to go on"


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


SAP_DISCONNECTED_KEYWORDS = [
    "connection reset",
    "connection to partner",
    "connection broken",
    "wsaeconnreset",
    "partner",
    "broken",
    "desconect",
    "conexi",
]




# -- TypedDicts para retornos estructurados ------------------------------------


class Phase1Result(TypedDict):
    status:  str   # "ok" | "duplicate" | "already_separated" | "hu_not_found" | "error"
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


class SAPBusyError(SAPConnectionError):
    """SAP respondió, pero siguió ocupado más tiempo del permitido."""


class SAPDisconnectedError(SAPConnectionError):
    """La sesión SAP existe, pero está desconectada o no coincide con la esperada."""


class SAPCOMBlockedError(SAPConnectionError):
    """Una llamada COM/SAP GUI no regresó antes del timeout del worker."""


class SAPCOMFatalBlockedError(SAPCOMBlockedError):
    """SAP COM siguió bloqueado y la corrida actual debe abortarse."""


class SAPBusySessionError(SAPBusyError):
    """Hay sesiones SAP del usuario, pero ninguna esta libre para NEXHUS."""




# -- Centinela de timestamp invalido -------------------------------------------
# Usar datetime.min como valor explicito en Phase2Result cuando F2 falla.
_INVALID_TS = datetime.min




def _make_phase2_ts() -> datetime:
    """Retorna el timestamp de inicio de F2 para auditoria del pallet."""
    return timezone.now()




class SAPClient:
    _last_session_selection_code = 'SAP_NO_READY_SESSION'
    _last_session_selection_message = ''


    def __init__(self, sap_user: str | None = None, sap_client: str | None = None):
        self._session = None
        self._usuario = ""
        self._cliente = ""
        self._sistema = SISTEMA_SAP
        self._expected_user = self._expected_sap_user(sap_user)
        self._expected_client = self._expected_sap_client(sap_client)
        # El timestamp de F2 viaja en Phase2Result para auditoria del pallet.


    # -- Conexion --------------------------------------------------------------


    def connect(
        self,
        sistema: str = SISTEMA_SAP,
        auto_login: bool = True,
        *,
        initialize_com: bool = True,
    ) -> bool:
        """Conecta a SAP; el worker puede reutilizar su COM ya inicializado."""
        if initialize_com:
            pythoncom.CoInitialize()
        for attempt in range(2 if auto_login else 1):
            try:
                app = self._get_sap_app()
            except Exception as e:
                if auto_login and attempt == 0:
                    ok, _, message = self.open_and_login(
                        sistema=sistema,
                        sap_user=self._expected_user,
                        sap_client=self._expected_client,
                    )
                    if not ok:
                        raise SAPConnectionError(message)
                    continue
                raise SAPConnectionError(f"No se pudo obtener SAPGUI: {e}")


            session = self._find_ready_session(
                app,
                sistema,
                probe=SAP_PROBE_BEFORE_WORK,
                sap_user=self._expected_user,
                sap_client=self._expected_client,
            )
            if session is not None:
                self._session = session
                self._sistema = sistema
                self._usuario = session.Info.User
                self._cliente = self._get_session_client(session)
                log.info(
                    "sap_connected system=%s client=%s user=%s",
                    sistema,
                    self._cliente,
                    self._usuario,
                )
                return True

            selection_code = self._last_session_selection_code
            if selection_code == 'SAP_BUSY_SESSION':
                raise SAPBusySessionError(
                    self._last_session_selection_message
                    or 'SAP_BUSY_SESSION: hay sesiones SAP ocupadas o con modal.'
                )

            if auto_login and attempt == 0:
                log.warning(
                    "sap_no_ready_session identity=%s - restarting owned SAP session",
                    self._identity_label(sistema, self._expected_user, self._expected_client),
                )
                self.close_sessions(
                    sistema=sistema,
                    sap_user=self._expected_user,
                    sap_client=self._expected_client,
                )
                ok, _, message = self.open_and_login(
                    sistema=sistema,
                    sap_user=self._expected_user,
                    sap_client=self._expected_client,
                )
                if not ok:
                    raise SAPConnectionError(message)
                continue


        raise SAPConnectionError(
            f"No se encontro sesion SAP activa para "
            f"{self._identity_label(sistema, self._expected_user, self._expected_client)}"
        )


    @property
    def session(self):
        if self._session is None:
            raise SAPConnectionError("Sin sesion SAP.")
        return self._session


    def get_user(self) -> str:
        return self._usuario


    def get_client(self) -> str:
        return self._cliente


    # -- Inicio automatico de SAP ---------------------------------------------


    @staticmethod
    def _clean(value) -> str:
        return str(value or '').strip()


    @classmethod
    def _expected_sap_user(cls, sap_user: str | None = None) -> str:
        return cls._clean(SAP_LOGIN_USER if sap_user is None else sap_user)


    @classmethod
    def _expected_sap_client(cls, sap_client: str | None = None) -> str:
        return cls._clean(SAP_LOGIN_CLIENT if sap_client is None else sap_client)


    @staticmethod
    def _identity_label(sistema: str, sap_user: str = "", sap_client: str = "") -> str:
        parts = [f"system={sistema}"]
        if sap_client:
            parts.append(f"client={sap_client}")
        if sap_user:
            parts.append(f"user={sap_user}")
        return " ".join(parts)


    @classmethod
    def _get_session_client(cls, session) -> str:
        try:
            return cls._clean(getattr(session.Info, 'Client', ''))
        except Exception:
            return ''


    @classmethod
    def _connection_labels(cls, connection) -> list[str]:
        labels = []
        for attr in ('Description', 'Name', 'ConnectionString'):
            try:
                value = cls._clean(getattr(connection, attr, ''))
            except Exception:
                value = ''
            if value:
                labels.append(value)
        return labels


    @classmethod
    def _connection_display_name(cls, connection) -> str:
        labels = cls._connection_labels(connection)
        return ' | '.join(labels) if labels else '<unknown>'


    @classmethod
    def _connection_matches_expected(cls, connection) -> bool:
        expected = cls._clean(SAP_CONNECTION_NAME).upper()
        labels = [label.upper() for label in cls._connection_labels(connection)]
        if not expected or not labels:
            return True
        return any(label == expected or expected in label or label in expected for label in labels)


    @classmethod
    def _has_blocking_modal(cls, session) -> bool:
        return cls._find_on_session(session, "wnd[1]") is not None


    @classmethod
    def _session_transaction(cls, session) -> str:
        try:
            return cls._clean(getattr(session.Info, 'Transaction', ''))
        except Exception:
            return ''


    @classmethod
    def _session_user(cls, session) -> str:
        try:
            return cls._clean(getattr(session.Info, 'User', ''))
        except Exception:
            return ''


    @staticmethod
    def _session_busy(session) -> bool:
        return bool(getattr(session, 'Busy'))


    @staticmethod
    def _responsive_elapsed_ms(started_at: float) -> int:
        return int((time.perf_counter() - started_at) * 1000)


    @classmethod
    def _is_responsive_elapsed(cls, started_at: float) -> bool:
        return (time.perf_counter() - started_at) <= SAP_SESSION_RESPONSIVE_SECONDS


    @classmethod
    def _log_session_selection(
        cls,
        *,
        code: str,
        conn_index: int,
        session_index: int,
        connection_name: str,
        user: str,
        transaction: str,
        busy,
        has_modal,
        selected: bool,
        reason: str,
        started_at: float,
    ) -> None:
        log.info(
            "%s conn_index=%s session_index=%s connection=%r user=%r "
            "transaction=%r busy=%s has_modal=%s selected=%s reason=%s duration_ms=%s",
            code,
            conn_index,
            session_index,
            connection_name,
            user,
            transaction,
            busy,
            has_modal,
            selected,
            reason,
            cls._responsive_elapsed_ms(started_at),
        )


    @classmethod
    def _session_matches_identity(
        cls,
        session,
        sistema: str,
        sap_user: str | None = None,
        sap_client: str | None = None,
    ) -> bool:
        expected_user = cls._expected_sap_user(sap_user)
        expected_client = cls._expected_sap_client(sap_client)


        try:
            actual_system = cls._clean(session.Info.SystemName).upper()
            actual_user = cls._clean(session.Info.User).upper()
            actual_client = cls._get_session_client(session)
        except Exception as e:
            log.debug("sap_identity_read_failed error=%s", e)
            return False


        if actual_system != cls._clean(sistema).upper():
            return False


        if expected_client and actual_client != expected_client:
            log.debug(
                "sap_session_skip_client expected=%s actual=%s user=%s",
                expected_client,
                actual_client,
                actual_user,
            )
            return False


        if expected_user and actual_user != expected_user.upper():
            log.debug(
                "sap_session_skip_user expected=%s actual=%s client=%s",
                expected_user,
                actual_user,
                actual_client,
            )
            return False


        return True


    @staticmethod
    def _find_on_session(session, element_id: str):
        try:
            return session.findById(element_id)
        except Exception:
            return None


    @staticmethod
    def _get_sap_app():
        sap_gui = win32com.client.GetObject("SAPGUI")
        return sap_gui.GetScriptingEngine


    @classmethod
    def _wait_for_sap_app(cls, timeout: float | None = None):
        """
        Espera a que SAP Logon publique el ScriptingEngine de COM.


        Despues de reiniciar SAP, `saplogon.exe` puede estar abierto pero
        `GetObject("SAPGUI")` aun no estar disponible. Reintentar evita que el
        inicio automatico falle por una condicion de arranque de Windows/SAP.
        """
        timeout = SAP_STARTUP_TIMEOUT_SECONDS if timeout is None else timeout
        deadline = time.time() + timeout
        last_error = None


        while time.time() <= deadline:
            try:
                return cls._get_sap_app()
            except Exception as exc:
                last_error = exc
                log.debug("sap_scripting_engine_not_ready error=%s", exc)
                time.sleep(SAP_STARTUP_POLL_SECONDS)


        raise SAPConnectionError(
            f"SAP GUI no expuso ScriptingEngine despues de {timeout:.0f}s: {last_error}"
        )


    @staticmethod
    def _window_text(session, window_id: str) -> str:
        try:
            window = session.findById(window_id)
            return str(getattr(window, 'Text', '') or '')
        except Exception:
            return ''


    @classmethod
    def _session_text_snapshot(cls, session) -> str:
        active_title = str(getattr(session.ActiveWindow, 'Text', '') or '')
        wnd0_title = cls._window_text(session, "wnd[0]")
        wnd1_title = cls._window_text(session, "wnd[1]")
        sbar = cls._find_on_session(session, "wnd[0]/sbar")
        sbar_text = str(getattr(sbar, 'Text', '') or '') if sbar else ''
        return " ".join([active_title, wnd0_title, wnd1_title, sbar_text]).lower()


    @classmethod
    def _has_disconnected_dialog(cls, session) -> bool:
        combined = cls._session_text_snapshot(session)
        if any(keyword in combined for keyword in SAP_DISCONNECTED_KEYWORDS):
            log.warning("sap_session_disconnected_dialog text=%s", combined[:300])
            return True
        return False


    @classmethod
    def _is_login_screen(cls, session) -> bool:
        return bool(
            cls._find_on_session(session, FIELD_LOGIN_USER)
            or cls._find_on_session(session, FIELD_LOGIN_PASSWORD)
        )


    @staticmethod
    def _wait_session_idle(session, timeout: float = 5.0) -> bool:
        started_at = time.perf_counter()
        deadline = time.time() + timeout
        while time.time() <= deadline:
            try:
                if not session.Busy:
                    elapsed_ms = int((time.perf_counter() - started_at) * 1000)
                    if elapsed_ms >= 1000:
                        log.info("sap_wait_session_idle_done duration_ms=%s timeout_ms=%s", elapsed_ms, int(timeout * 1000))
                    return True
            except Exception:
                elapsed_ms = int((time.perf_counter() - started_at) * 1000)
                log.warning("sap_wait_session_idle_disconnected duration_ms=%s", elapsed_ms)
                return False
            time.sleep(POLL_INTERVAL)
        elapsed_ms = int((time.perf_counter() - started_at) * 1000)
        log.warning("sap_wait_session_idle_timeout duration_ms=%s timeout_ms=%s", elapsed_ms, int(timeout * 1000))
        return False


    @classmethod
    def _probe_session_for_work(cls, session) -> bool:
        """
        Ejecuta una navegacion inocua a SAP Easy Access.


        Leer propiedades COM no siempre dispara el popup de desconexion. Enviar
        `/n` obliga a SAP GUI a confirmar que la sesion sigue utilizable antes
        de que Celery capture un HU.
        """
        try:
            if cls._session_busy(session):
                log.warning("SAP_SESSION_BUSY_SKIPPED probe=false reason=busy_before_probe")
                return False
            if cls._has_blocking_modal(session):
                log.warning("SAP_SESSION_MODAL_SKIPPED probe=false reason=modal_before_probe")
                return False
            ok_code = cls._find_on_session(session, "wnd[0]/tbar[0]/okcd")
            wnd = cls._find_on_session(session, "wnd[0]")
            if ok_code is None or wnd is None:
                return False


            ok_code.Text = "/n"
            wnd.sendVKey(0)
            cls._wait_session_idle(session, timeout=5.0)
            time.sleep(0.3)


            if cls._has_disconnected_dialog(session) or cls._is_login_screen(session):
                return False


            _ = session.Info.User
            _ = session.Info.SystemName
            return True
        except Exception as e:
            log.warning("sap_session_probe_failed error=%s", e)
            return False


    @classmethod
    def _session_is_alive(cls, session, probe: bool = False) -> bool:
        """
        Fuerza una lectura activa de SAP GUI.


        SAP puede conservar objetos COM aunque la conexion LUP ya este rota; al
        leer ventana/barra de estado detectamos el popup "connection reset" antes
        de mandar trabajo a Celery.
        """
        try:
            _ = session.Info.SystemName
            _ = session.Info.User
            if cls._has_disconnected_dialog(session) or cls._is_login_screen(session):
                return False


            if cls._session_busy(session):
                log.warning("SAP_SESSION_BUSY_SKIPPED reason=session_is_alive")
                return False
            if cls._has_blocking_modal(session):
                log.warning("SAP_SESSION_MODAL_SKIPPED reason=session_is_alive")
                return False
            return cls._probe_session_for_work(session) if probe else True
        except Exception as e:
            log.warning("sap_session_not_alive error=%s", e)
            return False


    @classmethod
    def _find_ready_session(
        cls,
        app,
        sistema: str,
        probe: bool = False,
        sap_user: str | None = None,
        sap_client: str | None = None,
    ):
        cls._last_session_selection_code = 'SAP_NO_READY_SESSION'
        cls._last_session_selection_message = 'SAP_NO_READY_SESSION: no hay sesion SAP disponible para NEXHUS.'
        blocked_candidates = 0
        if int(app.Children.Count) == 0:
            log.warning("SAP_NO_READY_SESSION reason=no_connections")
            return None


        for i_conn in range(int(app.Children.Count)):
            conn = app.Children(i_conn)
            connection_name = cls._connection_display_name(conn)
            connection_matches = cls._connection_matches_expected(conn)
            for i_sess in range(int(conn.Children.Count)):
                started_at = time.perf_counter()
                sess = conn.Children(i_sess)
                user = ''
                transaction = ''
                busy = 'unknown'
                has_modal = 'unknown'
                try:
                    user = cls._session_user(sess)
                    transaction = cls._session_transaction(sess)
                    busy = cls._session_busy(sess)
                    if not cls._is_responsive_elapsed(started_at):
                        blocked_candidates += 1
                        cls._log_session_selection(
                            code='SAP_SESSION_BUSY_SKIPPED',
                            conn_index=i_conn,
                            session_index=i_sess,
                            connection_name=connection_name,
                            user=user,
                            transaction=transaction,
                            busy=busy,
                            has_modal=has_modal,
                            selected=False,
                            reason='session_not_responsive',
                            started_at=started_at,
                        )
                        continue

                    if not connection_matches:
                        cls._log_session_selection(
                            code='SAP_SESSION_SKIPPED',
                            conn_index=i_conn,
                            session_index=i_sess,
                            connection_name=connection_name,
                            user=user,
                            transaction=transaction,
                            busy=busy,
                            has_modal=has_modal,
                            selected=False,
                            reason='connection_mismatch',
                            started_at=started_at,
                        )
                        continue

                    if not cls._session_matches_identity(
                        sess,
                        sistema,
                        sap_user=sap_user,
                        sap_client=sap_client,
                    ):
                        cls._log_session_selection(
                            code='SAP_SESSION_SKIPPED',
                            conn_index=i_conn,
                            session_index=i_sess,
                            connection_name=connection_name,
                            user=user,
                            transaction=transaction,
                            busy=busy,
                            has_modal=has_modal,
                            selected=False,
                            reason='identity_mismatch',
                            started_at=started_at,
                        )
                        continue

                    if busy:
                        blocked_candidates += 1
                        cls._log_session_selection(
                            code='SAP_SESSION_BUSY_SKIPPED',
                            conn_index=i_conn,
                            session_index=i_sess,
                            connection_name=connection_name,
                            user=user,
                            transaction=transaction,
                            busy=busy,
                            has_modal='not_checked_busy',
                            selected=False,
                            reason='busy',
                            started_at=started_at,
                        )
                        continue

                    has_modal = cls._has_blocking_modal(sess)
                    if has_modal:
                        blocked_candidates += 1
                        cls._log_session_selection(
                            code='SAP_SESSION_MODAL_SKIPPED',
                            conn_index=i_conn,
                            session_index=i_sess,
                            connection_name=connection_name,
                            user=user,
                            transaction=transaction,
                            busy=busy,
                            has_modal=has_modal,
                            selected=False,
                            reason='modal',
                            started_at=started_at,
                        )
                        continue

                    if cls._session_is_alive(sess, probe=probe):
                        cls._log_session_selection(
                            code='SAP_READY_SESSION_SELECTED',
                            conn_index=i_conn,
                            session_index=i_sess,
                            connection_name=connection_name,
                            user=user,
                            transaction=transaction,
                            busy=busy,
                            has_modal=has_modal,
                            selected=True,
                            reason='ready',
                            started_at=started_at,
                        )
                        return sess
                    blocked_candidates += 1
                    cls._log_session_selection(
                        code='SAP_SESSION_BUSY_SKIPPED',
                        conn_index=i_conn,
                        session_index=i_sess,
                        connection_name=connection_name,
                        user=user,
                        transaction=transaction,
                        busy=busy,
                        has_modal=has_modal,
                        selected=False,
                        reason='not_alive_or_probe_failed',
                        started_at=started_at,
                    )
                except Exception as e:
                    blocked_candidates += 1
                    cls._log_session_selection(
                        code='SAP_SESSION_BUSY_SKIPPED',
                        conn_index=i_conn,
                        session_index=i_sess,
                        connection_name=connection_name,
                        user=user,
                        transaction=transaction,
                        busy=busy,
                        has_modal=has_modal,
                        selected=False,
                        reason=f'evaluation_error:{e}',
                        started_at=started_at,
                    )
                    continue
        if blocked_candidates:
            cls._last_session_selection_code = 'SAP_BUSY_SESSION'
            cls._last_session_selection_message = (
                'SAP_BUSY_SESSION: existen sesiones SAP del usuario/conexion, '
                'pero estan ocupadas, con modal o no respondieron rapido.'
            )
        log.warning(
            "%s blocked_candidates=%s",
            cls._last_session_selection_code,
            blocked_candidates,
        )
        return None


    @classmethod
    def close_sessions(
        cls,
        sistema: str = SISTEMA_SAP,
        sap_user: str | None = None,
        sap_client: str | None = None,
    ) -> int:
        """Cierra solo las sesiones SAP que coinciden con la identidad esperada."""
        closed = 0
        expected_user = cls._expected_sap_user(sap_user)
        expected_client = cls._expected_sap_client(sap_client)
        try:
            pythoncom.CoInitialize()
            try:
                app = cls._get_sap_app()
                for i_conn in reversed(range(int(app.Children.Count))):
                    conn = app.Children(i_conn)
                    for i_sess in reversed(range(int(conn.Children.Count))):
                        sess = conn.Children(i_sess)
                        try:
                            if not cls._session_matches_identity(
                                sess,
                                sistema,
                                sap_user=expected_user,
                                sap_client=expected_client,
                            ):
                                continue
                            wnd = cls._find_on_session(sess, "wnd[0]")
                            if wnd is None:
                                continue
                            wnd.Close()
                            time.sleep(0.5)
                            yes_button = cls._find_on_session(sess, "wnd[1]/usr/btnSPOP-OPTION1")
                            if yes_button:
                                yes_button.press()
                            closed += 1
                        except Exception as e:
                            log.debug("sap_close_session_skip error=%s", e)
            finally:
                pythoncom.CoUninitialize()
        except Exception as e:
            log.warning("sap_close_sessions_failed error=%s", e)


        if closed:
            log.info(
                "sap_sessions_closed identity=%s count=%s",
                cls._identity_label(sistema, expected_user, expected_client),
                closed,
            )
        return closed


    @staticmethod
    def _handle_multiple_logon(session) -> None:
        """Selecciona la opcion permitida cuando SAP muestra multiples sesiones."""
        try:
            popup = SAPClient._find_on_session(session, "wnd[1]")
            active_title = getattr(session.ActiveWindow, 'Text', '')
            popup_title = getattr(popup, 'Text', '') if popup else ''
            if active_title == MULTI_LOGON_TITLE or popup_title == MULTI_LOGON_TITLE:
                session.findById("wnd[1]/usr/radMULTI_LOGON_OPT2").select()
                session.findById("wnd[1]/tbar[0]/btn[0]").press()
                time.sleep(1)
                log.info("sap_multi_logon_option_selected")
        except Exception as e:
            log.debug("sap_multi_logon_skip error=%s", e)


    @classmethod
    def ensure_session_ready(
        cls,
        sistema: str = SISTEMA_SAP,
        sap_user: str | None = None,
        sap_client: str | None = None,
    ) -> tuple[bool, str, str]:
        """
        Verifica una sesion SAP activa. Si no existe, abre SAP Logon y autentica
        con las credenciales configuradas en .env.
        """
        expected_user = cls._expected_sap_user(sap_user)
        expected_client = cls._expected_sap_client(sap_client)
        connected, user = cls.check_session(
            sistema,
            probe=SAP_PROBE_BEFORE_WORK,
            sap_user=expected_user,
            sap_client=expected_client,
        )
        if connected:
            return True, user, 'Sesion SAP activa'


        cls.close_sessions(
            sistema=sistema,
            sap_user=expected_user,
            sap_client=expected_client,
        )
        return cls.open_and_login(
            sistema=sistema,
            sap_user=expected_user,
            sap_client=expected_client,
        )


    @classmethod
    def open_and_login(
        cls,
        sistema: str = SISTEMA_SAP,
        sap_user: str | None = None,
        sap_client: str | None = None,
    ) -> tuple[bool, str, str]:
        """Abre SAP Logon, crea la conexion configurada y llena la pantalla de login."""
        if not os.path.isfile(SAP_LOGON_EXE):
            return False, '', f'saplogon.exe no encontrado: {SAP_LOGON_EXE}'


        expected_user = cls._expected_sap_user(sap_user)
        expected_client = cls._expected_sap_client(sap_client)


        try:
            os.startfile(SAP_LOGON_EXE)


            pythoncom.CoInitialize()
            try:
                app = cls._wait_for_sap_app()
                connection = app.OpenConnection(SAP_CONNECTION_NAME, True)
                time.sleep(2)


                login_started_at = time.perf_counter()
                session = connection.Children(0)
                if cls._session_busy(session):
                    log.warning(
                        "SAP_SESSION_BUSY_SKIPPED conn_index=new session_index=0 "
                        "connection=%r reason=login_session_busy duration_ms=%s",
                        cls._connection_display_name(connection),
                        cls._responsive_elapsed_ms(login_started_at),
                    )
                    return False, '', 'SAP_BUSY_SESSION: la sesion nueva de login esta ocupada.'
                if cls._has_blocking_modal(session):
                    log.warning(
                        "SAP_SESSION_MODAL_SKIPPED conn_index=new session_index=0 "
                        "connection=%r reason=login_session_modal duration_ms=%s",
                        cls._connection_display_name(connection),
                        cls._responsive_elapsed_ms(login_started_at),
                    )
                    return False, '', 'SAP_BUSY_SESSION: la sesion nueva de login tiene un modal activo.'
                if not cls._is_responsive_elapsed(login_started_at):
                    log.warning(
                        "SAP_SESSION_BUSY_SKIPPED conn_index=new session_index=0 "
                        "connection=%r reason=login_session_not_responsive duration_ms=%s",
                        cls._connection_display_name(connection),
                        cls._responsive_elapsed_ms(login_started_at),
                    )
                    return False, '', 'SAP_BUSY_SESSION: la sesion nueva de login no respondio a tiempo.'
                client_field = cls._find_on_session(session, FIELD_LOGIN_CLIENT)
                user_field = cls._find_on_session(session, FIELD_LOGIN_USER)
                password_field = cls._find_on_session(session, FIELD_LOGIN_PASSWORD)
                language_field = cls._find_on_session(session, FIELD_LOGIN_LANGUAGE)


                if user_field and password_field:
                    if not expected_user or not SAP_LOGIN_PASSWORD:
                        return (
                            False,
                            '',
                            'Credenciales SAP no configuradas: define SAP_LOGIN_USER y SAP_LOGIN_PASSWORD.',
                        )


                    if client_field and expected_client:
                        client_field.text = expected_client
                    user_field.text = expected_user
                    password_field.text = SAP_LOGIN_PASSWORD
                    if language_field:
                        language_field.text = SAP_LOGIN_LANGUAGE or 'EN'
                    session.findById("wnd[0]").sendVKey(0)
                    time.sleep(2)


                cls._handle_multiple_logon(session)
            finally:
                pythoncom.CoUninitialize()


            connected, user = cls.check_session(
                sistema,
                probe=SAP_PROBE_BEFORE_WORK,
                sap_user=expected_user,
                sap_client=expected_client,
            )
            if connected:
                return True, user, 'SAP inicializado y autenticado correctamente'
            return (
                False,
                '',
                f'No se encontro sesion SAP para '
                f'{cls._identity_label(sistema, expected_user, expected_client)} '
                'despues del login SAP',
            )


        except Exception as e:
            log.exception("sap_auto_login_failed")
            return False, '', f'No se pudo inicializar SAP automaticamente: {e}'


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
        started_at = time.perf_counter()
        deadline = time.time() + timeout
        while True:
            try:
                if not self._session.Busy:
                    elapsed_ms = int((time.perf_counter() - started_at) * 1000)
                    if elapsed_ms >= 1000:
                        log.info("sap_wait_idle_done duration_ms=%s timeout_ms=%s", elapsed_ms, int(timeout * 1000))
                    return True
            except Exception:
                elapsed_ms = int((time.perf_counter() - started_at) * 1000)
                log.warning("sap_wait_idle_disconnected duration_ms=%s", elapsed_ms)
                return True
            if time.time() > deadline:
                elapsed_ms = int((time.perf_counter() - started_at) * 1000)
                log.warning("sap_wait_idle_timeout duration_ms=%s timeout_ms=%s", elapsed_ms, int(timeout * 1000))
                return False
            time.sleep(POLL_INTERVAL)


    @staticmethod
    def _normalized_transaction_code(transaction_code: str) -> str:
        """Normaliza /nZTRANSACCION para compararlo con Session.Info.Transaction."""
        normalized = str(transaction_code or '').strip().upper()
        return normalized[2:] if normalized.startswith('/N') else normalized


    def _current_transaction_code(self) -> str:
        try:
            return str(self._session.Info.Transaction or '').strip()
        except Exception:
            return ''


    def _wait_for_transaction_ready(
        self,
        tx_code: str,
        ready_field: str | None = None,
        timeout: float = SAP_TRANSACTION_READY_TIMEOUT_SECONDS,
    ) -> tuple[bool, str, int, bool]:
        """
        Espera la transaccion destino sin retrasos fijos largos.

        La pantalla puede reportar ``Busy=False`` antes de que SAP actualice sus
        controles; por eso se exige un retardo minimo y luego se confirma la
        transaccion y el campo destino o el arbol de navegacion.
        """
        started_at = time.monotonic()
        deadline = started_at + max(timeout, SAP_TRANSACTION_MIN_WAIT_SECONDS)
        expected_transaction = self._normalized_transaction_code(tx_code)
        busy_seen = False

        if SAP_TRANSACTION_MIN_WAIT_SECONDS:
            time.sleep(SAP_TRANSACTION_MIN_WAIT_SECONDS)

        while True:
            try:
                is_busy = bool(self._session.Busy)
            except Exception:
                elapsed_ms = int((time.monotonic() - started_at) * 1000)
                return False, 'session_unavailable', elapsed_ms, busy_seen

            busy_seen = busy_seen or is_busy
            current_transaction = self._current_transaction_code()
            transaction_ready = (
                not is_busy
                and self._normalized_transaction_code(current_transaction) == expected_transaction
            )
            if transaction_ready:
                if ready_field and self._find(ready_field) is not None:
                    elapsed_ms = int((time.monotonic() - started_at) * 1000)
                    return True, 'field', elapsed_ms, busy_seen
                if ready_field and self._find(TREE_PATH) is not None:
                    elapsed_ms = int((time.monotonic() - started_at) * 1000)
                    return False, 'tree', elapsed_ms, busy_seen
                if ready_field is None:
                    elapsed_ms = int((time.monotonic() - started_at) * 1000)
                    return True, 'transaction', elapsed_ms, busy_seen

            if time.monotonic() >= deadline:
                elapsed_ms = int((time.monotonic() - started_at) * 1000)
                log.warning(
                    "SAP_BUSY tx=%s readiness_timeout duration_ms=%s timeout_ms=%s busy_seen=%s",
                    tx_code,
                    elapsed_ms,
                    int(timeout * 1000),
                    busy_seen,
                )
                return False, 'timeout', elapsed_ms, busy_seen
            time.sleep(POLL_INTERVAL)


    def _get_sbar_text(self) -> str:
        obj = self._find("wnd[0]/sbar")
        try:
            return obj.Text if obj else ""
        except Exception:
            return ""


    def _raise_if_session_invalid(self, context: str) -> None:
        if not self._session_matches_identity(
            self.session,
            self._sistema,
            sap_user=self._expected_user,
            sap_client=self._expected_client,
        ):
            raise SAPDisconnectedError(
                f"Sesion SAP no coincide con la identidad esperada durante {context}: "
                f"{self._identity_label(self._sistema, self._expected_user, self._expected_client)}"
            )


        if not self._session_is_alive(self.session):
            raise SAPDisconnectedError(f"Sesion SAP desconectada durante {context}")


    def is_session_healthy(self) -> bool:
        """Comprueba una sesion ya conectada sin ejecutar el probe activo /n."""
        if self._session is None:
            return False
        return (
            self._session_matches_identity(
                self._session,
                self._sistema,
                sap_user=self._expected_user,
                sap_client=self._expected_client,
            )
            and self._session_is_alive(self._session, probe=False)
        )


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


    @staticmethod
    def _is_hu_already_in_destination_storage_location(message: str) -> bool:
        """Reconoce el aviso SAP que confirma una separazione ya aplicada."""
        normalized = " ".join(str(message or "").lower().split())
        return SAP_HU_ALREADY_IN_DESTINATION_MESSAGE in normalized


    @staticmethod
    def _is_phase2_material_types_error(message: str) -> bool:
        """Reconoce el rechazo de ZMMTIJSEP que no genera movimiento ni receipt."""
        normalized = " ".join(str(message or "").lower().split())
        return (
            SAP_PHASE2_MATERIAL_TYPES_MESSAGE in normalized
            and SAP_PHASE2_IMPOSSIBLE_TO_CONTINUE_MESSAGE in normalized
        )


    def _get_phase2_screen_error(self) -> tuple[bool, str]:
        """Detecta y lee el mensaje que ZMMTIJSEP solo muestra al rechazar un HU."""
        first_field = self._find("wnd[0]/usr/txtGV_MESSAGE01")
        if first_field is None:
            return False, ""

        message_parts = []
        for index in range(1, 11):
            field = first_field if index == 1 else self._find(
                f"wnd[0]/usr/txtGV_MESSAGE{index:02d}"
            )
            if field is None:
                continue
            try:
                text = str(field.Text).strip()
            except Exception:
                continue
            if text:
                message_parts.append(text)
        return True, " ".join(message_parts)


    def _build_phase2_screen_error_result(
        self,
        hu_code: str,
        t_start: float,
        screen_message: str,
    ) -> Phase2Result:
        """Convierte un mensaje de pantalla de ZMMTIJSEP en resultado F2 fallido."""
        duration = int((time.time() - t_start) * 1000)
        message = " ".join(screen_message.split()) or 'SAP reporto un error sin detalle.'
        if self._is_phase2_material_types_error(screen_message):
            log.warning("hu_phase2_material_types_error hu=%s msg=%r", hu_code, screen_message)
        else:
            log.warning("hu_phase2_screen_error hu=%s msg=%r", hu_code, screen_message)

        return Phase2Result(
            status="error",
            message=message[:255],
            duration_ms=duration,
            phase2_ts=_INVALID_TS,
        )


    # -- Navegacion ------------------------------------------------------------


    def _abrir_transaccion(
        self,
        tx_code: str,
        origin: Origin | None = None,
        ready_field: str | None = None,
    ) -> bool:
        okcd = self._find("wnd[0]/tbar[0]/okcd")
        if okcd:
            okcd.Text = tx_code
        wnd = self._find("wnd[0]")
        if wnd:
            wnd.sendVKey(0)
        field_ready, readiness, elapsed_ms, busy_seen = self._wait_for_transaction_ready(
            tx_code,
            ready_field=ready_field,
        )
        self._raise_if_session_invalid(f"abrir transaccion {tx_code}")
        log.info(
            "tx_opened code=%s current_tx=%s readiness=%s field_ready=%s "
            "busy_seen=%s duration_ms=%s min_wait_ms=%s",
            tx_code,
            self._current_transaction_code(),
            readiness,
            field_ready,
            busy_seen,
            elapsed_ms,
            int(SAP_TRANSACTION_MIN_WAIT_SECONDS * 1000),
        )
        return field_ready


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


        field_ready = self._abrir_transaccion(
            TX_MOVEINBHU,
            origin=origin,
            ready_field=FIELD_F1_HU,
        )


        if not field_ready and self._find(FIELD_F1_HU) is None:
            self._navegar_nodo(NODE_MOVEINBHU, origin=origin)


        if self._find(FIELD_F1_HU) is not None:
            log.info("phase1_ready - ctxtP_HU confirmed")
        else:
            log.warning("phase1_field_not_visible sbar=%r", self._get_sbar_text())


    def setup_phase2(self, origin: Origin | None = None) -> None:
        log.info("setup_phase2_start")
        self._close_all_popups()


        field_ready = self._abrir_transaccion(
            TX_TIJSEP,
            origin=origin,
            ready_field=FIELD_F2_HU,
        )


        if not field_ready and self._find(FIELD_F2_HU) is None:
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
                    self._raise_if_session_invalid("F1")
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
            if not self._wait_idle(timeout=TIMEOUT_SAP):
                log.warning("SAP_BUSY hu=%s phase=F1 timeout_ms=%s", hu_code, int(TIMEOUT_SAP * 1000))
                return Phase1Result(
                    status="error",
                    message=f"SAP ocupado: no respondio en {TIMEOUT_SAP:g}s",
                    sbar="",
                )
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

                if self._is_hu_already_in_destination_storage_location(alv_message):
                    log.info(
                        "hu_phase1_already_separated hu=%s msg=%r",
                        hu_code,
                        alv_message,
                    )
                    wnd_back = self._find("wnd[0]")
                    if wnd_back:
                        wnd_back.sendVKey(3)
                    self._wait_idle()
                    time.sleep(wait_short)
                    return Phase1Result(
                        status="already_separated",
                        message="Ya se hizo el Acknowledge",
                        sbar=sbar,
                    )

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
                    self._raise_if_session_invalid("F2")
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

            if not self._wait_idle(timeout=TIMEOUT_SAP):
                log.warning("SAP_BUSY hu=%s phase=F2 step=submit timeout_ms=%s", hu_code, int(TIMEOUT_SAP * 1000))
                return Phase2Result(
                    status="error",
                    message=f"SAP ocupado: no respondio en {TIMEOUT_SAP:g}s",
                    duration_ms=int((time.time() - t_start) * 1000),
                    phase2_ts=_INVALID_TS,
                )

            phase2_error_visible, phase2_screen_message = self._get_phase2_screen_error()
            log.info(
                "hu_phase2_message_field_check hu=%s stage=before_enter visible=%s message=%r",
                hu_code,
                phase2_error_visible,
                phase2_screen_message,
            )
            enter_button = self._find("wnd[0]/usr/btnENTER")
            if phase2_error_visible:
                # SAP requiere confirmar el mensaje para volver a una pantalla limpia,
                # pero el resultado ya fue capturado y no debe continuar a ZE16.
                if enter_button is not None:
                    enter_button.press()
                    self._wait_idle()
                return self._build_phase2_screen_error_result(
                    hu_code,
                    t_start,
                    phase2_screen_message,
                )

            if enter_button is not None:
                enter_button.press()


            if phase2_wait > 0:
                time.sleep(phase2_wait)


            if not self._wait_idle(timeout=TIMEOUT_SAP):
                log.warning("SAP_BUSY hu=%s phase=F2 step=confirm timeout_ms=%s", hu_code, int(TIMEOUT_SAP * 1000))
                return Phase2Result(
                    status="error",
                    message=f"SAP ocupado: no respondio en {TIMEOUT_SAP:g}s",
                    duration_ms=int((time.time() - t_start) * 1000),
                    phase2_ts=_INVALID_TS,
                )


            elapsed = time.time() - t_start
            if elapsed < wait_short:
                time.sleep(wait_short - elapsed)


            self._wait_idle()

            phase2_error_visible, phase2_screen_message = self._get_phase2_screen_error()
            phase2_feedback = " ".join(
                part for part in (phase2_screen_message, self._get_sbar_text()) if part
            )
            popup = self._get_popup()
            if popup is not None:
                popup_title = ""
                popup_text = ""
                try:
                    popup_title = str(popup.Text).strip()
                except Exception:
                    pass

                message_label = self._find("wnd[1]/usr/txtMESSTXT1")
                if message_label is None:
                    message_label = self._find("wnd[1]/usr/lbl[1,2]")
                if message_label is not None:
                    try:
                        popup_text = str(message_label.Text).strip()
                    except Exception:
                        pass
                phase2_feedback = " ".join(
                    part for part in (popup_text, popup_title, phase2_feedback) if part
                )

            if self._is_phase2_material_types_error(phase2_feedback):
                if popup is not None:
                    popup_window = self._find("wnd[1]")
                    if popup_window is not None:
                        popup_window.sendVKey(0)
                        self._wait_idle()

                duration = int((time.time() - t_start) * 1000)
                message = (
                    'HU contiene diferentes tipos de material. '
                    f'Separazione no puede continuar. SAP: {phase2_feedback}'
                )
                log.warning("hu_phase2_material_types_error hu=%s msg=%r", hu_code, phase2_feedback)
                return Phase2Result(
                    status="error",
                    message=message[:255],
                    duration_ms=duration,
                    phase2_ts=_INVALID_TS,
                )

            if phase2_error_visible:
                return self._build_phase2_screen_error_result(
                    hu_code,
                    t_start,
                    phase2_screen_message,
                )


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
    def check_session(
        sistema: str = SISTEMA_SAP,
        probe: bool = False,
        sap_user: str | None = None,
        sap_client: str | None = None,
    ) -> tuple[bool, str]:
        try:
            pythoncom.CoInitialize()
            try:
                app = SAPClient._get_sap_app()
                sess = SAPClient._find_ready_session(
                    app,
                    sistema,
                    probe=probe,
                    sap_user=sap_user,
                    sap_client=sap_client,
                )
                if sess is not None:
                    return True, sess.Info.User
            finally:
                pythoncom.CoUninitialize()
        except Exception:
            pass
        return False, ""
