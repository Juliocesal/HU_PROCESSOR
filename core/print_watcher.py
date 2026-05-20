
import time
import threading
import logging
import ctypes
import ctypes.wintypes

log = logging.getLogger(__name__)

_PRINT_TITLES = ("Print", "Imprimir")

INPUT_MOUSE          = 0
MOUSEEVENTF_MOVE     = 0x0001
MOUSEEVENTF_LEFTDOWN = 0x0002
MOUSEEVENTF_LEFTUP   = 0x0004
MOUSEEVENTF_ABSOLUTE = 0x8000


class MOUSEINPUT(ctypes.Structure):
    _fields_ = [
        ("dx",          ctypes.c_long),
        ("dy",          ctypes.c_long),
        ("mouseData",   ctypes.c_ulong),
        ("dwFlags",     ctypes.c_ulong),
        ("time",        ctypes.c_ulong),
        ("dwExtraInfo", ctypes.POINTER(ctypes.c_ulong)),
    ]


class INPUT(ctypes.Structure):
    class _INPUT(ctypes.Union):
        _fields_ = [("mi", MOUSEINPUT)]
    _anonymous_ = ("_input",)
    _fields_    = [("type", ctypes.c_ulong), ("_input", _INPUT)]


class PrintDialogWatcher:
    """
    Watcher que confirma automáticamente los diálogos de impresión de Windows.
    Usa tres métodos en cascada para máxima confiabilidad:
      1. SendMessage(BM_CLICK) directo al botón — sin foco, instantáneo
      2. SendInput click simulado — requiere foco pero funciona con PrintDlg
      3. PostMessage(WM_KEYDOWN, VK_RETURN) — fallback final
    """

    def __init__(self, idle_timeout: float = 8.0, poll_interval: float = 0.15):
        self._idle_timeout    = idle_timeout
        self._poll_interval   = poll_interval
        self._thread: threading.Thread | None = None
        self._stop_event      = threading.Event()
        self._confirmed_count = 0
        self._last_hwnd       = 0
        self._expected_count  = 0

    # ── API pública ───────────────────────────────────────────────────────────

    def start(self, expected_count: int = 0):
        # Detener el thread anterior de forma determinista
        if self._thread and self._thread.is_alive():
            self._stop_event.set()
            self._thread.join(timeout=1.0)  # espera REAL, no sleep ciego
            if self._thread.is_alive():
                log.warning("print_watcher old thread did not stop cleanly")

        # Limpiar estado DESPUÉS del join — nunca antes
        self._stop_event.clear()
        self._confirmed_count = 0
        self._last_hwnd       = 0
        self._expected_count  = expected_count
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        log.info(
            "print_watcher_started idle_timeout=%.1fs expected=%d",
            self._idle_timeout, expected_count
        )

    def stop(self):
        self._stop_event.set()

    @property
    def confirmed_count(self) -> int:
        return self._confirmed_count

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    # ── Internals ─────────────────────────────────────────────────────────────

    def _find_print_dialog(self) -> int:
        try:
            import win32gui
            for title in _PRINT_TITLES:
                hwnd = win32gui.FindWindow(None, title)
                if hwnd and win32gui.IsWindowVisible(hwnd):
                    if hwnd != self._last_hwnd:
                        return hwnd
        except Exception as e:
            log.debug("find_error: %s", e)
        return 0

    def _find_ok_button(self, hwnd: int) -> tuple[int, int, int]:
        """Retorna (ok_hwnd, center_x, center_y) o (0,0,0)."""
        try:
            import win32gui
            ok_hwnd = 0

            def _enum(child, _):
                nonlocal ok_hwnd
                try:
                    if win32gui.GetClassName(child) == "Button":
                        text = win32gui.GetWindowText(child).strip().upper()
                        if text in ("OK", "ACEPTAR"):
                            ok_hwnd = child
                except Exception:
                    pass

            win32gui.EnumChildWindows(hwnd, _enum, None)
            if not ok_hwnd:
                return (0, 0, 0)

            rect = win32gui.GetWindowRect(ok_hwnd)
            cx = (rect[0] + rect[2]) // 2
            cy = (rect[1] + rect[3]) // 2
            return (ok_hwnd, cx, cy)
        except Exception as e:
            log.debug("find_ok_error: %s", e)
            return (0, 0, 0)

    def _method1_send_message(self, ok_hwnd: int) -> bool:
        """
        Método 1: SendMessage BM_CLICK directo al botón.
        Funciona SIN foco cuando el diálogo es de la misma sesión de usuario.
        Es síncrono — espera a que el botón procese el click.
        """
        try:
            import win32con
            # SendMessage (no Post) — síncrono, espera respuesta
            ctypes.windll.user32.SendMessageW(
                ok_hwnd, win32con.BM_CLICK, 0, 0
            )
            log.info("method1_BM_CLICK ok_hwnd=%d", ok_hwnd)
            return True
        except Exception as e:
            log.debug("method1_failed: %s", e)
            return False

    def _method2_send_input(self, hwnd: int, cx: int, cy: int) -> bool:
        """
        Método 2: SetForegroundWindow + SendInput.
        Funciona cuando el diálogo es PrintDlg() nativo que ignora BM_CLICK.
        """
        try:
            import win32gui

            # Trick para forzar SetForegroundWindow entre procesos:
            # attachear el thread input de nuestro proceso al de la ventana objetivo
            import win32process
            import win32api

            our_tid   = win32api.GetCurrentThreadId()
            target_tid, _ = win32process.GetWindowThreadProcessId(hwnd)

            attached = False
            if our_tid != target_tid:
                try:
                    ctypes.windll.user32.AttachThreadInput(our_tid, target_tid, True)
                    attached = True
                except Exception:
                    pass

            try:
                win32gui.SetForegroundWindow(hwnd)
                time.sleep(0.06)
            except Exception:
                pass
            finally:
                if attached:
                    try:
                        ctypes.windll.user32.AttachThreadInput(our_tid, target_tid, False)
                    except Exception:
                        pass

            # SendInput con coordenadas absolutas normalizadas
            sw = ctypes.windll.user32.GetSystemMetrics(0)
            sh = ctypes.windll.user32.GetSystemMetrics(1)
            nx = int(cx * 65535 / sw)
            ny = int(cy * 65535 / sh)

            move = INPUT(type=INPUT_MOUSE, mi=MOUSEINPUT(
                dx=nx, dy=ny, mouseData=0,
                dwFlags=MOUSEEVENTF_MOVE | MOUSEEVENTF_ABSOLUTE,
                time=0, dwExtraInfo=None))
            down = INPUT(type=INPUT_MOUSE, mi=MOUSEINPUT(
                dx=nx, dy=ny, mouseData=0,
                dwFlags=MOUSEEVENTF_LEFTDOWN | MOUSEEVENTF_ABSOLUTE,
                time=0, dwExtraInfo=None))
            up = INPUT(type=INPUT_MOUSE, mi=MOUSEINPUT(
                dx=nx, dy=ny, mouseData=0,
                dwFlags=MOUSEEVENTF_LEFTUP | MOUSEEVENTF_ABSOLUTE,
                time=0, dwExtraInfo=None))

            inputs = (INPUT * 3)(move, down, up)
            result = ctypes.windll.user32.SendInput(3, inputs, ctypes.sizeof(INPUT))
            log.info("method2_SendInput result=%d pos=(%d,%d)", result, cx, cy)
            return result > 0

        except Exception as e:
            log.debug("method2_failed: %s", e)
            return False

    def _method3_enter_key(self, hwnd: int) -> bool:
        """
        Método 3: PostMessage VK_RETURN a la ventana principal.
        Fallback final — funciona en la mayoría de diálogos modales.
        """
        try:
            import win32con
            ctypes.windll.user32.PostMessageW(
                hwnd, win32con.WM_KEYDOWN, win32con.VK_RETURN, 0
            )
            time.sleep(0.05)
            ctypes.windll.user32.PostMessageW(
                hwnd, win32con.WM_KEYUP, win32con.VK_RETURN, 0
            )
            log.info("method3_VK_RETURN hwnd=%d", hwnd)
            return True
        except Exception as e:
            log.debug("method3_failed: %s", e)
            return False

    def _confirm_dialog(self, hwnd: int) -> bool:
        """
        Intenta confirmar el diálogo con 3 métodos en cascada.
        Verifica después de cada uno si la ventana cerró.
        Si cerró → éxito. Si no → intenta el siguiente.
        """
        ok_hwnd, cx, cy = self._find_ok_button(hwnd)

        # ── Método 1: BM_CLICK directo (sin foco) ────────────────────────────
        if ok_hwnd:
            self._method1_send_message(ok_hwnd)
            time.sleep(0.15)
            if self._dialog_closed(hwnd):
                log.info("confirm_success method=BM_CLICK hwnd=%d", hwnd)
                return True

        # ── Método 2: SendInput con foco forzado ─────────────────────────────
        if ok_hwnd and cx and cy:
            self._method2_send_input(hwnd, cx, cy)
            time.sleep(0.2)
            if self._dialog_closed(hwnd):
                log.info("confirm_success method=SendInput hwnd=%d", hwnd)
                return True

        # ── Método 3: Enter key como fallback ────────────────────────────────
        self._method3_enter_key(hwnd)
        time.sleep(0.2)
        if self._dialog_closed(hwnd):
            log.info("confirm_success method=VK_RETURN hwnd=%d", hwnd)
            return True

        # Si ningún método cerró el diálogo, reportar como fallido
        log.warning("confirm_all_methods_failed hwnd=%d", hwnd)
        return False

    def _dialog_closed(self, hwnd: int) -> bool:
        """True si la ventana ya no existe o no es visible."""
        try:
            import win32gui
            return (
                not win32gui.IsWindow(hwnd)
                or not win32gui.IsWindowVisible(hwnd)
            )
        except Exception:
            return True

    def _wait_dialog_close(self, hwnd: int, timeout: float = 4.0) -> bool:
        """Polling hasta que el hwnd desaparezca o timeout."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self._dialog_closed(hwnd):
                return True
            time.sleep(0.05)
        return False

    def _run(self):
        # Sin clear() aquí — el evento ya viene limpio desde start()
        last_activity = time.time()

        while not self._stop_event.is_set():
            hwnd = self._find_print_dialog()

            if hwnd:
                confirmed = self._confirm_dialog(hwnd)

                if confirmed:
                    self._last_hwnd = hwnd
                    # Esperar cierre completo antes de buscar el siguiente
                    self._wait_dialog_close(hwnd, timeout=4.0)
                    self._confirmed_count += 1
                    last_activity = time.time()
                    self._last_hwnd = 0
                    log.info("print_dialog_confirmed count=%d", self._confirmed_count)

                    # Si se alcanzó el expected_count, salir temprano
                    if self._expected_count > 0 and self._confirmed_count >= self._expected_count:
                        log.info(
                            "print_watcher_all_confirmed expected=%d confirmed=%d",
                            self._expected_count, self._confirmed_count
                        )
                        break
                else:
                    # El diálogo no cerró con ningún método —
                    # esperar un poco y reintentar en el siguiente ciclo
                    time.sleep(0.3)

            else:
                if time.time() - last_activity > self._idle_timeout:
                    log.info(
                        "print_watcher_idle_timeout confirmed=%d",
                        self._confirmed_count
                    )
                    break

            time.sleep(self._poll_interval)

        # Asegurar que el evento esté SET al terminar (sea por timeout o stop)
        # Esto mantiene coherencia de estado para el próximo start()
        self._stop_event.set()
        log.info("print_watcher_stopped confirmed=%d", self._confirmed_count)
