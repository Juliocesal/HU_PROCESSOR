
import ctypes
import ctypes.wintypes
import win32gui
import win32con
import win32api
import win32process
import time

def main():
    print("Buscando ventana Print...\n")

    # 1. Encontrar todas las ventanas visibles con "Print" o "Imprimir"
    found = []
    def enum_cb(hwnd, _):
        if win32gui.IsWindowVisible(hwnd):
            title = win32gui.GetWindowText(hwnd)
            cls   = win32gui.GetClassName(hwnd)
            if title.strip().lower() in ("print", "imprimir"):
                found.append((hwnd, title, cls))
    win32gui.EnumWindows(enum_cb, None)

    if not found:
        print("❌ No se encontró ninguna ventana Print/Imprimir visible.")
        print("   Asegúrate de que el diálogo esté abierto antes de correr esto.")
        return

    for hwnd, title, cls in found:
        print(f"✅ Ventana encontrada:")
        print(f"   hwnd  = {hwnd}")
        print(f"   title = '{title}'")
        print(f"   class = '{cls}'")

        # 2. Info del proceso dueño
        tid, pid = win32process.GetWindowThreadProcessId(hwnd)
        print(f"   pid   = {pid}  tid={tid}")

        # 3. Botones hijos
        print(f"\n   Botones hijos:")
        buttons = []
        def enum_btn(child, _):
            try:
                c = win32gui.GetClassName(child)
                t = win32gui.GetWindowText(child)
                r = win32gui.GetWindowRect(child)
                buttons.append((child, c, t, r))
            except:
                pass
        win32gui.EnumChildWindows(hwnd, enum_btn, None)
        for child, c, t, r in buttons:
            print(f"     hwnd={child} class='{c}' text='{t}' rect={r}")

        # 4. Intentar cada método y reportar si el diálogo cerró
        print(f"\n   ── Probando métodos ──")

        # Método A: BM_CLICK directo
        ok_hwnd = next((b[0] for b in buttons if b[2].strip().upper() in ("OK","ACEPTAR")), 0)
        if ok_hwnd:
            print(f"   [A] SendMessage BM_CLICK → ok_hwnd={ok_hwnd} ... ", end="", flush=True)
            ctypes.windll.user32.SendMessageW(ok_hwnd, win32con.BM_CLICK, 0, 0)
            time.sleep(0.3)
            closed = not win32gui.IsWindow(hwnd) or not win32gui.IsWindowVisible(hwnd)
            print(f"cerró={closed}")
            if closed:
                print("   ✅ MÉTODO A FUNCIONA")
                return

        # Método B: WM_COMMAND IDOK
        print(f"   [B] PostMessage WM_COMMAND IDOK=1 ... ", end="", flush=True)
        ctypes.windll.user32.PostMessageW(hwnd, win32con.WM_COMMAND, 1, 0)
        time.sleep(0.3)
        closed = not win32gui.IsWindow(hwnd) or not win32gui.IsWindowVisible(hwnd)
        print(f"cerró={closed}")
        if closed:
            print("   ✅ MÉTODO B FUNCIONA")
            return

        # Método C: VK_RETURN a la ventana
        print(f"   [C] PostMessage VK_RETURN ... ", end="", flush=True)
        ctypes.windll.user32.PostMessageW(hwnd, win32con.WM_KEYDOWN, win32con.VK_RETURN, 0)
        time.sleep(0.3)
        closed = not win32gui.IsWindow(hwnd) or not win32gui.IsWindowVisible(hwnd)
        print(f"cerró={closed}")
        if closed:
            print("   ✅ MÉTODO C FUNCIONA")
            return

        # Método D: AttachThreadInput + SetForegroundWindow + SendInput
        print(f"   [D] AttachThreadInput + SendInput click ... ", end="", flush=True)
        if ok_hwnd:
            rect = win32gui.GetWindowRect(ok_hwnd)
            cx = (rect[0] + rect[2]) // 2
            cy = (rect[1] + rect[3]) // 2

            our_tid = win32api.GetCurrentThreadId()
            ctypes.windll.user32.AttachThreadInput(our_tid, tid, True)
            win32gui.SetForegroundWindow(hwnd)
            time.sleep(0.1)

            sw = ctypes.windll.user32.GetSystemMetrics(0)
            sh = ctypes.windll.user32.GetSystemMetrics(1)

            class MI(ctypes.Structure):
                _fields_ = [("dx",ctypes.c_long),("dy",ctypes.c_long),
                             ("mouseData",ctypes.c_ulong),("dwFlags",ctypes.c_ulong),
                             ("time",ctypes.c_ulong),("dwExtraInfo",ctypes.POINTER(ctypes.c_ulong))]
            class INP(ctypes.Structure):
                class U(ctypes.Union):
                    _fields_ = [("mi", MI)]
                _anonymous_ = ("u",)
                _fields_ = [("type",ctypes.c_ulong),("u",U)]

            nx = int(cx * 65535 / sw)
            ny = int(cy * 65535 / sh)
            inputs = (INP * 3)(
                INP(type=0, u=INP.U(mi=MI(dx=nx,dy=ny,mouseData=0,dwFlags=0x0001|0x8000,time=0,dwExtraInfo=None))),
                INP(type=0, u=INP.U(mi=MI(dx=nx,dy=ny,mouseData=0,dwFlags=0x0002|0x8000,time=0,dwExtraInfo=None))),
                INP(type=0, u=INP.U(mi=MI(dx=nx,dy=ny,mouseData=0,dwFlags=0x0004|0x8000,time=0,dwExtraInfo=None))),
            )
            ctypes.windll.user32.SendInput(3, inputs, ctypes.sizeof(INP))
            ctypes.windll.user32.AttachThreadInput(our_tid, tid, False)
            time.sleep(0.3)
            closed = not win32gui.IsWindow(hwnd) or not win32gui.IsWindowVisible(hwnd)
            print(f"cerró={closed}")
            if closed:
                print("   ✅ MÉTODO D FUNCIONA")
                return

        print("\n❌ Ningún método funcionó.")
        print("   Pega el output completo de este script para diagnóstico.")

if __name__ == "__main__":
    main()