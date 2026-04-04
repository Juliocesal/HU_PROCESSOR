import sys
import os

def resource_path(relative_path: str) -> str:
    """
    Resuelve la ruta de un recurso, compatible con PyInstaller.
    En desarrollo: ruta relativa al directorio del script.
    En .exe: ruta al directorio temporal _MEIPASS.
    """
    if hasattr(sys, '_MEIPASS'):
        base = sys._MEIPASS
    else:
        base = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base, relative_path).replace("\\", "/")