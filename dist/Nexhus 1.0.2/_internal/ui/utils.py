import sys
import os

def resource_path(relative_path: str) -> str:
    """Resuelve rutas tanto en desarrollo como en el .exe"""
    if hasattr(sys, '_MEIPASS'):
        base = sys._MEIPASS
    else:
        # En desarrollo: sube un nivel desde ui/ para llegar a la raíz
        base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base, relative_path).replace("\\", "/")