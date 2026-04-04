"""
main_queue.py — Sistema de cola de HUs en tiempo real

Uso:
    python main_queue.py

Requisitos:
    pip install PyQt6 pywin32
"""

import sys
import logging
from pathlib import Path

# Compatible con PyInstaller .exe y desarrollo normal
if hasattr(sys, '_MEIPASS'):
    BASE_DIR = Path(sys._MEIPASS)
else:
    BASE_DIR = Path(__file__).resolve().parent

if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))


def setup_logging():
    log_dir = BASE_DIR / "logs"
    log_dir.mkdir(exist_ok=True)
    from datetime import datetime
    log_file = log_dir / f"hu_queue_{datetime.now().strftime('%Y%m%d')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, encoding="utf-8"),
        ]
    )


def main():
    try:
        import pythoncom
        pythoncom.CoInitialize()
    except ImportError:
        pass

    setup_logging()

    missing = []
    try:
        import win32com.client  # noqa
    except ImportError:
        missing.append("pywin32  → pip install pywin32")
    try:
        import PyQt6  # noqa
    except ImportError:
        missing.append("PyQt6   → pip install PyQt6")

    if missing:
        print("\n[ERROR] Faltan dependencias:\n")
        for d in missing:
            print(f"  {d}")
        sys.exit(1)

    from ui.queue_window import run_queue_app
    run_queue_app()


if __name__ == "__main__":
    main()