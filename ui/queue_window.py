"""
ui/queue_window.py  ·  HUFlow Engine — Réplica pixel-perfect SAP Fiori
=======================================================================
Código escrito completamente desde cero a partir del análisis
pixel a pixel de la imagen de referencia.

Paleta extraída:
  Shell bar:    #1B2537
  SAP green:    #1B9E40
  Page bg:      #F5F6F7
  Card white:   #FFFFFF
  Card border:  #E5E5E5
  Alt row:      #FAFBFC
  Blue primary: #0064D9
  Blue light:   #EBF4FF
  Blue muted:   #BDD9F5
  Green:        #107E3E / #188918
  Red:          #BB0000
  Text:         #32363A
  Text muted:   #6A7279
  Sep pallet:   #AABBCC
"""

import sys
import logging
import csv
import os
import re
import subprocess
from datetime import datetime
# Si utils.py está en ui/utils.py
from ui.utils import resource_path

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QPushButton, QLineEdit, QTableWidget, QTableWidgetItem,
    QHeaderView, QProgressBar, QMessageBox, QFrame, QCheckBox,
    QFileDialog, QScrollArea, QSizePolicy, QMenu
)
from PyQt6.QtCore import Qt, QTimer, pyqtSignal, QObject, QSize
from PyQt6.QtGui import QColor, QFont, QPalette, QIcon

from core.hu_origins import detect_origin, is_pallet_separator, ORIGIN_RULES, UNKNOWN_ORIGIN
from core.queue_model import HUQueue, HUItem, QueueWorker
from core.sap_client import SAPClient, SISTEMA_SAP
from ui.guided_tour import GuidedTourOverlay
from PyQt6.QtSvgWidgets import QSvgWidget

log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
#  DESIGN TOKENS  (extraídos píxel a píxel de la imagen)
# ══════════════════════════════════════════════════════════════════════════════

class C:
    """Colores y tamaños exactos de la imagen."""

    # ── Shell bar ─────────────────────────────────────────────────────────────
    SHELL        = "#1B2537"
    SHELL_BORDER = "#253448"
    SHELL_MUTED  = "#8C9BAB"
    SAP_GREEN    = "#1B9E40"    # logo SAP

    # ── Fondo página ──────────────────────────────────────────────────────────
    PAGE         = "#F5F6F7"
    CARD         = "#FFFFFF"
    CARD_BORDER  = "#E0E0E0"    # borde ligeramente más oscuro para contraste
    BORDER       = "#E8E8E8"    # borde general
    SURFACE      = "#FAFBFC"    # superficie sutil
    ROW_ALT      = "#FAFBFC"

    # ── Azul primario Fiori ───────────────────────────────────────────────────
    BLUE         = "#0d2038"
    BLUE_DARK    = "#102744"
    BLUE_HOVER   = "#0070F5"
    BLUE_LIGHT   = "#EBF4FF"    # badge pallet bg
    BLUE_BORDER  = "#BDD9F5"    # badge pallet border
    BLUE_LINE    = "#C0D8F5"    # línea underline input

    # ── Semánticos ────────────────────────────────────────────────────────────
    GREEN        = "#107E3E"    # success text/border
    GREEN_TEXT   = "#188918"    # success value
    GREEN_BG     = "#F1FAF1"
    GREEN_MUT    = "#CFEACF"

    RED          = "#BB0000"
    RED_BG       = "#FFF0F0"
    RED_MUT      = "#FAD0D0"

    ORANGE       = "#E76500"
    ORANGE_BG    = "#FEF3E6"

    PROC_BG      = "#EBF4FF"
    PROC_MUT     = "#BDD9F5"

    # ── Texto ─────────────────────────────────────────────────────────────────
    TEXT         = "#32363A"    # texto principal
    TEXT_SECONDARY = "#6F7A83"  # texto secundario (más suave)
    MUTED        = "#6A7279"    # texto secundario
    LIGHT        = "#89919A"    # texto terciario
    SEP_TEXT     = "#AABBCC"    # texto separador pallet

    # ── Tabla ─────────────────────────────────────────────────────────────────
    TBL_BORDER   = "#E8E8E8"
    TBL_HDR_SEP  = "#E0E0E0"

    # ── Footer ────────────────────────────────────────────────────────────────
    FOOTER       = "#FFFFFF"
    FOOTER_BRD   = "#E5E5E5"

    # ── Tipografía ────────────────────────────────────────────────────────────
    FONT         = "72, Arial, Helvetica, sans-serif"
    MONO         = "Lucida Console, Courier New, monospace"

    # ── Elevación / sombra (simulada via borde más visible) ───────────────────
    CARD_RADIUS  = "6px"
    CARD_PAD_H   = 18
    CARD_PAD_V   = 14


# ── Colores por estado ────────────────────────────────────────────────────────

def status_colors(status: str) -> tuple[str, str]:
    """→ (background, foreground)"""
    return {
        "ok":         (C.GREEN_MUT,  C.GREEN),
        "duplicate":  ("#FADDAD",    C.ORANGE),
        "error":      (C.RED_MUT,    C.RED),
        "processing": (C.PROC_MUT,   C.BLUE),
    }.get(status, ("#F5F5F5", C.MUTED))


# ══════════════════════════════════════════════════════════════════════════════
#  PUENTE THREAD-SAFE
# ══════════════════════════════════════════════════════════════════════════════

class Bridge(QObject):
    item_update  = pyqtSignal(object)
    stats_update = pyqtSignal(dict)
    sp01_done    = pyqtSignal(dict)
    pallet_done  = pyqtSignal(int)
    error        = pyqtSignal(str)


# ══════════════════════════════════════════════════════════════════════════════
#  QSS GLOBAL
# ══════════════════════════════════════════════════════════════════════════════

QSS = f"""
QWidget {{
    font-family: {C.FONT};
    font-size: 9pt;
    color: {C.TEXT};
}}
QMainWindow, QWidget {{ background: {C.PAGE}; }}

/* ── Cards: fondo sólido blanco, borde sutil, esquinas redondeadas ── */
WhiteCard {{
    background: {C.CARD};
    border: 1px solid {C.CARD_BORDER};
    border-radius: {C.CARD_RADIUS};
}}
WhiteCard > QWidget {{
    background: transparent;
    border: none;
}}
WhiteCard QLabel {{
    background: transparent;
    border: none;
}}
WhiteCard QLineEdit {{
    background: transparent;
}}
WhiteCard QCheckBox {{
    background: transparent;
}}
WhiteCard QPushButton {{
    background: white;
}}

/* ── KPICard ── */
KPICard {{
    background: {C.CARD};
    border: 1px solid {C.CARD_BORDER};
    border-radius: {C.CARD_RADIUS};
}}
KPICard QLabel {{
    background: transparent;
    border: none;
}}

/* ── ProgressCard ── */
ProgressCard {{
    background: {C.CARD};
    border: 1px solid {C.CARD_BORDER};
    border-radius: {C.CARD_RADIUS};
}}
ProgressCard QLabel {{
    background: transparent;
    border: none;
}}

QScrollBar:vertical {{
    background: transparent; width: 6px; border: none; margin: 0;
}}
QScrollBar::handle:vertical {{
    background: #C5CAD0; border-radius: 3px; min-height: 20px;
}}
QScrollBar::handle:vertical:hover {{ background: #A0A8B2; }}
QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{ height: 0; }}
QScrollBar:horizontal {{
    background: transparent; height: 6px; border: none;
}}
QScrollBar::handle:horizontal {{
    background: #C5CAD0; border-radius: 3px; min-width: 20px;
}}
QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {{ width: 0; }}
QToolTip {{
    background: {C.SHELL}; color: white; border: none;
    padding: 4px 8px; border-radius: 3px; font-size: 8pt;
}}
QMessageBox {{ background: {C.CARD}; }}
QMessageBox QLabel {{ color: {C.TEXT}; background: transparent; font-size: 9pt; }}
QMessageBox QPushButton {{
    background: {C.BLUE}; color: white; border: none;
    border-radius: 4px; padding: 6px 20px;
    font-weight: bold; min-width: 80px;
}}
QMessageBox QPushButton:hover {{ background: {C.BLUE_DARK}; }}
"""


# ══════════════════════════════════════════════════════════════════════════════
#  COMPONENTES ATÓMICOS
# ══════════════════════════════════════════════════════════════════════════════

class WhiteCard(QWidget):
    """
    Tarjeta con fondo sólido blanco (#FFFFFF), borde sutil #E0E0E0,
    border-radius 6px y padding interno consistente.
    Base de todas las cards — nunca usa transparencia.
    """
    def __init__(self, parent=None,
                 pad_h=C.CARD_PAD_H, pad_v=C.CARD_PAD_V,
                 border_color=C.CARD_BORDER,
                 border_width=1, radius=6):
        super().__init__(parent)
        self.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)
        self._bw = border_width
        self._bc = border_color
        self._r  = radius
        self._apply_card_style()
        self._vl = QVBoxLayout(self)
        self._vl.setContentsMargins(pad_h, pad_v, pad_h, pad_v)
        self._vl.setSpacing(0)

    def _apply_card_style(self):
        self.setStyleSheet(f"""
            WhiteCard {{
                background: {C.CARD};
                border: {self._bw}px solid {self._bc};
                border-radius: {self._r}px;
            }}
            WhiteCard QWidget {{
                background: transparent;
                border: none;
            }}
            WhiteCard QLabel {{
                background: transparent;
                border: none;
            }}
            WhiteCard QLineEdit {{
                background: transparent;
            }}
            WhiteCard QCheckBox {{
                background: transparent;
            }}
        """)

    def set_border(self, color: str, width: int = 1):
        self._bc = color
        self._bw = width
        self._apply_card_style()

    def lay(self):               return self._vl
    def addWidget(self, w, **k): self._vl.addWidget(w, **k)
    def addLayout(self, l, **k): self._vl.addLayout(l, **k)
    def addStretch(self, n=1):   self._vl.addStretch(n)
    def addSpacing(self, n):     self._vl.addSpacing(n)


class KPICard(WhiteCard):
    """
    KPI idéntico a la imagen:
      label top   8pt gris normal
      número      28pt bold coloreado
      sub-label   8pt gris normal

    Fondo sólido blanco, borde #E0E0E0, border-radius 6px.
    """
    def __init__(self, label: str, sublabel: str,
                 value: str = "0", color: str = C.TEXT, parent=None):
        super().__init__(parent, pad_h=C.CARD_PAD_H, pad_v=C.CARD_PAD_V)
        self.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)
        self._color = color

        self._lbl_top = QLabel(label)
        self._lbl_top.setStyleSheet(
            f"font-size:8pt; color:{C.MUTED}; background:transparent; border:none;"
        )
        self._lbl_top.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._vl.addWidget(self._lbl_top)

        self._num = QLabel(value)
        self._num.setStyleSheet(
            f"font-size:28pt; font-weight:bold; color:{color};"
            " background:transparent; border:none; line-height:1.1;"
        )
        self._num.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._vl.addWidget(self._num)

        self._sub = QLabel(sublabel)
        self._sub.setStyleSheet(
            f"font-size:8pt; color:{C.MUTED}; background:transparent; border:none;"
        )
        self._sub.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._vl.addWidget(self._sub)

    def set_value(self, v: str):
        if self._num.text() != v:
            self._num.setText(v)

    def set_color(self, color: str):
        self._num.setStyleSheet(
            f"font-size:28pt; font-weight:bold; color:{color};"
            " background:transparent; border:none; line-height:1.1;"
        )


class PhaseCard(WhiteCard):
    """
    Un paso del pipeline (F1 / F2 / SP01).
    Exactamente como la imagen: checkbox azul + código + tcode + desc.
    Fondo sólido blanco, borde #E0E0E0.
    """
    def __init__(self, code: str, tcode: str, desc: str, parent=None):
        super().__init__(parent, pad_h=8, pad_v=7)
        self.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)
        self.setFixedWidth(112)

        # Fila: checkbox + código
        row = QHBoxLayout()
        row.setSpacing(5)
        row.setContentsMargins(0, 0, 0, 0)
        row.setAlignment(Qt.AlignmentFlag.AlignCenter)

        self._chk = QCheckBox()
        self._chk.setChecked(True)
        self._chk.setStyleSheet(f"""
        QCheckBox {{
        background: transparent;
        border: none;
        spacing: 0;
        color: {C.TEXT};      
        }}
        QCheckBox::indicator {{
        width: 14px; height: 14px;
        border: 1.5px solid {C.BLUE};
        border-radius: 2px; background: white;
        }}
        QCheckBox::indicator:checked {{
        background: {C.BLUE_HOVER}; border-color: {C.BLUE_HOVER};
        image: url("{resource_path('icons/check.svg')}");
        }}
        QCheckBox::indicator:unchecked {{
        background: white; border-color: #B0C4D8;
        }}
        QCheckBox:disabled {{ opacity: 0.45; }}
        """)
        row.addWidget(self._chk)

        lc = QLabel(code)
        lc.setAlignment(Qt.AlignmentFlag.AlignCenter)
        lc.setStyleSheet(
            f"font-size:9.5pt; font-weight:bold; color:{C.TEXT};"
            " background:transparent; border:none;"
        )
        row.addWidget(lc)
        self._vl.addLayout(row)

        lt = QLabel(tcode)
        lt.setAlignment(Qt.AlignmentFlag.AlignCenter)
        lt.setStyleSheet(
            f"font-size:7.5pt; font-weight:bold; color:{C.TEXT};"
            " background:transparent; border:none;"
        )
        self._vl.addSpacing(1)
        self._vl.addWidget(lt)

        ld = QLabel(desc)
        ld.setAlignment(Qt.AlignmentFlag.AlignCenter)
        ld.setStyleSheet(
            f"font-size:7pt; color:{C.MUTED}; background:transparent; border:none;"
        )
        self._vl.addWidget(ld)

    @property
    def chk(self) -> QCheckBox:
        return self._chk


class PipelineRow(QWidget):
    """F1 → F2 → SP01 idéntico a la imagen."""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setStyleSheet("background:transparent;")
        h = QHBoxLayout(self)
        h.setContentsMargins(0, 0, 0, 0)
        h.setSpacing(8)

        defs = [
            ("F1",   "ZMOVEINBHU", "Acknowledge"),
            ("F2",   "ZMMTJI.SEP", "Spool"),
            ("SP01", "SP01",       "Imprimir"),
        ]
        self._cards = [PhaseCard(*d) for d in defs]

        for i, card in enumerate(self._cards):
            if i > 0:
                arr = QLabel("→")
                arr.setStyleSheet(
                    f"color:{C.MUTED}; font-size:11pt; background:transparent;"
                )
                arr.setAlignment(Qt.AlignmentFlag.AlignCenter)
                h.addWidget(arr)
            h.addWidget(card)

    @property
    def chk_f1(self):   return self._cards[0].chk
    @property
    def chk_f2(self):   return self._cards[1].chk
    @property
    def chk_sp01(self): return self._cards[2].chk


class ShellBar(QWidget):
    """
    Barra superior idéntica a la imagen.
    ≡ | SAP | Logistics › HUFlow Engine | ● CONECTADO | fecha/hora | JL LOPEZHUC
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self._window = parent  # Reference to QueueWindow
        self.setObjectName("ShellBar")
        self.setFixedHeight(44)
        self.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)

        self.setStyleSheet(f"""
        #ShellBar {{
        background: {C.BLUE};
        border-bottom: 2px solid {C.BLUE_DARK};
        }}
        #ShellBar QLabel {{
        color: white;
        background: transparent;
        border: none;
        }}
        #ShellBar QFrame {{
        background: {C.SHELL_MUTED}55;
        }}
        """)
        h = QHBoxLayout(self)
        h.setContentsMargins(12, 0, 16, 0)
        h.setSpacing(0)

        # Hamburger (botón menú lateral)
        self.ham_btn = QPushButton("≡")
        self.ham_btn.setFixedSize(30, 30)
        self.ham_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self.ham_btn.setStyleSheet(
            "QPushButton { background:transparent; color:white; font-size:16pt; border:none; }"
            "QPushButton:hover { background: rgba(255,255,255,0.1); }"
        )
        h.addWidget(self.ham_btn)
        h.addSpacing(8)

        # Menú hamburguesa
        self.ham_menu = QMenu(self)
        self.ham_menu.addAction("Tutorial", lambda: self._window._start_tour() if self._window else None)
        self.ham_menu.addSeparator()
        self.ham_menu.addAction("Exportar", lambda: self._window._export() if self._window else None)

        self.ham_btn.clicked.connect(lambda: self.ham_menu.exec(self.ham_btn.mapToGlobal(self.ham_btn.rect().bottomLeft())))

        # Logo SAP
        sap = QLabel("HU PROCESSOR")
        sap.setFixedSize(150, 26)
        sap.setAlignment(Qt.AlignmentFlag.AlignCenter)
        sap.setStyleSheet(f"""
            background:{C.SAP_GREEN}; color:white;
            font-weight:900; font-size:11pt;
            font-family:Arial Black,Arial,sans-serif;
            border-radius:3px;
        """)
        h.addWidget(sap)
        h.addSpacing(14)

        # Breadcrumb
        _lbl(h, "Essilor Luxottica", "color:white; font-size:9pt; background:transparent;")
        _lbl(h, "  ›  ", f"color:{C.SHELL_MUTED}; font-size:10pt; background:transparent;")
        _lbl(h, "HUFlow Engine",
             "color:white; font-size:9pt; font-weight:bold; background:transparent;")

        h.addStretch()

        # ● CONECTADO
        self._dot = QLabel("●")
        self._dot.setStyleSheet(
            f"color:{C.GREEN_TEXT}; font-size:9pt; background:transparent;"
        )
        h.addWidget(self._dot)
        h.addSpacing(5)

        self._conn = QLabel("CONECTADO")
        self._conn.setStyleSheet(
            f"color:{C.GREEN_TEXT}; font-size:8.5pt; font-weight:bold;"
            " background:transparent; letter-spacing:0.4px;"
        )
        h.addWidget(self._conn)
        h.addSpacing(18)

        _vsep(h)
        h.addSpacing(18)

        # Reloj
        self._clk = QLabel()
        self._clk.setStyleSheet(
            f"color:white; font-size:8.5pt; background:transparent;"
        )
        self._tick()
        t = QTimer(self)
        t.timeout.connect(self._tick)
        t.start(1000)
        h.addWidget(self._clk)
        h.addSpacing(18)

        _vsep(h)
        h.addSpacing(14)

        # Avatar
        self._av = QLabel("JL")
        self._av.setFixedSize(28, 28)
        self._av.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._av.setStyleSheet(f"""
            background:{C.BLUE}; color:white;
            font-size:8pt; font-weight:bold;
            border-radius:14px;
        """)
        h.addWidget(self._av)
        h.addSpacing(6)

        self._usr = QLabel(os.getenv("USERNAME", "USER").upper()[:10])
        self._usr.setStyleSheet("color:white; font-size:8.5pt; background:transparent;")
        h.addWidget(self._usr)
        h.addSpacing(12)

    def _tick(self):
        now  = datetime.now()
        DAYS = ["Lunes","Martes","Miércoles","Jueves","Viernes","Sábado","Domingo"]
        MONS = ["enero","febrero","marzo","abril","mayo","junio",
                "julio","agosto","septiembre","octubre","noviembre","diciembre"]
        self._clk.setText(
            f"{DAYS[now.weekday()]}, {now.day} de {MONS[now.month-1]} de {now.year}"
            f"    {now.strftime('%H:%M:%S')}"
        )

    def set_connection(self, ok: bool, user: str = ""):
        c = C.GREEN_TEXT if ok else C.RED
        txt = "CONECTADO" if ok else "SIN SESIÓN"
        self._dot.setStyleSheet(f"color:{c}; font-size:9pt; background:transparent;")
        self._conn.setText(txt)
        self._conn.setStyleSheet(
            f"color:{c}; font-size:8.5pt; font-weight:bold;"
            " background:transparent; letter-spacing:0.4px;"
        )
        if user:
            parts = user.split()
            ini = "".join(p[0].upper() for p in parts[:2]) if parts else user[:2].upper()
            self._av.setText(ini)
            self._usr.setText(user.upper()[:10])


class ProgressCard(WhiteCard):
    """
    Card de progreso con fondo sólido blanco.
    [● DETENIDO pill]  [████ barra rayada ████]  100%
    Proceso completado: 171 de 171 unidades procesadas
    """
    MODE_IDLE      = "idle"
    MODE_RUNNING   = "running"
    MODE_PAUSED    = "paused"
    MODE_ERROR     = "error"
    MODE_STOPPED   = "stopped"
    MODE_COMPLETED = "completed"

    _CFG = {
        #             badge_color  badge_bg     badge_border  badge_txt       status_color  bold
        MODE_IDLE:    (C.MUTED,    C.PAGE,      C.CARD_BORDER,"● EN ESPERA",  C.MUTED,      False),
        MODE_RUNNING: (C.BLUE,     C.PROC_BG,   C.BLUE_BORDER,"▶ PROCESANDO", C.BLUE,       True),
        MODE_PAUSED:  (C.ORANGE,   C.ORANGE_BG, C.ORANGE_BG,  "⏸ PAUSADO",   C.ORANGE,     True),
        MODE_ERROR:   (C.RED,      C.RED_BG,    C.RED_MUT,    "⚠ ERROR",      C.RED,        True),
        MODE_STOPPED: (C.RED,      C.RED_BG,    C.RED_MUT,    "● DETENIDO",   C.MUTED,      False),
        MODE_COMPLETED:(C.GREEN,   C.GREEN_BG,  C.GREEN_MUT,  "✓ COMPLETADO", C.GREEN_TEXT, True),
    }

    def __init__(self, parent=None):
        super().__init__(parent, pad_h=C.CARD_PAD_H, pad_v=10)
        self.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)
        self.setFixedHeight(70)

        # Fila: badge + barra + %
        row1 = QHBoxLayout()
        row1.setSpacing(12)
        row1.setContentsMargins(0, 0, 0, 0)

        self._badge = QLabel("● EN ESPERA")
        self._badge.setFixedSize(116, 24)
        self._badge.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._badge.setStyleSheet(f"""
            background:{C.PAGE}; color:{C.MUTED};
            font-size:7.5pt; font-weight:bold;
            border-radius:12px; border:1px solid {C.CARD_BORDER};
            padding:0 8px; letter-spacing:0.4px;
        """)
        row1.addWidget(self._badge)

        self._bar = QProgressBar()
        self._bar.setValue(0)
        self._bar.setFixedHeight(18)
        self._bar.setTextVisible(False)
        self._bar.setStyleSheet(f"""
            QProgressBar {{
                background:#E2E6EA; border:none; border-radius:9px;
            }}
            QProgressBar::chunk {{
                background:qlineargradient(
                    x1:0,y1:0, x2:1,y2:0,
                    stop:0.00 #1255B0, stop:0.08 #1976D2,
                    stop:0.16 #1255B0, stop:0.24 #1976D2,
                    stop:0.32 #1255B0, stop:0.40 #1976D2,
                    stop:0.48 #1255B0, stop:0.56 #1976D2,
                    stop:0.64 #1255B0, stop:0.72 #1976D2,
                    stop:0.80 #1255B0, stop:0.88 #1976D2,
                    stop:0.96 #1255B0, stop:1.00 #1976D2
                );
                border-radius:9px;
            }}
        """)
        row1.addWidget(self._bar, stretch=1)

        self._pct = QLabel("0%")
        self._pct.setFixedWidth(46)
        self._pct.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        self._pct.setStyleSheet(
            f"font-size:11pt; font-weight:bold; color:{C.TEXT}; background:transparent;"
        )
        row1.addWidget(self._pct)

        self._vl.addLayout(row1)
        self._vl.addSpacing(4)

        # Texto de estado
        self._status = QLabel("Listo. Escanea HUs para comenzar.")
        self._status.setStyleSheet(
            f"font-size:8pt; color:{C.MUTED}; background:transparent;"
        )
        self._vl.addWidget(self._status)

    def update(self, done: int, total: int):
        if total > 0:
            self._bar.setMaximum(total)
            self._bar.setValue(done)
            self._pct.setText(f"{int(done/total*100)}%")
        else:
            self._bar.setMaximum(100)
            self._bar.setValue(0)
            self._pct.setText("0%")

    def set_mode(self, mode: str, text: str = ""):
        bc, bbg, bbr, blbl, sc, bold = self._CFG.get(mode, self._CFG[self.MODE_IDLE])
        self._badge.setText(blbl)
        self._badge.setStyleSheet(f"""
            background:{bbg}; color:{bc};
            font-size:7.5pt; font-weight:bold;
            border-radius:12px; border:1px solid {bbr};
            padding:0 8px; letter-spacing:0.4px;
        """)
        if text:
            self._status.setText(text)
        self._status.setStyleSheet(
            f"font-size:8pt; color:{sc}; background:transparent;"
            + (" font-weight:600;" if bold else "")
        )


class FooterBar(QWidget):
    """
    Barra inferior fija — idéntica a la imagen.
    [▶ INICIAR PROCESO]  ■ Proceso detenido.    ↓ EXPORTAR | ■ DETENER  ✂ LIMPIAR COLA
    Fondo sólido blanco con borde superior.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFixedHeight(54)
        self.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)
        self.setStyleSheet(f"""
            FooterBar {{
                background: {C.FOOTER};
                border-top: 1px solid {C.FOOTER_BRD};
            }}
        """)
        h = QHBoxLayout(self)
        h.setContentsMargins(20, 0, 20, 0)
        h.setSpacing(10)

        self.btn_start    = _foot_btn("▶  INICIAR PROCESO", "primary")
        self.btn_continue = _foot_btn("▶  CONTINUAR",       "success")
        self.btn_continue.setVisible(False)
        h.addWidget(self.btn_start)
        h.addWidget(self.btn_continue)

        self._lbl = QLabel("■  Proceso detenido.")
        self._lbl.setStyleSheet(f"font-size:9pt; color:{C.MUTED}; background:transparent;")
        h.addWidget(self._lbl)
        h.addStretch()

        self.btn_stop  = _foot_btn("■  DETENER",     "danger")
        self.btn_stop.setEnabled(False)
        h.addWidget(self.btn_stop)

        self.btn_clear = _foot_btn("✂  LIMPIAR HUs", "ghost")
        h.addWidget(self.btn_clear)

    def set_status(self, txt: str, color: str = C.MUTED):
        self._lbl.setText(txt)
        self._lbl.setStyleSheet(
            f"font-size:9pt; color:{color}; background:transparent;"
        )


# ── Helpers de widgets pequeños ───────────────────────────────────────────────

def _lbl(layout, text: str, style: str) -> QLabel:
    l = QLabel(text)
    l.setStyleSheet(style)
    layout.addWidget(l)
    return l


def _vsep(layout):
    s = QFrame()
    s.setFrameShape(QFrame.Shape.VLine)
    s.setFixedHeight(20)
    s.setFixedWidth(1)
    s.setStyleSheet(f"background:{C.SHELL_MUTED}55; border:none;")
    layout.addWidget(s)


def _foot_btn(text: str, style: str) -> QPushButton:
    b = QPushButton(text)
    b.setFixedHeight(34)
    b.setCursor(Qt.CursorShape.PointingHandCursor)
    if style == "primary":
        b.setStyleSheet(f"""
            QPushButton {{
                background:{C.BLUE_HOVER}; color:white; font-weight:bold;
                font-size:9pt; border:none; border-radius:4px; padding:0 16px;
            }}
            QPushButton:hover {{ background:{C.BLUE_HOVER}; }}
            QPushButton:disabled {{ background:#C8D4E0; color:#9AAABB; }}
        """)
    elif style == "success":
        b.setStyleSheet(f"""
            QPushButton {{
                background:{C.GREEN}; color:white; font-weight:bold;
                font-size:9pt; border:none; border-radius:4px; padding:0 16px;
            }}
            QPushButton:hover {{ background:#0D6B32; }}
        """)
    elif style == "danger":
        b.setStyleSheet(f"""
            QPushButton {{
                background:white; color:{C.RED}; font-weight:bold;
                font-size:9pt; border:1px solid {C.RED};
                border-radius:4px; padding:0 14px;
            }}
            QPushButton:hover {{ background:{C.RED_BG}; }}
            QPushButton:disabled {{ color:#CCAAAA; border-color:#DDB8B8; }}
        """)
    else:   # ghost
        b.setStyleSheet(f"""
            QPushButton {{
                background:white; color:{C.TEXT};
                font-size:9pt; border:1px solid {C.CARD_BORDER};
                border-radius:4px; padding:0 14px;
            }}
            QPushButton:hover {{ background:{C.PAGE}; border-color:#B0B8C4; }}
            QPushButton:disabled {{ color:{C.MUTED}; }}
        """)
    return b


def _icon_btn(icon: str) -> QPushButton:
    """Botón icono cuadrado para la cabecera de la tabla."""
    b = QPushButton(icon)
    b.setFixedSize(30, 30)
    b.setStyleSheet(f"""
        QPushButton {{
            background:transparent; border:1px solid {C.CARD_BORDER};
            border-radius:4px; font-size:11pt; color:{C.MUTED};
        }}
        QPushButton:hover {{ background:{C.PAGE}; }}
    """)
    return b


def _pallet_badge(text: str) -> QWidget:
    """Badge pill azul claro P01/P02 — idéntico a la imagen."""
    w = QWidget()
    w.setStyleSheet("background:transparent;")
    h = QHBoxLayout(w)
    h.setContentsMargins(8, 0, 8, 0)
    h.setAlignment(Qt.AlignmentFlag.AlignCenter)
    lbl = QLabel(text)
    lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
    lbl.setFixedHeight(22)
    lbl.setStyleSheet(f"""
        background:{C.BLUE_LIGHT}; color:{C.BLUE};
        font-size:8pt; font-weight:bold;
        border-radius:4px; border:1px solid {C.BLUE_BORDER};
        padding:0 8px; min-width:36px;
    """)
    h.addWidget(lbl)
    return w


def _ok_cell(text: str) -> QWidget:
    """✅ OK verde + texto — idéntico a la imagen."""
    w = QWidget()
    w.setStyleSheet("background:transparent;")
    h = QHBoxLayout(w)
    h.setContentsMargins(10, 0, 6, 0)
    h.setSpacing(6)
    dot = QLabel("✅")
    dot.setStyleSheet("font-size:9pt; background:transparent;")
    h.addWidget(dot)
    lbl = QLabel(text)
    lbl.setStyleSheet(
        f"font-size:9pt; color:{C.GREEN_TEXT}; font-weight:bold; background:transparent;"
    )
    h.addWidget(lbl)
    h.addStretch()
    return w


# ══════════════════════════════════════════════════════════════════════════════
#  VENTANA PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

class QueueWindow(QMainWindow):

    def __init__(self):
        super().__init__()
        self._queue  = HUQueue()
        self._worker = QueueWorker(self._queue)
        self._bridge = Bridge()
        self._rows: dict[str, int] = {}
        self._pallet_sep_rows: dict[int, int] = {}  # pallet_id -> row number
        self._last_ui_pallet = 0  # Rastrear último pallet mostrado en UI
        self._auto_start_enabled = False  # Control para auto-iniciar proceso
        self._tour = None  # Tour overlay

        self._wire_signals()
        self._build_ui()
        self._setup_sap_timer()
        self._init_pallet()

    # ── Señales ───────────────────────────────────────────────────────────────

    def _wire_signals(self):
        self._bridge.item_update.connect(self._on_item)
        self._bridge.stats_update.connect(self._on_stats)
        self._bridge.sp01_done.connect(self._on_sp01)
        self._bridge.pallet_done.connect(self._on_pallet_done)
        self._bridge.error.connect(self._on_err)

        self._worker.on_item_update  = lambda x: self._bridge.item_update.emit(x)
        self._worker.on_stats_update = lambda s: self._bridge.stats_update.emit(s)
        self._worker.on_sp01_done    = lambda r: self._bridge.sp01_done.emit(r)
        self._worker.on_pallet_done  = lambda p: self._bridge.pallet_done.emit(p)
        self._worker.on_error        = lambda e: self._bridge.error.emit(e)

    # ── Construcción UI ───────────────────────────────────────────────────────

    def _build_ui(self):
        self.setWindowTitle("HUFlow Engine  ·  LHJ ")
        
        # Configurar icono de la ventana
        icon_path = resource_path("logo.ico")
        if os.path.exists(icon_path):
            self.setWindowIcon(QIcon(icon_path))
        
        self.setMinimumSize(1120, 780)
        self.setStyleSheet(QSS)

        root = QWidget()
        root.setStyleSheet(f"background:{C.PAGE};")
        self.setCentralWidget(root)

        vroot = QVBoxLayout(root)
        vroot.setSpacing(0)
        vroot.setContentsMargins(0, 0, 0, 0)

        # Shell
        self._shell = ShellBar(self)
        vroot.addWidget(self._shell)

        # Scroll
        sa = QScrollArea()
        sa.setWidgetResizable(True)
        sa.setFrameShape(QFrame.Shape.NoFrame)
        sa.setStyleSheet(f"background:{C.PAGE}; border:none;")
        body_w = QWidget()
        body_w.setStyleSheet(f"background:{C.PAGE};")
        self._body = QVBoxLayout(body_w)
        self._body.setContentsMargins(24, 20, 24, 14)
        self._body.setSpacing(16)
        sa.setWidget(body_w)
        vroot.addWidget(sa, stretch=1)

        # Footer
        self._footer = FooterBar()
        self._footer.btn_start.clicked.connect(self._start)
        self._footer.btn_continue.clicked.connect(self._cont)
        self._footer.btn_stop.clicked.connect(self._stop)
        self._footer.btn_clear.clicked.connect(self._clear)
        vroot.addWidget(self._footer)

        # Tour overlay
        self._tour = GuidedTourOverlay(self)

        self._populate_body()

    def _populate_body(self):
        b = self._body

        # ── ROW 1: Título izquierda + Fases SAP derecha ────────────────────
        r1 = QHBoxLayout()
        r1.setSpacing(24)

        tc = QVBoxLayout()
        tc.setSpacing(4)
        h1 = QLabel("Escaneo de handling Units (HUs)")
        h1.setStyleSheet(
            f"font-size:19pt; font-weight:bold; color:{C.TEXT}; background:transparent;"
        )
        tc.addWidget(h1)
        h2 = QLabel("Escanea códigos HU para procesar pallets y ejecutar confirmación en SAP.")
        h2.setStyleSheet(f"font-size:9pt; color:{C.MUTED}; background:transparent;")
        tc.addWidget(h2)
        r1.addLayout(tc, stretch=1)

        fc = QVBoxLayout()
        fc.setSpacing(6)
        fl = QLabel("Fases SAP")
        fl.setStyleSheet(
            f"font-size:8pt; font-weight:bold; color:{C.MUTED};"
            " background:transparent; letter-spacing:0.3px;"
        )
        fl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        fc.addWidget(fl)
        self._pipeline = PipelineRow()
        fc.addWidget(self._pipeline)
        r1.addLayout(fc)

        b.addLayout(r1)

        self.chk_f1   = self._pipeline.chk_f1
        self.chk_f2   = self._pipeline.chk_f2
        self.chk_sp01 = self._pipeline.chk_sp01

        # ── ROW 2: Scan card + 4 KPIs ─────────────────────────────────────
        r2 = QHBoxLayout()
        r2.setSpacing(12)

        self._scan_card = self._make_scan_card()
        r2.addWidget(self._scan_card, stretch=3)

        self._kpi_total   = KPICard("Total",      "Unidades",   "0", C.BLUE_HOVER)
        self._kpi_ok      = KPICard("Procesadas", "Completadas","0", C.GREEN_TEXT)
        self._kpi_err     = KPICard("Errores",    "Fallos",     "0", C.MUTED)
        self._kpi_pend    = KPICard("Pendientes", "En cola",    "0", C.MUTED)

        for k in [self._kpi_total, self._kpi_ok, self._kpi_err, self._kpi_pend]:
            r2.addWidget(k, stretch=1)

        b.addLayout(r2)

        # ── ROW 3: Progress card ───────────────────────────────────────────
        self._prog = ProgressCard()
        b.addWidget(self._prog)

        # ── ROW 4: Tabla card ──────────────────────────────────────────────
        b.addWidget(self._make_table_card(), stretch=1)

    # ── Scan card ─────────────────────────────────────────────────────────────

    def _make_scan_card(self) -> WhiteCard:
        card = WhiteCard(pad_h=C.CARD_PAD_H, pad_v=C.CARD_PAD_V,
                         border_color=C.BLUE_HOVER, border_width=0.5)

        # ── 1. INPUT PRINCIPAL ─────────────────────────────────────────────
        row = QHBoxLayout()
        row.setSpacing(12)
        row.setContentsMargins(0, 0, 0, 0)

        # Ícono con fondo sutil
        ico_wrap = QFrame()
        ico_wrap.setFixedSize(34, 34)
        ico_wrap.setStyleSheet(f"""
            QFrame {{
                background:{C.BLUE_LIGHT};
                border-radius:8px;
            }}
        """)
        ico_layout = QVBoxLayout(ico_wrap)
        ico_layout.setContentsMargins(0, 0, 0, 0)

        ico = QLabel()
        ico.setFixedSize(34, 34)
        ico.setAlignment(Qt.AlignmentFlag.AlignCenter)
        ico.setStyleSheet(f"""
            image: url('{resource_path('icons/scancode.svg')}');
            background: transparent;
        """)
        ico_layout.addWidget(ico)
        row.addWidget(ico_wrap)

        # Columna: input + hint integrado
        col = QVBoxLayout()
        col.setSpacing(2)
        col.setContentsMargins(0, 0, 0, 0)

        self.scan_input = QLineEdit()
        self.scan_input.setPlaceholderText("Escanear código HU...")
        self.scan_input.setAlignment(Qt.AlignmentFlag.AlignLeft)
        self.scan_input.setFixedHeight(28)
        self.scan_input.setStyleSheet(f"""
            QLineEdit {{
                background: transparent;
                color: {C.BLUE_HOVER};
                font-size: 13pt;
                font-weight: bold;
                border: none;
                border-bottom: 2px solid {C.BLUE_LINE};
                border-radius: 0;
                padding: 0 4px;
            }}
            QLineEdit:focus {{
                border-bottom: 2px solid {C.BLUE_HOVER};
            }}
            QLineEdit::placeholder {{
                color: {C.BLUE_HOVER};
                font-weight: bold;
                font-size: 13pt;
            }}
        """)
        self.scan_input.returnPressed.connect(self._on_scan)
        col.addWidget(self.scan_input)

        # Hint inline (bajo el input, no flotante)
        self._hint = QLabel("Presiona Enter para confirmar")
        self._hint.setAlignment(Qt.AlignmentFlag.AlignLeft)
        self._hint.setStyleSheet(f"""
            font-size: 8pt;
            color: {C.MUTED};
            background: transparent;
        """)
        col.addWidget(self._hint)

        row.addLayout(col, stretch=1)
        card.addLayout(row)

        # ── DIVISOR VISUAL ─────────────────────────────────────────────────
        card.addSpacing(10)

        divider = QFrame()
        divider.setFrameShape(QFrame.Shape.HLine)
        divider.setFixedHeight(1)
        divider.setStyleSheet(f"background:{C.BORDER}; border:none;")
        card.addWidget(divider)

        card.addSpacing(10)

        # ── 2. ESTADO + ACCIÓN SECUNDARIA (misma fila) ─────────────────────
        r2 = QHBoxLayout()
        r2.setContentsMargins(0, 0, 0, 0)
        r2.setSpacing(8)

        # Indicador de estado con dot
        status_row = QHBoxLayout()
        status_row.setSpacing(6)
        status_row.setContentsMargins(0, 0, 0, 0)

        dot = QLabel("●")
        dot.setFixedSize(14, 14)
        dot.setAlignment(Qt.AlignmentFlag.AlignCenter)
        dot.setStyleSheet(f"font-size:7pt; color:{C.MUTED}; background:transparent;")
        status_row.addWidget(dot)

        self._pal_lbl = QLabel("Pallets escaneados: —")
        self._pal_lbl.setStyleSheet(f"""
            font-size: 8pt;
            color: {C.BLUE};
            font-weight: bold;
            background: transparent;
        """)
        status_row.addWidget(self._pal_lbl)

        r2.addLayout(status_row, stretch=1)

        np = QPushButton("+ Nuevo pallet")
        np.setFixedHeight(26)
        np.setCursor(Qt.CursorShape.PointingHandCursor)
        np.setStyleSheet(f"""
            QPushButton {{
                background: transparent;
                color: {C.BLUE};
                font-size: 8pt;
                font-weight: bold;
                border: 0.5px solid {C.BLUE};
                border-radius: 6px;
                padding: 0 12px;
            }}
            QPushButton:hover {{
                background: {C.BLUE_LIGHT};
            }}
            QPushButton:pressed {{
                background: {C.BLUE_LINE};
            }}
        """)
        np.clicked.connect(self._new_pallet)
        r2.addWidget(np)

        card.addLayout(r2)
        card.addSpacing(10)

        # ── 3. OPCIÓN TERCIARIA — configuración diferenciada ───────────────
        chk_frame = QFrame()
        chk_frame.setStyleSheet(f"""
            QFrame {{
                background: {C.SURFACE};
                border: 0.5px solid {C.BORDER};
                border-radius: 6px;
            }}
        """)
        chk_inner = QHBoxLayout(chk_frame)
        chk_inner.setContentsMargins(10, 7, 10, 7)
        chk_inner.setSpacing(8)

        self._chk_auto_start = QCheckBox(
            "Iniciar proceso automáticamente al escanear primer HU"
        )
        self._chk_auto_start.setChecked(False)
        self._chk_auto_start.setStyleSheet(f"""
            QCheckBox {{
                background: transparent;
                spacing: 8px;
                color: {C.TEXT_SECONDARY};
                font-size: 8pt;
            }}
            QCheckBox::indicator {{
                width: 14px;
                height: 14px;
                border: 1.5px solid {C.BLUE_HOVER};
                border-radius: 3px;
                background: white;
            }}
            QCheckBox::indicator:checked {{
                background: {C.BLUE_HOVER};
                border-color: {C.BLUE_HOVER};
                image: url("{resource_path('icons/check.svg')}");
            }}
        """)
        self._chk_auto_start.toggled.connect(self._on_auto_start_toggled)
        chk_inner.addWidget(self._chk_auto_start)

        card.addWidget(chk_frame)

        return card

    # ── Tabla card ────────────────────────────────────────────────────────────

    def _make_table_card(self) -> WhiteCard:
        card = WhiteCard(pad_h=0, pad_v=0)
        card.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)

        vl = QVBoxLayout()
        vl.setContentsMargins(0, 0, 0, 0)
        vl.setSpacing(0)

        hdr = QWidget()
        hdr.setFixedHeight(50)
        hdr.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)
        hdr.setStyleSheet(
            f"background:{C.CARD}; border-bottom:1px solid {C.CARD_BORDER};"
            f" border-top-left-radius:6px; border-top-right-radius:6px;"
        )
        hh = QHBoxLayout(hdr)
        hh.setContentsMargins(18, 0, 14, 0)
        hh.setSpacing(8)

        ttl = QLabel("Listado de Pallets Procesados")
        ttl.setStyleSheet(
            f"font-size:10.5pt; font-weight:bold; color:{C.TEXT}; background:transparent;"
        )
        hh.addWidget(ttl)
        hh.addStretch()

        vl.addWidget(hdr)

        self.table = QTableWidget(0, 5)
        self.table.setHorizontalHeaderLabels([
            "Pallet", "Origen", "Código HU", "F1 — ZMOVEINBHU", "F2 — ZMMTJI.SEP"
        ])

        self.table.setStyleSheet(f"""
            QTableWidget {{
                background:{C.CARD}; color:{C.TEXT};
                gridline-color:{C.TBL_BORDER}; border:none;
                font-size:9pt; outline:none;
                selection-background-color:{C.BLUE_LIGHT};
                selection-color:{C.BLUE};
                alternate-background-color:{C.ROW_ALT};
            }}
            QTableWidget::item {{
                padding:2px 10px; border:none;
                border-bottom:1px solid {C.TBL_BORDER};
            }}
            QTableWidget::item:selected {{
                background:{C.BLUE_LIGHT}; color:{C.BLUE};
            }}
            QHeaderView {{ background:{C.CARD}; }}
            QHeaderView::section {{
                background:{C.CARD}; color:{C.TEXT};
                font-size:9pt; font-weight:bold;
                padding:7px 10px; border:none;
                border-bottom:2px solid {C.TBL_HDR_SEP};
                border-right:1px solid {C.TBL_BORDER};
            }}
            QHeaderView::section:last {{ border-right:none; }}
        """)

        hh2 = self.table.horizontalHeader()
        hh2.setSectionResizeMode(0, QHeaderView.ResizeMode.Fixed)
        hh2.setSectionResizeMode(1, QHeaderView.ResizeMode.Fixed)
        hh2.setSectionResizeMode(2, QHeaderView.ResizeMode.Fixed)
        hh2.setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)
        hh2.setSectionResizeMode(4, QHeaderView.ResizeMode.Fixed)
        self.table.setColumnWidth(0, 82)
        self.table.setColumnWidth(1, 120)
        self.table.setColumnWidth(2, 180)
        self.table.setColumnWidth(4, 280)

        self.table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table.setAlternatingRowColors(True)
        self.table.verticalHeader().setVisible(False)
        self.table.verticalHeader().setDefaultSectionSize(32)
        self.table.setShowGrid(False)
        self.table.setFrameShape(QFrame.Shape.NoFrame)
        self.table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self._on_table_context_menu)

        # ── Contenedor flotante para sticky pallet (sobre la tabla) ─────────────
        table_container = QWidget()
        table_container.setStyleSheet("background:transparent;")
        table_layout = QVBoxLayout(table_container)
        table_layout.setContentsMargins(0, 0, 0, 0)
        table_layout.setSpacing(0)

        # Agregar tabla al contenedor
        table_layout.addWidget(self.table, stretch=1)

        # Sticky pallet header (flota sobre tabla justo debajo del header)
        self._sticky_pallet = QLabel()
        self._sticky_pallet.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._sticky_pallet.setStyleSheet(
            f"color:{C.SEP_TEXT}; font-size:9pt; font-weight:bold; "
            f"background:{C.CARD}; padding:6px; border-bottom:1px solid {C.TBL_BORDER};"
        )
        self._sticky_pallet.setVisible(False)
        self._sticky_pallet.setFixedHeight(28)
        self._sticky_pallet.setParent(table_container)  # Establecer parent para posicionamiento

        vl.addWidget(table_container, stretch=1)

        # Conectar scroll de tabla para actualizar sticky pallet
        self.table.verticalScrollBar().valueChanged.connect(self._update_sticky_pallet)

        card.addLayout(vl)
        return card

    # ── SAP timer ─────────────────────────────────────────────────────────────

    def _setup_sap_timer(self):
        t = QTimer(self)
        t.timeout.connect(self._check_sap)
        t.start(4000)
        self._check_sap()

        # Actualizar pallet timer (dinámico) cada segundo
        self._live_timer = QTimer(self)
        self._live_timer.timeout.connect(self._refresh_pallet_times)
        self._live_timer.start(1000)

    def _refresh_pallet_times(self):
        for pid in list(self._pallet_sep_rows.keys()):
            proc_time = self._queue.get_pallet_processing_time(pid)
            if proc_time is not None:
                self._update_pallet_separator(pid)

    def _check_sap(self):
        ok, user = SAPClient.check_session(SISTEMA_SAP)
        self._shell.set_connection(ok, user if ok else "")

    # ── Primer pallet ─────────────────────────────────────────────────────────

    def _init_pallet(self):
        pid = self._queue.new_pallet()
        self._last_ui_pallet = pid
        self._pal_lbl.setText(f"Pallets escaneados: {pid}")
        self._insert_sep(pid)

    def _insert_sep(self, pid: int):
        """Separador de pallet — línea de guiones centrada como en la imagen."""
        r = self.table.rowCount()
        self.table.insertRow(r)
        self.table.setRowHeight(r, 28)

        self._pallet_sep_rows[pid] = r

        hu_codes = self._queue.get_hu_codes_for_pallet(pid)
        hu_count = len(hu_codes)
        
        # Obtener tiempo de procesamiento si está disponible
        proc_time = self._queue.get_pallet_processing_time(pid)
        time_suffix = f"  Tiempo: {proc_time}" if proc_time else ""

        dashes = "- - - - - - - - - - - - - - - - - - -"
        html_txt = f"{dashes}   PALLET {pid}  <span style='color:#000000;font-weight:bold;'>({hu_count} HUS)</span>{time_suffix}   {dashes}"

        lbl = QLabel(html_txt)
        lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        lbl.setStyleSheet(f"color:{C.SEP_TEXT}; font-size:9pt; font-weight:bold; background:transparent;")
        self.table.setCellWidget(r, 0, lbl)
        self.table.setSpan(r, 0, 1, 5)
        self._update_sticky_pallet()  # Actualizar sticky si es necesario

    def _update_pallet_separator(self, pid: int):
        """Actualiza el contador de HUs en el separador del pallet."""
        if pid not in self._pallet_sep_rows:
            return

        r = self._pallet_sep_rows[pid]
        hu_codes = self._queue.get_hu_codes_for_pallet(pid)
        hu_count = len(hu_codes)
        
        # Obtener tiempo de procesamiento si está disponible
        proc_time = self._queue.get_pallet_processing_time(pid)
        time_suffix = f"  Tiempo: {proc_time}" if proc_time else ""

        dashes = "- - - - - - - - - - - - - - - - - - -"
        html_txt = f"{dashes}   PALLET {pid}  <span style='color:#000000;font-weight:bold;'>({hu_count} HUS)</span>{time_suffix}   {dashes}"

        lbl = self.table.cellWidget(r, 0)
        if lbl and isinstance(lbl, QLabel):
            lbl.setText(html_txt)
        self._update_sticky_pallet()  # Actualizar sticky si el contador cambió

    def _update_sticky_pallet(self):
        """Muestra el separador del pallet actual si está fuera de la vista durante scroll."""
        if not hasattr(self, '_sticky_pallet'):
            return

        # Encontrar el separador más cercano visible o fuera de vista
        current_pallet_id = None
        current_sep_row = None

        # Obtener rango de filas visibles
        viewport = self.table.viewport()
        first_visible_row = self.table.rowAt(viewport.rect().top())
        last_visible_row = self.table.rowAt(viewport.rect().bottom())

        if first_visible_row < 0:
            first_visible_row = 0
        if last_visible_row < 0:
            last_visible_row = self.table.rowCount() - 1

        # Si hay scroll hacia abajo, buscar el separador más reciente antes de las filas visibles
        if first_visible_row > 0:
            for pallet_id in sorted(self._pallet_sep_rows.keys(), reverse=True):
                sep_row = self._pallet_sep_rows[pallet_id]
                if sep_row < first_visible_row:
                    # Este separador está arriba del viewport
                    current_pallet_id = pallet_id
                    current_sep_row = sep_row
                    break

        # Si el separador está visible, no mostrar sticky
        if current_sep_row is not None:
            sep_top = self.table.rowViewportPosition(current_sep_row)
            if sep_top >= 0:  # El separador es visible
                self._sticky_pallet.setVisible(False)
                return

        # Mostrar sticky si hay un separador fuera de vista
        if current_pallet_id is not None:
            hu_codes = self._queue.get_hu_codes_for_pallet(current_pallet_id)
            hu_count = len(hu_codes)
            dashes = "- - - - - - - - - - - - - - - - - - -"
            html_txt = f"{dashes}   PALLET {current_pallet_id}  <span style='color:#000000;font-weight:bold;'>({hu_count} HUS)</span>   {dashes}"
            self._sticky_pallet.setText(html_txt)
            self._sticky_pallet.setVisible(True)

            # Posicionar justo debajo del header de la tabla
            header_height = self.table.horizontalHeader().height()
            table_width = self.table.width()
            self._sticky_pallet.setGeometry(0, header_height, table_width, 28)
        else:
            self._sticky_pallet.setVisible(False)

    # ── Escaneo ───────────────────────────────────────────────────────────────

    def _flash(self, msg: str, color: str):
        self._hint.setText(msg)
        self._hint.setStyleSheet(
            f"font-size:8pt; color:{color}; background:transparent; font-weight:600;"
        )
        QTimer.singleShot(2500, lambda: (
            self._hint.setText("Presiona Enter para confirmar"),
            self._hint.setStyleSheet(
                f"font-size:8pt; color:{C.MUTED}; background:transparent;"
            )
        ))

    def _on_scan(self):
        raw = self.scan_input.text().strip()
        self.scan_input.clear()
        if not raw:
            return
        if is_pallet_separator(raw):
            # _new_pallet ya valida que el actual no esté vacío
            self._new_pallet()
            return

        # Validar longitud mínima
        if len(raw) < 10:
            self._flash(
                f"⚠ Código HU inválido: '{raw}' tiene solo {len(raw)} caracteres. Mínimo: 11",
                C.ORANGE
            )
            return

        item = self._queue.add_hu(raw)
        if item is None:
            self._flash(f"⚠ Duplicado: {raw}", C.ORANGE)
            return

        # ¿El modelo creó un pallet nuevo automáticamente (origen distinto / auto_pallet)?
        if item.pallet_id != self._last_ui_pallet:
            self._last_ui_pallet = item.pallet_id
            self._insert_sep(item.pallet_id)
            self._pal_lbl.setText(f"Pallets escaneados: {self._get_display_pallet_count()}")

        self._add_row(item)
        self._update_pallet_separator(item.pallet_id)
        stats = self._queue.stats
        self._refresh_kpis(stats)
        self.table.scrollToBottom()
        self._flash(
            f"✓  {raw}  →  Pallet {item.pallet_id}  [{item.origin.label}]",
            C.GREEN_TEXT
        )
        mode = ProgressCard.MODE_RUNNING if self._worker.is_running() else ProgressCard.MODE_IDLE
        self._set_mode(
            f"HU {'en cola' if self._worker.is_running() else 'agregada'}: "
            f"{raw} — Pallet {item.pallet_id}", mode
        )

        if stats["total"] == 1 and self._auto_start_enabled and not self._worker.is_running():
            QTimer.singleShot(500, self._start)

    def _new_pallet(self):
        """
        Crea un nuevo pallet o reutiliza el actual si está vacío.
        Si el pallet activo no tiene HUs, simplemente avisa al usuario
        en lugar de crear un separador duplicado.
        """
        # Verificar si el pallet actual ya está vacío
        if self._last_ui_pallet > 0:
            hu_codes = self._queue.get_hu_codes_for_pallet(self._last_ui_pallet)
            if len(hu_codes) == 0:
                self._flash(
                    f"⚠ El pallet actual (P{self._last_ui_pallet:02d}) está vacío. "
                    "Escanea un HU primero.",
                    C.ORANGE
                )
                self.scan_input.setFocus()
                return

        # Crear (o reciclar) un nuevo pallet en el modelo
        pid = self._queue.new_pallet()

        # Si el modelo devolvió el mismo ID (ya había uno vacío), no insertar sep duplicado
        if pid == self._last_ui_pallet:
            self._flash(
                f"⚠ El pallet P{pid:02d} ya está vacío.",
                C.ORANGE
            )
            self.scan_input.setFocus()
            return

        self._last_ui_pallet = pid
        self._pal_lbl.setText(f"Pallets escaneados: {self._get_display_pallet_count()}")
        self._insert_sep(pid)
        self._set_mode(f"Nuevo pallet #{pid} iniciado.", ProgressCard.MODE_IDLE)
        self.scan_input.setFocus()

    def _on_auto_start_toggled(self, checked: bool):
        self._auto_start_enabled = checked

    def _on_table_context_menu(self, position):
        row = self.table.rowAt(position.y())
        if row < 0:
            return
        menu = QMenu(self)
        delete_action = menu.addAction("Borrar fila")
        action = menu.exec(self.table.mapToGlobal(position))
        if action == delete_action:
            self._delete_row(row)

    def _delete_row(self, row: int):
        pallet_id_to_delete = None
        for pid, sep_row in self._pallet_sep_rows.items():
            if sep_row == row:
                pallet_id_to_delete = pid
                break

        if pallet_id_to_delete is not None:
            if pallet_id_to_delete == 1:
                self._flash("⚠ No puedes borrar el pallet inicial.", C.ORANGE)
                return
            self._delete_pallet(pallet_id_to_delete)
        else:
            self._delete_hu_row(row)

    def _delete_pallet(self, pallet_id: int):
        """
        Borra un pallet completo (separador + todas sus filas de HU).
        Libera el ID para reciclarlo si el worker no está activo.
        Si era el pallet activo, activa el anterior con HUs o el primero vacío disponible.
        """
        hu_codes = list(self._queue.get_hu_codes_for_pallet(pallet_id))

        # 1. Eliminar HUs del modelo
        for hu_code in hu_codes:
            self._queue.remove_hu(hu_code)
            self._rows.pop(hu_code, None)

        # 2. Eliminar filas de la tabla (separador + filas HU) en orden inverso
        rows_to_delete = []
        if pallet_id in self._pallet_sep_rows:
            rows_to_delete.append(self._pallet_sep_rows[pallet_id])

        for i in range(self.table.rowCount()):
            item_widget = self.table.item(i, 2)
            if item_widget and item_widget.text() in hu_codes:
                rows_to_delete.append(i)

        for row in sorted(set(rows_to_delete), reverse=True):
            self.table.removeRow(row)

        # 3. Limpiar el separador del dict y actualizar filas
        self._pallet_sep_rows.pop(pallet_id, None)
        self._update_pallet_sep_rows_indices()

        # 4. Actualizar pallet activo en UI antes de liberar ID
        remaining = sorted(self._pallet_sep_rows.keys())
        if pallet_id == self._last_ui_pallet:
            if remaining:
                # Activar el último pallet restante
                self._last_ui_pallet = remaining[-1]
                self._queue.set_active_pallet(self._last_ui_pallet)
            else:
                # Sin pallets restantes: marca activo como 0 para liberar correctamente
                self._last_ui_pallet = 0
                self._queue.set_active_pallet(0)

        # 5. Liberar el ID en el modelo (para reciclarlo)
        self._queue.release_pallet(pallet_id)

        # 6. Si no queda ninguno, iniciar pallet inicial
        if not remaining:
            self._init_pallet()
            self._refresh_kpis(self._queue.stats)
            self._flash(
                f"✓ Pallet {pallet_id} y sus {len(hu_codes)} HUs eliminados.",
                C.GREEN_TEXT
            )
            return

        self._pal_lbl.setText(f"Pallets escaneados: {self._get_display_pallet_count()}")
        self._refresh_kpis(self._queue.stats)
        self._flash(
            f"✓ Pallet {pallet_id} y sus {len(hu_codes)} HUs eliminados.",
            C.GREEN_TEXT
        )

    def _delete_hu_row(self, row: int):
        """
        Borra una sola fila de HU.
        Si el pallet queda vacío tras el borrado, elimina también su separador
        y libera el ID para reciclarlo (a menos que sea el pallet activo actual,
        en cuyo caso solo queda vacío y listo para recibir el siguiente HU).
        """
        hu_item = self.table.item(row, 2)
        if hu_item is None:
            return

        hu_code   = hu_item.text()
        pallet_id = self._queue.remove_hu(hu_code)
        self._rows.pop(hu_code, None)

        # Borrar fila de la tabla
        self.table.removeRow(row)
        self._update_pallet_sep_rows_indices()

        if pallet_id is not None:
            if self._queue.pallet_is_empty(pallet_id):
                if pallet_id == self._last_ui_pallet:
                    # Pallet activo quedó vacío: mantener el separador visible
                    # (el usuario puede seguir escaneando HUs para este pallet)
                    self._update_pallet_separator(pallet_id)
                    self._flash(
                        f"✓ HU {hu_code} eliminado. "
                        f"Pallet P{pallet_id:02d} vacío — listo para nuevos HUs.",
                        C.GREEN_TEXT
                    )
                else:
                    # Pallet no activo quedó vacío: eliminar separador y reciclar ID
                    if pallet_id in self._pallet_sep_rows:
                        sep_row = self._pallet_sep_rows[pallet_id]
                        self.table.removeRow(sep_row)
                        self._pallet_sep_rows.pop(pallet_id, None)
                        self._update_pallet_sep_rows_indices()
                    self._queue.release_pallet(pallet_id)
                    self._flash(f"✓ HU {hu_code} eliminado.", C.GREEN_TEXT)
            else:
                self._update_pallet_separator(pallet_id)
                self._flash(f"✓ HU {hu_code} eliminado.", C.GREEN_TEXT)

        self._pal_lbl.setText(f"Pallets escaneados: {self._get_display_pallet_count()}")
        self._refresh_kpis(self._queue.stats)

    def _get_display_pallet_count(self) -> int:
        """Número de separadores de pallet actualmente visibles en la tabla."""
        return len(self._pallet_sep_rows)

    def _update_pallet_sep_rows_indices(self):
        new_sep_rows = {}
        for i in range(self.table.rowCount()):
            widget = self.table.cellWidget(i, 0)
            if widget and isinstance(widget, QLabel):
                text = widget.text()
                match = re.search(r'PALLET (\d+)', text)
                if match:
                    pid = int(match.group(1))
                    new_sep_rows[pid] = i
        self._pallet_sep_rows = new_sep_rows

    # ── Tabla ─────────────────────────────────────────────────────────────────

    def _add_row(self, item: HUItem):
        r = self.table.rowCount()
        self.table.insertRow(r)
        self._rows[item.hu_code] = r

        bg, fg = status_colors(item.status)

        self.table.setCellWidget(r, 0, _pallet_badge(f"P{item.pallet_id:02d}"))
        self._cell(r, 1, item.origin.label.split("(")[0].strip(), C.TEXT, C.CARD)

        c2 = QTableWidgetItem(item.hu_code)
        c2.setTextAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        f2 = QFont("Lucida Console, Courier New")
        f2.setPointSize(9)
        f2.setBold(True)
        c2.setFont(f2)
        c2.setForeground(QColor(C.TEXT))
        c2.setBackground(QColor(C.CARD))
        self.table.setItem(r, 2, c2)

        self._cell(r, 3, item.f1_display, fg, bg)
        self._cell(r, 4, item.f2_display, fg, bg)

    def _cell(self, row: int, col: int, text: str,
              fg: str = C.TEXT, bg: str = C.CARD,
              align=Qt.AlignmentFlag.AlignLeft):
        c = QTableWidgetItem(text)
        c.setTextAlignment(align | Qt.AlignmentFlag.AlignVCenter)
        c.setForeground(QColor(fg))
        c.setBackground(QColor(bg))
        self.table.setItem(row, col, c)

    def _clean_sap_codes(self, text: str) -> str:
        """Eliminar códigos SAP como @5C@, @5B@ del texto."""
        if not text:
            return text
        # Eliminar códigos SAP @XXXX@
        import re
        return re.sub(r'@[0-9A-F]{2}@\s*', '', text).strip()

    def _err_cell(self, text: str) -> QWidget:
        """❌ Error con mensaje completo + botón copiar."""
        # Limpiar códigos SAP del texto
        clean_text = self._clean_sap_codes(text)
        
        w = QWidget()
        w.setStyleSheet("background:transparent;")
        h = QHBoxLayout(w)
        h.setContentsMargins(6, 0, 4, 0)
        h.setSpacing(5)

        dot = QLabel("❌")
        dot.setStyleSheet("font-size:9pt; background:transparent;")
        dot.setFixedWidth(18)
        h.addWidget(dot)

        lbl = QLabel(clean_text)
        lbl.setStyleSheet(
            f"font-size:8pt; color:{C.RED}; font-weight:600; background:transparent;"
        )
        lbl.setToolTip(clean_text)
        lbl.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        lbl.setWordWrap(False)
        h.addWidget(lbl, stretch=1)

        btn_copy = QPushButton("⧉")
        btn_copy.setFixedSize(22, 22)
        btn_copy.setToolTip("Copiar error al portapapeles")
        btn_copy.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_copy.setStyleSheet(f"""
            QPushButton {{
                background:{C.RED_BG}; color:{C.RED};
                font-size:11pt; border:1px solid {C.RED_MUT};
                border-radius:3px; padding:0;
            }}
            QPushButton:hover {{ background:{C.RED_MUT}; border-color:{C.RED}; }}
            QPushButton:pressed {{ background:{C.RED}; color:white; }}
        """)

        def _copy(checked=False, msg=clean_text, b=btn_copy):
            QApplication.clipboard().setText(msg)
            b.setText("✓")
            b.setStyleSheet(f"""
                QPushButton {{
                    background:{C.GREEN_BG}; color:{C.GREEN};
                    font-size:10pt; border:1px solid {C.GREEN_MUT};
                    border-radius:3px; padding:0;
                }}
            """)
            QTimer.singleShot(1800, lambda: (
                b.setText("⧉"),
                b.setStyleSheet(f"""
                    QPushButton {{
                        background:{C.RED_BG}; color:{C.RED};
                        font-size:11pt; border:1px solid {C.RED_MUT};
                        border-radius:3px; padding:0;
                    }}
                    QPushButton:hover {{ background:{C.RED_MUT}; border-color:{C.RED}; }}
                """)
            ))

        btn_copy.clicked.connect(_copy)
        h.addWidget(btn_copy)
        return w

    def _on_item(self, item: HUItem):
        r = self._rows.get(item.hu_code)
        if r is None:
            return

        bg, fg = status_colors(item.status)

        # F1
        if item.status == "ok":
            self.table.setCellWidget(r, 3, _ok_cell(item.f1_display))
        elif item.status == "error":
            self.table.setCellWidget(r, 3, self._err_cell(item.f1_display or item.phase1_msg or "Error"))
        else:
            self.table.removeCellWidget(r, 3)
            self._cell(r, 3, item.f1_display, fg, bg)

        # F2
        if item.status == "ok":
            self.table.setCellWidget(r, 4, _ok_cell(item.f2_display))
        elif item.status == "error" and not item.phase2_msg:
            # Error en F1 — F2 nunca se ejecutó
            self.table.removeCellWidget(r, 4)
            self._cell(r, 4, "—  omitido", C.MUTED, C.CARD)
        elif item.status == "error":
            # Error en F2
            self.table.setCellWidget(r, 4, self._err_cell(item.f2_display or "Error"))
        else:
            self.table.removeCellWidget(r, 4)
            self._cell(r, 4, item.f2_display, fg, bg)

        # Actualizar el separador del pallet en tiempo real para mostrar tiempo
        self._update_pallet_separator(item.pallet_id)

        if item.status == "processing":
            self._set_mode(
                f"Procesando → Pallet {item.pallet_id}  |  HU: {item.hu_code}",
                ProgressCard.MODE_RUNNING
            )
        elif item.status == "error":
            # Error capturado - no mostrar en progreso
            pass

    # ── KPIs + progreso ───────────────────────────────────────────────────────

    def _on_stats(self, s: dict):
        self._refresh_kpis(s)

    def _refresh_kpis(self, s: dict):
        self._kpi_total.set_value(str(s["total"]))
        self._kpi_ok.set_value(str(s["ok"]))
        self._kpi_err.set_value(str(s["errors"]))
        self._kpi_pend.set_value(str(s["pending"]))
        self._pal_lbl.setText(f"Pallets escaneados: {self._get_display_pallet_count()}")
        self._kpi_err.set_color(C.RED if s["errors"] > 0 else C.MUTED)

        done = s["ok"] + s["errors"]
        self._prog.update(done, s["total"])
        
        # Actualizar estado del botón Iniciar
        self._update_start_button()

    def _set_mode(self, text: str, mode: str):
        self._prog.set_mode(mode, text)
        color = {
            ProgressCard.MODE_IDLE:      C.MUTED,
            ProgressCard.MODE_RUNNING:   C.BLUE,
            ProgressCard.MODE_PAUSED:    C.ORANGE,
            ProgressCard.MODE_ERROR:     C.RED,
            ProgressCard.MODE_STOPPED:   C.MUTED,
            ProgressCard.MODE_COMPLETED: C.GREEN_TEXT,
        }.get(mode, C.MUTED)
        self._footer.set_status(f"■  {text}", color)

    # ── Worker ────────────────────────────────────────────────────────────────

    def _start(self):
        if self._queue.stats["total"] == 0:
            QMessageBox.warning(self, "Cola vacía",
                                "Escanea HUs antes de iniciar el proceso.")
            return
        ok, _ = SAPClient.check_session(SISTEMA_SAP)
        if not ok:
            QMessageBox.critical(self, "Sin sesión SAP",
                f"No hay sesión activa en {SISTEMA_SAP}.\n"
                "Inicia sesión en SAP antes de continuar.")
            return
        self._worker.start(
            run_f1=self.chk_f1.isChecked(),
            run_f2=self.chk_f2.isChecked(),
            run_sp01=self.chk_sp01.isChecked(),
        )
        self._set_running(True)
        self._set_mode("Proceso iniciado. Puedes continuar escaneando HUs...",
                       ProgressCard.MODE_RUNNING)

    def _stop(self, update_footer=True):
        self._worker.stop()
        self._set_running(False)
        self._set_paused(False)
        self._prog.set_mode(ProgressCard.MODE_STOPPED, "Proceso detenido.")
        if update_footer:
            self._footer.set_status("■  Proceso detenido.", C.MUTED)

    def _cont(self):
        self._set_paused(False)
        self._worker.resume()
        self._set_mode("Reanudando proceso...", ProgressCard.MODE_RUNNING)

    def _on_pallet_done(self, pid: int):
        self._set_paused(True)
        codes = self._queue.get_hu_codes_for_pallet(pid)
        lst = ", ".join(codes[:3]) + (f" +{len(codes)-3} más" if len(codes) > 3 else "")
        # Actualizar el separador del pallet para mostrar tiempo de procesamiento
        self._update_pallet_separator(pid)
        self._set_mode(
            f"Pallet {pid} impreso ({len(codes)} HUs) — Presiona ▶ CONTINUAR.",
            ProgressCard.MODE_PAUSED
        )

    def _on_sp01(self, result: dict):
        self._set_mode(f"SP01: {result['message']}", ProgressCard.MODE_COMPLETED)
        self._stop()
        s = self._queue.stats
        QMessageBox.information(
            self, "Proceso completado",
            f"Cola procesada exitosamente.\n\n"
            f"Pallets: {s['pallets']}  |  HUs: {s['total']}\n"
            f"OK: {s['ok']}  |  Errores: {s['errors']}\n\n"
            f"SP01: {result['message']}"
        )

    def _on_err(self, msg: str):
        self._stop(update_footer=False)  # No actualizar footer al haber error
        clean_msg = self._clean_sap_codes(msg)
        QMessageBox.critical(self, "Error SAP", f"Error en el proceso:\n\n{clean_msg}")

    # ── Estado UI ─────────────────────────────────────────────────────────────

    def _set_running(self, r: bool):
        self._footer.btn_stop.setEnabled(r)
        self._footer.btn_clear.setEnabled(not r)
        self.chk_f1.setEnabled(not r)
        self.chk_f2.setEnabled(not r)
        self.chk_sp01.setEnabled(not r)
        self._chk_auto_start.setEnabled(not r)
        # Actualizar btn_start según si hay HU pendientes
        self._update_start_button()

    def _update_start_button(self):
        """
        Desactiva btn_start solo si:
        - El proceso NO está corriendo
        - No hay HU pendientes (todo fue procesado)
        Lo reactiva cuando hay HU pendientes.
        """
        if self._worker.is_running():
            self._footer.btn_start.setEnabled(False)
        else:
            # Habilitar solo si hay HU pendientes
            has_pending = self._queue.stats["pending"] > 0
            self._footer.btn_start.setEnabled(has_pending)

    def _set_paused(self, p: bool):
        self._footer.btn_continue.setVisible(p)
        self._footer.btn_start.setVisible(not p)
        self._footer.btn_stop.setEnabled(not p)

    # ── Exportar ──────────────────────────────────────────────────────────────

    def _export(self):
        if self._queue.stats["total"] == 0:
            QMessageBox.information(self, "Cola vacía", "No hay datos para exportar.")
            return
        default = f"HUFlow_Export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        path, _ = QFileDialog.getSaveFileName(
            self, "Exportar datos de cola", default,
            "Archivos CSV (*.csv);;Todos los archivos (*)"
        )
        if not path:
            return
        try:
            with open(path, 'w', newline='', encoding='utf-8-sig') as f:
                w = csv.writer(f)
                w.writerow(["Pallet","Origen","Código HU","Estado",
                             "F1","F1 msg","F2","F2 msg","F2 ms",
                             "Agregada","Procesada"])
                for item in self._queue.get_all():
                    w.writerow([
                        f"P{item.pallet_id:02d}",
                        item.origin.label.split("(")[0].strip(),
                        item.hu_code, item.status_display,
                        "ZMOVEINBHU", item.phase1_msg or "",
                        "ZMMTIJSEP",  item.phase2_msg or "",
                        item.phase2_ms or "",
                        item.added_at.strftime("%d/%m/%Y %H:%M:%S") if item.added_at else "",
                        item.processed_at.strftime("%d/%m/%Y %H:%M:%S") if item.processed_at else "",
                    ])
            try:
                if sys.platform == "win32":   os.startfile(path)
                elif sys.platform == "darwin": subprocess.Popen(["open", path])
                else:                          subprocess.Popen(["xdg-open", path])
            except Exception:
                pass
            QMessageBox.information(self, "Exportación exitosa",
                                    f"Datos exportados a:\n{path}")
        except Exception as e:
            QMessageBox.critical(self, "Error al exportar",
                                 f"No se pudo exportar:\n{e}")

    # ── Limpiar ───────────────────────────────────────────────────────────────

    def _clear(self):
        if self._worker.is_running():
            QMessageBox.warning(self, "Proceso activo",
                                "Detén el proceso antes de limpiar la cola.")
            return
        if QMessageBox.question(
            self, "Limpiar cola",
            "Esto eliminará todas las HUs.\n¿Continuar?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        ) == QMessageBox.StandardButton.No:
            return

        self._queue.clear()
        self._rows.clear()
        self._pallet_sep_rows.clear()
        self.table.setRowCount(0)
        self._refresh_kpis({"total":0,"pending":0,"ok":0,"errors":0,"pallets":0})
        self._prog.update(0, 0)
        self._prog.set_mode(ProgressCard.MODE_IDLE, "Cola limpiada. Listo para escanear.")
        self._footer.set_status("■  Cola limpiada. Listo para escanear.", C.MUTED)
        self._pal_lbl.setText("Pallets escaneados: —")
        self._init_pallet()
        self.scan_input.setFocus()

    def _start_tour(self):
        """Iniciar el tutorial interactivo."""
        tour_steps = [
            {
                'widget': self.scan_input,
                'title': '1. Escanear HU',
                'description': 'Ingresa el código del HU. Presiona Enter para procesar.',
                'arrow_from': 'bottom'
            },
            {
                'widget': self.table,
                'title': '2. Tabla de HUs',
                'description': 'Los HUs escaneados aparecen aquí agrupados por pallets automáticamente según su origen.',
                'arrow_from': 'right'
            },
            {
                'widget': self._pipeline,
                'title': '3. Fases SAP',
                'description': 'F1: Movimiento en SAP (ZMOVEINBHU) | F2: Separación (ZMMTIJSEP) | SP01: Impresión de etiquetas',
                'arrow_from': 'left'
            },
            {
                'widget': self._chk_auto_start,
                'title': '4. Auto-inicio',
                'description': 'Activa esto para iniciar automáticamente la lectura cuando escanees el primer HU.',
                'arrow_from': 'bottom'
            },
            {
                'widget': self._footer.btn_start,
                'title': '5. Botón Iniciar',
                'description': 'Presiona aquí para comenzar el procesamiento de los HUs escaneados.',
                'arrow_from': 'top'
            },

            {
                'widget': self._kpi_total,
                'title': '7. KPIs (Indicadores)',
                'description': 'Panel de estadísticas que muestra Total de HUs, Procesadas, Errores y Pendientes en tiempo real.',
                'arrow_from': 'bottom'
            },
            {
                'widget': self._footer.btn_stop,
                'title': '8. Botón Detener',
                'description': 'Pausa el procesamiento en cualquier momento. El proceso se puede reanudar después.',
                'arrow_from': 'top'
            },
            {
                'widget': self._footer.btn_clear,
                'title': '9. Limpiar HUs',
                'description': 'Borra todas las HUs de la cola y limpia la tabla. Usa esto para comenzar de cero.',
                'arrow_from': 'top'
            },
            {
                'widget': self._shell.ham_btn,
                'title': '6. Menú principal',
                'description': 'Presiona aquí para ver las opciones de menú (Tutorial, Exportar).',
                'arrow_from': 'bottom'
            },
        ]
        
        self._tour.set_tour_steps(tour_steps)
        self._tour.start_tour()


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def run_queue_app():
    app = QApplication(sys.argv)
    app.setStyle("Fusion")

    pal = QPalette()
    pal.setColor(QPalette.ColorRole.Window,          QColor(C.PAGE))
    pal.setColor(QPalette.ColorRole.WindowText,      QColor(C.TEXT))
    pal.setColor(QPalette.ColorRole.Base,            QColor(C.CARD))
    pal.setColor(QPalette.ColorRole.AlternateBase,   QColor(C.ROW_ALT))
    pal.setColor(QPalette.ColorRole.ToolTipBase,     QColor(C.SHELL))
    pal.setColor(QPalette.ColorRole.ToolTipText,     QColor("white"))
    pal.setColor(QPalette.ColorRole.Text,            QColor(C.TEXT))
    pal.setColor(QPalette.ColorRole.Button,          QColor(C.CARD))
    pal.setColor(QPalette.ColorRole.ButtonText,      QColor(C.TEXT))
    pal.setColor(QPalette.ColorRole.BrightText,      QColor(C.RED))
    pal.setColor(QPalette.ColorRole.Link,            QColor(C.BLUE))
    pal.setColor(QPalette.ColorRole.Highlight,       QColor(C.BLUE))
    pal.setColor(QPalette.ColorRole.HighlightedText, QColor("white"))
    app.setPalette(pal)

    # Configurar icono de la aplicación (para la ventana del escritorio)
    icon_path = os.path.join(os.path.dirname(__file__), "logo.ico")
    if os.path.exists(icon_path):
        app.setWindowIcon(QIcon(icon_path))

    w = QueueWindow()
    w.show()
    w.scan_input.setFocus()
    sys.exit(app.exec())