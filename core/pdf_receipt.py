import os
import sys
import time
import glob
import shutil
import atexit
import logging
import tempfile
import threading
import subprocess
from datetime import datetime
from io import BytesIO
from django.conf import settings


log = logging.getLogger(__name__)

# ── Nombre de la app (carpeta de config en home del usuario) ──────────────────
APP_NAME = "NexHus"

# ── Ruta al SumatraPDF portable bundled con la app ───────────────────────────
# Usa BASE_DIR de Django para encontrar la carpeta raíz del proyecto.
# ── Después ──────────────────────────────────────────────────────────────────
SUMATRA_BUNDLED = str(settings.BASE_DIR / "core" / "vendors" / "SumatraPDF.exe")
# ── Lock de impresión por instancia de proceso ────────────────────────────────
# Cada usuario tiene su propio proceso → su propio lock. No interfieren entre sí.
_print_lock = threading.Lock()


# ── Colores ───────────────────────────────────────────────────────────────────
class PDF_C:
    SHELL       = "#1B2537"
    BLUE        = "#0d2038"
    BLUE_HOVER  = "#0070F5"
    GREEN       = "#107E3E"
    GREEN_TEXT  = "#188918"
    RED         = "#BB0000"
    TEXT        = "#32363A"
    MUTED       = "#6A7279"
    LIGHT       = "#89919A"
    CARD_BDR    = "#B0B6BD"
    PAGE_BG     = "#C8CDD3"
    PAGE_BG_ALT = "#E8ECEF"
    HEADER_BG   = "#0d2038"
    BADGE_BG    = "#EBF4FF"
    BADGE_BDR   = "#BDD9F5"


def _hex_to_rgb(hex_color: str):
    """Convierte '#RRGGBB' a tupla (r, g, b) en rango 0–1."""
    h = hex_color.lstrip("#")
    return tuple(int(h[i:i + 2], 16) / 255 for i in (0, 2, 4))


# ─────────────────────────────────────────────────────────────────────────────
# CONFIG POR USUARIO
# Cada usuario del sistema operativo tiene su propia carpeta ~/.myapp/
# con su propio user_config.ini, evitando que se pisen entre sí.
# ─────────────────────────────────────────────────────────────────────────────

def _get_config_path() -> str:
    """
    Retorna la ruta del archivo de configuración del usuario actual.
    Ejemplo: C:\\Users\\JUAN\\.myapp\\user_config.ini
    """
    user_home = os.path.expanduser("~")
    config_dir = os.path.join(user_home, f".{APP_NAME}")
    os.makedirs(config_dir, exist_ok=True)
    return os.path.join(config_dir, "user_config.ini")


def get_receipt_printer() -> str | None:
    """
    Retorna el nombre de la impresora configurada para el usuario actual,
    o None si no hay ninguna (usa la impresora por defecto del sistema).
    """
    import configparser
    config = configparser.ConfigParser()
    config.read(_get_config_path(), encoding="utf-8")
    name = config.get("printing", "receipt_printer", fallback="").strip()
    return name if name else None


# ─────────────────────────────────────────────────────────────────────────────
# DETECCIÓN DE SUMATRAPDF — bundled primero, sistema como fallback
#
# Orden de búsqueda:
#   1. vendors/SumatraPDF.exe  ← portable bundled con la app (prioridad máxima)
#   2. PATH del sistema
#   3. %LOCALAPPDATA% del usuario actual
#   4. %APPDATA% del usuario actual
#   5. Program Files (instalación global)
#
# Si no se encuentra en ningún lado, `verify_sumatra_on_startup()` muestra
# un aviso claro al usuario con la URL de descarga.
# ─────────────────────────────────────────────────────────────────────────────

def find_sumatra() -> str | None:
    """
    Busca SumatraPDF.exe priorizando el ejecutable portable bundled.

    Retorna:
        Ruta absoluta al ejecutable, o None si no se encontró.
    """
    # 1. Bundled portable — siempre disponible si el equipo tiene la carpeta vendors/
    if os.path.exists(SUMATRA_BUNDLED):
        log.debug("sumatra_found_bundled path=%s", SUMATRA_BUNDLED)
        return SUMATRA_BUNDLED

    # 2. PATH del sistema (instalación global o portable que el usuario agregó al PATH)
    found_in_path = shutil.which("SumatraPDF")
    if found_in_path:
        log.debug("sumatra_found_in_path path=%s", found_in_path)
        return found_in_path

    # 3 & 4. AppData del usuario actual (sin hardcodear nombre de usuario)
    local_app = os.path.expandvars(r"%LOCALAPPDATA%\SumatraPDF\SumatraPDF.exe")
    roaming   = os.path.expandvars(r"%APPDATA%\SumatraPDF\SumatraPDF.exe")

    # 5. Instalaciones globales estándar
    prog_files    = r"C:\Program Files\SumatraPDF\SumatraPDF.exe"
    prog_files_86 = r"C:\Program Files (x86)\SumatraPDF\SumatraPDF.exe"

    for candidate in [local_app, roaming, prog_files, prog_files_86]:
        if os.path.exists(candidate):
            log.debug("sumatra_found_system path=%s", candidate)
            return candidate

    log.error("sumatra_not_found — bundled=%s | sistema=no encontrado", SUMATRA_BUNDLED)
    return None


def verify_sumatra_on_startup() -> bool:
    """
    Verifica que SumatraPDF esté disponible al arrancar la app.
    Llama a esta función en el __init__ o al iniciar la ventana principal.

    Retorna:
        True  → SumatraPDF encontrado, todo OK.
        False → No encontrado, se mostró aviso al usuario.

    Ejemplo de uso en main.py:
        from pdf_printer import verify_sumatra_on_startup
        if not verify_sumatra_on_startup():
            sys.exit(1)  # o simplemente deshabilitar el botón de impresión
    """
    exe = find_sumatra()

    if exe:
        log.info("sumatra_ok path=%s", exe)
        return True

    # ── No encontrado: construir mensaje de aviso ─────────────────────────────
    bundled_dir = os.path.dirname(SUMATRA_BUNDLED)
    msg = (
        "SumatraPDF no está disponible.\n\n"
        "Para habilitar la impresión de PDFs tienes dos opciones:\n\n"
        "  Opción A (recomendada) — Portable:\n"
        f"    1. Descarga SumatraPDF portable desde:\n"
        f"       https://www.sumatrapdfreader.org/download-free-pdf-viewer\n"
        f"    2. Copia SumatraPDF.exe a la carpeta:\n"
        f"       {bundled_dir}\n\n"
        "  Opción B — Instalador:\n"
        "    Instala SumatraPDF normalmente en tu sistema.\n\n"
        "La app funcionará con normalidad pero no podrá imprimir hasta resolver esto."
    )

    log.warning("sumatra_missing — mostrando aviso al usuario")

    # Mostrar aviso: usa tkinter si está disponible, si no imprime en consola
    try:
        import tkinter as tk
        from tkinter import messagebox
        root = tk.Tk()
        root.withdraw()  # ocultar ventana raíz
        messagebox.showwarning("SumatraPDF no encontrado", msg)
        root.destroy()
    except Exception:
        # Si no hay GUI disponible (servidor, terminal), loguear y continuar
        print(f"\n{'='*60}\n⚠️  AVISO: {msg}\n{'='*60}\n")

    return False


# ─────────────────────────────────────────────────────────────────────────────
# LIMPIEZA AUTOMÁTICA DE PDFs TEMPORALES
# Se registra con atexit para limpiar al cerrar la app.
# Solo borra archivos con el prefijo "pallet_" del directorio temporal.
# ─────────────────────────────────────────────────────────────────────────────

def _cleanup_temp_pdfs() -> None:
    """Elimina PDFs temporales generados por esta app al cerrar el proceso."""
    pattern = os.path.join(tempfile.gettempdir(), "pallet_*.pdf")
    removed = 0
    for filepath in glob.glob(pattern):
        try:
            os.remove(filepath)
            removed += 1
        except Exception as e:
            log.debug("cleanup_skip path=%s error=%s", filepath, e)
    if removed:
        log.info("cleanup_temp_pdfs removed=%d", removed)


# Registrar limpieza automática al salir
atexit.register(_cleanup_temp_pdfs)


# ─────────────────────────────────────────────────────────────────────────────
# CLASE PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

class PalletReceiptPDF:
    """
    Genera el PDF de recibos de un pallet y lo imprime.

    Uso típico:
        path = PalletReceiptPDF.generate(
            pallet_id=1,
            origin_label="Tailandia (THA)",
            receipts={"TH0000267998": "00000069693553"},
        )
        PalletReceiptPDF.print_pdf(path)
    """

    # ── Layout ────────────────────────────────────────────────────────────────
    COLS        = 3
    MARGIN_MM   = 5
    GUTTER_MM   = 5
    ROW_H_MM    = 58
    HEADER_H_MM = 23
    FOOTER_H_MM = 5

    @classmethod
    def generate(
        cls,
        pallet_id: int,
        origin_label: str,
        receipts: dict[str, str],
        hu_display_map: dict[str, str] | None = None,
        printed_by: str | None = None,
        output_path: str | None = None,
    ) -> str:
        """
        Genera el PDF y retorna la ruta del archivo creado.

        Args:
            pallet_id    : ID del pallet.
            origin_label : etiqueta de origen (ej. "Tailandia (THA)").
            receipts     : dict { hu_code → receipt_id }.
            output_path  : ruta de salida; si es None, crea archivo temporal.

        Retorna:
            Ruta absoluta del PDF generado.

        Raises:
            ImportError : si reportlab o python-barcode no están instalados.
        """
        started_at = time.perf_counter()
        try:
            from reportlab.lib.pagesizes import A4
            from reportlab.lib.units import mm
            from reportlab.pdfgen import canvas as rl_canvas
        except ImportError as e:
            raise ImportError(
                "reportlab no instalado. "
                "Ejecuta: pip install reportlab --break-system-packages"
            ) from e

        if not output_path:
            tmp_dir = tempfile.gettempdir()
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = os.path.join(tmp_dir, f"pallet_{pallet_id}_{ts}.pdf")

        log.info(
            "pdf_generate pallet=%d origin=%s hu_count=%d path=%s",
            pallet_id, origin_label, len(receipts), output_path,
        )

        page_w, page_h = A4
        margin   = cls.MARGIN_MM * mm
        gutter   = cls.GUTTER_MM * mm
        col_w    = (page_w - margin * 2 - gutter * (cls.COLS - 1)) / cls.COLS
        row_h    = cls.ROW_H_MM * mm
        header_h = cls.HEADER_H_MM * mm
        footer_h = cls.FOOTER_H_MM * mm

        fecha = datetime.now().strftime("%d/%m/%Y %H:%M")
        printed_by = (
            printed_by
            or getattr(settings, "SAP_LOGIN_USER", "")
            or "Desconocido"
        ).strip()
        c = rl_canvas.Canvas(output_path, pagesize=A4)

        def _draw_header(canvas_obj, page_num: int = 1):
            r, g, b = _hex_to_rgb(PDF_C.HEADER_BG)
            canvas_obj.setFillColorRGB(r, g, b)
            canvas_obj.rect(0, page_h - header_h, page_w, header_h, fill=1, stroke=0)

            canvas_obj.setFillColorRGB(1, 1, 1)
            canvas_obj.setFont("Helvetica-Bold", 13)
            canvas_obj.drawCentredString(
                page_w / 2,
                page_h - 8 * mm,
                f"Pallet {pallet_id}  —  {origin_label}",
            )

            canvas_obj.setFont("Helvetica", 8)
            canvas_obj.setFillColorRGB(0.8, 0.9, 1.0)
            suffix = f"  |  Pág. {page_num}" if page_num > 1 else ""
            canvas_obj.drawCentredString(
                page_w / 2,
                page_h - 14 * mm,
                f"{fecha}  |  Usuario: {printed_by}  |  Total HUs: {len(receipts)}{suffix}",
            )

        def _draw_footer(canvas_obj, page_num: int):
            r, g, b = _hex_to_rgb(PDF_C.CARD_BDR)
            canvas_obj.setStrokeColorRGB(r, g, b)
            canvas_obj.setLineWidth(0.3)
            canvas_obj.line(margin, footer_h, page_w - margin, footer_h)

            r, g, b = _hex_to_rgb(PDF_C.MUTED)
            canvas_obj.setFillColorRGB(r, g, b)
            canvas_obj.setFont("Helvetica", 6)
            canvas_obj.drawString(margin, 3 * mm, f"Generado: {fecha}  |  Usuario: {printed_by}")
            canvas_obj.drawRightString(
                page_w - margin, 3 * mm,
                f"Pallet {pallet_id}  |  Pág. {page_num}",
            )

        def _draw_receipt_header(canvas_obj, page_num: int, total_pages: int):
            canvas_obj.setFillColorRGB(1, 1, 1)
            canvas_obj.rect(0, page_h - header_h, page_w, header_h, fill=1, stroke=0)

            canvas_obj.setFillColorRGB(0, 0, 0)
            canvas_obj.rect(margin, page_h - 17 * mm, 1.7 * mm, 12 * mm, fill=1, stroke=0)

            canvas_obj.setFont("Helvetica-Bold", 13)
            canvas_obj.drawString(
                margin + 3.2 * mm,
                page_h - 9.2 * mm,
                f"PALLET {pallet_id} - {origin_label.upper()}",
            )

            canvas_obj.setFont("Helvetica", 7)
            canvas_obj.drawString(
                margin + 3.2 * mm,
                page_h - 15.2 * mm,
                f"Generado: {fecha} | Usuario: {printed_by}",
            )

            canvas_obj.setFont("Helvetica-Bold", 7)
            canvas_obj.drawRightString(page_w - margin, page_h - 8.3 * mm, f"Total HUs: {len(receipts)}")
            canvas_obj.drawRightString(page_w - margin, page_h - 14.2 * mm, f"Pag. {page_num}/{total_pages}")

            canvas_obj.setStrokeColorRGB(0, 0, 0)
            canvas_obj.setLineWidth(1.2)
            canvas_obj.line(margin, page_h - header_h + 2 * mm, page_w - margin, page_h - header_h + 2 * mm)

        def _draw_receipt_footer(canvas_obj, page_num: int):
            canvas_obj.setFillColorRGB(1, 1, 1)

        def _draw_barcode(canvas_obj, value: str, x_pos: float, y_pos: float, width: float, height: float) -> bool:
            if not value:
                return False

            try:
                import barcode as bc_lib
                from barcode.writer import ImageWriter
                from reportlab.lib.utils import ImageReader

                buf = BytesIO()
                code128 = bc_lib.get("code128", value, writer=ImageWriter())
                code128.write(buf, options={
                    "module_width": 0.30,
                    "module_height": 13.0,
                    "quiet_zone": 1.0,
                    "font_size": 0,
                    "text_distance": 1.0,
                    "write_text": False,
                    "background": "white",
                    "foreground": "black",
                })
                buf.seek(0)

                canvas_obj.drawImage(
                    ImageReader(buf),
                    x_pos,
                    y_pos,
                    width=width,
                    height=height,
                    preserveAspectRatio=False,
                    mask="auto",
                )
                return True
            except ImportError:
                log.warning("python-barcode no instalado - omitiendo barcode para receipt=%s", value)
            except Exception as e:
                log.warning("barcode_error receipt=%s error=%s", value, e)
            return False

        def _draw_checkbox(canvas_obj, x_pos: float, y_pos: float, label: str):
            box = 3.3 * mm
            canvas_obj.setStrokeColorRGB(0, 0, 0)
            canvas_obj.setLineWidth(0.5)
            canvas_obj.rect(x_pos, y_pos, box, box, fill=0, stroke=1)
            canvas_obj.setFillColorRGB(0, 0, 0)
            canvas_obj.setFont("Helvetica-Bold", 6)
            canvas_obj.drawString(x_pos + box + 1.2 * mm, y_pos + 0.4 * mm, label)

        def _draw_receipt_card(canvas_obj, idx: int, hu_code: str, receipt_id: str, x_pos: float, y_top: float):
            y_bottom = y_top - row_h
            card_h = row_h - 2 * mm
            card_y = y_bottom + 1 * mm
            strip_h = 8.5 * mm
            incident_h = 20.5 * mm

            canvas_obj.setFillColorRGB(1, 1, 1)
            canvas_obj.roundRect(x_pos, card_y, col_w, card_h, 1 * mm, fill=1, stroke=0)
            canvas_obj.setStrokeColorRGB(0, 0, 0)
            canvas_obj.setLineWidth(0.7)
            canvas_obj.roundRect(x_pos, card_y, col_w, card_h, 1 * mm, fill=0, stroke=1)

            canvas_obj.setFillColorRGB(0, 0, 0)
            canvas_obj.roundRect(x_pos, card_y + card_h - strip_h, col_w, strip_h, 1 * mm, fill=1, stroke=0)
            canvas_obj.rect(x_pos, card_y + card_h - strip_h, col_w, 2 * mm, fill=1, stroke=0)
            canvas_obj.setFillColorRGB(1, 1, 1)
            canvas_obj.setFont("Helvetica-Bold", 9)
            canvas_obj.drawString(x_pos + 3 * mm, card_y + card_h - 5.6 * mm, f"#{idx + 1}")

            content_x = x_pos + 3 * mm
            content_w = col_w - 6 * mm
            content_top = card_y + card_h - strip_h - 4 * mm

            hu_for_print = hu_display_map.get(hu_code, hu_code)
            hu_display = hu_for_print if len(hu_for_print) <= 22 else hu_for_print[:19] + "..."
            receipt_display = receipt_id if len(receipt_id) <= 22 else receipt_id[:19] + "..."

            canvas_obj.setFillColorRGB(0, 0, 0)
            canvas_obj.setFont("Helvetica", 5.4)
            canvas_obj.drawString(content_x, content_top, "HANDLING UNIT")
            canvas_obj.setFont("Helvetica-Bold", 8)
            canvas_obj.drawString(content_x, content_top - 4.2 * mm, hu_display)

            canvas_obj.setFont("Helvetica", 5.4)
            canvas_obj.drawString(content_x, content_top - 9 * mm, "RECEPCION")

            bar_y = card_y + incident_h + 3.0 * mm
            bar_h = 9.7 * mm
            barcode_ok = _draw_barcode(canvas_obj, receipt_id, content_x, bar_y, content_w, bar_h)
            if not barcode_ok:
                canvas_obj.setFont("Courier-Bold", 8)
                canvas_obj.drawCentredString(x_pos + col_w / 2, bar_y + bar_h / 2, receipt_display or "SIN RECIBO")

            canvas_obj.setFont("Courier", 5.4)
            canvas_obj.drawCentredString(x_pos + col_w / 2, bar_y - 2.5 * mm, receipt_display)

            incident_y = card_y
            canvas_obj.setFillColorRGB(0.95, 0.95, 0.95)
            canvas_obj.rect(x_pos + 0.4 * mm, incident_y + 0.4 * mm, col_w - 0.8 * mm, incident_h, fill=1, stroke=0)
            canvas_obj.setStrokeColorRGB(0, 0, 0)
            canvas_obj.setLineWidth(0.55)
            canvas_obj.line(x_pos, incident_y + incident_h, x_pos + col_w, incident_y + incident_h)

            canvas_obj.setFillColorRGB(0, 0, 0)
            canvas_obj.setFont("Helvetica-Bold", 5.2)
            canvas_obj.drawString(content_x, incident_y + incident_h - 4.2 * mm, "INCIDENCIAS")
            _draw_checkbox(canvas_obj, content_x, incident_y + incident_h - 8.7 * mm, "No cerro")
            _draw_checkbox(canvas_obj, content_x, incident_y + incident_h - 13.9 * mm, "Qty/0 Se recorre")
            _draw_checkbox(canvas_obj, content_x, incident_y + incident_h - 19.1 * mm, "Otro")

        hu_display_map = hu_display_map or {}
        items    = list(receipts.items())
        cur_col  = 0
        cur_row  = 0
        page_num = 1

        y_content_start = page_h - header_h - margin
        y_content_end   = footer_h + margin

        def _cell_y(row_idx: int) -> float:
            return y_content_start - row_idx * row_h

        rows_per_page = max(1, int((y_content_start - y_content_end) / row_h))
        cards_per_page = max(1, rows_per_page * cls.COLS)
        total_pages = max(1, (len(items) + cards_per_page - 1) // cards_per_page)

        _draw_receipt_header(c, page_num, total_pages)

        for idx, (hu_code, receipt_id) in enumerate(items):

            if cur_row >= rows_per_page:
                _draw_receipt_footer(c, page_num)
                c.showPage()
                page_num += 1
                cur_row = 0
                cur_col = 0
                _draw_receipt_header(c, page_num, total_pages)

            x   = margin + cur_col * (col_w + gutter)
            y   = _cell_y(cur_row)
            bot = y - row_h

            _draw_receipt_card(c, idx, hu_code, receipt_id, x, y)
            cur_col += 1
            if cur_col >= cls.COLS:
                cur_col = 0
                cur_row += 1
            continue

            if idx % 2 == 0:
                r, g, b = _hex_to_rgb(PDF_C.PAGE_BG_ALT)
            else:
                r, g, b = _hex_to_rgb(PDF_C.PAGE_BG)
            c.setFillColorRGB(r, g, b)
            c.rect(x, bot + 1 * mm, col_w, row_h - 1 * mm, fill=1, stroke=0)

            r, g, b = _hex_to_rgb(PDF_C.CARD_BDR)
            c.setStrokeColorRGB(r, g, b)
            c.setLineWidth(0.4)
            c.rect(x, bot + 1 * mm, col_w, row_h - 1 * mm, fill=0, stroke=1)

            r, g, b = _hex_to_rgb(PDF_C.MUTED)
            c.setFillColorRGB(r, g, b)
            c.setFont("Helvetica-Bold", 7)
            c.drawString(x + 1.5 * mm, y - 4 * mm, f"#{idx + 1}")

            cb = 3.5 * mm
            r, g, b = _hex_to_rgb(PDF_C.LIGHT)
            c.setStrokeColorRGB(r, g, b)
            c.rect(x + col_w - cb - 1.5 * mm, y - 5 * mm, cb, cb, fill=0, stroke=1)

            inner_padding = 3 * mm
            inner_x = x + inner_padding
            inner_w = col_w - inner_padding * 2
            inner_y = bot + 4 * mm
            inner_h = row_h - 14 * mm
            c.setStrokeColorRGB(*_hex_to_rgb(PDF_C.CARD_BDR))
            c.setLineWidth(0.35)
            c.rect(inner_x, inner_y, inner_w, inner_h, fill=0, stroke=1)

            content_center_x = inner_x + inner_w / 2
            content_top_y    = inner_y + inner_h - 4 * mm

            r, g, b = _hex_to_rgb(PDF_C.TEXT)
            c.setFillColorRGB(r, g, b)
            c.setFont("Courier-Bold", 9)
            hu_for_print = hu_display_map.get(hu_code, hu_code)
            hu_display = hu_for_print if len(hu_for_print) <= 22 else hu_for_print[:20] + "..."
            c.drawCentredString(content_center_x, content_top_y, f"HU: {hu_display}")

            r, g, b = _hex_to_rgb(PDF_C.MUTED)
            c.setFillColorRGB(r, g, b)
            c.setFont("Helvetica", 7.5)
            rec_display = receipt_id if len(receipt_id) <= 24 else receipt_id[:22] + "…"
            c.drawCentredString(content_center_x, content_top_y - 6 * mm, f"Rec: {rec_display}")

            barcode_drawn = False
            bar_h = 13 * mm
            try:
                import barcode as bc_lib
                from barcode.writer import ImageWriter
                from reportlab.lib.utils import ImageReader

                buf = BytesIO()
                code128 = bc_lib.get("code128", receipt_id, writer=ImageWriter())
                code128.write(buf, options={
                    "module_width":  0.35,
                    "module_height": 9.0,
                    "quiet_zone":    1.5,
                    "font_size":     0,
                    "text_distance": 1.0,
                    "write_text":    False,
                    "background":    "white",
                    "foreground":    "black",
                })
                buf.seek(0)

                bar_w = inner_w - 4 * mm
                bar_x = inner_x + (inner_w - bar_w) / 2
                bar_y = inner_y + 2 * mm

                c.drawImage(
                    ImageReader(buf), bar_x, bar_y,
                    width=bar_w, height=bar_h,
                    preserveAspectRatio=False, mask="auto",
                )
                barcode_drawn = True

            except ImportError:
                log.warning("python-barcode no instalado — omitiendo barcode para %s", hu_code)
            except Exception as e:
                log.warning("barcode_error hu=%s error=%s", hu_code, e)

            if not barcode_drawn:
                r, g, b = _hex_to_rgb(PDF_C.BLUE_HOVER)
                c.setFillColorRGB(r, g, b)
                c.setFont("Courier-Bold", 7)
                c.drawCentredString(content_center_x, inner_y + bar_h / 2, receipt_id)

            cur_col += 1
            if cur_col >= cls.COLS:
                cur_col = 0
                cur_row += 1

        _draw_receipt_footer(c, page_num)
        c.save()

        elapsed_ms = int((time.perf_counter() - started_at) * 1000)
        log.info(
            "pdf_generated path=%s pages=%d duration_ms=%d",
            output_path,
            page_num,
            elapsed_ms,
        )
        return output_path

    @classmethod
    def print_pdf(cls, pdf_path: str, printer_name: str | None = None) -> bool:
        """
        Imprime el PDF usando SumatraPDF en modo silencioso.

        Orden de resolución de impresora:
          1. Argumento `printer_name` (explícito)
          2. Config del usuario actual (~/.myapp/user_config.ini)
          3. Impresora por defecto del sistema

        Args:
            pdf_path     : ruta absoluta del PDF a imprimir.
            printer_name : nombre de la impresora (opcional).

        Retorna:
            True si SumatraPDF arrancó correctamente.
        """
        # Resolver impresora
        if printer_name is None:
            printer_name = get_receipt_printer()

        print_started_at = time.perf_counter()
        log.info(
            "pdf_print user=%s path=%s printer=%s",
            os.getlogin(), pdf_path, printer_name or "system_default",
        )

        with _print_lock:
            try:
                if sys.platform == "win32":

                    if not os.path.isfile(pdf_path):
                        log.error("print_aborted — pdf not found: %s", pdf_path)
                        return False

                    sumatra_exe = find_sumatra()
                    if sumatra_exe is None:
                        log.error(
                            "print_aborted — SumatraPDF no encontrado. "
                            "Descárgalo en https://www.sumatrapdfreader.org"
                        )
                        return False

                    if printer_name:
                        cmd = [sumatra_exe, "-print-to", printer_name, "-silent", pdf_path]
                    else:
                        cmd = [sumatra_exe, "-print-to-default", "-silent", pdf_path]

                    log.info("sumatra_cmd=%s", cmd)

                    sumatra_started_at = time.perf_counter()
                    proc = subprocess.Popen(
                        cmd,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.PIPE,
                    )

                    try:
                        _, stderr_bytes = proc.communicate(timeout=15)
                        sumatra_ms = int((time.perf_counter() - sumatra_started_at) * 1000)
                        stderr_text = stderr_bytes.decode("utf-8", errors="replace").strip()

                        # SumatraPDF puede retornar != 0 en éxito según la versión
                        if proc.returncode != 0:
                            log.warning(
                                "sumatra_returncode=%d stderr=%s — "
                                "puede ser normal en esta versión",
                                proc.returncode, stderr_text or "(vacío)",
                            )
                        else:
                            log.info("sumatra_print_ok returncode=0")

                        if stderr_text and any(
                            kw in stderr_text.lower()
                            for kw in ("error", "failed", "cannot", "invalid", "not found")
                        ):
                            log.error("sumatra_stderr_error stderr=%s", stderr_text)
                            return False

                        spool_started_at = time.perf_counter()
                        spool_ok = cls._wait_for_print_job_to_finish(
                            pdf_path,
                            printer_name=printer_name,
                            timeout=60,
                        )
                        spool_ms = int((time.perf_counter() - spool_started_at) * 1000)
                        total_ms = int((time.perf_counter() - print_started_at) * 1000)
                        log.info(
                            "pdf_print_done ok=%s total_ms=%d sumatra_ms=%d spool_ms=%d",
                            spool_ok,
                            total_ms,
                            sumatra_ms,
                            spool_ms,
                        )
                        return spool_ok

                    except subprocess.TimeoutExpired:
                        proc.kill()
                        log.error("sumatra_timeout — proceso terminado path=%s", pdf_path)
                        return False

                elif sys.platform == "darwin":
                    subprocess.Popen(
                        ["osascript", "-e",
                         f'tell app "Preview" to print POSIX file "{pdf_path}" with dialog']
                    )
                    log.info("pdf_print_via_osascript")
                    return True

                else:
                    subprocess.Popen(["xdg-open", pdf_path])
                    log.info("pdf_opened_via_xdg")
                    return True

            except FileNotFoundError:
                log.error("sumatra_exec_not_found")
                return False
            except Exception as e:
                log.error("pdf_print_error path=%s error=%s", pdf_path, e)
                return False

    @staticmethod
    def _wait_for_print_job_to_finish(
        pdf_path: str,
        printer_name: str | None = None,
        timeout: float = 60,
    ) -> bool:
        """
        Espera hasta que el spooler de Windows ya no tenga este PDF en cola.
        If the job disappears too quickly to observe, Sumatra's successful exit is
        treated as confirmation that the job was handed to the spooler.
        """
        if sys.platform != "win32":
            return True

        started_at = time.perf_counter()
        try:
            import win32print
        except Exception as e:
            log.warning("print_spool_wait_unavailable error=%s", e)
            return True

        try:
            resolved_printer = printer_name or win32print.GetDefaultPrinter()
            document_name = os.path.basename(pdf_path).lower()
            deadline = datetime.now().timestamp() + timeout
            seen_job = False

            while datetime.now().timestamp() < deadline:
                handle = win32print.OpenPrinter(resolved_printer)
                try:
                    jobs = win32print.EnumJobs(handle, 0, 99, 1)
                finally:
                    win32print.ClosePrinter(handle)

                matching = [
                    job for job in jobs
                    if document_name in str(job.get('pDocument', '')).lower()
                    or document_name in str(job.get('pUserName', '')).lower()
                ]

                if matching:
                    seen_job = True
                    time.sleep(0.5)
                    continue

                if seen_job:
                    elapsed_ms = int((time.perf_counter() - started_at) * 1000)
                    log.info(
                        "print_spool_job_finished printer=%s pdf=%s duration_ms=%d",
                        resolved_printer,
                        pdf_path,
                        elapsed_ms,
                    )
                    return True

                remaining = deadline - datetime.now().timestamp()

                if remaining > 57:
                    time.sleep(0.5)
                    continue

                elapsed_ms = int((time.perf_counter() - started_at) * 1000)
                log.info(
                    "print_spool_job_not_observed printer=%s pdf=%s duration_ms=%d",
                    resolved_printer,
                    pdf_path,
                    elapsed_ms,
                )
                return True

            elapsed_ms = int((time.perf_counter() - started_at) * 1000)
            log.error(
                "print_spool_timeout printer=%s pdf=%s duration_ms=%d",
                resolved_printer,
                pdf_path,
                elapsed_ms,
            )
            return False
        except Exception as e:
            log.warning("print_spool_wait_error path=%s error=%s", pdf_path, e)
            return True

