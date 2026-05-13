from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas
from reportlab.lib import colors
import barcode
from barcode.writer import ImageWriter
from io import BytesIO
import os
import sys
import tempfile


def generar_pdf_receipts(
    pallet_id: int,
    origin_label: str,
    receipts: dict[str, str],   # { hu_code: receipt_id }
    output_path: str,
) -> str:
    """
    Genera PDF con grid 3 columnas.
    Cada celda: HU + Receipt ID + código de barras.
    Retorna el path del PDF generado.
    """
    c = canvas.Canvas(output_path, pagesize=A4)
    page_w, page_h = A4

    margin      = 8  * mm
    cols        = 3
    col_w       = (page_w - margin * (cols + 1)) / cols
    row_h       = 38 * mm
    header_h    = 18 * mm

    # ── Encabezado ────────────────────────────────────────────────
    c.setFillColor(colors.HexColor("#2C3E50"))
    c.rect(0, page_h - header_h, page_w, header_h, fill=1, stroke=0)

    c.setFillColor(colors.white)
    c.setFont("Helvetica-Bold", 13)
    c.drawCentredString(page_w / 2, page_h - 10 * mm,
                        f"Pallet {pallet_id} — {origin_label}")

    c.setFont("Helvetica", 8)
    from datetime import datetime
    fecha = datetime.now().strftime("%d/%m/%Y %H:%M")
    c.drawCentredString(page_w / 2, page_h - 15 * mm,
                        f"{fecha}  |  Total HUs: {len(receipts)}")

    # ── Grid de celdas ────────────────────────────────────────────
    items    = list(receipts.items())
    y_start  = page_h - header_h - margin
    cur_col  = 0
    cur_row  = 0

    for idx, (hu_code, receipt_id) in enumerate(items):

        x = margin + cur_col * (col_w + margin)
        y = y_start - cur_row * row_h

        # Nueva página si no cabe
        if y - row_h < margin:
            c.showPage()
            y_start = page_h - margin
            cur_row = 0
            y       = y_start
            # Mini encabezado en páginas extra
            c.setFillColor(colors.HexColor("#ECF0F1"))
            c.rect(0, page_h - 10*mm, page_w, 10*mm, fill=1, stroke=0)
            c.setFillColor(colors.HexColor("#2C3E50"))
            c.setFont("Helvetica-Bold", 8)
            c.drawCentredString(page_w/2, page_h - 7*mm,
                                f"Pallet {pallet_id} — {origin_label} (cont.)")

        # Borde celda
        c.setStrokeColor(colors.HexColor("#CCCCCC"))
        c.setLineWidth(0.3)
        c.rect(x, y - row_h + 1*mm, col_w, row_h - 1*mm, fill=0)

        # Número de ítem
        c.setFillColor(colors.HexColor("#666666"))
        c.setFont("Helvetica-Bold", 7)
        c.drawString(x + 1*mm, y - 4*mm, f"#{idx + 1}")

        # Checkbox esquina derecha
        cb_size = 3.5 * mm
        c.setStrokeColor(colors.HexColor("#666666"))
        c.rect(x + col_w - cb_size - 1*mm, y - 4.5*mm, cb_size, cb_size, fill=0)

        # HU code
        c.setFillColor(colors.black)
        c.setFont("Helvetica-Bold", 8)
        c.drawString(x + 1*mm, y - 9*mm, f"HU:  {hu_code}")

        # Receipt ID
        c.setFillColor(colors.HexColor("#444444"))
        c.setFont("Helvetica", 8)
        c.drawString(x + 1*mm, y - 13*mm, f"Rec: {receipt_id}")

        # Código de barras del Receipt ID
        try:
            buf = BytesIO()
            code128 = barcode.get("code128", receipt_id, writer=ImageWriter())
            code128.write(buf, options={
                "module_width":  0.4,
                "module_height": 8.0,
                "quiet_zone":    1.0,
                "font_size":     0,
                "text_distance": 1.0,
                "write_text":    False,
            })
            buf.seek(0)

            bar_w = col_w - 4*mm
            bar_h = 12 * mm
            bar_x = x + (col_w - bar_w) / 2
            bar_y = y - row_h + 3*mm

            from reportlab.lib.utils import ImageReader
            c.drawImage(ImageReader(buf), bar_x, bar_y,
                        width=bar_w, height=bar_h,
                        preserveAspectRatio=False, mask="auto")
        except Exception as e:
            c.setFillColor(colors.red)
            c.setFont("Helvetica", 6)
            c.drawCentredString(x + col_w/2, y - row_h + 6*mm, "ERROR BARCODE")

        # Avanzar grid
        cur_col += 1
        if cur_col >= cols:
            cur_col = 0
            cur_row += 1

    # ── Pie de página ─────────────────────────────────────────────
    total_pages = c.getPageNumber()
    c.setStrokeColor(colors.HexColor("#DDDDDD"))
    c.setLineWidth(0.2)
    c.line(margin, 8*mm, page_w - margin, 8*mm)
    c.setFillColor(colors.HexColor("#888888"))
    c.setFont("Helvetica", 6)
    c.drawString(margin, 4*mm, f"Generado: {fecha}")
    c.drawRightString(page_w - margin, 4*mm, f"Pallet {pallet_id}")

    c.save()
    return output_path


def imprimir_pdf(pdf_path: str):
    """Abre el PDF en el visor por defecto del sistema (usuario puede imprimir con Ctrl+P)."""
    try:
        if sys.platform == "win32":
            try:
                os.startfile(pdf_path)
                print(f"[PDF] Abierto en visor por defecto: {pdf_path}")
            except OSError:
                import subprocess
                edge = r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"
                if os.path.exists(edge):
                    subprocess.Popen([edge, pdf_path])
                    print(f"[PDF] Abierto en Edge: {pdf_path}")
                else:
                    subprocess.Popen(["explorer", pdf_path])
                    print(f"[PDF] Abierto en Explorer: {pdf_path}")
        elif sys.platform == "darwin":
            import subprocess
            subprocess.Popen(["open", pdf_path])
            print(f"[PDF] Abierto en visor macOS: {pdf_path}")
        else:
            import subprocess
            subprocess.Popen(["xdg-open", pdf_path])
            print(f"[PDF] Abierto en visor Linux: {pdf_path}")
    except Exception as e:
        print(f"[PDF] Error al abrir: {e}")


# ── Test ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    receipts_test = {
        "TH0000267998": "00000069693553",
        "TH0000267997": "00000069693554",
        "TH0000268036": "00000069693562",
    }

    path = os.path.join(tempfile.gettempdir(), "pallet_1_THA.pdf")
    generar_pdf_receipts(
        pallet_id    = 1,
        origin_label = "Tailandia (THA)",
        receipts     = receipts_test,
        output_path  = path,
    )
    print(f"[PDF] Generado: {path}")

    # Descomentar para abrir en el visor (usuario puede imprimir con Ctrl+P):
    # imprimir_pdf(path)
