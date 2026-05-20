
import logging
import os
import time

log = logging.getLogger(__name__)

# ── Constantes de navegación

TX_ZE16         = "ZE16"
TABLE_NAME      = "ZSDO_TRF_GD_STEP"
GRID_PATH       = "wnd[0]/usr/cntlGRID1/shellcont/shell"
FIELD_PLANT     = "wnd[0]/usr/ctxtI1-LOW"
FIELD_HU_SINGLE = "wnd[0]/usr/txtI6-LOW"
FIELD_MAX_SEL   = "wnd[0]/usr/txtMAX_SEL"
BTN_MULTISEL    = "wnd[0]/usr/btn%_I6_%_APP_%-VALU_PUSH"

MULTISEL_TABLE = (
    "wnd[1]/usr/tabsTAB_STRIP/tabpSIVA/ssubSCREEN_HEADER:SAPLALDB:3010/"
    "tblSAPLALDBSINGLE"
)
MULTISEL_FIELD_TPL = (
    "wnd[1]/usr/tabsTAB_STRIP/tabpSIVA/ssubSCREEN_HEADER:SAPLALDB:3010/"
    "tblSAPLALDBSINGLE/txtRSCSEL_255-SLOW_I[1,{row}]"
)

COL_HU      = "ZHU"
COL_RECEIPT = "ZREF_NUMB"

# Filas visibles del popup de Multiple Selection
VISIBLE_ROWS = 8

# VKey 13 = Shift+F1 en SAP GUI inserta nueva línea en la tabla
VKEY_INSERT_ROW = 13

# ── Constantes 

# Tiempo de espera tras sendVKey 13 para que SAP genere la nueva fila
VKEY_WAIT        = 0.15   # segundos — ajustar si SAP es lento
FIELD_RETRY_ATTEMPTS  = 4
FIELD_RETRY_INTERVAL  = 0.08   # segundos entre reintentos de campo

DEFAULT_PLANT   = os.getenv("SAP_PLANT", "MX03")
WAIT_AFTER_EXEC = float(os.getenv("SAP_ZE16_WAIT", "1.5"))


class ZE16Error(Exception):
    """Error controlado en la consulta ZE16."""


class ZE16Client:
    """
    Consulta ZE16 usando la sesión SAP ya abierta.
    No abre una conexión nueva — recibe la sesión del SAPClient.
    """

    def __init__(self, session):
        self._session = session

    # ── Helpers privados ──────────────────────────────────────────────────────

    def _find(self, element_id: str):
        """Wrapper seguro sobre findById; retorna None si el elemento no existe."""
        try:
            return self._session.findById(element_id)
        except Exception:
            return None

    def _send_vkey(self, wnd_id: str, key: int) -> bool:
        wnd = self._find(wnd_id)
        if wnd:
            wnd.sendVKey(key)
            return True
        log.warning("send_vkey_window_not_found wnd=%s key=%d", wnd_id, key)
        return False

    def _normalize_hu(self, hu: str) -> str:
        normalized = hu.strip().upper()
        numeric = normalized.lstrip("0")

        if numeric.startswith("29") and numeric.isdigit():
            return numeric.zfill(20)

        return normalized

    # ── Resiliencia de campo ──────────────────────────────────────────────────

    def _get_field_with_retry(
        self,
        row: int,
        context: str,
        ingresados: int,
        total: int,
    ):
        """
        Obtiene un campo del popup con reintentos.

        Separa la falla de "campo no visible aún" de cualquier otro error,
        dando a SAP tiempo suficiente para renderizar tras un VKey.

        Args:
            row        : índice de fila en la tabla MULTISEL.
            context    : descripción del contexto para el log (ej. "inicial" o "insert_row").
            ingresados : HUs ya escritos exitosamente (para el mensaje de error).
            total      : total de HUs a ingresar.

        Retorna:
            El elemento SAP GUI del campo.

        Raises:
            ZE16Error: si el campo no aparece tras todos los reintentos.
        """
        for attempt in range(FIELD_RETRY_ATTEMPTS):
            field = self._find(MULTISEL_FIELD_TPL.format(row=row))
            if field is not None:
                if attempt > 0:
                    log.debug(
                        "field_found_after_retry row=%d context=%s attempt=%d",
                        row, context, attempt,
                    )
                return field
            time.sleep(FIELD_RETRY_INTERVAL)

        raise ZE16Error(
            f"Campo row={row} no encontrado tras {FIELD_RETRY_ATTEMPTS} intentos "
            f"[context={context}] (ingresados {ingresados}/{total})"
        )

    # ── Navegación ────────────────────────────────────────────────────────────

    def _open_ze16(self) -> None:
        """
        Navega a ZE16 y selecciona la tabla ZSDO_TRF_GD_STEP.

        Secuencia:
          /nZE16 → Enter → escribir nombre de tabla → F8 → Enter

        Antes de navegar cierra cualquier popup activo (F12) para evitar
        que la sesión quede en un estado inesperado.
        """
        log.debug("ze16_open start")

        if self._find("wnd[1]"):
            log.debug("ze16_open closing unexpected popup")
            self._send_vkey("wnd[1]", 12)

        okcd = self._find("wnd[0]/tbar[0]/okcd")
        if okcd is None:
            raise ZE16Error(
                "No se encontró la barra de comandos SAP (okcd). "
                "Verifica que la sesión esté activa."
            )
        okcd.text = f"/n{TX_ZE16}"
        self._send_vkey("wnd[0]", 0)

        table_field = self._find("wnd[0]/usr/ctxtDATABROWSE-TABLENAME")
        if table_field:
            table_field.text = TABLE_NAME
            self._send_vkey("wnd[0]", 8)
        else:
            log.warning(
                "ze16_open table_field_not_found — la transacción puede "
                "haber abierto directamente en el formulario de selección"
            )

        self._send_vkey("wnd[0]", 0)
        log.debug("ze16_open done")

    # ── Llenado de Multiple Selection ─────────────────────────────────────────

    def _fill_multisel(self, hu_codes: list[str]) -> None:
        """
        Rellena el popup de Multiple Selection usando inserción de filas.

          Bloque inicial — filas 0 a 7:
            Se escriben directamente por índice de fila, igual que el VBA.

          Bloque de inserción — HU 9 en adelante:
            Por cada HU adicional:
              1. sendVKey 13 (Shift+F1) → SAP inserta una nueva fila al inicio
                 y desplaza las existentes hacia abajo.
              2. Espera activa hasta que row[0] esté disponible.
              3. Se escribe el HU en row[0].
            El último HU NO lleva sendVKey 13 posterior — el F8 de confirmación
            cierra el popup directamente.

        Esta estrategia evita completamente el scroll programático y el
        portapapeles, replicando exactamente el comportamiento del grabador SAP.

        Args:
            hu_codes: lista completa de HU codes a ingresar (ya normalizados).

        Raises:
            ZE16Error: si algún campo no se encuentra o VKey falla,
                       indicando siempre cuántos HUs se lograron ingresar.
        """
        total      = len(hu_codes)
        ingresados = 0

        # ── Bloque inicial: filas 0–7 ─────────────────────────────────────────
        for row_idx, hu in enumerate(hu_codes[:VISIBLE_ROWS]):
            field = self._get_field_with_retry(
                row=row_idx,
                context="inicial",
                ingresados=ingresados,
                total=total,
            )
            field.text = hu
            ingresados += 1
            log.debug("multisel_inicial row=%d hu=%s", row_idx, hu)

        if total <= VISIBLE_ROWS:
            log.info(
                "multisel_fill_done ingresados=%d/%d (sin inserción necesaria)",
                ingresados, total,
            )
            return

        # ── Bloque inserción: HU 9 en adelante ───────────────────────────────
        #
        # Flujo por cada HU adicional:
        #   sendVKey 13 → nueva fila en row[0] → escribir HU en row[0]
        #
        # El último HU de la lista NO lleva sendVKey 13 al final,
        # igual que en el VBA de referencia.
        #
        remaining = hu_codes[VISIBLE_ROWS:]

        for extra_idx, hu in enumerate(remaining):
            is_last = (extra_idx == len(remaining) - 1)

            # 1. Insertar nueva fila (Shift+F1)
            ok = self._send_vkey("wnd[1]", VKEY_INSERT_ROW)
            if not ok:
                raise ZE16Error(
                    f"No se pudo enviar VKey {VKEY_INSERT_ROW} (insert row) "
                    f"(ingresados {ingresados}/{total})"
                )

            # 2. Espera activa: verificar que row[0] esté disponible
            field = self._get_field_with_retry(
                row=0,
                context=f"insert_row extra_idx={extra_idx}",
                ingresados=ingresados,
                total=total,
            )

            # 3. Escribir HU en la nueva fila
            field.text = hu
            ingresados += 1
            log.debug(
                "multisel_insert extra_idx=%d is_last=%s hu=%s",
                extra_idx, is_last, hu,
            )

        log.info("multisel_fill_done ingresados=%d/%d", ingresados, total)

    # ── Lectura del grid de resultados ────────────────────────────────────────

    def _read_grid(self, hu_filter: set[str] | None = None) -> dict[str, str]:
        """
        Lee el grid de resultados y retorna {hu_code_normalizado: receipt_id}.
        """
        grid = self._find(GRID_PATH)
        if grid is None:
            log.warning("ze16_grid_not_found path=%s", GRID_PATH)
            return {}

        try:
            row_count = int(grid.rowCount)
        except Exception as e:
            log.warning("ze16_rowCount_failed error=%s", e)
            return {}

        log.info("ze16_grid_rowCount=%d", row_count)

        receipts: dict[str, str] = {}
        for i in range(row_count):
            try:
                hu_raw  = str(grid.getCellValue(i, COL_HU)).strip()
                receipt = str(grid.getCellValue(i, COL_RECEIPT)).strip()

                if not hu_raw or not receipt:
                    continue

                hu_norm = self._normalize_hu(hu_raw)

                if hu_filter is not None and hu_norm not in hu_filter:
                    continue

                receipts[hu_norm] = receipt.lstrip("0") or "0"
                log.debug("ze16_row_%d hu=%s receipt=%s", i, hu_norm, receipts[hu_norm])

            except Exception as e:
                log.debug("ze16_row_%d read_error=%s", i, e)

        return receipts

    # ── API pública ───────────────────────────────────────────────────────────

    def get_receipts_for_pallet(
        self,
        hu_codes: list[str],
        plant: str = DEFAULT_PLANT,
        wait_after_exec: float = WAIT_AFTER_EXEC,
    ) -> dict[str, str]:
        """
        Consulta ZE16 para todos los HUs de un pallet en una sola ejecución.

        Utiliza Multiple Selection con inserción de filas (Shift+F1 / VKey 13)
        para HUs más allá de los 8 visibles — sin scroll, sin portapapeles.

        Args:
            hu_codes       : lista de HU codes del pallet (al menos 1).
            plant          : código de planta SAP.
            wait_after_exec: segundos de espera tras F8 para carga del grid.

        Retorna:
            dict { hu_code_normalizado: receipt_id } con los HUs encontrados.
            Los HUs sin resultado no aparecen en el dict.

        Raises:
            ZE16Error: si ocurre un error irrecuperable de navegación o llenado.
        """
        if not hu_codes:
            return {}

        hu_codes_norm = [self._normalize_hu(hu) for hu in hu_codes]
        hu_filter     = set(hu_codes_norm)

        log.info("ze16_pallet_start hu_count=%d plant=%s", len(hu_codes_norm), plant)

        try:
            self._open_ze16()

            # ── Planta ───────────────────────────────────────────────────────
            plant_field = self._find(FIELD_PLANT)
            if plant_field:
                plant_field.text = plant
            else:
                log.warning("ze16_plant_field_not_found — continuando sin filtro de planta")

            # ── Un solo HU: flujo simple sin popup ───────────────────────────
            if len(hu_codes_norm) == 1:
                hu_field = self._find(FIELD_HU_SINGLE)
                if hu_field is None:
                    raise ZE16Error("Campo HU (I6-LOW) no encontrado en ZE16")
                hu_field.text = hu_codes_norm[0]

            # ── Múltiples HUs: Multiple Selection con inserción de filas ──────
            else:
                btn = self._find(BTN_MULTISEL)
                if btn is None:
                    raise ZE16Error("Botón Multiple Selection (I6) no encontrado")
                btn.press()
                time.sleep(0.3)   # esperar apertura del popup

                self._fill_multisel(hu_codes_norm)

                # F8 confirma el popup de Multiple Selection
                self._send_vkey("wnd[1]", 8)
                time.sleep(0.2)

            # ── Quitar límite de filas de resultado ───────────────────────────
            max_sel = self._find(FIELD_MAX_SEL)
            if max_sel:
                max_sel.text = "0"

            # ── Ejecutar consulta (F8) ────────────────────────────────────────
            self._send_vkey("wnd[0]", 8)
            time.sleep(wait_after_exec)

            # ── Leer resultados del grid ──────────────────────────────────────
            receipts = self._read_grid(hu_filter=hu_filter)

            missing = [hu for hu in hu_codes_norm if hu not in receipts]
            if missing:
                log.warning(
                    "ze16_missing_receipts count=%d hus=%s",
                    len(missing), missing,
                )

            log.info(
                "ze16_pallet_done found=%d/%d receipts=%s",
                len(receipts), len(hu_codes_norm), receipts,
            )
            return receipts

        except ZE16Error:
            raise
        except Exception as e:
            log.error("ze16_pallet_error error=%s", e, exc_info=True)
            raise ZE16Error(f"Error inesperado en ZE16: {e}") from e

    def get_receipt_single_hu(
        self,
        hu_code: str,
        plant: str = DEFAULT_PLANT,
        wait_after_exec: float = WAIT_AFTER_EXEC,
    ) -> str | None:
        """
        Consulta ZE16 para un solo HU.
        Útil como fallback o para depuración individual.

        Retorna:
            receipt_id string sin ceros a la izquierda, o None si no se encontró.
        """
        hu_norm = self._normalize_hu(hu_code)
        try:
            results = self.get_receipts_for_pallet(
                [hu_norm], plant=plant, wait_after_exec=wait_after_exec
            )
            receipt = results.get(hu_norm)
            log.info("ze16_single_hu hu=%s receipt=%s", hu_norm, receipt)
            return receipt
        except ZE16Error as e:
            log.error("ze16_single_hu_error hu=%s error=%s", hu_norm, e)
            return None
