import win32com.client
import time


def test_ze16_single_hu(session, hu_code: str, plant: str = "MX03") -> str | None:
    """
    Test: consulta ZE16 para un solo HU y retorna su Receipt ID.
    """
    try:
        # Abrir transacción
        session.findById("wnd[0]/tbar[0]/okcd").text = "ZE16"
        session.findById("wnd[0]").sendVKey(0)
        session.findById("wnd[0]/usr/ctxtDATABROWSE-TABLENAME").text = "ZSDO_TRF_GD_STEP"
        session.findById("wnd[0]").sendVKey(8)  # confirmar tabla
        session.findById("wnd[0]").sendVKey(0)  # enter

        # Filtros
        session.findById("wnd[0]/usr/ctxtI1-LOW").text = plant
        session.findById("wnd[0]/usr/txtI6-LOW").text = hu_code

        # Ejecutar
        session.findById("wnd[0]").sendVKey(8)  # F8

        # Leer resultado
        grid = session.findById("wnd[0]/usr/cntlGRID1/shellcont/shell")

        if grid.rowCount == 0:
            print(f"[ZE16] No rows for HU={hu_code}")
            return None

        receipt = grid.getCellValue(0, "ZREF_NUMB").strip()
        print(f"[ZE16] HU={hu_code} → Receipt={receipt}")
        return receipt

    except Exception as e:
        print(f"[ZE16] Error: {e}")
        return None


def get_receipts_for_pallet(session, hu_codes: list[str], plant: str = "MX03") -> dict[str, str]:
    """
    Consulta ZE16 para todos los HUs de un pallet en una sola ejecución.
    Retorna: { "TH0000267998": "00000069693553", ... }
    """
    try:
        # Abrir transacción
        session.findById("wnd[0]/tbar[0]/okcd").text = "ZE16"
        session.findById("wnd[0]").sendVKey(0)
        session.findById("wnd[0]").sendVKey(0)

        # Plant + primer HU en el campo simple
        session.findById("wnd[0]/usr/ctxtI1-LOW").text = plant
        session.findById("wnd[0]/usr/txtI6-LOW").text = hu_codes[0]

        # Abrir Multiple Selection
        session.findById("wnd[0]/usr/btn%_I6_%_APP_%-VALU_PUSH").press()

        # El primer HU ya está en fila 0 (lo puso el campo simple)
        # Pegar el resto desde la fila 1
        for i, hu in enumerate(hu_codes[1:], start=1):
            session.findById(
                f"wnd[1]/usr/tabsTAB_STRIP/tabpSIVA/ssubSCREEN_HEADER:SAPLALDB:3010/"
                f"tblSAPLALDBSINGLE/txtRSCSEL_255-SLOW_I[1,{i}]"
            ).text = hu

        # Confirmar popup con F8
        session.findById("wnd[1]").sendVKey(8)

        # Limpiar MAX_SEL para traer todas las filas
        session.findById("wnd[0]/usr/txtMAX_SEL").text = "0"

        # Ejecutar F8
        session.findById("wnd[0]").sendVKey(8)

        time.sleep(1)

        # Leer grid — columna HU es ZHU (no HU_NUMBER)
        grid = session.findById("wnd[0]/usr/cntlGRID1/shellcont/shell")
        row_count = grid.rowCount
        print(f"[ZE16] rowCount={row_count}")

        receipts = {}
        for i in range(row_count):
            hu = grid.getCellValue(i, "ZHU").strip()
            receipt = grid.getCellValue(i, "ZREF_NUMB").strip()
            if hu and receipt:
                receipts[hu] = receipt
                print(f"[ZE16]   {hu} → {receipt}")

        print(f"[ZE16] {len(receipts)}/{len(hu_codes)} receipts encontrados")
        return receipts

    except Exception as e:
        print(f"[ZE16] Error: {e}")
        return {}


def main():
    sap_gui = win32com.client.GetObject("SAPGUI")
    app = sap_gui.GetScriptingEngine
    conn = app.Children(0)
    session = conn.Children(0)

    # Test múltiples HUs (Multiple Selection)
    hus_pallet = [
        "TH0000267998",
        "TH0000268036",
        "TH0000267997",
    ]

    result = get_receipts_for_pallet(session, hus_pallet)
    print(f"\nResultado final: {result}")


if __name__ == "__main__":
    main()
