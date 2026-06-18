"""Reglas de origen para Handling Units (HUs) escaneadas.

El orden de ``ORIGIN_RULES`` es intencional: los prefijos mas especificos deben
evaluarse antes que los prefijos mas generales.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Origin:
    code: str
    label: str
    auto_pallet: bool
    color: str
    phase2_wait: float
    wait_long: float
    wait_short: float
    wait_tree: float
    wait_receipt_refresh: float


ORIGIN_RULES: list[tuple[str, Origin]] = [
    (
        "TH",
        Origin(
            "THA",
            "Tailandia (THA)",
            auto_pallet=False,
            color="#BDD7EE",
            phase2_wait=0.2,
            wait_long=0.2,
            wait_short=0.1,
            wait_tree=0.5,
            wait_receipt_refresh=0.4,
        ),
    ),
    (
        "T",
        Origin(
            "CNA",
            "China (CNA)",
            auto_pallet=False,
            color="#BDD7EE",
            phase2_wait=0.2,
            wait_long=0.2,
            wait_short=0.1,
            wait_tree=0.5,
            wait_receipt_refresh=0.4,
        ),
    ),
    (
        "C10",
        Origin(
            "BRA/ATL",
            "ATL, (BRA/ATL)",
            auto_pallet=False,
            color="#C6EFCE",
            phase2_wait=0.2,
            wait_long=0.2,
            wait_short=0.1,
            wait_tree=0.5,
            wait_receipt_refresh=0.4,
        ),
    ),
    (
        "29",
        Origin(
            "ITA",
            "Italia (ITA)",
            auto_pallet=True,
            color="#FFEB9C",
            phase2_wait=0.2,
            wait_long=0.2,
            wait_short=0.1,
            wait_tree=0.5,
            wait_receipt_refresh=0.4,
        ),
    ),
    (
        "ELPS",
        Origin(
            "FHR",
            "Foothill Ranch (ELPS)",
            auto_pallet=True,
            color="#F4B084",
            phase2_wait=0.2,
            wait_long=0.2,
            wait_short=0.1,
            wait_tree=0.5,
            wait_receipt_refresh=0.4,
        ),
    ),
]

UNKNOWN_ORIGIN = Origin(
    "UNK",
    "Desconocido",
    auto_pallet=False,
    color="#E2EFDA",
    phase2_wait=0.2,
    wait_long=0.2,
    wait_short=0.1,
    wait_tree=0.5,
    wait_receipt_refresh=0.4,
)

PALLET_SEPARATOR_CODE = "PALLET"
ATL_FAST_CODES = {"BRA/ATL"}


def detect_origin(hu_code: str) -> Origin:
    """Devuelve el origen configurado para una HU o ``UNKNOWN_ORIGIN``."""
    code = hu_code.strip().upper()
    for prefix, origin in ORIGIN_RULES:
        if code.startswith(prefix.upper()):
            return origin
    return UNKNOWN_ORIGIN


def is_pallet_separator(scanned_value: str) -> bool:
    """Devuelve True cuando el valor escaneado es el separador de pallet."""
    return scanned_value.strip().upper() == PALLET_SEPARATOR_CODE


def resolve_effective_origin(origin: Origin, pallet_size: int) -> Origin:
    """
    Devuelve el perfil de tiempos que debe usarse para ZE16/PDF del pallet.

    Los pallets BRA/ATL con mas de una HU usan tiempos de THA porque esas HUs
    son de una pieza y se comportan mas rapido en SAP.
    """
    if origin.code in ATL_FAST_CODES and pallet_size > 1:
        for _, candidate in ORIGIN_RULES:
            if candidate.code == "THA":
                return candidate
    return origin
