"""
core/hu_origins.py
Detecta el origen y comportamiento de pallet de una HU
basado en su prefijo.

Reglas de negocio:
  T100...  → China (THA/CNA)  — multi-HU por pallet, separador manual
  C10...   → Atlanta (ATL)    — híbrido 1-10+ HUs, separador manual
  29...    → Italia (ITA)     — siempre 1 HU por pallet (auto-pallet)
  otros    → desconocido       — separador manual como fallback
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Origin:
    code:        str    # ej: "THA", "ITA", "ATL"
    label:       str    # ej: "China (THA/CNA)"
    auto_pallet: bool   # True = cada HU es su propio pallet automáticamente
    color:       str    # color hex para la UI
    phase2_wait: float  # segundos de espera extra en ZMMTIJSEP para este origen
    wait_long:   float  # espera larga en SAP (para navegación)
    wait_short:  float  # espera corta en SAP
    wait_tree:   float  # espera extra para que cargue el árbol interno
    wait_sp01_refresh: float  # segundos entre recargas de spools


# Tabla de orígenes por prefijo — orden importa (más específico primero)
# Orígenes rápidos (THA, CNA): tiempos menores
# Orígenes lentos (ITA, BRA/ATL, FHR): tiempos mayores
ORIGIN_RULES: list[tuple[str, Origin]] = [
    ("TH", Origin("THA", "Tailandia (THA)", auto_pallet=False, color="#BDD7EE", 
                    phase2_wait=0.5, wait_long=0.5, wait_short=0.3, wait_tree=1.0, wait_sp01_refresh=1.5)),
    ("T", Origin("CNA", "China (CNA)", auto_pallet=False, color="#BDD7EE", 
                    phase2_wait=1.0, wait_long=0.5, wait_short=0.5, wait_tree=1.0, wait_sp01_refresh=1.5)),
    ("C10",  Origin("BRA/ATL", "ATL, (BRA/ATL)", auto_pallet=False, color="#C6EFCE", 
                    phase2_wait=1.5, wait_long=1.4, wait_short=1.4, wait_tree=4.5, wait_sp01_refresh=4.5)),
    ("29",   Origin("ITA", "Italia (ITA)", auto_pallet=True, color="#FFEB9C", 
                    phase2_wait=1.5, wait_long=1.6, wait_short=1.6, wait_tree=4.5, wait_sp01_refresh=4.0)),
    ("ELPS", Origin("FHR", "Foothill Ranch (ELPS)", auto_pallet=True, color="#F4B084", 
                    phase2_wait=1.5, wait_long=1.6, wait_short=1.6, wait_tree=4.5, wait_sp01_refresh=4.0)),
]
UNKNOWN_ORIGIN = Origin("UNK", "Desconocido", auto_pallet=False, color="#E2EFDA", 
                        phase2_wait=2.0, wait_long=1.0, wait_short=1.0, wait_tree=3.0, wait_sp01_refresh=3.0)

# Código especial de separador de pallet (case-insensitive)
PALLET_SEPARATOR_CODE = "PALLET"


def detect_origin(hu_code: str) -> Origin:
    """
    Determina el origen de una HU por su prefijo.
    Retorna UNKNOWN_ORIGIN si no hay coincidencia.
    """
    code = hu_code.strip().upper()
    for prefix, origin in ORIGIN_RULES:
        if code.startswith(prefix.upper()):
            return origin
    return UNKNOWN_ORIGIN


def is_pallet_separator(scanned_value: str) -> bool:
    """
    True si el valor escaneado es el código especial de separador de pallet.
    Case-insensitive, ignora espacios.
    """
    return scanned_value.strip().upper() == PALLET_SEPARATOR_CODE.upper()


def needs_auto_pallet(hu_code: str) -> bool:
    """
    True si esta HU debe crear su propio pallet automáticamente
    (sin necesitar código separador).
    Aplica a Italia (prefijo 29).
    """
    return detect_origin(hu_code).auto_pallet