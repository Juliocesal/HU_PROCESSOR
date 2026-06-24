"""Utilidades livianas para medir el flujo operativo sin alterar su ejecucion."""

import logging
import time


def log_performance(
    logger: logging.Logger,
    stage: str,
    started_at: float,
    **context,
) -> float:
    """Registra una duracion monotónica en milisegundos y la devuelve."""
    duration_ms = round((time.perf_counter() - started_at) * 1000, 1)
    context_text = " ".join(
        f"{key}={value}" for key, value in context.items() if value is not None
    )
    logger.info(
        "perf stage=%s duration_ms=%.1f%s",
        stage,
        duration_ms,
        f" {context_text}" if context_text else "",
    )
    return duration_ms
