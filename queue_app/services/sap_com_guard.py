import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Callable

from django.conf import settings

log = logging.getLogger(__name__)

_global_lock = threading.Lock()
_global_metrics = {
    'aborted_runs': 0,
    'estimated_blocked_threads': 0,
}


def _max_consecutive_timeouts() -> int:
    try:
        value = int(getattr(settings, 'SAP_COM_MAX_CONSECUTIVE_TIMEOUTS', 2))
    except (TypeError, ValueError):
        value = 2
    return max(1, value)


def _cooldown_seconds() -> float:
    try:
        value = float(getattr(settings, 'SAP_COM_COOLDOWN_SECONDS', 30))
    except (TypeError, ValueError):
        value = 30.0
    return max(0.0, value)


def get_sap_com_global_metrics() -> dict:
    with _global_lock:
        return dict(_global_metrics)


def _update_global_metric(key: str, delta: int) -> None:
    with _global_lock:
        _global_metrics[key] = max(0, int(_global_metrics.get(key, 0)) + delta)


@dataclass
class SAPCOMCircuitBreaker:
    """
    Circuit breaker local de una corrida Celery.

    Hotfix deliberadamente simple: mientras SAP COM viva en threads, no podemos
    matar una llamada COM bloqueada. El breaker evita crear threads nuevos sin
    limite y deja documentado el punto que puede migrarse a proceso aislado.
    """

    status_callback: Callable | None = None
    consecutive_timeouts: int = 0
    aborted_runs: int = 0
    total_wait_ms: int = 0
    estimated_blocked_threads: int = 0
    _lock: threading.RLock = field(default_factory=threading.RLock, init=False, repr=False)

    def metrics(self) -> dict:
        global_metrics = get_sap_com_global_metrics()
        with self._lock:
            return {
                'consecutive_timeouts': self.consecutive_timeouts,
                'aborted_runs': self.aborted_runs,
                'total_wait_ms': self.total_wait_ms,
                'estimated_blocked_threads': self.estimated_blocked_threads,
                'global_aborted_runs': global_metrics.get('aborted_runs', 0),
                'global_estimated_blocked_threads': global_metrics.get('estimated_blocked_threads', 0),
            }

    def record_success(self, operation: str, logger: logging.Logger | None = None) -> None:
        logger = logger or log
        with self._lock:
            had_timeout = self.consecutive_timeouts > 0
            metrics = self.metrics() if had_timeout else None
            self.consecutive_timeouts = 0

        if had_timeout:
            logger.info("SAP_COM_RECOVERED operation=%s metrics=%s", operation, metrics)
            self._emit_status(
                'SAP volvió a responder.',
                badge='SAP',
                mode='running',
                footer='Se continuará con la corrida actual.',
            )

    def record_late_completion(self, operation: str, logger: logging.Logger | None = None) -> None:
        logger = logger or log
        with self._lock:
            if self.estimated_blocked_threads > 0:
                self.estimated_blocked_threads -= 1
                _update_global_metric('estimated_blocked_threads', -1)
            metrics = self.metrics()

        logger.info("SAP_COM_RECOVERED operation=%s late_completion=true metrics=%s", operation, metrics)

    def handle_timeout(
        self,
        *,
        operation: str,
        elapsed_ms: int,
        timeout_ms: int,
        logger: logging.Logger | None = None,
        cooldown: bool = True,
        emit_status: bool = True,
    ) -> bool:
        """
        Registra timeout, aplica cooldown y devuelve True si la corrida debe abortar.
        """
        logger = logger or log
        with self._lock:
            self.consecutive_timeouts += 1
            self.total_wait_ms += max(0, elapsed_ms)
            self.estimated_blocked_threads += 1
            _update_global_metric('estimated_blocked_threads', 1)
            metrics = self.metrics()
            consecutive = self.consecutive_timeouts

        logger.error(
            "SAP_COM_BLOCKED operation=%s duration_ms=%s timeout_ms=%s metrics=%s",
            operation,
            elapsed_ms,
            timeout_ms,
            metrics,
        )
        if emit_status:
            self._emit_status(
                'SAP está ocupado o no respondió a tiempo.',
                badge='SAP',
                mode='waiting',
                footer='Puede haber otra automatización cargando datos. Se esperará antes de continuar.',
            )

        if cooldown:
            self._cooldown(operation, logger)
        if self._sap_recovered_after_cooldown(operation, logger):
            return False

        if consecutive >= _max_consecutive_timeouts():
            with self._lock:
                self.aborted_runs += 1
                _update_global_metric('aborted_runs', 1)
                metrics = self.metrics()
            logger.critical(
                "SAP_COM_BLOCKED_FATAL operation=%s consecutive_timeouts=%s metrics=%s",
                operation,
                consecutive,
                metrics,
            )
            if emit_status:
                self._emit_status(
                    'Corrida detenida: SAP no respondió después de varios intentos.',
                    badge='SAP',
                    mode='error',
                    footer='No se crearán más hilos COM en esta corrida. Libera SAP y vuelve a iniciar.',
                )
            return True

        return False

    def _cooldown(self, operation: str, logger: logging.Logger) -> None:
        cooldown = _cooldown_seconds()
        logger.warning(
            "SAP_COM_COOLDOWN_STARTED operation=%s cooldown_seconds=%s",
            operation,
            cooldown,
        )
        if cooldown:
            time.sleep(cooldown)
        logger.warning(
            "SAP_COM_COOLDOWN_FINISHED operation=%s cooldown_seconds=%s",
            operation,
            cooldown,
        )

    def _sap_recovered_after_cooldown(self, operation: str, logger: logging.Logger) -> bool:
        try:
            from queue_app.services.diagnostics_service import get_fast_sap_status

            status = get_fast_sap_status()
        except Exception as exc:
            logger.warning("sap_com_recovery_probe_failed operation=%s error=%s", operation, exc)
            return False

        if status.get('status') == 'connected' and status.get('connected'):
            self.record_success(operation, logger)
            return True

        logger.warning(
            "sap_com_recovery_probe_not_ready operation=%s status=%s code=%s reason=%s",
            operation,
            status.get('status'),
            status.get('code'),
            status.get('reason'),
        )
        return False

    def _emit_status(self, message: str, *, badge: str, mode: str, footer: str) -> None:
        if not self.status_callback:
            return

        try:
            self.status_callback(message, badge=badge, mode=mode, footer=footer)
        except Exception as exc:
            log.warning("sap_com_status_emit_failed error=%s", exc)
