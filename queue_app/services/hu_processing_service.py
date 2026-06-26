import logging
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from dataclasses import dataclass, field
from threading import current_thread

from django.conf import settings
from django.utils import timezone

from queue_app.services.queue_runtime import QueueOwnershipLost, _ensure_queue_ownership
from queue_app.services.receipt_service import _run_pallet_boundary
from queue_app.services.performance_service import log_performance
from queue_app.services.sap_com_guard import SAPCOMCircuitBreaker

log = logging.getLogger(__name__)


SAP_COM_CONNECT_TIMEOUT_SECONDS = max(float(getattr(settings, 'SAP_COM_CONNECT_TIMEOUT_SECONDS', 30)), 1.0)
SAP_COM_PHASE_TIMEOUT_SECONDS = max(float(getattr(settings, 'SAP_COM_PHASE_TIMEOUT_SECONDS', 60)), 1.0)
SAP_COM_CALL_WARN_SECONDS = max(float(getattr(settings, 'SAP_COM_CALL_WARN_SECONDS', 5)), 0.0)


@dataclass
class _SAPComThreadState:
    client: object | None = None
    checked_pallet_id: int | None = None
    com_initialized: bool = False


@dataclass
class SAPWorkerSession:
    """
    Sesion COM exclusiva de una tarea Celery, reutilizada entre HUs secuenciales.

    Todas las llamadas SAP viven en un hilo STA dedicado. El worker espera cada
    operacion con timeout explicito; si COM queda bloqueado por otra
    automatizacion SAP, se descarta el hilo y se devuelve error controlado.
    """

    status_callback: object | None = None
    force_new_login_on_first_connect: bool = False
    com_breaker: SAPCOMCircuitBreaker = field(init=False, repr=False)
    _executor: ThreadPoolExecutor | None = field(default=None, init=False, repr=False)
    _state: _SAPComThreadState | None = field(default=None, init=False, repr=False)

    def __post_init__(self):
        self.com_breaker = SAPCOMCircuitBreaker(status_callback=self.status_callback)

    def _ensure_worker(self) -> tuple[ThreadPoolExecutor, _SAPComThreadState]:
        if self._executor is None or self._state is None:
            self._state = _SAPComThreadState()
            self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix='sap-com-worker')
        return self._executor, self._state

    def _execute_in_com_thread(self, state: _SAPComThreadState, operation: str, callback):
        import pythoncom

        started_at = time.perf_counter()
        log.info(
            "SAP_COM_OPERATION_START operation=%s thread=%s com_initialized=%s has_client=%s checked_pallet=%s",
            operation,
            current_thread().name,
            state.com_initialized,
            bool(state.client),
            state.checked_pallet_id,
        )
        if not state.com_initialized:
            pythoncom.CoInitialize()
            state.com_initialized = True
            log.info("sap_com_thread_initialized operation=%s", operation)
        try:
            result = callback(state)
            log.info(
                "SAP_COM_OPERATION_DONE operation=%s duration_ms=%s has_client=%s checked_pallet=%s",
                operation,
                int((time.perf_counter() - started_at) * 1000),
                bool(state.client),
                state.checked_pallet_id,
            )
            return result
        except Exception as exc:
            log.exception(
                "SAP_COM_OPERATION_ERROR operation=%s duration_ms=%s error=%s",
                operation,
                int((time.perf_counter() - started_at) * 1000),
                exc,
            )
            raise

    def _discard_worker(self, *, reason: str) -> None:
        executor = self._executor
        self._executor = None
        self._state = None
        if executor is not None:
            executor.shutdown(wait=False, cancel_futures=True)
        log.warning("sap_com_worker_discarded reason=%s", reason)

    def _run(self, operation: str, callback, *, timeout: float):
        from core.sap_client import SAPCOMBlockedError, SAPCOMFatalBlockedError

        executor, state = self._ensure_worker()
        started_at = time.perf_counter()
        future = executor.submit(self._execute_in_com_thread, state, operation, callback)
        try:
            result = future.result(timeout=timeout)
            self.com_breaker.record_success(operation, log)
            return result
        except FutureTimeoutError as exc:
            elapsed_ms = int((time.perf_counter() - started_at) * 1000)
            timeout_ms = int(timeout * 1000)
            log.error(
                "SAP_COM_OPERATION_TIMEOUT operation=%s duration_ms=%s timeout_ms=%s "
                "state_has_client=%s state_checked_pallet=%s state_com_initialized=%s",
                operation,
                elapsed_ms,
                timeout_ms,
                bool(state.client),
                state.checked_pallet_id,
                state.com_initialized,
            )
            future.cancel()
            future.add_done_callback(
                lambda _future: self.com_breaker.record_late_completion(operation, log)
            )
            self._discard_worker(reason=f'timeout:{operation}')
            fatal = self.com_breaker.handle_timeout(
                operation=operation,
                elapsed_ms=elapsed_ms,
                timeout_ms=timeout_ms,
                logger=log,
            )
            if fatal:
                raise SAPCOMFatalBlockedError(
                    f"SAP COM no respondio despues de "
                    f"{self.com_breaker.metrics()['consecutive_timeouts']} timeout(s). "
                    "Corrida detenida para evitar acumulacion de hilos COM."
                ) from exc
            raise SAPCOMBlockedError(
                f"SAP COM bloqueado en {operation} tras {timeout:g}s. "
                "Puede haber otra automatizacion SAP ocupando SAP GUI."
            ) from exc
        finally:
            elapsed_ms = int((time.perf_counter() - started_at) * 1000)
            warn_ms = int(SAP_COM_CALL_WARN_SECONDS * 1000)
            if warn_ms and elapsed_ms >= warn_ms:
                log.warning(
                    "sap_com_call_slow operation=%s duration_ms=%s warn_ms=%s",
                    operation,
                    elapsed_ms,
                    warn_ms,
                )

    @staticmethod
    def _acquire_client(state: _SAPComThreadState, pallet_id: int, force_new_login=False):
        from core.sap_client import SAPClient

        acquire_started_at = time.perf_counter()
        log.info(
            "SAP_ACQUIRE_START pallet=%s has_client=%s checked_pallet=%s",
            pallet_id,
            bool(state.client),
            state.checked_pallet_id,
        )
        if state.client is None:
            connect_started_at = time.perf_counter()
            log.info("SAP_ACQUIRE_CONNECT_START pallet=%s reason=no_cached_client", pallet_id)
            state.client = SAPClient()
            connect_kwargs = {'initialize_com': False}
            if force_new_login:
                connect_kwargs['force_new_login'] = True
            state.client.connect(**connect_kwargs)
            try:
                from queue_app.services.diagnostics_service import set_cached_sap_connected

                sap_user = state.client.get_user()
                if isinstance(sap_user, str) and sap_user.strip():
                    set_cached_sap_connected(user=sap_user)
            except Exception as exc:
                log.debug("sap_status_cache_worker_connect_failed error=%s", exc)
            state.checked_pallet_id = pallet_id
            log.info(
                "SAP_ACQUIRE_CONNECT_DONE pallet=%s duration_ms=%s user=%s client=%s",
                pallet_id,
                int((time.perf_counter() - connect_started_at) * 1000),
                state.client.get_user(),
                state.client.get_client(),
            )
            log.info(
                "SAP_ACQUIRE_DONE pallet=%s action=connected duration_ms=%s",
                pallet_id,
                int((time.perf_counter() - acquire_started_at) * 1000),
            )
            return state.client, 'connected'

        if state.checked_pallet_id == pallet_id:
            log.info(
                "SAP_ACQUIRE_DONE pallet=%s action=reused duration_ms=%s",
                pallet_id,
                int((time.perf_counter() - acquire_started_at) * 1000),
            )
            return state.client, 'reused'

        health_started_at = time.perf_counter()
        log.info("SAP_ACQUIRE_HEALTH_START pallet=%s previous_pallet=%s", pallet_id, state.checked_pallet_id)
        is_healthy = state.client.is_session_healthy()
        log_performance(
            log,
            'sap.session_health',
            health_started_at,
            pallet=pallet_id,
            healthy=is_healthy,
        )
        state.checked_pallet_id = pallet_id
        if is_healthy:
            log.info(
                "SAP_ACQUIRE_DONE pallet=%s action=health_checked duration_ms=%s",
                pallet_id,
                int((time.perf_counter() - acquire_started_at) * 1000),
            )
            return state.client, 'health_checked'

        log.warning("sap_worker_session_reconnect pallet=%s", pallet_id)
        reconnect_started_at = time.perf_counter()
        log.info("SAP_ACQUIRE_CONNECT_START pallet=%s reason=unhealthy_cached_client", pallet_id)
        state.client = SAPClient()
        state.client.connect(initialize_com=False)
        try:
            from queue_app.services.diagnostics_service import set_cached_sap_connected

            sap_user = state.client.get_user()
            if isinstance(sap_user, str) and sap_user.strip():
                set_cached_sap_connected(user=sap_user)
        except Exception as exc:
            log.debug("sap_status_cache_worker_reconnect_failed error=%s", exc)
        log.info(
            "SAP_ACQUIRE_CONNECT_DONE pallet=%s duration_ms=%s user=%s client=%s",
            pallet_id,
            int((time.perf_counter() - reconnect_started_at) * 1000),
            state.client.get_user(),
            state.client.get_client(),
        )
        log.info(
            "SAP_ACQUIRE_DONE pallet=%s action=reconnected duration_ms=%s",
            pallet_id,
            int((time.perf_counter() - acquire_started_at) * 1000),
        )
        return state.client, 'reconnected'

    def acquire(self, pallet_id: int):
        """Devuelve una sesion sana y solo la sondea al cambiar de pallet."""
        return self._run(
            'sap.acquire',
            lambda state: self._acquire_client(
                state,
                pallet_id,
                force_new_login=self.force_new_login_on_first_connect,
            ),
            timeout=SAP_COM_CONNECT_TIMEOUT_SECONDS,
        )

    def prepare(self, pallet_id: int) -> str:
        """Prepara la sesion SAP y retorna la accion realizada para logging."""
        _client, action = self.acquire(pallet_id)
        return action

    def call(self, pallet_id: int, operation: str, callback, *, timeout: float | None = None):
        """Ejecuta una operacion SAP con timeout explicito en el hilo COM."""
        return self._run(
            operation,
            lambda state: callback(self._acquire_client(state, pallet_id)[0]),
            timeout=timeout or SAP_COM_PHASE_TIMEOUT_SECONDS,
        )

    def invalidate(self) -> None:
        """Obliga a reconectar si SAP reporta una sesion invalida durante una HU."""
        self._discard_worker(reason='invalidate')

    def close_current_session(self, *, timeout: float | None = None) -> int:
        """Cierra solo la sesion SAP que el worker activo ya esta usando."""
        if self._executor is None or self._state is None or self._state.client is None:
            log.info("sap_worker_close_current_session_skipped reason=no_cached_client")
            return 0
        return self._run(
            'sap.close_worker_session',
            _close_current_worker_session,
            timeout=timeout or SAP_COM_CONNECT_TIMEOUT_SECONDS,
        )

    def close(self) -> None:
        """Libera el executor COM al terminar la corrida Celery."""
        executor = self._executor
        state = self._state
        self._executor = None
        self._state = None
        if executor is None:
            return

        if state is not None and state.com_initialized:
            try:
                future = executor.submit(self._execute_in_com_thread, state, 'sap.com_uninitialize', _uninitialize_com)
                future.result(timeout=3)
            except Exception as exc:
                log.warning("sap_com_thread_uninitialize_failed error=%s", exc)
        executor.shutdown(wait=False, cancel_futures=True)

    def metrics(self) -> dict:
        return self.com_breaker.metrics()


def _uninitialize_com(state: _SAPComThreadState):
    import pythoncom

    if state.com_initialized:
        pythoncom.CoUninitialize()
        state.com_initialized = False
        log.info("sap_com_thread_uninitialized")


def _close_current_worker_session(state: _SAPComThreadState) -> int:
    client = state.client
    if client is None:
        return 0
    close_current_session = getattr(client, 'close_current_session', None)
    if not callable(close_current_session):
        return 0
    closed = int(close_current_session() or 0)
    if closed:
        state.client = None
        state.checked_pallet_id = None
        try:
            from queue_app.services.diagnostics_service import set_cached_sap_disconnected

            set_cached_sap_disconnected()
        except Exception as exc:
            log.debug("sap_status_cache_worker_close_failed error=%s", exc)
    return closed


def _calculate_elapsed_ms(started_at) -> int:
    """Convierte un timestamp de inicio en milisegundos transcurridos."""
    if not started_at:
        return 0
    return max(0, int((timezone.now() - started_at).total_seconds() * 1000))


def _process_hu_item(
    hu_item_id: int,
    run_f1=True,
    run_f2=True,
    run_pallet_boundary=True,
    emit_pallet_completion=True,
    owner: str | None = None,
    run_pallet_boundary_func=_run_pallet_boundary,
    sap_session: SAPWorkerSession | None = None,
):
    import pythoncom
    from core.hu_origins import detect_origin
    from core.sap_client import (
        SAPBusyError,
        SAPCOMBlockedError,
        SAPCOMFatalBlockedError,
        SAPConnectionError,
        SAPDisconnectedError,
        SAPClient,
    )
    from queue_app.models import HUItem
    from queue_app.utils import emit_item_update, emit_queue_status, emit_stats_update

    hu_started_at = time.perf_counter()
    hu_code_for_log = str(hu_item_id)
    manage_com = sap_session is None
    if manage_com:
        pythoncom.CoInitialize()

    try:
        try:
            db_started_at = time.perf_counter()
            item = HUItem.objects.select_related('pallet').get(pk=hu_item_id)
            log_performance(log, 'db.fetch_hu', db_started_at, hu=item.hu_code, hu_id=hu_item_id)
        except HUItem.DoesNotExist:
            log.error("process_hu_item missing hu_id=%s", hu_item_id)
            return None

        if item.status != HUItem.STATUS_PENDING:
            log.warning("process_hu_item skip hu=%s status=%s", item.hu_code, item.status)
            return item

        origin = detect_origin(item.hu_code)
        hu_code_for_log = item.hu_code

        _ensure_queue_ownership(owner, 'mark_hu_processing')
        item.status = HUItem.STATUS_PROCESSING
        item.processing_started_at = timezone.now()
        item.error_msg = ''
        item.pdf_status = ''
        item.pdf_msg = ''
        item.pdf_ms = 0
        _ensure_queue_ownership(owner, 'save_hu_processing')
        db_started_at = time.perf_counter()
        item.save(update_fields=[
            'status',
            'processing_started_at',
            'error_msg',
            'pdf_status',
            'pdf_msg',
            'pdf_ms',
        ])
        from queue_app.services.stats_service import invalidate_queue_stats_cache

        invalidate_queue_stats_cache()
        log_performance(log, 'db.mark_hu_processing', db_started_at, hu=item.hu_code)
        emit_item_update(item)
        emit_stats_update(item.pallet)

        _ensure_queue_ownership(owner, 'emit_sap_connect_status')
        emit_queue_status(
            f'Conectando con SAP para HU {item.hu_code}.',
            badge='SAP',
            mode='running',
            footer=f'Preparando sesion SAP para Pallet P{item.pallet_id:02d}.',
            active_pallet_id=item.pallet_id,
            active_hu_count=1,
        )
        sap_started_at = time.perf_counter()
        if sap_session is None:
            sap = SAPClient()
            sap.connect()
            log_performance(log, 'sap.connect_hu', sap_started_at, hu=item.hu_code)
            log.info("process_hu_item sap_connected hu=%s", item.hu_code)
        else:
            sap = None
            sap_action = sap_session.prepare(item.pallet_id)
            log_performance(
                log,
                'sap.acquire_hu',
                sap_started_at,
                hu=item.hu_code,
                pallet=item.pallet_id,
                action=sap_action,
            )
            log.info("process_hu_item sap_%s hu=%s", sap_action, item.hu_code)

        res1 = None

        if run_f1:
            emit_queue_status(
                f'HU {item.hu_code}: iniciando F1 Acknowledge.',
                badge='F1',
                mode='running',
                footer='Capturando HU en ZMOVEINBHU.',
                active_pallet_id=item.pallet_id,
                active_hu_count=1,
            )
            sap_started_at = time.perf_counter()
            if sap_session is None:
                sap.setup_phase1(origin=origin)
            else:
                sap_session.call(
                    item.pallet_id,
                    'sap.f1.setup',
                    lambda client: client.setup_phase1(origin=origin),
                )
            log_performance(log, 'sap.f1.setup', sap_started_at, hu=item.hu_code)
            sap_started_at = time.perf_counter()
            if sap_session is None:
                res1 = sap.process_hu_phase1(item.hu_code, origin=origin)
            else:
                res1 = sap_session.call(
                    item.pallet_id,
                    'sap.f1.execute',
                    lambda client: client.process_hu_phase1(item.hu_code, origin=origin),
                )
            log_performance(
                log,
                'sap.f1.execute',
                sap_started_at,
                hu=item.hu_code,
                result=res1['status'],
            )
            _ensure_queue_ownership(owner, 'save_f1_result')
            item.phase1_msg = res1['message']
            item.f1_done_at = timezone.now()

            if res1['status'] in ('error', 'hu_not_found'):
                item.status = (
                    HUItem.STATUS_HU_NOT_FOUND
                    if res1['status'] == 'hu_not_found'
                    else HUItem.STATUS_ERROR
                )
                item.processed_at = timezone.now()
                item.processing_ms = _calculate_elapsed_ms(item.processing_started_at)
                item.error_msg = res1['message']
                _ensure_queue_ownership(owner, 'save_hu_f1_error')
                db_started_at = time.perf_counter()
                item.save(update_fields=[
                    'status',
                    'phase1_msg',
                    'f1_done_at',
                    'processed_at',
                    'processing_ms',
                    'error_msg',
                ])
                invalidate_queue_stats_cache()
                log_performance(log, 'db.save_f1_error', db_started_at, hu=item.hu_code)
                emit_item_update(item)
                emit_stats_update(item.pallet)
                log.warning(
                    "process_hu_item f1_failed hu=%s reason=%s",
                    item.hu_code,
                    res1['message'],
                )
                if run_pallet_boundary:
                    run_pallet_boundary_func(
                        item.pallet_id,
                        emit_completion=emit_pallet_completion,
                        owner=owner,
                    )
                return item

            if res1['status'] == 'already_separated':
                item.phase1_msg = res1['message']
                log.info("process_hu_item phase1_destination_notice hu=%s", item.hu_code)
            elif res1['status'] == 'duplicate':
                item.phase1_msg = res1['message'] or 'Ya se hizo el Acknowledge'

        if run_f2:
            emit_queue_status(
                f'HU {item.hu_code}: {"F1 listo. " if run_f1 else ""}Iniciando F2 Separazione.',
                badge='F2',
                mode='running',
                footer='Procesando separazione en ZMMTIJSEP.',
                active_pallet_id=item.pallet_id,
                active_hu_count=1,
            )
            sap_started_at = time.perf_counter()
            if sap_session is None:
                sap.setup_phase2(origin=origin)
            else:
                sap_session.call(
                    item.pallet_id,
                    'sap.f2.setup',
                    lambda client: client.setup_phase2(origin=origin),
                )
            log_performance(log, 'sap.f2.setup', sap_started_at, hu=item.hu_code)
            sap_started_at = time.perf_counter()
            if sap_session is None:
                res2 = sap.process_hu_phase2(
                    item.hu_code,
                    phase2_wait=origin.phase2_wait,
                    origin=origin,
                )
            else:
                res2 = sap_session.call(
                    item.pallet_id,
                    'sap.f2.execute',
                    lambda client: client.process_hu_phase2(
                        item.hu_code,
                        phase2_wait=origin.phase2_wait,
                        origin=origin,
                    ),
                )
            log_performance(
                log,
                'sap.f2.execute',
                sap_started_at,
                hu=item.hu_code,
                result=res2['status'],
            )
            _ensure_queue_ownership(owner, 'save_f2_result')
            item.phase2_msg = res2['message']
            item.phase2_ms = res2['duration_ms']

            if res2['status'] == 'ok':
                pallet = item.pallet
                pallet.f2_done_at = res2['phase2_ts']
                _ensure_queue_ownership(owner, 'save_pallet_f2_done')
                pallet.save(update_fields=['f2_done_at'])
                item.status = (
                    HUItem.STATUS_OK
                    if (
                        not run_f1
                        or (res1 and res1['status'] in ('ok', 'already_separated'))
                    )
                    else HUItem.STATUS_DUPLICATE
                )
            else:
                item.status = HUItem.STATUS_ERROR
                item.error_msg = res2['message']
        else:
            item.status = (
                HUItem.STATUS_DUPLICATE
                if res1 and res1['status'] == 'duplicate'
                else HUItem.STATUS_OK
            )

        item.processed_at = timezone.now()
        item.processing_ms = _calculate_elapsed_ms(item.processing_started_at)
        if item.status in (HUItem.STATUS_OK, HUItem.STATUS_DUPLICATE):
            item.error_msg = ''
        elif not item.error_msg:
            item.error_msg = item.phase2_msg or item.phase1_msg or 'Error de procesamiento'
        _ensure_queue_ownership(owner, 'save_hu_final_result')
        db_started_at = time.perf_counter()
        item.save(update_fields=[
            'status',
            'phase1_msg',
            'phase2_msg',
            'phase2_ms',
            'error_msg',
            'f1_done_at',
            'processed_at',
            'processing_ms',
        ])
        invalidate_queue_stats_cache()
        log_performance(log, 'db.save_hu_final', db_started_at, hu=item.hu_code, status=item.status)
        emit_item_update(item)
        emit_stats_update(item.pallet)

        if run_pallet_boundary:
            run_pallet_boundary_func(
                item.pallet_id,
                emit_completion=emit_pallet_completion,
                owner=owner,
            )

        return item

    except QueueOwnershipLost:
        log.warning("process_hu_item stopped_lost_ownership hu_id=%s owner=%s", hu_item_id, owner)
        raise
    except SAPCOMFatalBlockedError as e:
        if sap_session is not None:
            sap_session.invalidate()
        log.critical("process_hu_item SAP_COM_BLOCKED_FATAL hu_id=%s error=%s", hu_item_id, e)
        _mark_item_error(hu_item_id, str(e), owner=owner)
        raise
    except SAPConnectionError as e:
        if sap_session is not None:
            sap_session.invalidate()
        if isinstance(e, SAPCOMBlockedError):
            log.error("process_hu_item SAP_COM_BLOCKED hu_id=%s error=%s", hu_item_id, e)
        elif isinstance(e, SAPBusyError):
            log.error("process_hu_item SAP_BUSY hu_id=%s error=%s", hu_item_id, e)
        elif isinstance(e, SAPDisconnectedError):
            log.error("process_hu_item SAP_DISCONNECTED hu_id=%s error=%s", hu_item_id, e)
        else:
            log.error("process_hu_item sap_error hu_id=%s error=%s", hu_item_id, e)
        return _mark_item_error(hu_item_id, str(e), owner=owner)
    except Exception as e:
        log.exception("process_hu_item fatal_error hu_id=%s", hu_item_id)
        return _mark_item_error(hu_item_id, str(e), owner=owner)
    finally:
        if manage_com:
            pythoncom.CoUninitialize()
        log_performance(log, 'queue.hu_total', hu_started_at, hu=hu_code_for_log)


def _mark_item_error(hu_item_id: int, message: str, owner: str | None = None):
    from queue_app.models import HUItem
    from queue_app.utils import emit_item_update, emit_stats_update

    try:
        _ensure_queue_ownership(owner, 'mark_item_error')
        item = HUItem.objects.select_related('pallet').get(pk=hu_item_id)
        if not item.processing_started_at:
            item.processing_started_at = timezone.now()
        item.status = HUItem.STATUS_ERROR
        item.phase1_msg = message
        item.error_msg = message
        item.processed_at = timezone.now()
        item.processing_ms = _calculate_elapsed_ms(item.processing_started_at)
        _ensure_queue_ownership(owner, 'save_mark_item_error')
        item.save(update_fields=[
            'status',
            'phase1_msg',
            'error_msg',
            'processing_started_at',
            'processed_at',
            'processing_ms',
        ])
        from queue_app.services.stats_service import invalidate_queue_stats_cache

        invalidate_queue_stats_cache()
        emit_item_update(item)
        emit_stats_update(item.pallet)
        return item
    except Exception:
        log.exception("_mark_item_error failed hu_id=%s", hu_item_id)
        return None
