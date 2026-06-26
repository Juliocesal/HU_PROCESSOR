import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from datetime import datetime

from django.conf import settings
from django.db import connection
from django.db.models import Count, Q
from django.utils import timezone

from queue_app.models import HUItem, Pallet
from queue_app.services.queue_runtime import (
    QUEUE_WORKER_HEARTBEAT_STALE_SECONDS,
    get_processing_pallet_ids,
    get_queue_idle_remaining_seconds,
    get_queue_lock_owner,
    get_queue_stop_request_age_seconds,
    get_queue_worker_heartbeat_age_seconds,
    is_continuous_queue_armed,
    is_queue_locked,
    is_queue_stop_requested,
)
from queue_app.services.stats_service import (
    calculate_queue_operational_status,
    pallets_ready_for_pdf_queryset,
)

log = logging.getLogger(__name__)

CELERY_NO_WORKER_MESSAGE = (
    'Celery apagado. Redis responde, pero no hay workers activos. '
    'La cola NO se procesara hasta iniciar Celery.'
)
SAP_START_BLOCKED_MESSAGE = (
    'SAP está ocupado o no respondió a tiempo. Puede haber otra automatización '
    'cargando datos. Daphne, Celery y Redis siguen activos.'
)
SAP_START_BLOCKED_STATUSES = {'timeout', 'busy', 'stale', 'unavailable', 'unknown'}
SAP_START_BLOCKED_REASONS = {'inflight_probe', 'circuit_breaker'}
SAP_STATUS_CACHE_TTL_SECONDS = 10
SAP_STATUS_STALE_SECONDS = 60
SAP_STATUS_TIMEOUT_SECONDS = 1.5
SAP_STATUS_CIRCUIT_BREAKER_SECONDS = 20
SAP_STATUS_SLOW_PROBE_COOLDOWN_SECONDS = int(
    getattr(settings, 'SAP_STATUS_SLOW_PROBE_COOLDOWN_SECONDS', 180)
)
SAP_STATUS_SKIP_LIVE_PROBE_WHILE_QUEUE_LOCKED = bool(
    getattr(settings, 'SAP_STATUS_SKIP_LIVE_PROBE_WHILE_QUEUE_LOCKED', True)
)
REDIS_STATUS_TIMEOUT_SECONDS = 0.25

_sap_status_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix='sap-status')
_sap_status_lock = threading.Lock()
_sap_status_future = None
_sap_status_cache = None
_sap_status_circuit_until = 0.0
_sap_status_last_log = {}


def _diagnostic_error_message(exc: Exception) -> str:
    """Evita exponer trazas internas cuando DEBUG esta desactivado."""
    if settings.DEBUG:
        return str(exc)[:300]
    return exc.__class__.__name__


def _sap_status_log_once(key: str, level: int, message: str, *, interval: int = 60) -> None:
    """Evita spam de logs cuando SAP queda caido o no responde por varios minutos."""
    now = time.time()
    last = _sap_status_last_log.get(key, 0)
    if now - last < interval:
        return

    _sap_status_last_log[key] = now
    log.log(level, message)


def _sap_status_payload(
    status: str,
    *,
    connected=False,
    user='',
    message='',
    source='live',
    stale=False,
    checked_at='',
    code='',
    reason='',
) -> dict:
    return {
        'connected': bool(connected),
        'status': status,
        'user': user if connected else '',
        'configured_user': getattr(settings, 'SAP_LOGIN_USER', ''),
        'message': message,
        'checked_at': checked_at,
        'stale': bool(stale),
        'source': source,
        'code': code,
        'reason': reason,
    }


def set_cached_sap_connected(user='', message='SAP conectado en worker NEXHUS.') -> None:
    """Actualiza el cache cuando Celery ya confirmo una sesion SAP real."""
    global _sap_status_cache, _sap_status_circuit_until

    with _sap_status_lock:
        _sap_status_cache = _sap_status_payload(
            'connected',
            connected=True,
            user=user,
            message=message,
            source='worker',
            stale=False,
            checked_at=timezone.now().isoformat(),
        )
        _sap_status_circuit_until = 0.0


def set_cached_sap_disconnected(message='Sesion SAP NEXHUS cerrada por el worker.') -> None:
    """Actualiza el cache cuando Celery cierra la sesion SAP que tenia tomada."""
    global _sap_status_cache

    with _sap_status_lock:
        _sap_status_cache = _sap_status_payload(
            'disconnected',
            connected=False,
            message=message,
            source='worker',
            stale=False,
            checked_at=timezone.now().isoformat(),
        )


def _live_sap_status_probe() -> dict:
    """Consulta SAP COM en un thread separado para que el request HTTP no se bloquee."""
    from core.sap_client import SAPClient

    started_at = time.perf_counter()
    configured_user = getattr(settings, 'SAP_LOGIN_USER', '')
    log.info("SAP_STATUS_PROBE_START configured_user=%r", configured_user)
    try:
        ok, user = SAPClient.check_session()
        checked_at = timezone.now().isoformat()
        payload = _sap_status_payload(
            'connected' if ok else 'disconnected',
            connected=ok,
            user=user,
            message=(
                f'Sesion SAP NEXHUS activa para {configured_user}'
                if ok and configured_user
                else 'Sesion SAP activa'
                if ok
                else f'No hay sesion SAP NEXHUS activa para {configured_user}'
                if configured_user
                else 'Sesion SAP desconectada o no valida'
            ),
            source='live',
            stale=False,
            checked_at=checked_at,
        )
        payload['duration_ms'] = int((time.perf_counter() - started_at) * 1000)
        log.info(
            "SAP_STATUS_PROBE_DONE status=%s connected=%s user=%r duration_ms=%s",
            payload.get('status'),
            payload.get('connected'),
            payload.get('user'),
            payload['duration_ms'],
        )
        return payload
    except Exception as exc:
        payload = _sap_status_payload(
            'unavailable',
            message='No se pudo validar SAP.',
            source='error',
            stale=True,
            checked_at=timezone.now().isoformat(),
            code='SAP_COM_BLOCKED',
            reason='probe_error',
        )
        payload['error'] = _diagnostic_error_message(exc)
        payload['duration_ms'] = int((time.perf_counter() - started_at) * 1000)
        log.warning(
            "SAP_STATUS_PROBE_ERROR duration_ms=%s error=%s",
            payload['duration_ms'],
            _diagnostic_error_message(exc),
        )
        return payload


def _sap_status_from_cache(
    status: str,
    source: str,
    message: str,
    *,
    code='',
    reason='',
) -> dict:
    cached = _sap_status_cache or {}
    checked_at = cached.get('checked_at', '')
    age = 999999.0
    if checked_at:
        try:
            checked = datetime.fromisoformat(checked_at)
            age = max(0.0, (timezone.now() - checked).total_seconds())
        except Exception:
            age = 999999.0

    stale = age > SAP_STATUS_STALE_SECONDS
    connected = bool(cached.get('connected')) and not stale and status != 'unavailable'
    return _sap_status_payload(
        status,
        connected=connected,
        user=cached.get('user', ''),
        message=message,
        source=source,
        stale=stale,
        checked_at=checked_at,
        code=code or cached.get('code', ''),
        reason=reason or cached.get('reason', ''),
    )


def _harvest_sap_status_future_locked() -> None:
    global _sap_status_future, _sap_status_cache, _sap_status_circuit_until

    if _sap_status_future is None or not _sap_status_future.done():
        return

    try:
        _sap_status_cache = _sap_status_future.result()
        duration_ms = int(_sap_status_cache.get('duration_ms') or 0)
        timeout_ms = int(SAP_STATUS_TIMEOUT_SECONDS * 1000)
        if duration_ms > timeout_ms:
            _sap_status_circuit_until = time.time() + SAP_STATUS_SLOW_PROBE_COOLDOWN_SECONDS
            _sap_status_log_once(
                'sap_status_slow_probe_cooldown',
                logging.WARNING,
                (
                    "sap_status_slow_probe_cooldown_started "
                    f"duration_ms={duration_ms} cooldown_s={SAP_STATUS_SLOW_PROBE_COOLDOWN_SECONDS}"
                ),
                interval=30,
            )
        else:
            _sap_status_circuit_until = 0.0
        _sap_status_log_once(
            'sap_status_recovered',
            logging.INFO,
            f"sap_status_recovered status={_sap_status_cache.get('status')}",
            interval=30,
        )
    except Exception as exc:
        _sap_status_cache = _sap_status_payload(
            'unavailable',
            message='No se pudo validar SAP.',
            source='error',
            stale=True,
            checked_at=timezone.now().isoformat(),
            code='SAP_COM_BLOCKED',
            reason='probe_error',
        )
        _sap_status_cache['error'] = _diagnostic_error_message(exc)
        _sap_status_circuit_until = time.time() + SAP_STATUS_CIRCUIT_BREAKER_SECONDS
        _sap_status_log_once(
            'sap_status_error',
            logging.WARNING,
            f"sap_status_error error={_diagnostic_error_message(exc)}",
        )
    finally:
        _sap_status_future = None


def get_fast_sap_status() -> dict:
    """
    Retorna estado SAP con timeout y cache para proteger Daphne de COM bloqueado.

    Estados posibles: connected, disconnected, unknown, timeout, stale,
    unavailable. `connected` se mantiene por compatibilidad con la UI previa.
    """
    global _sap_status_future, _sap_status_cache, _sap_status_circuit_until

    now = time.time()
    with _sap_status_lock:
        _harvest_sap_status_future_locked()

        if _sap_status_future is not None:
            _sap_status_log_once(
                'sap_status_inflight',
                logging.INFO,
                'sap_status_cache_used reason=inflight_probe',
                interval=30,
            )
            return _sap_status_from_cache(
                'timeout',
                'cache',
                'SAP no respondio a tiempo. Estado anterior usado temporalmente.',
                code='SAP_COM_BLOCKED',
                reason='inflight_probe',
            )

        if _sap_status_cache:
            checked_at = _sap_status_cache.get('checked_at', '')
            try:
                checked = datetime.fromisoformat(checked_at)
                age = max(0.0, (timezone.now() - checked).total_seconds())
            except Exception:
                age = SAP_STATUS_STALE_SECONDS + 1

            if age <= SAP_STATUS_CACHE_TTL_SECONDS:
                _sap_status_log_once(
                    'sap_status_cache',
                    logging.INFO,
                    'sap_status_cache_used reason=fresh_cache',
                    interval=30,
                )
                cached = dict(_sap_status_cache)
                cached['source'] = 'cache'
                cached['stale'] = False
                return cached

        if now < _sap_status_circuit_until:
            _sap_status_log_once(
                'sap_status_circuit',
                logging.WARNING,
                'sap_status_circuit_breaker_active',
                interval=30,
            )
            return _sap_status_from_cache(
                'stale',
                'cache',
                'Verificacion SAP pausada temporalmente por timeouts recientes.',
                code='SAP_COM_BLOCKED',
                reason='circuit_breaker',
            )

        if SAP_STATUS_SKIP_LIVE_PROBE_WHILE_QUEUE_LOCKED:
            try:
                if is_queue_locked():
                    cached = _sap_status_cache or {}
                    _sap_status_log_once(
                        'sap_status_queue_locked',
                        logging.INFO,
                        'sap_status_live_probe_skipped reason=queue_locked',
                        interval=30,
                    )
                    if cached.get('connected'):
                        return _sap_status_payload(
                            'connected',
                            connected=True,
                            user=cached.get('user', ''),
                            message='SAP activo en worker NEXHUS.',
                            source='cache',
                            stale=False,
                            checked_at=cached.get('checked_at', ''),
                            code='SAP_WORKER_ACTIVE',
                            reason='queue_locked',
                        )
                    return _sap_status_from_cache(
                        'stale',
                        'cache',
                        'Verificacion SAP pausada mientras NEXHUS procesa la cola.',
                        code='SAP_WORKER_ACTIVE',
                        reason='queue_locked',
                    )
            except Exception as exc:
                log.debug("sap_status_queue_lock_check_failed error=%s", exc)

        _sap_status_future = _sap_status_executor.submit(_live_sap_status_probe)
        future = _sap_status_future

    try:
        result = future.result(timeout=SAP_STATUS_TIMEOUT_SECONDS)
    except FutureTimeoutError:
        with _sap_status_lock:
            _sap_status_circuit_until = time.time() + SAP_STATUS_CIRCUIT_BREAKER_SECONDS
        _sap_status_log_once(
            'sap_status_timeout',
            logging.WARNING,
            'sap_status_timeout circuit_breaker_started',
            interval=30,
        )
        return _sap_status_from_cache(
            'timeout',
            'timeout',
            'SAP no respondio a tiempo. Estado SAP no confirmado.',
            code='SAP_STATUS_TIMEOUT',
            reason='live_timeout',
        )
    except Exception as exc:
        with _sap_status_lock:
            _sap_status_circuit_until = time.time() + SAP_STATUS_CIRCUIT_BREAKER_SECONDS
        _sap_status_log_once(
            'sap_status_error',
            logging.WARNING,
            f"sap_status_error error={_diagnostic_error_message(exc)}",
        )
        payload = _sap_status_payload(
            'unavailable',
            message='No se pudo validar SAP.',
            source='error',
            stale=True,
            checked_at=timezone.now().isoformat(),
            code='SAP_COM_BLOCKED',
            reason='probe_error',
        )
        payload['error'] = _diagnostic_error_message(exc)
        return payload

    with _sap_status_lock:
        if _sap_status_future is future:
            _sap_status_cache = result
            _sap_status_future = None
            _sap_status_circuit_until = 0.0

    return result


def sap_start_decision(sap_status_func=None) -> dict:
    """
    Decide si el inicio debe bloquearse o si Celery debe abrir login directo.

    Fase 2 opcional: coordinar scripts externos con un lock Redis/archivo con
    TTL para marcar "SAP automation running" sin depender solo del probe COM.
    """
    if sap_status_func is None:
        sap_status_func = get_fast_sap_status

    status = sap_status_func()
    if not isinstance(status, dict):
        status = {
            'status': 'unknown',
            'message': 'Estado SAP invalido.',
            'source': 'invalid',
            'code': 'SAP_COM_BLOCKED',
            'reason': 'invalid_status',
        }

    sap_state = status.get('status') or ('connected' if status.get('connected') else 'unknown')
    reason = status.get('reason') or ''
    code = status.get('code') or ''
    connected = bool(status.get('connected'))
    user = status.get('user') or ''

    if connected:
        return {
            'blocked': False,
            'force_new_sap_login': False,
            'sap': status,
        }

    if (
        sap_state in {'timeout', 'stale', 'disconnected'}
        or reason in SAP_START_BLOCKED_REASONS
        or code in {'SAP_COM_BLOCKED', 'SAP_STATUS_TIMEOUT'}
    ) and not user:
        log.warning(
            "PROCESS_START_ALLOW_DIRECT_SAP_LOGIN sap_status=%s reason=%s source=%s code=%s",
            sap_state,
            reason,
            status.get('source'),
            code,
        )
        return {
            'blocked': False,
            'force_new_sap_login': True,
            'sap': status,
            'code': code or 'SAP_OPEN_LOGIN_DIRECT',
        }

    blocked = (
        sap_state in SAP_START_BLOCKED_STATUSES
        or reason in SAP_START_BLOCKED_REASONS
        or code in {'SAP_COM_BLOCKED', 'SAP_STATUS_TIMEOUT'}
    )
    if not blocked:
        return {
            'blocked': False,
            'force_new_sap_login': False,
            'sap': status,
        }

    final_code = 'SAP_STATUS_TIMEOUT' if sap_state == 'timeout' or code == 'SAP_STATUS_TIMEOUT' else 'SAP_COM_BLOCKED'
    return {
        'blocked': True,
        'force_new_sap_login': False,
        'code': final_code,
        'message': SAP_START_BLOCKED_MESSAGE,
        'sap': status,
    }


def sap_start_blocker(sap_status_func=None) -> dict | None:
    decision = sap_start_decision(sap_status_func=sap_status_func)
    return decision if decision.get('blocked') else None


def _check_redis_status() -> dict:
    started = time.perf_counter()
    try:
        import redis

        client = redis.Redis.from_url(
            settings.CELERY_BROKER_URL,
            socket_connect_timeout=REDIS_STATUS_TIMEOUT_SECONDS,
            socket_timeout=REDIS_STATUS_TIMEOUT_SECONDS,
        )
        client.ping()
        return {
            'ok': True,
            'message': 'Redis responde.',
            'action': 'Redis esta disponible para broker/cache.',
            'latency_ms': round((time.perf_counter() - started) * 1000, 1),
        }
    except Exception as exc:
        return {
            'ok': False,
            'message': 'Redis no responde.',
            'action': 'Revisar servicio Redis, puerto y CELERY_BROKER_URL.',
            'error': _diagnostic_error_message(exc),
            'latency_ms': round((time.perf_counter() - started) * 1000, 1),
        }


def _queue_diagnostic_snapshot_without_redis(redis_status: dict | None = None) -> dict:
    """Snapshot degradado para diagnostico cuando Redis no responde."""
    item_counts = HUItem.objects.aggregate(
        processing_hus=Count('id', filter=Q(status=HUItem.STATUS_PROCESSING)),
        pending_hus=Count('id', filter=Q(status=HUItem.STATUS_PENDING)),
    )
    pallet_counts = Pallet.objects.aggregate(
        ready_pallets=Count('id', filter=Q(status=Pallet.STATUS_READY)),
        active_pallets=Count('id', filter=Q(status=Pallet.STATUS_ACTIVE)),
    )
    return {
        'is_locked': False,
        'lock_owner': '',
        'stop_requested': False,
        'stop_request_age_seconds': None,
        'continuous_armed': False,
        'processing_pallet_ids': [],
        'idle_remaining_seconds': None,
        'worker_heartbeat_age_seconds': None,
        'worker_heartbeat_alive': False,
        'worker_stale': False,
        'processing_hus': item_counts['processing_hus'],
        'pending_hus': item_counts['pending_hus'],
        'ready_pallets': pallet_counts['ready_pallets'],
        'active_pallets': pallet_counts['active_pallets'],
        'pdf_pending': pallets_ready_for_pdf_queryset().count(),
        'last_operational_status': None,
        'runtime_unavailable': True,
        'source': 'database_only',
        'error': (redis_status or {}).get('error', 'Redis no responde.'),
    }


def _queue_snapshot_suggests_active_worker(queue_status: dict | None) -> bool:
    if not queue_status or queue_status.get('error'):
        return False

    if queue_status.get('worker_heartbeat_alive'):
        return True

    if queue_status.get('worker_stale'):
        return False

    if queue_status.get('processing_hus') or queue_status.get('processing_pallet_ids'):
        return True

    if queue_status.get('idle_remaining_seconds') is not None:
        return True

    operational_status = queue_status.get('last_operational_status') or {}
    return bool(
        queue_status.get('is_locked')
        and operational_status.get('mode') in {'running', 'waiting'}
    )


def _celery_busy_status(started: float) -> dict:
    return {
        'ok': True,
        'level': 'warn',
        'message': (
            'Celery no respondio al ping de control, pero la cola esta activa. '
            'El worker probablemente esta ocupado en SAP/PDF o en espera controlada.'
        ),
        'action': (
            'Si la UI sigue recibiendo eventos, no reiniciar Celery. '
            'Si no hay eventos por mas de 45s, revisar worker, SAP y WebSocket.'
        ),
        'workers': [],
        'control_ping_ok': False,
        'latency_ms': round((time.perf_counter() - started) * 1000, 1),
    }


def _check_celery_status(redis_ok: bool, queue_status: dict | None = None) -> dict:
    if not redis_ok:
        return {
            'ok': False,
            'message': 'No se verifica Celery porque Redis no responde.',
            'action': 'Levantar Redis antes de validar o iniciar workers Celery.',
            'workers': [],
        }

    started = time.perf_counter()
    try:
        from celery import current_app

        ping = current_app.control.inspect(timeout=1).ping() or {}
        workers = sorted(ping.keys())
        if not workers:
            if _queue_snapshot_suggests_active_worker(queue_status):
                return _celery_busy_status(started)

            return {
                'ok': False,
                'level': 'error',
                'message': CELERY_NO_WORKER_MESSAGE,
                'action': 'Iniciar el worker Celery antes de procesar o reprocesar.',
                'workers': [],
                'control_ping_ok': False,
                'latency_ms': round((time.perf_counter() - started) * 1000, 1),
            }

        return {
            'ok': True,
            'level': 'ok',
            'message': f'{len(workers)} worker(s) Celery respondieron.',
            'action': 'Celery esta listo para recibir tareas.',
            'workers': workers,
            'control_ping_ok': True,
            'latency_ms': round((time.perf_counter() - started) * 1000, 1),
        }
    except Exception as exc:
        if _queue_snapshot_suggests_active_worker(queue_status):
            status = _celery_busy_status(started)
            status['error'] = _diagnostic_error_message(exc)
            return status

        return {
            'ok': False,
            'level': 'error',
            'message': 'No se pudo consultar Celery.',
            'action': 'Revisar worker Celery, broker Redis y permisos de red.',
            'error': _diagnostic_error_message(exc),
            'workers': [],
            'control_ping_ok': False,
            'latency_ms': round((time.perf_counter() - started) * 1000, 1),
        }


def _check_database_status() -> dict:
    started = time.perf_counter()
    db_settings = settings.DATABASES.get('default', {})
    engine = str(db_settings.get('ENGINE', ''))
    name = str(db_settings.get('NAME', ''))

    try:
        with connection.cursor() as cursor:
            cursor.execute('SELECT 1')
            cursor.fetchone()

        status = {
            'ok': True,
            'level': 'ok',
            'message': 'Base de datos responde.',
            'action': 'DB disponible para lectura/escritura segun permisos del proceso.',
            'latency_ms': round((time.perf_counter() - started) * 1000, 1),
            'engine': engine,
        }

        if settings.DEBUG:
            status['name'] = name

        if connection.vendor == 'sqlite' and name and name != ':memory:':
            db_path = os.path.abspath(name)
            db_dir = os.path.dirname(db_path) or os.getcwd()
            db_file_writable = os.path.exists(db_path) and os.access(db_path, os.W_OK)
            db_dir_writable = os.access(db_dir, os.W_OK)
            status['writable'] = bool(db_file_writable and db_dir_writable)

            if not status['writable']:
                status.update({
                    'ok': False,
                    'level': 'error',
                    'message': 'SQLite responde, pero la base no parece escribible.',
                    'action': 'Revisar permisos de archivo/carpeta. Puede provocar: attempt to write a readonly database.',
                })

        return status
    except Exception as exc:
        message = 'Base de datos no responde.'
        action = 'Revisar conexion, credenciales, permisos y disponibilidad del servidor DB.'
        lowered = str(exc).lower()

        if 'readonly database' in lowered or 'read-only database' in lowered:
            message = 'La base de datos esta en modo solo lectura.'
            action = 'Revisar permisos/ruta de DB. Django necesita escribir cambios de cola y trazabilidad.'

        return {
            'ok': False,
            'level': 'error',
            'message': message,
            'action': action,
            'error': _diagnostic_error_message(exc),
            'latency_ms': round((time.perf_counter() - started) * 1000, 1),
            'engine': engine,
            **({'name': name} if settings.DEBUG else {}),
        }


def _check_sap_status_for_diagnostics() -> dict:
    status = get_fast_sap_status()
    status['ok'] = status.get('status') == 'connected'
    status['action'] = (
        'SAP conectado y disponible para verificacion rapida.'
        if status['ok']
        else 'Si SAP esta invalido, cerrar/reabrir SAP o revisar login automatico.'
    )
    return status


def _queue_diagnostic_snapshot() -> dict:
    try:
        heartbeat_age = get_queue_worker_heartbeat_age_seconds()
        heartbeat_alive = (
            heartbeat_age is not None
            and heartbeat_age <= QUEUE_WORKER_HEARTBEAT_STALE_SECONDS
        )
        locked = is_queue_locked()
        lock_owner = get_queue_lock_owner() if locked else ''
        return {
            'is_locked': locked,
            'lock_owner': lock_owner,
            'stop_requested': is_queue_stop_requested(),
            'stop_request_age_seconds': get_queue_stop_request_age_seconds(),
            'continuous_armed': is_continuous_queue_armed(),
            'processing_pallet_ids': sorted(get_processing_pallet_ids()),
            'idle_remaining_seconds': get_queue_idle_remaining_seconds(),
            'worker_heartbeat_age_seconds': heartbeat_age,
            'worker_heartbeat_alive': heartbeat_alive,
            'worker_stale': bool(
                locked
                and heartbeat_age is not None
                and heartbeat_age > QUEUE_WORKER_HEARTBEAT_STALE_SECONDS
            ),
            'processing_hus': HUItem.objects.filter(status=HUItem.STATUS_PROCESSING).count(),
            'pending_hus': HUItem.objects.filter(status=HUItem.STATUS_PENDING).count(),
            'ready_pallets': Pallet.objects.filter(status=Pallet.STATUS_READY).count(),
            'active_pallets': Pallet.objects.filter(status=Pallet.STATUS_ACTIVE).count(),
            'pdf_pending': pallets_ready_for_pdf_queryset().count(),
            'last_operational_status': calculate_queue_operational_status(),
        }
    except Exception as exc:
        return {
            'is_locked': False,
            'lock_owner': '',
            'stop_requested': False,
            'stop_request_age_seconds': None,
            'continuous_armed': False,
            'processing_pallet_ids': [],
            'idle_remaining_seconds': None,
            'worker_heartbeat_age_seconds': None,
            'worker_heartbeat_alive': False,
            'worker_stale': False,
            'processing_hus': 0,
            'pending_hus': 0,
            'ready_pallets': 0,
            'active_pallets': 0,
            'pdf_pending': 0,
            'last_operational_status': None,
            'error': _diagnostic_error_message(exc),
        }


def iniciar_sap():
    """Inicia o valida la sesion SAP usando el login automatico configurado."""
    from core.sap_client import SAPClient

    connected, user, message = SAPClient.ensure_session_ready()
    log.info("iniciar_sap result connected=%s user=%s message=%s", connected, user, message)
    return connected
