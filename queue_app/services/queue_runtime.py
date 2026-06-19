import json
import logging
import threading
import time
from uuid import uuid4

from django.conf import settings

log = logging.getLogger(__name__)

QUEUE_LOCK_KEY = 'nexhus:queue_processing_lock'
QUEUE_STOP_KEY = 'nexhus:queue_stop_requested'
QUEUE_ACTIVE_PALLETS_KEY = 'nexhus:queue_active_pallets'
QUEUE_CONTINUOUS_ARMED_KEY = 'nexhus:queue_continuous_armed'
QUEUE_IDLE_DEADLINE_KEY = 'nexhus:queue_idle_deadline'
QUEUE_WORKER_HEARTBEAT_KEY = 'nexhus:queue_worker_heartbeat'
QUEUE_LAST_STATUS_KEY = 'nexhus:queue_last_status'
QUEUE_LOCK_TTL_SECONDS = 60 * 60 * 6
QUEUE_WORKER_HEARTBEAT_TTL_SECONDS = 75
QUEUE_WORKER_HEARTBEAT_STALE_SECONDS = 45
QUEUE_WORKER_HEARTBEAT_INTERVAL_SECONDS = 10
QUEUE_DEFAULT_IDLE_TIMEOUT_SECONDS = 3
QUEUE_IDLE_POLL_SECONDS = 2


class QueueOwnershipLost(RuntimeError):
    """Senal controlada para detener workers viejos sin escribir estados atrasados."""


_LOCAL_QUEUE_OWNERS = set()
_LOCAL_QUEUE_OWNERS_LOCK = threading.Lock()
_REDIS_LOCK_CLIENT = None
_REDIS_LOCK_CLIENT_URL = ''
_REDIS_LOCK_CLIENT_LOCK = threading.Lock()
_REDIS_FAST_CLIENT = None
_REDIS_FAST_CLIENT_URL = ''
_REDIS_FAST_CLIENT_LOCK = threading.Lock()


def _remember_local_owner(owner: str) -> None:
    with _LOCAL_QUEUE_OWNERS_LOCK:
        _LOCAL_QUEUE_OWNERS.add(owner)


def _forget_local_owner(owner: str) -> None:
    with _LOCAL_QUEUE_OWNERS_LOCK:
        _LOCAL_QUEUE_OWNERS.discard(owner)


def _has_local_owner(owner: str) -> bool:
    with _LOCAL_QUEUE_OWNERS_LOCK:
        return owner in _LOCAL_QUEUE_OWNERS


def _get_redis_lock_client():
    import redis

    global _REDIS_LOCK_CLIENT, _REDIS_LOCK_CLIENT_URL

    broker_url = settings.CELERY_BROKER_URL
    if _REDIS_LOCK_CLIENT is not None and _REDIS_LOCK_CLIENT_URL == broker_url:
        return _REDIS_LOCK_CLIENT

    with _REDIS_LOCK_CLIENT_LOCK:
        if _REDIS_LOCK_CLIENT is None or _REDIS_LOCK_CLIENT_URL != broker_url:
            _REDIS_LOCK_CLIENT = redis.Redis.from_url(
                broker_url,
                socket_connect_timeout=2,
                socket_timeout=2,
                health_check_interval=30,
            )
            _REDIS_LOCK_CLIENT_URL = broker_url

    return _REDIS_LOCK_CLIENT


def _get_redis_fast_client():
    """Cliente Redis de timeout corto para estados visuales no autoritativos."""
    import redis

    global _REDIS_FAST_CLIENT, _REDIS_FAST_CLIENT_URL

    broker_url = settings.CELERY_BROKER_URL
    if _REDIS_FAST_CLIENT is not None and _REDIS_FAST_CLIENT_URL == broker_url:
        return _REDIS_FAST_CLIENT

    with _REDIS_FAST_CLIENT_LOCK:
        if _REDIS_FAST_CLIENT is None or _REDIS_FAST_CLIENT_URL != broker_url:
            _REDIS_FAST_CLIENT = redis.Redis.from_url(
                broker_url,
                socket_connect_timeout=0.25,
                socket_timeout=0.25,
                health_check_interval=30,
            )
            _REDIS_FAST_CLIENT_URL = broker_url

    return _REDIS_FAST_CLIENT


def _new_queue_owner_token(task_name: str, task_id: str | None) -> str:
    """Genera un owner unico por ejecucion para invalidar workers viejos."""
    return f'{task_name}:{task_id or "manual"}:{uuid4().hex}'


def _acquire_queue_lock(owner: str) -> bool:
    try:
        client = _get_redis_lock_client()
        lock_value = f'{owner}|{time.time()}'
        acquired = bool(client.set(QUEUE_LOCK_KEY, lock_value, nx=True, ex=QUEUE_LOCK_TTL_SECONDS))
        if acquired:
            _remember_local_owner(owner)
            client.delete(QUEUE_STOP_KEY)
            client.delete(QUEUE_ACTIVE_PALLETS_KEY)
            client.delete(QUEUE_IDLE_DEADLINE_KEY)
            client.delete(QUEUE_WORKER_HEARTBEAT_KEY)
            client.delete(QUEUE_LAST_STATUS_KEY)
            log.info("queue_lock_acquired owner=%s", owner)
            return True

        if _queue_lock_is_stale():
            previous_owner = get_queue_lock_owner()
            log.warning(
                "queue_lock_stale_recovered owner=%s invalidated_owner=%s",
                owner,
                previous_owner,
            )
            clear_queue_runtime_state(
                reason='stale_lock_recovery',
                invalidated_owner=previous_owner,
            )
            acquired = bool(client.set(QUEUE_LOCK_KEY, lock_value, nx=True, ex=QUEUE_LOCK_TTL_SECONDS))
            if acquired:
                _remember_local_owner(owner)
                client.delete(QUEUE_STOP_KEY)
                client.delete(QUEUE_ACTIVE_PALLETS_KEY)
                client.delete(QUEUE_IDLE_DEADLINE_KEY)
                client.delete(QUEUE_WORKER_HEARTBEAT_KEY)
                client.delete(QUEUE_LAST_STATUS_KEY)
                log.info("queue_lock_acquired owner=%s", owner)
            return acquired

        return False
    except Exception as e:
        log.error("queue_lock_acquire_failed owner=%s error=%s", owner, e)
        return False


def _release_queue_lock(owner: str) -> None:
    try:
        client = _get_redis_lock_client()
        current = client.get(QUEUE_LOCK_KEY)
        if current and _queue_lock_owner(current) == owner:
            client.delete(QUEUE_LOCK_KEY)
            client.delete(QUEUE_STOP_KEY)
            client.delete(QUEUE_ACTIVE_PALLETS_KEY)
            client.delete(QUEUE_IDLE_DEADLINE_KEY)
            client.delete(QUEUE_WORKER_HEARTBEAT_KEY)
            log.info("queue_lock_released owner=%s", owner)
        elif current:
            log.warning(
                "queue_lock_release_skipped owner=%s current_owner=%s",
                owner,
                _queue_lock_owner(current),
            )
    except Exception as e:
        log.warning("queue_lock_release_failed owner=%s error=%s", owner, e)
    finally:
        _forget_local_owner(owner)


def _queue_lock_owner(raw_value) -> str:
    value = raw_value.decode('utf-8', errors='replace') if isinstance(raw_value, bytes) else str(raw_value)
    return value.rsplit('|', 1)[0]


def get_queue_lock_owner() -> str:
    """Devuelve el owner token actual del lock Redis, si existe."""
    try:
        current = _get_redis_lock_client().get(QUEUE_LOCK_KEY)
        return _queue_lock_owner(current) if current else ''
    except Exception as e:
        log.warning("queue_lock_owner_read_failed error=%s", e)
        return ''


def _queue_lock_owned_by(owner: str) -> bool:
    return bool(owner and get_queue_lock_owner() == owner)


def _ensure_queue_ownership(owner: str | None, action: str) -> None:
    """Corta escrituras si el owner token ya fue invalidado por stop/recovery."""
    if not owner:
        return

    if not _has_local_owner(owner):
        log.debug("queue_ownership_validation_skipped owner=%s action=%s", owner, action)
        return

    current_owner = get_queue_lock_owner()
    if current_owner == owner:
        return

    log.warning(
        "Worker perdio ownership; se detiene sin escribir cambios. owner=%s current_owner=%s action=%s",
        owner,
        current_owner or '<none>',
        action,
    )
    raise QueueOwnershipLost('Worker perdio ownership; se detiene sin escribir cambios.')


def _queue_lock_age_seconds() -> int | None:
    try:
        raw = _get_redis_lock_client().get(QUEUE_LOCK_KEY)
        if not raw:
            return None

        value = raw.decode('utf-8', errors='replace')
        if '|' not in value:
            return None

        _owner, raw_timestamp = value.rsplit('|', 1)
        return max(0, int(time.time() - float(raw_timestamp)))
    except Exception as e:
        log.warning("queue_lock_age_failed error=%s", e)
        return None


def _queue_lock_is_stale() -> bool:
    heartbeat_age = get_queue_worker_heartbeat_age_seconds()
    if heartbeat_age is not None and heartbeat_age <= QUEUE_WORKER_HEARTBEAT_STALE_SECONDS:
        return False

    lock_age = _queue_lock_age_seconds()
    if lock_age is None:
        # Locks created before timestamps cannot prove liveness after heartbeat expired.
        return heartbeat_age is None or heartbeat_age > QUEUE_WORKER_HEARTBEAT_STALE_SECONDS

    return lock_age > QUEUE_WORKER_HEARTBEAT_STALE_SECONDS


def is_queue_locked() -> bool:
    """Devuelve True cuando un worker posee el lock de procesamiento en Redis."""
    try:
        return bool(_get_redis_lock_client().exists(QUEUE_LOCK_KEY))
    except Exception as e:
        log.warning("queue_lock_status_failed error=%s", e)
        return False


def is_queue_locked_quick() -> bool:
    """Lectura rapida del lock para KPIs/UI; no debe decidir mutaciones."""
    try:
        return bool(_get_redis_fast_client().exists(QUEUE_LOCK_KEY))
    except Exception as e:
        log.debug("queue_lock_quick_status_failed error=%s", e)
        return False


def _touch_queue_worker_heartbeat(owner: str) -> None:
    """Marca que el worker que posee la cola sigue vivo."""
    try:
        client = _get_redis_lock_client()
        current = client.get(QUEUE_LOCK_KEY)
        if not current or _queue_lock_owner(current) != owner:
            log.debug(
                "queue_worker_heartbeat_skipped owner=%s current_owner=%s",
                owner,
                _queue_lock_owner(current) if current else '<none>',
            )
            return

        client.set(
            QUEUE_WORKER_HEARTBEAT_KEY,
            f'{owner}|{time.time()}',
            ex=QUEUE_WORKER_HEARTBEAT_TTL_SECONDS,
        )
    except Exception as e:
        log.warning("queue_worker_heartbeat_touch_failed owner=%s error=%s", owner, e)


def get_queue_worker_heartbeat_age_seconds() -> int | None:
    """Devuelve la edad del ultimo heartbeat del worker activo."""
    try:
        raw = _get_redis_lock_client().get(QUEUE_WORKER_HEARTBEAT_KEY)
        if not raw:
            return None

        value = raw.decode('utf-8', errors='replace')
        _owner, raw_timestamp = value.rsplit('|', 1)
        return max(0, int(time.time() - float(raw_timestamp)))
    except Exception as e:
        log.warning("queue_worker_heartbeat_read_failed error=%s", e)
        return None


def _start_queue_worker_heartbeat(owner: str):
    """Mantiene vivo el heartbeat incluso durante llamadas bloqueantes a SAP/PDF."""
    stop_event = threading.Event()

    def heartbeat_loop() -> None:
        while not stop_event.wait(QUEUE_WORKER_HEARTBEAT_INTERVAL_SECONDS):
            _touch_queue_worker_heartbeat(owner)

    thread = threading.Thread(
        target=heartbeat_loop,
        name='nexhus-queue-worker-heartbeat',
        daemon=True,
    )
    thread.start()
    return stop_event, thread


def clear_queue_runtime_state(reason: str = 'manual_recovery', invalidated_owner: str | None = None) -> None:
    """
    Limpia estado runtime de Redis cuando el worker desaparecio.

    Se usa solo para recuperacion operativa: no toca HUs/pallets por si SAP
    alcanzo a terminar una parte del trabajo antes de que Celery se cerrara.
    """
    try:
        client = _get_redis_lock_client()
        previous_owner = invalidated_owner if invalidated_owner is not None else get_queue_lock_owner()
        client.delete(
            QUEUE_LOCK_KEY,
            QUEUE_STOP_KEY,
            QUEUE_ACTIVE_PALLETS_KEY,
            QUEUE_IDLE_DEADLINE_KEY,
            QUEUE_WORKER_HEARTBEAT_KEY,
            QUEUE_CONTINUOUS_ARMED_KEY,
            QUEUE_LAST_STATUS_KEY,
        )
        log.warning(
            "queue_runtime_state_cleared reason=%s invalidated_owner=%s",
            reason,
            previous_owner or '<none>',
        )
    except Exception as e:
        log.warning("queue_runtime_state_clear_failed error=%s", e)


def arm_continuous_queue() -> None:
    """Mantiene armado el modo continuo despues de un inicio manual."""
    try:
        _get_redis_lock_client().set(
            QUEUE_CONTINUOUS_ARMED_KEY,
            '1',
            ex=QUEUE_LOCK_TTL_SECONDS,
        )
    except Exception as e:
        log.warning("queue_continuous_arm_failed error=%s", e)


def disarm_continuous_queue() -> None:
    """Desactiva la reactivacion automatica al detener o limpiar la cola."""
    try:
        _get_redis_lock_client().delete(QUEUE_CONTINUOUS_ARMED_KEY)
        _get_redis_lock_client().delete(QUEUE_IDLE_DEADLINE_KEY)
    except Exception as e:
        log.warning("queue_continuous_disarm_failed error=%s", e)


def is_continuous_queue_armed() -> bool:
    """Indica si un cierre de pallet puede reactivar Celery automaticamente."""
    try:
        return bool(_get_redis_lock_client().exists(QUEUE_CONTINUOUS_ARMED_KEY))
    except Exception as e:
        log.warning("queue_continuous_arm_check_failed error=%s", e)
        return False


def _set_processing_pallet_ids(pallet_ids: list[int], owner: str | None = None) -> None:
    """Guarda en Redis los pallets que pertenecen a la corrida Celery actual."""
    _ensure_queue_ownership(owner, 'set_processing_pallet_ids')
    try:
        client = _get_redis_lock_client()
        client.delete(QUEUE_ACTIVE_PALLETS_KEY)
        if pallet_ids:
            client.sadd(QUEUE_ACTIVE_PALLETS_KEY, *[str(pallet_id) for pallet_id in pallet_ids])
            client.expire(QUEUE_ACTIVE_PALLETS_KEY, QUEUE_LOCK_TTL_SECONDS)
    except Exception as e:
        log.warning("queue_active_pallets_set_failed error=%s", e)


def get_processing_pallet_ids() -> set[int]:
    """Devuelve los IDs de pallets que el worker ya tomo para la corrida actual."""
    try:
        raw_ids = _get_redis_lock_client().smembers(QUEUE_ACTIVE_PALLETS_KEY)
        return {
            int(raw_id.decode('utf-8', errors='replace'))
            for raw_id in raw_ids
            if raw_id
        }
    except Exception as e:
        log.warning("queue_active_pallets_read_failed error=%s", e)
        return set()


def _set_queue_idle_deadline(timeout_seconds: float, owner: str | None = None) -> None:
    """Guarda hasta cuando el worker esperara otro pallet antes de cerrar SAP."""
    _ensure_queue_ownership(owner, 'set_queue_idle_deadline')
    try:
        deadline = time.time() + max(0, float(timeout_seconds))
        client = _get_redis_lock_client()
        client.set(
            QUEUE_IDLE_DEADLINE_KEY,
            str(deadline),
            ex=QUEUE_LOCK_TTL_SECONDS,
        )
    except Exception as e:
        log.warning("queue_idle_deadline_set_failed error=%s", e)


def _clear_queue_idle_deadline(owner: str | None = None) -> None:
    """Limpia la marca de espera cuando el worker vuelve a trabajar o termina."""
    _ensure_queue_ownership(owner, 'clear_queue_idle_deadline')
    try:
        _get_redis_lock_client().delete(QUEUE_IDLE_DEADLINE_KEY)
    except Exception as e:
        log.warning("queue_idle_deadline_clear_failed error=%s", e)


def get_queue_idle_remaining_seconds() -> int | None:
    """Devuelve segundos restantes de espera continua, si existe esa ventana."""
    try:
        raw_deadline = _get_redis_lock_client().get(QUEUE_IDLE_DEADLINE_KEY)
        if not raw_deadline:
            return None

        deadline = float(raw_deadline.decode('utf-8', errors='replace'))
        return max(0, int(deadline - time.time()))
    except Exception as e:
        log.warning("queue_idle_deadline_read_failed error=%s", e)
        return None


def set_queue_last_status(payload: dict) -> None:
    """Guarda un estado operativo breve para snapshots de nuevas ventanas."""
    try:
        _get_redis_lock_client().set(
            QUEUE_LAST_STATUS_KEY,
            json.dumps(payload),
            ex=QUEUE_LOCK_TTL_SECONDS,
        )
    except Exception as e:
        log.warning("queue_last_status_set_failed error=%s", e)


def get_queue_last_status() -> dict | None:
    """Lee el ultimo estado operativo publicado por Celery/Django."""
    try:
        raw = _get_redis_lock_client().get(QUEUE_LAST_STATUS_KEY)
        if not raw:
            return None
        value = raw.decode('utf-8', errors='replace') if isinstance(raw, bytes) else str(raw)
        payload = json.loads(value)
        return payload if isinstance(payload, dict) else None
    except Exception as e:
        log.warning("queue_last_status_read_failed error=%s", e)
        return None


def clear_queue_last_status() -> None:
    """Limpia mensajes operativos transitorios cuando la cola vuelve a cero."""
    try:
        _get_redis_lock_client().delete(QUEUE_LAST_STATUS_KEY)
    except Exception as e:
        log.warning("queue_last_status_clear_failed error=%s", e)


def request_queue_stop() -> bool:
    """
    Solicita al worker activo detenerse en el siguiente punto seguro.

    El trabajo en SAP GUI no debe cortarse a mitad de HU. La tarea revisa esta
    marca entre HUs y antes de ZE16/PDF para detener la cola sin corromper SAP.
    """
    try:
        client = _get_redis_lock_client()
        if not client.exists(QUEUE_LOCK_KEY):
            return False
        if not client.exists(QUEUE_STOP_KEY):
            client.set(QUEUE_STOP_KEY, str(time.time()), ex=QUEUE_LOCK_TTL_SECONDS)
        else:
            client.expire(QUEUE_STOP_KEY, QUEUE_LOCK_TTL_SECONDS)
        return True
    except Exception as e:
        log.error("queue_stop_request_failed error=%s", e)
        return False


def is_queue_stop_requested() -> bool:
    """Indica si ya existe una solicitud de detencion pendiente."""
    try:
        return bool(_get_redis_lock_client().exists(QUEUE_STOP_KEY))
    except Exception as e:
        log.warning("queue_stop_status_failed error=%s", e)
        return False


def get_queue_stop_request_age_seconds() -> int | None:
    """Devuelve cuanto tiempo lleva esperando una solicitud de detencion."""
    try:
        raw = _get_redis_lock_client().get(QUEUE_STOP_KEY)
        if not raw:
            return None

        value = raw.decode('utf-8', errors='replace')
        try:
            started_at = float(value)
        except ValueError:
            # Compatibilidad con locks viejos guardados como "1".
            started_at = 1.0
        return max(0, int(time.time() - started_at))
    except Exception as e:
        log.warning("queue_stop_age_failed error=%s", e)
        return None


def _stop_requested(owner: str | None = None) -> bool:
    _ensure_queue_ownership(owner, 'check_stop_requested')
    try:
        return bool(_get_redis_lock_client().exists(QUEUE_STOP_KEY))
    except Exception as e:
        log.warning("queue_stop_check_failed error=%s", e)
        return False
