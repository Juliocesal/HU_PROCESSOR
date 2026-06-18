import json
import logging
import threading
import time
from uuid import uuid4

from celery import shared_task
from django.conf import settings
from django.utils import timezone

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
    """Señal controlada para detener workers viejos sin escribir estados atrasados."""


_LOCAL_QUEUE_OWNERS = set()
_LOCAL_QUEUE_OWNERS_LOCK = threading.Lock()


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

    return redis.Redis.from_url(
        settings.CELERY_BROKER_URL,
        socket_connect_timeout=2,
        socket_timeout=2,
    )


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


def _stopped_result(pallets_processed: int, hus_processed: int, errors: int) -> dict:
    return {
        'status': 'stopped',
        'message': 'Proceso detenido por el usuario.',
        'pallets_processed': pallets_processed,
        'hus_processed': hus_processed,
        'errors': errors,
    }


def _collect_processable_pallet_ids(run_pdf: bool) -> tuple[list[int], set[int]]:
    """Devuelve pallets cerrados/listos y separa cuáles solo esperan PDF."""
    from queue_app.models import HUItem, Pallet
    from queue_app.utils import pallets_ready_for_pdf_queryset

    pending_pallet_ids = list(
        Pallet.objects.filter(
            status=Pallet.STATUS_READY,
            items__status=HUItem.STATUS_PENDING,
        )
        .distinct()
        .order_by('id')
        .values_list('id', flat=True)
    )
    pdf_pallet_ids = set(
        pallets_ready_for_pdf_queryset()
        .order_by('id')
        .values_list('id', flat=True)
    ) if run_pdf else set()
    return sorted(set(pending_pallet_ids) | pdf_pallet_ids), pdf_pallet_ids


def _active_pallet_wait_context() -> dict:
    """Describe el pallet abierto cuando el worker continuo espera accion humana."""
    from queue_app.models import HUItem, Pallet

    active = (
        Pallet.objects
        .filter(status=Pallet.STATUS_ACTIVE)
        .order_by('-id')
        .first()
    )
    if not active:
        return {
            'message': 'Modo continuo activo. Escanea HUs para preparar el siguiente pallet.',
            'footer': 'Esperando nuevos HUs.',
            'active_pallet_id': None,
            'active_hu_count': 0,
        }

    hu_count = active.items.count()
    pending_count = active.items.filter(status=HUItem.STATUS_PENDING).count()
    pallet_label = f'P{active.pk:02d}'
    if hu_count:
        return {
            'message': (
                f'Pallet {pallet_label} abierto con {hu_count} HU(s). '
                'Cierra el pallet para continuar el proceso.'
            ),
            'footer': (
                f'Esperando cierre de {pallet_label}. '
                'Usa Nuevo pallet o escanea PALLET.'
            ),
            'active_pallet_id': active.pk,
            'active_hu_count': pending_count,
        }

    return {
        'message': f'Pallet {pallet_label} abierto sin HUs. Escanea para continuar.',
        'footer': f'Esperando HUs en {pallet_label}.',
        'active_pallet_id': active.pk,
        'active_hu_count': 0,
    }


def _mark_pallet_processing_started(pallet, owner: str | None = None) -> None:
    """Marca el inicio del ciclo F1->PDF la primera vez que Celery toma el pallet."""
    if pallet.processing_started_at:
        return

    _ensure_queue_ownership(owner, 'mark_pallet_processing_started')
    pallet.processing_started_at = timezone.now()
    pallet.processing_finished_at = None
    pallet.save(update_fields=['processing_started_at', 'processing_finished_at'])


def _mark_pallet_processing_finished(pallet, owner: str | None = None) -> None:
    """Marca fin de ciclo cuando el pallet termina sin pasar por PDF."""
    if not pallet.processing_started_at or pallet.processing_finished_at:
        return

    _ensure_queue_ownership(owner, 'mark_pallet_processing_finished')
    pallet.processing_finished_at = timezone.now()
    pallet.save(update_fields=['processing_finished_at'])


def _calculate_elapsed_ms(started_at) -> int:
    """Convierte un timestamp de inicio en milisegundos transcurridos."""
    if not started_at:
        return 0
    return max(0, int((timezone.now() - started_at).total_seconds() * 1000))


def _close_sap_after_queue_idle() -> None:
    """Cierra SAP al finalizar la corrida para evitar sesiones zombie por inactividad."""
    if not getattr(settings, 'SAP_CLOSE_WHEN_QUEUE_IDLE', True):
        return

    try:
        from core.sap_client import SAPClient

        closed = SAPClient.close_sessions()
        log.info("process_queue_task sap_idle_close closed=%s", closed)
    except Exception as e:
        log.warning("process_queue_task sap_idle_close_failed error=%s", e)


@shared_task(bind=True, max_retries=0)
def process_queue_task(
    self,
    run_f1=True,
    run_f2=True,
    run_pdf=True,
    continuous=False,
    idle_timeout=None,
):
    """
    Procesa toda la cola en orden deterministico:
    pallet -> todas sus HUs -> ZE16/PDF/impresion -> siguiente pallet.
    """
    from queue_app.models import HUItem, Pallet
    from queue_app.utils import (
        emit_queue_done,
        emit_pallet_done,
        emit_queue_status,
        emit_receipt_done,
        pallets_ready_for_pdf_queryset,
    )

    owner = _new_queue_owner_token('process_queue_task', self.request.id)
    if not _acquire_queue_lock(owner):
        message = 'Queue processing already active; start request ignored.'
        log.warning(message)
        emit_queue_status(
            message,
            badge='EN CURSO',
            mode='running',
            footer='Se ignoro una solicitud duplicada porque otro worker controla la cola.',
            is_running=True,
        )
        return {'ok': False, 'error': message}

    _touch_queue_worker_heartbeat(owner)
    heartbeat_stop, heartbeat_thread = _start_queue_worker_heartbeat(owner)
    pallets_processed = 0
    hus_processed = 0
    errors = 0
    idle_deadline = None
    idle_status_sent = False
    idle_timeout = (
        QUEUE_DEFAULT_IDLE_TIMEOUT_SECONDS
        if continuous and idle_timeout is None
        else float(idle_timeout or 0)
    )

    try:
        while True:
            _ensure_queue_ownership(owner, 'queue_loop')
            _touch_queue_worker_heartbeat(owner)
            if _stop_requested(owner):
                final = _stopped_result(pallets_processed, hus_processed, errors)
                _ensure_queue_ownership(owner, 'emit_queue_done_stop')
                emit_queue_done(final)
                return final

            pallet_ids, pdf_ready_pallet_ids = _collect_processable_pallet_ids(run_pdf)
            if not pallet_ids:
                _set_processing_pallet_ids([], owner=owner)
                if not continuous or idle_timeout <= 0 or (
                    pallets_processed == 0 and hus_processed == 0 and errors == 0
                ):
                    final_status = 'ok' if errors == 0 else 'error'
                    final_message = (
                        'All pallets were processed and printed successfully'
                        if errors == 0 and (pallets_processed or hus_processed)
                        else (
                            f'Queue finished with {errors} error(s)'
                            if errors
                            else 'No closed pallets ready to process.'
                        )
                    )
                    final = {
                        'status': final_status if (pallets_processed or hus_processed or errors) else 'error',
                        'message': final_message,
                        'pallets_processed': pallets_processed,
                        'hus_processed': hus_processed,
                        'errors': errors,
                    }
                    _ensure_queue_ownership(owner, 'emit_queue_done_empty')
                    emit_queue_done(final)
                    log.info("process_queue_task done result=%s", final)
                    return final

                now = time.monotonic()
                if idle_deadline is None:
                    idle_deadline = now + idle_timeout
                    _set_queue_idle_deadline(idle_timeout, owner=owner)
                    idle_status_sent = False
                    log.info("process_queue_task idle_wait timeout=%ss", idle_timeout)

                if not idle_status_sent:
                    _touch_queue_worker_heartbeat(owner)
                    context = _active_pallet_wait_context()
                    emit_queue_status(
                        context['message'],
                        badge='EN ESPERA',
                        mode='waiting',
                        footer=context['footer'],
                        active_pallet_id=context['active_pallet_id'],
                        active_hu_count=context['active_hu_count'],
                        remaining_seconds=max(0, int(idle_deadline - now)),
                    )
                    idle_status_sent = True

                if now >= idle_deadline:
                    _ensure_queue_ownership(owner, 'emit_idle_timeout_status')
                    emit_queue_status(
                        'Tiempo de espera agotado. Cerrando sesion SAP hasta el siguiente inicio.',
                        badge='SAP',
                        mode='waiting',
                        footer='El worker no encontro otro pallet cerrado dentro de la ventana de espera.',
                        remaining_seconds=0,
                    )
                    final_status = 'ok' if errors == 0 else 'error'
                    final_message = (
                        'All pallets were processed and printed successfully'
                        if errors == 0
                        else f'Queue finished with {errors} error(s)'
                    )
                    final = {
                        'status': final_status,
                        'message': final_message,
                        'pallets_processed': pallets_processed,
                        'hus_processed': hus_processed,
                        'errors': errors,
                    }
                    _ensure_queue_ownership(owner, 'emit_queue_done_idle_timeout')
                    emit_queue_done(final)
                    log.info("process_queue_task done result=%s", final)
                    return final

                time.sleep(QUEUE_IDLE_POLL_SECONDS)
                continue

            idle_deadline = None
            idle_status_sent = False
            _clear_queue_idle_deadline(owner=owner)
            _set_processing_pallet_ids(pallet_ids, owner=owner)
            log.info("process_queue_task batch_start pallets=%s", pallet_ids)

            for pallet_id in pallet_ids:
                _ensure_queue_ownership(owner, 'pallet_loop')
                _touch_queue_worker_heartbeat(owner)
                if _stop_requested(owner):
                    final = _stopped_result(pallets_processed, hus_processed, errors)
                    _ensure_queue_ownership(owner, 'emit_queue_done_stop_pallet_loop')
                    emit_queue_done(final)
                    return final

                pallet = Pallet.objects.get(pk=pallet_id)
                item_ids = list(
                    pallet.items.filter(status=HUItem.STATUS_PENDING)
                    .order_by('added_at', 'id')
                    .values_list('id', flat=True)
                )

                if not item_ids:
                    if pallet_id not in pdf_ready_pallet_ids:
                        continue
                else:
                    _mark_pallet_processing_started(pallet, owner=owner)

                log.info(
                    "process_queue_task pallet_start pallet=%s hu_count=%s",
                    pallet_id,
                    len(item_ids),
                )

                for item_id in item_ids:
                    _ensure_queue_ownership(owner, 'hu_loop')
                    _touch_queue_worker_heartbeat(owner)
                    if _stop_requested(owner):
                        final = _stopped_result(pallets_processed, hus_processed, errors)
                        _ensure_queue_ownership(owner, 'emit_queue_done_stop_hu_loop')
                        emit_queue_done(final)
                        return final

                    item = _process_hu_item(
                        item_id,
                        run_f1=run_f1,
                        run_f2=run_f2,
                        run_pallet_boundary=False,
                        emit_pallet_completion=False,
                        owner=owner,
                    )
                    _ensure_queue_ownership(owner, 'hu_processed')
                    _touch_queue_worker_heartbeat(owner)
                    hus_processed += 1
                    if item and item.status in (HUItem.STATUS_ERROR, HUItem.STATUS_HU_NOT_FOUND):
                        errors += 1

                pallet_errors = pallet.items.filter(
                    status__in=[HUItem.STATUS_ERROR, HUItem.STATUS_HU_NOT_FOUND]
                ).count()
                if pallet_errors:
                    if run_pdf:
                        result = {
                            'status': 'error',
                            'message': f'PDF omitido: {pallet_errors} HU(s) con error en pallet {pallet_id}',
                            'marked': 0,
                        }
                        _save_pdf_result(pallet, result, None, owner=owner)
                        _ensure_queue_ownership(owner, 'emit_receipt_done_error')
                        emit_receipt_done(pallet_id, result)
                    else:
                        _mark_pallet_processing_finished(pallet, owner=owner)

                    _ensure_queue_ownership(owner, 'emit_queue_status_pallet_errors')
                    emit_queue_status(
                        f'Pallet P{pallet_id:02d} requiere revision: {pallet_errors} HU(s) con error.',
                        badge='REVISION',
                        mode='error',
                        footer=f'Revisa los errores de P{pallet_id:02d} antes de imprimir.',
                    )
                    log.warning(
                        "process_queue_task pallet_has_errors pallet=%s errors=%s",
                        pallet_id,
                        pallet_errors,
                    )
                    pallets_processed += 1
                    _ensure_queue_ownership(owner, 'emit_pallet_done_errors')
                    emit_pallet_done(pallet_id, pallet.items.count())
                    continue

                if run_pdf:
                    if _stop_requested(owner):
                        final = _stopped_result(pallets_processed, hus_processed, errors)
                        _ensure_queue_ownership(owner, 'emit_queue_done_stop_before_pdf')
                        emit_queue_done(final)
                        return final

                    _ensure_queue_ownership(owner, 'emit_queue_status_pdf')
                    emit_queue_status(
                        f'Pallet P{pallet_id:02d} listo. Generando ZE16/PDF e impresion.',
                        badge='PDF',
                        mode='running',
                        footer=f'Generando e imprimiendo recibo de P{pallet_id:02d}.',
                    )
                    _touch_queue_worker_heartbeat(owner)
                    result = _run_pallet_boundary(pallet_id, emit_completion=True, owner=owner)
                    _ensure_queue_ownership(owner, 'pallet_boundary_complete')
                    _touch_queue_worker_heartbeat(owner)
                    if result and result.get('status') != 'ok':
                        errors += 1
                        log.error(
                            "process_queue_task pallet_receipt_failed pallet=%s result=%s",
                            pallet_id,
                            result,
                        )
                        pallets_processed += 1
                        _ensure_queue_ownership(owner, 'emit_pallet_done_receipt_failed')
                        emit_pallet_done(pallet_id, pallet.items.count())
                        continue
                else:
                    _mark_pallet_processing_finished(pallet, owner=owner)

                pallets_processed += 1
                _ensure_queue_ownership(owner, 'emit_pallet_done')
                emit_pallet_done(pallet_id, pallet.items.count())
                log.info("process_queue_task pallet_done pallet=%s", pallet_id)

    except QueueOwnershipLost as e:
        log.warning("process_queue_task stopped_lost_ownership owner=%s error=%s", owner, e)
        return {'ok': False, 'stopped': True, 'error': str(e)}
    except Exception as e:
        log.exception("process_queue_task fatal_error")
        final = {
            'status': 'error',
            'message': str(e),
            'pallets_processed': pallets_processed,
            'hus_processed': hus_processed,
            'errors': errors + 1,
        }
        if _queue_lock_owned_by(owner):
            emit_queue_done(final)
        else:
            log.warning("process_queue_task fatal_done_skipped_lost_ownership owner=%s", owner)
        return final
    finally:
        if _queue_lock_owned_by(owner):
            _close_sap_after_queue_idle()
        else:
            log.warning("process_queue_task skip_sap_close_lost_ownership owner=%s", owner)
        heartbeat_stop.set()
        heartbeat_thread.join(timeout=1)
        _release_queue_lock(owner)


@shared_task(bind=True, max_retries=0)
def process_hu_task(self, hu_item_id: int, run_f1=True, run_f2=True):
    """
    Tarea de compatibilidad para una HU. La UI actual inicia process_queue_task.
    """
    owner = _new_queue_owner_token('process_hu_task', self.request.id)
    if not _acquire_queue_lock(owner):
        log.warning("process_hu_task skipped because queue lock is active hu_id=%s", hu_item_id)
        return {'ok': False, 'error': 'Queue processing already active'}

    try:
        return _process_hu_item(
            hu_item_id,
            run_f1=run_f1,
            run_f2=run_f2,
            run_pallet_boundary=True,
            emit_pallet_completion=True,
            owner=owner,
        )
    except QueueOwnershipLost as e:
        log.warning("process_hu_task stopped_lost_ownership owner=%s hu_id=%s error=%s", owner, hu_item_id, e)
        return {'ok': False, 'stopped': True, 'error': str(e)}
    finally:
        _release_queue_lock(owner)


def _process_hu_item(
    hu_item_id: int,
    run_f1=True,
    run_f2=True,
    run_pallet_boundary=True,
    emit_pallet_completion=True,
    owner: str | None = None,
):
    import pythoncom
    from core.hu_origins import detect_origin
    from core.sap_client import SAPClient, SAPConnectionError
    from queue_app.models import HUItem
    from queue_app.utils import emit_item_update, emit_queue_status, emit_stats_update

    pythoncom.CoInitialize()

    try:
        try:
            item = HUItem.objects.select_related('pallet').get(pk=hu_item_id)
        except HUItem.DoesNotExist:
            log.error("process_hu_item missing hu_id=%s", hu_item_id)
            return None

        if item.status != HUItem.STATUS_PENDING:
            log.warning("process_hu_item skip hu=%s status=%s", item.hu_code, item.status)
            return item

        origin = detect_origin(item.hu_code)

        _ensure_queue_ownership(owner, 'mark_hu_processing')
        item.status = HUItem.STATUS_PROCESSING
        item.processing_started_at = timezone.now()
        item.error_msg = ''
        item.pdf_status = ''
        item.pdf_msg = ''
        item.pdf_ms = 0
        _ensure_queue_ownership(owner, 'save_hu_processing')
        item.save(update_fields=[
            'status',
            'processing_started_at',
            'error_msg',
            'pdf_status',
            'pdf_msg',
            'pdf_ms',
        ])
        emit_item_update(item)
        emit_stats_update(item.pallet)

        sap = SAPClient()
        _ensure_queue_ownership(owner, 'emit_sap_connect_status')
        emit_queue_status(
            f'Conectando con SAP para HU {item.hu_code}.',
            badge='SAP',
            mode='running',
            footer=f'Preparando sesion SAP para Pallet P{item.pallet_id:02d}.',
            active_pallet_id=item.pallet_id,
            active_hu_count=1,
        )
        sap.connect()
        log.info("process_hu_item sap_connected hu=%s", item.hu_code)

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
            sap.setup_phase1(origin=origin)
            res1 = sap.process_hu_phase1(item.hu_code, origin=origin)
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
                item.save(update_fields=[
                    'status',
                    'phase1_msg',
                    'f1_done_at',
                    'processed_at',
                    'processing_ms',
                    'error_msg',
                ])
                emit_item_update(item)
                emit_stats_update(item.pallet)
                log.warning(
                    "process_hu_item f1_failed hu=%s reason=%s",
                    item.hu_code,
                    res1['message'],
                )
                if run_pallet_boundary:
                    _run_pallet_boundary(
                        item.pallet_id,
                        emit_completion=emit_pallet_completion,
                        owner=owner,
                    )
                return item

            if res1['status'] == 'duplicate':
                item.phase1_msg = 'Ya se hizo el Acknowledge'

        if run_f2:
            emit_queue_status(
                f'HU {item.hu_code}: {"F1 listo. " if run_f1 else ""}Iniciando F2 Separazione.',
                badge='F2',
                mode='running',
                footer='Procesando separazione en ZMMTIJSEP.',
                active_pallet_id=item.pallet_id,
                active_hu_count=1,
            )
            sap.setup_phase2(origin=origin)
            res2 = sap.process_hu_phase2(
                item.hu_code,
                phase2_wait=origin.phase2_wait,
                origin=origin,
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
                    if (not run_f1 or (res1 and res1['status'] == 'ok'))
                    else HUItem.STATUS_DUPLICATE
                )
            else:
                item.status = HUItem.STATUS_ERROR
                item.error_msg = res2['message']
        else:
            item.status = HUItem.STATUS_OK

        item.processed_at = timezone.now()
        item.processing_ms = _calculate_elapsed_ms(item.processing_started_at)
        if item.status in (HUItem.STATUS_OK, HUItem.STATUS_DUPLICATE):
            item.error_msg = ''
        elif not item.error_msg:
            item.error_msg = item.phase2_msg or item.phase1_msg or 'Error de procesamiento'
        _ensure_queue_ownership(owner, 'save_hu_final_result')
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
        emit_item_update(item)
        emit_stats_update(item.pallet)

        if run_pallet_boundary:
            _run_pallet_boundary(
                item.pallet_id,
                emit_completion=emit_pallet_completion,
                owner=owner,
            )

        return item

    except QueueOwnershipLost:
        log.warning("process_hu_item stopped_lost_ownership hu_id=%s owner=%s", hu_item_id, owner)
        raise
    except SAPConnectionError as e:
        log.error("process_hu_item sap_error hu_id=%s error=%s", hu_item_id, e)
        return _mark_item_error(hu_item_id, str(e), owner=owner)
    except Exception as e:
        log.exception("process_hu_item fatal_error hu_id=%s", hu_item_id)
        return _mark_item_error(hu_item_id, str(e), owner=owner)
    finally:
        pythoncom.CoUninitialize()


def _run_pallet_boundary(pallet_id: int, emit_completion=True, owner: str | None = None):
    """
    Ejecuta el recibo del pallet solo cuando todas sus HUs terminaron.
    """
    from queue_app.models import HUItem, Pallet

    try:
        pallet = Pallet.objects.get(pk=pallet_id)
    except Pallet.DoesNotExist:
        return {'status': 'error', 'message': f'Pallet {pallet_id} no existe', 'marked': 0}

    items = pallet.items.all()
    still_running = items.filter(
        status__in=[HUItem.STATUS_PENDING, HUItem.STATUS_PROCESSING]
    ).exists()

    if still_running:
        log.debug("_run_pallet_boundary pallet=%s still active, skip", pallet_id)
        return {'status': 'pending', 'message': 'Pallet still processing', 'marked': 0}

    if pallet.status == Pallet.STATUS_DONE and pallet.receipt_done_at:
        log.debug("_run_pallet_boundary pallet=%s already done, skip", pallet_id)
        return {'status': 'ok', 'message': 'Pallet already printed', 'marked': 0}

    valid_hus = list(
        items.filter(status__in=[HUItem.STATUS_OK, HUItem.STATUS_DUPLICATE])
        .values_list('hu_code', flat=True)
    )

    if not valid_hus:
        log.warning("_run_pallet_boundary pallet=%s without valid HUs", pallet_id)
        return {'status': 'error', 'message': 'Sin HUs validos para ZE16/PDF', 'marked': 0}

    log.info(
        "_run_pallet_boundary pallet=%s hu_count=%s -> ze16_pdf_task_sync",
        pallet_id,
        len(valid_hus),
    )
    _ensure_queue_ownership(owner, 'run_pallet_boundary')
    result = ze16_pdf_task_sync(pallet_id, emit_completion=emit_completion, owner=owner)

    if result.get('status') == 'ok':
        _ensure_queue_ownership(owner, 'mark_pallet_done')
        pallet.status = Pallet.STATUS_DONE
        pallet.save(update_fields=['status'])

    return result


def _build_hu_display_map(ze16_client, hu_codes: list[str]) -> dict[str, str]:
    """
    Mantiene las llaves HU normalizadas de SAP e imprime ITA sin ceros iniciales.

    ZE16 requiere HUs ITA de 20 digitos, pero el recibo debe mostrar la HU tal
    como la escanearon los operadores.
    """
    display_map = {}
    for hu_code in hu_codes:
        normalized_hu = ze16_client._normalize_hu(hu_code)
        display_hu = (
            normalized_hu.lstrip('0')
            if normalized_hu.lstrip('0').startswith('29')
            else hu_code.strip()
        )
        display_map[normalized_hu] = display_hu
    return display_map


def _ze16_receipt_retry_config() -> tuple[int, float]:
    try:
        attempts = int(getattr(settings, 'SAP_ZE16_RECEIPT_MAX_ATTEMPTS', 4))
    except (TypeError, ValueError):
        attempts = 4

    try:
        delay_seconds = float(getattr(settings, 'SAP_ZE16_RECEIPT_RETRY_DELAY_SECONDS', 5))
    except (TypeError, ValueError):
        delay_seconds = 5

    return max(1, attempts), max(0.0, delay_seconds)


def _get_ze16_receipts_with_retries(
    ze16,
    hu_codes: list[str],
    *,
    pallet_id: int,
    hu_count: int,
    owner: str | None = None,
    status_callback=None,
    sleep_func=time.sleep,
) -> tuple[dict[str, str], int]:
    attempts, delay_seconds = _ze16_receipt_retry_config()

    for attempt in range(1, attempts + 1):
        _ensure_queue_ownership(owner, f'ze16_receipt_attempt_{attempt}')
        if status_callback:
            status_callback(
                f'Pallet P{pallet_id:02d}: consultando receipts en ZE16 ({attempt}/{attempts}).',
                badge='ZE16',
                mode='running',
                footer=f'Buscando receipts para {hu_count} HU(s).',
                active_pallet_id=pallet_id,
                active_hu_count=hu_count,
            )

        receipts = ze16.get_receipts_for_pallet(hu_codes)
        _ensure_queue_ownership(owner, f'ze16_receipts_received_{attempt}')
        if receipts:
            return receipts, attempt

        log.warning(
            "ze16_receipts_empty pallet=%s attempt=%s/%s",
            pallet_id,
            attempt,
            attempts,
        )
        if attempt < attempts and delay_seconds:
            if status_callback:
                status_callback(
                    f'Pallet P{pallet_id:02d}: ZE16 no devolvio receipts. Reintentando.',
                    badge='ZE16',
                    mode='running',
                    footer=f'Siguiente intento en {delay_seconds:g}s.',
                    active_pallet_id=pallet_id,
                    active_hu_count=hu_count,
                )
            sleep_func(delay_seconds)

    return {}, attempts


def ze16_pdf_task_sync(pallet_id: int, emit_completion=True, owner: str | None = None):
    """
    Paso sincronico ZE16 + PDF + impresion. Retorna solo despues de print_pdf.
    """
    import pythoncom
    from core.hu_origins import detect_origin, resolve_effective_origin
    from core.pdf_receipt import PalletReceiptPDF
    from core.sap_client import SAPClient
    from core.ze16_client import ZE16Client, ZE16Error
    from queue_app.models import HUItem, Pallet
    from queue_app.utils import emit_queue_status, emit_receipt_done

    pythoncom.CoInitialize()
    result = {'status': 'error', 'message': 'Unknown ZE16/PDF error', 'marked': 0}
    pdf_started = None

    try:
        _ensure_queue_ownership(owner, 'ze16_pdf_start')
        pallet = Pallet.objects.get(pk=pallet_id)
        hu_codes = list(
            pallet.items.filter(status__in=[HUItem.STATUS_OK, HUItem.STATUS_DUPLICATE])
            .values_list('hu_code', flat=True)
        )
        hu_count = len(hu_codes)

        if not hu_codes:
            result = {'status': 'error', 'message': 'Sin HUs validos para ZE16', 'marked': 0}
            _save_pdf_result(pallet, result, pdf_started, owner=owner)
            return result

        origin = detect_origin(hu_codes[0])
        effective_origin = resolve_effective_origin(origin, hu_count)

        emit_queue_status(
            f'Pallet P{pallet_id:02d}: conectando SAP para ZE16.',
            badge='ZE16',
            mode='running',
            footer='Preparando consulta de receipts.',
            active_pallet_id=pallet_id,
            active_hu_count=hu_count,
        )
        sap = SAPClient()
        sap.connect()
        _ensure_queue_ownership(owner, 'ze16_sap_connected')
        printed_by = getattr(settings, 'SAP_LOGIN_USER', '') or sap.get_user()

        ze16 = ZE16Client(sap.session)
        receipts, receipt_attempts = _get_ze16_receipts_with_retries(
            ze16,
            hu_codes,
            pallet_id=pallet_id,
            hu_count=hu_count,
            owner=owner,
            status_callback=emit_queue_status,
        )
        hu_display_map = _build_hu_display_map(ze16, hu_codes)

        if not receipts:
            result = {
                'status': 'error',
                'message': (
                    f'ZE16: sin Receipt IDs para pallet {pallet_id} '
                    f'tras {receipt_attempts} intento(s)'
                ),
                'marked': 0,
            }
            _save_pdf_result(pallet, result, pdf_started, owner=owner)
            return result

        pdf_started = time.perf_counter()
        emit_queue_status(
            f'Pallet P{pallet_id:02d}: generando PDF de recibo.',
            badge='PDF',
            mode='running',
            footer=f'Creando recibo con {len(receipts)} receipt(s).',
            active_pallet_id=pallet_id,
            active_hu_count=hu_count,
        )
        pdf_path = PalletReceiptPDF.generate(
            pallet_id=pallet_id,
            origin_label=effective_origin.label,
            receipts=receipts,
            hu_display_map=hu_display_map,
            printed_by=printed_by,
        )
        _ensure_queue_ownership(owner, 'pdf_generated')
        emit_queue_status(
            f'Pallet P{pallet_id:02d}: enviando PDF a impresora.',
            badge='PRINT',
            mode='running',
            footer='Esperando confirmacion del sistema de impresion.',
            active_pallet_id=pallet_id,
            active_hu_count=hu_count,
        )
        printed = PalletReceiptPDF.print_pdf(pdf_path)
        _ensure_queue_ownership(owner, 'pdf_printed')

        if not printed:
            result = {
                'status': 'error',
                'message': f'PDF generado pero no confirmado por impresora: {pdf_path}',
                'marked': 0,
            }
            _save_pdf_result(pallet, result, pdf_started, owner=owner)
            return result

        found = len(receipts)
        suffix = f" ({found}/{hu_count} con receipt)" if found < hu_count else ""
        result = {
            'status': 'ok',
            'message': f'ZE16+PDF OK - {found} recibos{suffix}',
            'marked': found,
        }

        _save_pdf_result(pallet, result, pdf_started, owner=owner)

        log.info("ze16_pdf_task_sync done pallet=%s result=%s", pallet_id, result)
        return result

    except QueueOwnershipLost:
        log.warning("ze16_pdf_task_sync stopped_lost_ownership pallet=%s owner=%s", pallet_id, owner)
        raise
    except ZE16Error as e:
        log.error("ze16_pdf_task_sync ZE16Error pallet=%s error=%s", pallet_id, e)
        result = {'status': 'error', 'message': f'ZE16 Error: {e}', 'marked': 0}
        if 'pallet' in locals():
            _save_pdf_result(pallet, result, pdf_started, owner=owner)
        return result
    except Exception as e:
        log.exception("ze16_pdf_task_sync error pallet=%s", pallet_id)
        result = {'status': 'error', 'message': str(e), 'marked': 0}
        if 'pallet' in locals():
            _save_pdf_result(pallet, result, pdf_started, owner=owner)
        return result
    finally:
        if emit_completion and (not owner or _queue_lock_owned_by(owner)):
            emit_receipt_done(pallet_id, result)
        pythoncom.CoUninitialize()


def _save_pdf_result(pallet, result: dict, started_at: float | None, owner: str | None = None) -> dict:
    """Guarda resultado y duracion del tramo PDF: generar archivo + imprimir."""
    from queue_app.models import HUItem, Pallet

    _ensure_queue_ownership(owner, 'save_pdf_result')
    pdf_ms = max(0, int((time.perf_counter() - started_at) * 1000)) if started_at else 0
    result['pdf_ms'] = pdf_ms

    pallet.pdf_status = (
        Pallet.PDF_STATUS_OK
        if result.get('status') == 'ok'
        else Pallet.PDF_STATUS_ERROR
    )
    pallet.pdf_msg = (result.get('message') or '')[:255]
    pallet.pdf_ms = pdf_ms
    completed_at = timezone.now()
    pallet.processing_finished_at = completed_at

    update_fields = ['pdf_status', 'pdf_msg', 'pdf_ms', 'processing_finished_at']
    if result.get('status') == 'ok':
        pallet.receipt_done_at = completed_at
        update_fields.append('receipt_done_at')

    item_updates = {
        'pdf_status': pallet.pdf_status,
        'pdf_msg': pallet.pdf_msg,
        'pdf_ms': pdf_ms,
    }
    if completed_at:
        item_updates['receipt_done_at'] = completed_at
    pallet.items.all().update(**item_updates)

    pallet.save(update_fields=update_fields)
    result['pdf_status'] = pallet.pdf_status
    result['pdf_display'] = pallet.pdf_display
    result['pdf_msg'] = pallet.pdf_msg
    result['processing_time_display'] = pallet.processing_time_display
    result['processing_started_at'] = (
        pallet.processing_started_at.isoformat()
        if pallet.processing_started_at
        else ''
    )
    result['processing_finished_at'] = (
        pallet.processing_finished_at.isoformat()
        if pallet.processing_finished_at
        else ''
    )
    result['receipt_done_at'] = pallet.receipt_done_at.isoformat() if pallet.receipt_done_at else ''
    return result


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
        emit_item_update(item)
        emit_stats_update(item.pallet)
        return item
    except Exception:
        log.exception("_mark_item_error failed hu_id=%s", hu_item_id)
        return None
