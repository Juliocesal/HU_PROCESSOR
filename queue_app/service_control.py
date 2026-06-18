import socket
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

from celery import current_app
from django.conf import settings


CELERY_PAUSED_KEY = 'nexhus:service_control:celery_paused'
SERVICE_ACTION_TIMEOUT_SECONDS = 3


@dataclass(frozen=True)
class ServiceCommandResult:
    ok: bool
    message: str
    action: str = ''
    level: str = 'info'

    def as_dict(self) -> dict:
        return {
            'ok': self.ok,
            'message': self.message,
            'action': self.action,
            'level': self.level,
        }


def service_control_enabled() -> bool:
    return bool(
        getattr(settings, 'NEXHUS_SERVICE_CONTROL_ENABLED', False)
        and getattr(settings, 'NEXHUS_SERVICE_CONTROL_PASSWORD', '')
    )


def service_control_not_configured_message() -> str:
    return (
        'Control de servicios no configurado. Define '
        'NEXHUS_SERVICE_CONTROL_PASSWORD y NEXHUS_SERVICE_CONTROL_ENABLED=true.'
    )


def get_services_status() -> dict:
    redis_status = _redis_status()
    celery_status = _celery_status(redis_status['ok'])
    daphne_status = _daphne_status()
    return {
        'redis': redis_status,
        'celery': celery_status,
        'daphne': daphne_status,
    }


def execute_service_action(service: str, action: str) -> ServiceCommandResult:
    service = (service or '').strip().lower()
    action = (action or '').strip().lower()

    handlers = {
        'redis': {
            'start': _start_redis,
            'restart': _restart_redis,
        },
        'celery': {
            'start': _start_celery,
            'stop': _stop_celery,
            'pause': _pause_celery,
            'resume': _resume_celery,
            'restart': _restart_celery,
        },
        'daphne': {},
    }

    handler = handlers.get(service, {}).get(action)
    if not handler:
        return ServiceCommandResult(
            ok=False,
            message='Accion no soportada para este servicio.',
            action='Usa una accion disponible en la tarjeta del servicio.',
            level='warn',
        )

    try:
        return handler()
    except Exception as exc:
        return ServiceCommandResult(
            ok=False,
            message='No se pudo ejecutar la accion del servicio.',
            action=str(exc)[:180],
            level='error',
        )


def _redis_client(timeout=1):
    import redis

    return redis.Redis.from_url(
        settings.REDIS_URL,
        socket_connect_timeout=timeout,
        socket_timeout=timeout,
    )


def _redis_status() -> dict:
    started = time.perf_counter()
    try:
        info = _redis_client(timeout=1).info(section='server')
        return {
            'key': 'redis',
            'label': 'Redis',
            'ok': True,
            'state': 'running',
            'level': 'ok',
            'message': 'Redis activo.',
            'detail': f"version={info.get('redis_version', 'n/a')}",
            'latency_ms': round((time.perf_counter() - started) * 1000, 1),
            'actions': _service_actions(
                start=False,
                stop=False,
                pause=False,
                resume=False,
                restart=True,
            ),
        }
    except Exception as exc:
        return {
            'key': 'redis',
            'label': 'Redis',
            'ok': False,
            'state': 'stopped',
            'level': 'error',
            'message': 'Redis no responde.',
            'detail': str(exc)[:180],
            'latency_ms': round((time.perf_counter() - started) * 1000, 1),
            'actions': _service_actions(
                start=True,
                stop=False,
                pause=False,
                resume=False,
                restart=False,
            ),
        }


def _celery_status(redis_ok: bool) -> dict:
    if not redis_ok:
        return {
            'key': 'celery',
            'label': 'Celery',
            'ok': False,
            'state': 'unknown',
            'level': 'error',
            'message': 'No se puede verificar Celery sin Redis.',
            'detail': 'Inicia Redis antes de Celery.',
            'workers': [],
            'actions': _service_actions(
                start=False,
                stop=False,
                pause=False,
                resume=False,
                restart=False,
            ),
        }

    try:
        ping = current_app.control.inspect(timeout=1).ping() or {}
        workers = sorted(ping.keys())
        paused = _is_celery_paused()

        if not workers:
            return {
                'key': 'celery',
                'label': 'Celery',
                'ok': False,
                'state': 'stopped',
                'level': 'error',
                'message': 'Celery apagado.',
                'detail': 'No hay workers activos.',
                'workers': [],
                'actions': _service_actions(
                    start=True,
                    stop=False,
                    pause=False,
                    resume=False,
                    restart=False,
                ),
            }

        return {
            'key': 'celery',
            'label': 'Celery',
            'ok': True,
            'state': 'paused' if paused else 'running',
            'level': 'warn' if paused else 'ok',
            'message': 'Celery pausado.' if paused else 'Celery activo.',
            'detail': ', '.join(workers),
            'workers': workers,
            'actions': _service_actions(
                start=False,
                stop=True,
                pause=not paused,
                resume=paused,
                restart=True,
            ),
        }
    except Exception as exc:
        return {
            'key': 'celery',
            'label': 'Celery',
            'ok': False,
            'state': 'unknown',
            'level': 'error',
            'message': 'No se pudo consultar Celery.',
            'detail': str(exc)[:180],
            'workers': [],
            'actions': _service_actions(
                start=True,
                stop=False,
                pause=False,
                resume=False,
                restart=False,
            ),
        }


def _daphne_status() -> dict:
    host = '127.0.0.1'
    port = int(getattr(settings, 'NEXHUS_DAPHNE_PORT', 8000))
    running = _tcp_port_open(host, port)
    return {
        'key': 'daphne',
        'label': 'Daphne',
        'ok': running,
        'state': 'running' if running else 'stopped',
        'level': 'ok' if running else 'error',
        'message': f'Daphne responde en puerto {port}.' if running else f'Daphne no responde en puerto {port}.',
        'detail': 'Daphne solo muestra estado. Reiniciar desde consola o supervisor del servidor.',
        'actions': _service_actions(
            start=False,
            stop=False,
            pause=False,
            resume=False,
            restart=False,
        ),
    }


def _service_actions(*, start: bool, stop: bool, pause: bool, resume: bool, restart: bool) -> dict:
    return {
        'start': bool(start),
        'stop': bool(stop),
        'pause': bool(pause),
        'resume': bool(resume),
        'restart': bool(restart),
    }


def _tcp_port_open(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


def _start_redis() -> ServiceCommandResult:
    if _redis_status()['ok']:
        return ServiceCommandResult(True, 'Redis ya esta activo.', level='ok')

    redis_exe = Path(getattr(settings, 'NEXHUS_REDIS_EXE', ''))
    if not redis_exe.exists():
        return ServiceCommandResult(
            False,
            'No se encontro redis-server.exe.',
            f'Configura NEXHUS_REDIS_EXE. Ruta actual: {redis_exe}',
            'error',
        )

    _run_detached_command(
        title='NEXHUS Redis',
        command=(
            f'cd /d "{redis_exe.parent}"\n'
            f'"{redis_exe}"'
        ),
    )
    return ServiceCommandResult(True, 'Redis iniciado.', 'Espera unos segundos y actualiza estado.', 'ok')


def _stop_redis() -> ServiceCommandResult:
    try:
        _redis_client(timeout=1).shutdown(nosave=True)
    except Exception:
        if not _redis_status()['ok']:
            return ServiceCommandResult(True, 'Redis detenido.', level='ok')
        raise

    return ServiceCommandResult(True, 'Solicitud de apagado enviada a Redis.', level='ok')


def _restart_redis() -> ServiceCommandResult:
    if _redis_status()['ok']:
        _stop_redis()
        _wait_for_status(lambda: not _redis_status()['ok'], timeout_seconds=5)

    result = _start_redis()
    if not result.ok:
        return result

    return ServiceCommandResult(
        True,
        'Redis reiniciado.',
        'Espera unos segundos y actualiza estado.',
        'ok',
    )


def _start_celery() -> ServiceCommandResult:
    if not _redis_status()['ok']:
        return ServiceCommandResult(False, 'No se puede iniciar Celery sin Redis activo.', 'Inicia Redis primero.', 'error')

    if _celery_status(redis_ok=True)['ok']:
        return ServiceCommandResult(True, 'Celery ya esta activo.', level='ok')

    activate = Path(settings.BASE_DIR) / 'venv' / 'Scripts' / 'activate.bat'
    if not activate.exists():
        return ServiceCommandResult(
            False,
            'No se encontro el entorno virtual.',
            f'Ruta esperada: {activate}',
            'error',
        )

    _run_detached_command(
        title='NEXHUS Celery',
        command=(
            f'cd /d "{settings.BASE_DIR}"\n'
            f'call "{activate}"\n'
            'celery -A config worker -l info -P solo --concurrency=1'
        ),
    )
    return ServiceCommandResult(True, 'Celery iniciado.', 'Espera unos segundos y actualiza estado.', 'ok')


def _stop_celery() -> ServiceCommandResult:
    if not _redis_status()['ok']:
        return ServiceCommandResult(False, 'No se puede detener Celery porque Redis no responde.', level='error')

    current_app.control.broadcast('shutdown', reply=False)
    _clear_celery_paused()
    return ServiceCommandResult(True, 'Solicitud de apagado enviada a Celery.', level='ok')


def _restart_celery() -> ServiceCommandResult:
    if not _redis_status()['ok']:
        return ServiceCommandResult(False, 'No se puede reiniciar Celery sin Redis activo.', 'Inicia Redis primero.', 'error')

    if _celery_status(redis_ok=True)['ok']:
        _stop_celery()
        _wait_for_status(lambda: not _celery_status(redis_ok=True)['ok'], timeout_seconds=8)

    result = _start_celery()
    if not result.ok:
        return result

    return ServiceCommandResult(
        True,
        'Celery reiniciado.',
        'Espera unos segundos y actualiza estado.',
        'ok',
    )


def _pause_celery() -> ServiceCommandResult:
    if not _redis_status()['ok']:
        return ServiceCommandResult(False, 'No se puede pausar Celery sin Redis activo.', level='error')

    current_app.control.cancel_consumer('celery', reply=True, timeout=SERVICE_ACTION_TIMEOUT_SECONDS)
    _set_celery_paused(True)
    return ServiceCommandResult(True, 'Celery pausado.', 'No tomara tareas nuevas hasta reanudar.', 'warn')


def _resume_celery() -> ServiceCommandResult:
    if not _redis_status()['ok']:
        return ServiceCommandResult(False, 'No se puede reanudar Celery sin Redis activo.', level='error')

    current_app.control.add_consumer('celery', reply=True, timeout=SERVICE_ACTION_TIMEOUT_SECONDS)
    _clear_celery_paused()
    return ServiceCommandResult(True, 'Celery reanudado.', 'Puede tomar tareas nuevas.', 'ok')


def _start_daphne() -> ServiceCommandResult:
    if _daphne_status()['ok']:
        return ServiceCommandResult(True, 'Daphne ya esta activo.', level='ok')

    activate = Path(settings.BASE_DIR) / 'venv' / 'Scripts' / 'activate.bat'
    if not activate.exists():
        return ServiceCommandResult(
            False,
            'No se encontro el entorno virtual.',
            f'Ruta esperada: {activate}',
            'error',
        )

    bind = getattr(settings, 'NEXHUS_DAPHNE_BIND', '0.0.0.0')
    port = int(getattr(settings, 'NEXHUS_DAPHNE_PORT', 8000))
    _run_detached_command(
        title='NEXHUS Daphne',
        command=(
            f'cd /d "{settings.BASE_DIR}"\n'
            f'call "{activate}"\n'
            f'daphne -b {bind} -p {port} config.asgi:application'
        ),
    )
    return ServiceCommandResult(True, 'Daphne iniciado.', 'Si el puerto estaba libre, la UI quedara disponible en unos segundos.', 'ok')


def _stop_daphne() -> ServiceCommandResult:
    return ServiceCommandResult(
        False,
        'Apagar Daphne desde esta misma UI esta deshabilitado.',
        'Si Daphne se apaga, esta pagina ya no puede volver a encenderlo. Usa un supervisor externo.',
        'warn',
    )


def _pause_daphne() -> ServiceCommandResult:
    return ServiceCommandResult(False, 'Daphne no soporta pausa desde la UI.', level='warn')


def _resume_daphne() -> ServiceCommandResult:
    return ServiceCommandResult(False, 'Daphne no soporta reanudar desde la UI.', level='warn')


def _run_detached_command(*, title: str, command: str) -> None:
    creationflags = getattr(subprocess, 'CREATE_NEW_CONSOLE', 0)
    batch_path = _write_service_batch(title=title, command=command)
    subprocess.Popen(
        ['cmd.exe', '/k', str(batch_path)],
        cwd=str(settings.BASE_DIR),
        creationflags=creationflags,
        close_fds=False,
    )


def _write_service_batch(*, title: str, command: str) -> Path:
    runtime_dir = Path(getattr(settings, 'LOCAL_DATA_DIR', settings.BASE_DIR)) / 'service_control'
    runtime_dir.mkdir(parents=True, exist_ok=True)
    safe_name = ''.join(char if char.isalnum() else '_' for char in title).strip('_') or 'service'
    batch_path = runtime_dir / f'{safe_name}.bat'
    batch_path.write_text(
        '\n'.join([
            '@echo off',
            f'title {title}',
            command,
            '',
        ]),
        encoding='utf-8',
    )
    return batch_path


def _wait_for_status(predicate, *, timeout_seconds: float) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.5)
    return False


def _is_celery_paused() -> bool:
    try:
        return bool(_redis_client(timeout=1).exists(CELERY_PAUSED_KEY))
    except Exception:
        return False


def _set_celery_paused(paused: bool) -> None:
    try:
        client = _redis_client(timeout=1)
        if paused:
            client.set(CELERY_PAUSED_KEY, '1', ex=60 * 60 * 24)
        else:
            client.delete(CELERY_PAUSED_KEY)
    except Exception:
        pass


def _clear_celery_paused() -> None:
    _set_celery_paused(False)
