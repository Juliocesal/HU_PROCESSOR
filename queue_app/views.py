import csv
import json
import logging
import os
import time
from datetime import datetime
from django.conf import settings
from django.core.management.color import no_style
from django.db import connection
from django.http import JsonResponse, HttpResponse
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_POST, require_GET
from django.shortcuts import render
from django.utils import timezone

from core.hu_origins import detect_origin, is_pallet_separator

from queue_app.models import HUItem, Pallet, ScanLog
from queue_app.tasks import (
    arm_continuous_queue,
    clear_queue_runtime_state,
    disarm_continuous_queue,
    get_queue_idle_remaining_seconds,
    get_processing_pallet_ids,
    get_queue_stop_request_age_seconds,
    get_queue_worker_heartbeat_age_seconds,
    is_continuous_queue_armed,
    is_queue_locked,
    is_queue_stop_requested,
    process_queue_task,
    QUEUE_WORKER_HEARTBEAT_STALE_SECONDS,
    request_queue_stop,
)
from queue_app.utils import (
    calculate_queue_operational_status,
    calculate_queue_stats,
    emit_hu_deleted,
    emit_current_queue_status_if_available,
    emit_item_update,
    emit_pallet_created,
    emit_pallet_deleted,
    emit_queue_done,
    emit_queue_status,
    emit_queue_cleared,
    emit_stats_update,
    pallets_ready_for_pdf_queryset,
)

log = logging.getLogger(__name__)

QUEUE_CONTINUOUS_IDLE_TIMEOUT_SECONDS = 360
QUEUE_SAFE_STOP_GRACE_SECONDS = 30
CELERY_NO_WORKER_MESSAGE = (
    'Celery apagado. Redis responde, pero no hay workers activos. '
    'La cola NO se procesara hasta iniciar Celery.'
)
CELERY_START_BLOCKED_MESSAGE = (
    'No se puede iniciar proceso: Celery no esta activo. '
    'Redis esta conectado, pero ningun worker respondio.'
)
HU_CODE_MIN_LENGTH = 10
HU_CODE_MAX_LENGTH = 15
REPROCESS_ERROR_STATUSES = [HUItem.STATUS_ERROR, HUItem.STATUS_HU_NOT_FOUND]
REPROCESS_ALL_STATUSES = [
    HUItem.STATUS_OK,
    HUItem.STATUS_DUPLICATE,
    HUItem.STATUS_ERROR,
    HUItem.STATUS_HU_NOT_FOUND,
]


# ══════════════════════════════════════════════════════════════════════════════
#  PÁGINA PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

@ensure_csrf_cookie
def queue_view(request):
    """
    Renderiza la UI principal.
    Equivale a run_queue_app() — muestra la ventana con la cola actual.
    """
    pallets = Pallet.objects.prefetch_related('items').order_by('id')
    stats   = _get_stats()

    return render(request, 'queue_app/queue.html', {
        'pallets': pallets,
        'stats':   stats,
    })


# ══════════════════════════════════════════════════════════════════════════════
#  SCAN — escanear HU o separador de pallet
# ══════════════════════════════════════════════════════════════════════════════


@require_POST
def scan_hu(request):
    """
    Equivale a _on_scan() en queue_window.py.
    Recibe el código escaneado y decide si es HU o separador de pallet.
    """
    try:
        payload = json.loads(request.body)
        if not isinstance(payload, dict):
            raise ValueError
        raw = payload.get('code', '').strip()
    except (json.JSONDecodeError, ValueError, AttributeError):
        return JsonResponse({'ok': False, 'error': 'JSON inválido'}, status=400)

    run_options = _run_options_from_payload(payload)

    if not raw:
        return JsonResponse({'ok': False, 'error': 'Código vacío'}, status=400)

    # ── Separador de pallet ───────────────────────────────────────────────────
    if is_pallet_separator(raw):
        result = _new_pallet_logic(**run_options)
        if result.get('ok'):
            result['stats'] = _get_stats()
        return JsonResponse(result)

    # ── Validar longitud — misma regla que _on_scan() ────────────────────────
    if not (HU_CODE_MIN_LENGTH <= len(raw) <= HU_CODE_MAX_LENGTH):
        return JsonResponse({
            'ok':      False,
            'error':   (
                f"Código HU inválido: '{raw}' tiene {len(raw)} caracteres. "
                f"Rango permitido: {HU_CODE_MIN_LENGTH}-{HU_CODE_MAX_LENGTH}"
            ),
            'type':    'validation_error',
        }, status=400)

    # ── Duplicado ─────────────────────────────────────────────────────────────
    if HUItem.objects.filter(hu_code=raw).exists():
        ScanLog.objects.create(hu_code=raw, result='duplicate', message='Ya existe en cola')
        return JsonResponse({
            'ok':    False,
            'error': f'Duplicado: {raw}',
            'type':  'duplicate',
        }, status=409)

    # ── Crear HUItem en DB ────────────────────────────────────────────────────
    origin  = detect_origin(raw)
    pallet  = _get_or_create_active_pallet(raw, origin)
    item    = HUItem.objects.create(
        hu_code     = raw,
        pallet      = pallet,
        origin_code = origin.code,
        status      = HUItem.STATUS_PENDING,
    )
    ScanLog.objects.create(hu_code=raw, result='queued', message=f'Pallet {pallet.pk}')
    auto_start_result = {}
    if origin.auto_pallet:
        _mark_pallet_ready(pallet)
        auto_start_result = _auto_start_queue_after_pallet_close(**run_options)

    emit_item_update(item)
    emit_stats_update(item.pallet)
    emit_current_queue_status_if_available()

    log.info("scan_hu hu=%s pallet=%s origin=%s", raw, pallet.pk, origin.code)

    return JsonResponse({
        'ok':        True,
        'hu_code':   raw,
        'pallet_id': pallet.pk,
        'origin':    origin.label,
        'status':    item.status,
        'stats':     _get_stats(),
        **auto_start_result,
    })


# ══════════════════════════════════════════════════════════════════════════════
#  PALLET — crear nuevo pallet manualmente
# ══════════════════════════════════════════════════════════════════════════════


@require_POST
def new_pallet(request):
    """
    Equivale a _new_pallet() en queue_window.py.
    Crea un nuevo pallet si el actual tiene HUs.
    """
    run_options = _run_options_from_payload(_parse_json_body(request))
    result = _new_pallet_logic(**run_options)
    if result.get('ok'):
        result['stats'] = _get_stats()
    return JsonResponse(result)


def _new_pallet_logic(run_f1=True, run_f2=True, run_pdf=True) -> dict:
    """Lógica compartida entre scan (separador) y botón nuevo pallet."""
    active = Pallet.objects.filter(status=Pallet.STATUS_ACTIVE).order_by('-id').first()

    # Si el pallet activo está vacío, no crear uno nuevo
    if active and not active.items.exists():
        return {
            'ok':        False,
            'error':     f'El pallet actual (P{active.pk:02d}) está vacío. Escanea un HU primero.',
            'type':      'empty_pallet',
            'pallet_id': active.pk,
        }

    if active:
        _mark_pallet_ready(active)

    pallet = Pallet.objects.create(status=Pallet.STATUS_ACTIVE)
    auto_start_result = _auto_start_queue_after_pallet_close(
        run_f1=run_f1,
        run_f2=run_f2,
        run_pdf=run_pdf,
    )
    emit_pallet_created(pallet)
    log.info("new_pallet created id=%s", pallet.pk)

    return {
        'ok':        True,
        'pallet_id': pallet.pk,
        'message':   f'Nuevo pallet #{pallet.pk} iniciado.',
        **auto_start_result,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  BORRAR HU / PALLET
# ══════════════════════════════════════════════════════════════════════════════


@require_POST
def delete_hu(request, hu_code):
    """
    Equivale a _delete_hu_row() en queue_window.py.
    Solo permite borrar HUs en estado pending o error.
    """
    queue_error = _queue_mutation_error('borrar HUs')
    if queue_error:
        return JsonResponse({'ok': False, 'error': queue_error, 'type': 'queue_running'}, status=409)

    try:
        item = HUItem.objects.get(hu_code=hu_code)
    except HUItem.DoesNotExist:
        return JsonResponse({'ok': False, 'error': 'HU no encontrada'}, status=404)

    if item.status in (HUItem.STATUS_PROCESSING, HUItem.STATUS_OK, HUItem.STATUS_DUPLICATE):
        return JsonResponse({
            'ok':    False,
            'error': f'HU ya procesada ({item.status_display}) — no se puede borrar',
        }, status=409)

    pallet = item.pallet
    item.delete()
    pdf_reset = _recalculate_pallet_after_hu_delete(pallet)
    log.info("delete_hu hu=%s pallet=%s", hu_code, pallet.pk)

    # Si el pallet quedó vacío y no es el único, borrarlo también
    if not pallet.items.exists() and Pallet.objects.count() > 1:
        pallet.delete()
        stats = _get_stats()
        emit_hu_deleted(hu_code, pallet.pk, pallet_deleted=True, stats=stats)
        emit_stats_update(None)
        return JsonResponse({
            'ok': True,
            'pallet_deleted': True,
            'stats': stats,
        })

    for remaining_item in pallet.items.select_related('pallet'):
        emit_item_update(remaining_item)
    stats = _get_stats()
    emit_hu_deleted(hu_code, pallet.pk, stats=stats)
    emit_stats_update(pallet)

    return JsonResponse({
        'ok': True,
        'pallet_deleted': False,
        'pdf_reset': pdf_reset,
        'pallet_id': pallet.pk,
        'pdf_status': pallet.pdf_status,
        'pdf_display': pallet.pdf_display,
        'pdf_msg': pallet.pdf_msg,
        'stats': stats,
    })


def _recalculate_pallet_after_hu_delete(pallet: Pallet) -> bool:
    """Limpia el estado PDF si el pallet vuelve a ser imprimible tras borrar una HU."""
    pallet.refresh_from_db()
    if not pallet.items.exists():
        return False

    has_errors = pallet.items.filter(
        status__in=[HUItem.STATUS_ERROR, HUItem.STATUS_HU_NOT_FOUND]
    ).exists()
    if has_errors or pallet.pdf_status == Pallet.PDF_STATUS_OK:
        return False

    if pallet.pdf_status or pallet.pdf_msg or pallet.pdf_ms:
        pallet.pdf_status = ''
        pallet.pdf_msg = ''
        pallet.pdf_ms = 0
        pallet.receipt_done_at = None
        pallet.processing_finished_at = None
        pallet.status = Pallet.STATUS_READY
        pallet.save(update_fields=[
            'pdf_status',
            'pdf_msg',
            'pdf_ms',
            'receipt_done_at',
            'processing_finished_at',
            'status',
        ])
        return True

    return False



@require_POST
def delete_pallet(request, pallet_id):
    """
    Equivale a _delete_pallet() en queue_window.py.
    Borra el pallet y todos sus HUs si es seguro.
    """
    queue_error = _queue_mutation_error('borrar pallets')
    if queue_error:
        return JsonResponse({'ok': False, 'error': queue_error, 'type': 'queue_running'}, status=409)

    try:
        pallet = Pallet.objects.get(pk=pallet_id)
    except Pallet.DoesNotExist:
        return JsonResponse({'ok': False, 'error': 'Pallet no encontrado'}, status=404)

    if not pallet.is_safe_to_delete:
        return JsonResponse({
            'ok':    False,
            'error': 'Pallet en proceso — no se puede borrar',
        }, status=409)

    count = pallet.items.count()
    pallet.delete()   # CASCADE elimina los HUItems
    stats = _get_stats()
    emit_pallet_deleted(pallet_id, stats=stats)
    log.info("delete_pallet id=%s hu_count=%s", pallet_id, count)

    return JsonResponse({'ok': True, 'deleted_hus': count, 'stats': stats})


# ══════════════════════════════════════════════════════════════════════════════
#  LIMPIAR / REPROCESAR
# ══════════════════════════════════════════════════════════════════════════════


@require_POST
def clear_queue(request):
    """Equivale a _clear() — borra toda la cola."""
    if is_queue_locked():
        return JsonResponse({
            'ok': False,
            'error': 'Hay una tarea de procesamiento activa. Detenla y espera la confirmacion antes de limpiar.',
        }, status=409)

    # No permitir si hay HUs procesando
    if HUItem.objects.filter(status=HUItem.STATUS_PROCESSING).exists():
        return JsonResponse({
            'ok':    False,
            'error': 'Hay HUs procesando — detén el proceso antes de limpiar',
        }, status=409)

    disarm_continuous_queue()
    deleted_items   = HUItem.objects.all().delete()
    deleted_pallets = Pallet.objects.all().delete()
    _reset_queue_sequences()
    # Crear pallet inicial vacío
    pallet = Pallet.objects.create(status=Pallet.STATUS_ACTIVE)
    stats = _get_stats()
    emit_queue_cleared(pallet.pk, stats=stats)

    log.info("clear_queue done reset_pallet_id=%s", pallet.pk)
    return JsonResponse({'ok': True, 'pallet_id': pallet.pk, 'stats': stats})


def _reset_queue_sequences():
    """Reinicia los IDs de la cola para que Limpiar HUs vuelva a P01."""
    table_names = [Pallet._meta.db_table, HUItem._meta.db_table]

    with connection.cursor() as cursor:
        if connection.vendor == 'sqlite':
            cursor.execute(
                "DELETE FROM sqlite_sequence WHERE name IN (%s, %s)",
                table_names,
            )
            return

        for sql in connection.ops.sequence_reset_sql(no_style(), [Pallet, HUItem]):
            cursor.execute(sql)


def _ensure_sap_session():
    from core.sap_client import SAPClient

    return SAPClient.ensure_session_ready()


def _sap_session_error_response():
    connected, user, message = _ensure_sap_session()
    if connected:
        return None

    return JsonResponse({
        'ok': False,
        'error': message,
        'sap_connected': False,
        'sap_user': user,
    }, status=409)


def _close_sap_session_if_idle() -> None:
    """Cierra SAP si se abrio pero no se pudo despachar una tarea Celery."""
    if not getattr(settings, 'SAP_CLOSE_WHEN_QUEUE_IDLE', True):
        return

    try:
        from core.sap_client import SAPClient

        SAPClient.close_sessions()
    except Exception as e:
        log.warning("close_sap_session_if_idle_failed error=%s", e)


def _has_potential_queue_work(run_pdf=True) -> bool:
    """
    Indica si hay trabajo que justifique abrir SAP.

    Incluye pallets activos con HUs porque el endpoint de procesamiento los
    cierra justo antes de despachar Celery.
    """
    has_active_items = Pallet.objects.filter(
        status=Pallet.STATUS_ACTIVE,
        items__isnull=False,
    ).exists()
    has_pending = HUItem.objects.filter(
        status=HUItem.STATUS_PENDING,
        pallet__status__in=[Pallet.STATUS_ACTIVE, Pallet.STATUS_READY],
    ).exists()
    has_pdf = run_pdf and pallets_ready_for_pdf_queryset().exists()
    return has_active_items or has_pending or has_pdf


def _parse_json_body(request) -> dict:
    """Lee JSON opcional sin fallar en endpoints que tambien aceptan body vacio."""
    if not request.body:
        return {}

    try:
        payload = json.loads(request.body.decode('utf-8') or '{}')
    except (json.JSONDecodeError, UnicodeDecodeError):
        return {}

    return payload if isinstance(payload, dict) else {}


def _coerce_bool(value, default=True) -> bool:
    if isinstance(value, bool):
        return value

    if value is None:
        return default

    if isinstance(value, str):
        return value.strip().lower() not in {'0', 'false', 'no', 'off'}

    return bool(value)


def _run_options_from_payload(payload: dict) -> dict:
    """Opciones F1/F2/PDF que deben respetarse si se reactiva Celery."""
    return {
        'run_f1': _coerce_bool(payload.get('run_f1'), True),
        'run_f2': _coerce_bool(payload.get('run_f2'), True),
        'run_pdf': _coerce_bool(payload.get('run_pdf'), True),
    }


def _has_ready_queue_work(run_pdf=True) -> bool:
    has_pending = HUItem.objects.filter(
        status=HUItem.STATUS_PENDING,
        pallet__status=Pallet.STATUS_READY,
    ).exists()
    return has_pending or (run_pdf and pallets_ready_for_pdf_queryset().exists())


def _dispatch_continuous_queue(run_f1=True, run_f2=True, run_pdf=True) -> None:
    process_queue_task.delay(
        run_f1=run_f1,
        run_f2=run_f2,
        run_pdf=run_pdf,
        continuous=True,
        idle_timeout=QUEUE_CONTINUOUS_IDLE_TIMEOUT_SECONDS,
    )
    arm_continuous_queue()


def _celery_start_blocker() -> dict | None:
    """
    Verifica broker y workers antes de abrir SAP.

    Redis activo solo confirma transporte; sin workers Celery la cola quedaria
    en espera indefinida. Por eso este bloqueo se ejecuta antes de iniciar o
    reusar una sesion SAP.
    """
    redis_status = _check_redis_status()
    celery_status = _check_celery_status(redis_status['ok'])

    if redis_status['ok'] and celery_status['ok']:
        return None

    if not redis_status['ok']:
        message = (
            'No se puede iniciar proceso: Redis no responde. '
            'Revisa el servicio Redis y el puerto configurado antes de iniciar la cola.'
        )
    else:
        message = CELERY_START_BLOCKED_MESSAGE

    return {
        'message': message,
        'redis': redis_status,
        'celery': celery_status,
    }


def _celery_start_blocker_response():
    blocker = _celery_start_blocker()
    if not blocker:
        return None

    return JsonResponse({
        'ok': False,
        'error': blocker['message'],
        'redis': blocker['redis'],
        'celery': blocker['celery'],
    }, status=503)


def _auto_start_queue_after_pallet_close(run_f1=True, run_f2=True, run_pdf=True) -> dict:
    """
    Reactiva Celery solo cuando el modo continuo ya fue armado manualmente.

    Esto evita arrancar SAP por accidente durante la captura inicial, pero permite
    que un pallet cerrado despues del timeout reactive la cola sin otro click.
    """
    if not is_continuous_queue_armed():
        return {'auto_started': False, 'auto_start_reason': 'not_armed'}

    if is_queue_locked() or HUItem.objects.filter(status=HUItem.STATUS_PROCESSING).exists():
        return {'auto_started': False, 'auto_start_reason': 'already_running'}

    if not _has_ready_queue_work(run_pdf=run_pdf):
        return {'auto_started': False, 'auto_start_reason': 'no_ready_work'}

    celery_blocker = _celery_start_blocker()
    if celery_blocker:
        return {
            'auto_started': False,
            'auto_start_reason': 'celery_unavailable',
            'auto_start_error': celery_blocker['message'],
            'celery': celery_blocker['celery'],
            'redis': celery_blocker['redis'],
        }

    connected, user, message = _ensure_sap_session()
    if not connected:
        return {
            'auto_started': False,
            'auto_start_reason': 'sap_unavailable',
            'auto_start_error': message,
            'sap_user': user,
        }

    try:
        _dispatch_continuous_queue(run_f1=run_f1, run_f2=run_f2, run_pdf=run_pdf)
    except Exception as e:
        log.exception("auto_start_queue_after_pallet_close_failed")
        return {
            'auto_started': False,
            'auto_start_reason': 'dispatch_failed',
            'auto_start_error': f'No se pudo iniciar Celery/Redis: {e}',
        }

    emit_stats_update(None)
    emit_queue_status(
        'Pallet cerrado. El worker continuo se reactivo automaticamente.',
        badge='AUTO',
        mode='running',
        footer='Celery tomara el siguiente pallet listo sin otro click.',
        is_running=True,
    )
    return {'auto_started': True, 'auto_start_reason': 'pallet_closed'}


def _queue_mutation_error(action: str) -> str:
    """
    Protege la cola contra cambios destructivos mientras Celery/SAP procesa.

    El escaneo sigue permitido para preparar el siguiente lote. Esta validación
    evita desincronización si otro navegador o una llamada directa intenta
    borrar datos mientras existe lock Redis o HUs en processing.
    """
    if is_queue_locked() or HUItem.objects.filter(status=HUItem.STATUS_PROCESSING).exists():
        return f'No se puede {action} mientras la cola está en procesamiento.'

    return ''



@require_POST
def reprocess_queue(request):
    """
    Equivale a _reprocess() — resetea HUs procesados a pending
    y dispara las tasks de Celery de nuevo.
    """
    lock_response = _queue_lock_blocker_response('reprocesar')
    if lock_response:
        return lock_response

    if HUItem.objects.filter(status=HUItem.STATUS_PROCESSING).exists():
        return JsonResponse({
            'ok':    False,
            'error': 'Hay HUs procesando — espera a que terminen',
        }, status=409)

    try:
        payload = json.loads(request.body.decode('utf-8') or '{}')
    except json.JSONDecodeError:
        payload = {}

    mode = payload.get('mode', 'all')
    if mode not in ('all', 'errors'):
        return JsonResponse({'ok': False, 'error': 'Modo de reproceso invalido'}, status=400)

    target_statuses = (
        REPROCESS_ERROR_STATUSES
        if mode == 'errors'
        else REPROCESS_ALL_STATUSES
    )

    items = list(HUItem.objects.filter(status__in=target_statuses).select_related('pallet'))
    count = len(items)

    if count == 0:
        return JsonResponse({'ok': False, 'error': 'No hay HUs para reprocesar'}, status=400)

    celery_error = _celery_start_blocker_response()
    if celery_error:
        return celery_error

    sap_error = _sap_session_error_response()
    if sap_error:
        return sap_error

    affected_pallet_ids = {item.pallet_id for item in items}
    for item in items:
        item.status = HUItem.STATUS_PENDING
        item.phase1_msg = ''
        item.phase2_msg = ''
        item.phase2_ms = 0
        item.error_msg = ''
        item.processing_started_at = None
        item.processing_ms = 0
        item.processed_at = None
        item.f1_done_at = None
        item.receipt_done_at = None
        item.pdf_status = ''
        item.pdf_msg = ''
        item.pdf_ms = 0
        item.save(update_fields=[
            'status',
            'phase1_msg',
            'phase2_msg',
            'phase2_ms',
            'error_msg',
            'processing_started_at',
            'processing_ms',
            'processed_at',
            'f1_done_at',
            'receipt_done_at',
            'pdf_status',
            'pdf_msg',
            'pdf_ms',
        ])
        emit_item_update(item)

    Pallet.objects.filter(pk__in=affected_pallet_ids).update(
        status      = Pallet.STATUS_READY,
        processing_started_at = None,
        processing_finished_at = None,
        f2_done_at  = None,
        receipt_done_at = None,
        pdf_status = '',
        pdf_msg    = '',
        pdf_ms     = 0,
    )

    emit_stats_update(None)
    try:
        _dispatch_continuous_queue()
    except Exception as e:
        _close_sap_session_if_idle()
        log.exception("reprocess_queue celery_dispatch_failed")
        return JsonResponse({
            'ok': False,
            'error': f'No se pudo iniciar Celery/Redis: {e}',
        }, status=503)

    emit_queue_status(
        f'Reproceso preparado. {count} HU(s) vuelven a la cola.',
        badge='REPROCESO',
        mode='running',
        footer='Celery retomara la cola con los HUs marcados como pendientes.',
        is_running=True,
    )
    log.info("reprocess_queue mode=%s count=%d", mode, count)
    return JsonResponse({'ok': True, 'count': count, 'mode': mode})


# ══════════════════════════════════════════════════════════════════════════════
#  INICIAR PROCESAMIENTO
# ══════════════════════════════════════════════════════════════════════════════


@require_POST
def start_processing(request):
    """
    Inicia el procesamiento de todos los HUs pendientes.
    Se llama desde el botón "Iniciar" en el frontend.
    """
    lock_response = _queue_lock_blocker_response('iniciar el proceso')
    if lock_response:
        return lock_response

    _close_active_pallet_for_processing()
    pending_items = HUItem.objects.filter(
        status=HUItem.STATUS_PENDING,
        pallet__status=Pallet.STATUS_READY,
    )
    count = pending_items.count()
    pdf_count = pallets_ready_for_pdf_queryset().count()

    if count == 0 and pdf_count == 0:
        _close_sap_session_if_idle()
        return JsonResponse({
            'ok':    False,
            'error': 'No hay HUs pendientes ni PDFs por imprimir',
        }, status=400)

    celery_error = _celery_start_blocker_response()
    if celery_error:
        return celery_error

    sap_error = _sap_session_error_response()
    if sap_error:
        return sap_error

    try:
        _dispatch_continuous_queue()
    except Exception as e:
        _close_sap_session_if_idle()
        log.exception("start_processing celery_dispatch_failed")
        return JsonResponse({
            'ok': False,
            'error': f'No se pudo iniciar Celery/Redis: {e}',
        }, status=503)

    emit_queue_status(
        'Cola enviada a Celery. Preparando SAP para el primer pallet.',
        badge='INICIO',
        mode='running',
        footer=f'{count} HU(s) pendiente(s), {pdf_count} PDF(s) por imprimir.',
        is_running=True,
    )
    log.info("start_processing launched queue task for %s HUs", count)
    return JsonResponse({
        'ok':    True,
        'count': count,
        'pdf_count': pdf_count,
        'message': f'Iniciando procesamiento de {count} HU{"s" if count != 1 else ""}...',
    })


# ══════════════════════════════════════════════════════════════════════════════
#  EXPORTAR CSV — mismas columnas que _export()
# ══════════════════════════════════════════════════════════════════════════════

@require_GET
def export_csv(request):
    """Equivale a _export() — descarga CSV con los datos de la cola actual."""
    filename = f"HUFlow_Export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    response = HttpResponse(content_type='text/csv; charset=utf-8-sig')
    response['Content-Disposition'] = f'attachment; filename="{filename}"'

    writer = csv.writer(response)
    writer.writerow([
        'Pallet', 'Origen', 'Código HU', 'Estado',
        'F1', 'F1 msg', 'F2', 'F2 msg', 'F2 ms',
        'PDF', 'PDF msg', 'PDF ms',
        'Problema', 'Tiempo procesamiento ms',
        'Agregada', 'Inicio procesamiento', 'Procesada',
    ])

    # FUTURA BD DE CONSULTA:
    # Este queryset contiene las variables principales del historico HU.
    # Aqui se agregarian los WHERE del reporte, por ejemplo:
    # .filter(hu_code=...), .filter(origin_code=...), .filter(status__in=[...]),
    # .filter(processed_at__range=(inicio, fin)) o .filter(pallet_id=...).
    for item in HUItem.objects.select_related('pallet').order_by('pallet_id', 'added_at'):
        writer.writerow([
            f"P{item.pallet_id:02d}",
            item.origin_code,
            item.hu_code,
            item.status_display,
            'ZMOVEINBHU', item.phase1_msg or '',
            'ZMMTIJSEP',  item.phase2_msg or '',
            item.phase2_ms or '',
            item.pdf_status or item.pallet.pdf_status,
            item.pdf_msg or item.pallet.pdf_msg or '',
            item.pdf_ms or item.pallet.pdf_ms or '',
            item.error_msg or '',
            item.processing_ms or '',
            item.added_at.strftime('%d/%m/%Y %H:%M:%S')   if item.added_at   else '',
            item.processing_started_at.strftime('%d/%m/%Y %H:%M:%S') if item.processing_started_at else '',
            item.processed_at.strftime('%d/%m/%Y %H:%M:%S') if item.processed_at else '',
        ])

    return response


# ══════════════════════════════════════════════════════════════════════════════
#  INICIAR SAP
# ══════════════════════════════════════════════════════════════════════════════


@require_POST
def iniciar_sap_endpoint(request):
    """
    Endpoint para iniciar la conexión con SAP.
    Equivale a la función iniciar_sap() — llama a SAP y abre la sesión.

    Retorna:
        JSON con status ok=True si la conexión fue exitosa.
    """
    from core.sap_client import SAPClient

    if not _has_potential_queue_work(run_pdf=True):
        _close_sap_session_if_idle()
        return JsonResponse({
            'ok': False,
            'error': 'No hay HUs pendientes ni PDFs por imprimir',
        }, status=400)

    connected, user, message = SAPClient.ensure_session_ready()
    if connected:
        log.info("iniciar_sap_endpoint conexion exitosa user=%s", user)
        return JsonResponse({'ok': True, 'message': message, 'user': user})

    log.error("iniciar_sap_endpoint failed message=%s", message)
    return JsonResponse({'ok': False, 'error': message}, status=500)


# ══════════════════════════════════════════════════════════════════════════════
#  STATUS SAP — equivale al timer _check_sap()
# ══════════════════════════════════════════════════════════════════════════════

@require_GET
def sap_status(request):
    """
    Equivale al QTimer que llama _check_sap() cada 5 segundos.
    El browser lo polling cada 5s para mostrar el indicador de conexión SAP.
    """
    from core.sap_client import SAPClient
    try:
        ok, user = SAPClient.check_session()
        return JsonResponse({
            'connected': ok,
            'user': user,
            'message': 'Sesion SAP activa' if ok else 'Sesion SAP desconectada o no valida',
        })
    except Exception as e:
        return JsonResponse({'connected': False, 'user': '', 'error': str(e)})


@require_GET
def system_status(request):
    """
    Estado operativo para la consola de diagnostico de soporte.

    El endpoint siempre responde JSON para que la UI pueda explicar fallas de
    Redis, Celery, SAP o cola sin convertirse en otro error 500.
    """
    redis_status = _check_redis_status()
    queue_status = _queue_diagnostic_snapshot()
    celery_status = _check_celery_status(redis_status['ok'], queue_status)
    database_status = _check_database_status()

    return JsonResponse({
        'ok': redis_status['ok'] and celery_status['ok'] and database_status['ok'],
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'debug': bool(settings.DEBUG),
        'django': {
            'ok': True,
            'message': 'Django/Daphne responde correctamente.',
            'action': 'Si la UI no responde, revisar Daphne/ASGI y el puerto publicado.',
        },
        'redis': redis_status,
        'celery': celery_status,
        'database': database_status,
        'sap': _check_sap_status_for_diagnostics(),
        'queue': queue_status,
    })


# ══════════════════════════════════════════════════════════════════════════════
#  STATS — para actualizar KPIs desde JS
# ══════════════════════════════════════════════════════════════════════════════

@require_GET
def stats_view(request):
    return JsonResponse(_get_stats())


# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS INTERNOS
# ══════════════════════════════════════════════════════════════════════════════

def _get_stats() -> dict:
    return calculate_queue_stats()


def _diagnostic_error_message(exc: Exception) -> str:
    """Evita exponer trazas internas cuando DEBUG esta desactivado."""
    if settings.DEBUG:
        return str(exc)[:300]
    return exc.__class__.__name__


def _check_redis_status() -> dict:
    started = time.perf_counter()
    try:
        import redis

        client = redis.Redis.from_url(
            settings.CELERY_BROKER_URL,
            socket_connect_timeout=1,
            socket_timeout=1,
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
    try:
        from core.sap_client import SAPClient

        connected, user = SAPClient.check_session()
        return {
            'ok': bool(connected),
            'connected': bool(connected),
            'user': user if connected else '',
            'message': 'Sesion SAP activa.' if connected else 'Sesion SAP desconectada o no valida.',
            'action': 'Si SAP esta invalido, cerrar/reabrir SAP o revisar login automatico.',
        }
    except Exception as exc:
        return {
            'ok': False,
            'connected': False,
            'user': '',
            'message': 'No se pudo validar SAP.',
            'action': 'Cerrar/reabrir SAP o revisar scripting/login automatico.',
            'error': _diagnostic_error_message(exc),
        }


def _queue_diagnostic_snapshot() -> dict:
    try:
        heartbeat_age = get_queue_worker_heartbeat_age_seconds()
        heartbeat_alive = (
            heartbeat_age is not None
            and heartbeat_age <= QUEUE_WORKER_HEARTBEAT_STALE_SECONDS
        )
        locked = is_queue_locked()
        return {
            'is_locked': locked,
            'stop_requested': is_queue_stop_requested(),
            'stop_request_age_seconds': get_queue_stop_request_age_seconds(),
            'continuous_armed': is_continuous_queue_armed(),
            'processing_pallet_ids': sorted(get_processing_pallet_ids()),
            'idle_remaining_seconds': get_queue_idle_remaining_seconds(),
            'worker_heartbeat_age_seconds': heartbeat_age,
            'worker_heartbeat_alive': heartbeat_alive,
            'worker_stale': bool(locked and not heartbeat_alive),
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


def _get_or_create_active_pallet(hu_code: str, origin) -> Pallet:
    """
    Equivale a la lógica de asignación de pallet en HUQueue.add_hu().
    Decide si el HU va al pallet activo o crea uno nuevo.
    """
    active = Pallet.objects.filter(status=Pallet.STATUS_ACTIVE).order_by('-id').first()
    queue_locked = is_queue_locked()
    processing_pallet_ids = get_processing_pallet_ids() if queue_locked else set()

    # Sin pallets — crear el primero
    if not active:
        return Pallet.objects.create(
            status=Pallet.STATUS_ACTIVE,
            origin_code=origin.code,
        )

    if active.pk in processing_pallet_ids:
        return Pallet.objects.create(
            status=Pallet.STATUS_ACTIVE,
            origin_code=origin.code,
        )

    # Pallet activo vacío — usarlo
    if not active.items.exists():
        active.origin_code = origin.code
        active.save(update_fields=['origin_code'])
        return active

    # Origen distinto o auto_pallet → pallet nuevo
    if active.origin_code != origin.code or origin.auto_pallet:
        _mark_pallet_ready(active)
        return Pallet.objects.create(
            status=Pallet.STATUS_ACTIVE,
            origin_code=origin.code,
        )


    return active


def _mark_pallet_ready(pallet: Pallet) -> None:
    """Cierra un pallet con HUs para que Celery pueda tomarlo."""
    if pallet.status == Pallet.STATUS_ACTIVE and pallet.items.exists():
        pallet.status = Pallet.STATUS_READY
        pallet.save(update_fields=['status'])


def _close_active_pallet_for_processing() -> None:
    """Cierra el pallet abierto actual cuando el usuario inicia el proceso."""
    active = Pallet.objects.filter(status=Pallet.STATUS_ACTIVE).order_by('-id').first()
    if active and active.items.exists():
        _mark_pallet_ready(active)


def iniciar_sap():
    """
    Inicia la conexión con SAP.

    Pasos:
      1. Verifica que SAPgui (saplogon.exe) esté disponible
      2. Abre SAP
      3. Obtiene la sesión activa
      4. Abre conexión a SAP Production

    Retorna:
        bool: True si la conexión fue exitosa, False si hubo error.
    """
    from core.sap_client import SAPClient

    connected, user, message = SAPClient.ensure_session_ready()
    log.info("iniciar_sap result connected=%s user=%s message=%s", connected, user, message)
    return connected


def _queue_runtime_is_orphaned(queue_status: dict, celery_status: dict | None = None) -> bool:
    return bool(queue_status.get('is_locked') and queue_status.get('worker_stale'))


def _stop_request_is_stale(queue_status: dict) -> bool:
    age = queue_status.get('stop_request_age_seconds')
    return bool(
        queue_status.get('is_locked')
        and queue_status.get('stop_requested')
        and age is not None
        and age >= QUEUE_SAFE_STOP_GRACE_SECONDS
    )


def _recover_orphaned_queue_runtime_data(message: str | None = None) -> dict:
    """
    Recupera la UI cuando Celery fue cerrado a la fuerza.

    En ese escenario nadie leera QUEUE_STOP_KEY ni liberara el lock en Redis, asi
    que la recuperacion debe liberar solo el estado runtime y emitir un cierre
    visual. Las HUs en processing se marcan como error para revision manual.
    """
    now = timezone.now()
    message = message or (
        'Celery se detuvo mientras la cola estaba activa. '
        'Se libero el lock y la UI quedo lista para revisar o reintentar.'
    )

    interrupted_items = list(
        HUItem.objects
        .select_related('pallet')
        .filter(status=HUItem.STATUS_PROCESSING)
    )
    affected_pallet_ids = {item.pallet_id for item in interrupted_items}

    for item in interrupted_items:
        item.status = HUItem.STATUS_ERROR
        item.error_msg = 'Proceso interrumpido: Celery se cerro antes de confirmar el resultado.'
        item.processed_at = now
        if item.processing_started_at and not item.processing_ms:
            item.processing_ms = max(
                0,
                int((now - item.processing_started_at).total_seconds() * 1000),
            )
        item.save(update_fields=[
            'status',
            'error_msg',
            'processed_at',
            'processing_ms',
        ])
        emit_item_update(item)

    if affected_pallet_ids:
        Pallet.objects.filter(pk__in=affected_pallet_ids).update(
            processing_finished_at=now,
        )

    clear_queue_runtime_state()
    emit_stats_update(None)
    result = {
        'status': 'stopped',
        'message': message,
        'pallets_processed': 0,
        'hus_processed': 0,
        'errors': len(interrupted_items),
    }
    emit_queue_done(result)
    log.warning(
        "detener_queue orphaned_runtime_recovered interrupted_hus=%s pallets=%s",
        len(interrupted_items),
        sorted(affected_pallet_ids),
    )
    return {
        'ok': True,
        'recovered': True,
        'message': message,
        'interrupted_hus': len(interrupted_items),
        'stats': calculate_queue_stats(),
    }


def _recover_orphaned_queue_runtime(message: str | None = None) -> JsonResponse:
    return JsonResponse(_recover_orphaned_queue_runtime_data(message))


def _queue_lock_blocker_response(action: str) -> JsonResponse | None:
    queue_status = _queue_diagnostic_snapshot()
    if not queue_status.get('is_locked'):
        return None

    if _queue_runtime_is_orphaned(queue_status) or _stop_request_is_stale(queue_status):
        if queue_status.get('processing_hus'):
            data = _recover_orphaned_queue_runtime_data(
                'Se libero un lock anterior, pero habia HUs marcadas como processing. '
                'Revisa esas HUs antes de iniciar otra corrida.'
            )
            data['ok'] = False
            data['error'] = data['message']
            return JsonResponse(data, status=409)

        clear_queue_runtime_state()
        emit_stats_update(None)
        emit_queue_status(
            'Lock anterior liberado. Puedes iniciar el procesamiento nuevamente.',
            badge='RECUPERADO',
            mode='waiting',
            footer='No habia HUs en processing; se limpio solo el estado runtime.',
            is_running=False,
        )
        return None

    return JsonResponse({
        'ok': False,
        'error': f'No se puede {action}: ya hay una tarea de procesamiento activa.',
    }, status=409)


# ══════════════════════════════════════════════════════════════════════════════
#  DETENER PROCESO
# ══════════════════════════════════════════════════════════════════════════════


@require_POST
def detener_queue(request):
    """
    Equivale a btn_stop — marca el proceso como detenido en el lado servidor.
    En este diseño el stop es principalmente UI.
    Si en el futuro quieres cancelar tasks de Celery, aquí va la lógica.
    """
    body = _parse_json_body(request)
    force_requested = bool(body.get('force'))
    queue_status = _queue_diagnostic_snapshot()
    redis_status = _check_redis_status()
    celery_status = _check_celery_status(redis_status['ok'], queue_status)
    if _queue_runtime_is_orphaned(queue_status, celery_status):
        return _recover_orphaned_queue_runtime()

    if _stop_request_is_stale(queue_status):
        message = (
            'La detencion segura no recibio confirmacion del worker. '
            'Se libero la cola para evitar que la UI quede bloqueada.'
        )
        return _recover_orphaned_queue_runtime(message)

    if force_requested and queue_status.get('stop_requested'):
        remaining = max(
            0,
            QUEUE_SAFE_STOP_GRACE_SECONDS - int(queue_status.get('stop_request_age_seconds') or 0),
        )
        return JsonResponse({
            'ok': True,
            'waiting_for_safe_stop': True,
            'message': (
                f'Detencion segura en curso. Si no termina, podras liberar la cola en {remaining}s.'
            ),
            'force_available_after_seconds': remaining,
        })

    stopped = request_queue_stop()
    log.info("detener_queue stop_requested=%s", stopped)
    if not stopped:
        return JsonResponse({'ok': False, 'error': 'No hay proceso activo'}, status=409)
    disarm_continuous_queue()
    emit_queue_status(
        'Detencion solicitada. El worker se detendra en el siguiente punto seguro.',
        badge='DETENIENDO',
        mode='waiting',
        footer='SAP terminara la operacion actual antes de liberar la cola.',
        is_running=True,
    )
    return JsonResponse({
        'ok': True,
        'message': 'Detencion solicitada. Esperando cierre seguro.',
        'waiting_for_safe_stop': True,
        'force_available_after_seconds': QUEUE_SAFE_STOP_GRACE_SECONDS,
    })


@require_POST
def procesar_pendientes(request):
    """Dispara una task secuencial para todos los HUs pendientes."""
    body   = json.loads(request.body) if request.body else {}
    run_f1 = body.get('run_f1', True)
    run_f2 = body.get('run_f2', True)
    run_pdf = body.get('run_pdf', True)

    lock_response = _queue_lock_blocker_response('iniciar el proceso')
    if lock_response:
        return lock_response

    if HUItem.objects.filter(status=HUItem.STATUS_PROCESSING).exists():
        return JsonResponse({'ok': False, 'error': 'Ya hay HUs procesando'}, status=409)

    _close_active_pallet_for_processing()
    items = HUItem.objects.filter(
        status=HUItem.STATUS_PENDING,
        pallet__status=Pallet.STATUS_READY,
    )
    count = items.count()
    pdf_count = pallets_ready_for_pdf_queryset().count() if run_pdf else 0

    if not count and not pdf_count:
        _close_sap_session_if_idle()
        return JsonResponse({'ok': False, 'error': 'No hay HUs pendientes ni PDFs por imprimir'})

    celery_error = _celery_start_blocker_response()
    if celery_error:
        return celery_error

    sap_error = _sap_session_error_response()
    if sap_error:
        return sap_error

    try:
        _dispatch_continuous_queue(run_f1=run_f1, run_f2=run_f2, run_pdf=run_pdf)
    except Exception as e:
        _close_sap_session_if_idle()
        log.exception("procesar_pendientes celery_dispatch_failed")
        return JsonResponse({
            'ok': False,
            'error': f'No se pudo iniciar Celery/Redis: {e}',
        }, status=503)

    emit_queue_status(
        'Cola enviada a Celery. Preparando SAP para el primer pallet.',
        badge='INICIO',
        mode='running',
        footer=f'{count} HU(s) pendiente(s), {pdf_count} PDF(s) por imprimir.',
        is_running=True,
    )
    log.info(
        "procesar_pendientes sequential_task_started count=%s pdf_count=%s",
        count,
        pdf_count,
    )
    return JsonResponse({'ok': True, 'count': count, 'pdf_count': pdf_count})
