import csv
import json
import logging
import os
import secrets
import time
import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from datetime import datetime
from django.conf import settings
from django.core.management.color import no_style
from django.db import connection, transaction, IntegrityError, DatabaseError
from django.http import JsonResponse, HttpResponse
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_POST, require_GET
from django.shortcuts import render
from django.utils import timezone

from core.hu_origins import detect_origin, is_pallet_separator

from queue_app.models import HUItem, Pallet, ScanLog
from queue_app.service_control import (
    execute_service_action,
    get_services_status,
    service_control_enabled,
    service_control_not_configured_message,
)
from queue_app.tasks import (
    arm_continuous_queue,
    clear_queue_runtime_state,
    disarm_continuous_queue,
    get_queue_idle_remaining_seconds,
    get_queue_lock_owner,
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
SCAN_BATCH_MAX_ITEMS = 100
SAP_STATUS_CACHE_TTL_SECONDS = 10
SAP_STATUS_STALE_SECONDS = 60
SAP_STATUS_TIMEOUT_SECONDS = 1.5
SAP_STATUS_CIRCUIT_BREAKER_SECONDS = 20
REPROCESS_ERROR_STATUSES = [HUItem.STATUS_ERROR, HUItem.STATUS_HU_NOT_FOUND]
REPROCESS_ALL_STATUSES = [
    HUItem.STATUS_OK,
    HUItem.STATUS_DUPLICATE,
    HUItem.STATUS_ERROR,
    HUItem.STATUS_HU_NOT_FOUND,
]

_sap_status_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix='sap-status')
_sap_status_lock = threading.Lock()
_sap_status_future = None
_sap_status_cache = None
_sap_status_circuit_until = 0.0
_sap_status_last_log = {}


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


def documentation_view(request):
    """Renderiza la documentacion operativa y tecnica del proyecto."""
    return render(request, 'queue_app/documentation.html')


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
        try:
            result = _new_pallet_logic(**run_options)
        except DatabaseError as exc:
            return _scan_retryable_error_response(
                raw,
                exc,
                status=503,
                error_type='database_error',
            )
        except Exception as exc:
            return _scan_retryable_error_response(raw, exc)

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

    origin = detect_origin(raw)
    try:
        with transaction.atomic():

    # ── Duplicado ─────────────────────────────────────────────────────────────
            if HUItem.objects.select_for_update().filter(hu_code=raw).exists():
                ScanLog.objects.create(
                    hu_code=raw,
                    result='duplicate',
                    message='HU ya registrado.',
                )
                log.info("scan_hu duplicate hu=%s", raw)
                return JsonResponse({
                    'ok': False,
                    'error': 'HU ya registrado.',
                    'message': 'HU ya registrado.',
                    'type': 'duplicate',
                }, status=409)

    # ── Crear HUItem en DB ────────────────────────────────────────────────────
            pallet = _get_or_create_active_pallet(raw, origin)
            item = HUItem.objects.create(
                hu_code=raw,
                pallet=pallet,
                origin_code=origin.code,
                status=HUItem.STATUS_PENDING,
            )
            ScanLog.objects.create(
                hu_code=raw,
                result='queued',
                message=f'Pallet {pallet.pk}',
            )
            if origin.auto_pallet:
                _mark_pallet_ready(pallet)
    except IntegrityError:
        log.warning("scan_hu duplicate_concurrent hu=%s", raw)
        _record_duplicate_scan(raw, 'Este HU fue escaneado por otra estacion.')
        return JsonResponse({
            'ok': False,
            'error': 'Este HU fue escaneado por otra estacion.',
            'message': 'Este HU fue escaneado por otra estacion.',
            'type': 'duplicate',
        }, status=409)
    except DatabaseError as exc:
        return _scan_retryable_error_response(
            raw,
            exc,
            status=503,
            error_type='database_error',
        )
    except Exception as exc:
        return _scan_retryable_error_response(raw, exc)

    auto_start_result = {}
    if origin.auto_pallet:
        try:
            auto_start_result = _auto_start_queue_after_pallet_close(**run_options)
        except Exception as exc:
            log.exception("scan_hu auto_start_failed hu=%s", raw)
            auto_start_result = {
                'auto_started': False,
                'auto_start_reason': 'auto_start_failed',
                'auto_start_error': _diagnostic_error_message(exc),
            }

    response_warnings = []
    needs_resync = False
    try:
        emit_item_update(item)
        emit_stats_update(item.pallet)
        emit_current_queue_status_if_available()
    except Exception as exc:
        log.exception("scan_hu websocket_emit_failed hu=%s", raw)
        needs_resync = True
        response_warnings.append(
            'El HU se guardo, pero la UI necesita resincronizarse.'
        )

    log.info("scan_hu hu=%s pallet=%s origin=%s", raw, pallet.pk, origin.code)

    return JsonResponse({
        'ok':        True,
        'hu_code':   raw,
        'pallet_id': pallet.pk,
        'origin':    origin.label,
        'status':    item.status,
        'stats':     _get_stats(),
        'warnings':  response_warnings,
        'needs_resync': needs_resync,
        **auto_start_result,
    })


# ══════════════════════════════════════════════════════════════════════════════
#  PALLET — crear nuevo pallet manualmente
# ══════════════════════════════════════════════════════════════════════════════


def _process_scan_batch_entry(entry, default_options: dict) -> dict:
    """Procesa una lectura pendiente del navegador y devuelve resultado individual."""
    if isinstance(entry, dict):
        raw = str(entry.get('code', '')).strip()
        entry_id = str(entry.get('id', '')).strip()
        option_payload = entry.get('options') if isinstance(entry.get('options'), dict) else default_options
    else:
        raw = str(entry or '').strip()
        entry_id = ''
        option_payload = default_options

    run_options = _run_options_from_payload(option_payload)
    if not raw:
        return {
            'id': entry_id,
            'code': raw,
            'ok': False,
            'error': 'Codigo vacio',
            'type': 'validation_error',
            'status_code': 400,
        }

    if is_pallet_separator(raw):
        try:
            result = _new_pallet_logic(**run_options)
        except DatabaseError as exc:
            result = _scan_retryable_error_payload(
                raw,
                exc,
                status=503,
                error_type='database_error',
            )
            result.update({'id': entry_id, 'code': raw, 'status_code': 503})
            return result
        except Exception as exc:
            result = _scan_retryable_error_payload(raw, exc)
            result.update({'id': entry_id, 'code': raw, 'status_code': 503})
            return result

        if result.get('ok'):
            result['stats'] = _get_stats()
        result.update({'id': entry_id, 'code': raw, 'status_code': 200})
        return result

    if not (HU_CODE_MIN_LENGTH <= len(raw) <= HU_CODE_MAX_LENGTH):
        return {
            'id': entry_id,
            'code': raw,
            'ok': False,
            'error': (
                f"Codigo HU invalido: '{raw}' tiene {len(raw)} caracteres. "
                f"Rango permitido: {HU_CODE_MIN_LENGTH}-{HU_CODE_MAX_LENGTH}"
            ),
            'type': 'validation_error',
            'status_code': 400,
        }

    origin = detect_origin(raw)
    try:
        with transaction.atomic():
            if HUItem.objects.select_for_update().filter(hu_code=raw).exists():
                ScanLog.objects.create(
                    hu_code=raw,
                    result='duplicate',
                    message='HU ya registrado.',
                )
                log.info("scan_hu_batch duplicate hu=%s", raw)
                return {
                    'id': entry_id,
                    'code': raw,
                    'ok': False,
                    'error': 'HU ya registrado.',
                    'message': 'HU ya registrado.',
                    'type': 'duplicate',
                    'status_code': 409,
                }

            pallet = _get_or_create_active_pallet(raw, origin)
            item = HUItem.objects.create(
                hu_code=raw,
                pallet=pallet,
                origin_code=origin.code,
                status=HUItem.STATUS_PENDING,
            )
            ScanLog.objects.create(
                hu_code=raw,
                result='queued',
                message=f'Pallet {pallet.pk}',
            )
            if origin.auto_pallet:
                _mark_pallet_ready(pallet)
    except IntegrityError:
        log.warning("scan_hu_batch duplicate_concurrent hu=%s", raw)
        _record_duplicate_scan(raw, 'Este HU fue escaneado por otra estacion.')
        return {
            'id': entry_id,
            'code': raw,
            'ok': False,
            'error': 'Este HU fue escaneado por otra estacion.',
            'message': 'Este HU fue escaneado por otra estacion.',
            'type': 'duplicate',
            'status_code': 409,
        }
    except DatabaseError as exc:
        result = _scan_retryable_error_payload(
            raw,
            exc,
            status=503,
            error_type='database_error',
        )
        result.update({'id': entry_id, 'code': raw, 'status_code': 503})
        return result
    except Exception as exc:
        result = _scan_retryable_error_payload(raw, exc)
        result.update({'id': entry_id, 'code': raw, 'status_code': 503})
        return result

    auto_start_result = {}
    if origin.auto_pallet:
        try:
            auto_start_result = _auto_start_queue_after_pallet_close(**run_options)
        except Exception as exc:
            log.exception("scan_hu_batch auto_start_failed hu=%s", raw)
            auto_start_result = {
                'auto_started': False,
                'auto_start_reason': 'auto_start_failed',
                'auto_start_error': _diagnostic_error_message(exc),
            }

    warnings = []
    needs_resync = False
    try:
        emit_item_update(item)
        emit_stats_update(item.pallet)
        emit_current_queue_status_if_available()
    except Exception:
        log.exception("scan_hu_batch websocket_emit_failed hu=%s", raw)
        needs_resync = True
        warnings.append('El HU se guardo, pero la UI necesita resincronizarse.')

    log.info("scan_hu_batch hu=%s pallet=%s origin=%s", raw, pallet.pk, origin.code)
    return {
        'id': entry_id,
        'code': raw,
        'ok': True,
        'hu_code': raw,
        'pallet_id': pallet.pk,
        'origin': origin.label,
        'status': item.status,
        'status_code': 200,
        'warnings': warnings,
        'needs_resync': needs_resync,
        **auto_start_result,
    }


@require_POST
def scan_hu_batch(request):
    """Procesa en backend los HUs que quedaron pendientes en la cola local."""
    payload = _parse_json_body(request)
    items = payload.get('items')
    if not isinstance(items, list) or not items:
        return JsonResponse({'ok': False, 'error': 'No hay HUs pendientes por guardar.'}, status=400)

    limited_items = items[:SCAN_BATCH_MAX_ITEMS]
    results = [_process_scan_batch_entry(entry, payload) for entry in limited_items]
    saved = sum(1 for result in results if result.get('ok'))
    duplicates = sum(1 for result in results if result.get('type') == 'duplicate')
    retryable = sum(1 for result in results if result.get('retryable'))
    rejected = sum(
        1 for result in results
        if not result.get('ok') and not result.get('retryable') and result.get('type') != 'duplicate'
    )

    return JsonResponse({
        'ok': True,
        'results': results,
        'saved': saved,
        'duplicates': duplicates,
        'retryable': retryable,
        'rejected': rejected,
        'remaining_not_processed': max(0, len(items) - SCAN_BATCH_MAX_ITEMS),
        'stats': _get_stats(),
    })


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
    """Logica compartida entre scan (separador) y boton nuevo pallet."""
    with transaction.atomic():
        active = _locked_active_pallet()

        # Si el pallet activo esta vacio, no crear uno nuevo.
        if active and not active.items.exists():
            return {
                'ok':        False,
                'error':     f'El pallet actual (P{active.pk:02d}) esta vacio. Escanea un HU primero.',
                'type':      'empty_pallet',
                'pallet_id': active.pk,
            }

        if active:
            _mark_pallet_ready(active)

        pallet = Pallet.objects.create(status=Pallet.STATUS_ACTIVE)
        pallet_id = pallet.pk

    try:
        auto_start_result = _auto_start_queue_after_pallet_close(
            run_f1=run_f1,
            run_f2=run_f2,
            run_pdf=run_pdf,
        )
    except Exception as exc:
        log.exception("new_pallet auto_start_failed pallet=%s", pallet_id)
        auto_start_result = {
            'auto_started': False,
            'auto_start_reason': 'auto_start_failed',
            'auto_start_error': _diagnostic_error_message(exc),
        }

    response_warnings = []
    needs_resync = False
    try:
        emit_pallet_created(pallet)
    except Exception as exc:
        log.exception("new_pallet websocket_emit_failed pallet=%s", pallet_id)
        needs_resync = True
        response_warnings.append(
            'El pallet se creo, pero la UI necesita resincronizarse.'
        )

    log.info("new_pallet created id=%s", pallet_id)

    return {
        'ok':        True,
        'pallet_id': pallet_id,
        'message':   f'Nuevo pallet #{pallet_id} iniciado.',
        'warnings':  response_warnings,
        'needs_resync': needs_resync,
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
    try:
        result = _ensure_sap_session()
    except Exception as exc:
        log.exception("sap_session_validation_failed")
        return JsonResponse({
            'ok': False,
            'error': f'No se pudo validar la sesion SAP: {_diagnostic_error_message(exc)}',
            'sap_connected': False,
            'sap_user': '',
        }, status=409)

    if not isinstance(result, (list, tuple)) or len(result) < 3:
        log.warning("sap_session_invalid_response result=%r", result)
        return JsonResponse({
            'ok': False,
            'error': 'No se pudo validar la sesion SAP: respuesta invalida del inicializador.',
            'sap_connected': False,
            'sap_user': '',
        }, status=409)

    connected, user, message = result[:3]
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


def _record_duplicate_scan(hu_code: str, message: str) -> None:
    """Audita duplicados sin convertir una carrera de escaneo en error 500."""
    try:
        ScanLog.objects.create(
            hu_code=hu_code,
            result='duplicate',
            message=message[:255],
        )
    except Exception as exc:
        log.warning("scan_hu duplicate_log_failed hu=%s error=%s", hu_code, exc)


def _scan_retryable_error_payload(
    hu_code: str,
    exc: Exception,
    *,
    status=503,
    error_type='scan_error',
) -> dict:
    """Payload recuperable para conservar lecturas cuando DB/Django falla."""
    log.exception("scan_hu retryable_error hu=%s type=%s", hu_code, error_type)
    message = (
        'No se pudo guardar el HU en este momento. '
        'El navegador lo conservara y lo reintentara automaticamente.'
    )
    return {
        'ok': False,
        'error': message,
        'message': message,
        'type': error_type,
        'retryable': True,
        'status': status,
        'detail': _diagnostic_error_message(exc),
    }


def _scan_retryable_error_response(
    hu_code: str,
    exc: Exception,
    *,
    status=503,
    error_type='scan_error',
) -> JsonResponse:
    """
    Devuelve JSON recuperable para que el frontend conserve y reintente la lectura.

    El objetivo es que un fallo temporal de DB/Django no convierta un escaneo
    fisico en un HU perdido para el operador.
    """
    return JsonResponse(
        _scan_retryable_error_payload(
            hu_code,
            exc,
            status=status,
            error_type=error_type,
        ),
        status=status,
    )


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
def _sap_status_log_once(key: str, level: int, message: str, *, interval: int = 60) -> None:
    """Evita spam de logs cuando SAP queda caido o no responde por varios minutos."""
    now = time.time()
    last = _sap_status_last_log.get(key, 0)
    if now - last < interval:
        return

    _sap_status_last_log[key] = now
    log.log(level, message)


def _sap_status_payload(status: str, *, connected=False, user='', message='',
                        source='live', stale=False, checked_at='') -> dict:
    return {
        'connected': bool(connected),
        'status': status,
        'user': user if connected else '',
        'message': message,
        'checked_at': checked_at,
        'stale': bool(stale),
        'source': source,
    }


def _live_sap_status_probe() -> dict:
    """Consulta SAP COM en un thread separado para que el request HTTP no se bloquee."""
    from core.sap_client import SAPClient

    try:
        ok, user = SAPClient.check_session()
        checked_at = timezone.now().isoformat()
        return _sap_status_payload(
            'connected' if ok else 'disconnected',
            connected=ok,
            user=user,
            message='Sesion SAP activa' if ok else 'Sesion SAP desconectada o no valida',
            source='live',
            stale=False,
            checked_at=checked_at,
        )
    except Exception as exc:
        payload = _sap_status_payload(
            'unavailable',
            message='No se pudo validar SAP.',
            source='error',
            stale=True,
            checked_at=timezone.now().isoformat(),
        )
        payload['error'] = _diagnostic_error_message(exc)
        return payload


def _sap_status_from_cache(status: str, source: str, message: str) -> dict:
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
    )


def _harvest_sap_status_future_locked() -> None:
    global _sap_status_future, _sap_status_cache, _sap_status_circuit_until

    if _sap_status_future is None or not _sap_status_future.done():
        return

    try:
        _sap_status_cache = _sap_status_future.result()
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
            )

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
        )
        payload['error'] = _diagnostic_error_message(exc)
        return payload

    with _sap_status_lock:
        if _sap_status_future is future:
            _sap_status_cache = result
            _sap_status_future = None
            _sap_status_circuit_until = 0.0

    return result


@require_GET
def sap_status(request):
    """
    Estado rapido de SAP para el indicador de la UI.
    Usa cache corto, timeout y circuit breaker para proteger Daphne si SAP COM
    no responde.
    """
    return JsonResponse(get_fast_sap_status())


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

SERVICE_CONTROL_SESSION_KEY = 'nexhus_service_control_authenticated'


def _service_control_is_authenticated(request) -> bool:
    return bool(request.session.get(SERVICE_CONTROL_SESSION_KEY))


def _service_control_auth_response(request) -> JsonResponse | None:
    if not service_control_enabled():
        return JsonResponse({
            'ok': False,
            'authenticated': False,
            'enabled': False,
            'error': service_control_not_configured_message(),
        }, status=403)

    if not _service_control_is_authenticated(request):
        return JsonResponse({
            'ok': False,
            'authenticated': False,
            'enabled': True,
            'error': 'Login de soporte requerido.',
        }, status=401)

    return None


@require_POST
def service_control_login(request):
    """Autentica el panel de soporte sin exponer passwords del sistema."""
    if not service_control_enabled():
        return JsonResponse({
            'ok': False,
            'authenticated': False,
            'enabled': False,
            'error': service_control_not_configured_message(),
        }, status=403)

    try:
        payload = json.loads(request.body.decode('utf-8') or '{}')
    except (json.JSONDecodeError, UnicodeDecodeError):
        payload = {}

    password = str(payload.get('password', ''))
    expected = str(getattr(settings, 'NEXHUS_SERVICE_CONTROL_PASSWORD', ''))
    if not secrets.compare_digest(password, expected):
        log.warning("service_control_login_failed remote=%s", request.META.get('REMOTE_ADDR'))
        return JsonResponse({
            'ok': False,
            'authenticated': False,
            'enabled': True,
            'error': 'Clave de soporte incorrecta.',
        }, status=403)

    request.session[SERVICE_CONTROL_SESSION_KEY] = True
    request.session.set_expiry(60 * 30)
    return JsonResponse({
        'ok': True,
        'authenticated': True,
        'enabled': True,
        'services': get_services_status(),
    })


@require_POST
def service_control_logout(request):
    request.session.pop(SERVICE_CONTROL_SESSION_KEY, None)
    return JsonResponse({'ok': True, 'authenticated': False})


@require_GET
def service_control_status(request):
    auth_error = _service_control_auth_response(request)
    if auth_error:
        return auth_error

    return JsonResponse({
        'ok': True,
        'authenticated': True,
        'enabled': True,
        'services': get_services_status(),
    })


@require_POST
def service_control_action(request, service: str, action: str):
    auth_error = _service_control_auth_response(request)
    if auth_error:
        return auth_error

    result = execute_service_action(service, action)
    log.warning(
        "service_control_action service=%s action=%s ok=%s message=%s",
        service,
        action,
        result.ok,
        result.message,
    )
    return JsonResponse({
        **result.as_dict(),
        'authenticated': True,
        'enabled': True,
        'services': get_services_status(),
    }, status=200 if result.ok else 400)


@require_GET
def stats_view(request):
    return JsonResponse(_get_stats())


@require_GET
def queue_snapshot(request):
    """Devuelve una foto completa de la cola para resincronizar solo la UI."""
    snapshot = _build_queue_snapshot_payload()
    log.info(
        "queue_snapshot requested items=%s is_running=%s",
        len(snapshot['items']),
        snapshot['stats'].get('is_running'),
    )
    return JsonResponse({'ok': True, **snapshot})


# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS INTERNOS
# ══════════════════════════════════════════════════════════════════════════════

def _get_stats() -> dict:
    return calculate_queue_stats()


def _build_queue_snapshot_payload() -> dict:
    """Arma el mismo estado base que consume la UI al conectar o resincronizar."""
    items = HUItem.objects.select_related('pallet').order_by('added_at', 'id')
    return {
        'items': [_serialize_queue_snapshot_item(item) for item in items],
        'stats': calculate_queue_stats(),
        'queue_status': calculate_queue_operational_status(),
        'snapshot_at': timezone.now().isoformat(),
    }


def _serialize_queue_snapshot_item(item: HUItem) -> dict:
    pallet = item.pallet
    return {
        'hu_code': item.hu_code,
        'status': item.status,
        'f1_display': item.f1_display,
        'f2_display': item.f2_display,
        'phase1_msg': item.phase1_msg,
        'phase2_msg': item.phase2_msg,
        'phase2_ms': item.phase2_ms,
        'pallet_id': item.pallet_id,
        'origin_code': item.origin_code,
        'pdf_status': pallet.pdf_status,
        'pdf_display': pallet.pdf_display,
        'pdf_msg': pallet.pdf_msg,
        'pdf_ms': pallet.pdf_ms,
        'processing_time_display': pallet.processing_time_display,
        'processing_started_at': (
            pallet.processing_started_at.isoformat()
            if pallet.processing_started_at
            else ''
        ),
        'processing_finished_at': (
            pallet.processing_finished_at.isoformat()
            if pallet.processing_finished_at
            else ''
        ),
        'receipt_done_at': (
            pallet.receipt_done_at.isoformat()
            if pallet.receipt_done_at
            else ''
        ),
    }


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


def _locked_active_pallet() -> Pallet | None:
    """Obtiene el pallet activo bajo bloqueo transaccional y corrige duplicados vacios."""
    active_pallets = list(
        Pallet.objects
        .select_for_update()
        .filter(status=Pallet.STATUS_ACTIVE)
        .order_by('-id')
    )
    if not active_pallets:
        return None

    active = active_pallets[0]
    for stale in active_pallets[1:]:
        if stale.items.exists():
            stale.status = Pallet.STATUS_READY
            stale.save(update_fields=['status'])
            log.warning(
                "active_pallet_duplicate_closed kept=%s closed=%s",
                active.pk,
                stale.pk,
            )
        else:
            stale_id = stale.pk
            stale.delete()
            log.warning(
                "active_pallet_duplicate_empty_deleted kept=%s deleted=%s",
                active.pk,
                stale_id,
            )

    return active


def _get_or_create_active_pallet(hu_code: str, origin) -> Pallet:
    """
    Equivale a la lógica de asignación de pallet en HUQueue.add_hu().
    Decide si el HU va al pallet activo o crea uno nuevo.
    """
    if not connection.in_atomic_block:
        with transaction.atomic():
            return _get_or_create_active_pallet(hu_code, origin)

    active = _locked_active_pallet()
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
    if not connection.in_atomic_block:
        with transaction.atomic():
            _mark_pallet_ready(pallet)
        return

    pallet = Pallet.objects.select_for_update().get(pk=pallet.pk)
    if pallet.status == Pallet.STATUS_ACTIVE and pallet.items.exists():
        pallet.status = Pallet.STATUS_READY
        pallet.save(update_fields=['status'])


def _close_active_pallet_for_processing() -> None:
    """Cierra el pallet abierto actual cuando el usuario inicia el proceso."""
    with transaction.atomic():
        active = _locked_active_pallet()
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
    invalidated_owner = get_queue_lock_owner()
    clear_queue_runtime_state(
        reason='orphaned_runtime_recovery',
        invalidated_owner=invalidated_owner,
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
        "detener_queue orphaned_runtime_recovered invalidated_owner=%s interrupted_hus=%s pallets=%s",
        invalidated_owner or '<none>',
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

        invalidated_owner = queue_status.get('lock_owner') or get_queue_lock_owner()
        clear_queue_runtime_state(
            reason='lock_blocker_stale_runtime',
            invalidated_owner=invalidated_owner,
        )
        log.warning(
            "queue_lock_blocker stale_runtime_recovered invalidated_owner=%s",
            invalidated_owner or '<none>',
        )
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
