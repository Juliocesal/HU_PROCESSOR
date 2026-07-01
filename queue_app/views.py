import json
import logging
from django.db import IntegrityError, DatabaseError
from django.http import JsonResponse
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_POST, require_GET
from django.shortcuts import render

from core.hu_origins import detect_origin, is_pallet_separator

from queue_app.models import HUItem, Pallet
from queue_app.services.pallet_service import (
    close_active_pallet_for_processing as _close_active_pallet_for_processing,
    create_next_active_pallet,
    get_or_create_active_pallet as _service_get_or_create_active_pallet,
    mark_pallet_ready as _mark_pallet_ready,
    recalculate_pallet_after_hu_delete as _recalculate_pallet_after_hu_delete,
)
from queue_app.services.scan_service import (
    DuplicateHUScan,
    queue_hu_scan,
    record_duplicate_scan as _record_duplicate_scan,
    retryable_scan_error_payload,
)
from queue_app.services.snapshot_service import (
    build_queue_snapshot_payload as _build_queue_snapshot_payload,
)
from queue_app.services.diagnostics_service import (
    CELERY_NO_WORKER_MESSAGE,
    _check_celery_status,
    _check_database_status,
    _check_redis_status,
    _check_sap_status_for_diagnostics,
    _diagnostic_error_message,
    _queue_diagnostic_snapshot as _service_queue_diagnostic_snapshot,
    _queue_diagnostic_snapshot_without_redis,
    get_fast_sap_status,
    iniciar_sap as _diagnostics_iniciar_sap,
    sap_start_blocker as _diagnostics_sap_start_blocker,
    sap_start_decision as _diagnostics_sap_start_decision,
)
from queue_app.services.service_control_api import (
    service_control_action_response,
    service_control_auth_response,
    service_control_is_authenticated,
    service_control_login_response,
    service_control_logout_response,
    service_control_status_response,
)
from queue_app.services.read_only_api import (
    documentation_response,
    export_csv_response,
    queue_snapshot_response,
    sap_status_response,
    stats_response,
    system_status_response,
)
from queue_app.services.queue_mutation_api import (
    clear_queue_response,
    delete_hu_response,
    delete_pallet_response,
    new_pallet_result,
    queue_mutation_error,
    reprocess_queue_response,
    reset_queue_sequences,
)
from queue_app.services.process_control_api import (
    auto_start_queue_after_pallet_close as _service_auto_start_queue_after_pallet_close,
    celery_start_blocker as _service_celery_start_blocker,
    celery_start_blocker_response as _service_celery_start_blocker_response,
    close_sap_session_if_idle as _service_close_sap_session_if_idle,
    detener_queue_response,
    dispatch_continuous_queue as _service_dispatch_continuous_queue,
    has_potential_queue_work as _service_has_potential_queue_work,
    has_ready_queue_work as _service_has_ready_queue_work,
    procesar_pendientes_response,
    queue_lock_blocker_response as _service_queue_lock_blocker_response,
    queue_runtime_is_orphaned,
    recover_orphaned_queue_runtime_data as _service_recover_orphaned_queue_runtime_data,
    recover_orphaned_queue_runtime_response,
    sap_start_blocker_response as _service_sap_start_blocker_response,
    start_processing_response,
    stop_request_is_stale,
)
from queue_app.service_control import (
    execute_service_action,
    get_services_status,
    service_control_enabled,
    service_control_not_configured_message,
)
from queue_app.services.queue_runtime import (
    arm_continuous_queue,
    clear_queue_runtime_state,
    disarm_continuous_queue,
    get_processing_pallet_ids,
    get_queue_lock_owner,
    is_continuous_queue_armed,
    is_queue_locked,
    request_queue_stop,
)
from queue_app.tasks import process_queue_task
from queue_app.utils import (
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
CELERY_START_BLOCKED_MESSAGE = (
    'No se puede iniciar proceso: Celery no esta activo. '
    'Redis esta conectado, pero ningun worker respondio.'
)
HU_CODE_MIN_LENGTH = 10
HU_CODE_MAX_LENGTH = 15
SCAN_BATCH_MAX_ITEMS = 100


def _get_or_create_active_pallet(hu_code: str, origin) -> Pallet:
    """Compatibilidad para tests y vistas; la regla vive en pallet_service."""
    return _service_get_or_create_active_pallet(
        hu_code,
        origin,
        is_queue_locked_func=is_queue_locked,
        get_processing_pallet_ids_func=get_processing_pallet_ids,
    )


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
    return documentation_response(request)


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
            stats = _safe_scan_stats('scan_pallet_separator')
            if stats is not None:
                result['stats'] = stats
            else:
                result['needs_resync'] = True
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
        scan_result = queue_hu_scan(
            raw,
            origin,
            get_or_create_active_pallet=_get_or_create_active_pallet,
            mark_pallet_ready=_mark_pallet_ready,
            log_label='scan_hu',
        )
        item = scan_result.item
        pallet = scan_result.pallet
    except DuplicateHUScan:
        return JsonResponse({
            'ok': False,
            'error': 'HU ya registrado.',
            'message': 'HU ya registrado.',
            'type': 'duplicate',
        }, status=409)
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

    stats = _safe_scan_stats('scan_hu')
    response_warnings = []
    needs_resync = False
    try:
        emit_item_update(item)
        if stats is not None:
            emit_stats_update(item.pallet, stats=stats)
        emit_current_queue_status_if_available(stats=stats)
    except Exception as exc:
        log.exception("scan_hu websocket_emit_failed hu=%s", raw)
        needs_resync = True
        response_warnings.append(
            'El HU se guardo, pero la UI necesita resincronizarse.'
        )

    log.info("scan_hu hu=%s pallet=%s origin=%s", raw, pallet.pk, origin.code)

    response = {
        'ok':        True,
        'hu_code':   raw,
        'pallet_id': pallet.pk,
        'origin':    origin.label,
        'status':    item.status,
        'warnings':  response_warnings,
        'needs_resync': needs_resync,
        **auto_start_result,
    }
    if stats is not None:
        response['stats'] = stats
    else:
        response['needs_resync'] = True
        response['warnings'].append(
            'El HU se guardo, pero no se pudieron recalcular los indicadores.'
        )
    return JsonResponse(response)


# ══════════════════════════════════════════════════════════════════════════════
#  PALLET — crear nuevo pallet manualmente
# ══════════════════════════════════════════════════════════════════════════════


def _process_scan_batch_entry(entry, default_options: dict, *, emit_realtime=True) -> dict:
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
            stats = _safe_scan_stats('scan_batch_pallet_separator')
            if stats is not None:
                result['stats'] = stats
            else:
                result['needs_resync'] = True
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
        scan_result = queue_hu_scan(
            raw,
            origin,
            get_or_create_active_pallet=_get_or_create_active_pallet,
            mark_pallet_ready=_mark_pallet_ready,
            log_label='scan_hu_batch',
        )
        item = scan_result.item
        pallet = scan_result.pallet
    except DuplicateHUScan:
        return {
            'id': entry_id,
            'code': raw,
            'ok': False,
            'error': 'HU ya registrado.',
            'message': 'HU ya registrado.',
            'type': 'duplicate',
            'status_code': 409,
        }
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
    if emit_realtime:
        stats = _safe_scan_stats('scan_hu_batch')
        try:
            emit_item_update(item)
            if stats is not None:
                emit_stats_update(item.pallet, stats=stats)
            emit_current_queue_status_if_available(stats=stats)
        except Exception:
            log.exception("scan_hu_batch websocket_emit_failed hu=%s", raw)
            needs_resync = True
            warnings.append('El HU se guardo, pero la UI necesita resincronizarse.')

    log.info("scan_hu_batch hu=%s pallet=%s origin=%s", raw, pallet.pk, origin.code)
    result = {
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
    if not emit_realtime:
        result['_item_id'] = item.pk
    return result


@require_POST
def scan_hu_batch(request):
    """Procesa en backend los HUs que quedaron pendientes en la cola local."""
    payload = _parse_json_body(request)
    items = payload.get('items')
    if not isinstance(items, list) or not items:
        return JsonResponse({'ok': False, 'error': 'No hay HUs pendientes por guardar.'}, status=400)

    limited_items = items[:SCAN_BATCH_MAX_ITEMS]
    results = [
        _process_scan_batch_entry(entry, payload, emit_realtime=False)
        for entry in limited_items
    ]
    saved_item_ids = [
        result.pop('_item_id')
        for result in results
        if result.get('_item_id')
    ]
    saved = sum(1 for result in results if result.get('ok'))
    duplicates = sum(1 for result in results if result.get('type') == 'duplicate')
    retryable = sum(1 for result in results if result.get('retryable'))
    rejected = sum(
        1 for result in results
        if not result.get('ok') and not result.get('retryable') and result.get('type') != 'duplicate'
    )

    stats = _safe_scan_stats('scan_hu_batch')
    warnings = []
    needs_resync = False
    if saved_item_ids:
        try:
            saved_items = (
                HUItem.objects
                .filter(pk__in=saved_item_ids)
                .select_related('pallet')
                .order_by('added_at', 'id')
            )
            for item in saved_items:
                emit_item_update(item)
            if stats is not None:
                emit_stats_update(stats=stats)
            emit_current_queue_status_if_available(stats=stats)
        except Exception:
            log.exception("scan_hu_batch websocket_emit_failed count=%s", len(saved_item_ids))
            needs_resync = True
            warnings.append('Los HUs se guardaron, pero la UI necesita resincronizarse.')

    response = {
        'ok': True,
        'results': results,
        'saved': saved,
        'duplicates': duplicates,
        'retryable': retryable,
        'rejected': rejected,
        'remaining_not_processed': max(0, len(items) - SCAN_BATCH_MAX_ITEMS),
        'warnings': warnings,
        'needs_resync': needs_resync,
    }
    if stats is not None:
        response['stats'] = stats
    return JsonResponse(response)


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
    return new_pallet_result(
        run_f1=run_f1,
        run_f2=run_f2,
        run_pdf=run_pdf,
        create_next_active_pallet_func=create_next_active_pallet,
        auto_start_queue_after_pallet_close_func=_auto_start_queue_after_pallet_close,
        emit_pallet_created_func=emit_pallet_created,
        diagnostic_error_message_func=_diagnostic_error_message,
        logger=log,
    )


# ══════════════════════════════════════════════════════════════════════════════
#  BORRAR HU / PALLET
# ══════════════════════════════════════════════════════════════════════════════


@require_POST
def delete_hu(request, hu_code):
    return delete_hu_response(
        hu_code,
        queue_mutation_error_func=_queue_mutation_error,
        recalculate_pallet_after_hu_delete_func=_recalculate_pallet_after_hu_delete,
        get_stats_func=_get_stats,
        emit_hu_deleted_func=emit_hu_deleted,
        emit_item_update_func=emit_item_update,
        emit_stats_update_func=emit_stats_update,
        logger=log,
    )


@require_POST
def delete_pallet(request, pallet_id):
    return delete_pallet_response(
        pallet_id,
        queue_mutation_error_func=_queue_mutation_error,
        get_stats_func=_get_stats,
        emit_pallet_deleted_func=emit_pallet_deleted,
        logger=log,
    )


# ══════════════════════════════════════════════════════════════════════════════
#  LIMPIAR / REPROCESAR
# ══════════════════════════════════════════════════════════════════════════════


@require_POST
def clear_queue(request):
    return clear_queue_response(
        is_queue_locked_func=is_queue_locked,
        disarm_continuous_queue_func=disarm_continuous_queue,
        get_stats_func=_get_stats,
        emit_queue_cleared_func=emit_queue_cleared,
        logger=log,
    )


def _reset_queue_sequences():
    reset_queue_sequences()


def _close_sap_session_if_idle() -> None:
    _service_close_sap_session_if_idle(logger=log)


def _has_potential_queue_work(run_pdf=True) -> bool:
    return _service_has_potential_queue_work(
        run_pdf=run_pdf,
        pdf_queryset_func=pallets_ready_for_pdf_queryset,
    )


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


def _scan_retryable_error_payload(
    hu_code: str,
    exc: Exception,
    *,
    status=503,
    error_type='scan_error',
) -> dict:
    """Payload recuperable para conservar lecturas cuando DB/Django falla."""
    return retryable_scan_error_payload(
        hu_code,
        exc,
        status=status,
        error_type=error_type,
        detail_formatter=_diagnostic_error_message,
    )


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


def _safe_scan_stats(context: str) -> dict | None:
    """Calcula KPIs para escaneo sin convertir un HU guardado en HTTP 500."""
    try:
        return _get_stats()
    except Exception:
        log.exception("%s stats_failed_after_scan", context)
        return None


def _has_ready_queue_work(run_pdf=True) -> bool:
    return _service_has_ready_queue_work(
        run_pdf=run_pdf,
        pdf_queryset_func=pallets_ready_for_pdf_queryset,
    )


def _dispatch_continuous_queue(
    run_f1=True,
    run_f2=True,
    run_pdf=True,
    force_new_sap_login=False,
) -> None:
    _service_dispatch_continuous_queue(
        run_f1=run_f1,
        run_f2=run_f2,
        run_pdf=run_pdf,
        force_new_sap_login=force_new_sap_login,
        process_queue_task=process_queue_task,
        arm_continuous_queue_func=arm_continuous_queue,
        idle_timeout_seconds=QUEUE_CONTINUOUS_IDLE_TIMEOUT_SECONDS,
    )


def _celery_start_blocker() -> dict | None:
    return _service_celery_start_blocker(
        redis_status_func=_check_redis_status,
        celery_status_func=_check_celery_status,
        blocked_message=CELERY_START_BLOCKED_MESSAGE,
    )


def _celery_start_blocker_response():
    return _service_celery_start_blocker_response(
        celery_start_blocker_func=_celery_start_blocker,
    )


def _sap_start_blocker_response():
    return _service_sap_start_blocker_response(
        sap_start_blocker_func=_diagnostics_sap_start_blocker,
        logger=log,
    )


def _sap_start_decision():
    return _diagnostics_sap_start_decision()


def _auto_start_queue_after_pallet_close(run_f1=True, run_f2=True, run_pdf=True) -> dict:
    return _service_auto_start_queue_after_pallet_close(
        run_f1=run_f1,
        run_f2=run_f2,
        run_pdf=run_pdf,
        is_continuous_queue_armed_func=is_continuous_queue_armed,
        is_queue_locked_func=is_queue_locked,
        has_ready_queue_work_func=_has_ready_queue_work,
        celery_start_blocker_func=_celery_start_blocker,
        sap_start_decision_func=_sap_start_decision,
        dispatch_continuous_queue_func=_dispatch_continuous_queue,
        emit_stats_update_func=emit_stats_update,
        emit_queue_status_func=emit_queue_status,
        logger=log,
    )


def _queue_mutation_error(action: str) -> str:
    return queue_mutation_error(action, is_queue_locked_func=is_queue_locked)



@require_POST
def reprocess_queue(request):
    return reprocess_queue_response(
        request,
        queue_lock_blocker_response_func=_queue_lock_blocker_response,
        celery_start_blocker_response_func=_celery_start_blocker_response,
        sap_start_decision_func=_sap_start_decision,
        dispatch_continuous_queue_func=_dispatch_continuous_queue,
        close_sap_session_if_idle_func=_close_sap_session_if_idle,
        emit_item_update_func=emit_item_update,
        emit_stats_update_func=emit_stats_update,
        emit_queue_status_func=emit_queue_status,
        logger=log,
    )


# ══════════════════════════════════════════════════════════════════════════════
#  INICIAR PROCESAMIENTO
# ══════════════════════════════════════════════════════════════════════════════


@require_POST
def start_processing(request):
    return start_processing_response(
        queue_lock_blocker_response_func=_queue_lock_blocker_response,
        close_active_pallet_for_processing_func=_close_active_pallet_for_processing,
        close_sap_session_if_idle_func=_close_sap_session_if_idle,
        celery_start_blocker_response_func=_celery_start_blocker_response,
        sap_start_decision_func=_sap_start_decision,
        dispatch_continuous_queue_func=_dispatch_continuous_queue,
        emit_queue_status_func=emit_queue_status,
        pdf_queryset_func=pallets_ready_for_pdf_queryset,
        logger=log,
    )


# ══════════════════════════════════════════════════════════════════════════════
#  EXPORTAR CSV — mismas columnas que _export()
# ══════════════════════════════════════════════════════════════════════════════

@require_GET
def export_csv(request):
    return export_csv_response()


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
    Estado rapido de SAP para el indicador de la UI.
    Usa cache corto, timeout y circuit breaker para proteger Daphne si SAP COM
    no responde.
    """
    return sap_status_response(sap_status_func=get_fast_sap_status)


@require_GET
def system_status(request):
    """
    Estado operativo para la consola de diagnostico de soporte.

    El endpoint siempre responde JSON para que la UI pueda explicar fallas de
    Redis, Celery, SAP o cola sin convertirse en otro error 500.
    """
    return system_status_response(
        redis_status_func=_check_redis_status,
        queue_status_func=_queue_diagnostic_snapshot,
        degraded_queue_status_func=_queue_diagnostic_snapshot_without_redis,
        celery_status_func=_check_celery_status,
        database_status_func=_check_database_status,
        sap_status_func=_check_sap_status_for_diagnostics,
    )


# ══════════════════════════════════════════════════════════════════════════════
#  STATS — para actualizar KPIs desde JS
# ══════════════════════════════════════════════════════════════════════════════

def _service_control_is_authenticated(request) -> bool:
    return service_control_is_authenticated(request)


def _service_control_auth_response(request) -> JsonResponse | None:
    return service_control_auth_response(
        request,
        enabled_func=service_control_enabled,
        not_configured_message_func=service_control_not_configured_message,
        authenticated_func=_service_control_is_authenticated,
    )


@require_POST
def service_control_login(request):
    return service_control_login_response(
        request,
        enabled_func=service_control_enabled,
        not_configured_message_func=service_control_not_configured_message,
        get_services_status_func=get_services_status,
        logger=log,
    )


@require_POST
def service_control_logout(request):
    return service_control_logout_response(request)


@require_GET
def service_control_status(request):
    return service_control_status_response(
        request,
        auth_response_func=_service_control_auth_response,
        get_services_status_func=get_services_status,
    )


@require_POST
def service_control_action(request, service: str, action: str):
    return service_control_action_response(
        request,
        service,
        action,
        auth_response_func=_service_control_auth_response,
        execute_service_action_func=execute_service_action,
        get_services_status_func=get_services_status,
        logger=log,
    )


@require_GET
def stats_view(request):
    return stats_response(stats_func=_get_stats)


@require_GET
def queue_snapshot(request):
    return queue_snapshot_response(
        snapshot_func=_build_queue_snapshot_payload,
        logger=log,
    )


# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS INTERNOS
# ══════════════════════════════════════════════════════════════════════════════

def _get_stats() -> dict:
    return calculate_queue_stats()


def _queue_diagnostic_snapshot() -> dict:
    """Fachada compatible con tests que parchean helpers desde queue_app.views."""
    status = dict(_service_queue_diagnostic_snapshot())
    locked = is_queue_locked()
    if locked != status.get('is_locked'):
        status['is_locked'] = locked
        status['lock_owner'] = get_queue_lock_owner() if locked else ''
        if locked:
            status['worker_stale'] = False
    return status


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
    return _diagnostics_iniciar_sap()


def _queue_runtime_is_orphaned(queue_status: dict, celery_status: dict | None = None) -> bool:
    return queue_runtime_is_orphaned(queue_status, celery_status)


def _stop_request_is_stale(queue_status: dict) -> bool:
    return stop_request_is_stale(
        queue_status,
        safe_stop_grace_seconds=QUEUE_SAFE_STOP_GRACE_SECONDS,
    )


def _recover_orphaned_queue_runtime_data(message: str | None = None) -> dict:
    return _service_recover_orphaned_queue_runtime_data(
        message,
        get_queue_lock_owner_func=get_queue_lock_owner,
        clear_queue_runtime_state_func=clear_queue_runtime_state,
        emit_item_update_func=emit_item_update,
        emit_stats_update_func=emit_stats_update,
        emit_queue_done_func=emit_queue_done,
        calculate_queue_stats_func=calculate_queue_stats,
        logger=log,
    )


def _recover_orphaned_queue_runtime(message: str | None = None) -> JsonResponse:
    return recover_orphaned_queue_runtime_response(
        message,
        get_queue_lock_owner_func=get_queue_lock_owner,
        clear_queue_runtime_state_func=clear_queue_runtime_state,
        emit_item_update_func=emit_item_update,
        emit_stats_update_func=emit_stats_update,
        emit_queue_done_func=emit_queue_done,
        calculate_queue_stats_func=calculate_queue_stats,
        logger=log,
    )


def _queue_lock_blocker_response(action: str) -> JsonResponse | None:
    return _service_queue_lock_blocker_response(
        action,
        queue_diagnostic_snapshot_func=_queue_diagnostic_snapshot,
        queue_runtime_is_orphaned_func=_queue_runtime_is_orphaned,
        stop_request_is_stale_func=_stop_request_is_stale,
        recover_orphaned_queue_runtime_data_func=_recover_orphaned_queue_runtime_data,
        get_queue_lock_owner_func=get_queue_lock_owner,
        clear_queue_runtime_state_func=clear_queue_runtime_state,
        emit_stats_update_func=emit_stats_update,
        emit_queue_status_func=emit_queue_status,
        logger=log,
    )


# ══════════════════════════════════════════════════════════════════════════════
#  DETENER PROCESO
# ══════════════════════════════════════════════════════════════════════════════


@require_POST
def detener_queue(request):
    return detener_queue_response(
        request,
        parse_json_body_func=_parse_json_body,
        queue_diagnostic_snapshot_func=_queue_diagnostic_snapshot,
        redis_status_func=_check_redis_status,
        celery_status_func=_check_celery_status,
        queue_runtime_is_orphaned_func=_queue_runtime_is_orphaned,
        stop_request_is_stale_func=_stop_request_is_stale,
        recover_orphaned_queue_runtime_response_func=_recover_orphaned_queue_runtime,
        request_queue_stop_func=request_queue_stop,
        disarm_continuous_queue_func=disarm_continuous_queue,
        emit_queue_status_func=emit_queue_status,
        safe_stop_grace_seconds=QUEUE_SAFE_STOP_GRACE_SECONDS,
        logger=log,
    )


@require_POST
def procesar_pendientes(request):
    return procesar_pendientes_response(
        request,
        queue_lock_blocker_response_func=_queue_lock_blocker_response,
        close_active_pallet_for_processing_func=_close_active_pallet_for_processing,
        pdf_queryset_func=pallets_ready_for_pdf_queryset,
        close_sap_session_if_idle_func=_close_sap_session_if_idle,
        celery_start_blocker_response_func=_celery_start_blocker_response,
        sap_start_blocker_response_func=_sap_start_blocker_response,
        sap_start_decision_func=_sap_start_decision,
        dispatch_continuous_queue_func=_dispatch_continuous_queue,
        emit_queue_status_func=emit_queue_status,
        logger=log,
    )
