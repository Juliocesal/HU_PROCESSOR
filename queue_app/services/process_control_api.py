import json

from django.conf import settings
from django.http import JsonResponse
from django.utils import timezone

from queue_app.models import HUItem, Pallet


def ensure_sap_session():
    from core.sap_client import SAPClient

    return SAPClient.ensure_session_ready()


def sap_session_error_response(*, ensure_sap_session_func, diagnostic_error_message_func, logger):
    try:
        result = ensure_sap_session_func()
    except Exception as exc:
        logger.exception("sap_session_validation_failed")
        return JsonResponse({
            'ok': False,
            'error': f'No se pudo validar la sesion SAP: {diagnostic_error_message_func(exc)}',
            'sap_connected': False,
            'sap_user': '',
        }, status=409)

    if not isinstance(result, (list, tuple)) or len(result) < 3:
        logger.warning("sap_session_invalid_response result=%r", result)
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


def close_sap_session_if_idle(*, logger) -> None:
    """Cierra SAP si se abrio pero no se pudo despachar una tarea Celery."""
    if not getattr(settings, 'SAP_CLOSE_WHEN_QUEUE_IDLE', True):
        return

    try:
        from core.sap_client import SAPClient

        SAPClient.close_sessions()
    except Exception as exc:
        logger.warning("close_sap_session_if_idle_failed error=%s", exc)


def has_potential_queue_work(*, run_pdf=True, pdf_queryset_func) -> bool:
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
    has_pdf = run_pdf and pdf_queryset_func().exists()
    return has_active_items or has_pending or has_pdf


def has_ready_queue_work(*, run_pdf=True, pdf_queryset_func) -> bool:
    has_pending = HUItem.objects.filter(
        status=HUItem.STATUS_PENDING,
        pallet__status=Pallet.STATUS_READY,
    ).exists()
    return has_pending or (run_pdf and pdf_queryset_func().exists())


def dispatch_continuous_queue(
    *,
    run_f1=True,
    run_f2=True,
    run_pdf=True,
    process_queue_task,
    arm_continuous_queue_func,
    idle_timeout_seconds: int,
) -> None:
    process_queue_task.delay(
        run_f1=run_f1,
        run_f2=run_f2,
        run_pdf=run_pdf,
        continuous=True,
        idle_timeout=idle_timeout_seconds,
    )
    arm_continuous_queue_func()


def celery_start_blocker(
    *,
    redis_status_func,
    celery_status_func,
    blocked_message: str,
) -> dict | None:
    """
    Verifica broker y workers antes de abrir SAP.

    Redis activo solo confirma transporte; sin workers Celery la cola quedaria
    en espera indefinida. Por eso este bloqueo se ejecuta antes de iniciar o
    reusar una sesion SAP.
    """
    redis_status = redis_status_func()
    celery_status = celery_status_func(redis_status['ok'])

    if redis_status['ok'] and celery_status['ok']:
        return None

    if not redis_status['ok']:
        message = (
            'No se puede iniciar proceso: Redis no responde. '
            'Revisa el servicio Redis y el puerto configurado antes de iniciar la cola.'
        )
    else:
        message = blocked_message

    return {
        'message': message,
        'redis': redis_status,
        'celery': celery_status,
    }


def celery_start_blocker_response(*, celery_start_blocker_func) -> JsonResponse | None:
    blocker = celery_start_blocker_func()
    if not blocker:
        return None

    return JsonResponse({
        'ok': False,
        'error': blocker['message'],
        'redis': blocker['redis'],
        'celery': blocker['celery'],
    }, status=503)


def auto_start_queue_after_pallet_close(
    *,
    run_f1=True,
    run_f2=True,
    run_pdf=True,
    is_continuous_queue_armed_func,
    is_queue_locked_func,
    has_ready_queue_work_func,
    celery_start_blocker_func,
    ensure_sap_session_func,
    dispatch_continuous_queue_func,
    emit_stats_update_func,
    emit_queue_status_func,
    logger,
) -> dict:
    """
    Reactiva Celery solo cuando el modo continuo ya fue armado manualmente.

    Esto evita arrancar SAP por accidente durante la captura inicial, pero permite
    que un pallet cerrado despues del timeout reactive la cola sin otro click.
    """
    if not is_continuous_queue_armed_func():
        return {'auto_started': False, 'auto_start_reason': 'not_armed'}

    if is_queue_locked_func() or HUItem.objects.filter(status=HUItem.STATUS_PROCESSING).exists():
        return {'auto_started': False, 'auto_start_reason': 'already_running'}

    if not has_ready_queue_work_func(run_pdf=run_pdf):
        return {'auto_started': False, 'auto_start_reason': 'no_ready_work'}

    celery_blocker = celery_start_blocker_func()
    if celery_blocker:
        return {
            'auto_started': False,
            'auto_start_reason': 'celery_unavailable',
            'auto_start_error': celery_blocker['message'],
            'celery': celery_blocker['celery'],
            'redis': celery_blocker['redis'],
        }

    connected, user, message = ensure_sap_session_func()
    if not connected:
        return {
            'auto_started': False,
            'auto_start_reason': 'sap_unavailable',
            'auto_start_error': message,
            'sap_user': user,
        }

    try:
        dispatch_continuous_queue_func(run_f1=run_f1, run_f2=run_f2, run_pdf=run_pdf)
    except Exception as exc:
        logger.exception("auto_start_queue_after_pallet_close_failed")
        return {
            'auto_started': False,
            'auto_start_reason': 'dispatch_failed',
            'auto_start_error': f'No se pudo iniciar Celery/Redis: {exc}',
        }

    emit_stats_update_func(None)
    emit_queue_status_func(
        'Pallet cerrado. El worker continuo se reactivo automaticamente.',
        badge='AUTO',
        mode='running',
        footer='Celery tomara el siguiente pallet listo sin otro click.',
        is_running=True,
    )
    return {'auto_started': True, 'auto_start_reason': 'pallet_closed'}


def start_processing_response(
    *,
    queue_lock_blocker_response_func,
    close_active_pallet_for_processing_func,
    close_sap_session_if_idle_func,
    celery_start_blocker_response_func,
    sap_session_error_response_func,
    dispatch_continuous_queue_func,
    emit_queue_status_func,
    pdf_queryset_func,
    logger,
) -> JsonResponse:
    lock_response = queue_lock_blocker_response_func('iniciar el proceso')
    if lock_response:
        return lock_response

    close_active_pallet_for_processing_func()
    pending_items = HUItem.objects.filter(
        status=HUItem.STATUS_PENDING,
        pallet__status=Pallet.STATUS_READY,
    )
    count = pending_items.count()
    pdf_count = pdf_queryset_func().count()

    if count == 0 and pdf_count == 0:
        close_sap_session_if_idle_func()
        return JsonResponse({
            'ok':    False,
            'error': 'No hay HUs pendientes ni PDFs por imprimir',
        }, status=400)

    celery_error = celery_start_blocker_response_func()
    if celery_error:
        return celery_error

    sap_error = sap_session_error_response_func()
    if sap_error:
        return sap_error

    try:
        dispatch_continuous_queue_func()
    except Exception as exc:
        close_sap_session_if_idle_func()
        logger.exception("start_processing celery_dispatch_failed")
        return JsonResponse({
            'ok': False,
            'error': f'No se pudo iniciar Celery/Redis: {exc}',
        }, status=503)

    emit_queue_status_func(
        'Cola enviada a Celery. Preparando SAP para el primer pallet.',
        badge='INICIO',
        mode='running',
        footer=f'{count} HU(s) pendiente(s), {pdf_count} PDF(s) por imprimir.',
        is_running=True,
    )
    logger.info("start_processing launched queue task for %s HUs", count)
    return JsonResponse({
        'ok':    True,
        'count': count,
        'pdf_count': pdf_count,
        'message': f'Iniciando procesamiento de {count} HU{"s" if count != 1 else ""}...',
    })


def queue_runtime_is_orphaned(queue_status: dict, celery_status: dict | None = None) -> bool:
    return bool(queue_status.get('is_locked') and queue_status.get('worker_stale'))


def stop_request_is_stale(queue_status: dict, *, safe_stop_grace_seconds: int) -> bool:
    age = queue_status.get('stop_request_age_seconds')
    return bool(
        queue_status.get('is_locked')
        and queue_status.get('stop_requested')
        and age is not None
        and age >= safe_stop_grace_seconds
    )


def recover_orphaned_queue_runtime_data(
    message: str | None = None,
    *,
    get_queue_lock_owner_func,
    clear_queue_runtime_state_func,
    emit_item_update_func,
    emit_stats_update_func,
    emit_queue_done_func,
    calculate_queue_stats_func,
    logger,
) -> dict:
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
    invalidated_owner = get_queue_lock_owner_func()
    clear_queue_runtime_state_func(
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
        emit_item_update_func(item)

    if affected_pallet_ids:
        Pallet.objects.filter(pk__in=affected_pallet_ids).update(
            processing_finished_at=now,
        )

    emit_stats_update_func(None)
    result = {
        'status': 'stopped',
        'message': message,
        'pallets_processed': 0,
        'hus_processed': 0,
        'errors': len(interrupted_items),
    }
    emit_queue_done_func(result)
    logger.warning(
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
        'stats': calculate_queue_stats_func(),
    }


def recover_orphaned_queue_runtime_response(message: str | None = None, **kwargs) -> JsonResponse:
    return JsonResponse(recover_orphaned_queue_runtime_data(message, **kwargs))


def queue_lock_blocker_response(
    action: str,
    *,
    queue_diagnostic_snapshot_func,
    queue_runtime_is_orphaned_func,
    stop_request_is_stale_func,
    recover_orphaned_queue_runtime_data_func,
    get_queue_lock_owner_func,
    clear_queue_runtime_state_func,
    emit_stats_update_func,
    emit_queue_status_func,
    logger,
) -> JsonResponse | None:
    queue_status = queue_diagnostic_snapshot_func()
    if not queue_status.get('is_locked'):
        return None

    if queue_runtime_is_orphaned_func(queue_status) or stop_request_is_stale_func(queue_status):
        if queue_status.get('processing_hus'):
            data = recover_orphaned_queue_runtime_data_func(
                'Se libero un lock anterior, pero habia HUs marcadas como processing. '
                'Revisa esas HUs antes de iniciar otra corrida.'
            )
            data['ok'] = False
            data['error'] = data['message']
            return JsonResponse(data, status=409)

        invalidated_owner = queue_status.get('lock_owner') or get_queue_lock_owner_func()
        clear_queue_runtime_state_func(
            reason='lock_blocker_stale_runtime',
            invalidated_owner=invalidated_owner,
        )
        logger.warning(
            "queue_lock_blocker stale_runtime_recovered invalidated_owner=%s",
            invalidated_owner or '<none>',
        )
        emit_stats_update_func(None)
        emit_queue_status_func(
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


def detener_queue_response(
    request,
    *,
    parse_json_body_func,
    queue_diagnostic_snapshot_func,
    redis_status_func,
    celery_status_func,
    queue_runtime_is_orphaned_func,
    stop_request_is_stale_func,
    recover_orphaned_queue_runtime_response_func,
    request_queue_stop_func,
    disarm_continuous_queue_func,
    emit_queue_status_func,
    safe_stop_grace_seconds: int,
    logger,
) -> JsonResponse:
    body = parse_json_body_func(request)
    force_requested = bool(body.get('force'))
    queue_status = queue_diagnostic_snapshot_func()
    redis_status = redis_status_func()
    celery_status = celery_status_func(redis_status['ok'], queue_status)
    if queue_runtime_is_orphaned_func(queue_status, celery_status):
        return recover_orphaned_queue_runtime_response_func()

    if stop_request_is_stale_func(queue_status):
        message = (
            'La detencion segura no recibio confirmacion del worker. '
            'Se libero la cola para evitar que la UI quede bloqueada.'
        )
        return recover_orphaned_queue_runtime_response_func(message)

    if force_requested and queue_status.get('stop_requested'):
        remaining = max(
            0,
            safe_stop_grace_seconds - int(queue_status.get('stop_request_age_seconds') or 0),
        )
        return JsonResponse({
            'ok': True,
            'waiting_for_safe_stop': True,
            'message': (
                f'Detencion segura en curso. Si no termina, podras liberar la cola en {remaining}s.'
            ),
            'force_available_after_seconds': remaining,
        })

    stopped = request_queue_stop_func()
    logger.info("detener_queue stop_requested=%s", stopped)
    if not stopped:
        return JsonResponse({'ok': False, 'error': 'No hay proceso activo'}, status=409)
    disarm_continuous_queue_func()
    emit_queue_status_func(
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
        'force_available_after_seconds': safe_stop_grace_seconds,
    })


def procesar_pendientes_response(
    request,
    *,
    queue_lock_blocker_response_func,
    close_active_pallet_for_processing_func,
    pdf_queryset_func,
    close_sap_session_if_idle_func,
    celery_start_blocker_response_func,
    sap_session_error_response_func,
    dispatch_continuous_queue_func,
    emit_queue_status_func,
    logger,
) -> JsonResponse:
    body = json.loads(request.body) if request.body else {}
    run_f1 = body.get('run_f1', True)
    run_f2 = body.get('run_f2', True)
    run_pdf = body.get('run_pdf', True)

    lock_response = queue_lock_blocker_response_func('iniciar el proceso')
    if lock_response:
        return lock_response

    if HUItem.objects.filter(status=HUItem.STATUS_PROCESSING).exists():
        return JsonResponse({'ok': False, 'error': 'Ya hay HUs procesando'}, status=409)

    close_active_pallet_for_processing_func()
    items = HUItem.objects.filter(
        status=HUItem.STATUS_PENDING,
        pallet__status=Pallet.STATUS_READY,
    )
    count = items.count()
    pdf_count = pdf_queryset_func().count() if run_pdf else 0

    if not count and not pdf_count:
        close_sap_session_if_idle_func()
        return JsonResponse({'ok': False, 'error': 'No hay HUs pendientes ni PDFs por imprimir'})

    celery_error = celery_start_blocker_response_func()
    if celery_error:
        return celery_error

    sap_error = sap_session_error_response_func()
    if sap_error:
        return sap_error

    try:
        dispatch_continuous_queue_func(run_f1=run_f1, run_f2=run_f2, run_pdf=run_pdf)
    except Exception as exc:
        close_sap_session_if_idle_func()
        logger.exception("procesar_pendientes celery_dispatch_failed")
        return JsonResponse({
            'ok': False,
            'error': f'No se pudo iniciar Celery/Redis: {exc}',
        }, status=503)

    emit_queue_status_func(
        'Cola enviada a Celery. Preparando SAP para el primer pallet.',
        badge='INICIO',
        mode='running',
        footer=f'{count} HU(s) pendiente(s), {pdf_count} PDF(s) por imprimir.',
        is_running=True,
    )
    logger.info(
        "procesar_pendientes sequential_task_started count=%s pdf_count=%s",
        count,
        pdf_count,
    )
    return JsonResponse({'ok': True, 'count': count, 'pdf_count': pdf_count})
