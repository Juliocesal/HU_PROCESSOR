import json

from django.core.management.color import no_style
from django.db import connection, transaction
from django.http import JsonResponse

from queue_app.models import HUItem, Pallet


REPROCESS_ERROR_STATUSES = [HUItem.STATUS_ERROR, HUItem.STATUS_HU_NOT_FOUND]
REPROCESS_ALL_STATUSES = [
    HUItem.STATUS_OK,
    HUItem.STATUS_DUPLICATE,
    HUItem.STATUS_ERROR,
    HUItem.STATUS_HU_NOT_FOUND,
]
PDF_RETRY_BLOCKED_HU_STATUSES = [
    HUItem.STATUS_PENDING,
    HUItem.STATUS_PROCESSING,
    HUItem.STATUS_ERROR,
    HUItem.STATUS_HU_NOT_FOUND,
]


def _pdf_error_pallets_queryset():
    """Pallets que pueden repetir ZE16/PDF sin volver a ejecutar F1/F2."""
    return (
        Pallet.objects
        .filter(
            status=Pallet.STATUS_READY,
            pdf_status=Pallet.PDF_STATUS_ERROR,
            items__status__in=[HUItem.STATUS_OK, HUItem.STATUS_DUPLICATE],
        )
        .exclude(items__status__in=PDF_RETRY_BLOCKED_HU_STATUSES)
        .distinct()
    )


def new_pallet_result(
    *,
    run_f1=True,
    run_f2=True,
    run_pdf=True,
    create_next_active_pallet_func,
    auto_start_queue_after_pallet_close_func,
    emit_pallet_created_func,
    diagnostic_error_message_func,
    logger,
) -> dict:
    """Crea el siguiente pallet y conserva la reactivacion del worker por callback."""
    pallet, empty_pallet = create_next_active_pallet_func()
    if empty_pallet:
        return {
            'ok':        False,
            'error':     f'El pallet actual (P{empty_pallet.pk:02d}) esta vacio. Escanea un HU primero.',
            'type':      'empty_pallet',
            'pallet_id': empty_pallet.pk,
        }

    pallet_id = pallet.pk

    try:
        auto_start_result = auto_start_queue_after_pallet_close_func(
            run_f1=run_f1,
            run_f2=run_f2,
            run_pdf=run_pdf,
        )
    except Exception as exc:
        logger.exception("new_pallet auto_start_failed pallet=%s", pallet_id)
        auto_start_result = {
            'auto_started': False,
            'auto_start_reason': 'auto_start_failed',
            'auto_start_error': diagnostic_error_message_func(exc),
        }

    response_warnings = []
    needs_resync = False
    try:
        emit_pallet_created_func(pallet)
    except Exception:
        logger.exception("new_pallet websocket_emit_failed pallet=%s", pallet_id)
        needs_resync = True
        response_warnings.append(
            'El pallet se creo, pero la UI necesita resincronizarse.'
        )

    logger.info("new_pallet created id=%s", pallet_id)

    return {
        'ok':        True,
        'pallet_id': pallet_id,
        'message':   f'Nuevo pallet #{pallet_id} iniciado.',
        'warnings':  response_warnings,
        'needs_resync': needs_resync,
        **auto_start_result,
    }


def delete_hu_response(
    hu_code,
    *,
    queue_mutation_error_func,
    recalculate_pallet_after_hu_delete_func,
    get_stats_func,
    emit_hu_deleted_func,
    emit_item_update_func,
    emit_stats_update_func,
    logger,
) -> JsonResponse:
    queue_error = queue_mutation_error_func('borrar HUs')
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
    pdf_reset = recalculate_pallet_after_hu_delete_func(pallet)
    logger.info("delete_hu hu=%s pallet=%s", hu_code, pallet.pk)

    if not pallet.items.exists() and Pallet.objects.count() > 1:
        pallet.delete()
        stats = get_stats_func()
        emit_hu_deleted_func(hu_code, pallet.pk, pallet_deleted=True, stats=stats)
        emit_stats_update_func(None)
        return JsonResponse({
            'ok': True,
            'pallet_deleted': True,
            'stats': stats,
        })

    for remaining_item in pallet.items.select_related('pallet'):
        emit_item_update_func(remaining_item)
    stats = get_stats_func()
    emit_hu_deleted_func(hu_code, pallet.pk, stats=stats)
    emit_stats_update_func(pallet)

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


def delete_pallet_response(
    pallet_id,
    *,
    queue_mutation_error_func,
    get_stats_func,
    emit_pallet_deleted_func,
    logger,
) -> JsonResponse:
    queue_error = queue_mutation_error_func('borrar pallets')
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
    pallet.delete()
    stats = get_stats_func()
    emit_pallet_deleted_func(pallet_id, stats=stats)
    logger.info("delete_pallet id=%s hu_count=%s", pallet_id, count)

    return JsonResponse({'ok': True, 'deleted_hus': count, 'stats': stats})


def clear_queue_response(
    *,
    is_queue_locked_func,
    disarm_continuous_queue_func,
    get_stats_func,
    emit_queue_cleared_func,
    logger,
) -> JsonResponse:
    if is_queue_locked_func():
        return JsonResponse({
            'ok': False,
            'error': 'Hay una tarea de procesamiento activa. Detenla y espera la confirmacion antes de limpiar.',
        }, status=409)

    if HUItem.objects.filter(status=HUItem.STATUS_PROCESSING).exists():
        return JsonResponse({
            'ok':    False,
            'error': 'Hay HUs procesando — detén el proceso antes de limpiar',
        }, status=409)

    disarm_continuous_queue_func()
    HUItem.objects.all().delete()
    Pallet.objects.all().delete()
    reset_queue_sequences()
    pallet = Pallet.objects.create(status=Pallet.STATUS_ACTIVE)
    stats = get_stats_func()
    emit_queue_cleared_func(pallet.pk, stats=stats)

    logger.info("clear_queue done reset_pallet_id=%s", pallet.pk)
    return JsonResponse({'ok': True, 'pallet_id': pallet.pk, 'stats': stats})


def reset_queue_sequences():
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


def queue_mutation_error(action: str, *, is_queue_locked_func) -> str:
    """
    Protege cambios destructivos mientras Celery/SAP procesa.

    El escaneo sigue permitido; esta validacion evita borrar datos mientras
    existe lock Redis o HUs en processing.
    """
    if is_queue_locked_func() or HUItem.objects.filter(status=HUItem.STATUS_PROCESSING).exists():
        return f'No se puede {action} mientras la cola está en procesamiento.'

    return ''


def reprocess_queue_response(
    request,
    *,
    queue_lock_blocker_response_func,
    celery_start_blocker_response_func,
    sap_session_error_response_func,
    dispatch_continuous_queue_func,
    close_sap_session_if_idle_func,
    emit_item_update_func,
    emit_stats_update_func,
    emit_queue_status_func,
    logger,
) -> JsonResponse:
    lock_response = queue_lock_blocker_response_func('reprocesar')
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
    if mode not in ('all', 'errors', 'pdf_errors'):
        return JsonResponse({'ok': False, 'error': 'Modo de reproceso invalido'}, status=400)

    if mode == 'pdf_errors':
        pallets = list(_pdf_error_pallets_queryset())
        affected_pallet_ids = {pallet.pk for pallet in pallets}
        if not affected_pallet_ids:
            return JsonResponse({
                'ok': False,
                'error': 'No hay pallets con error ZE16/PDF listos para reintentar.',
            }, status=400)

        celery_error = celery_start_blocker_response_func()
        if celery_error:
            return celery_error

        sap_error = sap_session_error_response_func()
        if sap_error:
            return sap_error

        with transaction.atomic():
            Pallet.objects.filter(pk__in=affected_pallet_ids).update(
                status=Pallet.STATUS_READY,
                receipt_done_at=None,
                pdf_status='',
                pdf_msg='',
                pdf_ms=0,
            )
            HUItem.objects.filter(pallet_id__in=affected_pallet_ids).update(
                receipt_done_at=None,
                pdf_status='',
                pdf_msg='',
                pdf_ms=0,
            )

        items = list(
            HUItem.objects
            .filter(pallet_id__in=affected_pallet_ids)
            .select_related('pallet')
            .order_by('pallet_id', 'added_at', 'id')
        )
        for item in items:
            emit_item_update_func(item)

        count = len(items)
        emit_stats_update_func(None)
        try:
            dispatch_continuous_queue_func()
        except Exception as exc:
            close_sap_session_if_idle_func()
            logger.exception("reprocess_pdf_errors celery_dispatch_failed")
            return JsonResponse({
                'ok': False,
                'error': f'No se pudo iniciar Celery/Redis: {exc}',
            }, status=503)

        pallet_count = len(affected_pallet_ids)
        emit_queue_status_func(
            f'Reintento ZE16/PDF preparado para {pallet_count} pallet(s).',
            badge='REINTENTO PDF',
            mode='running',
            footer='Celery volvera a consultar ZE16 e imprimir los receipts pendientes.',
            is_running=True,
        )
        logger.info(
            "reprocess_queue mode=pdf_errors pallets=%d hus=%d",
            pallet_count,
            count,
        )
        return JsonResponse({
            'ok': True,
            'count': count,
            'pallet_count': pallet_count,
            'pallet_ids': sorted(affected_pallet_ids),
            'mode': mode,
        })

    target_statuses = (
        REPROCESS_ERROR_STATUSES
        if mode == 'errors'
        else REPROCESS_ALL_STATUSES
    )

    items = list(HUItem.objects.filter(status__in=target_statuses).select_related('pallet'))
    count = len(items)

    if count == 0:
        return JsonResponse({'ok': False, 'error': 'No hay HUs para reprocesar'}, status=400)

    celery_error = celery_start_blocker_response_func()
    if celery_error:
        return celery_error

    sap_error = sap_session_error_response_func()
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
        emit_item_update_func(item)

    Pallet.objects.filter(pk__in=affected_pallet_ids).update(
        status=Pallet.STATUS_READY,
        processing_started_at=None,
        processing_finished_at=None,
        f2_done_at=None,
        receipt_done_at=None,
        pdf_status='',
        pdf_msg='',
        pdf_ms=0,
    )

    emit_stats_update_func(None)
    try:
        dispatch_continuous_queue_func()
    except Exception as exc:
        close_sap_session_if_idle_func()
        logger.exception("reprocess_queue celery_dispatch_failed")
        return JsonResponse({
            'ok': False,
            'error': f'No se pudo iniciar Celery/Redis: {exc}',
        }, status=503)

    emit_queue_status_func(
        f'Reproceso preparado. {count} HU(s) vuelven a la cola.',
        badge='REPROCESO',
        mode='running',
        footer='Celery retomara la cola con los HUs marcados como pendientes.',
        is_running=True,
    )
    logger.info("reprocess_queue mode=%s count=%d", mode, count)
    return JsonResponse({'ok': True, 'count': count, 'mode': mode})
