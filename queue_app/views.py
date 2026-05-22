import csv
import json
import logging
from datetime import datetime
from django.core.management.color import no_style
from django.db import connection
from django.http import JsonResponse, HttpResponse
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_POST, require_GET
from django.shortcuts import render

from core.hu_origins import detect_origin, is_pallet_separator

from queue_app.models import HUItem, Pallet, ScanLog
from queue_app.tasks import is_queue_locked, process_queue_task, request_queue_stop
from queue_app.utils import (
    calculate_queue_stats,
    emit_item_update,
    emit_pallet_created,
    emit_stats_update,
    pallets_ready_for_pdf_queryset,
)

log = logging.getLogger(__name__)

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

    if not raw:
        return JsonResponse({'ok': False, 'error': 'Código vacío'}, status=400)

    # ── Separador de pallet ───────────────────────────────────────────────────
    if is_pallet_separator(raw):
        result = _new_pallet_logic()
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

    emit_item_update(item)
    emit_stats_update(item.pallet)

    log.info("scan_hu hu=%s pallet=%s origin=%s", raw, pallet.pk, origin.code)

    return JsonResponse({
        'ok':        True,
        'hu_code':   raw,
        'pallet_id': pallet.pk,
        'origin':    origin.label,
        'status':    item.status,
        'stats':     _get_stats(),
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
    result = _new_pallet_logic()
    if result.get('ok'):
        result['stats'] = _get_stats()
    return JsonResponse(result)


def _new_pallet_logic() -> dict:
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

    pallet = Pallet.objects.create(status=Pallet.STATUS_ACTIVE)
    emit_pallet_created(pallet)
    log.info("new_pallet created id=%s", pallet.pk)

    return {
        'ok':        True,
        'pallet_id': pallet.pk,
        'message':   f'Nuevo pallet #{pallet.pk} iniciado.',
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
        emit_stats_update(None)
        return JsonResponse({
            'ok': True,
            'pallet_deleted': True,
            'stats': _get_stats(),
        })

    for remaining_item in pallet.items.select_related('pallet'):
        emit_item_update(remaining_item)
    emit_stats_update(pallet)

    return JsonResponse({
        'ok': True,
        'pallet_deleted': False,
        'pdf_reset': pdf_reset,
        'pallet_id': pallet.pk,
        'pdf_status': pallet.pdf_status,
        'pdf_display': pallet.pdf_display,
        'pdf_msg': pallet.pdf_msg,
        'stats': _get_stats(),
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
        pallet.status = Pallet.STATUS_ACTIVE
        pallet.save(update_fields=[
            'pdf_status',
            'pdf_msg',
            'pdf_ms',
            'receipt_done_at',
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
    log.info("delete_pallet id=%s hu_count=%s", pallet_id, count)

    return JsonResponse({'ok': True, 'deleted_hus': count})


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

    deleted_items   = HUItem.objects.all().delete()
    deleted_pallets = Pallet.objects.all().delete()
    _reset_queue_sequences()
    # Crear pallet inicial vacío
    pallet = Pallet.objects.create(status=Pallet.STATUS_ACTIVE)

    log.info("clear_queue done reset_pallet_id=%s", pallet.pk)
    return JsonResponse({'ok': True, 'pallet_id': pallet.pk})


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


def _sap_session_error_response():
    from core.sap_client import SAPClient

    connected, user, message = SAPClient.ensure_session_ready()
    if connected:
        return None

    return JsonResponse({
        'ok': False,
        'error': message,
        'sap_connected': False,
        'sap_user': user,
    }, status=409)



@require_POST
def reprocess_queue(request):
    """
    Equivale a _reprocess() — resetea HUs procesados a pending
    y dispara las tasks de Celery de nuevo.
    """
    if HUItem.objects.filter(status=HUItem.STATUS_PROCESSING).exists():
        return JsonResponse({
            'ok':    False,
            'error': 'Hay HUs procesando — espera a que terminen',
        }, status=409)

    if is_queue_locked():
        return JsonResponse({
            'ok': False,
            'error': 'Hay una tarea de procesamiento activa. Espera a que termine antes de reprocesar.',
        }, status=409)

    sap_error = _sap_session_error_response()
    if sap_error:
        return sap_error

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

    affected_pallet_ids = {item.pallet_id for item in items}
    for item in items:
        item.status = HUItem.STATUS_PENDING
        item.phase1_msg = ''
        item.phase2_msg = ''
        item.phase2_ms = 0
        item.processed_at = None
        item.f1_done_at = None
        item.receipt_done_at = None
        item.save(update_fields=[
            'status',
            'phase1_msg',
            'phase2_msg',
            'phase2_ms',
            'processed_at',
            'f1_done_at',
            'receipt_done_at',
        ])
        emit_item_update(item)

    Pallet.objects.filter(pk__in=affected_pallet_ids).update(
        status      = Pallet.STATUS_ACTIVE,
        f2_done_at  = None,
        receipt_done_at = None,
        pdf_status = '',
        pdf_msg    = '',
        pdf_ms     = 0,
    )

    emit_stats_update(None)
    try:
        process_queue_task.delay()
    except Exception as e:
        log.exception("reprocess_queue celery_dispatch_failed")
        return JsonResponse({
            'ok': False,
            'error': f'No se pudo iniciar Celery/Redis: {e}',
        }, status=503)

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
    pending_items = HUItem.objects.filter(status=HUItem.STATUS_PENDING)
    count = pending_items.count()

    if count == 0:
        return JsonResponse({
            'ok':    False,
            'error': 'No hay HUs pendientes para procesar',
        }, status=400)

    if is_queue_locked():
        return JsonResponse({
            'ok': False,
            'error': 'Ya hay una tarea de procesamiento activa',
        }, status=409)

    sap_error = _sap_session_error_response()
    if sap_error:
        return sap_error

    try:
        process_queue_task.delay()
    except Exception as e:
        log.exception("start_processing celery_dispatch_failed")
        return JsonResponse({
            'ok': False,
            'error': f'No se pudo iniciar Celery/Redis: {e}',
        }, status=503)

    log.info("start_processing launched queue task for %s HUs", count)
    return JsonResponse({
        'ok':    True,
        'count': count,
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
        'Agregada', 'Procesada',
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
            item.pallet.pdf_display,
            item.pallet.pdf_msg or '',
            item.pallet.pdf_ms or '',
            item.added_at.strftime('%d/%m/%Y %H:%M:%S')   if item.added_at   else '',
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
        return JsonResponse({'connected': ok, 'user': user})
    except Exception as e:
        return JsonResponse({'connected': False, 'user': '', 'error': str(e)})


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


def _get_or_create_active_pallet(hu_code: str, origin) -> Pallet:
    """
    Equivale a la lógica de asignación de pallet en HUQueue.add_hu().
    Decide si el HU va al pallet activo o crea uno nuevo.
    """
    active = Pallet.objects.filter(status=Pallet.STATUS_ACTIVE).order_by('-id').first()

    # Sin pallets — crear el primero
    if not active:
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
        return Pallet.objects.create(
            status=Pallet.STATUS_ACTIVE,
            origin_code=origin.code,
        )


    return active

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
    stopped = request_queue_stop()
    log.info("detener_queue stop_requested=%s", stopped)
    if not stopped:
        return JsonResponse({'ok': False, 'error': 'No hay proceso activo'}, status=409)
    return JsonResponse({'ok': True, 'message': 'Detencion solicitada. Esperando cierre seguro.'})


@require_POST
def procesar_pendientes(request):
    """Dispara una task secuencial para todos los HUs pendientes."""
    body   = json.loads(request.body) if request.body else {}
    run_f1 = body.get('run_f1', True)
    run_f2 = body.get('run_f2', True)
    run_pdf = body.get('run_pdf', True)

    items = HUItem.objects.filter(status=HUItem.STATUS_PENDING)
    count = items.count()
    pdf_count = pallets_ready_for_pdf_queryset().count() if run_pdf else 0

    if not count and not pdf_count:
        return JsonResponse({'ok': False, 'error': 'No hay HUs pendientes ni PDFs por imprimir'})

    if HUItem.objects.filter(status=HUItem.STATUS_PROCESSING).exists():
        return JsonResponse({'ok': False, 'error': 'Ya hay HUs procesando'}, status=409)

    if is_queue_locked():
        return JsonResponse({'ok': False, 'error': 'Ya hay una tarea de procesamiento activa'}, status=409)

    sap_error = _sap_session_error_response()
    if sap_error:
        return sap_error

    try:
        process_queue_task.delay(run_f1=run_f1, run_f2=run_f2, run_pdf=run_pdf)
    except Exception as e:
        log.exception("procesar_pendientes celery_dispatch_failed")
        return JsonResponse({
            'ok': False,
            'error': f'No se pudo iniciar Celery/Redis: {e}',
        }, status=503)

    log.info(
        "procesar_pendientes sequential_task_started count=%s pdf_count=%s",
        count,
        pdf_count,
    )
    return JsonResponse({'ok': True, 'count': count, 'pdf_count': pdf_count})
