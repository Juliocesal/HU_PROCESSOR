import csv
from datetime import datetime

from django.conf import settings
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render

from queue_app.models import HUItem


def documentation_response(request):
    """Renderiza la documentacion operativa y tecnica del proyecto."""
    return render(request, 'queue_app/documentation.html')


def export_csv_response() -> HttpResponse:
    """Genera el CSV operativo de la cola actual sin modificar datos."""
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
            'ZMMTIJSEP', item.phase2_msg or '',
            item.phase2_ms or '',
            item.pdf_status or item.pallet.pdf_status,
            item.pdf_msg or item.pallet.pdf_msg or '',
            item.pdf_ms or item.pallet.pdf_ms or '',
            item.error_msg or '',
            item.processing_ms or '',
            item.added_at.strftime('%d/%m/%Y %H:%M:%S') if item.added_at else '',
            item.processing_started_at.strftime('%d/%m/%Y %H:%M:%S') if item.processing_started_at else '',
            item.processed_at.strftime('%d/%m/%Y %H:%M:%S') if item.processed_at else '',
        ])

    return response


def sap_status_response(*, sap_status_func) -> JsonResponse:
    return JsonResponse(sap_status_func())


def system_status_response(
    *,
    redis_status_func,
    queue_status_func,
    degraded_queue_status_func=None,
    celery_status_func,
    database_status_func,
    sap_status_func,
) -> JsonResponse:
    redis_status = redis_status_func()
    if redis_status['ok'] or degraded_queue_status_func is None:
        queue_status = queue_status_func()
    else:
        queue_status = degraded_queue_status_func(redis_status)
    celery_status = celery_status_func(redis_status['ok'], queue_status)
    database_status = database_status_func()

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
        'sap': sap_status_func(),
        'queue': queue_status,
    })


def stats_response(*, stats_func) -> JsonResponse:
    return JsonResponse(stats_func())


def queue_snapshot_response(*, snapshot_func, logger) -> JsonResponse:
    """Devuelve una foto completa de la cola para resincronizar solo la UI."""
    snapshot = snapshot_func()
    logger.info(
        "queue_snapshot requested items=%s is_running=%s",
        len(snapshot['items']),
        snapshot['stats'].get('is_running'),
    )
    return JsonResponse({'ok': True, **snapshot})
