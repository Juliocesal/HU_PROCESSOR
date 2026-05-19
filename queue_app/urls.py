from django.urls import path
from . import views

urlpatterns = [
    # Página principal
    path('',                        views.queue_view,     name='queue'),

    # Acciones de escaneo y cola
    path('scan/',                   views.scan_hu,        name='scan_hu'),
    path('pallet/nuevo/',           views.new_pallet,     name='new_pallet'),
    path('hu/<str:hu_code>/borrar/',views.delete_hu,      name='delete_hu'),
    path('pallet/<int:pallet_id>/borrar/', views.delete_pallet, name='delete_pallet'),
    path('cola/limpiar/',           views.clear_queue,    name='clear_queue'),
    path('cola/reprocesar/',        views.reprocess_queue,name='reprocess_queue'),
    path('cola/iniciar/',           views.start_processing, name='start_processing'),

    # Datos / utilidades
    path('exportar/',               views.export_csv,     name='export_csv'),
    path('api/stats/',              views.stats_view,     name='stats'),
    path('api/sap-status/',         views.sap_status,     name='sap_status'),
    path('api/sap/iniciar/',        views.iniciar_sap_endpoint, name='iniciar_sap'),
    path('cola/detener/',           views.detener_queue,  name='detener_queue'),
    path('api/procesar/', views.procesar_pendientes, name='procesar_pendientes'),
]