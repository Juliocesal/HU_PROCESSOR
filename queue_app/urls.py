from django.urls import path

from . import views


urlpatterns = [
    path('', views.queue_view, name='queue'),
    path('documentation/', views.documentation_view, name='documentation'),

    path('scan/', views.scan_hu, name='scan_hu'),
    path('scan/batch/', views.scan_hu_batch, name='scan_hu_batch'),
    path('pallet/nuevo/', views.new_pallet, name='new_pallet'),
    path('hu/<str:hu_code>/borrar/', views.delete_hu, name='delete_hu'),
    path('pallet/<int:pallet_id>/borrar/', views.delete_pallet, name='delete_pallet'),

    path('cola/limpiar/', views.clear_queue, name='clear_queue'),
    path('cola/reprocesar/', views.reprocess_queue, name='reprocess_queue'),
    path('cola/iniciar/', views.start_processing, name='start_processing'),
    path('cola/detener/', views.detener_queue, name='detener_queue'),

    path('exportar/', views.export_csv, name='export_csv'),
    path('api/stats/', views.stats_view, name='stats'),
    path('api/queue-snapshot/', views.queue_snapshot, name='queue_snapshot'),
    path('api/sap-status/', views.sap_status, name='sap_status'),
    path('api/system-status/', views.system_status, name='system_status'),
    path('api/service-control/login/', views.service_control_login, name='service_control_login'),
    path('api/service-control/logout/', views.service_control_logout, name='service_control_logout'),
    path('api/service-control/status/', views.service_control_status, name='service_control_status'),
    path(
        'api/service-control/<str:service>/<str:action>/',
        views.service_control_action,
        name='service_control_action',
    ),
    path('api/sap/iniciar/', views.iniciar_sap_endpoint, name='iniciar_sap'),
    path('api/procesar/', views.procesar_pendientes, name='procesar_pendientes'),
]
