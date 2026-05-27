from django.contrib import admin

from .models import HUItem, Pallet, ScanLog


@admin.register(HUItem)
class HUItemAdmin(admin.ModelAdmin):
    list_display = [
        'hu_code',
        'pallet',
        'origin_code',
        'status',
        'processing_ms',
        'pdf_status',
        'added_at',
        'processed_at',
    ]
    list_filter = ['status', 'origin_code', 'pdf_status']
    search_fields = ['hu_code', 'phase1_msg', 'phase2_msg', 'pdf_msg', 'error_msg']


@admin.register(Pallet)
class PalletAdmin(admin.ModelAdmin):
    list_display = ['id', 'origin_code', 'status', 'pdf_status', 'pdf_ms', 'created_at', 'hu_count']
    list_filter = ['status', 'origin_code', 'pdf_status']


@admin.register(ScanLog)
class ScanLogAdmin(admin.ModelAdmin):
    list_display = ['hu_code', 'result', 'scanned_at', 'message']
    list_filter = ['result']
    search_fields = ['hu_code', 'message']
