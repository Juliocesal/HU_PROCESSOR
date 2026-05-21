from django.db import models
from django.utils import timezone


class Pallet(models.Model):
    """Pallet persistido en base de datos con timestamps de ciclo de vida."""

    STATUS_ACTIVE = 'active'
    STATUS_DONE = 'done'

    # Variables para futura BD de consulta historica de pallets:
    # WHERE sugeridos: id/pallet, origin_code, status, created_at,
    # f2_done_at y receipt_done_at.
    created_at = models.DateTimeField(default=timezone.now)
    f2_done_at = models.DateTimeField(null=True, blank=True)
    receipt_done_at = models.DateTimeField(
        null=True,
        blank=True,
        db_column='sp01_done_at',
        help_text='Timestamp de la generacion/impresion de recibo ZE16.',
    )
    status = models.CharField(max_length=20, default=STATUS_ACTIVE)
    origin_code = models.CharField(max_length=20, blank=True, default='')

    class Meta:
        ordering = ['id']

    def __str__(self):
        return f"Pallet #{self.pk} ({self.status})"

    @property
    def processing_time_display(self) -> str:
        """Duracion legible desde creacion del pallet hasta recibo terminado."""
        end = self.receipt_done_at or self.f2_done_at or timezone.now()
        total_seconds = int((end - self.created_at).total_seconds())
        if total_seconds < 60:
            return f"{total_seconds}s"

        minutes = total_seconds // 60
        seconds = total_seconds % 60
        return f"{minutes}min" if seconds == 0 else f"{minutes}min {seconds}s"

    @property
    def is_safe_to_delete(self) -> bool:
        """Solo pallets no procesados o fallidos pueden eliminarse de la cola."""
        blocked_statuses = [
            HUItem.STATUS_PROCESSING,
            HUItem.STATUS_OK,
            HUItem.STATUS_DUPLICATE,
        ]
        return not self.items.filter(status__in=blocked_statuses).exists()

    @property
    def hu_count(self) -> int:
        return self.items.count()


class HUItem(models.Model):
    """Handling Unit escaneada dentro de la cola de procesamiento."""

    STATUS_PENDING = 'pending'
    STATUS_PROCESSING = 'processing'
    STATUS_OK = 'ok'
    STATUS_DUPLICATE = 'duplicate'
    STATUS_ERROR = 'error'
    STATUS_HU_NOT_FOUND = 'hu_not_found'

    STATUS_CHOICES = [
        (STATUS_PENDING, 'Pendiente'),
        (STATUS_PROCESSING, 'Procesando...'),
        (STATUS_OK, 'OK'),
        (STATUS_DUPLICATE, 'Ya procesado'),
        (STATUS_ERROR, 'Error'),
        (STATUS_HU_NOT_FOUND, 'HU no existe'),
    ]

    # Variables para futura BD de consulta historica de HUs:
    # hu_code=HU, pallet_id=Pallet, origin_code=Origen, status=Estado general,
    # phase1_msg=Estado/mensaje F1, phase2_msg=Estado/mensaje F2,
    # phase2_ms=tiempo F2, added_at=hora escaneo, processed_at=hora final,
    # f1_done_at=hora F1, receipt_done_at=hora ZE16/PDF.
    # WHERE sugeridos: hu_code, pallet_id, origin_code, status,
    # processed_at__range, added_at__range, f1_done_at__range.
    hu_code = models.CharField(max_length=50, unique=True)
    pallet = models.ForeignKey(
        Pallet,
        on_delete=models.CASCADE,
        related_name='items',
    )
    origin_code = models.CharField(max_length=20, blank=True, default='')
    added_at = models.DateTimeField(default=timezone.now)
    status = models.CharField(
        max_length=20,
        choices=STATUS_CHOICES,
        default=STATUS_PENDING,
    )

    phase1_msg = models.CharField(max_length=255, blank=True, default='')
    phase2_msg = models.CharField(max_length=255, blank=True, default='')
    phase2_ms = models.IntegerField(default=0)

    processed_at = models.DateTimeField(null=True, blank=True)
    f1_done_at = models.DateTimeField(null=True, blank=True)
    receipt_done_at = models.DateTimeField(
        null=True,
        blank=True,
        db_column='sp01_done_at',
        help_text='Timestamp de recibo ZE16/PDF para esta HU.',
    )

    class Meta:
        ordering = ['added_at']

    def __str__(self):
        return f"{self.hu_code} ({self.status})"

    @property
    def status_display(self) -> str:
        return dict(self.STATUS_CHOICES).get(self.status, self.status)

    @property
    def f1_display(self) -> str:
        if self.status == self.STATUS_PENDING:
            return ''
        if self.status == self.STATUS_PROCESSING:
            return '...'
        if self.status in (self.STATUS_OK, self.STATUS_DUPLICATE):
            return self.phase1_msg or 'OK'
        return self.phase1_msg or 'Error'

    @property
    def f2_display(self) -> str:
        if self.status in (self.STATUS_PENDING, self.STATUS_PROCESSING):
            return ''
        if self.status == self.STATUS_OK:
            return f'OK ({self.phase2_ms}ms)'
        if self.status == self.STATUS_DUPLICATE:
            return self.phase2_msg or ''
        return self.phase2_msg or ''


class ScanLog(models.Model):
    """Registro de auditoria por cada escaneo, incluyendo duplicados y errores."""

    # Variables para auditoria de captura:
    # WHERE sugeridos: hu_code, result, scanned_at__range.
    hu_code = models.CharField(max_length=50)
    scanned_at = models.DateTimeField(default=timezone.now)
    result = models.CharField(max_length=20)
    message = models.CharField(max_length=255, blank=True)

    class Meta:
        ordering = ['-scanned_at']

    def __str__(self):
        return f"{self.hu_code} -> {self.result} @ {self.scanned_at:%H:%M:%S}"
