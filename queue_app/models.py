from django.db import models
from django.utils import timezone


class Pallet(models.Model):
    """Pallet persistido en base de datos con timestamps de ciclo de vida."""

    STATUS_ACTIVE = 'active'
    STATUS_READY = 'ready'
    STATUS_DONE = 'done'
    PDF_STATUS_OK = 'ok'
    PDF_STATUS_ERROR = 'error'

    # SQL Server / historico operacional:
    # Esta tabla representa la entidad padre del proceso. El DBA debe conservar
    # `id` como identificador del pallet y usarlo para relacionar HUs, tiempos
    # F1/F2/PDF y consultas de auditoria.
    # Campos principales para reporte:
    # - id: numero interno de pallet usado por la UI como P01, P02, etc.
    # - origin_code: origen del pallet (THA, BRA/ATL, ITA, FHR, CNA, desconocido).
    # - status: estado general del pallet dentro de la cola.
    # - created_at: hora en que se creo el pallet.
    # - processing_started_at: hora real de inicio del proceso F1.
    # - processing_finished_at: hora en que el pallet termina, con exito o error.
    # - f2_done_at: hora en que termino F2/Separazione.
    # - receipt_done_at: hora de generacion/confirmacion de impresion PDF.
    # - pdf_status, pdf_msg, pdf_ms: resultado, mensaje y duracion del PDF.
    # WHERE recomendados: id, origin_code, status, created_at__range,
    # processing_started_at__range, processing_finished_at__range,
    # receipt_done_at__range y pdf_status.
    created_at = models.DateTimeField(default=timezone.now)
    processing_started_at = models.DateTimeField(null=True, blank=True)
    processing_finished_at = models.DateTimeField(null=True, blank=True)
    f2_done_at = models.DateTimeField(null=True, blank=True)
    receipt_done_at = models.DateTimeField(
        null=True,
        blank=True,
        db_column='sp01_done_at',
        help_text='Timestamp de la generacion/impresion de recibo ZE16.',
    )
    pdf_status = models.CharField(max_length=20, blank=True, default='')
    pdf_msg = models.CharField(max_length=255, blank=True, default='')
    pdf_ms = models.IntegerField(default=0)
    status = models.CharField(max_length=20, default=STATUS_ACTIVE)
    origin_code = models.CharField(max_length=20, blank=True, default='')

    class Meta:
        ordering = ['id']

    def __str__(self):
        return f"Pallet #{self.pk} ({self.status})"

    @property
    def processing_time_display(self) -> str:
        """Duracion legible desde el inicio real de F1 hasta finalizar impresion."""
        if not self.processing_started_at:
            return ''

        end = self.processing_finished_at or self.receipt_done_at or timezone.now()
        total_seconds = max(0, int((end - self.processing_started_at).total_seconds()))
        if total_seconds < 60:
            return f"{total_seconds}s"

        minutes = total_seconds // 60
        seconds = total_seconds % 60
        return f"{minutes}min" if seconds == 0 else f"{minutes}min {seconds}s"

    @property
    def pdf_duration_display(self) -> str:
        """Duracion legible del ciclo PDF: generar archivo y confirmar impresion."""
        if self.pdf_ms <= 0:
            return ''

        if self.pdf_ms < 1000:
            return f"{self.pdf_ms}ms"

        seconds = self.pdf_ms / 1000
        return f"{seconds:.1f}s".replace(".0s", "s")

    @property
    def pdf_display(self) -> str:
        """Texto compacto para la columna PDF de la UI."""
        if self.pdf_status == self.PDF_STATUS_OK:
            duration = self.pdf_duration_display
            return f"OK ({duration})" if duration else "OK"

        if self.pdf_status == self.PDF_STATUS_ERROR:
            return self.pdf_msg or "Error PDF"

        return ''

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

    # SQL Server / historico por HU:
    # Esta tabla es la fuente principal para consultar que HUs se procesaron,
    # cuales fallaron y en que momento cambio su estado. El DBA debe mantener
    # la relacion `pallet_id` para unir cada HU con `Pallet`.
    # Campos principales para reporte:
    # - hu_code: codigo HU escaneado por el operador.
    # - pallet_id: pallet al que pertenece el HU.
    # - origin_code: origen detectado para reglas de agrupacion.
    # - status: estado final o actual del HU (pending, processing, ok, error, etc.).
    # - phase1_msg: resultado/mensaje del paso F1 Acknowledge.
    # - phase2_msg: resultado/mensaje del paso F2 Separazione.
    # - phase2_ms: tiempo medido para F2.
    # - error_msg: problema presentado cuando el estado final no fue exitoso.
    # - processing_started_at: hora en que Celery tomo el HU para SAP.
    # - processing_ms: duracion total F1/F2 medida para el HU.
    # - added_at: hora de captura/escaneo en UI.
    # - f1_done_at: hora en que termino F1.
    # - processed_at: hora final del procesamiento del HU.
    # - receipt_done_at: hora de recibo ZE16/PDF cuando aplica al HU.
    # - pdf_status, pdf_msg, pdf_ms: resultado PDF copiado al HU para consulta directa.
    # WHERE recomendados: hu_code, pallet_id, origin_code, status,
    # added_at__range, f1_done_at__range, processed_at__range,
    # pdf_status y processing_ms.
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
    error_msg = models.CharField(max_length=255, blank=True, default='')

    processing_started_at = models.DateTimeField(null=True, blank=True)
    processing_ms = models.IntegerField(default=0)
    processed_at = models.DateTimeField(null=True, blank=True)
    f1_done_at = models.DateTimeField(null=True, blank=True)
    receipt_done_at = models.DateTimeField(
        null=True,
        blank=True,
        db_column='sp01_done_at',
        help_text='Timestamp de recibo ZE16/PDF para esta HU.',
    )
    pdf_status = models.CharField(max_length=20, blank=True, default='')
    pdf_msg = models.CharField(max_length=255, blank=True, default='')
    pdf_ms = models.IntegerField(default=0)

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

    @property
    def processing_duration_display(self) -> str:
        """Duracion legible del procesamiento SAP de esta HU."""
        if self.processing_ms <= 0:
            return ''
        if self.processing_ms < 1000:
            return f'{self.processing_ms}ms'
        seconds = self.processing_ms / 1000
        return f'{seconds:.1f}s'.replace('.0s', 's')


class ScanLog(models.Model):
    """Registro de auditoria por cada escaneo, incluyendo duplicados y errores."""

    # SQL Server / auditoria de captura:
    # Esta tabla permite reconstruir intentos de escaneo, duplicados, errores de
    # validacion y eventos que no siempre llegan a convertirse en HUItem.
    # WHERE recomendados: hu_code, result y scanned_at__range.
    hu_code = models.CharField(max_length=50)
    scanned_at = models.DateTimeField(default=timezone.now)
    result = models.CharField(max_length=20)
    message = models.CharField(max_length=255, blank=True)

    class Meta:
        ordering = ['-scanned_at']

    def __str__(self):
        return f"{self.hu_code} -> {self.result} @ {self.scanned_at:%H:%M:%S}"
