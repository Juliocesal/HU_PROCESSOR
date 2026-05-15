from django.db import models
from django.utils import timezone


# ── Pallet ────────────────────────────────────────────────────────────────────

class Pallet(models.Model):
    """
    Antes era solo un int (pallet_id) en HUQueue.
    Ahora es una fila en la DB con sus timestamps propios.
    """

    STATUS_ACTIVE    = 'active'
    STATUS_DONE      = 'done'

    created_at   = models.DateTimeField(default=timezone.now)
    f2_done_at   = models.DateTimeField(null=True, blank=True)
    sp01_done_at = models.DateTimeField(null=True, blank=True)
    status       = models.CharField(max_length=20, default=STATUS_ACTIVE)

    # Origen del pallet (código del primer HU que entró)
    origin_code  = models.CharField(max_length=20, blank=True, default='')

    class Meta:
        ordering = ['id']

    def __str__(self):
        return f"Pallet #{self.pk} ({self.status})"

    # Equivalente a HUQueue.get_pallet_processing_time()
    @property
    def processing_time_display(self) -> str:
        start = self.created_at
        end   = self.sp01_done_at or self.f2_done_at or timezone.now()
        delta = (end - start).total_seconds()
        if delta < 60:
            return f"{int(delta)}s"
        mins = int(delta // 60)
        secs = int(delta % 60)
        return f"{mins}min" if secs == 0 else f"{mins}min {secs}s"

    # Equivalente a HUQueue.pallet_is_safe_to_delete()
    @property
    def is_safe_to_delete(self) -> bool:
        return not self.items.filter(
            status__in=['processing', 'ok', 'duplicate']
        ).exists()

    # Stats rápido del pallet
    @property
    def hu_count(self) -> int:
        return self.items.count()


# ── HUItem ────────────────────────────────────────────────────────────────────

class HUItem(models.Model):
    """
    Conversión directa del dataclass HUItem a modelo Django.
    Cada campo del dataclass = columna en la tabla.
    """

    # ── Mismos estados que en queue_model.py ──────────────────────────────────
    STATUS_PENDING      = 'pending'
    STATUS_PROCESSING   = 'processing'
    STATUS_OK           = 'ok'
    STATUS_DUPLICATE    = 'duplicate'
    STATUS_ERROR        = 'error'
    STATUS_HU_NOT_FOUND = 'hu_not_found'

    STATUS_CHOICES = [
        (STATUS_PENDING,      'Pendiente'),
        (STATUS_PROCESSING,   'Procesando...'),
        (STATUS_OK,           'OK'),
        (STATUS_DUPLICATE,    'Ya procesado'),
        (STATUS_ERROR,        'Error'),
        (STATUS_HU_NOT_FOUND, 'HU no existe'),
    ]

    # ── Campos — equivalentes exactos del dataclass ───────────────────────────
    hu_code      = models.CharField(max_length=50, unique=True)
    pallet       = models.ForeignKey(
        Pallet,
        on_delete=models.CASCADE,
        related_name='items'
    )

    # Origin se guarda como código string (THA, ITA, ATL, etc.)
    # La lógica de Origin sigue viviendo en core/hu_origins.py
    origin_code  = models.CharField(max_length=20, blank=True, default='')

    added_at     = models.DateTimeField(default=timezone.now)
    status       = models.CharField(
        max_length=20,
        choices=STATUS_CHOICES,
        default=STATUS_PENDING
    )

    phase1_msg   = models.CharField(max_length=255, blank=True, default='')
    phase2_msg   = models.CharField(max_length=255, blank=True, default='')
    phase2_ms    = models.IntegerField(default=0)

    processed_at  = models.DateTimeField(null=True, blank=True)
    f1_done_at    = models.DateTimeField(null=True, blank=True)
    sp01_done_at  = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ['added_at']

    def __str__(self):
        return f"{self.hu_code} ({self.status})"

    # ── Properties — misma lógica que el dataclass original ───────────────────

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


# ── ScanLog — registro de cada escaneo ───────────────────────────────────────

class ScanLog(models.Model):
    """
    Tabla de auditoría. Cada vez que se escanea una HU queda registrado,
    incluso si fue duplicado o error. No existía en la app desktop
    (se perdía al cerrar) — en web lo tenemos gratis.
    """
    hu_code    = models.CharField(max_length=50)
    scanned_at = models.DateTimeField(default=timezone.now)
    result     = models.CharField(max_length=20)   # ok / duplicate / error
    message    = models.CharField(max_length=255, blank=True)

    class Meta:
        ordering = ['-scanned_at']

    def __str__(self):
        return f"{self.hu_code} → {self.result} @ {self.scanned_at:%H:%M:%S}"