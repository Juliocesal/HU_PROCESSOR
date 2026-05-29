from django.db import migrations, models
from django.utils import timezone


def backfill_finished_at(apps, schema_editor):
    Pallet = apps.get_model('queue_app', 'Pallet')
    now = timezone.now()

    for pallet in Pallet.objects.filter(processing_started_at__isnull=False):
        if pallet.processing_finished_at:
            continue

        if pallet.receipt_done_at:
            pallet.processing_finished_at = pallet.receipt_done_at
        elif pallet.pdf_status:
            pallet.processing_finished_at = pallet.f2_done_at or now
        else:
            continue

        pallet.save(update_fields=['processing_finished_at'])


class Migration(migrations.Migration):

    dependencies = [
        ('queue_app', '0004_huitem_traceability_fields'),
    ]

    operations = [
        migrations.AddField(
            model_name='pallet',
            name='processing_finished_at',
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.RunPython(backfill_finished_at, migrations.RunPython.noop),
    ]
