from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('queue_app', '0002_pallet_pdf_tracking'),
    ]

    operations = [
        migrations.AddField(
            model_name='pallet',
            name='processing_started_at',
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
