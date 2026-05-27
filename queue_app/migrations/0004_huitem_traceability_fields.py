from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('queue_app', '0003_pallet_processing_started_at'),
    ]

    operations = [
        migrations.AddField(
            model_name='huitem',
            name='error_msg',
            field=models.CharField(blank=True, default='', max_length=255),
        ),
        migrations.AddField(
            model_name='huitem',
            name='processing_ms',
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name='huitem',
            name='processing_started_at',
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='huitem',
            name='pdf_msg',
            field=models.CharField(blank=True, default='', max_length=255),
        ),
        migrations.AddField(
            model_name='huitem',
            name='pdf_ms',
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name='huitem',
            name='pdf_status',
            field=models.CharField(blank=True, default='', max_length=20),
        ),
    ]
