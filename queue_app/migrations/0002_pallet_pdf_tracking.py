from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('queue_app', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='pallet',
            name='pdf_status',
            field=models.CharField(blank=True, default='', max_length=20),
        ),
        migrations.AddField(
            model_name='pallet',
            name='pdf_msg',
            field=models.CharField(blank=True, default='', max_length=255),
        ),
        migrations.AddField(
            model_name='pallet',
            name='pdf_ms',
            field=models.IntegerField(default=0),
        ),
    ]
