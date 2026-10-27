from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('bims', '0557_sitesetting_metadata_upload_template'),
    ]

    operations = [
        migrations.AddField(
            model_name='sitesetting',
            name='enable_ogc_api',
            field=models.BooleanField(
                default=False,
                help_text='Let logged-in users publish their filters as live layers (OGC API - Features) for ArcGIS or QGIS'),
        ),
    ]
