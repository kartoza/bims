from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('bims', '0521_uploadsession_log_file'),
    ]

    operations = [
        migrations.AddField(
            model_name='sourcereference',
            name='publish_to_gbif',
            field=models.BooleanField(
                default=True,
                help_text='Allow this source reference to be published to GBIF. '
                          'Uncheck to exclude it from all GBIF publish schedules.'
            ),
        ),
    ]
