import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('bims', '0515_sourcereferenceauthor_and_more'),
    ]

    operations = [
        # Make gbif_config nullable (existing rows keep their value)
        migrations.AlterField(
            model_name='gbifpublishcontact',
            name='gbif_config',
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.CASCADE,
                related_name='contacts',
                help_text='The GBIF config these contacts belong to (leave blank for schedule-level contacts).',
                to='bims.gbifpublishconfig',
            ),
        ),
        # Add the optional schedule FK
        migrations.AddField(
            model_name='gbifpublishcontact',
            name='gbif_publish',
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.CASCADE,
                related_name='contacts',
                help_text='The GBIF publish schedule these contacts belong to (leave blank for config-level contacts).',
                to='bims.gbifpublish',
            ),
        ),
    ]
