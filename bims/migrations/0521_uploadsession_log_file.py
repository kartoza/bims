from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('bims', '0520_checklistversion_deletions_count_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='uploadsession',
            name='log_file',
            field=models.FileField(
                blank=True,
                null=True,
                upload_to='upload-session-log/',
                help_text='Log file capturing per-row processing output for this upload session',
            ),
        ),
    ]
