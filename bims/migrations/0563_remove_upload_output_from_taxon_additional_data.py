from django.db import migrations

# Columns of the taxa uploader's error and success files, saved in
# additional_data when such a file was uploaded again.
UPLOAD_OUTPUT_KEYS = ('error_message', 'link')
BATCH_SIZE = 500


def remove_upload_output(apps, schema_editor):
    for model_name in ('Taxonomy', 'TaxonomyUpdateProposal'):
        model = apps.get_model('bims', model_name)
        for key in UPLOAD_OUTPUT_KEYS:
            changed = []
            for obj in model.objects.filter(
                additional_data__has_key=key
            ).only('id', 'additional_data').iterator():
                obj.additional_data.pop(key, None)
                changed.append(obj)
                if len(changed) >= BATCH_SIZE:
                    model.objects.bulk_update(changed, ['additional_data'])
                    changed = []
            if changed:
                model.objects.bulk_update(changed, ['additional_data'])


class Migration(migrations.Migration):

    dependencies = [
        ('bims', '0562_ogc_layer_daily_usage'),
    ]

    operations = [
        migrations.RunPython(remove_upload_output, migrations.RunPython.noop),
    ]
