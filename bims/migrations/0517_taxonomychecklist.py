import datetime
from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


def load_default_taxonomy_checklist(apps, schema_editor):
    """Create an initial published TaxonomyChecklist row."""
    TaxonomyChecklist = apps.get_model('bims', 'TaxonomyChecklist')
    if not TaxonomyChecklist.objects.exists():
        TaxonomyChecklist.objects.create(
            title='BIMS Taxonomy Checklist',
            version='1.0',
            description=(
                'Freshwater Biodiversity Information System (BIMS) taxonomy '
                'checklist exported in Catalogue of Life Data Package (ColDP) format.'
            ),
            license='https://creativecommons.org/licenses/by/4.0/',
            citation='',
            doi='',
            released_at=datetime.date.today(),
            is_published=True,
        )


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('bims', '0516_gbifpublishcontact_gbif_publish'),
    ]

    operations = [
        migrations.CreateModel(
            name='TaxonomyChecklist',
            fields=[
                ('id', models.AutoField(
                    auto_created=True,
                    primary_key=True,
                    serialize=False,
                    verbose_name='ID',
                )),
                ('title', models.CharField(
                    default='BIMS Taxonomy Checklist',
                    help_text='Human-readable dataset title used in ColDP metadata.yaml.',
                    max_length=255,
                )),
                ('version', models.CharField(
                    help_text='Version string for this checklist release (e.g. "1.0", "2025-04").',
                    max_length=50,
                )),
                ('description', models.TextField(
                    blank=True,
                    default='',
                    help_text='Free-text description of the dataset.',
                )),
                ('license', models.URLField(
                    default='https://creativecommons.org/licenses/by/4.0/',
                    help_text=(
                        'License URL or SPDX identifier for the dataset '
                        '(e.g. https://creativecommons.org/licenses/by/4.0/).'
                    ),
                    max_length=255,
                )),
                ('citation', models.TextField(
                    blank=True,
                    default='',
                    help_text=(
                        'Suggested citation string. '
                        'Leave blank to auto-generate from organisation, title and domain.'
                    ),
                )),
                ('doi', models.CharField(
                    blank=True,
                    default='',
                    help_text=(
                        'Persistent identifier (DOI or URL) for this checklist version, '
                        'e.g. https://doi.org/10.XXXX/YYYY.'
                    ),
                    max_length=255,
                )),
                ('released_at', models.DateField(
                    blank=True,
                    null=True,
                    help_text='Official release date of this checklist version (YYYY-MM-DD).',
                )),
                ('is_published', models.BooleanField(
                    default=False,
                    help_text=(
                        'Mark this checklist as published. '
                        'The metadata endpoint serves the latest published checklist.'
                    ),
                )),
                ('contact', models.ForeignKey(
                    blank=True,
                    null=True,
                    on_delete=django.db.models.deletion.SET_NULL,
                    related_name='checklist_contact',
                    to=settings.AUTH_USER_MODEL,
                    help_text='Primary contact person for this checklist.',
                )),
                ('creators', models.ManyToManyField(
                    blank=True,
                    related_name='checklist_creator',
                    to=settings.AUTH_USER_MODEL,
                    help_text='Dataset creators / authors.',
                )),
            ],
            options={
                'verbose_name': 'Taxonomy Checklist',
                'verbose_name_plural': 'Taxonomy Checklists',
                'ordering': ['-released_at', '-id'],
            },
        ),
        migrations.RunPython(
            load_default_taxonomy_checklist,
            migrations.RunPython.noop,
        ),
    ]
