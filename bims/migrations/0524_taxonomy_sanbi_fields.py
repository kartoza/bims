import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('bims', '0523_taxonomy_aphia_id_taxonomyupdateproposal_aphia_id_and_more'),
    ]

    operations = [
        # include_in_rli on taxonomy tables
        migrations.AddField(
            model_name='taxonomy',
            name='include_in_rli',
            field=models.BooleanField(default=False, verbose_name='Include in RLI'),
        ),
        migrations.AddField(
            model_name='taxonomyupdateproposal',
            name='include_in_rli',
            field=models.BooleanField(default=False, verbose_name='Include in RLI'),
        ),
        # Named national conservation assessments (SANBI 2016, SANBI 2026, future types)
        migrations.CreateModel(
            name='TaxonNationalConservationAssessment',
            fields=[
                ('id', models.AutoField(
                    auto_created=True, primary_key=True,
                    serialize=False, verbose_name='ID',
                )),
                ('assessment_label', models.CharField(
                    help_text='E.g. "2026 SANBI Red List", "2016 SANBI backcast"',
                    max_length=200,
                    verbose_name='Assessment Label',
                )),
                ('iucn_status', models.ForeignKey(
                    blank=True,
                    null=True,
                    on_delete=django.db.models.deletion.SET_NULL,
                    to='bims.iucnstatus',
                    verbose_name='Conservation Status',
                )),
                ('taxonomy', models.ForeignKey(
                    on_delete=django.db.models.deletion.CASCADE,
                    related_name='national_conservation_assessments',
                    to='bims.taxonomy',
                )),
            ],
            options={
                'verbose_name': 'Taxon National Conservation Assessment',
                'verbose_name_plural': 'Taxon National Conservation Assessments',
            },
        ),
        migrations.AlterUniqueTogether(
            name='taxonnationalconservationassessment',
            unique_together={('taxonomy', 'assessment_label')},
        ),
    ]
