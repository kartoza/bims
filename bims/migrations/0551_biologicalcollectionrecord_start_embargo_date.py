from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('bims', '0550_add_conservation_status_chart_use_taxon_count'),
    ]

    operations = [
        migrations.AddField(
            model_name='biologicalcollectionrecord',
            name='start_embargo_date',
            field=models.DateField(blank=True, help_text='The date when the embargo on the data starts. Together with the end embargo date, the data is only visible to its owner between these dates.', null=True),
        ),
    ]
