# coding=utf-8
from django.contrib.gis import forms
from crispy_forms.helper import FormHelper
from crispy_forms.layout import (
    Layout,
    Fieldset,
    Field,
)
from bims.models import BiologicalCollectionRecord


class BioRecordsForm(forms.ModelForm):
    class Meta:
        model = BiologicalCollectionRecord
        fields = (
            'original_species_name',
            'present',
            'collection_date',
            'abundance_number',
            'abundance_type',
            'biotope',
            'specific_biotope',
            'substratum',
            'sampling_effort',
            'sampling_method',
            'notes',
            'taxonomy',
            'ready_for_validation',
        )

    def __init__(self, *args, **kwargs):
        form_title = ''
        self.helper = FormHelper()
        layout = Layout(
            Fieldset(
                form_title,
                Field('original_species_name', css_class='form-control'),
                Field('ready_for_validation', css_class=''),
                Field('present', css_class=''),
                Field('collection_date', css_class='form-control'),
                Field('abundance_number', css_class='form-control'),
                Field('abundance_type', css_class='form-control'),
                Field('biotope', css_class='form-control'),
                Field('specific_biotope', css_class='form-control'),
                Field('substratum', css_class='form-control'),
                Field('sampling_effort', css_class='form-control'),
                Field('sampling_method', css_class='form-control'),
                Field('notes', css_class='form-control'),
                Field('taxonomy', css_class='form-control'),
            )
        )
        self.helper.layout = layout
        self.helper.html5_required = False
        self.helper.form_tag = False
        super(BioRecordsForm, self).__init__(*args, **kwargs)

    def clean_abundance_number(self):
        value = self.cleaned_data.get('abundance_number')
        if value is not None and value <= 0:
            raise forms.ValidationError(
                'Abundance must be greater than zero.'
            )
        return value
