# coding=utf-8
from django import forms
from django.core.exceptions import ValidationError
from django.core.validators import FileExtensionValidator
from bims.models.upload_request import UploadType
from bims.models.licence import Licence

ALLOWED_EXTENSIONS = [
    'csv', 'xlsx', 'xls', 'zip',
    'gpkg', 'geojson',
]

FILE_UPLOAD_MAX_SIZE_MB = 50


class UploadForm(forms.Form):
    title = forms.CharField(max_length=255)
    name = forms.CharField(max_length=200)
    email = forms.EmailField()
    upload_type = forms.ModelChoiceField(
        queryset=UploadType.objects.none(),
        empty_label=None,
        to_field_name='code'
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['upload_type'].queryset = UploadType.objects.all().order_by('order')
    upload_file = forms.FileField(
        validators=[FileExtensionValidator(
            allowed_extensions=ALLOWED_EXTENSIONS)
        ]
    )
    licence = forms.ModelChoiceField(
        queryset=Licence.objects.all(),
        empty_label=None,
    )
    notes = forms.CharField(widget=forms.Textarea, required=False)
    gbif_consent = forms.BooleanField(required=False)
    decoy = forms.CharField(required=False)
    recaptcha_token = forms.CharField(required=False)

    source = forms.CharField(required=False)

    def clean_decoy(self):
        v = self.cleaned_data.get('decoy', '')
        if v:
            raise ValidationError("Invalid submission.")
        return v

    def clean_upload_file(self):
        f = self.cleaned_data['upload_file']
        max_mb = FILE_UPLOAD_MAX_SIZE_MB
        if f.size > max_mb * 1024 * 1024:
            raise ValidationError(f"File too large (>{max_mb}MB).")
        return f
