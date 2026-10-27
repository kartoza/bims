import json

from django import forms
from django.http import HttpResponseRedirect
from django.urls import reverse
from django.utils.translation import gettext_lazy as _
from preferences.admin import PreferencesAdmin

from bims.models.site_setting import SiteSetting

SECRET_INPUTS = [
    'recaptcha_secret_key',
    'iucn_api_key',
    'cites_token_api',
    'virtual_museum_token',
    'resend_api_key',
    'github_feedback_token',
    'gbif_password',
    'github_private_key',
    'google_analytics_key',
]


class PrettyJSONWidget(forms.Textarea):
    def format_value(self, value):
        if value in (None, ""):
            return ""
        try:
            if isinstance(value, str):
                value = json.loads(value)
            return json.dumps(value, indent=2, ensure_ascii=False)
        except (TypeError, ValueError):
            return value


class SiteSettingAdminForm(forms.ModelForm):
    class Meta:
        model = SiteSetting
        fields = '__all__'

    gbif_harvest_exclusion_rules = forms.JSONField(
        required=False,
        widget=PrettyJSONWidget(
            attrs={
                'rows': 12,
                'cols': 100,
                'style': 'font-family:monospace;',
            }
        ),
        help_text=SiteSetting._meta.get_field(
            'gbif_harvest_exclusion_rules'
        ).help_text,
        label=_('GBIF harvest exclusion rules'),
    )

    recaptcha_secret_key = forms.CharField(
        widget=forms.PasswordInput(render_value=True),
        required=False
    )
    iucn_api_key = forms.CharField(
        widget=forms.PasswordInput(render_value=True),
        required=False
    )
    cites_token_api = forms.CharField(
        widget=forms.PasswordInput(render_value=True),
        required=False
    )
    virtual_museum_token = forms.CharField(
        widget=forms.PasswordInput(render_value=True),
        required=False
    )
    resend_api_key = forms.CharField(
        widget=forms.PasswordInput(render_value=True),
        required=False
    )
    github_feedback_token = forms.CharField(
        widget=forms.PasswordInput(render_value=True),
        required=False
    )
    minisass_token = forms.CharField(
        widget=forms.PasswordInput(render_value=True),
        required=False
    )

    github_private_key = forms.CharField(
        required=False,
        widget=forms.Textarea(
            attrs={
                'rows': 10,
                'placeholder': '-----BEGIN PRIVATE KEY-----\\n...\\n-----END PRIVATE KEY-----',
                'style': 'font-family:monospace;',
            }
        ),
        help_text=SiteSetting._meta.get_field('github_private_key').help_text,
        label=_('GitHub App private key (PEM)'),
    )

    google_analytics_key = forms.CharField(
        widget=forms.PasswordInput(render_value=True),
        required=False,
        label=_('Google Analytics Key'),
        help_text=_('Used for tracking site visits and analytics.'),
    )

    def __init__(self, *args, **kwargs):
        super(SiteSettingAdminForm, self).__init__(*args, **kwargs)
        if self.instance and self.instance.pk:
            for secret_input in SECRET_INPUTS:
                if secret_input in self.fields:
                    # Never show actual secret; keep UX consistent with placeholders.
                    self.fields[secret_input].widget.attrs['placeholder'] = '******'
                    self.fields[secret_input].required = False
                    # Specifically blank out PEM initial so it is not rendered at all.
                    if secret_input == 'github_private_key':
                        self.fields[secret_input].initial = ''


class SiteSettingAdmin(PreferencesAdmin):
    form = SiteSettingAdminForm

    readonly_fields = ("gbif_excluded_project_ids_effective_display",)

    def changelist_view(self, request, extra_context=None):
        obj = SiteSetting.objects.first()
        if obj:
            url = reverse(
                'admin:%s_%s_change' % (obj._meta.app_label, obj._meta.model_name),
                args=[obj.pk],
            )
            return HttpResponseRedirect(url)
        return super().changelist_view(request, extra_context=extra_context)

    fieldsets = (
        (_("General"), {
            "fields": (
                "site_notice",
                "disclaimer_form_text",
                "disclaimer_doc_text",
                "default_data_source",
                "default_site_name",
                "site_code_generator",
                "github_repo_path",
                "copyright_text",
            ),
            "classes": ("wide",),
        }),
        (_("Landing & Docs"), {
            "fields": (
                "landing_page_partners_title",
                "blog_page_link",
                "docs_page_link",
                "homepage_redirect_url",
            ),
            "classes": ("collapse",),
        }),
        (_("Map"), {
            "fields": (
                "default_basemap",
                "default_center_map",
                "default_extent_map",
                "map_min_zoom",
                "spatial_filter_layer_style",
                "map_default_filters",
                "default_location_site_cluster",
                "geoserver_location_site_layer",
                "show_third_party_layer",
            ),
        }),
        (_("Boundary & Park Layers"), {
            "fields": (
                "base_country_code",
                "boundary_key",
                "site_boundary",
                "park_wfs_url",
                "park_wfs_layer_name",
                "park_wfs_attribute_key",
                "park_layer",
                "park_layer_csv",
                "section_layer_csv",
            ),
        }),
        (_("Uploads & Templates"), {
            "fields": (
                "readme_download",
                "taxonomic_upload_template",
                "occurrence_upload_guidelines",
                "occurrence_upload_template",
                "water_temperature_upload_template",
                "physico_chemical_upload_template",
                "metadata_upload_template",
                "auto_validate_taxa_on_upload",
                "invasion_label",
            ),
        }),
        (_("Features / Toggles"), {
            "fields": (
                "enable_sass",
                "enable_water_temperature",
                "enable_climate_data",
                "enable_harvest_worms",
                "enable_harvest_taxonworks",
                "enable_checklist_versioning",
                "enable_ecosystem_type",
                "enable_ogc_api",
                "enable_download_request_approval",
                "max_download_records",
                "download_request_expiry_months",
                "show_module_summary_on_dashboard",
                "show_general_summary_on_landing",
                "show_metagroup_on_landing",
                "conservation_status_chart_use_taxon_count",
                "enable_remove_all_occurrences_tool",
                "allow_taxa_edit_in_admin",
                "allow_public_taxa_view",
            ),
            "classes": ("collapse",),
        }),
        (_("GitHub App"), {  # NEW dedicated section
            "fields": (
                "github_app_id",
                "github_private_key",
                "github_upload_repo",
                "github_upload_assignees",
            ),
        }),
        (_("External Integrations & Tokens"), {
            "fields": (
                "github_feedback_repo",
                "github_feedback_token",
                "recaptcha_site_key",
                "recaptcha_secret_key",
                "minisass_token",
                "iucn_api_key",
                "cites_token_api",
                "virtual_museum_token",
                "resend_api_key",
                "google_analytics_key",
                "default_from_email",
            ),
            "classes": ("collapse",),
        }),
        (_("Pesticide Dashboard"), {
            "fields": ("pesticide_quaternary_data_file",),
            "classes": ("collapse",),
        }),
        (_("GBIF"), {
            "fields": (
                "gbif_username",
                "gbif_password",
                "gbif_harvest_exclusion_rules",
                "gbif_excluded_project_ids",
                "gbif_excluded_project_ids_effective_display"
            ),
        }),
    )

    def gbif_excluded_project_ids_effective_display(self, obj):
        """
        Read-only view of the effective exclusions:
        CSV from admin + tenant defaults (e.g. 'fbis' on FBIS tenant).
        """
        if not obj:
            return ""
        vals = getattr(obj, "gbif_excluded_project_ids_effective", []) or []
        return ", ".join(vals) if vals else "(none)"

    gbif_excluded_project_ids_effective_display.short_description = _(
        "Excluded projectIds (effective)"
    )

    def save_model(self, request, obj, form, change):
        if change:
            for secret_input in SECRET_INPUTS:
                if secret_input in form.fields:
                    if secret_input in form.changed_data:
                        setattr(obj, secret_input, form.cleaned_data.get(secret_input))
                    else:
                        setattr(obj, secret_input, getattr(obj, secret_input))
        obj.save()

    class Media:
        js = ("admin/js/sitesetting_sections.js",)
