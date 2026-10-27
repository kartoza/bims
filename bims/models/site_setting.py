from django.core.validators import RegexValidator
from django.db import models, connection
from django.db.models import JSONField
from preferences.models import Preferences
from django_cryptography.fields import encrypt
from django.db.models.signals import post_save
from django.dispatch import receiver


def gbif_default_exclusion_rules():
    return [
        {
            "field": "informationWithheld",
            "condition": "not_empty",
            "description": (
                "Skip records where the data provider has withheld or obscured "
                "coordinate information (e.g. iNaturalist observer-requested obscuring)."
            ),
        }
    ]


class SiteSetting(Preferences):
    SITE_CODE_GENERATOR_CHOICES = (
        ('bims', 'BIMS (2 Site Name + 2 Site Description + Site count)'),
        ('fbis', 'FBIS (2 Secondary catchment + 4 River + Site count)'),
        ('fbis_africa', 'FBIS Africa (3 × ISO Country Code + base-36 Site count)'),
        (
            'rbis',
            'RBIS (Catchment + Province ID + District ID + Site count)'
        ),
        (
            'sanparks',
            'SANPARKS (1st three park name + site count)'
        ),
        (
            'kafue',
            'KAFUE (1st five district name + site count)'
        ),
        (
            'fips',
            'FIPS (GBIF continent code + basin name + site count)'
        ),
    )
    site_notice = models.TextField(
        null=True,
        blank=True
    )

    map_default_filters = JSONField(
        default=dict,
        null=True,
        blank=True,
        help_text='Which filters are selected by default, '
                  'the format must be as follows : '
                  '[{"filter_key": "sourceCollection", '
                  '"filter_values": ["bims"]}]'
    )

    auto_validate_taxa_on_upload = models.BooleanField(
        default=True,
        help_text='If True, taxa from CSV uploads are automatically validated.'
    )

    default_location_site_cluster = models.CharField(
        max_length=100,
        help_text='SQL view name of the location site cluster which '
                  'used on the map',
        default='default_location_site_cluster'
    )

    default_data_source = models.CharField(
        max_length=100,
        help_text='Default data source when adding new collection',
        null=True,
        blank=True
    )

    default_site_name = models.CharField(
        max_length=150,
        help_text='Default site name',
        null=True,
        blank=True
    )

    readme_download = models.FileField(
        null=True,
        blank=True,
        help_text='README that bundled with the downloaded occurrence data'
    )

    taxonomic_upload_template = models.FileField(
        null=True,
        blank=True,
        help_text=(
            'File template for taxonomic uploader'
        )
    )

    occurrence_upload_template = models.FileField(
        null=True,
        blank=True,
        help_text=(
            'File template for occurrence uploader'
        )
    )

    water_temperature_upload_template = models.FileField(
        null=True,
        blank=True,
        help_text=(
            'File template for water temperature uploader'
        )
    )

    physico_chemical_upload_template = models.FileField(
        null=True,
        blank=True,
        help_text=(
            'File template for physico-chemical data uploader'
        )
    )

    metadata_upload_template = models.FileField(
        null=True,
        blank=True,
        help_text=(
            'File template for dataset metadata, offered to data providers '
            'in the upload portal'
        )
    )

    landing_page_partners_title = models.CharField(
        default='PARTNERS',
        help_text='Header title for Partners section in landing page',
        max_length=150
    )

    spatial_filter_layer_style = models.CharField(
        max_length=100,
        help_text='Style name for spatial filter layer',
        default='red_outline',
        blank=True
    )

    github_feedback_repo = models.CharField(
        max_length=100,
        help_text='Github repo for users`s feedback',
        default='',
        blank=True
    )

    github_feedback_token = models.CharField(
        max_length=100,
        help_text='Access token for Github feedback repo',
        default='',
        blank=True
    )

    github_private_key = encrypt(
        models.TextField(
            help_text=(
                'Private key (PEM) for your GitHub App. Paste the FULL key including '
                'the "-----BEGIN PRIVATE KEY-----" / "-----END PRIVATE KEY-----" lines. '
                'If stored on one line, you may use \\n for newlines.'
            ),
            default='',
            blank=True
        )
    )

    github_upload_assignees = models.CharField(
        max_length=255,
        help_text=(
            'Comma-separated GitHub usernames to assign to newly created issues. '
            'Assignees must have access to the repository.'
        ),
        default='',
        blank=True
    )

    github_app_id = models.CharField(
        max_length=50,
        help_text='GitHub App ID (integration ID) as shown on the GitHub App settings page.',
        default='',
        blank=True
    )

    github_upload_repo = models.CharField(
        max_length=200,
        help_text=(
            'GitHub repository where upload issues are created, '
            'in "owner/repo" format (e.g. "my-org/upload-tracker"). '
            'The GitHub App must be installed on this repository.'
        ),
        default='',
        blank=True
    )

    occurrence_upload_guidelines = models.FileField(
        null=True,
        blank=True,
        help_text='Guidelines document for uploading occurrence data (e.g. PDF/DOCX).'
    )

    recaptcha_site_key = models.CharField(
        default='',
        max_length=150,
        blank=True
    )

    recaptcha_secret_key = models.CharField(
        default='',
        max_length=150,
        blank=True
    )

    minisass_token = models.CharField(
        default='',
        blank=True
    )

    iucn_api_key = models.CharField(
        max_length=255,
        default='',
        help_text=(
            'Token key for IUCN api'
        ),
        blank=True
    )

    disclaimer_form_text = models.CharField(
        max_length=300,
        default='I agree to these data being shared via the FBIS '
                'platform for visualisation and download by '
                'registered FBIS users',
        blank=True
    )

    disclaimer_doc_text = models.CharField(
        max_length=300,
        default='I hereby confirm that I am the owner of these '
                'data and/or document and agree to these being shared '
                'via the FBIS platform for download by registered FBIS users.',
        blank=True
    )

    default_basemap = models.CharField(
        max_length=100,
        default='Terrain',
        help_text='The default basemap layer that is displayed on the map'
    )

    default_center_map = models.CharField(
        max_length=100,
        default='22.948492328125,-31.12543669218031',
    )

    default_extent_map = models.CharField(
        max_length=100,
        default='5.207535937500003,-37.72038269917067,47.3950359375,'
                '-18.54426493227018'
    )

    blog_page_link = models.CharField(
        blank=True,
        max_length=100,
        help_text='Link to blog page'
    )

    docs_page_link = models.CharField(
        blank=True,
        max_length=100,
        help_text='Link to docs page'
    )

    site_code_generator = models.CharField(
        max_length=50,
        choices=SITE_CODE_GENERATOR_CHOICES,
        blank=True,
        default='bims',
        help_text='How site code generated'
    )

    base_country_code = models.CharField(
        max_length=100,
        blank=True,
        default='',
        help_text='Base country code for the site, '
                  'using ISO 3166-1 (See here for the list : '
                  'https://wiki.openstreetmap.org/wiki/Nominatim/'
                  'Country_Codes)'
    )

    show_third_party_layer = models.BooleanField(
        default=False,
        help_text='Show third party layer selector in Map screen'
    )

    enable_sass = models.BooleanField(
        default=False,
        help_text='Enable or disable SASS'
    )

    enable_water_temperature = models.BooleanField(
        default=False,
        help_text='Enable or disable Water Temperature data'
    )

    enable_climate_data = models.BooleanField(
        default=False,
        help_text='Enable or disable Climate data'
    )

    enable_harvest_worms = models.BooleanField(
        default=False,
        help_text='Enable or disable Harvest from WoRMS'
    )

    enable_harvest_taxonworks = models.BooleanField(
        default=False,
        help_text='Enable or disable Harvest from TaxonWorks'
    )

    enable_checklist_versioning = models.BooleanField(
        default=True,
        help_text='Enable or disable the Taxonomic Checklist Versioning feature'
    )

    enable_download_request_approval = models.BooleanField(
        default=False,
        help_text=(
            'Download requests must be approved by the staff before they '
            'are sent to users'
        )
    )
    max_download_records = models.IntegerField(
        default=50000,
        help_text=(
            'Maximum number of occurrence records that can be downloaded '
            'without staff approval. Set to 0 for no limit. Requests '
            'exceeding this number always require explicit staff approval '
            'even if automatic approval is enabled.'
        )
    )
    download_request_expiry_months = models.PositiveSmallIntegerField(
        default=2,
        null=True,
        blank=True,
        help_text=(
            'Number of months to keep approved download files before they '
            'are automatically deleted. Leave blank or set to 0 for files '
            'to never expire.'
        )
    )

    show_module_summary_on_dashboard = models.BooleanField(
        default=False,
        help_text=(
            'Show summative figure for data by taxon group'
        )
    )
    show_general_summary_on_landing = models.BooleanField(
        default=False,
        help_text='Display the general statistics block on the landing page.'
    )
    show_metagroup_on_landing = models.BooleanField(
        default=False,
        help_text=(
            'Display the metagroup (broader organism group) widget on the '
            'landing page.'
        ),
    )

    conservation_status_chart_use_taxon_count = models.BooleanField(
        default=False,
        help_text=(
            'Base the conservation status chart on the front page on the '
            'number of taxa instead of the number of occurrence records.'
        ),
    )

    enable_remove_all_occurrences_tool = models.BooleanField(
        default=False,
        help_text=(
            'Enable tool to remove all occurrences for a taxon group'
        )
    )

    allow_taxa_edit_in_admin = models.BooleanField(
        default=False,
        help_text=(
            'Enable this to allow superusers to edit taxa directly from the '
            'admin popup in the taxa management page'
        )
    )

    allow_public_taxa_view = models.BooleanField(
        default=False,
        help_text=(
            'Allow non-logged-in users to view the taxa management page. '
            'Public users will only see validated taxa. '
            'Downloading still requires login.'
        )
    )

    restrict_taxon_to_single_group = models.BooleanField(
        default=True,
        help_text=(
            'When enabled, a taxon that already belongs to a taxon group '
            'cannot be added to a different group.'
        )
    )

    github_repo_path = models.CharField(
        default='',
        blank=True,
        max_length=255,
        help_text=(
            'Path to git repository from where the version info can be '
            'retrieved'
        )
    )

    boundary_key = models.CharField(
        max_length=100,
        blank=True,
        default='',
        help_text='The key of the boundary data form GeoContext'
    )

    site_boundary = models.ForeignKey(
        'bims.Boundary',
        blank=True,
        on_delete=models.SET_NULL,
        null=True,
        help_text=(
            'Boundary used for checking if the site is '
            'within the correct boundary'
        )
    )

    copyright_text = models.CharField(
        max_length=200,
        blank=True,
        default='Copyright © Kartoza'
    )

    pesticide_quaternary_data_file = models.FileField(
        null=True,
        blank=True,
        help_text=(
            'File containing pesticide data per quaternary. '
            'If not provided, the pesticide dashboard will be unavailable.'
        )
    )

    enable_ecosystem_type = models.BooleanField(
        default=False
    )

    enable_ogc_api = models.BooleanField(
        default=False,
        help_text='Let logged-in users publish their filters as live layers '
                  '(OGC API - Features) for ArcGIS or QGIS'
    )

    resend_api_key = models.CharField(
        max_length=128,
        default='',
        blank=True,
    )

    default_from_email = models.EmailField(
        default='',
        blank=True
    )

    cites_token_api = models.CharField(
        max_length=200,
        blank=True,
        default=''
    )

    virtual_museum_token = models.CharField(
        max_length=200,
        blank=True,
        default=''
    )

    homepage_redirect_url = models.CharField(
        max_length=128,
        default='',
        blank=True
    )

    geoserver_location_site_layer = models.CharField(
        max_length=128,
        default='bims:location_site_view',
        blank=True
    )

    park_wfs_url = models.CharField(
        max_length=200,
        default='https://maps.kartoza.com/geoserver/wfs',
        blank=True
    )

    park_wfs_layer_name = models.CharField(
        max_length=200,
        default='',
        blank=True
    )

    park_layer_csv = models.FileField(
        null=True,
        blank=True,
        upload_to='park_layer_csv/'
    )

    section_layer_csv = models.FileField(
        null=True,
        blank=True,
        upload_to='section_layer_csv/'
    )

    park_wfs_attribute_key = models.CharField(
        max_length=200,
        default='',
        blank=True,
        help_text='Park attribute key'
    )

    park_layer = models.ForeignKey(
        'cloud_native_gis.Layer',
        blank=True,
        on_delete=models.SET_NULL,
        null=True,
    )

    map_min_zoom = models.IntegerField(
        default=5,
        help_text="Minimum zoom level allowed for the map. Lower values show a wider area."
    )

    gbif_username = models.CharField(
        max_length=150,
        blank=True,
        help_text="GBIF username used for API downloads.",
        default='',
        validators=[RegexValidator(r'^[\w.@+-]+$', "Enter a valid username.")],
    )

    gbif_password = encrypt(models.CharField(
        max_length=256,
        blank=True,
        help_text="GBIF password used for API downloads (encrypted at rest).",
        default='',
    ))

    gbif_excluded_project_ids = models.TextField(
        blank=True,
        default="",
        help_text=(
            "Comma-separated GBIF projectId values to exclude for this tenant "
            "(case-insensitive). Example: 'fbis, legacy123'"
        ),
    )

    gbif_harvest_exclusion_rules = JSONField(
        blank=True,
        null=True,
        default=gbif_default_exclusion_rules,
        help_text=(
            "JSON list of rules applied during GBIF harvesting. Each rule is an object "
            "with 'field' (DwC field name), 'condition' (not_empty | equals | contains | "
            "greater_than | less_than), optional 'value', and optional 'description'. "
            "A record matching ANY rule is skipped. "
            "Example: [{\"field\": \"informationWithheld\", \"condition\": \"not_empty\", "
            "\"description\": \"Skip obscured/withheld coordinates\"}]"
        ),
    )

    google_analytics_key = models.CharField(
        max_length=255,
        blank=True,
        default='',
        help_text="Google Analytics Measurement ID or API key."
    )

    invasion_label = models.CharField(
        max_length=100,
        default='Invasion',
        blank=True,
        help_text=(
            "Label for the invasion column in taxa checklist downloads. "
            "Default is 'Invasion'. SANParks uses 'National NEMBA Status'."
        )
    )

    # ------------------------------------------------------------------
    # Default exclusion rules applied when the field is empty/null.
    # Stored here so callers always get a safe baseline.
    # ------------------------------------------------------------------
    GBIF_DEFAULT_EXCLUSION_RULES = gbif_default_exclusion_rules()

    @property
    def gbif_exclusion_rules_effective(self) -> list:
        """Return configured rules, falling back to the default baseline."""
        rules = self.gbif_harvest_exclusion_rules
        if rules is None or not isinstance(rules, list):
            return self.GBIF_DEFAULT_EXCLUSION_RULES
        return rules

    def _tenant_default_exclusions(self) -> set[str]:
        """Default per tenant: for FBIS tenant, exclude 'fbis'."""
        schema = (getattr(connection, "schema_name", "") or "").lower()
        return {"fbis"} if schema == "fbis" else set()

    def _csv_to_set(self) -> set[str]:
        raw = (self.gbif_excluded_project_ids or "")
        return {p.strip().lower() for p in raw.split(",") if p.strip()}

    @property
    def gbif_excluded_project_ids_effective(self) -> list[str]:
        """CSV + tenant defaults -> normalized, deduped, sorted list."""
        vals = self._csv_to_set() | self._tenant_default_exclusions()
        return sorted(vals)

    @property
    def project_name(self):
        if self.default_data_source:
            return self.default_data_source.lower()
        if self.site_code_generator:
            return self.site_code_generator.lower()
        return 'bims'


@receiver(post_save, sender=SiteSetting)
def site_setting_saved(sender, **kwargs):
    from bims.tasks.module_summary import queue_module_summary_refresh
    queue_module_summary_refresh()
