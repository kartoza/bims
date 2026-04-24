import json
from django.db import models, connection
from django.conf import settings
from django.db.models.signals import post_save, post_delete
from django.dispatch import receiver
from django_celery_beat.models import PeriodicTask, CrontabSchedule
from django_cryptography.fields import encrypt
from django_tenants.utils import schema_context, get_public_schema_name


class PublishPeriod(models.TextChoices):
    DAILY = "DAILY", "Daily"
    WEEKLY = "WEEKLY", "Weekly"
    MONTHLY = "MONTHLY", "Monthly"
    CUSTOM = "CUSTOM", "Custom (cron)"


class GbifPublishConfig(models.Model):
    """Configuration for GBIF publishing credentials and settings."""
    name = models.CharField(
        max_length=255,
        help_text="A descriptive name for this configuration (e.g., 'Production', 'Test')"
    )
    gbif_api_url = models.URLField(
        default="https://api.gbif-uat.org/v1",
        help_text="GBIF API URL: https://api.gbif.org/v1 (production), "
                  "https://api.gbif-uat.org/v1 (UAT), "
                  "https://api.gbif-test.org/v1 (test). "
                  "Ensure credentials match the environment."
    )
    license_url = models.URLField(
        default="https://creativecommons.org/publicdomain/zero/1.0/legalcode",
        help_text="License URL for the dataset"
    )
    export_base_url = models.URLField(
        blank=True,
        help_text="Base URL where DwC-A exports will be accessible (e.g., https://yoursite.com)"
    )
    username = models.CharField(
        max_length=255,
        help_text="GBIF username for authentication"
    )
    password = encrypt(models.CharField(
        max_length=255,
        help_text="GBIF password for authentication (encrypted at rest)."
    ))
    publishing_org_key = models.CharField(
        max_length=64,
        help_text="GBIF Publishing Organization Key (UUID)"
    )
    installation_key = models.CharField(
        max_length=64,
        help_text="GBIF Installation Key (UUID)",
        blank=True,
        default="",
    )
    is_active = models.BooleanField(
        default=True,
        help_text="Whether this configuration is active"
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "GBIF Publish Config"
        verbose_name_plural = "GBIF Publish Configs"

    def __str__(self):
        return f"{self.name} ({self.gbif_api_url})"


class GbifPublish(models.Model):
    source_reference = models.ForeignKey(
        'bims.SourceReference',
        on_delete=models.CASCADE,
        related_name="gbif_publish_schedules",
        null=True,
        blank=True,
    )
    gbif_config = models.ForeignKey(
        GbifPublishConfig,
        on_delete=models.PROTECT,
        related_name="publish_schedules",
        help_text="GBIF configuration to use for publishing"
    )
    enabled = models.BooleanField(default=False)

    period = models.CharField(
        max_length=10,
        choices=PublishPeriod.choices,
        default=PublishPeriod.DAILY)
    run_at = models.TimeField(
        null=True, blank=True)
    day_of_week = models.CharField(
        max_length=9, blank=True)
    day_of_month = models.PositiveSmallIntegerField(
        null=True, blank=True)
    cron_expression = models.CharField(
        max_length=100, blank=True, default="")
    timezone = models.CharField(
        max_length=64, default="UTC")

    last_publish = models.DateTimeField(null=True, blank=True)

    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "GBIF Publish Schedule"
        verbose_name_plural = "GBIF Publish Schedules"

    def __str__(self):
        ref = str(self.source_reference) if self.source_reference else "all"
        return f"GBIF Publish[{ref}] - {self.period}"


class PublishTrigger(models.TextChoices):
    MANUAL = "manual", "Manual"
    SCHEDULED = "scheduled", "Scheduled"


class PublishStatus(models.TextChoices):
    PENDING = "pending", "Pending"
    RUNNING = "running", "Running"
    SUCCESS = "success", "Success"
    ERROR = "error", "Error"
    NO_RECORDS = "no_records", "No Records"


class GbifPublishSession(models.Model):
    """Stores results of each GBIF publish run."""
    schedule = models.ForeignKey(
        GbifPublish,
        on_delete=models.CASCADE,
        related_name="sessions",
        help_text="The publish schedule that triggered this session"
    )
    source_reference = models.ForeignKey(
        'bims.SourceReference',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="gbif_publish_sessions"
    )
    gbif_config = models.ForeignKey(
        GbifPublishConfig,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="sessions"
    )

    status = models.CharField(
        max_length=20,
        choices=PublishStatus.choices,
        default=PublishStatus.PENDING
    )
    trigger = models.CharField(
        max_length=10,
        choices=PublishTrigger.choices,
        default=PublishTrigger.MANUAL
    )

    start_time = models.DateTimeField(auto_now_add=True)
    end_time = models.DateTimeField(null=True, blank=True)

    dataset_key = models.CharField(
        max_length=64,
        blank=True,
        help_text="GBIF dataset UUID returned after successful registration"
    )
    records_published = models.PositiveIntegerField(
        default=0,
        help_text="Number of records published in this session"
    )
    archive_url = models.URLField(
        blank=True,
        help_text="URL to the DwC-A archive file"
    )

    error_message = models.TextField(
        blank=True,
        help_text="Error message if the publish failed"
    )
    log_file = models.FileField(
        upload_to='gbif-publish-session-log/',
        null=True,
        blank=True
    )

    class Meta:
        verbose_name = "GBIF Publish Session"
        verbose_name_plural = "GBIF Publish Sessions"
        ordering = ["-start_time"]

    def __str__(self):
        return f"Publish Session {self.id} - {self.status} ({self.start_time})"

    @property
    def duration(self):
        if self.end_time and self.start_time:
            return self.end_time - self.start_time
        return None


class RoleType(models.TextChoices):
    AUTHOR = "author", "Author"
    CONTENT_PROVIDER = "contentProvider", "Content Provider"
    CUSTODIAN_STEWARD = "custodianSteward", "Custodian Steward"
    DISTRIBUTOR = "distributor", "Distributor"
    EDITOR = "editor", "Editor"
    METADATA_PROVIDER = "metadataProvider", "Metadata Provider"
    ORIGINATOR = "originator", "Originator"
    POINT_OF_CONTACT = "pointOfContact", "Point of Contact"
    PRINCIPAL_INVESTIGATOR = "principalInvestigator", "Principal Investigator"
    PROCESSOR = "processor", "Processor"
    PUBLISHER = "publisher", "Publisher"
    USER = "user", "User"


class GbifPublishContact(models.Model):
    """
    Contact information embedded in the EML metadata.

    A contact belongs to either a GbifPublishConfig (shared across all schedules
    that use that config) or a specific GbifPublish schedule (overrides/additions
    for that schedule only).  Exactly one of gbif_config / gbif_publish must be set.
    """
    gbif_config = models.ForeignKey(
        GbifPublishConfig,
        on_delete=models.CASCADE,
        related_name="contacts",
        null=True,
        blank=True,
        help_text="The GBIF config these contacts belong to (leave blank for schedule-level contacts)."
    )
    gbif_publish = models.ForeignKey(
        GbifPublish,
        on_delete=models.CASCADE,
        related_name="contacts",
        null=True,
        blank=True,
        help_text="The GBIF publish schedule these contacts belong to (leave blank for config-level contacts)."
    )
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        help_text=(
            "Optional: link a user to auto-populate blank fields "
            "(name, email, organisation, position)."
        ),
    )
    role = models.CharField(
        max_length=32,
        choices=RoleType.choices,
        default=RoleType.ORIGINATOR,
        help_text=(
            "How this person or organisation is related to the resource "
            "(EML roleType). E.g. originator, author, pointOfContact, publisher."
        ),
    )
    individual_name_given = models.CharField(
        max_length=255,
        blank=True,
        default="",
        verbose_name="Given name",
        help_text="First name. Leave blank to use the linked user's first_name.",
    )
    individual_name_sur = models.CharField(
        max_length=255,
        blank=True,
        default="",
        verbose_name="Surname",
        help_text="Last name / surname. Leave blank to use the linked user's last_name.",
    )
    organization_name = models.CharField(
        max_length=512,
        blank=True,
        default="",
        verbose_name="Organisation name",
        help_text=(
            "Full name of the organisation. "
            "Leave blank to use the linked user's organisation."
        ),
    )
    position_name = models.CharField(
        max_length=255,
        blank=True,
        default="",
        verbose_name="Position / title",
        help_text=(
            "Title or position associated with this contact. "
            "Leave blank to use the linked user's role (bims_profile.role)."
        ),
    )
    delivery_point = models.CharField(
        max_length=512,
        blank=True,
        default="",
        verbose_name="Delivery point (street address)",
        help_text="Street address or PO Box.",
    )
    city = models.CharField(
        max_length=255,
        blank=True,
        default="",
    )
    postal_code = models.CharField(
        max_length=32,
        blank=True,
        default="",
    )
    country = models.CharField(
        max_length=255,
        blank=True,
        default="",
        help_text="Country name or ISO 3166-1 alpha-2 code.",
    )
    phone = models.CharField(
        max_length=64,
        blank=True,
        default="",
        help_text="Telephone number including country code, e.g. +27 21 123 4567.",
    )
    electronic_mail_address = models.EmailField(
        blank=True,
        default="",
        verbose_name="Email address",
        help_text=(
            "Contact email address. "
            "Leave blank to use the linked user's email."
        ),
    )
    online_url = models.URLField(
        blank=True,
        default="",
        verbose_name="Online URL",
        help_text="Link to associated online information, usually a web site.",
    )

    class Meta:
        verbose_name = "GBIF Publish Contact"
        verbose_name_plural = "GBIF Publish Contacts"
        ordering = ["id"]

    def __str__(self):
        name = self.resolved_given_name or self.resolved_sur_name or ""
        org = self.resolved_organization_name
        label = f"{name} ({org})" if org else name
        return label or f"Contact #{self.pk}"

    def _user_attr(self, attr, default=""):
        if self.user_id:
            return (getattr(self.user, attr, None) or "").strip()
        return default

    def _profile_attr(self):
        """Return bims_profile.role.display_name for the linked user, or ''."""
        try:
            if self.user_id:
                return (self.user.bims_profile.role.display_name or "").strip()
        except Exception:
            pass
        return ""

    @property
    def resolved_given_name(self):
        return self.individual_name_given or self._user_attr("first_name")

    @property
    def resolved_sur_name(self):
        return self.individual_name_sur or self._user_attr("last_name")

    @property
    def resolved_organization_name(self):
        return self.organization_name or self._user_attr("organization")

    @property
    def resolved_position_name(self):
        return self.position_name or self._profile_attr()

    @property
    def resolved_email(self):
        return self.electronic_mail_address or self._user_attr("email")


@receiver(post_save, sender=GbifPublishContact)
def fill_gbif_publish_contact_from_user(sender, instance: 'GbifPublishContact', **kwargs):
    """Fill blank optional fields from the linked user's profile on save."""
    if not instance.user_id:
        return

    user = instance.user
    update_fields = []

    def _fill(field, value):
        if not getattr(instance, field) and value:
            setattr(instance, field, value)
            update_fields.append(field)

    # Look for another contact for the same user within the same owner
    # (config-level or schedule-level) to copy already-resolved fields from.
    if instance.gbif_publish_id:
        sibling_filter = {"gbif_publish_id": instance.gbif_publish_id}
    else:
        sibling_filter = {"gbif_config_id": instance.gbif_config_id}
    sibling = (
        GbifPublishContact.objects
        .filter(user_id=user.id, **sibling_filter)
        .exclude(id=instance.id)
        .first()
    )

    if sibling:
        for field in (
                "individual_name_given", "individual_name_sur",
                "electronic_mail_address", "organization_name",
                "postal_code", "position_name", "delivery_point",
                "city", "phone", "country", "online_url",
        ):
            _fill(field, (getattr(sibling, field, "") or "").strip())
    else:
        _fill("individual_name_given", (getattr(user, "first_name", "") or "").strip())
        _fill("individual_name_sur", (getattr(user, "last_name", "") or "").strip())
        _fill("electronic_mail_address", (getattr(user, "email", "") or "").strip())
        _fill("organization_name", (getattr(user, "organization", "") or "").strip())
        _fill("postal_code", (getattr(user, "zipcode", "") or "").strip())
        _fill("delivery_point", (getattr(user, "delivery", "") or "").strip())
        _fill("city", (getattr(user, "city", "") or "").strip())
        _fill("phone", (getattr(user, "voice", "") or "").strip())
        _fill("country", (getattr(user, "country", "") or "").strip())
        _fill("position_name", (getattr(user, "position", "") or "").strip())
        try:
            role_name = (user.bims_profile.role.display_name or "").strip()
            _fill("position_name", role_name)
        except Exception:
            pass

    if update_fields:
        sender.objects.filter(pk=instance.pk).update(
            **{f: getattr(instance, f) for f in update_fields}
        )


def _mins_hrs(t):
    return (str(t.minute), str(t.hour)) if t else ("0", "2")


@receiver(post_delete, sender=GbifPublish)
def gbif_publish_post_delete(sender, instance: GbifPublish, **kwargs):
    schema = str(connection.schema_name)
    with schema_context(get_public_schema_name()):
        PeriodicTask.objects.filter(args=json.dumps([schema, instance.id])).delete()


@receiver(post_save, sender=GbifPublish)
def sync_gbif_publish_periodic_task(sender, instance: GbifPublish, **kwargs):
    name = f"GBIF publish: source_reference={instance.source_reference_id} id={instance.id}"

    if instance.period == PublishPeriod.CUSTOM and instance.cron_expression:
        m, h, dom, mon, dow = (instance.cron_expression.split() + ["*"]*5)[:5]
        cron_params = dict(minute=m, hour=h, day_of_month=dom, month_of_year=mon, day_of_week=dow)
    else:
        minute, hour = _mins_hrs(instance.run_at)
        if instance.period == PublishPeriod.DAILY:
            cron_params = dict(
                minute=minute,
                hour=hour,
                day_of_week="*",
                day_of_month="*",
                month_of_year="*")
        elif instance.period == PublishPeriod.WEEKLY:
            cron_params = dict(
                minute=minute,
                hour=hour,
                day_of_week=(instance.day_of_week or "mon"),
                day_of_month="*", month_of_year="*")
        elif instance.period == PublishPeriod.MONTHLY:
            cron_params = dict(
                minute=minute,
                hour=hour,
                day_of_week="*",
                day_of_month=str(instance.day_of_month or 1),
                month_of_year="*")
        else:
            cron_params = dict(
                minute=minute,
                hour=hour,
                day_of_week="*",
                day_of_month="*",
                month_of_year="*")

    schema = str(connection.schema_name)

    with schema_context(get_public_schema_name()):
        crontab, _ = CrontabSchedule.objects.get_or_create(
            timezone=instance.timezone, **cron_params
        )
        PeriodicTask.objects.update_or_create(
            name=name,
            queue='update',
            defaults={
                "task": "bims.tasks.gbif_publish.run_scheduled_gbif_publish",
                "crontab": crontab,
                "args": json.dumps([schema, instance.id]),
                "enabled": instance.enabled,
            },
        )


@receiver(post_save, sender=GbifPublish)
def seed_contacts_from_source_reference_authors(sender, instance: GbifPublish, created: bool, **kwargs):
    """On first creation, add each source reference author as a schedule-level contact."""
    if not created or not instance.source_reference_id:
        return

    from bims.models.source_reference import SourceReferenceAuthor

    contacts = []
    for item in instance.source_reference.author_list:
        if isinstance(item, SourceReferenceAuthor):
            linked_author = item.author
            user = linked_author.user
            name_source = user if user else linked_author
        else:
            user = item
            name_source = item
        contacts.append(GbifPublishContact(
            gbif_publish=instance,
            user=user,
            individual_name_given=name_source.first_name or "",
            individual_name_sur=name_source.last_name or "",
        ))
    GbifPublishContact.objects.bulk_create(contacts)
