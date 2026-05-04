from datetime import datetime

from django.contrib.auth import get_user_model
from django.db import models
from django.conf import settings
from taggit.managers import TaggableManager
from taggit.models import TaggedItemBase

from bims.models.taxonomy import Taxonomy, AbstractTaxonomy, TaxonTag
from bims.models.taxon_group_taxonomy import TaxonGroupTaxonomy
from bims.tasks.send_notification import send_mail_notification
from bims.utils.domain import get_current_domain


class CustomTaggedUpdateTaxonomy(TaggedItemBase):
    content_object = models.ForeignKey(
        'TaxonomyUpdateProposal',
        on_delete=models.CASCADE)
    tag = models.ForeignKey(
        TaxonTag,
        on_delete=models.CASCADE,
        related_name="%(app_label)s_%(class)s_items",
    )

    class Meta:
        verbose_name = "Custom Tagged Taxonomy"
        verbose_name_plural = "Custom Tagged Taxa"


class TaxonomyUpdateProposal(AbstractTaxonomy):
    # Status of the proposal (e.g., pending, approved, rejected)
    status = models.CharField(
        max_length=20,
        choices=[
            ('pending', 'Pending'),
            ('approved', 'Approved'),
            ('rejected', 'Rejected')
        ],
        default='pending'
    )

    fada_id = models.CharField(
        verbose_name='FADA ID',
        unique=False,
        null=True,
        blank=True,
        max_length=50
    )

    new_data = models.BooleanField(
        default=False
    )

    tags = TaggableManager(
        blank=True,
        related_name='taxonomy_proposal_tags'
    )

    owner = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        models.SET_NULL,
        blank=True,
        null=True,
        related_name='taxonomy_proposal_owner'
    )

    biographic_distributions = TaggableManager(
        through=CustomTaggedUpdateTaxonomy,
        related_name='taxonomy_proposal_bio_distribution',
        blank=True,
    )

    vernacular_names = models.ManyToManyField(
        to='VernacularName',
        related_name='%(class)s_vernacular_names',
        blank=True,
    )

    parent = models.ForeignKey(
        verbose_name='Parent',
        to='bims.Taxonomy',
        on_delete=models.SET_NULL,
        related_name='%(class)s_parent',
        null=True,
        blank=True
    )

    national_conservation_status = models.ForeignKey(
        'IUCNStatus',
        models.SET_NULL,
        related_name='%(class)s_national_conservation_status',
        verbose_name='National Conservation Status',
        null=True,
        blank=True,
    )

    iucn_status = models.ForeignKey(
        'IUCNStatus',
        models.SET_NULL,
        related_name='%(class)s_iucn_status',
        verbose_name='Global Red List Status (IUCN)',
        null=True,
        blank=True,
    )

    endemism = models.ForeignKey(
        'Endemism',
        models.SET_NULL,
        related_name='%(class)s_endemism',
        verbose_name='Endemism',
        null=True,
        blank=True
    )

    accepted_taxonomy = models.ForeignKey(
        related_name='taxonomy_update_proposal_accepted_taxonomy',
        to='bims.Taxonomy',
        on_delete=models.SET_NULL,
        null=True,
        blank=True
    )

    subgenus = models.ForeignKey(
        'bims.Taxonomy',
        null=True,
        blank=True,
        related_name='taxonomy_update_proposal_subgenus',
        on_delete=models.SET_NULL,
    )

    original_taxonomy = models.ForeignKey(
        Taxonomy,
        verbose_name='Original Taxonomy',
        on_delete=models.CASCADE,
        related_name='proposals',
        null=True,
    )

    taxon_group = models.ForeignKey(
        'bims.TaxonGroup',
        on_delete=models.CASCADE,
        null=True,
        blank=True
    )

    # Reference to the taxon group currently under review or validation.
    # This may change based on the proposal's review process.
    taxon_group_under_review = models.ForeignKey(
        'bims.TaxonGroup',
        related_name='taxon_group_under_review',
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        verbose_name="Taxon Group Under Review"
    )

    reviewers = models.ManyToManyField(
        to=settings.AUTH_USER_MODEL,
        blank=True,
        through='TaxonomyUpdateReviewer',
        related_name='taxonomy_update_proposals_reviewers',
    )

    updated_at = models.DateTimeField(auto_now=True)

    # Set when this proposal is captured into a published ChecklistVersion.
    # Null means the proposal has been approved but not yet released.
    captured_in_version = models.ForeignKey(
        'bims.ChecklistVersion',
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name='captured_proposals',
        help_text=(
            'The first ChecklistVersion in which this approved proposal '
            'was included. Null = approved but not yet in any release.'
        ),
    )

    @property
    def last_modified(self):
        return self.updated_at or None

    class Meta:
        db_table = 'bims_taxonomy_update_proposal'
        # unique_together = ('original_taxonomy', 'taxon_group', 'status')

    def reject_data(self, reviewer: settings.AUTH_USER_MODEL, comments: str = ''):
        if self.status == 'pending':
            self.status = 'rejected'
            TaxonomyUpdateReviewer.objects.update_or_create(
                taxonomy_update_proposal=self,
                reviewer=reviewer,
                defaults={
                    'status': 'rejected',
                    'comments': comments
                }
            )
            # Only save status — do not update last_modified_by or other data fields
            self.save(update_fields=['status'])

            current_site = get_current_domain()
            from_email = settings.DEFAULT_FROM_EMAIL

            if self.collector_user:
                recipients = [self.collector_user.email]
                subject = '[{}] Taxon {} Submission Rejected'.format(
                    current_site,
                    'Addition' if self.new_data else 'Update'
                )
                message = (f"You have received the following notice from {current_site}:"
                           f"\n\nThe following data was rejected:"
                           f"\n{self.canonical_name}"
                           f"\n\nAnd the reason for rejection:"
                           f"\n{comments if comments else 'No specific reason provided.'}"
                           f"\n\nPlease review the details and feel free to contact us if "
                           f"you have any questions or need further assistance."
                           )
                send_mail_notification.delay(
                    subject,
                    message,
                    from_email,
                    recipients
                )
            # Also send the email to expert and superuser
            staff_subject = '[{}] Taxon {} Submission Rejection Notification'.format(
                current_site,
                'Addition' if self.new_data else 'Update'
            )

            recipients = []
            experts = self.taxon_group.get_all_experts()
            for expert in experts:
                if expert.email not in recipients:
                    recipients.append(expert.email)
            superusers = list(
                get_user_model().objects.filter(
                    is_superuser=True).values_list('email', flat=True)
            )
            recipients = list(set(superusers + recipients))
            message_for_staff = (
                f"Dear Staff/Expert,"
                f"\n\nPlease be informed that a taxon at {current_site} has been rejected."
                f"\n\nSubmission Details:"
                f"\nTaxon Name: {self.canonical_name}"
                f"\n\nReason for Rejection:"
                f"\n{comments if comments else 'No specific reason provided.'}")

            send_mail_notification.delay(
                staff_subject,
                message_for_staff,
                from_email,
                recipients
            )

            if self.new_data:
                TaxonGroupTaxonomy.objects.filter(
                    taxongroup=self.taxon_group,
                    taxonomy=self.original_taxonomy
                ).delete()
            else:
                TaxonGroupTaxonomy.objects.filter(
                    taxongroup=self.taxon_group,
                    taxonomy=self.original_taxonomy,
                ).update(
                    is_validated=True
                )

    def validate_taxon(self, taxon_group, taxonomy):
        TaxonGroupTaxonomy.objects.filter(
            taxongroup=taxon_group,
            taxonomy=taxonomy,
        ).update(
            is_validated=True
        )
        if taxon_group.parent:
            self.validate_taxon(taxon_group.parent, taxonomy)

    def approve(self, reviewer: settings.AUTH_USER_MODEL, suppress_emails: bool = False):
        """
        Apply the proposed changes to the associated Taxonomy instance
        and update its status to 'approved'.
        """
        TaxonomyUpdateReviewer.objects.update_or_create(
            taxonomy_update_proposal=self,
            reviewer=reviewer,
            defaults={
                'status': 'approved'
            }
        )
        top_level_taxon_group = self.taxon_group.get_top_level_parent()

        if top_level_taxon_group != self.taxon_group_under_review:
            if not suppress_emails:
                parent_expert = list(top_level_taxon_group.experts.values_list(
                    'email', flat=True
                ))
                superuser = list(
                    get_user_model().objects.filter(
                        is_superuser=True).values_list('email', flat=True)
                )
                recipients = list(set(parent_expert + superuser))
                current_site = get_current_domain()
                subject = '[{}] Taxon Validation Required'.format(current_site)
                from_email = settings.DEFAULT_FROM_EMAIL
                message = (f"Dear Validator,\n\nThe taxon '{self.original_taxonomy.canonical_name}' "
                           f"has been validated by the current expert.\n\nIt now requires "
                           f"validation to be added to the taxon group '{top_level_taxon_group.name}'.")
                send_mail_notification.delay(
                    subject,
                    message,
                    from_email,
                    recipients
                )
            self.taxon_group_under_review = top_level_taxon_group
            self.save()
        else:
            # Only top level experts can approve data
            if top_level_taxon_group.experts.filter(
                id=reviewer.id
            ).exists() or reviewer.is_superuser:
                fields_to_update = [
                    'scientific_name',
                    'canonical_name',
                    'legacy_canonical_name',
                    'rank',
                    'taxonomic_status',
                    'author',
                    'endemism',
                    'iucn_status',
                    'accepted_taxonomy',
                    'species_group',
                    'parent',
                    'tags',
                    'biographic_distributions',
                    'additional_data',
                    'vernacular_names',
                    'gbif_key',
                    'gbif_data',
                    'fada_id',
                    'origin',
                    'subgenus']
                for field in fields_to_update:
                    if field == 'tags':
                        self.original_taxonomy.tags.clear()
                        self.original_taxonomy.tags.set(getattr(self, field).all())
                    elif field == 'biographic_distributions':
                        self.original_taxonomy.biographic_distributions.clear()
                        self.original_taxonomy.biographic_distributions.set(
                            getattr(self, field).all())
                    elif field == 'vernacular_names':
                        self.original_taxonomy.vernacular_names.clear()
                        self.original_taxonomy.vernacular_names.set(
                            getattr(self, field).all())
                    elif field == 'endemism' and getattr(self, field) is None:
                        # Don't overwrite existing endemism with None unless
                        # the original taxonomy also has no endemism
                        pass
                    elif field == 'subgenus':
                        self.original_taxonomy.subgenus_id = self.subgenus_id
                    else:
                        setattr(
                            self.original_taxonomy,
                            field, getattr(self, field))
                self.original_taxonomy.hierarchical_data = {}
                self.original_taxonomy.last_modified_by = self.last_modified_by
                self.original_taxonomy.save()
                self.status = 'approved'
                self.save()

                self.validate_taxon(
                    self.taxon_group,
                    self.original_taxonomy
                )

                if not suppress_emails:
                    self.send_success_emails(reviewer)

    def send_success_emails(self, reviewer, comments: str = "",):
        """
        Send notification emails to the collector and staff upon successful validation.

        Args:
            reviewer (User, optional): User reviewing the taxon
            comments (str, optional): Comments regarding the approval.
        """
        current_site = get_current_domain()
        from_email = settings.DEFAULT_FROM_EMAIL

        # Email to the collector
        if self.collector_user:
            submission_type = 'Addition' if self.new_data else 'Update'
            collector_subject = f'[{current_site}] Taxon {submission_type} Submission Approved'
            collector_message = (
                f"Dear {self.collector_user.get_full_name()},\n\n"
                f"Your submission for the taxon '{self.canonical_name}' "
                f"has been successfully approved by "
                f"the experts at {current_site}.\n\n"
            )
            send_mail_notification.delay(
                collector_subject,
                collector_message,
                from_email,
                [self.collector_user.email]
            )

        # Email to staff and experts
        staff_subject = (
            f'[{current_site}] Taxon '
            f'{"Addition" if self.new_data else "Update"} Submission Approved'
        )

        # Gather all expert emails
        recipients = []
        experts = self.taxon_group.get_all_experts()
        for expert in experts:
            if expert.email not in recipients:
                recipients.append(expert.email)
        superusers = list(
            get_user_model().objects.filter(
                is_superuser=True).values_list('email', flat=True)
        )
        recipients = list(set(superusers + recipients))

        staff_message = (
            f"Dear Staff/Expert,\n\n"
            f"The taxon '{self.canonical_name}' has been successfully validated and "
            f"approved at {current_site}.\n\n"
            f"Submission Details:\n"
            f"Taxon Name: {self.canonical_name}\n\n"
            f"Taxon Group: {self.taxon_group.name}\n\n"
            f"Approved by: {reviewer.get_full_name()}\n"
            f"Date: {datetime.now().strftime('%Y-%m-%d')}\n\n"
        )

        send_mail_notification.delay(
            staff_subject,
            staff_message,
            from_email,
            recipients
        )


class TaxonomyUpdateReviewer(models.Model):
    taxonomy_update_proposal = models.ForeignKey(
        TaxonomyUpdateProposal,
        on_delete=models.CASCADE,
    )
    reviewer = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        null=True,
        blank=True
    )
    status = models.CharField(
        max_length=20,
        choices=[
            ('pending', 'Pending'),
            ('approved', 'Approved'),
            ('rejected', 'Rejected')
        ],
        default='pending'
    )
    comments = models.TextField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
