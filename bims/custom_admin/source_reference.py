from bims.models.biological_collection_record import BiologicalCollectionRecord
from django.contrib import admin
from django.contrib import messages

from polymorphic.admin import (
    PolymorphicParentModelAdmin,
    PolymorphicChildModelAdmin,
    PolymorphicChildModelFilter)
from bims.models.source_reference import (
    DatabaseRecord,
    SourceReference,
    SourceReferenceBibliography,
    SourceReferenceDatabase, SourceReferenceDocument, merge_source_references
)


class HasGbifPublishFilter(admin.SimpleListFilter):
    title = "GBIF publish schedule"
    parameter_name = "has_gbif_publish"

    def lookups(self, request, model_admin):
        return (
            ("yes", "Has GBIF publish schedule"),
            ("no", "No GBIF publish schedule"),
        )

    def queryset(self, request, queryset):
        from bims.models.gbif_publish import GbifPublish
        scheduled_ids = GbifPublish.objects.values_list(
            "source_reference_id", flat=True
        ).distinct()
        if self.value() == "yes":
            return queryset.filter(pk__in=scheduled_ids)
        if self.value() == "no":
            return queryset.exclude(pk__in=scheduled_ids)
        return queryset


class PublishToGbifFilter(admin.SimpleListFilter):
    title = "Publish to GBIF"
    parameter_name = "publish_to_gbif"

    def lookups(self, request, model_admin):
        return (
            ("yes", "Allowed"),
            ("no", "Excluded"),
        )

    def queryset(self, request, queryset):
        if self.value() == "yes":
            return queryset.filter(publish_to_gbif=True)
        if self.value() == "no":
            return queryset.filter(publish_to_gbif=False)
        return queryset


class DatabaseRecordAdmin(admin.ModelAdmin):
    list_display = ('name', 'url')


class SourceReferenceBibliographyAdmin(PolymorphicChildModelAdmin):
    list_display = ('source', 'note', 'has_metadata')
    base_model = SourceReferenceBibliography
    fields = ('source', 'document', 'note', 'source_name', 'verified', 'mobile', 'publish_to_gbif', 'metadata_file')

    def has_metadata(self, obj):
        return 'Yes' if obj.metadata_file else 'No'
    has_metadata.short_description = 'Has Metadata'


class SourceReferenceDatabaseAdmin(PolymorphicChildModelAdmin):
    list_display = ('source', 'note', 'has_metadata')
    base_model = SourceReferenceDatabase
    fields = ('source', 'document', 'note', 'source_name', 'verified', 'mobile', 'publish_to_gbif', 'metadata_file')

    def has_metadata(self, obj):
        return 'Yes' if obj.metadata_file else 'No'
    has_metadata.short_description = 'Has Metadata'


class SourceReferenceDocumentAdmin(PolymorphicChildModelAdmin):
    list_display = ('source', 'note', 'has_metadata')
    base_model = SourceReferenceDocument
    fields = ('source', 'note', 'source_name', 'verified', 'mobile', 'publish_to_gbif', 'metadata_file')

    def has_metadata(self, obj):
        return 'Yes' if obj.metadata_file else 'No'
    has_metadata.short_description = 'Has Metadata'


class SourceReferenceAdmin(PolymorphicParentModelAdmin):
    """ The SourceReference """
    base_model = SourceReference
    list_display = (
        'source_reference_title',
        'reference_type',
        'verified',
        'total_records',
        'has_metadata',
        'has_gbif_publish',
        'publish_to_gbif',
    )
    child_models = (
        SourceReferenceBibliography,
        SourceReferenceDatabase,
        SourceReferenceDocument,
        SourceReference
    )
    list_filter = (PolymorphicChildModelFilter, HasGbifPublishFilter, PublishToGbifFilter)
    search_fields = (
        'sourcereferencebibliography__source__title',
        'sourcereferencedocument__source__title',
        'sourcereferencedatabase__source__name',
        'source_name',
    )

    def source_reference_title(self, obj):
        try:
            return obj.sourcereferencebibliography.title
        except SourceReferenceBibliography.DoesNotExist:
            pass
        try:
            return obj.sourcereferencedatabase.title
        except SourceReferenceDatabase.DoesNotExist:
            pass
        try:
            return obj.sourcereferencedocument.title
        except SourceReferenceDocument.DoesNotExist:
            pass
        return obj.title

    def reference_type(self, obj):
        try:
            return obj.sourcereferencebibliography.reference_type
        except SourceReferenceBibliography.DoesNotExist:
            pass
        try:
            return obj.sourcereferencedatabase.reference_type
        except SourceReferenceDatabase.DoesNotExist:
            pass
        try:
            return obj.sourcereferencedocument.reference_type
        except SourceReferenceDocument.DoesNotExist:
            pass
        return obj.reference_type

    def total_records(self, obj):
        return BiologicalCollectionRecord.objects.filter(
            source_reference=obj
        ).count()

    def has_metadata(self, obj):
        return 'Yes' if obj.metadata_file else 'No'

    def has_gbif_publish(self, obj):
        from bims.models.gbif_publish import GbifPublish
        return GbifPublish.objects.filter(source_reference=obj).exists()
    has_gbif_publish.boolean = True

    def publish_to_gbif(self, obj):
        return obj.publish_to_gbif
    publish_to_gbif.boolean = True

    source_reference_title.short_description = 'Title'
    reference_type.short_description = 'Reference Type'
    total_records.short_description = 'Total Occurrences'
    has_metadata.short_description = 'Has Metadata'
    has_gbif_publish.short_description = 'GBIF Publish Schedule'
    publish_to_gbif.short_description = 'Publish to GBIF'

    actions = ['merge_source_references', 'enable_gbif_publish', 'disable_gbif_publish']

    def enable_gbif_publish(self, request, queryset):
        updated = queryset.update(publish_to_gbif=True)
        self.message_user(request, f'{updated} source reference(s) marked as allowed for GBIF publishing.')
    enable_gbif_publish.short_description = 'Allow GBIF publishing for selected'

    def disable_gbif_publish(self, request, queryset):
        updated = queryset.update(publish_to_gbif=False)
        self.message_user(request, f'{updated} source reference(s) excluded from GBIF publishing.')
    disable_gbif_publish.short_description = 'Exclude selected from GBIF publishing'

    def merge_source_references(self, request, queryset):

        verified = queryset.filter(verified=True)
        if queryset.count() <= 1:
            self.message_user(
                request, 'Need more than 1 source reference', messages.ERROR
            )
            return

        if not verified.exists():
            self.message_user(
                request, 'Missing verified source reference', messages.ERROR)
            return

        if verified.count() > 1:
            self.message_user(
                request, 'There are more than 1 verified source reference',
                messages.ERROR)
            return

        gbif_published = queryset.filter(publish_to_gbif=True)
        if gbif_published.exists():
            titles = ', '.join(
                f'"{sr.title}"' for sr in gbif_published
            )
            self.message_user(
                request,
                f'Merge blocked: the following source reference(s) are marked as '
                f'"Publish to GBIF": {titles}. '
                f'You will need to manually remove their occurrences from GBIF, '
                f'set "Publish to GBIF" to No for each, then re-push from the '
                f'merged source reference.',
                messages.ERROR,
            )
            return

        merge_source_references(primary_source_reference=verified.first(),
                                source_reference_list=queryset)

    merge_source_references.short_description = 'Merge source references'


try:
    admin.site.unregister(DatabaseRecord)
    admin.site.unregister(SourceReferenceBibliography)
    admin.site.unregister(SourceReferenceDatabase)
    admin.site.unregister(SourceReferenceDocument)
    admin.site.unregister(SourceReference)
except Exception:  # noqa
    pass

admin.site.register(DatabaseRecord, DatabaseRecordAdmin)
admin.site.register(
    SourceReferenceBibliography, SourceReferenceBibliographyAdmin)
admin.site.register(SourceReferenceDatabase, SourceReferenceDatabaseAdmin)
admin.site.register(SourceReferenceDocument, SourceReferenceDocumentAdmin)
admin.site.register(SourceReference, SourceReferenceAdmin)
