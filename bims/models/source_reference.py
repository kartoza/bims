# coding=utf-8
"""
    Source reference that overridden with it's own source
"""
import re
import json
from collections import OrderedDict

from django.contrib.sites.models import Site
from django.db import models
from django.conf import settings
from django.db.models import Q
from django.db.models.fields.reverse_related import ForeignObjectRel
from django.dispatch import receiver
from polymorphic.models import PolymorphicModel
from td_biblio.models.bibliography import Entry, Author
from geonode.documents.models import Document
from bims.helpers.remove_duplicates import remove_duplicates
from bims.utils.decorator import prevent_recursion
from bims.models.bims_document import BimsDocument, BimsDocumentAuthorship

PUBLISHED_REPORT = 'Published book, report or thesis'


def format_authors(users):
    """Format queryset of users to readable format"""
    if not users:
        return ''
    index = 0
    author = u''
    for doc_author in users:
        if not doc_author:
            continue
        if (0 < index <
                len(users) - 1):
            author += ', '
        elif index > 0 and index == len(users) - 1:
            author += ' & '
        author += u'{first_name} {last_name}'.format(
            first_name=doc_author.first_name,
            last_name=doc_author.last_name
        )
        index += 1
    return author


class SourceIsNotFound(Exception):
    """ Exception if source is not found"""

    def __init__(self):
        message = 'Source is not found'
        errors = message
        super(SourceIsNotFound, self).__init__(message)
        self.errors = errors


class CategoryIsNotRecognized(Exception):
    """ If category is not recognized """

    def __init__(self):
        message = 'Category is not recognized'
        errors = message
        super(CategoryIsNotRecognized, self).__init__(message)
        self.errors = errors


class DatabaseRecord(models.Model):
    """ Model that has database records """
    name = models.CharField(
        null=False,
        blank=False,
        max_length=256
    )
    description = models.TextField(
        null=True, blank=True)
    url = models.URLField(
        null=True, blank=True)

    def __unicode__(self):
        name = '%s' % self.name
        if self.url:
            name += ' (%s)' % self.url
        return u'%s' % name

    def __str__(self):
        name = '%s' % self.name
        if self.url:
            name += ' (%s)' % self.url
        return '%s' % name


class SourceReference(PolymorphicModel):
    """ Source reference model """
    note = models.TextField(
        null=True, blank=True)
    source_name = models.CharField(
        blank=True,
        default='',
        max_length=512
    )
    verified = models.BooleanField(
        default=False
    )
    mobile = models.BooleanField(
        help_text='Also available for mobile app',
        default=False,
    )
    active_sites = models.ManyToManyField(
        to=Site,
        related_name='source_reference_active_sites',
        blank=True,
        help_text='Identifies the sites where the source reference is actively used.'
    )
    metadata_file = models.FileField(
        upload_to='source_reference_metadata/',
        null=True,
        blank=True,
        help_text='Upload metadata documentation (PDF or Word document) describing this source reference'
    )
    source_authors = models.ManyToManyField(
        Author,
        blank=True,
        related_name='source_references',
        help_text='Author(s) of this source reference',
        through='SourceReferenceAuthor'
    )
    source_date = models.DateField(
        null=True,
        blank=True,
        help_text='Date of this source reference'
    )
    publish_to_gbif = models.BooleanField(
        default=True,
        help_text='Allow this source reference to be published to GBIF. '
                  'Uncheck to exclude it from all GBIF publish schedules.'
    )

    @property
    def reference_source(self):
        """Source of the reference"""
        if self.source_name:
            return self.source_name
        return '-'

    @property
    def occurrences(self):
        from bims.models.biological_collection_record import (
            BiologicalCollectionRecord
        )
        return BiologicalCollectionRecord.objects.filter(
            source_reference=self.id
        ).count()

    @property
    def water_temperature(self):
        from bims.models.water_temperature import WaterTemperature
        return WaterTemperature.objects.filter(
            source_reference_id=self.id
        ).count()

    @property
    def chemical_records(self):
        from bims.models.chemical_record import (
            ChemicalRecord
        )
        return ChemicalRecord.objects.filter(
            source_reference=self.id
        ).count()

    @property
    def year(self):
        if self.source_date:
            return str(self.source_date.year)
        if re.search(r'[12]\d{3}', self.source_name):
            year = re.findall(r'[12]\d{3}', self.source_name)[0]
            return year
        return '-'

    @property
    def author_list(self):
        """Return list of Author objects"""
        return list(
            SourceReferenceAuthor.objects.filter(source_reference_id=self.id).order_by('order'))

    @property
    def author_names_list(self):
        """Return authors formatted as 'A, B & C' or 'A & B' or 'A'."""
        names = [
            a.author.get_formatted_name() for a in
            SourceReferenceAuthor.objects.filter(source_reference_id=self.id).order_by('order')]
        if not names:
            return ''
        if len(names) == 1:
            return names[0]
        return '{} & {}'.format(', '.join(names[:-1]), names[-1])

    @property
    def authors(self):
        """Return authors as a formatted string"""
        return self.author_names_list or '-'

    @property
    def title(self):
        if self.note:
            return self.note
        if self.source_name:
            return self.source_name
        else:
            return 'Unpublished data'

    @property
    def reference_type(self):
        return 'Unpublished data'

    def link_template(self):
        """Returns html template containing the reference data"""
        if self.note:
            return (
                '<i class="fa fa-file-o" aria-hidden="true"></i>'
                ' {note}'.format(note=self.note)
            )
        return (
            '<i class="fa fa-file-o" aria-hidden="true"></i>'
            ' Unpublished data'
        )

    @property
    def source(self):
        return self

    def is_published_report(self):
        return self.reference_type == PUBLISHED_REPORT

    def is_database(self):
        return self.reference_type == 'Database'

    def is_bibliography(self):
        return self.reference_type == 'Peer-reviewed scientific article'

    def is_unpublished(self):
        return self.reference_type == 'Unpublished data'

    def __unicode__(self):
        if not self.get_source_unicode():
            if self.note:
                return self.note
            else:
                return 'None'
        else:
            return self.get_source_unicode()

    def __str__(self):
        if not self.get_source_unicode():
            if self.note:
                return self.note
            else:
                return 'None'
        else:
            return self.get_source_unicode()

    def get_source_unicode(self):
        try:
            return self.sourcereferencebibliography.__unicode__()
        except SourceReferenceBibliography.DoesNotExist:
            pass
        try:
            return self.sourcereferencedatabase.__unicode__()
        except SourceReferenceDatabase.DoesNotExist:
            pass
        try:
            return self.sourcereferencedocument.__unicode__()
        except SourceReferenceDocument.DoesNotExist:
            pass
        return None

    @staticmethod
    def create_source_reference(category, source_id, note):
        """ Create source reference based on category """
        source = None
        _SourceModel = None
        try:
            if category == 'bibliography':
                source = Entry.objects.get(id=source_id)
                _SourceModel = SourceReferenceBibliography
            elif category == 'document':
                source = Document.objects.get(id=source_id)
                _SourceModel = SourceReferenceDocument
            elif category == 'database':
                source = DatabaseRecord.objects.get(id=source_id)
                _SourceModel = SourceReferenceDatabase
            elif category:
                raise CategoryIsNotRecognized()
            else:
                _SourceModel = SourceReference
        except (
                Entry.DoesNotExist,
                Document.DoesNotExist,
                DatabaseRecord.DoesNotExist):
            raise SourceIsNotFound()
        model_fields = {
            'note': note
        }
        if source:
            model_fields['source'] = source
        if not category:
            try:
                source_reference, _ = SourceReference.objects.get_or_create(
                    **model_fields)
            except SourceReference.MultipleObjectsReturned:
                source_reference = remove_duplicates(
                    SourceReference.objects.filter(
                        **model_fields
                    ))
            return source_reference
        try:
            source_reference, created = _SourceModel.objects.get_or_create(
                **model_fields
            )
        except _SourceModel.MultipleObjectsReturned:
            source_reference = _SourceModel.objects.filter(
                **model_fields
            )[0]
        return source_reference


class SourceReferenceAuthor(models.Model):
    """Through model for ordered authors on a SourceReference."""
    source_reference = models.ForeignKey(
        SourceReference,
        on_delete=models.CASCADE,
        related_name='source_reference_authors',
        db_column='sourcereference_id'
    )
    author = models.ForeignKey(
        Author,
        on_delete=models.CASCADE,
        related_name='source_reference_authorships'
    )
    order = models.PositiveIntegerField(default=0)

    class Meta:
        ordering = ['order']
        unique_together = ('source_reference', 'author')
        db_table = 'bims_sourcereference_source_authors'


class SourceReferenceBibliography(SourceReference):
    """ Source reference with bibliography source"""
    source = models.ForeignKey(Entry, null=True, on_delete=models.CASCADE)
    document = models.ForeignKey(
        Document, null=True, blank=True, on_delete=models.SET_NULL)

    @property
    def reference_source(self):
        """Source of the reference"""
        source = '-'
        if self.source.journal:
            source = self.source.journal.name
        return source

    @property
    def title(self):
        return self.source.title

    @property
    def author_list(self):
        authors = self.source.get_authors()
        if authors:
            users = []
            for author in authors:
                if author.user:
                    users.append(author.user)
                elif author:
                    author.save()
                    if author.user:
                        users.append(author.user)
            authors = users
        if self.document:
            try:
                bims_document = BimsDocument.objects.get(
                    document__id=self.document.id
                )
                bims_authors = list(bims_document.authors.all())
                if bims_authors:
                    if len(bims_authors) != len(authors) or any(a not in authors for a in bims_authors):
                        authors = bims_authors
            except BimsDocument.DoesNotExist:
                pass
        return authors

    @property
    def authors(self):
        authors_name = format_authors(self.author_list).strip()
        return authors_name if authors_name else '-'

    @property
    def year(self):
        try:
            bims_document = BimsDocument.objects.get(
                document=self.document
            )
            if bims_document.year:
                return bims_document.year
        except BimsDocument.DoesNotExist:
            pass
        if self.source.publication_date:
            return self.source.publication_date.year
        return '-'

    @staticmethod
    def create_entry(author_string, journal, date):
        pass

    @property
    def reference_type(self):
        return 'Peer-reviewed scientific article'

    def link_template(self):
        """Returns html template containing the reference data"""
        link = 'http://dx.doi.org/{doi}'.format(doi=self.source.doi)
        if not self.source.doi:
            if self.source.url:
                link = self.source.url
            elif self.document:
                link = '{media}{url}'.format(
                    media=settings.MEDIA_URL,
                    url=self.document.doc_file,
                )
        return (
            '<i class="fa fa-newspaper-o" aria-hidden="true"></i>'
            ' <a href="{link}" '
            'target="_blank">{source}</a>'.format(
                link=link,
                source=self.title
            )
        )

    def __unicode__(self):
        return u'%s' % self.source

    def __str__(self):
        return '%s' % self.source


class SourceReferenceDatabase(SourceReference):
    """ Source reference with database source"""
    source = models.ForeignKey(DatabaseRecord, on_delete=models.CASCADE)
    document = models.ForeignKey(
        Document, null=True, blank=True, on_delete=models.SET_NULL)

    @property
    def title(self):
        return self.source.name

    @property
    def reference_type(self):
        return 'Database'

    @property
    def year(self):
        if self.source_date:
            return str(self.source_date.year)
        if re.search(r'[12]\d{3}', self.source_name):
            year = re.findall(r'[12]\d{3}', self.source_name)[0]
            return year
        return '-'

    def link_template(self):
        """Returns html template containing the reference data"""
        if self.source.url:
            return (
                '<i class="fa fa-database" aria-hidden="true"></i>'
                ' <a href="{url}" '
                'target="_blank">{source}</a>'.format(
                    url=self.source.url,
                    source=self.title
                )
            )
        return (
            '<i class="fa fa-database" aria-hidden="true"></i>'
            ' {source}'.format(
                source=self.source
            )
        )

    def __unicode__(self):
        return u'%s' % self.source


class SourceReferenceDocument(SourceReference):
    """ Source reference with database source"""
    source = models.ForeignKey(Document, on_delete=models.CASCADE)

    @property
    def reference_source(self):
        try:
            source = json.loads(
                self.source.supplemental_information
            )['document_source']
        except:  # noqa
            source = '-'
        return source

    @property
    def year(self):
        try:
            bims_doc = BimsDocument.objects.get(
                document=self.source
            )
            if bims_doc.year:
                return bims_doc.year
        except BimsDocument.DoesNotExist:
            pass
        if self.source.date:
            return self.source.date.year
        return '-'

    @property
    def title(self):
        return self.source.title

    @property
    def author_list(self):
        try:
            bims_doc = BimsDocument.objects.get(
                document=self.source
            )
            authors = BimsDocumentAuthorship.objects.filter(
                bimsdocument_id=bims_doc
            )
            if authors.exists():
                users = []
                for author in authors:
                    users.append(author.profile)
                return users
        except BimsDocument.DoesNotExist:
            pass
        return [self.source.owner] if self.source.owner else []

    @property
    def authors(self):
        authors_string = format_authors(self.author_list)
        return authors_string if authors_string else '-'

    def link_template(self):
        """Returns html template containing the reference data"""
        href = ''
        if self.source.doc_file:
            href = '"{media}{url}" download'.format(
                media=settings.MEDIA_URL,
                url=self.source.doc_file,
            )
        elif self.source.doc_url:
            href = '"{url}" target="_blank"'.format(
                url=self.source.doc_url,
            )
        return (
            '<i class="fa fa-file-pdf-o" aria-hidden="true"></i>'
            ' <a href={href}>{source}</a>'.format(
                href=href,
                source=self.title
            )
        )

    @property
    def reference_type(self):
        return PUBLISHED_REPORT

    def __unicode__(self):
        return u'%s' % self.source


LIST_SOURCE_REFERENCES = OrderedDict([
    (SourceReferenceDatabase().reference_type, SourceReferenceDatabase),
    (
        SourceReferenceBibliography().reference_type,
        SourceReferenceBibliography),
    (SourceReferenceDocument().reference_type, SourceReferenceDocument),
    (SourceReference().reference_type, SourceReference),
])


@receiver(models.signals.post_save, sender=SourceReferenceDatabase)
@receiver(models.signals.post_save, sender=SourceReferenceBibliography)
@receiver(models.signals.post_save, sender=SourceReference)
@receiver(models.signals.post_save, sender=SourceReferenceDocument)
@prevent_recursion
def source_reference_post_save_handler(sender, instance, **kwargs):
    from bims.tasks.source_reference import (
        generate_source_reference_filter
    )
    generate_source_reference_filter.delay()


def disconnect_source_reference_signals():
    models.signals.post_save.disconnect(receiver=source_reference_post_save_handler, sender=SourceReferenceDatabase)
    models.signals.post_save.disconnect(receiver=source_reference_post_save_handler, sender=SourceReferenceBibliography)
    models.signals.post_save.disconnect(receiver=source_reference_post_save_handler, sender=SourceReference)
    models.signals.post_save.disconnect(receiver=source_reference_post_save_handler, sender=SourceReferenceDocument)

def reconnect_source_reference_signals():
    models.signals.post_save.connect(receiver=source_reference_post_save_handler, sender=SourceReferenceDatabase)
    models.signals.post_save.connect(receiver=source_reference_post_save_handler, sender=SourceReferenceBibliography)
    models.signals.post_save.connect(receiver=source_reference_post_save_handler, sender=SourceReference)
    models.signals.post_save.connect(receiver=source_reference_post_save_handler, sender=SourceReferenceDocument)


def merge_source_references(primary_source_reference, source_reference_list):
    """
    Merge multiple source references into one primary_source_reference
    """
    if not primary_source_reference and not source_reference_list:
        return

    print('Merging %s source reference' % len(source_reference_list))

    source_references = SourceReference.objects.filter(
        id__in=source_reference_list.values_list('id', flat=True)
    ).exclude(id=primary_source_reference.id)

    primary_source_reference = SourceReference.objects.get(
        id=primary_source_reference.id
    )

    links = [
        rel.get_accessor_name() for rel in primary_source_reference._meta.get_fields() if
        issubclass(type(rel), ForeignObjectRel) and rel.get_accessor_name() not in
        ['sourcereferencebibliography', 'sourcereferencedatabase', 'sourcereferencedocument']
    ]

    if links:
        for source_reference in source_references:
            for link in links:
                try:
                    objects = getattr(source_reference, link).all()
                    if objects.count() > 0:
                        update_dict = {
                            getattr(source_reference, link).field.name: primary_source_reference
                        }
                        objects.update(**update_dict)
                except Exception as e:  # noqa
                    print(e)
                    continue
            if (
                    source_reference.source and
                    source_reference.source != primary_source_reference.source
            ):
                source_reference.source.delete()
            else:
                source_reference.delete()
