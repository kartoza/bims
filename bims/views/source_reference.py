import json
from urllib.parse import urlencode

from braces.views import LoginRequiredMixin
from django.contrib.sites.models import Site
from django.views.generic import ListView, UpdateView, View, CreateView
from django.db.models import Q
from django.contrib.auth import get_user_model
from django.urls import reverse
from django.http import HttpResponseRedirect, JsonResponse
from django.http import Http404
from django.contrib import messages
from django.contrib.auth.mixins import UserPassesTestMixin
from geonode.base.models import HierarchicalKeyword, TaggedContentItem
from rest_framework.generics import ListAPIView
from rest_framework.pagination import PageNumberPagination
from urllib.parse import unquote


from bims.utils.user import get_user_from_name
from bims.models.source_reference import (
    SourceReference,
    SourceReferenceBibliography,
    SourceReferenceDatabase,
    SourceReferenceDocument,
    SourceReferenceAuthor,
    DatabaseRecord,
    PUBLISHED_REPORT,
)
from bims.serializers.source_reference_serializer import (
    SourceReferenceSerializer
)
from td_biblio.models.bibliography import (
    Author, AuthorEntryRank, Journal, Entry
)
from bims.models.bims_document import (
    BimsDocument,
    BimsDocumentAuthorship
)
from geonode.documents.models import Document
from bims.models.biological_collection_record import (
    BiologicalCollectionRecord
)
from bims.models.chemical_record import (
    ChemicalRecord
)


class SourceReferencePagination(PageNumberPagination):
    page_size = 20
    page_size_query_param = 'page_size'
    max_page_size = 1000


class SourceReferenceList(View):
    source_reference_type = {
        'database': SourceReferenceDatabase,
        'bibliography': SourceReferenceBibliography,
        'document': SourceReferenceDocument
    }
    collectors = None
    type_filter = ''
    search_query = ''

    def get(self, request, *args, **kwargs):
        """Check GET request parameters validity and store them"""
        # -- Type query
        source_reference_type = self.request.GET.get('type', None)
        if source_reference_type:
            self.type_filter = source_reference_type
        else:
            self.type_filter = ''

        # -- Search query
        self.search_query = self.request.GET.get('q', '')

        # -- Collectors
        self.collectors = self.request.GET.get('collectors', None)
        if self.collectors:
            self.collectors = self.collectors.split(',')
        return super(SourceReferenceList, self).get(
            request, *args, **kwargs)

    def get_queryset(self):
        """
        Add GET requests filters
        """
        filters = dict()
        qs = SourceReference.objects.all()

        if self.collectors:
            qs = qs.filter(
                Q(sourcereferencebibliography__source__authors__user__in=
                  self.collectors) |
                Q(sourcereferencebibliography__document__bimsdocument__authors__in=  # noqa
                  self.collectors) |
                Q(sourcereferencedocument__source__bimsdocument__authors__in=
                  self.collectors)
            )

        if self.type_filter:
            or_condition = Q()
            type_filters = self.type_filter.split(',')
            for type_filter in type_filters:
                if type_filter in self.source_reference_type:
                    or_condition |= Q(**{
                        'instance_of':
                            self.source_reference_type[type_filter]
                    })
                else:
                    for source_reference_type in self.source_reference_type:
                        or_condition &= Q(**{
                            'not_instance_of':
                                self.source_reference_type[
                                    source_reference_type]})
            qs = qs.filter(or_condition)

        if self.search_query:
            qs = qs.filter(
                Q(sourcereferencebibliography__source__title__icontains=
                  self.search_query) |
                Q(sourcereferencedocument__source__title__icontains=
                  self.search_query) |
                Q(sourcereferencedatabase__source__name__icontains=
                  self.search_query) |
                Q(note__icontains=self.search_query) |
                Q(source_name__icontains=self.search_query)
            )

        qs = qs.filter(**filters).distinct('id')

        # Return filtered queryset
        return qs


class SourceReferenceAPIView(SourceReferenceList, ListAPIView):
    pagination_class = SourceReferencePagination
    serializer_class = SourceReferenceSerializer


class SourceReferenceListView(SourceReferenceList, ListView):
    model = SourceReference
    template_name = 'source_reference_list.html'
    paginate_by = 15

    def get_context_data(self, object_list=None, **kwargs):
        context = super().get_context_data(**kwargs)
        data = self.get_queryset()
        # - Get owner object from the id passed from the request
        collector_owner = None
        if self.request.GET.get('collectors'):
            try:
                ids = self.request.GET.get('collectors').split(',')
                user_model = get_user_model()
                collector_owner = user_model.objects.filter(
                    id__in=ids
                )
            except ValueError:
                # Couldn't parse the ids
                pass
        context['type'] = [
            {
                'title': 'Unpublished',
                'count': data.exclude(
                    Q(instance_of=SourceReferenceDatabase) |
                    Q(instance_of=SourceReferenceDocument) |
                    Q(instance_of=SourceReferenceBibliography)
                ).count(),
                'key': 'unpublished',
                'selected': 'unpublished' in self.type_filter
            },
            {
                'title': 'Database',
                'count': data.instance_of(
                    SourceReferenceDatabase
                ).count(),
                'key': 'database',
                'selected': 'database' in self.type_filter
            },
            {
                'title': 'Published book, report or thesis',
                'count': data.instance_of(
                    SourceReferenceDocument
                ).count(),
                'key': 'document',
                'selected': 'document' in self.type_filter
            },
            {
                'title': 'Peer-reviewed scientific article',
                'count': data.instance_of(
                    SourceReferenceBibliography
                ).count(),
                'key': 'bibliography',
                'selected': 'bibliography' in self.type_filter
            }
        ]
        context['search'] = self.search_query
        context['collector_owner'] = collector_owner
        return context


class DeleteSourceReferenceView(UserPassesTestMixin, View):

    def test_func(self):
        if self.request.user.is_anonymous:
            return False
        if self.request.user.is_superuser:
            return True
        return self.request.user.has_perm('bims.change_sourcereference')

    def post(self, request, *args, **kwargs):
        reference_id = request.POST.get('reference_id')
        if not reference_id:
            return JsonResponse(
                {'success': False, 'message': 'No reference ID provided.'},
                status=400
            )

        try:
            source_reference = SourceReference.objects.get(id=reference_id)
        except SourceReference.DoesNotExist:
            return JsonResponse(
                {'success': False, 'message': 'Source reference does not exist.'},
                status=404
            )

        collection_records = BiologicalCollectionRecord.objects.filter(
            source_reference=source_reference
        )
        if collection_records.exists():
            return JsonResponse(
                {
                    'success': False,
                    'message': (
                        f'Source reference "{source_reference.title}" is linked to collection records. '
                        f'Delete those records before removing this reference.'
                    )
                },
                status=400
            )

        source_reference_name = source_reference.title
        database_record = None
        if isinstance(source_reference, SourceReferenceDatabase):
            database_record = source_reference.source
        source_reference.delete()
        if database_record:
            database_record.delete()

        return JsonResponse(
            {'success': True, 'message': f'Source reference "{source_reference_name}" successfully deleted!'}
        )


class EditSourceReferenceView(UserPassesTestMixin, UpdateView):
    template_name = 'edit_source_reference.html'
    model = SourceReference
    fields = ['note', 'source_name']

    def test_func(self):
        if self.request.user.is_anonymous:
            return False
        if self.request.user.is_superuser:
            return True
        return self.request.user.has_perm('bims.change_sourcereference')

    def get_success_url(self):
        next_url = self.request.GET.get('next')
        if next_url:
            return next_url
        else:
            return reverse(
                'edit-source-reference',
                args=(
                    self.object.id,
                ))

    def get_user_from_string(self, user_string):
        """Return user object from string of user name"""
        first_name = user_string.split(' ')[0]
        last_name = ' '.join(user_string.split(' ')[1:])
        user = get_user_from_name(
            first_name=first_name,
            last_name=last_name
        )
        return user

    def _collect_authors_from_post(self, post_dict):
        """
        Collect Author objects from author_N / author_id_N POST keys.
        """
        indices = set()
        for key in post_dict:
            if key.startswith('author_id_'):
                try:
                    indices.add(int(key[len('author_id_'):]))
                except ValueError:
                    pass
            elif key.startswith('author_'):
                try:
                    indices.add(int(key[len('author_'):]))
                except ValueError:
                    pass

        if not indices:
            return None

        author_objects = []
        for idx in sorted(indices):
            id_key = f'author_id_{idx}'
            name_key = f'author_{idx}'

            if id_key in post_dict and post_dict[id_key].strip():
                user = get_user_model().objects.get(
                    id=post_dict[id_key].strip()
                )
                author_obj = None
                try:
                    author_obj = Author.objects.get(
                        user=user,
                        first_name=user.first_name,
                        last_name=user.last_name
                    )
                except Author.DoesNotExist:
                    first_initial = user.first_name[0] if user.first_name else ''
                    author_obj, _ = Author.objects.get_or_create(
                        first_name=user.first_name,
                        last_name=user.last_name,
                        defaults={
                            'user': user,
                            'first_initial': first_initial
                        }
                    )
                except Author.MultipleObjectsReturned:
                    author_obj = Author.objects.filter(
                        user=user
                    ).first()
                if author_obj:
                    author_objects.append(author_obj)
            elif name_key in post_dict and post_dict[name_key].strip():
                raw = post_dict[name_key].strip()
                user = self.get_user_from_string(raw)
                if user:
                    try:
                        author_obj = Author.objects.get(
                            user=user,
                            first_name=user.first_name,
                            last_name=user.last_name
                        )
                    except Author.DoesNotExist:
                        first_initial = user.first_name[0] if user.first_name else ''
                        author_obj, _ = Author.objects.get_or_create(
                            first_name=user.first_name,
                            last_name=user.last_name,
                            defaults={
                                'user': user,
                                'first_initial': first_initial
                            }
                        )
                    except Author.MultipleObjectsReturned:
                        author_obj = Author.objects.filter(
                            user=user
                        ).first()
                else:
                    # No matching user — create/get Author by name only
                    parts = raw.split(' ', 1)
                    first_name = parts[0]
                    last_name = parts[1] if len(parts) > 1 else ''
                    first_initial = first_name[0] if first_name else ''
                    author_obj, _ = Author.objects.get_or_create(
                        first_name=first_name,
                        last_name=last_name,
                        defaults={'first_initial': first_initial}
                    )
                author_objects.append(author_obj)

        return author_objects

    def update_published_report_reference(self, post_dict):
        order = 1
        year = post_dict.get('year', None)
        title = post_dict.get('title', None)
        source = post_dict.get('source', None)
        doc_url = post_dict.get('doc_url', None)
        doc_type = post_dict.get('doc_type', None)
        doc_file = self.request.FILES.get('doc_file', None)
        notes = post_dict.get('notes', None)

        try:
            bims_doc = BimsDocument.objects.get(
                document__id=self.object.source.id)
        except BimsDocument.DoesNotExist:
            bims_doc = BimsDocument.objects.create(
                document=self.object.source
            )
        bims_doc.year = year
        bims_doc.authors.clear()
        # - Update title
        if title:
            self.object.source.title = title

        # - Update file
        if doc_type == 'doc_url' and doc_url:
            self.object.source.doc_url = doc_url
            self.object.source.doc_file = None
        elif doc_type == 'doc_file' and doc_file:
            self.object.source.doc_url = None
            self.object.source.doc_file = doc_file

        # - Update source
        if source:
            try:
                self.object.source.supplemental_information = json.dumps({
                    'document_source': source
                })
            except KeyError:
                pass
        for key in post_dict:
            author_user = None
            # From user id
            if 'author_id' in key:
                author_id = post_dict[key].strip()
                try:
                    author_user = get_user_model().objects.get(
                        id=author_id
                    )
                except get_user_model().DoesNotExist:
                    continue
            # From user name
            elif 'author' in key:
                user_string = post_dict[key].strip()
                author_user = self.get_user_from_string(user_string)
            if author_user:
                bims_doc_author, _ = (
                    BimsDocumentAuthorship.objects.get_or_create(
                        bimsdocument=bims_doc,
                        profile=author_user
                    )
                )
                bims_doc_author.ordering = order
                bims_doc_author.save()
            order += 1
        # update note
        self.object.note = notes
        self.object.save()
        self.object.source.save()
        bims_doc.save()

    def update_bibliography_reference(self, post_dict):
        rank = 0
        title = post_dict.get('title', None)
        year = post_dict.get('year', None)
        doi = post_dict.get('doi', None)
        source = post_dict.get('source', None)
        source_id = post_dict.get('source_id', None)
        url = post_dict.get('url', None)
        pdf_file = self.request.FILES.get('pdf_file', None)
        pdf_file_id = post_dict.get('pdf_file_id', None)
        notes = post_dict.get('notes', None)

        # - Check required fields
        if not title or not year:
            raise Http404('Incorrect POST body')
        # - Update title
        self.object.source.title = title
        # - Update year
        if year:
            self.object.source.publication_date = (
                self.object.source.publication_date.replace(
                    year=int(year)
                )
            )
        # - Update DOI
        if doi or self.object.source.doi:
            self.object.source.doi = doi

        if not pdf_file_id and self.object.document:
            self.object.document.delete()
            self.object.document = None

        if pdf_file:
            document, _ = Document.objects.get_or_create(
                owner=self.request.user,
                is_published=True,
                abstract=title,
                title=pdf_file.name,
                doc_file=pdf_file,
                supplemental_information=json.dumps({
                    'document_source': source
                })
            )
            if self.object.document:
                if self.object.document != document:
                    self.object.document.delete()
                    self.object.document = None

            self.object.document = document

        # - Update journal name
        if source_id:
            try:
                journal = Journal.objects.get(id=source_id)
                if source:
                    journal.name = source
                    journal.save()
                    self.object.source.journal = journal
                else:
                    self.object.source.journal = None
            except Journal.DoesNotExist:
                pass
        else:
            if source:
                journals = Journal.objects.filter(
                    name__iexact=source.strip()
                )
                if journals.exists():
                    self.object.source.journal = journals[0]
                else:
                    journal = Journal.objects.create(
                        name=source.strip()
                    )
                    self.object.source.journal = journal

        if url or self.object.source.url:
            self.object.source.url = url

        self.object.source.authors.clear()
        # - Update authors
        for key in post_dict:
            author_user = None
            if 'author_id' in key:
                author_id = post_dict[key].strip()
                try:
                    author_user = get_user_model().objects.get(
                        id=author_id
                    )
                except get_user_model().DoesNotExist:
                    continue
            elif 'author' in key:
                user_string = post_dict[key].strip()
                author_user = self.get_user_from_string(user_string)
            if author_user:
                try:
                    author = Author.objects.get(
                        user=author_user
                    )
                except Author.MultipleObjectsReturned:
                    author = Author.objects.filter(user=author_user)[0]
                except Author.DoesNotExist:
                    author = Author.objects.create(
                        first_name=author_user.first_name,
                        last_name=author_user.last_name,
                        user=author_user
                    )
                try:
                    author_entry_rank = AuthorEntryRank.objects.get(
                        author=author,
                        entry=self.object.source
                    )
                    author_entry_rank.rank = rank
                    author_entry_rank.save()
                except AuthorEntryRank.DoesNotExist:
                    AuthorEntryRank.objects.create(
                        author=author,
                        entry=self.object.source,
                        rank=rank
                    )
                    rank += 1
        # update note
        self.object.note = notes
        self.object.save()
        self.object.source.save()

    def set_ordered_authors(self, author_objects):
        SourceReferenceAuthor.objects.filter(
            source_reference=self.object
        ).delete()
        seen_ids = set()
        order = 0
        if author_objects:
            for author in author_objects:
                if author.id in seen_ids:
                    continue
                seen_ids.add(author.id)
                SourceReferenceAuthor.objects.create(
                    source_reference=self.object,
                    author=author,
                    order=order,
                )
                order += 1

    def update_database_reference(self, post_dict):
        title = post_dict.get('title', '')
        source_name = post_dict.get('source_name', '')
        self.object.source_name = source_name
        self.object.source.name = title
        self.object.source.save()
        author_objects = self._collect_authors_from_post(post_dict)
        self.set_ordered_authors(author_objects)
        source_date = post_dict.get('source_date', '').strip() or None
        if source_date is not None:
            self.object.source_date = source_date or None

    def update_unpublished(self, post_dict):
        title = post_dict.get('title', '')
        source_name = post_dict.get('source_name', '')
        self.object.note = title
        author_objects = self._collect_authors_from_post(post_dict)
        self.set_ordered_authors(author_objects)
        source_date = post_dict.get('source_date', '').strip() or None
        if source_date is not None:
            self.object.source_date = source_date or None
        self.object.save()
        source_references = SourceReference.objects.filter(
            note=title.strip(),
            source_name=source_name.strip()
        ).exclude(
            Q(instance_of=SourceReferenceDatabase) |
            Q(instance_of=SourceReferenceBibliography) |
            Q(instance_of=SourceReferenceDocument)
        )
        if source_references.count() > 1:
            source_reference = source_references[0]
            BiologicalCollectionRecord.objects.filter(
                source_reference__in=source_references
            ).update(source_reference=source_reference)
            ChemicalRecord.objects.filter(
                source_reference__in=source_references
            ).update(source_reference=source_reference)
            source_references.exclude(id=source_reference.id).delete()

    def form_valid(self, form):
        post_dict = self.request.POST.dict()
        if self.object.is_published_report():
            self.update_published_report_reference(
                post_dict
            )
        elif self.object.is_database():
            self.update_database_reference(
                post_dict
            )
        elif self.object.is_bibliography():
            self.update_bibliography_reference(
                post_dict
            )
        else:
            self.update_unpublished(
                post_dict
            )

        metadata_file = self.request.FILES.get('metadata_file')
        remove_metadata = self.request.POST.get('remove_metadata_file')
        if remove_metadata:
            self.object.metadata_file.delete(save=False)
            self.object.metadata_file = None
        if metadata_file:
            if self.object.metadata_file:
                self.object.metadata_file.delete(save=False)
            self.object.metadata_file = metadata_file
        self.object.save()

        return super(EditSourceReferenceView, self).form_valid(
            form
        )

    def get_context_data(self, **kwargs):
        context = super(
            EditSourceReferenceView, self).get_context_data(**kwargs)
        context['past_url'] = self.request.GET.get('next')
        if self.object.is_published_report():
            try:
                context['source_name'] = json.loads(
                    self.object.source.supplemental_information
                )['document_source']
            except:  # noqa
                pass
        return context


class AddSourceReferenceView(LoginRequiredMixin, CreateView):
    template_name = 'source_references/add_source_reference.html'
    model = SourceReference
    fields = '__all__'
    object = None

    def get_success_url(self):
        if self.request.GET.get('next'):
            source_reference_type = 'unpublished'
            if self.object.is_database():
                source_reference_type = 'database'
            elif self.object.is_bibliography():
                source_reference_type = 'bibliography'
            elif self.object.is_published_report():
                source_reference_type = 'document'
            url_params = ({
                'sr': self.object.title,
                'srt': source_reference_type
            })
            next_url = self.request.GET.get('next')
            if '?' not in next_url:
                next_url += '?'
            else:
                next_url += '&'
            next_url += urlencode(url_params)
            return next_url
        return '/source-references/'

    def handle_peer_reviewed(self, post_data):
        if (
                SourceReferenceBibliography.objects.filter(
                    source__doi=post_data.get('doi')).exists()
        ):
            messages.add_message(
                self.request,
                messages.ERROR,
                'Entry already exists',
                extra_tags='source-reference'
            )
            return False

        if post_data.get('doi'):
            try:
                entry = Entry.objects.get(doi=post_data.get('doi'))
                source_reference, created = (
                    SourceReferenceBibliography.objects.get_or_create(
                        source=entry,
                        note=post_data.get('notes', '')
                    )
                )
                self.object = source_reference
            except Entry.DoesNotExist:
                return False
            except SourceReferenceBibliography.MultipleObjectsReturned:
                return False
        return True


    def handle_database_record(self, post_data):
        if not self.request.user.is_superuser or not self.request.user.is_staff:
            return False
        if (
                DatabaseRecord.objects.filter(
                    name__iexact=post_data.get('name')).exists()
        ):
            messages.add_message(
                self.request,
                messages.ERROR,
                'Database already exists',
                extra_tags='source-reference'
            )
            return False
        else:
            database_record = DatabaseRecord.objects.create(
                name=post_data.get('name'),
                description=post_data.get('description', ''),
                url=post_data.get('url', '')
            )
            source_reference, created = (
                SourceReferenceDatabase.objects.get_or_create(
                    source=database_record
                )
            )
            self.object = source_reference
            self._save_source_reference_authors(source_reference, post_data)
            source_date = post_data.get('source_date', '').strip() or None
            source_name = post_data.get('source', '').strip() or None
            if source_date or source_name:
                if source_date:
                    source_reference.source_date = source_date
                if source_name:
                    source_reference.source_name = source_name
                source_reference.save()
        return True

    def get_user_from_string(self, user_string):
        """Return user object from string of user name"""
        first_name = user_string.split(' ')[0]
        last_name = ' '.join(user_string.split(' ')[1:])
        user = get_user_from_name(
            first_name=first_name,
            last_name=last_name
        )
        return user

    def handle_published_report(self, post_data, file_data):
        if (
            SourceReferenceDocument.objects.filter(
                source__title__iexact=post_data.get('name')
            ).exists()
        ):
            messages.add_message(
                self.request,
                messages.ERROR,
                'Published report already exists',
                extra_tags='source_reference'
            )
            return False

        document, _ = Document.objects.get_or_create(
            owner=self.request.user,
            is_published=True,
            abstract=post_data.get('description', ''),
            title=post_data.get('title', ''),
            doc_type=post_data.get('doc_type', None),
            doc_file=file_data.get('report_file', None),
            doc_url=post_data.get('report_url', None),
            supplemental_information=json.dumps({
                'document_source': post_data.get('source')
            })
        )

        # tag keyword of document as Bims Source Reference
        keyword = None
        try:
            keyword = HierarchicalKeyword.objects.get(
                slug='bims_source_reference')
        except HierarchicalKeyword.DoesNotExist:
            try:
                last_keyword = HierarchicalKeyword.objects.filter(
                    depth=1).order_by('path').last()
                if not last_keyword:
                    path = '0000'
                else:
                    path = last_keyword.path
                path = "{:04d}".format(int(path) + 1)
                keyword, created = HierarchicalKeyword.objects.get_or_create(
                    slug='bims_source_reference',
                    name='Bims Source Reference',
                    depth=1,
                    path=path)
            except Exception:  # noqa
                pass
        if keyword:
            TaggedContentItem.objects.get_or_create(
                content_object=document, tag=keyword)

        bims_document = BimsDocument.objects.create(
            document=document,
            year=post_data.get('year', None),
        )

        # Update authors
        try:
            author_ids = post_data.get('author_ids', None)
            author_names = post_data.get('author_names', None)
            if author_ids or author_names:
                bims_document.authors.clear()
            if author_ids:
                author_ids = author_ids.split(',')
                for author_id in author_ids:
                    bims_document.authors.add(author_id)
            if author_names:
                author_names = author_names.split(',')
                for author_name in author_names:
                    author_user = self.get_user_from_string(
                        author_name.strip()
                    )
                    bims_document.authors.add(
                        author_user.id
                    )
        except KeyError:
            pass

        source_reference, created = (
            SourceReferenceDocument.objects.get_or_create(
                source=document
            )
        )
        self.object = source_reference

        return True

    def _save_source_reference_authors(self, source_reference, post_data):
        """Save authors to an unpublished source reference from author_ids and author_names."""
        author_ids = post_data.get('author_ids', '').strip()
        author_names = post_data.get('author_names', '').strip()

        if not author_ids and not author_names:
            return

        author_objects = []

        if author_ids:
            for user_id in author_ids.split(','):
                user_id = user_id.strip()
                if not user_id:
                    continue
                try:
                    user = get_user_model().objects.get(id=user_id)
                    try:
                        author = Author.objects.get(
                            user=user,
                            first_name=user.first_name,
                            last_name=user.last_name
                        )
                    except Author.DoesNotExist:
                        first_initial = user.first_name[0] if user.first_name else ''
                        author, _ = Author.objects.get_or_create(
                            first_name=user.first_name,
                            last_name=user.last_name,
                            defaults={'user': user, 'first_initial': first_initial}
                        )
                    except Author.MultipleObjectsReturned:
                        author = Author.objects.filter(user=user).first()
                    if author:
                        author_objects.append(author)
                except get_user_model().DoesNotExist:
                    continue

        if author_names:
            for name in author_names.split(','):
                name = name.strip()
                if not name:
                    continue
                user = self.get_user_from_string(name)
                if user:
                    try:
                        author = Author.objects.get(
                            user=user,
                            first_name=user.first_name,
                            last_name=user.last_name
                        )
                    except Author.DoesNotExist:
                        first_initial = user.first_name[0] if user.first_name else ''
                        author, _ = Author.objects.get_or_create(
                            first_name=user.first_name,
                            last_name=user.last_name,
                            defaults={'user': user, 'first_initial': first_initial}
                        )
                    except Author.MultipleObjectsReturned:
                        author = Author.objects.filter(user=user).first()
                else:
                    parts = name.split(' ', 1)
                    first_name = parts[0]
                    last_name = parts[1] if len(parts) > 1 else ''
                    first_initial = first_name[0] if first_name else ''
                    author, _ = Author.objects.get_or_create(
                        first_name=first_name,
                        last_name=last_name,
                        defaults={'first_initial': first_initial}
                    )
                if author:
                    author_objects.append(author)

        SourceReferenceAuthor.objects.filter(source_reference=source_reference).delete()
        seen_ids = set()
        for order, author in enumerate(author_objects):
            if author.id in seen_ids:
                continue
            seen_ids.add(author.id)
            SourceReferenceAuthor.objects.create(
                source_reference=source_reference,
                author=author,
                order=order,
            )

    def get_context_data(self, **kwargs):
        context = super(
            AddSourceReferenceView, self).get_context_data(**kwargs)
        if self.request.user.is_superuser or self.request.user.is_staff:
            reference_type = [
                'Unpublished',
                'Database',
                PUBLISHED_REPORT,
                'Peer-reviewed scientific article'
            ]
        else:
            reference_type = [
                PUBLISHED_REPORT
            ]
        context['params'] = {
            'reference_type': json.dumps(reference_type)
        }.items()
        encoded_next_url = self.request.GET.get('next', '')
        next_url = ''
        if encoded_next_url:
            next_url = unquote(encoded_next_url)
        context['past_url'] = next_url
        return context

    def form_valid(self, form):
        post_dict = self.request.POST.dict()
        file_dict = self.request.FILES.dict()
        reference_type = self.request.POST.get('reference_type')
        processed = False
        if 'database' in reference_type.lower():
            processed = self.handle_database_record(post_data=post_dict)
        elif (
                'published book' in reference_type.lower() or
                'published report' in reference_type.lower()):
            processed = self.handle_published_report(
                post_data=post_dict,
                file_data=file_dict
            )
        elif 'peer-reviewed' in reference_type.lower():
            processed = self.handle_peer_reviewed(
                post_data=post_dict
            )
        else: # Unpublished
            if self.request.user.is_superuser or self.request.user.is_staff:
                source_reference, created = SourceReference.objects.get_or_create(
                    note=post_dict.get('notes', ''),
                    source_name=post_dict.get('source', '')
                )
                self.object = source_reference
                self._save_source_reference_authors(source_reference, post_dict)
                source_date = post_dict.get('source_date', '').strip() or None
                if source_date:
                    source_reference.source_date = source_date
                    source_reference.save()
                processed = True

        if not processed:
            return self.form_invalid(form)

        return HttpResponseRedirect(self.get_success_url())
