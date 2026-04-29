# noinspection PyUnresolvedReferences,PyPackageRequirements
import datetime
import factory
import random

from django.contrib.sites.models import Site

from bims.models.sampling_method import SamplingMethod

from bims.models.chem import Chem, Unit

from bims.models.source_reference import SourceReferenceDocument

from bims.models.upload_session import UploadSession
from django.conf import settings
from django.contrib.gis.geos import Point, GEOSGeometry
from django.utils import timezone
from django.db.models import signals
from django.contrib.auth.models import Permission, Group
from django.contrib.contenttypes.models import ContentType
from geonode.documents.models import Document
from bims.factories import EntryFactory
from bims.models import (
    LocationType,
    LocationSite,
    Profile,
    IUCNStatus,
    Survey,
    BiologicalCollectionRecord,
    Boundary,
    BoundaryType,
    Cluster,
    Endemism,
    Taxonomy,
    TaxonGroup,
    FbisUUID,
    DataSource,
    Biotope,
    SourceReference,
    SourceReferenceBibliography,
    SourceReferenceDatabase,
    DatabaseRecord,
    LocationContext,
    LocationContextGroup,
    TaxonImage,
    VernacularName,
    ChemicalRecord,
    SiteImage,
    WaterTemperature,
    WaterTemperatureThreshold,
    UserBoundary,
    HarvestSession,
    TaxonomyUpdateProposal,
    TaxonGroupTaxonomy,
    RecordType,
    Dataset
)
from sass.models import River
from cloud_native_gis.models.layer import Layer


class LocationTypeF(factory.django.DjangoModelFactory):
    """
    Location type factory
    """

    class Meta:
        model = LocationType

    name = factory.Sequence(lambda n: 'Test location type %s' % n)
    description = u'Only for testing'
    allowed_geometry = 'POINT'


@factory.django.mute_signals(signals.post_save)
class LocationSiteF(factory.django.DjangoModelFactory):
    """
    Location site factory
    """

    class Meta:
        model = LocationSite

    name = factory.Sequence(lambda n: 'Site name %s' % n)
    location_type = factory.SubFactory(LocationTypeF)
    geometry_point = Point(
        random.uniform(-30.0, 30.0),
        random.uniform(-30.0, 30.0)
    )


class LocationContextGroupF(factory.django.DjangoModelFactory):
    """
    Location context group factory
    """
    class Meta:
        model = LocationContextGroup

    name = factory.Sequence(lambda n: 'name %s' % n)
    key = factory.Sequence(lambda n: 'key %s' % n)
    geocontext_group_key = factory.Sequence(lambda n: 'group_key %s' % n)
    layer_name = factory.Sequence(lambda n: 'layer_name %s' % n)


class LocationContextF(factory.django.DjangoModelFactory):
    """
    Location context factory
    """
    class Meta:
        model = LocationContext

    site = factory.SubFactory(LocationSiteF)
    group = factory.SubFactory(LocationContextGroupF)
    value = factory.Sequence(lambda n: 'value %s' % n)


class IUCNStatusF(factory.django.DjangoModelFactory):
    """
    Iucn status factory
    """
    class Meta:
        model = IUCNStatus

    category = factory.Sequence(lambda n: u'Test name %s' % n)
    sensitive = False


@factory.django.mute_signals(signals.post_save)
class SurveyF(factory.django.DjangoModelFactory):
    """
    Survey factory
    """
    class Meta:
        model = Survey

    date = timezone.now()
    site = factory.SubFactory(LocationSiteF)


class EndemismF(factory.django.DjangoModelFactory):
    """
    Endemism factory
    """
    class Meta:
        model = Endemism

    id = factory.Sequence(lambda n: n)
    name = factory.Sequence(lambda n: 'name %s' % n)
    description = factory.Sequence(
        lambda n: 'description %s' % n
    )


class GroupF(factory.django.DjangoModelFactory):
    class Meta:
        model = Group

    name = factory.Sequence(lambda n: 'group %s' % n)


class ContentTypeF(factory.django.DjangoModelFactory):
    class Meta:
        model = ContentType

    app_label = factory.Sequence(lambda n: 'app %s' % n)
    model = factory.Sequence(lambda n: 'model %s' % n)


class PermissionF(factory.django.DjangoModelFactory):
    class Meta:
        model = Permission

    name = factory.Sequence(lambda n: 'permission %s' % n)
    content_type = factory.SubFactory(ContentTypeF)


class UserF(factory.django.DjangoModelFactory):
    class Meta:
        model = settings.AUTH_USER_MODEL
        django_get_or_create = ('id', )

    id = factory.Sequence(lambda n: n)
    username = factory.Sequence(lambda n: "username%s" % n)
    first_name = factory.Sequence(lambda n: "first_name%s" % n)
    last_name = factory.Sequence(lambda n: "last_name%s" % n)
    email = factory.Sequence(lambda n: "email%s@example.com" % n)
    password = factory.PostGenerationMethodCall('set_password', 'password')
    is_staff = False
    is_active = True
    is_superuser = False
    last_login = timezone.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)
    date_joined = timezone.datetime(1999, 1, 1, tzinfo=datetime.timezone.utc)

    @classmethod
    def _prepare(cls, create, **kwargs):
        password = kwargs.pop('password', None)
        user = super(UserF, cls)._prepare(create, **kwargs)
        if password:
            user.set_password(password)
            if create:
                user.save()
        return user


class ProfileF(factory.django.DjangoModelFactory):
    """
    Profile site factory
    """

    class Meta:
        model = Profile

    user = factory.SubFactory(UserF)
    qualifications = factory.Sequence(lambda n: "qualifications%s" % n)
    other = factory.Sequence(lambda n: "other%s" % n)


class UploadSessionF(factory.django.DjangoModelFactory):
    """
    Upload session factory
    """
    class Meta:
        model = UploadSession
    uploader = factory.SubFactory(UserF)


class TaxonomyF(factory.django.DjangoModelFactory):
    """
    Taxon identifier factory
    """
    class Meta:
        model = Taxonomy
        django_get_or_create = ('id',)

    id = factory.Sequence(lambda n: n + 1)
    iucn_status = factory.SubFactory(IUCNStatusF)
    scientific_name = factory.Sequence(lambda n: u'Scientific name %s' % n)
    canonical_name = factory.Sequence(lambda n: u'Canonical name %s' % n)

    @factory.post_generation
    def vernacular_names(self, create, extracted, **kwargs):
        if not create:
            # Simple build, do nothing.
            return

        if extracted:
            # A list of groups were passed in, use them
            for vernacular_name in extracted:
                self.vernacular_names.add(vernacular_name)


class VernacularNameF(factory.django.DjangoModelFactory):
    """
    Vernacular name identifier factory
    """
    class Meta:
        model = VernacularName

    name = factory.Sequence(lambda n: n)
    source = factory.Sequence(lambda n: u'source name %s' % n)


class TaxonGroupTaxonomyF(factory.django.DjangoModelFactory):

    class Meta:
        model = TaxonGroupTaxonomy


class TaxonGroupF(factory.django.DjangoModelFactory):
    """
    Taxon group factory
    """
    class Meta:
        model = TaxonGroup

    id = factory.Sequence(lambda n: n)
    name = factory.Sequence(lambda n: u'Name %s' % n)
    site = Site.objects.get_current()

    @factory.post_generation
    def experts(self, create, extracted, **kwargs):
        if not create:
            # Simple build, do nothing.
            return

        if extracted:
            # A list of groups were passed in, use them
            for expert in extracted:
                self.experts.add(expert)

    @factory.post_generation
    def taxonomies(self, create, extracted, **kwargs):
        if not create:
            # Simple build, do nothing.
            return

        if extracted:
            # A list of groups were passed in, use them
            for taxonomy in extracted:
                TaxonGroupTaxonomyF.create(
                    taxonomy=taxonomy,
                    taxongroup=self
                )


class TaxonomyUpdateProposalF(factory.django.DjangoModelFactory):
    """
    Taxonomy update proposal
    """
    class Meta:
        model = TaxonomyUpdateProposal
        django_get_or_create = ('id',)

    id = factory.Sequence(lambda n: n + 1)
    iucn_status = factory.SubFactory(IUCNStatusF)
    scientific_name = factory.Sequence(lambda n: u'Scientific name %s' % n)
    canonical_name = factory.Sequence(lambda n: u'Canonical name %s' % n)
    taxon_group = factory.SubFactory(TaxonGroupF)
    original_taxonomy = factory.SubFactory(TaxonomyF)

    @factory.post_generation
    def vernacular_names(self, create, extracted, **kwargs):
        if not create:
            # Simple build, do nothing.
            return

        if extracted:
            # A list of groups were passed in, use them
            for vernacular_name in extracted:
                self.vernacular_names.add(vernacular_name)


@factory.django.mute_signals(signals.post_save)
class BiologicalCollectionRecordF(factory.django.DjangoModelFactory):
    """
    Biological collection record factory
    """
    class Meta:
        model = BiologicalCollectionRecord

    site = factory.SubFactory(LocationSiteF)
    original_species_name = factory.Sequence(
            lambda n: u'Test original species name %s' % n)
    present = True
    collection_date = timezone.now()
    collector = factory.Sequence(
            lambda n: u'Test collector %s' % n)
    owner = factory.SubFactory(UserF)
    notes = factory.Sequence(
            lambda n: u'Test notes %s' % n)
    taxonomy = factory.SubFactory(TaxonomyF)
    survey = factory.SubFactory(SurveyF)
    validated = True


class UnitF(factory.django.DjangoModelFactory):
    class Meta:
        model = Unit

    unit = factory.Sequence(lambda n: u'unit %s' % n)
    unit_name = factory.Sequence(lambda n: u'unit name %s' % n)


class ChemF(factory.django.DjangoModelFactory):
    class Meta:
        model = Chem

    chem_code = factory.Sequence(
        lambda n: u'code %s' % n)
    chem_unit = factory.SubFactory(UnitF)


class BoundaryTypeF(factory.django.DjangoModelFactory):
    """
    Boundary type factory
    """
    class Meta:
        model = BoundaryType

    name = factory.Sequence(lambda n: u'Test name %s' % n)


class BoundaryF(factory.django.DjangoModelFactory):
    """
    Boundary factory
    """
    class Meta:
        model = Boundary

    name = factory.Sequence(lambda n: u'Test name %s' % n)
    type = factory.SubFactory(BoundaryTypeF)


class ClusterF(factory.django.DjangoModelFactory):
    """
    Cluster factory
    """
    class Meta:
        model = Cluster

    boundary = factory.SubFactory(BoundaryF)
    module = factory.Sequence(lambda n: u'Test module %s' % n)
    site_count = 1
    details = ''


class FbisUUIDF(factory.django.DjangoModelFactory):
    """
    FbisUUID factory
    """
    class Meta:
        model = FbisUUID

    id = factory.Sequence(lambda n: n)
    uuid = factory.Sequence(lambda n: 'uuid %s' % n)
    content_type = factory.SubFactory(ContentTypeF)


class DataSourceF(factory.django.DjangoModelFactory):
    """
    Data source factory
    """
    class Meta:
        model = DataSource

    name = factory.Sequence(lambda n: 'data-source %s' % n)


class BiotopeF(factory.django.DjangoModelFactory):
    class Meta:
        model = Biotope

    @factory.post_generation
    def taxon_group(self, create, extracted, **kwargs):
        if not create:
            # Simple build, do nothing.
            return

        if extracted:
            # A list of groups were passed in, use them
            for taxon_group in extracted:
                self.taxon_group.add(taxon_group)


class SourceReferenceF(factory.django.DjangoModelFactory):
    class Meta:
        model = SourceReference


class RecordTypeF(factory.django.DjangoModelFactory):
    class Meta:
        model = RecordType

    name = factory.Sequence(lambda n: 'record-type %s' % n)


class SourceReferenceBibliographyF(factory.django.DjangoModelFactory):
    class Meta:
        model = SourceReferenceBibliography

    source = factory.SubFactory(EntryFactory)

    @factory.post_generation
    def active_sites(self, create, extracted, **kwargs):
        if not create:
            return

        if extracted:
            for active_site in extracted:
                self.active_sites.add(active_site)


class DatabaseRecordF(factory.django.DjangoModelFactory):
    class Meta:
        model = DatabaseRecord


class SourceReferenceDatabaseF(factory.django.DjangoModelFactory):
    class Meta:
        model = SourceReferenceDatabase

    source = factory.SubFactory(DatabaseRecordF)


class DocumentF(factory.django.DjangoModelFactory):
    class Meta:
        model = Document

    title = factory.Sequence(lambda n: u'Document title %s' % n)


class SourceReferenceDocumentF(factory.django.DjangoModelFactory):
    class Meta:
        model = SourceReferenceDocument

    source = factory.SubFactory(DocumentF)

    @factory.post_generation
    def active_sites(self, create, extracted, **kwargs):
        if not create:
            return

        if extracted:
            for active_site in extracted:
                self.active_sites.add(active_site)


class TaxonImageF(factory.django.DjangoModelFactory):
    class Meta:
        model = TaxonImage
    taxonomy = factory.SubFactory(TaxonomyF)


class SiteImageF(factory.django.DjangoModelFactory):
    class Meta:
        model = SiteImage


class WaterTemperatureF(factory.django.DjangoModelFactory):
    class Meta:
        model = WaterTemperature


class WaterTemperatureThresholdF(factory.django.DjangoModelFactory):
    class Meta:
        model = WaterTemperatureThreshold


class ChemicalRecordF(factory.django.DjangoModelFactory):
    class Meta:
        model = ChemicalRecord

    survey = factory.SubFactory(SurveyF)
    chem = factory.SubFactory(ChemF)


class SamplingMethodF(factory.django.DjangoModelFactory):
    class Meta:
        model = SamplingMethod

    sampling_method = factory.Sequence(lambda n: u'method %s' % n)


class RiverF(factory.django.DjangoModelFactory):
    class Meta:
        model = River

    name = factory.Sequence(lambda n: u'river %s' % n)


class UserBoundaryF(factory.django.DjangoModelFactory):
    """
    User boundary factory
    """
    class Meta:
        model = UserBoundary

    name = factory.Sequence(lambda n: u'name %s' % n)
    geometry = GEOSGeometry('MULTIPOLYGON(((0 0, 4 0, 4 4, 0 4, 0 0), (10 10, 14 10, 14 14, 10 14, 10 10)))')


class HarvestSessionF(factory.django.DjangoModelFactory):
    """
    Harvest session factory
    """
    class Meta:
        model = HarvestSession

    module_group = factory.SubFactory(TaxonGroupF)


class SiteF(factory.django.DjangoModelFactory):
    """
    Django Site model factory
    """
    class Meta:
        model = Site

    id = factory.Sequence(lambda n: n + 1000)
    name = factory.Sequence(lambda n: u'name %s' % n)
    domain = factory.Sequence(lambda n: u'domain %s' % n)


class DatasetF(factory.django.DjangoModelFactory):
    """
    Dataset model factory
    """
    class Meta:
        model = Dataset

    name = factory.Sequence(lambda n: u'name %s' % n)
    abbreviation = factory.Sequence(lambda n: u'abbreviation %s' % n)


class LayerF(factory.django.DjangoModelFactory):
    class Meta:
        model = Layer

    name = factory.Sequence(lambda n: u'name %s' % n)
