from dateutil.parser import parse
from django.test import TestCase
from django.urls import reverse
from django_tenants.test.cases import FastTenantTestCase
from django_tenants.test.client import TenantClient

from bims.models.survey import Survey
from bims.tests.model_factories import (
    UserF,
    LocationSiteF,
    SourceReferenceF,
)
from sass.tests.model_factories import (
    SiteVisitF,
    SiteVisitTaxonF,
    SassTaxonF,
    TaxonAbundanceF,
)


class TestSassFormView(FastTenantTestCase):
    def setUp(self):
        self.client = TenantClient(self.tenant)

    def test_update_sass(self):
        user = UserF.create()
        self.client.login(
            username=user.username,
            password='password'
        )
        site = LocationSiteF.create()
        sass_site_visit = SiteVisitF.create(
            location_site=site,
            owner=user
        )
        date = '2022/02/02'
        response = self.client.post(
            reverse('sass-update-page', kwargs={
                'sass_id': sass_site_visit.id
            }), {
                'owner': user.id,
                'date': date,
                'source_reference': SourceReferenceF.create().id,
            }
        )

        self.assertEqual(response.status_code, 302)
        self.assertTrue(
            Survey.objects.filter(
                owner=user,
                date=parse(date),
                site=site
            ).exists()
        )

    def test_post_without_source_reference_is_rejected(self):
        user = UserF.create()
        self.client.login(username=user.username, password='password')
        site = LocationSiteF.create()
        for source_reference in ('', 'abc', '999999'):
            response = self.client.post(
                reverse('sass-form-page', kwargs={'site_id': site.id}),
                {
                    'owner': user.id,
                    'date': '2022/02/02',
                    'source_reference': source_reference,
                }
            )
            self.assertEqual(response.status_code, 400)
        self.assertFalse(Survey.objects.filter(site=site).exists())


class TestOwnerOrganisationSassForm(FastTenantTestCase):
    def setUp(self):
        self.client = TenantClient(self.tenant)

    def test_new_form_context_prefills_from_user_organization(self):
        """New SASS form pre-fills owner_organisation from the logged-in user's organization."""
        user = UserF.create()
        user.organization = 'Aquatic Institute'
        user.save()
        self.client.login(username=user.username, password='password')
        site = LocationSiteF.create()
        response = self.client.get('/sass/{}/'.format(site.id))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context.get('owner_organisation'), 'Aquatic Institute')

    def test_edit_context_prefills_from_institution_id(self):
        """Edit SASS form pre-fills owner_organisation from SiteVisitTaxon.institution_id."""
        user = UserF.create()
        self.client.login(username=user.username, password='password')
        site = LocationSiteF.create()
        site_visit = SiteVisitF.create(location_site=site, owner=user)
        taxon = SiteVisitTaxonF.create(site_visit=site_visit, site=site)
        taxon.institution_id = 'WaterResearch SA'
        taxon.save()
        response = self.client.get(
            reverse('sass-update-page', kwargs={'sass_id': site_visit.id})
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context.get('owner_organisation'), 'WaterResearch SA')

    def test_edit_context_falls_back_to_owner_org_when_institution_id_is_bims(self):
        """owner_organisation falls back to owner.organization when institution_id is 'bims'."""
        user = UserF.create()
        user.organization = 'River Trust'
        user.save()
        self.client.login(username=user.username, password='password')
        site = LocationSiteF.create()
        site_visit = SiteVisitF.create(location_site=site, owner=user)
        taxon = SiteVisitTaxonF.create(site_visit=site_visit, site=site)
        taxon.institution_id = 'bims'
        taxon.save()
        response = self.client.get(
            reverse('sass-update-page', kwargs={'sass_id': site_visit.id})
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context.get('owner_organisation'), 'River Trust')

    def test_edit_context_falls_back_to_owner_org_when_institution_id_is_healthyrivers(self):
        """owner_organisation falls back to owner.organization when institution_id is 'healthyrivers'."""
        user = UserF.create()
        user.organization = 'Wetland Org'
        user.save()
        self.client.login(username=user.username, password='password')
        site = LocationSiteF.create()
        site_visit = SiteVisitF.create(location_site=site, owner=user)
        taxon = SiteVisitTaxonF.create(site_visit=site_visit, site=site)
        taxon.institution_id = 'healthyrivers'
        taxon.save()
        response = self.client.get(
            reverse('sass-update-page', kwargs={'sass_id': site_visit.id})
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context.get('owner_organisation'), 'Wetland Org')

    def test_read_view_context_has_owner_organisation(self):
        """SASS read/view page exposes owner_organisation in context."""
        user = UserF.create()
        site = LocationSiteF.create()
        site_visit = SiteVisitF.create(location_site=site, owner=user)
        taxon = SiteVisitTaxonF.create(site_visit=site_visit, site=site)
        taxon.institution_id = 'BioMonitor'
        taxon.save()
        response = self.client.get('/sass/view/{}/'.format(site_visit.id))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context.get('owner_organisation'), 'BioMonitor')

    def test_post_saves_owner_organisation_to_institution_id(self):
        """Submitting owner_organisation saves it as institution_id on SiteVisitTaxon."""
        from sass.models import SiteVisitTaxon
        user = UserF.create()
        self.client.login(username=user.username, password='password')
        site = LocationSiteF.create()
        site_visit = SiteVisitF.create(location_site=site, owner=user)
        sass_taxon = SassTaxonF.create()
        abundance = TaxonAbundanceF.create()
        response = self.client.post(
            reverse('sass-update-page', kwargs={'sass_id': site_visit.id}),
            {
                'owner': user.id,
                'date': '2023/06/01',
                'owner_organisation': 'FieldOrg',
                'source_reference': SourceReferenceF.create().id,
                'taxon_list-{}-TOT'.format(sass_taxon.id): abundance.abc,
            }
        )
        self.assertEqual(response.status_code, 302)
        taxon = SiteVisitTaxon.objects.filter(
            site_visit=site_visit,
            sass_taxon=sass_taxon,
        ).first()
        self.assertIsNotNone(taxon)
        self.assertEqual(taxon.institution_id, 'FieldOrg')
