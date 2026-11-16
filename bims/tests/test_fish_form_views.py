# coding=utf-8
"""Tests for models."""

from django.test import TestCase, Client
from allauth.utils import get_user_model
from django_tenants.test.cases import FastTenantTestCase
from django_tenants.test.client import TenantClient

from bims.models.survey import Survey
from bims.tests.model_factories import (
    LocationSiteF,
    UserF,
)


class TestFishFormView(TestCase):
    """ Tests Fish Form View.
    """

    def setUp(self):
        """
        Sets up before each test
        """
        self.client = Client()
        self.location_site = LocationSiteF.create()

    def test_get_fish_form(self):
        """
        Test fish form get method
        """
        # Login
        user = get_user_model().objects.create(
            is_staff=True,
            is_active=True,
            is_superuser=True,
            username='@.test')
        user.set_password('psst')
        user.save()
        resp = self.client.login(
            username='@.test',
            password='psst'
        )
        self.assertTrue(resp)

        response = self.client.get(
            '/fish-form/?siteId={}'.format(
                self.location_site.id
            )
        )
        self.assertIsNotNone(response.context)


class TestCollectionFormSourceReferenceRequired(FastTenantTestCase):

    def setUp(self):
        self.client = TenantClient(self.tenant)
        self.location_site = LocationSiteF.create()
        self.user = UserF.create()
        self.client.login(username=self.user.username, password='password')

    def test_post_without_source_reference_is_rejected(self):
        for source_reference in (None, '', 'abc', '999999'):
            data = {
                'date': '2022-02-02',
                'owner_id': self.user.id,
                'site-id': self.location_site.id,
            }
            if source_reference is not None:
                data['source_reference'] = source_reference
            response = self.client.post(
                '/fish-form/?siteId={}'.format(self.location_site.id), data)
            self.assertEqual(response.status_code, 400)
        self.assertFalse(
            Survey.objects.filter(site=self.location_site).exists())
