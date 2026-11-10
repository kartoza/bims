from django.contrib.auth import get_user_model
from django.db import connection
from django.test import TestCase
from django_tenants.test.cases import FastTenantTestCase
from django_tenants.test.client import TenantClient
from django.urls import reverse
from bims.tests.model_factories import (
    UserF, BiologicalCollectionRecordF
)
from bims.utils.user import merge_users, get_user
from bims.models import BiologicalCollectionRecord


class TestUser(TestCase):
    """Test all user functions"""

    def setUp(self):
        pass

    def test_merge_users(self):
        """Test a function to merge multiple users"""
        user_1 = UserF.create()
        user_2 = UserF.create()
        BiologicalCollectionRecordF.create(
            owner=user_1,
            collector_user=user_2
        )
        BiologicalCollectionRecordF.create(
            owner=user_2,
            collector_user=user_1
        )
        self.assertTrue(
            BiologicalCollectionRecord.objects.filter(
                owner=user_2
            ).exists()
        )
        merge_users(
            primary_user=user_1,
            user_list=[user_1.id, user_2.id]
        )
        self.assertTrue(
            BiologicalCollectionRecord.objects.filter(
                owner=user_1
            ).count(),
            2
        )
        self.assertTrue(
            BiologicalCollectionRecord.objects.filter(
                collector_user=user_1
            ).count(),
            2
        )
        self.assertFalse(
            BiologicalCollectionRecord.objects.filter(
                owner=user_2
            ).exists()
        )

    def _create_unmapped_table(self):
        """A table that references the user table without a Django model,
        like oauth2_provider_accesstoken when its app is not installed."""
        user_table = get_user_model()._meta.db_table
        with connection.cursor() as cursor:
            cursor.execute(
                'CREATE TABLE merge_test_token ('
                'id serial PRIMARY KEY, '
                'user_id integer REFERENCES "%s" (id), '
                'token varchar(20))' % user_table)
        self.addCleanup(self._drop_unmapped_table)

    @staticmethod
    def _drop_unmapped_table():
        with connection.cursor() as cursor:
            cursor.execute('DROP TABLE IF EXISTS merge_test_token')

    def test_merge_users_moves_unmapped_references(self):
        """Merging must not fail on tables Django does not know about."""
        self._create_unmapped_table()
        user_1 = UserF.create()
        user_2 = UserF.create()
        with connection.cursor() as cursor:
            cursor.execute(
                'INSERT INTO merge_test_token (user_id, token) '
                'VALUES (%s, %s)', [user_2.id, 'abc'])
        result = merge_users(
            primary_user=user_1, user_list=[user_1.id, user_2.id])
        self.assertEqual(result, {'merged': 1, 'failed': []})
        self.assertFalse(
            get_user_model().objects.filter(id=user_2.id).exists())
        with connection.cursor() as cursor:
            cursor.execute(
                'SELECT user_id FROM merge_test_token WHERE token = %s',
                ['abc'])
            self.assertEqual(cursor.fetchone()[0], user_1.id)

    def test_merge_users_keeps_inactive_primary(self):
        """The user to keep does not have to be the active one."""
        user_1 = UserF.create(is_active=False)
        user_2 = UserF.create(is_active=True)
        BiologicalCollectionRecordF.create(owner=user_2)
        result = merge_users(
            primary_user=user_1, user_list=[user_1.id, user_2.id])
        self.assertEqual(result['merged'], 1)
        self.assertEqual(
            BiologicalCollectionRecord.objects.filter(owner=user_1).count(),
            1)

    def test_get_user_from_username(self):
        username_1 = 'Dimas Ciputra'
        user = get_user(username_1)
        username_2 = 'Dimas T Ciputra'
        user_2 = get_user(username_2)
        self.assertEqual(user, user_2)

        username_3 = 'D_Ciputra'
        user_3 = get_user(username_3)
        username_4 = 'Dim_Ciputra'
        user_4 = get_user(username_4)
        self.assertNotEqual(user_3, user_4)


class TestMergeUsersAdmin(FastTenantTestCase):
    """The admin page used to choose the user to keep."""

    def test_merge_users_admin_page(self):
        """The admin action shows the users, then merges the chosen one."""
        admin = UserF.create(is_staff=True, is_superuser=True)
        user_1 = UserF.create()
        user_2 = UserF.create()
        client = TenantClient(self.tenant)
        client.force_login(admin)
        url = reverse('admin:merge_users')
        ids = '{},{}'.format(user_1.id, user_2.id)

        response = client.get(url, {'ids': ids})
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, user_1.username)
        self.assertContains(response, user_2.username)

        client.post(url, {'ids': ids, 'primary_user': user_1.id})
        User = get_user_model()
        self.assertTrue(User.objects.filter(id=user_1.id).exists())
        self.assertFalse(User.objects.filter(id=user_2.id).exists())
