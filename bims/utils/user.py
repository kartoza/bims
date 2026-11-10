import logging

from django.db import IntegrityError, connection, transaction
from django.db.models.fields.reverse_related import ForeignObjectRel, ManyToManyRel, OneToOneRel
from django.contrib.auth import get_user_model
from django.utils.text import slugify
from django.db.models import Q

from geonode.people.models import Profile
from bims.models.profile import Profile as BimsProfile

logger = logging.getLogger(__name__)


def get_user_from_name(first_name, last_name):
    """
    Get or create a User from first name and last name
    :param first_name: first name of the user
    :param last_name: last name of the user
    :return: User object
    """
    if not first_name:
        return None
    User = get_user_model()
    try:
        if last_name.strip():
            user = User.objects.get(
                Q(last_name__iexact=last_name),
                Q(first_name__iexact=first_name) |
                Q(first_name__istartswith=first_name[0])
            )
        else:
            user = User.objects.get(
                Q(first_name__iexact=first_name)
            )
    except (User.DoesNotExist, Profile.DoesNotExist, BimsProfile.DoesNotExist):
        username = slugify('{first_name} {last_name}'.format(
            first_name=first_name,
            last_name=last_name
        )).replace('-', '_')
        user, created = User.objects.get_or_create(
            username=username
        )
    except User.MultipleObjectsReturned:
        user = User.objects.filter(
            Q(last_name__iexact=last_name),
            Q(first_name__iexact=first_name) |
            Q(first_name__istartswith=first_name[0])
        ).order_by('id').first()
    user.last_name = last_name[0:30]
    user.first_name = first_name[0:30]
    user.save()
    return user


def get_user(user_name):
    """
    Get or create User object from username
    :param user_name: string of username
    :return: User object
    """
    user_name = user_name.split(' ')
    if len(user_name) > 1:
        last_name = user_name[len(user_name) - 1]
        first_name = ' '.join(user_name[0:len(user_name) - 1])
    else:
        first_name = user_name[0]
        last_name = ''
    first_name = first_name[0:30]
    last_name = last_name[0:30]
    return get_user_from_name(
        first_name,
        last_name
    )


def get_user_reverse(user_name):
    """
    Get or create User object from username
    :param user_name: string of username
    :return: User object
    """
    user_name = user_name.split(', ')
    if len(user_name) > 1:
        first_name = user_name[len(user_name) - 1]
        last_name = ' '.join(user_name[0:len(user_name) - 1])
    else:
        first_name = user_name[0]
        last_name = ''
    first_name = first_name[0:30]
    last_name = last_name[0:30]
    return get_user_from_name(
        first_name,
        last_name
    )


def create_users_from_string(user_string):
    """
    Create user objects from users string.
    e.g. `Tri, Dimas., Bob, Dylan & Jackson, Michael`
    to : [<User>`Dimas Tri`, <User>`Dylan Bob`, <User>`Michael Jackson`]
    :param user_string: string of User(s)
    :return: List of user object
    """
    list_user = []
    if not user_string:
        return list_user
    and_username = ''
    for user_split_1 in user_string.split(','):
        for user_name in user_split_1.split(' and '):
            if '&' in user_name:
                and_username = user_name
                continue
            user = get_user(user_name.strip())
            if user and user not in list_user:
                list_user.append(user)
    if and_username:
        for user_name in and_username.split('&'):
            user = get_user(user_name.strip())
            if user and user not in list_user:
                list_user.append(user)
    return list_user


def _database_references(model):
    """
    Foreign keys that point to the user table, read from the database.

    Django only knows the relations of the apps that are installed. A
    table such as oauth2_provider_accesstoken can reference the user table
    without its app being installed, and then neither the ORM update nor
    the ORM delete cascade can see it, so deleting the user fails with an
    IntegrityError. Yields (table, column) with both already quoted.
    """
    if connection.vendor != 'postgresql':
        return []
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT c.conrelid::regclass::text, quote_ident(a.attname)
            FROM pg_constraint c
            JOIN pg_attribute a
              ON a.attrelid = c.conrelid AND a.attnum = c.conkey[1]
            WHERE c.contype = 'f'
              AND array_length(c.conkey, 1) = 1
              AND c.confrelid = %s::regclass
            """,
            [connection.ops.quote_name(model._meta.db_table)]
        )
        return cursor.fetchall()


def _move_database_references(model, user_id, primary_id):
    """
    Point the rows of other tables that still reference user_id to
    primary_id. Rows that conflict with an existing row of the primary
    user (a unique constraint) are deleted instead.
    """
    for table, column in _database_references(model):
        try:
            with transaction.atomic():
                with connection.cursor() as cursor:
                    cursor.execute(
                        'UPDATE {t} SET {c} = %s WHERE {c} = %s'.format(
                            t=table, c=column),
                        [primary_id, user_id]
                    )
        except IntegrityError:
            with transaction.atomic():
                with connection.cursor() as cursor:
                    cursor.execute(
                        'DELETE FROM {t} WHERE {c} = %s'.format(
                            t=table, c=column),
                        [user_id]
                    )


def merge_users(primary_user, user_list):
    """
    Merge multiple users into one primary_user.
    Handles both FK and M2M reverse relations, and the foreign keys of
    tables that Django does not know about (see _database_references).
    Each user is merged in its own savepoint, so a user that fails to merge
    is rolled back and kept.

    :return: dict with the number of users merged and the usernames of
        the users that could not be merged
    """
    result = {'merged': 0, 'failed': []}
    if not primary_user or not user_list:
        return result

    User = get_user_model()
    users = User.objects.filter(
        id__in=user_list
    ).exclude(id=primary_user.id)

    relations = [
        rel for rel in primary_user._meta.get_fields()
        if issubclass(type(rel), ForeignObjectRel)
        and not isinstance(rel, OneToOneRel)
    ]

    for user in users:
        username = str(user)
        try:
            with transaction.atomic():
                _merge_user(User, primary_user, user, relations)
        except Exception as e:  # noqa
            logger.error('Failed to merge user %s: %s', username, e)
            result['failed'].append(username)
            continue
        result['merged'] += 1
    return result


def _merge_user(User, primary_user, user, relations):
    """Move everything of user to primary_user, then delete user."""
    for rel in relations:
        link = rel.get_accessor_name()
        if isinstance(rel, ManyToManyRel):
            if not rel.through._meta.auto_created:
                continue
            related_objects = getattr(user, link).all()
            if related_objects.exists():
                getattr(primary_user, link).add(*related_objects)
            continue

        manager = getattr(user, link)
        objects = manager.all()
        if not objects.exists():
            continue
        field_name = manager.field.name
        try:
            with transaction.atomic():
                objects.update(**{field_name: primary_user})
        except IntegrityError:
            # Bulk update failed due to a unique constraint;
            # re-assign row-by-row and delete any that conflict.
            for obj in list(objects):
                try:
                    with transaction.atomic():
                        type(obj).objects.filter(pk=obj.pk).update(
                            **{field_name: primary_user}
                        )
                except IntegrityError:
                    obj.delete()

    _move_database_references(User, user.pk, primary_user.pk)
    user.delete()
