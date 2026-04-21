from django.db import IntegrityError
from django.db.models.fields.reverse_related import ForeignObjectRel, ManyToManyRel, OneToOneRel
from django.contrib.auth import get_user_model
from django.utils.text import slugify
from django.db.models import Q

from geonode.people.models import Profile
from bims.models.profile import Profile as BimsProfile


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


def merge_users(primary_user, user_list):
    """
    Merge multiple users into one primary_user.
    Handles both FK and M2M reverse relations.
    Users that fail to merge are skipped from deletion.
    """
    if not primary_user or not user_list:
        return

    User = get_user_model()
    users = User.objects.filter(
        id__in=user_list
    ).exclude(id=primary_user.id)

    print('Merging %s users into %s' % (users.count(), str(primary_user)))

    relations = [
        rel for rel in primary_user._meta.get_fields()
        if issubclass(type(rel), ForeignObjectRel)
        and not isinstance(rel, OneToOneRel)
    ]

    failed_user_ids = set()

    for user in users:
        header = '----- {} -----'.format(str(user))
        print(header)
        for rel in relations:
            link = rel.get_accessor_name()
            try:
                if isinstance(rel, ManyToManyRel):
                    if not rel.through._meta.auto_created:
                        continue
                    related_objects = getattr(user, link).all()
                    if related_objects.exists():
                        print('Merging M2M {obj} from: {user}'.format(
                            obj=str(related_objects.model._meta.label),
                            user=str(user)
                        ))
                        getattr(primary_user, link).add(*related_objects)
                else:
                    manager = getattr(user, link)
                    objects = manager.all()
                    if not objects.exists():
                        continue
                    print('Updating {obj} for: {user}'.format(
                        obj=str(objects.model._meta.label),
                        user=str(user)
                    ))
                    field_name = manager.field.name
                    try:
                        objects.update(**{field_name: primary_user})
                    except IntegrityError:
                        # Bulk update failed due to a unique constraint;
                        # re-assign row-by-row and delete any that conflict.
                        for obj in list(objects):
                            try:
                                type(obj).objects.filter(pk=obj.pk).update(
                                    **{field_name: primary_user}
                                )
                            except IntegrityError:
                                obj.delete()
            except Exception as e:  # noqa
                print('Failed to merge {link}: {e}'.format(link=link, e=e))
                failed_user_ids.add(user.id)
                continue
        print('-' * len(header))

    if failed_user_ids:
        print('Skipping deletion of %s user(s) due to errors: %s' % (
            len(failed_user_ids), failed_user_ids
        ))
    users.exclude(id__in=failed_user_ids).delete()
