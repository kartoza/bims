# coding=utf-8
"""Search process model definition.

"""
import logging
import os
import json
import errno
from datetime import date

from django.contrib.sites.models import Site
from django.db import connection, DatabaseError
from django.db.utils import ProgrammingError
from django.conf import settings
from django.db import models
from django.db.models.signals import pre_delete
from django.dispatch.dispatcher import receiver

CLUSTER_GENERATION = 'cluster_generation'
SEARCH_RESULTS = 'search_results'
SITES_SUMMARY = 'sites_summary'
SPATIAL_DASHBOARD_MODULE_TOTALS = 'spatial_dashboard_module_totals'
SPATIAL_DASHBOARD_CONS_STATUS = 'spatial_dashboard_cons_status'
SPATIAL_DASHBOARD_RLI = 'spatial_dashboard_rli'
SPATIAL_DASHBOARD_MAP = 'spatial_dashboard_map'
SPATIAL_DASHBOARD_SUMMARY = 'spatial_dashboard_summary'
SPATIAL_DASHBOARD_SPECIES_DOWNLOAD = 'spatial_dashboard_species_download'
SPATIAL_DASHBOARD_NATIONAL_CONS_STATUS = 'spatial_dashboard_national_rli'
TAXON_SUMMARY = 'taxon_summary'

SEARCH_PROCESSING = 'processing'
SEARCH_FINISHED = 'finished'
SEARCH_FAILED = 'failed'

logger = logging.getLogger(__name__)


def format_search_result_raw_query(raw_query):
    if not raw_query:
        return
    formatted_params = ()
    params = raw_query[1]
    for param in params:
        formatted_param = param
        if (
                isinstance(param, str) or
                isinstance(param, int) or
                isinstance(param, date) or
                param == 'BioBaseData'
        ):
            formatted_param = '\'' + str(param) + '\''
        elif isinstance(param, list):
            formatted_param = str(param)
            formatted_param = formatted_param.replace('[u\'', '\'{"')
            formatted_param = formatted_param.replace('\',', '",')
            formatted_param = formatted_param.replace(' u\'', ' "')
            formatted_param = formatted_param.replace('\']', '"}\'')
        formatted_params += (formatted_param, )
    query_string = raw_query[0] % formatted_params
    return query_string


class SearchProcessQuerySet(models.QuerySet):
    def delete(self, *args, **kwargs):
        locked_objects = self.filter(locked=True)
        if locked_objects.exists():
            for obj in locked_objects:
                logger.warning(f"Attempted to delete locked SearchProcess with ID {obj.id}")
        unlocked_objects = self.filter(locked=False)
        return super(SearchProcessQuerySet, unlocked_objects).delete(*args, **kwargs)

class SearchProcessManager(models.Manager):
    def get_queryset(self):
        return SearchProcessQuerySet(self.model, using=self._db)


class SearchProcess(models.Model):
    """Search process model
    """
    SEARCH_TYPE = (
        (CLUSTER_GENERATION, 'Cluster Generation'),
        (SEARCH_RESULTS, 'Search Results'),
        (SITES_SUMMARY, 'Site Summary'),
        (TAXON_SUMMARY, 'Taxon Summary'),
        (SPATIAL_DASHBOARD_MODULE_TOTALS, 'Spatial Dashboard Module Totals'),
        (SPATIAL_DASHBOARD_CONS_STATUS, 'Spatial Dashboard Conservation Status'),
        (SPATIAL_DASHBOARD_RLI, 'Spatial Dashboard Red List Index'),
        (SPATIAL_DASHBOARD_MAP, 'Spatial Dashboard Map'),
        (SPATIAL_DASHBOARD_SUMMARY, 'Spatial Dashboard Summary'),
        (SPATIAL_DASHBOARD_SPECIES_DOWNLOAD, 'Spatial Dashboard Species Download'),
        (SPATIAL_DASHBOARD_NATIONAL_CONS_STATUS, 'Spatial Dashboard National RLI'),
    )
    objects = SearchProcessManager()

    file_path = models.CharField(
        blank=True,
        max_length=250,
    )
    category = models.CharField(
        max_length=50,
        choices=SEARCH_TYPE,
        blank=False
    )
    query = models.CharField(
        max_length=2000,
        blank=False
    )
    process_id = models.CharField(
        max_length=200,
        blank=True
    )
    finished = models.BooleanField(
        default=False
    )
    search_raw_query = models.TextField(
        null=True,
        blank=True
    )
    locked = models.BooleanField(
        default=False
    )
    site = models.ForeignKey(
        Site,
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        verbose_name="Associated Site",
        help_text="The site this record is associated with."
    )
    requester = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        models.SET_NULL,
        related_name='requester',
        blank=True,
        null=True,
    )

    def delete(self, *args, **kwargs):
        if self.locked:
            logger.warning(f"Attempted to delete locked SearchProcess with ID {self.id}")
        else:
            super(SearchProcess, self).delete(*args, **kwargs)

    def save(self, *args, **kwargs):
        super(SearchProcess, self).save(*args, **kwargs)

    def set_search_raw_query(self, raw_query):
        query_string = format_search_result_raw_query(raw_query)
        if not query_string:
            return
        self.search_raw_query = query_string
        self.save()

    def create_view(self):
        if self.process_id and self.search_raw_query:
            cursor = connection.cursor()
            try:
                sql = (
                    'DROP MATERIALIZED VIEW IF EXISTS "{view_name}"'.
                    format(
                        view_name=self.process_id
                    ))
                cursor.execute('''%s''' % sql)
            except (ProgrammingError, DatabaseError):
                pass
            sql = (
                'CREATE MATERIALIZED VIEW "{view_name}" AS {sql_raw}'.
                format(
                    view_name=self.process_id,
                    sql_raw=self.search_raw_query
                ))
            cursor.execute('''%s''' % sql)

    def delete_view(self):
        if self.finished and self.process_id and self.search_raw_query:
            cursor = connection.cursor()
            try:
                sql = (
                    'DROP MATERIALIZED VIEW IF EXISTS "{view_name}"'.
                    format(
                        view_name=self.process_id
                    ))
                cursor.execute('''%s''' % sql)
            except (ProgrammingError, DatabaseError):
                return

    def set_status(self, value, should_save_to_file=True):
        if value == SEARCH_FINISHED:
            self.finished = True
            self.save()
        if not should_save_to_file:
            return
        status = {
            'status': value,
            'process': self.process_id
        }
        self.save_to_file(results=status)

    def save_to_file(self, results):
        """
        Save dictionary results to file
        :param results: dictionary
        """
        with open(self.file_path, 'wb') as status_file:
            status_file.write(bytes(json.dumps(results).encode('utf-8')))

    def get_file_if_exits(self, finished=True):
        if os.path.exists(self.file_path) and self.finished == finished:
            with open(self.file_path, 'r') as raw_data:
                file_content = raw_data.read()
                try:
                    return json.loads(file_content)
                except ValueError:
                    return None
        else:
            self.finished = False
            self.save()
        return None

    def set_process_id(self, process_id):
        self.process_id = process_id
        path_folder = os.path.join(settings.MEDIA_ROOT, self.category)
        try:
            os.mkdir(path_folder)
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise
            pass
        self.file_path = os.path.join(path_folder, process_id)
        self.save()


@receiver(pre_delete, sender=SearchProcess)
def searchprocess_delete(sender, instance, **kwargs):
    import os
    if sender != SearchProcess:
        return
    instance.delete_view()
    if os.path.exists(instance.file_path):
        os.remove(instance.file_path)
