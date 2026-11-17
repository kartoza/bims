# coding=utf8
import ast
import csv
import json
import os
import errno
from datetime import datetime
from hashlib import sha256

from celery.result import AsyncResult
from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.gis.geos import Polygon
from django.db.models import Q
from django.http import Http404
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK, HTTP_403_FORBIDDEN
from rest_framework.views import APIView
from rest_framework.generics import ListAPIView

from bims.api_views.merge_sites import IsSuperUser
from bims.models.download_request import DownloadRequest
from bims.models.location_site import LocationSite
from bims.serializers.location_site_serializer import (
    LocationSiteSerializer,
    LocationSitesCoordinateSerializer
)
from bims.serializers.location_site_detail_serializer import \
    LocationSiteDetailSerializer
from bims.api_views.collection import GetCollectionAbstract
from bims.tasks import remove_dangling_sites, clear_taxa_not_associated_in_taxon_group
from bims.utils.search_process import (
    get_or_create_search_process,
)
from bims.models.search_process import (
    SITES_SUMMARY,
)
from bims.api_views.search import CollectionSearch
from bims.tasks.email_csv import send_csv_via_email
from bims.tasks.collection_record import download_gbif_ids
from bims.tasks.location_site_summary import (
    generate_location_site_summary
)
from bims.tasks.prune_outside_boundary import (
    prune_outside_boundary_gbif
)


class LocationSiteList(APIView):
    """
    List all location site
    """

    def get(self, request, *args):
        location_site = LocationSite.objects.all()
        # get by bbox
        bbox = request.GET.get('bbox', None)
        if bbox:
            geom_bbox = Polygon.from_bbox(
                tuple([float(edge) for edge in bbox.split(',')]))
            location_site = location_site.filter(
                Q(geometry_point__intersects=geom_bbox) |
                Q(geometry_line__intersects=geom_bbox) |
                Q(geometry_polygon__intersects=geom_bbox) |
                Q(geometry_multipolygon__intersects=geom_bbox)
            )
        serializer = LocationSiteSerializer(location_site, many=True)
        return Response(serializer.data)


class LocationSiteDetail(APIView):
    """
    Return detail of location site
    """

    def get_object(self, pk):
        try:
            return LocationSite.objects.get(pk=pk)
        except LocationSite.DoesNotExist:
            raise Http404

    def get(self, request):
        site_id = request.GET.get('siteId')
        filters = request.GET

        # Search collection
        search = CollectionSearch(filters)
        collection_results = search.process_search()
        context = {
            'collection_results': collection_results
        }
        location_site = self.get_object(site_id)
        serializer = LocationSiteDetailSerializer(
            location_site,
            context=context)
        return Response(serializer.data)


class LocationSitesSummary(APIView):
    """
        List cached location site summary based on collection record search.
    """
    def get(self, request):
        filters = request.GET.dict()
        search_uri = request.build_absolute_uri()
        if request.user.is_anonymous or not request.user.is_superuser:
            use_cached = True
        else:
            use_cached = ast.literal_eval(filters.get('cached', 'True'))
        state = 'STARTED'

        if 'requester' not in filters:
            filters['requester'] = self.request.user.id

        search_process, created = get_or_create_search_process(
            SITES_SUMMARY,
            query=search_uri,
            requester=self.request.user
        )

        if not use_cached:
            return Response(
                generate_location_site_summary(
                    filters,
                    search_process.id
                )
            )

        if created:
            async_result = generate_location_site_summary.delay(
                filters,
                search_process.id
            )
            search_process.process_id = async_result.id
            search_process.save()
        else:
            if search_process.file_path:
                if os.path.exists(search_process.file_path):
                    try:
                        raw_data = open(search_process.file_path)
                        return Response(json.load(raw_data))
                    except ValueError:
                        search_process.delete()
                        return Response({
                            'status': 'PROGRESS',
                        })
            else:
                task = AsyncResult(search_process.process_id)
                state = task.state

                if state == 'SUCCESS' and not search_process.file_path:
                    search_process.delete()
                    return Response({
                        'status': 'PROGRESS',
                    })

                if state == 'PENDING':
                    async_result = generate_location_site_summary.delay(
                        filters,
                        search_process.id
                    )
                    search_process.process_id = async_result.id
                    search_process.save()
                    state = 'STARTED'

        return Response({
            'status': state,
            'task_id': search_process.process_id
        })


class LocationSitesCoordinate(ListAPIView):
    """
        List paginated location site based on collection record search,
        there may be duplication.
    """
    serializer_class = LocationSitesCoordinateSerializer

    def get_queryset(self):
        query_value = self.request.GET.get('search')
        filters = self.request.GET
        (
            collection_results,
            site_results,
            fuzzy_search
        ) = GetCollectionAbstract.apply_filter(
            query_value,
            filters,
            ignore_bbox=True,
            only_site=True)
        return collection_results


def generate_gbif_ids_data(
        path_file,
        request,
        send_email=False,
        user_id=None
):
    def get_download_request(download_request_id):
        try:
            return DownloadRequest.objects.get(id=download_request_id)
        except DownloadRequest.DoesNotExist:
            return None

    filters = request
    headers = ['TAXON', 'GBIF SPECIES LINK', 'GBIF OCCURRENCE LINK']
    download_request_id = filters.get('downloadRequestId', '')
    download_request = None
    if download_request_id:
        try:
            download_request = DownloadRequest.objects.get(
                id=download_request_id
            )
        except DownloadRequest.DoesNotExist:
            pass

    if download_request and download_request.rejected:
        download_request.processing = False
        download_request.save()
        return False

    search = CollectionSearch(filters)
    collection_results = search.process_search()
    collection_results = collection_results.exclude(
        upstream_id=''
    )

    if collection_results.count() > 0:
        with open(path_file, 'w') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=headers)
            writer.writeheader()
            total_records = collection_results.count()
            for index, record in enumerate(collection_results):
                writer.writerow({
                    'TAXON': record.taxonomy.canonical_name,
                    'GBIF SPECIES LINK': 'https://gbif.org/species/{0}'.format(
                        record.taxonomy.gbif_key),
                    'GBIF OCCURRENCE LINK': (
                        'https://gbif.org/occurrence/{}'.format(
                            record.upstream_id
                        )
                    )
                })

                download_request = get_download_request(download_request_id)
                if download_request and not download_request.rejected:
                    download_request.progress = (
                        f'{index + 1}/{total_records}'
                    )
                    download_request.save()
                elif download_request.rejected:
                    return False

    if download_request:
        download_request.processing = False
        download_request.request_file = path_file
        download_request.save()

    download_request = get_download_request(download_request_id)
    if (
        send_email and user_id and
        download_request and download_request.approved
    ):
        UserModel = get_user_model()
        try:
            user = UserModel.objects.get(id=user_id)
            send_csv_via_email(
                user_id=user.id,
                csv_file=path_file,
                file_name='GBIFRecords',
                download_request_id=download_request_id
            )
        except UserModel.DoesNotExist:
            pass

    return True


class GbifIdsDownloader(APIView):

    def get_hashed_name(self, request):
        query_string = json.dumps(
            request.GET.dict()
        ) + datetime.today().strftime('%Y%m%d')
        return sha256(
            query_string.encode('utf-8')
        ).hexdigest()

    def get(self, request):
        filename = self.get_hashed_name(request)
        filename += '.csv'

        # Check if filename exists
        folder = settings.PROCESSED_CSV_PATH
        path_folder = os.path.join(settings.MEDIA_ROOT, folder)
        path_file = os.path.join(path_folder, filename)
        download_request_id = self.request.GET.get('downloadRequestId', '')

        try:
            os.mkdir(path_folder)
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise
            pass

        try:
            download_request = DownloadRequest.objects.get(
                id=download_request_id
            )
        except DownloadRequest.DoesNotExist:
            return Response({
                'status': 'failed',
                'message': 'Download request does not exist'
            })

        if os.path.exists(path_file) and download_request.approved:
            send_csv_via_email.delay(
                user_id=self.request.user.id,
                csv_file=path_file,
                file_name='GBIFRecords',
                download_request_id=download_request_id
            )
        else:
            if os.path.exists(path_file):
                return Response({
                    'status': 'failed',
                    'message': 'Download request has been requested'
                })

            download_gbif_ids.delay(
                path_file,
                self.request.GET,
                send_email=True,
                user_id=self.request.user.id)

        return Response({
            'status': 'processing',
            'filename': filename
        })


class DeleteDanglingLocationSites(APIView):

    permission_classes = (IsSuperUser,)

    def post(self, request, *args, **kwargs):
        if request.user.is_superuser:
            dry_run = bool(request.data.get("dry_run", False))
            remove_dangling_sites.delay(dry_run=dry_run)
            message = (
                "Dry run for dangling sites started in the background. "
                "A report will be emailed to superusers."
                if dry_run else
                "Deleting dangling sites in the background."
            )
            return Response({"message": message}, status=HTTP_200_OK)
        return Response(
            {"error": "Permission denied."},
            status=HTTP_403_FORBIDDEN)


class RemoveOutsideBoundaryGbifData(APIView):
    permission_classes = (IsSuperUser,)

    def post(self, request, *args, **kwargs):
        if not request.user.is_superuser:
            return Response({"error": "Permission denied."}, status=HTTP_403_FORBIDDEN)

        dry_run = bool(request.data.get("dry_run", False))
        delete_empty_sites = bool(request.data.get("delete_empty_sites", True))
        prune_outside_boundary_gbif.delay(dry_run=dry_run, delete_empty_sites=delete_empty_sites)

        return Response(
            {
                "message": (
                    "Starting background cleanup of GBIF occurrences and surveys "
                    "outside the configured boundary."
                    + (" (dry-run)" if dry_run else "")
                )
            },
            status=HTTP_200_OK,
        )
