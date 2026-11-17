from django.urls import path, re_path

from groundwater.views import (
    GroundwaterSiteView,
    GroundwaterDownloadView,
    GroundwaterDashboardMultipleSitesView,
    GroundwaterUploadView,
)

app_name = 'groundwater'

urlpatterns = [
    path('upload/', GroundwaterUploadView.as_view(),
         name='groundwater-upload'),
    path(
        'dashboard-multi-sites/',
        GroundwaterDashboardMultipleSitesView.as_view(),
        name='groundwater-dashboard-multi-sites',
    ),
    re_path(r'^(?P<site_id>\d+)/download/$',
            GroundwaterDownloadView.as_view(),
            name='groundwater-download'),
    re_path(r'^(?P<site_id>\d+)/$',
            GroundwaterSiteView.as_view(),
            name='groundwater-site'),
]
