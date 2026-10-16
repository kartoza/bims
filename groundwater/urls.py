from django.urls import path, re_path

from groundwater.views import (
    GroundwaterSiteView,
    GroundwaterDownloadView,
    GroundwaterDashboardMultipleSitesView,
)

app_name = 'groundwater'

urlpatterns = [
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
