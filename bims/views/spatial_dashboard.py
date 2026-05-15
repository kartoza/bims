# coding=utf-8
from braces.views import LoginRequiredMixin
from django.views.generic import TemplateView
from preferences import preferences

from bims.models.basemap_layer import BaseMapLayer
from bims.serializers.basemap_serializer import BaseMapLayerSerializer
from bims.utils.get_key import get_key


class SpatialDashboardView(LoginRequiredMixin, TemplateView):
    template_name = 'spatial_dashboard.html'

    def get_context_data(self, **kwargs):
        context = super(SpatialDashboardView, self).get_context_data(**kwargs)
        context['basemap_layers'] = BaseMapLayerSerializer(
            BaseMapLayer.objects.all().order_by('order'),
            many=True
        ).data
        context['geoserver_public_location'] = get_key(
            'GEOSERVER_PUBLIC_LOCATION'
        )
        context['is_sanparks'] = (
            preferences.SiteSetting.site_code_generator == 'sanparks'
        )
        return context
