"""mysite URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/1.8/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  re_path(r'^$', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  re_path(r'^$', Home.as_view(), name='home')
Including another URLconf
    1. Add a URL to urlpatterns:  re_path(r'^blog/', include('blog.urls'))
"""
from django.views.generic import TemplateView
from django.views.generic.base import RedirectView
from django.conf import settings
from django.conf.urls.static import static
from django.contrib import admin
from django.contrib.auth.decorators import login_required
from django.urls import include, path
from bims.views.documents import BimsDocumentUploadView
from django.urls import re_path
from django.contrib.flatpages import views
from bims.api_schema import schema_view


# GeoNode has to be in root url conf.
# It cannot be included in include() function because it contains i18n urls
urlpatterns = [
    re_path(r'^grappelli/', include('grappelli.urls')),
    re_path(r'^accounts/', include('allauth.urls')),
    re_path(r'^', include('bims.urls')),
    re_path(r'^mobile/', include('mobile.urls')),
    re_path(r'^', include('bims_theme.urls')),
    re_path(r'^', include('pesticide.urls')),
    # Redirect cloud_native_gis built-in schema pages to BIMS schema
    path('swagger/', RedirectView.as_view(url='/api/schema/swagger/', permanent=False)),
    path('redoc/', RedirectView.as_view(url='/api/schema/redoc/', permanent=False)),
    re_path(r'^', include('cloud_native_gis.urls')),
    re_path(r'^climate/', include('climate.urls')),
    re_path(r'^sass/', include('sass.urls')),
    re_path(r'^bibliography/',
        include(('td_biblio.urls', 'bibliography'),
                namespace = 'td_biblio')),

    # prometheus monitoring
    # re_path(r'', include('django_prometheus.urls')),
    re_path(r'^documents/upload/?$', login_required(
        BimsDocumentUploadView.as_view()),
        name='document_upload'),
    re_path(r'^api-auth/', include('rest_framework.urls')),

    re_path(r'^api/schema/swagger(?P<format>\.json|\.yaml)$',
        schema_view.without_ui(cache_timeout=0), name='schema-json'),
    re_path(r'^api/schema/swagger/$',
        schema_view.with_ui('swagger', cache_timeout=0), name='schema-swagger-ui'),
    re_path(r'^api/schema/redoc/$',
        schema_view.with_ui('redoc', cache_timeout=0), name='schema-redoc'),
    re_path(r'^geonode/?$',
        TemplateView.as_view(template_name='site_index.html'),
        name='home'),
    re_path(r'^pages/', include('django.contrib.flatpages.urls')),
    re_path(r'^page/', include('django.contrib.flatpages.urls')),
    re_path('^admin/', admin.site.urls),
    re_path(r"^(?P<url>.*/)$", views.flatpage),
]

if settings.DEBUG:
    urlpatterns += static(
        settings.MEDIA_URL, document_root=settings.MEDIA_ROOT
    )

    urlpatterns += [
        path('__debug__/', include('debug_toolbar.urls')),
    ]
