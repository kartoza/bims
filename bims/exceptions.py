# coding=utf-8
"""API exceptions shared by BIMS views and serializers."""
from rest_framework import status
from rest_framework.exceptions import APIException

from bims.utils.ogc_protection import OgcLayerThrottledError


class OgcLayerBackOff(APIException):
    """The layer could not be evaluated now; the client should retry."""

    default_code = 'ogc_layer_busy'

    def __init__(self, exc):
        self.status_code = (
            status.HTTP_429_TOO_MANY_REQUESTS
            if isinstance(exc, OgcLayerThrottledError)
            else status.HTTP_503_SERVICE_UNAVAILABLE)
        self.retry_after = exc.retry_after
        super().__init__(str(exc))
