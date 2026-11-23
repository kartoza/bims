# coding: utf-8
"""HTTP session for the GBIF and Catalogue of Life (COL) APIs.

Every request goes through one session that retries connection errors,
dropped connections (e.g. "Connection reset by peer"), rate limiting and
server errors with exponential backoff, and never waits forever.
"""
import threading

import requests
from django.conf import settings
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

RETRY_STATUSES = (429, 500, 502, 503, 504)

_local = threading.local()


def max_retries():
    """Retries after the first attempt (GBIF_MAX_RETRIES, default 5)."""
    return max(0, int(getattr(settings, 'GBIF_MAX_RETRIES', 5)))


def request_timeout():
    """Seconds to connect and to wait for each response (default 30)."""
    return float(getattr(settings, 'GBIF_REQUEST_TIMEOUT', 30))


def backoff_factor():
    """Backoff factor (GBIF_RETRY_BACKOFF, default 1). With 1 the waits
    before the retries are 0, 2, 4, 8 and 16 seconds, 30 s in total for 5
    retries."""
    return float(getattr(settings, 'GBIF_RETRY_BACKOFF', 1))


def build_session():
    retries = max_retries()
    retry = Retry(
        total=retries,
        connect=retries,
        read=retries,
        status=retries,
        other=retries,
        backoff_factor=backoff_factor(),
        status_forcelist=RETRY_STATUSES,
        allowed_methods=frozenset(['GET', 'HEAD']),
        respect_retry_after_header=True,
        # Return the last response instead of raising, callers check it
        raise_on_status=False,
    )
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    session.headers.update({'Accept': 'application/json'})
    return session


def gbif_session():
    """The session of the current thread, rebuilt when the retry settings
    change."""
    key = (max_retries(), backoff_factor())
    session = getattr(_local, 'session', None)
    if session is None or getattr(_local, 'key', None) != key:
        session = build_session()
        _local.session = session
        _local.key = key
    return session


def gbif_get(url, **kwargs):
    """GET with retries. Raises requests.RequestException once the retries
    are used up."""
    kwargs.setdefault('timeout', request_timeout())
    return gbif_session().get(url, **kwargs)


def describe_request_error(exc):
    """A short reason for a failed request, without the full URL."""
    if isinstance(exc, requests.exceptions.Timeout):
        return 'the request timed out'
    if isinstance(exc, requests.exceptions.ConnectionError):
        text = str(exc).lower()
        if 'reset by peer' in text or 'connection aborted' in text:
            return 'the connection was reset'
        if 'max retries exceeded' in text and 'too many' in text:
            return 'the server kept returning errors'
        return 'the server could not be reached'
    if isinstance(exc, requests.exceptions.HTTPError):
        status = getattr(exc.response, 'status_code', None)
        return f'the server returned HTTP {status}' if status else 'HTTP error'
    return exc.__class__.__name__
