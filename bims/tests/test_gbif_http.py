# coding: utf-8
"""Tests for the retrying GBIF/COL HTTP session."""
import json
import socket
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest import mock

import requests
from django.test import SimpleTestCase, override_settings

from bims.scripts.taxa_upload import network_error_message
from bims.utils import gbif_http
from bims.utils.gbif_http import describe_request_error, gbif_get


class FlakyHandler(BaseHTTPRequestHandler):
    """Fails the first ``server.failures`` requests, then answers 200."""

    def do_GET(self):
        server = self.server
        server.requests += 1
        if server.requests <= server.failures:
            if server.mode == 'reset':
                # Close without a response, like "Connection reset by peer"
                self.connection.setsockopt(
                    socket.SOL_SOCKET, socket.SO_LINGER,
                    b'\x01\x00\x00\x00\x00\x00\x00\x00')
                self.connection.close()
                return
            self.send_response(503)
            self.send_header('Content-Length', '0')
            self.end_headers()
            return
        body = json.dumps({'usage': {'key': 'SQF5W'}}).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


@override_settings(GBIF_RETRY_BACKOFF=0, GBIF_REQUEST_TIMEOUT=5)
class GbifSessionRetryTest(SimpleTestCase):

    def start_server(self, failures, mode):
        server = HTTPServer(('127.0.0.1', 0), FlakyHandler)
        server.failures = failures
        server.mode = mode
        server.requests = 0
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return server, f'http://127.0.0.1:{server.server_port}/match'

    @override_settings(GBIF_MAX_RETRIES=3)
    def test_dropped_connections_are_retried(self):
        server, url = self.start_server(failures=2, mode='reset')
        response = gbif_get(url)
        self.assertEqual(response.json()['usage']['key'], 'SQF5W')
        self.assertEqual(server.requests, 3)

    @override_settings(GBIF_MAX_RETRIES=3)
    def test_server_errors_are_retried(self):
        server, url = self.start_server(failures=3, mode='503')
        response = gbif_get(url)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(server.requests, 4)

    @override_settings(GBIF_MAX_RETRIES=2)
    def test_gives_up_after_max_retries(self):
        server, url = self.start_server(failures=10, mode='reset')
        with self.assertRaises(requests.exceptions.ConnectionError):
            gbif_get(url)
        # The first attempt and 2 retries
        self.assertEqual(server.requests, 3)

    @override_settings(GBIF_MAX_RETRIES=2)
    def test_last_server_error_is_returned_after_max_retries(self):
        server, url = self.start_server(failures=10, mode='503')
        response = gbif_get(url)
        self.assertEqual(response.status_code, 503)
        self.assertEqual(server.requests, 3)

    @override_settings(GBIF_MAX_RETRIES=0)
    def test_zero_retries_means_one_attempt(self):
        server, url = self.start_server(failures=1, mode='reset')
        with self.assertRaises(requests.exceptions.ConnectionError):
            gbif_get(url)
        self.assertEqual(server.requests, 1)

    def test_session_follows_the_retry_setting(self):
        with self.settings(GBIF_MAX_RETRIES=1):
            first = gbif_http.gbif_session()
            self.assertIs(gbif_http.gbif_session(), first)
            self.assertEqual(
                first.get_adapter('https://api.gbif.org').max_retries.total, 1)
        with self.settings(GBIF_MAX_RETRIES=4):
            second = gbif_http.gbif_session()
            self.assertIsNot(second, first)
            self.assertEqual(
                second.get_adapter('https://api.gbif.org').max_retries.total, 4)

    def test_timeout_is_set(self):
        with mock.patch.object(gbif_http, 'gbif_session') as session:
            gbif_get('https://api.gbif.org/v2/species/match')
        self.assertEqual(session.return_value.get.call_args.kwargs['timeout'], 5)


class NetworkErrorMessageTest(SimpleTestCase):

    def test_reasons(self):
        self.assertEqual(
            describe_request_error(requests.exceptions.ConnectionError(
                "('Connection aborted.', ConnectionResetError(104, "
                "'Connection reset by peer'))")),
            'the connection was reset')
        self.assertEqual(
            describe_request_error(requests.exceptions.ReadTimeout('x')),
            'the request timed out')
        self.assertEqual(
            describe_request_error(requests.exceptions.ConnectionError(
                'Name or service not known')),
            'the server could not be reached')

    @override_settings(GBIF_MAX_RETRIES=5)
    def test_upload_message(self):
        message = network_error_message(
            requests.exceptions.ConnectionError('Connection reset by peer'),
            'COL id 644MF')
        self.assertEqual(
            message,
            'Could not reach GBIF/COL for COL id 644MF after 6 attempts '
            '(the connection was reset). This is a temporary network '
            'problem; upload this row again.')
