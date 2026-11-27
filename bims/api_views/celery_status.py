# coding=utf-8
import json

from celery.result import AsyncResult
from django.http.response import HttpResponse
from rest_framework.views import APIView
from rest_framework.permissions import IsAuthenticated

from bims.models.upload_session import UploadSession


class CeleryStatus(APIView):
    """Check celery task status"""
    permission_classes = (IsAuthenticated,)

    def get(self, request, task_id, *args):
        task = AsyncResult(task_id)
        session = request.GET.get('session', '')

        json_response = {
            'task_id': task_id,
            'state': task.state,
            'result': str(task.result)
        }
        if task.state == 'PROGRESS' and isinstance(task.info, dict):
            json_response['progress'] = task.info

        if session == 'upload':
            try:
                upload_session = UploadSession.objects.get(
                    token=task_id
                )
                json_response['success'] = upload_session.success_notes
                json_response['error'] = upload_session.error_notes
            except UploadSession.DoesNotExist:
                pass

        return HttpResponse(
            json.dumps(json_response),
            content_type='application/json'
        )
