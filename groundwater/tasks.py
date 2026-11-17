from celery import shared_task


@shared_task(name='groundwater.tasks.groundwater_upload', queue='update')
def groundwater_upload(session_id):
    """Process the file of a groundwater upload session."""
    from bims.models.upload_session import UploadSession
    from groundwater.upload import GroundwaterCSVUpload

    try:
        upload_session = UploadSession.objects.get(id=session_id)
    except UploadSession.DoesNotExist:
        return
    # utf-8-sig removes the byte order mark Excel adds to CSV files
    GroundwaterCSVUpload(upload_session).start(encoding='utf-8-sig')
