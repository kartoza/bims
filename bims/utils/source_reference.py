from bims.models.source_reference import SourceReference

SOURCE_REFERENCE_REQUIRED_MESSAGE = 'You must select a source reference.'


def get_required_source_reference(source_reference_id):
    """
    Return the SourceReference for the submitted id, or None when the id is
    missing, not a number or does not match a source reference.
    """
    try:
        source_reference_id = int(str(source_reference_id).strip())
    except (TypeError, ValueError):
        return None
    return SourceReference.objects.filter(id=source_reference_id).first()
