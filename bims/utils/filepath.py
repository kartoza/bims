import os

from django.core.exceptions import SuspiciousFileOperation
from django.utils.text import get_valid_filename


def ensure_within_dir(path, base_dir):
    """Return an absolute path after verifying it stays under base_dir."""
    absolute_path = os.path.abspath(path)
    absolute_base_dir = os.path.abspath(base_dir)
    try:
        if os.path.commonpath([absolute_base_dir, absolute_path]) != absolute_base_dir:
            raise SuspiciousFileOperation('Attempted access outside permitted directory.')
    except ValueError:
        raise SuspiciousFileOperation('Attempted access outside permitted directory.')
    return absolute_path


def sanitize_path_component(value, fallback='file'):
    sanitized = get_valid_filename((value or '').strip())
    return sanitized or fallback
