import os

from django.core.exceptions import SuspiciousFileOperation
from django.utils.text import get_valid_filename


def ensure_within_dir(path, base_dir):
    """Return a real path after verifying it stays under base_dir.
    """
    real_path = os.path.realpath(path)
    real_base = os.path.realpath(base_dir)
    if real_path != real_base and not real_path.startswith(real_base + os.sep):
        raise SuspiciousFileOperation('Attempted access outside permitted directory.')
    return real_path


def sanitize_path_component(value, fallback='file'):
    sanitized = get_valid_filename((value or '').strip())
    return sanitized or fallback
