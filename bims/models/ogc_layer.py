# coding=utf-8
"""OGC layer model definition.

A saved BIMS filter that is served as an OGC API - Features collection,
so it can be added to GIS software as a live layer.
"""
import hashlib
import logging
import secrets
from datetime import timedelta

from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import IntegrityError, models, transaction
from django.utils import timezone

logger = logging.getLogger(__name__)

OGC_TOKEN_BYTES = 32
OGC_TOKEN_HINT_LENGTH = 6
# Do not write last_accessed more often than this
OGC_TOUCH_INTERVAL = timedelta(minutes=5)


def generate_ogc_token():
    return secrets.token_urlsafe(OGC_TOKEN_BYTES)


def hash_ogc_token(token):
    return hashlib.sha256(token.encode('utf-8')).hexdigest()


class OgcLayer(models.Model):
    """A saved filter published as an OGC API - Features layer.

    Only a hash of the layer token is stored. The plain token is available
    on ``raw_token`` right after the layer is created or the token is
    rotated, and cannot be recovered afterwards.
    """

    name = models.CharField(
        max_length=255
    )
    owner = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name='ogc_layers'
    )
    token_hash = models.CharField(
        max_length=64,
        unique=True,
        editable=False,
        help_text='SHA-256 of the secret part of the layer URL.'
    )
    token_hint = models.CharField(
        max_length=OGC_TOKEN_HINT_LENGTH,
        editable=False,
        help_text='Last characters of the token, to tell layers apart.'
    )
    query = models.JSONField(
        default=dict,
        blank=True,
        help_text='Search filter parameters, as sent by the BIMS filter panel.'
    )
    include_sensitive = models.BooleanField(
        default=False,
        help_text='Include sensitive data, if the owner is allowed to see it.'
    )
    include_private = models.BooleanField(
        default=False,
        help_text='Include private data, if the owner is allowed to see it.'
    )
    is_active = models.BooleanField(
        default=True,
        help_text='Uncheck to revoke the layer URL.'
    )
    expires_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text='The layer URL stops working after this date. '
                  'Leave empty for no expiry.'
    )
    created = models.DateTimeField(
        auto_now_add=True
    )
    last_accessed = models.DateTimeField(
        null=True,
        blank=True
    )
    record_count_cached = models.PositiveIntegerField(
        null=True,
        blank=True
    )
    record_count_updated = models.DateTimeField(
        null=True,
        blank=True
    )
    last_evaluated = models.DateTimeField(
        null=True,
        blank=True,
        help_text='When the layer filter was last evaluated successfully.'
    )
    last_evaluation_ms = models.PositiveIntegerField(
        null=True,
        blank=True,
        help_text='How long the last evaluation took, in milliseconds.'
    )
    request_count = models.PositiveBigIntegerField(
        default=0,
        help_text='Requests served. Updated every few minutes, '
                  'so it can be slightly behind.'
    )
    failure_count = models.PositiveIntegerField(
        default=0,
        help_text='Evaluations that timed out in a row. '
                  'Reset by a successful evaluation.'
    )
    disabled_reason = models.CharField(
        max_length=255,
        blank=True,
        default='',
        help_text='Why the layer was switched off automatically.'
    )

    raw_token = None

    class Meta:
        ordering = ('-created',)

    def __str__(self):
        return self.name

    @property
    def is_expired(self):
        return bool(self.expires_at and self.expires_at <= timezone.now())

    @property
    def is_available(self):
        """Whether the layer URL can currently be served."""
        return self.is_active and not self.is_expired

    @classmethod
    def get_by_token(cls, token):
        """Return the layer for a plain token, or None if there is none."""
        if not token:
            return None
        return cls.objects.filter(token_hash=hash_ogc_token(token)).first()

    def _set_token(self, token):
        self.raw_token = token
        self.token_hash = hash_ogc_token(token)
        self.token_hint = token[-OGC_TOKEN_HINT_LENGTH:]

    def rotate_token(self):
        """Replace the token, which invalidates the previous URL.

        :return: the new plain token. It is not stored anywhere.
        """
        self._set_token(generate_ogc_token())
        self.save(update_fields=['token_hash', 'token_hint'])
        return self.raw_token

    def touch(self, now=None):
        """Record an access, at most once per OGC_TOUCH_INTERVAL.

        :return: True if last_accessed was written.
        """
        now = now or timezone.now()
        stale_before = now - OGC_TOUCH_INTERVAL
        updated = OgcLayer.objects.filter(pk=self.pk).filter(
            models.Q(last_accessed__isnull=True) |
            models.Q(last_accessed__lt=stale_before)
        ).update(last_accessed=now)
        if updated:
            self.last_accessed = now
        return bool(updated)

    def record_failure(self):
        """Count an evaluation that timed out.

        The layer is switched off after OGC_LAYER_MAX_FAILURES timeouts in
        a row, so a filter the database cannot answer stops costing time.

        :return: True if the layer was switched off by this failure.
        """
        if not self.pk:
            return False
        OgcLayer.objects.filter(pk=self.pk).update(
            failure_count=models.F('failure_count') + 1)
        OgcLayerDailyUsage.add(self.pk, timeouts=1)
        self.failure_count = OgcLayer.objects.filter(
            pk=self.pk).values_list('failure_count', flat=True).first() or 0
        limit = settings.OGC_LAYER_MAX_FAILURES
        if not limit or self.failure_count < limit:
            return False
        reason = (
            'Switched off automatically after %d evaluations in a row '
            'timed out.' % self.failure_count)
        OgcLayer.objects.filter(pk=self.pk, is_active=True).update(
            is_active=False, disabled_reason=reason)
        self.is_active = False
        self.disabled_reason = reason
        logger.error(
            'OGC layer %s switched off: %s', self.pk, reason)
        return True

    def record_evaluation(self, duration_ms, now=None):
        """Store how long a successful evaluation took and reset failures."""
        if not self.pk:
            return
        now = now or timezone.now()
        OgcLayer.objects.filter(pk=self.pk).update(
            last_evaluated=now,
            last_evaluation_ms=duration_ms,
            failure_count=0)
        self.last_evaluated = now
        self.last_evaluation_ms = duration_ms
        self.failure_count = 0

    def reset_failures(self):
        """Clear the failure state, for example when a layer is re-enabled."""
        self.failure_count = 0
        self.disabled_reason = ''
        if self.pk:
            OgcLayer.objects.filter(pk=self.pk).update(
                failure_count=0, disabled_reason='')

    def add_requests(self, count):
        """Add to the request count without overwriting other updates."""
        if self.pk and count > 0:
            OgcLayer.objects.filter(pk=self.pk).update(
                request_count=models.F('request_count') + count)
            OgcLayerDailyUsage.add(self.pk, requests=count)

    def clean(self):
        super().clean()
        errors = {}
        limit = settings.OGC_MAX_LAYERS_PER_OWNER
        if limit and not self.pk and self.owner_id:
            active = OgcLayer.objects.filter(
                owner_id=self.owner_id, is_active=True).count()
            if active >= limit:
                errors['owner'] = (
                    'The owner already has %d active layers, which is the '
                    'limit. Revoke a layer first.' % limit)
        if not isinstance(self.query, dict):
            errors['query'] = 'The query must be a JSON object.'
        owner = getattr(self, 'owner', None) if self.owner_id else None
        if owner is not None:
            group_names = set(owner.groups.values_list('name', flat=True))
            if (
                self.include_sensitive and
                'SensitiveDataGroup' not in group_names
            ):
                errors['include_sensitive'] = (
                    'The owner is not allowed to access sensitive data.'
                )
            if (
                self.include_private and
                'PrivateDataGroup' not in group_names
            ):
                errors['include_private'] = (
                    'The owner is not allowed to access private data.'
                )
        if errors:
            raise ValidationError(errors)

    def save(self, *args, **kwargs):
        if not self.token_hash:
            self._set_token(generate_ogc_token())
        super().save(*args, **kwargs)


class OgcLayerDailyUsage(models.Model):
    """Usage of one layer on one day, for the admin usage page.

    Requests are added when the cached request count is written to the
    database (every few minutes), so a day can be slightly behind.
    """

    layer = models.ForeignKey(
        OgcLayer,
        on_delete=models.CASCADE,
        related_name='daily_usage'
    )
    date = models.DateField()
    requests = models.PositiveBigIntegerField(default=0)
    slow_requests = models.PositiveIntegerField(
        default=0,
        help_text='Requests slower than OGC_SLOW_REQUEST_SECONDS.'
    )
    slowest_ms = models.PositiveIntegerField(
        default=0,
        help_text='Duration of the slowest request of the day.'
    )
    timeouts = models.PositiveIntegerField(
        default=0,
        help_text='Evaluations of the layer that timed out.'
    )

    class Meta:
        ordering = ('-date',)
        constraints = [
            models.UniqueConstraint(
                fields=('layer', 'date'),
                name='unique_ogc_layer_daily_usage'),
        ]
        verbose_name_plural = 'OGC layer daily usage'

    def __str__(self):
        return '%s %s' % (self.layer_id, self.date)

    @classmethod
    def _value(cls, name, value):
        return models.Value(value, output_field=cls._meta.get_field(name))

    @classmethod
    def add(cls, layer_pk, requests=0, slow_requests=0, slowest_ms=0,
            timeouts=0, day=None):
        """Add to the usage of a layer for a day (today by default).
        """
        if not layer_pk:
            return
        day = day or timezone.localdate()
        added = {
            'requests': requests,
            'slow_requests': slow_requests,
            'timeouts': timeouts,
        }
        changes = {
            name: models.F(name) + cls._value(name, value)
            for name, value in added.items() if value
        }
        if slowest_ms:
            changes['slowest_ms'] = models.Case(
                models.When(slowest_ms__lt=slowest_ms,
                            then=cls._value('slowest_ms', slowest_ms)),
                default=models.F('slowest_ms'),
                output_field=cls._meta.get_field('slowest_ms'))
        if not changes:
            return
        try:
            with transaction.atomic():
                rows = cls.objects.filter(layer_id=layer_pk, date=day)
                if rows.update(**changes):
                    return
                try:
                    with transaction.atomic():
                        cls.objects.create(
                            layer_id=layer_pk, date=day,
                            slowest_ms=slowest_ms, **added)
                except IntegrityError:
                    rows.update(**changes)
        except Exception:
            logger.warning(
                'Could not store usage of OGC layer %s', layer_pk,
                exc_info=True)
