from django.db import models

from bims.models.location_site import LocationSite


class GroundwaterStation(LocationSite):
    """Proxy model representing a LocationSite used as a groundwater
    monitoring station."""

    class Meta:
        proxy = True
        app_label = 'groundwater'
        verbose_name = 'Groundwater Station'
        verbose_name_plural = 'Groundwater Stations'


class GroundwaterReading(models.Model):
    """Hourly groundwater measurement of a station.

    The measured parameters are provisional until the client confirms
    which ones are available.
    """

    location_site = models.ForeignKey(
        LocationSite,
        on_delete=models.CASCADE,
        related_name='groundwater_data',
        help_text='The station where the reading was recorded'
    )
    timestamp = models.DateTimeField(
        help_text='Date and time of the reading (hourly)'
    )
    water_level = models.FloatField(
        null=True,
        blank=True,
        help_text='Water level (m below ground level)'
    )
    water_temperature = models.FloatField(
        null=True,
        blank=True,
        help_text='Water temperature (°C)'
    )
    electrical_conductivity = models.FloatField(
        null=True,
        blank=True,
        help_text='Electrical conductivity (µS/cm)'
    )
    flag = models.CharField(
        max_length=100,
        blank=True,
        default=''
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        app_label = 'groundwater'
        ordering = ['timestamp']
        verbose_name = 'Groundwater reading'
        verbose_name_plural = 'Groundwater readings'
        constraints = [
            models.UniqueConstraint(
                fields=['location_site', 'timestamp'],
                name='unique_groundwater_reading_per_hour'
            ),
        ]
        indexes = [
            models.Index(
                fields=['location_site', 'timestamp'],
                name='groundwater_site_time_idx'
            ),
        ]

    def __str__(self):
        return f'{self.location_site_id} {self.timestamp:%Y-%m-%d %H:%M}'
