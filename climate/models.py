from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator
from bims.models.location_site import LocationSite


class Climate(models.Model):
    """
    Climate data model for storing daily weather measurements.

    Stores temperature, humidity, wind speed, and rainfall data
    for specific locations and dates.
    """

    location_site = models.ForeignKey(
        LocationSite,
        on_delete=models.CASCADE,
        related_name='climate_data',
        help_text='The location site where this climate data was recorded'
    )

    station_name = models.CharField(
        max_length=255,
        blank=True,
        default='',
        help_text='Name of the weather station'
    )
    date = models.DateField(
        help_text='Date of the climate observation'
    )
    year = models.IntegerField(
        validators=[MinValueValidator(1900), MaxValueValidator(2100)],
        help_text='Year of observation'
    )
    month = models.IntegerField(
        validators=[MinValueValidator(1), MaxValueValidator(12)],
        help_text='Month of observation (1-12)'
    )
    day = models.IntegerField(
        validators=[MinValueValidator(1), MaxValueValidator(31)],
        help_text='Day of observation (1-31)'
    )

    avg_temperature = models.FloatField(
        null=True,
        blank=True,
        help_text='Average daily temperature (°C)'
    )
    max_temperature = models.FloatField(
        null=True,
        blank=True,
        help_text='Maximum daily temperature (°C)'
    )
    min_temperature = models.FloatField(
        null=True,
        blank=True,
        help_text='Minimum daily temperature (°C)'
    )

    avg_humidity = models.FloatField(
        null=True,
        blank=True,
        validators=[MinValueValidator(0), MaxValueValidator(100)],
        help_text='Average daily relative humidity (%)'
    )
    max_humidity = models.FloatField(
        null=True,
        blank=True,
        validators=[MinValueValidator(0), MaxValueValidator(100)],
        help_text='Maximum relative humidity (%)'
    )
    min_humidity = models.FloatField(
        null=True,
        blank=True,
        validators=[MinValueValidator(0), MaxValueValidator(100)],
        help_text='Minimum relative humidity (%)'
    )
    avg_windspeed = models.FloatField(
        null=True,
        blank=True,
        validators=[MinValueValidator(0)],
        help_text='Average daily wind speed (m/s)'
    )
    daily_rainfall = models.FloatField(
        null=True,
        blank=True,
        validators=[MinValueValidator(0)],
        help_text='Daily rainfall (mm)'
    )
    flag = models.TextField(
        blank=True,
        null=True,
        default=''
    )
    created_at = models.DateTimeField(
        auto_now_add=True,
        help_text='When this record was created'
    )
    updated_at = models.DateTimeField(
        auto_now=True,
        help_text='When this record was last updated'
    )

    class Meta:
        app_label = 'climate'
        verbose_name = 'Climate Data'
        verbose_name_plural = 'Climate Data'
        ordering = ['-date']
        unique_together = ['location_site', 'date']
        indexes = [
            models.Index(fields=['location_site', 'date']),
            models.Index(fields=['date']),
            models.Index(fields=['year', 'month']),
        ]

    def __str__(self):
        return f"{self.location_site} - {self.date}"

    def save(self, *args, **kwargs):
        """
        Override save to automatically populate year, month, day
        from date field if not provided.
        """
        if self.date:
            self.year = self.date.year
            self.month = self.date.month
            self.day = self.date.day
        super().save(*args, **kwargs)
