import math
import random
from datetime import timedelta

from django.contrib.gis.geos import Point
from django.core.management.base import BaseCommand
from django.utils import timezone

from bims.models.location_site import LocationSite, generate_site_code
from bims.models.location_type import LocationType
from groundwater.models import GroundwaterReading

# name, latitude, longitude, mean level (m below ground), mean temperature
DUMMY_STATIONS = [
    ('Example Borehole - Skukuza', -24.9930, 31.5890, 12.5, 24.5),
    ('Example Borehole - Satara', -24.3960, 31.7740, 18.2, 25.5),
    ('Example Borehole - Letaba', -23.8500, 31.5900, 9.8, 26.0),
    ('Example Borehole - Punda Maria', -22.6900, 31.0200, 21.4, 25.0),
]


class Command(BaseCommand):
    """Create dummy hourly groundwater data so the groundwater dashboard
    can be demonstrated before the client data are available.

    Run on a tenant with:
        python manage.py tenant_command seed_groundwater_dummy_data \
            --schema=sanpark
    """

    help = 'Create dummy stations with hourly groundwater readings'

    def add_arguments(self, parser):
        parser.add_argument(
            '--days', type=int, default=365,
            help='Number of days of hourly data to generate'
        )
        parser.add_argument(
            '--seed', type=int, default=42,
            help='Random seed, for reproducible data'
        )

    def handle(self, *args, **options):
        days = options['days']
        rng = random.Random(options['seed'])
        location_type, _ = LocationType.objects.get_or_create(
            name='Groundwater Station',
            defaults={
                'allowed_geometry': 'POINT',
                'description': 'Groundwater monitoring station'
            }
        )
        end = timezone.now().replace(minute=0, second=0, microsecond=0)
        start = end - timedelta(days=days)

        for name, lat, lon, level_mean, temp_mean in DUMMY_STATIONS:
            site = self.get_or_create_station(
                name, lat, lon, location_type)
            # Dummy data only: regenerate the readings of the station
            GroundwaterReading.objects.filter(location_site=site).delete()

            readings = []
            hours = int((end - start).total_seconds() // 3600)
            drift = 0.0
            for i in range(hours + 1):
                timestamp = start + timedelta(hours=i)
                if rng.random() < 0.01:
                    # Occasional gaps, like a logger that was offline
                    continue
                day = i / 24.0
                # Slow seasonal cycle, small random walk and daily noise
                drift += rng.gauss(0, 0.004)
                level = (
                    level_mean
                    + 1.8 * math.sin(2 * math.pi * day / 365.0)
                    + drift
                    + 0.05 * math.sin(2 * math.pi * (i % 24) / 24.0)
                    + rng.gauss(0, 0.02)
                )
                temperature = (
                    temp_mean
                    + 1.2 * math.sin(2 * math.pi * day / 365.0 + 1.0)
                    + rng.gauss(0, 0.1)
                )
                conductivity = (
                    650 + 40 * math.sin(2 * math.pi * day / 365.0 + 2.0)
                    + rng.gauss(0, 5)
                )
                readings.append(GroundwaterReading(
                    location_site=site,
                    timestamp=timestamp,
                    water_level=round(level, 3),
                    water_temperature=round(temperature, 2),
                    electrical_conductivity=round(conductivity, 1),
                ))
            GroundwaterReading.objects.bulk_create(
                readings, batch_size=2000, ignore_conflicts=True)
            self.stdout.write(
                f'{name}: {len(readings)} hourly readings (site {site.id})')

    @staticmethod
    def get_or_create_station(name, lat, lon, location_type):
        site = LocationSite.objects.filter(name__iexact=name).first()
        if site:
            return site
        site = LocationSite.objects.create(
            name=name,
            latitude=lat,
            longitude=lon,
            geometry_point=Point(lon, lat),
            location_type=location_type,
            site_description=f'Groundwater monitoring station - {name}'
        )
        site_code, _ = generate_site_code(site, lat=lat, lon=lon)
        site.site_code = site_code
        site.save()
        return site
