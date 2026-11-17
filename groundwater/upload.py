"""Upload of hourly groundwater readings from a CSV or Excel file."""
import logging

from django.contrib.gis.geos import Point
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from bims.models.location_site import LocationSite, generate_site_code
from bims.models.location_type import LocationType
from bims.models.upload_session import UploadSession
from bims.scripts.data_upload import DataCSVUpload
from groundwater.models import GroundwaterReading

logger = logging.getLogger('bims')

STATION = 'Station'
LATITUDE = 'Latitude'
LONGITUDE = 'Longitude'
TIMESTAMP = 'Timestamp'
FLAG = 'Flag'
REQUIRED_HEADERS = [STATION, LATITUDE, LONGITUDE, TIMESTAMP]

# Model field -> accepted headers. The long names are the headers of the
# hourly CSV download of the dashboard.
MEASUREMENT_HEADERS = {
    'water_level': [
        'Water level', 'Water level (m below ground)'],
    'water_temperature': [
        'Water temperature', 'Water temperature (°C)'],
    'electrical_conductivity': [
        'Electrical conductivity', 'Electrical conductivity (µS/cm)'],
}

# Placeholder used in source files for a missing value
MISSING = '-999'

# Readings saved per database query
BATCH_SIZE = 1000

LOCATION_TYPE_NAME = 'Groundwater Station'


class RowError(Exception):
    pass


def clean_header(header):
    return (header or '').strip().lstrip('﻿').strip()


def parse_timestamp(value):
    """Parse a timestamp such as ``2024-03-01 13:00``. Timestamps without a
    UTC offset are in the site time zone."""
    value = (value or '').strip()
    if not value:
        raise RowError('Missing timestamp')
    parsed = parse_datetime(value.replace('/', '-'))
    if parsed is None:
        raise RowError(
            f'Invalid timestamp "{value}", use YYYY-MM-DD HH:MM')
    if timezone.is_naive(parsed):
        parsed = timezone.make_aware(
            parsed, timezone.get_current_timezone())
    return parsed


def parse_measurement(value, header):
    """Return the value as a float, or None when it is empty or -999."""
    value = (value or '').strip()
    if not value or value == MISSING:
        return None
    try:
        number = float(value)
    except ValueError:
        raise RowError(f'{header} "{value}" is not a number')
    if number == float(MISSING):
        return None
    return number


def parse_coordinate(value, header, limit):
    try:
        number = float((value or '').strip())
    except ValueError:
        raise RowError(f'{header} "{value}" is not a number')
    if not -limit <= number <= limit:
        raise RowError(f'{header} {number} is out of range')
    return number


class GroundwaterCSVUpload(DataCSVUpload):
    """Process the file of a groundwater upload session.

    Readings are saved in batches: an hourly file has about 8,800 rows
    per station per year. A reading that already exists for the same
    station and timestamp is updated.
    """

    model_name = 'groundwater/groundwaterreading'

    def __init__(self, upload_session):
        self.upload_session = upload_session
        self.stations = {}
        self.measurement_headers = {}
        self.saved = 0

    def process_started(self):
        self.upload_session.progress = 'Processing groundwater data...'
        self.upload_session.save()

    def process_ended(self):
        if not self.upload_session.canceled:
            self.upload_session.processed = True
            self.upload_session.progress = (
                f'Completed: {len(self.success_list)} readings uploaded, '
                f'{len(self.error_list)} errors'
            )
        self.upload_session.save()

    def success_file(self, success_row, data_id):
        success_row['link'] = (
            f'http://{self.domain}/admin/groundwater/'
            f'groundwaterreading/{data_id}/change/'
            if data_id else ''
        )
        self.success_list.append(success_row)

    def process_csv_dict_reader(self):
        raw_headers = self.csv_dict_reader.fieldnames or []
        header_map = {h: clean_header(h) for h in raw_headers}
        self.headers = [header_map[h] for h in raw_headers]

        missing = [h for h in REQUIRED_HEADERS if h not in self.headers]
        if missing:
            self.upload_session.progress = (
                f'Missing required headers: {", ".join(missing)}')
            self.upload_session.error_notes = self.upload_session.progress
            self.upload_session.canceled = True
            self.upload_session.save()
            return

        for field, names in MEASUREMENT_HEADERS.items():
            header = next((n for n in names if n in self.headers), None)
            if header:
                self.measurement_headers[field] = header
        if not self.measurement_headers:
            self.upload_session.progress = (
                'No measurement columns found, add at least one of: ' +
                ', '.join(names[0] for names in MEASUREMENT_HEADERS.values())
            )
            self.upload_session.error_notes = self.upload_session.progress
            self.upload_session.canceled = True
            self.upload_session.save()
            return

        batch = {}
        processed = 0
        for raw_row in self.csv_dict_reader:
            row = {
                header_map.get(k, clean_header(k)): (v or '')
                for k, v in raw_row.items() if k is not None
            }
            processed += 1
            try:
                reading = self.build_reading(row)
            except RowError as e:
                self.error_file(row, str(e))
                continue
            key = (reading.location_site_id, reading.timestamp)
            if key in batch:
                self.error_file(
                    batch[key][0],
                    'Replaced by a later row with the same station and '
                    'timestamp')
            batch[key] = (row, reading)

            if len(batch) >= BATCH_SIZE:
                self.save_batch(batch)
                batch = {}
                if self.is_canceled():
                    return
                self.update_progress(processed)

        if batch:
            self.save_batch(batch)
        self.update_progress(processed)
        self.finish(self.headers)

    def build_reading(self, row):
        station_name = row.get(STATION, '').strip()
        if not station_name:
            raise RowError('Missing station')
        latitude = parse_coordinate(row.get(LATITUDE), LATITUDE, 90)
        longitude = parse_coordinate(row.get(LONGITUDE), LONGITUDE, 180)
        timestamp = parse_timestamp(row.get(TIMESTAMP))

        values = {
            field: parse_measurement(row.get(header), header)
            for field, header in self.measurement_headers.items()
        }
        if all(value is None for value in values.values()):
            raise RowError('No measurement values')

        site = self.get_or_create_station(station_name, latitude, longitude)
        return GroundwaterReading(
            location_site=site,
            timestamp=timestamp,
            flag=row.get(FLAG, '').strip()[:100],
            **values
        )

    def save_batch(self, batch):
        readings = [reading for _, reading in batch.values()]
        GroundwaterReading.objects.bulk_create(
            readings,
            update_conflicts=True,
            unique_fields=['location_site', 'timestamp'],
            update_fields=[
                'water_level', 'water_temperature',
                'electrical_conductivity', 'flag', 'updated_at',
            ],
        )
        for row, reading in batch.values():
            self.success_file(row, reading.pk)
        self.saved += len(readings)

    def get_or_create_station(self, name, latitude, longitude):
        """Find the station by name, or create it.

        An existing site is used as it is, its coordinates and location
        type are not changed.
        """
        key = name.lower()
        if key in self.stations:
            return self.stations[key]
        site = LocationSite.objects.filter(name__iexact=name).first()
        if not site:
            location_type, _ = LocationType.objects.get_or_create(
                name=LOCATION_TYPE_NAME,
                defaults={
                    'allowed_geometry': 'POINT',
                    'description': 'Groundwater monitoring station'
                }
            )
            site = LocationSite.objects.create(
                name=name,
                latitude=latitude,
                longitude=longitude,
                geometry_point=Point(longitude, latitude),
                location_type=location_type,
                site_description=f'Groundwater monitoring station - {name}'
            )
            site_code, _ = generate_site_code(
                site, lat=latitude, lon=longitude)
            site.site_code = site_code
            site.save()
            logger.info('Created groundwater station %s', name)
        self.stations[key] = site
        return site

    def update_progress(self, processed):
        self.upload_session.progress = f'{processed}/{self.total_rows}'
        self.upload_session.save()

    def is_canceled(self):
        canceled = UploadSession.objects.filter(
            id=self.upload_session.id, canceled=True).exists()
        if canceled:
            logger.info('Groundwater upload canceled by user')
        return canceled
