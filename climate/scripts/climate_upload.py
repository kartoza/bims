# coding=utf-8
"""Climate data upload processor"""
import logging
from datetime import date
from django.contrib.gis.geos import Point
from bims.scripts.data_upload import DataCSVUpload
from bims.models.location_site import LocationSite, generate_site_code
from bims.models.location_type import LocationType
from climate.models import Climate

logger = logging.getLogger('bims')


class ClimateCSVUpload(DataCSVUpload):
    """Process climate data from CSV/Excel file."""

    model_name = 'climate/climate'

    def __init__(self, upload_session):
        self.upload_session = upload_session

    @staticmethod
    def row_value(row, key):
        """Get value from row, handling missing keys."""
        return str(row.get(key, '')).strip() if row.get(key) else ''

    def process_csv_dict_reader(self):
        """Process the CSV file."""
        headers = self.csv_dict_reader.fieldnames
        self.headers = headers

        # Validate required headers
        required_headers = [
            'Station', 'Latitude', 'Longitude', 'Year', 'Month', 'Day'
        ]
        missing_headers = [h for h in required_headers if h not in headers]
        if missing_headers:
            self.upload_session.error_notes = (
                f"Missing required headers: {', '.join(missing_headers)}"
            )
            self.upload_session.canceled = True
            self.upload_session.save()
            return

        row_number = 2  # Start at 2 (header is row 1)
        for row in self.csv_dict_reader:
            # Check if canceled
            from bims.models import UploadSession
            if UploadSession.objects.get(id=self.upload_session.id).canceled:
                logger.info('Upload canceled by user')
                return

            try:
                self.process_row(row, row_number)
            except Exception as e:
                logger.error(f"Error processing row {row_number}: {e}")
                self.error_file(row, f"Error processing row: {e}")
            row_number += 1

            # Update progress more frequently
            if row_number % 5 == 0 or row_number == self.total_rows:
                self.upload_session.progress = (
                    f"{row_number}/{self.total_rows} rows processed"
                )
                self.upload_session.save()

        # Create success and error files
        self.finish(headers)

    def process_row(self, row, row_number):
        """Process a single row."""
        # Extract basic fields
        station_name = self.row_value(row, 'Station')
        latitude = self.row_value(row, 'Latitude')
        longitude = self.row_value(row, 'Longitude')
        year = self.row_value(row, 'Year')
        month = self.row_value(row, 'Month')
        day = self.row_value(row, 'Day')

        # Validate required fields
        if not all([station_name, latitude, longitude, year, month, day]):
            self.error_file(row, "Missing required fields")
            return

        try:
            latitude = float(latitude)
            longitude = float(longitude)
            year = int(year)
            month = int(month)
            day = int(day)
            observation_date = date(year, month, day)
        except (ValueError, TypeError) as e:
            self.error_file(row, f"Invalid date or coordinates: {e}")
            return

        # Get or create location site
        location_site = self.get_or_create_location_site(
            station_name, latitude, longitude
        )

        if not location_site:
            self.error_file(row, "Could not create or find location site")
            return

        # Extract climate measurements
        avg_temp = self.get_float_value(row, 'Average daily temperature')
        avg_humidity = self.get_float_value(row, 'Average daily relative humidity')
        avg_windspeed = self.get_float_value(row, 'Average daily windspeed')
        daily_rainfall = self.get_float_value(row, 'Daily rainfall')
        max_temp = self.get_float_value(row, 'Maximum daily temperature')
        min_temp = self.get_float_value(row, 'Minimum daily temperature')
        max_humidity = self.get_float_value(row, 'Maximum relative humidity')
        min_humidity = self.get_float_value(row, 'Minimum relative humidity')
        flag = self.row_value(row, 'Flag')

        # Create or update climate record
        try:
            climate, created = Climate.objects.update_or_create(
                location_site=location_site,
                date=observation_date,
                defaults={
                    'station_name': station_name,
                    'avg_temperature': avg_temp,
                    'avg_humidity': avg_humidity,
                    'avg_windspeed': avg_windspeed,
                    'daily_rainfall': daily_rainfall,
                    'max_temperature': max_temp,
                    'min_temperature': min_temp,
                    'max_humidity': max_humidity,
                    'min_humidity': min_humidity,
                    'flag': flag
                }
            )
            self.success_file(row, climate.id)
            logger.info(f"{'Created' if created else 'Updated'} climate record: {climate.id}")
        except Exception as e:
            self.error_file(row, f"Error saving climate data: {e}")

    def get_float_value(self, row, key):
        """Get float value from row, return None if empty or invalid."""
        value = self.row_value(row, key)
        if value == '-999':
            return None
        if not value:
            return None
        try:
            return float(value)
        except ValueError:
            return None

    def get_or_create_location_site(self, station_name, latitude, longitude):
        """Get or create location site for the station."""
        try:
            # Try to find existing site by name and coordinates
            location_site = LocationSite.objects.filter(
                name__iexact=station_name,
                latitude=latitude,
                longitude=longitude
            ).first()

            if location_site:
                return location_site

            # Get or create location type for weather stations
            location_type, _ = LocationType.objects.get_or_create(
                name='Weather Station',
                defaults={
                    'allowed_geometry': 'POINT',
                    'description': 'Climate monitoring weather station'
                }
            )

            # Create new location site
            location_site = LocationSite.objects.create(
                name=station_name,
                latitude=latitude,
                longitude=longitude,
                geometry_point=Point(longitude, latitude),
                location_type=location_type,
                site_description=f'Climate monitoring station - {station_name}'
            )

            site_code, _ = generate_site_code(
                location_site,
                lat=latitude,
                lon=longitude,
            )
            location_site.site_code = site_code
            location_site.save()

            logger.info(f"Created new location site: {station_name} with site code: {site_code}")
            return location_site

        except Exception as e:
            logger.error(f"Error creating location site: {e}")
            return None

    def process_started(self):
        """Called when processing starts."""
        self.upload_session.progress = "Starting climate data upload..."
        self.upload_session.save()
        logger.info(f"Started climate upload for session {self.upload_session.id}")

    def process_ended(self):
        """Called when processing ends."""
        if not self.upload_session.canceled:
            self.upload_session.processed = True
            self.upload_session.progress = (
                f"Completed: {len(self.success_list)} records uploaded, "
                f"{len(self.error_list)} errors"
            )
        self.upload_session.save()
        logger.info(f"Finished climate upload for session {self.upload_session.id}")

        if not self.upload_session.canceled:
            self._clear_climate_search_results()

    def _clear_climate_search_results(self):
        """Delete SearchProcess records whose query references climate data."""
        from bims.models.search_process import SearchProcess
        deleted_count, _ = SearchProcess.objects.filter(
            search_raw_query__contains='climate_climate'
        ).delete()
        if deleted_count:
            logger.info(
                f"Cleared {deleted_count} climate-related search process record(s) "
                f"after climate data upload (session {self.upload_session.id})"
            )
