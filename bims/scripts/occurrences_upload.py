import csv
import uuid
import json
import logging

from preferences import preferences

from bims.scripts.collection_csv_keys import *  # noqa
from datetime import datetime
from dateutil.parser import parse as parse_datetime

from django.contrib.gis.geos import Point
from django.db.models import Q

from bims.models import (
    LocationType,
    LocationSite,
    Endemism,
    BiologicalCollectionRecord,
    SamplingMethod,
    Taxonomy,
    ChemicalRecord,
    Chem,
    Biotope,
    BIOTOPE_TYPE_BROAD,
    BIOTOPE_TYPE_SPECIFIC,
    BIOTOPE_TYPE_SUBSTRATUM,
    Survey,
    SurveyData,
    SurveyDataOption,
    SurveyDataValue,
    Hydroperiod,
    WetlandIndicatorStatus,
    RecordType,
    AbundanceType,
    SamplingEffortMeasure,
    Licence,
    ORIGIN_CATEGORIES,
)
from sass.models.river import River
from bims.models.taxon_origin import TaxonOrigin
from bims.signals.utils import disconnect_bims_signals, connect_bims_signals
from bims.utils.feature_info import get_feature_centroid
from bims.utils.user import create_users_from_string
from bims.scripts.data_upload import DataCSVUpload
from bims.tasks.location_site import update_location_context
from bims.scripts.collections_upload_source_reference import (
    process_source_reference
)
from bims.tasks.location_context import generate_spatial_scale_filter
from bims.tasks.source_reference import (
    generate_source_reference_filter
)
from bims.models.location_site import generate_site_code
from bims.location_site.river import fetch_river_name
from bims.utils.site_code import get_feature_data
from bims.models.location_context import LocationContext

logger = logging.getLogger("bims")

import time
import uuid
import logging
from logging import LoggerAdapter

logger = logging.getLogger("bims")


class _ContextAdapter(LoggerAdapter):
    def process(self, msg, kwargs):
        extra_in = kwargs.pop("extra", {}) or {}
        ctx = {**self.extra, **extra_in}
        prefix = (
            f"[occ:{ctx.get('run_id','-')}]"
            f"[row:{ctx.get('row_no','-')}]"
            f"[sp:{ctx.get('species','-')}]"
            f"[date:{ctx.get('date','-')}]"
            f"[site:{ctx.get('site','-')}]"
        )
        return f"{prefix} {msg}", kwargs


class OccurrenceProcessor(object):

    site_ids = []
    module_group = None
    fetch_location_context = True
    park_centroid = {}
    parks_data = {}
    section_data = {}

    def __init__(self, upload_session=None, log_every=200):
        self.upload_session = upload_session
        self.run_id = getattr(upload_session, "id", None) or uuid.uuid4().hex[:8]
        self._adapter = _ContextAdapter(logger, {"run_id": self.run_id, "row_no": "-", "species": "-", "date": "-", "site": "-"})
        self._start_ts = None
        self._row_no = 0
        self._log_every = log_every

        self._count_ok = 0
        self._count_err = 0
        self._count_skipped = 0
        self.survey = None

        self._file_logger = None
        self._file_handler = None
        self._setup_file_logger()

    def _setup_file_logger(self):
        """Create (or reuse) the log file on the upload session and attach a FileHandler."""
        if not self.upload_session or not self.upload_session.pk:
            return

        from django.core.files.base import ContentFile

        session = self.upload_session

        # Create the log file on first run; on resume the existing file is appended to.
        if not session.log_file:
            session.log_file.save(
                f"upload-session-{session.id}.log",
                ContentFile(
                    f"Upload session {session.id} started\n"
                    f"Category: {session.category}\n"
                    f"File: {session.process_file.name if session.process_file else '-'}\n"
                    "---\n"
                ),
                save=True,
            )

        log_path = session.log_file.path
        file_logger_name = f"bims.upload_session.{session.id}"
        self._file_logger = logging.getLogger(file_logger_name)
        self._file_logger.setLevel(logging.DEBUG)
        self._file_logger.propagate = False

        # Avoid duplicate handlers when restarted within the same process
        if not self._file_logger.handlers:
            handler = logging.FileHandler(log_path, mode="a", encoding="utf-8")
            handler.setFormatter(
                logging.Formatter("%(asctime)s %(levelname)-8s %(message)s")
            )
            self._file_logger.addHandler(handler)
            self._file_handler = handler

    def _teardown_file_logger(self):
        """Flush and close the FileHandler so the file is fully written."""
        if self._file_handler:
            self._file_handler.flush()
            self._file_handler.close()
            if self._file_logger:
                self._file_logger.removeHandler(self._file_handler)
            self._file_handler = None

    def _log(self, level, msg, **extra):
        if "row_no" not in extra:
            extra["row_no"] = self._row_no or "-"
        self._adapter.log(level, msg, extra=extra)
        if self._file_logger:
            formatted_msg, _ = self._adapter.process(msg, {"extra": extra})
            self._file_logger.log(level, formatted_msg)

    def _set_row_ctx(self, *, species=None, date=None, site=None):
        base = self._adapter.extra.copy()
        if species is not None:
            base["species"] = str(species or "-")
        if date is not None:
            base["date"] = str(date or "-")
        if site is not None:
            base["site"] = str(site or "-")
        base["row_no"] = self._row_no or "-"
        self._adapter.extra = base

    def start_process(self):
        disconnect_bims_signals()
        self._start_ts = time.time()
        self._log(logging.INFO, "Occurrence processing started")

    def update_location_site_context(self):
        self._log(logging.INFO, f"Queueing location context update for {len(self.site_ids)} site(s)")
        update_location_context.delay(
            location_site_id=",".join(self.site_ids),
            generate_site_code=True
        )

    def finish_process(self):
        if self.site_ids and self.fetch_location_context:
            self.update_location_site_context()

        self._log(logging.INFO, "Generating spatial and source reference filters…")
        generate_spatial_scale_filter()
        generate_source_reference_filter()

        connect_bims_signals()

        dur = time.time() - (self._start_ts or time.time())
        self._log(
            logging.INFO,
            f"Occurrence processing finished | ok={self._count_ok} err={self._count_err} skipped={self._count_skipped} duration={dur:.2f}s",
            row_no="-", species="-", date="-", site="-"
        )
        self._teardown_file_logger()

    # Called by subclass; also logs
    def handle_error(self, row, message):
        self._count_err += 1
        self._log(logging.ERROR, message)

    # Called by subclass; also logs
    def finish_processing_row(self, row, record):
        self._count_ok += 1
        self._log(logging.INFO, f"Saved record id={record.id} uuid={record.uuid}")

    def parse_date(self, row):
        def try_parse_date(date_str, date_format):
            try:
                return datetime.strptime(date_str, date_format)
            except ValueError:
                return None

        # Retrieve the date string from the row
        date_string = DataCSVUpload.row_value(row, SAMPLING_DATE)
        if not date_string:
            date_string = DataCSVUpload.row_value(row, SAMPLING_DATE_2)

        # If date_string is still None, handle the error
        if not date_string:
            self.handle_error(row=row, message="Missing sampling date")
            return None

        # Define possible date formats
        date_formats = [
            "%Y-%m-%d",
            "%Y/%m/%d",
            "%d/%m/%Y",
        ]

        # Attempt to parse the date string using the defined formats
        for date_format in date_formats:
            sampling_date = try_parse_date(date_string, date_format)
            if sampling_date:
                self._set_row_ctx(date=sampling_date.date())
                self._log(logging.DEBUG, f"Parsed date using format={date_format}")
                return sampling_date

        self.handle_error(row=row, message=f"Incorrect date format: {date_string}")
        return None

    def process_survey(
        self,
        record,
        location_site,
        sampling_date,
        collector,
        sampling_effort='',
        sampling_effort_link=None,
    ):
        """Process survey data.

        Rows that share the same site/date/collector but differ in
        sampling_effort value or measure are assigned to separate surveys.
        """
        base_filter = dict(
            site=location_site,
            date=sampling_date,
            collector_user=collector,
            owner=collector,
            validated=True,
        )
        candidate_surveys = Survey.objects.filter(**base_filter)

        matched_survey = None
        for survey in candidate_surveys:
            records = survey.biological_collection_record.all()
            if not records.exists():
                matched_survey = survey
                break
            if records.filter(
                sampling_effort=sampling_effort,
                sampling_effort_link=sampling_effort_link,
            ).exists():
                matched_survey = survey
                break

        if matched_survey:
            self.survey = matched_survey
            self._log(logging.DEBUG, f"Using survey id={self.survey.id}")
        else:
            self.survey = Survey.objects.create(**base_filter)
            self._log(
                logging.DEBUG,
                f"Created new survey id={self.survey.id} "
                f"(sampling_effort={sampling_effort!r}, "
                f"sampling_effort_link={sampling_effort_link})"
            )

        for survey_data_key in SURVEY_DATA:
            if survey_data_key in record and DataCSVUpload.row_value(record, survey_data_key):
                survey_data, _ = SurveyData.objects.get_or_create(
                    name=SURVEY_DATA[survey_data_key]
                )
                survey_option = SurveyDataOption.objects.filter(
                    option__iexact=DataCSVUpload.row_value(
                        record,
                        survey_data_key
                    ).strip(),
                    survey_data=survey_data
                )
                if not survey_option.exists():
                    survey_option = SurveyDataOption.objects.create(
                        options=DataCSVUpload.row_value(
                            record,
                            survey_data_key
                        ).strip(),
                        survey_data=survey_data
                    )
                else:
                    survey_option = survey_option[0]
                if survey_option:
                    SurveyDataValue.objects.get_or_create(
                        survey=self.survey,
                        survey_data=survey_data,
                        survey_data_option=survey_option,
                    )
                    self._log(logging.DEBUG, f"Survey data set: {SURVEY_DATA[survey_data_key]}")

    def location_site(self, record):
        """ Process location site data """
        location_site = None

        # -- Ecosystem type
        ecosystem_type = DataCSVUpload.row_value(
            record, ECOSYSTEM_TYPE
        )
        if not ecosystem_type:
            ecosystem_type = DataCSVUpload.row_value(record, ECOSYSTEM_TYPE_2)

        location_type, status = LocationType.objects.get_or_create(
            name="PointObservation",
            allowed_geometry="POINT"
        )
        site_description = DataCSVUpload.row_value(record, SITE_DESCRIPTION)
        refined_geo = DataCSVUpload.row_value(record, REFINED_GEO_ZONE)
        if not refined_geo:
            refined_geo = DataCSVUpload.row_value(record, ORIGINAL_GEO_ZONE)
        wetland_name = DataCSVUpload.row_value(record, WETLAND_NAME)
        user_wetland_name = DataCSVUpload.row_value(record, USER_WETLAND_NAME)
        user_hydrogeomorphic_type = DataCSVUpload.row_value(
            record,
            USER_HYDROGEOMORPHIC_TYPE
        )
        legacy_river_name = DataCSVUpload.row_value(record, USER_RIVER_NAME)
        if not legacy_river_name:
            legacy_river_name = DataCSVUpload.row_value(record, ORIGINAL_RIVER_NAME)

        user_river_or_wetland_name = DataCSVUpload.row_value(record, USER_RIVER_OR_WETLAND_NAME)

        if user_river_or_wetland_name:
            if ecosystem_type and ecosystem_type.lower() == "wetland":
                user_wetland_name = user_river_or_wetland_name
            else:
                legacy_river_name = user_river_or_wetland_name

        longitude = DataCSVUpload.row_value(record, LONGITUDE)
        latitude = DataCSVUpload.row_value(record, LATITUDE)

        park_name = DataCSVUpload.row_value(record, PARK_OR_MPA_NAME)
        section_name = DataCSVUpload.row_value(record, SECTION_NAME)

        accuracy_of_coordinates = DataCSVUpload.row_value(record, ACCURACY_OF_COORDINATES)
        if not accuracy_of_coordinates:
            accuracy_of_coordinates = 100

        # Resolve from section if no coordinates
        if not longitude and not latitude and section_name:
            section_file = preferences.SiteSetting.section_layer_csv
            if section_file:
                if not self.section_data:
                    with open(section_file.path, mode="r") as section_file_obj:
                        section_reader = csv.DictReader(section_file_obj, delimiter=",")
                        section_key = section_reader.fieldnames[0]
                        for section_row in section_reader:
                            _section_name = section_row[section_key].strip().lower()
                            lat = float(section_row["x"])
                            lon = float(section_row["y"])
                            self.section_data[_section_name] = {"lat": lat, "lon": lon}
                section_name_low = section_name.strip().lower()
                if section_name_low in self.section_data:
                    latitude = self.section_data[section_name_low]["lat"]
                    longitude = self.section_data[section_name_low]["lon"]
                    self._log(logging.DEBUG, f"Coords resolved from section '{section_name}' -> lat={latitude} lon={longitude}")

        # Resolve from park if still no coords
        if not longitude and not latitude and park_name:
            park_mpa_csv_file = preferences.SiteSetting.park_layer_csv
            wfs_url = preferences.SiteSetting.park_wfs_url
            layer_name = preferences.SiteSetting.park_wfs_layer_name
            attribute_key = preferences.SiteSetting.park_wfs_attribute_key
            attribute_value = park_name

            if park_mpa_csv_file:
                if not self.parks_data:
                    with open(park_mpa_csv_file.path, mode="r") as park_file:
                        park_reader = csv.DictReader(park_file, delimiter=",")
                        for park_row in park_reader:
                            _park_name = park_row["Park_Name"].strip().lower()
                            lat = float(park_row["x"])
                            lon = float(park_row["y"])
                            self.parks_data[_park_name] = {"lat": lat, "lon": lon}
                park_name_low = park_name.strip().lower()
                if park_name_low in self.parks_data:
                    latitude = self.parks_data[park_name_low]["lat"]
                    longitude = self.parks_data[park_name_low]["lon"]
                    self._log(logging.DEBUG, f"Coords resolved from park CSV '{park_name}' -> lat={latitude} lon={longitude}")
                else:
                    self.handle_error(row=record, message="Park or MPA name does not exist in the database")
                    return None
            else:
                if park_name in self.park_centroid:
                    latitude = self.park_centroid[park_name][0]
                    longitude = self.park_centroid[park_name][1]
                    self._log(logging.DEBUG, f"Coords resolved from cached centroid '{park_name}' -> lat={latitude} lon={longitude}")
                else:
                    # Check if there is already site with the same park name
                    site = LocationSite.objects.filter(name=park_name).first()
                    if site:
                        latitude = site.latitude
                        longitude = site.longitude
                        self.park_centroid[site.name] = [latitude, longitude]
                        self._log(logging.DEBUG, f"Coords resolved from existing site '{park_name}' -> lat={latitude} lon={longitude}")
                        # Check if site with same park name and accuracy of coordinates exists
                        site = LocationSite.objects.filter(
                            name=park_name,
                            accuracy_of_coordinates=accuracy_of_coordinates
                        ).exclude(site_code="").first()
                        if site:
                            self._set_row_ctx(site=site.site_code)
                            self._log(logging.INFO, f"Reusing existing site id={site.id} code={site.site_code}")
                            return site
                    else:
                        park_centroid = get_feature_centroid(
                            wfs_url,
                            layer_name,
                            attribute_key=attribute_key,
                            attribute_value=attribute_value
                        )
                        if park_centroid:
                            latitude = park_centroid[0]
                            longitude = park_centroid[1]
                            self.park_centroid[park_name] = park_centroid
                            self._log(logging.DEBUG, f"Coords resolved from WFS centroid '{park_name}' -> lat={latitude} lon={longitude}")
                        else:
                            self.handle_error(row=record, message="Park or MPA name does not exist in the database")
                            return None

        if not longitude or not latitude:
            self.handle_error(row=record, message="Missing latitude/longitude")
            return None

        try:
            latitude = float(latitude)
            longitude = float(longitude)
        except ValueError:
            self.handle_error(row=record, message="Incorrect latitude or longitude format")
            return None

        record_point = Point(longitude, latitude)

        # Create or get location site
        legacy_site_code = DataCSVUpload.row_value(record, USER_SITE_CODE)
        if not legacy_site_code:
            legacy_site_code = DataCSVUpload.row_value(record, ORIGINAL_SITE_CODE)

        location_site_name = ""
        if DataCSVUpload.row_value(record, LOCATION_SITE):
            location_site_name = DataCSVUpload.row_value(record, LOCATION_SITE)
        elif wetland_name:
            location_site_name = wetland_name
        elif park_name:
            location_site_name = park_name
        elif section_name:
            location_site_name = section_name

        # Find existing location site by data source site code
        data_source = (preferences.SiteSetting.default_data_source or '').upper()
        existing_site_code = DataCSVUpload.row_value(record, f"{data_source} Site Code") if data_source else ''
        if not existing_site_code:
            existing_site_code = DataCSVUpload.row_value(record, "Site Code")
        if existing_site_code:
            location_site = LocationSite.objects.filter(
                site_code__iexact=existing_site_code.strip()
            ).first()
            if location_site:
                self._set_row_ctx(site=location_site.site_code)
                self._log(logging.DEBUG, f"Matched existing site by site_code={existing_site_code.strip()}")

        # Find existing location site by lat and lon (starts-with)
        if not location_site:
            if len(str(latitude)) > 5 and len(str(longitude)) > 5:
                location_site = LocationSite.objects.filter(
                    latitude__startswith=latitude,
                    longitude__startswith=longitude,
                    ecosystem_type=ecosystem_type
                ).first()
                if location_site:
                    self._set_row_ctx(site=location_site.site_code)
                    self._log(logging.DEBUG, "Matched existing site by coordinate prefix")

        if not location_site:
            location_site = LocationSite.objects.filter(
                geometry_point=record_point,
                ecosystem_type=ecosystem_type
            ).first()
            if not location_site:
                location_site, status = LocationSite.objects.get_or_create(
                    geometry_point=record_point,
                    ecosystem_type=ecosystem_type,
                    location_type=location_type
                )
                self._log(logging.INFO, f"{'Created' if status else 'Selected'} site id={location_site.id}")

        # Update fields
        if not location_site.name and location_site_name:
            location_site.name = location_site_name
        if not location_site.location_type:
            location_site.location_type = location_type
        if not location_site.legacy_site_code and legacy_site_code:
            location_site.legacy_site_code = legacy_site_code
        if not location_site.site_description and site_description:
            location_site.site_description = site_description
        if refined_geo:
            location_site.refined_geomorphological = refined_geo
        if legacy_river_name:
            location_site.legacy_river_name = legacy_river_name
        if wetland_name or location_site.wetland_name:
            location_site.wetland_name = wetland_name
        if user_wetland_name or location_site.user_wetland_name:
            location_site.user_wetland_name = user_wetland_name
        if user_hydrogeomorphic_type or location_site.user_hydrogeomorphic_type:
            location_site.user_hydrogeomorphic_type = user_hydrogeomorphic_type
        river_name = ''
        if preferences.SiteSetting.default_data_source == 'fbis':
            if not location_site.river:
                fetched_river = fetch_river_name(
                    location_site.latitude,
                    location_site.longitude
                )
                if fetched_river:
                    river_name = fetched_river
                    rivers = River.objects.filter(
                        name=river_name
                    )
                    if rivers.exists():
                        river = rivers.first()
                    else:
                        river = River.objects.create(
                            name=river_name
                        )
                    location_site.river = river
                    self._log(
                        logging.DEBUG,
                        f"Fetched river_name={fetched_river} from vector layer"
                    )

            if not location_site.original_geomorphological:
                location_context = LocationContext.objects.filter(
                    site=location_site
                )
                geo_zone = location_context.value_from_key('geo_class_recoded')
                if not geo_zone or geo_zone == '-':
                    geo_zone = get_feature_data(
                        lon=location_site.longitude,
                        lat=location_site.latitude,
                        context_key='name',
                        layer_name='geomorphological',
                        tolerance=preferences.GeocontextSetting.tolerance,
                        location_site=location_site,
                    )
                if geo_zone and geo_zone != '-':
                    location_site.original_geomorphological = geo_zone
                    if not location_site.refined_geomorphological:
                        location_site.refined_geomorphological = geo_zone
                    self._log(
                        logging.DEBUG,
                        f"Fetched geo_zone={geo_zone} from vector layer"
                    )

        if not location_site.site_code:
            site_code, catchments_data = generate_site_code(
                location_site,
                lat=location_site.latitude,
                lon=location_site.longitude,
                river_name=river_name,
                ecosystem_type=location_site.ecosystem_type,
                wetland_name=user_wetland_name,
                **{
                    "site_desc": site_description,
                    "site_name": location_site_name
                }
            )
            location_site.site_code = site_code
            self._log(logging.DEBUG, f"Generated site_code={site_code}")
        if accuracy_of_coordinates:
            location_site.accuracy_of_coordinates = accuracy_of_coordinates
        location_site.save()

        self._set_row_ctx(site=location_site.site_code)
        return location_site

    def biotope(self, record, biotope_record_type, biotope_type):
        """Process biotope data"""
        biotope = None
        if (
            DataCSVUpload.row_value(record, biotope_record_type)
            and DataCSVUpload.row_value(record, biotope_record_type).lower() != "unspecified"
        ):
            try:
                biotope, biotope_created = Biotope.objects.get_or_create(
                    biotope_type=biotope_type,
                    name=DataCSVUpload.row_value(record, biotope_record_type)
                )
            except Biotope.MultipleObjectsReturned:
                biotopes = Biotope.objects.filter(
                    biotope_type=biotope_type,
                    name=DataCSVUpload.row_value(record, biotope_record_type)
                )
                if biotopes.filter(display_order__isnull=False).exists():
                    biotope = biotopes.filter(display_order__isnull=False)[0]
                else:
                    biotope = biotopes[0]
        return biotope

    def chemical_records(self, record, location_site, date):
        """Process chemical data"""
        chemical_units = {
            TEMP: TEMP,
            CONDUCTIVITY: CONDUCTIVITY,
            PH: PH,
            DISSOLVED_OXYGEN_MG: DISSOLVED_OXYGEN_MG,
            DISSOLVED_OXYGEN_PERCENT: DISSOLVED_OXYGEN_PERCENT,
            TURBIDITY: TURBIDITY,
            DEPTH_M: DEPTH_M,
            NBV: NBV,
            ORTHOPHOSPHATE: ORTHOPHOSPHATE,
            TOT: TOT,
            SILICA: SILICA,
            NH3_N: NH3_N,
            NH4_N: NH4_N,
            NO3_NO2_N: NO3_NO2_N,
            NO2_N: NO2_N,
            NO3_N: NO3_N,
            TIN: TIN,
            CHLA_B: CHLA_B,
            AFDM: AFDM
        }

        for chem_key in chemical_units:
            if chem_key not in record:
                continue
            chem_value = DataCSVUpload.row_value(record, chem_key).strip()
            if not chem_value:
                continue
            chem = Chem.objects.filter(
                chem_code__iexact=chemical_units[chem_key]
            )
            if chem.exists():
                chem = chem[0]
            else:
                chem = Chem.objects.create(
                    chem_code=chemical_units[chem_key],
                )
            chem_record, _ = ChemicalRecord.objects.get_or_create(
                date=date,
                chem=chem,
                location_site=location_site,
                survey=self.survey
            )
            chem_record.value = chem_value
            chem_record.save()

    def taxonomy(self, record):
        # if an endemism name exist in the row, check the endemism record
        # from database, if it exists, use that,
        # otherwise create a new one
        endemism = None
        if ENDEMISM in record and DataCSVUpload.row_value(record, ENDEMISM):
            endemism, endemism_created = Endemism.objects.get_or_create(
                name=DataCSVUpload.row_value(record, ENDEMISM)
            )
        # -- Processing taxon species --
        species_name = DataCSVUpload.row_value(record, SPECIES_NAME)
        taxon_rank = DataCSVUpload.row_value(record, TAXON_RANK).replace(" ", "").upper().strip()

        # enrich log context with species for downstream logs
        self._set_row_ctx(species=species_name)

        self._log(logging.DEBUG,
                  f"Lookup taxonomy rank={taxon_rank} name={species_name} group={self.module_group}")
        taxa = Taxonomy.objects.filter(
            canonical_name__iexact=species_name,
            rank=taxon_rank,
            taxongroup=self.module_group
        )
        if not taxa.exists():
            taxa = Taxonomy.objects.filter(
                legacy_canonical_name__icontains=species_name,
                rank=taxon_rank,
                taxongroup=self.module_group
            )
        if taxa.exists():
            taxonomy = taxa[0]
            self._log(logging.DEBUG,
                      f"Matched taxonomy id={taxonomy.id} status={taxonomy.taxonomic_status}")
        else:
            self.handle_error(row=record, message="Taxonomy does not exist for this group")
            return None

        if taxonomy and DataCSVUpload.row_value(record, SPECIES_NAME) not in str(taxonomy.canonical_name):
            taxonomy.legacy_canonical_name = DataCSVUpload.row_value(record, SPECIES_NAME)

        origin = None
        if ORIGIN in record and DataCSVUpload.row_value(record, ORIGIN):
            origin_key = ORIGIN_CATEGORIES.get(
                DataCSVUpload.row_value(record, ORIGIN).lower().strip(), ''
            )
            if origin_key:
                origin = TaxonOrigin.objects.filter(origin_key=origin_key).first()

        needs_save = False
        if endemism and taxonomy.endemism != endemism:
            taxonomy.endemism = endemism
            needs_save = True
            self._log(logging.DEBUG, "Updated taxonomy endemism")
        if origin and taxonomy.origin != origin:
            taxonomy.origin = origin
            needs_save = True
            self._log(logging.DEBUG, "Updated taxonomy origin")

        if needs_save:
            taxonomy.save()
        return taxonomy

    def process_data(self, row):
        # increment row counter and set early context
        self._row_no += 1
        self._set_row_ctx(
            species=DataCSVUpload.row_value(row, SPECIES_NAME),
            date=DataCSVUpload.row_value(row, SAMPLING_DATE),
            site=None
        )

        if self._row_no % self._log_every == 0:
            self._log(logging.INFO, f"Progress: processed {self._row_no} rows…")

        optional_data = {}

        try:
            # -- UUID
            uuid_value = ""
            if DataCSVUpload.row_value(row, UUID):
                try:
                    uuid.UUID(DataCSVUpload.row_value(row, UUID)[0:36])
                    uuid_value = DataCSVUpload.row_value(row, UUID)
                except ValueError:
                    self.handle_error(row=row, message="Bad UUID format")
                    return

            # -- Source reference
            message, source_reference = process_source_reference(
                reference=DataCSVUpload.row_value(row, SOURCE),
                reference_category=DataCSVUpload.row_value(row, REFERENCE_CATEGORY),
                doi=DataCSVUpload.row_value(row, DOI),
                document_title=DataCSVUpload.row_value(row, DOCUMENT_TITLE),
                document_link=DataCSVUpload.row_value(row, DOCUMENT_UPLOAD_LINK),
                document_url=DataCSVUpload.row_value(row, DOCUMENT_URL),
                document_author=DataCSVUpload.row_value(row, DOCUMENT_AUTHOR),
                source_year=DataCSVUpload.row_value(row, SOURCE_YEAR)
            )
            if message and not source_reference:
                self.handle_error(row=row, message=message)
                return
            else:
                optional_data["source_reference"] = source_reference
                if source_reference:
                    self._log(logging.DEBUG, f"Source reference resolved id={getattr(source_reference, 'id', None)}")

            # -- Sampling date
            sampling_date = self.parse_date(row)
            if not sampling_date:
                return

            # -- End embargo date
            end_embargo_date = None
            end_embargo_date_str = DataCSVUpload.row_value(row, END_EMBARGO_DATE)
            if end_embargo_date_str:
                try:
                    end_embargo_date = parse_datetime(
                        timestr=end_embargo_date_str, dayfirst=True)
                except Exception:
                    self.handle_error(
                        row=row,
                        message=f"Incorrect end embargo date format: {end_embargo_date_str}"
                    )
                    return

            # -- Licence
            licence = None
            licence_str = DataCSVUpload.row_value(row, LICENCE)
            if licence_str:
                licence_str = licence_str.strip().upper()
                licences = Licence.objects.filter(
                    identifier=licence_str
                )
                if not licences.exists():
                    licence = Licence.objects.create(
                        identifier=licence_str,
                        name=licence_str
                    )
                else:
                    licence = licences.first()

            if licence:
                optional_data["licence"] = licence

            # -- Processing Taxonomy
            taxonomy = self.taxonomy(row)
            if not taxonomy:
                return

            # -- Location site
            location_site = self.location_site(row)
            if not location_site:
                return

            # -- Processing collectors
            custodian = DataCSVUpload.row_value(row, CUSTODIAN)
            if not custodian:
                custodian = DataCSVUpload.row_value(row, CUSTODIAN_2)
            collector_owner_str = DataCSVUpload.row_value(row, COLLECTOR_OR_OWNER)
            if custodian and custodian != '-':
                optional_data["institution_id"] = custodian.strip()
            collectors = create_users_from_string(collector_owner_str)
            if not collectors:
                collectors = create_users_from_string(DataCSVUpload.row_value(row, COLLECTOR_OR_OWNER_2))
            if not collectors:
                self.handle_error(row=row, message="Missing collector/owner")
                return
            collector = collectors
            optional_data["collector"] = DataCSVUpload.row_value(row, COLLECTOR_OR_OWNER)
            if len(collectors) > 0:
                collector = collectors[0]
                optional_data["collector_user"] = collectors[0]
                optional_data["owner"] = collectors[0]
                # Add owner and creator to location site if missing
                if not location_site.owner:
                    location_site.owner = collectors[0]
                if not location_site.creator:
                    location_site.creator = collectors[0]
                location_site.save()
                if custodian:
                    for _collector in collectors:
                        _collector.organization = DataCSVUpload.row_value(row, CUSTODIAN)
                        _collector.save()

            # -- Optional data - Sampling effort (parsed early so process_survey
            #    can use the values to discriminate between surveys)
            sampling_effort = ""
            if SAMPLING_EFFORT_VALUE in row and DataCSVUpload.row_value(row, SAMPLING_EFFORT_VALUE):
                sampling_effort += DataCSVUpload.row_value(row, SAMPLING_EFFORT_VALUE)
            optional_data["sampling_effort"] = sampling_effort

            sampling_effort_measure = DataCSVUpload.row_value(row, SAMPLING_EFFORT)
            sampling_effort_link = None
            if sampling_effort_measure:
                if "min" in sampling_effort_measure.lower():
                    sampling_effort_link, _ = SamplingEffortMeasure.objects.get_or_create(name="Time(min)")
                elif (
                    "area" in sampling_effort_measure.lower()
                    or "m2" in sampling_effort_measure.lower()
                    or "meter" in sampling_effort_measure.lower()
                    or "metre" in sampling_effort_measure.lower()
                ):
                    sampling_effort_link, _ = SamplingEffortMeasure.objects.get_or_create(name="Area(m2)")
                elif "replicates" in sampling_effort_measure.lower():
                    sampling_effort_link, _ = SamplingEffortMeasure.objects.get_or_create(name="Replicates")
                else:
                    sampling_effort_link, _ = SamplingEffortMeasure.objects.get_or_create(
                        name=sampling_effort_measure
                    )
                optional_data["sampling_effort_link"] = sampling_effort_link

            # -- Get or create a survey (uses sampling effort to separate
            #    surveys that differ only in effort value/measure)
            self.process_survey(
                row,
                location_site,
                sampling_date,
                collector=collectors[0],
                sampling_effort=sampling_effort,
                sampling_effort_link=sampling_effort_link,
            )

            # -- Apply end embargo date to the survey
            if end_embargo_date and self.survey:
                Survey.objects.filter(id=self.survey.id).update(
                    end_embargo_date=end_embargo_date
                )

            # -- Optional data - Present
            if PRESENT in row:
                optional_data["present"] = bool(DataCSVUpload.row_value(row, PRESENT))

            # -- Optional data - Habitat
            habitat_value = DataCSVUpload.row_value(row, HABITAT)
            if HABITAT in row and habitat_value:
                habitat_choices = {v: k for k, v in BiologicalCollectionRecord.HABITAT_CHOICES}
                if habitat_value in habitat_choices:
                    habitat_value = habitat_choices[habitat_value]
                optional_data["collection_habitat"] = habitat_value

            # -- Optional data - Sampling method
            sampling_method = None
            if SAMPLING_METHOD in row and DataCSVUpload.row_value(row, SAMPLING_METHOD):
                if DataCSVUpload.row_value(row, SAMPLING_METHOD).lower() != "unspecified":
                    try:
                        sampling_method, sm_created = SamplingMethod.objects.get_or_create(
                            sampling_method=DataCSVUpload.row_value(row, SAMPLING_METHOD)
                        )
                    except SamplingMethod.MultipleObjectsReturned:
                        sampling_method = SamplingMethod.objects.filter(
                            sampling_method=DataCSVUpload.row_value(row, SAMPLING_METHOD)
                        )[0]
            if sampling_method:
                optional_data["sampling_method"] = sampling_method

            # -- Optional data - Processing biotope
            # Accept both "Broad biotope" and "Broad biotope/habitat" column names.
            optional_data["biotope"] = (
                self.biotope(row, BROAD_BIOTOPE, BIOTOPE_TYPE_BROAD)
                or self.biotope(row, BROAD_BIOTOPE_HABITAT, BIOTOPE_TYPE_BROAD)
            )
            optional_data["specific_biotope"] = (
                self.biotope(row, SPECIFIC_BIOTOPE, BIOTOPE_TYPE_SPECIFIC)
                or self.biotope(row, SPECIFIC_BIOTOPE_HABITAT, BIOTOPE_TYPE_SPECIFIC)
            )
            optional_data["substratum"] = self.biotope(row, SUBSTRATUM, BIOTOPE_TYPE_SUBSTRATUM)

            # -- Optional data - Abundance
            abundance_type = None
            abundance_number = None

            if DataCSVUpload.row_value(row, ABUNDANCE_MEASURE):
                abundance_type = DataCSVUpload.row_value(row, ABUNDANCE_MEASURE).lower()
                if "count" in abundance_type:
                    abundance_type = "number"
                elif "density" in abundance_type:
                    abundance_type = "density"
                elif "percentage" in abundance_type:
                    abundance_type = "percentage"

                abundance_type = AbundanceType.objects.filter(name__icontains=abundance_type).first()

            if DataCSVUpload.row_value(row, ABUNDANCE_VALUE):
                try:
                    abundance_number = float(DataCSVUpload.row_value(row, ABUNDANCE_VALUE))
                except ValueError:
                    pass
            if abundance_number is not None and abundance_number <= 0:
                self.handle_error(
                    row=row,
                    message=f"Abundance value '{abundance_number}' is invalid; "
                            f"abundance must be greater than zero."
                )
                return
            if abundance_number:
                optional_data["abundance_number"] = abundance_number
            if abundance_type:
                optional_data["abundance_type"] = abundance_type

            # -- Record type
            record_type_str = DataCSVUpload.row_value(row, RECORD_TYPE)
            if record_type_str:
                record_type = RecordType.objects.filter(name__iexact=record_type_str).first()
                if not record_type:
                    record_type = RecordType.objects.create(name=record_type_str)
            else:
                record_type = None
            optional_data["record_type"] = record_type

            # -- Wetland component
            hydroperiod = DataCSVUpload.row_value(row, HYDROPERIOD)
            wetland_indicator_status = DataCSVUpload.row_value(row, WETLAND_INDICATOR_STATUS)
            if hydroperiod:
                hydroperiod = Hydroperiod.objects.filter(name__iexact=hydroperiod).first()
            else:
                hydroperiod = None
            optional_data["hydroperiod"] = hydroperiod

            if wetland_indicator_status:
                wetland_indicator_status = WetlandIndicatorStatus.objects.filter(
                    name__iexact=wetland_indicator_status
                ).first()
            else:
                wetland_indicator_status = None
            optional_data["wetland_indicator_status"] = wetland_indicator_status

            # -- End embargo date on the occurrence record
            if end_embargo_date:
                optional_data["end_embargo_date"] = end_embargo_date

            # -- Processing chemical records
            self.chemical_records(row, location_site, sampling_date)

            species_name = DataCSVUpload.row_value(row, VERBATUM_NAME)
            if not species_name:
                species_name = DataCSVUpload.row_value(row, SPECIES_NAME)

            certainty_of_identification = DataCSVUpload.row_value(row, CERTAINTY_OF_IDENTIFICATION)
            date_accuracy = DataCSVUpload.row_value(row, DATE_ACCURACY)

            data_type = DataCSVUpload.row_value(row, DATA_TYPE)
            if data_type:
                data_type = data_type.lower()
                if "public" in data_type:
                    data_type = "public"
                elif "private" in data_type:
                    data_type = "private"
                elif "sensitive" in data_type:
                    data_type = "sensitive"
                else:
                    data_type = ""

            identified_by = DataCSVUpload.row_value(row, IDENTIFIED_BY)
            ecosystem_type = (
                    DataCSVUpload.row_value(row, ECOSYSTEM_TYPE) or
                    DataCSVUpload.row_value(row, ECOSYSTEM_TYPE_2)
            )

            record = None
            defaults = {
                "site": location_site,
                "original_species_name": species_name,
                "collection_date": sampling_date,
                "taxonomy": taxonomy,
                "collector_user": collector,
                "survey": self.survey,
                "validated": True,
                "certainty_of_identification": (certainty_of_identification if certainty_of_identification else ""),
                "date_accuracy": date_accuracy.lower() if date_accuracy else "",
                "data_type": data_type,
                "identified_by": identified_by if identified_by else "",
                "ecosystem_type": ecosystem_type,
                "additional_data": json.dumps(row),
            }

            defaults.update(optional_data)
            if self.module_group:
                defaults["module_group"] = self.module_group

            try:
                if uuid_value:
                    canonical_uuid = str(uuid.UUID(uuid_value))
                    record, created = BiologicalCollectionRecord.objects.update_or_create(
                        uuid=canonical_uuid,
                        defaults=defaults
                    )
                    self._log(logging.DEBUG, f"Upsert BCR via UUID: {'create' if created else 'update'}")
                    row[UUID] = record.uuid
                else:
                    record = BiologicalCollectionRecord.objects.create(**defaults)
                    self._log(logging.DEBUG, "Create BCR: no UUID provided")
                    row[UUID] = record.uuid
            except Exception as e:
                self.handle_error(row=row, message=str(e))
                return

            if str(record.site.id) not in self.site_ids:
                self.site_ids.append(str(record.site.id))

            self.finish_processing_row(row, record)

        except Exception as e:
            # Catch any unexpected row crash so the loop can continue
            self.handle_error(row=row, message=f"Unhandled error: {e}")


class OccurrencesCSVUpload(DataCSVUpload, OccurrenceProcessor):
    model_name = "biologicalcollectionrecord"

    def process_started(self):
        # Initialize OccurrenceProcessor’s runtime state for this session
        OccurrenceProcessor.__init__(self, upload_session=getattr(self, "upload_session", None))
        self.start_process()

    def process_ended(self):
        self.finish_process()

    def process_row(self, row):
        self.module_group = self.upload_session.module_group
        self.process_data(row)

    def handle_error(self, row, message):
        # Log + write to error file
        super().handle_error(row, message)
        self.error_file(
            error_row=row,
            error_message=message
        )

    def finish_processing_row(self, row, record):
        # Log + write to success file
        super().finish_processing_row(row, record)
        self.success_file(
            success_row=row,
            data_id=record.id
        )
