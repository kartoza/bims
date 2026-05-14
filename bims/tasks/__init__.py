from bims.tasks.collection_record import *  # noqa
from bims.tasks.collection_record import cleanup_expired_download_files
from bims.tasks.search import *  # noqa
from bims.tasks.location_site import *  # noqa
from bims.tasks.chemical_record import *  # noqa
from bims.tasks.location_context import *  # noqa
from bims.tasks.source_reference import *  # noqa
from bims.tasks.taxa_upload import *  # noqa
from bims.tasks.collections_upload import *  # noqa
from bims.tasks.harvest_collections import *  # noqa
from bims.tasks.duplicate_records import *  # noqa
from bims.tasks.download_taxa_list import *  # noqa
from bims.tasks.taxon_extra_attribute import *  # noqa
from bims.tasks.clean_data import *  # noqa
from bims.tasks.email_csv import *  # noqa
from bims.tasks.taxa import *  # noqa
from bims.tasks.harvest_gbif_species import *  # noqa
from bims.tasks.location_site_summary import *  # noqa
from bims.tasks.checklist import *  # noqa
from bims.tasks.coldp_export import *  # noqa
from bims.tasks.cites_info import fetch_and_save_cites_listing
from bims.tasks.virtual_museum_import import import_data_task
from bims.tasks.taxon_group import delete_occurrences_by_taxon_group
from bims.tasks.caches import reset_caches
from bims.tasks.dataset import retrieve_datasets_from_gbif
from bims.tasks.harvest_schedule import run_scheduled_gbif_harvest
from bims.tasks.harvest_worms_species import harvest_worms_species
from bims.tasks.harvest_taxonworks_species import harvest_taxonworks_species
from bims.tasks.harvest_bims_species import harvest_bims_species
from bims.tasks.gbif_publish import run_scheduled_gbif_publish
from bims.tasks.prune_outside_boundary import prune_outside_boundary_gbif
from bims.tasks.module_summary import generate_module_summary
from bims.tasks.spatial_dashboard import *  # noqa


@shared_task(name='bims.tasks.test_celery', queue='update')
def test_celery():
    print('testing')
