# coding=utf-8
import logging

from celery import shared_task

logger = logging.getLogger(__name__)


@shared_task(name='bims.tasks.spatial_dashboard_cons_status', queue='search')
def spatial_dashboard_cons_status(search_parameters=None, search_process_id=None):
    from bims.utils.celery import memcache_lock
    from bims.api_views.location_site_overview import LocationSiteOverviewData
    from bims.models.search_process import (
        SearchProcess,
        SEARCH_PROCESSING,
        SEARCH_FINISHED
    )

    if search_parameters is None:
        search_parameters = {}

    try:
        search_process = SearchProcess.objects.get(id=search_process_id)
    except SearchProcess.DoesNotExist:
        return

    lock_id = '{0}-lock-{1}'.format(
        search_process.file_path,
        search_process.process_id
    )
    oid = '{0}'.format(search_process.process_id)
    with memcache_lock(lock_id, oid) as acquired:
        if acquired:
            search_process.set_status(SEARCH_PROCESSING)

            overview_data = LocationSiteOverviewData()
            if search_process.requester and 'requester' not in search_parameters:
                search_parameters['requester'] = search_process.requester.id
            overview_data.search_filters = search_parameters

            # Categories excluded from the conservation status chart
            EXCLUDED_CATEGORIES = {
                'DD', 'DDD', 'DDT', 'NE', '',
            }

            biodiversity_data = overview_data.biodiversity_data() or {}
            modules = []
            for module_name, module_data in biodiversity_data.items():
                cons_status = module_data.get(
                    LocationSiteOverviewData.GROUP_CONS_STATUS, []
                )
                cleaned = []
                for item in cons_status:
                    category = item.get('iucn_category') or ''
                    if category in EXCLUDED_CATEGORIES:
                        continue
                    cleaned.append({
                        'name': item.get('name') or category,
                        'count': item.get('count', 0),
                        'colour': item.get('colour'),
                        'category': category,
                    })
                modules.append({
                    'name': module_name,
                    'cons_status': cleaned
                })

            results = {
                'modules': modules
            }
            search_process.set_status(SEARCH_FINISHED, False)
            search_process.save_to_file(results)
            return
    logger.info(
        'Search %s is already being processed by another worker',
        search_process.process_id
    )


def _compute_rli(taxa_to_modules, year_taxa_statuses, dd_categories,
                 use_fixed_pool=False):
    """Compute RLI values from per-year per-taxon statuses.

    Args:
        taxa_to_modules: dict mapping taxonomy_id -> set of module names
        year_taxa_statuses: dict mapping year -> list of (taxonomy_id, category)
        dd_categories: set of DD category codes
        use_fixed_pool: if True, fix the denominator species count (N) from the
            first assessment year so it remains constant across all years.
            This implements the IUCN RLI rule that the species pool is anchored
            to the first year of assessment.

    Returns:
        (per_module_year, aggregate_year) dicts with RLI results.
    """
    from collections import defaultdict

    # RLI category weights
    # Standard IUCN: LC=0, NT=1, VU=2, EN=3, CR=4, EW/EX=5
    # South African national: RE=5, CR PE=4, CA=0, RA=0, D=1
    # Legacy IUCN: LR/lc=0, LR/nt=1, LR/cd=1
    RLI_WEIGHTS = {
        'LC': 0, 'LR/lc': 0,
        'NT': 1, 'LR/nt': 1, 'LR/cd': 1,
        'VU': 2,
        'EN': 3,
        'CR': 4, 'CR PE': 4,
        'EW': 5,
        'EX': 5, 'RE': 5,
        'CA': 0, 'RA': 0, 'D': 1,
    }
    EXCLUDED_CATEGORIES = {'DD', 'DDD', 'DDT', 'NE', ''}
    W_MAX = 5

    per_module_year = {}
    aggregate_year = {}

    module_n_fixed = {}
    agg_n_fixed = None
    if use_fixed_pool and year_taxa_statuses:
        first_year = min(year_taxa_statuses.keys())
        _mod_counts = defaultdict(int)
        _agg_count = 0
        for tid, category in year_taxa_statuses[first_year]:
            if category in EXCLUDED_CATEGORIES:
                continue
            if RLI_WEIGHTS.get(category) is None:
                continue
            modules_for_tid = taxa_to_modules.get(tid, {'Unknown'})
            for mod in modules_for_tid:
                _mod_counts[mod] += 1
            _agg_count += 1
        module_n_fixed = dict(_mod_counts)
        agg_n_fixed = _agg_count

    for year, taxa_statuses in year_taxa_statuses.items():
        module_data = defaultdict(lambda: {
            'assessed': 0, 'weighted': 0, 'dd': 0,
            'categories': defaultdict(int),
        })
        agg = {
            'assessed': 0, 'weighted': 0, 'dd': 0,
            'categories': defaultdict(int),
        }

        for tid, category in taxa_statuses:
            modules = taxa_to_modules.get(tid, {'Unknown'})

            if category in EXCLUDED_CATEGORIES:
                if category in dd_categories:
                    for mod in modules:
                        module_data[mod]['dd'] += 1
                    agg['dd'] += 1
                continue

            weight = RLI_WEIGHTS.get(category)
            if weight is None:
                continue

            for mod in modules:
                module_data[mod]['assessed'] += 1
                module_data[mod]['weighted'] += weight
                module_data[mod]['categories'][category] += 1

            agg['assessed'] += 1
            agg['weighted'] += weight
            agg['categories'][category] += 1

        for mod, data in module_data.items():
            n = module_n_fixed.get(
                mod, data['assessed']) if use_fixed_pool else data['assessed']
            if n > 0:
                rli = 1 - (data['weighted'] / (W_MAX * n))
                per_module_year[(mod, year)] = {
                    'rli': round(rli, 4),
                    'assessed': data['assessed'],
                    'dd': data['dd'],
                    'categories': dict(data['categories']),
                }

        n_agg = agg_n_fixed if (use_fixed_pool and agg_n_fixed is not None) else agg['assessed']
        if n_agg > 0:
            rli = 1 - (agg['weighted'] / (W_MAX * n_agg))
            aggregate_year[year] = {
                'rli': round(rli, 4),
                'assessed': agg['assessed'],
                'dd': agg['dd'],
                'categories': dict(agg['categories']),
            }

    return per_module_year, aggregate_year


def _build_rli_output(per_module_year, aggregate_year):
    """Format RLI computation results into the API output structure."""
    module_series = {}
    for (mod, year), data in per_module_year.items():
        module_series.setdefault(mod, []).append({
            'year': int(year),
            'value': data['rli'],
            'num_assessed': data['assessed'],
            'num_dd': data['dd'],
            'categories': data['categories'],
        })

    series = []
    for mod, points in module_series.items():
        # Skip modules where no point has any real IUCN category assigned
        # (all taxa were Not Evaluated, NE, NA, or otherwise unrecognised).
        # NE/''/unknown taxa are excluded before populating `categories`, so
        # an empty categories dict across all points means no real assessment.
        if not any(point['categories'] for point in points):
            continue
        series.append({
            'name': mod,
            'points': sorted(points, key=lambda x: x['year']),
        })

    aggregate = []
    for year, data in aggregate_year.items():
        aggregate.append({
            'year': int(year),
            'value': data['rli'],
            'num_assessed': data['assessed'],
            'num_dd': data['dd'],
            'categories': data['categories'],
        })
    aggregate = sorted(aggregate, key=lambda x: x['year'])

    return series, aggregate


@shared_task(name='bims.tasks.spatial_dashboard_rli', queue='search')
def spatial_dashboard_rli(search_parameters=None, search_process_id=None):
    """Calculate the Red List Index (RLI) for species.

    The RLI tracks changes in species extinction risk over time using
    IUCN assessment history. Category weights range from 0 (Least Concern)
    to 5 (Extinct/Extinct in the Wild). Data Deficient and Not Evaluated
    species are excluded from the index but DD counts are reported for
    confidence interval estimation.

    Uses IUCNAssessment records when available for proper temporal tracking;
    falls back to current taxonomy IUCN status otherwise.
    """
    from collections import defaultdict
    from datetime import date
    from django.db.models import Q
    from bims.utils.celery import memcache_lock
    from bims.api_views.search import CollectionSearch
    from bims.models import Taxonomy
    from bims.models.iucn_assessment import IUCNAssessment
    from bims.models.search_process import (
        SearchProcess,
        SEARCH_PROCESSING,
        SEARCH_FINISHED
    )

    DD_CATEGORIES = {'DD', 'DDD', 'DDT'}

    if search_parameters is None:
        search_parameters = {}

    try:
        search_process = SearchProcess.objects.get(id=search_process_id)
    except SearchProcess.DoesNotExist:
        return

    lock_id = '{0}-lock-{1}'.format(
        search_process.file_path,
        search_process.process_id
    )
    oid = '{0}'.format(search_process.process_id)
    with memcache_lock(lock_id, oid) as acquired:
        if acquired:
            search_process.set_status(SEARCH_PROCESSING)

            if search_process.requester and 'requester' not in search_parameters:
                search_parameters['requester'] = search_process.requester.id

            search = CollectionSearch(search_parameters)
            collection_results = search.process_search()

            SPECIES_RANKS = ['SPECIES', 'SUBSPECIES', 'VARIETY']
            taxa_modules_qs = collection_results.filter(
                taxonomy__taxonomic_status='ACCEPTED',
                taxonomy__rank__in=SPECIES_RANKS,
                taxonomy__origin__origin_key='indigenous',
                taxonomy__include_in_rli=True,
            ).values(
                'taxonomy_id', 'module_group__name'
            ).distinct()

            taxa_to_modules = defaultdict(set)
            for row in taxa_modules_qs:
                tid = row['taxonomy_id']
                module = row['module_group__name'] or 'Unknown'
                taxa_to_modules[tid].add(module)

            taxonomy_ids = list(taxa_to_modules.keys())

            if not taxonomy_ids:
                results = {
                    'series': [],
                    'aggregate': [],
                    'metadata': {
                        'total_taxa': 0,
                        'total_assessed': 0,
                        'total_dd': 0,
                        'total_ne': 0,
                    }
                }
                search_process.set_status(SEARCH_FINISHED, False)
                search_process.save_to_file(results)
                return

            assessments = list(
                IUCNAssessment.objects.filter(
                    taxonomy_id__in=taxonomy_ids,
                    year_published__isnull=False,
                ).values(
                    'taxonomy_id',
                    'year_published',
                    'red_list_category_code',
                ).order_by('taxonomy_id', 'year_published')
            )

            taxon_timelines = defaultdict(list)
            all_years = set()
            for a in assessments:
                cat = (a['red_list_category_code'] or '').strip()
                year = a['year_published']
                if year and cat:
                    taxon_timelines[a['taxonomy_id']].append((year, cat))
                    all_years.add(year)

            if all_years:
                # Primary path: compute RLI from assessment history.
                # No back-casting: only species actually assessed in a given
                # year contribute to that year's calculation. The species pool
                # (denominator N) is fixed to the count from the first year of
                # assessment and does not change in subsequent years.
                year_taxa_statuses = defaultdict(list)
                for tid, timeline in taxon_timelines.items():
                    for assess_year, cat in timeline:
                        year_taxa_statuses[assess_year].append((tid, cat))

                per_module_year, aggregate_year = _compute_rli(
                    taxa_to_modules, dict(year_taxa_statuses), DD_CATEGORIES,
                    use_fixed_pool=True,
                )
            else:
                # Fallback: compute a current snapshot RLI from
                # the taxonomy's current iucn_status
                taxa_with_status = Taxonomy.objects.filter(
                    id__in=taxonomy_ids,
                    iucn_status__isnull=False,
                ).values('id', 'iucn_status__category')

                current_year = date.today().year
                statuses = []
                for row in taxa_with_status:
                    cat = row['iucn_status__category'] or ''
                    statuses.append((row['id'], cat))

                year_taxa_statuses = {current_year: statuses}
                per_module_year, aggregate_year = _compute_rli(
                    taxa_to_modules, year_taxa_statuses, DD_CATEGORIES
                )

            series, aggregate = _build_rli_output(
                per_module_year, aggregate_year
            )

            # Metadata: overall DD and NE counts for the taxon set
            total_dd = Taxonomy.objects.filter(
                id__in=taxonomy_ids,
                iucn_status__category__in=list(DD_CATEGORIES),
            ).count()
            total_ne = Taxonomy.objects.filter(
                id__in=taxonomy_ids,
            ).filter(
                Q(iucn_status__category='NE') |
                Q(iucn_status__isnull=True) |
                Q(iucn_status__category='')
            ).count()

            results = {
                'series': series,
                'aggregate': aggregate,
                'metadata': {
                    'total_taxa': len(taxonomy_ids),
                    'total_dd': total_dd,
                    'total_ne': total_ne,
                }
            }
            search_process.set_status(SEARCH_FINISHED, False)
            search_process.save_to_file(results)
            return
    logger.info(
        'Search %s is already being processed by another worker',
        search_process.process_id
    )


@shared_task(name='bims.tasks.spatial_dashboard_map', queue='search')
def spatial_dashboard_map(search_parameters=None, search_process_id=None):
    from bims.utils.celery import memcache_lock
    from bims.api_views.search import CollectionSearch
    from bims.models.search_process import (
        SearchProcess,
        SEARCH_PROCESSING,
        SEARCH_FINISHED
    )

    if search_parameters is None:
        search_parameters = {}

    try:
        search_process = SearchProcess.objects.get(id=search_process_id)
    except SearchProcess.DoesNotExist:
        return

    lock_id = '{0}-lock-{1}'.format(
        search_process.file_path,
        search_process.process_id
    )
    oid = '{0}'.format(search_process.process_id)
    with memcache_lock(lock_id, oid) as acquired:
        if acquired:
            search_process.set_status(SEARCH_PROCESSING)

            if search_process.requester and 'requester' not in search_parameters:
                search_parameters['requester'] = search_process.requester.id

            search = CollectionSearch(search_parameters)
            search.process_search()

            if search.location_sites_raw_query:
                search_process.set_search_raw_query(
                    search.location_sites_raw_query
                )
                search_process.create_view()
                view_name = search_process.process_id
            else:
                view_name = None

            results = {
                'extent': search.extent(),
                'sites_raw_query': view_name
            }
            search_process.set_status(SEARCH_FINISHED, False)
            search_process.save_to_file(results)
            return
    logger.info(
        'Search %s is already being processed by another worker',
        search_process.process_id
    )


@shared_task(name='bims.tasks.spatial_dashboard_summary', queue='search')
def spatial_dashboard_summary(search_parameters=None, search_process_id=None):
    from django.db.models import Case, When, Value, F, Count, CharField
    from bims.models import TaxonGroup, IUCNStatus
    from bims.utils.celery import memcache_lock
    from bims.api_views.search import CollectionSearch
    from bims.models.search_process import (
        SearchProcess,
        SEARCH_PROCESSING,
        SEARCH_FINISHED
    )

    if search_parameters is None:
        search_parameters = {}

    try:
        search_process = SearchProcess.objects.get(id=search_process_id)
    except SearchProcess.DoesNotExist:
        return

    lock_id = '{0}-lock-{1}'.format(
        search_process.file_path,
        search_process.process_id
    )
    oid = '{0}'.format(search_process.process_id)
    with memcache_lock(lock_id, oid) as acquired:
        if acquired:
            search_process.set_status(SEARCH_PROCESSING)

            if search_process.requester and 'requester' not in search_parameters:
                search_parameters['requester'] = search_process.requester.id

            search = CollectionSearch(search_parameters)
            collection_results = search.process_search()

            modules_qs = TaxonGroup.objects.filter(
                category='SPECIES_MODULE'
            ).order_by('display_order')
            module_names = list(modules_qs.values_list('name', flat=True))

            module_field = Case(
                When(module_group__name__isnull=True, then=Value('Unknown')),
                default=F('module_group__name'),
                output_field=CharField()
            )

            number_of_taxa = collection_results.annotate(
                module_name=module_field
            ).values(
                'module_name'
            ).annotate(
                count=Count('taxonomy_id', distinct=True)
            ).values('module_name', 'count')

            origin_field = Case(
                When(taxonomy__origin__isnull=True, then=Value('Unknown')),
                When(taxonomy__origin__origin_key='unknown', then=Value('Unknown')),
                default=F('taxonomy__origin__category'),
                output_field=CharField()
            )

            origin_counts = collection_results.annotate(
                module_name=module_field,
                origin_name=origin_field
            ).values(
                'module_name', 'origin_name'
            ).annotate(
                count=Count('taxonomy_id', distinct=True)
            ).values('module_name', 'origin_name', 'count')

            endemism_field = Case(
                When(taxonomy__endemism__name__isnull=True, then=Value('Unknown')),
                default=F('taxonomy__endemism__name'),
                output_field=CharField()
            )
            endemism_counts = collection_results.annotate(
                module_name=module_field,
                endemism_name=endemism_field
            ).values(
                'module_name', 'endemism_name'
            ).annotate(
                count=Count('taxonomy_id', distinct=True)
            ).values('module_name', 'endemism_name', 'count')

            iucn_labels = dict(IUCNStatus.CATEGORY_CHOICES)

            global_cons_counts = collection_results.filter(
                taxonomy__iucn_status__national=False
            ).annotate(
                module_name=module_field,
                cons_name=Case(
                    When(taxonomy__iucn_status__category__isnull=True, then=Value('NE')),
                    default=F('taxonomy__iucn_status__category'),
                    output_field=CharField()
                )
            ).values(
                'module_name', 'cons_name'
            ).annotate(
                count=Count('taxonomy_id', distinct=True)
            ).values('module_name', 'cons_name', 'count')

            national_cons_counts = collection_results.filter(
                taxonomy__iucn_status__national=True
            ).annotate(
                module_name=module_field,
                cons_name=Case(
                    When(taxonomy__iucn_status__category__isnull=True, then=Value('NE')),
                    default=F('taxonomy__iucn_status__category'),
                    output_field=CharField()
                )
            ).values(
                'module_name', 'cons_name'
            ).annotate(
                count=Count('taxonomy_id', distinct=True)
            ).values('module_name', 'cons_name', 'count')

            def rows_from_counts(rows, label_key, label_map=None):
                matrix = {}
                for row in rows:
                    raw_label = row[label_key] or 'Unknown'
                    if label_map and raw_label in label_map:
                        label = label_map[raw_label]
                    else:
                        label = raw_label
                    module = row['module_name'] or 'Unknown'
                    matrix.setdefault(label, {})
                    matrix[label][module] = row['count']
                return matrix

            summary = {
                'modules': module_names,
                'overview': {
                    'Number of Taxa': {
                        row['module_name']: row['count'] for row in number_of_taxa
                    }
                },
                'origin': rows_from_counts(origin_counts, 'origin_name'),
                'endemism': rows_from_counts(endemism_counts, 'endemism_name'),
                'cons_status_global': rows_from_counts(global_cons_counts, 'cons_name', iucn_labels),
                'cons_status_national': rows_from_counts(national_cons_counts, 'cons_name', iucn_labels)
            }

            for section_key in ['overview', 'origin', 'endemism', 'cons_status_global', 'cons_status_national']:
                section = summary.get(section_key, {})
                if isinstance(section, dict):
                    for row_key, values in section.items():
                        if isinstance(values, dict):
                            for module in module_names:
                                values.setdefault(module, 0)

            results = summary
            search_process.set_status(SEARCH_FINISHED, False)
            search_process.save_to_file(results)
            return
    logger.info(
        'Search %s is already being processed by another worker',
        search_process.process_id
    )


@shared_task(name='bims.tasks.spatial_dashboard_species_download', queue='search')
def spatial_dashboard_species_download(search_parameters=None, search_process_id=None):
    import csv
    from bims.utils.celery import memcache_lock
    from bims.api_views.search import CollectionSearch
    from bims.models.taxonomy import Taxonomy
    from bims.models.iucn_status import IUCNStatus
    from bims.models.location_context import LocationContext
    from bims.models.location_context_group import LocationContextGroup
    from bims.views.download_csv_taxa_list import is_sanparks_project
    from bims.serializers.bio_collection_serializer import SANPARK_PARK_NAME
    from bims.models.search_process import (
        SearchProcess,
        SEARCH_PROCESSING,
        SEARCH_FINISHED
    )
    from django.db.models import Q

    PARK_GROUP_KEYS = {
        'park_or_mpa_name', 'park_or_mpa',
        'parks_and_mpas', 'sanparks_and_mpas',
        'sanparks_mpas', 'parks_mpas'
    }

    if search_parameters is None:
        search_parameters = {}

    try:
        search_process = SearchProcess.objects.get(id=search_process_id)
    except SearchProcess.DoesNotExist:
        return

    lock_id = '{0}-lock-{1}'.format(
        search_process.file_path,
        search_process.process_id
    )
    oid = '{0}'.format(search_process.process_id)
    with memcache_lock(lock_id, oid) as acquired:
        if acquired:
            search_process.set_status(SEARCH_PROCESSING)

            if search_process.requester and 'requester' not in search_parameters:
                search_parameters['requester'] = search_process.requester.id

            search = CollectionSearch(search_parameters)
            collection_results = search.process_search()

            park_group = LocationContextGroup.objects.filter(
                Q(key__in=list(PARK_GROUP_KEYS)) | Q(name__iexact=SANPARK_PARK_NAME)
            ).first()

            taxon_parks = {}
            if park_group:
                site_ids = list(
                    collection_results.values_list('site_id', flat=True).distinct()
                )
                site_park = {}
                for lc in LocationContext.objects.filter(
                    site_id__in=site_ids,
                    group=park_group
                ).values('site_id', 'value'):
                    val = (lc['value'] or '').strip()
                    if val:
                        site_park[lc['site_id']] = val

                for row in collection_results.values(
                    'taxonomy_id', 'site_id'
                ).distinct():
                    tid = row['taxonomy_id']
                    park = site_park.get(row['site_id'], '')
                    if park:
                        taxon_parks.setdefault(tid, set()).add(park)
            else:
                for row in collection_results.values(
                    'taxonomy_id', 'site__name'
                ).distinct():
                    tid = row['taxonomy_id']
                    name = (row['site__name'] or '').strip()
                    if name:
                        taxon_parks.setdefault(tid, set()).add(name)

            taxa_ids = list(
                collection_results.values_list('taxonomy_id', flat=True).distinct()
            )

            category_labels = dict(IUCNStatus.CATEGORY_CHOICES)

            taxa = Taxonomy.objects.filter(
                id__in=taxa_ids
            ).select_related(
                'iucn_status',
            ).order_by('canonical_name')

            location_col = 'Park Name' if is_sanparks_project() else 'Site Name'

            csv_path = search_process.file_path + '.csv'
            headers = ['Scientific Name', location_col, 'Conservation Status Global']

            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                for taxon in taxa:
                    iucn_global = ''
                    if taxon.iucn_status:
                        iucn_global = category_labels.get(
                            taxon.iucn_status.category,
                            taxon.iucn_status.category
                        )
                    writer.writerow({
                        'Scientific Name': taxon.canonical_name or '',
                        location_col: ', '.join(sorted(taxon_parks.get(taxon.id, set()))),
                        'Conservation Status Global': iucn_global,
                    })

            if search_process.requester:
                from bims.tasks.email_csv import send_csv_via_email
                download_request_id = search_parameters.get('downloadRequestId')
                send_csv_via_email.delay(
                    user_id=search_process.requester.id,
                    csv_file=csv_path,
                    file_name='species-list',
                    download_request_id=download_request_id
                )

            results = {
                'status': SEARCH_FINISHED,
            }
            search_process.set_status(SEARCH_FINISHED, False)
            search_process.save_to_file(results)
            return
    logger.info(
        'Search %s is already being processed by another worker',
        search_process.process_id
    )


@shared_task(name='bims.tasks.spatial_dashboard_national_rli', queue='search')
def spatial_dashboard_national_cons_status(search_parameters=None, search_process_id=None):
    """Compute RLI values per taxon module and in aggregate for three national
    assessments: 2016 SANBI backcast, 2026 SANBI Red List, and current IUCN
    status.  Output mirrors the IUCN RLI format (series + aggregate).
    """
    from collections import defaultdict
    from bims.utils.celery import memcache_lock
    from bims.api_views.search import CollectionSearch
    from bims.models.search_process import (
        SearchProcess,
        SEARCH_PROCESSING,
        SEARCH_FINISHED,
    )
    from bims.models.taxon_conservation_assessment import TaxonNationalConservationAssessment
    from bims.models.taxonomy import Taxonomy
    from bims.scripts.species_keys import SANBI_2016_BACKCAST, SANBI_2026_REDLIST

    SPECIES_RANKS = ['SPECIES', 'SUBSPECIES', 'VARIETY']
    DD_CATEGORIES = {'DD', 'DDD', 'DDT'}

    ASSESSMENT_ORDER = [SANBI_2016_BACKCAST, SANBI_2026_REDLIST, 'Current IUCN Status']

    if search_parameters is None:
        search_parameters = {}

    try:
        search_process = SearchProcess.objects.get(id=search_process_id)
    except SearchProcess.DoesNotExist:
        return

    lock_id = '{0}-lock-{1}'.format(search_process.file_path, search_process.process_id)
    oid = '{0}'.format(search_process.process_id)

    with memcache_lock(lock_id, oid) as acquired:
        if not acquired:
            logger.info(
                'Search %s is already being processed by another worker',
                search_process.process_id
            )
            return

        search_process.set_status(SEARCH_PROCESSING)

        if search_process.requester and 'requester' not in search_parameters:
            search_parameters['requester'] = search_process.requester.id

        search = CollectionSearch(search_parameters)
        collection_results = search.process_search()

        taxa_modules_qs = collection_results.filter(
            taxonomy__taxonomic_status='ACCEPTED',
            taxonomy__rank__in=SPECIES_RANKS,
            taxonomy__origin__origin_key='indigenous',
            taxonomy__include_in_rli=True,
        ).values('taxonomy_id', 'module_group__name').distinct()

        taxa_to_modules = defaultdict(set)
        for row in taxa_modules_qs:
            tid = row['taxonomy_id']
            module = row['module_group__name'] or 'Unknown'
            taxa_to_modules[tid].add(module)

        taxonomy_ids = list(taxa_to_modules.keys())

        if not taxonomy_ids:
            search_process.set_status(SEARCH_FINISHED, False)
            search_process.save_to_file({'series': [], 'aggregate': []})
            return

        national_rows = (
            TaxonNationalConservationAssessment.objects
            .filter(taxonomy_id__in=taxonomy_ids)
            .exclude(iucn_status__isnull=True)
            .values('taxonomy_id', 'assessment_label', 'iucn_status__category')
        )
        assessment_statuses = defaultdict(list)
        for row in national_rows:
            label = row['assessment_label']
            if label in (SANBI_2016_BACKCAST, SANBI_2026_REDLIST):
                assessment_statuses[label].append(
                    (row['taxonomy_id'], row['iucn_status__category'] or '')
                )

        for row in Taxonomy.objects.filter(
            id__in=taxonomy_ids
        ).exclude(iucn_status__isnull=True).values('id', 'iucn_status__category'):
            assessment_statuses['Current IUCN Status'].append(
                (row['id'], row['iucn_status__category'] or '')
            )

        label_to_idx = {label: idx for idx, label in enumerate(ASSESSMENT_ORDER)}
        year_taxa_statuses = {
            label_to_idx[label]: statuses
            for label, statuses in assessment_statuses.items()
            if label in label_to_idx
        }

        per_module_year, aggregate_year = _compute_rli(
            taxa_to_modules, year_taxa_statuses, DD_CATEGORIES,
            use_fixed_pool=False,
        )

        idx_to_label = {idx: label for label, idx in label_to_idx.items()}

        module_series = defaultdict(list)
        for (mod, idx), data in per_module_year.items():
            module_series[mod].append({
                'label': idx_to_label[idx],
                'value': data['rli'],
                'num_assessed': data['assessed'],
                'num_dd': data['dd'],
                'categories': data['categories'],
            })

        series = []
        for mod, points in module_series.items():
            if not any(p['categories'] for p in points):
                continue
            series.append({
                'name': mod,
                'points': sorted(points, key=lambda p: label_to_idx[p['label']]),
            })

        aggregate = []
        for idx, data in aggregate_year.items():
            aggregate.append({
                'label': idx_to_label[idx],
                'value': data['rli'],
                'num_assessed': data['assessed'],
                'num_dd': data['dd'],
                'categories': data['categories'],
            })
        aggregate = sorted(aggregate, key=lambda p: label_to_idx[p['label']])

        search_process.set_status(SEARCH_FINISHED, False)
        search_process.save_to_file({'series': series, 'aggregate': aggregate})
