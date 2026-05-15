ON_GBIF = 'On GBIF'
GBIF_LINK = 'GBIF Link'
GBIF_URL = 'GBIF URL'
TAXON = 'Taxon'
SPECIES = 'Species'
SPECIES_GROUP = 'SpeciesGroup'
GENUS = 'Genus'
SUBFAMILY = 'SubFamily'
SUBORDER = 'SubOrder'
SUBCLASS = 'SubClass'
SUBSPECIES = 'SubSpecies'
CITES_LISTING = 'CITES listing'
SUBPHYLUM = 'SubPhylum'
FAMILY = 'Family'
ORDER = 'Order'
CLASS = 'Class'
PHYLUM = 'Phylum'
KINGDOM = 'Kingdom'
DIVISION = 'Division'
GROWTH_FORM = 'Growth form'
SCIENTIFIC_NAME = 'Scientific name and authority'
FORMER_SPECIES_NAME = 'Former scientific name'
COMMON_NAME = 'Common Name'
VERNACULAR_NAME = 'Vernacular name(s)'
VERNACULAR_NAME_LANG = 'Language'
ENDEMISM = 'Endemism'
CONSERVATION_STATUS = 'Conservation status'
CONSERVATION_STATUS_GLOBAL = 'Conservation status global'
CONSERVATION_STATUS_NATIONAL = 'Conservation status national'
ORIGIN = 'Origin'
INVASION = 'Invasion'
TAXON_RANK = 'Taxon Rank'
TAXONOMIC_STATUS = 'Taxonomic status'
FADA_ID = 'FADA ID'
VARIETY = 'Variety'
FORMA = 'Forma'
TRIBE = 'Tribe'
SUBTRIBE = 'SubTribe'
SYNONYM = 'Synonym'
ACCEPTED_TAXON = 'Accepted Taxon'
SUBGENUS = 'SubGenus'
AUTHORS = 'Author(s)'
BIOGRAPHIC_DISTRIBUTIONS = [
    'ANT', 'AT', 'AU', 'NA',
    'NT', 'OL', 'PA', 'PAC'
]
REFERENCES = 'References'

SANBI_2016_BACKCAST = '2016 SANBI backcast'
SANBI_2026_REDLIST = '2026 SANBI Red List'

INLCUDE_IN_RLI = 'Include in calculation'

PARENT_RANKS = {
    'KINGDOM': None,
    'PHYLUM': KINGDOM,
    'SUBPHYLUM': PHYLUM,
    'CLASS': SUBPHYLUM,
    'SUBCLASS': CLASS,
    'ORDER': SUBCLASS,
    'SUBORDER': ORDER,
    'FAMILY': SUBORDER,
    'SUBFAMILY': FAMILY,
    'TRIBE': SUBFAMILY,
    'SUBTRIBE': TRIBE,
    'GENUS': SUBTRIBE,
    'SUBGENUS': GENUS,
    'SPECIESGROUP': GENUS,
    'SPECIES': GENUS,
    'SUBSPECIES': SPECIES,
    'VARIETY': SPECIES,
    'FORMA': SPECIES,
}

ALL_TAXON_RANKS = list(
    PARENT_RANKS.keys()
)

RANK_HIERARCHY = [
    'KINGDOM',
    'PHYLUM',
    'SUBPHYLUM',
    'CLASS',
    'SUBCLASS',
    'ORDER',
    'SUBORDER',
    'FAMILY',
    'SUBFAMILY',
    'TRIBE',
    'SUBTRIBE',
    'GENUS',
    'SUBGENUS',
    'SPECIESGROUP',
    'SPECIES',
    'SUBSPECIES',
    'VARIETY',
    'FORMA'
]

RANK_TITLE = {
    'KINGDOM': KINGDOM,
    'PHYLUM': PHYLUM,
    'SUBPHYLUM': SUBPHYLUM,
    'CLASS': CLASS,
    'SUBCLASS': SUBCLASS,
    'ORDER': ORDER,
    'SUBORDER': SUBORDER,
    'FAMILY': FAMILY,
    'SUBFAMILY': SUBFAMILY,
    'TRIBE': TRIBE,
    'SUBTRIBE': SUBTRIBE,
    'GENUS': GENUS,
    'SUBGENUS': SUBGENUS,
    'SPECIES': SPECIES,
    'SUBSPECIES': SUBSPECIES,
    'VARIETY': VARIETY,
    'FORMA': FORMA
}


RANK_INDEX = {rank: idx for idx, rank in enumerate(RANK_HIERARCHY)}


def parent_rank(rank):
    """Return the immediate parent rank of the given rank."""
    return PARENT_RANKS.get(rank.upper())
