import re as _re

from bims.enums.taxon_addendum import TaxonAddendum

ADDENDUM_ABBREVIATIONS = {
    TaxonAddendum.SENSU_LATO.name: 's.l.',
    TaxonAddendum.PARTIM.name: 'partim',
}

ADDENDUM_FULL_TEXT = {
    TaxonAddendum.SENSU_LATO.name: TaxonAddendum.SENSU_LATO.value,
    TaxonAddendum.PARTIM.name: TaxonAddendum.PARTIM.value,
}


def ensure_fada_id(taxonomy) -> None:
    """
    Backfill fada_id on a taxonomy when it's added to a taxon group on
    the FADA site. Taxa can end up without a fada_id if they were only
    ever created as an ancestor (genus/family/...) for hierarchy
    purposes rather than added directly through the add-taxon flow.
    """
    from bims.templatetags.site import is_fada_site
    if not taxonomy or taxonomy.fada_id:
        return
    if not is_fada_site():
        return
    taxonomy.fada_id = f'FADA-{taxonomy.id}'
    taxonomy.save(update_fields=['fada_id'])


def resolve_addendum_code(raw_value: str) -> str:
    """
    Resolve a free-text addendum value to the stored addendum code.
    """
    value = (raw_value or '').strip().lower()
    if not value:
        return ''
    for addendum in TaxonAddendum:
        if value in (
            addendum.name.lower(),
            addendum.value.lower(),
            ADDENDUM_ABBREVIATIONS.get(addendum.name, '').lower(),
        ):
            return addendum.name
    return ''


def _addendum_name_pattern(addendum: TaxonAddendum) -> str:
    """Regex alternation matching an addendum's full text or abbreviation
    (e.g. 's.l.', 's. l.', 's l', 'sl' for SENSU_LATO)"""
    alternatives = [_re.escape(addendum.value)]
    abbreviation = ADDENDUM_ABBREVIATIONS.get(addendum.name)
    if abbreviation and abbreviation.lower() != addendum.value.lower():
        letters = [c for c in abbreviation if c.isalpha()]
        alternatives.append(r'\.?\s*'.join(letters) + r'\.?')
    return '|'.join(alternatives)


def strip_addendum_from_name(name: str) -> tuple:
    """
    Detect and strip a trailing addendum qualifier from a taxon name
    """
    name = (name or '').strip()
    if not name:
        return name, ''
    for addendum in TaxonAddendum:
        pattern = r'\s+(?:%s)\s*$' % _addendum_name_pattern(addendum)
        match = _re.search(pattern, name, flags=_re.IGNORECASE)
        if match:
            return name[:match.start()].strip(), addendum.name
    return name, ''


def get_addendum_display(addendum_code: str, abbreviate: bool = True) -> str:
    """
    Return the display text for an addendum code, e.g. 's.l.' or
    'sensu lato' for TaxonAddendum.SENSU_LATO.name. Returns '' when the
    code is empty or unrecognised.
    """
    if not addendum_code:
        return ''
    mapping = ADDENDUM_ABBREVIATIONS if abbreviate else ADDENDUM_FULL_TEXT
    return mapping.get(addendum_code, '')


def build_name_with_addendum(
        canonical: str, author: str = '',
        addendum_code: str = '', abbreviate: bool = True) -> str:
    """
    Build a 'canonical [addendum] author' string, e.g.:
      ('Aquanothrus montanus', 'Engelbrecht, 1975', 'SENSU_LATO', True)
        -> 'Aquanothrus montanus s.l. Engelbrecht, 1975'
    """
    parts = [
        (canonical or '').strip(),
        get_addendum_display(addendum_code, abbreviate=abbreviate),
        (author or '').strip(),
    ]
    return ' '.join(p for p in parts if p)


def canonical_with_subgenus(canonical: str, genus: str, subgenus: str) -> str:
    """
    Return canonical_name with a subgenus parenthetical inserted when needed.

    Examples:
      ('Aedes aegypti', 'Aedes', 'Stegomyia')       -> 'Aedes (Stegomyia) aegypti'
      ('Stegomyia',    'Aedes', 'Stegomyia')        -> 'Aedes (Stegomyia)'
      ('Aedes (Stegomyia) aegypti', 'Aedes', 'Stegomyia') -> unchanged
    """
    canonical = (canonical or '').strip()
    if not subgenus or not genus:
        return canonical

    # Accept "Genus (BareSubgenus)" or bare "BareSubgenus" as the subgenus value.
    m = _re.search(r'\((.+?)\)', subgenus)
    sg_name = m.group(1) if m else subgenus

    if f'({sg_name})' in canonical:
        return canonical

    genus_cap = genus[:1].upper() + genus[1:].lower()

    # Bare subgenus name as canonical (subgenus-rank taxon) — no epithet.
    if canonical.lower() == sg_name.lower():
        return f'{genus_cap} ({sg_name})'

    # Strip genus prefix and any existing parenthetical tokens.
    tokens = canonical.split()
    non_paren = [t for t in tokens if not (t.startswith('(') and t.endswith(')'))]
    if non_paren and non_paren[0].lower() == genus.lower():
        epithet_tokens = non_paren[1:]
    else:
        epithet_tokens = non_paren

    epithet = ' '.join(w.lower() for w in epithet_tokens)
    if epithet:
        return f'{genus_cap} ({sg_name}) {epithet}'
    return f'{genus_cap} ({sg_name})'
