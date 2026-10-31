from enum import Enum


class TaxonAddendum(Enum):
    """
    A nomenclatural qualifier appended to a taxon's scientific name.
    """

    # SENSU_LATO ("sensu lato", abbreviated "s.l.") is used when a species has
    # been split into subspecies but both the species and its subspecies are
    # kept as accepted taxa, to indicate the species entry is a broader
    # grouping that includes those subspecies
    SENSU_LATO = 'sensu lato'

    # PARTIM is used when one synonym was divided between two or
    # more accepted taxa, so the synonym only applies to part of each of them
    PARTIM = 'partim'
