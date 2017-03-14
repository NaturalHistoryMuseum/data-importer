import luigi

from ke2sql.tasks.base import BaseTask


class ETaxonomyTask(BaseTask):

    field_mappings = (
        ('ClaScientificNameBuilt', 'scientificName'),
        ('ClaKingdom', 'kingdom'),
        ('ClaPhylum', 'phylum'),
        ('ClaClass', 'class'),
        ('ClaOrder', 'order'),
        ('ClaSuborder', 'Suborder'),
        ('ClaSuperfamily', 'Superfamily'),
        ('ClaFamily', 'Family'),
        ('ClaSubfamily', 'Subfamily'),
        ('ClaGenus', 'genus'),
        ('ClaSubgenus', 'subgenus'),
        ('ClaSpecies', 'specificEpithet'),
        ('ClaSubspecies', 'infraspecificEpithet'),
        ('ClaRank', 'taxonRank')  # NB: CKAN uses rank internally
    )
