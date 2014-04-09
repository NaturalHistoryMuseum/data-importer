from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table, Column as SQLAlchemyColumn, Integer, Float, String, ForeignKey, Boolean, Date, UniqueConstraint, Enum, DateTime, func
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship, backref, RelationshipProperty as SQLAlchemyRelationshipProperty, validates
from sqlalchemy.exc import InvalidRequestError
import datetime
from sqlalchemy import event
from ke2sql.log import log
from ke2sql.model.base import Base
from ke2sql import config

__all__ = [
    'ArtefactModel',
    'BirdGroupParentModel',
    'BirdGroupPartModel',
    'BotanySpecimenModel',
    'BryozoaParentSpecimenModel',
    'BryozoaPartSpecimenModel',
    'CatalogueModel',
    'CollectionEventModel',
    'DNAPrepModel',
    'EggModel',
    'IndexLotMaterialModel',
    'IndexLotModel',
    'MammalGroupParentModel',
    'MammalGroupPartModel',
    'MeteoritesSpecimenModel',
    'MineralogySpecimenModel',
    'MultimediaModel',
    'NestModel',
    'OtherNumbersModel',
    'PalaeontologySpecimenModel',
    'ParasiteCardModel',
    'PartModel',
    'SexStageModel',
    'SilicaGelSpecimenModel',
    'SiteModel',
    'SpecimenModel',
    'StratigraphicUnitModel',
    'StratigraphyModel',
    'StubModel',
    'TaxonomyModel',
    'RelationshipProperty',
    'StratigraphicUnitAssociation',
    'HostParasiteAssociation',
    'Determination',
    'MineralogicalAge',
    'STRATIGRAPHIC_UNIT_TYPES'
]

STRATIGRAPHIC_UNIT_TYPES = {
    'Chronostratigraphy': ['ChrEon', 'ChrEra', 'ChrPeriod', 'ChrEpoch', 'ChrStage'],
    'Biostratigraphy': ['BioSuperzone', 'BioZone', 'BioSubzone', 'BioHorizon'],
    'Cultural phase': ['HumCulturalPhase', 'HumArchaeological'],
    'Lithostratigraphy': ['LitSupergroup', 'LitGroup', 'LitFormation', 'LitMember', 'LitBed'],
}

keemu_schema = config.get('database', 'schema')

# Modify some of the SQLAlchemy base objects
class Column(SQLAlchemyColumn):
    def __init__(self, *args, **kwargs):
        self.alias = kwargs.pop('alias', None)
        super(Column, self).__init__(*args, **kwargs)

class RelationshipProperty(SQLAlchemyRelationshipProperty):
    def __init__(self, *args, **kwargs):
        self.alias = kwargs.pop('alias', None)
        super(RelationshipProperty, self).__init__(*args, **kwargs)


def relationship(argument, secondary=None, alias=None, **kwargs):
    """Provide a relationship of a primary Mapper to a secondary Mapper."""

    return RelationshipProperty(argument, secondary=secondary, alias=alias, **kwargs)


class DataTypeException(Exception):
    pass

class BaseMixin(object):

    __table_args__ = {
        'schema': keemu_schema
    }

    _rebuilding = False

    def __init__(self, **kwargs):
        # Populate the fields
        # Try and set the IRN first so it can be used throughout (errors etc.,)
        # Won't exist for subclass field groups (other numbers for example)
        try:
            self.irn = kwargs['irn']
            del kwargs['irn']
        except KeyError:
            pass

        self._populate(**kwargs)

    def __iter__(self):
        # Define an iterator so we can loop through all values
        for prop in self.__mapper__.iterate_properties:
            value = getattr(self, prop.key, None)
            if value:
                yield prop.key, value

    def __setattr__(self, name, value):

        # If this isn't an SQLAlchemy internal attribute or a rebuild, check we're not setting a duplicate value
        if not (name.startswith('_') or self._rebuilding):
            # Is this a duplicate value (happens where multiple field aliases have been matched in the source data)
            existing_value = getattr(self, name)
            if value and existing_value and existing_value != value:
                log.error('Multiple aliased fields do not match for %s(%s).%s: %s != %s' % (self.__class__.__name__, self.irn, name, existing_value, value))

        # Set the value
        super(BaseMixin, self).__setattr__(name, value)

    def get_aliases(self):

        aliases = {}

        # Before populating, build a list aliases
        for prop in self.__mapper__.iterate_properties:
            # Build list of aliases & column types
            try:
                column = prop.columns[0]
                if column.alias:
                    for alias in column.alias if isinstance(column.alias, list) else [column.alias]:
                        aliases[alias] = column.key

            except AttributeError:
                pass

        return aliases

    def _populate(self, **kwargs):

        aliases = self.get_aliases()
        cls_ = type(self)

        # Loop through kwargs and set attributes
        for k in kwargs:
            value = kwargs[k]
            # If this is a field alias, get the source field
            try:
                k = aliases[k]
            except KeyError:
                pass

            # If the attribute exists, set it
            if hasattr(cls_, k):
                setattr(self, k, value)

    def rebuild(self, **kwargs):

        # Flag so we know we're rebuilding
        self._rebuilding = True

        # Delete all attributes first
        for attr, value in self:

            # Do not delete the IRN, otherwise we'll get an insert
            # Also ignore internal attributes (including _created & _modified)
            if attr in ('irn', 'type') or attr.startswith('_'):
                continue

            if isinstance(value, list):
                setattr(self, attr, [])
            else:
                setattr(self, attr, None)

        self._rebuilding = False

        # And then re-populate
        self._populate(**kwargs)


# Set up some data validators

def validate_int(value):
    if isinstance(value, basestring):
        value = int(value)
    else:
        assert isinstance(value, int)
    return value


def validate_bool(value):
    assert isinstance(value, bool)
    return value

validators = {
    Integer: validate_int,
    Boolean: validate_bool
}

# this event is called whenever an attribute
# on a class is instrumented
@event.listens_for(Base, 'attribute_instrument')
def configure_listener(class_, key, inst):
    if not hasattr(inst.property, 'columns'):
        return
    # this event is called whenever a "set"
    # occurs on that instrumented attribute
    @event.listens_for(inst, "set", retval=True)
    def set_(instance, value, orig_value, initiator):
        validator = validators.get(inst.property.columns[0].type.__class__)
        if value is not None and validator:
            try:
                value = validator(value)
            except (ValueError, TypeError, AssertionError):
                log.error('Data type error: %s is not of type %s for %s(%s).%s' % (value, inst.property.columns[0].type, class_.__name__, instance.irn, key))
                return None

        return value

# Relationship tables
catalogue_multimedia = Table('catalogue_multimedia', Base.metadata,
                            Column('catalogue_irn', Integer, ForeignKey('%s.catalogue.irn' % keemu_schema, ondelete='CASCADE'), primary_key=True),
                            Column('multimedia_irn', Integer, ForeignKey('%s.multimedia.irn' % keemu_schema, ondelete='CASCADE'), primary_key=True),
                            schema=keemu_schema
)

catalogue_associated_record = Table('catalogue_associated_record', Base.metadata,
                            Column('catalogue_irn', Integer, ForeignKey('%s.catalogue.irn' % keemu_schema, ondelete='CASCADE'), primary_key=True),
                            Column('associated_irn', Integer, ForeignKey('%s.catalogue.irn' % keemu_schema, ondelete='CASCADE'), primary_key=True),
                            schema=keemu_schema
)

specimen_sex_stage = Table('specimen_sex_stage', Base.metadata,
                           Column('specimen_irn', Integer, ForeignKey('%s.specimen.irn' % keemu_schema, ondelete='CASCADE'), primary_key=True),
                           Column('sex_stage_id', Integer, ForeignKey('%s.sex_stage.id' % keemu_schema, ondelete='CASCADE'), primary_key=True),
                           schema=keemu_schema
)

specimen_mineralogical_age = Table('specimen_mineralogical_age', Base.metadata,
                                   Column('mineralogy_irn', Integer, ForeignKey('%s.mineralogy.irn' % keemu_schema, ondelete='CASCADE'), primary_key=True),
                                   Column('mineralogical_age_id', Integer, ForeignKey('%s.mineralogical_age.id' % keemu_schema, ondelete='CASCADE'), primary_key=True),
                                   schema=keemu_schema
)

class MultimediaModel(BaseMixin, Base):

    """
    Multimedia: Images & videos only
    """
    __tablename__ = 'multimedia'

    irn = Column(Integer, primary_key=True)
    _created = Column(DateTime, nullable=False, server_default=func.now())
    _modified = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    title = Column(String, alias='MulTitle')
    mime_type = Column(String, alias='MulMimeType')
    mime_format = Column(String, alias='MulMimeFormat')


class SiteModel(BaseMixin, Base):
    __tablename__ = 'site'

    irn = Column(Integer, primary_key=True)
    _created = Column(DateTime, nullable=False, server_default=func.now())
    _modified = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    continent = Column(String, alias=['LocContinentMain', 'PolContinent'])
    country = Column(String, alias=['LocCountryMain', 'PolPD1'])
    state_province = Column(String, alias=['LocProvinceStateTerritoryMain', 'PolPD2'])
    county = Column(String, alias=['LocDistrictCountyShireMain', 'PolPD3'])
    locality = Column(String, alias=['LocNhmVerbatimLocality', 'LocPreciseLocation'])
    vice_county = Column(String, alias='Cat1ViceUkViceCounty')
    parish = Column(String, alias='PolParish')
    town = Column(String, alias='LocTownship')
    nearest_named_place = Column(String, alias='LocNearestNamedPlace')

    # Ocean & lakes
    ocean = Column(String, alias='LocOceanMain')
    island_group = Column(String, alias='LocIslandGrouping')
    island = Column(String, alias='LocIslandName')
    lake = Column(String, alias='AquNhmLake')
    river_basin = Column(String, alias='AquRiverBasin')

    # Lat/long
    geodetic_datum = Column(String, alias='LatDatum')
    georef_method = Column(String, alias=['LatDetSource', 'LatLatLongDetermination'])
    latitude = Column(String, alias='LatLatitude')
    decimal_latitude = Column(Float, alias='LatLatitudeDecimal')
    longitude = Column(String, alias='LatLongitude')
    decimal_longitude = Column(Float, alias='LatLongitudeDecimal')

    # Physical
    minimum_elevation_in_meters = Column(String, alias='PhyAltitudeFromMtr')
    maximum_elevation_in_meters = Column(String, alias='PhyAltitudeToMtr')
    minimum_depth_in_meters = Column(String, alias='PhyDepthFromMtr')
    maximum_depth_in_meters = Column(String, alias='PhyDepthToMtr')

    # Mineralogy location data
    mineral_complex = Column(String, alias='GeoNhmComplex')
    mine = Column(String, alias='GeoNhmStandardMine')
    mining_district = Column(String, alias='GeoNhmMiningDistrict')
    tectonic_province = Column(String, alias='GeoNhmTectonicProvince')
    geology_region = Column(String, alias='GeoNhmRegion')


class StratigraphyModel(BaseMixin, Base):

    """
    StratigraphyModel

    Changelog:
    131120: Removed absolute date field. Only 31 records using it
    131206: Removed subdivision = Column(String, alias='ChrSubdivision') (26)
            Removed region = Column(String, alias='ChrRegion') - Duplicates sites (only 22 with region don't have site)
            taxonomic_group = Column(String, alias='BioTaxonomicGroup') - Duplicates determination
    """

    __tablename__ = 'stratigraphy'

    irn = Column(Integer, primary_key=True)
    _created = Column(DateTime, nullable=False, server_default=func.now())
    _modified = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    biozone = Column(String, alias='BioKindofBiozone')
    sedimentology = Column(String, alias='OthSedimentology')
    sedimentary_facies = Column(String, alias='OthSedimentaryFacies')

    # Relationships
    stratigraphic_unit = association_proxy('stratigraphy', 'unit')


class StratigraphicUnitModel(BaseMixin, Base):
    """
    The actual stratigraphic units - Mesozoic, Jurassic etc.,
    """
    __tablename__ = 'stratigraphic_unit'

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)


class StratigraphicUnitAssociation(BaseMixin, Base):
    """
    Association object for Stratigraphy => StratigraphicUnitModel
    """
    __tablename__ = 'stratigraphy_stratigraphic_unit'

    stratigraphy_irn = Column(Integer, ForeignKey(StratigraphyModel.irn), primary_key=True, nullable=False)
    unit_id = Column(Integer, ForeignKey(StratigraphicUnitModel.id), primary_key=True, nullable=False)
    types = [i[3:].lower() for group in STRATIGRAPHIC_UNIT_TYPES.values() for i in group]

    stratigraphic_type = Column(Enum(*types, name='stratigraphic_type', schema=keemu_schema), primary_key=True, nullable=False)
    direction = Column(Enum('from', 'to', name='direction', schema=keemu_schema), primary_key=True, nullable=False)  # From / to

    # bidirectional attribute/collection of "specimen"/"determinations"
    stratigraphy = relationship(StratigraphyModel, backref=backref("stratigraphic_unit", cascade="all, delete-orphan"))

    # reference to the "Taxonomy" object
    unit = relationship("StratigraphicUnitModel")

    def __init__(self, stratigraphy_irn=None, unit_id=None, stratigraphic_type=None, direction=None):
        self.stratigraphy_irn = stratigraphy_irn
        self.unit_id = unit_id
        self.stratigraphic_type = stratigraphic_type
        self.direction = direction


class CollectionEventModel(BaseMixin, Base):

    """
    StratigraphyModel

    Changelog:
    131205: Removed stratigraphy = relationship("StratigraphyModel", secondary=collection_event_stratigraphy, alias='GeoStratigraphyRef') (53 records using it)
                    geological_details = Column(String, alias='GeoOutcropDetails') (4)
                    geological_features = Column(String, alias='GeoGeologicalFeatures') (0)
                    geological_lithology = Column(String, alias='GeoLithologicalDetails') (5)
    131213: Removed site = Column(Integer, ForeignKey(SiteModel.irn), alias='ColSiteRef')
            The catalogue site field is populated (via PalCol1SiteRef?) with collection event site irn
            There are no records with collection event and not site
    """

    __tablename__ = 'collection_event'

    irn = Column(Integer, primary_key=True)
    _created = Column(DateTime, nullable=False, server_default=func.now())
    _modified = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    # Collection
    # DarYearCollected will use either date_collected_from or date_collected_to, whichever is populated
    date_collected_from = Column(String, alias='ColDateVisitedFrom')
    time_collected_from = Column(String, alias='ColTimeVisitedFrom')
    date_collected_to = Column(String, alias='ColDateVisitedTo')  # Needs to be string to pass validation
    time_collected_to = Column(String, alias='ColTimeVisitedTo')
    collection_event_code = Column(String, alias='ColCollectionEventCode')
    collection_method = Column(String, alias='ColCollectionMethod')

    # Depth / height
    depth_from_metres = Column(String, alias='DepFromMetres')
    depth_to_metres = Column(String, alias='DepToMetres')

    # Expedition
    expedition_start_date = Column(String, alias='ExpStartDate')
    # DwC recommends using expedition name as collector
    expedition_name = Column(String, alias='ExpExpeditionName')
    vessel_name = Column(String, alias=['ExpVesselName', 'AquVesselName'])
    vessel_type = Column(String, alias=['ExpVesselType', 'AquVesselType'])

    # Collector
    collector_name = Column(String, alias='ColParticipantString')
    collector_number = Column(String, alias='ColPrimaryParticipantRef')

    def __repr__(self):
        return 'CollectionEvent(%s)' % repr(self.irn)


class TaxonomyModel(BaseMixin, Base):
    __tablename__ = 'taxonomy'

    irn = Column(Integer, primary_key=True)
    _created = Column(DateTime, nullable=False, server_default=func.now())
    _modified = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())
    scientific_name = Column(String, alias='ClaScientificName')
    kingdom = Column(String, alias='ClaKingdom')
    phylum = Column(String, alias='ClaPhylum')
    taxonomic_class = Column(String, alias='ClaClass')
    order = Column(String, alias='ClaOrder')
    suborder = Column(String, alias='ClaSuborder')
    superfamily = Column(String, alias='ClaSuperfamily')
    family = Column(String, alias='ClaFamily')
    subfamily = Column(String, alias='ClaSubfamily')
    genus = Column(String, alias='ClaGenus')
    subgenus = Column(String, alias='ClaSubgenus')
    species = Column(String, alias='ClaSpecies')
    subspecies = Column(String, alias='ClaSubspecies')
    validity = Column(String, alias='TaxValidity')
    rank = Column(String, alias='ClaRank')
    scientific_name_author = Column(String, alias='AutBasionymAuthorString')
    scientific_name_author_year = Column(String, alias='AutBasionymDate')
    currently_accepted_name = Column(Boolean, alias='ClaCurrentlyAccepted')

    def __repr__(self):
        return 'Taxonomy(%s)' % repr(self.irn)


class CatalogueModel(BaseMixin, Base):
    """
    Everything from the catalogue table inherits from this table, which allows us to have one table containing all IRNs
    To be used in multimedia etc., relationships
    """

    __tablename__ = 'catalogue'

    irn = Column(Integer, primary_key=True, nullable=False)
    _created = Column(DateTime, nullable=False, server_default=func.now())
    _modified = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())
    ke_date_modified = Column(Date, alias='AdmDateModified')
    ke_date_inserted = Column(Date, alias='AdmDateInserted')
    type = Column(String)

    # All catalogue records can have multimedia
    multimedia = relationship("MultimediaModel", secondary=catalogue_multimedia, alias=['MulMultiMediaRef', 'ImaMultimediaRef'])

    # All catalogue records can be associated with each other
    associated_record = relationship("CatalogueModel",
                                        secondary=catalogue_associated_record,
                                        primaryjoin=irn == catalogue_associated_record.c.catalogue_irn,
                                        secondaryjoin=irn == catalogue_associated_record.c.associated_irn,
                                        backref=backref("associated_to", viewonly=True),
                                        alias=['AssRegistrationNumberRef', 'EntRelOtherObjectRef']
    )

    # Polymorphic identity to allow for subclasses
    __mapper_args__ = {
        'polymorphic_identity': 'specimen',
        'polymorphic_on': type,
        'batch': True,
    }

    @validates('ke_date_modified', 'ke_date_inserted')
    def validate_ke_date(self, key, value):

        if value:
            try:
                # Validate ke dates are in the format YYYY-MM-DD
                datetime.datetime.strptime(value, '%Y-%m-%d')
                return value
            except ValueError:
                log.error('Data type error: %s cannot be formatted as a date for %s(%s).%s' % (value, self.__class__.__name__, self.irn, key))


class SpecimenModel(CatalogueModel):

    """
    Main specimen model - all fields not shared with indexlots

    Changelog:
    131101: Added back PalCol2CollectionEventRef - sumCollectionEventRef isn't always populated
    131109: removed field imaged (RegImaged); Conflicts with multimedia
    131125: Merged specimen_unit = Column(String, alias='ColKind') and kind_of_object = Column(String, alias=['CatKindOfObject', 'EntCatKindOfObject'])
            There are only 6 records with different values:  EGG xC/2 | Egg ...
    131125: Merged preparation_type = Column(String, alias='PreType') (only 6 records) into kind_of_collection = Column(String, alias='CatKindOfCollection')
    """
    __tablename__ = 'specimen'

    # base fields
    irn = Column(Integer, ForeignKey(CatalogueModel.irn, ondelete='CASCADE'), primary_key=True)
    collection_department = Column(String, alias='ColDepartment')
    collection_sub_department = Column(String, alias='ColSubDepartment')
    specimen_unit = Column(String, alias=['ColKind', 'CatKindOfObject', 'EntCatKindOfObject'])
    curation_unit = Column(String, alias='RegCurationUnit')
    catalogue_number = Column(String, alias=['DarCatalogNumber', 'MinBmNumber', 'BotRegRegistrationNumber', 'EntCatCatalogueNumber', 'RegRegistrationNumber', 'PalRegFullRegistrationNumber'])
    preservation = Column(String, alias=['EntCatPreservation', 'CatPreservative'])
    verbatim_label_data = Column(String, alias=['EntLabVerbatimLabelData', 'PalVerLabel'])
    donor_name = Column(String, alias='PalAcqAccLotDonorFullName')
    date_catalogued = Column(String, alias='EntCatDateCatalogued')
    kind_of_collection = Column(String, alias='CatKindOfCollection')

    specimen_count = Column(Integer, alias=['DarIndividualCount', 'EntCatSpecimenCount'])

    preparation = Column(String, alias='PreProcess')
    preparation_type = Column(String, alias='PreType')
    weight = Column(String, alias='DarObservedWeight')

    registration_code = Column(String, alias='RegCode')

    # DwC - it's easier just to use these DwC fields
    type_status = Column(String, alias='DarTypeStatus')

    # EntIdeDateIdentified can be an array if there's multiple determinations, so use DwC fields
    date_identified_day = Column(Integer, alias='DarDayIdentified')
    date_identified_month = Column(Integer, alias='DarMonthIdentified')
    date_identified_year = Column(Integer, alias='DarYearIdentified')
    # Identification (for DwC)
    identification_qualifier = Column(String, alias='DarIdentificationQualifier')
    identified_by = Column(String, alias='DarIdentifiedBy')

    # Relationships
    site_irn = Column(Integer, ForeignKey(SiteModel.irn), alias=['sumSiteRef', 'PalCol1SiteRef'])
    site = relationship("SiteModel")

    collection_event_irn = Column(Integer, ForeignKey(CollectionEventModel.irn), alias=['sumCollectionEventRef', 'PalCol2CollectionEventRef'])
    collection_event = relationship("CollectionEventModel")

    # association proxy of "specimen_taxonomy" collection to "determination" attribute
    determination = association_proxy('specimen_taxonomy', 'taxonomy')

    other_numbers = relationship("OtherNumbersModel", cascade='all, delete-orphan')

    sex_stage = relationship("SexStageModel", secondary=specimen_sex_stage)

    part_record = relationship("PartModel", primaryjoin="PartModel.parent_irn == SpecimenModel.irn")

    __mapper_args__ = {
        'polymorphic_identity': 'specimen'
    }

class StubModel(SpecimenModel):
    """
    Placeholder for stub records
    """
    __mapper_args__ = {
        'polymorphic_identity': 'stub'
    }


class Determination(BaseMixin, Base):
    """
    Association object for Specimen => Taxonomy (determinations)
    """
    __tablename__ = 'determination'

    taxonomy_irn = Column(Integer, ForeignKey(TaxonomyModel.irn), primary_key=True)
    specimen_irn = Column(Integer, ForeignKey(SpecimenModel.irn), primary_key=True)
    filed_as = Column(Boolean)

    # # bidirectional attribute/collection of "specimen"/"determinations"
    specimen = relationship(SpecimenModel, backref=backref("specimen_taxonomy", cascade="all, delete-orphan"))

    # reference to the "Taxonomy" object
    taxonomy = relationship(TaxonomyModel, lazy=False)

    def __init__(self, taxonomy_irn=None, specimen_irn=None, filed_as=None):
        self.taxonomy_irn = taxonomy_irn
        self.specimen_irn = specimen_irn
        self.filed_as = filed_as


class OtherNumbersModel(BaseMixin, Base):

    __tablename__ = 'other_numbers'

    id = Column(Integer, primary_key=True)
    irn = Column(Integer, ForeignKey(SpecimenModel.irn), primary_key=True)
    kind = Column(String, alias=['EntCatOtherNumbersKind', 'MinCodes'])
    value = Column(String, alias=['EntCatOtherNumbersValue', 'MinOtherNumbers'])


class SexStageModel(BaseMixin, Base):

    __tablename__ = 'sex_stage'

    __table_args__ = (UniqueConstraint('count', 'sex', 'stage'), {'schema': keemu_schema})

    id = Column(Integer, primary_key=True)
    count = Column(Integer, alias='EntSexCount')
    sex = Column(String, alias='EntSexSex')
    stage = Column(String, alias='EntSexStage')


class ArtefactModel(CatalogueModel):

    __tablename__ = 'artefact'

    irn = Column(Integer, ForeignKey(CatalogueModel.irn, ondelete='CASCADE'), primary_key=True)
    kind = Column(String, alias='ArtKind')
    name = Column(String, alias='ArtName')

    __mapper_args__ = {
        'polymorphic_identity': 'artefact',
        'inherit_condition': (irn == CatalogueModel.irn)
    }


class PartModel(SpecimenModel):
    """
    Fields common to all part specimens
    All part specimens derive from this

    Changelog:
    131125: Removed completeness = Column(String, alias='PrtCompleteness') Only 283 records.

    """
    __tablename__ = 'part'

    irn = Column(Integer, ForeignKey(SpecimenModel.irn), primary_key=True)
    parent_irn = Column(Integer, ForeignKey(SpecimenModel.irn), alias='RegRegistrationParentRef')
    parent = relationship("SpecimenModel", viewonly=True, primaryjoin="PartModel.parent_irn == SpecimenModel.irn", backref="part")
    part_type = Column(String, alias='PrtType')

    __mapper_args__ = {
        'polymorphic_identity': 'part_specimen',
        'inherit_condition': (irn == SpecimenModel.irn)
    }


class BirdGroupParentModel(SpecimenModel):
    # Relationship is in BirdGroupPart
    __mapper_args__ = {
        'polymorphic_identity': 'bird_group_parent',
    }


class BirdGroupPartModel(PartModel):
    __mapper_args__ = {
        'polymorphic_identity': 'bird_group_part',
    }


class BryozoaParentSpecimenModel(SpecimenModel):
    __mapper_args__ = {
        'polymorphic_identity': 'bryozoa_parent',
    }


class BryozoaPartSpecimenModel(PartModel):
    __mapper_args__ = {
        'polymorphic_identity': 'bryozoa_part',
    }


class DNAPrepModel(SpecimenModel):

    """
    Changelog:
    131125: Removed protocol_used = Column(String, alias='DnaProtocolUsed')
            Duplicates data in extraction_method and doesn't contain data where protocol_used is null

    """

    __tablename__ = 'dna_preparation'

    irn = Column(Integer, ForeignKey(SpecimenModel.irn, ondelete='CASCADE'), primary_key=True)
    extraction_method = Column(String, alias='DnaExtractionMethod')
    resuspended_in = Column(String, alias='DnaReSuspendedIn')
    total_volume = Column(String, alias='DnaTotalVolume')

    __mapper_args__ = {
        'polymorphic_identity': 'dna_preparation',
    }


class ParasiteCardModel(SpecimenModel):
    __tablename__ = 'parasite_card'

    irn = Column(Integer, ForeignKey(SpecimenModel.irn, ondelete='CASCADE'), primary_key=True)
    barcode = Column(String, alias='CardBarcode')

    # association proxy of "host_parasite_taxonomy" collection to "host_parasite" attribute
    host_parasite = association_proxy('host_parasite_taxonomy', 'taxonomy')

    __mapper_args__ = {
        'polymorphic_identity': 'parasite_card',
    }


class HostParasiteAssociation(BaseMixin, Base):
    """
    Association object for Specimen => Taxonomy (determinations)
    """
    __tablename__ = 'host_parasite'

    parasite_card_irn = Column(Integer, ForeignKey(ParasiteCardModel.irn), primary_key=True)
    taxonomy_irn = Column(Integer, ForeignKey(TaxonomyModel.irn), primary_key=True)
    parasite_host = Column(Enum('host', 'parasite', name='parasite_host', schema=keemu_schema), primary_key=True, nullable=False)
    stage = Column(String)

    # bidirectional attribute/collection of parasite taxonomy
    host_parasite = relationship('ParasiteCardModel', backref=backref("host_parasite_taxonomy", cascade="all, delete-orphan"))

    # reference to the "Taxonomy" object
    taxonomy = relationship("TaxonomyModel")

    def __init__(self, parasite_card_irn=None, taxonomy_irn=None, parasite_host=None, stage=None):
        self.parasite_card_irn = parasite_card_irn
        self.taxonomy_irn = taxonomy_irn
        self.parasite_host = parasite_host
        self.stage = stage

class EggModel(PartModel):

    """
    Changelog:
    131125: Removed (no data) shape = Column(String, alias='EggShape')
                              number_of_host_eggs = Column(Integer, alias='EggNumberOfHostEggs')
                              number_of_parasite_eggs = Column(Integer, alias='EggNumberOfParasiteEggs')
                              clutch_parasite = Column(String, alias='EggClutchParasite')
                              ground_colour = Column(String, alias='EggGroundColour')
                              accessory_colouration = Column(String, alias='EggAccessoryColouration')
                              other_colouration = Column(String, alias='EggOtherColouration')
                              pattern = Column(String, alias='EggPattern')
    140213: Switched back to PartModel
    """

    __tablename__ = 'egg'

    irn = Column(Integer, ForeignKey(PartModel.irn, ondelete='CASCADE'), primary_key=True)
    clutch_size = Column(Integer, alias='EggClutchSize')
    set_mark = Column(String, alias='EggSetMark')

    __mapper_args__ = {
        'polymorphic_identity': 'egg',
        'inherit_condition': (irn == SpecimenModel.irn)
    }


class MammalGroupParentModel(SpecimenModel):
    __mapper_args__ = {
        'polymorphic_identity': 'mammal_group_parent',
    }


class MammalGroupPartModel(PartModel):
    __mapper_args__ = {
        'polymorphic_identity': 'mammal_group_part',
    }


class NestModel(SpecimenModel):

    __tablename__ = 'nest'

    irn = Column(Integer, ForeignKey(SpecimenModel.irn, ondelete='CASCADE'), primary_key=True)
    nest_shape = Column(String, alias='NesShape')
    nest_site = Column(String, alias='NesSite')

    __mapper_args__ = {
        'polymorphic_identity': 'nest',
    }


class SilicaGelSpecimenModel(SpecimenModel):

    __tablename__ = 'silica_gel'

    irn = Column(Integer, ForeignKey(SpecimenModel.irn, ondelete='CASCADE'), primary_key=True)
    population_code = Column(String, alias='SilPopulationCode')

    __mapper_args__ = {
        'polymorphic_identity': 'silica_gel',
    }


class IndexLotModel(CatalogueModel):

    __tablename__ = 'indexlot'

    irn = Column(Integer, ForeignKey(CatalogueModel.irn, ondelete='CASCADE'), primary_key=True)
    material = Column(Boolean, alias='EntIndMaterial')
    is_type = Column(Boolean, alias='EntIndType')
    media = Column(Boolean, alias='EntIndMedia')
    kind_of_material = Column(String, alias='EntIndKindOfMaterial')
    kind_of_media = Column(String, alias='EntIndKindOfMedia')
    # Index lots link through to the collection index which holds the taxonomic name
    # EntIndIndexLotTaxonNameLocalRef holds a local copy of taxonomy IRN
    taxonomy_irn = Column(Integer, ForeignKey(TaxonomyModel.irn, ondelete='SET NULL'), alias='EntIndIndexLotTaxonNameLocalRef')
    determination = relationship("TaxonomyModel")

    material_detail = relationship("IndexLotMaterialModel", cascade='all, delete-orphan')

    __mapper_args__ = {
        'polymorphic_identity': 'indexlot',
    }


class IndexLotMaterialModel(BaseMixin, Base):
    """
    Table has row for every unique series
    """
    __tablename__ = 'indexlot_material'

    id = Column(Integer, primary_key=True)
    irn = Column(Integer, ForeignKey(IndexLotModel.irn), primary_key=True)
    count = Column(Integer, alias='EntIndCount')
    sex = Column(String, alias='EntIndSex')
    types = Column(String, alias='EntIndTypes')
    stage = Column(String, alias='EntIndStage')
    primary_type_number = Column(String, alias='EntIndPrimaryTypeNo')


# Botany

class BotanySpecimenModel(SpecimenModel):
    """
    Extra botany fields.
    These are common across all botany specimen types, from the Ecology tab
    """
    __tablename__ = 'botany'

    irn = Column(Integer, ForeignKey(SpecimenModel.irn, ondelete='CASCADE'), primary_key=True)
    exsiccati = Column(String, alias='CollExsiccati')
    exsiccati_number = Column(Integer, alias='ColExsiccatiNumber')
    # This is called "Label locality" in existing NHM online DBs
    site_description = Column(String, alias='ColSiteDescription')
    plant_description = Column(String, alias='ColPlantDescription')
    habitat_verbatim = Column(String, alias='ColHabitatVerbatim')
    cultivated = Column(Boolean, alias='FeaCultivated')
    plant_form = Column(String, alias='FeaPlantForm')

    # Polymorphic identity
    __mapper_args__ = {
        'polymorphic_identity': 'botany',
        'inherit_condition': (irn == SpecimenModel.irn)
    }


# Palaeontology
class PalaeontologySpecimenModel(SpecimenModel):
    """
    Extra paleo fields

    Changelog:
    131121: Removed dating - only 84 records using it
    """
    __tablename__ = 'palaeontology'

    irn = Column(Integer, ForeignKey(SpecimenModel.irn, ondelete='CASCADE'), primary_key=True)
    # Paleo catalogue: this is used to create the fossil group
    catalogue_description = Column(String, alias='PalRegDescription')

    # Stratigraphy
    # We're going to use PalStrStratigraphyRefLocal, rather than PalStrStratigraphyRef as it has just the most current ref
    stratigraphy_irn = Column(Integer, ForeignKey(StratigraphyModel.irn), alias='PalStrStratigraphyRefLocal')
    stratigraphy = relationship("StratigraphyModel", primaryjoin="StratigraphyModel.irn == PalaeontologySpecimenModel.stratigraphy_irn")

    # Polymorphic identity
    __mapper_args__ = {
        'polymorphic_identity': 'palaeontology',
        'inherit_condition': (irn == SpecimenModel.irn)
    }


# Mineralogy

class MineralogySpecimenModel(SpecimenModel):
    """
    Extra mineralogy fields.
    These are common across mineralogy specimen types: Ore, Gem, Petrology etc.,

    Changelog:
    131125: Removed assemblage = Column(String, alias='MinMineralAssemblage')
    131206: Removed identification = Column(String, alias='MinIdentificationText') (2)
    """
    __tablename__ = 'mineralogy'

    irn = Column(Integer, ForeignKey(SpecimenModel.irn, ondelete='CASCADE'), primary_key=True)
    date_registered = Column(String, alias='MinDateRegistered')
    identification_as_registered = Column(String, alias='MinIdentificationAsRegistered')
    occurrence = Column(String, alias='MinPetOccurance')
    commodity = Column(String, alias='MinOreCommodity')
    deposit_type = Column(String, alias='MinOreDepositType')
    texture = Column(String, alias='MinTextureStructure')
    identification_variety = Column(String, alias='MinIdentificationVariety')
    identification_other = Column(String, alias='MinIdentificationOther')
    host_rock = Column(String, alias='MinHostRock')

    # Mineralogical Age
    mineralogical_age = relationship("MineralogicalAge", secondary=specimen_mineralogical_age)

    # Polymorphic identity
    __mapper_args__ = {
        'polymorphic_identity': 'mineralogy',
        'inherit_condition': (irn == SpecimenModel.irn)
    }


class MineralogicalAge(BaseMixin, Base):
    """
    Association object for Specimen => Taxonomy (determinations)

    Changelog:
    131125: Removed: method = Column(String, alias='MinAgeDataMethod'). Not used
    """

    __tablename__ = 'mineralogical_age'
    __table_args__ = (UniqueConstraint('age', 'age_type'), {'schema': keemu_schema})

    id = Column(Integer, primary_key=True)
    age = Column(String, alias='MinAgeDataAge')
    age_type = Column(String, alias='MinAgeDataType')


# Mineralogy models (based on  ColKind)

class MeteoritesSpecimenModel(SpecimenModel):

    """
    Meteorites model

    Changelog:
    131125: Removed: recovery_time = Column(String, alias='MinMetRecoveryTime'). Only 1705 records
    131125: Added registered_weight & registered_weight_unit
    131125: Removed: recovery_source = Column(String, alias='MinMetRecoverySource') only ~1000 records and contains addresses
    """

    __tablename__ = 'meteorite'

    irn = Column(Integer, ForeignKey(SpecimenModel.irn, ondelete='CASCADE'), primary_key=True)
    meteorite_type = Column(String, alias='MinMetType')
    meteorite_group = Column(String, alias='MinMetGroup')
    chondrite_achondrite = Column(String, alias='MinMetChondriteAchondrite')
    meteorite_class = Column(String, alias='MinMetClass')
    pet_type = Column(String, alias='MinMetPetType')
    pet_sub_type = Column(String, alias='MinMetPetSubtype')
    recovery = Column(String, alias='MinMetRecoveryFindFall')
    recovery_date = Column(String, alias='MinMetRecoveryDate')
    recovery_weight = Column(String, alias='MinMetRecoveryWeight')
    # TODO: Rerun to add these?
    registered_weight = Integer(String, alias='MinMetWeightAsRegistered')
    registered_weight_unit = Integer(String, alias='MinMetWeightAsRegisteredUnit')

    __mapper_args__ = {
        'polymorphic_identity': 'meteorite',
    }


