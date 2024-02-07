from itertools import chain
from pathlib import Path
from typing import List, Optional

from dataimporter.lib.dbs import Index
from dataimporter.lib.model import SourceRecord
from dataimporter.lib.view import View, ViewLink, ManyToOneViewLink, ManyToManyViewLink


class MediaLink(ManyToManyViewLink):
    """
    A ViewLink representing the connection between ecatalogue records and emultimedia
    records. This is used for all ecatalogue derivative views and their links to images
    from emultimedia, i.e. the specimens, index lots, and artefacts views. This view can
    also be used for 3D links too as they are emultimedia records.

    No specific requirements are placed on the base and foreign view types, however,
    there are some expectations. The base view's records should contain a field called
    MulMultiMediaRef which contains 1+ IDs of emultimedia records. Base records without
    this field have no associated media. Any IDs found in this field which are available
    in the foreign view are linked into the base record's transformed data dict via the
    DwC associatedMedia field, and a supplementary non-DwC associatedMediaCount field is
    also populated.
    """

    MEDIA_ID_REF_FIELD = "MulMultiMediaRef"
    MEDIA_TARGET_FIELD = "associatedMedia"
    MEDIA_COUNT_TARGET_FIELD = "associatedMediaCount"

    def __init__(self, path: Path, base_view: View, media_view: View):
        """
        :param path: the path to store the index data in
        :param base_view: the base view
        :param media_view: the media view
        """
        super().__init__(path, base_view, media_view, MediaLink.MEDIA_ID_REF_FIELD)

    def transform(self, base_record: SourceRecord, data: dict):
        """
        Add any media linked to the given record to the given data dict. This method
        accounts for the possibility that associated media already exists in the data
        dict, which is currently not a factor we have to deal with, but with the promise
        of adding 3D links will be soon.

        :param base_record: the base record object
        :param data: the data transformed from the base record object view the base
                     view's transform method
        """
        media = self.get_foreign_record_data(base_record)
        if media:
            existing_media = data.get(MediaLink.MEDIA_TARGET_FIELD, [])
            # order by media ID
            data[MediaLink.MEDIA_TARGET_FIELD] = sorted(
                chain(existing_media, media), key=lambda m: int(m["_id"])
            )
            data[MediaLink.MEDIA_COUNT_TARGET_FIELD] = len(existing_media) + len(media)


class TaxonomyLink(ManyToOneViewLink):
    """
    A ViewLink representing the connection between ecatalogue records and etaxonomy
    records. This is used for all ecatalogue derivative views and their links to
    taxonomy records etaxonomy, i.e. the specimens and index lots. Specimens and index
    lots use different fields to define the reference IDs and therefore this view link
    takes an additional field parameter to define that source field.

    The mapping is one-to-one with exactly one ID sourced from the base record. When
    transforming the base record using the linked taxonomy record, the two data dicts
    are merged.
    """

    INDEXLOT_ID_REF_FIELD = "EntIndIndexLotTaxonNameLocalRef"
    CARD_PARASITE_ID_REF_FIELD = "CardParasiteRef"

    def transform(self, base_record: SourceRecord, data: dict):
        """
        Updates the data with the taxonomy data from the linked to record (if there is
        one). This is done in place by merging the taxonomy data into the data dict and
        only adding data from the taxonomy record if it isn't already in the data dict.

        :param base_record: the base record
        :param data: the transformed data from the base record
        """
        taxonomy_data = self.get_foreign_record_data(base_record)
        if taxonomy_data is not None:
            data.update(
                (key, value) for key, value in taxonomy_data.items() if key not in data
            )


class GBIFLink(ViewLink):
    """
    A ViewLink representing the connection between specimen records and GBIF records.
    This view link is a bit different to the others in this module as it isn't a direct
    ID to ID mapping, the GBIF records map back to the specimen records via the shared
    occurrenceID field they both have.

    The mapping is one-to-one but with two ID maps to translate from GBIF ID to
    occurrenceID and then occurrenceID to specimen EMu ID.
    """

    EMU_GUID_FIELD = "AdmGUIDPreferredValue"
    GBIF_OCCURRENCE_FIELD = "occurrenceID"

    def __init__(self, path: Path, specimen_view: View, gbif_view: View):
        """
        :param path: the path to store the database data in
        :param specimen_view: the base specimen view
        :param gbif_view: the foreign GBIF view
        """
        super().__init__(path.name, specimen_view, gbif_view)
        self.path = path
        # a one-to-one index from base id -> occurrence id
        self.base_id_map = Index(path / "base_index")
        # a one-to-one index from gbif id -> occurrence id
        self.gbif_id_map = Index(path / "gbif_index")

    def update_from_base(self, base_records: List[SourceRecord]):
        """
        Extracts the AdmGUIDPreferredValue from each base record adds them to the base
        ID map. AdmGUIDPreferredValue is the source of the occurrenceID field.

        :param base_records: the changed base records
        """
        self.base_id_map.put_many(
            (base_record.id, occurrence_id)
            for base_record in base_records
            if (occurrence_id := base_record.get_first_value(GBIFLink.EMU_GUID_FIELD))
        )

    def update_from_foreign(self, gbif_records: List[SourceRecord]):
        """
        Propagate the changes in the given GBIF records to the base specimen records
        linked to them. Additionally, update the GBIF ID -> occurrenceID map.

        :param gbif_records: the updated GBIF records
        """
        # first create a dict of gbif IDs to occurrence IDs
        updates = {
            gbif_record.id: occurrence_id
            for gbif_record in gbif_records
            if (
                occurrence_id := gbif_record.get_first_value(
                    GBIFLink.GBIF_OCCURRENCE_FIELD
                )
            )
        }
        if updates:
            # update the GBIF ID -> occurrence ID map
            self.gbif_id_map.put_many(updates.items())
            # use the map we made above to map the occurrence IDs back to specimen IDs
            base_ids = {
                base_id
                for occurrence_id in updates.values()
                for base_id in self.base_id_map.get_keys(occurrence_id)
            }
            # propagate the GBIF record changes to the specimen record view queue
            if base_ids:
                base_records = list(self.base_view.db.get_records(base_ids))
                if base_records:
                    self.base_view.queue(base_records)

    def transform(self, base_record: SourceRecord, data: dict):
        """
        Updates the data with the GBIF data from the linked to GBIF record (if there is
        one). This is done in place just updating the base data dict with the GBIF data.

        :param base_record: the base record
        :param data: the transformed data from the base record
        """
        occurrence_id = base_record.get_first_value(GBIFLink.EMU_GUID_FIELD)
        if occurrence_id:
            gbif_id = self.gbif_id_map.get_key(occurrence_id)
            if gbif_id:
                gbif_data = self.foreign_view.get_and_transform(gbif_id)
                if gbif_data:
                    data.update(gbif_data)

    def clear_from_base(self):
        """
        Clears out the specimen (base) ID to occurrence ID map.
        """
        self.base_id_map.clear()

    def clear_from_foreign(self):
        """
        Clears out the gbif (foreign) ID to occurrence ID map.
        """
        self.gbif_id_map.clear()


class PreparationSpecimenLink(ViewLink):
    """
    A ViewLink representing the link between a preparation record and the specimen
    voucher record it was created from.

    The mapping is one-to-one with exactly one ID sourced from the base prep record.
    When transforming the base record using the linked specimen record, we copy some
    fields from the specimen record over to the base prep record, essentially for
    searching convenience. The full list of fields that are copied is below.
    """

    # the EMu field on the prep records which links to the specimen voucher record
    SPECIMEN_ID_REF_FIELD = "EntPreSpecimenRef"
    # the EMu field on mammal part prep records which links to the specimen voucher
    # record
    PARENT_SPECIMEN_ID_REF_FIELD = "RegRegistrationParentRef"
    # the Portal fields which are copied from the specimen to the prep data dict
    # TODO: missing CollEventDateVisitedFrom, CollEventName_tab, and kinda ColSite
    MAPPED_SPECIMEN_FIELDS = [
        "barcode",
        "scientificName",
        "order",
        "identifiedBy",
        # this is a ColSite substitute which uses sumPreciseLocation
        "locality",
        "decimalLatitude",
        "decimalLongitude",
    ]

    def __init__(self, path: Path, prep_view: View, specimen_view: View):
        """
        :param path: the path to store the ViewLink data in
        :param prep_view: the preparation view
        :param specimen_view: the specimen view
        """
        super().__init__(path.name, prep_view, specimen_view)
        self.path = path
        # a many-to-one index from prep id -> specimen id via EntPreSpecimenRef
        self.prep_id_map = Index(path / "prep_id_map")
        # a many-to-one index from prep id -> specimen id via RegRegistrationParentRef
        self.parent_id_map = Index(path / "parent_id_map")

    @staticmethod
    def _is_mammal_part_prep(record: SourceRecord) -> bool:
        return (
            record.get_first_value("ColRecordType", default="").lower()
            == "mammal group part"
        )

    def update_from_base(self, prep_records: List[SourceRecord]):
        """
        Extracts the linked foreign ID from each of the given records and adds them to
        the ID map.

        :param prep_records: the changed prep records
        """
        prep_to_specimen_map = {}
        prep_to_parent_specimen_map = {}

        for prep_record in prep_records:
            # first try to get the specimen ID using the prep voucher ref field
            specimen_id = prep_record.get_first_value(self.SPECIMEN_ID_REF_FIELD)
            if specimen_id:
                prep_to_specimen_map[prep_record.id] = specimen_id
                continue

            # no specimen ID from voucher ref, check to see if the prep is a mammal part
            if not self._is_mammal_part_prep(prep_record):
                continue

            # it is a mammal part, try getting the specimen ID using the parent ref
            parent_id = prep_record.get_first_value(self.PARENT_SPECIMEN_ID_REF_FIELD)
            if parent_id:
                prep_to_parent_specimen_map[prep_record.id] = parent_id

        # update the ID maps if needed
        if prep_to_specimen_map:
            self.prep_id_map.put_many(prep_to_specimen_map.items())
        if prep_to_parent_specimen_map:
            self.parent_id_map.put_many(prep_to_parent_specimen_map.items())

    def update_from_foreign(self, specimen_records: List[SourceRecord]):
        """
        Propagate the changes in the given specimen records to the prep records linked
        to them.

        :param specimen_records: the updated specimen records
        """
        # do a reverse lookup to find the potentially many prep IDs associated with each
        # updated specimen ID, and store them in a set. First, find them using the prep
        # ID ref field's map
        prep_ids = {
            prep_id
            for specimen_record in specimen_records
            for prep_id in self.prep_id_map.get_keys(specimen_record.id)
        }
        # now add the preps from the parent ref map
        prep_ids.update(
            prep_id
            for specimen_record in specimen_records
            for prep_id in self.parent_id_map.get_keys(specimen_record.id)
        )

        if prep_ids:
            prep_records = list(self.base_view.db.get_records(prep_ids))
            if prep_records:
                # if there are associated base records, queue changes to them on the
                # base view
                self.base_view.queue(prep_records)

    def get_foreign_record_data(self, prep_record: SourceRecord) -> Optional[dict]:
        # try with the specimen voucher ref first
        specimen_id = prep_record.get_first_value(self.SPECIMEN_ID_REF_FIELD)
        # if we can't find it using the specimen voucher ref, try the mammal parent ref
        if not specimen_id and self._is_mammal_part_prep(prep_record):
            specimen_id = prep_record.get_first_value(self.PARENT_SPECIMEN_ID_REF_FIELD)
        if specimen_id:
            return self.foreign_view.get_and_transform(specimen_id)

    def clear_from_base(self):
        """
        Clears out the ID map.
        """
        self.prep_id_map.clear()
        self.parent_id_map.clear()

    def transform(self, prep_record: SourceRecord, data: dict):
        """
        Transform the given prep record's data with data from the linked voucher
        specimen, if one exists.

        :param prep_record: the prep record
        :param data: the data dict to update
        """
        specimen_data = self.get_foreign_record_data(prep_record)
        if specimen_data is not None:
            # from DwC
            data[
                "associatedOccurrences"
            ] = f"Voucher: {specimen_data.pop('occurrenceID')}"
            # not from DwC
            data["specimenID"] = specimen_data.pop("_id")
            data.update(
                (field, value)
                for field in PreparationSpecimenLink.MAPPED_SPECIMEN_FIELDS
                if (value := specimen_data.get(field)) is not None
            )
