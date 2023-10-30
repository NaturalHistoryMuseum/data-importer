from itertools import chain
from operator import itemgetter
from pathlib import Path
from typing import List

from dataimporter.dbs import Index
from dataimporter.model import SourceRecord
from dataimporter.view import View, ViewLink


class MediaLink(ViewLink):
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
        super().__init__(path.name, base_view, media_view)
        self.path = path
        # a one-to-many index from base id -> media ids
        self.id_map = Index(path / "id_map")

    def update_from_base(self, base_records: List[SourceRecord]):
        """
        Update the ID map with the IDs of each base record provided along with the IDs
        of the media records that are linked to each base record. This is a one to many
        mapping as multiple media IDs can be present on each base record.

        :param base_records: the updated base records
        """
        self.id_map.put_one_to_many(
            (record.id, record.iter_all_values(MediaLink.MEDIA_ID_REF_FIELD))
            for record in base_records
        )

    def update_from_foreign(self, media_records: List[SourceRecord]):
        """
        Propagate the changes in the given media records to the base records linked to
        them.

        :param media_records: the updated media records
        """
        # do a reverse lookup to find the potentially many base IDs associated with each
        # updated media ID, and store them in a set
        base_ids = {
            base_id
            for media_record in media_records
            for base_id in self.id_map.reverse_get(media_record.id)
        }

        if base_ids:
            base_records = list(self.base_view.db.get_records(base_ids))
            if base_records:
                # if there are associated base records, queue changes to them on the
                # base view
                self.base_view.queue(base_records)

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
        media = list(
            self.foreign_view.find_and_transform(
                base_record.iter_all_values(MediaLink.MEDIA_ID_REF_FIELD)
            )
        )
        if media:
            existing_media = data.get(MediaLink.MEDIA_TARGET_FIELD, [])
            # TODO: could we order in a more useful way, e.g. category?
            data[MediaLink.MEDIA_TARGET_FIELD] = sorted(
                chain(existing_media, media), key=itemgetter("_id")
            )
            # TODO: integer?
            data[MediaLink.MEDIA_COUNT_TARGET_FIELD] = len(existing_media) + len(media)

    def clear_from_base(self):
        """
        Clears out the ID map.
        """
        self.id_map.clear()


class TaxonomyLink(ViewLink):
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

    def __init__(self, path: Path, base_view: View, taxonomy_view: View, field: str):
        """
        :param path: the storage path for the ID map database
        :param base_view: the base view
        :param taxonomy_view: the taxonomy view
        :param field: the field on base records which contains the linked taxonomy
                      record ID
        """
        super().__init__(path.name, base_view, taxonomy_view)
        self.path = path
        self.field = field
        # a one-to-one index from base id -> taxonomy id
        self.id_map = Index(path / "id_map")

    def update_from_base(self, base_records: List[SourceRecord]):
        """
        Extracts the linked taxonomy ID from each of the given records and adds them to
        the ID map.

        :param base_records: the changed base records
        """
        self.id_map.put_one_to_one(
            (base_record.id, taxonomy_id)
            for base_record in base_records
            if (taxonomy_id := base_record.get_first_value(self.field))
        )

    def update_from_foreign(self, taxonomy_records: List[SourceRecord]):
        """
        Propagate the changes in the given taxonomy records to the base records linked
        to them.

        :param taxonomy_records: the updated taxonomy records
        """
        # do a reverse lookup to find the potentially many base IDs associated with each
        # updated taxonomy ID, and store them in a set
        base_ids = {
            base_id
            for taxonomy_record in taxonomy_records
            for base_id in self.id_map.reverse_get(taxonomy_record.id)
        }

        if base_ids:
            base_records = list(self.base_view.db.get_records(base_ids))
            if base_records:
                # if there are associated base records, queue changes to them on the
                # base view
                self.base_view.queue(base_records)

    def transform(self, base_record: SourceRecord, data: dict):
        """
        Updates the data with the taxonomy data from the linked to record (if there is
        one). This is done in place by merging the taxonomy data into the data dict and
        only adding data from the taxonomy record if it isn't already in the data dict.

        :param base_record: the base record
        :param data: the transformed data from the base record
        """
        taxonomy_id = base_record.get_first_value(self.field)
        if taxonomy_id:
            taxonomy = self.foreign_view.get_and_transform(taxonomy_id)
            if taxonomy is not None:
                data.update(
                    (key, value) for key, value in taxonomy.items() if key not in data
                )

    def clear_from_base(self):
        """
        Clears out the ID map.
        """
        self.id_map.clear()


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
        self.base_id_map.put_one_to_one(
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
            self.gbif_id_map.put_one_to_one(updates.items())
            # use the map we made above to map the occurrence IDs back to specimen IDs
            base_ids = {
                base_id
                for occurrence_id in updates.values()
                for base_id in self.base_id_map.reverse_get(occurrence_id)
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
        occurrence_id = base_record.get_first_value("AdmGUIDPreferredValue")
        if occurrence_id:
            gbif_id = self.gbif_id_map.reverse_get_one(occurrence_id)
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