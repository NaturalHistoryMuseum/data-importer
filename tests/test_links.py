from pathlib import Path
from unittest.mock import MagicMock

from dataimporter.dbs import DataDB
from dataimporter.links import (
    MediaLink,
    TaxonomyLink,
    GBIFLink,
    PreparationSpecimenLink,
)
from dataimporter.model import SourceRecord
from dataimporter.view import View


class TestMediaLink:
    def test_update_from_base(self, tmp_path: Path):
        base_view = View(tmp_path / "base_view", DataDB(tmp_path / "base_data"))
        media_view = View(tmp_path / "media_view", DataDB(tmp_path / "media_view"))
        media_link = MediaLink(tmp_path / "media_link", base_view, media_view)

        base_records = [
            SourceRecord("b1", {MediaLink.MEDIA_ID_REF_FIELD: ("m1", "m2")}, "base"),
            SourceRecord("b2", {MediaLink.MEDIA_ID_REF_FIELD: "m3"}, "base"),
            SourceRecord("b3", {MediaLink.MEDIA_ID_REF_FIELD: "m4"}, "base"),
            SourceRecord("b4", {MediaLink.MEDIA_ID_REF_FIELD: "m2"}, "base"),
            SourceRecord("b5", {MediaLink.MEDIA_ID_REF_FIELD: ("m2", "m3")}, "base"),
            SourceRecord("b6", {"not the ref field": ("m1", "m4")}, "base"),
        ]

        media_link.update_from_base(base_records)

        assert list(media_link.id_map.get("b1")) == ["m1", "m2"]
        assert list(media_link.id_map.get("b2")) == ["m3"]
        assert list(media_link.id_map.get("b3")) == ["m4"]
        assert list(media_link.id_map.get("b4")) == ["m2"]
        assert list(media_link.id_map.get("b5")) == ["m2", "m3"]

        assert list(media_link.id_map.reverse_get("m1")) == ["b1"]
        assert list(media_link.id_map.reverse_get("m2")) == ["b1", "b4", "b5"]
        assert list(media_link.id_map.reverse_get("m3")) == ["b2", "b5"]
        assert list(media_link.id_map.reverse_get("m4")) == ["b3"]

    def test_update_from_foreign(self, tmp_path: Path):
        base_view = View(tmp_path / "base_view", DataDB(tmp_path / "base_data"))
        media_view = View(tmp_path / "media_view", DataDB(tmp_path / "media_view"))
        media_link = MediaLink(tmp_path / "media_link", base_view, media_view)

        base_records = [
            SourceRecord("b1", {MediaLink.MEDIA_ID_REF_FIELD: ("m1", "m2")}, "base"),
            SourceRecord("b2", {MediaLink.MEDIA_ID_REF_FIELD: "m3"}, "base"),
            SourceRecord("b3", {MediaLink.MEDIA_ID_REF_FIELD: "m4"}, "base"),
            SourceRecord("b4", {MediaLink.MEDIA_ID_REF_FIELD: "m2"}, "base"),
            SourceRecord("b5", {MediaLink.MEDIA_ID_REF_FIELD: ("m2", "m3")}, "base"),
            SourceRecord("b6", {"not the ref field": ("m1", "m4")}, "base"),
        ]
        # need these to be in the base database
        base_view.db.put_many(base_records)
        media_link.update_from_base(base_records)
        media_records = [
            SourceRecord("m1", {"a": "b"}, "media"),
            SourceRecord("m2", {"a": "b"}, "media"),
            # skip m3
            SourceRecord("m4", {"a": "b"}, "media"),
            SourceRecord("m5", {"a": "b"}, "media"),
        ]
        # override the queue method on the base view for testing
        base_view.queue = MagicMock()

        # the thing we're testing
        media_link.update_from_foreign(media_records)

        queued_base_records = base_view.queue.call_args.args[0]
        assert len(queued_base_records) == 4
        # b1
        assert base_records[0] in queued_base_records
        # b3
        assert base_records[2] in queued_base_records
        # b4
        assert base_records[3] in queued_base_records
        # b5
        assert base_records[4] in queued_base_records

    def test_transform_missing(self, tmp_path: Path):
        base_view = View(tmp_path / "base_view", DataDB(tmp_path / "base_data"))
        media_view = View(tmp_path / "media_view", DataDB(tmp_path / "media_view"))
        media_link = MediaLink(tmp_path / "media_link", base_view, media_view)

        base_records = [
            SourceRecord("b1", {MediaLink.MEDIA_ID_REF_FIELD: "m1"}, "base"),
        ]
        media_link.update_from_base(base_records)

        data = {}
        media_link.transform(base_records[0], data)

        assert MediaLink.MEDIA_TARGET_FIELD not in data
        assert MediaLink.MEDIA_COUNT_TARGET_FIELD not in data

    def test_transform_new_single(self, tmp_path: Path):
        base_view = View(tmp_path / "base_view", DataDB(tmp_path / "base_data"))
        media_view = View(tmp_path / "media_view", DataDB(tmp_path / "media_view"))
        media_link = MediaLink(tmp_path / "media_link", base_view, media_view)

        base_records = [
            SourceRecord("b1", {MediaLink.MEDIA_ID_REF_FIELD: "m1"}, "base"),
        ]
        media_link.update_from_base(base_records)

        # because we're not actually using the image view, just a dummy view which
        # returns the data as is when transformed, we need to add the _id into the
        # source record's data
        media_records = [
            SourceRecord("m1", {"a": "b", "_id": "m1"}, "media"),
        ]
        media_view.db.put_many(media_records)

        data = {}
        media_link.transform(base_records[0], data)

        assert data[MediaLink.MEDIA_TARGET_FIELD] == [
            media_view.transform(media_records[0])
        ]
        assert data[MediaLink.MEDIA_COUNT_TARGET_FIELD] == 1

    def test_transform_new_multiple_with_missing(self, tmp_path: Path):
        base_view = View(tmp_path / "base_view", DataDB(tmp_path / "base_data"))
        media_view = View(tmp_path / "media_view", DataDB(tmp_path / "media_view"))
        media_link = MediaLink(tmp_path / "media_link", base_view, media_view)

        base_records = [
            SourceRecord(
                "b1", {MediaLink.MEDIA_ID_REF_FIELD: ("m1", "m2", "m3", "m4")}, "base"
            )
        ]
        media_link.update_from_base(base_records)

        # because we're not actually using the image view, just a dummy view which
        # returns the data as is when transformed, we need to add the _id into the
        # source record's data
        media_records = [
            SourceRecord("m1", {"a": "b", "_id": "m1"}, "media"),
            SourceRecord("m3", {"a": "c", "_id": "m3"}, "media"),
            SourceRecord("m4", {"a": "d", "_id": "m4"}, "media"),
        ]
        media_view.db.put_many(media_records)

        data = {}
        media_link.transform(base_records[0], data)

        assert data[MediaLink.MEDIA_TARGET_FIELD] == [
            media_view.transform(media_records[0]),
            media_view.transform(media_records[1]),
            media_view.transform(media_records[2]),
        ]
        assert data[MediaLink.MEDIA_COUNT_TARGET_FIELD] == 3

    def test_transform_existing_media_in_data(self, tmp_path: Path):
        base_view = View(tmp_path / "base_view", DataDB(tmp_path / "base_data"))
        media_view = View(tmp_path / "media_view", DataDB(tmp_path / "media_view"))
        media_link = MediaLink(tmp_path / "media_link", base_view, media_view)

        base_records = [
            SourceRecord("b1", {MediaLink.MEDIA_ID_REF_FIELD: "m5"}, "base"),
        ]
        media_link.update_from_base(base_records)

        # because we're not actually using the image view, just a dummy view which
        # returns the data as is when transformed, we need to add the _id into the
        # source record's data
        media_records = [
            SourceRecord("m5", {"a": "b", "_id": "m5"}, "media"),
        ]
        media_view.db.put_many(media_records)

        # include two existing media item in the data already, with IDs that surround
        # the above media record to be added
        existing_media_data_m2 = {
            "_id": "m2",
            "a": "c",
        }
        existing_media_data_m8 = {
            "_id": "m8",
            "a": "b",
        }
        data = {
            MediaLink.MEDIA_TARGET_FIELD: [
                existing_media_data_m2,
                existing_media_data_m8,
            ],
            MediaLink.MEDIA_COUNT_TARGET_FIELD: 2,
        }
        media_link.transform(base_records[0], data)

        # check everything is included and in the right order
        assert data[MediaLink.MEDIA_TARGET_FIELD] == [
            existing_media_data_m2,
            media_view.transform(media_records[0]),
            existing_media_data_m8,
        ]
        assert data[MediaLink.MEDIA_COUNT_TARGET_FIELD] == 3

    def test_clear_from_base(self, tmp_path: Path):
        base_view = View(tmp_path / "base_view", DataDB(tmp_path / "base_data"))
        media_view = View(tmp_path / "media_view", DataDB(tmp_path / "media_view"))
        media_link = MediaLink(tmp_path / "media_link", base_view, media_view)

        base_records = [
            SourceRecord("b1", {MediaLink.MEDIA_ID_REF_FIELD: ("m1", "m2")}, "base"),
            SourceRecord("b2", {MediaLink.MEDIA_ID_REF_FIELD: "m3"}, "base"),
            SourceRecord("b3", {MediaLink.MEDIA_ID_REF_FIELD: "m4"}, "base"),
            SourceRecord("b4", {MediaLink.MEDIA_ID_REF_FIELD: "m2"}, "base"),
            SourceRecord("b5", {MediaLink.MEDIA_ID_REF_FIELD: ("m2", "m3")}, "base"),
            SourceRecord("b6", {"not the ref field": ("m1", "m4")}, "base"),
        ]
        media_link.update_from_base(base_records)
        assert media_link.id_map.size() > 0

        media_link.clear_from_base()

        assert media_link.id_map.size() == 0


class TestTaxonomyLink:
    def test_update_from_base(self, tmp_path: Path):
        field = "taxonomy_ids"

        base_view = View(tmp_path / "base_view", DataDB(tmp_path / "base_data"))
        taxonomy_view = View(tmp_path / "taxon_view", DataDB(tmp_path / "taxon_view"))
        taxonomy_link = TaxonomyLink(
            tmp_path / "taxonomy_link", base_view, taxonomy_view, field
        )

        base_records = [
            SourceRecord("b1", {field: "t1"}, "base"),
            # this scenario is not expected, but sensible to check for it given EMu can
            # do anything at any time
            SourceRecord("b2", {field: ("t2", "t3")}, "base"),
            SourceRecord("b3", {"not_the_field": "t4"}, "base"),
            SourceRecord("b4", {field: "t1"}, "base"),
        ]

        taxonomy_link.update_from_base(base_records)

        assert taxonomy_link.id_map.get_one("b1") == "t1"
        assert taxonomy_link.id_map.get_one("b2") == "t2"
        assert taxonomy_link.id_map.get_one("b3") is None
        assert taxonomy_link.id_map.get_one("b4") == "t1"

    def test_update_from_foreign(self, tmp_path: Path):
        field = "taxonomy_ids"

        base_view = View(tmp_path / "base_view", DataDB(tmp_path / "base_data"))
        taxonomy_view = View(tmp_path / "taxon_view", DataDB(tmp_path / "taxon_view"))
        taxonomy_link = TaxonomyLink(
            tmp_path / "taxonomy_link", base_view, taxonomy_view, field
        )

        base_records = [
            SourceRecord("b1", {field: "t1"}, "base"),
            # this scenario is not expected, but sensible to check for it given EMu can
            # do anything at any time
            SourceRecord("b2", {field: ("t2", "t3")}, "base"),
            SourceRecord("b3", {"not_the_field": "t4"}, "base"),
            SourceRecord("b4", {field: "t1"}, "base"),
        ]
        base_view.db.put_many(base_records)
        taxonomy_link.update_from_base(base_records)

        taxonomy_records = [
            SourceRecord("t1", {"x": "1"}, "taxon"),
            SourceRecord("t2", {"x": "2"}, "taxon"),
            SourceRecord("t3", {"x": "2"}, "taxon"),
            SourceRecord("t4", {"x": "2"}, "taxon"),
        ]

        # replace the queue method on the base view with a mock
        base_view.queue = MagicMock()

        taxonomy_link.update_from_foreign(taxonomy_records)

        queued_base_records = base_view.queue.call_args.args[0]
        assert len(queued_base_records) == 3
        # b1
        assert base_records[0] in queued_base_records
        # b2
        assert base_records[3] in queued_base_records
        # b4
        assert base_records[3] in queued_base_records

    def test_transform_missing(self, tmp_path: Path):
        field = "taxonomy_ids"

        base_view = View(tmp_path / "base_view", DataDB(tmp_path / "base_data"))
        taxonomy_view = View(tmp_path / "taxon_view", DataDB(tmp_path / "taxon_view"))
        taxonomy_link = TaxonomyLink(
            tmp_path / "taxonomy_link", base_view, taxonomy_view, field
        )

        base_record = SourceRecord("b1", {field: "t1"}, "base")
        taxonomy_link.update_from_base([base_record])
        data = {"beans": "always"}
        taxonomy_link.transform(base_record, data)

        assert data == {"beans": "always"}

    def test_transform(self, tmp_path: Path):
        field = "taxonomy_ids"

        base_view = View(tmp_path / "base_view", DataDB(tmp_path / "base_data"))
        taxonomy_view = View(tmp_path / "taxon_view", DataDB(tmp_path / "taxon_view"))
        taxonomy_link = TaxonomyLink(
            tmp_path / "taxonomy_link", base_view, taxonomy_view, field
        )

        base_record = SourceRecord("b1", {field: "t1"}, "base")
        taxonomy_link.update_from_base([base_record])

        taxonomy_record = SourceRecord("t1", {"x": "5", "y": "7"}, "taxonomy")
        taxonomy_view.db.put_many([taxonomy_record])

        data = {"x": "3", "z": "8"}
        taxonomy_link.transform(base_record, data)

        assert data == {"x": "3", "y": "7", "z": "8"}

    def test_clear_from_base(self, tmp_path: Path):
        field = "taxonomy_ids"
        base_view = View(tmp_path / "base_view", DataDB(tmp_path / "base_data"))
        taxonomy_view = View(tmp_path / "taxon_view", DataDB(tmp_path / "taxon_view"))
        taxonomy_link = TaxonomyLink(
            tmp_path / "taxonomy_link", base_view, taxonomy_view, field
        )

        base_records = [
            SourceRecord("b1", {field: "t1"}, "base"),
            SourceRecord("b2", {field: ("t2", "t3")}, "base"),
            SourceRecord("b3", {"not_the_field": "t4"}, "base"),
            SourceRecord("b4", {field: "t1"}, "base"),
        ]
        base_view.db.put_many(base_records)
        taxonomy_link.update_from_base(base_records)
        assert taxonomy_link.id_map.size() > 0

        taxonomy_link.clear_from_base()

        assert taxonomy_link.id_map.size() == 0


class TestGBIFLink:
    def test_update_from_base(self, tmp_path: Path):
        base_view = View(tmp_path / "base_view", DataDB(tmp_path / "base_data"))
        gbif_view = View(tmp_path / "gbif_view", DataDB(tmp_path / "gbif_view"))
        gbif_link = GBIFLink(tmp_path / "gbif_link", base_view, gbif_view)

        base_records = [
            SourceRecord("b1", {GBIFLink.EMU_GUID_FIELD: "guid1"}, "base"),
            # this scenario is not expected, but sensible to check for it given EMu can
            # do anything at any time
            SourceRecord("b2", {GBIFLink.EMU_GUID_FIELD: ("guid2", "guid3")}, "base"),
            SourceRecord("b3", {"not_the_field": "t4"}, "base"),
        ]

        gbif_link.update_from_base(base_records)

        assert gbif_link.base_id_map.get_one("b1") == "guid1"
        assert gbif_link.base_id_map.get_one("b2") == "guid2"
        assert gbif_link.base_id_map.get_one("b3") is None

    def test_update_from_foreign(self, tmp_path: Path):
        base_view = View(tmp_path / "base_view", DataDB(tmp_path / "base_data"))
        gbif_view = View(tmp_path / "gbif_view", DataDB(tmp_path / "gbif_view"))
        gbif_link = GBIFLink(tmp_path / "gbif_link", base_view, gbif_view)

        base_records = [
            SourceRecord("b1", {GBIFLink.EMU_GUID_FIELD: "guid1"}, "base"),
            # this scenario is not expected, but sensible to check for it given EMu can
            # do anything at any time
            SourceRecord("b2", {GBIFLink.EMU_GUID_FIELD: ("guid2", "guid3")}, "base"),
            SourceRecord("b3", {"not_the_field": "t4"}, "base"),
        ]
        base_view.db.put_many(base_records)
        gbif_link.update_from_base(base_records)

        gbif_records = [
            SourceRecord("g1", {GBIFLink.GBIF_OCCURRENCE_FIELD: "guid2"}, "taxon"),
            SourceRecord("g2", {GBIFLink.GBIF_OCCURRENCE_FIELD: "guid3"}, "taxon"),
            SourceRecord("g3", {GBIFLink.GBIF_OCCURRENCE_FIELD: "guid1"}, "taxon"),
        ]

        # replace the queue method on the base view with a mock
        base_view.queue = MagicMock()

        gbif_link.update_from_foreign(gbif_records)

        queued_base_records = base_view.queue.call_args.args[0]
        assert len(queued_base_records) == 2
        # b1
        assert base_records[0] in queued_base_records
        # b2
        assert base_records[1] in queued_base_records

        assert gbif_link.gbif_id_map.get_one("g1") == "guid2"
        assert gbif_link.gbif_id_map.get_one("g2") == "guid3"
        assert gbif_link.gbif_id_map.get_one("g3") == "guid1"

    def test_transform_missing(self, tmp_path: Path):
        base_view = View(tmp_path / "base_view", DataDB(tmp_path / "base_data"))
        gbif_view = View(tmp_path / "gbif_view", DataDB(tmp_path / "gbif_view"))
        gbif_link = GBIFLink(tmp_path / "gbif_link", base_view, gbif_view)

        base_record = SourceRecord("b1", {GBIFLink.EMU_GUID_FIELD: "guid1"}, "base")
        base_view.db.put_many([base_record])
        gbif_link.update_from_base([base_record])

        data = {"a": "54"}
        gbif_link.transform(base_record, data)

        assert data == {"a": "54"}

    def test_transform(self, tmp_path: Path):
        base_view = View(tmp_path / "base_view", DataDB(tmp_path / "base_data"))
        gbif_view = View(tmp_path / "gbif_view", DataDB(tmp_path / "gbif_view"))
        gbif_link = GBIFLink(tmp_path / "gbif_link", base_view, gbif_view)

        base_records = [
            SourceRecord("b1", {GBIFLink.EMU_GUID_FIELD: "guid1"}, "base"),
            # this scenario is not expected, but sensible to check for it given EMu can
            # do anything at any time
            SourceRecord("b2", {GBIFLink.EMU_GUID_FIELD: ("guid2", "guid3")}, "base"),
            SourceRecord("b3", {"not_the_field": "t4"}, "base"),
        ]
        base_view.db.put_many(base_records)
        gbif_link.update_from_base(base_records)

        gbif_records = [
            SourceRecord(
                "g1", {GBIFLink.GBIF_OCCURRENCE_FIELD: "guid1", "x": "5"}, "taxon"
            ),
            SourceRecord(
                "g2", {GBIFLink.GBIF_OCCURRENCE_FIELD: "guid2", "x": "7"}, "taxon"
            ),
            SourceRecord(
                "g3", {GBIFLink.GBIF_OCCURRENCE_FIELD: "guid3", "x": "100"}, "taxon"
            ),
        ]
        gbif_view.db.put_many(gbif_records)
        gbif_link.update_from_foreign(gbif_records)

        data = {}
        gbif_link.transform(base_records[0], data)

        assert data[GBIFLink.GBIF_OCCURRENCE_FIELD] == "guid1"
        assert data["x"] == "5"

    def test_clear_from_base(self, tmp_path: Path):
        base_view = View(tmp_path / "base_view", DataDB(tmp_path / "base_data"))
        gbif_view = View(tmp_path / "gbif_view", DataDB(tmp_path / "gbif_view"))
        gbif_link = GBIFLink(tmp_path / "gbif_link", base_view, gbif_view)

        base_records = [
            SourceRecord("b1", {GBIFLink.EMU_GUID_FIELD: "guid1"}, "base"),
        ]
        base_view.db.put_many(base_records)
        gbif_link.update_from_base(base_records)
        assert gbif_link.base_id_map.size() > 0

        gbif_link.clear_from_base()

        assert gbif_link.base_id_map.size() == 0

    def test_clear_from_foreign(self, tmp_path: Path):
        base_view = View(tmp_path / "base_view", DataDB(tmp_path / "base_data"))
        gbif_view = View(tmp_path / "gbif_view", DataDB(tmp_path / "gbif_view"))
        gbif_link = GBIFLink(tmp_path / "gbif_link", base_view, gbif_view)

        gbif_records = [
            SourceRecord(
                "g1", {GBIFLink.GBIF_OCCURRENCE_FIELD: "guid1", "x": "5"}, "taxon"
            ),
        ]
        gbif_view.db.put_many(gbif_records)
        gbif_link.update_from_foreign(gbif_records)
        assert gbif_link.gbif_id_map.size() > 0

        gbif_link.clear_from_foreign()

        assert gbif_link.gbif_id_map.size() == 0


class TestPreparationSpecimenLink:
    def test_update_from_base(self, tmp_path: Path):
        base_view = View(tmp_path / "base_view", DataDB(tmp_path / "base_data"))
        specimen_view = View(
            tmp_path / "specimen_view", DataDB(tmp_path / "specimen_view")
        )
        prep_link = PreparationSpecimenLink(
            tmp_path / "prep_spec_link", base_view, specimen_view
        )

        base_records = [
            SourceRecord(
                "p1", {PreparationSpecimenLink.SPECIMEN_ID_REF_FIELD: "s1"}, "base"
            ),
            # this scenario is not expected, but sensible to check for it given EMu can
            # do anything at any time
            SourceRecord(
                "p2",
                {PreparationSpecimenLink.SPECIMEN_ID_REF_FIELD: ("s2", "s3")},
                "base",
            ),
            SourceRecord("p3", {"not_the_field": "s4"}, "base"),
            SourceRecord(
                "p4", {PreparationSpecimenLink.SPECIMEN_ID_REF_FIELD: "s1"}, "base"
            ),
        ]

        prep_link.update_from_base(base_records)

        assert prep_link.id_map.get_one("p1") == "s1"
        assert prep_link.id_map.get_one("p2") == "s2"
        assert prep_link.id_map.get_one("p3") is None
        assert prep_link.id_map.get_one("p4") == "s1"

    def test_update_from_foreign(self, tmp_path: Path):
        base_view = View(tmp_path / "base_view", DataDB(tmp_path / "base_data"))
        specimen_view = View(
            tmp_path / "specimen_view", DataDB(tmp_path / "specimen_view")
        )
        prep_link = PreparationSpecimenLink(
            tmp_path / "prep_spec_link", base_view, specimen_view
        )

        base_records = [
            SourceRecord(
                "p1", {PreparationSpecimenLink.SPECIMEN_ID_REF_FIELD: "s1"}, "base"
            ),
            # this scenario is not expected, but sensible to check for it given EMu can
            # do anything at any time
            SourceRecord(
                "p2",
                {PreparationSpecimenLink.SPECIMEN_ID_REF_FIELD: ("s2", "s3")},
                "base",
            ),
            SourceRecord("p3", {"not_the_field": "s4"}, "base"),
            SourceRecord(
                "p4", {PreparationSpecimenLink.SPECIMEN_ID_REF_FIELD: "s1"}, "base"
            ),
        ]
        base_view.db.put_many(base_records)
        prep_link.update_from_base(base_records)

        specimen_records = [
            SourceRecord("s1", {"x": "1"}, "specimen"),
            SourceRecord("s2", {"x": "2"}, "specimen"),
            SourceRecord("s3", {"x": "3"}, "specimen"),
            SourceRecord("s4", {"x": "4"}, "specimen"),
        ]

        # replace the queue method on the base view with a mock
        base_view.queue = MagicMock()

        prep_link.update_from_foreign(specimen_records)

        queued_base_records = base_view.queue.call_args.args[0]
        assert len(queued_base_records) == 3
        # p1
        assert base_records[0] in queued_base_records
        # p2
        assert base_records[3] in queued_base_records
        # p4
        assert base_records[3] in queued_base_records

    def test_transform_missing(self, tmp_path: Path):
        base_view = View(tmp_path / "base_view", DataDB(tmp_path / "base_data"))
        specimen_view = View(
            tmp_path / "specimen_view", DataDB(tmp_path / "specimen_view")
        )
        prep_link = PreparationSpecimenLink(
            tmp_path / "prep_spec_link", base_view, specimen_view
        )

        base_record = SourceRecord(
            "p1", {PreparationSpecimenLink.SPECIMEN_ID_REF_FIELD: "s1"}, "base"
        )
        prep_link.update_from_base([base_record])
        data = {"beans": "always"}
        prep_link.transform(base_record, data)

        assert data == {"beans": "always"}

    def test_transform(self, tmp_path: Path):
        base_view = View(tmp_path / "base_view", DataDB(tmp_path / "base_data"))
        specimen_view = View(
            tmp_path / "specimen_view", DataDB(tmp_path / "specimen_view")
        )
        prep_link = PreparationSpecimenLink(
            tmp_path / "prep_spec_link", base_view, specimen_view
        )

        base_record = SourceRecord(
            "p1", {PreparationSpecimenLink.SPECIMEN_ID_REF_FIELD: "s1"}, "base"
        )
        prep_link.update_from_base([base_record])

        mapped_field_data = {
            field: f"{field} data"
            for field in PreparationSpecimenLink.MAPPED_SPECIMEN_FIELDS
        }
        # set one of the fields to None
        mapped_none_test_field = PreparationSpecimenLink.MAPPED_SPECIMEN_FIELDS[0]
        mapped_field_data[mapped_none_test_field] = None
        specimen_record = SourceRecord(
            "s1",
            {
                "occurrenceID": "5",
                "_id": "8",
                "an_addition_field": "some value which shouldn't be copied over",
                **mapped_field_data,
            },
            "specimen",
        )
        specimen_view.db.put_many([specimen_record])

        data = {"x": "3", "z": "9"}
        prep_link.transform(base_record, data)

        assert mapped_none_test_field not in data
        del mapped_field_data[mapped_none_test_field]
        assert data == {
            "x": "3",
            "z": "9",
            "associatedOccurrences": "Voucher: 5",
            "specimenID": "8",
            **mapped_field_data,
        }

    def test_clear_from_base(self, tmp_path: Path):
        base_view = View(tmp_path / "base_view", DataDB(tmp_path / "base_data"))
        specimen_view = View(
            tmp_path / "specimen_view", DataDB(tmp_path / "specimen_view")
        )
        prep_link = PreparationSpecimenLink(
            tmp_path / "prep_spec_link", base_view, specimen_view
        )

        base_records = [
            SourceRecord(
                "p1", {PreparationSpecimenLink.SPECIMEN_ID_REF_FIELD: "s1"}, "base"
            ),
            SourceRecord(
                "p2",
                {PreparationSpecimenLink.SPECIMEN_ID_REF_FIELD: ("s2", "s3")},
                "base",
            ),
            SourceRecord("p3", {"not_the_field": "s4"}, "base"),
            SourceRecord(
                "p4", {PreparationSpecimenLink.SPECIMEN_ID_REF_FIELD: "s1"}, "base"
            ),
        ]
        base_view.db.put_many(base_records)
        prep_link.update_from_base(base_records)
        assert prep_link.id_map.size() > 0

        prep_link.clear_from_base()

        assert prep_link.id_map.size() == 0
