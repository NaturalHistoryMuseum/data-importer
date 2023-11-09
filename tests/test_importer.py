from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
from elasticsearch_dsl import Search
from freezegun import freeze_time
from splitgill.utils import to_timestamp

from dataimporter.lib.config import Config, MongoConfig, ElasticsearchConfig
from dataimporter.emu.dumps import FIRST_VERSION
from dataimporter.importer import DataImporter, EMuStatus
from dataimporter.lib.model import SourceRecord
from tests.helpers.dumps import create_dump
from tests.helpers.dumps import (
    create_ecatalogue,
    EcatalogueType,
    create_emultimedia,
    create_etaxonomy,
    create_eaudit,
)


@pytest.fixture
def config(tmp_path: Path) -> Config:
    mongo_config = MongoConfig("mongo", 27017)
    elasticsearch_config = ElasticsearchConfig(["http://es:9200"])
    return Config(
        data_path=tmp_path / "data",
        dumps_path=tmp_path / "dumps",
        specimen_id="specimen-id",
        artefact_id="artefact-id",
        indexlot_id="indexlot-id",
        preparation_id="preparation-id",
        iiif_base_url="https://not.a.real.domain.com/media",
        mongo_config=mongo_config,
        es_config=elasticsearch_config,
        gbif_username=None,
        gbif_password=None,
    )


class TestDataImporter:
    def test_init(self, config: Config):
        # just running this and making sure it doesn't explode is a pretty good starting
        # point given how much is done in this init
        importer = DataImporter(config)

        # check the databases that we expect are created
        assert "ecatalogue" in importer.dbs
        assert "emultimedia" in importer.dbs
        assert "etaxonomy" in importer.dbs
        assert "gbif" in importer.dbs

        # check the views that we expect are created
        assert "mss" in importer.views
        assert "image" in importer.views
        assert "taxonomy" in importer.views
        assert "gbif" in importer.views
        assert "artefact" in importer.views
        assert "indexlot" in importer.views
        assert "specimen" in importer.views
        assert "preparation" in importer.views

        def check_view_link(name):
            assert name in importer.links
            base_name, foreign_name = name.split("_")
            assert importer.links[name].base_view is importer.views[base_name]
            assert importer.links[name].foreign_view is importer.views[foreign_name]

        # check that the view links we expect are created
        check_view_link("artefact_image")
        check_view_link("indexlot_image")
        check_view_link("indexlot_taxonomy")
        check_view_link("specimen_image")
        check_view_link("specimen_taxonomy")
        check_view_link("specimen_gbif")
        check_view_link("preparation_specimen")

        # check that the Splitgill databases we expect are created
        assert "specimen" in importer.sg_dbs
        assert "indexlot" in importer.sg_dbs
        assert "artefact" in importer.sg_dbs
        assert "mss" in importer.sg_dbs
        assert "preparation" in importer.sg_dbs

        importer.close()

    def test_queue_emu_changes(self, config: Config):
        importer = DataImporter(config)

        first_dump_date = date(2023, 10, 3)
        # create an ecatalogue dump with one record per view
        create_dump(
            config.dumps_path,
            "ecatalogue",
            first_dump_date,
            create_ecatalogue(
                "1", EcatalogueType.specimen, MulMultiMediaRef="1", CardParasiteRef="1"
            ),
            create_ecatalogue(
                "2",
                EcatalogueType.indexlot,
                MulMultiMediaRef="2",
                EntIndIndexLotTaxonNameLocalRef="2",
            ),
            create_ecatalogue("3", EcatalogueType.artefact, MulMultiMediaRef="3"),
            create_ecatalogue("4", EcatalogueType.preparation, EntPreSpecimenRef="1"),
        )
        # create an emultimedia dump with 3 images each with an ID that matches the
        # linked IDs above in the ecatalogue dump via the MulMultiMediaRef field
        create_dump(
            config.dumps_path,
            "emultimedia",
            first_dump_date,
            create_emultimedia("1"),
            create_emultimedia("2"),
            create_emultimedia("3"),
        )
        # create an etaxonomy dump with 2 records one matching the specimen made above
        # and one matching the index lot
        create_dump(
            config.dumps_path,
            "etaxonomy",
            first_dump_date,
            create_etaxonomy("1"),
            create_etaxonomy("2"),
        )

        importer.queue_emu_changes()

        assert importer.dbs["ecatalogue"].size() == 4
        assert importer.dbs["emultimedia"].size() == 3
        assert importer.dbs["etaxonomy"].size() == 2
        assert importer.views["specimen"].changes.size() == 1
        assert importer.views["indexlot"].changes.size() == 1
        assert importer.views["artefact"].changes.size() == 1
        assert importer.views["preparation"].changes.size() == 1
        assert importer.views["image"].changes.size() == 3
        assert importer.views["mss"].changes.size() == 3

        # flush all the view queues
        for view in importer.views.values():
            view.flush()
            assert view.changes.size() == 0

        second_dump_date = date(2023, 10, 4)
        create_dump(
            config.dumps_path,
            "eaudit",
            second_dump_date,
            # delete the index lot
            create_eaudit("2", "ecatalogue"),
            # delete the media on the artefact
            create_eaudit("3", "emultimedia"),
            # delete the taxonomy of the specimen
            create_eaudit("1", "etaxonomy"),
        )

        importer.queue_emu_changes()

        # these should all be the same
        assert importer.dbs["ecatalogue"].size() == 4
        assert importer.dbs["emultimedia"].size() == 3
        assert importer.dbs["etaxonomy"].size() == 2
        # 1 indexlot delete + specimen update because of the taxonomy delete
        assert importer.views["specimen"].changes.size() == 2
        # 1 indexlot delete
        assert importer.views["indexlot"].changes.size() == 1
        # 1 indexlot delete + artefact update because of the multimedia delete
        assert importer.views["artefact"].changes.size() == 2
        # 1 indexlot delete, 1 specimen change by taxonomy change which is pushed down
        assert importer.views["preparation"].changes.size() == 2
        # 1 multimedia delete
        assert importer.views["image"].changes.size() == 1
        # 1 multimedia delete
        assert importer.views["mss"].changes.size() == 1

        for view in importer.views.values():
            view.flush()
            assert view.changes.size() == 0

        third_dump_date = date(2023, 10, 8)
        # update all the multimedia records + a new record unlinked to anything
        create_dump(
            config.dumps_path,
            "emultimedia",
            third_dump_date,
            create_emultimedia("1"),
            create_emultimedia("2"),
            create_emultimedia("3"),
            create_emultimedia("4"),
        )

        importer.queue_emu_changes()

        assert importer.dbs["ecatalogue"].size() == 4
        # there's a new emultimedia record now
        assert importer.dbs["emultimedia"].size() == 4
        assert importer.dbs["etaxonomy"].size() == 2
        # an image update on an associated image, so 1
        assert importer.views["specimen"].changes.size() == 1
        # an image update on an associated image, so 1
        assert importer.views["indexlot"].changes.size() == 1
        # an image update on an associated image, so 1
        assert importer.views["artefact"].changes.size() == 1
        # an image update on an associated specimen's image, so 1
        assert importer.views["preparation"].changes.size() == 1
        assert importer.views["image"].changes.size() == 4
        assert importer.views["mss"].changes.size() == 4

    def test_queue_gbif_changes(self, config: Config):
        gbif_records = [
            SourceRecord("1", {"x": "1"}, "gbif"),
            SourceRecord("2", {"x": "1"}, "gbif"),
            SourceRecord("3", {"x": "3"}, "gbif"),
        ]
        change_records_mock = MagicMock(return_value=gbif_records)
        with patch("dataimporter.importer.get_changed_records", change_records_mock):
            with DataImporter(config) as importer:
                importer.queue_gbif_changes()

                assert importer.dbs["gbif"].size() == 3
                assert importer.views["gbif"].changes.size() == 3

    @freeze_time("2023-10-20 11:04:31")
    @pytest.mark.usefixtures("reset_mongo", "reset_elasticsearch")
    def test_add_to_mongo_and_sync_to_elasticsearch_artefact(self, config: Config):
        name = "artefact"
        # before the frozen time
        dump_date = date(2023, 10, 3)
        # create an ecatalogue dump with 8 artefacts
        create_dump(
            config.dumps_path,
            "ecatalogue",
            dump_date,
            *[
                create_ecatalogue(
                    str(i),
                    EcatalogueType[name],
                    MulMultiMediaRef=str(i),
                    PalArtObjectName=f"{i} beans",
                )
                for i in range(1, 9)
            ],
        )
        # create an emultimedia dump with 8 images
        create_dump(
            config.dumps_path,
            "emultimedia",
            dump_date,
            *[create_emultimedia(str(i), MulTitle=f"image {i}") for i in range(1, 9)],
        )

        with DataImporter(config) as importer:
            importer.queue_emu_changes()

            importer.add_to_mongo(name)

            sg_db = importer.sg_dbs[name]
            assert sg_db.get_mongo_version() == to_timestamp(
                datetime(2023, 10, 20, 11, 4, 31)
            )
            assert sg_db.data_collection.count_documents({}) == 8

            # having parallel=True seems to break in testing, maybe it's something to do
            # with the test setup or something to do with pytest, who knows
            importer.sync_to_elasticsearch(name, parallel=False)

            assert sg_db.get_elasticsearch_version() == to_timestamp(
                datetime(2023, 10, 20, 11, 4, 31)
            )

            search_base = Search(
                using=config.get_elasticsearch_client(), index=sg_db.latest_index_name
            )
            assert search_base.count() == 8
            assert (
                search_base.filter(
                    "term", **{"parsed.artefactName.k": "3 beans"}
                ).count()
                == 1
            )
            # this comes from the image
            assert (
                search_base.filter(
                    "term", **{"parsed.associatedMedia.title.k": "image 4"}
                ).count()
                == 1
            )

    @freeze_time("2023-10-20 11:04:31")
    @pytest.mark.usefixtures("reset_mongo", "reset_elasticsearch")
    def test_add_to_mongo_and_sync_to_elasticsearch_indexlot(self, config: Config):
        name = "indexlot"
        # before the frozen time
        dump_date = date(2023, 10, 3)
        # create an ecatalogue dump with 8 indexlots
        create_dump(
            config.dumps_path,
            "ecatalogue",
            dump_date,
            *[
                create_ecatalogue(
                    str(i),
                    EcatalogueType[name],
                    MulMultiMediaRef=str(i),
                    EntIndIndexLotTaxonNameLocalRef=str(i),
                    EntIndMaterial=f"{i} lemons",
                )
                for i in range(1, 9)
            ],
        )
        # create an emultimedia dump with 8 images
        create_dump(
            config.dumps_path,
            "emultimedia",
            dump_date,
            *[create_emultimedia(str(i), MulTitle=f"image {i}") for i in range(1, 9)],
        )
        # create an etaxonomy dump with 8 records
        create_dump(
            config.dumps_path,
            "etaxonomy",
            dump_date,
            *[create_etaxonomy(str(i), ClaKingdom=f"kingdom {i}") for i in range(1, 9)],
        )

        with DataImporter(config) as importer:
            importer.queue_emu_changes()

            importer.add_to_mongo(name)

            sg_db = importer.sg_dbs[name]
            assert sg_db.get_mongo_version() == to_timestamp(
                datetime(2023, 10, 20, 11, 4, 31)
            )
            assert sg_db.data_collection.count_documents({}) == 8

            # having parallel=True seems to break in testing, maybe it's something to do
            # with the test setup or something to do with pytest, who knows
            importer.sync_to_elasticsearch(name, parallel=False)

            assert sg_db.get_elasticsearch_version() == to_timestamp(
                datetime(2023, 10, 20, 11, 4, 31)
            )
            search_base = Search(
                using=config.get_elasticsearch_client(), index=sg_db.latest_index_name
            )
            assert search_base.count() == 8
            assert (
                search_base.filter("term", **{"parsed.material.k": "3 lemons"}).count()
                == 1
            )
            # this comes from the image
            assert (
                search_base.filter(
                    "term", **{"parsed.associatedMedia.title.k": "image 4"}
                ).count()
                == 1
            )
            # this comes from the taxonomy
            assert (
                search_base.filter("term", **{"parsed.kingdom.k": "kingdom 4"}).count()
                == 1
            )

    @freeze_time("2023-10-20 11:04:31")
    @pytest.mark.usefixtures("reset_mongo", "reset_elasticsearch")
    def test_add_to_mongo_and_sync_to_elasticsearch_specimen(self, config: Config):
        name = "specimen"
        # before the frozen time
        dump_date = date(2023, 10, 3)
        # create an ecatalogue dump with 8 specimens
        create_dump(
            config.dumps_path,
            "ecatalogue",
            dump_date,
            *[
                create_ecatalogue(
                    str(i),
                    EcatalogueType[name],
                    MulMultiMediaRef=str(i),
                    CardParasiteRef=str(i),
                    sumPreciseLocation=f"{i} Number Road",
                )
                for i in range(1, 9)
            ],
        )
        # create an emultimedia dump with 8 images
        create_dump(
            config.dumps_path,
            "emultimedia",
            dump_date,
            *[create_emultimedia(str(i), MulTitle=f"image {i}") for i in range(1, 9)],
        )
        # create an etaxonomy dump with 8 records
        create_dump(
            config.dumps_path,
            "etaxonomy",
            dump_date,
            *[create_etaxonomy(str(i), ClaKingdom=f"kingdom {i}") for i in range(1, 9)],
        )

        with DataImporter(config) as importer:
            importer.queue_emu_changes()

            importer.add_to_mongo(name)

            sg_db = importer.sg_dbs[name]
            assert sg_db.get_mongo_version() == to_timestamp(
                datetime(2023, 10, 20, 11, 4, 31)
            )
            assert sg_db.data_collection.count_documents({}) == 8

            # having parallel=True seems to break in testing, maybe it's something to do
            # with the test setup or something to do with pytest, who knows
            importer.sync_to_elasticsearch(name, parallel=False)

            assert sg_db.get_elasticsearch_version() == to_timestamp(
                datetime(2023, 10, 20, 11, 4, 31)
            )
            search_base = Search(
                using=config.get_elasticsearch_client(), index=sg_db.latest_index_name
            )
            assert search_base.count() == 8
            assert (
                search_base.filter(
                    "term", **{"parsed.locality.k": "3 Number Road"}
                ).count()
                == 1
            )
            # this comes from the image
            assert (
                search_base.filter(
                    "term", **{"parsed.associatedMedia.title.k": "image 4"}
                ).count()
                == 1
            )
            # this comes from the taxonomy
            assert (
                search_base.filter("term", **{"parsed.kingdom.k": "kingdom 4"}).count()
                == 1
            )

    @freeze_time("2023-10-20 11:04:31")
    @pytest.mark.usefixtures("reset_mongo", "reset_elasticsearch")
    def test_add_to_mongo_and_sync_to_elasticsearch_mss(self, config: Config):
        name = "mss"
        # before the frozen time
        dump_date = date(2023, 10, 3)
        # create an emultimedia dump with 8 images
        create_dump(
            config.dumps_path,
            "emultimedia",
            dump_date,
            *[
                create_emultimedia(str(i), DocIdentifier=f"banana-{i}.jpg")
                for i in range(1, 9)
            ],
        )

        with DataImporter(config) as importer:
            importer.queue_emu_changes()

            importer.add_to_mongo(name)

            sg_db = importer.sg_dbs[name]
            assert sg_db.get_mongo_version() == to_timestamp(
                datetime(2023, 10, 20, 11, 4, 31)
            )
            assert sg_db.data_collection.count_documents({}) == 8

            # having parallel=True seems to break in testing, maybe it's something to do
            # with the test setup or something to do with pytest, who knows
            importer.sync_to_elasticsearch(name, parallel=False)

            assert sg_db.get_elasticsearch_version() == to_timestamp(
                datetime(2023, 10, 20, 11, 4, 31)
            )
            search_base = Search(
                using=config.get_elasticsearch_client(), index=sg_db.latest_index_name
            )
            assert search_base.count() == 8
            assert (
                search_base.filter("term", **{"parsed.file.k": "banana-4.jpg"}).count()
                == 1
            )

    @freeze_time("2023-10-20 11:04:31")
    @pytest.mark.usefixtures("reset_mongo", "reset_elasticsearch")
    def test_add_to_mongo_and_sync_to_elasticsearch_preparation(self, config: Config):
        name = "preparation"
        # before the frozen time
        dump_date = date(2023, 10, 3)
        # create an ecatalogue dump with 8 specimens and 8 preparations
        ecat_records = [
            *[
                create_ecatalogue(
                    str(i),
                    EcatalogueType[name],
                    EntPreSpecimenRef=str(i + 8),
                    EntPreStorageMedium=f"Ethanol ({i}%)",
                )
                for i in range(1, 9)
            ],
            *[
                create_ecatalogue(
                    str(i),
                    EcatalogueType.specimen,
                    MulMultiMediaRef=str(i),
                    CardParasiteRef=str(i),
                    EntCatBarcode=f"000-00-0-{i}",
                )
                for i in range(9, 17)
            ],
        ]
        create_dump(config.dumps_path, "ecatalogue", dump_date, *ecat_records)
        # create an emultimedia dump with 8 images
        create_dump(
            config.dumps_path,
            "emultimedia",
            dump_date,
            *[create_emultimedia(str(i), MulTitle=f"image {i}") for i in range(9, 17)],
        )
        # create an etaxonomy dump with 8 records
        create_dump(
            config.dumps_path,
            "etaxonomy",
            dump_date,
            *[create_etaxonomy(str(i), ClaOrder=f"order {i}") for i in range(9, 17)],
        )

        with DataImporter(config) as importer:
            importer.queue_emu_changes()

            importer.add_to_mongo(name)

            sg_db = importer.sg_dbs[name]
            assert sg_db.get_mongo_version() == to_timestamp(
                datetime(2023, 10, 20, 11, 4, 31)
            )
            assert sg_db.data_collection.count_documents({}) == 8

            # having parallel=True seems to break in testing, maybe it's something to do
            # with the test setup or something to do with pytest, who knows
            importer.sync_to_elasticsearch(name, parallel=False)

            assert sg_db.get_elasticsearch_version() == to_timestamp(
                datetime(2023, 10, 20, 11, 4, 31)
            )
            search_base = Search(
                using=config.get_elasticsearch_client(), index=sg_db.latest_index_name
            )
            assert search_base.count() == 8
            assert (
                search_base.filter(
                    "term", **{"parsed.mediumType.k": "Ethanol (6%)"}
                ).count()
                == 1
            )
            # check a field that should have been copied from the voucher specimen
            assert (
                search_base.filter(
                    "term", **{"parsed.barcode.k": "000-00-0-12"}
                ).count()
                == 1
            )
            # check a field that should have been copied from the voucher specimen's
            # taxonomy
            assert (
                search_base.filter("term", **{"parsed.order.k": "order 11"}).count()
                == 1
            )

    def test_queue_changes_redactions(self, config: Config):
        importer = DataImporter(config)

        changed_records = [
            SourceRecord("1", {"a": "a"}, "test"),
            SourceRecord("2", {"a": "b"}, "test"),
            SourceRecord("3", {"a": "c"}, "test"),
        ]

        # redact records 2 and 3
        importer.redaction_database.add_ids("ecatalogue", ["2", "3"], "reason_1")

        # queue all the change records
        importer.queue_changes(changed_records, "ecatalogue")

        assert "1" in importer.dbs["ecatalogue"]
        assert "2" not in importer.dbs["ecatalogue"]
        assert "3" not in importer.dbs["ecatalogue"]

    def test_redact_records(self, config: Config):
        importer = DataImporter(config)

        records = [
            SourceRecord("1", {"a": "a"}, "test"),
            SourceRecord("2", {"a": "b"}, "test"),
            SourceRecord("3", {"a": "c"}, "test"),
        ]

        # queue all the records
        importer.queue_changes(records, "ecatalogue")

        # redact records 2 and 3
        redacted_count = importer.redact_records("ecatalogue", ["2", "3"], "reason1")

        assert redacted_count == 2
        assert "1" in importer.dbs["ecatalogue"]
        assert "2" not in importer.dbs["ecatalogue"]
        assert "3" not in importer.dbs["ecatalogue"]


class TestEMuStatus:
    def test_get_default(self, tmp_path: Path):
        status = EMuStatus(tmp_path / "status.txt")
        assert status.get() == FIRST_VERSION

    def test_get(self, tmp_path: Path):
        status = EMuStatus(tmp_path / "status.txt")
        status.path.write_text("23-02-2021", encoding="utf-8")

        assert status.get() == date(2021, 2, 23)

    def test_update_no_file(self, tmp_path: Path):
        status = EMuStatus(tmp_path / "status.txt")

        status.update(date(2023, 4, 19))

        assert status.path.exists()
        assert status.path.read_text(encoding="utf-8") == "19-04-2023"

    def test_update_with_file(self, tmp_path: Path):
        status = EMuStatus(tmp_path / "status.txt")
        status.path.write_text("03-11-2023")

        status.update(date(2023, 4, 19))

        assert status.path.exists()
        assert status.path.read_text(encoding="utf-8") == "19-04-2023"

    def test_clear_no_file(self, tmp_path: Path):
        status = EMuStatus(tmp_path / "status.txt")
        status.clear()
        assert not status.path.exists()

    def test_clear_with_file(self, tmp_path: Path):
        status = EMuStatus(tmp_path / "status.txt")
        status.path.write_text("17-12-2022")
        status.clear()
        assert not status.path.exists()
