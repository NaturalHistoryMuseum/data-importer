from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
from freezegun import freeze_time
from splitgill.manager import SplitgillDatabase
from splitgill.utils import to_timestamp

from dataimporter.config import Config, MongoConfig, ElasticsearchConfig
from dataimporter.importer import DataImporter, EMuStatus
from dataimporter.emu.dumps import FIRST_VERSION
from dataimporter.model import SourceRecord
from tests.helpers.samples.dumps import (
    create_ecatalogue_dump,
    create_emultimedia_dump,
    create_etaxonomy_dump,
    create_eaudit_dump,
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

        # check that the view links we expect are created
        assert "artefact_image" in importer.links
        assert "indexlot_image" in importer.links
        assert "indexlot_taxonomy" in importer.links
        assert "specimen_image" in importer.links
        assert "specimen_taxonomy" in importer.links
        assert "specimen_gbif" in importer.links

        # check that the Splitgill databases we expect are created
        assert "specimen" in importer.sg_dbs
        assert "indexlot" in importer.sg_dbs
        assert "artefact" in importer.sg_dbs
        assert "mss" in importer.sg_dbs

        importer.close()

    def test_queue_emu_changes(self, config: Config):
        config.dumps_path.mkdir(exist_ok=True)
        date_1 = "20230905"
        create_ecatalogue_dump(config.dumps_path, date_1)
        create_emultimedia_dump(config.dumps_path, date_1)
        create_etaxonomy_dump(config.dumps_path, date_1)

        with DataImporter(config) as importer:
            importer.queue_emu_changes()

            assert importer.dbs["ecatalogue"].size() == 53 + 2000 + 10000
            assert importer.dbs["emultimedia"].size() == 12242
            assert importer.dbs["etaxonomy"].size() == 1879

            assert importer.views["specimen"].changes.size() == 10000
            assert importer.views["indexlot"].changes.size() == 2000
            assert importer.views["artefact"].changes.size() == 53
            assert importer.views["image"].changes.size() == 12195
            assert importer.views["mss"].changes.size() == 12195

            # flush all the queues
            for view in importer.views.values():
                view.flush()
                assert view.changes.size() == 0

            # now create an audit dump with one image deleted which is associated with 4
            # index lots, and one artefact deleted
            indexlot_image_irn_to_delete = "4712705"
            artefact_irn_to_delete = "2475123"
            create_eaudit_dump(
                config.dumps_path,
                {
                    "emultimedia": [indexlot_image_irn_to_delete],
                    "ecatalogue": [artefact_irn_to_delete],
                },
                "20231005",
            )

            importer.queue_emu_changes()

            # the deleted image should be in the image queue
            assert importer.views["image"].changes.size() == 1
            # the deleted image should be propagated to the 4 index lots that reference
            # it, plus the deleted artefact will be queued here too
            assert importer.views["indexlot"].changes.size() == 5
            # the deleted artefact should be in the artefact queue
            assert importer.views["artefact"].changes.size() == 1

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
    @pytest.mark.usefixtures("reset_mongo")
    @pytest.mark.parametrize(
        ("name", "count"),
        [("artefact", 53), ("indexlot", 2000), ("specimen", 10000), ("mss", 12195)],
    )
    def test_add_to_mongo(self, name: str, count: int, config: Config):
        config.dumps_path.mkdir(exist_ok=True)
        dump_date = "20230905"

        if name == "mss":
            # just use emultimedia dumps for the mss view
            create_emultimedia_dump(config.dumps_path, dump_date)
        else:
            # for the other views, only use the data associated with each view, makes
            # things faster
            dump_options = {
                "include_artefacts": name == "artefact",
                "include_indexlots": name == "indexlot",
                "include_specimens": name == "specimen",
            }
            create_ecatalogue_dump(config.dumps_path, dump_date, **dump_options)
            create_emultimedia_dump(config.dumps_path, dump_date, **dump_options)
            create_etaxonomy_dump(config.dumps_path, dump_date, **dump_options)

        with DataImporter(config) as importer:
            importer.queue_emu_changes()

            importer.add_to_mongo(name)

            sg_db = importer.sg_dbs[name]

            assert sg_db.get_mongo_version() == to_timestamp(
                datetime(2023, 10, 20, 11, 4, 31)
            )
            assert sg_db.data_collection.count_documents({}) == count

    @freeze_time("2023-10-20 11:04:31")
    @pytest.mark.usefixtures("reset_mongo", "reset_elasticsearch")
    @pytest.mark.parametrize(
        ("name", "count"),
        [("artefact", 53), ("indexlot", 2000), ("specimen", 10000), ("mss", 12195)],
    )
    def test_sync_to_elasticsearch(self, name: str, count: int, config: Config):
        config.dumps_path.mkdir(exist_ok=True)
        dump_date = "20230905"

        # setup the EMu dumps we're going to use
        if name == "mss":
            # just use emultimedia dumps for the mss view
            create_emultimedia_dump(config.dumps_path, dump_date)
        else:
            # for the other views, only use the data associated with each view, makes
            # things faster
            dump_options = {
                "include_artefacts": name == "artefact",
                "include_indexlots": name == "indexlot",
                "include_specimens": name == "specimen",
            }
            create_ecatalogue_dump(config.dumps_path, dump_date, **dump_options)
            create_emultimedia_dump(config.dumps_path, dump_date, **dump_options)
            create_etaxonomy_dump(config.dumps_path, dump_date, **dump_options)

        with DataImporter(config) as importer:
            # queue the changes from the dumps
            importer.queue_emu_changes()
            # add the data to mongo
            importer.add_to_mongo(name)

            # having parallel=True seems to break in testing, maybe it's something to do
            # with the test setup or something to do with pytest, who knows
            importer.sync_to_elasticsearch(name, parallel=False)

            sg_db: SplitgillDatabase = importer.sg_dbs[name]

            assert sg_db.get_elasticsearch_version() == to_timestamp(
                datetime(2023, 10, 20, 11, 4, 31)
            )
            assert (
                config.get_elasticsearch_client().count(
                    body={}, index=sg_db.latest_index_name
                )["count"]
                == count
            )


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
