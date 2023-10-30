from datetime import date, datetime
from functools import partial
from itertools import groupby
from pathlib import Path
from typing import Iterable, Dict

from splitgill.manager import SplitgillClient, SplitgillDatabase
from splitgill.model import Record
from splitgill.utils import partition

from dataimporter.config import Config
from dataimporter.dbs import DataDB
from dataimporter.emu.dumps import (
    find_emu_dumps,
    is_valid_eaudit_record,
    convert_eaudit_to_delete,
    FIRST_VERSION,
)
from dataimporter.emu.views.artefact import ArtefactView
from dataimporter.emu.views.image import ImageView
from dataimporter.emu.views.indexlot import IndexLotView
from dataimporter.emu.views.mss import MSSView
from dataimporter.emu.views.specimen import SpecimenView
from dataimporter.emu.views.taxonomy import TaxonomyView
from dataimporter.ext.gbif import GBIFView, get_changed_records
from dataimporter.links import MediaLink, TaxonomyLink, GBIFLink
from dataimporter.model import SourceRecord
from dataimporter.view import View, ViewLink


class DataImporter:
    """
    Main manager class for the data importer.

    This is where all the action happens and where all the databases, views, and
    relationships are setup.
    """

    def __init__(self, config: Config):
        """
        :param config: a config object
        """
        self.config = config
        # client for mongo and elasticsearch via Splitgill
        self.client = SplitgillClient(
            config.get_mongo_client(), config.get_elasticsearch_client()
        )

        # make sure the data path exists
        self.config.data_path.mkdir(exist_ok=True)
        # create all the paths for data storage
        self.dbs_path = config.data_path / "dbs"
        self.views_path = config.data_path / "views"
        self.links_path = config.data_path / "links"
        # make sure they exist
        self.dbs_path.mkdir(exist_ok=True)
        self.views_path.mkdir(exist_ok=True)
        self.links_path.mkdir(exist_ok=True)

        # CREATE THE DATABASES
        # create the data DBs for each EMu table (note, not the eaudit table!)
        ecatalogue_db = DataDB(self.dbs_path / "ecatalogue")
        emultimedia_db = DataDB(self.dbs_path / "emultimedia")
        etaxonomy_db = DataDB(self.dbs_path / "etaxonomy")

        # create a data DB for GBIF data
        gbif_db = DataDB(self.dbs_path / "gbif")

        # CREATE THE VIEWS
        mss_view = MSSView(self.views_path / "mss", emultimedia_db)
        image_view = ImageView(self.views_path / "image", emultimedia_db)
        taxonomy_view = TaxonomyView(self.views_path / "taxonomy", etaxonomy_db)
        gbif_view = GBIFView(self.views_path / "gbif", gbif_db)
        artefact_view = ArtefactView(self.views_path / "artefact", ecatalogue_db)
        indexlot_view = IndexLotView(self.views_path / "indexlot", ecatalogue_db)
        specimen_view = SpecimenView(self.views_path / "specimen", ecatalogue_db)

        # CREATE THE VIEW LINKS
        # first artefact links
        artefact_images = MediaLink(
            self.links_path / "artefact_image", artefact_view, image_view
        )

        # next indexlot links
        indexlot_images = MediaLink(
            self.links_path / "indexlot_image", indexlot_view, image_view
        )
        indexlot_taxonomy = TaxonomyLink(
            self.links_path / "indexlot_taxonomy",
            indexlot_view,
            taxonomy_view,
            "EntIndIndexLotTaxonNameLocalRef",
        )

        # next specimen links
        specimen_images = MediaLink(
            self.links_path / "specimen_image", specimen_view, image_view
        )
        specimen_taxonomy = TaxonomyLink(
            self.links_path / "specimen_taxonomy",
            indexlot_view,
            taxonomy_view,
            "CardParasiteRef",
        )
        specimen_gbif = GBIFLink(
            self.links_path / "specimen_gbif", specimen_view, gbif_view
        )

        # SETUP STATE
        # store all the dbs, view, and links in dicts for easy access via their names
        self.dbs: Dict[str, DataDB] = {
            db.name: db for db in [ecatalogue_db, emultimedia_db, etaxonomy_db, gbif_db]
        }
        self.views: Dict[str, View] = {
            view.name: view
            for view in [
                mss_view,
                image_view,
                taxonomy_view,
                gbif_view,
                artefact_view,
                indexlot_view,
                specimen_view,
            ]
        }
        self.links: Dict[str, ViewLink] = {
            link.name: link
            for link in [
                artefact_images,
                indexlot_images,
                indexlot_taxonomy,
                specimen_images,
                specimen_taxonomy,
                specimen_gbif,
            ]
        }

        # this is where store the last date we have fully imported from EMu
        self.emu_status = EMuStatus(config.data_path / "emu_last_date.txt")

        # create the Portal side Splitgill databases for ingest and index
        self.sg_dbs = {
            "specimen": SplitgillDatabase(config.specimen_id, self.client),
            "indexlot": SplitgillDatabase(config.indexlot_id, self.client),
            "artefact": SplitgillDatabase(config.artefact_id, self.client),
            "mss": SplitgillDatabase("mss", self.client),
        }

    def _queue_changes(self, records: Iterable[SourceRecord], db_name: str):
        """
        Update the records in the data DB with the given name. The views based on the DB
        that is being updated will also be updated.

        :param records: an iterable of records to queue
        :param db_name: the name of the database to update
        """
        batch_size = 5000
        db = self.dbs[db_name]
        # find the views based on the db
        views = [view for view in self.views.values() if view.db.name == db.name]
        for batch in partition(records, batch_size):
            db.put_many(batch)
            for view in views:
                view.queue(batch)

    def queue_emu_changes(self):
        """
        Look for new EMu dumps, upsert the records into the appropriate DataDB and then
        queue the changes into the derived views.
        """
        last_queued = self.emu_status.get()
        dumps = find_emu_dumps(self.config.dumps_path, after=last_queued)

        # this is the order we want to process the tables produced on the same date in
        dump_queue_order = ["eaudit", "ecatalogue", "emultimedia", "etaxonomy"]
        # sort the dumps by date first and then the above queue order secondarily
        dumps.sort(key=lambda d: (d.date, dump_queue_order.index(d.table)))

        for dump_date, dump_group in groupby(dumps, lambda d: d.date):
            for dump in dump_group:
                # normal tables are immediately processable, but if the dump is from the
                # eaudit table we need to do some additional work because each audit
                # record refers to a potentially different table from which it is
                # deleting a record
                if dump.table != "eaudit":
                    self._queue_changes(dump.read(), dump.table)
                else:
                    # wrap the dump stream in a filter to only allow through records we
                    # want to process
                    filtered_dump = filter(
                        partial(is_valid_eaudit_record, tables=set(self.dbs.keys())),
                        dump.read(),
                    )
                    # queue the changes to each table's database in turn
                    for table, records in groupby(
                        filtered_dump, key=lambda record: record.data["AudTable"]
                    ):
                        # convert the raw audit records into delete records as we queue
                        # them
                        self._queue_changes(
                            map(convert_eaudit_to_delete, records), table
                        )
            # we've handled all the dumps from this date, update the last date stored on
            # disk in case we fail later to avoid redoing work
            self.emu_status.update(dump_date)

    def queue_gbif_changes(self):
        """
        Retrieve the latest GBIF records, check which ones have changed compared to the
        ones stored in the gbif data DB, and then queue them into the GBIF view.
        """
        self._queue_changes(
            get_changed_records(
                self.dbs["gbif"], self.config.gbif_username, self.config.gbif_password
            ),
            "gbif",
        )

    def add_to_mongo(self, view_name: str):
        """
        Add the queued changes in the given view to MongoDB.

        :param view_name: the name of the view
        """
        view = self.views[view_name]
        database = self.sg_dbs[view_name]
        database.add(
            Record(record.id, view.transform(record)) for record in view.iter_changed()
        )

    def sync_to_elasticsearch(self, sg_name: str, parallel: bool = True):
        """
        Synchronise the given Splitgill database with Elasticsearch.

        :param sg_name:
        :param parallel:
        :return:
        """
        database = self.sg_dbs[sg_name]
        database.sync(parallel=parallel)

    def __enter__(self) -> "DataImporter":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """
        Close the views and data DBs down.
        """
        for view in self.views.values():
            view.close()
        for db in self.dbs.values():
            db.close()


class EMuStatus:
    """
    Class controlling the EMu dump status.
    """

    def __init__(self, path: Path):
        """
        :param path: the file path to store the last date fully imported from EMu
        """
        self.path = path

    def get(self) -> date:
        """
        Get the last date fully imported from EMu. If no status file is found, return
        the constant FIRST_VERSION value.

        :return: a date
        """
        if not self.path.exists():
            return FIRST_VERSION

        date_as_str = self.path.read_text(encoding="utf-8").strip()
        return datetime.strptime(date_as_str, "%d-%m-%Y").date()

    def update(self, last_queued: date):
        """
        Update the last date status value with the given date.

        :param last_queued: the date to write
        """
        date_as_str = last_queued.strftime("%d-%m-%Y")
        self.path.write_text(date_as_str, encoding="utf-8")

    def clear(self):
        """
        Clear the last date status by deleting the status file.
        """
        self.path.unlink(missing_ok=True)
