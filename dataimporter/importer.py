from datetime import date, datetime
from functools import partial
from itertools import groupby
from pathlib import Path
from typing import Iterable, List, Optional, Union

from splitgill.manager import SplitgillClient, SplitgillDatabase
from splitgill.model import Record
from splitgill.utils import partition, now

from dataimporter.emu.dumps import (
    find_emu_dumps,
    is_valid_eaudit_record,
    convert_eaudit_to_delete,
    FIRST_VERSION,
)
from dataimporter.emu.views.artefact import ArtefactView
from dataimporter.emu.views.image import ImageView
from dataimporter.emu.views.indexlot import IndexLotView
from dataimporter.emu.views.mammalpart import MammalPartView
from dataimporter.emu.views.mss import MSSView
from dataimporter.emu.views.preparation import PreparationView
from dataimporter.emu.views.specimen import SpecimenView
from dataimporter.emu.views.taxonomy import TaxonomyView
from dataimporter.ext.gbif import GBIFView, get_changed_records
from dataimporter.lib.config import Config
from dataimporter.lib.dbs import Store
from dataimporter.lib.model import SourceRecord
from dataimporter.lib.options import DEFAULT_OPTIONS
from dataimporter.lib.view import View


class StoreNotFound(Exception):
    def __init__(self, name: str):
        super().__init__(f"Store {name} not found")
        self.name = name


class ViewNotFound(Exception):
    def __init__(self, name: str):
        super().__init__(f"View {name} not found")
        self.name = name


class ViewIsNotPublished(Exception):
    def __init__(self, view: View):
        super().__init__(f"View {view.name} does not have a Splitgill database")
        self.view = view.name


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
        self.stores_path = config.data_path / "stores"
        self.views_path = config.data_path / "views"
        # make sure they exist
        self.stores_path.mkdir(exist_ok=True)
        self.views_path.mkdir(exist_ok=True)

        # create the stores we need (note not eaudit!)
        ecatalogue_store = Store(self.stores_path / "ecatalogue")
        emultimedia_store = Store(self.stores_path / "emultimedia")
        etaxonomy_store = Store(self.stores_path / "etaxonomy")
        gbif_store = Store(self.stores_path / "gbif")
        self.stores = [ecatalogue_store, emultimedia_store, etaxonomy_store, gbif_store]

        # create the views we need
        mss_view = MSSView(self.views_path / "mss", emultimedia_store, "mss")
        image_view = ImageView(
            self.views_path / "image", emultimedia_store, config.iiif_base_url
        )
        taxonomy_view = TaxonomyView(self.views_path / "taxonomy", etaxonomy_store)
        gbif_view = GBIFView(self.views_path / "gbif", gbif_store)
        artefact_view = ArtefactView(
            self.views_path / "artefact",
            ecatalogue_store,
            image_view,
            config.artefact_id,
        )
        indexlot_view = IndexLotView(
            self.views_path / "indexlot",
            ecatalogue_store,
            image_view,
            taxonomy_view,
            config.indexlot_id,
        )
        mammal_part_view = MammalPartView(
            self.views_path / "mammalpart", ecatalogue_store
        )
        specimen_view = SpecimenView(
            self.views_path / "specimen",
            ecatalogue_store,
            image_view,
            taxonomy_view,
            gbif_view,
            mammal_part_view,
            config.specimen_id,
        )
        prep_view = PreparationView(
            self.views_path / "preparation",
            ecatalogue_store,
            specimen_view,
            config.preparation_id,
        )
        self.views = [
            image_view,
            mss_view,
            taxonomy_view,
            gbif_view,
            artefact_view,
            indexlot_view,
            specimen_view,
            prep_view,
            mammal_part_view,
        ]

        # this is where store the last date we have fully imported from EMu
        self.emu_status = EMuStatus(config.data_path / "emu_last_date.txt")

    def get_store(self, name: str) -> Store:
        """
        Get the store with the given name. If the store doesn't exist, a StoreNotFound
        exception is raised.

        :param name: the name of the store
        :return: the Store instance
        """
        for store in self.stores:
            if store.name == name:
                return store
        raise StoreNotFound(name)

    def get_view(self, name: str) -> View:
        """
        Get the view with the given name. If the view doesn't exist, a ViewNotFound
        exception is raised.

        :param name: the name of the view
        :return: the View instance
        """
        for view in self.views:
            if view.name == name:
                return view
        raise ViewNotFound(name)

    def get_database(self, view: Union[str, View]) -> SplitgillDatabase:
        """
        Returns a new SplitgillDatabase instance for the given view. If the view doesn't
        have an associated SplitgillDatabase name, then a ViewDoesNotHaveDatabase
        exception is raised. If the view parameter is passed as a str and the view does
        not exist, a ViewNotFound exception is raised.

        :param view: a View instance or a view's name
        :return: a SplitgillDatabase instance
        """
        if isinstance(view, str):
            view = self.get_view(view)
        if not view.is_published:
            raise ViewIsNotPublished(view)
        return SplitgillDatabase(view.published_name, self.client)

    def queue_changes(self, records: Iterable[SourceRecord], store_name: str):
        """
        Update the records in the store with the given name. The views based on the DB
        that is being updated will also be updated.

        :param records: an iterable of records to queue
        :param store_name: the name of the store to update
        """
        store = self.get_store(store_name)
        # find the views based on the store
        views = [view for view in self.views if view.store.name == store.name]

        for batch in partition(records, 1000):
            store.put(batch)
            for view in views:
                view.queue(batch)

    def queue_emu_changes(self) -> Optional[date]:
        """
        Look for new EMu dumps and if any are found beyond the date of the last queued
        EMu import, add the next day's data to the stores and view queues.

        :return the date that was queued or None if no dumps were found
        """
        last_queued = self.emu_status.get()
        dump_sets = find_emu_dumps(self.config.dumps_path, after=last_queued)
        if not dump_sets:
            return None

        next_day_dump_set = dump_sets[0]

        store_names = {store.name for store in self.stores if store.name != "gbif"}
        for dump in next_day_dump_set.dumps:
            # normal tables are immediately processable, but if the dump is from
            # the eaudit table we need to do some additional work because each
            # audit record refers to a potentially different table from which it
            # is deleting a record
            if dump.table != "eaudit":
                self.queue_changes(dump.read(), dump.table)
            else:
                # wrap the dump stream in a filter to only allow through records
                # we want to process
                filtered_dump = filter(
                    partial(is_valid_eaudit_record, tables=store_names),
                    dump.read(),
                )
                # queue the changes to each table's database in turn
                for table, records in groupby(
                    filtered_dump, key=lambda record: record.data["AudTable"]
                ):
                    # convert the raw audit records into delete records as we
                    # queue them
                    self.queue_changes(map(convert_eaudit_to_delete, records), table)

        self.emu_status.update(next_day_dump_set.date)
        return next_day_dump_set.date

    def queue_gbif_changes(self):
        """
        Retrieve the latest GBIF records, check which ones have changed compared to the
        ones stored in the gbif data DB, and then queue them into the GBIF view.
        """
        self.queue_changes(
            get_changed_records(
                self.get_store("gbif"),
                self.config.gbif_username,
                self.config.gbif_password,
            ),
            "gbif",
        )

    def redact_records(
        self, store_name: str, record_ids: List[str], redaction_id: str
    ) -> int:
        """
        Deletes the given record IDs from the named DataDB. This doesn't delete any data
        from the views, MongoDB, or Elasticsearch. This will need to be done manually.

        This operation should only be performed in extreme circumstances where we need
        to remove data that should never have been released and should not be searchable
        at all, even in historic searches. This is because it breaks the core idea of
        the versioned Splitgill system.

        :param store_name: the name of the store to remove the records from
        :param record_ids: a list of str record IDs to redact
        :param redaction_id: an ID for the redaction, so it's traceable
        :return: the number of records deleted
        """
        store = self.get_store(store_name)
        return store.redact(record_ids, redaction_id)

    def release_records(self, up_to: int):
        """
        Releases embargoed records from each store up to the given up_to value. Released
        records queued to the views impacted.

        :param up_to: records with an embargo before this timestamp will be released
        """
        for store in self.stores:
            self.queue_changes(store.release_records(up_to), store.name)

    def add_to_mongo(self, view_name: str, everything: bool = False) -> Optional[int]:
        """
        Add the queued changes in the given view to MongoDB.

        :param view_name: the name of the view
        :param everything: whether to add all records to MongoDB even if they haven't
               changed. Default: False.
        :return: the new version committed, or None if no changes were made
        """
        self.release_records(now())

        view = self.get_view(view_name)
        database = self.get_database(view)

        if everything:
            changed_records = view.iter_all()
        else:
            changed_records = view.iter_changed()
        records = (
            Record(record.id, view.transform(record))
            if record
            else Record.delete(record.id)
            for record in changed_records
        )

        database.ingest(records, commit=False, modified_field="modified")
        # send the options anyway, even if there's no change to them
        database.update_options(DEFAULT_OPTIONS, commit=False)
        committed = database.commit()
        # flush the queue as we've handled everything in it now
        view.flush()
        return committed

    def sync_to_elasticsearch(self, view_name: str, resync: bool = False):
        """
        Synchronise the given Splitgill database with Elasticsearch.

        :param view_name: the name of the view the Splitgill database will use
        :param resync: whether to resync all records to Elasticsearch even if they
               haven't changed
        """
        view = self.get_view(view_name)
        database = self.get_database(view)
        database.sync(resync=resync)

    def force_merge(self, view_name: str) -> dict:
        """
        Performs a force merge on the given view name's Elasticsearch indices. This may
        take a while! This is good for cleaning up deleted documents.

        :param view_name:
        :return:
        """
        view = self.get_view(view_name)
        database = self.get_database(view)
        client = self.client.elasticsearch
        return client.options(request_timeout=None).indices.forcemerge(
            index=database.indices.wildcard,
            allow_no_indices=True,
            wait_for_completion=True,
            max_num_segments=1,
        )

    def __enter__(self) -> "DataImporter":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """
        Close the views and data DBs down.
        """
        for view in self.views:
            view.close()
        for store in self.stores:
            store.close()


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
