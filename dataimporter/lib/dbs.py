import shutil
from contextlib import contextmanager, ExitStack
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Tuple, List, Dict

import msgpack
import plyvel
from splitgill.utils import now, partition

from dataimporter.lib.model import SourceRecord


class DB:
    """
    A class wrapping the plyvel database object with some useful functionality.
    """

    def __init__(self, path: Path):
        """
        :param path: the path to the database
        """
        self.path = path
        self.path.mkdir(parents=True, exist_ok=True)
        self.db = plyvel.DB(str(path), create_if_missing=True)

    def get(self, key: bytes) -> Optional[bytes]:
        """
        Returns the value associated with this key.

        :param key: the key
        :return: the value, or None if the key isn't present in this DB
        """
        return self.db.get(key)

    def close(self):
        self.db.close()

    @property
    def name(self) -> str:
        return self.path.name

    def put(self, keys_and_values: Iterable[Tuple[bytes, bytes]]):
        """
        Writes the given keys and values to the database. Both keys and values must be
        bytes. This method intentionally takes an iterable so be careful with length!

        :param keys_and_values: an iterable of 2-tuples representing the keys and values
        """
        with self.get_writer() as wb:
            for key, value in keys_and_values:
                wb.put(key, value)

    def purge(self, keys: Iterable[bytes]):
        """
        Deletes the given keys from the database. Note this is a hard delete and the
        keys are completed removed from the database. The keys must be bytes. This
        method intentionally takes an iterable so be careful with length!

        :param keys: an iterable of bytes
        """
        with self.get_writer() as wb:
            for key in keys:
                wb.delete(key)

    @contextmanager
    def get_writer(self):
        """
        Creates a write batch on the underlying plyvel database and yields it.

        The write batch is always opened with transaction=True.
        """
        with self.db.write_batch(transaction=True) as wb:
            yield wb

    def __contains__(self, key: bytes) -> bool:
        """
        Checks if the given bytes key is in this plyvel database. If it is returns True,
        if not returns False.

        :param key: they bytes key to lookup
        :return: True if the database contains the key, False if not
        """
        return self.db.get(key) is not None

    def keys(self, **iterator_kwargs) -> Iterable[bytes]:
        """
        Yields the keys from the database. By default, this is in lowest to highest
        order.

        :param iterator_kwargs: any additional plyvel iterator kwargs
        :return: yields bytes keys
        """
        yield from self.db.iterator(
            include_key=True, include_value=False, **iterator_kwargs
        )

    def values(self, **iterator_kwargs) -> Iterable[bytes]:
        """
        Yields the values from the database. By default, this is in lowest to highest
        order by key.

        :param iterator_kwargs: any additional plyvel iterator kwargs
        :return: yields bytes values
        """
        yield from self.db.iterator(
            include_key=False, include_value=True, **iterator_kwargs
        )

    def items(self, **iterator_kwargs) -> Iterable[Tuple[bytes, bytes]]:
        """
        Yields the key, value pairs from the database. By default, this is in lowest to
        highest order by key.

        :param iterator_kwargs: any additional plyvel iterator kwargs
        :return: yields 2-tuples of bytes key and bytes value
        """
        yield from self.db.iterator(
            include_key=True, include_value=True, **iterator_kwargs
        )

    def size(self) -> int:
        """
        Returns the count of the number of keys in the database. This is achieved by
        iterating over the keys and counting them, one by one.

        :return: the number of keys in the database
        """
        return sum(1 for _ in self.keys())

    def clear(self):
        """
        Clear the database of all data.

        For speed, this is achieved by deleting the root database directory and then
        recreating it.
        """
        self.close()
        shutil.rmtree(self.path)
        self.path.mkdir(parents=True, exist_ok=True)
        self.db = plyvel.DB(str(self.path), create_if_missing=True)

    def is_empty(self) -> bool:
        """
        Returns whether the database is empty.

        :return: True if the database has no data in it, False if not.
        """
        a_key = next(iter(self.keys()), None)
        return True if a_key is None else False


@dataclass
class PutResult:
    """
    A class to hold the result of a put call on a Store.
    """

    # the number of records that were inserted or updated
    upserted: int
    # the number of records that were deletes (i.e. had no data)
    deleted: int
    # the number of records that were embargoed and therefore added to the embargo DB
    embargoed: int
    # the number of records that were ignored because they were previously redacted
    redacted: int


def make_unpacker() -> msgpack.Unpacker:
    """
    Creates a new msgpack Unpacker object and returns it. This unpacker uses
    use_list=False which is faster and more importantly, required, as we assume list-
    like data structures are stored as tuples in this lib.

    :return: a new Unpacker object
    """
    return msgpack.Unpacker(use_list=False)


def iter_source_records(
    raw_data_stream: Iterable[bytes], yield_deleted: bool
) -> Iterable[SourceRecord]:
    """
    Iterate over a bytes stream, converting the bytes to SourceRecords and yielding
    them.

    :param raw_data_stream: the raw bytes stream
    :param yield_deleted: whether to yield deleted records
    :return: yield SourceRecord objects
    """
    unpacker = make_unpacker()
    for batch in partition(raw_data_stream, 1000):
        unpacker.feed(b"".join(batch))
        records = (SourceRecord(*params) for params in unpacker)
        if yield_deleted:
            yield from records
        else:
            # filter out the deleted records
            yield from filter(None, records)


# this character is in DB keys in a couple of places and cannot be used in record IDs
SPLITTER = "^"
SPLITTER_BYTES = SPLITTER.encode("utf-8")


class InvalidRecordID(Exception):
    def __init__(self, record: SourceRecord):
        super().__init__(
            f"Invalid record ID ({record.id}), cannot include '{SPLITTER}'"
        )
        self.record = record


class Store:
    """
    Class representing a store of data.

    This includes record data, embargo data, and indexes.
    """

    def __init__(self, path: Path):
        self.root = path
        self.data = DB(self.root / "data")
        self.embargoes = DB(self.root / "embargoes")
        self.redactions = DB(self.root / "redactions")
        self.index_root = self.root / "indexes"
        self.indexes: Dict[str, Index] = {}
        self._redaction_cache = self.get_all_redacted_ids()

    @property
    def name(self) -> str:
        return self.root.name

    def get_record(
        self, record_id: str, return_deleted: bool = False
    ) -> Optional[SourceRecord]:
        """
        Retrieves the record from the store with the given ID.

        If the record is not found, None is returned. If the record has been deleted it
        is not returned and None is returned instead, unless return_deleted is True.

        :param record_id: the record's ID
        :param return_deleted: whether to return deleted records or None instead
        :return: a SourceRecord object with the given ID or None
        """
        packed_record_data = self.data.get(record_id.encode("utf-8"))
        if packed_record_data is None:
            return None
        unpacker = make_unpacker()
        unpacker.feed(packed_record_data)
        record = SourceRecord(*next(unpacker))
        if not return_deleted and record.is_deleted:
            return None
        return record

    def get_records(
        self, record_ids: Iterable[str], yield_deleted: bool = False
    ) -> Iterable[SourceRecord]:
        """
        Given an iterable of record IDs, yield the records from the database with those
        IDs in the order they are requested.

        If a record isn't found, then it is skipped, and we move on to the next ID
        without yielding anything. If a record has been deleted it is not yielded unless
        yield_deleted is set to True.

        :param record_ids: the record IDs
        :param yield_deleted: whether to yield deleted records or not
        :return: yields SourceRecord objects
        """
        data = (self.data.get(record_id.encode("utf-8")) for record_id in record_ids)
        yield from iter_source_records(filter(None, data), yield_deleted)

    def iter(self, yield_deleted: bool = False) -> Iterable[SourceRecord]:
        """
        Yields all records in this database, in ascending ID order.

        If a record has been deleted it is not yielded unless yield_deleted is set to
        True.

        :param yield_deleted: whether to yield deleted records or not
        :return: yields SourceRecord objects
        """
        yield from iter_source_records(self.data.values(), yield_deleted)

    def has(self, record_id: str, allow_deleted: bool = False) -> bool:
        """
        Checks whether the given record ID is in the store. If the record exists but has
        been deleted, False is returned, unless allow_deleted is set to True.

        :param record_id: the record ID
        :param allow_deleted: whether to treat deleted records as extant or not
        :return: whether record is in the store or not
        """
        return self.get_record(record_id, return_deleted=allow_deleted) is not None

    def put(self, records: List[SourceRecord]) -> PutResult:
        """
        Given a list of records, add them to the database. This is essentially an
        upsert, new records are inserted, existing records are just overridden, deleted
        records are deleted.

        The records are added in one transactional batch.

        :param records: a list of records
        :return: a PutResult object
        """
        deleted = 0
        upserted = 0
        embargoed = 0
        redacted = 0
        embargo_threshold = now()
        # cache the pack method as we're going to be using it a lot
        pack = msgpack.Packer().pack

        # keep track of deleted records and which indexes have updates
        index_deletes = []
        field_updates = {field: [] for field in self.indexes.keys()}

        with ExitStack() as stack:
            dwb = stack.enter_context(self.data.get_writer())
            ewb = stack.enter_context(self.embargoes.get_writer())

            for record in records:
                # ensure the record ID is legal
                if SPLITTER in record.id:
                    raise InvalidRecordID(record)

                # ignore redacted records
                if record.id in self._redaction_cache:
                    redacted += 1
                    continue

                record_id = record.id.encode("utf-8")
                data = pack((record.id, record.data, record.source))

                if record.is_deleted:
                    if self.has(record.id):
                        deleted += 1
                        dwb.put(record_id, data)
                        ewb.delete(record_id)
                        index_deletes.append(record)
                    continue

                embargo = record.get_embargo()
                if embargo is not None and embargo > embargo_threshold:
                    embargoed += 1
                    # add the record to the embargo DB
                    ewb.put(record_id, data)
                    # put a deleted version of the record into data DB
                    dwb.put(
                        record_id,
                        pack((record.id, {}, f"{record.source} [embargoed]")),
                    )
                    # indicate to the indexes that this record has been deleted
                    index_deletes.append(record)
                    continue

                upserted += 1
                dwb.put(record_id, data)
                # keep track of the records that may need their index values updated
                for field, index_updates in field_updates.items():
                    # the index will actually check for non-empty values and only
                    # index those, but this is a quick dirty check just to get the
                    # record queued up for the index to then look at properly later
                    if field in record:
                        index_updates.append(record)

        # update the indexes as needed
        for field, index_updates in field_updates.items():
            if index_updates or index_deletes:
                self.indexes[field].update(index_updates, index_deletes)

        return PutResult(upserted, deleted, embargoed, redacted)

    def delete(self, record_ids: Iterable[str], source: str) -> int:
        """
        Deletes all the record data associated with the given record IDs by setting
        their data to the empty dict. This is a soft delete, for a hard delete, use the
        purge method.

        If a record is embargoed, this will also be deleted.

        :param record_ids: an iterable of record ids to delete
        :param source: source value to store with the deleted records
        :return: the number of IDs that were deleted (i.e. existed and were removed)
        """
        deleted = 0

        for batch in partition(record_ids, 1000):
            result = self.put(
                [SourceRecord(record_id, {}, source) for record_id in batch]
            )
            deleted += result.deleted

        return deleted

    def purge(self, record_ids: Iterable[str]) -> int:
        """
        Purges all the record data associated with the given record IDs. This is a hard
        delete, for a soft delete, use the delete method.

        Extra warning: this is NOT the same as deleting a SourceRecord by setting its
        data property to an empty dict, this is a hard delete where the data is removed.
        It should only be used for redactions in extreme cases.

        :param record_ids: an iterable of record ids to delete
        :return: the number of IDs that were deleted (i.e. existed and were removed)
        """
        purged = 0

        # create an iterable which converts each record ID into bytes
        raw_ids = (record_id.encode("utf-8") for record_id in record_ids)
        for batch in partition(raw_ids, 1000):
            # purge from the data DB and the embargo DB
            for db in (self.data, self.embargoes):
                to_delete = [raw_id for raw_id in batch if raw_id in db]
                purged += len(to_delete)
                db.purge(to_delete)

        return purged

    def redact(self, record_ids: Iterable[str], redaction_id: str) -> int:
        """
        Add all the given record IDs to the redaction list for this store. The redaction
        ID is stored for later retrieval if needed. This isn't expected to be an ID that
        is represented in this system but perhaps can be looked up in a different system
        to understand why the record has been redacted.

        :param record_ids: an iterable of record IDs to redact
        :param redaction_id: an identifier for why the records have been redacted
        :return: the number of IDs that were deleted
        """
        purged = self.purge(record_ids)
        with self.redactions.get_writer() as wb:
            for record_id in record_ids:
                wb.put(record_id.encode("utf-8"), redaction_id.encode("utf-8"))
        # update the redaction cache
        self._redaction_cache = self.get_all_redacted_ids()
        return purged

    def is_redacted(self, record_id: str) -> bool:
        """
        Checks if the given record is redacted.

        :param record_id: the record ID
        :return: True if the record is redacted, False if not
        """
        return self.get_redaction_id(record_id) is not None

    def get_redaction_id(self, record_id: str) -> Optional[str]:
        """
        Retrieves the redaction ID associated with the given record ID. If the record is
        not redacted, returns None.

        :param record_id: the record ID
        :return: None if the record is not redacted, otherwise, returns the redaction ID
        """
        redaction_id = self.redactions.get(record_id.encode("utf-8"))
        if redaction_id is None:
            return None
        return redaction_id.decode("utf-8")

    def get_all_redacted_ids(self) -> Dict[str, str]:
        """
        Return a dict of redacted record IDs along with the redaction ID for each.

        :return: a dict of record IDs -> retraction IDs
        """
        return {
            key.decode("utf-8"): value.decode("utf-8")
            for key, value in self.redactions.items()
        }

    def create_index(self, field: str) -> "Index":
        """
        Creates a new index on the given field. If no existing data is found and there
        is data in this database, the index will also be populated and therefore this
        may take a bit of time.

        :param field: the field to create the index on
        :return: an Index object representing the index
        """
        if field in self.indexes:
            return self.indexes[field]

        path = self.index_root / field
        # check this before making the index
        already_exists = path.exists()
        index = Index(path, field)

        # if the index didn't exist before we just created it, populate it
        if not already_exists:
            # no need to iterate through deleted records given the index is new
            index.populate(self.iter(yield_deleted=False))

        self.indexes[field] = index
        return index

    def populate_index(self, field: str, clear_first: bool = False):
        """
        Populate the index on the given field. If clear_first is True, empties the index
        first.

        :param field: the indexed field
        :param clear_first: whether to delete the index's data first
        """
        index = self.indexes[field]
        if clear_first:
            index.clear()
        # if the index has been cleared out, no need to iter the deleted records
        index.populate(self.iter(yield_deleted=not clear_first))

    def release_records(self, up_to: int) -> Iterable[SourceRecord]:
        """
        Move records from the embargo DB to the data DB which should be released from
        their embargo based on the up_to parameter. Any record with an embargo date
        before this up_to timestamp will be released.

        :param up_to: the timestamp threshold to release up to
        :return: yields SourceRecord objects for each released record
        """
        released = (
            record
            for record in iter_source_records(self.embargoes.values(), False)
            if record.get_embargo() <= up_to
        )
        # deal with 1000 records at a time
        for batch in partition(released, 1000):
            self.embargoes.purge(record.id.encode("utf-8") for record in batch)
            self.put(batch)
            yield from batch

    def is_embargoed(self, record_id: str, timestamp: int) -> bool:
        """
        Checks whether the given record ID's associated embargo date is greater than the
        given timestamp. If the record doesn't have an associated embargo date or if it
        deleted, or if we have never seen this record before, then False is returned.

        :param record_id: the record ID
        :param timestamp: the timestamp to compare the record's embargo date with
        :return: True if the record is embargoed past the given date, False if not
        """
        if self.has(record_id):
            # this is an available record, can't be embargoed
            return False
        packed_record_data = self.embargoes.get(record_id.encode("utf-8"))
        if packed_record_data is None:
            # the record is also not embargoed, we just don't know anything about it
            return False
        unpacker = make_unpacker()
        unpacker.feed(packed_record_data)
        record = SourceRecord(*next(unpacker))
        return record.get_embargo() > timestamp

    def iter_embargoed_ids(self, timestamp: Optional[int] = None) -> Iterable[str]:
        """
        Yields the IDs in this Store that are embargoed.

        :param timestamp: the threshold to check the embargoes against. If None, all
                          record IDs currently in the embargo database are yielded
        :return: yields record IDs
        """
        if timestamp is None:
            yield from (raw_id.decode("utf-8") for raw_id in self.embargoes.keys())
        else:
            for raw_id in self.embargoes.keys():
                record_id = raw_id.decode("utf-8")
                if self.is_embargoed(record_id, timestamp):
                    yield record_id

    def size(self, include_deletes=False) -> int:
        """
        Returns the number of records present in this store.

        :param include_deletes: whether to count deleted records or not
        :return: the count
        """
        if include_deletes:
            return self.data.size()
        else:
            return sum(
                1 for record in self.iter(yield_deleted=True) if not record.is_deleted
            )

    def close(self):
        self.data.close()
        self.embargoes.close()
        self.redactions.close()
        for index in self.indexes.values():
            index.close()


# todo: implement in memory caching of small (definition tbc) indexes
class Index:
    """
    Class representing an index of field values -> record IDs, allowing quick access to
    the IDs of the records that contain a given field value.
    """

    def __init__(self, path: Path, field: str):
        """
        :param path: the path to store the index at
        :param field: the field to index
        """
        self.field = field
        self.db = DB(path)

    def clear(self):
        self.db.clear()

    def close(self):
        self.db.close()

    def update(self, updates: List[SourceRecord], deletes: List[SourceRecord]):
        """
        Update the index with the given updated and deleted records.

        :param updates:
        :param deletes:
        :return:
        """
        # write any updates
        if updates:
            with self.db.get_writer() as wb:
                for record in updates:
                    for value in record.iter_all_values(self.field, clean=True):
                        wb.put(f"{value}{SPLITTER}{record.id}".encode("utf-8"), b"")

        # todo: find a way to make this faster, maybe in memory cache of the reverse
        #       lookups? Or just keep another DB for the reverse lookup?
        if deletes:
            # because we optimise for lookups with the foreign values, this is a bit
            # slow as we have to do a full scan of the whole keyspace
            keys_to_delete = {record.id.encode("utf-8") for record in deletes}
            self.db.purge(
                key
                for key in self.db.keys()
                if key.rsplit(SPLITTER_BYTES)[1] in keys_to_delete
            )

    def populate(self, records: Iterable[SourceRecord]):
        for batch in partition(records, 1000):
            updates = [record for record in batch if self.field in record]
            deletes = [record for record in batch if record.is_deleted]
            self.update(updates, deletes)

    def lookup(self, values: Iterable[str]) -> Iterable[str]:
        for value in values:
            prefix = f"{value}{SPLITTER}".encode("utf-8")
            prefix_length = len(prefix)
            for raw_key in self.db.keys(prefix=prefix):
                yield raw_key[prefix_length:].decode("utf-8")


class ChangeQueue:
    """
    A database that acts as a queue of IDs that have changed.
    """

    def __init__(self, path: Path):
        self.db = DB(path)

    def size(self) -> int:
        return self.db.size()

    def clear(self):
        self.db.clear()

    def close(self):
        self.db.close()

    def put_many(self, records: List[SourceRecord]):
        """
        Update the queue with the given records. The IDs are added in a transaction.

        :param records: the records that have changed
        """
        self.put_many_ids([record.id for record in records])

    def put_many_ids(self, record_ids: List[str]):
        """
        Update the queue with the given record IDs. The IDs are added in a transaction.

        :param record_ids: the record IDs that have changed
        """
        self.db.put((record_id.encode("utf-8"), b"") for record_id in record_ids)

    def iter(self) -> Iterable[str]:
        """
        Yields the IDs one by one.

        :return: yields the changed int IDs from the queue
        """
        yield from (key.decode("utf-8") for key in self.db.keys())

    def is_queued(self, record_id: str) -> bool:
        """
        Determines whether the record ID provided has already been queued.

        :param record_id: the record ID to check
        :return: True if the record ID is already in the queue, False if not
        """
        return record_id.encode("utf-8") in self.db
