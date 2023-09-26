import shutil
from pathlib import Path
from typing import Iterable, Optional, Tuple, List

import msgpack
import plyvel
from cytoolz.itertoolz import partition_all
from splitgill.utils import parse_to_timestamp, now

from dataimporter.model import SourceRecord


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

    @property
    def name(self) -> str:
        return self.path.name

    def keys(self, **iterator_kwargs) -> Iterable[bytes]:
        """
        Yields the keys from the database. By default, this is in lowest to highest
        order.

        :param iterator_kwargs: any kwargs that you could pass to plyvel's db.iterator
                                method.
        :return: yields bytes keys
        """
        yield from self.db.iterator(
            include_key=True, include_value=False, **iterator_kwargs
        )

    def values(self, **iterator_kwargs) -> Iterable[bytes]:
        """
        Yields the values from the database. By default, this is in lowest to highest
        order by key.

        :param iterator_kwargs: any kwargs that you could pass to plyvel's db.iterator
                                method.
        :return: yields bytes values
        """
        yield from self.db.iterator(
            include_key=False, include_value=True, **iterator_kwargs
        )

    def items(self, **iterator_kwargs) -> Iterable[Tuple[bytes, bytes]]:
        """
        Yields the key, value pairs from the database. By default, this is in lowest to
        highest order by key.

        :param iterator_kwargs: any kwargs that you could pass to plyvel's db.iterator
                                method.
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
        Clear the database of all data. This is achieved by deleting the database and
        then recreating it as it's the fastest way to do it.
        """
        self.close()
        shutil.rmtree(self.path)
        self.path.mkdir(parents=True, exist_ok=True)
        self.db = plyvel.DB(str(self.path), create_if_missing=True)

    def close(self):
        """
        Closes the database.
        """
        self.db.close()


class DataDB(DB):
    """
    Class representing a data database. This is a database where the keys are integer
    IDs and the data is a VersionedRecord. The VersionedRecord will be serialised for
    storage by msgpack.
    """

    def __init__(self, path: Path):
        """
        :param path: the path to the database
        """
        super().__init__(path)
        # record data is stored in msgpack format, these are reusable packer and
        # unpacker objects
        self._packer = msgpack.Packer()
        # use_list=False is important as this ensures all list like data structures are
        # recreated as tuples when the data is deserialised
        self._unpacker = msgpack.Unpacker(use_list=False)

    def __iter__(self) -> Iterable[SourceRecord]:
        """
        Yields the records in this database, in ascending ID order.

        :return: yields VersionedRecord objects
        """
        # cache so that we don't have to look it up on each iteration
        unpacker = self._unpacker
        # read 1000 records worth of raw data at a time
        # TODO: check 1000 - could be larger?
        # TODO: use a bytearray?
        for batch in partition_all(1000, self.values()):
            unpacker.feed(b"".join(batch))
            yield from (SourceRecord(*params) for params in unpacker)

    def put_many(self, records: List[SourceRecord]):
        """
        Given a list of records, add them to the database. This is essentially an
        upsert, new records are inserted, existing records are just overriden.

        The records are added in one transactional batch.

        :param records: a list of records
        """
        # cache the pack method as we're going to be using it a lot
        pack = self._packer.pack

        with self.db.write_batch(transaction=True) as wb:
            for record in records:
                wb.put(
                    record.id.encode("utf-8"),
                    pack((record.id, record.data, record.source)),
                )

    def get_record(self, record_id: str) -> Optional[SourceRecord]:
        """
        Retrieves the record from the store with the given ID, if it is in the store. If
        it's not, returns None.

        :param record_id: the record's ID
        :return: a SourceRecord object with the given ID, None if the record can't be
                 found
        """
        packed_record_data = self.db.get(record_id.encode("utf-8"))
        if packed_record_data is None:
            return None
        self._unpacker.feed(packed_record_data)
        return SourceRecord(*next(self._unpacker))

    def get_records(self, record_ids: Iterable[str]) -> Iterable[SourceRecord]:
        """
        Given an iterable of record IDs, yield the records from the database with those
        IDs in the order they are requested. If a record isn't found, then it is
        skipped, and we move on to the next ID.

        :param record_ids: the record IDs
        :return: yields VersionedRecord objects
        """
        # TODO: it's probably not worth it, but if this becomes a bottleneck it may be
        #       worth investigating implementing a sort merge, similar to how SQL joins
        #       work, if the record_ids parameter is sorted.
        # cache so that we don't have to look these up on each iteration
        unpacker = self._unpacker
        get = self.db.get

        # read 1000 records worth of raw data at a time
        for batch in partition_all(1000, record_ids):
            data = filter(None, (get(record_id.encode("utf-8")) for record_id in batch))
            unpacker.feed(b"".join(data))
            yield from (SourceRecord(*params) for params in unpacker)


class LinkDB(DB):
    """
    Database providing link information between two tables (these tables can be the
    same).

    The data is stored in a way to enable quick lookups from the foreign table back to
    the base table. This is because in the EMu records, we get the link information from
    the base record and the foreign record has no idea what it is linked to. So this
    database provides a quick way of looking up those connections from the foreign table
    side.
    """

    def __init__(self, path: Path, field: str):
        """
        :param path: the database path
        :param field: the field on the base table which contains the foreign IDs
        """
        super().__init__(path)
        self.field = field

    def put_many(self, records: List[SourceRecord]):
        """
        Update the database with the given records. Each record will be iterated over
        and the linked field checked for values. Any values will be extracted and then
        added to the database. The linked ids will be added to the database in a
        transaction.

        :param records: a list of records
        """
        with self.db.write_batch(transaction=True) as wb:
            for record in records:
                for foreign_id in record.iter_all_values(self.field):
                    wb.put(f"{foreign_id}.{record.id}".encode("utf-8"), b"")

    def lookup(self, foreign_ids: Iterable[str]) -> Iterable[str]:
        """
        Lookup the linked base IDs for the given foreign IDs. Yielded one by one.

        TODO: explain why this works like it does, i.e. why we don't need to yield pairs

        :param foreign_ids: an iterable of foreign IDs to lookup
        :return: yields the linked base IDs
        """
        for foreign_id in foreign_ids:
            for key in self.keys(prefix=f"{foreign_id}.".encode("utf-8")):
                yield key.split(b".")[1].decode("utf-8")


class ChangeQueue(DB):
    """
    A database that acts as a queue of IDs that have changed.
    """

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
        with self.db.write_batch(transaction=True) as wb:
            for record_id in record_ids:
                wb.put(record_id.encode("utf-8"), b"")

    def __iter__(self) -> Iterable[str]:
        """
        Yields the IDs one by one.

        :return: yields the changed int IDs from the queue
        """
        yield from (key.decode("utf-8") for key in self.keys())


class EmbargoQueue(DB):
    """
    A database of record embargo statuses.
    """

    def put_many(self, records: List[SourceRecord]) -> List[SourceRecord]:
        """
        Update the database with the given records. The format of the database is keys
        are record IDs, values are the embargo date, stored as a UNIX timestamp.

        The embargo stored is the date furthest in the future found in the
        NhmSecEmbargoDate and NhmSecEmbargoExtensionDate fields. Only embargo dates that
        are beyond the current timestamp are added to the database.

        Updates are written in a transaction.

        :param records: the records to update from
        :return: a list of records that had embargo values
        """
        source_fields = ("NhmSecEmbargoDate", "NhmSecEmbargoExtensionDate")
        embargoed_records = []

        with self.db.write_batch(transaction=True) as wb:
            for record in records:
                embargo = None
                for value in record.iter_all_values(*source_fields):
                    try:
                        date = parse_to_timestamp(value, "%Y-%m-%d")
                        if embargo is None or date > embargo:
                            embargo = date
                    except ValueError:
                        pass
                # TODO: nothing else in the embargodb uses now, should they all use now
                #       or should they all take a parameter to define the threshold?
                if embargo is not None and embargo > now():
                    wb.put(record.id.encode("utf-8"), str(embargo).encode("utf-8"))
                    embargoed_records.append(record)

        return embargoed_records

    def iter_ids(self) -> Iterable[str]:
        """
        Yields all the IDs in the embargo queue.

        :return: yields the str IDs
        """
        yield from (key.decode("utf-8") for key in self.keys())

    def __iter__(self) -> Iterable[Tuple[str, int]]:
        """
        Yields 2-tuples containing a record's ID and the embargo as a timestamp.

        :return: (ID, embargo timestamp)
        """
        for record_id, embargo in self.items():
            yield record_id.decode("utf-8"), int(embargo)

    def __contains__(self, record_id: str) -> bool:
        """
        Check whether the given record ID is in this embargo database.

        Don't use this method to check if a record is embargoed, use is_embargoed
        instead.

        :param record_id: the record's ID
        :return: True if the ID has an embargo timestamp associated, False if not
        """
        return self.lookup_embargo_date(record_id) is not None

    def iter_released(self, up_to: int) -> Iterable[str]:
        """
        Yields the record IDs that have embargo timestamps before the given up_to
        timestamp.

        :param up_to: the exclusive embargo timestamp upper limit
        :return: yields record IDs
        """
        yield from (record_id for record_id, embargo in self if embargo < up_to)

    def flush_released(self, up_to: int) -> int:
        """
        Removes the record IDs that have embargo timestamps before the given up_to
        timestamp from the database. The IDs are removed in a transaction.

        :param up_to: the exclusive embargo timestamp upper limit
        :return: the number of record IDs removed
        """
        count = 0
        with self.db.write_batch(transaction=True) as wb:
            for record_id in self.iter_released(up_to):
                wb.delete(record_id.encode("utf-8"))
                count += 1
        return count

    def lookup_embargo_date(self, record_id: str) -> Optional[int]:
        """
        Given a record ID, return the embargo date from the database associated with it,
        or None if the record ID is not present in the database.

        :param record_id: the record ID
        :return: the embargo timestamp or None
        """
        embargo = self.db.get(record_id.encode("utf-8"))
        return int(embargo.decode("utf-8")) if embargo else None

    def is_embargoed(self, record_id: str, timestamp: int) -> bool:
        """
        Checks whether the given record ID's associated embargo date is greater than the
        given timestamp. If the record doesn't have an associated embargo date then
        False is returned.

        :param record_id: the record ID
        :param timestamp: the exclusive lower limit to compare the embargo date eaginst
        :return: True if the record is embargoed past the given date, False if not
        """
        embargo = self.lookup_embargo_date(record_id)
        if embargo is None:
            return False
        return embargo > timestamp

    def stats(self, up_to: int) -> Tuple[int, int]:
        """
        Counts how many records in the database have an embargo timestamp beyond the
        given up_to timestamp and how many do not. Returns these counts as a 2-tuple
        of the embargoed count and the not embargoed count.

        :param up_to: the exclusive embargo timestamp upper limit
        :return: a 2-tuple of (embargoed count, not embargoed count)
        """
        embargoed = 0
        released = 0
        for _, embargo_date in self:
            if embargo_date < up_to:
                released += 1
            else:
                embargoed += 1
        return embargoed, released
