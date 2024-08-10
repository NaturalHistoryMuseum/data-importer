from datetime import datetime

from freezegun import freeze_time
from itertools import chain
from pathlib import Path

import msgpack
import pytest
from splitgill.utils import partition, parse_to_timestamp, to_timestamp, now

from dataimporter.lib.dbs import (
    DB,
    Index,
    ChangeQueue,
    Store,
    make_unpacker,
    SPLITTER,
    InvalidRecordID,
)
from dataimporter.lib.model import SourceRecord


@pytest.fixture
def store(tmp_path: Path) -> Store:
    st = Store(tmp_path / "database")
    yield st
    st.close()


class TestDB:
    def test_name(self, tmp_path: Path):
        db = DB(tmp_path / "database")
        assert db.name == "database"

    def test_empty(self, tmp_path: Path):
        db = DB(tmp_path / "database")
        assert db.is_empty()
        db.put([(b"key", b"value")])
        assert not db.is_empty()

    def test_keys(self, tmp_path: Path):
        db = DB(tmp_path / "database")

        for i in range(10):
            db.db.put(str(i).encode("utf-8"), f"data {i}!".encode("utf-8"))

        assert list(db.keys()) == [str(i).encode("utf-8") for i in range(10)]

    def test_values(self, tmp_path: Path):
        db = DB(tmp_path / "database")

        for i in range(10):
            db.db.put(str(i).encode("utf-8"), f"data {i}!".encode("utf-8"))

        assert list(db.values()) == [f"data {i}!".encode("utf-8") for i in range(10)]

    def test_items(self, tmp_path: Path):
        db = DB(tmp_path / "database")

        for i in range(10):
            db.db.put(str(i).encode("utf-8"), f"data {i}!".encode("utf-8"))

        assert list(db.items()) == [
            (str(i).encode("utf-8"), f"data {i}!".encode("utf-8")) for i in range(10)
        ]

    def test_size(self, tmp_path: Path):
        db = DB(tmp_path / "database")

        for i in range(10):
            db.db.put(str(i).encode("utf-8"), f"data {i}!".encode("utf-8"))

        assert db.size() == 10

    def test_clear(self, tmp_path: Path):
        db = DB(tmp_path / "database")

        for i in range(10):
            db.db.put(str(i).encode("utf-8"), f"data {i}!".encode("utf-8"))

        assert db.size() == 10
        db.clear()
        assert db.size() == 0

    def test_close(self, tmp_path: Path):
        db = DB(tmp_path / "database")
        assert not db.db.closed
        db.close()
        assert db.db.closed


class TestStore:
    def test_invalid_id(self, store: Store):
        with pytest.raises(InvalidRecordID):
            store.put([SourceRecord(f"record-{SPLITTER}-1", {"a": "b"}, "test")])

    def test_iter_and_put_many(self, store: Store):
        assert not list(store.iter())

        records = [
            SourceRecord(i, {"a": "banana"}, "test")
            for i in sorted(map(str, range(5000)))
        ]

        total_inserted = 0

        for chunk in partition(records, 541):
            result = store.put(chunk)
            assert result.deleted == 0
            assert result.redacted == 0
            assert result.embargoed == 0
            total_inserted += result.upserted

        assert total_inserted == len(records)
        assert list(store.iter()) == records

    def test_embargoes(self, store: Store):
        with freeze_time("2020-01-01"):
            # check is_embargoed works when no data exists
            assert not store.is_embargoed("e-1", now())

            first_type = SourceRecord(
                "e-1", {"a": "banana", "NhmSecEmbargoDate": "2020-05-01"}, "test"
            )
            assert store.put([first_type]).embargoed == 1
            assert store.get_record(first_type.id) is None
            assert store.is_embargoed(first_type.id, now())

            second_type = SourceRecord(
                "e-2",
                {"a": "banana", "NhmSecEmbargoExtensionDate": "2020-05-01"},
                "test",
            )
            assert store.put([second_type]).embargoed == 1
            assert store.get_record(second_type.id) is None
            assert store.is_embargoed(second_type.id, now())

            both_type = SourceRecord(
                "e-3",
                {
                    "a": "banana",
                    "NhmSecEmbargoDate": "2019-12-01",
                    "NhmSecEmbargoExtensionDate": "2020-05-01",
                },
                "test",
            )
            assert store.put([both_type]).embargoed == 1
            assert store.get_record(both_type.id) is None
            assert store.is_embargoed(both_type.id, now())

            # I assume this shouldn't happen, but anything's possible
            both_type_reversed = SourceRecord(
                "e-4",
                {
                    "a": "banana",
                    "NhmSecEmbargoDate": "2020-05-01",
                    "NhmSecEmbargoExtensionDate": "2019-12-01",
                },
                "test",
            )
            assert store.put([both_type_reversed]).embargoed == 1
            assert store.get_record(both_type_reversed.id) is None
            assert store.is_embargoed(both_type_reversed.id, now())

            expired_embargo = SourceRecord(
                "e-5",
                {
                    "a": "banana",
                    "NhmSecEmbargoDate": "2019-11-01",
                    "NhmSecEmbargoExtensionDate": "2019-12-01",
                },
                "test",
            )
            assert store.put([expired_embargo]).embargoed == 0
            assert store.get_record(expired_embargo.id) == expired_embargo
            assert not store.is_embargoed(expired_embargo.id, now())

            assert list(store.iter_embargoed_ids()) == ["e-1", "e-2", "e-3", "e-4"]
            assert list(store.iter_embargoed_ids(timestamp=now())) == [
                "e-1",
                "e-2",
                "e-3",
                "e-4",
            ]
            assert not list(
                store.iter_embargoed_ids(timestamp=to_timestamp(datetime(2020, 6, 1)))
            )

        released = list(store.release_records(to_timestamp(datetime(2021, 1, 1))))
        assert len(released) == 4
        assert first_type in released
        assert second_type in released
        assert both_type in released
        assert both_type_reversed in released
        assert list(store.get_records(["e-1", "e-2", "e-3", "e-4"])) == [
            first_type,
            second_type,
            both_type,
            both_type_reversed,
        ]

    def test_redact(self, store: Store):
        record = SourceRecord("r-1", {"a": "Paru smells! :O"}, "test")
        assert store.put([record]).upserted == 1
        assert store.get_record(record.id) == record
        assert store.get_redaction_id(record.id) is None

        redaction_id = "truly-horrible-content"
        assert store.redact([record.id], redaction_id) == 1
        assert store.is_redacted(record.id)
        assert store.get_record(record.id) is None
        assert store.get_record(record.id, return_deleted=True) is None
        assert store.get_redaction_id(record.id) == redaction_id
        assert store.get_all_redacted_ids() == {record.id: redaction_id}
        assert not store.has(record.id)
        assert not list(store.get_records([record.id]))
        assert not list(store.iter())

        # try and add the record again
        assert store.put([record]).redacted == 1
        assert store.is_redacted(record.id)
        assert store.get_record(record.id) is None

    def test_get_record(self, store: Store):
        records = [SourceRecord(str(i), {"a": "banana"}, "test") for i in range(40)]

        assert store.put(records).upserted == 40

        assert store.get_record("41") is None
        assert store.get_record("23") == records[23]

        # delete a record
        to_delete = SourceRecord("4", {}, "test-delete")
        assert store.put([to_delete]).deleted == 1
        assert store.get_record("4") is None
        assert store.get_record("4", return_deleted=True) == to_delete

    def test_get_records(self, store: Store):
        records = [SourceRecord(str(i), {"a": "banana"}, "test") for i in range(40)]

        store.put(records)

        ids = [6, 10, 3, 50, 8, 9, 9, 35]
        assert list(store.get_records(map(str, ids))) == [
            records[6],
            records[10],
            records[3],
            # 50 is skipped as it shouldn't exist
            records[8],
            records[9],
            records[9],
            records[35],
        ]

        # let's delete a record
        to_delete = SourceRecord("3", {}, "test_delete")
        store.put([to_delete])
        assert list(store.get_records(["3", "5"], yield_deleted=False)) == [
            records[5],
        ]
        assert list(store.get_records(["3", "5"], yield_deleted=True)) == [
            to_delete,
            records[5],
        ]

    def test_get_records_with_interrupt(self, store: Store):
        # this test covers off an obscure bug which has been fixed so shouldn't be an
        # issue any more but there's no harm in having it here just in case! The issue
        # arose because the unpacker object in the DataDB was being reused by multiple
        # functions which were generators. The way the msgpack Unpacker object works is
        # that you feed in a bunch of bytes data and then iterate over it to read out
        # the Python objects represented. If the generator feeds 1000 objects worth of
        # raw data into the shared unpacker object and then only reads the first 5
        # objects out then the unpacker still has 995 objects ready to be read. So when
        # another function comes along and feeds more data and starts reading objects
        # out of the shared unpacker it gets the remaining 995 objects the previous
        # function added to the unpacker. This has been fixed by not using a shared
        # unpacker (we used a shared one for performance but really it was premature
        # optimisation and created a hard to find bug which bamboozled me for the best
        # part of a day!).

        # create a bunch of records and add them to the database
        records = [SourceRecord(str(i), {"a": "banana"}, "test") for i in range(40)]
        store.put(records)

        # now request the first 10 ids but only read out the first 5 records
        ids = list(map(str, range(10)))
        count = 0
        for expected_id, record in zip(ids, store.get_records(ids)):
            count += 1
            assert record.id == expected_id
            if count == 5:
                break

        # now get record 35 and check the id is correct
        just_35 = store.get_record("35")
        assert just_35.id == "35"
        # now read record 38 using get_records and check the id is correct
        just_38 = list(store.get_records(["38"]))
        assert len(just_38) == 1
        assert just_38[0].id == "38"

    def test_get_unpacker_unpacks_list_like_objects_as_tuples(self, store: Store):
        # goes in a list
        packed_list_raw = msgpack.packb([1, 2, 3, 4])
        unpacker = make_unpacker()
        unpacker.feed(packed_list_raw)
        # comes out a tuple
        assert unpacker.unpack() == (1, 2, 3, 4)

    def test_has(self, store: Store):
        store.put(
            [
                SourceRecord("1", {"a": "4"}, "test"),
                SourceRecord("43", {"a": "f"}, "test"),
            ]
        )
        assert store.has("1")
        assert store.has("43")
        assert not store.has("4")

    def test_delete(self, store: Store):
        store.put(
            [
                SourceRecord("1", {"a": "4"}, "test"),
                SourceRecord("43", {"a": "f"}, "test"),
                SourceRecord("600", {"a": "v"}, "test"),
                SourceRecord("34", {"a": "v"}, "test"),
            ]
        )

        deleted = store.delete(["1", "4"], source="testdelete")
        assert deleted == 1
        assert not store.has("1")
        assert store.has("43")

        deleted_second_time = store.delete(["1", "43", "600"], source="testdelete2")
        assert deleted_second_time == 2
        assert not store.has("1")
        assert not store.has("43")
        assert not store.has("600")
        assert store.has("34")

    def test_purge(self, store: Store):
        store.put(
            [
                SourceRecord("1", {"a": "4"}, "test"),
                SourceRecord("43", {"a": "f"}, "test"),
                SourceRecord("600", {"a": "v"}, "test"),
                SourceRecord("34", {"a": "v"}, "test"),
            ]
        )

        purged = store.purge(["1", "4"])
        assert purged == 1
        assert not store.has("1")
        assert not store.has("1", allow_deleted=True)
        assert store.has("43")

    def test_create_index(self, store: Store):
        records = [
            SourceRecord("record_01", {"theField": "banana"}, "test"),
            SourceRecord("record_02", {"theField": "banana"}, "test"),
            SourceRecord("record_03", {"theField": "orange"}, "test"),
            SourceRecord("record_04", {"theField": "lemon"}, "test"),
        ]
        store.put(records)
        index = store.create_index("theField")
        assert list(index.lookup(["banana"])) == ["record_01", "record_02"]

        store.populate_index("theField", clear_first=True)
        assert list(index.lookup(["banana"])) == ["record_01", "record_02"]

    def test_size(self, store: Store):
        assert store.size() == 0
        store.put([SourceRecord("1", {"a": "4"}, "test")])
        assert store.size() == 1
        assert store.size(include_deletes=True) == 1

        store.put([SourceRecord("2", {"a": "7"}, "test")])
        assert store.size() == 2
        assert store.size(include_deletes=True) == 2

        store.put([SourceRecord("1", {}, "test")])
        assert store.size() == 1
        assert store.size(include_deletes=True) == 2


class TestIndex:
    def test_update_and_lookup(self, tmp_path: Path):
        index = Index(tmp_path / "index", "theField")
        updates = [
            SourceRecord("record_01", {"theField": "banana"}, "test"),
            SourceRecord("record_02", {"theField": "banana"}, "test"),
            SourceRecord("record_03", {"theField": "orange"}, "test"),
            SourceRecord("record_04", {"theField": "lemon"}, "test"),
        ]

        index.update(updates, [])

        assert list(index.lookup(["banana"])) == ["record_01", "record_02"]
        assert list(index.lookup(["orange"])) == ["record_03"]
        assert list(index.lookup(["lemon"])) == ["record_04"]
        assert list(index.lookup(["kiwi"])) == []

    def test_deletes(self, tmp_path: Path):
        index = Index(tmp_path / "index", "theField")

        records = [
            SourceRecord("record_01", {"theField": "banana"}, "test"),
            SourceRecord("record_02", {"theField": "banana"}, "test"),
            SourceRecord("record_03", {"theField": "kiwi"}, "test"),
            SourceRecord("record_04", {"theField": "banana"}, "test"),
        ]

        index.update(records, [])
        assert list(index.lookup(["banana"])) == ["record_01", "record_02", "record_04"]
        assert list(index.lookup(["kiwi"])) == ["record_03"]

        # delete records 1 and 4
        deletes = [SourceRecord(f"record_0{i}", {}, "test") for i in [1, 4]]
        index.update([], deletes)
        assert list(index.lookup(["banana"])) == ["record_02"]
        assert list(index.lookup(["kiwi"])) == ["record_03"]


@pytest.fixture
def queue(tmp_path: Path) -> ChangeQueue:
    cq = ChangeQueue(tmp_path / "cq")
    yield cq
    cq.close()


class TestChangeQueue:
    def test_put_many_ids_and_iter(self, queue: ChangeQueue):
        ids = list(map(str, range(5)))
        queue.put_many_ids(ids)
        assert list(queue.iter()) == ids

    def test_put_many(self, queue: ChangeQueue):
        records = [
            SourceRecord(i, {"a": "x"}, "test") for i in sorted(map(str, range(50)))
        ]
        queue.put_many(records)
        assert list(queue.iter()) == [record.id for record in records]

    def test_put_duplicate(self, queue: ChangeQueue):
        ids1 = list(map(str, range(5)))
        ids2 = list(map(str, range(3, 9)))

        queue.put_many_ids(ids1)
        assert list(queue.iter()) == ids1
        assert queue.size() == len(ids1)

        queue.put_many_ids(ids2)
        crossover = set(chain(ids1, ids2))
        assert list(queue.iter()) == sorted(crossover)
        assert queue.size() == len(crossover)

    def test_is_queued(self, queue: ChangeQueue):
        queue.put_many_ids(["record_01", "record_02"])
        assert queue.is_queued("record_01")
        assert queue.is_queued("record_02")
        assert not queue.is_queued("record_05")
