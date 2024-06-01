from contextlib import closing
from datetime import datetime
from itertools import chain
from pathlib import Path
from typing import Optional

import msgpack
import pytest
from freezegun import freeze_time
from splitgill.utils import partition, now, to_timestamp

from dataimporter.lib.dbs import (
    DB,
    DataDB,
    Index,
    ChangeQueue,
    EmbargoQueue,
    RedactionDB,
)
from dataimporter.lib.model import SourceRecord


class TestDB:
    def test_name(self, tmp_path: Path):
        db = DB(tmp_path / "database")
        assert db.name == "database"

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


class TestDataDB:
    def test_iter_and_put_many(self, tmp_path: Path):
        db = DataDB(tmp_path / "database")
        assert not list(db)

        records = [
            SourceRecord(i, {"a": "banana"}, "test")
            for i in sorted(map(str, range(5000)))
        ]

        for chunk in partition(records, 541):
            db.put_many(chunk)

        assert list(db) == records

    def test_get_record(self, tmp_path: Path):
        db = DataDB(tmp_path / "database")

        records = [SourceRecord(str(i), {"a": "banana"}, "test") for i in range(40)]

        db.put_many(records)

        assert db.get_record("41") is None
        assert db.get_record("23") == records[23]

    def test_get_records(self, tmp_path: Path):
        db = DataDB(tmp_path / "database")

        records = [SourceRecord(str(i), {"a": "banana"}, "test") for i in range(40)]

        db.put_many(records)

        ids = [6, 10, 3, 50, 8, 9, 9, 35]
        assert list(db.get_records(map(str, ids))) == [
            records[6],
            records[10],
            records[3],
            # 50 is skipped as it shouldn't exist
            records[8],
            records[9],
            records[9],
            records[35],
        ]

    def test_get_records_with_interrupt(self, tmp_path: Path):
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
        db = DataDB(tmp_path / "database")
        records = [SourceRecord(str(i), {"a": "banana"}, "test") for i in range(40)]
        db.put_many(records)

        # now request the first 10 ids but only read out the first 5 records
        ids = list(map(str, range(10)))
        count = 0
        for expected_id, record in zip(ids, db.get_records(ids)):
            count += 1
            assert record.id == expected_id
            if count == 5:
                break

        # now get record 35 and check the id is correct
        just_35 = db.get_record("35")
        assert just_35.id == "35"
        # now read record 38 using get_records and check the id is correct
        just_38 = list(db.get_records(["38"]))
        assert len(just_38) == 1
        assert just_38[0].id == "38"

    def test_get_unpacker_unpacks_list_like_objects_as_tuples(self, tmp_path: Path):
        # goes in a list
        packed_list_raw = msgpack.packb([1, 2, 3, 4])
        unpacker = DataDB.get_unpacker()
        unpacker.feed(packed_list_raw)
        # comes out a tuple
        assert unpacker.unpack() == (1, 2, 3, 4)

    def test_contains(self, tmp_path: Path):
        db = DataDB(tmp_path / "database")

        db.put_many(
            [
                SourceRecord("1", {"a": "4"}, "test"),
                SourceRecord("43", {"a": "f"}, "test"),
            ]
        )

        assert "1" in db
        assert "43" in db
        assert "4" not in db

    def test_delete(self, tmp_path: Path):
        db = DataDB(tmp_path / "database")

        db.put_many(
            [
                SourceRecord("1", {"a": "4"}, "test"),
                SourceRecord("43", {"a": "f"}, "test"),
                SourceRecord("600", {"a": "v"}, "test"),
                SourceRecord("34", {"a": "v"}, "test"),
            ]
        )

        deleted = db.delete(["1", "4"])
        assert deleted == 1
        assert "1" not in db
        assert "43" in db

        deleted_second_time = db.delete(["1", "43", "600"])
        assert deleted_second_time == 2
        assert "1" not in db
        assert "43" not in db
        assert "600" not in db
        assert "34" in db


class TestIndex:
    def test_put_many_and_gets(self, tmp_path: Path):
        db = Index(tmp_path / "database")

        db.put_many([("a", "1"), ("b", "2"), ("c", "3"), ("e", "2")])

        assert list(db.get_values("a")) == ["1"]
        assert list(db.get_values("b")) == ["2"]
        assert list(db.get_values("c")) == ["3"]
        assert list(db.get_values("d")) == []
        assert list(db.get_values("e")) == ["2"]

        assert db.get_value("a") == "1"
        assert db.get_value("b") == "2"
        assert db.get_value("c") == "3"
        assert db.get_value("d") is None
        assert db.get_value("e") == "2"

        assert list(db.get_keys("1")) == ["a"]
        assert list(db.get_keys("2")) == ["b", "e"]
        assert list(db.get_keys("3")) == ["c"]
        assert list(db.get_keys("4")) == []

        assert db.get_key("1") == "a"
        assert db.get_key("2") == "b"
        assert db.get_key("3") == "c"
        assert db.get_key("4") is None

    def test_put_multiple_values_gets(self, tmp_path: Path):
        db = Index(tmp_path / "database")

        db.put_multiple_values(
            [("a", ("1", "2", "3")), ("b", ("4", "2")), ("c", ("3",))]
        )

        assert list(db.get_values("a")) == ["1", "2", "3"]
        assert list(db.get_values("b")) == ["2", "4"]
        assert list(db.get_values("c")) == ["3"]
        assert list(db.get_values("d")) == []

        assert db.get_value("a") == "1"
        assert db.get_value("b") == "2"
        assert db.get_value("c") == "3"
        assert db.get_value("d") is None

        assert list(db.get_keys("1")) == ["a"]
        assert list(db.get_keys("2")) == ["a", "b"]
        assert list(db.get_keys("3")) == ["a", "c"]
        assert list(db.get_keys("4")) == ["b"]
        assert list(db.get_keys("5")) == []

        assert db.get_key("1") == "a"
        assert db.get_key("2") == "a"
        assert db.get_key("3") == "a"
        assert db.get_key("4") == "b"
        assert db.get_key("5") is None

    def test_put_many_empty_iterables(self, tmp_path: Path):
        db = Index(tmp_path / "database")

        db.put_multiple_values(
            [("a", tuple()), ("b", ("4", "2")), ("c", iter(tuple()))]
        )

        assert list(db.get_values("a")) == []
        assert list(db.get_values("b")) == ["2", "4"]
        assert list(db.get_values("c")) == []


class TestChangeQueue:
    def test_put_many_ids_and_iter(self, tmp_path: Path):
        db = ChangeQueue(tmp_path / "database")

        ids = list(map(str, range(5)))

        db.put_many_ids(ids)

        assert list(db) == ids

    def test_put_many(self, tmp_path: Path):
        db = ChangeQueue(tmp_path / "database")

        records = [
            SourceRecord(i, {"a": "x"}, "test") for i in sorted(map(str, range(50)))
        ]

        db.put_many(records)

        assert list(db) == [record.id for record in records]

    def test_put_duplicate(self, tmp_path: Path):
        db = ChangeQueue(tmp_path / "database")
        ids1 = list(map(str, range(5)))
        ids2 = list(map(str, range(3, 9)))

        db.put_many_ids(ids1)
        assert list(db) == ids1
        assert db.size() == len(ids1)

        db.put_many_ids(ids2)
        crossover = set(chain(ids1, ids2))
        assert list(db) == sorted(crossover)
        assert db.size() == len(crossover)


class TestEmbargoQueue:
    put_many_scenarios = [
        # no embargo
        (SourceRecord("1", {"arms": "4"}, "test"), None),
        # a delete
        (SourceRecord("1", {}, "test"), None),
        # an embargo using NhmSecEmbargoDate
        (SourceRecord("1", {"NhmSecEmbargoDate": "2021-01-06"}, "test"), 1609891200000),
        # an embargo using NhmSecEmbargoExtensionDate
        (
            SourceRecord("1", {"NhmSecEmbargoExtensionDate": "2021-01-06"}, "test"),
            1609891200000,
        ),
        # an embargo in both NhmSecEmbargoDate and NhmSecEmbargoExtensionDate (same)
        (
            SourceRecord(
                "1",
                {
                    "NhmSecEmbargoDate": "2021-01-06",
                    "NhmSecEmbargoExtensionDate": "2021-01-06",
                },
                "test",
            ),
            1609891200000,
        ),
        # an embargo in both NhmSecEmbargoDate and NhmSecEmbargoExtensionDate (<)
        (
            SourceRecord(
                "1",
                {
                    "NhmSecEmbargoDate": "2021-01-04",
                    "NhmSecEmbargoExtensionDate": "2021-01-06",
                },
                "test",
            ),
            1609891200000,
        ),
        # an embargo in both NhmSecEmbargoDate and NhmSecEmbargoExtensionDate (>)
        (
            SourceRecord(
                "1",
                {
                    "NhmSecEmbargoDate": "2021-01-06",
                    "NhmSecEmbargoExtensionDate": "2021-01-04",
                },
                "test",
            ),
            1609891200000,
        ),
        # an embargo that is old in NhmSecEmbargoDate but new in
        # NhmSecEmbargoExtensionDate
        (
            SourceRecord(
                "1",
                {
                    "NhmSecEmbargoDate": "2020-05-06",
                    "NhmSecEmbargoExtensionDate": "2020-05-16",
                },
                "test",
            ),
            1589587200000,
        ),
        # incorrectly formatted embargo dates
        (
            SourceRecord(
                "1",
                {
                    # missing day
                    "NhmSecEmbargoDate": "2021-01",
                    # bad month
                    "NhmSecEmbargoExtensionDate": "2021-15-04",
                },
                "test",
            ),
            None,
        ),
    ]

    @freeze_time(datetime(2020, 5, 10))
    @pytest.mark.parametrize(("record", "embargo_timestamp"), put_many_scenarios)
    def test_scenarios(
        self, tmp_path: Path, record: SourceRecord, embargo_timestamp: Optional[int]
    ):
        """
        This test tests put_many as well as iter_ids, __iter__, __contains__,
        lookup_embargo_date, and is_embargoed.
        """
        db = EmbargoQueue(tmp_path / "embargoes")

        embargoed = db.put_many([record])

        if embargo_timestamp is None:
            assert not embargoed
            assert list(db.iter_ids()) == []
            assert list(db.__iter__()) == []
            assert record.id not in db
            assert db.lookup_embargo_date(record.id) is None
            assert not db.is_embargoed(record.id, now())
        else:
            assert embargoed == [record]
            assert list(db.iter_ids()) == [record.id]
            assert list(db.__iter__()) == [(record.id, embargo_timestamp)]
            assert record.id in db
            assert db.lookup_embargo_date(record.id) == embargo_timestamp
            assert db.is_embargoed(record.id, now())

    def test_iter_released(self, tmp_path: Path):
        db = EmbargoQueue(tmp_path / "embargoes")
        records = [
            SourceRecord(str(i), {"NhmSecEmbargoDate": f"2020-10-{i:2}"}, "test")
            for i in range(5, 15)
        ]
        # freeze time to the start of the month so they all get entered into the db
        with freeze_time(datetime(2020, 10, 1)):
            db.put_many(records)

        up_to = to_timestamp(datetime(2020, 10, 10))
        released = list(db.iter_released(up_to))

        # ids 5, 6, 7, 8, and 9 should be released
        assert released == list(map(str, range(5, 10)))

    def test_flush_released(self, tmp_path: Path):
        db = EmbargoQueue(tmp_path / "embargoes")
        records = [
            SourceRecord(str(i), {"NhmSecEmbargoDate": f"2020-10-{i:2}"}, "test")
            for i in range(5, 15)
        ]
        # freeze time to the start of the month so they all get entered into the db
        with freeze_time(datetime(2020, 10, 1)):
            db.put_many(records)

        up_to = to_timestamp(datetime(2020, 10, 10))
        count = db.flush_released(up_to)

        # ids 5, 6, 7, 8, and 9 should have been released and deleted
        assert count == 5
        for i in range(5, 15):
            if i < 10:
                assert str(i) not in db
            else:
                assert str(i) in db

    def test_stats(self, tmp_path: Path):
        db = EmbargoQueue(tmp_path / "embargoes")
        records = [
            SourceRecord(str(i), {"NhmSecEmbargoDate": f"2020-10-{i:2}"}, "test")
            for i in range(5, 15)
        ]
        # freeze time to the start of the month so they all get entered into the db
        with freeze_time(datetime(2020, 10, 1)):
            db.put_many(records)

        up_to = to_timestamp(datetime(2020, 10, 10))
        embargoed, released = db.stats(up_to)

        assert embargoed == 5
        assert released == 5


@pytest.fixture
def redaction_db(tmp_path: Path):
    with closing(RedactionDB(tmp_path / "redactions")) as rdb:
        yield rdb


class TestRedactionDB:
    def test_add_ids(self, redaction_db: RedactionDB):
        redaction_db.add_ids("db_1", ["1", "5", "1040"], "red_1")
        redaction_db.add_ids("db_1", ["4", "5"], "red_2")
        redaction_db.add_ids("db_2", ["4003", "4004"], "red_2")

        assert redaction_db.size() == 6
        assert redaction_db.is_redacted("db_1", "1")
        assert redaction_db.is_redacted("db_1", "4")
        assert redaction_db.is_redacted("db_1", "5")
        assert redaction_db.is_redacted("db_1", "1040")
        assert not redaction_db.is_redacted("db_1", "3")

        assert redaction_db.get_all_redacted_ids("db_1") == {
            "1": "red_1",
            "4": "red_2",
            # this one gets the last value when it's added multiple times
            "5": "red_2",
            "1040": "red_1",
        }

    def test_is_redacted(self, redaction_db: RedactionDB):
        redaction_db.add_ids("test", ["50", "7"], "reasons")

        assert redaction_db.is_redacted("test", "50")
        assert redaction_db.is_redacted("test", "7")
        assert not redaction_db.is_redacted("test", "4")
        assert not redaction_db.is_redacted("test_2", "50")

    def test_get_all_redacted_ids(self, redaction_db: RedactionDB):
        redaction_db.add_ids("test", ["50", "7"], "reasons")

        assert redaction_db.get_all_redacted_ids("test") == {
            "50": "reasons",
            "7": "reasons",
        }
        assert redaction_db.get_all_redacted_ids("test_2") == {}
