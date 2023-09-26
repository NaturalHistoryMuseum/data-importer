from datetime import datetime
from itertools import chain
from pathlib import Path
from typing import Optional

import pytest
from freezegun import freeze_time
from splitgill.utils import partition, parse_to_timestamp, now, to_timestamp

from dataimporter.dbs.dbs import DB, DataDB, LinkDB, ChangeQueue, EmbargoQueue
from dataimporter.model import SourceRecord
from dataimporter.dbs.converters import int_to_str


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
            SourceRecord(int_to_str(i), {"a": "banana"}, "test") for i in range(5000)
        ]

        for chunk in partition(records, 541):
            db.put_many(chunk)

        assert list(db) == records

    def test_get_record(self, tmp_path: Path):
        db = DataDB(tmp_path / "database")

        records = [
            SourceRecord(int_to_str(i), {"a": "banana"}, "test") for i in range(40)
        ]

        db.put_many(records)

        assert db.get_record(int_to_str(41)) is None
        assert db.get_record(int_to_str(23)) == records[23]

    def test_get_records(self, tmp_path: Path):
        db = DataDB(tmp_path / "database")

        records = [
            SourceRecord(int_to_str(i), {"a": "banana"}, "test") for i in range(40)
        ]

        db.put_many(records)

        ids = [6, 10, 3, 50, 8, 9, 9, 35]
        assert list(db.get_records(map(int_to_str, ids))) == [
            records[6],
            records[10],
            records[3],
            # 50 is skipped as it shouldn't exist
            records[8],
            records[9],
            records[9],
            records[35],
        ]


class TestLinkDB:
    def test_put_many_and_lookup(self, tmp_path: Path):
        db = LinkDB(tmp_path / "database", "linked")

        records = [
            # one to one from 1 -> 6
            SourceRecord("1", {"x": "A", "linked": "6"}, "test"),
            # one to many, oh boy! 2 -> 8 and 2 -> 5
            SourceRecord("2", {"x": "B", "linked": ("8", "5")}, "test"),
            # no linked field present so should be ignored
            SourceRecord("3", {"x": "C"}, "test"),
            # another one to one with 4 -> 8 but, this is the second id that links to 8
            SourceRecord("4", {"x": "D", "linked": "8"}, "test"),
        ]

        db.put_many(records)

        assert db.size() == 4
        assert list(db.lookup(["6", "8", "5"])) == ["1", "2", "4", "2"]


class TestChangeQueue:
    def test_put_many_ids_and_iter(self, tmp_path: Path):
        db = ChangeQueue(tmp_path / "database")

        ids = list(map(str, range(5)))

        db.put_many_ids(ids)

        assert list(db) == ids

    def test_put_many(self, tmp_path: Path):
        db = ChangeQueue(tmp_path / "database")

        records = [SourceRecord(int_to_str(i), {"a": "x"}, "test") for i in range(50)]

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
