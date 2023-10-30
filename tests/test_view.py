from pathlib import Path
from typing import List
from unittest.mock import MagicMock, call

import pytest
from freezegun import freeze_time
from splitgill.utils import now

from dataimporter.dbs import DataDB
from dataimporter.model import SourceRecord
from dataimporter.view import FilterResult, SUCCESS_RESULT, View, ViewLink


def test_filter_result():
    assert FilterResult(True)
    assert not FilterResult(False, "not ok!")
    assert SUCCESS_RESULT


@pytest.fixture
def view(tmp_path: Path):
    v = View(tmp_path / "test_view", DataDB(tmp_path / "test_db"))
    yield v
    v.close()


class TestView:
    def test_is_member(self, view: View):
        # default success
        assert view.is_member(MagicMock())

    def test_make_data(self, view: View):
        assert view.make_data(SourceRecord("1", {"a": "b"}, "test")) == {"a": "b"}

    def test_transform(self, view: View):
        assert view.transform(SourceRecord("1", {"a": "b"}, "test")) == {"a": "b"}

    def test_transform_delete(self, view: View):
        view.make_data = MagicMock(return_value={})
        mock_view_link = MagicMock(transform=MagicMock())
        view.view_links_as_base.add(mock_view_link)
        assert view.transform(SourceRecord("1", {}, "test")) == {}
        # check that the make_data method isn't called
        assert not view.make_data.called
        # check that the mock view link's transform method isn't called
        assert not mock_view_link.transform.called

    def test_transform_removes_nones(self, view: View):
        # replace the make_data function with one that returns a dict containing a None
        # value
        view.make_data = lambda r: {"a": None, "b": "cheese!"}
        assert view.transform(SourceRecord("1", {"does": "not matter"}, "test")) == {
            "b": "cheese!"
        }

    def test_find_and_transform_all_good(self, view: View):
        record_1 = SourceRecord("5", {"name": "Egg", "legs": "4"}, "test")
        record_2 = SourceRecord("6", {"name": "Egg", "legs": "4"}, "test")
        record_3 = SourceRecord("9", {"name": "Egg", "legs": "4"}, "test")
        view.db.put_many([record_1, record_2, record_3])
        view.queue([record_1, record_2, record_3])

        data = list(view.find_and_transform(["6", "9", "2", "5"]))

        assert data == [record_2.data, record_3.data, record_1.data]

    @freeze_time("2023-09-09")
    def test_find_and_transform_embargo(self, view: View):
        record_1 = SourceRecord(
            "5", {"name": "Egg", "legs": "4", "NhmSecEmbargoDate": "2023-10-10"}, "test"
        )
        record_2 = SourceRecord("6", {"name": "Egg", "legs": "4"}, "test")
        record_3 = SourceRecord("9", {"name": "Egg", "legs": "4"}, "test")
        view.db.put_many([record_1, record_2, record_3])
        view.queue([record_1, record_2, record_3])

        assert view.embargoes.is_embargoed(record_1.id, now())

        data = list(view.find_and_transform(["6", "9", "2", "5"]))

        assert data == [record_2.data, record_3.data]

    def test_find_and_transform_delete(self, view: View):
        record_1 = SourceRecord("5", {}, "test")
        record_2 = SourceRecord("6", {"name": "Egg", "legs": "4"}, "test")
        record_3 = SourceRecord("9", {"name": "Egg", "legs": "4"}, "test")
        view.db.put_many([record_1, record_2, record_3])
        view.queue([record_1, record_2, record_3])

        data = list(view.find_and_transform(["6", "9", "2", "5"]))

        assert data == [record_2.data, record_3.data]

    def test_find_and_transform_not_member(self, view: View):
        def egg_member_check(record: SourceRecord) -> FilterResult:
            if record.get_first_value("name") != "Egg":
                return FilterResult(False, "not an egg!")
            return SUCCESS_RESULT

        view.is_member = egg_member_check

        record_1 = SourceRecord("5", {"name": "Leg", "legs": "4"}, "test")
        record_2 = SourceRecord("6", {"name": "Egg", "legs": "5"}, "test")
        record_3 = SourceRecord("9", {"name": "Egg", "legs": "6"}, "test")
        view.db.put_many([record_1, record_2, record_3])
        view.queue([record_1, record_2, record_3])

        data = list(view.find_and_transform(["6", "9", "2", "5"]))

        assert data == [record_2.data, record_3.data]

    def test_get_and_transform(self, view: View):
        record = SourceRecord("1", {"name": "Egg", "legs": "4"}, "test")
        view.db.put_many([record])
        assert view.get_and_transform("1") == record.data
        assert view.get_and_transform("2") is None

    @freeze_time("2023-09-09")
    def test_get_and_transform_embargo(self, view: View):
        record_1 = SourceRecord("1", {"name": "Egg", "legs": "4"}, "test")
        record_2 = SourceRecord(
            "2", {"name": "Egg", "NhmSecEmbargoDate": "2023-10-10"}, "test"
        )
        view.db.put_many([record_1, record_2])
        view.queue([record_1, record_2])
        assert view.get_and_transform("1") == record_1.data
        assert view.get_and_transform("2") is None

    def test_get_and_transform_delete(self, view: View):
        record_1 = SourceRecord("1", {}, "test")
        record_2 = SourceRecord("2", {"name": "Egg", "legs": "4"}, "test")
        view.db.put_many([record_1, record_2])
        view.queue([record_1, record_2])

        assert view.get_and_transform("1") is None
        assert view.get_and_transform("2") == record_2.data

    def test_get_and_transform_not_member(self, view: View):
        def egg_member_check(record: SourceRecord) -> FilterResult:
            if record.get_first_value("name") != "Egg":
                return FilterResult(False, "not an egg!")
            return SUCCESS_RESULT

        view.is_member = egg_member_check

        record_1 = SourceRecord("1", {"name": "Leg", "legs": "4"}, "test")
        record_2 = SourceRecord("2", {"name": "Egg", "legs": "5"}, "test")
        view.db.put_many([record_1, record_2])
        view.queue([record_1, record_2])

        assert view.get_and_transform("1") is None
        assert view.get_and_transform("2") == record_2.data

    def test_link(self, tmp_path: Path):
        view_1 = View(tmp_path / "test_view_1", DataDB(tmp_path / "test_db_1"))
        view_2 = View(tmp_path / "test_view_2", DataDB(tmp_path / "test_db_2"))
        view_link = MagicMock(base_view=view_1, foreign_view=view_2)

        view_1.link(view_link)
        view_2.link(view_link)

        assert view_link in view_1.view_links_as_base
        assert view_link in view_2.view_links_as_foreign

    @freeze_time("2023/10/16")
    def test_queue(self, view: View):
        def is_member(r) -> FilterResult:
            if r.data.get("member") == "yes":
                return FilterResult(True)
            else:
                return FilterResult(False, "Not a member!")

        # replace the view is_member method with our override
        view.is_member = is_member

        mock_base_link = MagicMock()
        mock_foreign_link = MagicMock()
        view.view_links_as_base.add(mock_base_link)
        view.view_links_as_foreign.add(mock_foreign_link)

        records = [
            # a delete
            SourceRecord("1", {}, "test"),
            # a member
            SourceRecord("2", {"member": "yes"}, "test"),
            # not a member
            SourceRecord("3", {"member": "no"}, "test"),
            # an embargoed member record
            SourceRecord(
                "4", {"member": "yes", "NhmSecEmbargoDate": "2025-02-05"}, "test"
            ),
        ]

        view.queue(records)

        assert list(view.changes) == ["1", "2", "4"]
        assert list(view.embargoes.iter_ids()) == ["4"]

        # check that the base links are updated with the members
        mock_base_link.update_from_base.assert_called_once_with(
            [records[1], records[3]]
        )

        # check that the foreign links have the changes to members and deletions
        # propagated
        mock_foreign_link.update_from_foreign.assert_has_calls(
            [call([records[1], records[3]]), call([records[0]])]
        )

    def test_queue_new_releases(self, view: View):
        records = [
            SourceRecord("1", {"NhmSecEmbargoDate": "2025-02-05"}, "test"),
            SourceRecord("2", {"NhmSecEmbargoDate": "2024-02-05"}, "test"),
            SourceRecord("3", {"NhmSecEmbargoDate": "2023-02-05"}, "test"),
        ]

        view.db.put_many(records)

        # add all the records to the embargoes db
        with freeze_time("2022/10/16"):
            view.embargoes.put_many(records)

        # release only records 2 and 3
        with freeze_time("2024/10/16"):
            view.queue_new_releases()

        assert list(view.changes) == ["2", "3"]
        assert list(view.embargoes.iter_ids()) == ["1"]

    def test_count(self, view: View):
        records = [
            SourceRecord("1", {"a": "5"}, "test"),
            SourceRecord("2", {"b": "x"}, "test"),
        ]

        view.queue(records)

        assert view.count() == 2

    def test_iter_changed(self, view: View):
        records = [
            SourceRecord(i, {"a": i}, "test") for i in sorted(map(str, range(100)))
        ]
        view.db.put_many(records)

        view.queue(records)

        assert list(view.iter_changed()) == records

    def test_flush(self, view: View):
        records = [
            # use int_to_str to maintain the sort order
            SourceRecord(i, {"a": i}, "test")
            for i in sorted(map(str, range(100)))
        ]
        records.append(SourceRecord("100", {"NhmSecEmbargoDate": "2025-02-05"}, "test"))

        with freeze_time("2024/10/16"):
            view.db.put_many(records)
            view.queue(records)

        assert view.count() == 101

        with freeze_time("2026/10/16"):
            view.flush()

        assert view.count() == 0
        assert view.embargoes.size() == 0

    @freeze_time("2024/10/16")
    def test_rebuild(self, view: View):
        mock_base_link = MagicMock()
        mock_foreign_link = MagicMock()
        view.view_links_as_base.add(mock_base_link)
        view.view_links_as_foreign.add(mock_foreign_link)

        records = [
            # use int_to_str to maintain the sort order
            SourceRecord(i, {"a": i}, "test")
            for i in sorted(map(str, range(100)))
        ]
        records.append(SourceRecord("100", {"NhmSecEmbargoDate": "2025-02-05"}, "test"))

        view.db.put_many(records)
        view.queue(records)

        view.rebuild()

        assert view.count() == 101
        assert view.embargoes.size() == 1
        assert mock_base_link.clear_from_base.called
        assert mock_foreign_link.clear_from_foreign.called

    def test_close(self, view: View):
        mock_base_link = MagicMock()
        mock_foreign_link = MagicMock()
        view.view_links_as_base.add(mock_base_link)
        view.view_links_as_foreign.add(mock_foreign_link)

        view.close()

        assert view.embargoes.db.closed
        assert view.changes.db.closed
        assert mock_base_link.close.called
        assert mock_foreign_link.close.called


class TestViewLink:
    def test_eq(self):
        class ViewLinkForTesting(ViewLink):
            def update_from_base(self, base_records: List[SourceRecord]):
                pass

            def update_from_foreign(self, foreign_records: List[SourceRecord]):
                pass

            def transform(self, record: SourceRecord, data: dict):
                pass

        v1 = ViewLinkForTesting("test1", MagicMock(), MagicMock())
        v1_again = ViewLinkForTesting("test1", MagicMock(), MagicMock())
        v2 = ViewLinkForTesting("test2", MagicMock(), MagicMock())

        assert v1 == v1_again
        assert v1 != v2
        assert v1 != object()
