from pathlib import Path
from unittest.mock import MagicMock

import pytest
from freezegun import freeze_time
from splitgill.utils import now

from dataimporter.lib.dbs import Store
from dataimporter.lib.model import SourceRecord
from dataimporter.lib.view import FilterResult, SUCCESS_RESULT, View, make_link, ID


def test_filter_result():
    assert FilterResult(True)
    assert not FilterResult(False, "not ok!")
    assert SUCCESS_RESULT


@pytest.fixture
def view(tmp_path: Path):
    v = View(tmp_path / "test_view", Store(tmp_path / "test_store"))
    yield v
    v.close()


class TestView:
    def test_is_member(self, view: View):
        # default success
        assert view.is_member(MagicMock())

    def test_transform(self, view: View):
        assert view.transform(SourceRecord("1", {"a": "b"}, "test")) == {"a": "b"}

    def test_transform_delete(self, view: View):
        assert view.transform(SourceRecord("1", {}, "test")) == {}

    def test_transform_does_not_remove_nones(self, view: View):
        record = SourceRecord("1", {"a": "b", "c": None, "d": ""}, "test")
        assert view.transform(record) == record.data

    def test_find_and_transform_all_good(self, view: View):
        record_1 = SourceRecord("5", {"name": "Egg", "legs": "4"}, "test")
        record_2 = SourceRecord("6", {"name": "Egg", "legs": "4"}, "test")
        record_3 = SourceRecord("9", {"name": "Egg", "legs": "4"}, "test")
        view.store.put([record_1, record_2, record_3])

        data = list(view.find_and_transform(["6", "9", "2", "5"]))

        assert data == [record_2.data, record_3.data, record_1.data]

    @freeze_time("2023-09-09")
    def test_find_and_transform_embargo(self, view: View):
        record_1 = SourceRecord(
            "5", {"name": "Egg", "legs": "4", "NhmSecEmbargoDate": "2023-10-10"}, "test"
        )
        record_2 = SourceRecord("6", {"name": "Egg", "legs": "4"}, "test")
        record_3 = SourceRecord("9", {"name": "Egg", "legs": "4"}, "test")
        view.store.put([record_1, record_2, record_3])

        assert view.store.is_embargoed(record_1.id, now())

        data = list(view.find_and_transform(["6", "9", "2", "5"]))

        assert data == [record_2.data, record_3.data]

    def test_find_and_transform_delete(self, view: View):
        record_1 = SourceRecord("5", {}, "test")
        record_2 = SourceRecord("6", {"name": "Egg", "legs": "4"}, "test")
        record_3 = SourceRecord("9", {"name": "Egg", "legs": "4"}, "test")
        view.store.put([record_1, record_2, record_3])

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
        view.store.put([record_1, record_2, record_3])

        data = list(view.find_and_transform(["6", "9", "2", "5"]))

        assert data == [record_2.data, record_3.data]

    def test_get_and_transform(self, view: View):
        record = SourceRecord("1", {"name": "Egg", "legs": "4"}, "test")
        view.store.put([record])
        assert view.get_and_transform("1") == record.data
        assert view.get_and_transform("2") is None

    @freeze_time("2023-09-09")
    def test_get_and_transform_embargo(self, view: View):
        record_1 = SourceRecord("1", {"name": "Egg", "legs": "4"}, "test")
        record_2 = SourceRecord(
            "2", {"name": "Egg", "NhmSecEmbargoDate": "2023-10-10"}, "test"
        )
        view.store.put([record_1, record_2])
        assert view.get_and_transform("1") == record_1.data
        assert view.get_and_transform("2") is None

    def test_get_and_transform_delete(self, view: View):
        record_1 = SourceRecord("1", {}, "test")
        record_2 = SourceRecord("2", {"name": "Egg", "legs": "4"}, "test")
        view.store.put([record_1, record_2])

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
        view.store.put([record_1, record_2])

        assert view.get_and_transform("1") is None
        assert view.get_and_transform("2") == record_2.data

    def test_link(self, tmp_path: Path):
        view_1 = View(tmp_path / "test_view_1", Store(tmp_path / "test_store_1"))
        view_2 = View(tmp_path / "test_view_2", Store(tmp_path / "test_store_2"))

        link = make_link(view_1, ID, view_2, ID)

        assert not view_1.dependants
        assert view_2.dependants == [link]

    @freeze_time("2023/10/16")
    def test_queue(self, view: View):
        def is_member(r) -> FilterResult:
            if r.data.get("member") == "yes":
                return FilterResult(True)
            else:
                return FilterResult(False, "Not a member!")

        # replace the view is_member method with our override
        view.is_member = is_member
        mock_link = MagicMock()
        view.add_dependant(mock_link)

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

        view.store.put(records)
        view.queue(records)

        assert list(view.changes.iter()) == ["1", "2", "4"]
        assert list(view.store.iter_embargoed_ids()) == ["4"]

        # check that the link is updated with the members and deletes
        mock_link.queue_changes.assert_called_once_with(
            [records[0], records[1], records[3]]
        )

    def test_queue_avoid_inf_loop(self, view: View):
        def is_member(r) -> FilterResult:
            if r.data.get("member") == "yes":
                return FilterResult(True)
            else:
                return FilterResult(False, "Not a member!")

        """
        This shouldn't really happen but it's possible we could end up modelling a
        circular reference and we should avoid infinite loops if it happens. If it does
        ever happen it's likely to be recordA -> recordB -> recordA but the quicker way
        to check is by doing recordA -> recordA which is what this test tests.
        """

        # replace the view is_member method with our override
        view.is_member = is_member
        mock_link = MagicMock()
        view.add_dependant(mock_link)

        record = SourceRecord("1", {"member": "yes"}, "test")

        # queue the record
        view.queue([record])
        # check the link queue_changes method was called
        mock_link.queue_changes.assert_called_once_with([record])

        # mock the scenario where our record references itself, i.e. queue_changes would
        # call view.queue again but we're going to call it instead
        # first, reset the mock
        mock_link.reset_mock()
        view.queue([record])
        # check the link queue_changes method was not called
        mock_link.queue_changes.assert_not_called()

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
        view.store.put(records)
        view.queue(records)
        assert list(view.iter_changed()) == records

    def test_flush(self, view: View):
        records = [
            SourceRecord(i, {"a": i}, "test") for i in sorted(map(str, range(100)))
        ]
        view.store.put(records)
        view.queue(records)
        assert view.count() == 100
        view.flush()
        assert view.count() == 0

    @freeze_time("2024/10/16")
    def test_rebuild(self, view: View):
        records = [SourceRecord(str(i), {"a": str(i)}, "test") for i in range(200)]
        view.store.put(records)
        view.queue([records[0], records[-1]])
        assert view.count() == 2

        view.rebuild()

        assert view.count() == 200

    def test_close(self, view: View):
        view.close()
        assert view.changes.db.db.closed
