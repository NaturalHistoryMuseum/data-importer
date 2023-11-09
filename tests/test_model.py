from dataimporter.lib.model import SourceRecord


class TestRecord:
    def test_is_deleted(self):
        assert SourceRecord("1", {}, "test").is_deleted
        assert not SourceRecord("1", {"a": "4"}, "test").is_deleted

    def test_contains(self):
        record = SourceRecord("1", {"a": "4", "b": "26"}, "test")
        assert "a" in record
        assert "b" in record
        assert "c" not in record

    def test_equals(self):
        record_1 = SourceRecord("1", {"a": "4", "b": "26"}, "test")
        record_2 = SourceRecord("1", {"a": "4", "b": "26"}, "test")
        assert record_1 == record_2

        # different ID
        record_3 = SourceRecord("2", {"a": "4", "b": "26"}, "test")
        assert record_1 != record_3

        # different data
        record_4 = SourceRecord("1", {"a": "6", "b": "26"}, "test")
        assert record_1 != record_4

        # different source
        record_5 = SourceRecord("1", {"a": "4", "b": "26"}, "test2")
        assert record_1 == record_5

        # sanity check
        assert record_1 != object()
        # check we're implementing __eq__ right
        assert record_1.__eq__(object()) == NotImplemented

    def test_iter_all_values(self):
        record = SourceRecord("1", {"a": "4", "b": ("a", "b"), "c": "banana"}, "test")

        # just a single field with a string
        assert list(record.iter_all_values("a")) == ["4"]
        # two fields, one is a string, one is a tuple
        assert list(record.iter_all_values("a", "b")) == ["4", "a", "b"]
        # three fields, some strings, some tuples
        assert list(record.iter_all_values("b", "c", "a")) == ["a", "b", "banana", "4"]
        # an invalid field
        assert list(record.iter_all_values("x")) == []
        # a valid field and an invalid field
        assert list(record.iter_all_values("d", "c")) == ["banana"]
        # multiple invalid fields
        assert list(record.iter_all_values("x", "y", "z")) == []

    def test_iter_all_values_cleaning(self):
        # check some combinations using the clean parameter
        record = SourceRecord("1", {"a": "", "b": ("", "b"), "c": "banana"}, "test")

        assert list(record.iter_all_values("a", clean=True)) == []
        assert list(record.iter_all_values("a", "b", clean=True)) == ["b"]
        assert list(record.iter_all_values("a", clean=False)) == [""]
        assert list(record.iter_all_values("a", "b", clean=False)) == ["", "", "b"]

    def test_get_all_values(self):
        record = SourceRecord("1", {"a": "4", "b": ("a", "b"), "c": "banana"}, "test")

        assert record.get_all_values("x") is None
        assert record.get_all_values("a") == "4"
        assert record.get_all_values("a", "x") == "4"
        assert record.get_all_values("a", "b") == ("4", "a", "b")
        assert record.get_all_values("c", "a") == ("banana", "4")

        # try all these again but with reduce=False
        assert record.get_all_values("x", reduce=False) is None
        assert record.get_all_values("a", reduce=False) == ("4",)
        assert record.get_all_values("a", "x", reduce=False) == ("4",)
        assert record.get_all_values("a", "b", reduce=False) == ("4", "a", "b")
        assert record.get_all_values("c", "a", reduce=False) == ("banana", "4")

    def test_get_all_values_cleaning(self):
        # check some combinations using the clean parameter
        record = SourceRecord("1", {"a": "", "b": ("", "b"), "c": "banana"}, "test")

        assert record.get_all_values("a", clean=True) is None
        assert record.get_all_values("a", "b", clean=True) == "b"
        assert record.get_all_values("a", clean=False) == ""
        assert record.get_all_values("a", "b", clean=False) == ("", "", "b")

        # try all these again but with reduce=False
        assert record.get_all_values("a", clean=True, reduce=False) is None
        assert record.get_all_values("a", "b", clean=True, reduce=False) == ("b",)
        assert record.get_all_values("a", clean=False, reduce=False) == ("",)
        assert record.get_all_values("a", "b", clean=False, reduce=False) == (
            "",
            "",
            "b",
        )

    def test_get_first_value(self):
        record = SourceRecord("1", {"a": "4", "b": ("a", "b"), "c": "banana"}, "test")

        assert record.get_first_value("x") is None
        default = "cheese!"
        assert record.get_first_value("x", default=default) is default
        assert record.get_first_value("a") == "4"
        assert record.get_first_value("a", "b") == "4"
        assert record.get_first_value("b", "a") == "a"

    def test_get_first_value_cleaning(self):
        # check some combinations using the clean parameter
        record = SourceRecord("1", {"a": "", "b": ("", "b"), "c": "banana"}, "test")

        assert record.get_first_value("a", clean=True) is None
        default = "xxxx"
        assert record.get_first_value("a", clean=True, default=default) is default
        assert record.get_first_value("a", "b", clean=True) == "b"
        assert record.get_first_value("a", clean=False) == ""
        assert record.get_first_value("a", "b", clean=False) == ""
