from dataimporter.model import VersionedRecord


class TestRecord:
    def test_is_deleted(self):
        assert VersionedRecord(1, 2, {}).is_deleted
        assert not VersionedRecord(1, 2, {"a": "4"}).is_deleted

    def test_contains(self):
        record = VersionedRecord(1, 2, {"a": "4", "b": "26"})
        assert "a" in record
        assert "b" in record
        assert "c" not in record
