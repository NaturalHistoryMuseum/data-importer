import gzip
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

from dataimporter.emu.dumps import (
    find_emu_dumps,
    EMuDump,
    EMU_ID_FIELD,
    is_valid_eaudit_record,
    EMU_TABLES,
    convert_eaudit_to_delete,
)
from dataimporter.lib.model import SourceRecord
from tests.helpers.dumps import create_dump


class TestFindEMuDumps:
    def test_no_files(self, tmp_path: Path):
        assert not find_emu_dumps(tmp_path)

    def test_after_works(self, tmp_path: Path):
        after = date(2020, 3, 14)

        for day in range(12, 18):
            create_dump(tmp_path, "ecatalogue", date(2020, 3, day))

        dumps = find_emu_dumps(tmp_path, after=after)
        assert len(dumps) == 3

    def test_skip_invalid(self, tmp_path: Path):
        for emu_table in EMU_TABLES:
            create_dump(tmp_path, emu_table, date(2020, 3, 1))
        create_dump(tmp_path, "invalid", date(2020, 3, 2))

        dumps = find_emu_dumps(tmp_path)
        assert len(dumps) == len(EMU_TABLES)


class TestEMuDump:
    def test_eq(self):
        # eq should only care about the version and the table
        assert EMuDump(MagicMock(), "eaudit", date(2020, 7, 20)) == EMuDump(
            MagicMock(), "eaudit", date(2020, 7, 20)
        )
        assert not EMuDump(MagicMock(), "ecatalogue", date(2020, 7, 20)) == EMuDump(
            MagicMock(), "eaudit", date(2020, 7, 20)
        )
        assert not EMuDump(MagicMock(), "emultimedia", date(2020, 7, 20)) == EMuDump(
            MagicMock(), "emultimedia", date(2020, 7, 21)
        )
        assert EMuDump(MagicMock(), "emultimedia", date(2020, 7, 20)) != object()

    def test_size(self, tmp_path: Path):
        path = create_dump(tmp_path, "etaxonomy", date(2020, 2, 4))

        dump = EMuDump(path, "etaxonomy", date(2020, 2, 4))

        assert path.stat().st_size == dump.size

    def test_count(self, tmp_path: Path):
        records = [{"x": i} for i in range(10)]
        path = create_dump(tmp_path, "etaxonomy", date(2020, 2, 4), *records)
        dump = EMuDump(path, "etaxonomy", date(2020, 2, 4))
        assert dump.count == 10

    def test_count_empty(self, tmp_path: Path):
        path = create_dump(tmp_path, "etaxonomy", date(2020, 2, 4))
        dump = EMuDump(path, "etaxonomy", date(2020, 2, 4))
        assert dump.count == 0

    def test_read(self, tmp_path: Path):
        records = [{"x": str(i)} for i in range(10)]
        path = create_dump(tmp_path, "etaxonomy", date(2020, 2, 4), *records)
        dump = EMuDump(path, "etaxonomy", date(2020, 2, 4))

        read_records = list(dump.read())

        assert len(read_records) == len(records)
        assert read_records == [
            SourceRecord(
                str(i),
                {"rownum": str(i), EMU_ID_FIELD: str(i), **record},
                dump.name,
            )
            for i, record in enumerate(records, start=1)
        ]

    def test_read_empty(self, tmp_path: Path):
        path = create_dump(tmp_path, "etaxonomy", date(2020, 2, 4))
        dump = EMuDump(path, "etaxonomy", date(2020, 2, 4))

        assert not list(dump.read())

    def test_read_missing_irn(self, tmp_path: Path):
        path = tmp_path / "a_bad_dump.gz"

        with gzip.open(path, "wt", encoding="utf-8") as f:
            # don't write an irn
            f.writelines([f"rownum=1\n", f"x:1=beans\n", "###\n"])

        dump = EMuDump(path, "etaxonomy", date(2020, 2, 4))

        assert not list(dump.read())

    def test_read_multiple_values(self, tmp_path: Path):
        records = [{"x": (str(i), str(i + 1), str(i + 5))} for i in range(10)]
        path = create_dump(tmp_path, "etaxonomy", date(2020, 2, 4), *records)
        dump = EMuDump(path, "etaxonomy", date(2020, 2, 4))

        read_records = list(dump.read())

        assert len(read_records) == len(records)
        assert read_records == [
            SourceRecord(
                str(i),
                {"rownum": str(i), EMU_ID_FIELD: str(i), **record},
                dump.name,
            )
            for i, record in enumerate(records, start=1)
        ]

    def test_read_blank_lines_and_no_delimiter_end(self, tmp_path: Path):
        records = [{"x": (str(i), str(i + 1), str(i + 5))} for i in range(10)]
        path = create_dump(tmp_path, "etaxonomy", date(2020, 2, 4), *records)
        with gzip.open(path, "at") as f:
            # add a couple of new lines and don't add a ### at the end either
            f.write("\n\n")

        dump = EMuDump(path, "etaxonomy", date(2020, 2, 4))
        read_records = list(dump.read())
        assert len(read_records) == len(records)


class TestIsValidEAuditRecord:
    def test_is_valid(self):
        tables = {"ecatalogue"}
        record = SourceRecord(
            "1",
            {"AudOperation": "delete", "AudTable": "ecatalogue", "AudKey": "23"},
            "test",
        )
        assert is_valid_eaudit_record(record, tables)

    def test_is_not_valid_bad_table(self):
        tables = {"ecatalogue"}
        record = SourceRecord(
            "1",
            {"AudOperation": "delete", "AudTable": "emultimedia", "AudKey": "23"},
            "test",
        )
        assert not is_valid_eaudit_record(record, tables)

    def test_is_not_valid_bad_operation(self):
        tables = {"ecatalogue"}
        record = SourceRecord(
            "1",
            {"AudOperation": "not delete", "AudTable": "ecatalogue", "AudKey": "23"},
            "test",
        )
        assert not is_valid_eaudit_record(record, tables)

    def test_is_not_valid_bad_key(self):
        tables = {"ecatalogue"}
        record = SourceRecord(
            "1", {"AudOperation": "delete", "AudTable": "ecatalogue"}, "test"
        )
        assert not is_valid_eaudit_record(record, tables)

    def test_is_not_valid_missing_everything(self):
        tables = {"ecatalogue"}
        record = SourceRecord("1", {}, "test")
        assert not is_valid_eaudit_record(record, tables)


def test_convert_eaudit_to_delete():
    audit_record = SourceRecord(
        "1",
        {"AudOperation": "delete", "AudTable": "ecatalogue", "AudKey": "23"},
        "test",
    )
    expected_record = SourceRecord("23", {}, "test")

    converted_record = convert_eaudit_to_delete(audit_record)

    assert convert_eaudit_to_delete(audit_record) == expected_record
    assert converted_record.is_deleted
