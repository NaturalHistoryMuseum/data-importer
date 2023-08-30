import gzip
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

from splitgill.utils import to_timestamp

from dataimporter.emu.dumps import (
    EMuTable,
    find_emu_dumps,
    EMuDump,
    EMuAuditDump,
    EMU_ID_FIELD,
    EMuRecord,
)
from tests.helpers.dumps import create_dump


class TestEMuRecord:
    def test_is_deleted(self):
        assert EMuRecord(1, 2, {}).is_deleted
        assert not EMuRecord(1, 2, {"a": "4"}).is_deleted

    def test_contains(self):
        record = EMuRecord(1, 2, {"a": "4", "b": "26"})
        assert "a" in record
        assert "b" in record
        assert "c" not in record


class TestEMuTable:
    def test_ordering(self):
        tables = sorted(EMuTable)
        assert tables[0] == EMuTable.eaudit

    def test_is_stored(self):
        for table in EMuTable:
            if table == EMuTable.eaudit:
                assert not table.is_stored
            else:
                assert table.is_stored


class TestFindEMuDumps:
    def test_no_files(self, tmp_path: Path):
        assert not find_emu_dumps(tmp_path)

    def test_after_works(self, tmp_path: Path):
        after = to_timestamp(datetime(2020, 3, 15))

        for day in range(12, 18):
            create_dump(tmp_path, EMuTable.ecatalogue, datetime(2020, 3, day))

        dumps = find_emu_dumps(tmp_path, after=after)
        assert len(dumps) == 3

    def test_skip_invalid(self, tmp_path: Path):
        create_dump(tmp_path, EMuTable.ecatalogue, datetime(2020, 3, 1))
        create_dump(tmp_path, "invalid", datetime(2020, 3, 2))

        dumps = find_emu_dumps(tmp_path)
        assert len(dumps) == 1

    def test_audit_dumps(self, tmp_path: Path):
        path_1 = create_dump(tmp_path, EMuTable.eaudit, datetime(2020, 2, 1))
        path_2 = create_dump(tmp_path, EMuTable.ecatalogue, datetime(2020, 3, 1))

        dumps = find_emu_dumps(tmp_path)

        assert len(dumps) == 2
        assert dumps[0] == EMuAuditDump(path_1, EMuTable.eaudit, "20200201")
        assert dumps[1] == EMuDump(path_2, EMuTable.ecatalogue, "20200301")
        assert isinstance(dumps[0], EMuAuditDump)
        assert isinstance(dumps[0], EMuDump)

    def test_order(self, tmp_path: Path):
        path_4 = create_dump(tmp_path, EMuTable.etaxonomy, datetime(2020, 2, 4))
        path_2 = create_dump(tmp_path, EMuTable.ecatalogue, datetime(2020, 2, 1))
        path_1 = create_dump(tmp_path, EMuTable.eaudit, datetime(2020, 2, 1))
        path_5 = create_dump(tmp_path, EMuTable.emultimedia, datetime(2020, 3, 4))
        path_3 = create_dump(tmp_path, EMuTable.eaudit, datetime(2020, 2, 4))

        dumps = find_emu_dumps(tmp_path)

        assert dumps == [
            EMuAuditDump(path_1, EMuTable.eaudit, "20200201"),
            EMuAuditDump(path_2, EMuTable.ecatalogue, "20200201"),
            EMuAuditDump(path_3, EMuTable.eaudit, "20200204"),
            EMuAuditDump(path_4, EMuTable.etaxonomy, "20200204"),
            EMuAuditDump(path_5, EMuTable.emultimedia, "20200304"),
        ]


class TestEMuDump:
    def test_version_parsing(self):
        dump = EMuDump(MagicMock(), MagicMock(), "2020720")
        assert dump.version == to_timestamp(datetime(2020, 7, 20))

    def test_is_audit(self):
        audit_dump = EMuDump(MagicMock(), EMuTable.eaudit, "2020720")
        not_audit_dump = EMuDump(MagicMock(), EMuTable.ecatalogue, "2020720")

        assert audit_dump.is_audit
        assert not not_audit_dump.is_audit

    def test_eq(self):
        # eq should only care about the version and the table
        assert EMuDump(MagicMock(), EMuTable.eaudit, "2020720") == EMuDump(
            MagicMock(), EMuTable.eaudit, "2020720"
        )
        assert not EMuDump(MagicMock(), EMuTable.ecatalogue, "2020720") == EMuDump(
            MagicMock(), EMuTable.eaudit, "2020720"
        )
        assert not EMuDump(MagicMock(), EMuTable.emultimedia, "2020720") == EMuDump(
            MagicMock(), EMuTable.emultimedia, "2020721"
        )

    def test_lt(self):
        a = EMuDump(MagicMock(), EMuTable.ecatalogue, "2020720")
        b = EMuDump(MagicMock(), EMuTable.ecatalogue, "2020721")
        assert a < b

        a = EMuDump(MagicMock(), EMuTable.ecatalogue, "2020720")
        b = EMuDump(MagicMock(), EMuTable.emultimedia, "2020720")
        assert a < b

        a = EMuDump(MagicMock(), EMuTable.emultimedia, "2020720")
        b = EMuDump(MagicMock(), EMuTable.eaudit, "2020720")
        assert b < a

    def test_size(self, tmp_path: Path):
        path = create_dump(tmp_path, EMuTable.etaxonomy, datetime(2020, 2, 4))

        dump = EMuDump(path, EMuTable.etaxonomy, "20200204")

        assert path.stat().st_size == dump.size

    def test_count(self, tmp_path: Path):
        records = [{"x": i} for i in range(10)]
        path = create_dump(tmp_path, EMuTable.etaxonomy, datetime(2020, 2, 4), *records)
        dump = EMuDump(path, EMuTable.etaxonomy, "20200204")
        assert dump.count == 10

    def test_count_empty(self, tmp_path: Path):
        path = create_dump(tmp_path, EMuTable.etaxonomy, datetime(2020, 2, 4))
        dump = EMuDump(path, EMuTable.etaxonomy, "20200204")
        assert dump.count == 0

    def test_iter(self, tmp_path: Path):
        records = [{"x": str(i)} for i in range(10)]
        path = create_dump(tmp_path, EMuTable.etaxonomy, datetime(2020, 2, 4), *records)
        dump = EMuDump(path, EMuTable.etaxonomy, "20200204")

        read_records = list(dump)

        assert len(read_records) == len(records)
        assert read_records == [
            EMuRecord(
                i, dump.version, {"rownum": str(i), EMU_ID_FIELD: str(i), **record}
            )
            for i, record in enumerate(records, start=1)
        ]

    def test_iter_empty(self, tmp_path: Path):
        path = create_dump(tmp_path, EMuTable.etaxonomy, datetime(2020, 2, 4))
        dump = EMuDump(path, EMuTable.etaxonomy, "20200204")

        assert not list(dump)

    def test_iter_missing_irn(self, tmp_path: Path):
        path = tmp_path / "a_bad_dump.gz"

        with gzip.open(path, "wt", encoding="utf-8") as f:
            # don't write an irn
            f.writelines([f"rownum=1\n", f"x:1=beans\n", "###\n"])

        dump = EMuDump(path, EMuTable.etaxonomy, "20200204")

        assert not list(dump)

    def test_iter_multiple_values(self, tmp_path: Path):
        records = [{"x": (str(i), str(i + 1), str(i + 5))} for i in range(10)]
        path = create_dump(tmp_path, EMuTable.etaxonomy, datetime(2020, 2, 4), *records)
        dump = EMuDump(path, EMuTable.etaxonomy, "20200204")

        read_records = list(dump)

        assert len(read_records) == len(records)
        assert read_records == [
            EMuRecord(
                i, dump.version, {"rownum": str(i), EMU_ID_FIELD: str(i), **record}
            )
            for i, record in enumerate(records, start=1)
        ]

    def test_iter_blank_lines_and_no_delimiter_end(self, tmp_path: Path):
        records = [{"x": (str(i), str(i + 1), str(i + 5))} for i in range(10)]
        path = create_dump(tmp_path, EMuTable.etaxonomy, datetime(2020, 2, 4), *records)
        with gzip.open(path, "at") as f:
            # add a couple of new lines and don't add a ### at the end either
            f.write("\n\n")

        dump = EMuDump(path, EMuTable.etaxonomy, "20200204")
        read_records = list(dump)
        assert len(read_records) == len(records)


class TestEMuAuditDump:
    def test_iter(self, tmp_path: Path):
        records = [
            {"AudOperation": "delete", "AudKey": str(i), "AudTable": "ecatalogue"}
            for i in range(10)
        ]
        # add a record we should ignore
        records.append(
            {
                "AudOperation": "not a delete!",
                "AudKey": "100",
                "AudTable": "ecatalogue",
            }
        )
        # add a delete on a table we don't deal with
        records.append(
            {
                "AudOperation": "delete",
                "AudKey": "101",
                "AudTable": "not an emu table ever",
            }
        )

        path = create_dump(tmp_path, EMuTable.eaudit, datetime(2020, 1, 4), *records)
        dump = EMuAuditDump(path, EMuTable.eaudit, "20200104")

        read_records = list(dump)
        assert len(read_records) == 11
        # check they are all deletes
        assert all(record.data["AudOperation"] == "delete" for record in read_records)
        assert all(
            record.data["AudTable"] == "ecatalogue" for record in read_records[:-1]
        )
        assert read_records[-1].data["AudTable"] == "not an emu table ever"
