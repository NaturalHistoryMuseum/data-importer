import gzip
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

from dataimporter.emu.dumps import (
    EMuTable,
    find_emu_dumps,
    EMuDump,
    EMuAuditDump,
    EMU_ID_FIELD,
)
from dataimporter.model import SourceRecord
from dataimporter.dbs.converters import int_to_str
from tests.helpers.dumps import create_dump


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
        after = date(2020, 3, 15)

        for day in range(12, 18):
            create_dump(tmp_path, EMuTable.ecatalogue, date(2020, 3, day))

        dumps = find_emu_dumps(tmp_path, after=after)
        assert len(dumps) == 3

    def test_skip_invalid(self, tmp_path: Path):
        create_dump(tmp_path, EMuTable.ecatalogue, date(2020, 3, 1))
        create_dump(tmp_path, "invalid", date(2020, 3, 2))

        dumps = find_emu_dumps(tmp_path)
        assert len(dumps) == 1

    def test_audit_dumps(self, tmp_path: Path):
        path_1 = create_dump(tmp_path, EMuTable.eaudit, date(2020, 2, 1))
        path_2 = create_dump(tmp_path, EMuTable.ecatalogue, date(2020, 3, 1))

        dumps = find_emu_dumps(tmp_path)

        assert len(dumps) == 2
        assert dumps[0] == EMuAuditDump(path_1, EMuTable.eaudit, date(2020, 2, 1))
        assert dumps[1] == EMuDump(path_2, EMuTable.ecatalogue, date(2020, 3, 1))
        assert isinstance(dumps[0], EMuAuditDump)
        assert isinstance(dumps[0], EMuDump)

    def test_order(self, tmp_path: Path):
        path_4 = create_dump(tmp_path, EMuTable.etaxonomy, date(2020, 2, 4))
        path_2 = create_dump(tmp_path, EMuTable.ecatalogue, date(2020, 2, 1))
        path_1 = create_dump(tmp_path, EMuTable.eaudit, date(2020, 2, 1))
        path_5 = create_dump(tmp_path, EMuTable.emultimedia, date(2020, 3, 4))
        path_3 = create_dump(tmp_path, EMuTable.eaudit, date(2020, 2, 4))

        dumps = find_emu_dumps(tmp_path)

        assert dumps == [
            EMuAuditDump(path_1, EMuTable.eaudit, date(2020, 2, 1)),
            EMuAuditDump(path_2, EMuTable.ecatalogue, date(2020, 2, 1)),
            EMuAuditDump(path_3, EMuTable.eaudit, date(2020, 2, 4)),
            EMuAuditDump(path_4, EMuTable.etaxonomy, date(2020, 2, 4)),
            EMuAuditDump(path_5, EMuTable.emultimedia, date(2020, 3, 4)),
        ]


class TestEMuDump:
    def test_is_audit(self):
        audit_dump = EMuDump(MagicMock(), EMuTable.eaudit, date(2020, 7, 20))
        not_audit_dump = EMuDump(MagicMock(), EMuTable.ecatalogue, date(2020, 7, 20))

        assert audit_dump.is_audit
        assert not not_audit_dump.is_audit

    def test_eq(self):
        # eq should only care about the version and the table
        assert EMuDump(MagicMock(), EMuTable.eaudit, date(2020, 7, 20)) == EMuDump(
            MagicMock(), EMuTable.eaudit, date(2020, 7, 20)
        )
        assert not EMuDump(
            MagicMock(), EMuTable.ecatalogue, date(2020, 7, 20)
        ) == EMuDump(MagicMock(), EMuTable.eaudit, date(2020, 7, 20))
        assert not EMuDump(
            MagicMock(), EMuTable.emultimedia, date(2020, 7, 20)
        ) == EMuDump(MagicMock(), EMuTable.emultimedia, date(2020, 7, 21))

    def test_lt(self):
        a = EMuDump(MagicMock(), EMuTable.ecatalogue, date(2020, 7, 20))
        b = EMuDump(MagicMock(), EMuTable.ecatalogue, date(2020, 7, 21))
        assert a < b

        a = EMuDump(MagicMock(), EMuTable.ecatalogue, date(2020, 7, 20))
        b = EMuDump(MagicMock(), EMuTable.emultimedia, date(2020, 7, 20))
        assert a < b

        a = EMuDump(MagicMock(), EMuTable.emultimedia, date(2020, 7, 20))
        b = EMuDump(MagicMock(), EMuTable.eaudit, date(2020, 7, 20))
        assert b < a

    def test_size(self, tmp_path: Path):
        path = create_dump(tmp_path, EMuTable.etaxonomy, date(2020, 2, 4))

        dump = EMuDump(path, EMuTable.etaxonomy, date(2020, 2, 4))

        assert path.stat().st_size == dump.size

    def test_count(self, tmp_path: Path):
        records = [{"x": i} for i in range(10)]
        path = create_dump(tmp_path, EMuTable.etaxonomy, date(2020, 2, 4), *records)
        dump = EMuDump(path, EMuTable.etaxonomy, date(2020, 2, 4))
        assert dump.count == 10

    def test_count_empty(self, tmp_path: Path):
        path = create_dump(tmp_path, EMuTable.etaxonomy, date(2020, 2, 4))
        dump = EMuDump(path, EMuTable.etaxonomy, date(2020, 2, 4))
        assert dump.count == 0

    def test_iter(self, tmp_path: Path):
        records = [{"x": str(i)} for i in range(10)]
        path = create_dump(tmp_path, EMuTable.etaxonomy, date(2020, 2, 4), *records)
        dump = EMuDump(path, EMuTable.etaxonomy, date(2020, 2, 4))

        read_records = list(dump)

        assert len(read_records) == len(records)
        assert read_records == [
            SourceRecord(
                int_to_str(i),
                {"rownum": str(i), EMU_ID_FIELD: str(i), **record},
                dump.name,
            )
            for i, record in enumerate(records, start=1)
        ]

    def test_iter_empty(self, tmp_path: Path):
        path = create_dump(tmp_path, EMuTable.etaxonomy, date(2020, 2, 4))
        dump = EMuDump(path, EMuTable.etaxonomy, date(2020, 2, 4))

        assert not list(dump)

    def test_iter_missing_irn(self, tmp_path: Path):
        path = tmp_path / "a_bad_dump.gz"

        with gzip.open(path, "wt", encoding="utf-8") as f:
            # don't write an irn
            f.writelines([f"rownum=1\n", f"x:1=beans\n", "###\n"])

        dump = EMuDump(path, EMuTable.etaxonomy, date(2020, 2, 4))

        assert not list(dump)

    def test_iter_multiple_values(self, tmp_path: Path):
        records = [{"x": (str(i), str(i + 1), str(i + 5))} for i in range(10)]
        path = create_dump(tmp_path, EMuTable.etaxonomy, date(2020, 2, 4), *records)
        dump = EMuDump(path, EMuTable.etaxonomy, date(2020, 2, 4))

        read_records = list(dump)

        assert len(read_records) == len(records)
        assert read_records == [
            SourceRecord(
                int_to_str(i),
                {"rownum": str(i), EMU_ID_FIELD: str(i), **record},
                dump.name,
            )
            for i, record in enumerate(records, start=1)
        ]

    def test_iter_blank_lines_and_no_delimiter_end(self, tmp_path: Path):
        records = [{"x": (str(i), str(i + 1), str(i + 5))} for i in range(10)]
        path = create_dump(tmp_path, EMuTable.etaxonomy, date(2020, 2, 4), *records)
        with gzip.open(path, "at") as f:
            # add a couple of new lines and don't add a ### at the end either
            f.write("\n\n")

        dump = EMuDump(path, EMuTable.etaxonomy, date(2020, 2, 4))
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

        path = create_dump(tmp_path, EMuTable.eaudit, date(2020, 1, 4), *records)
        dump = EMuAuditDump(path, EMuTable.eaudit, date(2020, 1, 4))

        read_records = list(dump)
        assert len(read_records) == 11
        # check they are all deletes
        assert all(record.data["AudOperation"] == "delete" for record in read_records)
        assert all(
            record.data["AudTable"] == "ecatalogue" for record in read_records[:-1]
        )
        assert read_records[-1].data["AudTable"] == "not an emu table ever"
