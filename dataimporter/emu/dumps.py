import gzip
import itertools
import re
from datetime import date, datetime
from functools import cached_property
from pathlib import Path
from typing import Iterable, Any, Optional, Set
from typing import List

from dataimporter.lib.model import SourceRecord, Data

EMU_ID_FIELD = "irn"
# this is arbitrary-ish, but it's the day before the first good full dumps we have
FIRST_VERSION = date(2017, 8, 29)
# the EMu tables we currently use
EMU_TABLES = {"eaudit", "ecatalogue", "emultimedia", "etaxonomy"}


def find_emu_dumps(
    root: Path,
    after: date = FIRST_VERSION,
) -> List["EMuDump"]:
    """
    Find all the EMu dumps in the given path and return them as a list of EMuDump
    objects.

    :param root: the root directory
    :param after: only dumps after this date will be returned, defaults to the day
                  before the first full EMu dump from 30/08/17 (see FIRST_VERSION at the
                  module level)
    :return: a list of EMuDump objects
    """
    dumps = []
    dump_matcher = re.compile(
        r"(?P<table>\w+)\.(?:deleted-)?export\.(?P<date>[0-9]{8})\.gz"
    )

    for path in root.iterdir():
        match = dump_matcher.match(path.name)
        if match:
            table, date_str = match.groups()

            if table not in EMU_TABLES:
                # ignore invalid tables
                continue

            dump_date = datetime.strptime(date_str, "%Y%m%d").date()
            if dump_date <= after:
                # ignore old dumps
                continue

            dumps.append(EMuDump(path, table, dump_date))

    return dumps


class EMuDump:
    """
    Class representing an EMu export (or "dump") texexport file.

    Each file represents data from a single table.
    """

    def __init__(self, path: Path, table: str, dump_date: date):
        """
        :param path: the full path to the dump file
        :param table: the table the dump file is from
        :param dump_date: the date of the export
        """
        self.path = path
        self.table = table
        self.date = dump_date

    @property
    def name(self) -> str:
        return self.path.name

    def __str__(self) -> str:
        return f"Dump {self.table}@{self.date} [{self.path}]"

    def __eq__(self, other: Any):
        if isinstance(other, EMuDump):
            return self.date == other.date and self.table == other.table
        return NotImplemented

    @property
    def size(self) -> int:
        """
        Returns the size of the dump in bytes. This is the size of the compressed dump
        file.

        :return: the file size of the dump
        """
        return self.path.stat().st_size

    @cached_property
    def count(self) -> int:
        """
        Returns the number of records in the dump by counting the number of IRNs we
        find. This requires reading the whole dump.

        :return: the number of records in the dump
        """
        irn_field_prefix = f"{EMU_ID_FIELD}:1="
        with gzip.open(self.path, "rt", encoding="utf-8") as f:
            return sum(1 for line in f if line.lstrip().startswith(irn_field_prefix))

    def read(self) -> Iterable[SourceRecord]:
        """
        Reads the dump file and yield an EMuRecord object per record found in the dump.
        If a record read from the dump doesn't have a detectable IRN then no record is
        yielded and that record is skipped.

        :return: yields EMuRecord objects
        """
        # cache this so we don't have to look it up everytime we want to use it
        source = self.name

        with gzip.open(self.path, "rt", encoding="utf-8") as f:
            # state variables for each record
            emu_id: Optional[str] = None
            data: Data = {}

            # each record is delimited in the EMu dump using a line with just ### on it.
            # This chain here ensures that the file ends with a ### line even if one
            # isn't in the file, thus forcing the record to be yielded if it's valid.
            # Note that I've never seen a file not end with ### in the real world, but
            # anything's possible with EMu!
            for line in itertools.chain(f, ["###"]):
                line = line.strip()
                if not line:
                    continue

                if line != "###":
                    # the format is <field>:<index>=<value>
                    field, value = line.split("=", 1)
                    field = field.split(":", 1)[0]

                    if field == EMU_ID_FIELD:
                        emu_id = value

                    existing = data.get(field)
                    if existing is None:
                        # the value isn't in the data dict, add it
                        data[field] = value
                    else:
                        if isinstance(existing, tuple):
                            # there is an existing set of values in the data dict, add
                            # the new value in a new tuple
                            data[field] = (*existing, value)
                        else:
                            # there is an existing value (just one) in the data dict,
                            # add the new value in a new tuple
                            data[field] = (existing, value)
                else:
                    if emu_id is not None:
                        yield SourceRecord(emu_id, data, source)

                    # refresh our record state holding variables
                    emu_id = None
                    data = {}


def is_valid_eaudit_record(record: SourceRecord, tables: Set[str]) -> bool:
    """
    Given a record and a set of current tables, returns True if the record is an audit
    deletion record on one of the tables.

    :param record: the record to check
    :param tables: the tables the deletion must have occurred in to be valid
    :return: whether the record is a valid audit deletion on a table we are tracking
    """
    return (
        # we only want delete operations
        record.data.get("AudOperation") == "delete"
        # we only want deletions on tables we actually have data for already
        and record.data.get("AudTable") in tables
        # AudKey is the irn of the deleted record, so it must have this field
        and "AudKey" in record.data
    )


def convert_eaudit_to_delete(record: SourceRecord) -> SourceRecord:
    """
    Convert the given eaudit record into a deletion record.

    :param record: the audit record in full
    :return: a new SourceRecord object which will produce a deletion on the target
             record ID in the target table
    """
    return SourceRecord(record.data["AudKey"], {}, record.source)
