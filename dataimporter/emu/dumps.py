import gzip
import itertools
import re
from datetime import datetime, timezone
from enum import Enum
from functools import cached_property, total_ordering
from pathlib import Path
from typing import Iterable, Any
from typing import List

from splitgill.utils import to_timestamp, parse_to_timestamp

from dataimporter.model import VersionedRecord

EMU_ID_FIELD = "irn"
# this is arbitrary-ish but it's the time of the first good full dumps we have
FIRST_VERSION = to_timestamp(datetime(2017, 8, 30))


@total_ordering
class EMuTable(Enum):
    """
    Enumeration of the EMu tables we currently handle.

    The value of the enum indicates the order they should be ingested in with EAudit
    first and then the others after.
    """

    eaudit = 0
    ecatalogue = 1
    emultimedia = 2
    etaxonomy = 3

    def __lt__(self, other):
        # implemented for the total_ordering annotation on the class and allow the
        # values to be used to prioritise the ingest of the tables
        if isinstance(other, EMuTable):
            return self.value < other.value
        return NotImplemented

    @property
    def is_stored(self) -> bool:
        """
        Whether the table's data should be stored or not.

        Currently, only EAudit is ignored as it is actually providing information about
        the other tables (like deletes).
        """
        return self != EMuTable.eaudit


def find_emu_dumps(root: Path, after: int = FIRST_VERSION) -> List["EMuDump"]:
    """
    Find all the EMu dumps in the given path and return them as a list of EMuDump
    objects. The list returned will be sorted in the order that the dumps should be
    processed.

    :param root: the root directory
    :param after: only dumps on or after this version will be returned, defaults to the
                  first full EMu dump from 30/08/17 (see FIRST_VERSION at the module
                  level)
    :return: a sorted list of EMuDump objects
    """
    dumps = []
    dump_matcher = re.compile(
        r"(?P<table>\w+)\.(?:deleted-)?export\.(?P<date>[0-9]{8})\.gz"
    )

    for path in root.iterdir():
        match = dump_matcher.match(path.name)
        if match:
            table_name, date = match.groups()
            try:
                table = EMuTable[table_name]
            except KeyError as e:
                # ignore as we don't deal with this table
                continue

            if table is EMuTable.eaudit:
                dump = EMuAuditDump(path, table, date)
            else:
                dump = EMuDump(path, table, date)

            if dump.version >= after:
                dumps.append(dump)

    return sorted(dumps)


class EMuDump:
    """
    Class representing an EMu export (or "dump") texexport file.

    Each file represents data from a single table.
    """

    def __init__(self, path: Path, table: EMuTable, date: str):
        """
        :param path: the full path to the dump file
        :param table: the table the dump file is from
        :param date: the date string of the export
        """
        self.path = path
        self.table = table
        self.date = date
        # convert the date into a version timestamp
        self.version = parse_to_timestamp(date, "%Y%m%d", tzinfo=timezone.utc)

    @property
    def is_audit(self):
        """
        Is this an audit dump?

        :return: True if it is, False if not.
        """
        return self.table == EMuTable.eaudit

    def __str__(self) -> str:
        return f"Dump {self.table}@{self.version}/{self.date} [{self.path}]"

    def __eq__(self, other: Any):
        if isinstance(other, EMuDump):
            return self.version == other.version and self.table == other.table
        return NotImplemented

    def __lt__(self, other: Any):
        if isinstance(other, EMuDump):
            # order by version, then table. The main goal here is to ensure the versions
            # are ordered correctly and the audit dumps are ordered before normal tables
            # as we need to do deletes first
            return (self.version, self.table) < (other.version, other.table)

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

    def __iter__(self) -> Iterable[VersionedRecord]:
        """
        Reads the dump file and yield an EMuRecord object per record found in the dump.
        If a record read from the dump doesn't have a detectable IRN then no record is
        yielded and that record is skipped.

        :return: yields EMuRecord objects
        """
        # cache this, so we don't have to look it up everytime we want to use it (for
        # performance)
        version = self.version

        with gzip.open(self.path, "rt", encoding="utf-8") as f:
            # state variables for each record
            emu_id = None
            data = {}

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
                        emu_id = int(value)

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
                        yield VersionedRecord(emu_id, version, data)

                    # refresh our record state holding variables
                    emu_id = None
                    data = {}


class EMuAuditDump(EMuDump):
    """
    Class representing an EMu audit table export (or "dump") texexport file.

    Each file represents data from the EAudit table which accounts for changes to any
    table in EMu. We specifically filter the audit table dumps for deletions, this is
    achieved through an overriden __iter__ method.
    """

    def __iter__(self) -> Iterable[VersionedRecord]:
        def record_filter(record: VersionedRecord):
            # filter the dump's records so that only valid deletions are yielded
            return (
                # we only want delete operations
                record.data.get("AudOperation") == "delete"
                # AudKey is the irn of the deleted record, so it must have this field
                and "AudKey" in record.data
                # and this is the table the record was deleted from
                and "AudTable" in record.data
            )

        yield from filter(record_filter, super().__iter__())
