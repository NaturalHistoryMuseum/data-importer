import gzip
from datetime import date
from pathlib import Path
from typing import Union

from dataimporter.emu.dumps import EMuTable, EMU_ID_FIELD


def create_dump(
    root: Path, table: Union[str, EMuTable], dump_date: date, *records: dict
) -> Path:
    """
    Creates an EMu dump using the given parameters to form the path in the root, and
    then adding the records. If no records are provided, a valid dump is still
    generated, it will just include no records.

    :param root: the directory to put the dump in
    :param table: the EMu table being dumped, doesn't need to be valid (hence the
                  str|EMuTable type)
    :param dump_date: the date of the dump
    :param records: 0+ records as dicts
    :return: the path of the created dump
    """
    export_part = "export"
    if isinstance(table, EMuTable):
        # eaudit dumps have a slightly different name format to normal dumps
        if table == EMuTable.eaudit:
            export_part = "deleted-export"
        table = table.name

    # form the path
    dump = root / f"{table}.{export_part}.{dump_date.strftime('%Y%m%d')}.gz"

    with gzip.open(dump, "wt", encoding="utf-8") as f:
        for row, record in enumerate(records, start=1):
            # create rownum and irn values for the record if it doesn't have them
            row = record.get("rownum", row)
            irn = record.get(EMU_ID_FIELD, row)
            f.writelines(
                [
                    f"rownum={row}\n",
                    f"{EMU_ID_FIELD}:1={irn}\n",
                ]
            )

            # write the other values
            for key, value in record.items():
                # we've done these, ignore if found
                if key == EMU_ID_FIELD or key == "rownum":
                    continue

                if not isinstance(value, (tuple, list)):
                    value = [value]

                f.writelines([f"{key}:{i}={v}\n" for i, v in enumerate(value)])

            f.write("###\n")

    return dump
