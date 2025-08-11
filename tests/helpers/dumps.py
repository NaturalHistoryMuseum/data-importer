import gzip
from datetime import date
from enum import Enum
from pathlib import Path
from typing import Optional, Tuple
from uuid import uuid4

from dataimporter.emu.dumps import EMU_ID_FIELD


def create_dump(root: Path, table: str, dump_date: date, *records: dict) -> Path:
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
    export_part = 'export'
    # eaudit dumps have a slightly different name format to normal dumps
    if table == 'eaudit':
        export_part = 'deleted-export'

    # form the path (and make sure it exists!)
    root.mkdir(exist_ok=True, parents=True)
    dump = root / f'{table}.{export_part}.{dump_date.strftime("%Y%m%d")}.gz'

    with gzip.open(dump, 'wt', encoding='utf-8') as f:
        for row, record in enumerate(records, start=1):
            # create rownum and irn values for the record if it doesn't have them
            row = record.get('rownum', row)
            irn = record.get(EMU_ID_FIELD, row)
            f.writelines(
                [
                    f'rownum={row}\n',
                    f'{EMU_ID_FIELD}:1={irn}\n',
                ]
            )

            # write the other values
            for key, value in record.items():
                # we've done these, ignore if found
                if key == EMU_ID_FIELD or key == 'rownum':
                    continue

                if not isinstance(value, (tuple, list)):
                    value = [value]

                f.writelines([f'{key}:{i}={v}\n' for i, v in enumerate(value)])

            f.write('###\n')

    return dump


class EcatalogueType(Enum):
    """
    An ecatalogue record type, the value assigned to each enum option is the
    ColRecordType value each is filtered on (well, for specimen, it's an example one as
    there are many.
    """

    specimen = 'Specimen'
    indexlot = 'Index Lot'
    artefact = 'Artefact'
    preparation = 'Preparation'


def create_ecatalogue(
    irn: str, ecatalogue_type: EcatalogueType, guid: Optional[str] = None, **extras
) -> dict:
    """
    Make a sample ecatalogue record of the given type with the given irn, optional guid
    and extra key/value pairs. Returns a dict which should pass the given type's view's
    is_member function.

    :param irn: the ID of the record
    :param ecatalogue_type: the EcatalogueType of the record to return
    :param guid: a guid for the record, can be None in which case one is generated
    :param extras: any extra key/value pairs to include in the dict
    :return: a dict
    """
    base = {
        'irn': irn,
        'ColRecordType': ecatalogue_type.value,
        'AdmPublishWebNoPasswordFlag': 'Y',
        'AdmGUIDPreferredValue': guid if guid is not None else str(uuid4()),
        'ColDepartment': 'Entomology',
    }
    if ecatalogue_type == EcatalogueType.preparation:
        base['ColSubDepartment'] = 'Molecular Collections'
    base.update(extras)
    return base


def create_emultimedia(irn: str, guid: Optional[str] = None, **extras):
    """
    Make a sample emultimedia record with the given irn, optional guid and extra
    key/value pairs. Returns a dict which should pass the image view's is_member
    function.

    :param irn: the ID of the record
    :param guid: a guid for the record, can be None in which case one is generated
    :param extras: any extra key/value pairs to include in the dict
    :return: a dict
    """
    return {
        'irn': irn,
        'MulMimeType': 'image',
        'AdmGUIDPreferredValue': guid if guid is not None else str(uuid4()),
        'AdmPublishWebNoPasswordFlag': 'Y',
        # image doesn't need this, but MSS does so might as well include it
        'DocIdentifier': 'banana.jpg',
        **extras,
    }


def create_etaxonomy(irn: str, **extras):
    """
    Make a sample etaxonomy record with the given irn, optional guid and extra key/value
    pairs. Returns a dict which should pass the taxonomy view's is_member function.

    :param irn: the ID of the record
    :param extras: any extra key/value pairs to include in the dict
    :return: a dict
    """
    return {
        'irn': irn,
        'AdmPublishWebNoPasswordFlag': 'Y',
        **extras,
    }


def create_eaudit(irn_to_delete: str, table_to_delete_from: str) -> dict:
    """
    Make a sample eaudit record which deletes the given irn from the given table.

    :param irn_to_delete: the ID of the record to delete
    :param table_to_delete_from: the table to delete the record from
    :return: a dict
    """
    return {
        # doesn't matter what the irn of this record is so just always set it to -1
        'irn': '-1',
        'AudOperation': 'delete',
        'AudTable': table_to_delete_from,
        'AudKey': irn_to_delete,
    }


def read_emu_extract(text: str) -> Tuple[str, dict]:
    """
    Reads the given text and turns it into a record ID and a dict of the record's data.
    The text is expected to be in the EMu dump format.

    :param text: some raw text
    :return: the ID and the data as a 2-tuple
    """
    data = {}
    for line in text.split('\n'):
        line = line.strip()
        if not line:
            continue

        # the format is <field>:<index>=<value>
        field, value = line.split('=', 1)
        field = field.split(':', 1)[0]

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

    return data['irn'], data
