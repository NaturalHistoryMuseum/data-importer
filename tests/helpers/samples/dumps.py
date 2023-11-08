from enum import Enum
from typing import Optional
from uuid import uuid4


class EcatalogueType(Enum):
    specimen = "Specimen"
    indexlot = "Index Lot"
    artefact = "Artefact"
    preparation = "Preparation"


def create_ecatalogue(
    irn: str, ecatalogue_type: EcatalogueType, guid: Optional[str] = None, **extras
) -> dict:
    base = {
        "irn": irn,
        "ColRecordType": ecatalogue_type.value,
        "AdmPublishWebNoPasswordFlag": "Y",
        "AdmGUIDPreferredValue": guid if guid is not None else str(uuid4()),
        "ColDepartment": "Entomology",
    }
    if ecatalogue_type == EcatalogueType.preparation:
        base["ColSubDepartment"] = "Molecular Collections"
    base.update(extras)
    return base


def create_emultimedia(irn: str, guid: Optional[str] = None, **extras):
    return {
        "irn": irn,
        "MulMimeType": "image",
        "AdmGUIDPreferredValue": guid if guid is not None else str(uuid4()),
        "AdmPublishWebNoPasswordFlag": "Y",
        # image doesn't need this, but MSS does so might as well include it
        "DocIdentifier": "banana.jpg",
        **extras,
    }


def create_etaxonomy(irn: str, **extras):
    return {
        "irn": irn,
        "AdmPublishWebNoPasswordFlag": "Y",
        **extras,
    }


def create_eaudit(irn_to_delete: str, table_to_delete_from: str) -> dict:
    return {
        # doesn't matter what the irn of this record is so just always set it to -1
        "irn": "-1",
        "AudOperation": "delete",
        "AudTable": table_to_delete_from,
        "AudKey": irn_to_delete,
    }
