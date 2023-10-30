import gzip
import shutil
from itertools import count
from pathlib import Path
from typing import List, Dict

ECATALOGUE_ARTEFACT_SAMPLE_DUMP = (
    Path(__file__).parent / "emu" / "ecatalogue_artefact_53_sample.gz"
)
ECATALOGUE_INDEXLOT_SAMPLE_DUMP = (
    Path(__file__).parent / "emu" / "ecatalogue_indexlot_2000_sample.gz"
)
ECATALOGUE_SPECIMEN_SAMPLE_DUMP = (
    Path(__file__).parent / "emu" / "ecatalogue_specimen_10000_sample.gz"
)

EMULTIMEDIA_ARTEFACT_SAMPLE_DUMP = (
    Path(__file__).parent / "emu" / "emultimedia_artefact_565_sample.gz"
)
EMULTIMEDIA_INDEXLOT_SAMPLE_DUMP = (
    Path(__file__).parent / "emu" / "emultimedia_indexlot_406_sample.gz"
)
EMULTIMEDIA_SPECIMEN_SAMPLE_DUMP = (
    Path(__file__).parent / "emu" / "emultimedia_specimen_11271_sample.gz"
)

ETAXONOMY_ARTEFACT_SAMPLE_DUMP = (
    Path(__file__).parent / "emu" / "etaxonomy_artefact_1_sample.gz"
)
ETAXONOMY_INDEXLOT_SAMPLE_DUMP = (
    Path(__file__).parent / "emu" / "etaxonomy_indexlot_1880_sample.gz"
)
ETAXONOMY_SPECIMEN_SAMPLE_DUMP = (
    Path(__file__).parent / "emu" / "etaxonomy_specimen_1_sample.gz"
)


def create_ecatalogue_dump(
    path: Path,
    date: str,
    include_artefacts: bool = True,
    include_indexlots: bool = True,
    include_specimens: bool = True,
):
    dump_file = path / f"ecatalogue.export.{date}.gz"
    dumps = []
    if include_artefacts:
        dumps.append(ECATALOGUE_ARTEFACT_SAMPLE_DUMP)
    if include_indexlots:
        dumps.append(ECATALOGUE_INDEXLOT_SAMPLE_DUMP)
    if include_specimens:
        dumps.append(ECATALOGUE_SPECIMEN_SAMPLE_DUMP)

    with dump_file.open("wb") as g:
        for dump in dumps:
            with dump.open("rb") as f:
                shutil.copyfileobj(f, g)


def create_emultimedia_dump(
    path: Path,
    date: str,
    include_artefacts: bool = True,
    include_indexlots: bool = True,
    include_specimens: bool = True,
):
    dump_file = path / f"emultimedia.export.{date}.gz"
    dumps = []
    if include_artefacts:
        dumps.append(EMULTIMEDIA_ARTEFACT_SAMPLE_DUMP)
    if include_indexlots:
        dumps.append(EMULTIMEDIA_INDEXLOT_SAMPLE_DUMP)
    if include_specimens:
        dumps.append(EMULTIMEDIA_SPECIMEN_SAMPLE_DUMP)

    with dump_file.open("wb") as g:
        for dump in dumps:
            with dump.open("rb") as f:
                shutil.copyfileobj(f, g)


def create_etaxonomy_dump(
    path: Path,
    date: str,
    include_artefacts: bool = True,
    include_indexlots: bool = True,
    include_specimens: bool = True,
):
    dump_file = path / f"etaxonomy.export.{date}.gz"
    dumps = []
    if include_artefacts:
        dumps.append(ETAXONOMY_ARTEFACT_SAMPLE_DUMP)
    if include_indexlots:
        dumps.append(ETAXONOMY_INDEXLOT_SAMPLE_DUMP)
    if include_specimens:
        dumps.append(ETAXONOMY_SPECIMEN_SAMPLE_DUMP)

    with dump_file.open("wb") as g:
        for dump in dumps:
            with dump.open("rb") as f:
                shutil.copyfileobj(f, g)


def create_eaudit_dump(path: Path, irns_to_delete: Dict[str, List[str]], date: str):
    dump_file = path / f"eaudit.deleted-export.{date}.gz"

    irn_generator = count(1)

    with gzip.GzipFile(dump_file, "wb") as g:
        for table, irns in irns_to_delete.items():
            for irn in irns:
                g.write(f"irn:1={next(irn_generator)}\n".encode("utf-8"))
                g.write(f"AudOperation:1=delete\n".encode("utf-8"))
                g.write(f"AudTable:1={table}\n".encode("utf-8"))
                g.write(f"AudKey:1={irn}\n".encode("utf-8"))
                g.write("###\n".encode("utf-8"))
