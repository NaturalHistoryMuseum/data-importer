import csv
from io import TextIOWrapper, BytesIO
from pathlib import Path
from zipfile import ZipFile

__all__ = (
    "SAMPLE_GBIF_ZIP_BYTES",
    "SAMPLE_GBIF_RECORDS",
    "SAMPLE_GBIF_RECORD_ID",
    "SAMPLE_GBIF_RECORD_DATA",
)

sample_download_zip_path = Path(__file__).parent.absolute() / "10_gbif_records.zip"

# first, just read the zip file as pure bytes
with sample_download_zip_path.open("rb") as f:
    SAMPLE_GBIF_ZIP_BYTES = f.read()

# next, parse the zip file, extract the csv and load the rows
with ZipFile(BytesIO(SAMPLE_GBIF_ZIP_BYTES)).open("test_download_id.csv") as raw_csv:
    with TextIOWrapper(raw_csv, encoding="utf-8") as csv_file:
        SAMPLE_GBIF_RECORDS = {
            row["gbifID"]: row
            for row in csv.DictReader(
                csv_file, dialect="excel-tab", quoting=csv.QUOTE_NONE
            )
        }

# pull all the records out into a dict of id -> data
SAMPLE_GBIF_RECORD_ID, SAMPLE_GBIF_RECORD_DATA = next(iter(SAMPLE_GBIF_RECORDS.items()))
