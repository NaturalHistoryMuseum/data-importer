import re
from functools import lru_cache
from typing import Optional

from ciso8601 import parse_datetime
from dateutil import tz

from dataimporter.model import SourceRecord
from dataimporter.view import FilterResult

DISALLOWED_STATUSES = {
    "DELETE",
    "DELETE-MERGED",
    "DUPLICATION",
    "Disposed of",
    "FROZEN ARK",
    "INVALID",
    "POSSIBLE TYPE",
    "PROBLEM",
    "Re-registered in error",
    "Reserved",
    "Retired",
    "Retired (see Notes)",
    "SCAN_cat",
    "See Notes",
    "Specimen missing - see notes",
    "Stub",
    "Stub Record",
    "Stub record",
}
DEPARTMENT_COLLECTION_CODES = {
    "Botany": "BOT",
    "Entomology": "BMNH(E)",
    "Mineralogy": "MIN",
    "Palaeontology": "PAL",
    "Zoology": "ZOO",
}

NO_PUBLISH = FilterResult(False, "Record is not published")
INVALID_GUID = FilterResult(False, "Invalid GUID")
INVALID_TYPE = FilterResult(False, "Record type invalid")
INVALID_STATUS = FilterResult(False, "Invalid record status")
INVALID_DEPARTMENT = FilterResult(False, "Invalid department")

GUID_REGEX = re.compile(
    "[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}", re.I
)


def is_web_published(record: SourceRecord) -> bool:
    """
    Checks if the record should be published on the Data Portal. If it should, returns
    True, else False.

    :param record: the record to check
    :return: True if the record can be published on the Portal, False if not
    """
    # TODO: check if the default should actually be n here, or whether we should do a
    #       check on == 'y' instead
    return (
        record.get_first_value("AdmPublishWebNoPasswordFlag", default="").lower() != "n"
    )


def is_valid_guid(record: SourceRecord) -> bool:
    """
    Checks if the record has a valid GUID. If it does, returns True, else returns False.

    :param record: the record to check
    :return: True if the record has a valid GUID, False if not
    """
    guid = record.get_first_value("AdmGUIDPreferredValue")
    return guid is not None and GUID_REGEX.match(guid)


@lru_cache(maxsize=65536)
def emu_date(date: str, time: str) -> Optional[str]:
    """
    Given the date and time from a pair of EMu date and time fields, returns the ISO
    formatted datetime.

    :param date: an EMu date string
    :param time: an EMu time string
    :return: None if either the date or the time are missing, otherwise
    """
    if not date or not time:
        return None

    try:
        # parse the string and then replace the timezone
        return parse_datetime(f"{date} {time}").replace(tzinfo=tz.UTC).isoformat()
    except ValueError:
        return None


@lru_cache
def translate_collection_code(department: str) -> Optional[str]:
    """
    Given a department from an EMu record, return the short code version we display on
    the Data Portal in the collectionCode field.

    :param department: the department name
    :return: the short code version of the department, or None if no match is found
    """
    return DEPARTMENT_COLLECTION_CODES.get(department)