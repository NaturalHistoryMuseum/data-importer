import re
from functools import lru_cache
from itertools import chain
from typing import Optional, Iterable

from ciso8601 import parse_datetime
from dateutil import tz

from dataimporter.lib.model import SourceRecord
from dataimporter.lib.view import FilterResult, Link

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
# a dict containing department names -> collection codes. This is used both for checking
# department validity (do we want to publish a record from this department?) as well as
# to map the departments to their published collection codes
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
INVALID_SUB_DEPARTMENT = FilterResult(False, "Invalid sub-department")

GUID_REGEX = re.compile(
    "[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}", re.I
)

MEDIA_ID_REF_FIELD = "MulMultiMediaRef"
MEDIA_TARGET_FIELD = "associatedMedia"
MEDIA_COUNT_TARGET_FIELD = "associatedMediaCount"


def is_web_published(record: SourceRecord) -> bool:
    """
    Checks if the record should be published on the Data Portal. If it should, returns
    True, else False.

    :param record: the record to check
    :return: True if the record can be published on the Portal, False if not
    """
    return record.get_first_value("AdmPublishWebNoPasswordFlag", lower=True) == "y"


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


def combine_text(lines: Iterable[str]) -> Optional[str]:
    """
    Combines several lines of text together into a str and returns it. If the text
    strips down to the empty string, None is returned. Only single \n characters are
    inserted between lines even if there are multiple new lines between text in the
    lines parameter.

    :param lines: a number of lines of text in an iterable of strs
    :return: the combined text or None if no text results after combining
    """
    text = "\n".join(filter(None, (line.strip() for line in lines)))
    return text if text else None


def add_associated_media(record: SourceRecord, data: dict, link: Link):
    media = link.lookup_and_transform(record)
    if media:
        existing_media = data.get(MEDIA_TARGET_FIELD, [])
        # order by media ID
        data[MEDIA_TARGET_FIELD] = sorted(
            chain(existing_media, media), key=lambda m: int(m["_id"])
        )
        data[MEDIA_COUNT_TARGET_FIELD] = len(existing_media) + len(media)


def merge(record: SourceRecord, data: dict, link: Link):
    other = link.lookup_and_transform_one(record)
    if other:
        data.update(
            (key, value) for key, value in other.items() if data.get(key) is None
        )


# a set of EXIF orientation values (both raw numbers and textual representations) which,
# if applied to an image, would cause the width and height to swap
EXIF_ORIENTATION_SWAP_VALUES = {
    "5",
    "mirror horizontal and rotate 270 cw",
    "6",
    "rotate 90 cw",
    "7",
    "mirror horizontal and rotate 90 cw",
    "8",
    "rotate 270 cw",
}


def orientation_requires_swap(record: SourceRecord) -> bool:
    """
    Determines whether the image represented by the given record object requires its
    width and height values to be swapped based on the EXIF orientation tag. If the tag
    isn't found or the tag indicates no width/height swap is needed, then False is
    returned. If the orientation tag is in the range 5-8 then a swap is required and
    True is returned.

    :param record: the SourceRecord object
    :return: True if the width and height need to be swapped, False if not
    """
    exif_tags = record.get_all_values("ExiTag", reduce=False, clean=False)
    exif_tag_values = record.get_all_values("ExiValue", reduce=False, clean=False)
    if exif_tags and exif_tag_values:
        try:
            # the orientation tag is 274 (0x0112), so look up the index of that in
            # the tags tuple and then the value will be at the same index in the
            # values tuple. Because EMu, the value can be either a number or the
            # text version, e.g. "7" or "Rotate 270 CW"
            orientation = exif_tag_values[exif_tags.index("274")]
            # if the orientation tag has one of the values in the swap set, return True
            return orientation.lower() in EXIF_ORIENTATION_SWAP_VALUES
        except ValueError:
            # the orientation tag wasn't present in the exif data, fall through
            pass

    return False
