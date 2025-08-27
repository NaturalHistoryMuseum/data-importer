import re
from collections import deque
from pathlib import Path
from typing import Optional

from dataimporter.emu.views.utils import (
    DEPARTMENT_COLLECTION_CODES,
    DISALLOWED_STATUSES,
    INVALID_DEPARTMENT,
    INVALID_GUID,
    INVALID_STATUS,
    INVALID_SUB_DEPARTMENT,
    INVALID_TYPE,
    NO_PUBLISH,
    emu_date,
    is_valid_guid,
    is_web_published,
)
from dataimporter.lib.dbs import Store
from dataimporter.lib.model import SourceRecord
from dataimporter.lib.view import (
    ID,
    SUCCESS_RESULT,
    FilterResult,
    View,
    make_link,
    strip_empty,
)

INVALID_PROJECT = FilterResult(False, 'Invalid project')
ON_LOAN = FilterResult(False, 'On loan')

# a regex to check if the current location summary string indicates that the item is on
# loan. This is pretty broad currently as it just looks for a use of the word "loan" but
# better to be overstrict than loosey goosey
on_loan_regex = re.compile(r'\bloan\b', re.I)

# the EMu field on the prep records which links to the specimen voucher record
SPECIMEN_ID_REF_FIELD = 'EntPreSpecimenRef'
# the EMu field on mammal part prep records which links to the specimen voucher
# record
PARENT_SPECIMEN_ID_REF_FIELD = 'RegRegistrationParentRef'

# the Portal fields which are copied from the specimen to the prep data dict
MAPPED_SPECIMEN_FIELDS = [
    'associatedMedia',
    'associatedMediaCount',
    'barcode',
    'scientificName',
    'order',
    'identifiedBy',
    # this is a ColSite substitute which uses sumPreciseLocation
    'locality',
    'decimalLatitude',
    'decimalLongitude',
]


def is_on_loan(record: SourceRecord) -> bool:
    """
    Checks if the given record is on loan or not. If it is, returns True.

    :param record: the source record
    :return: True if it is on loan, False otherwise
    """
    # this field contains the IRN of the current location and 3250522 is the IRN of the
    # "on loan" location
    if record.get_first_value('LocPermanentLocationRef') == '3250522':
        return True
    # check if the current location summary matches the on loan regex
    if on_loan_regex.search(
        record.get_first_value('LocCurrentSummaryData', default='')
    ):
        return True
    return False


class PreparationView(View):
    """
    View for preparation records.

    This view populates the preparation resource on the Data Portal.
    """

    def __init__(
        self, path: Path, store: Store, specimen_view: View, published_name: str
    ):
        super().__init__(path, store, published_name)
        self.specimen_view = specimen_view
        self.voucher_spec_link = make_link(
            self, SPECIMEN_ID_REF_FIELD, specimen_view, ID
        )
        self.voucher_parent_link = make_link(
            self, PARENT_SPECIMEN_ID_REF_FIELD, specimen_view, ID
        )
        self.voucher_prep_link = make_link(self, SPECIMEN_ID_REF_FIELD, self, ID)

    def is_member(self, record: SourceRecord) -> FilterResult:
        """
        Filters the given record, determining whether it should be included in the
        preparation resource or not. This view member filter checks for one of two kinds
        of record in order for the record to be included:

            - a preparation record from the Molecular Collections sub-department
            - a mammal group part record from the LS Mammals sub-department with the
              DToL project tag

        These two types have slightly different fields but are both preps.

        :param record: the record to filter
        :return: a FilterResult object
        """
        record_type = record.get_first_value('ColRecordType', lower=True)
        sub_department = record.get_first_value('ColSubDepartment', lower=True)

        if record_type == 'preparation':
            # if the record is a prep, it must be a molecular collections prep
            if sub_department != 'molecular collections':
                return INVALID_SUB_DEPARTMENT
        elif record_type == 'mammal group part':
            # if the record is a mammal group part, it must be a mammals record and be
            # a DToL project record
            if sub_department != 'ls mammals':
                return INVALID_SUB_DEPARTMENT
            if record.get_first_value('NhmSecProjectName') != 'Darwin Tree of Life':
                return INVALID_PROJECT
        else:
            # any other type is invalid
            return INVALID_TYPE

        if record.get_first_value('ColDepartment') not in DEPARTMENT_COLLECTION_CODES:
            return INVALID_DEPARTMENT

        return SUCCESS_RESULT

    def is_publishable(self, record: SourceRecord) -> FilterResult:
        """
        Filters the given record, determining whether it matches the publishing rules
        for preps records.

        :param record: the record to filter
        :return: a FilterResult object
        """
        if not is_web_published(record):
            return NO_PUBLISH

        if not is_valid_guid(record):
            return INVALID_GUID

        if record.get_first_value('SecRecordStatus') in DISALLOWED_STATUSES:
            return INVALID_STATUS

        if is_on_loan(record):
            return ON_LOAN

        return SUCCESS_RESULT

    @strip_empty
    def transform(self, record: SourceRecord) -> dict:
        """
        Converts the record's raw data to a dict which will be the data presented on the
        Data Portal.

        :param record: the record to project
        :return: a dict containing the data for this record that should be displayed on
            the Data Portal
        """
        # cache these for perf
        ga = record.get_all_values
        gf = record.get_first_value

        data = {
            '_id': record.id,
            'project': ga('NhmSecProjectName'),
            'identifier': gf('EntPreNumber'),
            'preparationType': gf('EntPrePreparationKind', 'PreType'),
            'preservation': gf('EntPreStorageMedium', 'CatPreservative'),
            'preparationContents': gf('EntPreContents', 'PrtType', 'PreBodyPart'),
            'preparationProcess': get_preparation_process(record),
            'preparationDate': gf('EntPreDate'),
            'purpose': get_purpose(record),
            'occurrenceID': gf('AdmGUIDPreferredValue'),
            'created': emu_date(gf('AdmDateInserted'), gf('AdmTimeInserted')),
            'modified': emu_date(gf('AdmDateModified'), gf('AdmTimeModified')),
        }

        # add specimen data if available
        voucher_data = self.get_voucher_data(record)
        if voucher_data:
            data['associatedOccurrences'] = f'Voucher: {voucher_data["occurrenceID"]}'
            data.update(
                (field, value)
                for field in MAPPED_SPECIMEN_FIELDS
                if (value := voucher_data.get(field)) is not None
            )

        return data

    def get_voucher_data(self, prep: SourceRecord) -> Optional[dict]:
        """
        Search for the voucher record of the given prep record, returning the
        transformed version of it if found. There are three links which can ultimately
        lead to a specimen record, so this method tries them all until it gets to one.

        :param prep: the prep record
        :return: the transformed voucher record, or None if no voucher record is found
        """
        # this used to be implemented recursively but because EMu allows records to self
        # reference in the linked fields we're using (probably not on purpose, but I've
        # seen it in the wild!) we need to use a stack and check for records we've
        # already seen to avoid infinite loops
        stack = deque([prep])
        seen = {prep.id}

        while stack:
            record = stack.popleft()

            refs = [self.voucher_spec_link.owner_ref, self.voucher_prep_link.owner_ref]
            if (
                record.get_first_value('ColRecordType', lower=True)
                == 'mammal group part'
            ):
                # adding this last means it will ultimately be checked first cause stack
                refs.append(self.voucher_parent_link.owner_ref)

            for ref in refs:
                parent_id = ref.get_value(record)
                if not parent_id or parent_id in seen:
                    continue

                parent = self.store.get_record(parent_id)
                if parent:
                    if self.specimen_view.is_member(parent):
                        # we found a specimen, return the data from it
                        return self.specimen_view.transform(parent)
                    else:
                        # otherwise, add the parent to the stack
                        stack.appendleft(parent)
                        seen.add(parent.id)

        return None


def get_preparation_process(record: SourceRecord) -> Optional[str]:
    """
    Given a record, returns a value for the preparationProcess field. This value is
    pulled from the EntPrePreparationMethod EMu field. The value of this field is
    cleaned to remove the sometimes used prefix "killing agent" which we don't want to
    nor need to expose on the Portal.

    :return: the string value, or None if there is no EntPrePreparationMethod value
    """
    process = record.get_first_value('EntPrePreparationMethod')
    if not process:
        return None
    else:
        return re.sub(r'^killing agent:?\s*', '', process, count=1, flags=re.I)


def get_purpose(record: SourceRecord) -> Optional[str]:
    """
    Given a record, extract the purpose of specimen value from a note text field. We
    don't know which note text field it will be because there's no note type, so we just
    look for the first note text value which starts "purpose of specimen:".

    :param record: the record
    :return: the purpose value or None if it was not found
    """
    # find all the note text fields
    note_fields = [key for key in record.data.keys() if key.startswith('NteText')]
    if note_fields:
        for value in record.iter_all_values(*note_fields):
            if value.lower().startswith('purpose of specimen:'):
                # "purpose of specimen:" is 20 chars long
                return value[20:].strip()

    return None
