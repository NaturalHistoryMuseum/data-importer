from dataimporter.emu.views.utils import (
    DISALLOWED_STATUSES,
    INVALID_DEPARTMENT,
    INVALID_STATUS,
    INVALID_SUB_DEPARTMENT,
    INVALID_TYPE,
    NO_PUBLISH,
    is_web_published,
)
from dataimporter.lib.model import SourceRecord
from dataimporter.lib.view import SUCCESS_RESULT, FilterResult, View, strip_empty

MISSING_KIND_OF_OBJECT = FilterResult(False, 'Missing CatKindOfObject')


class MammalPartView(View):
    """
    View for mammal part specimen records.
    """

    def is_member(self, record: SourceRecord) -> FilterResult:
        """
        Filters the given record, determining whether it should be included in the
        mammal part view or not.

        :param record: the record to filter
        :return: a FilterResult object
        """
        if not is_web_published(record):
            return NO_PUBLISH

        if record.get_first_value('ColRecordType', lower=True) != 'mammal group part':
            return INVALID_TYPE

        if record.get_first_value('SecRecordStatus') in DISALLOWED_STATUSES:
            return INVALID_STATUS

        if record.get_first_value('ColDepartment', lower=True) != 'zoology':
            return INVALID_DEPARTMENT

        if record.get_first_value('ColSubDepartment', lower=True) != 'ls mammals':
            return INVALID_SUB_DEPARTMENT

        if 'CatKindOfObject' not in record:
            return MISSING_KIND_OF_OBJECT

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
        # todo: fields names
        return {
            'part': record.get_first_value('CatKindOfObject', 'PrtType'),
        }
