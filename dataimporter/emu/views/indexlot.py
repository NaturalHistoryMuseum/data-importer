from pathlib import Path

from dataimporter.emu.views.image import ImageView
from dataimporter.emu.views.taxonomy import TaxonomyView
from dataimporter.emu.views.utils import (
    DEPARTMENT_COLLECTION_CODES,
    DISALLOWED_STATUSES,
    INVALID_DEPARTMENT,
    INVALID_GUID,
    INVALID_STATUS,
    INVALID_TYPE,
    MEDIA_ID_REF_FIELD,
    NO_PUBLISH,
    add_associated_media,
    emu_date,
    is_valid_guid,
    is_web_published,
    merge,
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

TAXONOMY_ID_REF_FIELD = 'EntIndIndexLotTaxonNameLocalRef'


class IndexLotView(View):
    """
    View for index lot records.

    This view populates the index lot resource on the Data Portal.
    """

    def __init__(
        self,
        path: Path,
        store: Store,
        image_view: ImageView,
        taxonomy_view: TaxonomyView,
        published_name: str,
    ):
        super().__init__(path, store, published_name)
        self.image_link = make_link(self, MEDIA_ID_REF_FIELD, image_view, ID)
        self.taxonomy_link = make_link(self, TAXONOMY_ID_REF_FIELD, taxonomy_view, ID)

    def is_member(self, record: SourceRecord) -> FilterResult:
        """
        Filters the given record, determining whether it should be included in the index
        lot resource or not.

        :param record: the record to filter
        :return: a FilterResult object
        """
        if record.get_first_value('ColRecordType') != 'Index Lot':
            return INVALID_TYPE

        if record.get_first_value('ColDepartment') not in DEPARTMENT_COLLECTION_CODES:
            return INVALID_DEPARTMENT

        return SUCCESS_RESULT

    def is_publishable(self, record: SourceRecord) -> FilterResult:
        """
        Filters the given record, determining whether it matches the publishing rules
        for the index lots resource.

        :param record: the record to filter
        :return: a FilterResult object
        """
        if not is_web_published(record):
            return NO_PUBLISH

        if not is_valid_guid(record):
            return INVALID_GUID

        if record.get_first_value('SecRecordStatus') in DISALLOWED_STATUSES:
            return INVALID_STATUS

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
        get_all = record.get_all_values
        get_first = record.get_first_value

        data = {
            '_id': record.id,
            'created': emu_date(
                get_first('AdmDateInserted'), get_first('AdmTimeInserted')
            ),
            'modified': emu_date(
                get_first('AdmDateModified'), get_first('AdmTimeModified')
            ),
            'material': get_first('EntIndMaterial'),
            'type': get_first('EntIndType'),
            'media': get_first('EntIndMedia'),
            'british': get_first('EntIndBritish'),
            'kindOfMaterial': get_first('EntIndKindOfMaterial'),
            'kindOfMedia': get_first('EntIndKindOfMedia'),
            'materialCount': get_all('EntIndCount'),
            'materialSex': get_all('EntIndSex'),
            'materialStage': get_all('EntIndStage'),
            'materialTypes': get_all('EntIndTypes'),
            'materialPrimaryTypeNumber': get_all('EntIndPrimaryTypeNo'),
        }

        # add multimedia links
        add_associated_media(record, data, self.image_link)

        # add taxonomy data
        merge(record, data, self.taxonomy_link)

        return data
