from dataimporter.emu.views.utils import NO_PUBLISH, is_web_published
from dataimporter.emu.views.utils import emu_date
from dataimporter.lib.model import SourceRecord
from dataimporter.lib.view import View, FilterResult, SUCCESS_RESULT


class TaxonomyView(View):
    """
    View for taxonomy records.

    This view doesn't have a Data Portal resource that it populates, instead the records
    that go through this view are merged into other record types.
    """

    def is_member(self, record: SourceRecord) -> FilterResult:
        """
        Filters the given record, determining whether it is a taxonomy record or not.

        :param record: the record to filter
        :return: a FilterResult object
        """
        if not is_web_published(record):
            return NO_PUBLISH

        return SUCCESS_RESULT

    def make_data(self, record: SourceRecord) -> dict:
        """
        Converts the record's raw data to a dict which will then be merged into other
        records and presented on the Data Portal.

        :param record: the record to project
        :return: a dict containing the data for this record that should be displayed on
                 the Data Portal
        """
        # cache for perf
        get_first = record.get_first_value

        return {
            "_id": record.id,
            "created": emu_date(
                get_first("AdmDateInserted"), get_first("AdmTimeInserted")
            ),
            "modified": emu_date(
                get_first("AdmDateModified"), get_first("AdmTimeModified")
            ),
            "scientificName": get_first("ClaScientificNameBuilt"),
            "currentScientificName": get_first("ClaCurrentSciNameLocal"),
            "taxonRank": get_first("ClaRank"),
            "kingdom": get_first("ClaKingdom"),
            "phylum": get_first("ClaPhylum"),
            "class": get_first("ClaClass"),
            "order": get_first("ClaOrder"),
            "suborder": get_first("ClaSuborder"),
            "superfamily": get_first("ClaSuperfamily"),
            "family": get_first("ClaFamily"),
            "subfamily": get_first("ClaSubfamily"),
            "genus": get_first("ClaGenus"),
            "subgenus": get_first("ClaSubgenus"),
            "specificEpithet": get_first("ClaSpecies"),
            "infraspecificEpithet": get_first("ClaSubspecies"),
        }
