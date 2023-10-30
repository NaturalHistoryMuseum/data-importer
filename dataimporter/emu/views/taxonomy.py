from dataimporter.emu.views.utils import NO_PUBLISH, is_web_published
from dataimporter.emu.views.utils import emu_date
from dataimporter.model import SourceRecord
from dataimporter.view import View, FilterResult, SUCCESS_RESULT


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
        get_all = record.get_all_values
        get_first = record.get_first_value

        return {
            "_id": record.id,
            "created": emu_date(
                get_first("AdmDateInserted"), get_first("AdmTimeInserted")
            ),
            "modified": emu_date(
                get_first("AdmDateModified"), get_first("AdmTimeModified")
            ),
            "scientificName": get_all("ClaScientificNameBuilt"),
            "currentScientificName": get_all("ClaCurrentSciNameLocal"),
            "taxonRank": get_all("ClaRank"),
            "kingdom": get_all("ClaKingdom"),
            "phylum": get_all("ClaPhylum"),
            "class": get_all("ClaClass"),
            "order": get_all("ClaOrder"),
            "suborder": get_all("ClaSuborder"),
            "superfamily": get_all("ClaSuperfamily"),
            "family": get_all("ClaFamily"),
            "subfamily": get_all("ClaSubfamily"),
            "genus": get_all("ClaGenus"),
            "subgenus": get_all("ClaSubgenus"),
            "specificEpithet": get_all("ClaSpecies"),
            "infraspecificEpithet": get_all("ClaSubspecies"),
        }
