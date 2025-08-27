from itertools import zip_longest

from fastnumbers import try_int

from dataimporter.emu.views.utils import (
    INVALID_GUID,
    NO_PUBLISH,
    is_valid_guid,
    is_web_published,
    orientation_requires_swap,
)
from dataimporter.lib.model import SourceRecord
from dataimporter.lib.view import SUCCESS_RESULT, FilterResult, View, strip_empty

MULTIMEDIA_NOT_IMAGE = FilterResult(False, 'Multimedia not an image')
MULTIMEDIA_NO_IDENTIFIER = FilterResult(False, 'Image had no identifier')


class MSSView(View):
    """
    View for MSS records.

    This view populates the MSS index which is used by the IIIF servers but not exposed
    on the Data Portal directly.
    """

    def is_member(self, record: SourceRecord) -> FilterResult:
        """
        Filters the given record, determining whether it is an MSS record or not.

        :param record: the record to filter
        :return: a FilterResult object
        """
        if record.get_first_value('MulMimeType') != 'image':
            return MULTIMEDIA_NOT_IMAGE

        if not record.get_first_value('DocIdentifier'):
            return MULTIMEDIA_NO_IDENTIFIER

        return SUCCESS_RESULT

    def is_publishable(self, record: SourceRecord) -> FilterResult:
        """
        Filters the given record, determining whether it matches the publishing rules
        for MSS records.

        :param record: the record to filter
        :return: a FilterResult object
        """
        if not is_valid_guid(record):
            return INVALID_GUID

        if not is_web_published(record):
            return NO_PUBLISH

        return SUCCESS_RESULT

    @strip_empty
    def transform(self, record: SourceRecord) -> dict:
        """
        Converts the record's raw data to a dict which will be stored in the MSS index.

        :param record: the record to project
        :return: a dict containing the data for this record that should be stored in the
            MSS index
        """
        # get all the doc identifiers as a tuple (would use get_all_values here but that
        # returns just a str if there's only one)
        identifiers = tuple(record.iter_all_values('DocIdentifier'))

        data = {
            'id': record.id,
            'mime': record.get_first_value('MulMimeFormat'),
            'guid': record.get_first_value('AdmGUIDPreferredValue'),
            # there will be 1+ ids due to the check we do in the is_member method so
            # this is safe
            'file': identifiers[0],
        }

        # add old MAM asset IDs if found
        old_asset_id = record.get_first_value('GenDigitalMediaId')
        if old_asset_id and old_asset_id != 'Pending':
            data['old_asset_id'] = old_asset_id

        # store a bool indicating whether the widths and heights of the main image and
        # derivatives need to be swapped due to the orientation tag on the image record
        swap = orientation_requires_swap(record)

        # grab the widths and heights of the original and all the derivatives
        widths = tuple(record.iter_all_values('DocWidth'))
        heights = tuple(record.iter_all_values('DocHeight'))

        # could be 0+ widths and heights, so we need to do this in a way that avoids
        # errors
        original_width = try_int(next(iter(widths), ''), on_fail=None)
        original_height = try_int(next(iter(heights), ''), on_fail=None)
        if original_width is not None or original_height is not None:
            # set the width and height of the original image at the root of the data
            # dict
            data['width'] = original_width if not swap else original_height
            data['height'] = original_height if not swap else original_width

        derivatives = []
        for identifier, width, height in zip_longest(
            identifiers[1:], widths[1:], heights[1:], fillvalue=''
        ):
            width = try_int(width, on_fail=None)
            height = try_int(height, on_fail=None)
            # ignore the triple if we don't have all of these values
            if identifier and width is not None and height is not None:
                derivatives.append(
                    {
                        'file': identifier,
                        'width': width if not swap else height,
                        'height': height if not swap else width,
                    }
                )

        if len(derivatives) > 1:
            # sort in ascending size order
            data['derivatives'] = sorted(
                derivatives, key=lambda d: (d['width'], d['height'])
            )

        return data
