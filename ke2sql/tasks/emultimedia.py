import luigi
from operator import is_not

from ke2sql.tasks.base import BaseTask


class EMultimediaTask(BaseTask):

    table = 'emultimedia'

    property_mappings = (
        # Record numbers
        ('GenDigitalMediaId', 'assetID'),
        ('MulTitle', 'title'),
        ('MulMimeFormat', 'mime'),
    )

    # Field level filters
    # FIXME: Do we want MIME type filters here?
    filters = {
        'GenDigitalMediaId': [
            (is_not, None),
            (is_not, 'Pending')
        ]
    }
