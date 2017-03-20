import luigi
from operator import is_not, ne

from ke2sql.tasks.keemu.base import KeemuBaseMixin


class KeemuEMultimediaMixin(KeemuBaseMixin):

    module = 'emultimedia'

    # All datasets use the same multimedia fields, so there is no need to build these
    fields = (
        # Record numbers
        ('GenDigitalMediaId', 'assetID'),
        ('MulTitle', 'title'),
        ('MulMimeFormat', 'mime'),
        ('MulCreator', 'creator'),
    )

    # Field level filters - MIME type is not required;
    # Non-images will not have a MAM asset identifier
    filters = {
        # MAM Asset Identifier
        'GenDigitalMediaId': [
            (is_not, None),
            (ne, 'Pending')
        ]
    }
