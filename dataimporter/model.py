from dataclasses import dataclass
from typing import Dict, Union, Tuple


@dataclass
class VersionedRecord:
    """
    A class representing a record with a version.
    """

    id: int
    version: int
    data: Dict[str, Union[str, Tuple[str]]]

    @property
    def is_deleted(self) -> bool:
        """
        Returns True if the record's data represents a deletion, False if not.

        :return: True if the record's data represents a deletion, False if not.
        """
        return not bool(self.data)

    def __contains__(self, field: str) -> bool:
        """
        Checks if the given field is present in this record's data.

        :param field: the field name
        :return: True if the field exists, False if not
        """
        return field in self.data
