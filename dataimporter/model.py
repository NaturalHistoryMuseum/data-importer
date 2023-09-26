from dataclasses import dataclass
from typing import Union, Tuple, Iterable, Optional, Dict, Any

Data = Dict[str, Union[str, Tuple[str, ...]]]


@dataclass
class SourceRecord:
    """
    A class representing a record from a source.
    """

    id: str
    data: Data
    # the name of the source of this record data
    source: str

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

    def __eq__(self, other: Any) -> bool:
        """
        Compares any object to this record to determine if they are the same. If the
        other object passed is a SourceRecord type object then the id and data fields
        will be compared and if they are both the same as this record then True is
        returned, otherwise False.

        If the other is not a SourceRecord, NotImplemented is returned.

        The source is completely ignored as it isn't important in comparisons (we don't
        care where something came from, it's the ID and data that matter).

        :param other: the other object
        :return: True if they're the same, False if not, or NotImplemented if the other
                 object isn't a SourceRecord
        """
        if isinstance(other, SourceRecord):
            return self.id == other.id and self.data == other.data
        return NotImplemented

    def iter_all_values(self, *fields: str, clean: bool = True) -> Iterable[str]:
        """
        Retrieves all the values from the given fields and yield them one by one. If
        there are no values available, nothing is yielded. If there are values, each one
        is yielded on its own. This means each value yielded will be a str, tuples are
        yielded element by element. For example, with the data dict:

            {
              "x": ("a","b","c"),
              "y": "d",
              "z": ("e", "f")
            }

        calling iter_all_values("x", "y", "z") yields "a", "b", "c", "d", "e", and "f".

        If the clean parameter is True (the default), then empty strings are removed. If
        it is False then they are yielded.

        :param fields: the fields to extract values from
        :param clean: whether to remove empty strings (default: True)
        :return: yields str values in the provided field order
        """
        for field in fields:
            value = self.data.get(field)
            if value is not None:
                if isinstance(value, tuple):
                    if clean:
                        yield from filter(None, value)
                    else:
                        yield from value
                else:
                    if not clean or value:
                        yield value

    def get_all_values(
        self, *fields: str, clean: bool = True
    ) -> Union[None, str, Tuple[str]]:
        """
        Retrieves all the values from the given fields and returns them. If there are no
        values available, None is returned. If there is only one value (even if it's a
        1-tuple) then the value is returned on its own. If there is more than one value,
        a tuple of all the values is returned. This means that with the data dict:

            {
              "x": ("a","b","c"),
              "y": "d",
              "z": ("e", "f")
            }

        calling get_all_values("x", "y", "z") returns ("a", "b", "c", "d", "e", "f").

        :param fields: the fields to extract the values from
        :param clean: whether to remove empty strings (default: True)
        :return: None if no values, a str if there's only one value, otherwise, a tuple
                 of str containing all the values, in provided field order.
        """
        values = tuple(self.iter_all_values(*fields, clean=clean))
        if len(values) == 0:
            return None
        elif len(values) == 1:
            return values[0]
        else:
            return values

    def get_first_value(
        self, *fields: str, clean: bool = True, default: Any = None
    ) -> Optional[str]:
        """
        Retrieves the first value present in the given fields. If the first field with a
        value in contains a tuple not a str, then the first element of the tuple is
        returned, not the tuple itself. The fields are iterated over in the order they
        are provided until a value is found.

        :param fields: the fields to extract a value from
        :param clean: whether to remove empty strings (default: True)
        :param default: the value to return if no values are found for the fields given
                        (default: None)
        :return: the first value from the given fields, or the default
        """
        return next(iter(self.iter_all_values(*fields, clean=clean)), default)
