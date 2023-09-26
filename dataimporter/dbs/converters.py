"""
This module contains a set of conversion functions for storing data in LevelDB.
Specifically, the functions in this module convert strings, bytes, and integers between
each of themselves. Bytes and strings are handled using UTF-8, whereas integers require
a special encoding to ensure we can sort them using byte-wise comparisons (as this is
the most efficient sorting available in plyvel). See the conversion functions that take
or return integers for the specific details of the encoding used.
"""

from fastnumbers import check_int

# the maximum integer we can represent as a sortable string is 78 digits
MAX_INT = int("9" * 78)


def str_to_bytes(string: str) -> bytes:
    """
    Converts a string to UTF-8 encoded bytes. This is just a convenience function.

    :param string: a string
    :return: some bytes
    """
    return string.encode("utf-8")


def str_to_int(string: str) -> int:
    """
    Converts an encoded string to an int. This simply strips the first two characters
    (the encoded length and the splitting _) and converts the remaining string into an
    int.

    :param string: the encoded positive integer as a string
    :return: an int
    """
    return int(string[2:])


def int_to_bytes(number: int) -> bytes:
    """
    Encodes the given number and converts the resulting string to bytes.

    Encoding the number is necessary so that it can be sorted using byte-wise
    comparisons in leveldb. This fixes the standard 1, 2, 20, 21, 3 problem without
    using zero padding which wastes space and requires a much lower maximum input value.
    The algorithm used is based on the one presented here:
    https://www.arangodb.com/2017/09/sorting-number-strings-numerically/ with a couple
    of tweaks.

    Essentially, we encode the length of the number before the number itself. This
    allows sorting to be done properly as the ASCII character is assigned using the
    number of digits in the number. For example, the number 1 gets the character 1 so is
    encoded as "1_1", whereas 10 gets the character 2 and is encoded "2_10". Because we
    are restricted to not use . in keys and for low number convenience, we start at
    character point 49 which is the character 1 and therefore all numbers less than
    1,000,000,000 are encoded with the numbers 1 to 9 which is convenient for users.

    This encoding structure can support a number with a maximum length of 78 digits
    (ASCII char 1 (49) to ~ (126)).

    This function only works on positive integers. If the input isn't valid, a
    ValueError is raised.

    :param number: the number to encode, must be positive
    :return: the encoded number as a bytes object
    """
    if not check_int(number):
        raise ValueError("Number must be a valid integer")
    if number < 0 or number > MAX_INT:
        raise ValueError(f"Number must be positive and no more than {MAX_INT}")
    # this could call int_to_str but for speed, don't bother. Not very DRY but meh,
    # we need the performance!
    return f"{chr(48 + len(str(number)))}_{number}".encode("utf-8")


def int_to_str(number: int) -> str:
    """
    Encodes the given number and returns the resulting string. See int_to_bytes for
    details.

    :param number: the number to encode, must be positive
    :return: the encoded number as a str object
    """
    if not check_int(number):
        raise ValueError("Number must be a valid integer")
    if number < 0 or number > MAX_INT:
        raise ValueError(f"Number must be positive and no more than {MAX_INT}")
    return f"{chr(48 + len(str(number)))}_{number}"


def bytes_to_str(some_bytes: bytes) -> str:
    """
    Converts a string to UTF-8 encoded bytes. This is just a convenience function.

    :param some_bytes: a bytes object
    :return: a string
    """
    return some_bytes.decode("utf-8")


def bytes_to_int(some_bytes: bytes) -> int:
    """
    Converts the encoded bytes to an int. This simply strips the first two characters
    (the encoded length and the splitting _) and converts the remaining string into an
    int. See int_to_bytes for details about the encoding process.

    :param some_bytes: the encoded positive integer as bytes
    :return: an int
    """
    return int(some_bytes.decode("utf-8")[2:])
