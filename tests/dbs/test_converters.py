import pytest

from dataimporter.dbs.converters import (
    str_to_bytes,
    int_to_bytes,
    MAX_INT,
    str_to_int,
    int_to_str,
    bytes_to_str,
    bytes_to_int,
)


def test_str_to_bytes():
    assert str_to_bytes("banana") == b"banana"
    assert str_to_bytes("😀") == b"\xf0\x9f\x98\x80"


def test_str_to_int():
    assert str_to_int("2_10") == 10
    assert str_to_int("1_0") == 0
    assert str_to_int(f"~_{MAX_INT}") == MAX_INT


def test_int_to_bytes():
    with pytest.raises(ValueError):
        assert int_to_bytes(20.5)

    with pytest.raises(ValueError):
        assert int_to_bytes(20.0)

    with pytest.raises(ValueError):
        assert int_to_bytes(-1)

    with pytest.raises(ValueError):
        assert int_to_bytes(MAX_INT + 1)

    assert int_to_bytes(10) == b"2_10"
    assert int_to_bytes(0) == b"1_0"
    assert int_to_bytes(MAX_INT - 1) == f"~_{MAX_INT - 1}".encode("utf-8")


def test_int_to_str():
    with pytest.raises(ValueError):
        assert int_to_str(20.5)

    with pytest.raises(ValueError):
        assert int_to_str(20.0)

    with pytest.raises(ValueError):
        assert int_to_str(-1)

    with pytest.raises(ValueError):
        assert int_to_str(MAX_INT + 1)

    assert int_to_str(10) == "2_10"
    assert int_to_str(0) == "1_0"
    assert int_to_str(MAX_INT - 1) == f"~_{MAX_INT - 1}"


def test_bytes_to_str():
    assert bytes_to_str(b"banana") == "banana"
    assert bytes_to_str(b"\xf0\x9f\x98\x80") == "😀"


def test_bytes_to_int():
    assert bytes_to_int(b"2_10") == 10
    assert bytes_to_int(b"1_0") == 0
    assert bytes_to_int(f"~_{MAX_INT}".encode("utf-8")) == MAX_INT
