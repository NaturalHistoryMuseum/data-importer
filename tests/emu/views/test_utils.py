from uuid import uuid4

from dataimporter.emu.views.utils import (
    is_web_published,
    is_valid_guid,
    emu_date,
    translate_collection_code,
    combine_text,
    orientation_requires_swap,
)
from dataimporter.lib.model import SourceRecord


def create_record(**data) -> SourceRecord:
    return SourceRecord(str(uuid4()), data, "test")


def test_is_web_published():
    # yes
    assert is_web_published(create_record(AdmPublishWebNoPasswordFlag="y"))
    assert is_web_published(create_record(AdmPublishWebNoPasswordFlag="Y"))
    # no
    assert not is_web_published(create_record(AdmPublishWebNoPasswordFlag="n"))
    assert not is_web_published(create_record(AdmPublishWebNoPasswordFlag="N"))
    assert not is_web_published(create_record(AdmPublishWebNoPasswordFlag=""))
    assert not is_web_published(create_record(publish="yes"))


def test_is_valid_guid():
    assert is_valid_guid(create_record(AdmGUIDPreferredValue=str(uuid4())))
    assert not is_valid_guid(create_record(AdmGUIDPreferredValue="nope!"))


def test_emu_date():
    assert emu_date("", "") is None
    assert emu_date("2005-07-02", "") is None
    assert emu_date("", "16:52:23.000") is None
    assert emu_date("2005-40-40", "290:12:23.000") is None
    assert emu_date("2005-07-02", "16:52:23.000") == "2005-07-02T16:52:23+00:00"


def test_translate_collection_code():
    assert translate_collection_code("Botany") == "BOT"
    assert translate_collection_code("Entomology") == "BMNH(E)"
    assert translate_collection_code("Mineralogy") == "MIN"
    assert translate_collection_code("Palaeontology") == "PAL"
    assert translate_collection_code("Zoology") == "ZOO"

    # not a department we have
    assert translate_collection_code("banana") is None
    # case sensitive
    assert translate_collection_code("zoology") is None


def test_combine_text():
    assert combine_text(["line 1", "", "line 3"]) == "line 1\nline 3"
    assert combine_text(["line 1\n", "\n\n\n", "line 3\n\n"]) == "line 1\nline 3"
    assert combine_text(["", "line 1", "\n", "\n", "line 3", ""]) == "line 1\nline 3"
    assert combine_text(["line\n1", "line\n2"]) == "line\n1\nline\n2"


class TestOrientationRequiresSwap:
    def test_no_tags(self):
        record = SourceRecord("1", {}, "test")
        assert not orientation_requires_swap(record)

    def test_with_tag_no_swap_number(self):
        data = {
            "ExiTag": ("284", "274", "266"),
            "ExiValue": (
                "Chunky",
                "1",
                "2",
            ),
        }
        record = SourceRecord("1", data, "test")
        assert not orientation_requires_swap(record)

    def test_with_tag_no_swap_text(self):
        data = {
            "ExiTag": ("284", "274", "266"),
            "ExiValue": (
                "Chunky",
                "Rotate 180",
                "2",
            ),
        }
        record = SourceRecord("1", data, "test")
        assert not orientation_requires_swap(record)

    def test_with_tag_swap_number(self):
        data = {
            "ExiTag": ("284", "274", "266"),
            "ExiValue": (
                "Chunky",
                "6",
                "2",
            ),
        }
        record = SourceRecord("1", data, "test")
        assert orientation_requires_swap(record)

    def test_with_tag_swap_text(self):
        data = {
            "ExiTag": ("284", "274", "266"),
            "ExiValue": (
                "Chunky",
                "Rotate 270 CW",
                "2",
            ),
        }
        record = SourceRecord("1", data, "test")
        assert orientation_requires_swap(record)

    def test_with_tag_swap_text_wrong_case(self):
        data = {
            "ExiTag": ("284", "274", "266"),
            "ExiValue": (
                "Chunky",
                "rOtAte 270 cW",
                "2",
            ),
        }
        record = SourceRecord("1", data, "test")
        assert orientation_requires_swap(record)

    def test_with_tag_swap_text_and_empty_values(self):
        # this tests ensures that empty exif values aren't being skipped. Basically, if
        # record.get_all_values is used then clean=False must be passed otherwise the
        # alignment of the tag tuple with the value tuple fails
        data = {
            "ExiTag": ("284", "271", "274", "266"),
            "ExiValue": (
                "Chunky",
                # this value is empty!
                "",
                "Rotate 270 CW",
                "2",
            ),
        }
        record = SourceRecord("1", data, "test")
        assert orientation_requires_swap(record)
