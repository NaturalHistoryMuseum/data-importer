from uuid import uuid4

from dataimporter.emu.views.utils import (
    is_web_published,
    is_valid_guid,
    emu_date,
    translate_collection_code,
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
