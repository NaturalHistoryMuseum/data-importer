from base64 import b64encode
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
import responses
from responses import matchers

from dataimporter.dbs import DataDB
from dataimporter.ext.gbif import (
    GBIFView,
    get_download_url,
    GBIFDownloadTimeout,
    GBIFDownloadError,
    get_changed_records,
    request_download,
)
from dataimporter.model import SourceRecord
from tests.helpers.samples.gbif import (
    SAMPLE_GBIF_RECORD_ID,
    SAMPLE_GBIF_RECORD_DATA,
    SAMPLE_GBIF_ZIP_BYTES,
    SAMPLE_GBIF_RECORDS,
)


class TestGBIFView:
    def test_transform(self, tmp_path: Path):
        gbif_view = GBIFView(tmp_path / "gbif", DataDB(tmp_path / "gbif_data"))
        gbif_record = SourceRecord(
            SAMPLE_GBIF_RECORD_ID, SAMPLE_GBIF_RECORD_DATA, "gbif"
        )
        data = gbif_view.transform(gbif_record)
        assert data == {
            "gbifID": SAMPLE_GBIF_RECORD_ID,
            "gbifIssue": (
                "GEODETIC_DATUM_ASSUMED_WGS84",
                "INSTITUTION_MATCH_FUZZY",
                "COLLECTION_MATCH_NONE",
                "MULTIMEDIA_URI_INVALID",
            ),
        }

    def test_transform_no_issues(self, tmp_path: Path):
        gbif_view = GBIFView(tmp_path / "gbif", DataDB(tmp_path / "gbif_data"))
        record_data = deepcopy(SAMPLE_GBIF_RECORD_DATA)
        record_data["issue"] = ""
        gbif_record = SourceRecord(SAMPLE_GBIF_RECORD_ID, record_data, "gbif")
        data = gbif_view.transform(gbif_record)
        assert data == {"gbifID": SAMPLE_GBIF_RECORD_ID}


class TestGetDownloadURL:
    @responses.activate
    def test_success(self):
        download_id = "test_download_id"
        download_link = "test_download_link"
        responses.get(
            f"https://api.gbif.org/v1/occurrence/download/{download_id}",
            json={"status": "SUCCEEDED", "downloadLink": download_link},
        )

        assert get_download_url(download_id) == download_link

    @responses.activate
    def test_timeout(self):
        download_id = "test_download_id"
        responses.get(
            f"https://api.gbif.org/v1/occurrence/download/{download_id}",
            json={"status": "PENDING"},
        )

        with patch("time.sleep") as mock_sleep:
            with pytest.raises(GBIFDownloadTimeout):
                get_download_url(download_id)
            assert mock_sleep.called

    @responses.activate
    def test_pending_to_succeeded(self):
        download_id = "test_download_id"
        download_link = "test_download_link"
        gbif_api_url = f"https://api.gbif.org/v1/occurrence/download/{download_id}"

        # respond with pending twice, then success
        responses.get(gbif_api_url, json={"status": "PENDING"})
        responses.get(gbif_api_url, json={"status": "PENDING"})
        responses.get(
            gbif_api_url, json={"status": "SUCCEEDED", "downloadLink": download_link}
        )
        with patch("time.sleep") as mock_sleep:
            assert get_download_url(download_id) == download_link
            assert mock_sleep.call_count == 2

    @responses.activate
    def test_pending_to_failures(self):
        download_id = "test_download_id"
        gbif_api_url = f"https://api.gbif.org/v1/occurrence/download/{download_id}"

        # respond with pending then failed
        responses.get(gbif_api_url, json={"status": "PENDING"})
        responses.get(gbif_api_url, json={"status": "FAILED"})
        with pytest.raises(GBIFDownloadError, match="FAILED"):
            with patch("time.sleep") as mock_sleep:
                get_download_url(download_id)
            assert mock_sleep.call_count == 1


@responses.activate
@patch("dataimporter.ext.gbif.request_download")
@patch("dataimporter.ext.gbif.get_download_url")
def test_changed_records(
    get_download_url_mock: MagicMock, request_download_mock: MagicMock, tmp_path: Path
):
    request_download_mock.return_value = "test_download_id"
    get_download_url_mock.return_value = "https://gbif.org/test_download_url"

    def download_request_callback(request):
        return 200, {"content-type": "application/octet-stream"}, SAMPLE_GBIF_ZIP_BYTES

    responses.add_callback(
        responses.GET,
        get_download_url_mock.return_value,
        callback=download_request_callback,
    )

    # create a clean db to which we will add 2 dummy records so that there is something
    # in the db for the function under test to check the new records against.
    gbif_db = DataDB(tmp_path / "gbif_data")

    # grab the first two records from the download that we have loaded as sample data
    samples = list(SAMPLE_GBIF_RECORDS.items())[:2]

    gbif_db.put_many(
        [
            # firstly, add a record with a valid ID from the download and the same data
            SourceRecord(samples[0][0], samples[0][1], "test"),
            # secondly, add a record with a valid ID from the download, but different
            # data
            SourceRecord(samples[1][0], {"not": "good", "data": "nope!"}, "test"),
        ]
    )
    assert gbif_db.size() == 2

    # now add all the records
    records = list(get_changed_records(gbif_db, "gbif_username", "gbif_password"))
    # one record should have been ignored because the data hasn't changed, 8 are new,
    # and 1 is an update
    assert len(records) == 9
    gbif_db.put_many(records)
    assert gbif_db.size() == 10
    assert gbif_db.get_record(samples[1][0]).data == samples[1][1]


@responses.activate
def test_request_download():
    gbif_username = "username"
    gbif_password = "password"
    basic_auth = f"{gbif_username}:{gbif_password}"
    mock_download_id = "this is a download id"

    responses.post(
        "https://api.gbif.org/v1/occurrence/download/request",
        body=mock_download_id,
        match=(
            # check that the request is authed
            matchers.header_matcher(
                {"Authorization": f"Basic {b64encode(basic_auth.encode()).decode()}"}
            ),
            matchers.json_params_matcher(
                {
                    # we don't need to check the entire json body, just make sure these
                    # are all in there
                    "creator": gbif_username,
                    "format": "SIMPLE_CSV",
                    "predicate": {
                        "type": "equals",
                        "key": "DATASET_KEY",
                        "value": "7e380070-f762-11e1-a439-00145eb45e9a",
                    },
                },
                strict_match=False,
            ),
        ),
    )

    download_id = request_download(gbif_username, gbif_password)

    assert download_id == mock_download_id
