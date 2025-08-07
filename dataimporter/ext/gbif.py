import csv
import tempfile
import time
from dataclasses import dataclass
from io import TextIOWrapper
from typing import Dict, Iterable
from zipfile import ZipFile

import requests
from requests.auth import HTTPBasicAuth

from dataimporter.lib.dbs import Store
from dataimporter.lib.model import SourceRecord
from dataimporter.lib.view import View


class GBIFView(View):
    """
    View for GBIF records.
    """

    def transform(self, record: SourceRecord) -> dict:
        """
        Converts the GBIF record's raw data to a dict which will then be embedded in
        specimen records and presented on the Data Portal.

        :param record: the record to project
        :return: a dict containing the data for this record that should be combined with
            a specimen record
        """
        data = {
            'gbifID': record.id,
        }
        issue_value = record.get_first_value('issue', default='').strip()
        if issue_value:
            # make a tuple and remove any empty values (in case of formatting weirds)
            data['gbifIssue'] = tuple(
                issue for issue in issue_value.split(';') if issue
            )
        return data


def get_changed_records(
    store: Store, gbif_username: str, gbif_password: str
) -> Iterable[SourceRecord]:
    """
    Get a stream of the latest records from GBIF. This function will take time to
    complete as it will request a new download of the NHM's specimen dataset on GBIF,
    download it, and then stream the records from the downloaded CSV that have changed
    compared to the ones in the data DB already.

    :param store: the GBIF Store
    :param gbif_username: a GBIF username for requesting the download
    :param gbif_password: a GBIF password for requesting the download
    :return: yields the changed SourceRecord objects
    """
    download_id = request_download(gbif_username, gbif_password)
    download = get_download_url(download_id)
    with requests.get(download.url, stream=True) as dl_r:
        with tempfile.NamedTemporaryFile() as tmp_file:
            # download the file to disk
            for chunk in dl_r.iter_content(chunk_size=4096):
                tmp_file.write(chunk)
            # close the http connection to gbif now that we're done downloading the file
            dl_r.close()
            # rewind so that we can read the file
            tmp_file.seek(0)
            # open the zip, read the occurrence file
            with ZipFile(tmp_file) as zip_file:
                with zip_file.open(f'{download_id}.csv') as raw_csv_file:
                    with TextIOWrapper(raw_csv_file, encoding='utf-8') as csv_file:
                        reader: Iterable[Dict[str, str]] = csv.DictReader(
                            csv_file, dialect='excel-tab', quoting=csv.QUOTE_NONE
                        )
                        for row in reader:
                            gbif_id = row['gbifID']
                            updated_record = SourceRecord(gbif_id, row, download_id)
                            existing_record = store.get_record(gbif_id)
                            # if the record has changed or is new, yield it
                            if existing_record != updated_record:
                                yield updated_record


def request_download(gbif_username: str, gbif_password: str) -> str:
    """
    Request a download of the NHM's specimen dataset from GBIF. To request a download we
    need to be authenticated with GBIF, hence the username and password parameters.

    :param gbif_username: GBIF account username
    :param gbif_password: GBIF account password
    :return: the GBIF download ID
    """
    download_filter = {
        'creator': gbif_username,
        'notificationAddresses': [],
        'sendNotification': False,
        'format': 'SIMPLE_CSV',
        'predicate': {
            'type': 'equals',
            'key': 'DATASET_KEY',
            # this is the NHM's specimen collection GBIF dataset key
            'value': '7e380070-f762-11e1-a439-00145eb45e9a',
            'matchCase': False,
        },
    }
    auth = HTTPBasicAuth(gbif_username, gbif_password)
    # request a new download
    response = requests.post(
        'https://api.gbif.org/v1/occurrence/download/request',
        json=download_filter,
        auth=auth,
    )
    return response.text


class GBIFDownloadTimeout(Exception):
    """
    If we time out when trying to access the GBIF download file (i.e. we wait for GBIF
    to generate the download, but it takes too long) this exception is raised.
    """

    def __init__(self, timeout: int, status: str):
        super().__init__(
            f'GBIF download link not available within timeout ({timeout} seconds, last '
            f'known status: {status}).'
        )
        self.timeout = timeout
        self.status = status


class GBIFDownloadError(Exception):
    """
    If we get a failed or killed status from GBIF when checking the download status we
    throw this exception to avoid waiting an hour for a download that is never going to
    succeed.
    """

    def __init__(self, status: str):
        super().__init__(
            f'GBIF download link not available due to error, status: {status}'
        )
        self.status = status


@dataclass
class GBIFDownload:
    """
    Represents a GBIF download URL and some associated metadata.
    """

    # this is the URL of the zip itself
    url: str
    # the size of the zip in bytes according to GBIF's API
    zip_size: int
    # the number of records contained in the zip according to GBIF's API
    records: int


def get_download_url(download_id: str) -> GBIFDownload:
    """
    Wait for the given download to be ready and then return the URL to download the
    file.

    This function tries every minute for an hour, checking the download status via the
    GBIF API. Once the status changes to "SUCCEEDED" the download URL is returned. If
    the status doesn't change to "SUCCEEDED" within the hour then a GBIFDownloadTimeout
    exception is raised. If a failed statuses appear, the function raises a
    GBIFDownloadError exception.

    :param download_id: the GBIF download ID
    :return: a GBIFDownload object
    :raise: GBIFDownloadTimeout if the download is not ready within the timeout
    """
    backoff_in_seconds = 60
    max_tries = 60
    url = f'https://api.gbif.org/v1/occurrence/download/{download_id}'
    status = 'PREPARING'

    for _ in range(max_tries):
        with requests.get(url) as r:
            download_info = r.json()
            status = download_info['status']
            if status == 'SUCCEEDED':
                return GBIFDownload(
                    download_info['downloadLink'],
                    download_info['size'],
                    download_info['totalRecords'],
                )
            elif status == 'FAILED':
                raise GBIFDownloadError(status)
            else:
                time.sleep(backoff_in_seconds)

    raise GBIFDownloadTimeout(max_tries * backoff_in_seconds, status)
