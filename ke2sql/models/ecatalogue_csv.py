import luigi
import luigi.postgres
import time
from psycopg2.extras import Json
import psycopg2
import logging
import datetime
import logging
import re
import tempfile
import json

from ke2sql.tasks.ecatalogue import ECatalogueTask
from ke2sql.models.catalogue import CatalogueModel

from ke2sql.lib.config import Config
from ke2sql.tasks.file import FileTask
from ke2sql.lib.parser import Parser

logger = logging.getLogger('luigi-interface')

class ECatalogueCSVTask(luigi.postgres.CopyToTable):
    date = luigi.IntParameter()
    limit = luigi.IntParameter(default=None)

    host = Config.get('database', 'host')
    database = Config.get('database', 'database')
    user = Config.get('database', 'username')
    password = Config.get('database', 'password')

    table = 'catalogue2'
    module = 'ecatalogue'
    model_cls = CatalogueModel

    columns = [
        ("irn", "PRIMARY KEY"),
        ("modified", "DATETIME NOT NULL"),
        ("created", "DATETIME NOT NULL"),
        ("deleted", "DATETIME"),
        ("record_type", "TEXT"),
        ("multimedia_irns", "ARRAY[INTEGER]"),  # Bit flaky
        ("multimedia_irn", "INTEGER REFERENCES products (product_no),"),
        ("properties", "JSONB")
    ]

    record_filters = {
        # List of record types (ColRecordType) to exclude from the import
        'excluded_types': [
            'Acquisition',
            'Bound Volume',
            'Bound Volume Page',
            'Collection Level Description',
            'DNA Card',  # 1 record, but keep an eye on this
            'Field Notebook',
            'Field Notebook (Double Page)',
            'Image',
            'Image (electronic)',
            'Image (non-digital)',
            'Image (digital)',
            'Incoming Loan',
            'L&A Catalogue',
            'Missing',
            'Object Entry',
            'object entry',  # FFS
            'Object entry',  # FFFS
            'PEG Specimen',
            'PEG Catalogue',
            'Preparation',
            'Rack File',
            'Tissue',  # Only 2 records. Watch.
            'Transient Lot'
        ],
        'excluded_statuses': [
            "DELETE",
            "DELETE-MERGED",
            "DUPLICATION",
            "Disposed of",
            "FROZEN ARK",
            "INVALID",
            "POSSIBLE TYPE",
            "PROBLEM",
            "Re-registered in error",
            "Reserved",
            "Retired",
            "Retired (see Notes)",
            "Retired (see Notes)Retired (see Notes)",
            "SCAN_cat",
            "See Notes",
            "Specimen missing - see notes",
            "Stub",
            "Stub Record",
            "Stub record"
        ],
        'collection_departments': [
            "Botany",
            "Entomology",
            "Mineralogy",
            "Palaeontology",
            "Zoology"
        ]
    }

    # Count total number of records (including skipped)
    record_count = 0
    # Count of records inserted / written to CSV
    insert_count = 0

    def __init__(self, *args, **kwargs):
        # Initiate a DB connection
        super(ECatalogueCSVTask, self).__init__(*args, **kwargs)
        self.model = self.model_cls()
        # Boolean denoting if this is the latest full export being run
        # If it is we can skip certain parts of the process - deleting records etc.,
        self.full_import = self.date == Config.getint('keemu', 'full_export_date')

    def requires(self):
        return FileTask(module_name=self.module, date=self.date)

    def rows(self):
        start_time = time.time()
        for record in Parser(self.input().path):
            self.record_count += 1
            if self._is_web_publishable(record) and self._is_valid_record(record):
                # multimedia = getattr(record, 'MulMultiMediaRef', None)
                # if multimedia:
                #     print(multimedia)

                self.insert_count += 1
                row = [
                    int(record.irn),
                    '',
                    '',
                    '',
                    'specimen',
                    json.dumps(record.to_dict(self.model.property_mappings)),
                ]
                yield row
            if self.limit and self.record_count >= self.limit:
                break
            if self.record_count % 1000 == 0:
                logger.debug('Record count: %d', self.record_count)

        logger.info('Inserted %d %s records in %d seconds', self.insert_count, self.module, time.time() - start_time)

    @staticmethod
    def _is_web_publishable(record):
        """
        Evaluate whether a record is importable
        At the very least a record will need AdmPublishWebNoPasswordFlag set to Y,
        Additional models will extend this to provide additional filters
        :param record:
        :return: boolean - false if not importable
        """
        if record.AdmPublishWebNoPasswordFlag.lower() != 'y':
            return False

        today_timestamp = time.time()
        embargo_dates = [
            getattr(record, 'NhmSecEmbargoDate', None),
            getattr(record, 'NhmSecEmbargoExtensionDate', None)
        ]
        for embargo_date in embargo_dates:
            if embargo_date:
                embargo_date_timestamp = time.mktime(datetime.datetime.strptime(embargo_date, "%Y-%m-%d").timetuple())
                if embargo_date_timestamp > today_timestamp:
                    return False

        return True

    def _is_valid_record(self, record):
        """
        Override base is_importable
        Add additional checks for:
            GUID - Must have GUID
            Record status - not on of the excluded record status
            Record type - not one of the excluded record types
            Collection department - one of the known departments
            record type
        :param record:
        :return: boolean - false if not importable
        """

        # Records must have a GUID
        if not getattr(record, 'AdmGUIDPreferredValue', None):
            return False

        # Does this record have an excluded status - Stub etc.,
        record_status = getattr(record, 'SecRecordStatus', None)
        if record_status in self.record_filters['excluded_statuses']:
            return False

        # Does this record have a record type that isn't excluded status - Stub etc.,
        record_type = getattr(record, 'ColRecordType', None)
        if not record_type or record_type in self.record_filters['excluded_types']:
            return False

        # Record must be in one of the known collection departments
        # (Otherwise the home page breaks)
        collection_department = getattr(record, 'ColDepartment', None)
        if collection_department not in self.record_filters['collection_departments']:
            return False

        return True

if __name__ == '__main__':
    luigi.run(main_task_cls=ECatalogueCSVTask)
