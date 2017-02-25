import os
import re
import abc
import gzip
import luigi
import time
import datetime
from psycopg2.extras import Json
from sqlalchemy.orm import sessionmaker

from ke2sql.lib import config
from ke2sql.lib.parser import Parser
from ke2sql.tasks.file_parser import FileTask
from ke2sql.lib import db


class BaseTask(luigi.Task):

    date = luigi.IntParameter()

    @abc.abstractproperty
    def model(self):
        return None

    @property
    def module(self):
        """
        Module - lower case class with model removed
        :return:
        """
        return self.__class__.__name__.lower().replace('task', '')

    def requires(self):
        return FileTask(module_name=self.module, date=self.date)

    def run(self):
        """
        Main run task - process the records
        """
        parser = Parser(self.input().path)
        for record in parser:
            print(record)
        # print(self.input())
        print('RUN')
        # for record in self.input():
        #     print('---------')


        # engine = db.get_engine()
        #
        # Session = sessionmaker(bind=engine)
        # session = Session()
        #
        #
        # for record in self._get_records():
        #     if self._is_importable(record):
        #         record_dict = {
        #             'irn': record.irn,
        #             'properties': Json(model.get_properties(record))
        #         }
        #         print(record_dict)


    def _is_importable(self, record):
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

