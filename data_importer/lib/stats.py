import math
from datetime import datetime as dt
from pprint import pformat

import abc
import requests

from data_importer.lib.config import Config, configparser


def get_milestones(cursor):
    return [
        SpecimenMilestone(cursor)
        ]


class BaseMilestone(object):
    name = ''

    try:
        log = Config.get('milestones', 'log')
    except configparser.NoOptionError:
        log = '/var/log/import-milestones.log'

    try:
        slackurl = Config.get('milestones', 'slack')
    except configparser.NoOptionError:
        slackurl = None

    def __init__(self, cursor):
        self.every = Config.getint('milestones', self.name)
        self.starting_records = self._get_database_record_count(cursor)
        self.matched_records = 0

    @property
    @abc.abstractmethod
    def query(self):
        """
        A query to count the number of relevant records already in the database.
        :return: string query
        """
        return ''

    def _get_database_record_count(self, cursor):
        """
        Get the number of relevant records currently in the database.
        :param cursor: a cursor connected to the database
        :return: int
        """
        cursor.execute(self.query)
        row_count = cursor.fetchone()[0]
        return row_count

    @abc.abstractmethod
    def match(self, record_dict):
        """
        Does this record match the criteria to be counted towards this milestone?
        :param record_dict: the record in dict form
        :return: boolean; True if it's relevant/matches, False if not
        """
        return True

    @property
    def next_milestone(self):
        """
        The next record count to aim for.
        :return: int
        """
        return math.ceil(self.current_count / self.every) * self.every

    @property
    def current_count(self):
        """
        The combined count of records already in the database at the start of processing
        and the number of records added since.
        :return: int
        """
        return self.starting_records + self.matched_records

    def log_item(self, record_dict):
        """
        Add a record to the log.
        :param record_dict: the record to be logged
        """
        entry = '{0}: {1} {2} (IRN: {3})'.format(
            dt.now().strftime('%Y-%m-%d:%H:%M:%S'),
            self.current_count,
            self.name,
            record_dict.get('irn', 'no IRN')
            )
        try:
            with open(self.log, 'a') as logfile:
                logfile.write(entry)
        except OSError:
            print(entry)

    def slack(self, record_dict):
        """
        Post the record to a slack channel.
        :param record_dict: the record to be posted
        """
        if self.slackurl is None:
            return
        try:
            entry = '{0}: {1} {2} (IRN: {3})'.format(
                dt.now().strftime('%Y-%m-%d:%H:%M:%S'),
                self.current_count,
                self.name,
                record_dict.get('irn', 'no IRN')
                )
            data = {
                'attachments': [
                    {
                        'fallback': entry,
                        'pretext': 'The data importer just reached {0} {1}!'.format(
                            self.current_count, self.name),
                        'color': '#616ad3',
                        'fields': [
                            {
                                'title': 'Record',
                                'value': '```\n' + pformat(record_dict,
                                                           depth=2) + '\n```',
                                'short': False
                                }
                            ]
                        }
                    ]
                }
            r = requests.post(self.slackurl, json=data)
            if not r.ok:
                raise requests.ConnectionError
        except requests.ConnectionError:
            print('Could not post to slack.')

    def check(self, record_dict):
        """
        Check if the current record meets the criteria and logs/notifies if it does.
        :param record_dict: the record
        """
        if self.match(record_dict):
            self.matched_records += 1
            if self.current_count == self.next_milestone:
                self.log_item(record_dict)
                self.slack(record_dict)


class SpecimenMilestone(BaseMilestone):
    name = 'specimens'

    def __init__(self, *args, **kwargs):
        from data_importer.tasks import specimen_record_types
        self.record_types = specimen_record_types
        super(SpecimenMilestone, self).__init__(*args, **kwargs)

    def match(self, record_dict):
        return record_dict.get('record_type', None) in self.record_types

    @property
    def query(self):
        record_types = "','".join(self.record_types)
        return "select count(*) from ecatalogue where record_type in ('{0}')".format(
            record_types)
