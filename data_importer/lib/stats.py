import math
from datetime import datetime as dt

import abc
import psycopg2
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
        try:
            cursor.execute(self.query)
            row_count = cursor.fetchone()[0]
        except psycopg2.ProgrammingError as e:
            if e.pgcode == psycopg2.errorcodes.UNDEFINED_TABLE:
                # the table we're attempting to count on doesn't exist yet, this just means this is the first import for
                # this table and therefore we should return a count of 0
                row_count = 0
                # also rollback the connection otherwise any subsequent sql will fail. This is safe to do at this point
                # without loss of any upserted records because this record count function is only called during
                # initialisation, before any data has even been read from the keemu dumps
                cursor.connection.rollback()
            else:
                # if it's not a undefined table issue then re-raise it
                raise e
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

    def _log_entry(self, record_dict):
        entry = '{0}: {1} {2} (IRN: {3}, GUID: {4})'.format(
            dt.now().strftime('%Y-%m-%d:%H:%M:%S'),
            self.current_count,
            self.name,
            record_dict.get('irn', 'no IRN'),
            record_dict.get('guid', 'no GUID')
            )
        return entry

    def log_item(self, record_dict):
        """
        Add a record to the log.
        :param record_dict: the record to be logged
        """
        entry = self._log_entry(record_dict)
        try:
            with open(self.log, 'a') as logfile:
                logfile.write(entry)
        except OSError:
            print(entry)

    def _slack_msg(self, record_dict):
        """
        Generate the slack message.
        :param record_dict: the record to be posted about
        :return:
        """
        entry = self._log_entry(record_dict)
        data = {
            'attachments': [
                {
                    'fallback': entry,
                    'pretext': 'The data importer just reached {0} {1}!'.format(
                        self.current_count, self.name),
                    'title': record_dict.get('irn', 'no IRN')
                    }
                ]
            }
        return data

    def slack(self, record_dict):
        """
        Post the record to a slack channel.
        :param record_dict: the record to be posted
        """
        if self.slackurl is None:
            return
        try:
            data = self._slack_msg(record_dict)
            r = requests.post(self.slackurl, json=data)
            if not r.ok:
                raise requests.ConnectionError(request=r.request, response=r)
        except requests.ConnectionError as e:
            try:
                with open(self.log + '.errors', 'a') as logfile:
                    logfile.write(e.response)
            except OSError:
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
        self.record_types = [i for i in specimen_record_types if 'Group Parent' not in i]
        super(SpecimenMilestone, self).__init__(*args, **kwargs)

    def match(self, record_dict):
        record_type = record_dict.get('record_type', None) in self.record_types
        embargoed = record_dict.get('embargo_date', None) is not None
        if embargoed:
            try:
                embargo_date = dt.strptime(record_dict.get('embargo_date'), '%Y%m%d')
            except:
                embargo_date = dt.now()
        else:
            embargo_date = dt.now()
        embargo_passed = embargo_date <= dt.now()
        return record_type and (not embargoed or embargo_passed)

    @staticmethod
    def _emoji(record_properties):
        """
        Find an emoji representing this record.
        :param record_properties: the record's properties (as a dict)
        :return: an emoji string representation (surrounded by colons)
        """
        emoji_dict = {
            'BMNH(E)': {
                None: ':bug:'
                },
            'BOT': {
                None: ':deciduous_tree:',
                'bryophytes': ':golf:',
                'diatoms': ':eight_pointed_black_star:',
                'flowering plants': ':blossom:',
                'seed plants: brit & irish': ':seedling:'
                },
            'MIN': {
                None: ':gem:'
                },
            'PAL': {
                None: ':t-rex:',
                'vertebrates': ':sauropod:',
                'invertebrates': ':trilobite:',
                'palaeobotany': ':fallen_leaf:'
                },
            'ZOO': {
                None: ':penguin:',
                'annelida': ':wavy_dash:',
                'aves': ':bird:',
                'cnidaria': ':space_invader:',
                'crustacea': ':crab:',
                'echinodermata': ':star:',
                'mammalia': ':monkey_face:',
                'mollusca': ':snail:',
                'pisces': ':fish:',
                'porifera': ':cloud:',
                'reptiles & amphibians': ':snake:'
                },
            None: {
                None: ':darwin:'
                },
            }

        coll = record_properties.get('collectionCode', None).upper()
        sub_dep = record_properties.get('subDepartment', None).lower()
        coll_emojis = emoji_dict.get(coll, emoji_dict.get(None, {}))
        emoji = coll_emojis.get(sub_dep, coll_emojis.get(None, None))
        return emoji or ':question:'

    @staticmethod
    def _colour(record_properties):
        """
        Get a colour for the slack message.
        :param record_properties: the record's properties (as a dict)
        :return: a hex colour code
        """
        colour_dict = {
            'BMNH(E)': '#563635',
            'BOT': '#f4e04d',
            'MIN': '#042d6b',
            'PAL': '#5daf57',
            'ZOO': '#af2d2d',
            None: '#ca7df9',
            }

        coll = record_properties.get('collectionCode', None).upper()
        return colour_dict.get(coll, '#ca7df9')

    def _slack_msg(self, record_dict):
        data = super(SpecimenMilestone, self)._slack_msg(record_dict)
        properties = record_dict['properties'].adapted
        data['attachments'][0]['color'] = self._colour(properties)
        data['attachments'][0]['author_name'] = properties.get('recordedBy',
                                                               properties.get(
                                                                   'identifiedBy',
                                                                   properties.get(
                                                                       'donorName',
                                                                       '-')))
        data['attachments'][0]['title'] = properties.get('scientificName',
                                                         properties.get('catalogNumber',
                                                                        'Record'))
        data['attachments'][0][
            'title_link'] = 'http://data.nhm.ac.uk/object/' + record_dict.get(
            'guid')
        data['attachments'][0]['footer'] = self._emoji(properties)
        created = properties.get('created', None)
        if created is not None:
            data['attachments'][0]['ts'] = dt.strptime(created, '%Y-%m-%d').timestamp()
        return data

    @property
    def query(self):
        record_types = "','".join(self.record_types)
        return "select count(*) from ecatalogue where record_type in ('{0}') and (" \
               "embargo_date is null or embargo_date < NOW()) and deleted is " \
               "null".format(record_types)
