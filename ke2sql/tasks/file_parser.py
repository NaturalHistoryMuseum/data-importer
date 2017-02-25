
import os
import re
import gzip
import luigi

from ke2sql.tasks.file import FileTask
from ke2sql.lib.record import Record
from ke2sql.lib.config import get_config


class FileParserTask(luigi.ExternalTask):
    """
    Read a KE EMu export file and return records
    """
    date = luigi.IntParameter()
    module_name = luigi.Parameter()

    def requires(self):
        file_name = '{model_name}.export.{date}.gz'.format(
            model_name=self.module_name,
            date=self.date
        )
        config = get_config()
        file_path = os.path.join(config.get('keemu', 'export_dir'), file_name)
        return FileTask(file_path)

    def run(self):
        for i in [1,2,3]:
            yield i

    # def output(self):
    #     record = Record()
    #     match = re.compile(':([0-9]?)+')
    #     for line in gzip.open(self.input().path, 'rt'):
    #         line = line.strip()
    #         if not line:
    #             continue
    #         # If is a line separator, write the record
    #         if line == '###':
    #             print('---')
    #             yield record
    #             record = Record()
    #         else:
    #             field_name, value = line.split('=', 1)
    #             # Replace field name indexes
    #             field_name = match.sub('', field_name)
    #             setattr(record, field_name, value)


if __name__ == '__main__':
    luigi.run(main_task_cls=FileParserTask)
