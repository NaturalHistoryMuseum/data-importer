#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import re
import luigi
from ke2sql.log import log
from ke2sql.model.keemu import *
from ke2sql.tasks.ke import KEDataTask
from ke2sql.tasks.collection_events import CollectionEventsTask
from ke2sql.tasks.multimedia import MultimediaTask
from ke2sql.tasks.sites import SitesTask
from ke2sql.tasks.taxonomy import TaxonomyTask
from ke2sql.tasks.stratigraphy import StratigraphyTask
from ke2sql.model.meta import config
from sqlalchemy import text


class CatalogueTask(KEDataTask):

    force = luigi.BooleanParameter(default=False)

    model_class = None  # Will be set dynamically based on the data values
    module = 'ecatalogue'

    # Regular expression for extracting year,month,day from PalDetDate
    re_date = re.compile('^([0-9]{4})-?([0-9]{2})?-?([0-9]{2})?')
    # Regular expression for finding model name
    re_model = re.compile('[a-zA-Z &]+')

    # List of record types we know we want to ignore.
    # If a record type is found not in this list and without a model, an error will be logged
    # See https://docs.google.com/a/benscott.co.uk/spreadsheet/ccc?key=0AlDxkyZfoDnfdGVtVmpaRVBHOHk4VGY2ZmNHbUtjemc&usp=drive_web#gid=0
    # There are more types than this, but many didn't have records when investigated.
    # Those without records are not in this list - it's good to log an error when these start appearing
    excluded_types = [
        'Acquisition',
        'Collection Level Description',
        'DNA Card', # 1 record, but keep an eye on this
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
    ]

    def requires(self):
        # Catalogue is dependent on all other modules

        # If force is set, we don't want to check all the other tasks have run (for dev purposes)
        if self.force:
            req = []
        else:
            # TODO: Add delete task
            req = [CollectionEventsTask(self.date), MultimediaTask(self.date), SitesTask(self.date), TaxonomyTask(self.date), StratigraphyTask(self.date)]

        # And add the usual KE EMU file dependency too
        req.append(super(CatalogueTask, self).requires()[0])
        return req

    def get_record(self, irn):
        """
        Overwrite the default that uses self.model rather than CatalogueModel
        """
        return self.session.query(CatalogueModel).filter_by(irn=irn).one()

    def process(self, data):

        # Try and get the model class
        self.model_class = self.get_model_class(data)

        # If we don't have a model class, continue to next record
        if not self.model_class:

            record_type = data.get('ColRecordType', 'Missing')

            # If record type is one we've knowingly excluded
            if record_type in self.excluded_types:
                log.debug('Skipping record %s: No model class for %s', data['irn'], record_type)
            else:
                # Critical error - log to DB
                log.critical('Unknown model class %s for %s. Investigate and then add to [excluded_types] if not required.', record_type, data['irn'])

            # Next record
            return

        # Filter out some of the records
        if not 'ColDepartment' in data:
            log.debug('Skipping record %s: No collection department', data['irn'])
            return None

        if not 'AdmDateInserted' in data:
            log.debug('Skipping record %s: No AdmDateInserted', data['irn'])
            return None

        # Skip records if SecRecordStatus is one of 'DELETE', 'Reserved', 'Stub', 'Stub Record', 'DELETE-MERGED'
        if 'SecRecordStatus' in data and data['SecRecordStatus'] in ['DELETE', 'Reserved', 'Stub', 'Stub Record', 'DELETE-MERGED']:
            log.debug('Skipping record %s: Incorrect record status', data['irn'])
            return None

        # Botany records include ones from Linnean Society. Should be excluded.
        if 'RegHerbariumCurrentOrgAcroLocal' in data and data['RegHerbariumCurrentOrgAcroLocal'] == 'LINN':
            log.debug('Skipping record %s: Non-BM botany record', data['irn'])
            return None

        # 4257 Artefacts have no kind or name. Skip them
        if data['ColRecordType'] == 'Artefact' and 'ArtKind' not in data and 'ArtName' not in data:
            return None

        # Process determinations
        determinations = data.get('EntIdeTaxonRef', None) or data.get('EntIndIndexLotTaxonNameLocalRef', None)

        if determinations:

            data['specimen_taxonomy'] = []

            determinations = self.ensure_list(determinations)

            # Load the taxonomy records for these determinations
            taxonomy_records = self.session.query(TaxonomyModel).filter(TaxonomyModel.irn.in_(determinations)).all()

            # Loop through all retrieved taxonomy records, and add a determination for them
            # This will act as a filter, removing all duplicates / missing taxa
            for taxonomy_record in taxonomy_records:
                filed_as = (taxonomy_record.irn == data.get('EntIdeFiledAsTaxonRef', None))
                data['specimen_taxonomy'].append(Determination(taxonomy_irn=taxonomy_record.irn, specimen_irn=data['irn'], filed_as=filed_as))

        # Parasite card host / parasites

        host_parasites = {
            'host': data.get('CardHostRef', []),
            'parasite': data.get('CardParasiteRef', []),
        }

        stages = self.ensure_list(data.get('CardParasiteStage', []))

        for host_parasite_type, refs in host_parasites.items():
            refs = self.ensure_list(refs)

            for i, ref in enumerate(refs):
                try:
                    stage = stages[i]
                except IndexError:
                    stage = None

                assoc_object = HostParasiteAssociation(taxonomy_irn=ref, parasite_card_irn=data['irn'], parasite_host=host_parasite_type, stage=stage)

                try:
                    data['host_parasite_taxonomy'].append(assoc_object)
                except KeyError:
                    data['host_parasite_taxonomy'] = [assoc_object]

        # Some special field mappings

        # Try to use PalDetDate is if DarYearIdentified is missing
        if not 'DarYearIdentified' in data:
            try:
                date_matches = self.re_date.search(data['PalDetDate'])
                if date_matches:
                    data['DarYearIdentified'] = date_matches.group(1)
                    data['DarMonthIdentified'] = date_matches.group(2)
                    data['DarDayIdentified'] = date_matches.group(3)
            except (KeyError, TypeError):
                # If PalDetDate doesn't exists or isn't a string (can also be a list if there's multiple determination dates - which we ignore)
                pass

        # EntCatCatalogueNumber requires EntCatPrefix if it's used in catalogue_number
        try:
            data['EntCatCatalogueNumber'] = '{0}{1}'.format(data['EntCatPrefix'], data['EntCatCatalogueNumber'])
        except KeyError:
            pass

        # Set egg part type if not already set
        if self.model_class is EggModel and 'PrtType' not in data:
            data['PrtType'] = 'egg'

        super(CatalogueTask, self).process(data)

    def get_model_class(self, data):
        """
        Retrieve the model class for a specimen record, using candidate classes based on name, dept etc.,
        """
        model_class = None

        # If it doesn't even have a record type, what's the point of keeping it?
        if not 'ColRecordType' in data:
            log.debug('Skipping record %s: No record type', data['irn'])
            return None

        # Build an array of potential candidate classes
        candidate_classes = []

        collection = data['ColKind'] if 'ColKind' in data else None
        collection_department = data['ColDepartment'] if 'ColDepartment' in data else None

        # KE EMU has case insensitive record types (2820735: specimen)
        # So make sure the first letter is capitalised
        record_type = data['ColRecordType'][0].capitalize() + data['ColRecordType'][1:]
        matches = self.re_model.match(record_type)

        if matches:
            cls = matches.group(0).replace(' ', '')

            if collection:
                # Add candidate class based on ColKind (used for mineralogy) MeteoritesSpecimenModel
                candidate_classes.append('{0}{1}Model'.format(data['ColKind'], cls))

            if collection_department:
                # Add candidate class BotanySpecimenModel
                candidate_classes.append('{0}{1}Model'.format(collection_department, cls))

            # Add candidate class SpecimenModel, ArtefactModel
            candidate_classes.append('{0}Model'.format(cls))

        for candidate_class in candidate_classes:
            if candidate_class in globals():
                # Do we have a model class for this candidate
                model_class = globals()[candidate_class]
                break

        return model_class

    def on_success(self):

        schema = config.get('database', 'schema')

        # On completion, rebuild the views (table)
        self.session.execute(text('DROP TABLE IF EXISTS {schema}.specimen_taxonomy'.format(schema=schema)))

        # TODO: This should only select the first determination. Limit isn't working
        self.session.execute(text(
            """
            CREATE TABLE {schema}.specimen_taxonomy AS
                    (SELECT DISTINCT ON(d.specimen_irn) specimen_irn, taxonomy_irn
                    FROM {schema}.determination d
                    INNER JOIN keemu.specimen s ON s.irn = d.specimen_irn
                    ORDER BY d.specimen_irn, filed_as DESC)
                UNION
                    (SELECT DISTINCT ON(s.irn) s.irn as specimen_irn, taxonomy_irn
                    FROM {schema}.SPECIMEN s
                    INNER JOIN keemu.part p ON p.irn = s.irn
                    INNER JOIN keemu.determination d ON p.parent_irn = d.specimen_irn
                    WHERE NOT EXISTS (SELECT 1 FROM keemu.determination WHERE specimen_irn = s.irn)
                    ORDER BY s.irn, filed_as DESC)
            """.format(schema=schema)
        ))

        # Add primary key
        self.session.execute(text('ALTER TABLE {schema}.specimen_taxonomy ADD PRIMARY KEY (specimen_irn)'.format(schema=schema)))
        self.session.commit()




