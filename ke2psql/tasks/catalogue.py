#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import re
from ke2psql.tasks.ke import KEDataTask
from ke2psql.model.keemu import *
from ke2psql import log
from collection_events import CollectionEventsTask
from multimedia import MultimediaTask
from sites import SitesTask
from taxonomy import TaxonomyTask
from stratigraphy import StratigraphyTask

class CatalogueTask(KEDataTask):

    model_class = None  # Will be set dynamically based on the data values
    module = 'ecatalogue'

    # Regular expression for extracting year,month,day from PalDetDate
    re_date = re.compile('^([0-9]{4})-?([0-9]{2})?-?([0-9]{2})?')
    # Regular expression for finding model name
    re_model = re.compile('[a-zA-Z &]+')


    def requires(self):
        # Catalogue is dependent on all other modules
        return [
            CollectionEventsTask(self.date),
            MultimediaTask(self.date),
            SitesTask(self.date),
            TaxonomyTask(self.date),
            StratigraphyTask(self.date)
        ]

    def process(self, data):

        log.critical('TEST')

        # Try and get the model class
        self.model_class = self.get_model_class(data)

        # If we don't have a model class, continue to next record
        if not self.model_class:
            log.debug('Skipping record %s: No model class for %s', data['irn'], data.get('ColRecordType', 'No record type'))
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
        # TODO: Test
        if data['ColRecordType'] == 'Artefact' and 'ArtKind' not in data and 'ArtName' not in data:
            return None

        # Try


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

        matches = self.re_model.match(data['ColRecordType'])

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





