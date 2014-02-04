#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

from ke2psql.tasks.ke import KEDataTask
from ke2psql.model.keemu import *
from sqlalchemy.exc import ProgrammingError

class StratigraphyTask(KEDataTask):

    model_class = StratigraphyModel
    file_name = 'enhmstratigraphy'

    def process(self, data):

        unit_names = []
        # Loop through all of the period types, see if its set in data
        # If it is and we don't already have it, add to period_names

        for group in STRATIGRAPHIC_UNIT_TYPES.values():
            for unit_type in group:
                # Every unit type has To & From: ChrEonTo & ChrEonFrom
                for direction in ['To', 'From']:
                    value = self._get_field_value(data, unit_type, direction)
                    # Do we have a value?
                    if value and value not in unit_names:
                        unit_names.append(value)

        if unit_names:
            data['stratigraphic_unit'] = []
            try:
                unit_models = self._get_stratigraphic_unit_model_by_names(unit_names)
            except ProgrammingError, e:
                print data['irn']
                raise e

            # Now loop through all the types, assigning the unit model
            for group in STRATIGRAPHIC_UNIT_TYPES.values():
                for unit_type in group:
                    for direction in ['To', 'From']:
                        value = self._get_field_value(data, unit_type, direction)

                        if value:
                            unit_model = unit_models[unicode(value)]
                            # First three chars of unit type aren't useful: Lit, Chr etc.,
                            unit_type = unit_type[3:].lower()
                            data['stratigraphic_unit'].append(StratigraphicUnitAssociation(stratigraphy_irn=data['irn'], unit_id=unit_model.id, type=unit_type, direction=direction.lower()))

        super(StratigraphyTask, self).process(data)

    @staticmethod
    def _get_field_value(data, unit_type, direction):
        """
        Helper function to get the value of a field
        """
        field_name = '{0}{1}'.format(unit_type, direction)
        field_value = data.get(field_name)
        # We don't want to give the entire stratigraphic history
        # We just want the current data - so use the first item
        # This affects only 298 records
        if isinstance(field_value, list):
            field_value = field_value[0]

        return field_value

    def _get_stratigraphic_unit_model_by_names(self, unit_names):
        """
        Recursive function for retrieving stratigraphic units
        """

        unit_models = {}
        results = self.session.query(StratigraphicUnitModel).filter(StratigraphicUnitModel.name.in_(unit_names)).all()

        for unit_model in results:
            unit_models[unicode(unit_model.name)] = unit_model

        # De we have any new unit models? If so, we need to create them
        missing_unit_names = list(set(unit_names) - set(unit_models.keys()))

        if missing_unit_names:

            for missing_unit_name in missing_unit_names:
                # log.warning('Creating stratigraphic unit %s', missing_unit_name)
                self.session.add(StratigraphicUnitModel(name=missing_unit_name))

            self.session.commit()

            # WARNING: this function calls itself after the cache has been cleared
            # To rebuild the periods. Possible recursion
            unit_models = self._get_stratigraphic_unit_model_by_names(unit_names)

        return unit_models





