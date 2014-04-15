#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import sys
import os
import luigi.postgres
from luigi.format import Gzip
from keparser import KEParser
import abc

from sqlalchemy.orm import class_mapper
from sqlalchemy import and_, UniqueConstraint, String
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.exc import ProgrammingError, DataError
from sqlalchemy.schema import CreateSchema

from ke2sql.log import log
from ke2sql.model.keemu import *
from ke2sql.model import meta
from ke2sql.model.meta import config


class KEFileTask(luigi.ExternalTask):

    # After main run:
    # TODO: Data param & schedule
    # TODO: Email errors
    # TODO: Ensure they run in order! Not a later one doesn't run before another one? Maybe in a TASK ALL that gets the date from the files?

    module = luigi.Parameter()
    date = luigi.DateParameter(default=None)
    file_name = luigi.Parameter(default='export')
    export_dir = luigi.Parameter(default=config.get('keemu', 'export_dir'))

    def output(self):

        import_file_path = self.get_file_path()
        file_format = Gzip if '.gz' in import_file_path else None
        target = luigi.LocalTarget(import_file_path, format=file_format)
        if not target.exists():
            raise Exception('Export file %s for %s does not exist (Path: %s).' % (self.module, self.date, target.path))
        return target

    def get_file_path(self):

        file_name = [self.module, self.file_name]
        if self.date:
            file_name.append(self.date.strftime('%Y%m%d'))

        path = os.path.join(self.export_dir, '.'.join(file_name))

        #  If we don't have a compressed file, try an uncompressed one
        if not os.path.isfile(path):
            path += '.gz'

        return path



class KEDataTask(luigi.postgres.CopyToTable):
    """
    Import taxonomy
    """

    date = luigi.DateParameter(default=None)

    host = config.get('database', 'host', 'localhost')
    database = config.get('database', 'database')
    user = config.get('database', 'username')
    password = config.get('database', 'password')
    # No table; we're going to use SQLAlchemy
    table = None

    keemu_schema_file = config.get('keemu', 'schema')
    keemu_schema = config.get('database', 'schema')

    session = meta.session

    @abc.abstractproperty
    def model_class(self):
        return None

    @abc.abstractproperty
    def module(self):
        return None

    def requires(self):
        return [KEFileTask(module=self.module, date=self.date)]

    def run(self):

        for input in self.input():
            # Only process files
            # Allows for other dependencies (eg: Catalogue)
            if isinstance(input, luigi.file.File):

                ke_data = KEParser(input.open('r'), schema_file=self.keemu_schema_file, input_file_path=input.path)

                for data in ke_data:

                    status = ke_data.get_status()

                    if status:
                        log.info(status)

                    self.process(data)

                self.finish()

    def finish(self):
        # Mark this task as complete
        self.output().touch()
        # Close the session
        self.session.close()

    def get_record(self, irn):
        return self.session.query(self.model_class).filter_by(irn=irn).one()

    def process(self, data):

        log.debug('Processing %s record %s', self.model_class.__name__.lower(), data['irn'])

        try:
            # Do we already have a record for this?
            record = self.get_record(data.get('irn'))

            # Is this a stub record? If it is, we want to change the type and reload.
            # Seems a bit of a hack, but SQLAlchemy does not have a simple way of modifying the type
            #  This only runs for catalogue records
            if isinstance(record, StubModel):

                polymorphic_type = self.model_class.__mapper_args__['polymorphic_identity']
                # Manually set type
                self.session.execute('UPDATE %s.catalogue SET type=:type WHERE irn=:irn' % self.keemu_schema, {'type': polymorphic_type, 'irn': data.get('irn')})

                # If this has a child table, insert the IRN so updates will work
                if self.model_class.__mapper__.local_table.name != 'specimen':
                    # And create empty row in the polymorphic table
                    self.session.execute('INSERT INTO %s.%s (irn) VALUES (:irn)' % (self.keemu_schema, self.model_class.__mapper__.local_table.name), {'irn': data.get('irn')})

                # Commit & expunge so the item can be reloaded
                self.session.commit()
                self.session.expunge(record)
                record = self.get_record(data.get('irn'))

            # Process the relationships
            data = self._process_relationships(data, record)

            # Populate the data
            record.rebuild(**data)

        except NoResultFound:

            data = self._process_relationships(data)
            # Create a new record
            record = self.model_class(**data)

        try:

            self.session.merge(record)
            self.session.commit()

        except DataError, e:
            # Save this error to the log - will need to follow up on these
            log.critical('DB DataError: record %s not created.' % data['irn'], {'data': data}, exc_info=e)

    def _process_relationships(self, data, record=None):

        # Basic relationship handling.

        # More complex scenarios are handled in the individual processing functions
        for prop in class_mapper(self.model_class).iterate_properties:

            # Skip the field if the property key is already set in the data object
            # The field has been set in the import types custom preprocess function

            if prop.key in data:
                continue

            # Is this a relationship property?
            # NB: This excludes backrefs, which will be using sqlalchemy.orm.properties.RelationshipProperty, not our own
            if type(prop) == RelationshipProperty:

                # Try and find a child model to use for this relationship
                try:
                    child_model = prop.mapper.class_
                    # If the child model has irn primary key, it relates to a KE EMu record
                    # And a simple relationship should be used
                    if child_model.__mapper__.primary_key[0].key == 'irn':
                        child_model = None

                except AttributeError:
                    child_model = None

                # This is a relationship to a secondary object like SexStage
                if child_model:

                    # If unique, we'll try loading the values from the database first
                    # And only create if they don't exist
                    unique = False

                    for constraint in child_model.__table__.constraints:
                        if constraint.__class__ == UniqueConstraint:
                            unique = True
                            break

                    fields = {}

                    for column in child_model.__table__.columns:
                        if column.alias:
                            for alias in self.ensure_list(column.alias):
                                fields[alias] = column.key

                    # Populate a list of fields
                    data_fields = self._populate_subfield_data(fields.keys(), data)

                    # If we have data retrieve / create a model record
                    if data_fields:
                        data[prop.key] = []
                        # Loop through all the list of fields
                        for field_list in data_fields:

                            # Sometimes nothing is populated - for example, EntSexSex just has None
                            # We want to skip these
                            if not [x for x in field_list.values() if x is not None]:
                                continue

                            if unique:
                                # Try and get record from database
                                try:

                                    filters = []
                                    for alias, key in fields.items():
                                        # Build the filters
                                        col = getattr(child_model, key)

                                        # Do we have a value for this field
                                        if alias not in field_list:
                                            field_list[alias] = None

                                        # String fields should always be lower case & '' for null to ensure unique constraints work correctly
                                        if isinstance(child_model.__table__.columns[key].type, String):
                                            try:
                                                field_list[alias].lower()
                                            except AttributeError:
                                                field_list[alias] = ''

                                        filters.append(col.__eq__(field_list[alias]))

                                    # Run the query
                                    data[prop.key].append(self.session.query(child_model).filter(and_(*filters)).one())

                                except NoResultFound:
                                    # Not found, create a new one
                                    data[prop.key].append(child_model(**field_list))

                            elif 'delete-orphan' in prop.cascade:
                                # If this property has a delete-orphan cascade, everything's fine
                                # SQLa will handle updates, removing old records
                                # But for non unique / no delete orphan relationships
                                # This code will create duplicate records in the associated table
                                # Not a problem now, but log a critical error in case it ever happens
                                data[prop.key].append(child_model(**field_list))
                            else:

                                log.critical('Record %s: Non-unique relationship used in %s.' % (data['irn'], prop.key))


                else:

                    # Basic relationship, in the format:
                    # stratigraphy = relationship("StratigraphyModel", secondary=collection_event_stratigraphy, alias='GeoStratigraphyRef')
                    field_names = prop.alias
                    irns = []

                    # Ensure it's a list
                    field_names = self.ensure_list(field_names)

                    for field_name in field_names:
                        value = data.get(field_name)
                        if value:
                            irns += self.ensure_list(value)

                    # Dedupe IRNS & ensure we are not linking to the same record - eg: 687077
                    try:
                        irns = list(set(irns))
                        irns.remove(data['irn'])
                    except ValueError:
                        pass

                    # Do we have any IRNs?
                    if irns:

                        # Get the relationship model class
                        relationship_model = prop.argument()

                        # Load the model objects and assign to the property
                        data[prop.key] = self.session.query(relationship_model).filter(relationship_model.irn.in_(irns)).all()
                        existing_irns = [record.irn for record in data[prop.key]]

                        # Do we have any missing IRNs
                        missing_irns = list(set(irns) - set(existing_irns))

                        if missing_irns:

                            # Is this a property we want to create stub records for
                            if prop.key == 'associated_record':
                                for missing_irn in missing_irns:
                                    data[prop.key].append(StubModel(irn=missing_irn))
                            else:
                                log.error('Missing IRN %s in relationship %s(%s).%s', ','.join(str(x) for x in missing_irns), self.model_class.__name__, data['irn'], prop.key)

            # This isn't a relationship property - but perform check to see if this a foreign key field
            else:

                try:

                    column = prop.columns[0]

                    foreign_key = column.foreign_keys.pop()
                    # Add the foreign key back
                    column.foreign_keys.add(foreign_key)
                    foreign_key_value = None

                    # Loop through aliases / key and see if we have a foreign key value
                    candidate_names = column.alias if column.alias else prop.key
                    candidate_names = self.ensure_list(candidate_names)

                    for candidate_name in candidate_names:
                        foreign_key_value = data.get(candidate_name)
                        if foreign_key_value:
                            break

                    # We do have a foreign key value, so now perform check to see if it exists
                    if foreign_key_value and isinstance(foreign_key_value, int):

                        result = self.session.execute("SELECT COUNT(*) as exists FROM %s WHERE %s = :foreign_key_value" % (foreign_key.column.table, foreign_key.column.name), {'foreign_key_value': foreign_key_value})
                        record = result.fetchone()

                        if not record.exists:
                            # If the record doesn't exist, create a stub for part parents
                            if prop.key == 'parent_irn':
                                self.session.add(StubModel(irn=foreign_key_value))
                            else:
                            # Otherwise, delete the property so it is not used
                            # Need to ensure all candidate names are unset
                                for candidate_name in candidate_names:
                                    try:
                                        del data[candidate_name]
                                    except KeyError:
                                        pass

                                log.error('%s(%s): Missing foreign key %s for %s field. Field removed from record.', self.model_class.__name__, data['irn'], foreign_key_value, prop.key)

                except (AttributeError, KeyError):
                    pass

        return data

    @staticmethod
    def ensure_list(value):
        # Ensure a variable is a list & convert to a list if it's not
        return value if isinstance(value, list) else [value]


    def _populate_subfield_data(self, fields, data):
        """
        Given a list of fields, group them into a list
        """
        nested_data = []

        for field in fields:
            if field in data:
                # convert to a list if it's not already one
                data[field] = self.ensure_list(data[field])

                for i, value in enumerate(data[field]):
                    # Convert empty string to None
                    if value == '':
                        value = None
                    try:
                        nested_data[i][field] = value
                    except IndexError:
                        nested_data.append({field: value})

                # Remove the field from the data - do not want to reuse
                del data[field]

        # Dedupe the data
        deduped_nested_data = []

        for i in nested_data:
            if i not in deduped_nested_data:
                deduped_nested_data.append(i)

        return deduped_nested_data




