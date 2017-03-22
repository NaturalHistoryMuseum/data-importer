#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '21/03/2017'.
"""

self.remote_ckan = ckanapi.RemoteCKAN(Config.get('ckan', 'site_url'), apikey=Config.get('ckan', 'api_key'))

def get_or_create_resource(self):
    """
    Either load a resource object
    Or if it doesn't exist, prompt user to see if they want to create the dataset package, and datastore
    @param package: params to create the package
    @param datastore: params to create the datastore
    @return: CKAN resource ID
    """

    resource_id = None

    try:
        # If the package exists, retrieve the resource
        ckan_package = self.remote_ckan.action.package_show(id=self.package_name)
    except ckanapi.NotFound:
        if yesno('Dataset {package_name} does not exist. Do you want to create it now?'.format(
                package_name=self.package_name
        )):
            package_dict = {
                'name': self.package_name,
                'notes': self.package_description,
                'title': self.package_title,
                'author': Config.get('ckan', 'dataset_author'),
                'license_id': Config.get('ckan', 'dataset_licence'),
                'resources': [
                    {
                        'name': self.resource_title,
                        'description': self.resource_description,
                        'format': self.resource_type,
                        'url': '_datastore_only_resource',
                        'url_type': 'dataset'
                    }
                ],
                'dataset_category': Config.get('ckan', 'dataset_type'),
                'owner_org': Config.get('ckan', 'owner_org'),
            }

            ckan_package = self.remote_ckan.action.package_create(**package_dict)

            print(ckan_package)

    # # If we don't have the resource ID, create
    # if not resource_id:
    #     log.info("Resource %s not found - creating", self.datastore['resource']['name'])
    #
    #     self.datastore['fields'] = [{'id': col, 'type': self.numpy_to_ckan_type(np_type)} for col, np_type in self.get_output_columns().iteritems()]
    #     self.datastore['resource']['package_id'] = ckan_package['id']
    #
    #     if self.indexed_fields:
    #         # Create BTREE indexes for all specified indexed fields
    #         self.datastore['indexes'] = [col['id'] for col in self.datastore['fields'] if col['id'] in self.indexed_fields]
    #     else:
    #         # Create BTREE indexes for all citext fields
    #         self.datastore['indexes'] = [col['id'] for col in self.datastore['fields'] if col['type'] == 'citext']
    #
    #     # API call to create the datastore
    #     resource_id = self.remote_ckan.action.datastore_create(**self.datastore)['resource_id']
    #
    #     # If this has geospatial fields, create geom columns
    #     if self.geospatial_fields:
    #         log.info("Creating geometry columns for %s", resource_id)
    #         self.geospatial_fields['resource_id'] = resource_id
    #         self.remote_ckan.action.create_geom_columns(**self.geospatial_fields)
    #
    #     log.info("Created datastore resource %s", resource_id)

    return resource_id