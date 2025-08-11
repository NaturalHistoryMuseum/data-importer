import click
import requests

from dataimporter.cli.utils import console, get_api_key, with_config
from dataimporter.lib.config import Config


@click.group('portal')
def portal_group():
    pass


@portal_group.command('init')
@with_config()
def init(config: Config):
    datasets = {
        'specimen': {
            'name': 'collection-specimens',
            'notes': "Specimen records from the Natural History Museum's collection",
            'title': 'Collection specimens',
            'author': 'Natural History Museum',
            'license_id': 'cc-zero',
            'dataset_category': 'Collections',
            'owner_org': 'nhm',
            'resources': [
                {
                    'id': config.specimen_id,
                    'name': 'Specimen records',
                    'description': 'Specimen records',
                    'format': 'dwc',
                    'package_id': 'collection-specimens',
                    'url': '_datastore_only_resource',
                    'url_type': 'dataset',
                }
            ],
        },
        'artefact': {
            'name': 'collection-artefacts',
            'notes': 'Cultural and historical artefacts from The Natural History Museum',
            'title': 'Artefacts',
            'author': 'Natural History Museum',
            'license_id': 'cc-zero',
            'dataset_category': 'Collections',
            'owner_org': 'nhm',
            'resources': [
                {
                    'id': config.artefact_id,
                    'name': 'Artefacts',
                    'description': 'Museum Artefacts',
                    'format': 'csv',
                    'package_id': 'collection-artefacts',
                    'url': '_datastore_only_resource',
                    'url_type': 'dataset',
                }
            ],
        },
        'indexlot': {
            'name': 'collection-indexlots',
            'notes': "Index Lot records from the Natural History Museum's collection",
            'title': 'Index Lot Collection',
            'author': 'Natural History Museum',
            'license_id': 'cc-zero',
            'dataset_category': 'Collections',
            'owner_org': 'nhm',
            'resources': [
                {
                    'id': config.indexlot_id,
                    'name': 'Index Lots',
                    'description': 'Species level record denoting the presence of a taxon in the Museum collection',
                    'format': 'csv',
                    'package_id': 'collection-indexlots',
                    'url': '_datastore_only_resource',
                    'url_type': 'dataset',
                }
            ],
        },
    }
    url = f'{config.portal_config.url}/api/action/package_create'
    api_key = get_api_key(config.portal_config.dsn, config.portal_config.admin_user)
    if api_key is None:
        console.log('No API key found, exiting')
    else:
        for name, package_def in datasets.items():
            requests.post(url, json=package_def, headers={'Authorization': api_key})
