from contextlib import suppress

import pytest
from elasticsearch import Elasticsearch
from pymongo import MongoClient


def _clear_mongo():
    with MongoClient("mongo", 27017) as client:
        database_names = client.list_database_names()
        for name in database_names:
            # the list_database_names function gives us back names like "admin" which we
            # can't drop, so catch any exceptions to avoid silly errors but provide
            # maximum clean up
            with suppress(Exception):
                client.drop_database(name)


@pytest.fixture
def reset_mongo():
    _clear_mongo()
    yield
    _clear_mongo()


def _clear_elasticsearch():
    with Elasticsearch("http://es:9200") as es:
        es.indices.delete(index="*")
        index_templates = es.indices.get_index_template(name="*")
        for index_template in index_templates["index_templates"]:
            with suppress(Exception):
                es.indices.delete_index_template(name=index_template["name"])


@pytest.fixture
def reset_elasticsearch():
    _clear_elasticsearch()
    yield
    _clear_elasticsearch()
