from pathlib import Path
from unittest.mock import MagicMock

import pytest
from elasticsearch import Elasticsearch
from pymongo import MongoClient

from dataimporter.lib.config import (
    ElasticsearchConfig,
    MongoConfig,
    Config,
    load,
    ConfigLoadError,
)


class TestElasticsearchConfig:
    def test_defaults(self):
        config = ElasticsearchConfig()
        assert config.hosts == ["http://localhost:9200"]

    def test_get_client(self):
        client = ElasticsearchConfig().get_client()
        assert isinstance(client, Elasticsearch)


class TestMongoConfig:
    def test_defaults(self):
        config = MongoConfig()
        assert config.host == "localhost"
        assert config.port == 27017

    def test_get_client(self):
        client = MongoConfig().get_client()
        assert isinstance(client, MongoClient)


class TestConfig:
    def test_str_paths_become_paths(self):
        config = Config(
            "/test/data",
            "/test/dump",
            MagicMock(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
        )

        assert isinstance(config.data_path, Path)
        assert isinstance(config.dumps_path, Path)

    def test_get_clients(self):
        config = Config(
            Path("/test/data"),
            Path("/test/dumps"),
            MagicMock(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
            MongoConfig(),
            ElasticsearchConfig(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
        )
        assert isinstance(config.get_elasticsearch_client(), Elasticsearch)
        assert isinstance(config.get_mongo_client(), MongoClient)


class TestLoad:
    def test_error(self):
        with pytest.raises(ConfigLoadError):
            load(Path("/not/a/path"))

    def test_valid(self, tmp_path: Path):
        raw_config = """
data_path: '/test/data'
dumps_path: '/test/dumps/'
specimen_id: 'specimen'
artefact_id: 'artefact'
indexlot_id: 'indexlot'
preparation_id: 'preparation'
sg_prefix: 'test'
iiif_base_url: 'https://not.a.real.domain.com/media'
bo_chunk_size: 100
bo_worker_count: 4
elasticsearch:
  hosts:
    - 'http://test_es:9200'
mongo:
  host: test_mongo
  port: 27017
portal:
  url: "http://10.0.11.20"
  dsn: 'postgres://ckan:password@db/ckan'
  admin_user: "admin"
        """
        config_path = tmp_path / "config.yml"
        config_path.write_text(raw_config)

        config = load(config_path)

        assert config.data_path == Path("/test/data")
        assert config.dumps_path == Path("/test/dumps")
        assert config.specimen_id == "specimen"
        assert config.artefact_id == "artefact"
        assert config.indexlot_id == "indexlot"
        assert config.es_config.hosts == ["http://test_es:9200"]
        assert config.mongo_config.host == "test_mongo"
        assert config.mongo_config.port == 27017
        assert config.portal_config.url == "http://10.0.11.20"
        assert config.portal_config.dsn == "postgres://ckan:password@db/ckan"
        assert config.portal_config.admin_user == "admin"
        assert config.bo_chunk_size == 100
        assert config.bo_worker_count == 4
