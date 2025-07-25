from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

from elasticsearch import Elasticsearch
from pymongo import MongoClient
from yaml import safe_load


@dataclass
class ElasticsearchConfig:
    hosts: List[str] = field(default_factory=lambda: ["http://localhost:9200"])

    def get_client(self) -> Elasticsearch:
        return Elasticsearch(hosts=self.hosts)


@dataclass
class MongoConfig:
    host: str = "localhost"
    port: int = 27017
    database: str = "sg"

    def get_client(self) -> MongoClient:
        return MongoClient(self.host, self.port)


@dataclass
class PortalConfig:
    url: str
    dsn: str
    admin_user: str


@dataclass
class Config:
    data_path: Path
    dumps_path: Path
    specimen_id: str
    artefact_id: str
    indexlot_id: str
    preparation_id: str
    sg_prefix: str
    iiif_base_url: str
    mongo_config: MongoConfig
    es_config: ElasticsearchConfig
    portal_config: PortalConfig
    gbif_username: Optional[str] = None
    gbif_password: Optional[str] = None
    source: Optional[Path] = None
    bo_chunk_size: int = 100
    bo_worker_count: int = 3

    def __post_init__(self):
        # make sure the paths are paths
        if not isinstance(self.data_path, Path):
            self.data_path = Path(self.data_path)
        if not isinstance(self.dumps_path, Path):
            self.dumps_path = Path(self.dumps_path)

    def get_elasticsearch_client(self) -> Elasticsearch:
        return self.es_config.get_client()

    def get_mongo_client(self) -> MongoClient:
        return self.mongo_config.get_client()

    def get_mongo_database_name(self) -> str:
        return self.mongo_config.database

    @property
    def lock_file(self) -> Path:
        """
        Path to the lock file to be used for the data directory.

        :return: the Path to the lock file
        """
        return self.data_path / "importer.lock"


class ConfigLoadError(Exception):
    def __init__(self, reason: str):
        super().__init__(f"Failed to load config due to {reason}")
        self.reason = reason


def load(path: Path) -> Config:
    """
    Given a path, load the configuration file at it, parse it and return a Config object
    based on its contents.

    :param path: the config file path to load
    :return: a Config object
    """
    try:
        with path.open() as f:
            raw: dict = safe_load(f)

        es_config = ElasticsearchConfig(**raw.pop("elasticsearch", {}))
        mongo_config = MongoConfig(**raw.pop("mongo", {}))
        portal_config = PortalConfig(**raw.pop("portal", {}))
        config = Config(
            **raw,
            mongo_config=mongo_config,
            es_config=es_config,
            portal_config=portal_config,
            source=path,
        )

        return config
    except Exception as e:
        raise ConfigLoadError(str(e))
