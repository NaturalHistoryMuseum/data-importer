from contextlib import suppress
from pathlib import Path

import pytest
from elasticsearch import Elasticsearch
from pymongo import MongoClient

from dataimporter.emu.views.artefact import ArtefactView
from dataimporter.emu.views.image import ImageView
from dataimporter.emu.views.indexlot import IndexLotView
from dataimporter.emu.views.mammalpart import MammalPartView
from dataimporter.emu.views.mss import MSSView
from dataimporter.emu.views.preparation import PreparationView
from dataimporter.emu.views.specimen import SpecimenView
from dataimporter.emu.views.taxonomy import TaxonomyView
from dataimporter.emu.views.threed import ThreeDView
from dataimporter.ext.gbif import GBIFView
from dataimporter.lib.dbs import Store


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


FAKE_IIIF_BASE = "https://not.a.real.domain.com/media"


@pytest.fixture
def ecatalogue(tmp_path: Path) -> Store:
    store = Store(tmp_path / "auto_ecatalogue")
    yield store
    store.close()


@pytest.fixture
def emultimedia(tmp_path: Path) -> Store:
    store = Store(tmp_path / "auto_emultimedia")
    yield store
    store.close()


@pytest.fixture
def etaxonomy(tmp_path: Path) -> Store:
    store = Store(tmp_path / "auto_etaxonomy")
    yield store
    store.close()


@pytest.fixture
def gbif(tmp_path: Path) -> Store:
    store = Store(tmp_path / "auto_gbif")
    yield store
    store.close()


@pytest.fixture
def image_view(tmp_path: Path, emultimedia: Store) -> ImageView:
    view = ImageView(
        tmp_path / "auto_image_view",
        emultimedia,
        FAKE_IIIF_BASE,
    )
    yield view
    view.close()


@pytest.fixture
def three_d_view(tmp_path: Path, emultimedia: Store) -> ThreeDView:
    view = ThreeDView(tmp_path / "auto_3d_view", emultimedia)
    yield view
    view.close()


@pytest.fixture
def taxonomy_view(tmp_path: Path, etaxonomy: Store) -> TaxonomyView:
    view = TaxonomyView(tmp_path / "auto_taxonomy_view", etaxonomy)
    yield view
    view.close()


@pytest.fixture
def artefact_view(
    tmp_path: Path, ecatalogue: Store, image_view: ImageView
) -> ArtefactView:
    view = ArtefactView(
        tmp_path / "auto_artefact_view", ecatalogue, image_view, "artefact"
    )
    yield view
    view.close()


@pytest.fixture
def indexlot_view(
    tmp_path: Path,
    ecatalogue: Store,
    image_view: ImageView,
    taxonomy_view: TaxonomyView,
) -> IndexLotView:
    view = IndexLotView(
        tmp_path / "auto_indexlot_view",
        ecatalogue,
        image_view,
        taxonomy_view,
        "indexlot",
    )
    yield view
    view.close()


@pytest.fixture
def gbif_view(tmp_path: Path, gbif: Store) -> GBIFView:
    view = GBIFView(tmp_path / "auto_gbif_view", gbif)
    yield view
    view.close()


@pytest.fixture
def mammal_part_view(tmp_path: Path, ecatalogue: Store) -> MammalPartView:
    view = MammalPartView(tmp_path / "auto_mp_view", ecatalogue)
    yield view
    view.close()


@pytest.fixture
def specimen_view(
    tmp_path: Path,
    ecatalogue: Store,
    image_view: ImageView,
    taxonomy_view: TaxonomyView,
    gbif_view: GBIFView,
    mammal_part_view: MammalPartView,
) -> SpecimenView:
    view = SpecimenView(
        tmp_path / "auto_specimen_view",
        ecatalogue,
        image_view,
        taxonomy_view,
        gbif_view,
        mammal_part_view,
        "specimen",
    )
    yield view
    view.close()


@pytest.fixture
def preparation_view(
    tmp_path: Path, ecatalogue: Store, specimen_view: SpecimenView
) -> PreparationView:
    view = PreparationView(
        tmp_path / "auto_preparation_view",
        ecatalogue,
        specimen_view,
        "preparation",
    )
    yield view
    view.close()


@pytest.fixture
def mss_view(tmp_path: Path, emultimedia: Store) -> MSSView:
    view = MSSView(tmp_path / "auto_mss_view", emultimedia)
    yield view
    view.close()
