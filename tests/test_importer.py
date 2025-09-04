from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Generator
from unittest.mock import MagicMock, patch

import pytest
from freezegun import freeze_time
from splitgill.model import Record
from splitgill.search import keyword
from splitgill.utils import to_timestamp

from dataimporter.emu.dumps import FIRST_VERSION
from dataimporter.importer import (
    DataImporter,
    EMuStatus,
    ImporterAlreadyRunning,
    StoreNotFound,
    use_importer,
)
from dataimporter.lib.config import (
    Config,
    ElasticsearchConfig,
    MongoConfig,
    PortalConfig,
)
from dataimporter.lib.model import SourceRecord
from tests.conftest import ES_HOST, ES_PORT, MONGO_HOST, MONGO_PORT
from tests.helpers.dumps import (
    EcatalogueType,
    create_dump,
    create_eaudit,
    create_ecatalogue,
    create_emultimedia,
    create_etaxonomy,
)


@pytest.fixture
def config(tmp_path: Path) -> Config:
    mongo_config = MongoConfig(MONGO_HOST, MONGO_PORT)
    elasticsearch_config = ElasticsearchConfig([f'http://{ES_HOST}:{ES_PORT}'])
    portal_config = PortalConfig(
        'http://localhost', 'postgres://ckan:password@db/ckan', 'admin'
    )
    return Config(
        data_path=tmp_path / 'data',
        dumps_path=tmp_path / 'dumps',
        specimen_id='specimen-id',
        artefact_id='artefact-id',
        indexlot_id='indexlot-id',
        preparation_id='preparation-id',
        sg_prefix='test-',
        iiif_base_url='https://not.a.real.domain.com/media',
        mongo_config=mongo_config,
        es_config=elasticsearch_config,
        portal_config=portal_config,
        gbif_username=None,
        gbif_password=None,
        bo_chunk_size=100,
        bo_worker_count=4,
    )


@pytest.fixture
def importer(config: Config) -> Generator[DataImporter, Any, None]:
    with use_importer(config) as imp:
        yield imp


def test_use_importer(config: Config):
    assert not config.data_path.exists()
    with use_importer(config):
        with pytest.raises(ImporterAlreadyRunning):
            # trying to open another one should fail
            with use_importer(config):
                pass


class TestDataImporter:
    def test_init(self, importer: DataImporter):
        # check the databases that we expect are created
        assert len(importer.stores) == 4
        assert importer.get_store('ecatalogue') is not None
        assert importer.get_store('emultimedia') is not None
        assert importer.get_store('etaxonomy') is not None
        assert importer.get_store('gbif') is not None
        with pytest.raises(StoreNotFound):
            importer.get_store('eaudit')

        # check the views that we expect are created
        assert len(importer.views) == 9
        assert importer.get_view('mss') is not None
        assert importer.get_view('image') is not None
        assert importer.get_view('taxonomy') is not None
        assert importer.get_view('gbif') is not None
        assert importer.get_view('artefact') is not None
        assert importer.get_view('indexlot') is not None
        assert importer.get_view('specimen') is not None
        assert importer.get_view('preparation') is not None

        importer.close()

    def test_queue_emu_changes(self, importer: DataImporter, config: Config):
        first_dump_date = date(2023, 10, 3)
        # create an ecatalogue dump with one record per view
        create_dump(
            config.dumps_path,
            'ecatalogue',
            first_dump_date,
            create_ecatalogue(
                '1', EcatalogueType.specimen, MulMultiMediaRef='1', CardParasiteRef='1'
            ),
            create_ecatalogue(
                '2',
                EcatalogueType.indexlot,
                MulMultiMediaRef='2',
                EntIndIndexLotTaxonNameLocalRef='2',
            ),
            create_ecatalogue('3', EcatalogueType.artefact, MulMultiMediaRef='3'),
            create_ecatalogue('4', EcatalogueType.preparation, EntPreSpecimenRef='1'),
        )
        # create an emultimedia dump with 3 images each with an ID that matches the
        # linked IDs above in the ecatalogue dump via the MulMultiMediaRef field
        create_dump(
            config.dumps_path,
            'emultimedia',
            first_dump_date,
            create_emultimedia('1'),
            create_emultimedia('2'),
            create_emultimedia('3'),
        )
        # create an etaxonomy dump with 2 records one matching the specimen made above
        # and one matching the index lot
        create_dump(
            config.dumps_path,
            'etaxonomy',
            first_dump_date,
            create_etaxonomy('1'),
            create_etaxonomy('2'),
        )

        assert importer.queue_emu_changes() == first_dump_date

        assert importer.get_store('ecatalogue').size() == 4
        assert importer.get_store('emultimedia').size() == 3
        assert importer.get_store('etaxonomy').size() == 2
        assert importer.get_view('specimen').changes.size() == 1
        assert importer.get_view('indexlot').changes.size() == 1
        assert importer.get_view('artefact').changes.size() == 1
        assert importer.get_view('preparation').changes.size() == 1
        assert importer.get_view('image').changes.size() == 3
        assert importer.get_view('mss').changes.size() == 3

        # flush all the view queues
        for view in importer.views:
            view.flush()
            assert view.changes.size() == 0

        second_dump_date = date(2023, 10, 4)
        create_dump(
            config.dumps_path,
            'eaudit',
            second_dump_date,
            # delete the index lot
            create_eaudit('2', 'ecatalogue'),
            # delete the media on the artefact
            create_eaudit('3', 'emultimedia'),
            # delete the taxonomy of the specimen
            create_eaudit('1', 'etaxonomy'),
        )

        assert importer.queue_emu_changes() == second_dump_date

        # these have all lost 1 to reflect the newly deleted records
        assert importer.get_store('ecatalogue').size() == 3
        assert importer.get_store('emultimedia').size() == 2
        assert importer.get_store('etaxonomy').size() == 1
        # 1 indexlot delete + specimen update because of the taxonomy delete
        assert importer.get_view('specimen').changes.size() == 2
        # 1 indexlot delete
        assert importer.get_view('indexlot').changes.size() == 1
        # 1 indexlot delete + artefact update because of the multimedia delete
        assert importer.get_view('artefact').changes.size() == 2
        # 1 indexlot delete, 1 specimen change by taxonomy change which is pushed down
        assert importer.get_view('preparation').changes.size() == 2
        # 1 multimedia delete
        assert importer.get_view('image').changes.size() == 1
        # 1 multimedia delete
        assert importer.get_view('mss').changes.size() == 1

        for view in importer.views:
            view.flush()
            assert view.changes.size() == 0

        third_dump_date = date(2023, 10, 8)
        # update all the multimedia records + a new record unlinked to anything
        create_dump(
            config.dumps_path,
            'emultimedia',
            third_dump_date,
            create_emultimedia('1'),
            create_emultimedia('2'),
            create_emultimedia('3'),
            create_emultimedia('4'),
        )

        assert importer.queue_emu_changes() == third_dump_date

        assert importer.get_store('ecatalogue').size() == 3
        # there's a new emultimedia record now
        assert importer.get_store('emultimedia').size() == 4
        assert importer.get_store('etaxonomy').size() == 1
        # an image update on an associated image, so 1
        assert importer.get_view('specimen').changes.size() == 1
        # image update on an associated image, but the index lot has been deleted so 0
        assert importer.get_view('indexlot').changes.size() == 0
        # an image update on an associated image, so 1
        assert importer.get_view('artefact').changes.size() == 1
        # an image update on an associated specimen's image, so 1
        assert importer.get_view('preparation').changes.size() == 1
        assert importer.get_view('image').changes.size() == 4
        assert importer.get_view('mss').changes.size() == 4

    def test_queue_emu_changes_only_one(self, config: Config, importer: DataImporter):
        first_dump_date = date(2023, 10, 3)
        create_dump(
            config.dumps_path,
            'ecatalogue',
            first_dump_date,
            create_ecatalogue('1', EcatalogueType.specimen),
        )
        create_dump(
            config.dumps_path, 'emultimedia', first_dump_date, create_emultimedia('1')
        )
        create_dump(
            config.dumps_path, 'etaxonomy', first_dump_date, create_etaxonomy('1')
        )

        second_dump_date = date(2023, 10, 4)
        create_dump(
            config.dumps_path,
            'ecatalogue',
            second_dump_date,
            create_ecatalogue('2', EcatalogueType.specimen),
        )
        create_dump(
            config.dumps_path, 'emultimedia', second_dump_date, create_emultimedia('2')
        )
        create_dump(
            config.dumps_path, 'etaxonomy', second_dump_date, create_etaxonomy('2')
        )

        assert importer.queue_emu_changes() == first_dump_date
        assert importer.emu_status.get() == first_dump_date
        assert importer.get_store('ecatalogue').size() == 1
        assert importer.get_store('emultimedia').size() == 1
        assert importer.get_store('etaxonomy').size() == 1

        assert importer.queue_emu_changes() == second_dump_date
        assert importer.emu_status.get() == second_dump_date
        assert importer.get_store('ecatalogue').size() == 2
        assert importer.get_store('emultimedia').size() == 2
        assert importer.get_store('etaxonomy').size() == 2

    def test_queue_gbif_changes(self, config: Config):
        gbif_records = [
            SourceRecord('1', {'x': '1'}, 'gbif'),
            SourceRecord('2', {'x': '1'}, 'gbif'),
            SourceRecord('3', {'x': '3'}, 'gbif'),
        ]
        change_records_mock = MagicMock(return_value=gbif_records)
        with patch('dataimporter.importer.get_changed_records', change_records_mock):
            with use_importer(config) as importer:
                importer.queue_gbif_changes()

                assert importer.get_store('gbif').size() == 3
                assert importer.get_view('gbif').changes.size() == 3

    @freeze_time('2023-10-20 11:04:31')
    @pytest.mark.usefixtures('reset_mongo', 'reset_elasticsearch')
    def test_add_to_mongo_and_sync_to_elasticsearch_artefact(self, config: Config):
        name = 'artefact'
        # before the frozen time
        dump_date = date(2023, 10, 3)
        # create an ecatalogue dump with 8 artefacts
        create_dump(
            config.dumps_path,
            'ecatalogue',
            dump_date,
            *[
                create_ecatalogue(
                    str(i),
                    EcatalogueType[name],
                    MulMultiMediaRef=str(i),
                    PalArtObjectName=f'{i} beans',
                )
                for i in range(1, 9)
            ],
        )
        # create an emultimedia dump with 8 images
        create_dump(
            config.dumps_path,
            'emultimedia',
            dump_date,
            *[create_emultimedia(str(i), MulTitle=f'image {i}') for i in range(1, 9)],
        )

        with use_importer(config) as importer:
            importer.queue_emu_changes()

            importer.add_to_mongo(name)

            database = importer.get_database(importer.get_view(name))
            assert database.get_committed_version() == to_timestamp(
                datetime(2023, 10, 20, 11, 4, 31)
            )
            assert database.data_collection.count_documents({}) == 8

            importer.sync_to_elasticsearch(name)

            assert database.get_elasticsearch_version() == to_timestamp(
                datetime(2023, 10, 20, 11, 4, 31)
            )

            search_base = database.search()
            assert search_base.count() == 8
            assert (
                search_base.filter(
                    'term', **{keyword('artefactName'): '3 beans'}
                ).count()
                == 1
            )
            # this comes from the image
            assert (
                search_base.filter(
                    'term', **{keyword('associatedMedia.title'): 'image 4'}
                ).count()
                == 1
            )

    @freeze_time('2023-10-20 11:04:31')
    @pytest.mark.usefixtures('reset_mongo', 'reset_elasticsearch')
    def test_add_to_mongo_and_sync_to_elasticsearch_indexlot(self, config: Config):
        name = 'indexlot'
        # before the frozen time
        dump_date = date(2023, 10, 3)
        # create an ecatalogue dump with 8 indexlots
        create_dump(
            config.dumps_path,
            'ecatalogue',
            dump_date,
            *[
                create_ecatalogue(
                    str(i),
                    EcatalogueType[name],
                    MulMultiMediaRef=str(i),
                    EntIndIndexLotTaxonNameLocalRef=str(i),
                    EntIndMaterial=f'{i} lemons',
                )
                for i in range(1, 9)
            ],
        )
        # create an emultimedia dump with 8 images
        create_dump(
            config.dumps_path,
            'emultimedia',
            dump_date,
            *[create_emultimedia(str(i), MulTitle=f'image {i}') for i in range(1, 9)],
        )
        # create an etaxonomy dump with 8 records
        create_dump(
            config.dumps_path,
            'etaxonomy',
            dump_date,
            *[create_etaxonomy(str(i), ClaKingdom=f'kingdom {i}') for i in range(1, 9)],
        )

        with use_importer(config) as importer:
            importer.queue_emu_changes()

            importer.add_to_mongo(name)

            database = importer.get_database(importer.get_view(name))
            assert database.get_committed_version() == to_timestamp(
                datetime(2023, 10, 20, 11, 4, 31)
            )
            assert database.data_collection.count_documents({}) == 8

            importer.sync_to_elasticsearch(name)

            assert database.get_elasticsearch_version() == to_timestamp(
                datetime(2023, 10, 20, 11, 4, 31)
            )
            search_base = database.search()
            assert search_base.count() == 8
            assert (
                search_base.filter('term', **{keyword('material'): '3 lemons'}).count()
                == 1
            )
            # this comes from the image
            assert (
                search_base.filter(
                    'term', **{keyword('associatedMedia.title'): 'image 4'}
                ).count()
                == 1
            )
            # this comes from the taxonomy
            assert (
                search_base.filter('term', **{keyword('kingdom'): 'kingdom 4'}).count()
                == 1
            )

    @freeze_time('2023-10-20 11:04:31')
    @pytest.mark.usefixtures('reset_mongo', 'reset_elasticsearch')
    def test_add_to_mongo_and_sync_to_elasticsearch_specimen(self, config: Config):
        name = 'specimen'
        # before the frozen time
        dump_date = date(2023, 10, 3)
        # create an ecatalogue dump with 8 specimens
        create_dump(
            config.dumps_path,
            'ecatalogue',
            dump_date,
            *[
                create_ecatalogue(
                    str(i),
                    EcatalogueType[name],
                    MulMultiMediaRef=str(i),
                    CardParasiteRef=str(i),
                    sumPreciseLocation=f'{i} Number Road',
                )
                for i in range(1, 9)
            ],
        )
        # create an emultimedia dump with 8 images
        create_dump(
            config.dumps_path,
            'emultimedia',
            dump_date,
            *[create_emultimedia(str(i), MulTitle=f'image {i}') for i in range(1, 9)],
        )
        # create an etaxonomy dump with 8 records
        create_dump(
            config.dumps_path,
            'etaxonomy',
            dump_date,
            *[create_etaxonomy(str(i), ClaKingdom=f'kingdom {i}') for i in range(1, 9)],
        )

        with use_importer(config) as importer:
            importer.queue_emu_changes()

            importer.add_to_mongo(name)

            database = importer.get_database(importer.get_view(name))

        assert database.get_committed_version() == to_timestamp(
            datetime(2023, 10, 20, 11, 4, 31)
        )
        assert database.data_collection.count_documents({}) == 8

        importer.sync_to_elasticsearch(name)

        assert database.get_elasticsearch_version() == to_timestamp(
            datetime(2023, 10, 20, 11, 4, 31)
        )
        search_base = database.search()
        assert search_base.count() == 8
        assert (
            search_base.filter('term', **{keyword('locality'): '3 Number Road'}).count()
            == 1
        )
        # this comes from the image
        assert (
            search_base.filter(
                'term', **{keyword('associatedMedia.title'): 'image 4'}
            ).count()
            == 1
        )
        # this comes from the taxonomy
        assert (
            search_base.filter('term', **{keyword('kingdom'): 'kingdom 4'}).count() == 1
        )

    @freeze_time('2023-10-20 11:04:31')
    @pytest.mark.usefixtures('reset_mongo', 'reset_elasticsearch')
    def test_add_to_mongo_and_sync_to_elasticsearch_mss(self, config: Config):
        name = 'mss'
        # before the frozen time
        dump_date = date(2023, 10, 3)
        # create an emultimedia dump with 8 images
        create_dump(
            config.dumps_path,
            'emultimedia',
            dump_date,
            *[
                create_emultimedia(str(i), DocIdentifier=f'banana-{i}.jpg')
                for i in range(1, 9)
            ],
        )

        with use_importer(config) as importer:
            importer.queue_emu_changes()

            importer.add_to_mongo(name)

            database = importer.get_database(importer.get_view(name))

        assert database.get_committed_version() == to_timestamp(
            datetime(2023, 10, 20, 11, 4, 31)
        )
        assert database.data_collection.count_documents({}) == 8

        importer.sync_to_elasticsearch(name)

        assert database.get_elasticsearch_version() == to_timestamp(
            datetime(2023, 10, 20, 11, 4, 31)
        )
        search_base = database.search()
        assert search_base.count() == 8
        assert (
            search_base.filter('term', **{keyword('file'): 'banana-4.jpg'}).count() == 1
        )

    @freeze_time('2023-10-20 11:04:31')
    @pytest.mark.usefixtures('reset_mongo', 'reset_elasticsearch')
    def test_add_to_mongo_and_sync_to_elasticsearch_preparation(self, config: Config):
        name = 'preparation'
        # before the frozen time
        dump_date = date(2023, 10, 3)
        # create an ecatalogue dump with 8 specimens and 8 preparations
        ecat_records = [
            *[
                create_ecatalogue(
                    str(i),
                    EcatalogueType[name],
                    EntPreSpecimenRef=str(i + 8),
                    EntPreStorageMedium=f'Ethanol ({i}%)',
                )
                for i in range(1, 9)
            ],
            *[
                create_ecatalogue(
                    str(i),
                    EcatalogueType.specimen,
                    MulMultiMediaRef=str(i),
                    CardParasiteRef=str(i),
                    EntCatBarcode=f'000-00-0-{i}',
                )
                for i in range(9, 17)
            ],
        ]
        create_dump(config.dumps_path, 'ecatalogue', dump_date, *ecat_records)
        # create an emultimedia dump with 8 images
        create_dump(
            config.dumps_path,
            'emultimedia',
            dump_date,
            *[create_emultimedia(str(i), MulTitle=f'image {i}') for i in range(9, 17)],
        )
        # create an etaxonomy dump with 8 records
        create_dump(
            config.dumps_path,
            'etaxonomy',
            dump_date,
            *[create_etaxonomy(str(i), ClaOrder=f'order {i}') for i in range(9, 17)],
        )

        with use_importer(config) as importer:
            importer.queue_emu_changes()

            importer.add_to_mongo(name)

            database = importer.get_database(importer.get_view(name))
            assert database.get_committed_version() == to_timestamp(
                datetime(2023, 10, 20, 11, 4, 31)
            )
            assert database.data_collection.count_documents({}) == 8

            importer.sync_to_elasticsearch(name)

            assert database.get_elasticsearch_version() == to_timestamp(
                datetime(2023, 10, 20, 11, 4, 31)
            )
            search_base = database.search()
            assert search_base.count() == 8
            assert (
                search_base.filter(
                    'term', **{keyword('preservation'): 'Ethanol (6%)'}
                ).count()
                == 1
            )
            # check a field that should have been copied from the voucher specimen
            assert (
                search_base.filter(
                    'term', **{keyword('barcode'): '000-00-0-12'}
                ).count()
                == 1
            )
            # check a field that should have been copied from the voucher specimen's
            # taxonomy
            assert (
                search_base.filter('term', **{keyword('order'): 'order 11'}).count()
                == 1
            )

    def test_queue_changes_redactions(self, importer: DataImporter):
        changed_records = [
            SourceRecord('1', {'a': 'a'}, 'test'),
            SourceRecord('2', {'a': 'b'}, 'test'),
            SourceRecord('3', {'a': 'c'}, 'test'),
        ]

        # redact records 2 and 3
        importer.redact_records('ecatalogue', ['2', '3'], 'reason_1')

        # queue all the change records
        importer.queue_changes(changed_records, 'ecatalogue')

        assert importer.get_store('ecatalogue').has('1')
        assert not importer.get_store('ecatalogue').has('2')
        assert not importer.get_store('ecatalogue').has('3')

    def test_redact_records(self, importer: DataImporter):
        records = [
            SourceRecord('1', {'a': 'a'}, 'test'),
            SourceRecord('2', {'a': 'b'}, 'test'),
            SourceRecord('3', {'a': 'c'}, 'test'),
        ]

        # queue all the records
        importer.queue_changes(records, 'ecatalogue')

        # redact records 2 and 3
        redacted_count = importer.redact_records('ecatalogue', ['2', '3'], 'reason1')

        assert redacted_count == 2
        assert importer.get_store('ecatalogue').has('1')
        assert not importer.get_store('ecatalogue').has('2')
        assert not importer.get_store('ecatalogue').has('3')

    @pytest.mark.usefixtures('reset_mongo', 'reset_elasticsearch')
    def test_depublish_records(self, config: Config):
        name = 'artefact'
        # create some artefact records
        artefact_records = [
            create_ecatalogue(
                str(i),
                EcatalogueType[name],
                PalArtObjectName=f'{i} beans',
            )
            for i in range(1, 9)
        ]
        first_dump_date = date(2025, 1, 1)
        # create an ecatalogue dump with 8 artefacts
        create_dump(
            config.dumps_path,
            'ecatalogue',
            first_dump_date,
            *artefact_records,
        )
        # create dump that depublishes one of those records
        second_dump_date = date(2025, 1, 3)
        updated_record = artefact_records[0]
        updated_record['SecRecordStatus'] = 'Retired'
        create_dump(config.dumps_path, 'ecatalogue', second_dump_date, updated_record)

        with use_importer(config) as importer, freeze_time('2025-01-02') as frozen_time:
            # import the first dump and check all 8 records are present
            importer.queue_emu_changes()
            importer.add_to_mongo(name)
            database = importer.get_database(importer.get_view(name))
            assert database.data_collection.count_documents({}) == 8
            mongo_record = database.data_collection.find_one(
                {'id': updated_record['irn']}
            )
            assert mongo_record is not None
            assert len(mongo_record['data']) == 2
            assert 'diffs' not in mongo_record

            importer.sync_to_elasticsearch(name)
            first_version = database.get_elasticsearch_version()
            search_base = database.search()
            assert search_base.count() == 8
            assert (
                search_base.filter(
                    'term',
                    **{keyword('artefactName'): updated_record['PalArtObjectName']},
                ).count()
                == 1
            )

            # import the second dump and check that one has been removed from the
            # current version
            frozen_time.tick(timedelta(days=2))  # advance forward 2 days

            importer.queue_emu_changes()
            importer.add_to_mongo(name)
            database = importer.get_database(importer.get_view(name))
            # it still exists in mongo; it hasn't been redacted
            assert database.data_collection.count_documents({}) == 8
            # but there shouldn't be any data in it
            updated_mongo_record = database.data_collection.find_one(
                {'id': updated_record['irn']}
            )
            assert updated_mongo_record is not None
            assert len(updated_mongo_record['data']) == 0
            assert len(updated_mongo_record['diffs']) == 1

            importer.sync_to_elasticsearch(name)
            assert search_base.count() == 7
            assert (
                search_base.filter(
                    'term',
                    **{keyword('artefactName'): updated_record['PalArtObjectName']},
                ).count()
                == 0
            )
            # check it still exists in the previous version
            assert database.search(first_version).count() == 8

    @pytest.mark.usefixtures('reset_mongo', 'reset_elasticsearch')
    def test_republish_records(self, config: Config):
        name = 'artefact'
        # create some artefact records
        artefact_records = [
            create_ecatalogue(
                str(i),
                EcatalogueType[name],
                PalArtObjectName=f'{i} beans',
            )
            for i in range(1, 9)
        ]
        first_dump_date = date(2025, 1, 1)

        # create an ecatalogue dump with 8 artefacts
        create_dump(
            config.dumps_path,
            'ecatalogue',
            first_dump_date,
            *artefact_records,
        )

        target_record = artefact_records[0]

        # create dump that depublishes one of those records
        depub_dump_date = date(2025, 1, 3)
        depub_record = target_record.copy()
        depub_record['AdmPublishWebNoPasswordFlag'] = 'N'
        create_dump(config.dumps_path, 'ecatalogue', depub_dump_date, depub_record)

        # create a third dump that REpublishes that record
        repub_dump_date = date(2025, 1, 5)
        repub_record = target_record.copy()
        repub_record['AdmPublishWebNoPasswordFlag'] = 'Y'
        create_dump(config.dumps_path, 'ecatalogue', repub_dump_date, repub_record)

        with use_importer(config) as importer, freeze_time('2025-01-02') as frozen_time:
            # import the first dump and check all 8 records are present
            importer.queue_emu_changes()
            importer.add_to_mongo(name)
            database = importer.get_database(importer.get_view(name))
            first_db_version = database.get_committed_version()
            importer.sync_to_elasticsearch(name)
            first_es_version = database.get_elasticsearch_version()
            search_base = database.search()

            assert database.data_collection.count_documents({}) == 8
            mongo_record = database.data_collection.find_one(
                {'id': target_record['irn']}
            )
            assert mongo_record is not None
            assert len(mongo_record['data']) > 0
            assert 'diffs' not in mongo_record

            assert search_base.count() == 8
            assert (
                search_base.filter(
                    'term',
                    **{keyword('artefactName'): target_record['PalArtObjectName']},
                ).count()
                == 1
            )

            frozen_time.tick(timedelta(days=2))  # advance forward 2 days

            # import the second dump and check that one has been removed from the
            # current version
            importer.queue_emu_changes()
            importer.add_to_mongo(name)
            second_db_version = database.get_committed_version()
            assert second_db_version != first_db_version
            importer.sync_to_elasticsearch(name)
            second_es_version = database.get_elasticsearch_version()
            assert second_es_version != first_es_version

            # it still exists in mongo; it hasn't been redacted
            assert database.data_collection.count_documents({}) == 8
            # but there shouldn't be any data in it
            depub_mongo_record = database.data_collection.find_one(
                {'id': target_record['irn']}
            )
            assert depub_mongo_record is not None
            assert len(depub_mongo_record['data']) == 0
            assert len(depub_mongo_record['diffs']) == 1

            # it shouldn't be in current elasticsearch
            assert search_base.count() == 7
            assert (
                search_base.filter(
                    'term',
                    **{keyword('artefactName'): target_record['PalArtObjectName']},
                ).count()
                == 0
            )
            # check it still exists in the previous version
            assert database.search(first_es_version).count() == 8

            frozen_time.tick(timedelta(days=2))  # advance forward 2 days

            # import the third dump and check that it has been added back
            importer.queue_emu_changes()
            importer.add_to_mongo(name)
            third_db_version = database.get_committed_version()
            assert third_db_version != second_db_version
            importer.sync_to_elasticsearch(name)
            third_es_version = database.get_elasticsearch_version()
            assert third_es_version != second_es_version

            assert database.data_collection.count_documents({}) == 8
            # there should now be data in the mongo record again
            repub_mongo_record = database.data_collection.find_one(
                {'id': target_record['irn']}
            )
            assert repub_mongo_record is not None
            assert len(repub_mongo_record['data']) > 0
            assert len(repub_mongo_record['diffs']) == 2

            assert search_base.count() == 8
            assert (
                search_base.filter(
                    'term',
                    **{keyword('artefactName'): target_record['PalArtObjectName']},
                ).count()
                == 1
            )
            # check the previous versions are still correct
            assert database.search(first_es_version).count() == 8
            assert database.search(second_es_version).count() == 7

    @pytest.mark.usefixtures('reset_mongo', 'reset_elasticsearch')
    def test_purge_records(self, config: Config):
        name = 'artefact'
        # create some artefact records
        artefact_records = [
            create_ecatalogue(
                str(i),
                EcatalogueType[name],
                PalArtObjectName=f'{i} beans',
            )
            for i in range(1, 9)
        ]
        first_dump_date = date(2025, 1, 1)
        # create an ecatalogue dump with 8 artefacts
        create_dump(
            config.dumps_path,
            'ecatalogue',
            first_dump_date,
            *artefact_records,
        )
        # make two records unpublishable and one no longer an artefact
        depublished_record_1 = artefact_records[0]
        depublished_record_1['SecRecordStatus'] = 'Retired'
        depublished_record_2 = artefact_records[1]
        depublished_record_2['AdmPublishWebNoPasswordFlag'] = 'N'
        non_member_record = artefact_records[2]
        non_member_record['ColRecordType'] = 'Banana'
        test_records = [depublished_record_1, depublished_record_2, non_member_record]
        # update the name for all of them so mongo recognises it as a change later
        for ix, tr in enumerate(test_records):
            tr.update({'PalArtObjectName': f'{ix} beanz'})
        second_dump = create_dump(
            config.dumps_path, 'ecatalogue', date(2025, 1, 3), *test_records
        )

        with use_importer(config) as importer, freeze_time('2025-01-02') as frozen_time:
            view = importer.get_view(name)
            db = importer.get_database(view)

            # import the first dump and check all 8 records are present
            importer.queue_emu_changes()
            importer.add_to_mongo(name)
            database = importer.get_database(importer.get_view(name))
            importer.sync_to_elasticsearch(name)
            first_db_version = database.get_committed_version()
            first_es_version = database.get_elasticsearch_version()
            search_base = database.search()

            assert database.data_collection.count_documents({}) == 8
            assert search_base.count() == 8

            for test_record in test_records:
                mongo_record = database.data_collection.find_one(
                    {'id': test_record['irn']}
                )
                assert mongo_record is not None
                assert len(mongo_record['data']) > 0
                assert 'diffs' not in mongo_record
                assert (
                    search_base.filter(
                        'term',
                        **{keyword('_id'): test_record['irn']},
                    ).count()
                    == 1
                )

            frozen_time.tick(timedelta(days=2))  # advance forward 2 days

            # force update the two invalid records (doing a normal ingest would
            # automatically remove the depublished record)
            source_records = [
                SourceRecord(r['irn'], r, second_dump.name) for r in test_records
            ]
            view.store.put(source_records)
            records = [Record(r.id, view.transform(r)) for r in source_records]
            db.ingest(records, modified_field='modified')
            second_db_version = db.get_committed_version()
            assert second_db_version != first_db_version
            importer.sync_to_elasticsearch(name)
            second_es_version = db.get_elasticsearch_version()
            assert second_es_version != first_es_version

            # check all 8 records are still present and contain data
            assert database.data_collection.count_documents({}) == 8
            assert search_base.count() == 8
            for test_record in test_records:
                mongo_record = database.data_collection.find_one(
                    {'id': test_record['irn']}
                )
                assert mongo_record is not None
                assert len(mongo_record['data']) > 0
                # check the source record has been updated
                source_record = view.store.get_record(mongo_record['id'])
                assert not view.is_publishable_member(source_record)
                assert (
                    search_base.filter(
                        'term',
                        **{keyword('artefactName'): test_record['PalArtObjectName']},
                    ).count()
                    == 1
                )

            frozen_time.tick(timedelta(days=1))  # advance forward 1 day

            # now purge and check that they're all deleted
            non_member_count, non_publishable_count, total_deleted = (
                importer.purge_unsuitable_records(name)
            )
            final_db_version = db.get_committed_version()
            assert final_db_version != second_db_version
            importer.sync_to_elasticsearch(name)
            final_es_version = db.get_elasticsearch_version()
            assert final_es_version != second_es_version
            assert non_member_count == 1
            assert non_publishable_count == 2
            assert total_deleted == 3
            # they should still exist in mongo
            assert database.data_collection.count_documents({}) == 8
            # but they shouldn't have any current data
            for test_record in test_records:
                mongo_record = database.data_collection.find_one(
                    {'id': test_record['irn']}
                )
                assert mongo_record is not None
                assert len(mongo_record['data']) == 0
                assert len(mongo_record['diffs']) == 2
            # they shouldn't exist in current elasticsearch
            assert search_base.count() == 5
            # but they should in the previous versions
            assert database.search(first_es_version).count() == 8
            assert database.search(second_es_version).count() == 8


class TestEMuStatus:
    def test_get_default(self, tmp_path: Path):
        status = EMuStatus(tmp_path / 'status.txt')
        assert status.get() == FIRST_VERSION

    def test_get(self, tmp_path: Path):
        status = EMuStatus(tmp_path / 'status.txt')
        status.path.write_text('23-02-2021', encoding='utf-8')

        assert status.get() == date(2021, 2, 23)

    def test_update_no_file(self, tmp_path: Path):
        status = EMuStatus(tmp_path / 'status.txt')

        status.update(date(2023, 4, 19))

        assert status.path.exists()
        assert status.path.read_text(encoding='utf-8') == '19-04-2023'

    def test_update_with_file(self, tmp_path: Path):
        status = EMuStatus(tmp_path / 'status.txt')
        status.path.write_text('03-11-2023')

        status.update(date(2023, 4, 19))

        assert status.path.exists()
        assert status.path.read_text(encoding='utf-8') == '19-04-2023'

    def test_clear_no_file(self, tmp_path: Path):
        status = EMuStatus(tmp_path / 'status.txt')
        status.clear()
        assert not status.path.exists()

    def test_clear_with_file(self, tmp_path: Path):
        status = EMuStatus(tmp_path / 'status.txt')
        status.path.write_text('17-12-2022')
        status.clear()
        assert not status.path.exists()
