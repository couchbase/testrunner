from pytests.fts.fts_callable import FTSCallable
from pytests.fts.serverless.sanity import FTSElixirSanity
from lib.metering_throttling import metering

class FTSMeterSanity(FTSElixirSanity):
    def setUp(self):
        self.doc_count = 1000
        self.scope = '_default'
        self.collection = '_default'
        return super().setUp()

    def tearDown(self):
        return super().tearDown()

    def suite_setUp(self):
        pass

    def suite_tearDown(self):
        pass

    def test_create_index(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)

            result = self.run_query(database, f'INSERT INTO {self.scope}.{self.collection} (key k, value v) select uuid() as k , {{"name": "San Francisco"}} as v from array_range(0,{self.doc_count}) d')

            self.init_input_servers(database)
            fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=self.scope,
                                       collections=self.collection, collection_index=True)

            _type = FTSElixirSanity.define_index_parameters_collection_related(container_type="collection", scope=self.scope,
                                                                      collection=self.collection)
            plan_params = self.construct_plan_params()

            before_fts_ru, before_fts_wu = meter.get_fts_rwu(database.id)
            fts_idx = fts_callable.create_fts_index("idx", source_type='couchbase',
                                                    source_name=database.id, index_type='fulltext-index',
                                                    index_params=None, plan_params=plan_params,
                                                    source_params=None, source_uuid=None, collection_index=True,
                                                    _type=_type, analyzer="standard",
                                                    scope=self.scope, collections=[self.collection], no_check=False)
            fts_callable.wait_for_indexing_complete(self.doc_count)
            after_fts_ru, after_fts_wu = meter.get_fts_rwu(database.id)

            self.assertEqual(after_fts_ru, before_fts_ru)
            self.assertTrue(after_fts_wu-before_fts_wu > 100 and after_fts_wu-before_fts_wu < 200)
