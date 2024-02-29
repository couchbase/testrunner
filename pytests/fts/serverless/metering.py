from pytests.fts.fts_callable import FTSCallable
from pytests.fts.serverless.sanity import FTSElixirSanity
from lib.metering_throttling import metering
import json
import random, string
import time
import math
from TestInput import TestInputSingleton


class FTSMeter(FTSElixirSanity):
    def setUp(self):
        self.input = TestInputSingleton.input
        self.scope = '_default'
        self.collection = '_default'
        self.doc_count = self.input.param("doc_count", 1000)
        self.value_size = self.input.param("value_size", 256)
        self.doc_values = self.input.param("doc_values", False)
        self.term_vectors = self.input.param("term_vectors", False)
        self.store_fields = self.input.param("store_fields", False)
        self.kv_wu, self.kv_ru = 1024, 4096
        self.fts_wu, self.fts_ru = 1024, 256
        self.doc_value = ''.join(random.choices(string.ascii_lowercase, k=self.value_size))
        self.doc_value2 = ''.join(random.choices(string.ascii_lowercase, k=self.value_size))
        self.composite_doc = {"fname": f'{self.doc_value}', "lname": f'{self.doc_value2}'}
        self.composite_index_key_size = len(self.doc_value) + len(self.doc_value2)
        self.doc_key_size = 36  # use uuid()
        self.composite_doc_size = len(json.dumps(self.composite_doc))
        return super().setUp()

    def tearDown(self):
        return super().tearDown()

    def test_create_index(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)

            result = self.run_query(database, f'INSERT INTO {self.scope}.{self.collection} (key k, value v) select uuid() as k , {self.composite_doc} as v from array_range(0,{self.doc_count}) d')

            self.init_input_servers(database)
            fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=self.scope,
                                       collections=self.collection, collection_index=True, is_elixir=True)

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

            max_fts_wu = self.doc_count * math.ceil((self.doc_key_size + self.composite_index_key_size) / self.fts_wu) * 5 + 2

            self.assertEqual(after_fts_ru, before_fts_ru)
            self.assertTrue(after_fts_wu-before_fts_wu > 0 and after_fts_wu-before_fts_wu <= max_fts_wu, f"The fts index didnt generate WUs within the range, please check. actual:{after_fts_wu-before_fts_wu}, range:0-{max_fts_wu}")

    def test_create_index_single_field(self):
        base_params = {"types": {f"{self.scope}.{self.collection}": {
                    "enabled": True,
                    "dynamic": True,
                    "default_analyzer": "keyword",
                    "properties": {
                    "fname": {
                    "enabled": True,
                    "dynamic": False,
                    "fields": [
                    {
                        "name": "fname",
                        "type": "text",
                        "analyzer": "keyword",
                        "store": False,
                        "index": True,
                        "include_term_vectors": False,
                        "include_in_all": False,
                        "docvalues": False}]}}}}}
        idx_params ={"types": {f"{self.scope}.{self.collection}": {
                    "enabled": True,
                    "dynamic": True,
                    "default_analyzer": "keyword",
                    "properties": {
                    "fname": {
                    "enabled": True,
                    "dynamic": False,
                    "fields": [
                    {
                        "name": "fname",
                        "type": "text",
                        "analyzer": "keyword",
                        "store": self.store_fields,
                        "index": True,
                        "include_term_vectors": self.term_vectors,
                        "include_in_all": False,
                        "docvalues": self.doc_values}]}}}}}
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)

            result = self.run_query(database, f'INSERT INTO {self.scope}.{self.collection} (key k, value v) select uuid() as k , {self.composite_doc} as v from array_range(0,{self.doc_count}) d')

            self.init_input_servers(database)
            fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=self.scope,
                                       collections=self.collection, collection_index=True, is_elixir=True)

            _type = FTSElixirSanity.define_index_parameters_collection_related(container_type="collection", scope=self.scope,
                                                                      collection=self.collection)
            plan_params = self.construct_plan_params()

            before_fts_ru, before_fts_wu = meter.get_fts_rwu(database.id)
            fts_idx = fts_callable.create_fts_index("default", source_type='couchbase',
                                                    source_name=database.id, index_type='fulltext-index',
                                                    index_params=None, plan_params=plan_params,
                                                    source_params=None, source_uuid=None, collection_index=True,
                                                    _type=_type, analyzer="standard",
                                                    scope=self.scope, collections=[self.collection], no_check=False)
            fts_callable.wait_for_indexing_complete(self.doc_count)
            after_fts_ru, after_fts_wu = meter.get_fts_rwu(database.id)
            default_fts_wu = after_fts_wu - before_fts_wu

            before_fts_ru, before_fts_wu = meter.get_fts_rwu(database.id)
            fts_idx = fts_callable.create_fts_index("idx", source_type='couchbase',
                                                    source_name=database.id, index_type='fulltext-index',
                                                    index_params=idx_params, plan_params=plan_params,
                                                    source_params=None, source_uuid=None, collection_index=True,
                                                    _type=_type, analyzer="standard",
                                                    scope=self.scope, collections=[self.collection], no_check=False,specify_fields=True)
            fts_callable.wait_for_indexing_complete(self.doc_count)
            after_fts_ru, after_fts_wu = meter.get_fts_rwu(database.id)

            if self.doc_values or self.term_vectors or self.store_fields:
                before_fts_ru, before_fts_wu = meter.get_fts_rwu(database.id)
                fts_idx = fts_callable.create_fts_index("idx-0", source_type='couchbase',
                                                        source_name=database.id, index_type='fulltext-index',
                                                        index_params=base_params, plan_params=plan_params,
                                                        source_params=None, source_uuid=None, collection_index=True,
                                                        _type=_type, analyzer="standard",
                                                        scope=self.scope, collections=[self.collection], no_check=False)
                fts_callable.wait_for_indexing_complete(self.doc_count)
                after_fts_ru, after_fts_wu = meter.get_fts_rwu(database.id)
                default_fts_wu = after_fts_wu - before_fts_wu

            self.assertEqual(after_fts_ru, before_fts_ru)
            if self.doc_values or self.term_vectors or self.store_fields:
                self.assertTrue(after_fts_wu - before_fts_wu >= default_fts_wu,
                                f"This index should be generating more WUs than default index, index:{after_fts_wu - before_fts_wu} default:{default_fts_wu}")
            else:
                self.assertTrue(after_fts_wu-before_fts_wu <= default_fts_wu, f"This index should not be generating more WUs than default index, index:{after_fts_wu-before_fts_wu} default:{default_fts_wu}")

    def test_create_index_multiple_fields(self):
        base_params ={"types": {f"{self.scope}.{self.collection}": {
                    "enabled": True,
                    "dynamic": True,
                    "default_analyzer": "keyword",
                    "properties": {
                    "fname": {
                    "enabled": True,
                    "dynamic": False,
                    "fields": [
                    {
                        "name": "fname",
                        "type": "text",
                        "analyzer": "keyword",
                        "store": False,
                        "index": True,
                        "include_term_vectors": False,
                        "include_in_all": False,
                        "docvalues": False
                    },
                    {
                        "name": "lname",
                        "type": "text",
                        "analyzer": "keyword",
                        "store": False,
                        "index": True,
                        "include_term_vectors": False,
                        "include_in_all": False,
                        "docvalues": False
                    }
                    ]}}}}}
        idx_params ={"types": {f"{self.scope}.{self.collection}": {
                    "enabled": True,
                    "dynamic": True,
                    "default_analyzer": "keyword",
                    "properties": {
                    "fname": {
                    "enabled": True,
                    "dynamic": False,
                    "fields": [
                    {
                        "name": "fname",
                        "type": "text",
                        "analyzer": "keyword",
                        "store": self.store_fields,
                        "index": True,
                        "include_term_vectors": self.term_vectors,
                        "include_in_all": False,
                        "docvalues": self.doc_values
                    },
                    {
                        "name": "lname",
                        "type": "text",
                        "analyzer": "keyword",
                        "store": self.store_fields,
                        "index": True,
                        "include_term_vectors": self.term_vectors,
                        "include_in_all": False,
                        "docvalues": self.doc_values
                    }
                    ]}}}}}
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)

            result = self.run_query(database, f'INSERT INTO {self.scope}.{self.collection} (key k, value v) select uuid() as k , {self.composite_doc} as v from array_range(0,{self.doc_count}) d')

            self.init_input_servers(database)
            fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=self.scope,
                                       collections=self.collection, collection_index=True, is_elixir=True)

            _type = FTSElixirSanity.define_index_parameters_collection_related(container_type="collection", scope=self.scope,
                                                                      collection=self.collection)
            plan_params = self.construct_plan_params()

            before_fts_ru, before_fts_wu = meter.get_fts_rwu(database.id)
            fts_idx = fts_callable.create_fts_index("default", source_type='couchbase',
                                                    source_name=database.id, index_type='fulltext-index',
                                                    index_params=None, plan_params=plan_params,
                                                    source_params=None, source_uuid=None, collection_index=True,
                                                    _type=_type, analyzer="standard",
                                                    scope=self.scope, collections=[self.collection], no_check=False)
            fts_callable.wait_for_indexing_complete(self.doc_count)
            after_fts_ru, after_fts_wu = meter.get_fts_rwu(database.id)
            default_fts_wu = after_fts_wu - before_fts_wu

            before_fts_ru, before_fts_wu = meter.get_fts_rwu(database.id)
            fts_idx = fts_callable.create_fts_index("idx", source_type='couchbase',
                                                    source_name=database.id, index_type='fulltext-index',
                                                    index_params=idx_params, plan_params=plan_params,
                                                    source_params=None, source_uuid=None, collection_index=True,
                                                    _type=_type, analyzer="standard",
                                                    scope=self.scope, collections=[self.collection], no_check=False,specify_fields=True)
            fts_callable.wait_for_indexing_complete(self.doc_count)
            after_fts_ru, after_fts_wu = meter.get_fts_rwu(database.id)

            if self.doc_values or self.term_vectors or self.store_fields:
                before_fts_ru, before_fts_wu = meter.get_fts_rwu(database.id)
                fts_idx = fts_callable.create_fts_index("idx-0", source_type='couchbase',
                                                        source_name=database.id, index_type='fulltext-index',
                                                        index_params=base_params, plan_params=plan_params,
                                                        source_params=None, source_uuid=None, collection_index=True,
                                                        _type=_type, analyzer="standard",
                                                        scope=self.scope, collections=[self.collection], no_check=False)
                fts_callable.wait_for_indexing_complete(self.doc_count)
                after_fts_ru, after_fts_wu = meter.get_fts_rwu(database.id)
                default_fts_wu = after_fts_wu - before_fts_wu

            self.assertEqual(after_fts_ru, before_fts_ru)
            if self.doc_values or self.term_vectors or self.store_fields:
                self.assertTrue(after_fts_wu - before_fts_wu >= default_fts_wu,
                                f"This index should be generating more WUs than default index, index:{after_fts_wu - before_fts_wu} default:{default_fts_wu}")
            else:
                self.assertTrue(after_fts_wu-before_fts_wu <= default_fts_wu, f"This index should not be generating more WUs than default index, index:{after_fts_wu-before_fts_wu} default:{default_fts_wu}")

    def test_geospatial_create(self):
        num_travel_sample_docs = 1968
        self.scope = "samples"
        self.collection = "airport"
        idx_params = {"types": {f"{self.scope}.{self.collection}": {
            "enabled": True,
            "dynamic": True,
            "default_analyzer": "keyword",
            "properties": {
                "geo": {
                    "enabled": True,
                    "dynamic": False,
                    "fields": [
                        {
                            "name": "geo",
                            "type": "geopoint",
                            "analyzer": "keyword",
                            "store": False,
                            "index": True,
                            "include_term_vectors": False,
                            "include_in_all": True,
                            "docvalues": False}]}}}}}
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)

            self.init_input_servers(database)
            fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=self.scope,
                                       collections=self.collection, collection_index=True, is_elixir=True)

            _type = FTSElixirSanity.define_index_parameters_collection_related(container_type="collection",
                                                                               scope=self.scope,
                                                                               collection=self.collection)
            plan_params = self.construct_plan_params()
            before_fts_ru, before_fts_wu = meter.get_fts_rwu(database.id)
            fts_idx = fts_callable.create_fts_index("idx", source_type='couchbase',
                                                    source_name=database.id, index_type='fulltext-index',
                                                    index_params=idx_params, plan_params=plan_params,
                                                    source_params=None, source_uuid=None, collection_index=True,
                                                    _type=_type, analyzer="standard",
                                                    scope=self.scope, collections=[self.collection], no_check=False,
                                                    specify_fields=True)
            fts_callable.wait_for_indexing_complete(num_travel_sample_docs)
            time.sleep(30)
            after_fts_ru, after_fts_wu = meter.get_fts_rwu(database.id)
            self.assertTrue(after_fts_wu - before_fts_wu > 0 and after_fts_wu - before_fts_wu < 5000 ,
                            f"The fts index didnt generate WUs within the range, please check. actual:{after_fts_wu - before_fts_wu}, range:0-5000")

    def test_search_index(self):
        self.provision_databases()
        for database in self.databases.values():
            self.init_input_servers(database)
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            result = self.run_query(database, f'INSERT INTO {self.scope}.{self.collection} (key k, value v) select uuid() as k , {self.composite_doc} as v from array_range(0,{self.doc_count}) d')
            fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=self.scope,
                                       collections=self.collection, collection_index=True, is_elixir=True)

            _type = FTSElixirSanity.define_index_parameters_collection_related(container_type="collection", scope=self.scope,
                                                                      collection=self.collection)
            plan_params = self.construct_plan_params()

            fts_idx = fts_callable.create_fts_index("idx", source_type='couchbase',
                                                    source_name=database.id, index_type='fulltext-index',
                                                    index_params=None, plan_params=plan_params,
                                                    source_params=None, source_uuid=None, collection_index=True,
                                                    _type=_type, analyzer="keyword",
                                                    scope=self.scope, collections=[self.collection], no_check=False)

            fts_callable.wait_for_indexing_complete(self.doc_count)
            before_fts_ru, before_fts_wu = meter.get_fts_rwu(database.id)
            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)

            for index in fts_callable.cb_cluster.get_indexes():
                query = {"match": f"{self.doc_value}", "field": "fname"}
                if self.doc_count <= 10:
                    expected_results = self.doc_count
                else:
                    expected_results = 10
                hits, matches, _, _ = index.execute_query(query,
                                                          zero_results_ok=False,
                                                          expected_hits=self.doc_count,
                                                          expected_no_of_results=expected_results)
                self.log.info("Hits: %s" % hits)
                self.log.info("Matches: %s" % matches)
                after_fts_ru, after_fts_wu = meter.get_fts_rwu(database.id)
                after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)
                self.assertEqual(after_kv_ru, before_kv_ru)
                self.assertEqual(after_kv_wu, before_kv_wu)
                self.assertEqual(after_fts_wu, before_fts_wu)
                max_fts_ru = self.doc_count * math.ceil((self.doc_key_size + (self.composite_index_key_size) / self.fts_ru))
                self.assertTrue(after_fts_ru - before_fts_ru > 0 and after_fts_ru - before_fts_ru < max_fts_ru, f"The fts index didnt generate RUs within the range, please check. actual:{after_fts_ru-before_fts_ru}, range:0-{max_fts_ru}")
                before_fts_ru = after_fts_ru
                before_kv_ru = after_kv_ru
                before_fts_wu = after_fts_wu
                before_kv_wu = after_kv_wu

    def test_search_index_single_field(self):
        idx_params ={"types": {f"{self.scope}.{self.collection}": {
                    "enabled": True,
                    "dynamic": True,
                    "default_analyzer": "keyword",
                    "properties": {
                    "fname": {
                    "enabled": True,
                    "dynamic": False,
                    "fields": [
                    {
                        "name": "fname",
                        "type": "text",
                        "analyzer": "keyword",
                        "store": False,
                        "index": True,
                        "include_term_vectors": False,
                        "include_in_all": False,
                        "docvalues": False}]}}}}}
        self.provision_databases()
        for database in self.databases.values():
            self.init_input_servers(database)
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            result = self.run_query(database, f'INSERT INTO {self.scope}.{self.collection} (key k, value v) select uuid() as k , {self.composite_doc} as v from array_range(0,{self.doc_count}) d')
            fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=self.scope,
                                       collections=self.collection, collection_index=True, is_elixir=True)

            _type = FTSElixirSanity.define_index_parameters_collection_related(container_type="collection", scope=self.scope,
                                                                      collection=self.collection)
            plan_params = self.construct_plan_params()

            fts_idx = fts_callable.create_fts_index("idx", source_type='couchbase',
                                                    source_name=database.id, index_type='fulltext-index',
                                                    index_params=idx_params, plan_params=plan_params,
                                                    source_params=None, source_uuid=None, collection_index=True,
                                                    _type=_type, analyzer="keyword",
                                                    scope=self.scope, collections=[self.collection], no_check=False,specify_fields=True)

            fts_callable.wait_for_indexing_complete(self.doc_count)
            before_fts_ru, before_fts_wu = meter.get_fts_rwu(database.id)
            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)

            for index in fts_callable.cb_cluster.get_indexes():
                query = {"match": f"{self.doc_value}", "field": "fname"}
                if self.doc_count <= 10:
                    expected_results = self.doc_count
                else:
                    expected_results = 10
                hits, matches, _, _ = index.execute_query(query,
                                                          zero_results_ok=False,
                                                          expected_hits=self.doc_count,
                                                          expected_no_of_results=expected_results)
                self.log.info("Hits: %s" % hits)
                self.log.info("Matches: %s" % matches)
                after_fts_ru, after_fts_wu = meter.get_fts_rwu(database.id)
                after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)
                self.assertEqual(after_kv_ru, before_kv_ru)
                self.assertEqual(after_kv_wu, before_kv_wu)
                self.assertEqual(after_fts_wu, before_fts_wu)
                max_fts_ru = self.doc_count * math.ceil((self.doc_key_size + ((self.composite_index_key_size)/2) / self.fts_ru))
                self.assertTrue(after_fts_ru - before_fts_ru > 0 and after_fts_ru - before_fts_ru <= max_fts_ru, f"The fts index didnt generate RUs within the range, please check. actual:{after_fts_ru-before_fts_ru}, range:0-{max_fts_ru}")
                before_fts_ru = after_fts_ru
                before_kv_ru = after_kv_ru
                before_fts_wu = after_fts_wu
                before_kv_wu = after_kv_wu

    def test_search_index_multiple_fields(self):
        idx_params ={"types": {f"{self.scope}.{self.collection}": {
                    "enabled": True,
                    "dynamic": True,
                    "default_analyzer": "keyword",
                    "properties": {
                    "fname": {
                    "enabled": True,
                    "dynamic": False,
                    "fields": [
                    {
                        "name": "fname",
                        "type": "text",
                        "analyzer": "keyword",
                        "store": False,
                        "index": True,
                        "include_term_vectors": False,
                        "include_in_all": False,
                        "docvalues": False
                    },
                    {
                        "name": "lname",
                        "type": "text",
                        "analyzer": "keyword",
                        "store": False,
                        "index": True,
                        "include_term_vectors": False,
                        "include_in_all": False,
                        "docvalues": False
                    }
                    ]}}}}}
        self.provision_databases()
        for database in self.databases.values():
            self.init_input_servers(database)
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            result = self.run_query(database, f'INSERT INTO {self.scope}.{self.collection} (key k, value v) select uuid() as k , {self.composite_doc} as v from array_range(0,{self.doc_count}) d')
            fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=self.scope,
                                       collections=self.collection, collection_index=True, is_elixir=True)

            _type = FTSElixirSanity.define_index_parameters_collection_related(container_type="collection", scope=self.scope,
                                                                      collection=self.collection)
            plan_params = self.construct_plan_params()

            fts_idx = fts_callable.create_fts_index("idx", source_type='couchbase',
                                                    source_name=database.id, index_type='fulltext-index',
                                                    index_params=idx_params, plan_params=plan_params,
                                                    source_params=None, source_uuid=None, collection_index=True,
                                                    _type=_type, analyzer="keyword",
                                                    scope=self.scope, collections=[self.collection], no_check=False,specify_fields=True)

            fts_callable.wait_for_indexing_complete(self.doc_count)
            before_fts_ru, before_fts_wu = meter.get_fts_rwu(database.id)
            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)

            for index in fts_callable.cb_cluster.get_indexes():
                query ={"conjuncts":[
                        {"match": f"{self.doc_value}", "field": "fname"},
                        {"match": f"{self.doc_value2}", "field": "lname"}]}
                if self.doc_count <= 10:
                    expected_results = self.doc_count
                else:
                    expected_results = 10
                hits, matches, _, _ = index.execute_query(query,
                                                          zero_results_ok=False,
                                                          expected_hits=self.doc_count,
                                                          expected_no_of_results=expected_results)
                self.log.info("Hits: %s" % hits)
                self.log.info("Matches: %s" % matches)
                after_fts_ru, after_fts_wu = meter.get_fts_rwu(database.id)
                after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)
                self.assertEqual(after_kv_ru, before_kv_ru)
                self.assertEqual(after_kv_wu, before_kv_wu)
                self.assertEqual(after_fts_wu, before_fts_wu)
                max_fts_ru = self.doc_count * math.ceil((self.doc_key_size + (self.composite_index_key_size) / self.fts_ru))
                self.assertTrue(after_fts_ru - before_fts_ru > 0 and after_fts_ru - before_fts_ru <= max_fts_ru, f"The fts index didnt generate RUs within the range, please check. actual:{after_fts_ru-before_fts_ru}, range:0-{max_fts_ru}")
                before_fts_ru = after_fts_ru
                before_kv_ru = after_kv_ru
                before_fts_wu = after_fts_wu
                before_kv_wu = after_kv_wu

    def test_search_geospatial(self):
        num_travel_sample_docs = 1968
        self.scope = "samples"
        self.collection = "airport"
        idx_params = {"types": {f"{self.scope}.{self.collection}": {
            "enabled": True,
            "dynamic": True,
            "default_analyzer": "keyword",
            "properties": {
                "geo": {
                    "enabled": True,
                    "dynamic": False,
                    "fields": [
                        {
                            "name": "geo",
                            "type": "geopoint",
                            "analyzer": "keyword",
                            "store": False,
                            "index": True,
                            "include_term_vectors": False,
                            "include_in_all": True,
                            "docvalues": False}]}}}}}
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)

            self.init_input_servers(database)
            fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=self.scope,
                                       collections=self.collection, collection_index=True, is_elixir=True)

            _type = FTSElixirSanity.define_index_parameters_collection_related(container_type="collection",
                                                                               scope=self.scope,
                                                                               collection=self.collection)
            plan_params = self.construct_plan_params()

            fts_idx = fts_callable.create_fts_index("idx", source_type='couchbase',
                                                    source_name=database.id, index_type='fulltext-index',
                                                    index_params=idx_params, plan_params=plan_params,
                                                    source_params=None, source_uuid=None, collection_index=True,
                                                    _type=_type, analyzer="standard",
                                                    scope=self.scope, collections=[self.collection], no_check=False,
                                                    specify_fields=True)
            fts_callable.wait_for_indexing_complete(num_travel_sample_docs)
            time.sleep(30)
            before_fts_ru, before_fts_wu = meter.get_fts_rwu(database.id)
            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)

            for index in fts_callable.cb_cluster.get_indexes():
                query = {"top_left": [-2.235143, 53.482358],
                                  "bottom_right": {"lon": 28.955043,"lat": 40.991862},
                         "field": "geo"}
                hits, matches, _, _ = index.execute_query(query,
                                                          zero_results_ok=False,
                                                          expected_hits=287,
                                                          expected_no_of_results=10)
                self.log.info("Hits: %s" % hits)
                self.log.info("Matches: %s" % matches)
                after_fts_ru, after_fts_wu = meter.get_fts_rwu(database.id)
                after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)
                self.assertEqual(after_kv_wu, before_kv_wu)
                self.assertEqual(after_fts_wu, before_fts_wu)
                self.assertTrue(after_fts_ru - before_fts_ru > 0 and after_fts_ru - before_fts_ru <= 2600, f"The fts index didnt generate RUs within the range, please check. actual:{after_fts_ru-before_fts_ru}, range:0-2600")
                before_fts_ru = after_fts_ru
                before_kv_ru = after_kv_ru
                before_fts_wu = after_fts_wu
                before_kv_wu = after_kv_wu

    def test_search_highlight(self):
        num_travel_sample_docs = 917
        self.scope = "samples"
        self.collection = "hotel"
        idx_params = {"types": {f"{self.scope}.{self.collection}": {
            "enabled": True,
            "dynamic": True,
            "default_analyzer": "standard",
            "properties": {
                "description": {
                    "enabled": True,
                    "dynamic": False,
                    "fields": [
                        {
                            "name": "description",
                            "type": "text",
                            "analyzer": "standard",
                            "store": True,
                            "index": True,
                            "include_term_vectors": True,
                            "include_in_all": False,
                            "docvalues": False}]}}}}}
        self.provision_databases()
        for database in self.databases.values():
            self.init_input_servers(database)
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=self.scope,
                                       collections=self.collection, collection_index=True, is_elixir=True)

            _type = FTSElixirSanity.define_index_parameters_collection_related(container_type="collection",
                                                                               scope=self.scope,
                                                                               collection=self.collection)
            plan_params = self.construct_plan_params()

            fts_idx = fts_callable.create_fts_index("idx", source_type='couchbase',
                                                    source_name=database.id, index_type='fulltext-index',
                                                    index_params=idx_params, plan_params=plan_params,
                                                    source_params=None, source_uuid=None, collection_index=True,
                                                    _type=_type, analyzer="keyword",
                                                    scope=self.scope, collections=[self.collection], no_check=False,specify_fields=True)

            fts_callable.wait_for_indexing_complete(num_travel_sample_docs)
            before_fts_ru, before_fts_wu = meter.get_fts_rwu(database.id)
            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)

            for index in fts_callable.cb_cluster.get_indexes():
                query = {"query": "complementary breakfast"}

                hits, matches, _, _ = index.execute_query(query,
                                                          zero_results_ok=False,
                                                          expected_hits=573,
                                                          expected_no_of_results=10,
                                                          highlight=True,
                                                          highlight_style="html",
                                                          highlight_fields=["description"])
                self.log.info("Hits: %s" % hits)
                after_fts_ru, after_fts_wu = meter.get_fts_rwu(database.id)
                after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)
                self.assertEqual(after_kv_ru, before_kv_ru)
                self.assertEqual(after_kv_wu, before_kv_wu)
                self.assertEqual(after_fts_wu, before_fts_wu)
                #35968 is the size of the biggest doc in travel-sample so using that value
                max_fts_ru = self.doc_count * math.ceil((self.doc_key_size + (35968) / self.fts_ru))
                self.assertTrue(after_fts_ru - before_fts_ru > 0 and after_fts_ru - before_fts_ru < max_fts_ru, f"The fts index didnt generate RUs within the range, please check. actual:{after_fts_ru-before_fts_ru}, range:0-{max_fts_ru}")
                before_fts_ru = after_fts_ru
                before_kv_ru = after_kv_ru
                before_fts_wu = after_fts_wu
                before_kv_wu = after_kv_wu

    def test_modify_fts_index(self):
        idx_params ={"types": {f"{self.scope}.{self.collection}": {
                    "enabled": True,
                    "dynamic": True,
                    "default_analyzer": "keyword",
                    "properties": {
                    "fname": {
                    "enabled": True,
                    "dynamic": False,
                    "fields": [
                    {
                        "name": "fname",
                        "type": "text",
                        "analyzer": "keyword",
                        "store": self.store_fields,
                        "index": True,
                        "include_term_vectors": self.term_vectors,
                        "include_in_all": False,
                        "docvalues": self.doc_values}]}}}}}
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)

            result = self.run_query(database, f'INSERT INTO {self.scope}.{self.collection} (key k, value v) select uuid() as k , {self.composite_doc} as v from array_range(0,{self.doc_count}) d')

            self.init_input_servers(database)
            fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=self.scope,
                                       collections=self.collection, collection_index=True, is_elixir=True)

            _type = FTSElixirSanity.define_index_parameters_collection_related(container_type="collection", scope=self.scope,
                                                                      collection=self.collection)
            plan_params = self.construct_plan_params()

            before_fts_ru, before_fts_wu = meter.get_fts_rwu(database.id)
            fts_idx = fts_callable.create_fts_index("idx", source_type='couchbase',
                                                    source_name=database.id, index_type='fulltext-index',
                                                    index_params=idx_params, plan_params=plan_params,
                                                    source_params=None, source_uuid=None, collection_index=True,
                                                    _type=_type, analyzer="standard",
                                                    scope=self.scope, collections=[self.collection], no_check=False,specify_fields=True)
            fts_callable.wait_for_indexing_complete(self.doc_count)
            after_fts1_ru, after_fts1_wu = meter.get_fts_rwu(database.id)
            original_fts_wu = after_fts1_wu - before_fts_wu

            fts_idx.index_definition['uuid'] = fts_idx.get_uuid()
            fts_idx.index_definition['params']['mapping']['types'] = {f"{self.scope}.{self.collection}": {
                    "enabled": True,
                    "dynamic": True,
                    "default_analyzer": "keyword",
                    "properties": {
                    "fname": {
                    "enabled": True,
                    "dynamic": False,
                    "fields": [
                    {
                        "name": "fname",
                        "type": "text",
                        "analyzer": "keyword",
                        "store": False,
                        "index": True,
                        "include_term_vectors": False,
                        "include_in_all": False,
                        "docvalues": False
                    },
                    {
                        "name": "lname",
                        "type": "text",
                        "analyzer": "keyword",
                        "store": False,
                        "index": True,
                        "include_term_vectors": False,
                        "include_in_all": False,
                        "docvalues": False
                    }
                    ]}}}}
            fts_idx.update()
            fts_callable.wait_for_indexing_complete(self.doc_count)
            after_fts_ru, after_fts_wu = meter.get_fts_rwu(database.id)
            self.assertEqual(after_fts_ru, after_fts1_ru)
            self.assertTrue(after_fts_wu-after_fts1_wu > 0 and after_fts_wu-after_fts1_wu < (original_fts_wu+15), f"The WUs should be less than original cost of creation, please check original cost:{original_fts_wu}, new cost:{after_fts_wu-after_fts1_wu}")

    def test_recreate_fts_index(self):
        idx_params = {"types": {f"{self.scope}.{self.collection}": {
            "enabled": True,
            "dynamic": True,
            "default_analyzer": "keyword",
            "properties": {
                "fname": {
                    "enabled": True,
                    "dynamic": False,
                    "fields": [
                        {
                            "name": "fname",
                            "type": "text",
                            "analyzer": "keyword",
                            "store": self.store_fields,
                            "index": True,
                            "include_term_vectors": self.term_vectors,
                            "include_in_all": False,
                            "docvalues": self.doc_values}]}}}}}
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)

            result = self.run_query(database,
                                    f'INSERT INTO {self.scope}.{self.collection} (key k, value v) select uuid() as k , {self.composite_doc} as v from array_range(0,{self.doc_count}) d')

            self.init_input_servers(database)
            fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=self.scope,
                                       collections=self.collection, collection_index=True, is_elixir=True)

            _type = FTSElixirSanity.define_index_parameters_collection_related(container_type="collection",
                                                                               scope=self.scope,
                                                                               collection=self.collection)
            plan_params = self.construct_plan_params()

            before_fts_ru, before_fts_wu = meter.get_fts_rwu(database.id)
            fts_idx = fts_callable.create_fts_index("idx", source_type='couchbase',
                                                    source_name=database.id, index_type='fulltext-index',
                                                    index_params=idx_params, plan_params=plan_params,
                                                    source_params=None, source_uuid=None, collection_index=True,
                                                    _type=_type, analyzer="standard",
                                                    scope=self.scope, collections=[self.collection], no_check=False,
                                                    specify_fields=True)
            fts_callable.wait_for_indexing_complete(self.doc_count)
            after_fts1_ru, after_fts1_wu = meter.get_fts_rwu(database.id)
            original_fts_wu = after_fts1_wu - before_fts_wu

            fts_idx.index_definition['uuid'] = fts_idx.get_uuid()
            fts_idx.index_definition['params']['mapping']['types'] = {f"{self.scope}.{self.collection}": {
                "enabled": True,
                "dynamic": True,
                "default_analyzer": "keyword",
                "properties": {
                    "lname": {
                        "enabled": True,
                        "dynamic": False,
                        "fields": [
                            {
                                "name": "lname",
                                "type": "text",
                                "analyzer": "keyword",
                                "store": False,
                                "index": True,
                                "include_term_vectors": False,
                                "include_in_all": False,
                                "docvalues": False
                            }
                        ]}}}}
            fts_idx.update()
            fts_callable.wait_for_indexing_complete(self.doc_count)
            after_fts_ru, after_fts_wu = meter.get_fts_rwu(database.id)
            self.assertEqual(after_fts_ru, after_fts1_ru)
            self.assertTrue(after_fts_wu-after_fts1_wu > 0 and after_fts_wu-after_fts1_wu < (original_fts_wu+5), f"The WUs should be less than the original cost of creation, please check original cost:{original_fts_wu}, new cost:{after_fts_wu-after_fts1_wu}")

    def test_fts_insert(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)

            result = self.run_query(database, f'INSERT INTO {self.scope}.{self.collection} (key k, value v) select uuid() as k , {self.composite_doc} as v from array_range(0,{self.doc_count}) d')

            self.init_input_servers(database)
            fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=self.scope,
                                       collections=self.collection, collection_index=True, is_elixir=True)

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
            before_fts_ru, before_fts_wu = meter.get_fts_rwu(database.id)

            self.new_doc = {"fname": ''.join(random.choices(string.ascii_lowercase, k=self.value_size))}
            insert_query = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {self.new_doc} as v FROM array_range(0,{self.doc_count}) d'
            self.run_query(database,insert_query)
            time.sleep(30)
            after_fts_ru, after_fts_wu = meter.get_fts_rwu(database.id)

            self.assertEqual(after_fts_ru, before_fts_ru)
            self.assertTrue(after_fts_wu-before_fts_wu > 0 and after_fts_wu-before_fts_wu < 50)

    def test_fts_insert_single_field(self):
        idx_params ={"types": {f"{self.scope}.{self.collection}": {
                    "enabled": True,
                    "dynamic": True,
                    "default_analyzer": "keyword",
                    "properties": {
                    "fname": {
                    "enabled": True,
                    "dynamic": False,
                    "fields": [
                    {
                        "name": "fname",
                        "type": "text",
                        "analyzer": "keyword",
                        "store": self.store_fields,
                        "index": True,
                        "include_term_vectors": self.term_vectors,
                        "include_in_all": False,
                        "docvalues": self.doc_values}]}}}}}
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)

            result = self.run_query(database, f'INSERT INTO {self.scope}.{self.collection} (key k, value v) select uuid() as k , {self.composite_doc} as v from array_range(0,{self.doc_count}) d')

            self.init_input_servers(database)
            fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=self.scope,
                                       collections=self.collection, collection_index=True, is_elixir=True)

            _type = FTSElixirSanity.define_index_parameters_collection_related(container_type="collection", scope=self.scope,
                                                                      collection=self.collection)
            plan_params = self.construct_plan_params()

            before_fts_ru, before_fts_wu = meter.get_fts_rwu(database.id)
            fts_idx = fts_callable.create_fts_index("idx", source_type='couchbase',
                                                    source_name=database.id, index_type='fulltext-index',
                                                    index_params=idx_params, plan_params=plan_params,
                                                    source_params=None, source_uuid=None, collection_index=True,
                                                    _type=_type, analyzer="standard",
                                                    scope=self.scope, collections=[self.collection], no_check=False,specify_fields=True)
            fts_callable.wait_for_indexing_complete(self.doc_count)
            before_fts_ru, before_fts_wu = meter.get_fts_rwu(database.id)

            self.new_doc = {"fname": ''.join(random.choices(string.ascii_lowercase, k=self.value_size))}
            insert_query = f'INSERT INTO {self.collection} (key k, value v) SELECT uuid() as k , {self.new_doc} as v FROM array_range(0,{self.doc_count}) d'
            self.run_query(database,insert_query)
            time.sleep(30)
            after_fts_ru, after_fts_wu = meter.get_fts_rwu(database.id)

            self.assertEqual(after_fts_ru, before_fts_ru)
            self.assertTrue(after_fts_wu-before_fts_wu > 0 and after_fts_wu-before_fts_wu < 70, f"The WUs are not in the expected range of 0-70, please check {after_fts_wu - before_fts_wu}")

    def test_fts_update(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)

            result = self.run_query(database, f'INSERT INTO {self.scope}.{self.collection} (key k, value v) select uuid() as k , {self.composite_doc} as v from array_range(0,{self.doc_count}) d')

            self.init_input_servers(database)
            fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=self.scope,
                                       collections=self.collection, collection_index=True, is_elixir=True)

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
            before_fts_ru, before_fts_wu = meter.get_fts_rwu(database.id)

            new_value = ''.join(random.choices(string.ascii_lowercase, k=self.value_size))
            insert_query = f'UPDATE {self.collection} SET mname = "{new_value}"'
            self.run_query(database,insert_query)
            time.sleep(30)
            after_fts_ru, after_fts_wu = meter.get_fts_rwu(database.id)

            self.assertEqual(after_fts_ru, before_fts_ru)
            self.assertTrue(after_fts_wu-before_fts_wu > 0 and after_fts_wu-before_fts_wu < 1020)

    def test_fts_update_single_field(self):
        idx_params ={"types": {f"{self.scope}.{self.collection}": {
                    "enabled": True,
                    "dynamic": True,
                    "default_analyzer": "keyword",
                    "properties": {
                    "fname": {
                    "enabled": True,
                    "dynamic": False,
                    "fields": [
                    {
                        "name": "fname",
                        "type": "text",
                        "analyzer": "keyword",
                        "store": self.store_fields,
                        "index": True,
                        "include_term_vectors": self.term_vectors,
                        "include_in_all": False,
                        "docvalues": self.doc_values}]}}}}}
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)

            result = self.run_query(database, f'INSERT INTO {self.scope}.{self.collection} (key k, value v) select uuid() as k , {self.composite_doc} as v from array_range(0,{self.doc_count}) d')

            self.init_input_servers(database)
            fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=self.scope,
                                       collections=self.collection, collection_index=True, is_elixir=True)

            _type = FTSElixirSanity.define_index_parameters_collection_related(container_type="collection", scope=self.scope,
                                                                      collection=self.collection)
            plan_params = self.construct_plan_params()

            before_fts_ru, before_fts_wu = meter.get_fts_rwu(database.id)
            fts_idx = fts_callable.create_fts_index("idx", source_type='couchbase',
                                                    source_name=database.id, index_type='fulltext-index',
                                                    index_params=idx_params, plan_params=plan_params,
                                                    source_params=None, source_uuid=None, collection_index=True,
                                                    _type=_type, analyzer="standard",
                                                    scope=self.scope, collections=[self.collection], no_check=False,specify_fields=True)
            fts_callable.wait_for_indexing_complete(self.doc_count)
            before_fts_ru, before_fts_wu = meter.get_fts_rwu(database.id)

            new_value = ''.join(random.choices(string.ascii_lowercase, k=self.value_size))
            insert_query = f'UPDATE {self.collection} SET fname = "{new_value}" where fname = "{self.doc_value}"'
            self.run_query(database,insert_query)
            time.sleep(30)
            after_fts_ru, after_fts_wu = meter.get_fts_rwu(database.id)

            self.assertEqual(after_fts_ru, before_fts_ru)
            self.assertTrue(after_fts_wu-before_fts_wu > 0 and after_fts_wu-before_fts_wu < 500 )

    def test_fts_delete(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)

            result = self.run_query(database, f'INSERT INTO {self.scope}.{self.collection} (key k, value v) select uuid() as k , {self.composite_doc} as v from array_range(0,{self.doc_count}) d')

            self.init_input_servers(database)
            fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=self.scope,
                                       collections=self.collection, collection_index=True, is_elixir=True)

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
            before_fts_ru, before_fts_wu = meter.get_fts_rwu(database.id)

            insert_query = f'DELETE FROM {self.collection} LIMIT 100'
            self.run_query(database,insert_query)
            time.sleep(30)
            after_fts_ru, after_fts_wu = meter.get_fts_rwu(database.id)

            for index in fts_callable.cb_cluster.get_indexes():
                query = {"match": f"{self.doc_value}", "field": "fname"}
                if self.doc_count <= 10:
                    expected_results = self.doc_count
                else:
                    expected_results = 10
                hits, matches, _, _ = index.execute_query(query,
                                                          zero_results_ok=False,
                                                          expected_hits=900,
                                                          expected_no_of_results=expected_results)
            self.assertEqual(after_fts_ru, before_fts_ru)
            self.assertEqual(after_fts_wu, before_fts_wu)

    def test_n1ql_search_meter(self):
        self.provision_databases(self.num_databases)
        for counter, database in enumerate(self.databases.values()):
            try:
                meter = metering(database.rest_host, database.admin_username, database.admin_password)
                self.cleanup_database(database_obj=database)
                scope_name = f'db_{counter}_scope_{random.randint(0, 1000)}'
                collection_name = f'db_{counter}_collection_{random.randint(0, 1000)}'
                self.create_scope(database_obj=database, scope_name=scope_name)
                self.create_collection(database_obj=database, scope_name=scope_name, collection_name=collection_name)
                self.load_databases(load_all_databases=False, num_of_docs=self.num_of_docs_per_collection,
                                    database_obj=database, scope=scope_name, collection=collection_name)
                self.init_input_servers(database)
                fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=scope_name,
                                           collections=collection_name, collection_index=True, is_elixir=True)

                _type = self.define_index_parameters_collection_related(container_type="collection", scope=scope_name,
                                                                        collection=collection_name)
                plan_params = self.construct_plan_params()
                fts_idx = fts_callable.create_fts_index("idx", source_type='couchbase',
                                                        source_name=database.id, index_type='fulltext-index',
                                                        index_params=None, plan_params=plan_params,
                                                        source_params=None, source_uuid=None, collection_index=True,
                                                        _type=_type, analyzer="standard",
                                                        scope=scope_name, collections=[collection_name], no_check=False)

                fts_callable.wait_for_indexing_complete(self.num_of_docs_per_collection)

                docs_indexed = fts_idx.get_indexed_doc_count()
                container_doc_count = self.num_of_docs_per_collection
                self.log.info(f"Docs in index {fts_idx.name}={docs_indexed}, kv docs={container_doc_count}")
                n1ql_search_query = {"explain": False, "fields": ["*"],"highlight": {},
                                     "query": {"match": "Algeria",
                                               "field": "country",
                                               "analyzer": "standard"},
                                     "sort": [{"by" : "field",
                                               "field" : "streetAddress"}]
                                     }
                before_fts_ru, before_fts_wu = meter.get_fts_rwu(database.id)

                n1ql_search_result = self.run_query(database,
                                        f'SELECT t1.age,t1.city,t1.country,t1.firstName,t1.lastName,t1.streetAddress,t1.suffix,t1.title from {scope_name}.{collection_name} as t1 WHERE SEARCH(t1, {n1ql_search_query}) and t1.age > 50 ORDER BY t1.streetAddress')
                after_fts_ru, after_fts_wu = meter.get_fts_rwu(database.id)
                self.assertTrue(after_fts_ru-before_fts_ru > 0 and after_fts_ru-before_fts_ru < 5000, f"Read unit should be greater than 0 and less than 5000 {after_fts_ru-before_fts_ru}")
            finally:
                fts_callable.delete_all()

    def test_n1ql_flex_meter(self):
        self.provision_databases(self.num_databases)
        for counter, database in enumerate(self.databases.values()):
            try:
                meter = metering(database.rest_host, database.admin_username, database.admin_password)
                self.cleanup_database(database_obj=database)
                scope_name = f'db_{counter}_scope_{random.randint(0, 1000)}'
                collection_name = f'db_{counter}_collection_{random.randint(0, 1000)}'
                self.create_scope(database_obj=database, scope_name=scope_name)
                self.create_collection(database_obj=database, scope_name=scope_name, collection_name=collection_name)
                self.load_databases(load_all_databases=False, num_of_docs=self.num_of_docs_per_collection,
                                    database_obj=database, scope=scope_name, collection=collection_name)
                self.init_input_servers(database)
                fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=scope_name,
                                           collections=collection_name, collection_index=True, is_elixir=True)

                _type = self.define_index_parameters_collection_related(container_type="collection", scope=scope_name,
                                                                        collection=collection_name)
                plan_params = self.construct_plan_params()
                fts_idx = fts_callable.create_fts_index("idx", source_type='couchbase',
                                                        source_name=database.id, index_type='fulltext-index',
                                                        index_params=None, plan_params=plan_params,
                                                        source_params=None, source_uuid=None, collection_index=True,
                                                        _type=_type, analyzer="keyword",
                                                        scope=scope_name, collections=[collection_name], no_check=False)
                fts_callable.wait_for_indexing_complete(self.num_of_docs_per_collection)
                docs_indexed = fts_idx.get_indexed_doc_count()
                container_doc_count = self.num_of_docs_per_collection
                self.log.info(f"Docs in index {fts_idx.name}={docs_indexed}, kv docs={container_doc_count}")

                before_fts_ru, before_fts_wu = meter.get_fts_rwu(database.id)
                flex_result = self.run_query(database,
                                             f'SELECT t1.age,t1.city,t1.country,t1.firstName,t1.lastName,t1.streetAddress,t1.suffix,t1.title from {scope_name}.{collection_name} as t1 USE INDEX (USING FTS, USING GSI) WHERE t1.country = "Algeria" ORDER BY t1.streetAddress limit 5')
                after_fts_ru, after_fts_wu = meter.get_fts_rwu(database.id)
                self.assertTrue(after_fts_ru-before_fts_ru > 0 and after_fts_ru-before_fts_ru < 5000, f"Read unit should be greater than 0 and less than 5000 {after_fts_ru-before_fts_ru}")
            finally:
                fts_callable.delete_all()
