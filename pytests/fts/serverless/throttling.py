from pytests.fts.fts_callable import FTSCallable
from pytests.fts.serverless.sanity import FTSElixirSanity
from lib.metering_throttling import metering
import random, string
import threading
import time
from lib.metering_throttling import throttling

class FTSThrottle(FTSElixirSanity):
    def setUp(self):
        self.doc_count = 10
        self.scope = '_default'
        self.collection = '_default'
        self.doc_value = ''.join(random.choices(string.ascii_lowercase, k=5048))
        self.doc_value2 = ''.join(random.choices(string.ascii_lowercase, k=5048))
        self.composite_doc = {"fname": f'{self.doc_value}', "lname": f'{self.doc_value2}'}
        return super().setUp()

    def tearDown(self):
        return super().tearDown()

    def test_throttle_create_index(self):
        self.provision_databases()
        for database in self.databases.values():
            throttle = throttling(database.rest_host, database.admin_username, database.admin_password)
            index_limit = throttle.get_bucket_limit(database.id,'searchThrottleLimit')
            before_count, before_seconds = throttle.get_metrics(database.id, service='fts')
            num_docs = index_limit*2
            result = self.run_query(database, f'INSERT INTO {self.collection} (key k, value v) select uuid() as k , {self.composite_doc} as v from array_range(0,{num_docs}) d')
            self.init_input_servers(database)
            fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=self.scope,
                                       collections=self.collection, collection_index=True, is_elixir=True)
            _type = FTSElixirSanity.define_index_parameters_collection_related(container_type="collection", scope=self.scope,
                                                                      collection=self.collection)
            plan_params = self.construct_plan_params()

            fts_idx = fts_callable.create_fts_index("idx", source_type='couchbase',
                                                    source_name=database.id, index_type='fulltext-index',
                                                    index_params=None, plan_params=plan_params,
                                                    source_params=None, source_uuid=None, collection_index=True,
                                                    _type=_type, analyzer="standard",
                                                    scope=self.scope, collections=[self.collection], no_check=False)
            fts_callable.wait_for_indexing_complete(num_docs)
            after_count, after_seconds = throttle.get_metrics(database.id, service='fts')
            self.assertTrue(after_count > before_count)
            self.assertTrue(after_seconds > before_seconds)

    def test_throttle_removed(self):
        self.provision_databases()
        for database in self.databases.values():
            throttle = throttling(database.rest_host, database.admin_username, database.admin_password)
            index_limit = throttle.get_bucket_limit(database.id,'searchThrottleLimit')
            num_docs = index_limit*2
            result = self.run_query(database, f'INSERT INTO {self.collection} (key k, value v) select uuid() as k , {self.composite_doc} as v from array_range(0,{num_docs}) d')
            self.init_input_servers(database)
            fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=self.scope,
                                       collections=self.collection, collection_index=True, is_elixir=True)
            _type = FTSElixirSanity.define_index_parameters_collection_related(container_type="collection", scope=self.scope,
                                                                      collection=self.collection)
            plan_params = self.construct_plan_params()

            fts_idx = fts_callable.create_fts_index("idx", source_type='couchbase',
                                                    source_name=database.id, index_type='fulltext-index',
                                                    index_params=None, plan_params=plan_params,
                                                    source_params=None, source_uuid=None, collection_index=True,
                                                    _type=_type, analyzer="standard",
                                                    scope=self.scope, collections=[self.collection], no_check=False)

            fts_callable.wait_for_indexing_complete(num_docs)
            throttle_count, throttle_seconds = throttle.get_metrics(database.id, service='fts')
            self.assertTrue(throttle_count > 0)
            self.assertTrue(throttle_seconds > 0)

            # Remove most of the data from the bucket so the next index creation does not trigger throttling
            self.run_query(database,f"DELETE FROM {self.collection} LIMIT {int(index_limit*1.5)}")
            # Wait to make sure effects of throttling are gone
            time.sleep(60)

            fts_idx = fts_callable.create_fts_index("idx1", source_type='couchbase',
                                                    source_name=database.id, index_type='fulltext-index',
                                                    index_params=None, plan_params=plan_params,
                                                    source_params=None, source_uuid=None, collection_index=True,
                                                    _type=_type, analyzer="standard",
                                                    scope=self.scope, collections=[self.collection], no_check=False)

            fts_callable.wait_for_indexing_complete(index_limit * 2 - int(index_limit*1.5))
            after_throttle_count, after_throttle_seconds = throttle.get_metrics(database.id, service='fts')
            # We expect no throttling to have occurred on the second create
            self.assertEqual(after_throttle_count, throttle_count)
            self.assertEqual(after_throttle_seconds, throttle_seconds)

    def test_throttle_search(self):
        self.provision_databases()
        for database in self.databases.values():
            throttle = throttling(database.rest_host, database.admin_username, database.admin_password)
            meter = metering(database.rest_host, database.admin_username, database.admin_password)

            index_limit = throttle.get_bucket_limit(database.id, 'searchThrottleLimit')
            num_docs = index_limit*6
            result = self.run_query(database,
                                    f'INSERT INTO {self.collection} (key k, value v) select uuid() as k , {self.composite_doc} as v from array_range(0,{num_docs}) d')
            self.init_input_servers(database)
            fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=self.scope,
                                       collections=self.collection, collection_index=True, is_elixir=True)
            _type = FTSElixirSanity.define_index_parameters_collection_related(container_type="collection",
                                                                               scope=self.scope,
                                                                               collection=self.collection)
            plan_params = self.construct_plan_params()
            before_reject_count = throttle.get_reject_count(database.id, service='fts')

            fts_idx = fts_callable.create_fts_index("idx", source_type='couchbase',
                                                    source_name=database.id, index_type='fulltext-index',
                                                    index_params=None, plan_params=plan_params,
                                                    source_params=None, source_uuid=None, collection_index=True,
                                                    _type=_type, analyzer="standard",
                                                    scope=self.scope, collections=[self.collection], no_check=False)
            fts_callable.wait_for_indexing_complete(num_docs)
            throttle.set_bucket_limit(database.id, value=10, service='searchThrottleLimit')
            time.sleep(10)

            before_fts_ru, before_fts_wu = meter.get_fts_rwu(database.id)
            before_reject_count = throttle.get_reject_count(database.id, service='fts')

            # create one thread per search
            t1 = threading.Thread(target=self.run_async_search,args=(fts_callable,num_docs))
            t2 = threading.Thread(target=self.run_async_search,args=(fts_callable,num_docs))
            t3 =threading.Thread(target=self.run_async_search, args=(fts_callable,num_docs))
            t4 =threading.Thread(target=self.run_async_search, args=(fts_callable,num_docs))


            # start threads
            t1.start()
            t2.start()
            t3.start()
            t4.start()


            # wait for thread to finish
            t1.join()
            t2.join()
            t3.join()
            t4.join()

            after_fts_ru, after_fts_wu = meter.get_fts_rwu(database.id)
            # get throttle count after
            after_count, after_seconds = throttle.get_metrics(database.id, service='fts')
            after_reject_count = throttle.get_reject_count(database.id, service='fts')
            self.assertTrue(after_reject_count > before_reject_count)

    def test_throttle_modify_indexes(self):
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
                        "store": True,
                        "index": True,
                        "include_term_vectors": False,
                        "include_in_all": False,
                        "docvalues": False}]}}}}}
        self.provision_databases()
        for database in self.databases.values():
            throttle = throttling(database.rest_host, database.admin_username, database.admin_password)

            index_limit = throttle.get_bucket_limit(database.id, 'searchThrottleLimit')
            num_docs = index_limit * 4
            result = self.run_query(database,
                                    f'INSERT INTO {self.collection} (key k, value v) select uuid() as k , {self.composite_doc} as v from array_range(0,{num_docs}) d')

            self.init_input_servers(database)
            fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=self.scope,
                                       collections=self.collection, collection_index=True, is_elixir=True)

            _type = FTSElixirSanity.define_index_parameters_collection_related(container_type="collection", scope=self.scope,
                                                                      collection=self.collection)
            plan_params = self.construct_plan_params()

            fts_idx = fts_callable.create_fts_index("idx", source_type='couchbase',
                                                    source_name=database.id, index_type='fulltext-index',
                                                    index_params=idx_params, plan_params=plan_params,
                                                    source_params=None, source_uuid=None, collection_index=True,
                                                    _type=_type, analyzer="standard",
                                                    scope=self.scope, collections=[self.collection], no_check=False,specify_fields=True)
            fts_callable.wait_for_indexing_complete(num_docs)

            before_count, before_seconds = throttle.get_metrics(database.id, service='fts')
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
            fts_callable.wait_for_indexing_complete(num_docs)
            after_count, after_seconds = throttle.get_metrics(database.id, service='fts')
            self.assertTrue(after_count > before_count)
            self.assertTrue(after_seconds > before_seconds)

    def test_throttle_multiple_threads(self):
        self.provision_databases()
        for database in self.databases.values():
            throttle = throttling(database.rest_host, database.admin_username, database.admin_password)
            meter = metering(database.rest_host, database.admin_username, database.admin_password)

            index_limit = throttle.get_bucket_limit(database.id, 'searchThrottleLimit')
            num_docs = index_limit/4
            result = self.run_query(database,
                                    f'INSERT INTO {self.collection} (key k, value v) select uuid() as k , {self.composite_doc} as v from array_range(0,{num_docs}) d')
            self.init_input_servers(database)
            fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=self.scope,
                                       collections=self.collection, collection_index=True, is_elixir=True)
            _type = FTSElixirSanity.define_index_parameters_collection_related(container_type="collection",
                                                                               scope=self.scope,
                                                                               collection=self.collection)
            before_count, before_seconds = throttle.get_metrics(database.id, service='fts')

            # create one thread per search
            t1 = threading.Thread(target=self.run_async_create, args=(fts_callable, database, "1"))
            t2 = threading.Thread(target=self.run_async_create, args=(fts_callable, database, "2"))
            t3 = threading.Thread(target=self.run_async_create, args=(fts_callable, database, "3"))
            t4 = threading.Thread(target=self.run_async_create, args=(fts_callable, database, "4"))

            # start threads
            t1.start()
            t2.start()
            t3.start()
            t4.start()

            # wait for thread to finish
            t1.join()
            t2.join()
            t3.join()
            t4.join()

            time.sleep(30)
            # get throttle count after
            after_count, after_seconds = throttle.get_metrics(database.id, service='fts')
            self.assertTrue(after_count > before_count)
            self.assertTrue(after_seconds > before_seconds)

    def test_throttle_search_and_create(self):
        self.provision_databases()
        for database in self.databases.values():
            throttle = throttling(database.rest_host, database.admin_username, database.admin_password)
            meter = metering(database.rest_host, database.admin_username, database.admin_password)

            index_limit = throttle.get_bucket_limit(database.id, 'searchThrottleLimit')
            num_docs = index_limit * 4
            result = self.run_query(database,
                                    f'INSERT INTO {self.collection} (key k, value v) select uuid() as k , {self.composite_doc} as v from array_range(0,{num_docs}) d')
            self.init_input_servers(database)
            fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=self.scope,
                                       collections=self.collection, collection_index=True, is_elixir=True)
            _type = FTSElixirSanity.define_index_parameters_collection_related(container_type="collection",
                                                                               scope=self.scope,
                                                                               collection=self.collection)
            before_count, before_seconds = throttle.get_metrics(database.id, service='fts')
            before_reject_count = throttle.get_reject_count(database.id, service='fts')

            # create one thread per search
            t1 = threading.Thread(target=self.run_async_create, args=(fts_callable, database, "1"))
            t2 = threading.Thread(target=self.run_async_create, args=(fts_callable, database, "2"))
            t3 = threading.Thread(target=self.run_async_create, args=(fts_callable, database, "3"))
            t4 = threading.Thread(target=self.run_async_search, args=(fts_callable,num_docs))

            # start threads
            t1.start()
            t2.start()
            t3.start()
            t4.start()

            # wait for thread to finish
            t1.join()
            t2.join()
            t3.join()
            t4.join()

            fts_callable.wait_for_indexing_complete(num_docs)
            # get throttle count after
            after_reject_count = throttle.get_reject_count(database.id, service='fts')
            after_count, after_seconds = throttle.get_metrics(database.id, service='fts')
            self.assertTrue(after_count > before_count)
            self.assertTrue(after_seconds > before_seconds)
            self.assertTrue(after_reject_count > before_reject_count)

    def test_throttle_multiple_databases(self):
        self.provision_databases(2)
        keys = self.databases.keys()
        database1 = self.databases[list(keys)[0]]
        database2 = self.databases[list(keys)[1]]
        try:
            throttle = throttling(database1.rest_host, database1.admin_username, database1.admin_password)
        except:
            throttle = throttling(database2.rest_host, database2.admin_username, database2.admin_password)

        index_limit = throttle.get_bucket_limit(database1.id, 'searchThrottleLimit')

        # We want to insert data such that one index hits throttling but the other does not, on the other database, the one that should not hit index throttling should be unaffected by other db
        result = self.run_query(database1,
                                f'INSERT INTO {self.collection} (key k, value v) select uuid() as k , {self.composite_doc} as v from array_range(0,{index_limit*4}) d')
        result2 = self.run_query(database2,
                                f'INSERT INTO {self.collection} (key k, value v) select uuid() as k , {self.composite_doc} as v from array_range(0,{index_limit/10}) d')

        self.init_input_servers(database1)
        fts_callable1 = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=self.scope,
                                   collections=self.collection, collection_index=True, is_elixir=True)

        self.init_input_servers(database2)
        fts_callable2 = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=self.scope,
                                   collections=self.collection, collection_index=True, is_elixir=True)
        # get throttle count before
        before_count_database1, _ = throttle.get_metrics(database1.id, service='fts')
        before_count_database2, _ = throttle.get_metrics(database2.id, service='fts')

        # create one thread per database
        t1 = threading.Thread(target=self.run_async_create, args=(fts_callable1, database1, "1"))
        t2 = threading.Thread(target=self.run_async_create, args=(fts_callable2, database2, "2"))

        # start threads
        t1.start()
        t2.start()

        # wait for thread to finish
        t1.join()
        t2.join()

        fts_callable1.wait_for_indexing_complete(index_limit*4)
        fts_callable2.wait_for_indexing_complete(index_limit/10)

        # get throttle count after
        after_count_database1, _ = throttle.get_metrics(database1.id, service='fts')
        after_count_database2, _ = throttle.get_metrics(database2.id, service='fts')
        self.assertTrue(after_count_database1 > before_count_database1)
        self.assertEqual(after_count_database2, before_count_database2)

    def test_throttle_index_rejection(self):
        self.provision_databases()
        for database in self.databases.values():
            throttle = throttling(database.rest_host, database.admin_username, database.admin_password)
            meter = metering(database.rest_host, database.admin_username, database.admin_password)

            index_limit = throttle.get_bucket_limit(database.id, 'searchThrottleLimit')
            num_docs = index_limit*4
            result = self.run_query(database,
                                    f'INSERT INTO {self.collection} (key k, value v) select uuid() as k , {self.composite_doc} as v from array_range(0,{num_docs}) d')
            self.init_input_servers(database)
            fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=self.scope,
                                       collections=self.collection, collection_index=True, is_elixir=True)
            _type = FTSElixirSanity.define_index_parameters_collection_related(container_type="collection",
                                                                               scope=self.scope,
                                                                               collection=self.collection)
            plan_params = self.construct_plan_params()

            fts_idx = fts_callable.create_fts_index("idx", source_type='couchbase',
                                                    source_name=database.id, index_type='fulltext-index',
                                                    index_params=None, plan_params=plan_params,
                                                    source_params=None, source_uuid=None, collection_index=True,
                                                    _type=_type, analyzer="standard",
                                                    scope=self.scope, collections=[self.collection], no_check=False)
            fts_callable.wait_for_indexing_complete(num_docs)
            #before_count, before_seconds = throttle.get_metrics(database.id, service='fts')
            before_reject_count = throttle.get_reject_count(database.id, service='fts')
            throttle.set_bucket_limit(database.id, value=10, service='searchThrottleLimit')


            # create one thread per search
            t1 = threading.Thread(target=self.run_async_create, args=(fts_callable, database, "1"))
            t2 = threading.Thread(target=self.run_async_create, args=(fts_callable, database, "2"))
            t3 = threading.Thread(target=self.run_async_create, args=(fts_callable, database, "3"))
            t4 = threading.Thread(target=self.run_async_create, args=(fts_callable, database, "4"))

            # start threads
            t1.start()
            t2.start()
            t3.start()
            t4.start()

            # wait for thread to finish
            t1.join()
            t2.join()
            t3.join()
            t4.join()

            fts_callable.wait_for_indexing_complete(num_docs)

            # get throttle count after
            after_count, after_seconds = throttle.get_metrics(database.id, service='fts')
            after_reject_count = throttle.get_reject_count(database.id, service='fts')
            #self.assertTrue(after_count > before_count)
            #self.assertTrue(after_seconds > before_seconds)
            self.assertTrue(after_reject_count > before_reject_count)

    def test_n1ql_search_throttle(self):
        self.provision_databases(self.num_databases)
        for counter, database in enumerate(self.databases.values()):
            try:
                throttle = throttling(database.rest_host, database.admin_username, database.admin_password)

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

                before_n1ql_reject_count = throttle.get_reject_count(database.id, service='query')

                throttle.set_bucket_limit(database.id, value=1, service='searchThrottleLimit')
                time.sleep(10)

                # create one thread per search
                t1 = threading.Thread(target=self.run_async_search_query, args=(database,scope_name,collection_name,n1ql_search_query))
                t2 = threading.Thread(target=self.run_async_search_query, args=(database,scope_name,collection_name,n1ql_search_query))
                t3 = threading.Thread(target=self.run_async_search_query, args=(database,scope_name,collection_name,n1ql_search_query))


                # start threads
                t1.start()
                t2.start()
                t3.start()

                # wait for thread to finish
                t1.join()
                t2.join()
                t3.join()

                after_n1ql_reject_count = throttle.get_reject_count(database.id, service='query')
                self.assertTrue(after_n1ql_reject_count > before_n1ql_reject_count)

            finally:
                fts_callable.delete_all()

    def test_n1ql_flex_throttle(self):
        self.provision_databases(self.num_databases)
        for counter, database in enumerate(self.databases.values()):
            try:
                throttle = throttling(database.rest_host, database.admin_username, database.admin_password)
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

                before_n1ql_reject_count = throttle.get_reject_count(database.id, service='query')

                throttle.set_bucket_limit(database.id, value=1, service='searchThrottleLimit')
                time.sleep(10)

                # create one thread per search
                t1 = threading.Thread(target=self.run_async_flex_query, args=(database,scope_name,collection_name))
                t2 = threading.Thread(target=self.run_async_flex_query, args=(database,scope_name,collection_name))
                t3 = threading.Thread(target=self.run_async_flex_query, args=(database,scope_name,collection_name))


                # start threads
                t1.start()
                t2.start()
                t3.start()

                # wait for thread to finish
                t1.join()
                t2.join()
                t3.join()

                after_n1ql_reject_count = throttle.get_reject_count(database.id, service='query')

                self.assertTrue(after_n1ql_reject_count > before_n1ql_reject_count)

            finally:
                fts_callable.delete_all()

    def test_throttle_index_rejection_removed(self):
        self.provision_databases()
        for database in self.databases.values():
            throttle = throttling(database.rest_host, database.admin_username, database.admin_password)
            meter = metering(database.rest_host, database.admin_username, database.admin_password)

            index_limit = throttle.get_bucket_limit(database.id, 'searchThrottleLimit')
            num_docs = index_limit * 4
            result = self.run_query(database,
                                    f'INSERT INTO {self.collection} (key k, value v) select uuid() as k , {self.composite_doc} as v from array_range(0,{num_docs}) d')
            self.init_input_servers(database)
            fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=self.scope,
                                       collections=self.collection, collection_index=True, is_elixir=True)
            _type = FTSElixirSanity.define_index_parameters_collection_related(container_type="collection",
                                                                               scope=self.scope,
                                                                               collection=self.collection)
            plan_params = self.construct_plan_params()

            fts_idx = fts_callable.create_fts_index("idx", source_type='couchbase',
                                                    source_name=database.id, index_type='fulltext-index',
                                                    index_params=None, plan_params=plan_params,
                                                    source_params=None, source_uuid=None, collection_index=True,
                                                    _type=_type, analyzer="standard",
                                                    scope=self.scope, collections=[self.collection], no_check=False)
            fts_callable.wait_for_indexing_complete(num_docs)
            before_reject_count = throttle.get_reject_count(database.id, service='fts')
            throttle.set_bucket_limit(database.id, value=10, service='searchThrottleLimit')

            # create one thread per search
            t1 = threading.Thread(target=self.run_async_create, args=(fts_callable, database, "1"))
            t2 = threading.Thread(target=self.run_async_create, args=(fts_callable, database, "2"))
            t3 = threading.Thread(target=self.run_async_create, args=(fts_callable, database, "3"))
            t4 = threading.Thread(target=self.run_async_create, args=(fts_callable, database, "4"))

            # start threads
            t1.start()
            t2.start()
            t3.start()
            t4.start()

            # wait for thread to finish
            t1.join()
            t2.join()
            t3.join()
            t4.join()

            fts_callable.wait_for_indexing_complete(num_docs)

            # get throttle count after
            after_count, after_seconds = throttle.get_metrics(database.id, service='fts')
            after_reject_count = throttle.get_reject_count(database.id, service='fts')
            self.assertTrue(after_reject_count > before_reject_count)

            # Raise limit so queries will no longer be autorejected and create a new query, should not be throttled or rejected
            throttle.set_bucket_limit(database.id, value=5000, service='searchThrottleLimit')
            # Remove most of the data from the bucket so the next index creation does not trigger throttling
            self.run_query(database,f"DELETE FROM {self.collection} LIMIT {int(index_limit*3.5)}")
            # Wait to make sure effects of throttling are gone
            time.sleep(60)

            fts_idx = fts_callable.create_fts_index("idx5", source_type='couchbase',
                                                    source_name=database.id, index_type='fulltext-index',
                                                    index_params=None, plan_params=plan_params,
                                                    source_params=None, source_uuid=None, collection_index=True,
                                                    _type=_type, analyzer="standard",
                                                    scope=self.scope, collections=[self.collection], no_check=False)
            fts_callable.wait_for_indexing_complete(index_limit * 4 - int(index_limit*3.5))
            new_count, new_seconds = throttle.get_metrics(database.id, service='fts')
            new_reject_count = throttle.get_reject_count(database.id, service='fts')
            self.assertEqual(new_reject_count, after_reject_count)
            self.assertEqual(new_count, after_count)
            self.assertEqual(new_seconds, after_seconds)

    def run_async_search_query(self,database,scope_name,collection_name,n1ql_search_query):
        n1ql_search_result = self.run_query(database,
                                            f'SELECT t1.age,t1.city,t1.country,t1.firstName,t1.lastName,t1.streetAddress,t1.suffix,t1.title from {scope_name}.{collection_name} as t1 WHERE SEARCH(t1, {n1ql_search_query}) and t1.age > 50 ORDER BY t1.streetAddress')
        self.log.info(n1ql_search_result)

    def run_async_flex_query(self,database,scope_name,collection_name):
        flex_result = self.run_query(database,
                                     f'SELECT t1.age,t1.city,t1.country,t1.firstName,t1.lastName,t1.streetAddress,t1.suffix,t1.title from {scope_name}.{collection_name} as t1 USE INDEX (USING FTS, USING GSI) WHERE t1.country = "Algeria" ORDER BY t1.streetAddress limit 5')
        self.log.info(flex_result)

    def run_async_search(self, fts_callable,expected_hits):
        for index in fts_callable.cb_cluster.get_indexes():
            query = {"match": f"{self.doc_value}", "field": "fname"}
            hits, matches, _, _ = index.execute_query(query,
                                                      zero_results_ok=False,
                                                      expected_hits=expected_hits,
                                                      expected_no_of_results=10000)
            self.log.info("Hits: %s" % hits)

    def run_async_create(self, fts_callable, database, prefix):
        _type = FTSElixirSanity.define_index_parameters_collection_related(container_type="collection",
                                                                           scope=self.scope,
                                                                           collection=self.collection)
        plan_params = self.construct_plan_params()
        idx_name = "idx" + prefix
        self.log.info(idx_name)
        fts_idx = fts_callable.create_fts_index(idx_name, source_type='couchbase',
                                                source_name=database.id, index_type='fulltext-index',
                                                index_params=None, plan_params=plan_params,
                                                source_params=None, source_uuid=None, collection_index=True,
                                                _type=_type, analyzer="standard",
                                                scope=self.scope, collections=[self.collection], no_check=False)
