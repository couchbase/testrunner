import json
from fts_base import FTSBaseTest
from lib.membase.api.rest_client import RestConnection
from lib.membase.api.exception import FTSException, ServerUnavailableException
from TestInput import TestInputSingleton

class StableTopFTS(FTSBaseTest):

    def setUp(self):
        super(StableTopFTS, self).setUp()

    def tearDown(self):
        super(StableTopFTS, self).tearDown()

    def check_fts_service_started(self):
        try:
            rest = RestConnection(self._cb_cluster.get_random_fts_node())
            rest.get_fts_index_definition("invalid_index")
        except ServerUnavailableException as e:
            raise FTSException("FTS service has not started: %s" %e)

    def create_simple_default_index(self):
        plan_params = self.construct_plan_params()
        self.load_data()
        self.create_fts_indexes_all_buckets(plan_params=plan_params)
        if self._update or self._delete:
            self.wait_for_indexing_complete()
            self.validate_index_count(equal_bucket_doc_count=True,
                                      zero_rows_ok=False)
            self.async_perform_update_delete(self.upd_del_fields)
            if self._update:
                self.sleep(60, "Waiting for updates to get indexed...")
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

    def query_in_dgm(self):
        self.create_simple_default_index()
        for index in self._cb_cluster.get_indexes():
            self.generate_random_queries(index, self.num_queries, self.query_types)
            self.run_query_and_compare(index)

    def run_default_index_query(self, query=None, expected_hits=None):
        self.create_simple_default_index()
        zero_results_ok = True
        if not expected_hits:
            expected_hits = int(self._input.param("expected_hits", 0))
            if expected_hits:
                zero_results_ok = False
        if not query:
            query = eval(self._input.param("query", str(self.sample_query)))
            if isinstance(query, str):
                query = json.loads(query)
            zero_results_ok = True
        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query,
                                             zero_results_ok=zero_results_ok,
                                             expected_hits=expected_hits)
            self.log.info("Hits: %s" % hits)

    def test_query_type(self):
        """
        uses RQG
        """
        self.load_data()
        index = self.create_index(
            self._cb_cluster.get_bucket_by_name('default'),
            "default_index")
        self.wait_for_indexing_complete()
        if self._update or self._delete:
            self.async_perform_update_delete(self.upd_del_fields)
            if self._update:
                self.sleep(60, "Waiting for updates to get indexed...")
            self.wait_for_indexing_complete()
        self.generate_random_queries(index, self.num_queries, self.query_types)
        self.run_query_and_compare(index)

    def test_query_type_on_alias(self):
        """
        uses RQG
        """
        self.load_data()
        index = self.create_index(
            self._cb_cluster.get_bucket_by_name('default'),
            "default_index")
        self.wait_for_indexing_complete()
        if self._update or self._delete:
            self.async_perform_update_delete(self.upd_del_fields)
            if self._update:
                self.sleep(60, "Waiting for updates to get indexed...")
            self.wait_for_indexing_complete()
        alias = self.create_alias([index])
        self.generate_random_queries(alias, self.num_queries, self.query_types)
        self.run_query_and_compare(alias)

    def test_match_all(self):
        self.run_default_index_query(query={"match_all": {}},
                                     expected_hits=self._num_items)

    def test_match_none(self):
        self.run_default_index_query(query={"match_none": {}},
                                     expected_hits=0)

    def index_utf16_dataset(self):
        self.load_utf16_data()
        try:
            bucket = self._cb_cluster.get_bucket_by_name('default')
            index = self.create_index(bucket, "default_index")
            # an exception will most likely be thrown from waiting
            self.wait_for_indexing_complete()
            self.validate_index_count(
                equal_bucket_doc_count=False,
                zero_rows_ok=True,
                must_equal=0)
        except Exception as e:
            raise FTSException("Exception thrown in utf-16 test :{0}".format(e))

    def create_simple_alias(self):
        self.load_data()
        bucket = self._cb_cluster.get_bucket_by_name('default')
        index = self.create_index(bucket, "default_index")
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        hits, _, _, _ = index.execute_query(self.sample_query,
                                     zero_results_ok=False)
        alias = self.create_alias([index])
        hits2, _, _, _ = alias.execute_query(self.sample_query,
                                      zero_results_ok=False)
        if hits != hits2:
            self.fail("Index query yields {0} hits while alias on same index "
                      "yields only {1} hits".format(hits, hits2))
        return index, alias

    def create_query_alias_on_multiple_indexes(self):

        #delete default bucket
        self._cb_cluster.delete_bucket("default")

        # create "emp" bucket
        self._cb_cluster.create_standard_buckets(bucket_size=1000,
                                                 name="emp",
                                                 port=11234,
                                                 num_replicas=0)
        emp = self._cb_cluster.get_bucket_by_name('emp')

        # create "wiki" bucket
        self._cb_cluster.create_standard_buckets(bucket_size=1000,
                                                 name="wiki",
                                                 port=11235,
                                                 num_replicas=0)
        wiki = self._cb_cluster.get_bucket_by_name('wiki')

        #load emp dataset into emp bucket
        emp_gen = self.get_generator(dataset="emp", num_items=self._num_items)
        wiki_gen = self.get_generator(dataset="wiki", num_items=self._num_items)
        if self.es:
            # make deep copies of the generators
            import copy
            emp_gen_copy = copy.deepcopy(emp_gen)
            wiki_gen_copy = copy.deepcopy(wiki_gen)

        load_tasks = self._cb_cluster.async_load_bucket_from_generator(
            bucket=emp,
            kv_gen=emp_gen)
        load_tasks += self._cb_cluster.async_load_bucket_from_generator(
            bucket=wiki,
            kv_gen=wiki_gen)

        if self.es:
            # create empty ES indexes
            self.es.create_empty_index("emp_es_index")
            self.es.create_empty_index("wiki_es_index")
            load_tasks.append(self.es.async_bulk_load_ES(index_name='emp_es_index',
                                                        gen=emp_gen_copy,
                                                        op_type='create'))

            load_tasks.append(self.es.async_bulk_load_ES(index_name='wiki_es_index',
                                                        gen=wiki_gen_copy,
                                                        op_type='create'))

        for task in load_tasks:
            task.result()

        # create indexes on both buckets
        emp_index = self.create_index(emp, "emp_index")
        wiki_index = self.create_index(wiki, "wiki_index")

        self.wait_for_indexing_complete()

        # create compound alias
        alias = self.create_alias(target_indexes=[emp_index, wiki_index],
                                  name="emp_wiki_alias")
        if self.es:
            self.es.create_alias(name="emp_wiki_es_alias",
                                 indexes=["emp_es_index", "wiki_es_index"])

        # run rqg on the alias
        self.generate_random_queries(alias, self.num_queries, self.query_types)
        self.run_query_and_compare(alias, es_index_name="emp_wiki_es_alias")

    def index_wiki(self):
        self.load_wiki(lang=self.lang)
        bucket = self._cb_cluster.get_bucket_by_name('default')
        index = self.create_index(bucket, "wiki_index")
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True,
                                  zero_rows_ok=False)

    def delete_index_then_query(self):
        self.load_data()
        bucket = self._cb_cluster.get_bucket_by_name('default')
        index = self.create_index(bucket, "default_index")
        self._cb_cluster.delete_fts_index(index.name)
        try:
            hits2, _, _, _ = index.execute_query(self.sample_query)
        except Exception as e:
            # expected, pass test
            self.log.error(" Expected exception: {0}".format(e))

    def drop_bucket_check_index(self):
        self.load_data()
        bucket = self._cb_cluster.get_bucket_by_name('default')
        index = self.create_index(bucket, "default_index")
        self._cb_cluster.delete_bucket("default")
        self.sleep(60, "waiting for bucket deletion to be known by fts")
        status, _ = index.get_index_defn()
        if status:
            self.fail("Able to retrieve index json from index "
                      "built on bucket that was deleted")

    def delete_index_having_alias(self):
        index, alias = self.create_simple_alias()
        self._cb_cluster.delete_fts_index(index.name)
        try:
            hits, _, _, _ = index.execute_query(self.sample_query)
            if hits != 0:
                self.fail("Query alias with deleted target returns query results!")
        except Exception as e:
            self.log.info("Expected exception :{0}".format(e))

    def create_alias_on_deleted_index(self):
        self.load_employee_dataset()
        bucket = self._cb_cluster.get_bucket_by_name('default')
        index = self.create_index(bucket, "default_index")
        self.wait_for_indexing_complete()
        from fts_base import INDEX_DEFAULTS
        alias_def = INDEX_DEFAULTS.ALIAS_DEFINITION
        alias_def['targets'][index.name] = {}
        alias_def['targets'][index.name]['indexUUID'] = index.get_uuid()
        index.delete()
        try:
            self.create_alias([index], alias_def)
            self.fail("Was able to create alias on deleted target")
        except Exception as e:
            self.log.info("Expected exception :{0}".format(e))

    def edit_index_new_name(self):
        self.load_employee_dataset()
        bucket = self._cb_cluster.get_bucket_by_name('default')
        index = self.create_index(bucket, 'sample_index')
        self.wait_for_indexing_complete()
        index.name = "new_index"
        try:
            index.update()
        except Exception as e:
            self.log.info("Expected exception: {0}".format(e))

    def edit_index(self):
        self.load_employee_dataset()
        bucket = self._cb_cluster.get_bucket_by_name('default')
        index = self.create_index(bucket, 'sample_index')
        self.wait_for_indexing_complete()
        #hits, _, _, _ = index.execute_query(self.sample_query)
        new_plan_param = {"maxPartitionsPerPIndex": 30}
        self.partitions_per_pindex = 30
        index.index_definition['planParams'] = \
            index.build_custom_plan_params(new_plan_param)
        index.index_definition['uuid'] = index.get_uuid()
        index.update()
        _, defn = index.get_index_defn()
        self.log.info(defn['indexDef'])

    def edit_index_negative(self):
        self.load_employee_dataset()
        bucket = self._cb_cluster.get_bucket_by_name('default')
        index = self.create_index(bucket, 'sample_index')
        self.wait_for_indexing_complete()
        hits, _, _, _ = index.execute_query(self.sample_query)
        new_plan_param = {"maxPartitionsPerPIndex": 30}
        self.partitions_per_pindex = 30
        # update params with plan params values to check for validation
        index.index_definition['params'] = \
            index.build_custom_index_params(new_plan_param)
        index.index_definition['uuid'] = index.get_uuid()
        try:
            index.update()
        except Exception as e:
            self.log.info("Expected exception: %s" % e)

    def index_query_beer_sample(self):
        #delete default bucket
        self._cb_cluster.delete_bucket("default")
        master = self._cb_cluster.get_master_node()
        from lib.remote.remote_util import RemoteMachineShellConnection
        shell = RemoteMachineShellConnection(master)
        shell.execute_command("""curl -v -u Administrator:password \
                         -X POST http://{0}:8091/sampleBuckets/install \
                      -d '["beer-sample"]'""".format(master.ip))
        shell.disconnect()
        self.sleep(20)
        bucket = self._cb_cluster.get_bucket_by_name("beer-sample")
        index = self.create_index(bucket, "beer-index")
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True,
                                  zero_rows_ok=False)

        query = {"match": "cafe", "field": "name"}
        hits, _, _, _ = index.execute_query(query,
                                         zero_results_ok=False,
                                         expected_hits=10)
        self.log.info("Hits: %s" % hits)

    def index_query_custom_mapping(self):
        """
         uses RQG for custom mapping
        """
        # create a custom map, disable default map
        index = self.create_index(
            bucket=self._cb_cluster.get_bucket_by_name('default'),
            index_name="custom_index")
        self.create_es_index_mapping(index.es_custom_map, index.index_definition)
        self.load_data()
        self.wait_for_indexing_complete()
        if self._update or self._delete:
            self.async_perform_update_delete(self.upd_del_fields)
            if self._update:
                self.sleep(60, "Waiting for updates to get indexed...")
            self.wait_for_indexing_complete()
        self.generate_random_queries(index, self.num_queries, self.query_types)
        self.run_query_and_compare(index)

    def index_edit_and_query_custom_mapping(self):
        """
        Index and query index, update map, query again, uses RQG
        """
        fail = False
        index = self.create_index(
            bucket=self._cb_cluster.get_bucket_by_name('default'),
            index_name="custom_index")
        self.create_es_index_mapping(index.es_custom_map,index.index_definition)
        self.load_data()
        self.wait_for_indexing_complete()
        self.generate_random_queries(index, self.num_queries, self.query_types)
        try:
            self.run_query_and_compare(index)
        except AssertionError as err:
            self.log.error(err)
            fail = True
        self.log.info("Editing custom index with new map...")
        index.generate_new_custom_map(seed=index.cm_id+10)
        index.index_definition['uuid'] = index.get_uuid()
        index.update()
        # updating mapping on ES is not easy, often leading to merge issues
        # drop and recreate the index, load again
        self.create_es_index_mapping(index.es_custom_map)
        self.load_data()
        self.wait_for_indexing_complete()
        self.run_query_and_compare(index)
        if fail:
            raise err

    def index_query_in_parallel(self):
        """
        Run rqg before querying is complete
        turn off es validation
        goal is to make sure there are no fdb or cbft crashes
        """
        index = self.create_index(
            bucket=self._cb_cluster.get_bucket_by_name('default'),
            index_name="default_index")
        self.load_data()
        self.generate_random_queries(index, self.num_queries, self.query_types)
        self.run_query_and_compare(index)

    def load_index_query_all_in_parallel(self):
        """
        Run rqg before querying is complete
        turn off es validation
        goal is to make sure there are no fdb or cbft crashes
        """
        index = self.create_index(
            bucket=self._cb_cluster.get_bucket_by_name('default'),
            index_name="default_index")
        self.sleep(20)
        self.generate_random_queries(index, self.num_queries, self.query_types)
        from threading import Thread
        threads = []
        threads.append(Thread(target=self.load_data,
                              name="loader thread",
                              args=()))
        threads.append(Thread(target=self.run_query_and_compare,
                              name="query thread",
                              args=(index,)))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    def index_edit_and_query_custom_analyzer(self):
        """
        Index and query index, update map, query again, uses RQG
        """
        fail = False
        index = self.create_index(
            bucket=self._cb_cluster.get_bucket_by_name('default'),
            index_name="custom_index")
        self.create_es_index_mapping(index.es_custom_map, index.index_definition)
        self.load_data()
        self.wait_for_indexing_complete()
        self.generate_random_queries(index, self.num_queries, self.query_types)
        try:
            self.run_query_and_compare(index)
        except AssertionError as err:
            self.log.error(err)
            fail = True
        self.log.info("Editing custom index with new custom analyzer...")
        index.update_custom_analyzer(seed=index.cm_id + 10)
        index.index_definition['uuid'] = index.get_uuid()
        index.update()
        # updating mapping on ES is not easy, often leading to merge issues
        # drop and recreate the index, load again
        self.create_es_index_mapping(index.es_custom_map,index.index_definition)
        self.wait_for_indexing_complete()
        try:
            self.run_query_and_compare(index)
        except AssertionError as err:
            self.log.error(err)
            fail = True
        if fail:
            raise err

    def index_delete_custom_analyzer(self):
        """
        Create Index and then update by deleting custom analyzer in use, or custom filter in use.
        """
        error_msg = TestInputSingleton.input.param('error_msg', '')

        fail = False
        index = self.create_index(
            bucket=self._cb_cluster.get_bucket_by_name('default'),
            index_name="custom_index")
        self.load_data()
        self.wait_for_indexing_complete()

        self.log.info("Editing custom index by deleting custom analyzer/filter in use...")
        index.update_custom_analyzer(seed=index.cm_id + 10)
        index.index_definition['uuid'] = index.get_uuid()
        try:
            index.update()
        except Exception as err:
            self.log.error(err)
            if err.message.count(error_msg,0,len(err.message)):
                self.log.info("Error is expected")
            else:
                self.log.info("Error is not expected")
                raise err

    def test_field_name_alias(self):
        """
        Test the Searchable As property in field mapping
        """
        self.load_data()

        index = self.create_index(
            self._cb_cluster.get_bucket_by_name('default'),
            "default_index")
        self.wait_for_indexing_complete()
        index.add_child_field_to_default_mapping(field_name=self.field_name,
                                                 field_type=self.field_type,
                                                 field_alias=self.field_alias)
        index.index_definition['uuid'] = index.get_uuid()
        index.update()
        self.sleep(5)
        self.wait_for_indexing_complete()
        zero_results_ok = True
        expected_hits = int(self._input.param("expected_hits", 0))
        if expected_hits:
            zero_results_ok = False
        query = eval(self._input.param("query", str(self.sample_query)))
        if isinstance(query, str):
            query = json.loads(query)
        zero_results_ok = True
        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query,
                                                zero_results_ok=zero_results_ok,
                                                expected_hits=expected_hits)
            self.log.info("Hits: %s" % hits)

    def test_one_field_multiple_analyzer(self):
        """
        1. Create an default FTS index on wiki dataset
        2. Update it to add a field mapping for revision.text.#text field with 'en' analyzer
        3. Should get 0 search results for a query
        4. Update it to add another field mapping for the same field, with 'fr' analyzer
        5. Same query should yield more results now.

        """
        self.load_data()
        index = self.create_index(
            self._cb_cluster.get_bucket_by_name('default'),
            "default_index")
        self.wait_for_indexing_complete()
        index.add_child_field_to_default_mapping(field_name=self.field_name,
                                                 field_type=self.field_type,
                                                 field_alias=self.field_alias,
                                                 analyzer="en")
        index.index_definition['uuid'] = index.get_uuid()
        index.update()
        self.sleep(5)
        self.wait_for_indexing_complete()
        zero_results_ok = True
        expected_hits = int(self._input.param("expected_hits1", 0))
        if expected_hits:
            zero_results_ok = False
        query = eval(self._input.param("query", str(self.sample_query)))
        if isinstance(query, str):
            query = json.loads(query)
        zero_results_ok = True
        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query,
                                                zero_results_ok=zero_results_ok,
                                                expected_hits=expected_hits)
            self.log.info("Hits: %s" % hits)

        index.add_analyzer_to_existing_field_map(field_name=field_name, field_type=field_type,
                                                     field_alias=field_alias, analyzer="fr")

        index.index_definition['uuid'] = index.get_uuid()
        index.update()
        self.sleep(5)
        self.wait_for_indexing_complete()
        zero_results_ok = True
        expected_hits = int(self._input.param("expected_hits2", 0))
        if expected_hits:
            zero_results_ok = False
        query = eval(self._input.param("query", str(self.sample_query)))
        if isinstance(query, str):
            query = json.loads(query)
        zero_results_ok = True
        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query,
                                                zero_results_ok=zero_results_ok,
                                                expected_hits=expected_hits)
            self.log.info("Hits: %s" % hits)

    def test_facets(self):
        field_indexed = self._input.param("field_indexed",True)
        self.load_data()
        index = self.create_index(
            self._cb_cluster.get_bucket_by_name('default'),
            "default_index")
        self.wait_for_indexing_complete()
        index.add_child_field_to_default_mapping(field_name="type",
                                                 field_type="text",
                                                 field_alias="type",
                                                 analyzer="keyword")
        if field_indexed:
            index.add_child_field_to_default_mapping(field_name="dept",
                                                 field_type="text",
                                                 field_alias="dept",
                                                 analyzer="keyword")
            index.add_child_field_to_default_mapping(field_name="salary",
                                                 field_type="number",
                                                 field_alias="salary")
            index.add_child_field_to_default_mapping(field_name="join_date",
                                                 field_type="datetime",
                                                 field_alias="join_date")
        index.index_definition['uuid'] = index.get_uuid()
        index.update()
        self.sleep(5)
        self.wait_for_indexing_complete()
        zero_results_ok = True
        expected_hits = int(self._input.param("expected_hits", 0))
        if expected_hits:
            zero_results_ok = False
        query = eval(self._input.param("query", str(self.sample_query)))
        if isinstance(query, str):
            query = json.loads(query)
        zero_results_ok = True
        try:
            for index in self._cb_cluster.get_indexes():
                hits, _, _, _, facets = index.execute_query_with_facets(query,
                                                zero_results_ok=zero_results_ok,
                                                expected_hits=expected_hits)
                self.log.info("Hits: %s" % hits)
                self.log.info("Facets: %s" % facets)
                if facets:
                    index.validate_facets_in_search_results(no_of_hits=hits,
                                                        facets_returned=facets)
                else:
                    if not (field_indexed) and hits==0:
                        self.log.info("No hits/facets returned as the field "
                                      "was not indexed")
        except Exception as err:
            self.log.error(err)
            self.fail("Testcase failed: "+ err.message)
