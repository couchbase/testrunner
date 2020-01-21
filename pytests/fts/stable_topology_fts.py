# coding=utf-8

import copy
import json
import random
from threading import Thread

import Geohash
from membase.helper.cluster_helper import ClusterOperationHelper
from remote.remote_util import RemoteMachineShellConnection

from TestInput import TestInputSingleton
from .fts_base import FTSBaseTest
from lib.membase.api.exception import FTSException, ServerUnavailableException
from lib.membase.api.rest_client import RestConnection


class StableTopFTS(FTSBaseTest):

    def setUp(self):
        super(StableTopFTS, self).setUp()

    def tearDown(self):
        super(StableTopFTS, self).tearDown()

    def suite_setUp(self):
        self.log.info("*** StableTopFTS: suite_setUp() ***")

    def suite_tearDown(self):
        self.log.info("*** StableTopFTS: suite_tearDown() ***")

    def check_fts_service_started(self):
        try:
            rest = RestConnection(self._cb_cluster.get_random_fts_node())
            rest.get_fts_index_definition("invalid_index")
        except ServerUnavailableException as e:
            raise FTSException("FTS service has not started: %s" %e)

    def create_simple_default_index(self):
        plan_params = self.construct_plan_params()
        self.load_data()
        self.wait_till_items_in_bucket_equal(self._num_items//2)
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

    def test_maxttl_setting(self):
        self.create_simple_default_index()
        maxttl = int(self._input.param("maxttl", None))
        self.sleep(maxttl,
                "Waiting for expiration at the elapse of bucket maxttl")
        self._cb_cluster.run_expiry_pager()
        self.wait_for_indexing_complete(item_count=0)
        self.validate_index_count(must_equal=0)
        for index in self._cb_cluster.get_indexes():
            query = eval(self._input.param("query", str(self.sample_query)))
            hits, _, _, _ = index.execute_query(query,
                                             zero_results_ok=True,
                                             expected_hits=0)
            self.log.info("Hits: %s" % hits)

    def query_in_dgm(self):
        self.create_simple_default_index()
        for index in self._cb_cluster.get_indexes():
            self.generate_random_queries(index, self.num_queries, self.query_types)
            self.run_query_and_compare(index)

    def run_default_index_query(self, query=None, expected_hits=None, expected_no_of_results=None):
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
        if expected_no_of_results is None:
            expected_no_of_results = self._input.param("expected_no_of_results", None)

        for index in self._cb_cluster.get_indexes():
            hits, matches, _, _ = index.execute_query(query,
                                                      zero_results_ok=zero_results_ok,
                                                      expected_hits=expected_hits,
                                                      expected_no_of_results=expected_no_of_results)
            self.log.info("Hits: %s" % hits)
            self.log.info("Matches: %s" % matches)

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
        if self.run_via_n1ql:
            n1ql_executor = self._cb_cluster
        else:
            n1ql_executor = None
        self.run_query_and_compare(index, n1ql_executor=n1ql_executor)

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

    def test_match_consistency(self):
        query = {"match_all": {}}
        self.create_simple_default_index()
        zero_results_ok = True
        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query,
                                             zero_results_ok=zero_results_ok,
                                             expected_hits=0,
                                             consistency_level=self.consistency_level,
                                             consistency_vectors=self.consistency_vectors
                                            )
            self.log.info("Hits: %s" % hits)
            for i in range(list(self.consistency_vectors.values())[0].values()[0]):
                self.async_perform_update_delete(self.upd_del_fields)
            hits, _, _, _ = index.execute_query(query,
                                                zero_results_ok=zero_results_ok,
                                                expected_hits=self._num_items,
                                                consistency_level=self.consistency_level,
                                                consistency_vectors=self.consistency_vectors)
            self.log.info("Hits: %s" % hits)

    def test_match_consistency_error(self):
        query = {"match_all": {}}
        fts_node = self._cb_cluster.get_random_fts_node()
        service_map = RestConnection(self._cb_cluster.get_master_node()).get_nodes_services()
        # select FTS node to shutdown
        for node_ip, services in service_map.items():
            ip = node_ip.split(':')[0]
            node = self._cb_cluster.get_node(ip, node_ip.split(':')[1])
            if node and 'fts' in services and 'kv' not in services:
                fts_node = node
                break
        self.create_simple_default_index()
        zero_results_ok = True
        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query,
                                                zero_results_ok=zero_results_ok,
                                                expected_hits=0,
                                                consistency_level=self.consistency_level,
                                                consistency_vectors=self.consistency_vectors)
            self.log.info("Hits: %s" % hits)
            try:
                from .fts_base import NodeHelper
                NodeHelper.stop_couchbase(fts_node)
                for i in range(list(self.consistency_vectors.values())[0].values()[0]):
                    self.async_perform_update_delete(self.upd_del_fields)
            finally:
                NodeHelper.start_couchbase(fts_node)
                NodeHelper.wait_service_started(fts_node)
                self.sleep(10)

            # "status":"remote consistency error" => expected_hits=-1
            hits, _, _, _ = index.execute_query(query,
                                                zero_results_ok=zero_results_ok,
                                                expected_hits=-1,
                                                consistency_level=self.consistency_level,
                                                consistency_vectors=self.consistency_vectors)
            ClusterOperationHelper.wait_for_ns_servers_or_assert([fts_node], self, wait_if_warmup=True)
            self.wait_for_indexing_complete()
            hits, _, _, _ = index.execute_query(query,
                                                zero_results_ok=zero_results_ok,
                                                expected_hits=self._num_items,
                                                consistency_level=self.consistency_level,
                                                consistency_vectors=self.consistency_vectors)
            self.log.info("Hits: %s" % hits)

    def test_match_consistency_long_timeout(self):
        timeout = self._input.param("timeout", None)
        query = {"match_all": {}}
        self.create_simple_default_index()
        zero_results_ok = True
        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query,
                                                zero_results_ok=zero_results_ok,
                                                expected_hits=0,
                                                consistency_level=self.consistency_level,
                                                consistency_vectors=self.consistency_vectors)
            self.log.info("Hits: %s" % hits)
            tasks = []
            for i in range(list(self.consistency_vectors.values())[0].values()[0]):
                tasks.append(Thread(target=self.async_perform_update_delete, args=(self.upd_del_fields,)))
            for task in tasks:
                task.start()
            num_items = self._num_items
            if timeout is None or timeout <= 60000:
                # Here we assume that the update takes more than 60 seconds
                # when we use timeout <= 60 sec we get timeout error
                # with None we have 60s by default
                num_items = 0
            try:
                hits, _, _, _ = index.execute_query(query,
                                                    zero_results_ok=zero_results_ok,
                                                    expected_hits=num_items,
                                                    consistency_level=self.consistency_level,
                                                    consistency_vectors=self.consistency_vectors,
                                                    timeout=timeout)
            finally:
                for task in tasks:
                    task.join()
            self.log.info("Hits: %s" % hits)

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
                                                 num_replicas=0)
        emp = self._cb_cluster.get_bucket_by_name('emp')

        # create "wiki" bucket
        self._cb_cluster.create_standard_buckets(bucket_size=1000,
                                                 name="wiki",
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
            self.log.info("Expected exception: {0}".format(e))

    def drop_bucket_check_index(self):
        count = 0
        self.load_data()
        bucket = self._cb_cluster.get_bucket_by_name('default')
        index = self.create_index(bucket, "default_index")
        self._cb_cluster.delete_bucket("default")
        self.sleep(20, "waiting for bucket deletion to be known by fts")
        try:
            count = index.get_indexed_doc_count()
        except Exception as e:
            self.log.info("Expected exception: {0}".format(e))
            # at this point, index has been deleted,
            # remove index from list of indexes
            self._cb_cluster.get_indexes().remove(index)
        if count:
            self.fail("Able to retrieve index json from index "
                      "built on bucket that was deleted")

    def delete_index_having_alias(self):
        index, alias = self.create_simple_alias()
        self._cb_cluster.delete_fts_index(index.name)
        hits, _, _, _ = alias.execute_query(self.sample_query)
        self.log.info("Hits: {0}".format(hits))
        if hits >= 0:
            self.fail("Query alias with deleted target returns query results!")

    def delete_index_having_alias_recreate_index_query(self):
        index, alias = self.create_simple_alias()
        hits1, _, _, _ = alias.execute_query(self.sample_query)
        self.log.info("Hits: {0}".format(hits1))
        index.delete()
        self.log.info("Recreating deleted index ...")
        bucket = self._cb_cluster.get_bucket_by_name('default')
        self.create_index(bucket, "default_index")
        self.wait_for_indexing_complete()
        hits2, _, _, _ = alias.execute_query(self.sample_query)
        self.log.info("Hits: {0}".format(hits2))
        if hits1 != hits2:
            self.fail("Hits from alias before index recreation: %s,"
                      " after recreation: %s" %(hits1, hits2))

    def create_alias_on_deleted_index(self):
        self.load_employee_dataset()
        bucket = self._cb_cluster.get_bucket_by_name('default')
        index = self.create_index(bucket, "default_index")
        self.wait_for_indexing_complete()
        from .fts_base import INDEX_DEFAULTS
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

    def update_index_during_large_indexing(self):
        """
            MB-22410 - Updating index with a large dirty write queue
            items = some millions defined at run_time using items param
        """
        rest = RestConnection(self._cb_cluster.get_random_fts_node())
        self.load_employee_dataset()
        bucket = self._cb_cluster.get_bucket_by_name('default')
        index = self.create_index(bucket, 'sample_index')
        # wait till half the keys are indexed
        self.wait_for_indexing_complete(self._num_items//2)
        status, stat_value = rest.get_fts_stats(index_name=index.name,
                                                bucket_name=bucket.name,
                                                stat_name='num_recs_to_persist')
        self.log.info("Data(metadata + docs) in write queue is {0}".
                      format(stat_value))
        new_plan_param = self.construct_plan_params()
        index.index_definition['planParams'] = \
            index.build_custom_plan_params(new_plan_param)
        index.index_definition['uuid'] = index.get_uuid()
        index.update()
        self.sleep(10, "Wait for index to get updated...")
        self.is_index_partitioned_balanced(index=index)
        _, defn = index.get_index_defn()
        self.log.info(defn['indexDef'])
        # see if the index is still query-able with all data
        self.wait_for_indexing_complete()
        hits, _, _, _ = index.execute_query(self.sample_query,
                                         zero_results_ok=False)
        self.log.info("Hits: %s" % hits)

    def delete_index_during_large_indexing(self):
        """
            MB-22410 - Deleting index with a large dirty write queue is slow
            items = 5M
        """
        self.load_employee_dataset()
        bucket = self._cb_cluster.get_bucket_by_name('default')
        index = self.create_index(bucket, 'sample_index')
        # wait till half the keys are indexed
        self.wait_for_indexing_complete(self._num_items//2)
        index.delete()
        self.sleep(5)
        try:
            _, defn = index.get_index_defn()
            self.log.info(defn)
            self.fail("ERROR: Index definition still exists after deletion! "
                      "%s" %defn['indexDef'])
        except Exception as e:
            self.log.info("Expected exception caught: %s" % e)

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
        self.load_sample_buckets(server=master, bucketName="beer-sample")
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
        if self.es:
            self.create_es_index_mapping(index.es_custom_map,
                                         index.index_definition)
        self.load_data()
        self.wait_for_indexing_complete()
        if self._update or self._delete:
            self.async_perform_update_delete(self.upd_del_fields)
            if self._update:
                self.sleep(60, "Waiting for updates to get indexed...")
            self.wait_for_indexing_complete()
        self.generate_random_queries(index, self.num_queries, self.query_types)
        if self.run_via_n1ql:
            n1ql_executor = self._cb_cluster
        else:
            n1ql_executor = None

        self.run_query_and_compare(index, n1ql_executor=n1ql_executor)

    def test_query_string_combinations(self):
        """
        uses RQG framework minus randomness for testing query-string combinations of '', '+', '-'
        {

            mterms := [
                [],                         // none
                ["+Wikipedia"],             // one term
                ["+Wikipedia", "+English"], // two terms
                ["+the"],                   // one term (stop word)
                ["+the", "+English"],       // two terms (one stop)
                ["+the", "+and"],           // two terms (both stop)
            ]

            sterms = [
                [],                         // none
                ["Category"],               // one term
                ["Category", "United"],     // two terms
                ["of"],                     // one term (stop word)
                ["of", "United"],           // two terms (one stop)
                ["of", "at"],               // two terms (both stop)
            ]

            nterms = [
                [],                         // none
                ["-language"],              // one term
                ["-language", "-States"],   // two terms
                ["-for"],                   // one term (stop word)
                ["-for", "-States"],        // two terms (one stop)
                ["-for", "-with"],          // two terms (both stop)
            ]
        }

        """
        self.load_data()

        index = self.create_index(
            self._cb_cluster.get_bucket_by_name('default'),
            "default_index")
        self.wait_for_indexing_complete()

        index.fts_queries = []
        mterms = [[],
                  ["+revision.text.#text:\"Wikipedia\""],
                  ["+revision.text.#text:\"Wikipedia\"", "+revision.text.#text:\"English\""],
                  ["+revision.text.#text:\"the\""],
                  ["+revision.text.#text:\"the\"", "+revision.text.#text:\"English\""],
                  ["+revision.text.#text:\"the\"", "+revision.text.#text:\"and\""]]
        sterms = [[],
                  ["revision.text.#text:\"Category\""],
                  ["revision.text.#text:\"Category\"", "revision.text.#text:\"United\""],
                  ["revision.text.#text:\"of\""],
                  ["revision.text.#text:\"of\"", "revision.text.#text:\"United\""],
                  ["revision.text.#text:\"of\"", "revision.text.#text:\"at\""]]
        nterms = [[],
                  ["-revision.text.#text:\"language\""],
                  ["-revision.text.#text:\"language\"", "-revision.text.#text:\"States\""],
                  ["-revision.text.#text:\"for\""],
                  ["-revision.text.#text:\"for\"", "-revision.text.#text:\"States\""],
                  ["-revision.text.#text:\"for\"", "-revision.text.#text:\"with\""]]
        for mterm in mterms:
            for sterm in sterms:
                for nterm in nterms:
                    clause  = (' '.join(mterm) + ' ' + ' '.join(sterm) + ' ' + ' '.join(nterm)).strip()
                    query = {"query": clause}
                    index.fts_queries.append(json.loads(json.dumps(query, ensure_ascii=False)))
                    if self.compare_es:
                        self.es.es_queries.append(json.loads(json.dumps({"query": {"query_string": query}},
                                                                     ensure_ascii=False)))
        self.run_query_and_compare(index)


    def index_edit_and_query_custom_mapping(self):
        """
        Index and query index, update map, query again, uses RQG
        """
        fail = False
        error = None
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
            error = err
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
            raise error

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
        self.create_es_index_mapping(index.es_custom_map, index.index_definition)
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
            if err.message.count(error_msg, 0, len(err.message)):
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
            hits, matches, time_taken, status = index.execute_query(query,
                                                zero_results_ok=zero_results_ok,
                                                expected_hits=expected_hits,
                                                consistency_level=self.consistency_level,
                                                consistency_vectors=self.consistency_vectors)
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

        index.add_analyzer_to_existing_field_map(field_name=self.field_name,
                                                 field_type=self.field_type,
                                                 field_alias=self.field_alias,
                                                 analyzer="fr")

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
        field_indexed = self._input.param("field_indexed", True)
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
                index.validate_facets_in_search_results(no_of_hits=hits,
                                                        facets_returned=facets)
        except Exception as err:
            self.log.error(err)
            self.fail("Testcase failed: "+ err.message)

    def test_facets_during_index(self):
        field_indexed = self._input.param("field_indexed", True)
        self.load_data()
        index = self.create_index(
            self._cb_cluster.get_bucket_by_name('default'),
            "default_index")
        self.sleep(5)
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
        query = eval(self._input.param("query", str(self.sample_query)))
        if isinstance(query, str):
            query = json.loads(query)

        while not self.is_index_complete(index.name):
            zero_results_ok = True
            try:
                hits, _, _, _, facets = index.execute_query_with_facets(query,
                                                                            zero_results_ok=zero_results_ok)
                self.log.info("Hits: %s" % hits)
                self.log.info("Facets: %s" % facets)
            except Exception as err:
                self.log.error(err)
                self.fail("Testcase failed: "+ err.message)

    def test_doc_config(self):
        # delete default bucket
        self._cb_cluster.delete_bucket("default")
        master = self._cb_cluster.get_master_node()

        # Load Travel Sample bucket and create an index
        self.load_sample_buckets(server=master, bucketName="travel-sample")
        bucket = self._cb_cluster.get_bucket_by_name("travel-sample")
        index = self.create_index(bucket, "travel-index")
        self.sleep(10)

        # Add Type Mapping
        index.add_type_mapping_to_index_definition(type="airport",
                                                   analyzer="en")
        index.add_type_mapping_to_index_definition(type="hotel",
                                                   analyzer="en")
        mode = self._input.param("mode", "type_field")
        index.add_doc_config_to_index_definition(mode=mode)
        index.index_definition['uuid'] = index.get_uuid()
        index.update()
        self.sleep(15)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True,
                                  zero_rows_ok=False)

        # Run Query
        expected_hits = int(self._input.param("expected_hits", 0))
        if not expected_hits:
            zero_results_ok = True
        else:
            zero_results_ok = False
        query = eval(self._input.param("query", str(self.sample_query)))
        try:
            for index in self._cb_cluster.get_indexes():
                hits, _, _, _ = index.execute_query(query,
                                                zero_results_ok=zero_results_ok,
                                                expected_hits=expected_hits,
                                                consistency_level=self.consistency_level,
                                                consistency_vectors=self.consistency_vectors)
                self.log.info("Hits: %s" % hits)
        except Exception as err:
            self.log.error(err)
            self.fail("Testcase failed: " + err.message)

    def test_boost_query_type(self):
        # Create bucket, create index
        self.load_data()
        self.wait_till_items_in_bucket_equal(items=self._num_items//2)
        index = self.create_index(
            self._cb_cluster.get_bucket_by_name('default'),
            "default_index")
        self.wait_for_indexing_complete()
        index.add_type_mapping_to_index_definition(type="emp",
                                                   analyzer="keyword")
        index.index_definition['uuid'] = index.get_uuid()
        index.update()
        self.sleep(15)
        self.wait_for_indexing_complete()
        zero_results_ok = False
        expected_hits = 5

        # Run Query w/o Boosting and compare the scores for Docs emp10000086 &
        # emp10000021. Should be the same
        query = {"query": "dept:Marketing name:Safiya"}
        if isinstance(query, str):
            query = json.loads(query)
        zero_results_ok = True
        try:
            for index in self._cb_cluster.get_indexes():
                hits, contents, _, _ = index.execute_query(query,
                                                           zero_results_ok=zero_results_ok,
                                                           expected_hits=expected_hits,
                                                           return_raw_hits=True)
                self.log.info("Hits: %s" % hits)
                self.log.info("Contents: %s" % contents)
                score_before_boosting_doc1 = index.get_score_from_query_result_content(
                    contents=contents, doc_id='emp10000021')
                score_before_boosting_doc2 = index.get_score_from_query_result_content(
                    contents=contents, doc_id='emp10000086')

                self.log.info("Scores before boosting:")
                self.log.info("")
                self.log.info("emp10000021: %s", score_before_boosting_doc1)
                self.log.info("emp10000086: %s", score_before_boosting_doc2)

        except Exception as err:
            self.log.error(err)
            self.fail("Testcase failed: " + err.message)

        if not score_before_boosting_doc1 == score_before_boosting_doc2:
            self.fail("Testcase failed: Scores for emp10000021 & emp10000086 "
                      "are not equal before boosting")

        # Run Query w/o Boosting and compare the scores for Docs emp10000021 &
        # emp10000086. emp10000021 score should have improved w.r.t. emp10000086
        query = {"query": "dept:Marketing^5 name:Safiya"}
        if isinstance(query, str):
            query = json.loads(query)
        zero_results_ok = True
        for index in self._cb_cluster.get_indexes():
            hits, contents, _, _ = index.execute_query(query,
                                                       zero_results_ok=zero_results_ok,
                                                       expected_hits=expected_hits,
                                                       return_raw_hits=True)
            self.log.info("Hits: %s" % hits)
            self.log.info("Contents: %s" % contents)
            score_after_boosting_doc1 = index.get_score_from_query_result_content(
                contents=contents, doc_id='emp10000021')
            score_after_boosting_doc2 = index.get_score_from_query_result_content(
                contents=contents, doc_id='emp10000086')

            self.log.info("Scores after boosting:")
            self.log.info("")
            self.log.info("emp10000021: %s", score_after_boosting_doc1)
            self.log.info("emp10000086: %s", score_after_boosting_doc2)
            assert score_after_boosting_doc1 == score_after_boosting_doc2
            assert score_before_boosting_doc1 < score_after_boosting_doc1
            assert score_before_boosting_doc2 < score_after_boosting_doc2


    def test_doc_id_query_type(self):
        # Create bucket, create index
        self.load_data()
        index = self.create_index(
            self._cb_cluster.get_bucket_by_name('default'),
            "default_index")
        self.wait_for_indexing_complete()
        index.add_type_mapping_to_index_definition(type="emp",
                                                   analyzer="keyword")

        index.index_definition['uuid'] = index.get_uuid()
        index.update()
        self.sleep(15)
        self.wait_for_indexing_complete()

        expected_hits = int(self._input.param("expected_hits", 0))
        query = eval(self._input.param("query", str(self.sample_query)))
        if isinstance(query, str):
            query = json.loads(query)
        # From the Query string, fetch the Doc IDs
        doc_ids = copy.deepcopy(query['ids'])

        # If invalid_doc_id param is passed, add this to the query['ids']
        invalid_doc_id = self._input.param("invalid_doc_id", 0)
        if invalid_doc_id:
            query['ids'].append(invalid_doc_id)

        # If disjuncts_query is passed, join query and disjuncts_query
        # to form a new query string
        disjuncts_query = self._input.param("disjuncts_query", None)
        if disjuncts_query:
            if isinstance(disjuncts_query, str):
                disjuncts_query = json.loads(disjuncts_query)
            new_query = {}
            new_query['disjuncts'] = []
            new_query['disjuncts'].append(disjuncts_query)
            new_query['disjuncts'].append(query)
            query = new_query

        # Execute Query
        zero_results_ok = False
        try:
            for index in self._cb_cluster.get_indexes():
                n1ql_query = "select d, meta().id from default d where search(d, "+json.dumps(query)+") and type='emp'"
                hits, contents, _, _ = index.execute_query(query,
                                            zero_results_ok=zero_results_ok,
                                            expected_hits=expected_hits,
                                            return_raw_hits=True)
                self.log.info("Hits: %s" % hits)
                self.log.info("Contents: %s" % contents)
                # For each doc id passed in the query, validate the
                # presence in the search results
                for doc_id in doc_ids:
                    self.assertTrue(index.is_doc_present_in_query_result_content
                                    (contents=contents, doc_id=doc_id), "Doc ID "
                                    "%s is not present in Search results"
                                    % doc_id)
                    if self.run_via_n1ql:
                        n1ql_results = self._cb_cluster.run_n1ql_query(query=n1ql_query)
                        self.assertTrue(index.is_doc_present_in_query_result_content
                                    (contents=n1ql_results['results'], doc_id=doc_id), "Doc ID "
                                    "%s is not present in N1QL Search results"
                                    % doc_id)
                    score = index.get_score_from_query_result_content\
                        (contents=contents, doc_id=doc_id)
                    self.log.info ("Score for Doc ID {0} is {1}".
                                   format(doc_id, score))
                if invalid_doc_id:
                    # Validate if invalid doc id was passed, it should
                    # not be present in the search results
                    self.assertFalse(index.is_doc_present_in_query_result_content
                                     (contents=contents, doc_id=invalid_doc_id),
                                     "Doc ID %s is present in Search results"
                                     % invalid_doc_id)

        except Exception as err:
            self.log.error(err)
            self.fail("Testcase failed: " + err.message)

    def test_sorting_of_results(self):
        self.load_data()
        self.wait_till_items_in_bucket_equal(self._num_items//2)
        index = self.create_index(
            self._cb_cluster.get_bucket_by_name('default'),
            "default_index")
        self.wait_for_indexing_complete()

        zero_results_ok = True
        expected_hits = int(self._input.param("expected_hits", 0))
        default_query = {"disjuncts": [{"match": "Safiya", "field": "name"},
                               {"match": "Palmer", "field": "name"}]}
        query = eval(self._input.param("query", str(default_query)))
        if expected_hits:
            zero_results_ok = False
        if isinstance(query, str):
            query = json.loads(query)

        try:
            for index in self._cb_cluster.get_indexes():
                sort_params = self.build_sort_params()
                hits, raw_hits, _, _ = index.execute_query(query = query,
                                                        zero_results_ok=zero_results_ok,
                                                        expected_hits=expected_hits,
                                                        sort_fields=sort_params,
                                                        return_raw_hits=True)

                self.log.info("Hits: %s" % hits)
                self.log.info("Doc IDs: %s" % raw_hits)
                if hits:
                    result = index.validate_sorted_results(raw_hits,
                                                           self.sort_fields_list)
                    if not result:
                        self.fail(
                            "Testcase failed. Actual results do not match expected.")
        except Exception as err:
            self.log.error(err)
            self.fail("Testcase failed: " + err.message)

    def test_sorting_of_results_during_indexing(self):
        self.load_data()
        self.wait_till_items_in_bucket_equal(self._num_items//2)
        index = self.create_index(
            self._cb_cluster.get_bucket_by_name('default'),
            "default_index")
        #self.wait_for_indexing_complete()

        self.sleep(5)

        zero_results_ok = True
        #expected_hits = int(self._input.param("expected_hits", 0))
        default_query = {"disjuncts": [{"match": "Safiya", "field": "name"},
                                       {"match": "Palmer", "field": "name"}]}
        query = eval(self._input.param("query", str(default_query)))
        if isinstance(query, str):
            query = json.loads(query)

        try:
            for index in self._cb_cluster.get_indexes():
                while not self.is_index_complete(index.name):
                    sort_params = self.build_sort_params()
                    hits, raw_hits, _, _ = index.execute_query(query = query,
                                                               zero_results_ok=zero_results_ok,
                                                               sort_fields=sort_params,
                                                               return_raw_hits=True)

                    self.log.info("Hits: %s" % hits)
                    self.log.info("Doc IDs: %s" % raw_hits)
                    #self.sleep(5)
        except Exception as err:
            self.log.error(err)
            self.fail("Testcase failed: " + err.message)

    def test_sorting_of_results_on_non_indexed_fields(self):
        self.load_data()
        index = self.create_index(
            self._cb_cluster.get_bucket_by_name('default'),
            "default_index")
        self.wait_for_indexing_complete()
        index.add_child_field_to_default_mapping(field_name="name",
                                                 field_type="text",
                                                 field_alias="name",
                                                 analyzer="en")
        index.index_definition['uuid'] = index.get_uuid()
        index.update()
        self.sleep(5)
        self.wait_for_indexing_complete()

        zero_results_ok = True
        expected_hits = int(self._input.param("expected_hits", 0))
        default_query = {"disjuncts": [{"match": "Safiya", "field": "name"},
                                       {"match": "Palmer", "field": "name"}]}

        query = eval(self._input.param("query", str(default_query)))
        if expected_hits:
            zero_results_ok = False
        if isinstance(query, str):
            query = json.loads(query)

        try:
            for index in self._cb_cluster.get_indexes():
                hits, raw_hits, _, _ = index.execute_query(query=query,
                                                           zero_results_ok=zero_results_ok,
                                                           expected_hits=expected_hits,
                                                           sort_fields=self.sort_fields_list,
                                                           return_raw_hits=True)

                self.log.info("Hits: %s" % hits)
                self.log.info("Doc IDs: %s" % raw_hits)
                if hits:
                    result = index.validate_sorted_results(raw_hits,
                                                       self.sort_fields_list)
                    if not result:
                        self.fail(
                            "Testcase failed. Actual results do not match expected.")
        except Exception as err:
            self.log.error(err)
            self.fail("Testcase failed: " + err.message)

    def test_scoring_tf_score(self):
        """
        Test if the TF score in the Scoring functionality works fine
        """
        test_data = ["{\\\"text\\\":\\\"cat - a lazy cat and a brown cat\\\"}",
                     "{\\\"text\\\":\\\"a lazy cat and a brown cat\\\"}",
                     "{\\\"text\\\":\\\"a lazy cat\\\"}"]

        self.create_test_dataset(self._master, test_data)
        self.wait_till_items_in_bucket_equal(items=len(test_data))
        plan_params = self.construct_plan_params()
        index = self.create_index(plan_params=plan_params,
                                  bucket=self._cb_cluster.get_bucket_by_name(
                                      'default'),
                                  index_name="default_index")
        self.wait_for_indexing_complete()
        self.sleep(5)
        zero_results_ok = True
        expected_hits = int(self._input.param("expected_hits", 0))
        if expected_hits:
            zero_results_ok = False
        query = eval(self._input.param("query", str(self.sample_query)))
        if isinstance(query, str):
            query = json.loads(query)
        zero_results_ok = True
        n1ql_query = "select search_score(d) as score, d.text, meta().id from default d where search(d," + json.dumps(query) + ")"
        for index in self._cb_cluster.get_indexes():
            hits, raw_hits, _, _ = index.execute_query(query=query,
                                                       zero_results_ok=zero_results_ok,
                                                       expected_hits=expected_hits,
                                                       return_raw_hits=True,
                                                       explain=True)
            tf_score1, _, _, _, _ = index.get_detailed_scores_for_doc(
                doc_id='1',
                search_results=raw_hits,
                weight='fieldWeight',
                searchTerm='cat')
            self.log.info("TF for Doc ID 1 = %s" % tf_score1)
            tf_score2, _, _, _, _ = index.get_detailed_scores_for_doc(
                doc_id='2',
                search_results=raw_hits,
                weight='fieldWeight',
                searchTerm='cat')
            self.log.info("TF for Doc ID 2 = %s" % tf_score2)
            tf_score3, _, _, _, _ = index.get_detailed_scores_for_doc(
                doc_id='3',
                search_results=raw_hits,
                weight='fieldWeight',
                searchTerm='cat')
            self.log.info("TF for Doc ID 3 = %s" % tf_score3)

            self.assertTrue(tf_score1 > tf_score2 > tf_score3,
                            "Testcase failed. TF score for Doc1 not > Doc2 not > Doc3")

            if self.run_via_n1ql:
                self.compare_n1ql_fts_scoring(n1ql_query=n1ql_query, raw_hits=raw_hits)

    def compare_n1ql_fts_scoring(self, n1ql_query='', raw_hits=[]):
        n1ql_results = self._cb_cluster.run_n1ql_query(query=n1ql_query)

        self.assertEqual(len(n1ql_results['results']), len(raw_hits),
                          "Return values are not the same for n1ql query and fts request.")
        for res in n1ql_results['results']:
            for hit in raw_hits:
                if res['id'] == hit['id']:
                    self.assertEqual(res['score'], hit['score'],
                                     "Scoring is not the same for n1ql result and fts request hit")

    def test_scoring_idf_score(self):
        """
        Test if the IDF score in the Scoring functionality works fine
        """
        test_data = ["{\\\"text\\\":\\\"a brown cat\\\"}",
                     "{\\\"text\\\":\\\"a lazy cat\\\"}",
                     "{\\\"text\\\":\\\"a lazy cat and a brown cat\\\"}",
                     "{\\\"text\\\":\\\"a brown dog\\\"}",
                     "{\\\"text\\\":\\\"a lazy dog\\\"}",
                     "{\\\"text\\\":\\\"a lazy dog and a brown dog\\\"}",
                     "{\\\"text\\\":\\\"a lazy fox and a brown fox\\\"}"]

        self.create_test_dataset(self._master, test_data)
        self.wait_till_items_in_bucket_equal(items=len(test_data))
        plan_params = self.construct_plan_params()
        index = self.create_index(plan_params=plan_params,
                                  bucket=self._cb_cluster.get_bucket_by_name(
                                      'default'),
                                  index_name="default_index")
        self.wait_for_indexing_complete()
        self.sleep(5)
        zero_results_ok = True
        expected_hits = int(self._input.param("expected_hits", 0))
        if expected_hits:
            zero_results_ok = False
        query = eval(self._input.param("query", str(self.sample_query)))
        if isinstance(query, str):
            query = json.loads(query)
        zero_results_ok = True
        n1ql_query = "select search_score(d) as score, d.text, meta().id from default d where search(d," + json.dumps(query) + ")"
        for index in self._cb_cluster.get_indexes():
            hits, raw_hits, _, _ = index.execute_query(query=query,
                                                       zero_results_ok=zero_results_ok,
                                                       expected_hits=expected_hits,
                                                       return_raw_hits=True,
                                                       explain=True)
            _, _, idf1, _, _ = index.get_detailed_scores_for_doc(doc_id='2',
                                                                 search_results=raw_hits,
                                                                 weight='fieldWeight',
                                                                 searchTerm='cat')
            self.log.info("IDF score for Doc ID 1 = %s" % idf1)

            _, _, idf2, _, _ = index.get_detailed_scores_for_doc(doc_id='2',
                                                                 search_results=raw_hits,
                                                                 weight='fieldWeight',
                                                                 searchTerm='lazy')
            self.log.info( "IDF score for Doc ID 2 = %s" % idf2)

            self.assertTrue(idf1 > idf2, "Testcase failed. IDF score for Doc1 "
                    "for search term 'cat' not > that of search term 'lazy'")

            if self.run_via_n1ql:
                self.compare_n1ql_fts_scoring(n1ql_query=n1ql_query, raw_hits=raw_hits)

    def test_scoring_field_norm_score(self):
        """
        Test if the Field Normalization score in the Scoring functionality works fine
        """
        test_data = ["{\\\"text\\\":\\\"a cat\\\"}",
                     "{\\\"text\\\":\\\"a lazy cat\\\"}",
                     "{\\\"text\\\":\\\"a lazy cat and a brown cat\\\"}"]

        self.create_test_dataset(self._master, test_data)
        self.wait_till_items_in_bucket_equal(items=len(test_data))
        plan_params = self.construct_plan_params()
        index = self.create_index(plan_params=plan_params,
                                  bucket=self._cb_cluster.get_bucket_by_name(
                                      'default'),
                                  index_name="default_index")
        self.wait_for_indexing_complete()
        self.sleep(5)
        zero_results_ok = True
        expected_hits = int(self._input.param("expected_hits", 0))
        if expected_hits:
            zero_results_ok = False
        query = eval(self._input.param("query", str(self.sample_query)))
        if isinstance(query, str):
            query = json.loads(query)
        zero_results_ok = True
        n1ql_query = "select search_score(d) as score, d.text, meta().id from default d where search(d," + json.dumps(query) + ")"
        for index in self._cb_cluster.get_indexes():
            hits, raw_hits, _, _ = index.execute_query(query=query,
                                                       zero_results_ok=zero_results_ok,
                                                       expected_hits=expected_hits,
                                                       return_raw_hits=True,
                                                       explain=True)
            _, field_norm1, _, _, _ = index.get_detailed_scores_for_doc(
                doc_id='1',
                search_results=raw_hits,
                weight='fieldWeight',
                searchTerm='cat')
            self.log.info(
                "Field Normalization score for Doc ID 1 = %s" % field_norm1)
            _, field_norm2, _, _, _ = index.get_detailed_scores_for_doc(
                doc_id='2',
                search_results=raw_hits,
                weight='fieldWeight',
                searchTerm='cat')
            self.log.info(
                "Field Normalization score for Doc ID 2 = %s" % field_norm2)
            _, field_norm3, _, _, _ = index.get_detailed_scores_for_doc(
                doc_id='3',
                search_results=raw_hits,
                weight='fieldWeight',
                searchTerm='cat')
            self.log.info(
                "Field Normalization score for Doc ID 3 = %s" % field_norm3)

            self.assertTrue(field_norm1 > field_norm2 > field_norm3,
                            "Testcase failed. Field Normalization score for "
                            "Doc1 not > Doc2 not > Doc3")

            if self.run_via_n1ql:
                self.compare_n1ql_fts_scoring(n1ql_query=n1ql_query, raw_hits=raw_hits)

    def test_scoring_query_norm_score(self):
        """
        Test if the Query Normalization score in the Scoring functionality works fine
        """
        test_data = ["{\\\"text\\\":\\\"a cat\\\"}",
                     "{\\\"text\\\":\\\"a lazy cat\\\"}",
                     "{\\\"text\\\":\\\"a lazy cat and a brown cat\\\"}"]

        self.create_test_dataset(self._master, test_data)
        self.wait_till_items_in_bucket_equal(items=len(test_data))
        plan_params = self.construct_plan_params()
        index = self.create_index(plan_params=plan_params,
                                  bucket=self._cb_cluster.get_bucket_by_name(
                                      'default'),
                                  index_name="default_index")
        self.wait_for_indexing_complete()
        self.sleep(5)
        zero_results_ok = True
        expected_hits = int(self._input.param("expected_hits", 0))
        if expected_hits:
            zero_results_ok = False
        query = eval(self._input.param("query", str(self.sample_query)))
        if isinstance(query, str):
            query = json.loads(query)
        zero_results_ok = True
        n1ql_query = "select search_score(d) as score, d.text, meta().id from default d where search(d," + json.dumps(query) + ")"
        for index in self._cb_cluster.get_indexes():
            hits, raw_hits, _, _ = index.execute_query(query=query,
                                                       zero_results_ok=zero_results_ok,
                                                       expected_hits=expected_hits,
                                                       return_raw_hits=True,
                                                       explain=True)
            _, _, _, query_norm1, _ = index.get_detailed_scores_for_doc(
                doc_id='1',
                search_results=raw_hits,
                weight='queryWeight',
                searchTerm='cat')
            self.log.info(
                "Query Normalization score for Doc ID 1 = %s" % query_norm1)
            _, _, _, query_norm2, _ = index.get_detailed_scores_for_doc(
                doc_id='2',
                search_results=raw_hits,
                weight='queryWeight',
                searchTerm='cat')
            self.log.info(
                "Query Normalization score for Doc ID 2 = %s" % query_norm2)
            _, _, _, query_norm3, _ = index.get_detailed_scores_for_doc(
                doc_id='3',
                search_results=raw_hits,
                weight='queryWeight',
                searchTerm='cat')
            self.log.info(
                "Query Normalization score for Doc ID 3 = %s" % query_norm3)

            self.assertTrue(query_norm1 == query_norm2 == query_norm3,
                            "Testcase failed. Query Normalization score for "
                            "Doc1 != Doc2 != Doc3")

            if self.run_via_n1ql:
                self.compare_n1ql_fts_scoring(n1ql_query=n1ql_query, raw_hits=raw_hits)

    def test_scoring_coord_score(self):
        """
        Test if the Coord score in the Scoring functionality works fine
        """
        test_data = ["{\\\"text\\\":\\\"a cat\\\"}",
                     "{\\\"text\\\":\\\"a lazy cat\\\"}"]

        self.create_test_dataset(self._master, test_data)
        self.wait_till_items_in_bucket_equal(items=len(test_data))
        plan_params = self.construct_plan_params()
        index = self.create_index(plan_params=plan_params,
                                  bucket=self._cb_cluster.get_bucket_by_name(
                                      'default'),
                                  index_name="default_index")
        self.wait_for_indexing_complete()
        self.sleep(5)
        zero_results_ok = True
        expected_hits = int(self._input.param("expected_hits", 0))
        if expected_hits:
            zero_results_ok = False
        query = eval(self._input.param("query", str(self.sample_query)))
        if isinstance(query, str):
            query = json.loads(query)
        zero_results_ok = True
        n1ql_query = "select search_score(d) as score, d.text, meta().id from default d where search(d," + json.dumps(query) + ")"
        for index in self._cb_cluster.get_indexes():
            hits, raw_hits, _, _ = index.execute_query(query=query,
                                                       zero_results_ok=zero_results_ok,
                                                       expected_hits=expected_hits,
                                                       return_raw_hits=True,
                                                       explain=True)
            _, _, _, _, coord1 = index.get_detailed_scores_for_doc(
                doc_id='1',
                search_results=raw_hits,
                weight='coord',
                searchTerm='')
            self.log.info(
                "Coord score for Doc ID 1 = %s" % coord1)
            _, _, _, _, coord2 = index.get_detailed_scores_for_doc(
                doc_id='2',
                search_results=raw_hits,
                weight='coord',
                searchTerm='')
            self.log.info(
                "Coord score for Doc ID 2 = %s" % coord2)

            self.assertTrue(coord1 < coord2,
                            "Testcase failed. Coord score for Doc1 not < Doc2")

            if self.run_via_n1ql:
                self.compare_n1ql_fts_scoring(n1ql_query=n1ql_query, raw_hits=raw_hits)

    def test_fuzzy_query(self):
        """
        Test if fuzzy queries work fine
        """
        test_data = [{"text":"simmer"},
                     {"text":"dimmer"},
                     {"text":"hammer"},
                     {"text":"shimmer"},
                     {"text":"rubber"},
                     {"text":"jabber"},
                     {"text":"kilmer"},
                     {"text":"year"},
                     {"text":"mumma"},
                     {"text":"tool stemmer"},
                     {"text":"he is weak at grammar"},
                     {"text":"sum of all the rows"}]

        self.create_test_dataset(self._master, test_data)
        self.wait_till_items_in_bucket_equal(items=len(test_data))
        index = self.create_index(bucket=self._cb_cluster.get_bucket_by_name(
                                      'default'),
                                  index_name="default_index")
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
            hits, content, _, _ = index.execute_query(query=query,
                                                       zero_results_ok=zero_results_ok,
                                                       expected_hits=expected_hits,
                                                       return_raw_hits=True)
            self.log.info("Docs in Search results = %s" % content)
            self.log.info("Expected Docs = %s" % self.expected_docs)
            if hits>0:
                all_expected_docs_present = True
                for doc in self.expected_docs_list:
                    all_expected_docs_present &= index.is_doc_present_in_query_result_content(content, doc)

                self.assertTrue(all_expected_docs_present, "All expected docs not in search results")

    def test_pagination_of_search_results(self):
        max_matches = self._input.param("query_max_matches", 10000000)
        show_results_from_item = self._input.param("show_results_from_item", 0)
        self.load_data()
        self.wait_till_items_in_bucket_equal(items = self._num_items//2)
        index = self.create_index(
            self._cb_cluster.get_bucket_by_name('default'),
            "default_index")
        self.wait_for_indexing_complete()

        zero_results_ok = True
        expected_hits = int(self._input.param("expected_hits", 0))
        default_query = {"match_all": "true", "field":"name"}
        query = eval(self._input.param("query", str(default_query)))
        if expected_hits:
            zero_results_ok = False
        if isinstance(query, str):
            query = json.loads(query)

        try:
            sort_params = self.build_sort_params()
            for index in self._cb_cluster.get_indexes():
                hits, doc_ids, _, _ = index.execute_query(query=query,
                                                           zero_results_ok=zero_results_ok,
                                                           expected_hits=expected_hits,
                                                           sort_fields=sort_params,
                                                          show_results_from_item=show_results_from_item)

                self.log.info("Hits: %s" % hits)
                self.log.info("Doc IDs: %s" % doc_ids)
                if hits:
                    self.log.info("Count of docs on page = %s" % len(doc_ids))
                    if (show_results_from_item >= 0 and show_results_from_item <=self._num_items):
                        items_on_page = self._num_items - show_results_from_item
                    elif show_results_from_item < 0:
                        items_on_page = self._num_items
                        show_results_from_item = 0
                    else:
                        items_on_page = 0

                    expected_items_on_page = min(items_on_page, max_matches)

                    self.assertEqual(len(doc_ids), expected_items_on_page, "Items per page are not correct")

                    doc_id_prefix='emp'
                    first_doc_id = 10000001

                    i = 0
                    expected_doc_present = True
                    while i < expected_items_on_page:
                        expected_doc_id = doc_id_prefix+str(first_doc_id+i+show_results_from_item)
                        expected_doc_present &= (expected_doc_id in doc_ids)
                        if not expected_doc_present:
                            self.log.info("Doc ID %s not in the search results page" % expected_doc_id)
                        i += 1
                    self.assertTrue(expected_doc_present, "Some docs not present in the results page")

        except Exception as err:
            self.log.error(err)
            self.fail("Testcase failed: " + err.message)

    def test_snippets_highlighting_of_search_term_in_results(self):
        self.load_data()
        index = self.create_index(
            self._cb_cluster.get_bucket_by_name('default'),
            "default_index")
        self.wait_for_indexing_complete()

        index.add_child_field_to_default_mapping("name", "text")
        index.add_child_field_to_default_mapping("manages.reports", "text")
        index.index_definition['uuid'] = index.get_uuid()
        index.update()
        self.sleep(10)
        self.wait_for_indexing_complete()

        zero_results_ok = True
        expected_hits = int(self._input.param("expected_hits", 0))
        default_query = {"match": "Safiya", "field": "name"}
        query = eval(self._input.param("query", str(default_query)))
        if expected_hits:
            zero_results_ok = False
        if isinstance(query, str):
            query = json.loads(query)

        try:
            for index in self._cb_cluster.get_indexes():
                n1ql_results = None
                if self.run_via_n1ql:
                    n1ql_query = "select b, search_meta(b.oouutt) as meta from default b where " \
                                 "search(b, {\"query\": " + json.dumps(
                        query) + ", \"explain\": true, \"highlight\": {}},{\"out\": \"oouutt\"})"
                    n1ql_results = self._cb_cluster.run_n1ql_query(query=n1ql_query)

                hits, contents, _, _ = index.execute_query(query=query,
                                                           zero_results_ok=zero_results_ok,
                                                           expected_hits=expected_hits,
                                                           return_raw_hits=True,
                                                           highlight=True,
                                                           highlight_style=self.highlight_style,
                                                           highlight_fields=self.highlight_fields_list)

                self.log.info("Hits: %s" % hits)
                self.log.info("Content: %s" % contents)
                result = True
                self.expected_results = json.loads(self.expected_results)
                if hits:
                    for expected_doc in self.expected_results:
                        result &= index.validate_snippet_highlighting_in_result_content(
                            contents, expected_doc['doc_id'],
                            expected_doc['field_name'], expected_doc['term'],
                            highlight_style=self.highlight_style)
                        if self.run_via_n1ql:
                            result &= index.validate_snippet_highlighting_in_result_content_n1ql(
                                n1ql_results['results'], expected_doc['doc_id'],
                                expected_doc['field_name'], expected_doc['term'],
                                highlight_style=self.highlight_style)
                    if not result:
                        self.fail(
                            "Testcase failed. Actual results do not match expected.")
        except Exception as err:
            self.log.error(err)
            self.fail("Testcase failed: " + err.message)

    def test_geo_query(self):
        """
        Tests both geo location and bounding box queries
        compares results against ES
        :return: Nothing
        """
        geo_index = self.create_geo_index_and_load()
        self.generate_random_geo_queries(geo_index, self.num_queries)
        if self.run_via_n1ql:
            n1ql_executor = self._cb_cluster
        else:
            n1ql_executor = None
        self.run_query_and_compare(geo_index, n1ql_executor=n1ql_executor)


    def test_sort_geo_query(self):
        """
        Generate random geo location queries and compare the results against
        Elasticsearch
        :return: Nothing
        """
        geo_index = self.create_geo_index_and_load()
        from .random_query_generator.rand_query_gen import FTSESQueryGenerator
        testcase_failed = False
        for i in range(self.num_queries):
            self.log.info("Running Query no --> " + str(i))
            fts_query, es_query = FTSESQueryGenerator.construct_geo_location_query()
            print(fts_query)
            print("fts_query location ---> " + str(fts_query["location"]))
            # If query has geo co-ordinates in form of an object
            if "lon" in fts_query["location"]:
                lon = fts_query["location"]["lon"]
                lat = fts_query["location"]["lat"]
            # If query has geo co-ordinates in form of a list
            elif isinstance(fts_query["location"], list):
                lon = fts_query["location"][0]
                lat = fts_query["location"][1]
            # If query has geo co-ordinates in form of a string or geohash
            elif isinstance(fts_query["location"], str):
                # If the location is in string format
                if "," in fts_query["location"]:
                    lat = float(fts_query["location"].split(",")[0])
                    lon = float(fts_query["location"].split(",")[1])
                else:
                    lat = float(Geohash.decode(fts_query["location"])[0])
                    lon = float (Geohash.decode(fts_query["location"])[1])
            unit = fts_query["distance"][-2:]

            location = None
            case = random.randint(0, 3)

            # Geo location as an object
            if case == 0:
                location = {"lon": lon,
                        "lat": lat}
            # Geo Location as array
            if case == 1:
                location = [lon, lat]

            # Geo Location as string
            if case == 2:
                location = "{0},{1}".format(lat, lon)

            # Geo Location as Geohash
            if case == 3:
                geohash = Geohash.encode(lat, lon, precision=random.randint(3, 8))
                location = geohash
            print("sort_fields_location ----> " + str(location))
            sort_fields = [
                {
                    "by": "geo_distance",
                    "field": "geo",
                    "unit": unit,
                    "location": location
                }
            ]

            hits, doc_ids, _, _ = geo_index.execute_query(
                query=fts_query,
                sort_fields=sort_fields)

            self.log.info("Hits from FTS: {0}".format(hits))
            self.log.info("First 50 docIDs: {0}". format(doc_ids[:50]))

            sort_fields_es = [
                {
                    "_geo_distance": {
                        "geo": location,
                        "order": "asc",
                        "unit": unit
                    }
                }
            ]
            es_query["sort"] = sort_fields_es

            hits2, doc_ids2, _ = self.es.search(index_name="es_index",
                           query=es_query)

            self.log.info("Hits from ES: {0}".format(hits2))
            self.log.info("First 50 doc_ids: {0}".format(doc_ids2[:50]))

            if doc_ids==doc_ids2:
                self.log.info("PASS: Sort order matches!")
            else:
                msg = "FAIL: Sort order mismatch!"
                self.log.error(msg)
                testcase_failed = True

            self.log.info("--------------------------------------------------"
                          "--------------------------------------------------")
        if testcase_failed:
            self.fail(msg)

    def test_xattr_support(self):
        """
        Tests if setting includeXAttrs in index definition
        breaks anything
        :return: Nothing
        """
        self.load_data()
        index = self._cb_cluster.create_fts_index(
            name='default_index',
            source_name='default',
            source_params={"includeXAttrs": True})
        self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        if self._update or self._delete:
            self.async_perform_update_delete(self.upd_del_fields)
            if self._update:
                self.sleep(60, "Waiting for updates to get indexed...")
            self.wait_for_indexing_complete()
        self.generate_random_queries(index, self.num_queries, self.query_types)
        self.run_query_and_compare(index)

    def test_ssl(self):
        """
        Tests if we are able to create an index and query over ssl port
        :return: Nothing
        """
        fts_ssl_port=18094
        import json, subprocess
        idx = {"sourceName": "default",
               "sourceType": "couchbase",
               "type": "fulltext-index"}

        qry = {"indexName": "default_index_1",
                 "query": {"field": "type", "match": "emp"},
                 "size": 10000000}

        self.load_data()
        cert = RestConnection(self._master).get_cluster_ceritificate()
        f = open('cert.pem', 'w')
        f.write(cert)
        f.close()

        cmd = "curl -g -k "+\
              "-XPUT -H \"Content-Type: application/json\" "+\
              "-u Administrator:password "+\
              "https://{0}:{1}/api/index/default_idx -d ".\
                  format(self._master.ip, fts_ssl_port) +\
              "\'{0}\'".format(json.dumps(idx))

        self.log.info("Running command : {0}".format(cmd))
        output = subprocess.check_output(cmd, shell=True)
        if json.loads(output) == {"status":"ok"}:
            query = "curl -g -k " + \
                    "-XPOST -H \"Content-Type: application/json\" " + \
                    "-u Administrator:password " + \
                    "https://{0}:18094/api/index/default_idx/query -d ". \
                        format(self._master.ip, fts_ssl_port) + \
                    "\'{0}\'".format(json.dumps(qry))
            self.sleep(20, "wait for indexing to complete")
            output = subprocess.check_output(query, shell=True)
            self.log.info("Hits: {0}".format(json.loads(output)["total_hits"]))
            if int(json.loads(output)["total_hits"]) != 1000:
                self.fail("Query over ssl failed!")
        else:
            self.fail("Index could not be created over ssl")


    def test_json_types(self):
        import couchbase
        self.load_data()
        self.create_simple_default_index()
        master = self._cb_cluster.get_master_node()
        dic ={}
        dic['null'] = None
        dic['number'] = 12345
        dic['date'] = "2018-01-21T18:25:43-05:00"
        dic['bool'] = True
        dic['string'] = "sample string json"
        dic['array'] = ['element1', 1234, True]
        try:
            from couchbase.cluster import Cluster
            from couchbase.cluster import PasswordAuthenticator
            cluster = Cluster('couchbase://{0}'.format(master.ip))
            authenticator = PasswordAuthenticator('Administrator', 'password')
            cluster.authenticate(authenticator)
            cb = cluster.open_bucket('default')
            for key, value in dic.items():
                cb.upsert(key, value)
        except Exception as e:
            self.fail(e)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        for index in self._cb_cluster.get_indexes():
            self.generate_random_queries(index, 5, self.query_types)
            self.run_query_and_compare(index)

    # This test is to validate if the value for score is 0 for all docs when score=none is specified in the search query.
    def test_score_none(self):
        # Create bucket, create index
        self.load_data()
        self.wait_till_items_in_bucket_equal(items=self._num_items // 2)
        index = self.create_index(
            self._cb_cluster.get_bucket_by_name('default'),
            "default_index")
        self.wait_for_indexing_complete()
        zero_results_ok = True
        expected_hits = int(self._input.param("expected_hits", 0))
        default_query = {"match": "Safiya", "field": "name"}
        query = eval(self._input.param("query", str(default_query)))
        if expected_hits:
            zero_results_ok = False
        if isinstance(query, str):
            query = json.loads(query)

        try:
            for index in self._cb_cluster.get_indexes():
                hits, contents, _, _ = index.execute_query(query=query,
                                                           zero_results_ok=zero_results_ok,
                                                           expected_hits=expected_hits,
                                                           return_raw_hits=True,
                                                           score="none")

                self.log.info("Hits: %s" % hits)
                self.log.info("Content: %s" % contents)
                result = True
                if hits == expected_hits:
                    for doc in contents:
                        # Check if the score of the doc is 0.
                        if "score" in doc:
                            self.assertEqual(doc["score"], 0, "Score is not 0 for doc {0}".format(doc["id"]))
                        else:
                            self.fail("Score key not present in search results for doc {0}".format(doc["id"]))
                    if not result:
                        self.fail(
                            "Testcase failed. Actual results do not match expected.")
                else:
                    self.fail("No. of hits not matching expected hits. Hits = {0}, Expected Hits = {1}".format(hits,
                                                                                                               expected_hits))
        except Exception as err:
            self.log.error(err)
            self.fail("Testcase failed: " + err.message)

    # This test checks the correctness of search results from queries with score=none and without score=none.
    def test_result_correctness_score_none(self):
        # Create bucket, create index
        self.load_data()
        self.wait_till_items_in_bucket_equal(items=self._num_items // 2)
        index = self.create_index(
            self._cb_cluster.get_bucket_by_name('default'),
            "default_index")
        self.wait_for_indexing_complete()
        zero_results_ok = True
        expected_hits = int(self._input.param("expected_hits", 0))
        default_query = {"match": "Safiya", "field": "name"}
        query = eval(self._input.param("query", str(default_query)))
        if expected_hits:
            zero_results_ok = False
        if isinstance(query, str):
            query = json.loads(query)

        try:
            for index in self._cb_cluster.get_indexes():
                hits, doc_ids_with_score_none, _, _ = index.execute_query(query=query,
                                                           zero_results_ok=zero_results_ok,
                                                           return_raw_hits=False,
                                                           score="none")

                self.log.info("Hits: %s" % hits)
                self.log.info("Docs: %s" % doc_ids_with_score_none)
                doc_ids_with_score_none.sort()
                hits, doc_ids_without_score_none, _, _ = index.execute_query(query=query,
                                                                          zero_results_ok=zero_results_ok,
                                                                          return_raw_hits=False)

                self.log.info("Hits: %s" % hits)
                self.log.info("Docs: %s" % doc_ids_without_score_none)
                doc_ids_without_score_none.sort()
                self.assertListEqual(doc_ids_with_score_none, doc_ids_without_score_none, "Doc Ids not equal")

        except Exception as err:
            self.log.error(err)
            self.fail("Testcase failed: " + err.message)

    # Tests the ASCII folding filter with different types of accented characters
    def test_ascii_folding_filter(self):
        # Reference for test data : http://www.jarte.com/help_new/accent_marks_diacriticals_and_special_characters.html
        test_data = [
            {"text": "Ápple"},
            {"text": "Àpple"},
            {"text": "Äpple"},
            {"text": "Âpple"},
            {"text": "Ãpple"},
            {"text": "Åpple"},
            {"text": "ápple"},
            {"text": "àpple"},
            {"text": "äpple"},
            {"text": "âpple"},
            {"text": "ãpple"},
            {"text": "åpple"},
            {"text": "Ðodge"},
            {"text": "ðodge"},
            {"text": "Élephant"},
            {"text": "élephant"},
            {"text": "Èlephant"},
            {"text": "èlephant"},
            {"text": "Ëlephant"},
            {"text": "ëlephant"},
            {"text": "Êlephant"},
            {"text": "êlephant"},
            {"text": "Íceland"},
            {"text": "íceland"},
            {"text": "Ìceland"},
            {"text": "ìceland"},
            {"text": "Ïceland"},
            {"text": "ïceland"},
            {"text": "Îceland"},
            {"text": "îceland"},
            {"text": "Órange"},
            {"text": "órange"},
            {"text": "Òrange"},
            {"text": "òrange"},
            {"text": "Örange"},
            {"text": "örange"},
            {"text": "Ôrange"},
            {"text": "ôrange"},
            {"text": "Õrange"},
            {"text": "õrange"},
            {"text": "Ørange"},
            {"text": "ørange"},
            {"text": "Únicorn"},
            {"text": "únicorn"},
            {"text": "Ùnicorn"},
            {"text": "ùnicorn"},
            {"text": "Ünicorn"},
            {"text": "ünicorn"},
            {"text": "Ûnicorn"},
            {"text": "ûnicorn"},
            {"text": "Ýellowstone"},
            {"text": "ýellowstone"},
            {"text": "Ÿellowstone"},
            {"text": "ÿellowstone"},
            {"text": "Ñocturnal"},
            {"text": "ñocturnal"},
            {"text": "Çelcius"},
            {"text": "çelcius"},
            {"text": "Œlcius"},
            {"text": "œlcius"},
            {"text": "Šmall"},
            {"text": "šmall"},
            {"text": "Žebra"},
            {"text": "žebra"},
            {"text": "Æsthetic"},
            {"text": "æsthetic"},
            {"text": "Þhonetic"},
            {"text": "þhonetic"},
            {"text": "Discuß"},
            {"text": "ÆꜴ"}

        ]

        search_terms = [
            {"term": "apple", "expected_hits": 6},
            {"term": "Apple", "expected_hits": 6},
            {"term": "dodge", "expected_hits": 1},
            {"term": "Dodge", "expected_hits": 1},
            {"term": "Elephant", "expected_hits": 4},
            {"term": "elephant", "expected_hits": 4},
            {"term": "iceland", "expected_hits": 4},
            {"term": "Iceland", "expected_hits": 4},
            {"term": "orange", "expected_hits": 6},
            {"term": "Orange", "expected_hits": 6},
            {"term": "unicorn", "expected_hits": 4},
            {"term": "Unicorn", "expected_hits": 4},
            {"term": "yellowstone", "expected_hits": 2},
            {"term": "Yellowstone", "expected_hits": 2},
            {"term": "nocturnal", "expected_hits": 1},
            {"term": "Nocturnal", "expected_hits": 1},
            {"term": "celcius", "expected_hits": 1},
            {"term": "Celcius", "expected_hits": 1},
            {"term": "oelcius", "expected_hits": 1},
            {"term": "OElcius", "expected_hits": 1},
            {"term": "small", "expected_hits": 1},
            {"term": "Small", "expected_hits": 1},
            {"term": "zebra", "expected_hits": 1},
            {"term": "Zebra", "expected_hits": 1},
            {"term": "aesthetic", "expected_hits": 1},
            {"term": "AEsthetic", "expected_hits": 1},
            {"term": "thhonetic", "expected_hits": 1},
            {"term": "THhonetic", "expected_hits": 1},
            {"term": "Discuss", "expected_hits": 1},
            {"term": "AEAO", "expected_hits": 1}
        ]

        self.create_test_dataset(self._master, test_data)
        self.wait_till_items_in_bucket_equal(items=len(test_data))

        index = self.create_index(
            self._cb_cluster.get_bucket_by_name('default'),
            "default_index")
        self.wait_for_indexing_complete()

        # Update index to have the child field "text"
        index.add_child_field_to_default_mapping("text", "text")
        index.index_definition['uuid'] = index.get_uuid()
        index.update()

        # Update index to have a custom analyzer which uses the ascii folding filter as a char filter
        index.index_definition["params"]["mapping"]["analysis"] = {}
        index.index_definition["params"]["mapping"]["analysis"] = json.loads(
            "{\"analyzers\": {\"asciiff\": {\"char_filters\": [\"asciifolding\"],\"tokenizer\": \"letter\",\"type\": \"custom\" }}}")

        index.index_definition["params"]["mapping"]["default_analyzer"] = "asciiff"

        index.index_definition['uuid'] = index.get_uuid()
        index.update()
        self.wait_for_indexing_complete()

        # Run queries
        try:
            for index in self._cb_cluster.get_indexes():
                all_queries_passed = True
                failed_search_terms = []
                for search_term in search_terms:
                    self.log.info("=============== Querying for term {0} ===============".format(search_term["term"]))
                    query = {'match': search_term["term"], 'field': 'text'}
                    expected_hits = search_term["expected_hits"]
                    hits, contents, _, _ = index.execute_query(query=query,
                                                               zero_results_ok=True,
                                                               return_raw_hits=True)

                    self.log.info("Hits: %s" % hits)
                    self.log.info("Content: %s" % contents)

                    if hits != expected_hits:
                        all_queries_passed = False
                        failed_search_terms.append(search_term["term"])

                self.assertTrue(all_queries_passed,
                                "All search terms did not return expected results. Terms for which queries failed : {0}".format(
                                    str(failed_search_terms)))
        except Exception as err:
            self.log.error(err)
            self.fail("Testcase failed: " + err.message)
