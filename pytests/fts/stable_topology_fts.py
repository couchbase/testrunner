# coding=utf-8

import copy
import json
import random
import time
from threading import Thread
import docker

import Geohash
from membase.helper.cluster_helper import ClusterOperationHelper
from remote.remote_util import RemoteMachineShellConnection

from TestInput import TestInputSingleton
from tasks.task import ESRunQueryCompare
from tasks.taskmanager import TaskManager
from lib.testconstants import FUZZY_FTS_SMALL_DATASET, FUZZY_FTS_LARGE_DATASET
from .fts_base import FTSBaseTest, INDEX_DEFAULTS, QUERY, download_from_s3
from lib.membase.api.exception import FTSException, ServerUnavailableException
from lib.membase.api.rest_client import RestConnection
from couchbase_helper.documentgenerator import SDKDataLoader
import threading


class StableTopFTS(FTSBaseTest):

    def setUp(self):
        super(StableTopFTS, self).setUp()
        self.log.info("Modifying quotas for each services in the cluster")
        try:
            RestConnection(self._cb_cluster.get_master_node()).modify_memory_quota(512, 400, 2000, 1024, 256)
        except Exception as e:
            print(e)
        self.doc_filter_query = {"match": "search", "field": "search_string"}
        self.index_wait_time = 20


    def tearDown(self):
        super(StableTopFTS, self).tearDown()

    def suite_setUp(self):
        pass

    def suite_tearDown(self):
        super(StableTopFTS, self).suite_tearDown()

    def check_fts_service_started(self):
        try:
            rest = RestConnection(self._cb_cluster.get_random_fts_node())
            rest.get_fts_index_definition("invalid_index")
        except ServerUnavailableException as e:
            raise FTSException("FTS service has not started: %s" %e)

    def create_simple_default_index(self, data_loader_output=False):
        plan_params = self.construct_plan_params()
        self.load_data(generator=None, data_loader_output=data_loader_output, num_items=self._num_items)
        if not (self.bucket_storage == "magma"):
            self.wait_till_items_in_bucket_equal(self._num_items//2)
        self.create_fts_indexes_all_buckets(plan_params=plan_params)
        if self.restart_couchbase:
            self._cb_cluster.restart_couchbase_on_all_nodes()
        time.sleep(180)
        if self._update or self._delete:
            self.wait_for_indexing_complete()
            self.validate_index_count(equal_bucket_doc_count=True,
                                      zero_rows_ok=False)
            self.sleep(20)
            self.async_perform_update_delete(self.upd_del_fields)
            if self._update:
                self.sleep(60, "Waiting for updates to get indexed...")
        self.wait_for_indexing_complete()
        time.sleep(20)
        self.validate_index_count(equal_bucket_doc_count=True)

    def test_index_docvalues_option(self):
        collection_index, type, index_scope, index_collections = self.define_index_parameters_collection_related()
        index = self.create_index(
            bucket=self._cb_cluster.get_bucket_by_name('default'),
            index_name="custom_index", collection_index=collection_index, _type=type,
            scope=index_scope, collections=index_collections)
        self.load_data()
        self.wait_for_indexing_complete()
        for node in self._cb_cluster.get_fts_nodes():
            ds = self.get_zap_docvalue_disksize(node)
            if ds:
                if float(ds) != float(0):
                    self.fail("zap files size with docvalue not empty with docValues = False")
                else:
                    self.log.info(" zap files size found to be : {0}".format(ds))
        if self.container_type == "collection":
            type = "scope1.collection1.emp"
        else:
            type = "emp"
        index.update_docvalues_email_custom_index(True, type)
        self.wait_for_indexing_complete()
        for node in self._cb_cluster.get_fts_nodes():
            ds = self.get_zap_docvalue_disksize(node)
            if ds:
                if float(ds) == float(0):
                    self.fail("zap files size with docvalue found to be empty with docValues = True")
                else:
                    self.log.info(" zap files size found to be : {0}".format(ds))

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

        collection_index, type, index_scope, index_collections = self.define_index_parameters_collection_related()
        index = self.create_index(
            self._cb_cluster.get_bucket_by_name('default'),
            "default_index", collection_index=collection_index, _type=type,
            scope=index_scope, collections=index_collections)
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
        expected_hits = int(self._input.param("expected_hits_num", self._num_items))
        self.create_simple_default_index(data_loader_output=True)
        if self.container_type == "collection":
            scan_vectors_before_mutations = self._get_mutation_vectors()
        zero_results_ok = True
        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query,
                                             zero_results_ok=zero_results_ok,
                                             expected_hits=0,
                                             consistency_level=self.consistency_level,
                                             consistency_vectors=self.consistency_vectors
                                            )
            self.log.info("Hits: %s" % hits)
            for i in range(list(list(self.consistency_vectors.values())[0].values())[0]):
                self.async_perform_update_delete(self.upd_del_fields)
            if self.container_type == "collection":
                scan_vectors_after_mutations = self._get_mutation_vectors()
                new_scan_vectors = scan_vectors_after_mutations - scan_vectors_before_mutations
                self.consistency_vectors = {}
                self.consistency_vectors[self._cb_cluster.get_indexes()[0].name] = self._convert_mutation_vector_to_scan_vector(new_scan_vectors)
                self.log.info(self.consistency_vectors)
            hits, _, _, _ = index.execute_query(query,
                                                zero_results_ok=zero_results_ok,
                                                expected_hits=expected_hits,
                                                consistency_level=self.consistency_level,
                                                consistency_vectors=self.consistency_vectors)
            self.log.info("Hits: %s" % hits)

    def test_match_consistency_error(self):
        query = {"match_all": {}}
        fts_node = self._cb_cluster.get_random_fts_node()
        service_map = RestConnection(self._cb_cluster.get_master_node()).get_nodes_services()
        # select FTS node to shutdown
        for node_ip, services in list(service_map.items()):
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
                for i in range(list(list(self.consistency_vectors.values())[0].values())[0]):
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
        self.create_simple_default_index(data_loader_output=True)
        if self.container_type == "collection":
            scan_vectors_before_mutations = self._get_mutation_vectors()
        zero_results_ok = True
        self.sleep(10)
        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query,
                                                zero_results_ok=zero_results_ok,
                                                expected_hits=0,
                                                consistency_level=self.consistency_level,
                                                consistency_vectors=self.consistency_vectors)
            self.log.info("Hits: %s" % hits)
            tasks = []
            tasks.append(Thread(target=self.async_perform_update_delete, args=(self.upd_del_fields,)))
            for i in range(list(list(self.consistency_vectors.values())[0].values())[0]):
                tasks.append(Thread(target=self.async_perform_update_delete, args=(self.upd_del_fields,)))
            for task in tasks:
                task.start()
            num_items = self._num_items
            if self.container_type == "collection":
                num_items = index.get_src_collections_doc_count()
            if timeout is None or timeout <= 60000:
                # Here we assume that the update takes more than 60 seconds
                # when we use timeout <= 60 sec we get timeout error
                # with None we have 60s by default
                num_items = 0
            try:
                if self.container_type == "collection":
                    self.sleep(20)
                    scan_vectors_after_mutations = self._get_mutation_vectors()
                    self.log.info(scan_vectors_after_mutations)
                    new_scan_vectors = scan_vectors_after_mutations - scan_vectors_before_mutations
                    self.consistency_vectors = {}
                    self.consistency_vectors[self._cb_cluster.get_indexes()[0].name] = self._convert_mutation_vector_to_scan_vector(new_scan_vectors)
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
            collection_index, type, index_scope, index_collections = self.define_index_parameters_collection_related()
            index = self.create_index(bucket, "default_index", collection_index=collection_index, _type=type,
                                      scope=index_scope, collections=index_collections)
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
        collection_index, type, index_scope, index_collections = self.define_index_parameters_collection_related()
        index = self.create_index(bucket, "default_index", collection_index=collection_index, _type=type,
                                  scope=index_scope, collections=index_collections)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        hits, _, _, _ = index.execute_query(self.sample_query,
                                     zero_results_ok=False)
        alias = self.create_alias([index], bucket)
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

        self.wait_for_indexing_complete(es_index="emp_es_index")
        self.wait_for_indexing_complete(es_index="wiki_es_index")

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
        collection_index, type, index_scope, index_collections = self.define_index_parameters_collection_related()
        index = self.create_index(bucket, "wiki_index", collection_index=collection_index, _type=type,
                                  scope=index_scope, collections=index_collections, analyzer=self.analyzer)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True,
                                  zero_rows_ok=False)

    def delete_index_then_query(self):
        self.load_data()
        bucket = self._cb_cluster.get_bucket_by_name('default')
        collection_index, type, index_scope, index_collections = self.define_index_parameters_collection_related()
        index = self.create_index(bucket, "default_index", collection_index=collection_index, _type=type,
                                  scope=index_scope, collections=index_collections)
        self.wait_for_indexing_complete()
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
        collection_index, _type, index_scope, index_collections = self.define_index_parameters_collection_related()
        index = self.create_index(bucket, "default_index", collection_index=collection_index, _type=_type,
                                  scope=index_scope, collections=index_collections)
        if self.container_type == "bucket":
            self._cb_cluster.delete_bucket("default")
        else:
            if type(self.collection) is list:
                for c in self.collection:
                    self._cb_cluster._drop_collection(bucket=bucket, scope=self.scope, collection=c, cli_client=self.cli_client)
            else:
                self._cb_cluster._drop_collection(bucket=bucket, scope=self.scope, collection=self.collection, cli_client=self.cli_client)
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
        collection_index, type, index_scope, index_collections = self.define_index_parameters_collection_related()
        self.create_index(bucket, "default_index", collection_index=collection_index, _type=type,
                          scope=index_scope, collections=index_collections)
        self.wait_for_indexing_complete()
        hits2, _, _, _ = alias.execute_query(self.sample_query)
        self.log.info("Hits: {0}".format(hits2))
        if hits1 != hits2:
            self.fail("Hits from alias before index recreation: %s,"
                      " after recreation: %s" %(hits1, hits2))

    def create_alias_on_deleted_index(self):
        self.load_employee_dataset()
        bucket = self._cb_cluster.get_bucket_by_name('default')
        collection_index, type, index_scope, index_collections = self.define_index_parameters_collection_related()
        index = self.create_index(bucket, "default_index", collection_index=collection_index, _type=type,
                                  scope=index_scope, collections=index_collections)
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
        collection_index, type, index_scope, index_collections = self.define_index_parameters_collection_related()
        index = self.create_index(bucket, "sample_index", collection_index=collection_index, _type=type,
                                  scope=index_scope, collections=index_collections)
        self.wait_for_indexing_complete()
        index.name = "new_index"
        try:
            index.update()
        except Exception as e:
            self.log.info("Expected exception: {0}".format(e))

    def edit_index(self):
        self.load_employee_dataset()
        bucket = self._cb_cluster.get_bucket_by_name('default')
        collection_index, type, index_scope, index_collections = self.define_index_parameters_collection_related()
        index = self.create_index(bucket, 'sample_index', collection_index=collection_index, _type=type,
                                  scope=index_scope, collections=index_collections)
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

    def test_metrics_endpoint_availability(self):
        # todo: use [global] section value instead of hardcode
        fts_port = 8094

        fts_node = self._cb_cluster.get_random_fts_node()
        endpoint = self._input.param("endpoint", None)

        self.load_data()
        bucket = self._cb_cluster.get_bucket_by_name('default')
        collection_index, tp, index_scope, index_collections = self.define_index_parameters_collection_related()
        index = self.create_index(bucket, "default_index", collection_index=collection_index, _type=tp,
                                  scope=index_scope, collections=index_collections)
        self.wait_for_indexing_complete(self._num_items//2)

        rest = RestConnection(self._cb_cluster.get_random_fts_node())
        status, content = rest.get_rest_endpoint_data(endpoint, ip=fts_node.ip, port=fts_port)
        self.assertTrue(status, f"Endpoint {endpoint} is not accessible.")

    def update_index_during_large_indexing(self):
        """
            MB-22410 - Updating index with a large dirty write queue
            items = some millions defined at run_time using items param
        """
        rest = RestConnection(self._cb_cluster.get_random_fts_node())
        self.load_employee_dataset()
        bucket = self._cb_cluster.get_bucket_by_name('default')
        collection_index, type, index_scope, index_collections = self.define_index_parameters_collection_related()
        index = self.create_index(bucket, 'sample_index', collection_index=collection_index, _type=type,
                                  scope=index_scope, collections=index_collections)
        # wait till half the keys are indexed
        self.wait_for_indexing_complete(self._num_items//5)
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
        collection_index, type, index_scope, index_collections = self.define_index_parameters_collection_related()
        index = self.create_index(bucket, 'sample_index', collection_index=collection_index, _type=type,
                                  scope=index_scope, collections=index_collections)
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
        collection_index, type, index_scope, index_collections = self.define_index_parameters_collection_related()
        index = self.create_index(bucket, 'sample_index', collection_index=collection_index, _type=type,
                                  scope=index_scope, collections=index_collections)
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


    def document_filter(self):
        #create scope and collection
        collection_list = eval(TestInputSingleton.input.param("collection", ["c1"]))
        self._cb_cluster.create_scope_using_rest(bucket="default",scope="s1")
        self._cb_cluster.create_collection_using_rest(bucket="default",scope="s1",collection=collection_list[0])
        result = None
            
        try:
            result = self.docfilter_data("default","s1",collection_list[0])
        except Exception as ex:
            self.fail(ex)

        res_ = result.decode('utf-8')
        res = json.loads(res_)

        #defining filters and groundtruth values
        term_filter = res["term_filter"]
        bool_filter = res["bool_filter"]
        numeric_filter = res["numeric_filter"]
        date_filter = res["date_filter"]
        conjunction_filter = res["conjunction_filter"]
        disjunction_filter = res["disjunction_filter"]

        term_filter_match = res["term_filter_match"]
        bool_filter_match = res["bool_filter_match"]
        numeric_filter_match = res["numeric_filter_match"]
        date_filter_match = res["date_filter_match"]
        conjunction_res = res["conjunction_filter_docs"]
        disjunction_res = res["disjunction_filter_docs"]

        filter_1 = res["filter_1"]
        filter_2 = res["filter_2"]
        filter_3 = res["filter_3"]

        filter_1_docs = res["filter_1_docs"]
        filter_2_docs = res["filter_2_docs"]
        filter_3_docs = res["filter_3_docs"]

        min_pass = res["min_pass"]
        
        """ individual filter tests """
        #only term filter
        index_ = self.construct_docfilter_index([("term_filter",term_filter)],"default","s1",collection_list[0],"i1")
        index = self._cb_cluster.create_fts_index(name="i1",source_name="default",scope="s1",payload=index_)
        index.index_definition['uuid'] = index.get_uuid()

        time.sleep(self.index_wait_time)

        term_data = term_filter_match #groundtruth
        hits, matches, _, _ = index.execute_query(self.doc_filter_query)

        if len(term_data) != hits:
            self.fail(f"term filter failed. Expected Hits : {len(term_data)} , Actual Hits : {hits}")
        
        if term_data != self.set_groundtruth(matches):
            self.fail(f"term filter failed. Expected Matches : {term_data} , Actual Matches : {matches}")
        
        self.log.info("Term filter test successful")

        #bool filter
        self.update_index_def(index,"bool_filter",bool_filter,collection_name=collection_list[0])   
        index.update()
        time.sleep(self.index_wait_time)

        hits, matches, _, _ = index.execute_query(self.doc_filter_query)

        if len(bool_filter_match) != hits:
            self.fail(f"Bool filter failed. Expected Hits : {len(bool_filter_match)} , Actual Hits : {hits}")
        
        if bool_filter_match  != self.set_groundtruth(matches):
            self.fail(f"Bool filter failed. Expected Matches : {bool_filter_match} , Actual Matches : {matches}")
        
        self.log.info("Bool filter test successful")

        #numeric filter
        self.update_index_def(index,"numeric_filter",numeric_filter,collection_name=collection_list[0])
        index.index_definition['uuid'] = index.get_uuid()
        index.update()
        time.sleep(self.index_wait_time)

        hits, matches, _, _ = index.execute_query(self.doc_filter_query)

        if len(numeric_filter_match) != hits:
            self.fail(f"Numeric filter failed. Expected Hits : {len(numeric_filter_match)} , Actual Hits : {hits}")
        
        if numeric_filter_match  != self.set_groundtruth(matches):
            self.fail(f"Numeric filter failed. Expected Matches : {numeric_filter_match} , Actual Matches : {matches}")
        
        self.log.info("Numeric filter test successful")


        #date time filter
        self.update_index_def(index,"date_filter",date_filter,collection_name=collection_list[0])   
        index.index_definition['uuid'] = index.get_uuid()
        index.update()
        time.sleep(self.index_wait_time)

        hits, matches, _, _ = index.execute_query(self.doc_filter_query)

        if len(date_filter_match) != hits:
            self.fail(f"Date time filter failed. Expected Hits : {len(date_filter_match)} , Actual Hits : {hits}")
        
        if date_filter_match  != self.set_groundtruth(matches):
            self.fail(f"Date time filter failed. Expected Matches : {date_filter_match} , Actual Matches : {matches}")
        
        self.log.info("Date filter test successful")

        #conjunction filter
        self.update_index_def(index,"conjunction_filter",conjunction_filter,collection_name=collection_list[0])
        index.index_definition['uuid'] = index.get_uuid()
        index.update()
        time.sleep(self.index_wait_time)

        hits, matches, _, _ = index.execute_query(self.doc_filter_query)

        if len(conjunction_res) != hits:
            self.fail(f"Conjunction filter failed. Expected Hits : {len(conjunction_res)} , Actual Hits : {hits}")
        
        if conjunction_res  != self.set_groundtruth(matches):
            self.fail(f"Conjunction filter failed. Expected Matches : {conjunction_res} , Actual Matches : {matches}")
        
        self.log.info("Conjunction filter test successful")

        #disjunction filter
        self.update_index_def(index,"disjunction_filter",disjunction_filter,min_pass=min_pass,collection_name=collection_list[0])
        index.index_definition['uuid'] = index.get_uuid() 
        index.update()
        time.sleep(self.index_wait_time)

        hits, matches, _, _ = index.execute_query(self.doc_filter_query)

        if len(disjunction_res) != hits:
            self.fail(f"Disjuntion filter failed. Expected Hits : {len(disjunction_res)} , Actual Hits : {hits}")
        
        if disjunction_res  != self.set_groundtruth(matches):
            self.fail(f"Disjuntion filter failed. Expected Matches : {disjunction_res} , Actual Matches : {matches}")
        
        self.log.info("Disjunction filter test successful")


        """ multiple filter tests """

        #filter1
        self.update_index_def(index,"filter_1",filter_1,collection_name=collection_list[0])
        index.index_definition['uuid'] = index.get_uuid()
        index.update()
        time.sleep(self.index_wait_time)

        hits, matches, _, _ = index.execute_query(self.doc_filter_query)

        if len(filter_1_docs) != hits:
            self.fail(f"Filter 1 failed. Expected Hits : {len(filter_1_docs)} , Actual Hits : {hits}")
        
        if filter_1_docs  != self.set_groundtruth(matches):
            self.fail(f"Filter 1 failed. Expected Matches : {filter_1_docs} , Actual Matches : {matches}")
        
        self.log.info("Filter 1 test successful")

        #filter2
        self.update_index_def(index,"filter_2",filter_2,collection_name=collection_list[0])
        index.index_definition['uuid'] = index.get_uuid()
        index.update()
        time.sleep(self.index_wait_time)

        hits, matches, _, _ = index.execute_query(self.doc_filter_query)

        if len(filter_2_docs) != hits:
            self.fail(f"Filter 2 failed. Expected Hits : {len(filter_2_docs)} , Actual Hits : {hits}")

        if filter_2_docs != self.set_groundtruth(matches):
            self.fail(f"Filter 2 failed. Expected Matches : {filter_2_docs} , Actual Matches : {matches}")
        
        self.log.info("Filter 2 test successful")

        #filter3
        self.update_index_def(index,"filter_3",filter_3,collection_name=collection_list[0])
        index.index_definition['uuid'] = index.get_uuid()
        index.update()
        time.sleep(self.index_wait_time)

        hits, matches, _, _ = index.execute_query(self.doc_filter_query)

        if len(filter_3_docs) != hits:
            self.fail(f"Filter 3 failed. Expected Hits : {len(filter_3_docs)} , Actual Hits : {hits}")
        
        if filter_3_docs != self.set_groundtruth(matches):
            self.fail(f"Filter 3 failed. Expected Matches : {filter_3_docs} , Actual Matches : {matches}")
        
        self.log.info("Filter 3 test successful")


    def index_query_custom_mapping(self):
        """
         uses RQG for custom mapping
        """
        # create a custom map, disable default map
        collection_index, type, index_scope, index_collections = self.define_index_parameters_collection_related()
        index = self.create_index(
            bucket=self._cb_cluster.get_bucket_by_name('default'),
            index_name="custom_index", collection_index=collection_index, _type=type,
            scope=index_scope, collections=index_collections)
        if self.es:
            self.create_es_index_mapping(index.es_custom_map,
                                         index.index_definition)
        self.load_data()
        time.sleep(150)
        self.wait_for_indexing_complete()
        if self._update or self._delete:
            self.async_perform_update_delete(self.upd_del_fields)
            if self.vector_search:
                if self._update:
                    index.faiss_index = self.create_faiss_index(self.update_gen)
                elif self._delete:
                    index.faiss_index = self.create_faiss_index(self.delete_gen)
            if self._update:
                self.sleep(60, "Waiting for updates to get indexed...")
            self.wait_for_indexing_complete()
        else:
            if self.vector_search:
                index.faiss_index = self.create_faiss_index(self.create_gen)
        self.generate_random_queries(index, self.num_queries, self.query_types)
        self.sleep(30, "additional wait time to be sure, fts index is ready")

        if self.run_via_n1ql:
            n1ql_executor = self._cb_cluster
        else:
            n1ql_executor = None
        self.run_query_and_compare(index, n1ql_executor=n1ql_executor, use_collections=collection_index)

    def test_collection_index_data_mutations(self):
        collection_index, type, index_scope, index_collections = self.define_index_parameters_collection_related()
        index = self.create_index(bucket=self._cb_cluster.get_bucket_by_name('default'),
                                  index_name="custom_index", collection_index=collection_index, _type=type,
                                  scope=index_scope, collections=index_collections)
        num_collections = TestInputSingleton.input.param("num_collections", 1)

        self.load_data()
        time.sleep(240)
        self.wait_for_indexing_complete()
        query = {"query": "mutated:0"}
        hits,_,_,_ = index.execute_query(query)
        self.assertEquals(hits, 1000 * num_collections, f"Expected hits does not match after insert. Expected - {1000 * num_collections}, found {hits}")

        self._update = True
        self.async_perform_update_delete(self.upd_del_fields)
        self.wait_for_indexing_complete()
        self.sleep(30, "sleep additional time.")
        query = {"query": "mutated:0"}
        hits,_,_,_ = index.execute_query(query)
        self.assertEquals(hits, 700*num_collections, f"Expected hits does not match after update. Expected - {700*num_collections}, found {hits}")

        self._update = False
        self._delete = True
        self.async_perform_update_delete(self.upd_del_fields)
        time.sleep(120)
        self.wait_for_indexing_complete()
        query = {"query": "type:emp"}
        time.sleep(120)
        hits,_,_,_ = index.execute_query(query)
        self.assertEquals(hits, 700*num_collections, f"Expected hits does not match after delete. Expected - {700*num_collections}, found {hits}")

        self._expires = 10
        self._update = True
        self._delete = False
        self.async_perform_update_delete()
        self.wait_for_indexing_complete()
        self.sleep(30, "Wait for docs expiration")
        query = {"query": "type:emp"}
        hits,_,_,_ = index.execute_query(query)
        self.assertEquals(hits, 400*num_collections, f"Expected hits does not match after expiration. Expected - {400*num_collections}, found {hits}")

    def test_collection_mutations_isolation(self):
        #delete unnecessary bucket
        self._cb_cluster.delete_bucket("default")
        #create bucket
        bucket_size = 200
        bucket_priority = None
        bucket_type = TestInputSingleton.input.param("bucket_type", "membase")
        maxttl = TestInputSingleton.input.param("maxttl", None)
        self._cb_cluster.create_default_bucket(
            bucket_size,
            self._num_replicas,
            eviction_policy='valueOnly',
            bucket_priority=bucket_priority,
            bucket_type=bucket_type,
            maxttl=maxttl,
            bucket_storage='couchstore',
            bucket_name='bucket1')
        #create 2 scopes
        self.cli_client.create_scope(bucket='bucket1', scope='scope1')
        self.cli_client.create_scope(bucket='bucket1', scope='scope2')
        #create collections with same name
        self.cli_client.create_collection(bucket='bucket1', scope='scope1', collection='collection1')
        self.cli_client.create_collection(bucket='bucket1', scope='scope2', collection='collection1')
        # create 2 indexes
        index1 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket1'),
                                   "index_scope1", collection_index=True, _type="scope1.collection1",
                                   scope="scope1", collections=["collection1"])
        index2 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket1'),
                                   "index_scope2", collection_index=True, _type="scope2.collection1",
                                   scope="scope2", collections=["collection1"])

        #load data into collections
        bucket = self._cb_cluster.get_bucket_by_name('bucket1')
        gen_create = SDKDataLoader(num_ops=1000, percent_create=100, percent_update=0, percent_delete=0,
                 load_pattern="uniform", start_seq_num=1, key_prefix="doc_", key_suffix="_",
                 scope="scope1", collection="collection1", json_template="emp", doc_expiry=0,
                 doc_size=500, get_sdk_logs=False, username="Administrator", password="password", timeout=1000,
                 start=0, end=0, op_type="create", all_collections=False, es_compare=False, es_host=None, es_port=None,
                 es_login=None, es_password=None)

        load_tasks = self._cb_cluster.async_load_bucket_from_generator(bucket, gen_create)
        for task in load_tasks:
            task.result()

        gen_create1 = SDKDataLoader(num_ops=1000, percent_create=100, percent_update=0, percent_delete=0,
                 load_pattern="uniform", start_seq_num=1, key_prefix="doc_", key_suffix="_",
                 scope="scope2", collection="collection1", json_template="emp", doc_expiry=0,
                 doc_size=500, get_sdk_logs=False, username="Administrator", password="password", timeout=1000,
                 start=0, end=0, op_type="create", all_collections=False, es_compare=False, es_host=None, es_port=None,
                 es_login=None, es_password=None)
        load_tasks = self._cb_cluster.async_load_bucket_from_generator(bucket, gen_create1)
        for task in load_tasks:
            task.result()

        # run all types of mutations on scope1, check that scope2 results remain the same
        query = {"query": "dept:Marketing"}
        untouched_hits,_,_,_ = index1.execute_query(query)
        gen_update2 = SDKDataLoader(num_ops=100, percent_create=0, percent_update=100, percent_delete=0,
                 load_pattern="uniform", start_seq_num=1, key_prefix="doc_", key_suffix="_",
                 scope="scope2", collection="collection1", json_template="emp", doc_expiry=0,
                 doc_size=500, get_sdk_logs=False, username="Administrator", password="password", timeout=1000,
                 start=0, end=100, op_type="update", all_collections=False, es_compare=False, es_host=None, es_port=None,
                 es_login=None, es_password=None)
        load_tasks = self._cb_cluster.async_load_bucket_from_generator(bucket, gen_update2)
        for task in load_tasks:
            task.result()
        query = {"query": "dept:Marketing"}
        hits,_,_,_ = index1.execute_query(query)
        self.assertEqual(hits, untouched_hits, "Update isolation test is failed")

        gen_delete2 = SDKDataLoader(num_ops=100, percent_create=0, percent_update=0, percent_delete=100,
                 load_pattern="uniform", start_seq_num=1, key_prefix="doc_", key_suffix="_",
                 scope="scope2", collection="collection1", json_template="emp", doc_expiry=0,
                 doc_size=500, get_sdk_logs=False, username="Administrator", password="password", timeout=1000,
                 start=0, end=100, op_type="delete", all_collections=False, es_compare=False, es_host=None, es_port=None,
                 es_login=None, es_password=None)
        load_tasks = self._cb_cluster.async_load_bucket_from_generator(bucket, gen_delete2)
        for task in load_tasks:
            task.result()
        query = {"query": "dept:Marketing"}
        hits,_,_,_ = index1.execute_query(query)
        self.assertEqual(hits, untouched_hits, "Delete isolation test is failed")

        gen_exp2 = SDKDataLoader(num_ops=100, percent_create=0, percent_update=100, percent_delete=0,
                 load_pattern="uniform", start_seq_num=500, key_prefix="doc_", key_suffix="_",
                 scope="scope2", collection="collection1", json_template="emp", doc_expiry=10,
                 doc_size=500, get_sdk_logs=False, username="Administrator", password="password", timeout=1000,
                 start=500, end=600, op_type="update", all_collections=False, es_compare=False, es_host=None, es_port=None,
                 es_login=None, es_password=None)
        load_tasks = self._cb_cluster.async_load_bucket_from_generator(bucket, kv_gen=gen_exp2, exp=10)
        for task in load_tasks:
            task.result()
        query = {"query": "dept:Marketing"}
        hits,_,_,_ = index1.execute_query(query)
        self.assertEqual(hits, untouched_hits, "Expiration isolation test is failed")

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
        collection_index, type, index_scope, index_collections = self.define_index_parameters_collection_related()
        index = self.create_index(
            bucket=self._cb_cluster.get_bucket_by_name('default'),
            index_name="custom_index", collection_index=collection_index, _type=type,
            scope=index_scope, collections=index_collections)
        self.create_es_index_mapping(index.es_custom_map, index.index_definition)
        self.load_data()
        self.wait_for_indexing_complete()
        self.generate_random_queries(index, self.num_queries, self.query_types)
        try:
            self.run_query_and_compare(index)
        except AssertionError as err:
            error_msg = str(err)
            self.log.error(err)
            fail = True
        self.log.info("Editing custom index with new map...")
        index.generate_new_custom_map(seed=index.cm_id+10, collection_index=collection_index, type_mapping=type)
        index.index_definition['uuid'] = index.get_uuid()
        index.update()
        # updating mapping on ES is not easy, often leading to merge issues
        # drop and recreate the index, load again
        self.create_es_index_mapping(index.es_custom_map)
        self.load_data()
        self.wait_for_indexing_complete()
        if self.run_via_n1ql:
            n1ql_executor = self._cb_cluster
        else:
            n1ql_executor = None

        self.run_query_and_compare(index, n1ql_executor=n1ql_executor)
        if fail:
            raise error_msg

    def index_query_in_parallel(self):
        """
        Run rqg before querying is complete
        turn off es validation
        goal is to make sure there are no fdb or cbft crashes
        """
        collection_index, type, index_scope, index_collections = self.define_index_parameters_collection_related()
        index = self.create_index(
            bucket=self._cb_cluster.get_bucket_by_name('default'),
            index_name="default_index", collection_index=collection_index, _type=type,
            scope=index_scope, collections=index_collections)
        self.load_data()
        self.generate_random_queries(index, self.num_queries, self.query_types)
        self.run_query_and_compare(index)

    def load_index_query_all_in_parallel(self):
        """
        Run rqg before querying is complete
        turn off es validation
        goal is to make sure there are no fdb or cbft crashes
        """
        collection_index, type, index_scope, index_collections = self.define_index_parameters_collection_related()
        index = self.create_index(
            bucket=self._cb_cluster.get_bucket_by_name('default'),
            index_name="default_index", collection_index=collection_index, _type=type,
            scope=index_scope, collections=index_collections)
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
        gen = copy.deepcopy(self.create_gen)
        task = self.es.async_bulk_load_ES(index_name='es_index',
                                   gen=gen,
                                   op_type='create')
        task.result()
        self.wait_for_indexing_complete()
        try:
            if self.run_via_n1ql:
                n1ql_executor = self._cb_cluster
            else:
                n1ql_executor = None

            self.run_query_and_compare(index, n1ql_executor=n1ql_executor)
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
            if str(err).count(error_msg, 0, len(str(err))):
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
        collection_index, type, index_scope, index_collections = self.define_index_parameters_collection_related()
        index = self.create_index(
            self._cb_cluster.get_bucket_by_name('default'),
            "default_index", collection_index=collection_index, _type=type,
            scope=index_scope, collections=index_collections)
        self.wait_for_indexing_complete()
        if self.container_type == 'bucket':
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
        else:
            index.add_child_field_to_default_collection_mapping(field_name="type",
                                                     field_type="text",
                                                     field_alias="type",
                                                     analyzer="keyword", scope=self.scope, collection=self.collection)
            if field_indexed:
                index.add_child_field_to_default_collection_mapping(field_name="dept",
                                                     field_type="text",
                                                     field_alias="dept",
                                                     analyzer="keyword", scope=self.scope, collection=self.collection)
                index.add_child_field_to_default_collection_mapping(field_name="salary",
                                                     field_type="number",
                                                     field_alias="salary", scope=self.scope, collection=self.collection)
                index.add_child_field_to_default_collection_mapping(field_name="join_date",
                                                     field_type="datetime",
                                                     field_alias="join_date", scope=self.scope, collection=self.collection)
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
            self.fail("Testcase failed: "+ str(err))

    def test_facets_during_index(self):
        field_indexed = self._input.param("field_indexed", True)
        self.load_data()
        collection_index, type, index_scope, index_collections = self.define_index_parameters_collection_related()
        index = self.create_index(
            self._cb_cluster.get_bucket_by_name('default'),
            "default_index", collection_index=collection_index, _type=type,
            scope=index_scope, collections=index_collections)
        self.sleep(5)
        if self.container_type == 'bucket':
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
        else:
            index.add_child_field_to_default_collection_mapping(field_name="type",
                                                     field_type="text",
                                                     field_alias="type",
                                                     analyzer="keyword", scope=self.scope, collection=self.collection)
            if field_indexed:
                index.add_child_field_to_default_collection_mapping(field_name="dept",
                                                         field_type="text",
                                                         field_alias="dept",
                                                         analyzer="keyword", scope=self.scope, collection=self.collection)
                index.add_child_field_to_default_collection_mapping(field_name="salary",
                                                         field_type="number",
                                                         field_alias="salary", scope=self.scope, collection=self.collection)
                index.add_child_field_to_default_collection_mapping(field_name="join_date",
                                                         field_type="datetime",
                                                         field_alias="join_date", scope=self.scope, collection=self.collection)
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
                self.fail("Testcase failed: "+ str(err))

    def test_doc_config(self):
        # delete default bucket
        self._cb_cluster.delete_bucket("default")
        master = self._cb_cluster.get_master_node()

        # Load Travel Sample bucket and create an index
        self.load_sample_buckets(server=master, bucketName="travel-sample")
        bucket = self._cb_cluster.get_bucket_by_name("travel-sample")
        index = self.create_index(bucket, "travel-index", scope="_default", collections=["_default"])
        self.sleep(10)
        self.wait_for_indexing_complete()

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
            self.fail("Testcase failed: " + str(err))

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
                    contents=contents, doc_id='emp10000045')
                score_before_boosting_doc2 = index.get_score_from_query_result_content(
                    contents=contents, doc_id='emp10000053')

                self.log.info("Scores before boosting:")
                self.log.info("")
                self.log.info("emp10000045: %s", score_before_boosting_doc1)
                self.log.info("emp10000053: %s", score_before_boosting_doc2)

        except Exception as err:
            self.log.error(err)
            self.fail("Testcase failed: " + str(err))

        if not score_before_boosting_doc1 == score_before_boosting_doc2:
            self.fail("Testcase failed: Scores for emp10000045 & emp10000053 "
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
                contents=contents, doc_id='emp10000045')
            score_after_boosting_doc2 = index.get_score_from_query_result_content(
                contents=contents, doc_id='emp10000053')

            self.log.info("Scores after boosting:")
            self.log.info("")
            self.log.info("emp10000045: %s", score_after_boosting_doc1)
            self.log.info("emp10000053: %s", score_after_boosting_doc2)
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
                                                   analyzer="standard")

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
            self.fail("Testcase failed: " + str(err))

    def test_sorting_of_results(self):
        self.load_data()
        self.wait_till_items_in_bucket_equal(self._num_items//2)
        collection_index, type, index_scope, index_collections = self.define_index_parameters_collection_related()
        index = self.create_index(
            self._cb_cluster.get_bucket_by_name('default'),
            "default_index", collection_index=collection_index, _type=type,
            scope=index_scope, collections=index_collections)
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
            self.fail("Testcase failed: " + str(err))

    def test_sorting_of_results_during_indexing(self):
        self.load_data()
        self.wait_till_items_in_bucket_equal(self._num_items//2)
        collection_index, type, index_scope, index_collections = self.define_index_parameters_collection_related()
        index = self.create_index(
            self._cb_cluster.get_bucket_by_name('default'),
            "default_index", collection_index=collection_index, _type=type,
            scope=index_scope, collections=index_collections)
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
            self.fail("Testcase failed: " + str(err))

    def test_sorting_of_results_on_non_indexed_fields(self):
        self.load_data()
        collection_index, type, index_scope, index_collections = self.define_index_parameters_collection_related()
        index = self.create_index(
            self._cb_cluster.get_bucket_by_name('default'),
            "default_index", collection_index=collection_index, _type=type,
            scope=index_scope, collections=index_collections)
        self.wait_for_indexing_complete()
        if self.container_type == 'bucket':
            index.add_child_field_to_default_mapping(field_name="name",
                                                     field_type="text",
                                                     field_alias="name",
                                                     analyzer="en")
        else:
            index.add_child_field_to_default_collection_mapping(field_name="name",
                                                                field_type="text",
                                                                field_alias="name",
                                                                analyzer="en", scope=self.scope,
                                                                collection=self.collection)
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
            self.fail("Testcase failed: " + str(err))

    def test_scoring_tf_score(self):
        """
        Test if the TF score in the Scoring functionality works fine
        """
        test_data = [{"text":"cat - a lazy cat and a brown cat"},
                     {"text":"a lazy cat and a brown cat"},
                     {"text":"a lazy cat"}]

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
        test_data = [{"text":"a brown cat"},
                     {"text":"a lazy cat"},
                     {"text":"a lazy cat and a brown cat"},
                     {"text":"a brown dog"},
                     {"text":"a lazy dog"},
                     {"text":"a lazy dog and a brown dog"},
                     {"text":"a lazy fox and a brown fox"}]

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
        test_data = [{"text":"a cat"},
                     {"text":"a lazy cat"},
                     {"text":"a lazy cat and a brown cat"}]

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
        test_data = [{"text":"a cat"},
                     {"text":"a lazy cat"},
                     {"text":"a lazy cat and a brown cat"}]

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
        test_data = [{"text":"a cat"},
                     {"text":"a lazy cat"}]

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
        default_query = {"match_all": "true", "field": "name"}
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
            self.fail("Testcase failed: " + str(err))

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
            self.fail("Testcase failed: " + str(err))

    def test_geospatial(self):
       geo_index = self.create_index_custom_shapes()
       self.generate_random_geoshape_queries(geo_index, self.num_queries)
       if self.run_via_n1ql:
            n1ql_executor = self._cb_cluster
       else:
            n1ql_executor = None
       self.run_query_and_compare(geo_index, n1ql_executor=n1ql_executor,dataset="geojson")

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

    def test_geo_polygon_query(self):
        """
        Tests both geo polygon queries
        compares results against ES
        :return: Nothing
        """
        geo_index = self.create_geo_index_and_load()
        self.generate_random_geo_polygon_queries(geo_index, self.num_queries, self.polygon_feature, self.num_vertices)
        if self.run_via_n1ql:
            n1ql_executor = self._cb_cluster
        else:
            n1ql_executor = None
        self.run_query_and_compare(geo_index, n1ql_executor=n1ql_executor)

    def test_geo_polygon_on_edge_corner_query(self):
        expected_hits = int(self._input.param("expected_hits", 0))
        expected_doc_ids = self._input.param("expected_doc_ids", None)
        polygon_points = str(self._input.param("polygon_points", None))
        geo_index = self.create_geo_index_and_load()

        query = '{"field": "geo", "polygon_points" : ' + polygon_points + '}'

        self.log.info(query)

        query = json.loads(query)

        contents = ""

        for index in self._cb_cluster.get_indexes():
            hits, contents, _, _ = index.execute_query(query=query,
                                                       zero_results_ok=True,
                                                       expected_hits=expected_hits,
                                                       return_raw_hits=True)

            self.log.info("Hits: %s" % hits)
            self.log.info("Content: %s" % contents)

        for doc_id in expected_doc_ids.split(","):
            doc_exist = False
            for content in contents:
                if content['id'] == doc_id:
                    self.log.info(content)
                    doc_exist = True
            if not doc_exist:
                self.fail("expected doc_id : " + str(doc_id) + " does not exist")


    def test_geo_polygon_with_holes_must_not(self):
        geo_index = self.create_geo_index_and_load()

        query = '{"must": {"conjuncts": [{"field": "geo", "polygon_points": ' \
                '[[-124.29807832031247, 38.01868304390075], ' \
                '[-122.34800507812497, 37.12617594722073], [-120.52976777343747, 38.35114759945404], ' \
                '[-120.72752167968747, 39.44978110907268], [-122.90834850139811, 40.22582625155702], ' \
                '[-124.24868053264811, 39.61072953444142]]}]}, ' \
                '"must_not": {"disjuncts": [{"field": "geo", "polygon_points": ' \
                '[[-122.56773164062497, 39.72703407666045], ' \
                '[-123.02915742187497, 38.96238669420149], [-122.07334687499997, 38.189396892659744], ' \
                '[-120.79893281249997, 38.585519836298694]]}]}}'

        self.log.info(query)

        query = json.loads(query)

        for index in self._cb_cluster.get_indexes():
            hits, contents, _, _ = index.execute_query(query=query,
                                                       zero_results_ok=False,
                                                       expected_hits=18,
                                                       return_raw_hits=True)

            self.log.info("Hits: %s" % hits)
            self.log.info("Content: %s" % contents)



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
            self.log.info(fts_query)
            self.log.info("fts_query location ---> " + str(fts_query["location"]))
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
                self.log.info("sort_fields_location ----> " + str(location))
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

            if self.es:
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
        collection_index, type, index_scope, index_collections = self.define_index_parameters_collection_related()
        plan_params = self.construct_plan_params()
        index = self._cb_cluster.create_fts_index(
            name='default_index',
            source_name='default',
            collection_index=collection_index,
            _type=type,
            plan_params=plan_params,
            source_params={"includeXAttrs": True},
            scope=index_scope,
            collections=index_collections)
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
        if self.container_type == 'bucket':
            idx = {"sourceName": "default",
                   "sourceType": "couchbase",
                   "type": "fulltext-index"}
        else:
            idx = {'type': 'fulltext-index',
                   'params': {'mapping': {'default_mapping': {'properties': {}, 'dynamic': False, 'enabled': False},
                                          'types': {self.scope+'.'+self.collection: {'default_analyzer': 'standard', 'dynamic': True, 'enabled': True}}
                                          },
                   'doc_config': {'mode': 'scope.collection.type_field', 'type_field': 'type'}},
                   'sourceType': 'couchbase', 'sourceName': 'default'}

        qry = {"indexName": "default_index_1",
                 "query": {"field": "type", "match": "emp"},
                 "size": 10000000}

        self.load_data()
        cert = RestConnection(self._master).get_cluster_ceritificate()
        f = open('cert.pem', 'w')
        f.write(cert)
        f.close()

        fts_node = self._cb_cluster.get_random_fts_node()

        cmd = "curl -g -k "+\
              "-XPUT -H \"Content-Type: application/json\" "+\
              "-u Administrator:password "+\
              "https://{0}:{1}/api/index/default_idx -d ".\
                  format(fts_node.ip, fts_ssl_port) +\
              "\'{0}\'".format(json.dumps(idx))

        self.log.info("Running command : {0}".format(cmd))
        output = subprocess.check_output(cmd, shell=True)
        if json.loads(output)["status"] == "ok":
            query = "curl -g -k " + \
                    "-XPOST -H \"Content-Type: application/json\" " + \
                    "-u Administrator:password " + \
                    "https://{0}:18094/api/index/default_idx/query -d ". \
                        format(fts_node.ip, fts_ssl_port) + \
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
            if self.container_type == 'bucket':
                for key, value in list(dic.items()):
                    cb.upsert(key, value)
            else:
                count = 1
                for key, value in list(dic.items()):
                    if not value:
                        query = "insert into default:default." + self.scope + "." + self.collection + " (key,value) values ('key_" + str(
                            count) + "', {'" + str(key) + "' : '" + str(value) + "'})"
                    else:
                        if isinstance(value, str):
                            query = "insert into default:default."+self.scope+"."+self.collection+" (key,value) values ('key_"+str(count)+"', {'"+str(key)+"' : '"+str(value)+"'})"
                        else:
                            query = "insert into default:default."+self.scope+"."+self.collection+" (key,value) values ('key_"+str(count)+"', {'"+str(key)+"' : "+str(value)+"})"
                    count += 1
                    cb.n1ql_query(query).execute()
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
            self.fail("Testcase failed: " + str(err))

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
            self.fail("Testcase failed: " + str(err))

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
            self.fail("Testcase failed: " + str(err))

    def test_snowball_stemmer_token_filter(self):
        # Reference for test data : http://www.jarte.com/help_new/accent_marks_diacriticals_and_special_characters.html

        all_test_data = {
                "generic": [
                    {"text": "This is something else 1"},
                    {"text": "This is something else 2"},
                    {"text": "This is other indtadfgadad"},
                    {"text": "This is not that"}
                ],
                "test_hu_data": [
                    {"text": "This is babakocsi"},
                    {"text": "This is babakocsijáért"},
                    {"text": "This is babakocsit"},
                    {"text": "This is babakocsiért"}
                ],
                "test_da_data": [
                    {"text": "This is indtage"},
                    {"text": "This is indtagelse"},
                    {"text": "This is indtager"},
                    {"text": "This is indtages"},
                    {"text": "This is indtaget"}
                ],
                "test_fr_data": [
                    {"text": "This is continu"},
                    {"text": "This is continua"},
                    {"text": "This is continuait"},
                    {"text": "This is continuant"},
                    {"text": "This is continuation"}
                ],
                "test_en_data": [
                    {"text": "This is enjoying"},
                    {"text": "This is enjoys"},
                    {"text": "This is enjoy"},
                    {"text": "This is enjoyed"},
                    {"text": "This is enjoyments"}
                ],
                "test_it_data": [
                    {"text": "This is abbandonata"},
                    {"text": "This is abbandonate"},
                    {"text": "This is abbandonati"},
                    {"text": "This is abbandonato"},
                    {"text": "This is abbandonava"}
                ],
                "test_es_data": [
                    {"text": "This is torá"},
                    {"text": "This is toreado"},
                    {"text": "This is toreándolo"},
                    {"text": "This is toreara"},
                    {"text": "This is torear"}
                ],
                "test_de_data": [
                    {"text": "This is aufeinanderfolge"},
                    {"text": "This is aufeinanderfolgen"},
                    {"text": "This is aufeinanderfolgend"},
                    {"text": "This is aufeinanderfolgende"},
                    {"text": "This is aufeinanderfolgenden"}
                ]

            }

        all_search_terms = {
                "search_hu_terms": [
                        {"term": "babakocs", "expected_hits": 4}
                    ],
                "search_da_terms": [
                        {"term": "indtag", "expected_hits": 5}
                    ],
                "search_fr_terms": [
                    {"term": "continu", "expected_hits": 5}
                ],
                "search_en_terms": [
                    {"term": "enjoy", "expected_hits": 5}
                ],
                "search_it_terms": [
                    {"term": "abbandon", "expected_hits": 5}
                ],
                "search_es_terms": [
                    {"term": "tor", "expected_hits": 5}
                ],
                "search_de_terms": [
                    {"term": "aufeinanderfolg", "expected_hits": 5}
                ]
            }

        test_data = all_test_data[self._input.param("test_data", "test_da_data")] + all_test_data["generic"]
        search_terms = all_search_terms[self._input.param("search_terms", "search_da_terms")]
        token_filter = self._input.param("token_filter", "stemmer_da_snowball")

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
            "{\"analyzers\": {\"customAnalyzer1\": {\"token_filters\": [\"" + token_filter + "\"],\"tokenizer\": \"whitespace\",\"type\": \"custom\" }}}")

        index.index_definition["params"]["mapping"]["default_analyzer"] = "customAnalyzer1"

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
            self.fail("Testcase failed: " + str(err))

    def test_drop_index_container(self):
        rest = RestConnection(self._cb_cluster.get_random_fts_node())

        drop_container = self._input.param("drop_container")
        drop_name = self._input.param("drop_name")
        plan_params = self.construct_plan_params()

        self.load_data(generator=None)
        self.wait_till_items_in_bucket_equal(self._num_items//2)
        self.create_fts_indexes_all_buckets(plan_params=plan_params)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

        if drop_container == 'collection':
            self.cli_client.delete_collection(scope=self.scope, collection=drop_name)
        elif drop_container == 'scope':
            self.cli_client.delete_scope(scope=drop_name)
        else:
            self._cb_cluster.delete_bucket("default")
        self.sleep(20)

        for idx in self._cb_cluster.get_indexes():
            status,dfn = rest.get_fts_index_definition(idx.name)
            self.assertEqual(status, False, "FTS index was not dropped after kv container drop.")
            self._cb_cluster.get_indexes().remove(idx)

    def test_drop_busy_index_container_building(self):
        rest = RestConnection(self._cb_cluster.get_random_fts_node())

        drop_container = self._input.param("drop_container")
        drop_name = self._input.param("drop_name")
        plan_params = self.construct_plan_params()

        self.load_data(generator=None)
        self.create_fts_indexes_all_buckets(plan_params=plan_params)

        if drop_container == 'collection':
            self.cli_client.delete_collection(scope=self.scope, collection=drop_name)
        elif drop_container == 'scope':
            self.cli_client.delete_scope(scope=drop_name)
        else:
            self._cb_cluster.delete_bucket("default")
        self.sleep(60)

        for idx in self._cb_cluster.get_indexes():
            status,dfn = rest.get_fts_index_definition(idx.name)
            self.assertEqual(status, False, "FTS index was not dropped during index build.")
            self._cb_cluster.get_indexes().remove(idx)

    def test_drop_busy_index_container_scan(self):
        rest = RestConnection(self._cb_cluster.get_random_fts_node())

        drop_container = self._input.param("drop_container")
        drop_name = self._input.param("drop_name")
        plan_params = self.construct_plan_params()

        self.load_data(generator=None)
        self.create_fts_indexes_all_buckets(plan_params=plan_params)
        self.wait_for_indexing_complete()
        index = self._cb_cluster.get_indexes()[0]
        query = self._input.param("query")
        import threading
        query_thread = threading.Thread(target=self._index_query_wrapper, args=(index, query))
        drop_thread = threading.Thread(target=self._drop_container_wrapper, args=(drop_container, drop_name))
        query_thread.daemon = True
        drop_thread.daemon = True
        query_thread.start()
        drop_thread.start()

        query_thread.join()
        drop_thread.join()
        self.sleep(30, "wait for index drop")

        for idx in self._cb_cluster.get_indexes():
            status,dfn = rest.get_fts_index_definition(idx.name)
            self.assertEqual(status, False, "FTS index was not dropped during index scan.")
            self._cb_cluster.get_indexes().remove(idx)

    def test_drop_busy_index_container_mutations(self):
        rest = RestConnection(self._cb_cluster.get_random_fts_node())

        drop_container = self._input.param("drop_container")
        drop_name = self._input.param("drop_name")
        plan_params = self.construct_plan_params()

        self.load_data(generator=None)
        self.create_fts_indexes_all_buckets(plan_params=plan_params)
        self.wait_for_indexing_complete()
        self._cb_cluster.run_n1ql_query(query=f"create primary index on default:default.{self.scope}.{self.collection}")
        index = self._cb_cluster.get_indexes()[0]
        query = f"update default:default.{self.scope}.{self.collection} set email='mutated@gmail.com'"
        import threading
        query_thread = threading.Thread(target=self._n1ql_query_wrapper, args=[query])
        drop_thread = threading.Thread(target=self._drop_container_wrapper, args=(drop_container, drop_name))
        query_thread.daemon = True
        drop_thread.daemon = True
        query_thread.start()
        drop_thread.start()

        query_thread.join()
        drop_thread.join()
        self.sleep(30, "wait for index drop")

        for idx in self._cb_cluster.get_indexes():
            status,dfn = rest.get_fts_index_definition(idx.name)
            self.assertEqual(status, False, "FTS index was not dropped during index scan.")
            self._cb_cluster.get_indexes().remove(idx)

    def test_concurrent_drop_index_and_container(self):
        rest = RestConnection(self._cb_cluster.get_random_fts_node())

        drop_container = self._input.param("drop_container")
        drop_name = self._input.param("drop_name")
        plan_params = self.construct_plan_params()

        self.load_data(generator=None)
        self.create_fts_indexes_all_buckets(plan_params=plan_params)
        self.wait_for_indexing_complete()
        index = self._cb_cluster.get_indexes()[0]

        import threading
        query_thread = threading.Thread(target=self._drop_index_wrapper, args=[index])
        drop_thread = threading.Thread(target=self._drop_container_wrapper, args=(drop_container, drop_name))
        query_thread.daemon = True
        drop_thread.daemon = True
        query_thread.start()
        drop_thread.start()

        query_thread.join()
        drop_thread.join()

        for idx in self._cb_cluster.get_indexes():
            status,dfn = rest.get_fts_index_definition(idx.name)
            self.assertEqual(status, False, "FTS index was not dropped during index scan.")
            self._cb_cluster.get_indexes().remove(idx)

    def _drop_index_wrapper(self, index):
        index.delete()

    def _index_query_wrapper(self, index, query):
        index.execute_query(query)

    def _drop_container_wrapper(self, drop_container, drop_name):
        if drop_container == 'collection':
            self.cli_client.delete_collection(scope=self.scope, collection=drop_name)
        elif drop_container == 'scope':
            self.cli_client.delete_scope(scope=drop_name)
        else:
            self._cb_cluster.delete_bucket("default")

    def _n1ql_query_wrapper(self, n1ql_query):
        self._cb_cluster.run_n1ql_query(query=n1ql_query)

    def test_create_drop_index(self):
        self.load_data(generator=None)
        self.wait_till_items_in_bucket_equal(self._num_items//2)
        plan_params = self.construct_plan_params()
        self.create_fts_indexes_all_buckets(plan_params=plan_params)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

        for idx in self._cb_cluster.get_indexes():
            idx.delete()

        self.assertEqual(len(self._cb_cluster.get_indexes()), 0, "FTS index cannot be deleted.")

    def test_create_index_multiple_scopes_negative(self):
        collection_index, type, index_scope, index_collections = self.define_index_parameters_collection_related()

        for bucket in self._cb_cluster.get_buckets():
            try:
                self.create_index(bucket, "fts_idx", index_params=None,
                                  plan_params=None, collection_index=collection_index, _type=type, analyzer="standard",
                                  scope=index_scope, collections=index_collections)
                self.assertTrue(False, "Successfully created FTS index for collections from different buckets!")
            except Exception as ex:
                self.assertTrue(str(ex).find("Error creating index")>=0 and
                                str(ex).find("multiple scopes found")>=0 and
                                str(ex).find("index can only span collections on a single scope")>=0,
                                "Non-expected error message is found while trying to create FTS index for more than one scope.")
                pass

    def test_create_index_missed_container_negative(self):
        missed_collection = 'missed_collection'
        missed_scope = 'missed_scope'

        _types = [f"{self.scope}.{missed_collection}",
                  f"_default.{missed_collection}",
                  f"{missed_scope}._default",
                  f"{missed_scope}.{self.collection}",
                  f"{missed_scope}.{missed_collection}"]

        self.load_data()
        collection_index = True
        for _type in _types:
            try:
                index = self._cb_cluster.create_fts_index(
                    name='default_index',
                    source_name='default',
                    collection_index=collection_index,
                    _type=_type,
                    source_params={"includeXAttrs": True})
                self.fail(f"FTS index is successfully created basing on non-existent kv container: {_type}")
            except Exception as e:
                self.log.info("Expected exception happened during fts index creation on missed kv container.")

    def prepare_for_score_none_fuzzy(self):
        fuzzy_dataset_size = self._input.param("fuzzy_dataset_size", "small")
        index_params = {}
        data_json = ""
        dataset = ""
        if "small" in fuzzy_dataset_size:
            data_json = "fuzzy_small_dataset.json"
            dataset = FUZZY_FTS_SMALL_DATASET
            index_params = INDEX_DEFAULTS.FUZZY_SMALL_INDEX_MAPPING
            self.query = QUERY.FUZZY_SMALL_INDEX_QUERY
        if "large" in fuzzy_dataset_size:
            data_json = "fuzzy_large_dataset.json"
            dataset = FUZZY_FTS_LARGE_DATASET
            index_params = INDEX_DEFAULTS.FUZZY_LARGE_INDEX_MAPPING
            self.query = QUERY.FUZZY_LARGE_INDEX_QUERY

        download_from_s3(dataset, "/tmp/" + data_json)

        self.cbimport_data(data_json_path="/tmp/" + data_json, server=self._cb_cluster.get_master_node())
        self.sleep(10)

        self.fts_index = self._cb_cluster.create_fts_index(name="fuzzy_index",
                                          source_name="default",
                                          index_params=index_params)
        self.wait_for_indexing_complete()

    def test_score_none_fuzzy(self):

        self.prepare_for_score_none_fuzzy()
        expected_hits = 0
        try:
            for index in self._cb_cluster.get_indexes():
                expected_hits, contents, _, _ = index.execute_query(query=self.query,
                                                                    zero_results_ok=False,
                                                                    return_raw_hits=True)
                self.log.info("Hits: %s" % expected_hits)
        except Exception as err:
            self.log.error(err)
            self.fail("Testcase failed: " + str(err))

        try:
            for index in self._cb_cluster.get_indexes():
                hits, contents, _, _ = index.execute_query(query=self.query,
                                                           zero_results_ok=False,
                                                           expected_hits=expected_hits,
                                                           return_raw_hits=True,
                                                           score="none")

                self.log.info("Hits: %s" % hits)
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
            self.fail("Testcase failed: " + str(err))

    def test_mem_utilization_score_none_fuzzy(self):
        self.prepare_for_score_none_fuzzy()

        self.sleep(60, "Waiting for 1 min before before collecting mem_usage")

        fts_nodes_mem_usage = self.get_fts_ram_used()
        self.log.info("fts_nodes_mem_usage: {0}".format(fts_nodes_mem_usage))

        self.fts_index.fts_queries.append(self.query)
        self.start_task_managers(10)
        for count in range(2000):
            for task_manager in self.task_managers:
                task_manager.schedule(ESRunQueryCompare(self.fts_index,
                                                        self.es,
                                                        query_index=0))
        self.sleep(600)

        self.shutdown_task_managers()

        self.sleep(60, "Waiting for 1 min before checking cpu utilization/mem_high again")

        mem_high = False
        for node in fts_nodes_mem_usage:
            mem_high = mem_high or self.check_if_fts_ram_usage_high(node["nodeip"], 2.0*float(node["mem_usage"]))

        if mem_high:
            self.fail("CPU utilization or memory usage found to be high")

    def test_create_index_same_name_same_scope_negative(self):
        scope_name = self._input.param("scope", "_default")
        #delete unnecessary bucket
        self._cb_cluster.delete_bucket("default")
        #create bucket
        bucket_size = 200
        bucket_priority = None
        bucket_type = TestInputSingleton.input.param("bucket_type", "membase")
        maxttl = TestInputSingleton.input.param("maxttl", None)
        self._cb_cluster.create_default_bucket(
            bucket_size,
            self._num_replicas,
            eviction_policy='valueOnly',
            bucket_priority=bucket_priority,
            bucket_type=bucket_type,
            maxttl=maxttl,
            bucket_storage='couchstore',
            bucket_name='bucket1')
        #create scope
        self.cli_client.create_scope(bucket='bucket1', scope=scope_name)
        #create collections with same name
        self.cli_client.create_collection(bucket='bucket1', scope=scope_name, collection='collection1')
        self.cli_client.create_collection(bucket='bucket1', scope=scope_name, collection='collection2')
        # create 2 indexes
        index1 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket1'),
                                   "index1", collection_index=True, _type=f"{scope_name}.collection1",
                                   scope=scope_name, collections=["collection1"])
        try:
            index2 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket1'),
                                       "index1", collection_index=True, _type=f"{scope_name}.collection2",
                                       scope=scope_name, collections=["collection2"], no_check=True)
        except Exception as e:
            self.log.info("Exceptin caught ::"+str(e)+"::")
            return
        self.fail(f"Successfully created 2 indexes with same name in scope {scope_name}")

    def test_create_index_same_name_diff_scope_negative(self):
        #delete unnecessary bucket
        self._cb_cluster.delete_bucket("default")
        #create bucket
        bucket_size = 200
        bucket_priority = None
        bucket_type = TestInputSingleton.input.param("bucket_type", "membase")
        maxttl = TestInputSingleton.input.param("maxttl", None)
        self._cb_cluster.create_default_bucket(
            bucket_size,
            self._num_replicas,
            eviction_policy='valueOnly',
            bucket_priority=bucket_priority,
            bucket_type=bucket_type,
            maxttl=maxttl,
            bucket_storage='couchstore',
            bucket_name='bucket1')
        #create scopes
        self.cli_client.create_scope(bucket='bucket1', scope="scope1")
        self.cli_client.create_scope(bucket='bucket1', scope="scope2")
        #create collections with same name
        self.cli_client.create_collection(bucket='bucket1', scope="scope1", collection='collection1')
        self.cli_client.create_collection(bucket='bucket1', scope="scope2", collection='collection1')
        # create 2 indexes
        index1 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket1'),
                                   "index1", collection_index=True, _type="scope1.collection1",
                                   scope="scope1", collections=["collection1"])
        try:
            index2 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket1'),
                                       "index1", collection_index=True, _type="scope2.collection1",
                                       scope="scope2", collections=["collection1"], no_check=True)
        except Exception as e:
            self.log.info("Exception caught ::" + str(e) + "::")
            return
        self.fail("Failed creating 2 indexes with same name in different scopes ")

    test_data = {
        "doc_1": {
            "num": 1,
            "str": "str_1",
            "bool": True,
            "array": ["array1_1", "array1_2"],
            "obj": {"key": "key1", "val": "val1"},
            "filler": "filler"
        },
        "doc_2": {
            "num": 2,
            "str": "str_2",
            "bool": False,
            "array": ["array2_1", "array2_2"],
            "obj": {"key": "key2", "val": "val2"},
            "filler": "filler"
        },
        "doc_3": {
            "num": 3,
            "str": "str_3",
            "bool": True,
            "array": ["array3_1", "array3_2"],
            "obj": {"key": "key3", "val": "val3"},
            "filler": "filler"
        },
        "doc_4": {
            "num": 4,
            "str": "str_4",
            "bool": False,
            "array": ["array4_1", "array4_2"],
            "obj": {"key": "key4", "val": "val4"},
            "filler": "filler"
        },
        "doc_5": {
            "num": 5,
            "str": "str_5",
            "bool": True,
            "array": ["array5_1", "array5_2"],
            "obj": {"key": "key5", "val": "val5"},
            "filler": "filler"
        },
        "doc_10": {
            "num": 10,
            "str": "str_10",
            "bool": False,
            "array": ["array10_1", "array10_2"],
            "obj": {"key": "key10", "val": "val10"},
            "filler": "filler"
        },
    }

    def test_search_before(self):
        bucket = self._cb_cluster.get_bucket_by_name('default')
        self._load_search_before_search_after_test_data(bucket.name, self.test_data)
        index = self.create_index(bucket, "idx1")
        self.wait_for_indexing_complete(len(self.test_data))
        full_size = len(self.test_data)
        partial_size = TestInputSingleton.input.param("partial_size", 1)
        partial_start_index = TestInputSingleton.input.param("partial_start_index", 3)
        sort_mode = eval(TestInputSingleton.input.param("sort_mode", '[_id]'))

        cluster = index.get_cluster()

        all_fts_query = {"explain": False, "fields": ["*"], "highlight": {}, "query": {"match": "filler", "field": "filler"},"size": full_size, "sort": sort_mode}
        all_hits, all_matches, _, _ = cluster.run_fts_query(index.name, all_fts_query)
        search_before_param = all_matches[partial_start_index]['sort']

        for i in range(0, len(search_before_param)):
            if search_before_param[i] == "_score":
                search_before_param[i] = str(all_matches[partial_start_index]['score'])

        search_before_fts_query = {"explain": False, "fields": ["*"], "highlight": {}, "query": {"match": "filler", "field": "filler"},"size": partial_size, "sort": sort_mode, "search_before": search_before_param}
        _, search_before_matches, _, _ = cluster.run_fts_query(index.name, search_before_fts_query)

        all_results_ids = []
        search_before_results_ids = []

        for match in all_matches:
            all_results_ids.append(match['id'])

        for match in search_before_matches:
            search_before_results_ids.append(match['id'])

        for i in range(0, partial_size-1):
            if i in range(0, len(search_before_results_ids) - 1):
                if search_before_results_ids[i] != all_results_ids[partial_start_index-partial_size+i]:
                    self.fail("test is failed")

    def test_search_after(self):
        bucket = self._cb_cluster.get_bucket_by_name('default')
        self._load_search_before_search_after_test_data(bucket.name, self.test_data)
        index = self.create_index(bucket, "idx1")
        self.wait_for_indexing_complete(len(self.test_data))
        full_size = len(self.test_data)
        partial_size = TestInputSingleton.input.param("partial_size", 1)
        partial_start_index = TestInputSingleton.input.param("partial_start_index", 3)
        sort_mode = eval(TestInputSingleton.input.param("sort_mode", '[_id]'))

        cluster = index.get_cluster()

        all_fts_query = {"explain": False, "fields": ["*"], "highlight": {}, "query": {"match": "filler", "field": "filler"},"size": full_size, "sort": sort_mode}
        all_hits, all_matches, _, _ = cluster.run_fts_query(index.name, all_fts_query)

        search_before_param = all_matches[partial_start_index]['sort']

        for i in range(0, len(search_before_param)):
            if search_before_param[i] == "_score":
                search_before_param[i] = str(all_matches[partial_start_index]['score'])

        search_before_fts_query = {"explain": False, "fields": ["*"], "highlight": {}, "query": {"match": "filler", "field": "filler"},"size": partial_size, "sort": sort_mode, "search_after": search_before_param}
        _, search_before_matches, _, _ = cluster.run_fts_query(index.name, search_before_fts_query)
        all_results_ids = []
        search_before_results_ids = []

        for match in all_matches:
            all_results_ids.append(match['id'])

        for match in search_before_matches:
            search_before_results_ids.append(match['id'])

        for i in range(0, partial_size-1):
            if i in range(0, len(search_before_results_ids)-1):
                if search_before_results_ids[i] != all_results_ids[partial_start_index+1+i]:
                    self.fail("test is failed")

    def test_search_before_multi_fields(self):
        bucket = self._cb_cluster.get_bucket_by_name('default')
        self._load_search_before_search_after_test_data(bucket.name, self.test_data)
        index = self.create_index(bucket, "idx1")
        self.wait_for_indexing_complete(len(self.test_data))

        full_size = len(self.test_data)
        partial_size = TestInputSingleton.input.param("partial_size", 1)
        partial_start_index = TestInputSingleton.input.param("partial_start_index", 3)
        sort_modes = ['str', 'num', 'bool', 'array', '_id', '_score']
        sort_mode = []
        fails = []

        cluster = index.get_cluster()
        for i in range(0, len(sort_modes)):
            for j in range(0, len(sort_modes)):
                sort_mode.clear()
                if sort_modes[i] == sort_modes[j]:
                    pass
                sort_mode.append(sort_modes[i])
                sort_mode.append(sort_modes[j])
                all_fts_query = {"explain": False, "fields": ["*"], "highlight": {}, "query": {"match": "filler", "field": "filler"},"size": full_size, "sort": sort_mode}
                all_hits, all_matches, _, _ = cluster.run_fts_query(index.name, all_fts_query)
                search_before_param = all_matches[partial_start_index]['sort']

                for i in range(0, len(search_before_param)):
                    if search_before_param[i] == "_score":
                        search_before_param[i] = str(all_matches[partial_start_index]['score'])

                search_before_fts_query = {"explain": False, "fields": ["*"], "highlight": {}, "query": {"match": "filler", "field": "filler"},"size": partial_size, "sort": sort_mode, "search_before": search_before_param}
                _, search_before_matches, _, _ = cluster.run_fts_query(index.name, search_before_fts_query)

                all_results_ids = []
                search_before_results_ids = []

                for match in all_matches:
                    all_results_ids.append(match['id'])

                for match in search_before_matches:
                    search_before_results_ids.append(match['id'])

                for i in range(0, partial_size-1):
                    if search_before_results_ids[i] != all_results_ids[full_size-partial_start_index-partial_size+i]:
                        fails.append(str(sort_mode))
        self.assertEqual(len(fails), 0, "Tests for the following sort modes are failed: "+str(fails))

    def test_search_after_multi_fields(self):
        bucket = self._cb_cluster.get_bucket_by_name('default')
        self._load_search_before_search_after_test_data(bucket.name, self.test_data)
        index = self.create_index(bucket, "idx1")
        self.wait_for_indexing_complete(len(self.test_data))

        full_size = len(self.test_data)
        partial_size = TestInputSingleton.input.param("partial_size", 1)
        partial_start_index = TestInputSingleton.input.param("partial_start_index", 3)
        sort_modes = ['str', 'num', 'bool', 'array', '_id', '_score']
        sort_mode = []
        fails = []

        cluster = index.get_cluster()
        for i in range(0, len(sort_modes)):
            for j in range(0, len(sort_modes)):
                sort_mode.clear()
                if sort_modes[i] == sort_modes[j]:
                    pass
                sort_mode.append(sort_modes[i])
                sort_mode.append(sort_modes[j])
                all_fts_query = {"explain": False, "fields": ["*"], "highlight": {}, "query": {"match": "filler", "field": "filler"},"size": full_size, "sort": sort_mode}
                all_hits, all_matches, _, _ = cluster.run_fts_query(index.name, all_fts_query)
                search_before_param = all_matches[partial_start_index]['sort']

                for i in range(0, len(search_before_param)):
                    if search_before_param[i] == "_score":
                        search_before_param[i] = str(all_matches[partial_start_index]['score'])

                search_before_fts_query = {"explain": False, "fields": ["*"], "highlight": {}, "query": {"match": "filler", "field": "filler"},"size": partial_size, "sort": sort_mode, "search_after": search_before_param}
                _, search_before_matches, _, _ = cluster.run_fts_query(index.name, search_before_fts_query)

                all_results_ids = []
                search_before_results_ids = []

                for match in all_matches:
                    all_results_ids.append(match['id'])

                for match in search_before_matches:
                    search_before_results_ids.append(match['id'])

                for i in range(0, partial_size-1):
                    if search_before_results_ids[i] != all_results_ids[partial_start_index+1+i]:
                        fails.append(str(sort_mode))
        self.assertEqual(len(fails), 0, "Tests for the following sort modes are failed: "+str(fails))

    def test_search_before_search_after_negative(self):
        expected_error = "cannot use search after and search before together"
        bucket = self._cb_cluster.get_bucket_by_name('default')
        self._load_search_before_search_after_test_data(bucket.name, self.test_data)
        index = self.create_index(bucket, "idx1")
        self.wait_for_indexing_complete(len(self.test_data))

        cluster = index.get_cluster()
        search_before_fts_query = {"explain": False, "fields": ["*"], "highlight": {}, "query": {"query": "filler:filler"},"size": 2, "search_before": ["doc_2"], "search_after": ["doc_4"], "sort": ["_id"]}

        response = cluster.run_fts_query_generalized(index.name, search_before_fts_query)
        if 'error' in response.keys():
            self.assertTrue(str(response['error']).index(expected_error) > 0, "Cannot find expected error message.")
        else:
            self.fail("Incorrect query was executed successfully.")

    query_result = None

    def run_fts_query_wrapper(self, index, fts_query):
        cluster = index.get_cluster()
        _, self.query_result,_,_ = cluster.run_fts_query(index.name, fts_query)


    def test_concurrent_search_before_query_index_build(self):
        bucket = self._cb_cluster.get_bucket_by_name('default')
        self._load_search_before_search_after_test_data(bucket.name, self.test_data)
        index = self.create_index(bucket, "idx1")
        self.wait_for_indexing_complete(len(self.test_data))

        cluster = index.get_cluster()

        search_before_fts_query = {"explain": False, "fields": ["*"], "highlight": {}, "query": {"match": "filler", "field": "filler"},"size": 2, "sort": ["str"], "search_before": ["str_4"]}
        _, search_before_matches, _, _ = cluster.run_fts_query(index.name, search_before_fts_query)

        data_filler_thread = threading.Thread(target=self._load_search_before_search_after_additional_data, args=(bucket.name, 1000 , 20))
        query_executor_thread = threading.Thread(target=self.run_fts_query_wrapper, args=(index, search_before_fts_query))
        data_filler_thread.daemon = True
        data_filler_thread.start()
        self.sleep(10)
        query_executor_thread.daemon = True
        query_executor_thread.start()

        data_filler_thread.join()
        query_executor_thread.join()
        self.log.info("idle results ::"+str(search_before_matches)+"::")
        self.log.info("busy results ::"+str(self.query_result)+"::")
        idle_ids = []
        busy_ids = []
        for match in search_before_matches:
            idle_ids.append(match['id'])
        for match in self.query_result:
            busy_ids.append(match['id'])
        self.assertEqual(idle_ids, busy_ids, "Results for idle and busy index states are different.")

    def test_concurrent_search_after_query_index_build(self):
        bucket = self._cb_cluster.get_bucket_by_name('default')
        self._load_search_before_search_after_test_data(bucket.name, self.test_data)
        index = self.create_index(bucket, "idx1")
        self.wait_for_indexing_complete(len(self.test_data))

        cluster = index.get_cluster()

        search_after_fts_query = {"explain": False, "fields": ["*"], "highlight": {}, "query": {"match": "filler", "field": "filler"},"size": 2, "sort": ["str"], "search_after": ["str_2"]}
        _, search_after_matches, _, _ = cluster.run_fts_query(index.name, search_after_fts_query)

        data_filler_thread = threading.Thread(target=self._load_search_before_search_after_additional_data, args=(bucket.name, 1000 , 20))
        query_executor_thread = threading.Thread(target=self.run_fts_query_wrapper, args=(index, search_after_fts_query))
        data_filler_thread.daemon = True
        data_filler_thread.start()
        self.sleep(10)
        query_executor_thread.daemon = True
        query_executor_thread.start()

        data_filler_thread.join()
        query_executor_thread.join()
        idle_ids = []
        busy_ids = []
        for match in search_after_matches:
            idle_ids.append(match['id'])
        for match in self.query_result:
            busy_ids.append(match['id'])
        self.assertEqual(idle_ids, busy_ids, "Results for idle and busy index states are different.")

    def test_search_before_after_n1ql_function(self):
        bucket = self._cb_cluster.get_bucket_by_name('default')
        self._load_search_before_search_after_test_data(bucket.name, self.test_data)
        index = self.create_index(bucket, "idx1")
        direction = TestInputSingleton.input.param("direction", "search_before")
        self.wait_for_indexing_complete(len(self.test_data))

        cluster = index.get_cluster()

        search_before_after_fts_query = {"explain": False, "fields": ["*"], "highlight": {}, "query": {"match": "filler", "field": "filler"},"size": 2, "sort": ["_id"], direction: ["doc_2"]}
        _, fts_matches, _, _ = cluster.run_fts_query(index.name, search_before_after_fts_query)
        n1ql_query = 'select meta().id from '+bucket.name+' where search('+bucket.name+', {"explain": false, "fields": ["*"], "highlight": {}, "query": {"match": "filler", "field": "filler"}, "size": 2, "sort": ["_id"], "'+direction+'": ["doc_2"]})'
        n1ql_results = self._cb_cluster.run_n1ql_query(query=n1ql_query)['results']
        fts_ids = []
        n1ql_ids = []
        for match in fts_matches:
            fts_ids.append(match['id'])
        for match in n1ql_results:
            n1ql_ids.append(match['id'])
        self.assertEqual(fts_ids, n1ql_ids, "Results for fts and n1ql queries are different.")



    def test_search_before_not_indexed_field(self):
        bucket = self._cb_cluster.get_bucket_by_name('default')
        self._load_search_before_search_after_test_data(bucket.name, self.test_data)
        index = self.create_index(bucket, "idx1")
        self.wait_for_indexing_complete(len(self.test_data))
        full_size = len(self.test_data)
        partial_size = 2
        partial_start_index = 3

        index.index_definition['params']['doc_config'] = {}
        doc_config = {}
        doc_config['mode'] = 'type_field'
        doc_config['type_field'] = 'filler'
        index.index_definition['params']['doc_config'] = doc_config

        index.add_type_mapping_to_index_definition(type="filler",
                                                   analyzer="standard")
        index.index_definition['params']['mapping'] = {
                    "default_analyzer": "standard",
                    "default_datetime_parser": "dateTimeOptional",
                    "default_field": "_all",
                    "default_mapping": {
                        "dynamic": False,
                        "enabled": False
                    },
                    "default_type": "_default",
                    "docvalues_dynamic": True,
                    "index_dynamic": True,
                    "store_dynamic": False,
                    "type_field": "_type",
                    "types": {
                        "filler": {
                            "default_analyzer": "standard",
                            "dynamic": False,
                            "enabled": True,
                            "properties": {
                                "num": {
                                    "enabled": True,
                                    "dynamic": False,
                                    "fields": [
                                        {
                                            "docvalues": True,
                                            "include_term_vectors": True,
                                            "index": True,
                                            "name": "num",
                                            "type": "number"
                                        }
                                    ]
                                }
                            }
                        }
                    }
                }
        index.index_definition['uuid'] = index.get_uuid()
        index.update()

        self.wait_for_indexing_complete(len(self.test_data))
        self.sleep(5)

        cluster = index.get_cluster()


        all_fts_query = {"explain": False, "fields": ["*"], "highlight": {}, "query": {"query": "num: >0"},"size": full_size, "sort": ["str"]}
        all_hits, all_matches, _, _ = cluster.run_fts_query(index.name, all_fts_query)
        search_before_param = all_matches[partial_start_index]['sort']

        for i in range(0, len(search_before_param)):
            if search_before_param[i] == "_score":
                search_before_param[i] = str(all_matches[partial_start_index]['score'])

        search_before_fts_query = {"explain": False, "fields": ["*"], "highlight": {}, "query": {"query": "num: >0"},"size": partial_size, "sort": ["str"], "search_before": search_before_param}
        total_hits, hit_list, time_taken, status = cluster.run_fts_query(index.name, search_before_fts_query)
        self.assertEqual(total_hits, full_size, "search_before query for non-indexed field is failed.")


    def test_search_after_not_indexed_field(self):

        bucket = self._cb_cluster.get_bucket_by_name('default')
        self._load_search_before_search_after_test_data(bucket.name, self.test_data)
        index = self.create_index(bucket, "idx1")
        self.wait_for_indexing_complete(len(self.test_data))
        full_size = len(self.test_data)
        partial_size = 2
        partial_start_index = 3

        index.index_definition['params']['doc_config'] = {}
        doc_config = {}
        doc_config['mode'] = 'type_field'
        doc_config['type_field'] = 'filler'
        index.index_definition['params']['doc_config'] = doc_config

        index.add_type_mapping_to_index_definition(type="filler",
                                                   analyzer="standard")
        index.index_definition['params']['mapping'] = {
                    "default_analyzer": "standard",
                    "default_datetime_parser": "dateTimeOptional",
                    "default_field": "_all",
                    "default_mapping": {
                        "dynamic": False,
                        "enabled": False
                    },
                    "default_type": "_default",
                    "docvalues_dynamic": True,
                    "index_dynamic": True,
                    "store_dynamic": False,
                    "type_field": "_type",
                    "types": {
                        "filler": {
                            "default_analyzer": "standard",
                            "dynamic": False,
                            "enabled": True,
                            "properties": {
                                "num": {
                                    "enabled": True,
                                    "dynamic": False,
                                    "fields": [
                                        {
                                            "docvalues": True,
                                            "include_term_vectors": True,
                                            "index": True,
                                            "name": "num",
                                            "type": "number"
                                        }
                                    ]
                                }
                            }
                        }
                    }
                }
        index.index_definition['uuid'] = index.get_uuid()
        index.update()

        self.wait_for_indexing_complete(len(self.test_data))
        self.sleep(5)

        cluster = index.get_cluster()

        all_fts_query = {"explain": False, "fields": ["*"], "highlight": {}, "query": {"query": "num: >0"},"size": full_size, "sort": ["str"]}
        all_hits, all_matches, _, _ = cluster.run_fts_query(index.name, all_fts_query)

        search_after_param = all_matches[partial_start_index]['sort']

        for i in range(0, len(search_after_param)):
            if search_after_param[i] == "_score":
                search_after_param[i] = str(all_matches[partial_start_index]['score'])

        search_after_fts_query = {"explain": False, "fields": ["*"], "highlight": {}, "query": {"query": "num: >0"},"size": partial_size, "sort": ["str"], "search_after": search_after_param}
        total_hits, hit_list, time_taken, status = cluster.run_fts_query(index.name, search_after_fts_query)
        self.assertEqual(total_hits, full_size, "search_after query for non-indexed field is failed.")

    def _load_search_before_search_after_test_data(self, bucket, test_data):
        for key in test_data:
            query = "insert into "+bucket+" (KEY, VALUE) VALUES " \
                                          "('"+str(key)+"', " \
                                          ""+str(test_data[key])+")"
            self._cb_cluster.run_n1ql_query(query=query)

    def _load_search_before_search_after_additional_data(self, bucket, num_records=5000, start_index=20):
        for i in range(start_index, num_records):
            query = 'insert into '+bucket+' (KEY, VALUE) VALUES ' \
                                          '("doc_'+str(i)+'", ' \
                    '{' \
                        '"num": '+str(i)+',' \
                        '"str": "ystr_'+str(i)+'",' \
                        '"bool": False,' \
                        '"array": ["array'+str(i)+'_1", "array'+str(i)+'_2"],' \
                        '"obj": {"key": "key'+str(i)+'", "val": "val'+str(i)+'"},' \
                        '"filler": "filler"' \
                    '})'
            self._cb_cluster.run_n1ql_query(query=query)

    def _create_oso_containers(self, bucket=None, num_scopes=1, collections_per_scope=1, docs_per_collection=10000):
        load_tasks = []
        for i in range(1, num_scopes+1):
            scope_name = "scope_"+str(i)
            self.cli_client.create_scope(bucket="default", scope=scope_name);
            self.sleep(10)
            for j in range(1, collections_per_scope+1):
                collection_name = "collection_"+str(j)
                self._cb_cluster._create_collection(bucket="default", scope=scope_name, collection=collection_name, cli_client=self.cli_client)

                #load data into collections
                gen_create = SDKDataLoader(num_ops=docs_per_collection, percent_create=100, percent_update=0, percent_delete=0,
                             load_pattern="uniform", start_seq_num=1, key_prefix="doc_", key_suffix="_",
                             scope=scope_name, collection=collection_name, json_template="emp", doc_expiry=0,
                             doc_size=500, get_sdk_logs=False, username="Administrator", password="password", timeout=1000,
                             start=0, end=0, op_type="create", all_collections=False, es_compare=False, es_host=None, es_port=None,
                             es_login=None, es_password=None)

                load_tasks = self._cb_cluster.async_load_bucket_from_generator(bucket, gen_create)
                for task in load_tasks:
                    task.result()

    def _fill_collection(self, bucket=None, scope=None, collection=None, num_docs=1000, start_seq_num=1):
        gen_create = SDKDataLoader(num_ops=num_docs, percent_create=100, percent_update=0, percent_delete=0,
                     load_pattern="uniform", start_seq_num=start_seq_num, key_prefix="doc_", key_suffix="_",
                     scope=scope, collection=collection, json_template="emp", doc_expiry=0,
                     doc_size=500, get_sdk_logs=False, username="Administrator", password="password", timeout=1000,
                     start=0, end=0, op_type="create", all_collections=False, es_compare=False, es_host=None, es_port=None,
                     es_login=None, es_password=None)#

        load_tasks = self._cb_cluster.async_load_bucket_from_generator(bucket, gen_create)
        for task in load_tasks:
            task.result()

    def _update_collection(self, bucket=None, scope=None, collection=None, num_docs=1000, start=1):
        gen_create = SDKDataLoader(num_ops=num_docs, percent_create=0, percent_update=100, percent_delete=0,
                     load_pattern="uniform", start_seq_num=start, key_prefix="doc_", key_suffix="_",
                     scope=scope, collection=collection, json_template="emp", doc_expiry=0,
                     doc_size=500, get_sdk_logs=False, username="Administrator", password="password", timeout=1000,
                     start=start, end=start+num_docs, op_type="create", all_collections=False, es_compare=False, es_host=None, es_port=None,
                     es_login=None, es_password=None)

        load_tasks = self._cb_cluster.async_load_bucket_from_generator(bucket, gen_create)
        for task in load_tasks:
            task.result()

    def _delete_from_collection(self, bucket=None, scope=None, collection=None, num_docs=1000, start=1):
        gen_create = SDKDataLoader(num_ops=num_docs, percent_create=0, percent_update=0, percent_delete=100,
                     load_pattern="uniform", start_seq_num=start, key_prefix="doc_", key_suffix="_",
                     scope=scope, collection=collection, json_template="emp", doc_expiry=0,
                     doc_size=500, get_sdk_logs=False, username="Administrator", password="password", timeout=1000,
                     start=start, end=start+num_docs, op_type="create", all_collections=False, es_compare=False, es_host=None, es_port=None,
                     es_login=None, es_password=None)

        load_tasks = self._cb_cluster.async_load_bucket_from_generator(bucket, gen_create)
        for task in load_tasks:
            task.result()

    def test_index_creation_oso(self):
        rest = RestConnection(self._cb_cluster.get_random_fts_node())
        rest.set_node_setting("useOSOBackfill", True)
        bucket = self._cb_cluster.get_bucket_by_name('default')
        num_scopes = TestInputSingleton.input.param("num_scopes", 5)
        collections_per_scope = TestInputSingleton.input.param("collections_per_scope", 20)
        docs_per_collection = TestInputSingleton.input.param("docs_per_collection", 10000)
        self._create_oso_containers(bucket=bucket, num_scopes=num_scopes, collections_per_scope=collections_per_scope, docs_per_collection=docs_per_collection)

        test_scope = "scope_"+str(num_scopes)
        test_collection = "test_collection"
        self._cb_cluster._create_collection(bucket="default", scope=test_scope, collection=test_collection, cli_client=self.cli_client)
        self._fill_collection(bucket=bucket, scope=test_scope, collection=test_collection, num_docs=1000)
        test_index = self.create_index(self._cb_cluster.get_bucket_by_name('default'),
                                       "test_index", collection_index=True, _type=f"{test_scope}.{test_collection}",
                                       scope=test_scope, collections=[test_collection])
        self.wait_for_indexing_complete_simple(item_count=1000, index=test_index)

        multi_collections = []
        for i in range(1, 4):
            coll_name = test_collection + "_" + str(i)
            self._cb_cluster._create_collection(bucket="default", scope=test_scope, collection=coll_name, cli_client=self.cli_client)
            self._fill_collection(bucket=bucket, scope=test_scope, collection=coll_name, num_docs=1000, start_seq_num=1000*i+1)
            multi_collections.append(coll_name)
        _type_multi = []
        index_collections = []
        for c in multi_collections:
            _type_multi.append(f"{test_scope}.{c}")
            index_collections.append(c)
        test_index_multi = self.create_index(self._cb_cluster.get_bucket_by_name('default'),
                                       "test_index_multi", collection_index=True, _type=_type_multi,
                                        scope=test_scope, collections=index_collections)
        self.wait_for_indexing_complete_simple(item_count=3000, index=test_index_multi)

        test_query = {"match": "emp", "field": "type"}
        hits_before, _, _, _ = test_index.execute_query(test_query)
        hits_before_multi, _, _, _ = test_index_multi.execute_query(test_query)

        additional_collections_per_scope = TestInputSingleton.input.param("additional_collections_per_scope", 2)

        for i in range(1, num_scopes+1):
            scope_name = "scope_" + str(i)
            for j in (collections_per_scope+2, additional_collections_per_scope+1):
                collection_name = "collection_" + str(j)
                self._cb_cluster._create_collection(bucket="default", scope=scope_name, collection=collection_name, cli_client=self.cli_client)
                self._fill_collection(bucket=bucket, scope=scope_name, collection=collection_name)

        hits_after, _, _, _ = test_index.execute_query(test_query)
        hits_after_multi, _, _, _ = test_index_multi.execute_query(test_query)
        errors = []
        try:
            self.assertEqual(hits_before, hits_after)
        except AssertionError as e:
            errors.append("Hits before and after additional data load do not match for single collection index.")
        try:
            self.assertEqual(hits_before_multi, hits_after_multi)
        except AssertionError as e:
            errors.append("Hits before and after additional data load do not match for multi collection index.")

        try:
            self.assertEqual(len(errors), 0)
        except AssertionError as ex:
            for err in errors:
                self.log.error(err)
            self.fail()

    def test_data_mutations_oso(self):
        rest = RestConnection(self._cb_cluster.get_random_fts_node())
        rest.set_node_setting("useOSOBackfill", True)

        bucket = self._cb_cluster.get_bucket_by_name('default')
        num_scopes = TestInputSingleton.input.param("num_scopes", 5)
        collections_per_scope = TestInputSingleton.input.param("collections_per_scope", 20)
        docs_per_collection = TestInputSingleton.input.param("docs_per_collection", 10000)
        self._create_oso_containers(bucket=bucket, num_scopes=num_scopes, collections_per_scope=collections_per_scope, docs_per_collection=docs_per_collection)

        test_scope = "scope_"+str(num_scopes)
        test_collection = "test_collection"
        self._cb_cluster._create_collection(bucket="default", scope=test_scope, collection=test_collection, cli_client=self.cli_client)
        self._fill_collection(bucket=bucket, scope=test_scope, collection=test_collection, num_docs=1000)

        test_index = self.create_index(self._cb_cluster.get_bucket_by_name('default'),
                                       "test_index", collection_index=True, _type=f"{test_scope}.{test_collection}",
                                       scope=test_scope, collections=[test_collection])
        self.wait_for_indexing_complete_simple(item_count=1000, index=test_index)

        multi_collections = []
        for i in range(1, 4):
            coll_name = test_collection + "_" + str(i)
            self._cb_cluster._create_collection(bucket="default", scope=test_scope, collection=coll_name, cli_client=self.cli_client)
            self._fill_collection(bucket=bucket, scope=test_scope, collection=coll_name, num_docs=1000, start_seq_num=1000*i+1)
            multi_collections.append(coll_name)
        _type_multi = []
        index_collections = []
        for c in multi_collections:
            _type_multi.append(f"{test_scope}.{c}")
            index_collections.append(c)
        test_index_multi = self.create_index(self._cb_cluster.get_bucket_by_name('default'),
                                       "test_index_multi", collection_index=True, _type=_type_multi,
                                             scope=test_scope, collections=index_collections)
        self.wait_for_indexing_complete_simple(item_count=3000, index=test_index_multi)

        additional_collections_per_scope = TestInputSingleton.input.param("additional_collections_per_scope", 2)

        for i in range(1, num_scopes+1):
            scope_name = "scope_" + str(i)
            for j in (collections_per_scope+2, additional_collections_per_scope+1):
                collection_name = "collection_" + str(j)
                self._cb_cluster._create_collection(bucket="default", scope=scope_name, collection=collection_name, cli_client=self.cli_client)
                self._fill_collection(bucket=bucket, scope=scope_name, collection=collection_name)

        for i in range(1, num_scopes+1):
            scope_name = "scope_" + str(i)
            for j in (1, additional_collections_per_scope+1):
                collection_name = "collection_" + str(j)
                self._update_collection(bucket=bucket, scope=scope_name, collection=collection_name)

        self._update_collection(bucket=bucket, scope=test_scope, collection=test_collection, num_docs=1000)
        for i in range(1, 4):
            self._update_collection(bucket=bucket, scope=test_scope, collection=test_collection+"_"+str(i), num_docs=1000, start=1000*i+1)

        self.wait_for_indexing_complete_simple(item_count=1000, index=test_index)
        self.wait_for_indexing_complete_simple(item_count=3000, index=test_index_multi)

        test_query = {"query": "mutated:1"}
        hits, _, _, _ = test_index.execute_query(test_query)
        hits_multi, _, _, _ = test_index_multi.execute_query(test_query)

        errors = []
        try:
            self.assertEqual(hits, 1000)
        except AssertionError as e:
            errors.append("Full update of test collection is failed, or fts index produces wrong results for test query.")
        try:
            self.assertEqual(hits_multi, 3000)
        except AssertionError as e:
            errors.append("Full update of test collections for multi collection index is failed, or multi collections fts index produces wrong results for test query.")

        try:
            self.assertEqual(len(errors), 0)
        except AssertionError as ex:
            for err in errors:
                self.log.error(err)
            self.fail()

    def test_doc_id_oso(self):
        rest = RestConnection(self._cb_cluster.get_random_fts_node())
        rest.set_node_setting("useOSOBackfill", True)

        bucket = self._cb_cluster.get_bucket_by_name('default')
        num_scopes = TestInputSingleton.input.param("num_scopes", 5)
        collections_per_scope = TestInputSingleton.input.param("collections_per_scope", 20)
        docs_per_collection = TestInputSingleton.input.param("docs_per_collection", 10000)
        self._create_oso_containers(bucket=bucket, num_scopes=num_scopes, collections_per_scope=collections_per_scope, docs_per_collection=docs_per_collection)

        test_scope = "scope_"+str(num_scopes)
        test_collection = "test_collection"
        self._cb_cluster._create_collection(bucket="default", scope=test_scope, collection=test_collection, cli_client=self.cli_client)
        self._fill_collection(bucket=bucket, scope=test_scope, collection=test_collection, num_docs=1000, start_seq_num=1001)

        test_index = self.create_index(self._cb_cluster.get_bucket_by_name('default'),
                                       "test_index", collection_index=True, _type=f"{test_scope}.{test_collection}",
                                       scope=test_scope, collections=[test_collection])

        additional_collections_per_scope = TestInputSingleton.input.param("additional_collections_per_scope", 2)

        for i in range(1, num_scopes+1):
            scope_name = "scope_" + str(i)
            for j in (collections_per_scope+2, additional_collections_per_scope+1):
                collection_name = "collection_" + str(j)
                self._cb_cluster._create_collection(bucket="default", scope=scope_name, collection=collection_name, cli_client=self.cli_client)
                self._fill_collection(bucket=bucket, scope=scope_name, collection=collection_name)

        self._fill_collection(bucket=bucket, scope=test_scope, collection=test_collection, num_docs=1000, start_seq_num=1)

        test_query = {"match": "emp", "field": "type"}
        hits, _, _, _ = test_index.execute_query(test_query)
        self.assertEqual(hits, 2000, "Test for doc_id special load order is failed.")

    def test_partial_rollback_oso(self):
        from lib.memcached.helper.data_helper import MemcachedClientHelper
        #items = 50000, update = True, upd = 30, upd_del_fields = ['dept']
        rest = RestConnection(self._cb_cluster.get_random_fts_node())
        rest.set_node_setting("useOSOBackfill", True)

        bucket = self._cb_cluster.get_bucket_by_name("default")
        self._cb_cluster.flush_buckets([bucket])

        num_scopes = TestInputSingleton.input.param("num_scopes", 5)
        collections_per_scope = TestInputSingleton.input.param("collections_per_scope", 20)
        docs_per_collection = TestInputSingleton.input.param("docs_per_collection", 10000)
        self._create_oso_containers(bucket=bucket, num_scopes=num_scopes, collections_per_scope=collections_per_scope, docs_per_collection=docs_per_collection)

        test_scope = "scope_"+str(num_scopes)
        test_collection = "test_collection"
        self._cb_cluster._create_collection(bucket="default", scope=test_scope, collection=test_collection, cli_client=self.cli_client)
        self._fill_collection(bucket=bucket, scope=test_scope, collection=test_collection, num_docs=15000)

        test_index = self.create_index(self._cb_cluster.get_bucket_by_name('default'),
                                       "test_index", collection_index=True, _type=f"{test_scope}.{test_collection}",
                                       scope=test_scope, collections=[test_collection])
        self.wait_for_indexing_complete_simple(item_count=15000, index=test_index)

        multi_collections = []
        for i in range(1, 4):
            coll_name = test_collection + "_" + str(i)
            self._cb_cluster._create_collection(bucket="default", scope=test_scope, collection=coll_name, cli_client=self.cli_client)
            self._fill_collection(bucket=bucket, scope=test_scope, collection=coll_name, num_docs=15000)
            multi_collections.append(coll_name)
        _type_multi = []
        index_collections = []
        for c in multi_collections:
            _type_multi.append(f"{test_scope}.{c}")
            index_collections.append(c)
        test_index_multi = self.create_index(self._cb_cluster.get_bucket_by_name('default'),
                                             "test_index_multi", collection_index=True, _type=_type_multi,
                                             scope=test_scope, collections=index_collections)
        self.wait_for_indexing_complete_simple(item_count=45000, index=test_index_multi)

        additional_collections_per_scope = TestInputSingleton.input.param("additional_collections_per_scope", 2)

        for i in range(1, num_scopes+1):
            scope_name = "scope_" + str(i)
            for j in (collections_per_scope+2, additional_collections_per_scope+1):
                collection_name = "collection_" + str(j)
                self._cb_cluster._create_collection(bucket="default", scope=scope_name, collection=collection_name, cli_client=self.cli_client)
                self._fill_collection(bucket=bucket, scope=scope_name, collection=collection_name)

        for i in range(1, num_scopes+1):
            scope_name = "scope_" + str(i)
            for j in (1, additional_collections_per_scope+1):
                collection_name = "collection_" + str(j)
                self._update_collection(bucket=bucket, scope=scope_name, collection=collection_name)

        # Stop Persistence on Node A & Node B
        self.log.info("Stopping persistence on {0}".
                      format(self._input.servers[:2]))
        mem_client = MemcachedClientHelper.direct_client(self._input.servers[0],
                                                         bucket)
        mem_client.stop_persistence()
        mem_client = MemcachedClientHelper.direct_client(self._input.servers[1],
                                                         bucket)
        mem_client.stop_persistence()

        # Perform mutations on the bucket
        if self._input.param("update", False):
            self._update_collection(bucket=bucket, scope=test_scope, collection=test_collection, num_docs=300)
            for i in range(1, 4):
                self._update_collection(bucket=bucket, scope=test_scope, collection=test_collection + "_" + str(i),
                                        num_docs=300, start=1000 * i + 1)

        if self._input.param("delete", False):
            self._delete_from_collection(bucket=bucket, scope=test_scope, collection=test_collection, num_docs=5000)
            for i in range(1, 4):
                self._delete_from_collection(bucket=bucket, scope=test_scope, collection="test_collection_"+str(i), num_docs=5000)
        self.wait_for_indexing_complete_simple(item_count=900, index=test_index)

        self.wait_for_indexing_complete_simple(item_count=2700, index=test_index_multi)

        # Run FTS Query to fetch the initial count of mutated items
        query = {"query": "mutated:>0"}

        hits1_simple_index, _, _, _ = test_index.execute_query(query)
        self.log.info("Hits for simple index before rollback: %s" % hits1_simple_index)
        hits1_multi_index, _, _, _ = test_index_multi.execute_query(query)
        self.log.info("Hits for multi index before rollback: %s" % hits1_multi_index)
        # Fetch count of docs in index and bucket
        before_simple_index_doc_count = test_index.get_indexed_doc_count()
        before_multi_index_doc_count = test_index_multi.get_indexed_doc_count()
        before_bucket_doc_count = test_index.get_src_bucket_doc_count()

        self.log.info("Docs in Bucket : %s, Docs in simple Index : %s, Docs in multi Index: %s" % (
            before_bucket_doc_count, before_simple_index_doc_count, before_multi_index_doc_count))

        # Kill memcached on Node A
        self.log.info("Killing memcached on {0}".format(self._master.ip))
        shell = RemoteMachineShellConnection(self._master)
        shell.kill_memcached()

        # Start persistence on Node B
        self.log.info("Starting persistence on {0}".
                      format(self._input.servers[1].ip))
        mem_client = MemcachedClientHelper.direct_client(self._input.servers[1],
                                                         bucket)
        mem_client.start_persistence()

        # Failover Node B
        failover_task = self._cb_cluster.async_failover(
            node=self._input.servers[1])
        failover_task.result()

        # Wait for Failover & FTS index rollback to complete
        self.wait_for_indexing_complete_simple(item_count=900, index=test_index)
        self.wait_for_indexing_complete_simple(item_count=2700, index=test_index_multi)

        # Run FTS query to fetch count of mutated items post rollback.
        hits2_simple_index, _, _, _ = test_index.execute_query(query)
        self.log.info("Hits for simple index after rollback: %s" % hits2_simple_index)
        hits2_multi_index, _, _, _ = test_index_multi.execute_query(query)
        self.log.info("Hits for multi index after rollback: %s" % hits2_multi_index)
        # Fetch count of docs in index and bucket
        after_simple_index_doc_count = test_index.get_indexed_doc_count()
        after_multi_index_doc_count = test_index_multi.get_indexed_doc_count()
        after_bucket_doc_count = test_index.get_src_bucket_doc_count()

        self.log.info("Docs in Bucket : %s, Docs in simple Index : %s, Docs in multi Index: %s" % (
            after_bucket_doc_count, after_simple_index_doc_count, after_multi_index_doc_count))

        # Validation : If there are deletes, validate the #docs in index goes
        #  up post rollback
        if self._input.param("delete", False):
            self.assertGreater(after_simple_index_doc_count, before_simple_index_doc_count,
                               "Deletes : Simple index count after rollback not "
                               "greater than before rollback")
            self.assertGreater(after_multi_index_doc_count, before_multi_index_doc_count,
                               "Deletes : Multi index count after rollback not "
                               "greater than before rollback")
        else:
            # For Updates, validate that #hits goes down in the query output
            # post rollback
            self.assertGreater(hits1_simple_index, hits2_simple_index,
                               "Mutated items before rollback are not more "
                               "than after rollback for simple index")
            self.assertGreater(hits1_multi_index, hits2_multi_index,
                               "Mutated items before rollback are not more "
                               "than after rollback for multi index")

        # Failover FTS node
        failover_fts_node = self._input.param("failover_fts_node", False)

        if failover_fts_node:
            failover_task = self._cb_cluster.async_failover(
                num_nodes=1)
            failover_task.result()
            self.sleep(10)

            # Run FTS query to fetch count of mutated items post FTS node failover.
            hits3_simple_index, _, _, _ = test_index.execute_query(query)
            hits3_multi_index, _, _, _ = test_index_multi.execute_query(query)

            self.log.info(
                "Hits after rollback and failover of primary FTS node for simple index: %s" % hits3_simple_index)
            self.log.info(
                "Hits after rollback and failover of primary FTS node for multi index: %s" % hits3_multi_index)
            self.assertEqual(hits2_simple_index, hits3_simple_index,
                                "Mutated items after FTS node failover are not equal to that after rollback for simple index")
            self.assertEqual(hits2_multi_index, hits3_multi_index,
                                "Mutated items after FTS node failover are not equal to that after rollback for multi index")

    def test_flush_bucket_oso(self):
        rest = RestConnection(self._cb_cluster.get_random_fts_node())
        rest.set_node_setting("useOSOBackfill", True)

        bucket = self._cb_cluster.get_bucket_by_name('default')
        num_scopes = TestInputSingleton.input.param("num_scopes", 5)
        collections_per_scope = TestInputSingleton.input.param("collections_per_scope", 20)
        docs_per_collection = TestInputSingleton.input.param("docs_per_collection", 10000)
        self._create_oso_containers(bucket=bucket, num_scopes=num_scopes, collections_per_scope=collections_per_scope, docs_per_collection=docs_per_collection)

        test_scope = "scope_"+str(num_scopes)
        test_collection = "test_collection"
        self._cb_cluster._create_collection(bucket="default", scope=test_scope, collection=test_collection, cli_client=self.cli_client)
        self._fill_collection(bucket=bucket, scope=test_scope, collection=test_collection, num_docs=1000)

        test_index = self.create_index(self._cb_cluster.get_bucket_by_name('default'),
                                       "test_index", collection_index=True, _type=f"{test_scope}.{test_collection}",
                                       scope=test_scope, collections=[test_collection])
        self.wait_for_indexing_complete_simple(item_count=1000, index=test_index)

        multi_collections = []
        for i in range(1, 4):
            coll_name = test_collection + "_" + str(i)
            self._cb_cluster._create_collection(bucket="default", scope=test_scope, collection=coll_name, cli_client=self.cli_client)
            self._fill_collection(bucket=bucket, scope=test_scope, collection=coll_name, num_docs=1000, start_seq_num=1000*i+1)
            multi_collections.append(coll_name)
        _type_multi = []
        index_collections = []
        for c in multi_collections:
            _type_multi.append(f"{test_scope}.{c}")
            index_collections.append(c)
        test_index_multi = self.create_index(self._cb_cluster.get_bucket_by_name('default'),
                                       "test_index_multi", collection_index=True, _type=_type_multi,
                                        scope=test_scope, collections=index_collections)
        self.wait_for_indexing_complete_simple(item_count=3000, index=test_index_multi)

        additional_collections_per_scope = TestInputSingleton.input.param("additional_collections_per_scope", 2)

        for i in range(1, num_scopes+1):
            scope_name = "scope_" + str(i)
            for j in (collections_per_scope+2, additional_collections_per_scope+1):
                collection_name = "collection_" + str(j)
                self._cb_cluster._create_collection(bucket="default", scope=scope_name, collection=collection_name, cli_client=self.cli_client)
                self._fill_collection(bucket=bucket, scope=scope_name, collection=collection_name)

        for i in range(1, num_scopes+1):
            scope_name = "scope_" + str(i)
            for j in (1, additional_collections_per_scope+1):
                collection_name = "collection_" + str(j)
                self._update_collection(bucket=bucket, scope=scope_name, collection=collection_name)

        self._update_collection(bucket=bucket, scope=test_scope, collection=test_collection, num_docs=1000)
        for i in range(1, 4):
            self._update_collection(bucket=bucket, scope=test_scope, collection=test_collection+"_"+str(i), num_docs=1000, start=1000*i+1)

        self.wait_for_indexing_complete_simple(item_count=1000, index=test_index)
        self.wait_for_indexing_complete_simple(item_count=3000, index=test_index_multi)

        self._cb_cluster.flush_buckets([bucket])
        self.sleep(60, "Waiting for flush to be happened.")


        simple_index_doc_count = test_index.get_indexed_doc_count()
        multi_index_doc_count = test_index_multi.get_indexed_doc_count()
        bucket_doc_count = test_index.get_src_bucket_doc_count()

        errors = []
        try:
            self.assertEquals(simple_index_doc_count, 0)
        except AssertionError as e:
            errors.append("Simple index contains documents after source bucket flush")
        try:
            self.assertEquals(multi_index_doc_count, 0)
        except AssertionError as e:
            errors.append("Multi index contains documents after source bucket flush")
        try:
            self.assertEquals(bucket_doc_count, 0)
        except AssertionError as e:
            errors.append("Source bucket contains documents after source bucket flush")

        try:
            self.assertEqual(len(errors), 0)
        except AssertionError as ex:
            for err in errors:
                self.log.error(err)
            self.fail()


