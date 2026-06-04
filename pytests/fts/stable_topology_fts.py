# coding=utf-8

import copy
import json
import random
import time
import threading
from threading import Thread

import Geohash
from membase.helper.cluster_helper import ClusterOperationHelper
from remote.remote_util import RemoteMachineShellConnection

from TestInput import TestInputSingleton
from tasks.task import ESRunQueryCompare
from lib.testconstants import FUZZY_FTS_SMALL_DATASET, FUZZY_FTS_LARGE_DATASET
from .fts_base import (FTSBaseTest, FTSIndex, INDEX_DEFAULTS, QUERY,
                       download_from_s3, _ScanPlusHashMap, _make_vec, UDFHelper)
from lib.membase.api.exception import FTSException, ServerUnavailableException
from lib.membase.api.rest_client import RestConnection
from couchbase_helper.documentgenerator import SDKDataLoader


class StableTopFTS(FTSBaseTest):

    def setUp(self):
        super(StableTopFTS, self).setUp()
        self.ignore_wiki = False
        self.log.info("Modifying quotas for each services in the cluster")
        try:
            RestConnection(self._cb_cluster.get_master_node()).modify_memory_quota(3000, 400, 3000, 1024, 256)
        except Exception as e:
            print(e)
        self.doc_filter_query = {"match": "search", "field": "search_string"}
        self.index_wait_time = 20

        if TestInputSingleton.input.param("synonym_source", None):
            self.synonym_source = eval(TestInputSingleton.input.param("synonym_source", None))
        else:
            self.synonym_source = None

        self.bucket_name = TestInputSingleton.input.param("bucket", None)
        self.num_docs = TestInputSingleton.input.param("num_docs", 50000)
        self.analyzer = TestInputSingleton.input.param("analyzer", "simple")
        self.format = TestInputSingleton.input.param("format", 1)

        self.idx = None
        if TestInputSingleton.input.param("idx", None):
            self.idx = eval(TestInputSingleton.input.param("idx", None))

        self.read_from_replica = TestInputSingleton.input.param("read_from_replica", False)
        self.parition_selection = TestInputSingleton.input.param("parition_selection", "")

        self.fts_nodes = None
        self.fts_target_node = None
        self.index_src=None
        self.validation_data = None
        if TestInputSingleton.input.param("custom_queries_enabled", False):
            from .udf_datagen.udf_datagen import generate_docs, compute_ground_truth
            self._udf = UDFHelper(self)
            docs = generate_docs()
            self._udf_gt = compute_ground_truth()
            self._udf_total = len(docs)
            self._udf._load_udf_docs(docs)
            self._udf._create_udf_index()
            self._udf._wait_udf_index(self._udf_total)
            self._udf._enable_udf()

    def read_from_replica_setup(self):
        #skips the validation check for random / random balanced queries
        if TestInputSingleton.input.param("skip_replica_validation", False):
            return

        #selecting the fts target node (random)
        self.fts_target_node = random.choice(self._cb_cluster.get_fts_nodes())
        self.fts_nodes = self._cb_cluster.get_fts_nodes()

        #store the active and replica stat for all indices present in the cluster. This should be executed post index creation
        self.rfr_stats = {}
        for index in self._cb_cluster.get_indexes():
            index_name = index.name
            _index_src = index._source_name

            #cfg inspection
            _,payload = RestConnection(self.fts_target_node).get_cfg_stats()
            node_map = {}

            for k,v in payload['nodeDefsKnown'].items():
                if k == "nodeDefs":
                    for a,b in v.items():
                        node_map[a] = b['hostPort'].split(':')[0]

            partition_map = {}
            for i,j in node_map.items():
                partition_map[j] = 0

            rep_partition_map = {}
            for i,j in node_map.items():
                rep_partition_map[j] = 0

            for k,v in payload['planPIndexes'].items():
                if k == "planPIndexes":
                    for a,b in v.items():
                        try:
                            key = a.split('.')[2]
                        except:
                            key = a

                        if index_name in key:
                            for m,n in b.items():
                                if m == 'nodes':
                                    for i,j in n.items():
                                        if j['priority'] == 0:
                                            partition_map[node_map[i]] += 1
                                        else:
                                            rep_partition_map[node_map[i]] += 1

            if TestInputSingleton.input.param("check_default_mode", False):
                self.validation_data = [self.num_index_partitions // len(self.fts_nodes), self.num_index_partitions - (self.num_index_partitions // len(self.fts_nodes))]
            else:
                local_target = partition_map[self.fts_target_node.ip] + rep_partition_map[self.fts_target_node.ip]

                total_partitions = self.num_index_partitions
                partition_rem = total_partitions - local_target

                self.validation_data = [local_target, partition_rem]

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
        if not (self.bucket_storage == "magma"):
            self.wait_till_items_in_bucket_equal(self._num_items)
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

        if self.search_history:
            self.validate_search_history()

        if self.index_insights:
            for index in self._cb_cluster.get_indexes():
                self.validate_index_insights(index.name, field="name",
                                             insight="termFrequencies")

    def run_default_index_query_rfr(self, query=None, expected_hits=None, expected_no_of_results=None):
        self.create_simple_default_index()
        if self.read_from_replica:
            self.read_from_replica_setup()
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
                                                      expected_no_of_results=expected_no_of_results,
                                                      variable_node=self.fts_target_node,
                                                      bucket_name=index._source_name,
                                                      validation_data=self.validation_data,
                                                      fts_nodes=self.fts_nodes)
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

        if self.search_history:
            self.validate_search_history(index_name=index.name)

        if self.index_insights:
            self.validate_index_insights(index.name, field="name",
                                         insight="termFrequencies")

    def test_query_type_rfr(self):
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

        if self.read_from_replica:
            self.read_from_replica_setup()

        self.generate_random_queries(index, self.num_queries, self.query_types)
        if self.run_via_n1ql:
            n1ql_executor = self._cb_cluster
        else:
            n1ql_executor = None
        self.run_query_and_compare(index, n1ql_executor=n1ql_executor, fts_nodes=self.fts_nodes, fts_target_node=self.fts_target_node,validation_data=self.validation_data)

    def test_basic_synonym_search(self):

        #start the synonym datagen server on port 5100
        self.synonym_datagen()

        #create collections for the source collection and synonym collections
        self._cb_cluster.create_scope_using_rest(bucket=self.bucket_name,scope=self.scope)
        self._cb_cluster.create_collection_using_rest(bucket=self.bucket_name,scope=self.scope,collection=self.collection)
        self._cb_cluster.create_collection_using_rest(bucket=self.bucket_name,scope=self.scope,collection=self.synonym_source[0][1])


        #load synonym data
        status, _ = RestConnection(self._cb_cluster.get_master_node()).load_synonyms(bucket=self.bucket_name,scope=self.scope,collection=self.synonym_source[0][1],host=self.master.ip,analyzer=self.analyzer,format=self.format)
        if not status:
            self.fail("Failed to load synonym data")

        #load source data
        status,response = RestConnection(self._cb_cluster.get_master_node()).load_synonym_source(bucket=self.bucket_name,scope=self.scope,collection=self.collection,host=self.master.ip,analyzer=self.analyzer,numDocs=self.num_docs)
        if not status:
            self.fail("Failed to load synonym source data")

        #groundtruth store
        raw_map = response['word_doc_map']
        word_map = response['final_word_doc_map']

        #create index
        _ = self._create_fts_index_parameterized(synonym_source=self.synonym_source[0])[0]
        time.sleep(30)
        self.run_synonym_query_and_compare(index_name="i1",rawmap=raw_map,wordmap=word_map)


    def test_wildcard_synonym_search(self):

        #start the synonym datagen server on port 5100
        self.synonym_datagen()

        #create collections for the source collection and synonym collection
        self._cb_cluster.create_scope_using_rest(bucket=self.bucket_name,scope=self.scope)
        self._cb_cluster.create_collection_using_rest(bucket=self.bucket_name,scope=self.scope,collection=self.collection)
        self._cb_cluster.create_collection_using_rest(bucket=self.bucket_name,scope=self.scope,collection=self.synonym_source[0][1])


        #load synonym data
        status, response = RestConnection(self._cb_cluster.get_master_node()).load_synonyms(bucket=self.bucket_name,scope=self.scope,collection=self.synonym_source[0][1],host=self.master.ip,analyzer=self.analyzer,format=self.format)
        if not status:
            self.fail("Failed to load synonym data")

        syn_map = response['synonym_map']

        #load source data
        status,response = RestConnection(self._cb_cluster.get_master_node()).load_synonym_source(bucket=self.bucket_name,scope=self.scope,collection=self.collection,host=self.master.ip,analyzer=self.analyzer,numDocs=self.num_docs)
        if not status:
            self.fail("Failed to load synonym source data")

        #groundtruth store
        raw_map = response['word_doc_map']
        word_map = response['final_word_doc_map']


        #create index
        _ = self._create_fts_index_parameterized(synonym_source=self.synonym_source[0])[0]
        _ = self._create_fts_index_parameterized(index_name="i2",wait_for_index_complete=False)[0]
        time.sleep(10)

        self.run_wildcard_synonym_query_and_compare(synonym_index="i1",index="i2",rawmap=raw_map,wordmap=word_map,synmap=syn_map,format=self.format)

    def test_fuzzy_synonym_search(self):

        #start the synonym datagen server on port 5100
        self.synonym_datagen()

        #create collections for the source collection and synonym collection
        self._cb_cluster.create_scope_using_rest(bucket=self.bucket_name,scope=self.scope)
        self._cb_cluster.create_collection_using_rest(bucket=self.bucket_name,scope=self.scope,collection=self.collection)
        self._cb_cluster.create_collection_using_rest(bucket=self.bucket_name,scope=self.scope,collection=self.synonym_source[0][1])


        #load synonym data
        status, response = RestConnection(self._cb_cluster.get_master_node()).load_synonyms(bucket=self.bucket_name,scope=self.scope,collection=self.synonym_source[0][1],host=self.master.ip,analyzer=self.analyzer,format=self.format)
        if not status:
            self.fail("Failed to load synonym data")

        syn_map = response['synonym_map']

        #load source data
        status,response = RestConnection(self._cb_cluster.get_master_node()).load_synonym_source(bucket=self.bucket_name,scope=self.scope,collection=self.collection,host=self.master.ip,analyzer=self.analyzer,numDocs=self.num_docs)
        if not status:
            self.fail("Failed to load synonym source data")

        #groundtruth store
        raw_map = response['word_doc_map']
        word_map = response['final_word_doc_map']


        #create index
        _ = self._create_fts_index_parameterized(synonym_source=self.synonym_source[0])[0]
        _ = self._create_fts_index_parameterized(index_name="i2",wait_for_index_complete=False)[0]
        time.sleep(10)

        self.run_fuzzy_synonym_query_and_compare(synonym_index="i1",index="i2",rawmap=raw_map,wordmap=word_map,synmap=syn_map,format=self.format)

    def test_match_phrase_synonym_search(self):

        #start the synonym datagen server on port 5100
        self.synonym_datagen()

        #create collections for the source collection and synonym collection
        self._cb_cluster.create_scope_using_rest(bucket=self.bucket_name,scope=self.scope)
        self._cb_cluster.create_collection_using_rest(bucket=self.bucket_name,scope=self.scope,collection=self.collection)
        self._cb_cluster.create_collection_using_rest(bucket=self.bucket_name,scope=self.scope,collection=self.synonym_source[0][1])


        #load synonym data
        status, response = RestConnection(self._cb_cluster.get_master_node()).load_synonyms(bucket=self.bucket_name,scope=self.scope,collection=self.synonym_source[0][1],host=self.master.ip,analyzer=self.analyzer,format=self.format)
        if not status:
            self.fail("Failed to load synonym data")

        syn_map = response['synonym_map']

        #load source data
        status,response = RestConnection(self._cb_cluster.get_master_node()).load_synonym_source(bucket=self.bucket_name,scope=self.scope,collection=self.collection,host=self.master.ip,analyzer=self.analyzer,numDocs=self.num_docs)
        if not status:
            self.fail("Failed to load synonym source data")

        #groundtruth store
        raw_map = response['word_doc_map']
        word_map = response['final_word_doc_map']


        #create index
        _ = self._create_fts_index_parameterized(synonym_source=self.synonym_source[0])[0]
        _ = self._create_fts_index_parameterized(index_name="i2",wait_for_index_complete=False)[0]
        time.sleep(10)

        self.run_match_phrase_synonym_query_and_compare(synonym_index="i1",index="i2",rawmap=raw_map,wordmap=word_map,synmap=syn_map,format=self.format)

    def test_prefix_synonym_search(self):

        #start the synonym datagen server on port 5100
        self.synonym_datagen()

        #create collections for the source collection and synonym collection
        self._cb_cluster.create_scope_using_rest(bucket=self.bucket_name,scope=self.scope)
        self._cb_cluster.create_collection_using_rest(bucket=self.bucket_name,scope=self.scope,collection=self.collection)
        self._cb_cluster.create_collection_using_rest(bucket=self.bucket_name,scope=self.scope,collection=self.synonym_source[0][1])


        #load synonym data
        status, response = RestConnection(self._cb_cluster.get_master_node()).load_synonyms(bucket=self.bucket_name,scope=self.scope,collection=self.synonym_source[0][1],host=self.master.ip,analyzer=self.analyzer,format=self.format)
        if not status:
            self.fail("Failed to load synonym data")

        syn_map = response['synonym_map']

        #load source data
        status,response = RestConnection(self._cb_cluster.get_master_node()).load_synonym_source(bucket=self.bucket_name,scope=self.scope,collection=self.collection,host=self.master.ip,analyzer=self.analyzer,numDocs=self.num_docs)
        if not status:
            self.fail("Failed to load synonym source data")

        #groundtruth store
        raw_map = response['word_doc_map']
        word_map = response['final_word_doc_map']


        #create index
        _ = self._create_fts_index_parameterized(synonym_source=self.synonym_source[0])[0]
        _ = self._create_fts_index_parameterized(index_name="i2",wait_for_index_complete=False)[0]
        time.sleep(10)

        self.run_prefix_synonym_query_and_compare(synonym_index="i1",index="i2",rawmap=raw_map,wordmap=word_map,synmap=syn_map,format=self.format)

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
            _index = self.create_index(bucket, "default_index", collection_index=collection_index, _type=type,
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
            # create empty ES indexes with unique names to avoid conflicts
            emp_es_index = f"emp_es_index_{FTSBaseTest.get_es_index_name()}"
            wiki_es_index = f"wiki_es_index_{FTSBaseTest.get_es_index_name()}"
            self.es.create_empty_index(emp_es_index)
            self.es.create_empty_index(wiki_es_index)
            load_tasks.append(self.es.async_bulk_load_ES(index_name=emp_es_index,
                                                         gen=emp_gen_copy,
                                                         op_type='create'))

            load_tasks.append(self.es.async_bulk_load_ES(index_name=wiki_es_index,
                                                         gen=wiki_gen_copy,
                                                         op_type='create'))

        for task in load_tasks:
            task.result()

        # create indexes on both buckets
        emp_index = self.create_index(emp, "emp_index")
        wiki_index = self.create_index(wiki, "wiki_index")

        self.wait_for_indexing_complete(es_index=emp_es_index)
        self.wait_for_indexing_complete(es_index=wiki_es_index)

        # create compound alias
        alias = self.create_alias(target_indexes=[emp_index, wiki_index],
                                  name="emp_wiki_alias")
        if self.es:
            emp_wiki_es_alias = f"emp_wiki_es_alias_{FTSBaseTest.get_es_index_name()}"
            self.es.create_alias(name=emp_wiki_es_alias,
                                 indexes=[emp_es_index, wiki_es_index])

        # run rqg on the alias
        self.generate_random_queries(alias, self.num_queries, self.query_types)
        self.run_query_and_compare(alias, es_index_name=emp_wiki_es_alias)

    def index_wiki(self):
        self.load_wiki(lang=self.lang)
        bucket = self._cb_cluster.get_bucket_by_name('default')
        collection_index, type, index_scope, index_collections = self.define_index_parameters_collection_related()
        _index = self.create_index(bucket, "wiki_index", collection_index=collection_index, _type=type,
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
        _index = self.create_index(bucket, "default_index", collection_index=collection_index, _type=tp,
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
        index = self._cb_cluster.create_fts_index(name="i1",source_name="default",scope="s1",collections=[collection_list[0]],payload=index_)
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
        if self.dataset == "all" and int(TestInputSingleton.input.param("doc_maps", 1)) == 1:
            #ignoring wiki results from the elastic result to match couchbase behaviour
            self.ignore_wiki = True

        self.run_query_and_compare(index, n1ql_executor=n1ql_executor, use_collections=collection_index,ignore_wiki=self.ignore_wiki)

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
        _index2 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket1'),
                                    "index_scope2", collection_index=True, _type="scope2.collection1",
                                    scope="scope2", collections=["collection1"])

        #load data into collections
        bucket = self._cb_cluster.get_bucket_by_name('bucket1')
        gen_create = SDKDataLoader(num_ops=100000, percent_create=100, percent_update=0, percent_delete=0,
                 load_pattern="uniform", start_seq_num=1, key_prefix="doc_", key_suffix="_",
                 scope="scope1", collection="collection1", json_template="emp", doc_expiry=0,
                 doc_size=500, get_sdk_logs=False, username="Administrator", password="password", timeout=1000,
                 start=0, end=0, op_type="create", all_collections=False, es_compare=False, es_host=None, es_port=None,
                 es_login=None, es_password=None)

        load_tasks = self._cb_cluster.async_load_bucket_from_generator(bucket, gen_create)
        for task in load_tasks:
            task.result()

        gen_create1 = SDKDataLoader(num_ops=100000, percent_create=100, percent_update=0, percent_delete=0,
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
        task = self.es.async_bulk_load_ES(
            index_name=FTSBaseTest.get_es_index_name(), gen=gen,
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

        _fail = False
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
        _geo_index = self.create_geo_index_and_load()

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
        _geo_index = self.create_geo_index_and_load()

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
                hits2, doc_ids2, _ = self.es.search(
                    index_name=FTSBaseTest.get_es_index_name(),
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
        create_response = json.loads(output)
        if create_response["status"] == "ok":
            full_index_name = create_response.get("name", "default_idx")
            query = "curl -g -k " + \
                    "-XPOST -H \"Content-Type: application/json\" " + \
                    "-u Administrator:password " + \
                    "https://{0}:18094/api/index/{1}/query -d ". \
                        format(fts_node.ip, full_index_name) + \
                    "\'{0}\'".format(json.dumps(qry))
            self.sleep(20, "wait for indexing to complete")
            output = subprocess.check_output(query, shell=True)
            self.log.info("Hits: {0}".format(json.loads(output)["total_hits"]))
            if int(json.loads(output)["total_hits"]) != 1000:
                self.fail("Query over ssl failed!")
        else:
            self.fail("Index could not be created over ssl")


    def test_json_types(self):
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
        _ = self._cb_cluster.get_indexes()[0]
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
        _ = self._cb_cluster.get_indexes()[0]
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
                _ = self._cb_cluster.create_fts_index(
                    name='default_index',
                    source_name='default',
                    collection_index=collection_index,
                    _type=_type,
                    source_params={"includeXAttrs": True})
                self.fail(f"FTS index is successfully created basing on non-existent kv container: {_type}")
            except Exception:
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
                                                        query_index=0,
                                                        es_index_name=FTSBaseTest.get_es_index_name()))
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
        _index1 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket1'),
                                    "index1", collection_index=True, _type=f"{scope_name}.collection1",
                                    scope=scope_name, collections=["collection1"])
        try:
            _index2 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket1'),
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
        _index1 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket1'),
                                    "index1", collection_index=True, _type="scope1.collection1",
                                    scope="scope1", collections=["collection1"])
        try:
            _index2 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket1'),
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

    def test_search_before_rfr(self):
        bucket = self._cb_cluster.get_bucket_by_name('default')
        self._load_search_before_search_after_test_data(bucket.name, self.test_data)
        index = self.create_index(bucket, "idx1")
        self.wait_for_indexing_complete(len(self.test_data))
        full_size = len(self.test_data)
        partial_size = TestInputSingleton.input.param("partial_size", 1)
        partial_start_index = TestInputSingleton.input.param("partial_start_index", 3)
        sort_mode = eval(TestInputSingleton.input.param("sort_mode", '[_id]'))

        if self.read_from_replica:
            self.read_from_replica_setup()

        cluster = index.get_cluster()
        all_fts_query = {"explain": False, "fields": ["*"], "ctl": {"partition_selection": self.parition_selection},"highlight": {}, "query": {"match": "filler", "field": "filler"},"size": full_size, "sort": sort_mode}
        all_hits, all_matches, _, _ = cluster.run_fts_query(index.name, all_fts_query,variable_node=self.fts_target_node,bucket_name=index._source_name,validation_data=self.validation_data,fts_nodes=self.fts_nodes)
        search_before_param = all_matches[partial_start_index]['sort']

        for i in range(0, len(search_before_param)):
            if search_before_param[i] == "_score":
                search_before_param[i] = str(all_matches[partial_start_index]['score'])

        search_before_fts_query = {"explain": False, "fields": ["*"], "ctl": {"partition_selection": self.parition_selection}, "highlight": {}, "query": {"match": "filler", "field": "filler"},"size": partial_size, "sort": sort_mode, "search_before": search_before_param}
        _, search_before_matches, _, _ = cluster.run_fts_query(index.name, search_before_fts_query,variable_node=self.fts_target_node,bucket_name=index._source_name,validation_data=self.validation_data,fts_nodes=self.fts_nodes)

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

        if self.read_from_replica:
            self.read_from_replica_setup()

        all_fts_query = {"explain": False, "fields": ["*"], "ctl": {"partition_selection": self.parition_selection}, "highlight": {}, "query": {"match": "filler", "field": "filler"},"size": full_size, "sort": sort_mode}
        all_hits, all_matches, _, _ = cluster.run_fts_query(index.name, all_fts_query)

        search_before_param = all_matches[partial_start_index]['sort']

        for i in range(0, len(search_before_param)):
            if search_before_param[i] == "_score":
                search_before_param[i] = str(all_matches[partial_start_index]['score'])

        search_before_fts_query = {"explain": False, "fields": ["*"], "ctl": {"partition_selection": self.parition_selection}, "highlight": {}, "query": {"match": "filler", "field": "filler"},"size": partial_size, "sort": sort_mode, "search_after": search_before_param}
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

    def test_search_after_rfr(self):
        bucket = self._cb_cluster.get_bucket_by_name('default')
        self._load_search_before_search_after_test_data(bucket.name, self.test_data)
        index = self.create_index(bucket, "idx1")
        self.wait_for_indexing_complete(len(self.test_data))
        full_size = len(self.test_data)
        partial_size = TestInputSingleton.input.param("partial_size", 1)
        partial_start_index = TestInputSingleton.input.param("partial_start_index", 3)
        sort_mode = eval(TestInputSingleton.input.param("sort_mode", '[_id]'))

        cluster = index.get_cluster()

        if self.read_from_replica:
            self.read_from_replica_setup()

        all_fts_query = {"explain": False, "fields": ["*"], "ctl": {"partition_selection": self.parition_selection}, "highlight": {}, "query": {"match": "filler", "field": "filler"},"size": full_size, "sort": sort_mode}
        all_hits, all_matches, _, _ = cluster.run_fts_query(index.name, all_fts_query,variable_node=self.fts_target_node,bucket_name=index._source_name,validation_data=self.validation_data,fts_nodes=self.fts_nodes)

        search_before_param = all_matches[partial_start_index]['sort']

        for i in range(0, len(search_before_param)):
            if search_before_param[i] == "_score":
                search_before_param[i] = str(all_matches[partial_start_index]['score'])

        search_before_fts_query = {"explain": False, "fields": ["*"], "ctl": {"partition_selection": self.parition_selection}, "highlight": {}, "query": {"match": "filler", "field": "filler"},"size": partial_size, "sort": sort_mode, "search_after": search_before_param}
        _, search_before_matches, _, _ = cluster.run_fts_query(index.name, search_before_fts_query,variable_node=self.fts_target_node,bucket_name=index._source_name,validation_data=self.validation_data,fts_nodes=self.fts_nodes)
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

    def test_search_before_multi_fields_rfr(self):
        bucket = self._cb_cluster.get_bucket_by_name('default')
        self._load_search_before_search_after_test_data(bucket.name, self.test_data)
        index = self.create_index(bucket, "idx1")
        self.wait_for_indexing_complete(len(self.test_data))

        if self.read_from_replica:
            self.read_from_replica_setup()

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
                all_fts_query = {"explain": False, "fields": ["*"], "ctl": {"partition_selection": self.parition_selection}, "highlight": {}, "query": {"match": "filler", "field": "filler"},"size": full_size, "sort": sort_mode}
                all_hits, all_matches, _, _ = cluster.run_fts_query(index.name, all_fts_query,variable_node=self.fts_target_node,bucket_name=index._source_name,validation_data=self.validation_data,fts_nodes=self.fts_nodes)
                search_before_param = all_matches[partial_start_index]['sort']

                for i in range(0, len(search_before_param)):
                    if search_before_param[i] == "_score":
                        search_before_param[i] = str(all_matches[partial_start_index]['score'])

                search_before_fts_query = {"explain": False, "fields": ["*"], "ctl": {"partition_selection": self.parition_selection}, "highlight": {}, "query": {"match": "filler", "field": "filler"},"size": partial_size, "sort": sort_mode, "search_before": search_before_param}
                _, search_before_matches, _, _ = cluster.run_fts_query(index.name, search_before_fts_query,variable_node=self.fts_target_node,bucket_name=index._source_name,validation_data=self.validation_data,fts_nodes=self.fts_nodes)

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

    def test_search_after_multi_fields_rfr(self):
        bucket = self._cb_cluster.get_bucket_by_name('default')
        self._load_search_before_search_after_test_data(bucket.name, self.test_data)
        index = self.create_index(bucket, "idx1")
        self.wait_for_indexing_complete(len(self.test_data))

        if self.read_from_replica:
            self.read_from_replica_setup()

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
                all_fts_query = {"explain": False, "fields": ["*"], "ctl": {"partition_selection": self.parition_selection}, "highlight": {}, "query": {"match": "filler", "field": "filler"},"size": full_size, "sort": sort_mode}
                all_hits, all_matches, _, _ = cluster.run_fts_query(index.name, all_fts_query,variable_node=self.fts_target_node,bucket_name=index._source_name,validation_data=self.validation_data,fts_nodes=self.fts_nodes)
                search_before_param = all_matches[partial_start_index]['sort']

                for i in range(0, len(search_before_param)):
                    if search_before_param[i] == "_score":
                        search_before_param[i] = str(all_matches[partial_start_index]['score'])

                search_before_fts_query = {"explain": False, "fields": ["*"], "ctl": {"partition_selection": self.parition_selection}, "highlight": {}, "query": {"match": "filler", "field": "filler"},"size": partial_size, "sort": sort_mode, "search_after": search_before_param}
                _, search_before_matches, _, _ = cluster.run_fts_query(index.name, search_before_fts_query,variable_node=self.fts_target_node,bucket_name=index._source_name,validation_data=self.validation_data,fts_nodes=self.fts_nodes)

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

    def test_search_before_search_after_negative_rfr(self):
        expected_error = "cannot use search after and search before together"
        bucket = self._cb_cluster.get_bucket_by_name('default')
        self._load_search_before_search_after_test_data(bucket.name, self.test_data)
        index = self.create_index(bucket, "idx1")
        self.wait_for_indexing_complete(len(self.test_data))

        if self.read_from_replica:
            self.read_from_replica_setup()

        cluster = index.get_cluster()
        search_before_fts_query = {"explain": False, "fields": ["*"], "ctl": {"partition_selection": self.parition_selection}, "highlight": {}, "query": {"query": "filler:filler"},"size": 2, "search_before": ["doc_2"], "search_after": ["doc_4"], "sort": ["_id"]}

        response = cluster.run_fts_query_generalized(index.name, search_before_fts_query,variable_node=self.fts_target_node,bucket_name=index._source_name,validation_data=self.validation_data,fts_nodes=self.fts_nodes)
        if 'error' in response.keys():
            self.assertTrue(str(response['error']).index(expected_error) > 0, "Cannot find expected error message.")
        else:
            self.fail("Incorrect query was executed successfully.")

    query_result = None

    def run_fts_query_wrapper(self, index, fts_query):
        cluster = index.get_cluster()
        _, self.query_result,_,_ = cluster.run_fts_query(index.name, fts_query,fts_nodes=self.fts_nodes,validation_data=self.validation_data,variable_node=self.fts_target_node,bucket_name=index._source_name)


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

    def test_concurrent_search_before_query_index_build_rfr(self):
        bucket = self._cb_cluster.get_bucket_by_name('default')
        self._load_search_before_search_after_test_data(bucket.name, self.test_data)
        index = self.create_index(bucket, "idx1")
        self.wait_for_indexing_complete(len(self.test_data))

        cluster = index.get_cluster()

        if self.read_from_replica:
            self.read_from_replica_setup()

        search_before_fts_query = {"explain": False, "fields": ["*"], "ctl": {"partition_selection": self.parition_selection}, "highlight": {}, "query": {"match": "filler", "field": "filler"},"size": 2, "sort": ["str"], "search_before": ["str_4"]}
        _, search_before_matches, _, _ = cluster.run_fts_query(index.name, search_before_fts_query,variable_node=self.fts_target_node,bucket_name=index._source_name,validation_data=self.validation_data,fts_nodes=self.fts_nodes)

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

    def test_concurrent_search_after_query_index_build_rfr(self):
        bucket = self._cb_cluster.get_bucket_by_name('default')
        self._load_search_before_search_after_test_data(bucket.name, self.test_data)
        index = self.create_index(bucket, "idx1")
        self.wait_for_indexing_complete(len(self.test_data))

        cluster = index.get_cluster()

        if self.read_from_replica:
            self.read_from_replica_setup()

        search_after_fts_query = {"explain": False, "fields": ["*"], "ctl": {"partition_selection": self.parition_selection}, "highlight": {}, "query": {"match": "filler", "field": "filler"},"size": 2, "sort": ["str"], "search_after": ["str_2"]}
        _, search_after_matches, _, _ = cluster.run_fts_query(index.name, search_after_fts_query,variable_node=self.fts_target_node,bucket_name=index._source_name,validation_data=self.validation_data,fts_nodes=self.fts_nodes)

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

    def test_search_before_after_n1ql_function_rfr(self):
        bucket = self._cb_cluster.get_bucket_by_name('default')
        self._load_search_before_search_after_test_data(bucket.name, self.test_data)
        index = self.create_index(bucket, "idx1")
        direction = TestInputSingleton.input.param("direction", "search_before")
        self.wait_for_indexing_complete(len(self.test_data))

        cluster = index.get_cluster()

        if self.read_from_replica:
            self.read_from_replica_setup()

        search_before_after_fts_query = {"explain": False, "fields": ["*"], "ctl": {"partition_selection": self.parition_selection}, "highlight": {}, "query": {"match": "filler", "field": "filler"},"size": 2, "sort": ["_id"], direction: ["doc_2"]}
        _, fts_matches, _, _ = cluster.run_fts_query(index.name, search_before_after_fts_query,variable_node=self.fts_target_node,bucket_name=index._source_name,validation_data=self.validation_data,fts_nodes=self.fts_nodes)
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

    _deep_pagination_index_mapping = {
        "default_analyzer": "standard",
        "default_datetime_parser": "dateTimeOptional",
        "default_field": "_all",
        "default_mapping": {
            "dynamic": False,
            "enabled": True,
            "properties": {
                "salary": {
                    "enabled": True,
                    "dynamic": False,
                    "fields": [
                        {
                            "docvalues": True,
                            "include_term_vectors": True,
                            "index": True,
                            "name": "salary",
                            "type": "number"
                        }
                    ]
                },
                "join_date": {
                    "enabled": True,
                    "dynamic": False,
                    "fields": [
                        {
                            "docvalues": True,
                            "include_term_vectors": True,
                            "index": True,
                            "name": "join_date",
                            "type": "datetime"
                        }
                    ]
                },
                "location": {
                    "enabled": True,
                    "dynamic": False,
                    "fields": [
                        {
                            "docvalues": True,
                            "include_term_vectors": True,
                            "index": True,
                            "name": "location",
                            "type": "geopoint"
                        }
                    ]
                },
                "dept": {
                    "enabled": True,
                    "dynamic": False,
                    "fields": [
                        {
                            "docvalues": True,
                            "include_term_vectors": True,
                            "index": True,
                            "name": "dept",
                            "type": "text"
                        }
                    ]
                }
            }
        },
        "default_type": "_default",
        "docvalues_dynamic": True,
        "index_dynamic": True,
        "store_dynamic": False,
        "type_field": "_type"
    }

    def _create_deep_pagination_nontextual_index(self, bucket):
        index = self.create_index(bucket, "dp_nontextual_idx")
        index.index_definition['params']['mapping'] = copy.deepcopy(self._deep_pagination_index_mapping)
        index.index_definition['uuid'] = index.get_uuid()
        index.update()
        return index

    def _get_nontextual_sort_mode(self, sort_type):
        if sort_type == "numeric":
            return [{"by": "field", "field": "salary", "type": "number"}, "_id"]
        elif sort_type == "datetime":
            return [{"by": "field", "field": "join_date", "type": "date"}, "_id"]
        elif sort_type == "geo":
            return [{"by": "geo_distance", "field": "location", "unit": "mi",
                      "location": {"lat": 40.7128, "lon": -74.0060}}, "_id"]
        elif sort_type == "numeric_geo":
            return [{"by": "field", "field": "salary", "type": "number"},
                    {"by": "geo_distance", "field": "location", "unit": "mi",
                     "location": {"lat": 40.7128, "lon": -74.0060}}, "_id"]
        elif sort_type == "datetime_numeric":
            return [{"by": "field", "field": "join_date", "type": "date"},
                    {"by": "field", "field": "salary", "type": "number"}, "_id"]
        else:
            self.fail("Unknown sort_type: {}".format(sort_type))

    def test_deep_pagination_search_after_nontextual(self):
        bucket = self._cb_cluster.get_bucket_by_name('default')
        self.load_employee_dataset(self._num_items)
        index = self._create_deep_pagination_nontextual_index(bucket)
        self.wait_for_indexing_complete()
        self.sleep(5)

        num_items = self._num_items
        partial_size = TestInputSingleton.input.param("partial_size", 2)
        partial_start_index = TestInputSingleton.input.param("partial_start_index", 3)
        sort_type = TestInputSingleton.input.param("sort_type", "numeric")
        sort_mode = self._get_nontextual_sort_mode(sort_type)

        cluster = index.get_cluster()

        all_fts_query = {"explain": False, "fields": ["*"], "highlight": {},
                         "query": {"match_all": {}},
                         "size": num_items, "sort": sort_mode}
        all_hits, all_matches, _, _ = cluster.run_fts_query(index.name, all_fts_query)

        search_after_param = all_matches[partial_start_index].get(
            'decoded_sort', all_matches[partial_start_index]['sort'])

        search_after_fts_query = {"explain": False, "fields": ["*"], "highlight": {},
                                  "query": {"match_all": {}},
                                  "size": partial_size, "sort": sort_mode,
                                  "search_after": search_after_param}
        _, search_after_matches, _, _ = cluster.run_fts_query(index.name, search_after_fts_query)

        all_results_ids = [match['id'] for match in all_matches]
        search_after_results_ids = [match['id'] for match in search_after_matches]

        for i in range(len(search_after_results_ids)):
            expected_idx = partial_start_index + 1 + i
            if expected_idx < len(all_results_ids):
                self.assertEqual(search_after_results_ids[i], all_results_ids[expected_idx],
                    "Deep pagination search_after mismatch at position {}: got {} expected {}".format(
                        i, search_after_results_ids[i], all_results_ids[expected_idx]))

    def test_deep_pagination_search_before_nontextual(self):
        bucket = self._cb_cluster.get_bucket_by_name('default')
        self.load_employee_dataset(self._num_items)
        index = self._create_deep_pagination_nontextual_index(bucket)
        self.wait_for_indexing_complete()
        self.sleep(5)

        num_items = self._num_items
        partial_size = TestInputSingleton.input.param("partial_size", 2)
        partial_start_index = TestInputSingleton.input.param("partial_start_index", 3)
        sort_type = TestInputSingleton.input.param("sort_type", "numeric")
        sort_mode = self._get_nontextual_sort_mode(sort_type)

        cluster = index.get_cluster()

        all_fts_query = {"explain": False, "fields": ["*"], "highlight": {},
                         "query": {"match_all": {}},
                         "size": num_items, "sort": sort_mode}
        all_hits, all_matches, _, _ = cluster.run_fts_query(index.name, all_fts_query)

        search_before_param = all_matches[partial_start_index].get(
            'decoded_sort', all_matches[partial_start_index]['sort'])

        search_before_fts_query = {"explain": False, "fields": ["*"], "highlight": {},
                                   "query": {"match_all": {}},
                                   "size": partial_size, "sort": sort_mode,
                                   "search_before": search_before_param}
        _, search_before_matches, _, _ = cluster.run_fts_query(index.name, search_before_fts_query)

        all_results_ids = [match['id'] for match in all_matches]
        search_before_results_ids = [match['id'] for match in search_before_matches]

        for i in range(len(search_before_results_ids)):
            expected_idx = partial_start_index - partial_size + i
            if 0 <= expected_idx < len(all_results_ids):
                self.assertEqual(search_before_results_ids[i], all_results_ids[expected_idx],
                    "Deep pagination search_before mismatch at position {}: got {} expected {}".format(
                        i, search_before_results_ids[i], all_results_ids[expected_idx]))

    def test_deep_pagination_full_walk_nontextual(self):
        bucket = self._cb_cluster.get_bucket_by_name('default')
        self.load_employee_dataset(self._num_items)
        index = self._create_deep_pagination_nontextual_index(bucket)
        self.wait_for_indexing_complete()
        self.sleep(5)

        num_items = self._num_items
        page_size = TestInputSingleton.input.param("page_size", 3)
        sort_type = TestInputSingleton.input.param("sort_type", "numeric")
        sort_mode = self._get_nontextual_sort_mode(sort_type)

        cluster = index.get_cluster()

        all_fts_query = {"explain": False, "fields": ["*"], "highlight": {},
                         "query": {"match_all": {}},
                         "size": num_items, "sort": sort_mode}
        _, all_matches, _, _ = cluster.run_fts_query(index.name, all_fts_query)
        all_results_ids = [match['id'] for match in all_matches]

        paginated_ids = []
        search_after_param = None
        pages_fetched = 0

        while True:
            page_query = {"explain": False, "fields": ["*"], "highlight": {},
                          "query": {"match_all": {}},
                          "size": page_size, "sort": sort_mode}
            if search_after_param:
                page_query["search_after"] = search_after_param

            _, page_matches, _, _ = cluster.run_fts_query(index.name, page_query)
            if not page_matches:
                break

            for match in page_matches:
                paginated_ids.append(match['id'])

            search_after_param = page_matches[-1].get(
                'decoded_sort', page_matches[-1]['sort'])
            pages_fetched += 1

            if len(page_matches) < page_size:
                break

        self.assertEqual(len(paginated_ids), len(all_results_ids),
            "Full walk collected {} docs but expected {}".format(
                len(paginated_ids), len(all_results_ids)))
        self.assertEqual(paginated_ids, all_results_ids,
            "Full walk result order does not match full query result order")

    def test_deep_pagination_multi_sort_nontextual(self):
        bucket = self._cb_cluster.get_bucket_by_name('default')
        self.load_employee_dataset(self._num_items)
        index = self._create_deep_pagination_nontextual_index(bucket)
        self.wait_for_indexing_complete()
        self.sleep(5)

        num_items = self._num_items
        partial_size = TestInputSingleton.input.param("partial_size", 2)
        partial_start_index = TestInputSingleton.input.param("partial_start_index", 3)
        sort_type = TestInputSingleton.input.param("sort_type", "numeric_geo")
        sort_mode = self._get_nontextual_sort_mode(sort_type)

        cluster = index.get_cluster()

        all_fts_query = {"explain": False, "fields": ["*"], "highlight": {},
                         "query": {"match_all": {}},
                         "size": num_items, "sort": sort_mode}
        all_hits, all_matches, _, _ = cluster.run_fts_query(index.name, all_fts_query)

        search_after_param = all_matches[partial_start_index].get(
            'decoded_sort', all_matches[partial_start_index]['sort'])

        search_after_fts_query = {"explain": False, "fields": ["*"], "highlight": {},
                                  "query": {"match_all": {}},
                                  "size": partial_size, "sort": sort_mode,
                                  "search_after": search_after_param}
        _, search_after_matches, _, _ = cluster.run_fts_query(index.name, search_after_fts_query)

        all_results_ids = [match['id'] for match in all_matches]
        search_after_results_ids = [match['id'] for match in search_after_matches]

        for i in range(len(search_after_results_ids)):
            expected_idx = partial_start_index + 1 + i
            if expected_idx < len(all_results_ids):
                self.assertEqual(search_after_results_ids[i], all_results_ids[expected_idx],
                    "Deep pagination multi-sort search_after mismatch at position {}: got {} expected {}".format(
                        i, search_after_results_ids[i], all_results_ids[expected_idx]))

    def _create_oso_containers(self, bucket=None, num_scopes=1, collections_per_scope=1, docs_per_collection=10000):
        load_tasks = []
        load_timeout = max(3000, docs_per_collection // 2)
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
                             doc_size=500, get_sdk_logs=False, username="Administrator", password="password", timeout=load_timeout,
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

    def test_exceed_fts_100_collection_limit(self):
        """Negative test: FTS index with >100 collections should fail."""
        bucket = self._cb_cluster.get_bucket_by_name('default')
        scope_name = "bulk_scope_0"
        over_limit = [f"bulk_coll_0_{i}" for i in range(101)]
        _type = [f"{scope_name}.{c}" for c in over_limit]

        index = FTSIndex(
            cluster=self._cb_cluster,
            name="idx_over_100_colls",
            source_name=bucket.name,
            type_mapping=_type,
            collection_index=True,
            scope=scope_name,
            collections=over_limit
        )
        for typ in _type:
            index.add_type_mapping_to_index_definition(
                type=typ, analyzer="standard")
        index.index_definition['params']['doc_config'] = {
            'mode': 'scope.collection.type_field',
            'type_field': 'type'
        }

        rest = RestConnection(self._cb_cluster.get_random_fts_node())
        status, content = rest.create_fts_index(
            index.name, index.index_definition,
            index._source_name, index.scope, mode="check")
        self.log.info(f"Create index with 101 collections: "
                      f"status={status}, content={content}")
        self.assertFalse(
            status,
            "Index creation should fail with >100 collections")

    def test_incremental_index_to_100_collections(self):
        """Create FTS index with 99 collections, then update to 100."""
        self.load_data(generator=None, num_items=self._num_items)

        bucket = self._cb_cluster.get_bucket_by_name('default')
        scope_name = "bulk_scope_0"
        first_99 = [f"bulk_coll_0_{i}" for i in range(99)]
        _type = [f"{scope_name}.{c}" for c in first_99]

        index = self._cb_cluster.create_fts_index(
            name="idx_incr_99",
            source_name=bucket.name,
            collection_index=True,
            _type=_type,
            analyzer="standard",
            scope=scope_name,
            collections=first_99
        )
        self.log.info(f"Created index '{index.name}' with 99 collections")

        coll_100 = "bulk_coll_0_99"
        new_type = f"{scope_name}.{coll_100}"
        index.add_type_mapping_to_index_definition(
            type=new_type, analyzer="standard")
        index.index_definition['uuid'] = index.get_uuid()
        index.update()
        self.log.info(f"Updated index '{index.name}' to 100 collections")

        self.wait_for_indexing_complete()

    def test_multi_index_query_10k_collections(self):
        """Create multiple FTS indexes across random subsets of collections,
        load data, then query all indexes in parallel."""
        num_indexes = self._input.param("num_fts_indexes", 20)
        colls_per_index = self._input.param("colls_per_index", 50)

        self.load_data(generator=None, num_items=self._num_items)

        bucket = self._cb_cluster.get_bucket_by_name('default')
        scope_name = "bulk_scope_0"
        total_colls = self.fts_collections
        all_coll_names = [f"bulk_coll_0_{i}" for i in range(total_colls)]

        indexes = []
        for idx_num in range(num_indexes):
            subset = random.sample(
                all_coll_names, min(colls_per_index, len(all_coll_names)))
            _type = [f"{scope_name}.{c}" for c in subset]
            index = self._cb_cluster.create_fts_index(
                name=f"multi_idx_{idx_num}",
                source_name=bucket.name,
                collection_index=True,
                _type=_type,
                analyzer="standard",
                scope=scope_name,
                collections=subset
            )
            self.log.info(
                f"Created index '{index.name}' spanning "
                f"{len(subset)} collections")
            indexes.append((index, len(subset)))

        self.wait_for_indexing_complete()

        query = {"match": "emp", "field": "type"}
        errors = []

        def _query_index(index, expected_docs):
            try:
                hits, _, _, _ = index.execute_query(
                    query=query, expected_hits=expected_docs)
                self.log.info(
                    f"Query on '{index.name}': hits={hits}, "
                    f"expected={expected_docs}")
            except Exception as e:
                errors.append(f"Index '{index.name}': {e}")

        threads = []
        for index, num_colls in indexes:
            expected = self._num_items * num_colls
            t = threading.Thread(target=_query_index,
                                 args=(index, expected))
            threads.append(t)
            t.start()
            self.log.info(
                f"Started query thread for '{index.name}' "
                f"(expected hits={expected})")

        for t in threads:
            t.join()

        if errors:
            for err in errors:
                self.log.error(err)
            self.fail(f"{len(errors)} out of {num_indexes} index queries "
                      f"failed: {errors}")

    # ======================================================================
    # scan_plus / scan_plus tests
    # ======================================================================

    def test_scan_plus_stress(self):
        """
        Stress/performance test for scan_plus consistency.

        Runs continuous CRUD (4 insert : 5 update : 1 delete) for `duration`
        seconds across `num_crud_threads` threads.  FTS queries are fired at
        random intervals (5–30 s).  For each query the HashMap is snapshotted
        at fire-time and used as ground truth.

        Validation (unidirectional, stress-friendly):
          • For every FTS hit: result.val >= snapshot.val
          • Docs absent from the snapshot (deleted before query time) must not
            appear in results — violations are collected and reported at the end.

        Params (via TestInput):
          duration         : total run time in seconds (default 600)
          num_crud_threads : parallel CRUD goroutines (default 4)
          initial_docs     : seed doc count (default 5)
          index_partitions : FTS index partition count (default 1)
          num_replicas     : FTS index replica count (default 0)
        """
        duration         = TestInputSingleton.input.param("duration", 600)
        num_crud_threads = TestInputSingleton.input.param("num_crud_threads", 4)
        initial_docs     = TestInputSingleton.input.param("initial_docs", 5)
        index_partitions = TestInputSingleton.input.param("index_partitions", 1)
        num_replicas     = TestInputSingleton.input.param("num_replicas", 0)
        getseqnos_retries = TestInputSingleton.input.param("getseqnos_retries", None)
        use_bucket_seqnos = TestInputSingleton.input.param("use_bucket_seqnos", None)
        num_workers       = TestInputSingleton.input.param("num_workers", None)

        bucket = self._cb_cluster.get_bucket_by_name('default')
        _, collection = self._scan_plus_setup_sdk()

        # One HashMap per thread; distribute initial_docs so total seeded == initial_docs
        base = initial_docs // num_crud_threads
        extra_docs = initial_docs % num_crud_threads
        hashmaps = []
        for t_idx in range(num_crud_threads):
            docs_for_thread = base + (1 if t_idx < extra_docs else 0)
            hm = _ScanPlusHashMap()
            for i in range(docs_for_thread):
                doc_id = f"scan_plus_t{t_idx}_init_{i}"
                collection.upsert(doc_id, {'val': 1})
                hm.insert(doc_id, 1)
            hashmaps.append(hm)

        total_initial_docs = initial_docs
        index = self.create_index(bucket, "scan_plus_stress_idx")
        index.add_child_field_to_default_mapping("val", "number")
        if index_partitions > 1:
            # update_index_partitions GETs the server definition (overwriting the
            # local one), so push the val mapping first so the GET picks it up.
            index.index_definition['uuid'] = index.get_uuid()
            index.update()
            index.update_index_partitions(index_partitions)
        if num_replicas > 0:
            index.update_num_replicas(num_replicas)
        index.index_definition['uuid'] = index.get_uuid()
        index.update()
        self.wait_for_indexing_complete(item_count=total_initial_docs)

        mgr_options = {}
        if getseqnos_retries is not None:
            mgr_options["getseqnos_retries"] = str(getseqnos_retries)
        if use_bucket_seqnos is not None:
            mgr_options["use_bucket_seqnos"] = str(use_bucket_seqnos).lower()
        if num_workers is not None:
            mgr_options["num_workers"] = str(num_workers)
        if mgr_options:
            self._scan_plus_set_manager_options(mgr_options)

        stop_event = threading.Event()
        violations = []

        crud_threads = []
        for t_idx in range(num_crud_threads):
            t = threading.Thread(
                target=self._scan_plus_crud_worker,
                args=(collection, hashmaps[t_idx], stop_event),
                kwargs={'thread_idx': t_idx},
                daemon=True,
            )
            t.start()
            crud_threads.append(t)

        start_time = time.time()
        try:
            while time.time() - start_time < duration:
                remaining = duration - (time.time() - start_time)
                wait = min(random.uniform(5, 30), remaining)
                if wait > 0:
                    time.sleep(wait)
                if time.time() - start_time < duration:
                    self._scan_plus_fire_and_validate_stress(index, hashmaps, violations)
        finally:
            stop_event.set()
            for t in crud_threads:
                t.join(timeout=30)
            if mgr_options:
                self._scan_plus_restore_manager_defaults()

        self._scan_plus_fire_and_validate_stress(index, hashmaps, violations)

        if violations:
            for v in violations:
                self.log.error(f"[scan_plus_stress] {v}")
            self.fail(
                f"scan_plus stress: {len(violations)} violation(s) detected"
            )
        else:
            self.log.info("[scan_plus_stress] PASSED — no violations detected")

    def test_scan_plus_functional(self):
        """
        Functional test for scan_plus consistency.

        CRUD runs in `num_batches` batches of `batch_duration` seconds each.
        After each batch all in-flight ops are drained before the HashMap is
        snapshotted and a scan_plus query is fired.

        Validation (exact, functional):
          • FTS doc count == snapshot doc count
          • For every doc in snapshot: FTS result.val == snapshot.val
          • No extra docs in FTS results that are absent from snapshot

        Params (via TestInput):
          batch_duration   : seconds of CRUD per batch (default 60)
          num_batches      : number of CRUD-pause-query cycles (default 3)
          num_crud_threads : parallel CRUD goroutines (default 4)
          initial_docs     : seed doc count (default 5)
          index_partitions     : FTS index partition count (default 1)
          num_replicas         : FTS index replica count (default 0)
          getseqnos_retries    : override scan_plus getseqnos_retries (default: server default 30)
          use_bucket_seqnos    : override scan_plus use_bucket_seqnos (default: server default false)
          num_workers          : override scan_plus num_workers (default: server default 10)
        """
        batch_duration = TestInputSingleton.input.param("batch_duration", 60)
        num_batches = TestInputSingleton.input.param("num_batches", 3)
        num_crud_threads = TestInputSingleton.input.param("num_crud_threads", 4)
        initial_docs = TestInputSingleton.input.param("initial_docs", 5)
        index_partitions = TestInputSingleton.input.param("index_partitions", 1)
        num_replicas = TestInputSingleton.input.param("num_replicas", 0)
        getseqnos_retries = TestInputSingleton.input.param("getseqnos_retries", None)
        use_bucket_seqnos = TestInputSingleton.input.param("use_bucket_seqnos", None)
        num_workers = TestInputSingleton.input.param("num_workers", None)

        mgr_options = {}
        if getseqnos_retries is not None:
            mgr_options["getseqnos_retries"] = str(getseqnos_retries)
        if use_bucket_seqnos is not None:
            mgr_options["use_bucket_seqnos"] = str(use_bucket_seqnos).lower()
        if num_workers is not None:
            mgr_options["num_workers"] = str(num_workers)
        if mgr_options:
            self._scan_plus_set_manager_options(mgr_options)

        bucket = self._cb_cluster.get_bucket_by_name('default')
        _, collection = self._scan_plus_setup_sdk()

        # One HashMap per thread; distribute initial_docs so total seeded == initial_docs
        base = initial_docs // num_crud_threads
        extra_docs = initial_docs % num_crud_threads
        hashmaps = []
        for t_idx in range(num_crud_threads):
            docs_for_thread = base + (1 if t_idx < extra_docs else 0)
            hm = _ScanPlusHashMap()
            for i in range(docs_for_thread):
                doc_id = f"scan_plus_t{t_idx}_init_{i}"
                collection.upsert(doc_id, {'val': 1})
                hm.insert(doc_id, 1)
            hashmaps.append(hm)

        total_initial_docs = initial_docs
        index = self.create_index(bucket, "scan_plus_functional_idx")
        index.add_child_field_to_default_mapping("val", "number")
        if index_partitions > 1:
            # update_index_partitions GETs the server definition (overwriting the
            # local one), so push the val mapping first so the GET picks it up.
            index.index_definition['uuid'] = index.get_uuid()
            index.update()
            index.update_index_partitions(index_partitions)
        if num_replicas > 0:
            index.update_num_replicas(num_replicas)
        index.index_definition['uuid'] = index.get_uuid()
        index.update()
        # Pass item_count so wait_for_indexing_complete doesn't return early
        # when the bucket stat briefly reads 0 (0==0 fast-exit path).
        self.wait_for_indexing_complete(item_count=total_initial_docs)

        drain_cond = threading.Condition()
        inflight_count = [0]
        all_errors = []

        for batch_num in range(1, num_batches + 1):
            self.log.info(
                f"[scan_plus_functional] Batch {batch_num}/{num_batches}: "
                f"starting CRUD ({batch_duration}s)"
            )
            stop_event = threading.Event()

            crud_threads = []
            for t_idx in range(num_crud_threads):
                t = threading.Thread(
                    target=self._scan_plus_crud_worker,
                    args=(collection, hashmaps[t_idx], stop_event,
                          drain_cond, inflight_count),
                    kwargs={'thread_idx': t_idx},
                    daemon=True,
                )
                t.start()
                crud_threads.append(t)

            time.sleep(batch_duration)

            stop_event.set()
            with drain_cond:
                while inflight_count[0] > 0:
                    drain_cond.wait(timeout=30)

            for t in crud_threads:
                t.join(timeout=30)

            total_size = sum(hm.size() for hm in hashmaps)
            self.log.info(
                f"[scan_plus_functional] Batch {batch_num}: CRUD drained, "
                f"total hashmap size={total_size}. Firing query…"
            )

            errors = self._scan_plus_fire_and_validate_functional(index, hashmaps, collection=collection)
            if errors:
                all_errors.extend(
                    [f"Batch {batch_num}: {e}" for e in errors]
                )
                for e in errors:
                    self.log.error(f"[scan_plus_functional] Batch {batch_num}: {e}")
            else:
                self.log.info(
                    f"[scan_plus_functional] Batch {batch_num}: PASSED"
                )

        if all_errors:
            for e in all_errors:
                self.log.error(f"[scan_plus_functional] {e}")
            if mgr_options:
                self._scan_plus_restore_manager_defaults()
            self.fail(
                f"scan_plus functional: {len(all_errors)} error(s) across "
                f"{num_batches} batch(es)"
            )
        else:
            self.log.info(
                "[scan_plus_functional] PASSED — all batches validated"
            )
        if mgr_options:
            self._scan_plus_restore_manager_defaults()

    def test_scan_plus_multi_collection(self):
        """
        scan_plus consistency across 3 collections covered by one FTS index.

        Creates a custom scope ('sp_scope') with 3 collections ('col_a', 'col_b',
        'col_c'), builds a single collection-aware FTS index spanning all three,
        then runs continuous CRUD against every collection simultaneously.

        Validates that scan_plus match_all queries return consistent results
        across the entire multi-collection index — no stale deletes, no stale
        val reads — with the same unidirectional stress check used by
        test_scan_plus_stress.

        Params (via TestInput):
          duration         : total run time in seconds (default 600)
          num_crud_threads : total CRUD threads, distributed across collections (default 6)
          initial_docs     : seed docs per collection (default 30)
          index_partitions : FTS index partition count (default 1)
          num_replicas     : FTS index replica count (default 0)
        """
        duration = TestInputSingleton.input.param("duration", 600)
        num_crud_threads = TestInputSingleton.input.param("num_crud_threads", 6)
        initial_docs = TestInputSingleton.input.param("initial_docs", 30)
        index_partitions = TestInputSingleton.input.param("index_partitions", 1)
        num_replicas = TestInputSingleton.input.param("num_replicas", 0)

        SCOPE = "sp_scope"
        COLLECTIONS = ["col_a", "col_b", "col_c"]

        bucket = self._cb_cluster.get_bucket_by_name('default')

        # Create scope + collections
        self._cb_cluster.create_scope_using_rest('default', SCOPE)
        for col in COLLECTIONS:
            self._cb_cluster.create_collection_using_rest('default', SCOPE, col)
        self.sleep(5, "Wait for collections to initialise")

        # SDK connections: one per collection
        cluster, _ = self._scan_plus_setup_sdk()
        cb_scope = cluster.bucket('default').scope(SCOPE)
        cb_cols = {col: cb_scope.collection(col) for col in COLLECTIONS}

        # Distribute CRUD threads across collections; spread remainder so total == num_crud_threads
        threads_per_col = num_crud_threads // len(COLLECTIONS)
        extra = num_crud_threads % len(COLLECTIONS)
        # Seed docs and build per-thread HashMaps; distribute initial_docs within each collection
        col_hashmaps = {}
        total_seeded = 0
        for col_idx, col in enumerate(COLLECTIONS):
            threads_for_col = threads_per_col + (1 if col_idx < extra else 0)
            col_hashmaps[col] = []
            seed_threads = max(1, threads_for_col)
            per_thread = initial_docs // seed_threads
            extra_docs = initial_docs % seed_threads
            for t_idx in range(seed_threads):
                docs_for_thread = per_thread + (1 if t_idx < extra_docs else 0)
                hm = _ScanPlusHashMap()
                for i in range(docs_for_thread):
                    doc_id = f"sp_{col}_{t_idx}_init_{i}"
                    cb_cols[col].upsert(doc_id, {'val': 1})
                    hm.insert(doc_id, 1)
                    total_seeded += 1
                col_hashmaps[col].append(hm)

        self.log.info(f"[scan_plus_multicol] Seeded {total_seeded} docs across {len(COLLECTIONS)} collections")

        _types = [f"{SCOPE}.{col}" for col in COLLECTIONS]
        index = self._cb_cluster.create_fts_index(
            "scan_plus_multicol_idx", source_name='default',
            scope=SCOPE, collection_index=True,
            _type=_types, collections=COLLECTIONS,
            plan_params={"numReplicas": num_replicas, "indexPartitions": index_partitions},
            source_params={"scopeParams": {"name": SCOPE,
                                           "collections": [{"name": col} for col in COLLECTIONS]}})
        # add_type_mapping_to_index_definition defaults dynamic=True; override to False
        # so only explicitly mapped fields (val) are indexed per collection
        for typ in _types:
            index.index_definition['params']['mapping']['types'][typ]['dynamic'] = False
        for col in COLLECTIONS:
            index.add_child_field_to_default_collection_mapping(
                "val", "number", scope=SCOPE, collection=col)
        index.index_definition['uuid'] = index.get_uuid()
        index.update()
        total_initial_docs = total_seeded
        self.wait_for_indexing_complete(item_count=total_initial_docs)

        # Flatten all hashmaps into one list for combined snapshot in validate
        all_hashmaps = [hm for col in COLLECTIONS for hm in col_hashmaps[col]]

        # Launch CRUD threads; global_t_idx is a running counter across all collections
        stop_event = threading.Event()
        violations = []
        crud_threads = []
        global_t_idx = 0
        for col_idx, col in enumerate(COLLECTIONS):
            threads_for_col = threads_per_col + (1 if col_idx < extra else 0)
            for t_idx in range(threads_for_col):
                t = threading.Thread(
                    target=self._scan_plus_crud_worker,
                    args=(cb_cols[col], col_hashmaps[col][t_idx], stop_event),
                    kwargs={'thread_idx': global_t_idx},
                    daemon=True,
                )
                t.start()
                crud_threads.append(t)
                global_t_idx += 1

        self.log.info(
            f"[scan_plus_multicol] Running {duration}s | "
            f"{len(crud_threads)} CRUD threads across {len(COLLECTIONS)} collections"
        )

        start_time = time.time()
        try:
            while time.time() - start_time < duration:
                remaining = duration - (time.time() - start_time)
                wait = min(random.uniform(5, 30), remaining)
                if wait > 0:
                    time.sleep(wait)
                if time.time() - start_time < duration:
                    self._scan_plus_fire_and_validate_stress(index, all_hashmaps, violations)
        finally:
            stop_event.set()
            for t in crud_threads:
                t.join(timeout=30)

        # Final query after CRUD fully stopped
        self._scan_plus_fire_and_validate_stress(index, all_hashmaps, violations)

        if violations:
            for v in violations:
                self.log.error(f"[scan_plus_multicol] {v}")
            self.fail(
                f"scan_plus multi-collection: {len(violations)} violation(s) detected"
            )
        else:
            self.log.info("[scan_plus_multicol] PASSED — no violations detected")

    def test_scan_plus_vector(self):
        """
        Verifies that scan_plus consistency is not broken when the FTS index
        contains a vector field and queries are KNN-based.

        Uses the same functional batch approach as test_scan_plus_functional:
        CRUD runs for batch_duration seconds, drains, then a KNN scan_plus
        query is fired with K=1000 (the FTS maximum).  Because the live corpus
        stays well under 1000 docs, every live doc is returned by the KNN query
        and the same exact val-consistency validation applies.

        Docs carry a `vec` field (1536-dim unit vector, deterministic per
        doc_id) alongside the usual `val` counter.  CRUD only increments val;
        vec is reconstructed from doc_id on every upsert so it never drifts.

        Params (via TestInput):
          batch_duration   : seconds of CRUD per batch (default 60)
          num_batches      : CRUD-drain-query cycles (default 3)
          num_crud_threads : parallel CRUD threads (default 4)
          initial_docs     : seed doc count, keep << 1000 (default 10)
          vec_dims         : vector dimensions (default 1536)
          num_replicas     : FTS index replica count (default 0)
        """
        batch_duration   = TestInputSingleton.input.param("batch_duration", 60)
        num_batches      = TestInputSingleton.input.param("num_batches", 3)
        num_crud_threads = TestInputSingleton.input.param("num_crud_threads", 4)
        initial_docs     = TestInputSingleton.input.param("initial_docs", 10)
        vec_dims         = TestInputSingleton.input.param("vec_dims", 1536)
        num_replicas     = TestInputSingleton.input.param("num_replicas", 0)

        KNN_K = 1000  # FTS max; k covers the whole corpus only if total docs < 1000
        knn_base = KNN_K // num_crud_threads
        knn_extra = KNN_K % num_crud_threads
        per_thread_caps = [knn_base + (1 if i < knn_extra else 0) for i in range(num_crud_threads)]

        _, collection = self._scan_plus_setup_sdk()

        base = initial_docs // num_crud_threads
        extra_docs = initial_docs % num_crud_threads
        hashmaps = []
        for t_idx in range(num_crud_threads):
            docs_for_thread = base + (1 if t_idx < extra_docs else 0)
            hm = _ScanPlusHashMap()
            for i in range(docs_for_thread):
                doc_id = f"sp_vec_t{t_idx}_init_{i}"
                collection.upsert(doc_id, {'val': 1, 'vec': _make_vec(doc_id, vec_dims)})
                hm.insert(doc_id, 1)
            hashmaps.append(hm)

        total_initial_docs = initial_docs

        index = self._cb_cluster.create_fts_index(
            "scan_plus_vec_idx", source_name='default',
            plan_params={"numReplicas": num_replicas})
        index.add_child_field_to_default_mapping("val", "number")
        # vec is a vector field; add_child_field_to_default_mapping doesn't support
        # dims/similarity, so set it directly on the already-established default_mapping
        index.index_definition['params']['mapping']['default_mapping']['properties']['vec'] = {
            "dynamic": False, "enabled": True,
            "fields": [{"name": "vec", "type": "vector", "dims": vec_dims,
                        "similarity": "l2_norm", "index": True}],
        }
        index.index_definition['uuid'] = index.get_uuid()
        index.update()
        self.wait_for_indexing_complete(item_count=total_initial_docs)

        # Fixed query vector — same for every batch.  With K=1000 >= corpus
        # size, all live docs are returned regardless of distance ordering.
        query_vec = [0.0] * vec_dims
        query_vec[0] = 1.0  # non-zero so l2_norm has a defined nearest neighbour
        knn_param = [{"field": "vec", "vector": query_vec, "k": KNN_K}]

        drain_cond = threading.Condition()
        inflight_count = [0]
        all_errors = []

        for batch_num in range(1, num_batches + 1):
            self.log.info(
                f"[scan_plus_vector] Batch {batch_num}/{num_batches}: "
                f"starting CRUD ({batch_duration}s)"
            )
            stop_event = threading.Event()
            crud_threads = []
            for t_idx in range(num_crud_threads):
                t = threading.Thread(
                    target=self._scan_plus_vec_crud_worker,
                    args=(collection, hashmaps[t_idx], t_idx, stop_event,
                          vec_dims, drain_cond, inflight_count),
                    kwargs={'max_docs': per_thread_caps[t_idx]},
                    daemon=True,
                )
                t.start()
                crud_threads.append(t)

            time.sleep(batch_duration)
            stop_event.set()
            with drain_cond:
                while inflight_count[0] > 0:
                    drain_cond.wait(timeout=30)
            for t in crud_threads:
                t.join(timeout=30)

            total_size = sum(hm.size() for hm in hashmaps)
            self.log.info(
                f"[scan_plus_vector] Batch {batch_num}: CRUD drained, "
                f"hashmap size={total_size}. Firing KNN query…"
            )
            errors = self._scan_plus_fire_and_validate_functional(
                index, hashmaps, collection=collection, knn=knn_param)
            if errors:
                all_errors.extend([f"Batch {batch_num}: {e}" for e in errors])
                for e in errors:
                    self.log.error(f"[scan_plus_vector] Batch {batch_num}: {e}")
            else:
                self.log.info(f"[scan_plus_vector] Batch {batch_num}: PASSED")

        if all_errors:
            for e in all_errors:
                self.log.error(f"[scan_plus_vector] {e}")
            self.fail(
                f"scan_plus vector: {len(all_errors)} error(s) across {num_batches} batch(es)"
            )
        else:
            self.log.info("[scan_plus_vector] PASSED — all batches validated")
    # =========================================================================
    # UDF Tests — Epic MB-65018
    # Related cases are grouped; failures collected and reported at end of each method.
    # =========================================================================

    # ── CT — Configuration / Toggle Tests ────────────────────────────────────

    def test_udf_ct(self):
        # CT-1..CT-6: toggle/configuration tests
        failures = []

        # CT-1: UDF disabled → custom_filter must fail (P0)
        self.log.info("CT-1: UDF disabled — custom_filter must be rejected with non-200")
        self._udf._disable_udf()
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 5,
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "source": "function f(doc,params){ return true; }",
            }},
        })
        self._udf._enable_udf()
        if s == 200:
            failures.append(f"CT-1: expected non-200 when UDF disabled, got {s}")
        self.log.info(f"CT-1: status={s}")

        # CT-2: UDF enabled → custom_filter succeeds (P0)
        self.log.info("CT-2: UDF enabled — custom_filter must succeed with > 0 hits")
        hits, err = self._udf._udf_query({
            "size": 5,
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "source": "function f(doc,params){ return true; }",
            }},
        })
        if err or hits == 0:
            failures.append(f"CT-2: expected > 0 hits when UDF enabled, got hits={hits} err={err}")
        self.log.info(f"CT-2: hits={hits}")

        # CT-3: Normal query unaffected when UDF disabled (P1)
        self.log.info("CT-3: normal query must work even when UDF is disabled")
        self._udf._disable_udf()
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 5, "query": {"match": "hotel", "field": "type"},
        })
        self._udf._enable_udf()
        if s != 200 or "total_hits" not in r:
            failures.append(f"CT-3: normal query failed when UDF disabled: status={s}")
        self.log.info(f"CT-3: total_hits={r.get('total_hits', '?')}")

        # CT-4: Rapid ON/OFF/ON sequence stabilises (P2)
        self.log.info("CT-4: rapid ON/OFF/ON toggle — final state must be enabled and functional")
        self._udf._enable_udf()
        s1, _ = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 5,
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "source": "function f(doc,params){return true;}",
            }},
        })
        self._udf._disable_udf()
        s2, _ = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 5,
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "source": "function f(doc,params){return true;}",
            }},
        })
        time.sleep(2)
        self._udf._enable_udf()
        s3, _ = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 5,
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "source": "function f(doc,params){return true;}",
            }},
        })
        if s1 != 200:
            failures.append(f"CT-4: first UDF query failed with {s1}")
        if s2 == 200:
            failures.append(f"CT-4: expected non-200 after disable, got {s2}")
        if s3 != 200:
            failures.append(f"CT-4: UDF query failed after re-enable: {s3}")
        self.log.info(f"CT-4: s1={s1} s2={s2} s3={s3}")

        # CT-5: New queries fail after disable; normal queries still work (P2)
        self.log.info("CT-5: after disable, new UDF queries fail but normal queries succeed")
        results = []
        def run_query():
            h, e = self._udf._udf_query({
                "size": 20,
                "query": {"custom_filter": {
                    "query": {"match": "hotel", "field": "type"},
                    "source": "function f(doc,params){ return true; }",
                }},
            })
            results.append((h, e))
        t = threading.Thread(target=run_query)
        t.start()
        time.sleep(0.05)
        self._udf._disable_udf()
        t.join(timeout=30)
        s_udf, _ = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 5,
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "source": "function f(doc,params){return true;}",
            }},
        })
        s_normal, r_normal = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 5, "query": {"match": "hotel", "field": "type"},
        })
        self._udf._enable_udf()
        if s_udf == 200:
            failures.append(f"CT-5: expected non-200 after disable, got {s_udf}")
        if s_normal != 200:
            failures.append(f"CT-5: normal query failed: {r_normal}")
        self.log.info(f"CT-5: udf_status={s_udf} normal_status={s_normal}")

        # CT-6: Memory overhead with UDF enabled — observational (P3)
        self.log.info("CT-6: observational — memory overhead with UDF enabled (~160 MB per node expected)")
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 5,
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "source": "function f(doc,params){return true;}",
            }},
        })
        if s != 200:
            failures.append(f"CT-6: UDF query failed: status={s}")
        self.log.info("CT-6: UDF flag active; memory overhead should be ~160 MB per FTS node (spec)")

        if failures:
            self.fail("test_udf_ct failed sub-cases:\n" + "\n".join(failures))
        self.log.info("test_udf_ct PASSED — all CT sub-cases passed")

    # ── CF — custom_filter Functionality ─────────────────────────────────────

    def test_udf_cf_basic(self):
        # CF-1..CF-5: basic custom_filter functionality
        failures = []
        gt = self._udf_gt

        # CF-1: Always-true filter passes all inner-query hits (P0)
        self.log.info("CF-1: always-true filter — inner query selects hotels, UDF returns true for all")
        hits, err = self._udf._udf_query({
            "size": 20,
            "fields": ["name", "type"],
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "fields": ["type"],
                "source": "function always_true(doc, params){ return true; }",
            }},
        })
        if err or hits != gt["cf_1_always_true"]:
            failures.append(f"CF-1: expected {gt['cf_1_always_true']} got {hits}, err={err}")
        self.log.info(f"CF-1: hits={hits}")

        # CF-2: Always-false filter drops all hits (P0)
        self.log.info("CF-2: always-false filter — UDF returns false for all, expect 0 hits")
        hits, err = self._udf._udf_query({
            "size": 20,
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "source": "function always_false(doc, params){ return false; }",
            }},
        })
        if err or hits != 0:
            failures.append(f"CF-2: expected 0 got {hits}, err={err}")
        self.log.info(f"CF-2: hits={hits}")

        # CF-3: Filter on cancellationPolicy === 'FREE' (P0)
        self.log.info("CF-3: filter cancellationPolicy === 'FREE' — only hotels with FREE policy pass")
        hits, err = self._udf._udf_query({
            "size": 20,
            "fields": ["name", "cancellationPolicy"],
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "fields": ["cancellationPolicy"],
                "source": "function free_cancel(doc, params){ var f = doc.fields || {}; return f.cancellationPolicy === 'free'; }",
            }},
        })
        if err or hits != gt["cf_3_free_policy"]:
            failures.append(f"CF-3: expected {gt['cf_3_free_policy']} got {hits}, err={err}")
        self.log.info(f"CF-3: hits={hits}")

        # CF-4: Numeric comparison price_per_night > 100 (P0)
        self.log.info("CF-4: numeric filter price_per_night > 100 — hotels above threshold pass")
        hits, err = self._udf._udf_query({
            "size": 20,
            "fields": ["name", "price_per_night"],
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "fields": ["price_per_night"],
                "params": {"min_price": 100},
                "source": "function price_filter(doc, params){ var f = doc.fields || {}; if (f.price_per_night === undefined) return false; return f.price_per_night > params.min_price; }",
            }},
        })
        if err or hits != gt["cf_4_price_gt_100"]:
            failures.append(f"CF-4: expected {gt['cf_4_price_gt_100']} got {hits}, err={err}")
        self.log.info(f"CF-4: hits={hits}")

        # CF-5: Computed arithmetic — price_per_night * stay_nights <= budget (P0)
        self.log.info("CF-5: arithmetic filter price_per_night * stay_nights <= budget (750 for 5 nights)")
        hits, err = self._udf._udf_query({
            "size": 20,
            "fields": ["name", "price_per_night"],
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "fields": ["price_per_night"],
                "params": {"stay_nights": 5, "budget": 750},
                "source": "function budget_filter(doc, params){ var f = doc.fields || {}; var price = f.price_per_night; if (price === undefined) return false; return (price * params.stay_nights) <= params.budget; }",
            }},
        })
        if err or hits != gt["cf_5_budget_750_5nights"]:
            failures.append(f"CF-5: expected {gt['cf_5_budget_750_5nights']} got {hits}, err={err}")
        self.log.info(f"CF-5: hits={hits}")

        if failures:
            self.fail("test_udf_cf_basic failed sub-cases:\n" + "\n".join(failures))
        self.log.info("test_udf_cf_basic PASSED — all CF basic sub-cases passed")

    def test_udf_cf_advanced(self):
        # CF-6..CF-13: advanced custom_filter functionality
        failures = []
        gt = self._udf_gt

        # CF-6: Boolean field filter — available === true (P1)
        self.log.info("CF-6: boolean filter available === true — only available hotels pass")
        hits, err = self._udf._udf_query({
            "size": 20,
            "fields": ["name", "available"],
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "fields": ["available"],
                "source": "function avail_filter(doc, params){ var f = doc.fields || {}; return f.available === true; }",
            }},
        })
        if err or hits != gt["cf_6_available"]:
            failures.append(f"CF-6: expected {gt['cf_6_available']} got {hits}, err={err}")
        self.log.info(f"CF-6: hits={hits}")

        # CF-7: Multi-valued array field — amenities contains 'pool' (P1)
        self.log.info("CF-7: array field filter amenities contains 'pool' — only hotels with pool pass")
        hits, err = self._udf._udf_query({
            "size": 20,
            "fields": ["name", "amenities"],
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "fields": ["amenities"],
                "source": "function pool_filter(doc, params){ var f = doc.fields || {}; var a = f.amenities; if (!Array.isArray(a)) a = [a]; return a.indexOf('pool') !== -1; }",
            }},
        })
        if err or hits != gt["cf_7_pool_amenity"]:
            failures.append(f"CF-7: expected {gt['cf_7_pool_amenity']} got {hits}, err={err}")
        self.log.info(f"CF-7: hits={hits}")

        # CF-8: Undefined field graceful handling — rating >= 4.0 with missing ratings (P1)
        self.log.info("CF-8: undefined field handling — filter rating >= 4.0, missing rating returns false")
        hits, err = self._udf._udf_query({
            "size": 20,
            "fields": ["name", "rating"],
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "fields": ["rating"],
                "params": {"min_rating": 4.0},
                "source": "function rating_filter(doc, params){ var f = doc.fields || {}; if (f.rating === undefined) return false; return f.rating >= params.min_rating; }",
            }},
        })
        if err or hits != gt["cf_8_rating_4plus"]:
            failures.append(f"CF-8: expected {gt['cf_8_rating_4plus']} got {hits}, err={err}")
        self.log.info(f"CF-8: hits={hits}")

        # CF-9: Type-based filter — brewery only (P1)
        self.log.info("CF-9: type-based filter type === 'brewery' — only brewery docs pass")
        hits, err = self._udf._udf_query({
            "size": 20,
            "fields": ["name", "type"],
            "query": {"custom_filter": {
                "query": {"match": "brewery", "field": "type"},
                "fields": ["type"],
                "source": "function brewery_only(doc, params){ var f = doc.fields || {}; return f.type === 'brewery'; }",
            }},
        })
        if err or hits != gt["cf_9_brewery_only"]:
            failures.append(f"CF-9: expected {gt['cf_9_brewery_only']} got {hits}, err={err}")
        self.log.info(f"CF-9: hits={hits}")

        # CF-10: Multi-field AND logic — available AND FREE (P1)
        self.log.info("CF-10: multi-field AND logic — available === true AND cancellationPolicy === 'FREE'")
        hits, err = self._udf._udf_query({
            "size": 20,
            "fields": ["name", "available", "cancellationPolicy"],
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "fields": ["available", "cancellationPolicy"],
                "source": "function avail_and_free(doc, params){ var f = doc.fields || {}; return f.available === true && f.cancellationPolicy === 'free'; }",
            }},
        })
        if err or hits != gt["cf_10_avail_and_free"]:
            failures.append(f"CF-10: expected {gt['cf_10_avail_and_free']} got {hits}, err={err}")
        self.log.info(f"CF-10: hits={hits}")

        # CF-11: Multi-field OR logic — available OR rating >= 4.5 (P1)
        self.log.info("CF-11: multi-field OR logic — available === true OR rating >= 4.5")
        hits, err = self._udf._udf_query({
            "size": 20,
            "fields": ["name", "available", "rating"],
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "fields": ["available", "rating"],
                "source": "function avail_or_highrate(doc, params){ var f = doc.fields || {}; return f.available === true || f.rating >= 4.5; }",
            }},
        })
        if err or hits != gt["cf_11_avail_or_rating45"]:
            failures.append(f"CF-11: expected {gt['cf_11_avail_or_rating45']} got {hits}, err={err}")
        self.log.info(f"CF-11: hits={hits}")

        # CF-12: doc._id accessible in custom_filter — filter by key prefix (P2)
        self.log.info("CF-12: doc._id accessible — filter by key prefix 'brewery'")
        hits, err = self._udf._udf_query({
            "size": 20,
            "fields": ["name"],
            "query": {"custom_filter": {
                "query": {"match_all": {}},
                "source": "function id_filter(doc, params){ return doc._id.indexOf('brewery') === 0; }",
            }},
        })
        if err or hits != gt["cf_12_brewery_id"]:
            failures.append(f"CF-12: expected {gt['cf_12_brewery_id']} got {hits}, err={err}")
        self.log.info(f"CF-12: hits={hits}")

        # CF-13: Exclude NON_REFUNDABLE and docs without cancellationPolicy (P2)
        self.log.info("CF-13: exclude NON_REFUNDABLE and docs without cancellationPolicy")
        hits, err = self._udf._udf_query({
            "size": 20,
            "fields": ["name", "cancellationPolicy"],
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "fields": ["cancellationPolicy"],
                "source": "function refundable_only(doc, params){ var f = doc.fields || {}; var p = f.cancellationPolicy; if (p === undefined || p === null) return false; return p !== 'non_refundable'; }",
            }},
        })
        if err or hits != gt["cf_13_refundable"]:
            failures.append(f"CF-13: expected {gt['cf_13_refundable']} got {hits}, err={err}")
        self.log.info(f"CF-13: hits={hits}")

        if failures:
            self.fail("test_udf_cf_advanced failed sub-cases:\n" + "\n".join(failures))
        self.log.info("test_udf_cf_advanced PASSED — all CF advanced sub-cases passed")

    # ── CS — custom_score Functionality ───────────────────────────────────────

    def test_udf_cs_basic(self):
        # CS-1..CS-3: basic custom_score functionality
        failures = []
        gt = self._udf_gt

        # CS-1: Fixed score overrides all doc scores (P0)
        self.log.info("CS-1: fixed score 10.0 — all hotel docs must have score=10.0")
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 20,
            "fields": ["name"],
            "query": {"custom_score": {
                "query": {"match": "hotel", "field": "type"},
                "source": "function fixed_score(doc, params){ return 10.0; }",
            }},
        })
        if s != 200:
            failures.append(f"CS-1: query failed status={s}")
        else:
            hits = r.get("total_hits", 0)
            if hits != gt["cf_hotels"]:
                failures.append(f"CS-1: expected {gt['cf_hotels']} hits got {hits}")
            wrong = [h["id"] for h in r.get("hits", []) if abs(h.get("score", 0) - 10.0) > 0.001]
            if wrong:
                failures.append(f"CS-1: score != 10.0 for {wrong}")
            self.log.info(f"CS-1: total_hits={hits}")

        # CS-2: Score multiplier on base score — result ≈ 2× base (P0)
        self.log.info("CS-2: score multiplier — custom_score returns doc.score * 2, verify each hit")
        _, base_r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 20, "fields": ["name"], "query": {"match": "hotel", "field": "type"},
        })
        base_scores = {h["id"]: h["score"] for h in base_r.get("hits", [])}
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 20,
            "fields": ["name"],
            "query": {"custom_score": {
                "query": {"match": "hotel", "field": "type"},
                "source": "function double_score(doc, params){ return doc.score * 2; }",
            }},
        })
        if s != 200:
            failures.append(f"CS-2: query failed status={s}")
        else:
            wrong = [
                f"{h['id']}: got={h.get('score', 0):.3f} expected≈{base_scores.get(h['id'], 0) * 2:.3f}"
                for h in r.get("hits", [])
                if abs(h.get("score", 0) - base_scores.get(h["id"], 0) * 2) > 0.01
            ]
            if wrong:
                failures.append(f"CS-2: score mismatch: {wrong}")
            self.log.info(f"CS-2: total_hits={r.get('total_hits', '?')}")

        # CS-3: Field-based score boost — abv boost for beers (P0)
        self.log.info("CS-3: field-based score boost abv*0.05 — high-abv beer should rank higher")
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 10,
            "fields": ["name", "abv"],
            "query": {"custom_score": {
                "query": {"match": "beer", "field": "type"},
                "fields": ["abv"],
                "params": {"mult": 0.05},
                "source": "function score_abv(doc, params){ var f = doc.fields || {}; var abv = f.abv || 0; return doc.score + (abv * params.mult); }",
            }},
        })
        if s != 200:
            failures.append(f"CS-3: query failed status={s}")
        else:
            hits = r.get("total_hits", 0)
            if hits != 5:
                failures.append(f"CS-3: expected 5 beer hits got {hits}")
            scores = {h["id"]: h["score"] for h in r.get("hits", [])}
            if "beer::004" in scores and "beer::005" in scores:
                if scores["beer::004"] <= scores["beer::005"]:
                    failures.append(f"CS-3: high-abv beer::004 should outscore beer::005: {scores}")
            self.log.info(f"CS-3: total_hits={hits}")

        if failures:
            self.fail("test_udf_cs_basic failed sub-cases:\n" + "\n".join(failures))
        self.log.info("test_udf_cs_basic PASSED — all CS basic sub-cases passed")

    def test_udf_cs_advanced(self):
        # CS-4..CS-8: advanced custom_score functionality
        failures = []

        # CS-4: Score = price_per_night — deterministic ordering (P1)
        self.log.info("CS-4: score = price_per_night — most expensive hotel must rank first")
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 20,
            "fields": ["name", "price_per_night"],
            "query": {"custom_score": {
                "query": {"match": "hotel", "field": "type"},
                "fields": ["price_per_night"],
                "source": "function price_as_score(doc, params){ var f = doc.fields || {}; return f.price_per_night || 0; }",
            }},
        })
        if s != 200:
            failures.append(f"CS-4: query failed status={s}")
        else:
            hits_list = r.get("hits", [])
            if not hits_list:
                failures.append("CS-4: no hits returned")
            else:
                top_id = hits_list[0]["id"]
                if top_id not in ("hotel::london_003", "hotel::paris_003"):
                    failures.append(f"CS-4: top result should be most expensive hotel, got {top_id}")
                self.log.info(f"CS-4: total_hits={r.get('total_hits', '?')} top={top_id}")

        # CS-5: Complex multi-factor scoring (P1)
        self.log.info("CS-5: complex multi-factor scoring combining distance and price factors")
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 20,
            "fields": ["name", "price_per_night", "distanceFromCenterKm"],
            "query": {"custom_score": {
                "query": {"match": "hotel", "field": "type"},
                "fields": ["distanceFromCenterKm", "price_per_night"],
                "params": {"max_distance_km": 5.0},
                "source": (
                    "function hotel_score(doc, params){"
                    "var maxD = params.max_distance_km || 5.0;"
                    "var f = doc.fields || {};"
                    "var dist = (f.distanceFromCenterKm !== undefined) ? f.distanceFromCenterKm : maxD;"
                    "var price = (f.price_per_night !== undefined) ? f.price_per_night : 9999;"
                    "var distFactor = 1.0 - Math.min(dist / maxD, 1.0);"
                    "var priceFactor = 1.0 / (1.0 + price / 200.0);"
                    "return doc.score * (1.0 + distFactor + priceFactor);"
                    "}"
                ),
            }},
        })
        if s != 200:
            failures.append(f"CS-5: query failed status={s}")
        else:
            hits_list = r.get("hits", [])
            if not hits_list:
                failures.append("CS-5: no hits returned")
            else:
                zero = [h["id"] for h in hits_list if h.get("score", 0) <= 0]
                if zero:
                    failures.append(f"CS-5: score must be > 0 for {zero}")
                self.log.info(f"CS-5: total_hits={r.get('total_hits', '?')}")

        # CS-6: Score = 0 for all documents — observational (P2)
        self.log.info("CS-6: zero score — observational, all docs return score=0")
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 10,
            "query": {"custom_score": {
                "query": {"match": "hotel", "field": "type"},
                "source": "function zero_score(doc, params){ return 0; }",
            }},
        })
        if s != 200:
            failures.append(f"CS-6: query failed status={s}")
        self.log.info(f"CS-6: total_hits={r.get('total_hits')} — zero-score hits returned")

        # CS-7: Negative score — query completes without crash (P2)
        self.log.info("CS-7: negative score — observational, query must complete without crash")
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 5,
            "query": {"custom_score": {
                "query": {"match": "hotel", "field": "type"},
                "source": "function neg_score(doc, params){ return -1.0; }",
            }},
        })
        if s != 200:
            failures.append(f"CS-7: query failed status={s}")
        self.log.info(f"CS-7: negative score result: total_hits={r.get('total_hits')}")

        # CS-8: doc.score available in custom_score and equals base score (P1)
        self.log.info("CS-8: doc.score echo — custom_score returns doc.score, must match base scores")
        _, base_r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 5, "fields": ["name"], "query": {"match": "hotel", "field": "type"},
        })
        base_scores = {h["id"]: h["score"] for h in base_r.get("hits", [])}
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 5,
            "fields": ["name"],
            "query": {"custom_score": {
                "query": {"match": "hotel", "field": "type"},
                "source": "function echo_score(doc, params){ return doc.score; }",
            }},
        })
        if s != 200:
            failures.append(f"CS-8: query failed status={s}")
        else:
            wrong = [
                f"{h['id']}: echo={h.get('score', 0):.4f} base={base_scores.get(h['id'], 0):.4f}"
                for h in r.get("hits", [])
                if abs(h.get("score", 0) - base_scores.get(h["id"], 0)) > 0.001
            ]
            if wrong:
                failures.append(f"CS-8: echo score mismatch: {wrong}")
            self.log.info(f"CS-8: total_hits={r.get('total_hits', '?')}")

        if failures:
            self.fail("test_udf_cs_advanced failed sub-cases:\n" + "\n".join(failures))
        self.log.info("test_udf_cs_advanced PASSED — all CS advanced sub-cases passed")

    # ── FL — Fields Loading Tests ──────────────────────────────────────────────

    def test_udf_fl(self):
        # FL-1..FL-7: fields loading tests
        failures = []

        # FL-1: Only listed fields visible to UDF (P1)
        self.log.info("FL-1: only listed fields visible — price_per_night in doc.fields, rating absent")
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 5, "fields": ["name"],
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "fields": ["price_per_night"],
                "source": "function check_fields(doc, params){ var f = doc.fields || {}; if (f.rating !== undefined) return false; return f.price_per_night !== undefined; }",
            }},
        })
        if s != 200 or r.get("total_hits", 0) == 0:
            failures.append(f"FL-1: expected > 0 results, got status={s} hits={r.get('total_hits', 0)}")
        self.log.info(f"FL-1: total_hits={r.get('total_hits', '?')}")

        # FL-2: fields: ["*"] — all stored fields visible (P1)
        self.log.info("FL-2: fields=['*'] — all stored fields visible to UDF")
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 3, "fields": ["name"],
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "fields": ["*"],
                "source": "function check_all(doc, params){ var f = doc.fields || {}; return f.price_per_night !== undefined && f.rating !== undefined && f.cancellationPolicy !== undefined; }",
            }},
        })
        if s != 200 or r.get("total_hits", 0) == 0:
            failures.append(f"FL-2: expected some docs with all 3 fields, got status={s} hits={r.get('total_hits', 0)}")
        self.log.info(f"FL-2: total_hits={r.get('total_hits', '?')}")

        # FL-3: fields not specified — doc.fields is undefined/empty (P1)
        self.log.info("FL-3: no fields specified — doc.fields is undefined/empty, all docs pass")
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 5,
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "source": "function check_empty(doc, params){ return (doc.fields === undefined || Object.keys(doc.fields || {}).length === 0); }",
            }},
        })
        if s != 200 or r.get("total_hits", 0) == 0:
            failures.append(f"FL-3: expected all hotels to pass when fields absent, got status={s} hits={r.get('total_hits', 0)}")
        self.log.info(f"FL-3: total_hits={r.get('total_hits', '?')}")

        # FL-4: Indexed-but-not-stored field → undefined in UDF (P1)
        self.log.info("FL-4: indexed-but-not-stored field is undefined in UDF — all docs pass")
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 5,
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "fields": ["indexed_only"],
                "source": "function check_not_stored(doc, params){ var f = doc.fields || {}; return f.indexed_only === undefined; }",
            }},
        })
        if s != 200 or r.get("total_hits", 0) == 0:
            failures.append(f"FL-4: all docs should pass (indexed_only not stored → undefined), got status={s} hits={r.get('total_hits', 0)}")
        self.log.info(f"FL-4: total_hits={r.get('total_hits', '?')}")

        # FL-5: Non-existent field → no error, just undefined (P1)
        self.log.info("FL-5: non-existent field — doc.fields value is undefined, no error thrown")
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 5,
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "fields": ["this_field_does_not_exist"],
                "source": "function check_missing(doc, params){ var f = doc.fields || {}; return f.this_field_does_not_exist === undefined; }",
            }},
        })
        if s != 200 or r.get("total_hits", 0) == 0:
            failures.append(f"FL-5: all docs pass when field missing, got status={s} hits={r.get('total_hits', 0)}")
        self.log.info(f"FL-5: total_hits={r.get('total_hits', '?')}")

        # FL-6: UDF fields vs SearchRequest.fields are independent (P1)
        self.log.info("FL-6: UDF fields and SearchRequest.fields are independent — price_per_night must not appear in response")
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 10, "fields": ["name", "rating"],
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "fields": ["price_per_night"],
                "params": {"min": 150},
                "source": "function price_gate(doc, params){ var f = doc.fields || {}; return (f.price_per_night || 0) >= params.min; }",
            }},
        })
        if s != 200:
            failures.append(f"FL-6: query failed status={s}")
        elif r.get("total_hits", 0) == 0:
            failures.append("FL-6: expected some hits")
        else:
            leaked = [h["id"] for h in r.get("hits", []) if "price_per_night" in h.get("fields", {})]
            if leaked:
                failures.append(f"FL-6: price_per_night appeared in response fields for {leaked}")
        self.log.info(f"FL-6: total_hits={r.get('total_hits', '?')}")

        # FL-7: Multi-valued amenities field exposed as array (P1)
        self.log.info("FL-7: multi-valued field amenities exposed as JavaScript array in UDF")
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 5, "fields": ["name", "amenities"],
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "fields": ["amenities"],
                "source": "function check_array(doc, params){ var f = doc.fields || {}; return Array.isArray(f.amenities); }",
            }},
        })
        if s != 200 or r.get("total_hits", 0) == 0:
            failures.append(f"FL-7: expected hotels with array amenities, got status={s} hits={r.get('total_hits', 0)}")
        self.log.info(f"FL-7: total_hits={r.get('total_hits', '?')}")

        if failures:
            self.fail("test_udf_fl failed sub-cases:\n" + "\n".join(failures))
        self.log.info("test_udf_fl PASSED — all FL sub-cases passed")

    # ── P — Params Tests ───────────────────────────────────────────────────────

    def test_udf_params(self):
        # P-1..P-4: params tests
        failures = []
        gt = self._udf_gt

        # P-1: Params are accessible in function (P1)
        self.log.info("P-1: params accessible in UDF — threshold=5 and category='hotel' passed via params")
        hits, err = self._udf._udf_query({
            "size": 20,
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "params": {"threshold": 5, "category": "hotel"},
                "source": "function check_params(doc, params){ return params.threshold === 5 && params.category === 'hotel'; }",
            }},
        })
        if err or hits != gt["p_1_params_check"]:
            failures.append(f"P-1: expected {gt['p_1_params_check']} got {hits}, err={err}")
        self.log.info(f"P-1: hits={hits}")

        # P-2: Nested object params (P2)
        self.log.info("P-2: nested object params — constraints.min_price=100 and constraints.min_rating=4.0")
        hits, err = self._udf._udf_query({
            "size": 20,
            "fields": ["name", "price_per_night", "rating"],
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "fields": ["price_per_night", "rating"],
                "params": {"constraints": {"min_price": 100, "min_rating": 4.0}, "label": "premium"},
                "source": "function nested_params(doc, params){ var f = doc.fields || {}; var c = params.constraints || {}; return (f.price_per_night || 0) >= c.min_price && (f.rating || 0) >= c.min_rating; }",
            }},
        })
        if err or hits != gt["p_2_nested_params"]:
            failures.append(f"P-2: expected {gt['p_2_nested_params']} got {hits}, err={err}")
        self.log.info(f"P-2: hits={hits}")

        # P-3: Array params (P2)
        self.log.info("P-3: array params — allowed_policies=['free','partial'] passed via params")
        hits, err = self._udf._udf_query({
            "size": 20,
            "fields": ["name", "cancellationPolicy"],
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "fields": ["cancellationPolicy"],
                "params": {"allowed_policies": ["free", "partial"]},
                "source": "function policy_whitelist(doc, params){ var f = doc.fields || {}; var allowed = params.allowed_policies || []; return allowed.indexOf(f.cancellationPolicy) !== -1; }",
            }},
        })
        if err or hits != gt["p_3_array_params"]:
            failures.append(f"P-3: expected {gt['p_3_array_params']} got {hits}, err={err}")
        self.log.info(f"P-3: hits={hits}")

        # P-4: Params not specified — params is empty/undefined (P1)
        self.log.info("P-4: no params specified — UDF handles undefined params gracefully")
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 5,
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "source": "function no_params(doc, params){ var threshold = (params && params.min) ? params.min : 100; return true; }",
            }},
        })
        if s != 200 or r.get("total_hits", 0) == 0:
            failures.append(f"P-4: expected hits even with no params, got status={s} hits={r.get('total_hits', 0)}")
        self.log.info(f"P-4: total_hits={r.get('total_hits', '?')}")

        if failures:
            self.fail("test_udf_params failed sub-cases:\n" + "\n".join(failures))
        self.log.info("test_udf_params PASSED — all P sub-cases passed")

    # ── JS — JavaScript Contract Tests ────────────────────────────────────────

    def test_udf_js(self):
        # JS-1..JS-6: JavaScript contract tests
        failures = []
        gt = self._udf_gt

        # JS-1: doc._id accessible and is a non-empty string (P1)
        self.log.info("JS-1: doc._id accessible — typeof doc._id === 'string' and length > 0 for all docs")
        hits, err = self._udf._udf_query({
            "size": 20,
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "source": "function check_id(doc, params){ return typeof doc._id === 'string' && doc._id.length > 0; }",
            }},
        })
        if err or hits != gt["js_1_id_check"]:
            failures.append(f"JS-1: expected {gt['js_1_id_check']} got {hits}, err={err}")
        self.log.info(f"JS-1: hits={hits}")

        # JS-2: doc.score accessible and positive in custom_score (P0)
        self.log.info("JS-2: doc.score accessible in custom_score — must be a number (not -999 sentinel)")
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 5,
            "query": {"custom_score": {
                "query": {"match": "hotel", "field": "type"},
                "source": "function check_score(doc, params){ if (typeof doc.score !== 'number') return -999; return doc.score; }",
            }},
        })
        if s != 200:
            failures.append(f"JS-2: query failed status={s}")
        else:
            sentinel = [h["id"] for h in r.get("hits", []) if abs(h.get("score", 0) - (-999)) < 1]
            if sentinel:
                failures.append(f"JS-2: doc.score is not a number for {sentinel}")
        self.log.info(f"JS-2: total_hits={r.get('total_hits', '?')}")

        # JS-3: doc.score accessible in custom_filter (P2)
        self.log.info("JS-3: doc.score accessible in custom_filter — filter doc.score > 0")
        hits, err = self._udf._udf_query({
            "size": 20,
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "source": "function filter_by_score(doc, params){ return doc.score > 0; }",
            }},
        })
        if err or hits != gt["js_3_score_positive"]:
            failures.append(f"JS-3: expected {gt['js_3_score_positive']} got {hits}, err={err}")
        self.log.info(f"JS-3: hits={hits}")

        # JS-4: Arrow function source is rejected (P0 — spec requirement)
        self.log.info("JS-4: arrow function source must be rejected — spec requires named function declaration")
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 5,
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "source": "(doc, params) => true",
            }},
        })
        if s == 200:
            failures.append("JS-4: expected compile error for arrow function, got 200")
        self.log.info(f"JS-4: arrow function status={s}")

        # JS-5: Multiple functions in source — helper function pattern (P2)
        self.log.info("JS-5: multiple functions in source — helper + main function pattern, no 500 error")
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 5,
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "source": "function helper(x){ return x > 0; } function main_filter(doc, params){ return helper(doc.score); }",
            }},
        })
        if s == 500:
            failures.append(f"JS-5: server error with multi-function source: {r}")
        self.log.info(f"JS-5: multi-function source: status={s} total_hits={r.get('total_hits')}")

        # JS-6: JS Math functions available (P2)
        self.log.info("JS-6: JS Math functions available — Math.min used in scoring, scores must be in [0,1]")
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 5, "fields": ["distanceFromCenterKm"],
            "query": {"custom_score": {
                "query": {"match": "hotel", "field": "type"},
                "fields": ["distanceFromCenterKm"],
                "source": "function use_math(doc, params){ var f = doc.fields || {}; var d = f.distanceFromCenterKm || 1; return 1.0 - Math.min(d / 5.0, 1.0); }",
            }},
        })
        if s != 200:
            failures.append(f"JS-6: query failed status={s}")
        elif r.get("total_hits", 0) == 0:
            failures.append("JS-6: expected hits")
        else:
            out_of_range = [
                f"{h['id']}: score={h.get('score', -1):.4f}"
                for h in r.get("hits", [])
                if not (0.0 <= h.get("score", -1) <= 1.0)
            ]
            if out_of_range:
                failures.append(f"JS-6: scores out of [0,1] range: {out_of_range}")
        self.log.info(f"JS-6: total_hits={r.get('total_hits', '?')}")

        if failures:
            self.fail("test_udf_js failed sub-cases:\n" + "\n".join(failures))
        self.log.info("test_udf_js PASSED — all JS sub-cases passed")

    # ── QC — Query Composition Tests ──────────────────────────────────────────

    def test_udf_qc_basic(self):
        # QC-1..QC-5: basic query composition tests
        failures = []
        gt = self._udf_gt

        # QC-1: custom_filter wrapping match_phrase inner query (P1)
        self.log.info("QC-1: custom_filter wrapping match_phrase 'luxury hotel' — observational")
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 10, "fields": ["name", "description"],
            "query": {"custom_filter": {
                "query": {"match_phrase": "luxury hotel", "field": "description"},
                "fields": ["type"],
                "source": "function all_pass(doc, params){ return true; }",
            }},
        })
        if s != 200:
            failures.append(f"QC-1: query failed status={s}")
        self.log.info(f"QC-1: match_phrase 'luxury hotel' total_hits={r.get('total_hits')}")

        # QC-2: custom_filter wrapping bool (must/conjuncts) inner query (P1)
        self.log.info("QC-2: custom_filter wrapping bool conjuncts — Paris hotels not NON_REFUNDABLE")
        hits, err = self._udf._udf_query({
            "size": 20, "fields": ["name", "city"],
            "query": {"custom_filter": {
                "query": {"must": {"conjuncts": [
                    {"match": "hotel", "field": "type"},
                    {"field": "city", "match": "Paris"},
                ]}},
                "fields": ["cancellationPolicy"],
                "source": "function not_nonrefund(doc, params){ var f = doc.fields || {}; return f.cancellationPolicy !== 'non_refundable'; }",
            }},
        })
        if err or hits != gt["qc_2_paris_not_nonrefund"]:
            failures.append(f"QC-2: expected {gt['qc_2_paris_not_nonrefund']} got {hits}, err={err}")
        self.log.info(f"QC-2: hits={hits}")

        # QC-3: custom_score wrapping bool query (P1)
        self.log.info("QC-3: custom_score wrapping bool query — Paris hotels scored by rating*2")
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 10, "fields": ["name", "rating"],
            "query": {"custom_score": {
                "query": {"must": {"conjuncts": [
                    {"match": "hotel", "field": "type"},
                    {"field": "city", "match": "Paris"},
                ]}},
                "fields": ["rating"],
                "source": "function rating_score(doc, params){ var f = doc.fields || {}; return (f.rating || 0) * 2; }",
            }},
        })
        if s != 200:
            failures.append(f"QC-3: query failed status={s}")
        elif not r.get("hits"):
            failures.append("QC-3: expected Paris hotel hits")
        else:
            all_ids = [h["id"] for h in r.get("hits", [])]
            if "hotel::paris_003" not in all_ids[:3]:
                failures.append(f"QC-3: paris_003 should be near top, got first 3: {all_ids[:3]}")
        self.log.info(f"QC-3: total_hits={r.get('total_hits', '?')}")

        # QC-4: custom_filter inside disjunction (P1)
        self.log.info("QC-4: custom_filter inside disjunction — breweries OR US country docs")
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 20, "fields": ["name", "type"],
            "query": {"should": {"disjuncts": [
                {"custom_filter": {
                    "query": {"match": "brewery", "field": "type"},
                    "fields": ["type"],
                    "source": "function brewery_only(doc, params){ var f = doc.fields || {}; return f.type === 'brewery'; }",
                }},
                {"field": "country", "match": "United States"},
            ]}},
        })
        if s != 200:
            failures.append(f"QC-4: query failed status={s}")
        elif r.get("total_hits", 0) < 5:
            failures.append(f"QC-4: expected at least 5 results (all breweries), got {r.get('total_hits', 0)}")
        self.log.info(f"QC-4: disjunction result total_hits={r.get('total_hits')}")

        # QC-5: custom_filter inside conjunction (must) (P1)
        self.log.info("QC-5: custom_filter inside conjunction — Paris hotels with FREE cancellation")
        hits, err = self._udf._udf_query({
            "size": 20, "fields": ["name", "type", "cancellationPolicy"],
            "query": {"must": {"conjuncts": [
                {"custom_filter": {
                    "query": {"match": "hotel", "field": "type"},
                    "fields": ["cancellationPolicy"],
                    "source": "function free_cancel(doc, params){ var f = doc.fields || {}; return f.cancellationPolicy === 'free'; }",
                }},
                {"field": "city", "match": "Paris"},
            ]}},
        })
        if err or hits != gt["qc_5_paris_free"]:
            failures.append(f"QC-5: expected {gt['qc_5_paris_free']} got {hits}, err={err}")
        self.log.info(f"QC-5: hits={hits}")

        if failures:
            self.fail("test_udf_qc_basic failed sub-cases:\n" + "\n".join(failures))
        self.log.info("test_udf_qc_basic PASSED — all QC basic sub-cases passed")

    def test_udf_qc_advanced(self):
        # QC-6..QC-9: advanced query composition tests
        failures = []
        gt = self._udf_gt

        # QC-6: custom_score wrapping custom_filter (chained) (P1)
        self.log.info("QC-6: custom_score wrapping custom_filter — available hotels scored cheapest-first")
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 20, "fields": ["name", "price_per_night"],
            "query": {"custom_score": {
                "query": {"custom_filter": {
                    "query": {"match": "hotel", "field": "type"},
                    "fields": ["available"],
                    "source": "function only_available(doc, params){ var f = doc.fields || {}; return f.available === true; }",
                }},
                "fields": ["price_per_night"],
                "source": "function cheap_first(doc, params){ var f = doc.fields || {}; var p = f.price_per_night || 9999; return 1000.0 / p; }",
            }},
        })
        if s != 200:
            failures.append(f"QC-6: query failed status={s}")
        else:
            total = r.get("total_hits", 0)
            if total != gt["qc_6_avail_hotels"]:
                failures.append(f"QC-6: expected {gt['qc_6_avail_hotels']} available hotels got {total}")
            else:
                ids = [h["id"] for h in r.get("hits", [])]
                if "hotel::paris_009" in ids and "hotel::london_003" in ids:
                    if ids.index("hotel::paris_009") >= ids.index("hotel::london_003"):
                        failures.append("QC-6: cheapest available hotel should rank highest")
        self.log.info(f"QC-6: total_hits={r.get('total_hits', '?')}")

        # QC-7: Nested custom_filter inside custom_filter (P2)
        self.log.info("QC-7: nested custom_filter — outer FREE filter wraps inner available filter")
        hits, err = self._udf._udf_query({
            "size": 10, "fields": ["name", "available", "cancellationPolicy"],
            "query": {"custom_filter": {
                "query": {"custom_filter": {
                    "query": {"match": "hotel", "field": "type"},
                    "fields": ["available"],
                    "source": "function avail_filter(doc, params){ var f = doc.fields || {}; return f.available === true; }",
                }},
                "fields": ["cancellationPolicy"],
                "source": "function free_filter(doc, params){ var f = doc.fields || {}; return f.cancellationPolicy === 'free'; }",
            }},
        })
        if err or hits != gt["qc_7_avail_and_free"]:
            failures.append(f"QC-7: expected {gt['qc_7_avail_and_free']} got {hits}, err={err}")
        self.log.info(f"QC-7: hits={hits}")

        # QC-8: custom_filter with numeric_range inner query (P1)
        self.log.info("QC-8: custom_filter with numeric_range — price 100-300 AND rating >= 4.0")
        hits, err = self._udf._udf_query({
            "size": 10, "fields": ["name", "price_per_night", "rating"],
            "query": {"custom_filter": {
                "query": {"field": "price_per_night", "min": 100.0, "max": 300.0,
                          "inclusive_min": True, "inclusive_max": True},
                "fields": ["rating"],
                "params": {"min_rating": 4.0},
                "source": "function rate_gate(doc, params){ var f = doc.fields || {}; return (f.rating || 0) >= params.min_rating; }",
            }},
        })
        if err or hits != gt["qc_8_price100_300_rating4"]:
            failures.append(f"QC-8: expected {gt['qc_8_price100_300_rating4']} got {hits}, err={err}")
        self.log.info(f"QC-8: hits={hits}")

        # QC-9: geo_distance inner query — skipped (location field not in test index) (P2)
        self.log.info("QC-9: custom_score with geo_distance — skipped (location field not in udf_test_index)")

        if failures:
            self.fail("test_udf_qc_advanced failed sub-cases:\n" + "\n".join(failures))
        self.log.info("test_udf_qc_advanced PASSED — all QC advanced sub-cases passed")

    # ── EH — Error Handling & Failure Semantics ───────────────────────────────

    def test_udf_eh_validation(self):
        # EH-1..EH-4 + EH-12: validation/compile errors
        failures = []

        # EH-1: Missing query field in custom_filter → HTTP 400 (P0)
        self.log.info("EH-1: missing 'query' field in custom_filter — expect HTTP 400")
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 5,
            "query": {"custom_filter": {"source": "function f(doc, params){ return true; }"}},
        })
        if s != 400:
            failures.append(f"EH-1: expected 400, got {s}")
        self.log.info(f"EH-1: status={s}")

        # EH-2: Missing source field in custom_filter → HTTP 400 (P0)
        self.log.info("EH-2: missing 'source' field in custom_filter — expect HTTP 400")
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 5,
            "query": {"custom_filter": {"query": {"match": "hotel", "field": "type"}}},
        })
        if s != 400:
            failures.append(f"EH-2: expected 400 for missing source, got {s}")
        self.log.info(f"EH-2: status={s}")

        # EH-3: Invalid JavaScript syntax → early compile failure (P0)
        self.log.info("EH-3: invalid JS syntax — expect early compile failure, non-200 status")
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 5,
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "source": "function f(doc, params) { return true",
            }},
        })
        if s == 200:
            failures.append(f"EH-3: expected compile error, got 200")
        self.log.info(f"EH-3: syntax error response status={s}")

        # EH-4: Missing named function declaration → error (P0)
        self.log.info("EH-4: bare return statement (no function declaration) — expect error")
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 5,
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "source": "return true;",
            }},
        })
        if s == 200:
            failures.append(f"EH-4: expected error for bare return, got 200")
        self.log.info(f"EH-4: bare return rejected with status={s}")

        # EH-12: Error response format when UDF disabled (P0)
        self.log.info("EH-12: error response format when UDF disabled — expect HTTP 400 with body")
        self._udf._disable_udf()
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "source": "function f(doc,params){return true;}",
            }},
        })
        self._udf._enable_udf()
        if s != 400:
            failures.append(f"EH-12: expected 400 when UDF disabled, got {s}")
        self.log.info(f"EH-12: disabled response body: {r}")

        if failures:
            self.fail("test_udf_eh_validation failed sub-cases:\n" + "\n".join(failures))
        self.log.info("test_udf_eh_validation PASSED — all EH validation sub-cases passed")

    def test_udf_eh_runtime(self):
        # EH-5..EH-11 + EH-13: runtime error semantics
        failures = []

        # EH-5: Runtime exception in custom_filter → hit dropped, query continues (P0)
        self.log.info("EH-5: runtime exception in UDF — hit dropped, query continues, 0 results expected")
        hits, err = self._udf._udf_query({
            "size": 20, "fields": ["name"],
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "source": "function throw_err(doc, params){ throw new Error('deliberate test error'); }",
            }},
        })
        if err:
            failures.append(f"EH-5: query itself must not fail (runtime errors are per-hit): {err}")
        elif hits != 0:
            failures.append(f"EH-5: all hits should be dropped on exception, got {hits}")
        self.log.info(f"EH-5: hits={hits}")

        # EH-6: Runtime exception in custom_score → fallback to base score (P0)
        self.log.info("EH-6: runtime exception in custom_score — fallback to base score, all hits preserved")
        _, base_r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 10, "fields": ["name"], "query": {"match": "hotel", "field": "type"},
        })
        base_total = base_r.get("total_hits", 0)
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 10, "fields": ["name"],
            "query": {"custom_score": {
                "query": {"match": "hotel", "field": "type"},
                "source": "function throw_score(doc, params){ throw new Error('score error'); }",
            }},
        })
        if s != 200:
            failures.append(f"EH-6: query must not fail on CS runtime exception: status={s}")
        elif r.get("total_hits", 0) != base_total:
            failures.append(f"EH-6: expected {base_total} hits with fallback scores, got {r.get('total_hits', 0)}")
        self.log.info(f"EH-6: total_hits={r.get('total_hits', '?')}")

        # EH-7: custom_filter returns non-boolean string — document behaviour (P1)
        self.log.info("EH-7: non-boolean string return from custom_filter — must not cause 500 error")
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 5,
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "source": "function str_return(doc, params){ return 'yes'; }",
            }},
        })
        if s == 500:
            failures.append(f"EH-7: must not return 500 for string return: {r}")
        self.log.info(f"EH-7: string return result: status={s} total_hits={r.get('total_hits')}")

        # EH-8: custom_filter returns null → hit dropped → 0 results (P1)
        self.log.info("EH-8: null return from custom_filter — hit dropped, expect 0 results")
        hits, err = self._udf._udf_query({
            "size": 5,
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "source": "function null_return(doc, params){ return null; }",
            }},
        })
        if err:
            failures.append(f"EH-8: query must not fail for null return: {err}")
        elif hits != 0:
            failures.append(f"EH-8: null return should drop all hits, got {hits}")
        self.log.info(f"EH-8: hits={hits}")

        # EH-9: custom_score returns non-numeric string → fallback to base score (P1)
        self.log.info("EH-9: non-numeric string score — fallback to base score expected")
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 5,
            "query": {"custom_score": {
                "query": {"match": "hotel", "field": "type"},
                "source": "function str_score(doc, params){ return 'high'; }",
            }},
        })
        if s != 200:
            failures.append(f"EH-9: expected fallback to base score, query failed: status={s}")
        self.log.info(f"EH-9: string score result: total_hits={r.get('total_hits')}")

        # EH-10: custom_score returns NaN → fallback to base score (P1)
        self.log.info("EH-10: NaN score return — fallback to base score expected")
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 5,
            "query": {"custom_score": {
                "query": {"match": "hotel", "field": "type"},
                "source": "function nan_score(doc, params){ return NaN; }",
            }},
        })
        if s != 200:
            failures.append(f"EH-10: expected fallback for NaN score, got status={s}")
        self.log.info(f"EH-10: NaN score result: total_hits={r.get('total_hits')}")

        # EH-11: custom_score returns Infinity — document behaviour (P2)
        self.log.info("EH-11: Infinity score — must not cause 500 error, observational")
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 5,
            "query": {"custom_score": {
                "query": {"match": "hotel", "field": "type"},
                "source": "function inf_score(doc, params){ return Infinity; }",
            }},
        })
        if s == 500:
            failures.append(f"EH-11: must not return 500 for Infinity score: {r}")
        self.log.info(f"EH-11: Infinity score result: status={s} total_hits={r.get('total_hits')}")

        # EH-13: Division by zero (Infinity from JS) handled gracefully (P2)
        self.log.info("EH-13: division by zero in UDF score — must not cause 500 error, observational")
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 5,
            "query": {"custom_score": {
                "query": {"match": "hotel", "field": "type"},
                "fields": ["price_per_night"],
                "source": "function div_by_price(doc, params){ var f = doc.fields || {}; var p = f.price_per_night || 0; return 1000 / p; }",
            }},
        })
        if s == 500:
            failures.append(f"EH-13: must not return 500 for div-by-zero: {r}")
        self.log.info(f"EH-13: div-by-zero result: status={s} total_hits={r.get('total_hits')}")

        if failures:
            self.fail("test_udf_eh_runtime failed sub-cases:\n" + "\n".join(failures))
        self.log.info("test_udf_eh_runtime PASSED — all EH runtime sub-cases passed")

    # ── SR — Security / Restriction Tests ─────────────────────────────────────

    def test_udf_sr_sandbox(self):
        # SR-1..SR-8: sandbox restrictions and state isolation
        failures = []

        # SR-1: No access to Node.js builtins (require/fs/process) (P1)
        self.log.info("SR-1: require() must not be accessible in V8 sandbox — expect 0 hits")
        hits, err = self._udf._udf_query({
            "size": 5,
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "source": "function try_require(doc, params){ try { var fs = require('fs'); return true; } catch(e) { return false; } }",
            }},
        })
        if err:
            failures.append(f"SR-1: query must not fail: {err}")
        elif hits != 0:
            failures.append(f"SR-1: require() must not be accessible, got {hits} hits")
        self.log.info(f"SR-1: hits={hits}")

        # SR-2: No access to process / env variables (P1)
        self.log.info("SR-2: process object must not be accessible in V8 sandbox — expect 0 hits")
        hits, err = self._udf._udf_query({
            "size": 5,
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "source": "function try_process(doc, params){ try { return typeof process !== 'undefined'; } catch(e) { return false; } }",
            }},
        })
        if err:
            failures.append(f"SR-2: query must not fail: {err}")
        elif hits != 0:
            failures.append(f"SR-2: process must not be accessible, got {hits} hits")
        self.log.info(f"SR-2: hits={hits}")

        # SR-3: Infinite loop times out — node must not hang (P0)
        self.log.info("SR-3: infinite loop in UDF — must time out, node must remain healthy")
        start = time.time()
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 5,
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "source": "function infinite_loop(doc, params){ while(true){} return true; }",
            }},
        })
        elapsed = time.time() - start
        if elapsed >= 60:
            failures.append(f"SR-3: FTS node appears hung (elapsed={elapsed:.1f}s)")
        if s == 500:
            failures.append(f"SR-3: must not return 500 for infinite loop: {r}")
        s2, r2 = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 5, "query": {"match": "hotel", "field": "type"},
        })
        if s2 != 200:
            failures.append(f"SR-3: FTS node unhealthy after infinite loop: {r2}")
        self.log.info(f"SR-3: infinite loop handled in {elapsed:.1f}s, status={s}")

        # SR-4: doc object limited to _id, score, fields (P1)
        self.log.info("SR-4: doc object shape restricted to _id, score, fields — no extra keys exposed")
        hits, err = self._udf._udf_query({
            "size": 5,
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "source": "function check_doc_shape(doc, params){ var keys = Object.keys(doc); var valid = ['_id','score','fields']; for(var k of keys){ if(valid.indexOf(k)===-1) return false; } return true; }",
            }},
        })
        if err:
            failures.append(f"SR-4: {err}")
        elif hits == 0:
            failures.append("SR-4: all hotel docs should pass doc-shape check")
        self.log.info(f"SR-4: hits={hits}")

        # SR-5: Global mutable state does NOT persist across requests (P0 — critical)
        self.log.info("SR-5: global state isolation — _flag set in request 1 must not be visible in request 2")
        h1, e1 = self._udf._udf_query({
            "size": 20,
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "source": "function set_flag(doc, params){ _flag = true; return true; }",
            }},
        })
        if e1:
            failures.append(f"SR-5 step1: {e1}")
        h2, e2 = self._udf._udf_query({
            "size": 20,
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "source": "function check_leak(doc, params){ return typeof _flag !== 'undefined'; }",
            }},
        })
        if e2:
            failures.append(f"SR-5 step2: {e2}")
        elif h2 > 0:
            failures.append(f"SR-5: SECURITY BUG — global state leaked across requests: h2={h2}")
        self.log.info(f"SR-5: h1={h1} h2={h2} (h2 must be 0)")

        # SR-6: Function constructor cannot escape sandbox (P0)
        self.log.info("SR-6: Function constructor sandbox escape must be blocked — expect 0 hits")
        hits, err = self._udf._udf_query({
            "size": 5,
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "source": "function try_func_ctor(doc, params){ try { var f = new Function('return typeof require'); return f() !== 'undefined'; } catch(e){ return false; } }",
            }},
        })
        if err:
            failures.append(f"SR-6: query must not fail: {err}")
        elif hits != 0:
            failures.append(f"SR-6: Function constructor sandbox escape must be blocked, got {hits}")
        self.log.info(f"SR-6: hits={hits}")

        # SR-7: eval is not available (P0)
        self.log.info("SR-7: eval must be blocked in V8 sandbox — expect 0 hits")
        hits, err = self._udf._udf_query({
            "size": 5,
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "source": "function try_eval(doc, params){ try { eval('1+1'); return true; } catch(e){ return false; } }",
            }},
        })
        if err:
            failures.append(f"SR-7: query must not fail: {err}")
        elif hits != 0:
            failures.append(f"SR-7: eval must be blocked in V8 sandbox, got {hits}")
        self.log.info(f"SR-7: hits={hits}")

        # SR-8: Prototype pollution does not affect subsequent queries (P1)
        self.log.info("SR-8: prototype pollution must not persist across requests")
        self._udf._udf_query({
            "size": 5,
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "source": "function pollute(doc, params){ try { Object.prototype.__injected__ = true; } catch(e){} return true; }",
            }},
        })
        hits, err = self._udf._udf_query({
            "size": 5,
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "source": "function check_polluted(doc, params){ return ({})['__injected__'] === true; }",
            }},
        })
        if err:
            failures.append(f"SR-8: {err}")
        elif hits > 0:
            failures.append(f"SR-8: prototype pollution leaked across requests: hits={hits}")
        self.log.info(f"SR-8: hits={hits} (must be 0)")

        if failures:
            self.fail("test_udf_sr_sandbox failed sub-cases:\n" + "\n".join(failures))
        self.log.info("test_udf_sr_sandbox PASSED — all SR sandbox sub-cases passed")

    def test_udf_sr_resilience(self):
        # SR-9..SR-13: resilience/resource tests
        failures = []

        # SR-9: ReDoS — catastrophic regex triggers execution timeout (P1)
        self.log.info("SR-9: ReDoS catastrophic regex — must time out, no 500 error, node stays healthy")
        start = time.time()
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 5,
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "source": "function redos(doc, params){ return /^(a+)+$/.test('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa!'); }",
            }},
        })
        elapsed = time.time() - start
        if elapsed >= 60:
            failures.append(f"SR-9: ReDoS caused hang ({elapsed:.1f}s)")
        if s == 500:
            failures.append(f"SR-9: must not return 500 for ReDoS: {r}")
        self.log.info(f"SR-9: ReDoS handled in {elapsed:.1f}s, status={s}")

        # SR-10: Error messages must not leak field values into response body (P1)
        self.log.info("SR-10: field values must not appear in error response body — data leak check")
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 5, "fields": ["name", "price_per_night"],
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "fields": ["price_per_night", "cancellationPolicy"],
                "source": "function leak_fields(doc, params){ throw new Error(JSON.stringify(doc.fields)); }",
            }},
        })
        resp_str = str(r)
        self.log.info(f"SR-10: error response body: {resp_str[:200]}")
        for val in ["150", "350", "FREE", "PARTIAL", "NON_REFUNDABLE"]:
            if val in resp_str:
                failures.append(f"SR-10: field value '{val}' leaked in error response")
        self.log.info(f"SR-10: data leak check complete for {len(resp_str)} char response")

        # SR-11: Memory bomb — large allocation bounded by V8 heap (P1)
        self.log.info("SR-11: memory bomb 50M array allocation — no hang, no 500, node remains healthy")
        start = time.time()
        s, r = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 5,
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "source": "function mem_bomb(doc, params){ try { var arr = new Array(50000000).fill('x'); return arr.length > 0; } catch(e){ return false; } }",
            }},
        })
        elapsed = time.time() - start
        if elapsed >= 60:
            failures.append(f"SR-11: memory bomb caused hang ({elapsed:.1f}s)")
        if s == 500:
            failures.append(f"SR-11: FTS must not crash on memory bomb: {r}")
        s2, _ = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 5, "query": {"match": "hotel", "field": "type"},
        })
        if s2 != 200:
            failures.append("SR-11: FTS node unhealthy after memory bomb")
        self.log.info(f"SR-11: elapsed={elapsed:.1f}s status={s}")

        # SR-12: WebAssembly not available in restricted context (P2)
        self.log.info("SR-12: WebAssembly must not be exposed in V8 sandbox — expect 0 hits")
        hits, err = self._udf._udf_query({
            "size": 5,
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "source": "function try_wasm(doc, params){ return typeof WebAssembly !== 'undefined'; }",
            }},
        })
        if err:
            failures.append(f"SR-12: query must not fail: {err}")
        elif hits != 0:
            failures.append(f"SR-12: WebAssembly must not be exposed in V8 sandbox, got {hits}")
        self.log.info(f"SR-12: hits={hits}")

        # SR-13: Worker pool starvation — slow UDFs must not block normal queries (P1)
        self.log.info("SR-13: slow UDF worker pool — normal queries must not be blocked by 5 slow UDF threads")
        slow_errors = []
        def run_slow():
            s_s, _ = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
                "size": 20,
                "query": {"custom_filter": {
                    "query": {"match": "hotel", "field": "type"},
                    "source": "function slow(doc, params){ var t = Date.now(); while(Date.now() - t < 200){} return true; }",
                }},
            })
            if s_s != 200:
                slow_errors.append(f"status={s_s}")
        threads = [threading.Thread(target=run_slow) for _ in range(5)]
        for t in threads:
            t.start()
        time.sleep(0.1)
        start = time.time()
        s_normal, r_normal = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 5, "query": {"match": "hotel", "field": "type"},
        })
        normal_elapsed = time.time() - start
        for t in threads:
            t.join(timeout=30)
        if s_normal != 200:
            failures.append(f"SR-13: normal query failed under UDF load: {r_normal}")
        if normal_elapsed >= 10:
            failures.append(f"SR-13: normal query took too long: {normal_elapsed:.1f}s")
        self.log.info(f"SR-13: normal query latency={normal_elapsed:.3f}s under {len(threads)} slow UDF threads")

        if failures:
            self.fail("test_udf_sr_resilience failed sub-cases:\n" + "\n".join(failures))
        self.log.info("test_udf_sr_resilience PASSED — all SR resilience sub-cases passed")

    # ── PF — Performance / Resource Tests ─────────────────────────────────────

    def test_udf_pf(self):
        # PF-1..PF-4: performance and resource tests
        failures = []

        # PF-1: custom_filter latency vs native query (P2)
        self.log.info("PF-1: latency comparison — custom_filter vs native query overhead (observational)")
        RUNS = 5
        native_times = []
        for _ in range(RUNS):
            t0 = time.time()
            self._udf._fts(self._udf._fts_index_path("query"), "POST", {
                "size": 10, "query": {"match": "hotel", "field": "type"},
            })
            native_times.append(time.time() - t0)
        avg_native = sum(native_times) / RUNS
        udf_times = []
        for _ in range(RUNS):
            t0 = time.time()
            self._udf._fts(self._udf._fts_index_path("query"), "POST", {
                "size": 10,
                "query": {"custom_filter": {
                    "query": {"match": "hotel", "field": "type"},
                    "source": "function f(doc,params){return true;}",
                }},
            })
            udf_times.append(time.time() - t0)
        avg_udf = sum(udf_times) / RUNS
        overhead_ms = (avg_udf - avg_native) * 1000
        self.log.info(f"PF-1: native={avg_native*1000:.1f}ms udf={avg_udf*1000:.1f}ms overhead={overhead_ms:.1f}ms")
        if avg_udf >= 1.0:
            failures.append(f"PF-1: UDF query took > 1s on 26 docs: {avg_udf:.3f}s")

        # PF-2: fields: ["*"] cost vs minimal field list (P2)
        self.log.info("PF-2: fields=['*'] vs minimal field list latency comparison (observational)")
        RUNS2 = 3
        def avg_latency(fields_list):
            times = []
            for _ in range(RUNS2):
                t0 = time.time()
                self._udf._fts(self._udf._fts_index_path("query"), "POST", {
                    "size": 10,
                    "query": {"custom_filter": {
                        "query": {"match": "hotel", "field": "type"},
                        "fields": fields_list,
                        "source": "function f(doc,params){return true;}",
                    }},
                })
                times.append(time.time() - t0)
            return sum(times) / RUNS2
        t_minimal = avg_latency(["price_per_night"])
        t_star = avg_latency(["*"])
        self.log.info(f"PF-2: fields=['price_per_night']={t_minimal*1000:.1f}ms fields=['*']={t_star*1000:.1f}ms")
        if t_star >= 2.0:
            failures.append(f"PF-2: fields=* took > 2s on 26 docs: {t_star:.3f}s")

        # PF-3: Script compilation cache — second request faster (P2)
        self.log.info("PF-3: compilation cache — second request with same source should be faster (observational)")
        unique_src = "function pf3_unique_abc(doc, params){ return true; }"
        t0 = time.time()
        self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 10,
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "source": unique_src,
            }},
        })
        t_first = time.time() - t0
        t0 = time.time()
        self._udf._fts(self._udf._fts_index_path("query"), "POST", {
            "size": 10,
            "query": {"custom_filter": {
                "query": {"match": "hotel", "field": "type"},
                "source": unique_src,
            }},
        })
        t_second = time.time() - t0
        self.log.info(f"PF-3: first={t_first*1000:.1f}ms second={t_second*1000:.1f}ms (expect second < first)")

        # PF-4: Concurrent UDF queries — worker pool saturation (P2)
        self.log.info("PF-4: concurrent UDF queries — 10 simultaneous custom_score queries must all succeed")
        results = []
        lock = threading.Lock()
        def run():
            s_r, _ = self._udf._fts(self._udf._fts_index_path("query"), "POST", {
                "size": 10,
                "query": {"custom_score": {
                    "query": {"match": "hotel", "field": "type"},
                    "source": "function s(doc,params){return doc.score;}",
                }},
            })
            with lock:
                results.append(s_r)
        threads = [threading.Thread(target=run) for _ in range(10)]
        t0 = time.time()
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)
        elapsed = time.time() - t0
        thread_failures = [s for s in results if s != 200]
        if thread_failures:
            failures.append(f"PF-4: {len(thread_failures)}/10 concurrent queries failed: {thread_failures}")
        self.log.info(f"PF-4: 10 concurrent UDF queries completed in {elapsed:.1f}s")

        if failures:
            self.fail("test_udf_pf failed sub-cases:\n" + "\n".join(failures))
        self.log.info("test_udf_pf PASSED — all PF sub-cases passed")
