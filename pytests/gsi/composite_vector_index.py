"""composite_vector_index.py: "This class test composite Vector indexes  for GSI"

__author__ = "Hemant Rajput"
__maintainer = "Hemant Rajput"
__email__ = "Hemant.Rajput@couchbase.com"
__git_user__ = "hrajput89"
__created_on__ = "19/06/24 03:27 pm"

"""
import datetime
import json
from copy import deepcopy
import string

import requests
from concurrent.futures import ThreadPoolExecutor

from requests.auth import HTTPBasicAuth
import huggingface_hub
if not hasattr(huggingface_hub, "cached_download"):
    huggingface_hub.cached_download = huggingface_hub.hf_hub_download
from sentence_transformers import SentenceTransformer

from couchbase_helper.documentgenerator import SDKDataLoader
from couchbase_helper.query_definitions import QueryDefinition, FULL_SCAN_ORDER_BY_TEMPLATE, \
    RANGE_SCAN_USE_INDEX_ORDER_BY_TEMPLATE, RANGE_SCAN_TEMPLATE, RANGE_SCAN_ORDER_BY_TEMPLATE
from gsi.base_gsi import BaseSecondaryIndexingTests
from membase.api.on_prem_rest_client import RestHelper, RestConnection
from scripts.multilevel_dict import MultilevelDict
from remote.remote_util import RemoteMachineShellConnection
from table_view import TableView
from memcached.helper.data_helper import MemcachedClientHelper
from threading import Thread
import random



class CompositeVectorIndex(BaseSecondaryIndexingTests):
    def setUp(self):
        super(CompositeVectorIndex, self).setUp()
        self.log.info("==============  CompositeVectorIndex setup has started ==============")
        self.encoder = SentenceTransformer(self.data_model, device="cpu")
        self.encoder.cpu()
        self.gsi_util_obj.set_encoder(encoder=self.encoder)
        self.namespaces = []
        self.multi_move = self.input.param("multi_move", False)
        self.build_phase = self.input.param("build_phase", "create")
        self.skip_default = self.input.param("skip_default", True)
        self.post_rebalance_action = self.input.param("post_rebalance_action", None)
        self.partitioned_index_action = self.input.param("partitioned_index_action", "rebalance_out")
        self.num_centroids = self.input.param("num_centroids", 256)
        self.target_process = self.input.param("target_process", "memcached")
        self.system_failure_scenario = self.input.param("system_failure_scenario", None)
        self.corrupt_file_type = self.input.param("corrupt_file_type", None)
        self.bhive_recovery_log_string = self.input.param("bhive_recovery_log_string", "bhiveSlice::doRecovery")
        self.num_indexes_batch = self.input.param("num_indexes_batch", 3)
        self.shard_based_rebalance = self.input.param("file_based_rebalance", False)

        self.log.info("==============  CompositeVectorIndex setup has ended ==============")

    def tearDown(self):
        self.log.info("==============  CompositeVectorIndex tearDown has started ==============")
        super(CompositeVectorIndex, self).tearDown()
        self.log.info("==============  CompositeVectorIndex tearDown has completed ==============")

    def suite_setUp(self):
        pass

    def suite_tearDown(self):
        pass

    def kill_indexer(self, index_node):
        remote = RemoteMachineShellConnection(index_node)
        remote.execute_command(command="pkill indexer")
        remote.disconnect()

    def kill_memcached(self, data_node):
        remote = RemoteMachineShellConnection(data_node)
        remote.execute_command(command="pkill memecached")
        remote.disconnect()
    def gen_table_view(self, query_stats_map, message="query stats"):
        table = TableView(self.log.info)
        table.set_headers(['Query', 'Recall', 'Accuracy'])
        for query in query_stats_map:
            table.add_row([query, query_stats_map[query][0]*100, query_stats_map[query][1]*100])
        table.display(message=message)

    def populate_vectors_in_xattr(self, bucket, scope):
        self.buckets = self.rest.get_buckets()
        bucket = bucket.split(":")[-1]
        metadata_bucket = "metadata_bucket"
        for iter_bucket in self.buckets:
            if iter_bucket.name == metadata_bucket:
                break
        else:
            self.bucket_params = self._create_bucket_params(server=self.master, size=100, bucket_storage="couchstore",
                                                            replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                            enable_replica_index=self.enable_replica_index,
                                                            eviction_policy=self.eviction_policy, lww=self.lww)
            self.cluster.create_standard_bucket(name=metadata_bucket, port=11222,
                                                bucket_params=self.bucket_params)

        eventing_node = self.get_nodes_from_services_map(service_type="eventing")
        eventing_endpoint = f"http://{eventing_node.ip}:8096"

        # Define the eventing function details
        function_name = f"GenVectors_{bucket.split(':')[-1]}_{scope}"
        app_code = """function OnUpdate(doc, meta, xattrs) {
                                  log("Doc created/updated", meta.id);
                                  // all user _xattrs in metadata KEY <= 16 chars, no long names
                                  if("descriptionVector" in doc && "colorRGBVector" in doc){
                                      try {
                                          couchbase.mutateIn(upsert_xattr, meta, [couchbase.MutateInSpec.insert("descVector", doc.descriptionVector, {"xattrs": true}), couchbase.MutateInSpec.insert("colorVector", doc.colorRGBVector, {"xattrs": true})]);
                                      } catch (e) {
                                          log("xattrs", e)
                                      }
                                      log("updated descVector and colorVector in " + meta.id + " of length " + doc.descriptionVector.length + " and " + doc.colorRGBVector.length);
                                  } else if("descriptionVector" in doc){
                                  try {
                                          couchbase.mutateIn(upsert_xattr, meta, [couchbase.MutateInSpec.insert("descVector", doc.descriptionVector, {"xattrs": true})]);
                                      } catch (e) {
                                          log("xattrs", e)
                                      }
                                      log("updated descVector in " + meta.id + " of length " + doc.descriptionVector.length);
                                  } else if("colorRGBVector" in doc){
                                  try {
                                          couchbase.mutateIn(upsert_xattr, meta, [couchbase.MutateInSpec.insert("colorVector", doc.colorRGBVector, {"xattrs": true})]);
                                      } catch (e) {
                                          log("xattrs", e)
                                      }
                                      log("updated colorVector in " + meta.id + " of length " + doc.colorRGBVector.length);
                                  }
                              }
                             """

        eventing_function = {
            "appname": function_name,
            "appcode": app_code,
            "depcfg": {
                "buckets": [{
                    "alias": "upsert_xattr",
                    "bucket_name": bucket,
                    "scope_name": scope,
                    "collection_name": "*",
                    "access": "rw"
                }],
                "source_bucket": bucket,
                "source_scope": scope,
                "source_collection": "*",
                "metadata_bucket": metadata_bucket,
                "metadata_scope": "_default",
                "metadata_collection": "_default",
            },
            "settings": {
                "dcp_stream_boundary": "everything",
                "deployment_status": True,
                "processing_status": True,
                "log_level": "INFO",
            },
            "function_scope": {
                "bucket": bucket,
                "scope": scope
            }
        }
        url = f"{eventing_endpoint}/api/v1/functions/{function_name}"
        headers = {'Content-Type': 'application/json'}

        # Send request to Couchbase REST API
        response = requests.post(url, data=json.dumps(eventing_function), headers=headers,
                                 auth=HTTPBasicAuth("Administrator", "password"))

        if response.status_code == 200:
            self.log.info(f"Eventing function '{function_name}' deployed successfully!")
        else:
            raise Exception(f"Failed to deploy eventing function: {response.status_code}, {response.text}")

    def test_composite_vector_sanity(self):
        self.single_distance_function = self.input.param("single_distance_function", False)
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        self.bhive_composite_comparison = self.input.param("bhive_composite_comparison", False)
        query_stats_map = {}
        if self.xattr_indexes:
            for namespace in self.namespaces:
                bucket, scope, _ = namespace.split('.')
                self.populate_vectors_in_xattr(bucket=bucket, scope=scope)


        simlarity_list = ["COSINE", "L2_SQUARED", "L2", "DOT", "EUCLIDEAN_SQUARED", "EUCLIDEAN"]
        query_comparison_list = []
        if self.bhive_composite_comparison:
            self.bhive_index = True
            similarity = random.choice(simlarity_list)
            simlarity_list = [similarity] * 2
        if self.single_distance_function:
            similarity = random.choice(simlarity_list)
            simlarity_list = [similarity]
        for similarity in simlarity_list:
            for namespace in self.namespaces:
                if self.bhive_index:
                    prefix = f'test_{similarity}_bhive'
                else:
                    prefix = f'test_{similarity}_composite'
                definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                          prefix=prefix,
                                                                          similarity=similarity,
                                                                          train_list=self.trainlist,
                                                                          scan_nprobes=self.scan_nprobes,
                                                                          array_indexes=False,
                                                                          xattr_indexes=self.xattr_indexes,
                                                                          limit=self.scan_limit, is_base64=self.base64,
                                                                          quantization_algo_color_vector=self.quantization_algo_color_vector,
                                                                          quantization_algo_description_vector=self.quantization_algo_description_vector,
                                                                          bhive_index=self.bhive_index, description_dimension=self.dimension,
                                                                         )
                create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions,
                                                                         namespace=namespace,
                                                                         bhive_index=self.bhive_index)
                select_queries = self.gsi_util_obj.get_select_queries(definition_list=definitions,
                                                                      namespace=namespace, limit=self.scan_limit)
                drop_queries = self.gsi_util_obj.get_drop_index_list(definition_list=definitions,
                                                                      namespace=namespace)
                self.gsi_util_obj.async_create_indexes(create_queries=create_queries, database=namespace, query_node=self.n1ql_node)
                self.wait_until_indexes_online(timeout=3600)
                self.log.info(f"select queries are {select_queries}")
                self.sleep(60)
                self.item_count_related_validations()
                self.validate_num_centroids_from_metadata()
                for query in select_queries:
                    # self.run_cbq_query(query=create)
                    if "DISTINCT" in query or "ANN_DISTANCE" not in query:
                        continue
                    query, recall, accuracy = self.validate_scans_for_recall_and_accuracy(select_query=query,
                                                                                          similarity=similarity,
                                                                                          scan_consitency=True)
                    query_stats_map[query] = [recall, accuracy]
                    # todo validate indexer metadata for indexes

                codebook_memory_per_index_map, aggregated_codebook_memory_utilization = self.get_per_index_codebook_memory_usage()
                self.log.info(f"codebook_memory_per_index_map : {codebook_memory_per_index_map}")
                self.log.info(f"aggregated_codebook_memory_utilization : {aggregated_codebook_memory_utilization}")

                for query in drop_queries:
                    self.run_cbq_query(query=query, server=self.n1ql_node)
                if self.bhive_composite_comparison:
                    self.bhive_index = False
                    query_stats_map_new = deepcopy(query_stats_map)
                    query_stats_map = {}
                    query_comparison_list.append(query_stats_map_new)
                    self.gen_table_view(query_stats_map=query_stats_map_new,
                                        message=f"Bhive is  {self.bhive_index}")

            # self.run_cbq_query(query=drop)
        if not self.bhive_composite_comparison:
            self.gen_table_view(query_stats_map=query_stats_map,
                                message=f"quantization value is {self.quantization_algo_description_vector}")
            query_comparison_list.append(query_stats_map)

        for query_stats_map in query_comparison_list:
            for query in query_stats_map:
                if "colorRGBVector" in query or "colorVector" in query or self.dimension == 4096:
                    continue
                self.assertGreaterEqual(query_stats_map[query][0] * 100, 70,
                                        f"recall for query {query} is less than threshold 70")
                #uncomment the below code snippet to do assertions for accuracy
                # self.assertGreaterEqual(query_stats_map[query][1] * 100, 70,
                #                         f"accuracy for query {query} is less than threshold 70")

        self.rest.delete_bucket(bucket='metadata_bucket')
        self.drop_index_node_resources_utilization_validations()

    def test_bhive_with_system_failures(self):
        try:
            self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
            self.set_persisted_snapshot_interval(interval=120 * 1000)
            query_stats_map = {}
            if self.xattr_indexes:
                for namespace in self.namespaces:
                    bucket, scope, _ = namespace.split('.')
                    self.populate_vectors_in_xattr(bucket=bucket, scope=scope)
            self.namespaces = ['test_bucket.test_scope_1.test_collection_1']
            similarity = "COSINE"
            namespace = self.namespaces[0]
            definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                        prefix=f'test_{similarity}',
                                                                        similarity=similarity, train_list=None,
                                                                        scan_nprobes=self.scan_nprobes,
                                                                        array_indexes=False,
                                                                        xattr_indexes=self.xattr_indexes,
                                                                        limit=self.scan_limit, is_base64=self.base64,
                                                                        quantization_algo_color_vector=self.quantization_algo_color_vector,
                                                                        quantization_algo_description_vector=self.quantization_algo_description_vector,
                                                                        bhive_index=True)
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions,
                                                                    namespace=namespace,
                                                                    bhive_index=self.bhive_index)
            select_queries = self.gsi_util_obj.get_select_queries(definition_list=definitions,
                                                                namespace=namespace, limit=self.scan_limit)
            metadata_dict_from_definitions = self.form_vector_index_metadata_dict_from_index_definitions(definitions)
            self.gsi_util_obj.async_create_indexes(create_queries=create_queries, database=namespace)
            self.wait_until_indexes_online(timeout=1800)
            metadata_dict_from_stats = self.get_vector_index_metadata_dict()
            if metadata_dict_from_stats != metadata_dict_from_definitions:
                raise AssertionError("Metadata dictionary from definitions and stats are not the same")
            self.validate_no_pending_mutations()
            self.compare_item_counts_between_kv_and_gsi()
            self.backstore_mainstore_check()
            self.validate_replica_indexes_item_counts()
            if self.system_failure_scenario:
                if self.system_failure_scenario == "run_stress_tool":
                    with ThreadPoolExecutor() as executor:
                        executor.submit(self.run_system_failure_scenario)
                else:
                    self.run_system_failure_scenario()
            if self.system_failure_scenario:
                self.sleep(300)
                self.log.info("Waiting for 5 minutes after running chaos action")
                for i in range(3):
                    definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                        prefix=f'test_{similarity}_system_failure_{i}',
                                                                        similarity=similarity, train_list=None,
                                                                        scan_nprobes=self.scan_nprobes,
                                                                        array_indexes=False,
                                                                        xattr_indexes=self.xattr_indexes,
                                                                        limit=self.scan_limit, is_base64=self.base64,
                                                                        quantization_algo_color_vector=self.quantization_algo_color_vector,
                                                                        quantization_algo_description_vector=self.quantization_algo_description_vector,
                                                                        bhive_index=self.bhive_index)
                    create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions,
                                                                        namespace=namespace,
                                                                        bhive_index=self.bhive_index,
                                                                        num_replica=1)
                try:
                    self.gsi_util_obj.async_create_indexes(create_queries=[create_queries[1]], database=namespace)
                except Exception as e:
                    self.log.info(f"Error creating index during system failure scenario: Ignoring this. {e}")
                self.sleep(120)
                self.cleanup_post_failure_scenario()
                self.sleep(300)
            self.wait_until_indexes_online(timeout=1800)
            self.validate_no_pending_mutations()
            self.sleep(120)
            self.compare_item_counts_between_kv_and_gsi()
            self.backstore_mainstore_check()
            self.validate_replica_indexes_item_counts()
            try:
                self.gsi_util_obj.async_create_indexes(create_queries=[create_queries[-1]], database=namespace)
            except Exception as e:
                self.log.info(f"Error creating index after cleaning up system failure: {e}")
                raise e
            for query in select_queries:
                if "DISTINCT" in query or "ANN_DISTANCE" not in query:
                    continue
                # TODO uncomment after limit logic is fixed.
                # query, recall, accuracy = self.validate_scans_for_recall_and_accuracy(select_query=query,
                #                                                                     similarity=similarity,
                #                                                                     scan_consitency=True,
                #                                                                     variable_limit=True)
                # query_stats_map[query] = [recall, accuracy]
            codebook_memory_per_index_map, aggregated_codebook_memory_utilization = self.get_per_index_codebook_memory_usage()
            self.log.info(f"codebook_memory_per_index_map : {codebook_memory_per_index_map}")
            self.log.info(f"aggregated_codebook_memory_utilization : {aggregated_codebook_memory_utilization}")
            self.drop_all_indexes()
            self.sleep(120)
            self.check_storage_directory_cleaned_up()
            # TODO uncomment after https://jira.issues.couchbase.com/browse/MB-65934 is fixed
            # if not self.validate_memory_released():
            #     raise AssertionError("Memory not released despite dropping all the indexes")
            if not self.validate_cpu_normalized():
                raise AssertionError("CPU not normalized despite dropping all the indexes")
            # self.gen_table_view(query_stats_map=query_stats_map,
                                # message=f"quantization value is {self.quantization_algo_description_vector}")
             # TODO uncomment after limit logic is fixed.
            # for query in query_stats_map:
            #     self.assertGreaterEqual(query_stats_map[query][0] * 100, 70,
            #                             f"recall for query {query} is less than threshold 70")

            self.rest.delete_bucket(bucket='metadata_bucket')
        finally:
            self.cleanup_post_failure_scenario()
            self.drop_all_indexes()

    def test_bhive_scan_inline_filtering_num_rows_filtered_num_rows_scanned(self):
        try:
            self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
            query_stats_map = {}
            if self.xattr_indexes:
                for namespace in self.namespaces:
                    bucket, scope, _ = namespace.split('.')
                    self.populate_vectors_in_xattr(bucket=bucket, scope=scope)
            self.namespaces = ['test_bucket.test_scope_1.test_collection_1']
            similarity = "COSINE"
            namespace = self.namespaces[0]
            definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                        prefix=f'test_{similarity}',
                                                                        similarity=similarity, train_list=None,
                                                                        scan_nprobes=self.scan_nprobes,
                                                                        array_indexes=False,
                                                                        xattr_indexes=self.xattr_indexes,
                                                                        limit=self.scan_limit, is_base64=self.base64,
                                                                        quantization_algo_color_vector=self.quantization_algo_color_vector,
                                                                        quantization_algo_description_vector=self.quantization_algo_description_vector,
                                                                        bhive_index=self.bhive_index)
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions,
                                                                    namespace=namespace,
                                                                    bhive_index=self.bhive_index)
            select_queries = self.gsi_util_obj.get_select_queries(definition_list=definitions,
                                                                namespace=namespace, limit=self.scan_limit)
            metadata_dict_from_definitions = self.form_vector_index_metadata_dict_from_index_definitions(definitions)
            self.gsi_util_obj.async_create_indexes(create_queries=create_queries, database=namespace)
            self.wait_until_indexes_online(timeout=1800)
            query_index_map = self.get_queries_with_inline_filters(select_queries)
            namespace_colon = namespace.replace(".", ":")
            query_inline_filter_map = {f"default:{namespace_colon}:{index}": queries for index, queries in query_index_map.items()}
            metadata_dict_from_stats = self.get_vector_index_metadata_dict()
            if metadata_dict_from_stats != metadata_dict_from_definitions:
                raise AssertionError("Metadata dictionary from definitions and stats are not the same")
            self.validate_no_pending_mutations()
            self.compare_item_counts_between_kv_and_gsi()
            self.backstore_mainstore_check()
            self.validate_replica_indexes_item_counts()
            for index in query_inline_filter_map:
                self.run_cbq_query(query=query_inline_filter_map[index])
            self.sleep(120)
            index_stats = self.get_index_stats(perNode=True)
            for node in index_stats:
                for bucket_path in index_stats[node]:
                    for index in index_stats[node][bucket_path]:
                        index_path = f"{bucket_path}:{index}"
                        index_path = index_path.replace(".", ".")
                        if index_path in query_inline_filter_map:
                            num_rows_scanned = index_stats[node][bucket_path][index]["num_rows_scanned"]
                            self.log.info(f"num_rows_scanned for {index_path} on {node} is {num_rows_scanned}")
                            num_rows_filtered = index_stats[node][bucket_path][index]["num_rows_filtered"]
                            self.log.info(f"num_rows_filtered for {index_path} on {node} is {num_rows_filtered}")
                            if num_rows_filtered == 0:
                                raise AssertionError(f"num_rows_filtered for {index_path} on {node} is 0")
                            if num_rows_scanned == 0:
                                raise AssertionError(f"num_rows_scanned for {index_path} on {node} is 0")
            codebook_memory_per_index_map, aggregated_codebook_memory_utilization = self.get_per_index_codebook_memory_usage()
            self.log.info(f"codebook_memory_per_index_map : {codebook_memory_per_index_map}")
            self.log.info(f"aggregated_codebook_memory_utilization : {aggregated_codebook_memory_utilization}")
            self.drop_all_indexes()
            self.sleep(120)
            self.check_storage_directory_cleaned_up()
            # TODO uncomment after https://jira.issues.couchbase.com/browse/MB-65934 is fixed
            # if not self.validate_memory_released():
            #     raise AssertionError("Memory not released despite dropping all the indexes")
            if not self.validate_cpu_normalized():
                raise AssertionError("CPU not normalized despite dropping all the indexes")
            # self.gen_table_view(query_stats_map=query_stats_map,
                                # message=f"quantization value is {self.quantization_algo_description_vector}")
             # TODO uncomment after limit logic is fixed.
            # for query in query_stats_map:
            #     self.assertGreaterEqual(query_stats_map[query][0] * 100, 70,
            #                             f"recall for query {query} is less than threshold 70")

            self.rest.delete_bucket(bucket='metadata_bucket')
        finally:
            self.drop_all_indexes()

    def test_create_index_negative_scenarios(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)

        collection_namespace = self.namespaces[0]
        # indexes with more than one vector field
        index_gen_1 = QueryDefinition(index_name='colorRGBVector_1', is_base64=self.base64,
                                      index_fields=['colorRGBVector VECTOR', 'descriptionVector VECTOR'],
                                      dimension=3, description="IVF,PQ3x8", similarity="L2_SQUARED")
        try:
            query = index_gen_1.generate_index_create_query(namespace=collection_namespace)
            self.run_cbq_query(query=query, server=self.n1ql_node)
        except Exception as err:
            err_msg = 'Cannot have more than one vector index key'
            self.assertTrue(err_msg in str(err), f"Index with more than one vector field is created: {err}")

        # indexes with include clause for a vector field
        index_gen_2 = QueryDefinition(index_name='colorRGBVector_2', is_base64=self.base64,
                                      index_fields=['colorRGBVector VECTOR INCLUDE MISSING DESC', 'description'],
                                      dimension=3, description="IVF,PQ3x8", similarity="L2_SQUARED")
        try:
            query = index_gen_2.generate_index_create_query(namespace=collection_namespace)
            self.run_cbq_query(query=query, server=self.n1ql_node)
        except Exception as err:
            err_msg = 'INCLUDE MISSING'
            self.assertTrue(err_msg in str(err), f"Index with INCLUDE clause is created: {err}")

        # indexes with empty collection defer build false
        scope = '_default'
        collection = '_default'

        self.collection_rest.create_scope_collection(bucket=self.test_bucket, scope=scope, collection=collection)
        collection_namespace = f"default:{self.test_bucket}.{scope}.{collection}"
        index_gen_3 = QueryDefinition(index_name='colorRGBVector_3', is_base64=self.base64,
                                      index_fields=['colorRGBVector VECTOR', 'description'],
                                      dimension=3, description="IVF,PQ3x8", similarity="L2_SQUARED")
        try:
            query = index_gen_3.generate_index_create_query(namespace=collection_namespace)
            self.run_cbq_query(query=query, server=self.n1ql_node)
        except Exception as err:
            err_msg = 'ErrTraining: InvalidTrainListSize: The number of documents: 0 in keyspace:'
            self.assertTrue(err_msg in str(err), f"Index with INCLUDE clause is created: {err}")

        # indexes with empty collection defer build true
        collection_namespace = f"default:{self.test_bucket}.{scope}.{collection}"
        index_gen_3 = QueryDefinition(index_name='colorRGBVector_4', is_base64=self.base64,
                                      index_fields=['colorRGBVector VECTOR', 'description'],
                                      dimension=3, description="IVF,PQ3x8", similarity="L2_SQUARED")

        query = index_gen_3.generate_index_create_query(namespace=collection_namespace, defer_build=True)
        self.run_cbq_query(query=query, server=self.n1ql_node)
        self.assertEqual(len(self.index_rest.get_indexer_metadata()['status']), 1, 'defer true index not created')

        # indexes with distinct clause
        index_gen_4 = QueryDefinition(index_name='colorRGBVector_2', is_base64=self.base64,
                                      index_fields=['colorRGBVector VECTOR', 'DISTINCT description'],
                                      dimension=3, description="IVF,PQ3x8", similarity="L2_SQUARED")
        try:
            query = index_gen_4.generate_index_create_query(namespace=collection_namespace)
            self.run_cbq_query(query=query, server=self.n1ql_node)
        except Exception as err:
            err_msg = 'Vector index with any field having array expression is currently not supported'
            self.assertTrue(err_msg in str(err), f"Index with INCLUDE clause is created: {err}")

        # indexes with all clause vector field
        index_gen_5 = QueryDefinition(index_name='colorRGBVector_2', is_base64=self.base64,
                                      index_fields=['ALL colorRGBVector VECTOR', 'description'],
                                      dimension=3, description="IVF,PQ3x8", similarity="L2_SQUARED")
        try:
            query = index_gen_5.generate_index_create_query(namespace=collection_namespace)
            self.run_cbq_query(query=query, server=self.n1ql_node)
        except Exception as err:
            err_msg = 'Array Index using Vector Index Key(all (`colorRGBVector`)) is not allowed.'
            self.assertTrue(err_msg in str(err), f"Index with ALL clause is created: {err}")

        # indexes with all clause scalar field
        index_gen_6 = QueryDefinition(index_name='colorRGBVector_2', is_base64=self.base64,
                                      index_fields=['colorRGBVector VECTOR', 'ALL evaluation'],
                                      dimension=3, description="IVF,PQ3x8", similarity="L2_SQUARED")
        try:
            query = index_gen_6.generate_index_create_query(namespace=collection_namespace)
            self.run_cbq_query(query=query, server=self.n1ql_node)
        except Exception as err:
            err_msg = 'Vector index with any field having array expression is currently not supported'
            self.assertTrue(err_msg in str(err), f"Index with ALL clause is created: {err}")

    def test_build_index_scenarios(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        collection_namespace = self.namespaces[0]

        # build a scalar and vector index sequentially
        scalar_idx = QueryDefinition(index_name='scalar', index_fields=['color'])
        vector_idx = QueryDefinition(index_name='vector', index_fields=['colorRGBVector VECTOR'], dimension=3,
                                     description="IVF,PQ3x8", similarity="L2_SQUARED", is_base64=self.base64)

        for idx in [scalar_idx, vector_idx]:
            query = idx.generate_index_create_query(namespace=collection_namespace, defer_build=True)
            self.run_cbq_query(query=query, server=self.n1ql_node)

        for idx in ['scalar', 'vector']:
            query = f"BUILD INDEX ON {collection_namespace}({idx}) USING GSI "
            self.run_cbq_query(query=query, server=self.n1ql_node)
            self.sleep(5)

        self.item_count_related_validations()

        meta_data = self.index_rest.get_indexer_metadata()['status']
        self.assertEqual(len(meta_data), 2, f'indexes are not built metadata {meta_data}')

        self.drop_index_node_resources_utilization_validations()

    def test_build_idx_less_than_required_centroids(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)

        collection_namespace = self.namespaces[0]

        vector_idx = QueryDefinition(index_name='vector', index_fields=['colorRGBVector VECTOR'], dimension=3,
                                     description="IVF204800,PQ3x8", similarity="L2_SQUARED",
                                     bhive_index=self.bhive_index, is_base64=self.base64)
        query = vector_idx.generate_index_create_query(namespace=collection_namespace, defer_build=True,
                                                       bhive_index=self.bhive_index)

        self.run_cbq_query(query=query, server=self.n1ql_node)
        # for the scenario where num docs less than centroids
        try:
            build_query = f"BUILD INDEX ON {collection_namespace}(vector) USING GSI "
            self.run_cbq_query(query=build_query, server=self.n1ql_node)
        except Exception as err:
            err_msg = 'are less than the minimum number of documents:'
            self.assertTrue(err_msg in str(err), f"Index with less docs are created: {err}")
        else:
            raise Exception("index has been built with less no of centroids")

    def test_kill_index_process_during_training(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        collection_namespace = self.namespaces[0]

        vector_idx = QueryDefinition(index_name='vector', index_fields=['descriptionVector VECTOR'], dimension=384,
                                     description="IVF,PQ32x8", similarity="L2_SQUARED", is_base64=self.base64,
                                     bhive_index=self.bhive_index)
        query = vector_idx.generate_index_create_query(namespace=collection_namespace, defer_build=False,
                                                       bhive_index=self.bhive_index)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        timeout = 0
        with ThreadPoolExecutor() as executor:
            executor.submit(self.run_cbq_query, query=query)
            self.sleep(5)
            while timeout < 360:
                index_state = self.index_rest.get_indexer_metadata()['status'][0]['status']
                if index_state == "Training":
                    break
                self.sleep(1)
                timeout = timeout + 1
            for node in index_nodes:
                self._kill_all_processes_index(server=node)
        self.sleep(10)
        self.item_count_related_validations()
        index_metadata = self.index_rest.get_indexer_metadata()['status']
        for idx in index_metadata:
            self.assertEqual("Ready", idx['status'], 'index has been errored out')
        self.drop_index_node_resources_utilization_validations()

    def test_concurrent_vector_index_builds(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)

        # build a scalar and vector index parallely on different namespaces

        index_build_list = []
        for item, namespace in enumerate(self.namespaces):
            idx_name = f'idx_{item}'
            scalar_idx = QueryDefinition(index_name=idx_name + 'scalar', index_fields=['color'])
            vector_idx = QueryDefinition(index_name=idx_name + 'vector', index_fields=['colorRGBVector VECTOR'],
                                         dimension=3, is_base64=self.base64,
                                         description="IVF,PQ3x8", similarity="L2_SQUARED")
            query1 = scalar_idx.generate_index_create_query(namespace=namespace, defer_build=True)
            query2 = vector_idx.generate_index_create_query(namespace=namespace, defer_build=True)
            self.run_cbq_query(query=query1, server=self.n1ql_node)
            self.run_cbq_query(query=query2, server=self.n1ql_node)
            build_query_1 = scalar_idx.generate_build_query(namespace=namespace)
            build_query_2 = vector_idx.generate_build_query(namespace=namespace)
            index_build_list.append(build_query_1)
            index_build_list.append(build_query_2)

        with ThreadPoolExecutor() as executor:
            for query in index_build_list:
                executor.submit(self.run_cbq_query, query=query)
        self.wait_until_indexes_online(timeout=600)
        self.item_count_related_validations()
        meta_data = self.index_rest.get_indexer_metadata()['status']
        self.assertEqual(len(meta_data), len(self.namespaces) * 2, f"indexes are not created meta data {meta_data}")
        self.drop_index_node_resources_utilization_validations()

    def test_refresh_connections_during_query_workload(self):
        '''
        This test is for MB-65153
        Steps -
        Run a continous query workload and during this workload refresh the certs
        The queries shouldn't fail and the uptime stat should not be zero
        '''
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=False)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        select_queries = []
        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                      similarity=self.similarity, train_list=None,
                                                                      scan_nprobes=self.scan_nprobes,
                                                                      array_indexes=False, bhive_index=self.bhive_index,
                                                                      limit=self.scan_limit, is_base64=self.base64,
                                                                      quantization_algo_description_vector=self.quantization_algo_description_vector,
                                                                      quantization_algo_color_vector=self.quantization_algo_color_vector)
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions, namespace=namespace,
                                                                     num_replica=1, bhive_index=self.bhive_index)
            self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, query_node=query_node)

            select_queries.extend(
                self.gsi_util_obj.get_select_queries(definition_list=definitions, namespace=namespace))
        self.wait_until_indexes_online(timeout=600)
        self.item_count_related_validations()
        with ThreadPoolExecutor() as executor:
            self.gsi_util_obj.query_event.set()
            executor.submit(self.gsi_util_obj.run_continous_query_load,
                            select_queries=select_queries, query_node=query_node, timeout=300, sleep_timer=5)
            self.sleep(60, "sleeping for query workload")
            for node in self.servers[:self.nodes_init]:
                rest = RestConnection(node)
                self.log.info("nodes connection reset")
                rest.reload_certificate()
        self.gsi_util_obj.query_event.clear()
        self.log.info(f"The query failed are {self.gsi_util_obj.query_errors}")
        #Assertions for whether queries have failed or not
        self.assertEqual(len(self.gsi_util_obj.query_errors), 0, f'There are query failures {self.gsi_util_obj.query_errors}')
        for node in index_nodes:
            rest = RestConnection(node)
            stats = rest.get_all_index_stats()
            self.log.info(f"stats for uptime are {stats['uptime']}")
            self.assertIsNot(stats['uptime'], '0', f"stats for uptime are {stats['uptime']}")

    def test_rebalance(self):
        redistribute = {"indexer.settings.rebalance.redistribute_indexes": True}
        self.index_rest.set_index_settings(redistribute)
        if self.shard_based_rebalance:
            self.enable_shard_based_rebalance()
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=False)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        select_queries = []
        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                      similarity=self.similarity, train_list=None,
                                                                      scan_nprobes=self.scan_nprobes,
                                                                      array_indexes=False, bhive_index=self.bhive_index,
                                                                      limit=self.scan_limit, is_base64=self.base64,
                                                                      quantization_algo_description_vector=self.quantization_algo_description_vector,
                                                                      quantization_algo_color_vector=self.quantization_algo_color_vector)
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions, namespace=namespace,
                                                                     num_replica=1, bhive_index=self.bhive_index)
            self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, query_node=query_node)

            select_queries.extend(
                self.gsi_util_obj.get_select_queries(definition_list=definitions, namespace=namespace))
        self.wait_until_indexes_online(timeout=600)
        self.item_count_related_validations()
        code_book_memory_map_before_rebalance, aggregated_code_book_memory_before_rebalance = self.get_per_index_codebook_memory_usage()
        # Recall and accuracy check
        for select_query in select_queries:
            # Skipping validation for recall and accuracy against primary index
            if "DISTINCT" in select_query:
                continue
            self.validate_scans_for_recall_and_accuracy(select_query=select_query)
        with ThreadPoolExecutor() as executor:
            # todo will uncomment out below post - https://jira.issues.couchbase.com/browse/MB-67778
            # self.gsi_util_obj.query_event.set()
            # executor.submit(self.gsi_util_obj.run_continous_query_load,
            #                 select_queries=select_queries, query_node=query_node)
            if self.rebalance_type == 'rebalance_in':
                add_nodes = [self.servers[self.nodes_init]]
                task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=add_nodes,
                                                    to_remove=[], services=['index', 'index'])
            elif self.rebalance_type == 'rebalance_swap':
                add_nodes = [self.servers[self.nodes_init]]
                task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=add_nodes,
                                                    to_remove=[index_nodes[0]], services=['index'])
            elif self.rebalance_type == 'rebalance_out':
                task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=[],
                                                    to_remove=[index_nodes[0]], services=['index'])
            else:
                self.fail('Incorrect rebalance operation')
            if self.cancel_rebalance:
                self.rest.stop_rebalance()
            if self.fail_rebalance:
                indexer_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
                server = random.choice(indexer_nodes)
                self.sleep(5)
                self._kill_all_processes_index(server=server)
            try:
                task.result()
                rebalance_status = RestHelper(self.rest).rebalance_reached()
                self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")
            except Exception as e:
                self.log.info(f"Rebalance failed as expected: {e}")
                if self.cancel_rebalance or self.fail_rebalance:
                    self.sleep(300, "Waiting for cleanup to complete")
                    self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=[],
                                             to_remove=[], services=[])
                rebalance_status = RestHelper(self.rest).rebalance_reached()
                self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")
            # self.gsi_util_obj.query_event.clear()
            data_nodes = self.get_nodes_from_services_map(service_type="kv")
            self.sleep(30)
            code_book_memory_map_after_rebalance, aggregated_code_book_memory_after_rebalance = self.get_per_index_codebook_memory_usage()
            index_names = self.get_all_indexes_in_the_cluster()
            if self.shard_based_rebalance:
                for index in index_names:
                    if "primary" in index:
                        continue
                    self.assertEqual(code_book_memory_map_before_rebalance[index], code_book_memory_map_after_rebalance[index],
                                     f"Codebook memory has changed for index {index}")

            # Todo: Add metadata validation
            if self.post_rebalance_action == "data_load":
                for namespace in self.namespaces:
                    bucket, scope, collection = namespace.split('.')
                    bucket = bucket.split(':')[-1]
                    self.gen_create = SDKDataLoader(num_ops=self.num_of_docs_per_collection, percent_create=100,
                                                    percent_update=0, percent_delete=0, scope=scope,
                                                    collection=collection, json_template="Cars",
                                                    output=True, username=self.username, password=self.password,
                                                    base64=self.base64,
                                                    model=self.data_model, workers=10, timeout=1500,
                                                    key_prefix="doc_77",
                                                    create_start=self.num_of_docs_per_collection,
                                                    create_end=self.num_of_docs_per_collection + 10000)
                    #self.load_docs_via_magma_server(server=data_nodes, bucket=bucket, gen=self.gen_create)
                    task = self.cluster.async_load_gen_docs(data_nodes, bucket=bucket,
                                                            generator=self.gen_create,
                                                            timeout_secs=1500, use_magma_loader=True)
                    task.result()
                self.sleep(60)
                _, stats = self._return_maps(perNode=True, map_from_index_nodes=True)
                index_item_count_map = {}
                partial_indexes = self.get_partial_indexes_name_list()
                for node in stats:
                    for namespace in stats[node]:
                        for index in stats[node][namespace]:
                            if index not in index_item_count_map:
                                index_item_count_map[index] = stats[node][namespace][index]["items_count"]
                            else:
                                index_item_count_map[index] += stats[node][namespace][index]["items_count"]
                for index in index_item_count_map:
                    if index in partial_indexes:
                        continue
                    self.assertEqual(index_item_count_map[index], self.num_of_docs_per_collection+10000, f"stats {stats}")


            if self.post_rebalance_action == "mutations":
                for namespace in self.namespaces:
                    keyspace = namespace.split(":")[-1]
                    bucket, scope, collection = keyspace.split(".")
                    bucket = bucket.split(':')[-1]
                    self.gen_update = SDKDataLoader(num_ops=self.num_of_docs_per_collection, percent_create=0,
                                                    percent_update=100, percent_delete=0, workers=16, scope=scope,
                                                    collection=collection, json_template="Cars", timeout=2000,
                                                    op_type="update", mutate=1, dim=384,
                                                    update_start=1, update_end=self.num_of_docs_per_collection)
                    # self.load_docs_via_magma_server(server=data_nodes, bucket=bucket, gen=self.gen_update)
                    task = self.cluster.async_load_gen_docs(data_nodes, bucket=bucket,
                                                            generator=self.gen_update,
                                                            timeout_secs=2000, use_magma_loader=True)
                    task.result()
                    self.sleep(60)
                    _, stats = self._return_maps(perNode=True, map_from_index_nodes=True)
                    partial_indexes = self.get_partial_indexes_name_list()
                    index_item_count_map = {}
                    for node in stats:
                        for namespace in stats[node]:
                            for index in stats[node][namespace]:
                                if index not in index_item_count_map:
                                    index_item_count_map[index] = stats[node][namespace][index]["items_count"]
                                else:
                                    index_item_count_map[index] += stats[node][namespace][index]["items_count"]
                    for index in index_item_count_map:
                        if index in partial_indexes:
                            continue
                        self.assertEqual(index_item_count_map[index], self.num_of_docs_per_collection, f"stats {stats}")

        if self.shard_based_rebalance:
            self.validate_shard_affinity()
            self.sleep(30)
            if not self.check_gsi_logs_for_shard_transfer():
                raise Exception("Shard based rebalance not triggered")

        for select_query in select_queries:
            # Skipping validation for recall and accuracy against primary index
            if "DISTINCT" in select_query:
                continue
            self.validate_scans_for_recall_and_accuracy(select_query=select_query)
        self.drop_index_node_resources_utilization_validations()

    def test_kv_rebalance(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename,
                                      skip_default_scope=self.skip_default)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        data_nodes = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        select_queries = []
        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                      similarity=self.similarity, train_list=None,
                                                                      scan_nprobes=self.scan_nprobes,
                                                                      array_indexes=False, bhive_index=self.bhive_index,
                                                                      limit=self.scan_limit, is_base64=self.base64,
                                                                      quantization_algo_description_vector=self.quantization_algo_description_vector,
                                                                      quantization_algo_color_vector=self.quantization_algo_color_vector)
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions, namespace=namespace,
                                                                     num_replica=1, bhive_index=self.bhive_index)
            self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, query_node=query_node[0])
            select_queries.extend(
                self.gsi_util_obj.get_select_queries(definition_list=definitions, namespace=namespace))

        self.wait_until_indexes_online(timeout=600)
        self.item_count_related_validations()

        for select_query in select_queries:
            if "ANN_DISTANCE" not in select_query:
                continue
            self.validate_scans_for_recall_and_accuracy(select_query=select_query)
        with ThreadPoolExecutor() as executor:
            self.gsi_util_obj.query_event.set()
            executor.submit(self.gsi_util_obj.run_continous_query_load,
                            select_queries=select_queries, query_node=query_node[1])
            if self.rebalance_type == 'rebalance_in':
                add_nodes = [self.servers[3]]
                task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=add_nodes,
                                                    to_remove=[], services=['kv'])
            elif self.rebalance_type == 'rebalance_swap':
                add_nodes = [self.servers[4]]
                task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=add_nodes,
                                                    to_remove=[data_nodes[0]], services=['kv'])
            elif self.rebalance_type == 'rebalance_out':
                task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=[],
                                                    to_remove=[data_nodes[0]], services=['kv'])
            task.result()
            rebalance_status = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")
            self.gsi_util_obj.query_event.clear()
            self.update_master_node()
            self.sleep(30)

            query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
            data_nodes = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=False)
            self.log.info(f"data nodes are {data_nodes}")

            # Todo: Add metadata validation
            if self.post_rebalance_action == "data_load":
                for namespace in self.namespaces:
                    keyspace = namespace.split(":")[-1]
                    bucket, scope, collection = keyspace.split(".")
                    self.gen_create = SDKDataLoader(num_ops=self.num_of_docs_per_collection, percent_create=100,
                                                    percent_update=0, percent_delete=0, scope=scope,
                                                    collection=collection, json_template="Cars",
                                                    output=True, username=self.username, password=self.password,
                                                    base64=self.base64,
                                                    model=self.data_model, workers=10, timeout=1500,
                                                    key_prefix="doc_77",
                                                    create_start=self.num_of_docs_per_collection,
                                                    create_end=self.num_of_docs_per_collection + 10000)
                    task = self.cluster.async_load_gen_docs(data_nodes, bucket=bucket,
                                                            generator=self.gen_create,
                                                            timeout_secs=300, use_magma_loader=True)
                    task.result()
            if self.post_rebalance_action == "mutations":
                for namespace in self.namespaces:
                    keyspace = namespace.split(":")[-1]
                    bucket, scope, collection = keyspace.split(".")
                    self.gen_update = SDKDataLoader(num_ops=self.num_of_docs_per_collection, percent_create=0,
                                                    percent_update=100, percent_delete=0, workers=16, scope=scope,
                                                    collection=collection, json_template="Cars", timeout=2000,
                                                    op_type="update", mutate=1, dim=384,
                                                    update_start=1, update_end=self.num_of_docs_per_collection, get_sdk_logs=True)
                    #self.load_docs_via_magma_server(server=data_nodes, bucket=bucket, gen=self.gen_update)
                    task = self.cluster.async_load_gen_docs(data_nodes, bucket=bucket,
                                                            generator=self.gen_update,
                                                            timeout_secs=2000, use_magma_loader=True)
                    task.result()
            self.sleep(60)
            self.update_master_node()
            _, stats = self._return_maps(perNode=True, map_from_index_nodes=True)
            partial_indexes = self.get_partial_indexes_name_list()
            index_item_count_map = {}
            for node in stats:
                for namespace in stats[node]:
                    for index in stats[node][namespace]:
                        if index not in index_item_count_map:
                            index_item_count_map[index] = stats[node][namespace][index]["items_count"]
                        else:
                            index_item_count_map[index] += stats[node][namespace][index]["items_count"]
            self.log.info(f"item count map : {index_item_count_map}")
            additional_docs = 0
            if self.post_rebalance_action == "data_load":
                additional_docs = 10000
            for index in index_item_count_map:
                if index in partial_indexes:
                    continue
                self.assertEqual(index_item_count_map[index], self.num_of_docs_per_collection+additional_docs, f"stats {stats}")

        for select_query in select_queries:
            if "ANN_DISTANCE" not in select_query:
                continue
            self.validate_scans_for_recall_and_accuracy(select_query=select_query)
        self.drop_index_node_resources_utilization_validations()

    def test_kv_rebalance_failure_or_rebalance_stop_and_retry_with_indexes(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename,
                                      skip_default_scope=self.skip_default)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        data_nodes = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        select_queries = []
        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                      similarity=self.similarity, train_list=None,
                                                                      scan_nprobes=self.scan_nprobes,
                                                                      array_indexes=False, bhive_index=self.bhive_index,
                                                                      limit=self.scan_limit, is_base64=self.base64,
                                                                      quantization_algo_description_vector=self.quantization_algo_description_vector,
                                                                      quantization_algo_color_vector=self.quantization_algo_color_vector)
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions, namespace=namespace,
                                                                     num_replica=1, bhive_index=self.bhive_index)
            self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, query_node=query_node[0])
            select_queries.extend(
                self.gsi_util_obj.get_select_queries(definition_list=definitions, namespace=namespace))


        self.item_count_related_validations()
        with ThreadPoolExecutor() as executor:
            self.gsi_util_obj.query_event.set()
            executor.submit(self.gsi_util_obj.run_continous_query_load,
                            select_queries=select_queries, query_node=query_node[1])
            add_nodes = [self.servers[4]]
            try:
                task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=add_nodes,
                                                    to_remove=[data_nodes[0]], services=['kv'])
                if self.cancel_rebalance:
                    # stop ongoing rebalance
                    self.rest.stop_rebalance()
                else:
                    # fail rebalance operation and then retry
                    self.stop_server(self.servers[self.nodes_init])
                task.result()
                self.log.error(f"Rebalance didn't fail: {task.result}")
            except Exception as err:
                self.log.info('Rebalance failed. See logs for detailed reason' in str(err))
            finally:
                if not self.cancel_rebalance:
                    self.start_server(self.servers[self.nodes_init])

            task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=[],
                                        to_remove=[], services=[])
            task.result()
            rebalance_status = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")
            self.gsi_util_obj.query_event.clear()
        self.update_master_node()
        self.n1ql_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        self.drop_index_node_resources_utilization_validations()

    def test_kv_autofailover_and_remove_node_with_indexes(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename,
                                      skip_default_scope=self.skip_default)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        data_nodes = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        select_queries = []
        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                      similarity=self.similarity, train_list=None,
                                                                      scan_nprobes=self.scan_nprobes,
                                                                      array_indexes=False, bhive_index=self.bhive_index,
                                                                      limit=self.scan_limit, is_base64=self.base64,
                                                                      quantization_algo_description_vector=self.quantization_algo_description_vector,
                                                                      quantization_algo_color_vector=self.quantization_algo_color_vector)
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions, namespace=namespace,
                                                                     num_replica=1, bhive_index=self.bhive_index)
            self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, query_node=query_node[0])
            select_queries.extend(
                self.gsi_util_obj.get_select_queries(definition_list=definitions, namespace=namespace))
        self.wait_until_indexes_online(timeout=600)
        self.sleep(10)
        self.item_count_related_validations()

        with ThreadPoolExecutor() as executor:
            self.gsi_util_obj.query_event.set()
            # Ensure query node is different from the node being failed over
            failover_node = data_nodes[0]
            selected_query_node = None
            for node in query_node:
                if node != failover_node:
                    selected_query_node = node
                    break
            if selected_query_node is None:
                # Fallback: use query_node[1] if no different node found
                selected_query_node = query_node[1]
            executor.submit(self.gsi_util_obj.run_continous_query_load,
                            select_queries=select_queries, query_node=selected_query_node)

            # perform auto failover of KV node
            # Ensure we don't failover the master node
            self.update_master_node()
            self.rest = RestConnection(self.master)
            failover_node = None
            for node in data_nodes:
                if node != self.master:
                    failover_node = node
                    break
            if failover_node is None:
                # Fallback: if all data nodes are master, use the first one
                failover_node = data_nodes[0]
                self.log.warning("All data nodes are master nodes, using first data node for failover")
            else:
                self.log.info(f"Selected non-master node {failover_node.ip} for failover")
            data_node = RemoteMachineShellConnection(failover_node)
            data_node.stop_server()
            self.sleep(40, "Wait for autofailover")
            try:
                rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                            [], [data_node])
                reached = RestHelper(self.rest).rebalance_reached(retry_count=150)
                self.assertTrue(reached, "rebalance failed, stuck or did not complete")
                rebalance.result()
            except Exception as e:
                self.log.error(f"Rebalance failed with error: {e}")
            finally:
                data_node.start_server()
            self.gsi_util_obj.query_event.clear()

            partial_index_list = self.get_partial_indexes_name_list()
            _, stats = self._return_maps(perNode=True, map_from_index_nodes=True)
            index_item_count_map = {}
            for node in stats:
                for namespace in stats[node]:
                    for index in stats[node][namespace]:
                        if index not in index_item_count_map:
                            index_item_count_map[index] = stats[node][namespace][index]["items_count"]
                        else:
                            index_item_count_map[index] += stats[node][namespace][index]["items_count"]
            self.log.info(f"item count map : {index_item_count_map}")
            for index in index_item_count_map:
                if index in partial_index_list:
                    continue
                self.assertEqual(index_item_count_map[index], self.num_of_docs_per_collection, f"stats {stats}")
        self.update_master_node()
        self.n1ql_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        self.drop_index_node_resources_utilization_validations()

    def kv_node_failover_recovery_and_addback_with_indexes(self):
        self.recovery_type = self.input.param('recovery_type', 'full')
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename,
                                      skip_default_scope=self.skip_default)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        data_nodes = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        select_queries = []
        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                      similarity=self.similarity, train_list=None,
                                                                      scan_nprobes=self.scan_nprobes,
                                                                      array_indexes=False, bhive_index=self.bhive_index,
                                                                      limit=self.scan_limit, is_base64=self.base64,
                                                                      quantization_algo_description_vector=self.quantization_algo_description_vector,
                                                                      quantization_algo_color_vector=self.quantization_algo_color_vector)
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions, namespace=namespace,
                                                                     num_replica=1, bhive_index=self.bhive_index)
            self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, query_node=query_node[0])
            select_queries.extend(
                self.gsi_util_obj.get_select_queries(definition_list=definitions, namespace=namespace))
        self.wait_until_indexes_online(timeout=600)
        self.sleep(10)
        self.item_count_related_validations()

        with ThreadPoolExecutor() as executor:
            self.gsi_util_obj.query_event.set()
            executor.submit(self.gsi_util_obj.run_continous_query_load,
                            select_queries=select_queries, query_node=query_node[1])
            self.log.info("Starting failover of KV node")
            # perform failover of KV node
            failover_task = self.cluster.async_failover([self.master], failover_nodes=[data_nodes[-1]], graceful=False)
            failover_task.result()
            self.log.info("Failover of KV node completed")
            self.log.info("Starting recovery of KV node")
            self.rest.add_back_node("ns_1@{}".format(data_nodes[-1].ip))
            self.rest.set_recovery_type("ns_1@{}".format(data_nodes[-1].ip), self.recovery_type)
            self.cluster.rebalance(self.servers[:self.nodes_init], [], [])
            self.log.info("Recovery of KV node completed")
            msg = "rebalance failed while recovering failover nodes {0}".format(
                data_nodes[-1])
            self.assertTrue(self.rest.monitorRebalance(stop_if_loop=True), msg)
            self.log.info("Rebalance completed. Clearing query event")
            self.gsi_util_obj.query_event.clear()
            partial_index_list = self.get_partial_indexes_name_list()
            _, stats = self._return_maps(perNode=True, map_from_index_nodes=True)
            index_item_count_map = {}
            for node in stats:
                for namespace in stats[node]:
                    for index in stats[node][namespace]:
                        if index not in index_item_count_map:
                            index_item_count_map[index] = stats[node][namespace][index]["items_count"]
                        else:
                            index_item_count_map[index] += stats[node][namespace][index]["items_count"]
            self.log.info(f"item count map : {index_item_count_map}")
            for index in index_item_count_map:
                if index in partial_index_list:
                    continue
                self.assertEqual(index_item_count_map[index], self.num_of_docs_per_collection, f"stats {stats}")
        self.update_master_node()
        self.n1ql_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        self.drop_index_node_resources_utilization_validations()

    def test_kv_and_indexing_rebalance_operations_with_different_topologies(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename,
                                      skip_default_scope=self.skip_default)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        data_nodes = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        select_queries = []
        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                      similarity=self.similarity, train_list=None,
                                                                      scan_nprobes=self.scan_nprobes,
                                                                      array_indexes=False, bhive_index=self.bhive_index,
                                                                      limit=self.scan_limit, is_base64=self.base64,
                                                                      quantization_algo_description_vector=self.quantization_algo_description_vector,
                                                                      quantization_algo_color_vector=self.quantization_algo_color_vector)
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions, namespace=namespace,
                                                                     num_replica=1, bhive_index=self.bhive_index)
            self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, query_node=query_node[0])
            select_queries.extend(
                self.gsi_util_obj.get_select_queries(definition_list=definitions, namespace=namespace))

        self.wait_until_indexes_online(timeout=600)
        self.sleep(10)
        self.item_count_related_validations()
        for select_query in select_queries:
            if "ANN_DISTANCE" not in select_query:
                continue
            self.validate_scans_for_recall_and_accuracy(select_query=select_query)


        for namespace in self.namespaces:
            keyspace = namespace.split(":")[-1]
            bucket, scope, collection = keyspace.split(".")

            self.gen_create = SDKDataLoader(num_ops=self.num_of_docs_per_collection, percent_create=100,
                                            percent_update=0, percent_delete=0, workers=16, scope=scope,
                                            collection=collection, json_template="Cars", timeout=2000,
                                            op_type="create", dim=384,
                                            create_start=self.num_of_docs_per_collection,
                                            create_end=(self.num_of_docs_per_collection +
                                                        self.num_of_docs_per_collection // 2))
            #self.load_docs_via_magma_server(server=data_nodes[0], bucket=bucket, gen=self.gen_create)
            task = self.cluster.async_load_gen_docs(data_nodes[0], bucket=bucket,
                                                    generator=self.gen_create,
                                                    timeout_secs=2000, use_magma_loader=True)
            task.result()

        with ThreadPoolExecutor() as executor:
            self.gsi_util_obj.query_event.set()
            executor.submit(self.gsi_util_obj.run_continous_query_load,
                            select_queries=select_queries, query_node=query_node[1])
            if self.rebalance_type == 'rebalance_in':
                add_nodes = [self.servers[4]]
                task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=add_nodes,
                                                    to_remove=[], services=['kv,n1ql'])
            elif self.rebalance_type == 'rebalance_swap':
                add_nodes = [self.servers[self.nodes_init]]
                task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=add_nodes,
                                                    to_remove=[data_nodes[0]], services=['kv,n1ql'])
            elif self.rebalance_type == 'rebalance_out':
                task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=[],
                                                    to_remove=[data_nodes[0]])
            task.result()
            self.update_master_node()
            rebalance_status = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")
            self.gsi_util_obj.query_event.clear()

        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        data_nodes = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        for namespace in self.namespaces:
            keyspace = namespace.split(":")[-1]
            bucket, scope, collection = keyspace.split(".")

            self.gen_update = SDKDataLoader(num_ops=self.num_of_docs_per_collection, percent_create=0,
                                            percent_update=100, percent_delete=0, workers=16, scope=scope,
                                            collection=collection, json_template="Cars", timeout=2000,
                                            op_type="update", mutate=1, dim=384,
                                            update_start=1,
                                            update_end=(self.num_of_docs_per_collection +
                                                        self.num_of_docs_per_collection // 2))
            # self.load_docs_via_magma_server(server=data_nodes[0], bucket=bucket, gen=self.gen_update)
            task = self.cluster.async_load_gen_docs(data_nodes[0], bucket=bucket,
                                                    generator=self.gen_update,
                                                    timeout_secs=2000, use_magma_loader=True)
            task.result()

        with ThreadPoolExecutor() as executor:
            self.gsi_util_obj.query_event.set()
            executor.submit(self.gsi_util_obj.run_continous_query_load,
                            select_queries=select_queries, query_node=query_node)
            if self.rebalance_type == 'rebalance_in':
                add_nodes = [self.servers[5]]
                task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=add_nodes,
                                                    to_remove=[], services=['index'])
            elif self.rebalance_type == 'rebalance_swap':
                add_nodes = [self.servers[5]]
                task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=add_nodes,
                                                    to_remove=[data_nodes[0]], services=['index'])
            elif self.rebalance_type == 'rebalance_out':
                task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=[],
                                                    to_remove=[data_nodes[0]])
            self.update_master_node()
            task.result()
            rebalance_status = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")
            self.gsi_util_obj.query_event.clear()

        self.n1ql_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        partial_index_list = self.get_partial_indexes_name_list()
        _, stats = self._return_maps(perNode=True, map_from_index_nodes=True)
        index_item_count_map = {}
        for node in stats:
            for namespace in stats[node]:
                for index in stats[node][namespace]:
                    if index not in index_item_count_map:
                        index_item_count_map[index] = stats[node][namespace][index]["items_count"]
                    else:
                        index_item_count_map[index] += stats[node][namespace][index]["items_count"]
        for index in index_item_count_map:
            if index in partial_index_list:
                    continue
            self.assertEqual(index_item_count_map[index], self.num_of_docs_per_collection +
                                                        self.num_of_docs_per_collection // 2, f"stats {stats}")

        for select_query in select_queries:
            if "ANN_DISTANCE" not in select_query:
                continue
            self.validate_scans_for_recall_and_accuracy(select_query=select_query)
        self.drop_index_node_resources_utilization_validations()

    def test_kv_and_indexing_failover_and_recovery_sequentially(self):
        self.recovery_type = self.input.param('recovery_type', 'full')
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename,
                                      skip_default_scope=self.skip_default)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        data_nodes = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        select_queries = []
        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                      similarity=self.similarity, train_list=None,
                                                                      scan_nprobes=self.scan_nprobes,
                                                                      array_indexes=False, bhive_index=self.bhive_index,
                                                                      limit=self.scan_limit, is_base64=self.base64,
                                                                      quantization_algo_description_vector=self.quantization_algo_description_vector,
                                                                      quantization_algo_color_vector=self.quantization_algo_color_vector)
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions, namespace=namespace,
                                                                     num_replica=1, bhive_index=self.bhive_index)
            self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, query_node=query_node[0])
            select_queries.extend(
                self.gsi_util_obj.get_select_queries(definition_list=definitions, namespace=namespace))

        for select_query in select_queries:
            if "ANN_DISTANCE" not in select_query:
                continue
            self.validate_scans_for_recall_and_accuracy(select_query=select_query)

        with ThreadPoolExecutor() as executor:
            self.gsi_util_obj.query_event.set()
            executor.submit(self.gsi_util_obj.run_continous_query_load,
                            select_queries=select_queries, query_node=query_node[1])

            # failover and recover KV and indexing node sequentially
            for node in data_nodes[2], index_nodes[2]:
                failover_task = self.cluster.async_failover([self.master], failover_nodes=[node], graceful=False)
                failover_task.result()

                self.rest.add_back_node("ns_1@{}".format(node.ip))
                self.rest.set_recovery_type("ns_1@{}".format(node.ip), self.recovery_type)
                self.cluster.rebalance(self.servers[:self.nodes_init], [], [])
                msg = "rebalance failed while recovering failover nodes {0}".format(
                    node)
                self.assertTrue(self.rest.monitorRebalance(stop_if_loop=True), msg)

            self.gsi_util_obj.query_event.clear()

        self.update_master_node()
        self.n1ql_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        partial_index_list = self.get_partial_indexes_name_list()
        _, stats = self._return_maps(perNode=True, map_from_index_nodes=True)
        index_item_count_map = {}
        for node in stats:
            for namespace in stats[node]:
                for index in stats[node][namespace]:
                    if index not in index_item_count_map:
                        index_item_count_map[index] = stats[node][namespace][index]["items_count"]
                    else:
                        index_item_count_map[index] += stats[node][namespace][index]["items_count"]
        self.log.info(f'index item count map is {index_item_count_map}')
        for index in index_item_count_map:
            if index in partial_index_list:
                continue
            self.assertEqual(index_item_count_map[index], self.num_of_docs_per_collection, f"stats {stats}")

        for select_query in select_queries:
            if "ANN_DISTANCE" not in select_query:
                continue
            self.validate_scans_for_recall_and_accuracy(select_query=select_query)

    def test_kv_and_indexing_failover_and_rebalance_out_sequentially(self):
        self.recovery_type = self.input.param('recovery_type', 'full')
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename,
                                      skip_default_scope=self.skip_default)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        data_nodes = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        select_queries = []
        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                      similarity=self.similarity, train_list=None,
                                                                      scan_nprobes=self.scan_nprobes,
                                                                      array_indexes=False, bhive_index=self.bhive_index,
                                                                      limit=self.scan_limit, is_base64=self.base64,
                                                                      quantization_algo_description_vector=self.quantization_algo_description_vector,
                                                                      quantization_algo_color_vector=self.quantization_algo_color_vector)
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions, namespace=namespace,
                                                                     num_replica=1, bhive_index=self.bhive_index)
            self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, query_node=query_node[0])
            select_queries.extend(
                self.gsi_util_obj.get_select_queries(definition_list=definitions, namespace=namespace))

        for select_query in select_queries:
            if "ANN_DISTANCE" not in select_query:
                continue
            self.validate_scans_for_recall_and_accuracy(select_query=select_query)

        with ThreadPoolExecutor() as executor:
            self.gsi_util_obj.query_event.set()
            executor.submit(self.gsi_util_obj.run_continous_query_load,
                            select_queries=select_queries, query_node=query_node[1])

            # failover and recover KV and indexing node sequentially
            for node in data_nodes[2], index_nodes[2]:
                failover_task = self.cluster.async_failover([self.master], failover_nodes=[node], graceful=False)
                failover_task.result()

                self.rest.add_back_node("ns_1@{}".format(node.ip))
                self.rest.set_recovery_type("ns_1@{}".format(node.ip), self.recovery_type)
                self.cluster.rebalance(self.servers[:self.nodes_init], [], [])
                msg = "rebalance failed while recovering failover nodes {0}".format(
                    node)
                self.assertTrue(self.rest.monitorRebalance(stop_if_loop=True), msg)

            self.gsi_util_obj.query_event.clear()

        self.update_master_node()
        self.n1ql_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        partial_index_list = self.get_partial_indexes_name_list()
        _, stats = self._return_maps(perNode=True, map_from_index_nodes=True)
        index_item_count_map = {}
        for node in stats:
            for namespace in stats[node]:
                for index in stats[node][namespace]:
                    if index not in index_item_count_map:
                        index_item_count_map[index] = stats[node][namespace][index]["items_count"]
                    else:
                        index_item_count_map[index] += stats[node][namespace][index]["items_count"]
        self.log.info(f"item count map : {index_item_count_map}")
        for index in index_item_count_map:
            if index in partial_index_list:
                continue
            self.assertEqual(index_item_count_map[index], self.num_of_docs_per_collection, f"stats {stats}")

        for select_query in select_queries:
            if "ANN_DISTANCE" not in select_query:
                continue
            self.validate_scans_for_recall_and_accuracy(select_query=select_query)

    def test_kv_and_indexing_rebalance_concurrently(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename,
                                      skip_default_scope=self.skip_default)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        data_nodes = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        select_queries = []
        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                      similarity=self.similarity, train_list=None,
                                                                      scan_nprobes=self.scan_nprobes,
                                                                      array_indexes=False, bhive_index=self.bhive_index,
                                                                      limit=self.scan_limit, is_base64=self.base64,
                                                                      quantization_algo_description_vector=self.quantization_algo_description_vector,
                                                                      quantization_algo_color_vector=self.quantization_algo_color_vector)
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions, namespace=namespace,
                                                                     num_replica=1, bhive_index=self.bhive_index)
            self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, query_node=query_node[0])
            select_queries.extend(
                self.gsi_util_obj.get_select_queries(definition_list=definitions, namespace=namespace))

        for select_query in select_queries:
            if "ANN_DISTANCE" not in select_query:
                continue
            self.validate_scans_for_recall_and_accuracy(select_query=select_query)
        with ThreadPoolExecutor() as executor:
            self.gsi_util_obj.query_event.set()
            executor.submit(self.gsi_util_obj.run_continous_query_load,
                            select_queries=select_queries, query_node=query_node[1])
            if self.rebalance_type == 'rebalance_in':
                add_nodes = [self.servers[4]]
                task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=add_nodes,
                                                    to_remove=[], services=['kv,n1ql', 'index'])
            elif self.rebalance_type == 'rebalance_swap':
                add_nodes = [self.servers[4], self.servers[5]]
                task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=add_nodes,
                                                    to_remove=[data_nodes[0], index_nodes[0]], services=['kv,n1ql', 'index'])
            elif self.rebalance_type == 'rebalance_out':
                task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=[],
                                                    to_remove=[data_nodes[0], index_nodes[0]], services=['kv,n1ql', 'index'])
            task.result()
            rebalance_status = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")
            self.gsi_util_obj.query_event.clear()
            self.n1ql_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
            self.update_master_node()
            self.sleep(30)

            # Todo: Add metadata validation
            if self.post_rebalance_action == "data_load":
                for namespace in self.namespaces:
                    keyspace = namespace.split(":")[-1]
                    bucket, scope, collection = keyspace.split(".")
                    self.gen_create = SDKDataLoader(num_ops=self.num_of_docs_per_collection, percent_create=100,
                                                    percent_update=0, percent_delete=0, scope=scope,
                                                    collection=collection, json_template="Cars",
                                                    output=True, username=self.username, password=self.password,
                                                    base64=self.base64,
                                                    model=self.data_model, workers=10, timeout=1500,
                                                    key_prefix="doc_77",
                                                    create_start=self.num_of_docs_per_collection,
                                                    create_end=self.num_of_docs_per_collection + 10000)
                    # self.load_docs_via_magma_server(server=data_nodes[1], bucket=bucket, gen=self.gen_create)
                    task = self.cluster.async_load_gen_docs(data_nodes[1], bucket=bucket,
                                                            generator=self.gen_create,
                                                            timeout_secs=1500, use_magma_loader=True)
                    task.result()
            if self.post_rebalance_action == "mutations":
                for namespace in self.namespaces:
                    keyspace = namespace.split(":")[-1]
                    bucket, scope, collection = keyspace.split(".")
                    self.gen_update = SDKDataLoader(num_ops=self.num_of_docs_per_collection, percent_create=0,
                                                    percent_update=100, percent_delete=0, workers=16, scope=scope,
                                                    collection=collection, json_template="Cars", timeout=2000,
                                                    op_type="update", mutate=1, dim=384,
                                                    update_start=1, update_end=self.num_of_docs_per_collection)
                    # self.load_docs_via_magma_server(server=data_nodes[1], bucket=bucket, gen=self.gen_create)
                    task = self.cluster.async_load_gen_docs(data_nodes[1], bucket=bucket,
                                                            generator=self.gen_create,
                                                            timeout_secs=2000, use_magma_loader=True)
                    task.result()
            self.sleep(60)
            self.update_master_node()
            self.n1ql_node = self.get_nodes_from_services_map(service_type="n1ql")
            partial_index_list = self.get_partial_indexes_name_list()
            _, stats = self._return_maps(perNode=True, map_from_index_nodes=True)
            index_item_count_map = {}
            for node in stats:
                for namespace in stats[node]:
                    for index in stats[node][namespace]:
                        if index not in index_item_count_map:
                            index_item_count_map[index] = stats[node][namespace][index]["items_count"]
                        else:
                            index_item_count_map[index] += stats[node][namespace][index]["items_count"]
            additional_docs = 1
            self.log.info(f"item count map : {index_item_count_map}")
            if self.post_rebalance_action == "data_load":
                additional_docs = 10000
            for index in index_item_count_map:
                if index in partial_index_list:
                    continue
                self.assertEqual(index_item_count_map[index], self.num_of_docs_per_collection+additional_docs, f"stats {stats}")

        for select_query in select_queries:
            if "ANN_DISTANCE" not in select_query:
                continue
            self.validate_scans_for_recall_and_accuracy(select_query=select_query)

    def test_kv_and_indexing_failover_and_recovery_concurrently(self):
        self.recovery_type = self.input.param('recovery_type', 'full')
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename,
                                      skip_default_scope=self.skip_default)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        data_nodes = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        select_queries = []
        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                      similarity=self.similarity, train_list=None,
                                                                      scan_nprobes=self.scan_nprobes,
                                                                      array_indexes=False, bhive_index=self.bhive_index,
                                                                      limit=self.scan_limit, is_base64=self.base64,
                                                                      quantization_algo_description_vector=self.quantization_algo_description_vector,
                                                                      quantization_algo_color_vector=self.quantization_algo_color_vector)
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions, namespace=namespace,
                                                                     num_replica=1, bhive_index=self.bhive_index)
            self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, query_node=query_node[0])
            select_queries.extend(
                self.gsi_util_obj.get_select_queries(definition_list=definitions, namespace=namespace))

        for select_query in select_queries:
            if "ANN_DISTANCE" not in select_query:
                continue
            self.validate_scans_for_recall_and_accuracy(select_query=select_query)

        with ThreadPoolExecutor() as executor:
            self.gsi_util_obj.query_event.set()
            executor.submit(self.gsi_util_obj.run_continous_query_load,
                            select_queries=select_queries, query_node=query_node[1])

            # failover and recover KV and indexing node concurrently
            nodes_to_failover = [data_nodes[2], index_nodes[2]]
            failover_task = self.cluster.async_failover([self.master], failover_nodes=nodes_to_failover, graceful=False)
            failover_task.result()

            for node in nodes_to_failover:
                self.rest.add_back_node("ns_1@{}".format(node.ip))
                self.rest.set_recovery_type("ns_1@{}".format(node.ip), self.recovery_type)

            self.cluster.rebalance(self.servers[:self.nodes_init], [], [])
            msg = "rebalance failed while recovering failover nodes {0} and {1}".format(nodes_to_failover[0],
                                                                                        nodes_to_failover[1])
            self.assertTrue(self.rest.monitorRebalance(stop_if_loop=True), msg)

            self.gsi_util_obj.query_event.clear()

        self.update_master_node()
        self.n1ql_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        partial_index_list = self.get_partial_indexes_name_list()
        _, stats = self._return_maps(perNode=True, map_from_index_nodes=True)
        index_item_count_map = {}
        self.log.info(f"item count map : {index_item_count_map}")
        for node in stats:
            for namespace in stats[node]:
                for index in stats[node][namespace]:
                    if index not in index_item_count_map:
                        index_item_count_map[index] = stats[node][namespace][index]["items_count"]
                    else:
                        index_item_count_map[index] += stats[node][namespace][index]["items_count"]
        for index in index_item_count_map:
            if index in partial_index_list:
                continue
            self.assertEqual(index_item_count_map[index], self.num_of_docs_per_collection, f"stats {stats}")

        for select_query in select_queries:
            if "ANN_DISTANCE" not in select_query:
                continue
            self.validate_scans_for_recall_and_accuracy(select_query=select_query)

    def test_drop_build_indexes_concurrently(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        query_list = []
        collection_namespace = self.namespaces[0]
        vector_idx = QueryDefinition(index_name='vector_rgb', index_fields=['colorRGBVector VECTOR'], dimension=3,
                                     description="IVF,PQ3x8", similarity="L2_SQUARED", is_base64=self.base64,
                                     bhive_index=self.bhive_index)
        query = vector_idx.generate_index_create_query(namespace=collection_namespace, defer_build=True,
                                                       bhive_index=self.bhive_index)
        self.run_cbq_query(query=query, server=self.n1ql_node)
        build_query = vector_idx.generate_build_query(namespace=collection_namespace)
        query_list.append(build_query)

        vector_idx_2 = QueryDefinition(index_name='vector_description', index_fields=['descriptionVector VECTOR'],
                                       dimension=384, is_base64=self.base64,
                                       description="IVF,PQ32x8", similarity="L2_SQUARED", bhive_index=self.bhive_index)
        query = vector_idx_2.generate_index_create_query(namespace=collection_namespace, defer_build=False,
                                                         bhive_index=self.bhive_index)
        self.run_cbq_query(query=query, server=self.n1ql_node)
        drop_query = vector_idx_2.generate_index_drop_query(namespace=collection_namespace)
        query_list.append(drop_query)

        with ThreadPoolExecutor() as executor:
            for query in query_list:
                executor.submit(self.run_cbq_query, query=query)
        self.wait_until_indexes_online(timeout=600)

        indexes_in_cluster = self.get_all_indexes_in_the_cluster()
        self.assertTrue(vector_idx.index_name in indexes_in_cluster, f"idx {vector_idx.index_name} not in the bucket")
        self.assertFalse(vector_idx_2.index_name in indexes_in_cluster, f"idx {vector_idx_2.index_name} in the bucket")

    def test_drop_index_during_phases(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)

        collection_namespace = self.namespaces[0]
        vector_idx = QueryDefinition(index_name='vector_description', index_fields=['descriptionVector VECTOR'],
                                     dimension=384, is_base64=self.base64,
                                     description="IVF,PQ32x8", similarity="L2_SQUARED", bhive_index=self.bhive_index)
        defer_build = False
        if self.build_phase == "create":
            defer_build = True
        query = vector_idx.generate_index_create_query(namespace=collection_namespace, defer_build=defer_build,
                                                       bhive_index=self.bhive_index)

        drop_query = vector_idx.generate_index_drop_query(namespace=collection_namespace)
        if self.build_phase == "create":
            self.run_cbq_query(query=query, server=self.n1ql_node)
            self.wait_until_indexes_online()
            self.run_cbq_query(query=drop_query, server=self.n1ql_node)

        else:
            timeout = 0
            with ThreadPoolExecutor() as executor:
                executor.submit(self.run_cbq_query, query=query)
                self.sleep(5)
                while timeout < 360:
                    index_state = self.index_rest.get_indexer_metadata()['status'][0]['status']
                    if index_state == self.build_phase:
                        try:
                            self.run_cbq_query(query=drop_query, server=self.n1ql_node)
                            break
                        except Exception as e:
                            self.log.info(str(e))
                            self.sleep(60)
                            self.wait_until_indexes_online()
                            break
                    self.sleep(1)
                    timeout = timeout + 1
            if timeout > 360:
                self.fail("timeout exceeded")

        self.assertEqual(len(self.index_rest.get_indexer_metadata()), 1, "index not dropped ")

    def test_concurrent_vector_index_drops(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)

        # drop vector index parallely on different namespaces

        index_drop_list = []
        for item, namespace in enumerate(self.namespaces):
            idx_name = f'idx_{item}'
            vector_idx = QueryDefinition(index_name=idx_name + 'vector', index_fields=['colorRGBVector VECTOR'],
                                         dimension=3, is_base64=self.base64,
                                         description="IVF,PQ3x8", similarity="L2_SQUARED", bhive_index=self.bhive_index)
            query1 = vector_idx.generate_index_create_query(namespace=namespace, bhive_index=self.bhive_index)
            self.run_cbq_query(query=query1, server=self.n1ql_node)
            drop_query_1 = vector_idx.generate_index_drop_query(namespace=namespace)
            index_drop_list.append(drop_query_1)

        with ThreadPoolExecutor() as executor:
            for query in index_drop_list:
                executor.submit(self.run_cbq_query, query=query)
        self.wait_until_indexes_online(timeout=600)
        index_metadata = self.index_rest.get_indexer_metadata()
        self.assertTrue(len(index_metadata) == 1 or len(index_metadata['status']) == 0, 'Indexes not dropped')

    def test_alter_index_alter_replica_count(self):
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if len(index_nodes) < 2:
            self.skipTest("Can't run Alter index tests with less than 3 Index nodes")
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        select_queries = set()
        namespace_index_map = {}
        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                      prefix='test',
                                                                      similarity=self.similarity, train_list=None,
                                                                      scan_nprobes=self.scan_nprobes,
                                                                      array_indexes=False,
                                                                      limit=self.scan_limit, is_base64=self.base64,
                                                                      quantization_algo_color_vector=self.quantization_algo_color_vector,
                                                                      quantization_algo_description_vector=self.quantization_algo_description_vector,
                                                                      bhive_index=self.bhive_index)
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions, namespace=namespace,
                                                                     num_replica=self.num_index_replica, bhive_index=self.bhive_index)
            select_queries.update(self.gsi_util_obj.get_select_queries(definition_list=definitions,
                                                                       namespace=namespace, limit=self.scan_limit))
            namespace_index_map[namespace] = definitions

            self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, database=namespace)

        self.item_count_related_validations()

        self.display_recall_and_accuracy_stats(select_queries=select_queries,
                                               message="results before reducing num replica count",
                                               stats_assertion=False, similarity=self.similarity)

        index_metadata = self.index_rest.get_indexer_metadata()['status']
        for index in index_metadata:
            self.assertEqual(index['numReplica'], self.num_index_replica, "No. of replicas are not matching")

        for namespace in namespace_index_map:
            definition_list = namespace_index_map[namespace]
            for definitions in definition_list:
                # to reduce the no of replicas
                self.alter_index_replicas(index_name=f"`{definitions.index_name}`", namespace=namespace,
                                          action='replica_count', num_replicas=self.num_index_replica - 1)
                self.wait_until_indexes_online()
                self.sleep(20)

        index_metadata = self.index_rest.get_indexer_metadata()['status']
        for index in index_metadata:
            self.assertEqual(index['numReplica'], self.num_index_replica - 1,
                             "No. of replicas are not matching post alter query")

        self.display_recall_and_accuracy_stats(select_queries=select_queries,
                                               message="results after reducing num replica count", similarity=self.similarity)

        # increasing replica count

        for namespace in namespace_index_map:
            definition_list = namespace_index_map[namespace]
            for definitions in definition_list:
                self.alter_index_replicas(index_name=f"`{definitions.index_name}`", namespace=namespace,
                                          action='replica_count', num_replicas=self.num_index_replica + 1)
                self.wait_until_indexes_online()
                self.sleep(10)

        index_metadata = self.index_rest.get_indexer_metadata()['status']
        for index in index_metadata:
            self.assertEqual(index['numReplica'], self.num_index_replica + 1,
                             "No. of replicas are not matching post alter query")

        self.display_recall_and_accuracy_stats(select_queries=select_queries,
                                               message="results after increasing num replica count", similarity=self.similarity)
        self.drop_index_node_resources_utilization_validations()

    def test_alter_replica_restricted_nodes(self):
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if len(index_nodes) < 2:
            self.skipTest("Can't run Alter index tests with less than  Index nodes")
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        select_queries = set()
        namespace_index_map = {}
        deploy_nodes = [f"{nodes.ip}:{self.node_port}" for nodes in index_nodes[:2]]
        num_replica = len(deploy_nodes) - 1
        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                      prefix='test',
                                                                      similarity=self.similarity, train_list=None,
                                                                      scan_nprobes=self.scan_nprobes,
                                                                      array_indexes=False, is_base64=self.base64,
                                                                      limit=self.scan_limit,
                                                                      quantization_algo_color_vector=self.quantization_algo_color_vector,
                                                                      quantization_algo_description_vector=self.quantization_algo_description_vector,
                                                                      bhive_index=self.bhive_index)
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions, namespace=namespace,
                                                                     deploy_node_info=deploy_nodes,
                                                                     num_replica=num_replica, bhive_index=self.bhive_index)
            select_queries.update(self.gsi_util_obj.get_select_queries(definition_list=definitions,
                                                                       namespace=namespace, limit=self.scan_limit))
            namespace_index_map[namespace] = definitions

            self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, database=namespace)

        self.item_count_related_validations()

        self.display_recall_and_accuracy_stats(select_queries=select_queries,
                                               message="results before moving indexes to specifc node",
                                               stats_assertion=False, similarity=self.similarity)

        replica_node = f"{index_nodes[-1].ip}:{self.node_port}"
        deploy_nodes.append(replica_node)

        index_metadata = self.index_rest.get_indexer_metadata()['status']
        for index in index_metadata:
            self.assertEqual(index['numReplica'], num_replica, "No. of replicas are not matching")

        for namespace in namespace_index_map:
            definition_list = namespace_index_map[namespace]
            for definitions in definition_list:
                self.alter_index_replicas(index_name=f"`{definitions.index_name}`", namespace=namespace,
                                          action='replica_count', num_replicas=num_replica + 1, nodes=deploy_nodes)
                self.sleep(20)
                self.wait_until_indexes_online()

        index_metadata = self.index_rest.get_indexer_metadata()['status']
        for index in index_metadata:
            self.assertEqual(index['numReplica'], num_replica + 1,
                             "No. of replicas are not matching post alter query")
        count = 0
        for index in index_metadata:
            if replica_node in index['hosts']:
                count += 1
        self.assertEqual(count, len(create_queries), f"index not present in the host metadata {index_metadata}")

        self.display_recall_and_accuracy_stats(select_queries=select_queries,
                                               message="results after moving indexes to specifc node", similarity=self.similarity)
        self.drop_index_node_resources_utilization_validations()

    def test_alter_index_alter_replica_id(self):
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if len(index_nodes) < 2:
            self.skipTest("Can't run Alter index tests with less than 2 Index nodes")
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        select_queries = set()
        namespace_index_map = {}
        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                      prefix='test',
                                                                      similarity=self.similarity, train_list=None,
                                                                      scan_nprobes=self.scan_nprobes,
                                                                      array_indexes=False, bhive_index=self.bhive_index,
                                                                      limit=self.scan_limit, is_base64=self.base64,
                                                                      quantization_algo_color_vector=self.quantization_algo_color_vector,
                                                                      quantization_algo_description_vector=self.quantization_algo_description_vector)
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions, namespace=namespace,
                                                                     num_replica=self.num_index_replica,
                                                                     bhive_index=self.bhive_index)
            select_queries.update(self.gsi_util_obj.get_select_queries(definition_list=definitions,
                                                                       namespace=namespace, limit=self.scan_limit))
            namespace_index_map[namespace] = definitions

            self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, database=namespace)

        self.item_count_related_validations()

        self.display_recall_and_accuracy_stats(select_queries=select_queries,
                                               message="results before dropping replica id", stats_assertion=False, similarity=self.similarity)

        index_metadata = self.index_rest.get_indexer_metadata()['status']
        for index in index_metadata:
            self.assertEqual(index['numReplica'], self.num_index_replica, "No. of replicas are not matching")

        for namespace in namespace_index_map:
            definition_list = namespace_index_map[namespace]
            for definitions in definition_list:
                self.alter_index_replicas(index_name=f"`{definitions.index_name}`", namespace=namespace,
                                          action='drop_replica', replica_id=1)
                self.sleep(20)
                self.wait_until_indexes_online()

        self.sleep(60)
        index_metadata = self.index_rest.get_indexer_metadata()['status']
        for index in index_metadata:
            self.assertTrue(index['replicaId'] != 1, f"Dropped wrong replica Id for index {index['indexName']} metadata {index_metadata}")


        # rebalancing in for replica repair
        index_node_in = self.servers[self.nodes_init]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [index_node_in], [],
                                                 services=['index'], cluster_config=self.cluster_config)
        rebalance.result()

        index_metadata = self.index_rest.get_indexer_metadata()['status']
        for index in index_metadata:
            self.assertEqual(index['numReplica'], self.num_index_replica, "No. of replicas are not matching")

        self.item_count_related_validations()

        self.display_recall_and_accuracy_stats(select_queries=select_queries,
                                               message="results before after replica id", similarity=self.similarity)
        self.drop_index_node_resources_utilization_validations()

    def test_alter_move_index(self):
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        select_queries = set()
        namespace_index_map = {}
        if self.multi_move:
            deploy_nodes = [f"{nodes.ip}:{self.node_port}" for nodes in index_nodes[:2]]
            num_replica = 1
        else:
            num_replica = 0
            deploy_nodes = [f"{index_nodes[0].ip}:{self.node_port}"]
        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                      prefix='test',
                                                                      similarity=self.similarity, train_list=None,
                                                                      scan_nprobes=self.scan_nprobes,
                                                                      array_indexes=False,
                                                                      limit=self.scan_limit, is_base64=self.base64,
                                                                      quantization_algo_color_vector=self.quantization_algo_color_vector,
                                                                      quantization_algo_description_vector=self.quantization_algo_description_vector,
                                                                      bhive_index=self.bhive_index)
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions, namespace=namespace,
                                                                     deploy_node_info=deploy_nodes,
                                                                     num_replica=num_replica, bhive_index=self.bhive_index)
            select_queries.update(self.gsi_util_obj.get_select_queries(definition_list=definitions,
                                                                       namespace=namespace, limit=self.scan_limit))
            namespace_index_map[namespace] = definitions

            self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, database=namespace)

        self.item_count_related_validations()

        self.display_recall_and_accuracy_stats(select_queries=select_queries,
                                               message="results before move index via alter query",
                                               stats_assertion=False, similarity=self.similarity)

        if self.multi_move:
            nodes_targetted = [f'{nodes.ip}:{self.node_port}' for nodes in index_nodes[2:]]

        for namespace in namespace_index_map:
            for definitions in namespace_index_map[namespace]:
                if self.multi_move:
                    self.alter_index_replicas(index_name=f"`{definitions.index_name}`", namespace=namespace,
                                              action='move',
                                              nodes=nodes_targetted)
                else:
                    self.alter_index_replicas(index_name=f"`{definitions.index_name}`", namespace=namespace,
                                              action='move',
                                              nodes=[f"{index_nodes[1].ip}:{self.node_port}"])
                self.sleep(60)
                self.wait_until_indexes_online()

        self.wait_until_indexes_online()
        index_info = self.index_rest.get_indexer_metadata()['status']

        for idx in index_info:
            if self.multi_move:
                self.assertIn(idx['hosts'][0], nodes_targetted,
                              f"Replica has not moved into target node meta data : {index_info}")
            else:
                self.assertEqual(index_nodes[1].ip, idx['hosts'][0].split(":")[0],
                                 f"Replica has not moved into target node meta data : {index_info}")

        self.display_recall_and_accuracy_stats(select_queries=select_queries,
                                               message="results after move index via alter query", similarity=self.similarity)
        self.drop_index_node_resources_utilization_validations()

    def test_scan_comparison_between_trained_and_untrained_indexes(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        desc_2 = "A BMW or Mercedes car with high safety rating and fuel efficiency"
        desc_vec2 = list(self.encoder.encode(desc_2))

        scan_desc_vec_2 = f"ANN_DISTANCE(descriptionVector, {desc_vec2}, '{self.similarity}', {self.scan_nprobes})"

        scan_color_vec_1 = f"ANN_DISTANCE(colorRGBVector, [43.0, 133.0, 178.0], '{self.similarity}', {self.scan_nprobes})"
        collection_namespace = self.namespaces[0]
        # indexes with more than one vector field
        trained_index_color_rgb_vector = QueryDefinition(index_name="trained_rgb",
                                                         index_fields=['colorRGBVector VECTOR'],
                                                         dimension=3,
                                                         description=f"IVF,{self.quantization_algo_color_vector}",
                                                         similarity=self.similarity, scan_nprobes=self.scan_nprobes,
                                                         limit=self.scan_limit, is_base64=self.base64,
                                                         query_template=FULL_SCAN_ORDER_BY_TEMPLATE.format(
                                                             f"color, colorRGBVector,"
                                                             f" {scan_color_vec_1}",
                                                             scan_color_vec_1))

        untrained_index_color_rgb_vector = QueryDefinition(index_name="untrained_rgb",
                                                           index_fields=['colorRGBVector VECTOR'],
                                                           dimension=3,
                                                           description=f"IVF,{self.quantization_algo_color_vector}",
                                                           similarity=self.similarity, scan_nprobes=self.scan_nprobes,
                                                           limit=self.scan_limit, is_base64=self.base64,
                                                           query_template=FULL_SCAN_ORDER_BY_TEMPLATE.format(
                                                               f"color, colorRGBVector,"
                                                               f" {scan_color_vec_1}",
                                                               scan_color_vec_1))

        trained_index_description_vector = QueryDefinition(index_name="trained_description",
                                                           index_fields=['descriptionVector VECTOR'],
                                                           dimension=384,
                                                           description=f"IVF,{self.quantization_algo_description_vector}",
                                                           similarity=self.similarity, scan_nprobes=self.scan_nprobes,
                                                           limit=self.scan_limit, is_base64=self.base64,
                                                           query_template=FULL_SCAN_ORDER_BY_TEMPLATE.format(
                                                               f"description, descriptionVector,"
                                                               f" {scan_desc_vec_2}",
                                                               scan_desc_vec_2))

        untrained_index_description_vector = QueryDefinition(index_name="untrained_description",
                                                             index_fields=['descriptionVector VECTOR'],
                                                             dimension=384,
                                                             description=f"IVF,{self.quantization_algo_description_vector}",
                                                             similarity=self.similarity, scan_nprobes=self.scan_nprobes,
                                                             limit=self.scan_limit, is_base64=self.base64,
                                                             query_template=FULL_SCAN_ORDER_BY_TEMPLATE.format(
                                                                 f"description, descriptionVector,"
                                                                 f" {scan_desc_vec_2}",
                                                                 scan_desc_vec_2))

        for idx in [trained_index_color_rgb_vector, untrained_index_color_rgb_vector, trained_index_description_vector,
                    untrained_index_description_vector]:
            if "untrained" in idx.index_name:
                defer_build = True
            else:
                defer_build = False
            query = idx.generate_index_create_query(namespace=collection_namespace, defer_build=defer_build)
            self.run_cbq_query(query=query, server=self.n1ql_node)
        self.wait_until_indexes_online(timeout=600)
        self.sleep(10)
        for query in [trained_index_color_rgb_vector, trained_index_description_vector]:
            select_query_with_explain = f"EXPLAIN {self.gsi_util_obj.get_select_queries(definition_list=[query], namespace=collection_namespace, limit=self.scan_limit)[0]}"
            index_used_select_query = \
                self.run_cbq_query(query=select_query_with_explain)['results'][0]['plan']['~children'][0]['~children'][
                    0][
                    'index']
            self.assertEqual(index_used_select_query, query.index_name, 'trained index not used for scans')
        self.drop_index_node_resources_utilization_validations()

    def test_compare_results_between_partitioned_and_non_partitioned_indexes(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)

        collection_namespace = self.namespaces[0]

        color_vec_2 = [90.0, 33.0, 18.0]
        scan_color_vec_2 = f"ANN_DISTANCE(colorRGBVector, {color_vec_2}, '{self.similarity}', {self.scan_nprobes})"

        desc_2 = "A BMW or Mercedes car with high safety rating and fuel efficiency"
        desc_vec2 = list(self.encoder.encode(desc_2))

        scan_desc_vec_2 = f"ANN_DISTANCE(descriptionVector, {desc_vec2}, '{self.similarity}', {self.scan_nprobes})"

        primary_idx = "CREATE PRIMARY INDEX `#primary123` ON `default`:`test_bucket`.`test_scope_1`.`test_collection_1`;"
        self.run_cbq_query(query=primary_idx, server=self.n1ql_node)

        if self.bhive_index:
            partitioned_index_color_rgb_vector = QueryDefinition(index_name='partitioned_color_rgb_bhive',
                                index_fields=['colorRGBVector VECTOR'],
                                dimension=3, description=f"IVF,{self.quantization_algo_color_vector}", similarity=self.similarity,
                                scan_nprobes=self.scan_nprobes,
                                limit=self.scan_limit, persist_full_vector=False,
                                query_use_index_template=RANGE_SCAN_USE_INDEX_ORDER_BY_TEMPLATE.format("colorRGBVector",
                                                                                   "year > 1980 OR "
                                                                                   "fuel = 'Diesel' ",
                                                                                   scan_color_vec_2),
                                partition_by_fields=['meta().id'], include_fields=['fuel', 'year'],train_list=self.trainlist)
            non_partitioned_index_color_rgb_vector = QueryDefinition(index_name='non_partitioned_color_rgb_bhive',
                                                                 index_fields=['colorRGBVector VECTOR'],
                                                                 dimension=3,
                                                                 description=f"IVF,{self.quantization_algo_color_vector}",
                                                                 similarity=self.similarity,
                                                                 scan_nprobes=self.scan_nprobes,
                                                                 limit=self.scan_limit, persist_full_vector=False,
                                                                 query_use_index_template=RANGE_SCAN_USE_INDEX_ORDER_BY_TEMPLATE.format(
                                                                     "colorRGBVector",
                                                                     "year > 1980 OR "
                                                                     "fuel = 'Diesel' ",
                                                                     scan_color_vec_2),
                                                                 include_fields=['fuel', 'year'],train_list=self.trainlist)
            partitioned_index_description_vector = QueryDefinition(index_name='partitioned_description_bhive',
                            index_fields=['descriptionVector VECTOR'],
                            dimension=384, description=f"IVF,{self.quantization_algo_description_vector}",
                            similarity=self.similarity, scan_nprobes=self.scan_nprobes,
                            limit=self.scan_limit,
                            query_use_index_template=RANGE_SCAN_USE_INDEX_ORDER_BY_TEMPLATE.format(f"descriptionVector",
                                                                               "rating = 2 and "
                                                                               "category in ['Convertible', "
                                                                               "'Luxury Car', 'Supercar']",
                                                                               scan_desc_vec_2),
                            partition_by_fields=['meta().id'], include_fields=['rating', 'category'],train_list=self.trainlist
                            )

            non_partitioned_index_description_vector = QueryDefinition(index_name='non_partitioned_description_bhive',
                                                                   index_fields=['descriptionVector VECTOR'],
                                                                   dimension=384,
                                                                   description=f"IVF,{self.quantization_algo_description_vector}",
                                                                   similarity=self.similarity,
                                                                   scan_nprobes=self.scan_nprobes,
                                                                   limit=self.scan_limit,
                                                                   query_use_index_template=RANGE_SCAN_USE_INDEX_ORDER_BY_TEMPLATE.format(
                                                                       f"descriptionVector",
                                                                       "rating = 2 and "
                                                                       "category in ['Convertible', "
                                                                       "'Luxury Car', 'Supercar']",
                                                                       scan_desc_vec_2),
                                                                   include_fields=['rating', 'category'],train_list=self.trainlist
                                                                   )
        else:
            partitioned_index_color_rgb_vector = QueryDefinition(index_name='partitioned_color',
                            index_fields=['rating', 'colorRGBVector Vector', 'category'],
                            dimension=3, description=f"IVF,{self.quantization_algo_color_vector}",
                            similarity=self.similarity, scan_nprobes=self.scan_nprobes,
                            query_use_index_template=RANGE_SCAN_USE_INDEX_ORDER_BY_TEMPLATE.format(f"color, colorRGBVector",
                                                                               "rating = 2 and "
                                                                               "category in ['Convertible', "
                                                                               "'Luxury Car', 'Supercar']",
                                                                               scan_color_vec_2),
                            partition_by_fields=['meta().id']
                            )

            non_partitioned_index_color_rgb_vector = QueryDefinition(index_name='non_partitioned_color',
                            index_fields=['rating', 'colorRGBVector Vector', 'category'],
                            dimension=3, description=f"IVF,{self.quantization_algo_color_vector}",
                            similarity=self.similarity, scan_nprobes=self.scan_nprobes,
                            query_use_index_template=RANGE_SCAN_USE_INDEX_ORDER_BY_TEMPLATE.format(f"color, colorRGBVector",
                                                                               "rating = 2 and "
                                                                               "category in ['Convertible', "
                                                                               "'Luxury Car', 'Supercar']",
                                                                               scan_color_vec_2)
                            )

            partitioned_index_description_vector = QueryDefinition("partitioned_descriptionVector",
                                                               index_fields=['rating', 'descriptionVector Vector',
                                                                             'category'],
                                                               dimension=384,
                                                               description=f"IVF,{self.quantization_algo_description_vector}",
                                                               similarity=self.similarity,
                                                               scan_nprobes=self.scan_nprobes,
                                                               limit=self.scan_limit, is_base64=self.base64,
                                                               query_use_index_template=RANGE_SCAN_USE_INDEX_ORDER_BY_TEMPLATE.format(
                                                                   "descriptionVector",
                                                                   "rating = 2 and "
                                                                   "category in ['Convertible', "
                                                                   "'Luxury Car', 'Supercar']",
                                                                   scan_desc_vec_2),
                                                               partition_by_fields=['meta().id'],
                                                               bhive_index=self.bhive_index
                                                               )

            non_partitioned_index_description_vector = QueryDefinition("non_partitioned_descriptionVector",
                                                               index_fields=['rating', 'descriptionVector Vector',
                                                                             'category'],
                                                               dimension=384,
                                                               description=f"IVF,{self.quantization_algo_description_vector}",
                                                               similarity=self.similarity,
                                                               scan_nprobes=self.scan_nprobes,
                                                               limit=self.scan_limit, is_base64=self.base64,
                                                               query_use_index_template=RANGE_SCAN_USE_INDEX_ORDER_BY_TEMPLATE.format(
                                                                   "descriptionVector",
                                                                   "rating = 2 and "
                                                                   "category in ['Convertible', "
                                                                   "'Luxury Car', 'Supercar']",
                                                                   scan_desc_vec_2),
                                                               bhive_index=self.bhive_index
                                                               )

        message = f"quantization value is {self.quantization_algo_description_vector}"
        select_queries = []
        for idx in [partitioned_index_color_rgb_vector, non_partitioned_index_color_rgb_vector,
                    partitioned_index_description_vector, non_partitioned_index_description_vector]:
            create_query = idx.generate_index_create_query(namespace=collection_namespace, bhive_index=self.bhive_index)
            self.run_cbq_query(query=create_query, server=self.n1ql_node)
            select_query = self.gsi_util_obj.get_select_queries(definition_list=[idx],
                                                                namespace=collection_namespace,
                                                                index_name=idx.index_name)[0]
            select_queries.append(select_query)
        self.item_count_related_validations()

        self.display_recall_and_accuracy_stats(select_queries=select_queries, message=message, similarity=self.similarity)
        self.drop_index_node_resources_utilization_validations()

    def test_replica_repair(self):
        index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        self.memory_fill = self.input.param("memory_fill", False)
        if self.memory_fill:
            self.install_tools()
        self.enable_shard_based_rebalance()
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        select_queries = set()
        namespace_index_map = {}
        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                      prefix='test',
                                                                      similarity=self.similarity, train_list=None,
                                                                      scan_nprobes=self.scan_nprobes,
                                                                      array_indexes=False,
                                                                      limit=self.scan_limit, is_base64=self.base64,
                                                                      quantization_algo_color_vector=self.quantization_algo_color_vector,
                                                                      quantization_algo_description_vector=self.quantization_algo_description_vector, bhive_index=self.bhive_index)
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions, namespace=namespace,
                                                                     num_replica=self.num_index_replica,
                                                                     defer_build=True, bhive_index=self.bhive_index)
            select_queries.update(self.gsi_util_obj.get_select_queries(definition_list=definitions,
                                                                       namespace=namespace, limit=self.scan_limit))
            build_query = self.gsi_util_obj.get_build_indexes_query(definition_list=definitions, namespace=namespace)
            namespace_index_map[namespace] = definitions

            self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, database=namespace)
        self.wait_until_indexes_online(defer_build=True)

        # rebalancing out an indexer node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [index_node[0]],
                                                 services=['index'], cluster_config=self.cluster_config)
        rebalance.result()

        self.sleep(30)

        self.run_cbq_query(query=build_query, server=self.n1ql_node)
        self.wait_until_indexes_online()

        if self.memory_fill:
            self.run_stress_tool(timeout=600)
            self.sleep(30)
        try:
            # rebalancing in indexer node
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [index_node[0]], [],
                                                     services=['index'], cluster_config=self.cluster_config)
            rebalance.result()

            index_metadata = self.index_rest.get_indexer_metadata()['status']
            if self.memory_fill:
                for index in index_metadata:
                    self.assertNotEqual(index['numReplica'], self.num_index_replica, "No. of replicas are not matching")
            else:
                for index in index_metadata:
                    self.assertEqual(index['numReplica'], self.num_index_replica, "No. of replicas are not matching")
            self.item_count_related_validations()
            self.drop_index_node_resources_utilization_validations()
        except Exception as ex:
            raise ex
        finally:
            self.kill_stress_tool()

    def test_vector_indexes_after_bucket_flush(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename,
                                      skip_default_scope=self.skip_default)
        self.rest.change_bucket_props(self.buckets[0], flushEnabled=1)

        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        select_queries = []
        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                      similarity=self.similarity, train_list=None,
                                                                      scan_nprobes=self.scan_nprobes,
                                                                      array_indexes=False,
                                                                      limit=self.scan_limit, is_base64=self.base64,
                                                                      quantization_algo_description_vector=self.quantization_algo_description_vector,
                                                                      quantization_algo_color_vector=self.quantization_algo_color_vector, bhive_index=self.bhive_index)
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions, namespace=namespace,
                                                                     num_replica=1, bhive_index=self.bhive_index)
            self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, query_node=query_node)

            select_queries.extend(
                self.gsi_util_obj.get_select_queries(definition_list=definitions, namespace=namespace))

        self.item_count_related_validations()

        for select_query in select_queries:
            if "ANN_DISTANCE" not in select_query:
                continue
            self.validate_scans_for_recall_and_accuracy(select_query=select_query)

        for bucket in self.buckets:
            try:
                self.rest.flush_bucket(bucket=bucket)
            except Exception as ex:
                if "unable to flush bucket" not in str(ex):
                    self.fail("flushing bucket failed with unexpected error message")
        self.sleep(60)
        #adding validations for item count post flushing of bucket
        _, stats = self._return_maps(perNode=True, map_from_index_nodes=True)
        index_item_count_map = {}
        for node in stats:
            for namespace in stats[node]:
                for index in stats[node][namespace]:
                    if index not in index_item_count_map:
                        index_item_count_map[index] = stats[node][namespace][index]["items_count"]
                    else:
                        index_item_count_map[index] += stats[node][namespace][index]["items_count"]
        for index in index_item_count_map:
            self.assertEqual(index_item_count_map[index], 0, f"stats {stats}")

        try:
            # Running the scan on an empty bucket
            for query in select_queries:
                self.run_cbq_query(query=query, server=query_node)
        except Exception as err:
            self.log.error(err)

        # restoring and running queries again to validate recall percentage
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename,
                                      skip_default_scope=self.skip_default)
        self.sleep(300)
        # adding validations for item count post recovery of bucket
        _, stats = self._return_maps(perNode=True, map_from_index_nodes=True)
        index_item_count_map = {}
        for node in stats:
            for namespace in stats[node]:
                for index in stats[node][namespace]:
                    if index not in index_item_count_map:
                        index_item_count_map[index] = stats[node][namespace][index]["items_count"]
                    else:
                        index_item_count_map[index] += stats[node][namespace][index]["items_count"]
        for index in index_item_count_map:
            if 'partial' not in index:
                self.assertEqual(index_item_count_map[index], self.num_of_docs_per_collection, f"stats {stats}")


        for select_query in select_queries:
            if "ANN_DISTANCE" not in select_query:
                continue
            self.validate_scans_for_recall_and_accuracy(select_query=select_query)
        self.drop_index_node_resources_utilization_validations()

    def test_recover_from_disk_snapshot(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename,
                                      skip_default_scope=self.skip_default)

        setting = {"indexer.settings.persisted_snapshot.moi.interval": 60000}
        self.index_rest.set_index_settings(setting)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        data_node = self.get_nodes_from_services_map(service_type="kv")
        select_queries = []
        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                      similarity=self.similarity, train_list=None,
                                                                      scan_nprobes=self.scan_nprobes,
                                                                      array_indexes=False, bhive_index=self.bhive_index,
                                                                      limit=self.scan_limit, is_base64=self.base64,
                                                                      quantization_algo_description_vector=self.quantization_algo_description_vector,
                                                                      quantization_algo_color_vector=self.quantization_algo_color_vector)
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions, namespace=namespace,
                                                                     num_replica=1, bhive_index=self.bhive_index)
            self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, query_node=query_node)

            select_queries.extend(
                self.gsi_util_obj.get_select_queries(definition_list=definitions, namespace=namespace))

        self.item_count_related_validations()
        index_stats = self.index_rest.get_index_stats()


        for select_query in select_queries:
            if "ANN_DISTANCE" not in select_query:
                continue
            self.validate_scans_for_recall_and_accuracy(select_query=select_query)

        doc_count = {}
        for namespace in self.namespaces:
            count_query = f"select count(year) from {namespace} where year > 0;"
            result = self.run_cbq_query(query=count_query, server=query_node)['results'][0]["$1"]
            doc_count[namespace] = result
        # changing the interval to 20 mins
        self.log.info(f"Doc count before recovery: {doc_count}")
        setting = {"indexer.settings.persisted_snapshot.moi.interval": 1200000}
        self.index_rest.set_index_settings(setting)
        disk_snapshots = MultilevelDict()
        for bucket, indexes in index_stats.items():
            for index, stats in indexes.items():
                for key, val in stats.items():
                    if 'num_disk_snapshots' in key:
                        disk_snapshots[bucket][index][key] = val

        # Loading new documents so that persisted snapshot wouldn't have these docs
        for namespace in self.namespaces:
            keyspace = namespace.split(":")[-1]
            bucket, scope, collection = keyspace.split(".")
            self.gen_update = SDKDataLoader(num_ops=self.num_of_docs_per_collection, percent_create=100,
                                            percent_update=0, percent_delete=0, scope=scope,
                                            collection=collection, json_template="Cars", key_prefix="new_doc", create_start=self.num_of_docs_per_collection,
                                            create_end=self.num_of_docs_per_collection * 2)

            task = self.cluster.async_load_gen_docs(data_node, bucket=bucket,
                                                    generator=self.gen_update,
                                                    timeout_secs=300, use_magma_loader=True)
            task.result()
            # self.load_docs_via_magma_server(server=data_node, bucket=bucket, gen=self.gen_update)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        for index_node in index_nodes:
            remote_client = RemoteMachineShellConnection(index_node)
            remote_client.terminate_process(process_name='indexer')
        self.sleep(30)
        index_stats = self.index_rest.get_index_stats()
        docs_pending = [f"{bucket}.{index}.{key}:{val}" for bucket, indexes in index_stats.items()
                        for index, stats in indexes.items()
                        for key, val in stats.items() if 'num_docs_pending' in key]
        self.log.info(f"Docs Pending after indexer kill: {docs_pending}")
        for namespace in self.namespaces:
            count_query = f"select count(year) from {namespace} where year > 0;"
            result = self.run_cbq_query(query=count_query, server=query_node)['results'][0]["$1"]
            self.log.info(f"Doc count after recovery for namespace {namespace}: {result}")
            self.assertEqual(result, doc_count[namespace] + self.num_of_docs_per_collection,
                             "Index hasn't recovered the docs within the given time")
        self.drop_index_node_resources_utilization_validations()

    def test_partial_rollback(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename)
        self.sleep(30)
        select_queries = set()
        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                      prefix='test',
                                                                      similarity=self.similarity, train_list=None,
                                                                      scan_nprobes=self.scan_nprobes,
                                                                      array_indexes=False, bhive_index=self.bhive_index,
                                                                      limit=self.scan_limit, is_base64=self.base64,
                                                                      quantization_algo_color_vector=self.quantization_algo_color_vector,
                                                                      quantization_algo_description_vector=self.quantization_algo_description_vector)
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions, namespace=namespace,
                                                                     num_replica=self.num_index_replica,
                                                                     bhive_index=self.bhive_index)
            select_queries.update(self.gsi_util_obj.get_select_queries(definition_list=definitions,
                                                                       namespace=namespace, limit=self.scan_limit))

            self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, database=namespace,
                                                 query_node=self.n1ql_node)

        self.item_count_related_validations()

        self.log.info("Stopping persistence on NodeA & NodeB")
        data_nodes = self.get_nodes_from_services_map(service_type="kv",
                                                      get_all_nodes=True)
        for data_node in data_nodes:
            for bucket in self.buckets:
                mem_client = MemcachedClientHelper.direct_client(data_node, bucket.name)
                mem_client.stop_persistence()
        self.sleep(10)

        for namespace in self.namespaces:
            bucket, scope, collection = namespace.split('.')
            bucket = bucket.split(':')[-1]

            self.gen_create = SDKDataLoader(num_ops=self.num_of_docs_per_collection, percent_create=100,
                                            percent_update=0, percent_delete=0, scope=scope,
                                            collection=collection, json_template="Cars",
                                            output=True, username=self.username, password=self.password,
                                            base64=self.base64,
                                            model=self.data_model, workers=10, timeout=1500, key_prefix="doc_77",
                                            create_start=0,
                                            create_end=10000, dim=384)

            # self.load_docs_via_magma_server(server=data_nodes[0], bucket=bucket, gen=self.gen_create)
            task = self.cluster.async_load_gen_docs(data_nodes[0], bucket=bucket,
                                                    generator=self.gen_create,
                                                    timeout_secs=1500, use_magma_loader=True)
            task.result()
            self.sleep(120)

        self.sleep(30)
        # Get count before rollback
        bucket_before_item_counts = {}
        for bucket in self.buckets:
            bucket_count_before_rollback = self.get_item_count(self.master, bucket.name)
            bucket_before_item_counts[bucket.name] = bucket_count_before_rollback
            self.log.info("Items in bucket {0} before rollback = {1}".format(
                bucket.name, bucket_count_before_rollback))

        # Index rollback count before rollback
        self._verify_bucket_count_with_index_count()

        # Kill memcached on Node A so that Node B becomes master
        self.log.info("Kill Memcached process on NodeA")
        shell = RemoteMachineShellConnection(data_nodes[0])
        shell.kill_memcached()

        # Start persistence on Node B
        self.log.info("Starting persistence on NodeB")
        for bucket in self.buckets:
            mem_client = MemcachedClientHelper.direct_client(data_nodes[1], bucket.name)
            mem_client.start_persistence()

        # Failover Node B
        self.log.info("Failing over NodeB")
        self.sleep(10)
        failover_task = self.cluster.async_failover(
            self.servers[:self.nodes_init], [data_nodes[1]], self.graceful,
            wait_for_pending=120)

        failover_task.result()

        # Wait for a couple of mins to allow rollback to complete
        # self.sleep(120)

        bucket_after_item_counts = {}
        for bucket in self.buckets:
            bucket_count_after_rollback = self.get_item_count(self.master, bucket.name)
            bucket_after_item_counts[bucket.name] = bucket_count_after_rollback
            self.log.info("Items in bucket {0} after rollback = {1}".format(
                bucket.name, bucket_count_after_rollback))

        for bucket in self.buckets:
            if bucket_after_item_counts[bucket.name] == bucket_before_item_counts[bucket.name]:
                self.log.info("Looks like KV rollback did not happen at all.")
        self._verify_bucket_count_with_index_count()
        self.display_recall_and_accuracy_stats(select_queries=select_queries,
                                               message="results after doing a partial roll back from kv side", similarity=self.similarity)
        self.drop_index_node_resources_utilization_validations()

    def test_recovery_post_deleting_recovery_files(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename,
                                      skip_default_scope=self.skip_default)

        setting = {"indexer.settings.persisted_snapshot.moi.interval": 60000}
        self.index_rest.set_index_settings(setting)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        select_queries = []
        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                      similarity=self.similarity, train_list=None,
                                                                      scan_nprobes=self.scan_nprobes,
                                                                      array_indexes=False, bhive_index=self.bhive_index,
                                                                      limit=self.scan_limit, is_base64=self.base64,
                                                                      quantization_algo_description_vector=self.quantization_algo_description_vector,
                                                                      quantization_algo_color_vector=self.quantization_algo_color_vector)
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions, namespace=namespace,
                                                                     num_replica=self.num_index_replica, bhive_index=self.bhive_index)
            self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, query_node=query_node)

            select_queries.extend(
                self.gsi_util_obj.get_select_queries(definition_list=definitions, namespace=namespace))


        self.item_count_related_validations()
        # deleting recovery files
        storage_dir = self.index_rest.get_indexer_internal_stats()['indexer.storage_dir']
        for index_node in index_nodes:
            remote = RemoteMachineShellConnection(index_node)
            remote.execute_command(
                command=f"find {storage_dir} -mindepth 1 -maxdepth 1 -type d ! -name \"*primary*\" -exec bash -c 'cd {{}}/mainIndex/recovery 2>/dev/null && ls -t | head -n 1 | xargs -I {{}} rm -f {{}}' \;")
            remote.disconnect()

        self.sleep(10)

        for index_node in index_nodes:
            self.kill_indexer(index_node=index_node)
            self.sleep(10)

        self.wait_until_indexes_online()

        # adding validations for item count post recovery of bucket
        partial_index_list = self.get_partial_indexes_name_list()
        _, stats = self._return_maps(perNode=True, map_from_index_nodes=True)
        index_item_count_map = {}
        for node in stats:
            for namespace in stats[node]:
                for index in stats[node][namespace]:
                    if index not in index_item_count_map:
                        index_item_count_map[index] = stats[node][namespace][index]["items_count"]
                    else:
                        index_item_count_map[index] += stats[node][namespace][index]["items_count"]
        for index in index_item_count_map:
            if index in partial_index_list:
                continue
            self.assertEqual(index_item_count_map[index], self.num_of_docs_per_collection, f"stats {stats}")

        #log validation for recovery of bhive indexes
        is_log_validation = self.check_gsi_logs_for_shard_transfer(log_string=self.bhive_recovery_log_string, msg="bhive recovery point")
        self.assertTrue(is_log_validation, "bhive recovery point not found in logs")
        self.drop_index_node_resources_utilization_validations()

    def test_crash_indexer_data_nodes(self):
        self.kill_parallel = self.input.param("kill_parallel", False)
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename,
                                      skip_default_scope=self.skip_default)

        setting = {"indexer.settings.persisted_snapshot.moi.interval": 60000}
        self.index_rest.set_index_settings(setting)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        data_nodes = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        select_queries = []
        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                      similarity=self.similarity, train_list=None,
                                                                      scan_nprobes=self.scan_nprobes,
                                                                      array_indexes=False, bhive_index=self.bhive_index,
                                                                      limit=self.scan_limit, is_base64=self.base64,
                                                                      quantization_algo_description_vector=self.quantization_algo_description_vector,
                                                                      quantization_algo_color_vector=self.quantization_algo_color_vector)
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions, namespace=namespace,
                                                                     num_replica=self.num_index_replica, bhive_index=self.bhive_index)
            self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, query_node=query_node)

            select_queries.extend(
                self.gsi_util_obj.get_select_queries(definition_list=definitions, namespace=namespace))

        self.item_count_related_validations()


        if self.kill_parallel:
            with ThreadPoolExecutor() as executor:
                for i in range(1, 11):
                    futures = [executor.submit(self.kill_indexer, node) for node in index_nodes]
                    futures.extend([executor.submit(self.kill_memcached, node) for node in data_nodes])

                for future in futures:
                    future.result()

        else:
            for i in range(1, 11):
                for index_node in index_nodes:
                    self.kill_indexer(index_node=index_node)
                    self.sleep(30)

            for i in range(1, 11):
                for data_node in data_nodes:
                    self.kill_memcached(data_node=data_node)

        self.wait_until_indexes_online()

        # adding validations for item count post recovery of bucket
        partial_index_list = self.get_partial_indexes_name_list()
        _, stats = self._return_maps(perNode=True, map_from_index_nodes=True)
        index_item_count_map = {}
        for node in stats:
            for namespace in stats[node]:
                for index in stats[node][namespace]:
                    if index not in index_item_count_map:
                        index_item_count_map[index] = stats[node][namespace][index]["items_count"]
                    else:
                        index_item_count_map[index] += stats[node][namespace][index]["items_count"]
        for index in index_item_count_map:
            if index in partial_index_list:
                continue
            self.assertEqual(index_item_count_map[index], self.num_of_docs_per_collection, f"stats {stats}")

        #log validation for recovery of bhive indexes
        is_log_validation = self.check_gsi_logs_for_shard_transfer(log_string=self.bhive_recovery_log_string, msg="bhive recovery point")
        self.assertTrue(is_log_validation, "bhive recovery point not found in logs")
        self.drop_index_node_resources_utilization_validations()

    def test_recovery_projector_crash(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename,
                                    skip_default_scope=self.skip_default)

        setting = {"indexer.settings.persisted_snapshot.moi.interval": 60000}
        self.index_rest.set_index_settings(setting)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        data_nodes = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        select_queries = []
        data_node = self.get_nodes_from_services_map(service_type="kv")
        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                    similarity=self.similarity, train_list=None,
                                                                    scan_nprobes=self.scan_nprobes,
                                                                    array_indexes=False, bhive_index=self.bhive_index,
                                                                    limit=self.scan_limit, is_base64=self.base64,
                                                                    quantization_algo_description_vector=self.quantization_algo_description_vector,
                                                                    quantization_algo_color_vector=self.quantization_algo_color_vector)
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions, namespace=namespace,
                                                                    num_replica=1, bhive_index=self.bhive_index)
            self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, query_node=query_node)

            select_queries.extend(
                self.gsi_util_obj.get_select_queries(definition_list=definitions, namespace=namespace))
        self.item_count_related_validations()
        index_stats = self.index_rest.get_index_stats()

        for select_query in select_queries:
            if "ANN_DISTANCE" not in select_query:
                continue
            self.validate_scans_for_recall_and_accuracy(select_query=select_query)

        doc_count = {}
        for namespace in self.namespaces:
            count_query = f"select count(year) from {namespace} where year > 0;"
            result = self.run_cbq_query(query=count_query, server=query_node)['results'][0]["$1"]
            doc_count[namespace] = result
        self.log.info(f"Doc count before recovery: {doc_count}")
        # changing the interval to 10 mins
        setting = {"indexer.settings.persisted_snapshot.moi.interval": 600000}
        self.index_rest.set_index_settings(setting)
        disk_snapshots = MultilevelDict()
        for bucket, indexes in index_stats.items():
            for index, stats in indexes.items():
                for key, val in stats.items():
                    if 'num_disk_snapshots' in key:
                        disk_snapshots[bucket][index][key] = val
        # Loading new documents so that persisted snapshot wouldn't have these docs
        for namespace in self.namespaces:
            keyspace = namespace.split(":")[-1]
            bucket, scope, collection = keyspace.split(".")
            self.gen_update = SDKDataLoader(num_ops=self.num_of_docs_per_collection, percent_create=100,
                                            percent_update=0, percent_delete=0, scope=scope,
                                            collection=collection, json_template="Cars", key_prefix="new_doc", create_start=self.num_of_docs_per_collection,
                                            create_end=self.num_of_docs_per_collection * 2)
            task = self.cluster.async_load_gen_docs(data_node, bucket=bucket,
                                                    generator=self.gen_update,
                                                    timeout_secs=300, use_magma_loader=True)
            task.result()
        data_nodes = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        for data_node in data_nodes:
            remote_client = RemoteMachineShellConnection(data_node)
            remote_client.terminate_process(process_name='projector')
        self.sleep(30)


        index_stats = self.index_rest.get_index_stats()
        docs_pending = [f"{bucket}.{index}.{key}:{val}" for bucket, indexes in index_stats.items()
                        for index, stats in indexes.items()
                        for key, val in stats.items() if 'num_docs_pending' in key]
        self.log.info(f"Docs Pending after projector kill: {docs_pending}")
        for namespace in self.namespaces:
            count_query = f"select count(year) from {namespace} where year > 0;"
            result = self.run_cbq_query(query=count_query, server=query_node)['results'][0]["$1"]
            self.log.info(f"Doc count after recovery for namespace {namespace}: {result}")
            self.assertEqual(result, doc_count[namespace] + self.num_of_docs_per_collection,
                            "Index hasn't recovered the docs within the given time")
        self.drop_index_node_resources_utilization_validations()

    def test_recovery_memcached_crash(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename,
                                    skip_default_scope=self.skip_default)

        setting = {"indexer.settings.persisted_snapshot.moi.interval": 60000}
        self.index_rest.set_index_settings(setting)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        data_nodes = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        select_queries = []
        data_node = self.get_nodes_from_services_map(service_type="kv")
        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                    similarity=self.similarity, train_list=None,
                                                                    scan_nprobes=self.scan_nprobes,
                                                                    array_indexes=False, bhive_index=self.bhive_index,
                                                                    limit=self.scan_limit, is_base64=self.base64,
                                                                    quantization_algo_description_vector=self.quantization_algo_description_vector,
                                                                    quantization_algo_color_vector=self.quantization_algo_color_vector)
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions, namespace=namespace,
                                                                    num_replica=1, bhive_index=self.bhive_index)
            self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, query_node=query_node)

            select_queries.extend(
                self.gsi_util_obj.get_select_queries(definition_list=definitions, namespace=namespace))
        self.item_count_related_validations()
        index_stats = self.index_rest.get_index_stats()

        for select_query in select_queries:
            if "ANN_DISTANCE" not in select_query:
                continue
            self.validate_scans_for_recall_and_accuracy(select_query=select_query)

        doc_count = {}
        for namespace in self.namespaces:
            count_query = f"select count(year) from {namespace} where year > 0;"
            result = self.run_cbq_query(query=count_query, server=query_node)['results'][0]["$1"]
            doc_count[namespace] = result
        self.log.info(f"Doc count before recovery: {doc_count}")
        # changing the interval to 10 mins
        setting = {"indexer.settings.persisted_snapshot.moi.interval": 600000}
        self.index_rest.set_index_settings(setting)
        disk_snapshots = MultilevelDict()
        for bucket, indexes in index_stats.items():
            for index, stats in indexes.items():
                for key, val in stats.items():
                    if 'num_disk_snapshots' in key:
                        disk_snapshots[bucket][index][key] = val

        # Loading new documents so that persisted snapshot wouldn't have these docs
        for namespace in self.namespaces:
            keyspace = namespace.split(":")[-1]
            bucket, scope, collection = keyspace.split(".")
            self.gen_update = SDKDataLoader(num_ops=self.num_of_docs_per_collection, percent_create=100,
                                            percent_update=0, percent_delete=0, scope=scope,
                                            collection=collection, json_template="Cars", key_prefix="new_doc", create_start=self.num_of_docs_per_collection,
                                            create_end=self.num_of_docs_per_collection * 2)
            task = self.cluster.async_load_gen_docs(data_node, bucket=bucket,
                                                    generator=self.gen_update,
                                                    timeout_secs=300, use_magma_loader=True)
            task.result()
        data_nodes = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        for data_node in data_nodes:
            remote_client = RemoteMachineShellConnection(data_node)
            remote_client.terminate_process(process_name='memcached')
        self.sleep(30)

        index_stats = self.index_rest.get_index_stats()
        docs_pending = [f"{bucket}.{index}.{key}:{val}" for bucket, indexes in index_stats.items()
                        for index, stats in indexes.items()
                        for key, val in stats.items() if 'num_docs_pending' in key]
        self.log.info(f"Docs Pending after memcached kill: {docs_pending}")

        # check if any rollback has happened
        num_rollback = self.index_rest.get_num_rollback_stat(bucket=self.buckets[0].name)
        self.assertGreaterEqual(num_rollback, 0, "No rollback has happened")
        for namespace in self.namespaces:
            count_query = f"select count(year) from {namespace} where year > 0;"
            result = self.run_cbq_query(query=count_query, server=query_node)['results'][0]["$1"]
            self.log.info(f"Doc count after recovery for namespace {namespace}: {result}")
            self.assertEqual(result, doc_count[namespace] + self.num_of_docs_per_collection,
                            "Index hasn't recovered the docs within the given time")
        self.drop_index_node_resources_utilization_validations()

    def test_crash_and_recovery_scenarios_during_index_building(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename,
                                    skip_default_scope=self.skip_default)

        setting = {"indexer.settings.persisted_snapshot.moi.interval": 60000}
        self.index_rest.set_index_settings(setting)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        data_nodes = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        select_queries = []
        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                    similarity=self.similarity, train_list=None,
                                                                    scan_nprobes=self.scan_nprobes,
                                                                    array_indexes=False, bhive_index=self.bhive_index,
                                                                    limit=self.scan_limit, is_base64=self.base64,
                                                                    quantization_algo_description_vector=self.quantization_algo_description_vector,
                                                                    quantization_algo_color_vector=self.quantization_algo_color_vector)
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions, namespace=namespace,
                                                                    defer_build=True, num_replica=1,
                                                                    bhive_index=self.bhive_index)
            self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, query_node=query_node)
            build_query = self.gsi_util_obj.get_build_indexes_query(definition_list=definitions, namespace=namespace)
            select_queries.extend(
                self.gsi_util_obj.get_select_queries(definition_list=definitions, namespace=namespace))


        self.run_cbq_query(query=build_query, server=self.n1ql_node)

        timeout = 0
        while timeout < 360:
            index_state = self.index_rest.get_indexer_metadata()['status'][2]['status']
            if index_state == self.build_phase:
                break
            self.sleep(5)
            timeout = timeout + 1

        if self.target_process == "memcached":
            remote = RemoteMachineShellConnection(data_nodes[1])
            remote.kill_memcached()
        elif self.target_process == "projector":
            remote = RemoteMachineShellConnection(data_nodes[1])
            remote.terminate_process(process_name=self.targetProcess)
        elif self.target_process == "indexer":
            remote = RemoteMachineShellConnection(index_nodes[0])
            remote.terminate_process(process_name=self.targetProcess)
        else:
            for index_node in index_nodes:
                remote = RemoteMachineShellConnection(index_node)
                remote.stop_server()
            self.sleep(30)
            for index_node in index_nodes:
                remote = RemoteMachineShellConnection(index_node)
                remote.start_server()

        self.wait_until_indexes_online()

        for namespace in self.namespaces:
            keyspace = namespace.split(":")[-1]
            bucket, scope, collection = keyspace.split(".")
            self.gen_create = SDKDataLoader(num_ops=self.num_of_docs_per_collection, percent_create=100,
                                            percent_update=0, percent_delete=0, workers=16, scope=scope,
                                            collection=collection, json_template="Cars", timeout=2000,
                                            op_type="create", dim=384,
                                            create_start=self.num_of_docs_per_collection,
                                            create_end=(self.num_of_docs_per_collection +
                                                        self.num_of_docs_per_collection // 10))
            # self.load_docs_via_magma_server(server=data_nodes[0], bucket=bucket, gen=self.gen_create)
            task = self.cluster.async_load_gen_docs(data_nodes[0], bucket=bucket,
                                                    generator=self.gen_create,
                                                    timeout_secs=2000, use_magma_loader=True)
            task.result()

        # verify index count matches bucket item count
        self._verify_bucket_count_with_index_count()

        query_stats_map = {}
        for query in select_queries:
            if "ANN_DISTANCE" not in query:
                continue
            query, recall, accuracy = self.validate_scans_for_recall_and_accuracy(select_query=query)
            query_stats_map[query] = [recall, accuracy]
        self.gen_table_view(query_stats_map=query_stats_map,
                            message=f"quantization value is {self.quantization_algo_description_vector}")
        self.drop_index_node_resources_utilization_validations()

    def test_create_index_without_vector_data(self):
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                             num_of_docs_per_collection=self.num_of_docs_per_collection,
                                             json_template=self.json_template,
                                             load_default_coll=False)

        collection_namespace = self.namespaces[0]

        data_node = self.get_nodes_from_services_map(service_type="kv")

        # Building vector indexes without qualifying
        vector_idx = QueryDefinition(index_name='vector', index_fields=['colorRGBVector VECTOR'], dimension=3,
                                     description="IVF,PQ3x8", similarity="L2_SQUARED", is_base64=self.base64)
        query = vector_idx.generate_index_create_query(namespace=collection_namespace, defer_build=True)

        self.run_cbq_query(query=query, server=self.n1ql_node)
        # building index with data not qualified for vector indexes

        query = f"BUILD INDEX ON {collection_namespace}(vector) USING GSI "
        self.run_cbq_query(query=query, server=self.n1ql_node)
        status = self.wait_until_indexes_online(timeout=60)
        self.assertFalse(status, "Index created successfully")

        #Loading valid docs for vector indexes
        for namespace in self.namespaces:
            _, keyspace = namespace.split(':')
            bucket, scope, collection = keyspace.split('.')
            bucket = bucket.split(':')[-1]

            self.gen_create = SDKDataLoader(num_ops=self.num_of_docs_per_collection, percent_create=100,
                                            percent_update=0, percent_delete=0, scope=scope,
                                            collection=collection, json_template="Cars",
                                            output=True, username=self.username, password=self.password,
                                            base64=self.base64,
                                            model=self.data_model, workers=10, timeout=1500, key_prefix="doc_77",
                                            create_start=self.num_of_docs_per_collection,
                                            create_end=self.num_of_docs_per_collection + 10000)
            # self.load_docs_via_magma_server(server=data_node, bucket=bucket, gen=self.gen_create)
            task = self.cluster.async_load_gen_docs(data_node, bucket=bucket,
                                                    generator=self.gen_create,
                                                    timeout_secs=1500, use_magma_loader=True)
            task.result()

        self.run_cbq_query(query=query, server=self.n1ql_node)
        self.assertEqual(len(self.index_rest.get_indexer_metadata()['status']), 1,
                         "Index not created successfully")
        self.drop_index_node_resources_utilization_validations()


    def test_drop_multiple_indexes_in_training_state(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        create_queries = set()
        drop_queries = set()

        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                      prefix='test',
                                                                      similarity=self.similarity, train_list=None,
                                                                      scan_nprobes=self.scan_nprobes,
                                                                      array_indexes=False, bhive_index=self.bhive_index,
                                                                      limit=self.scan_limit, is_base64=self.base64,
                                                                      quantization_algo_color_vector=self.quantization_algo_color_vector,
                                                                      quantization_algo_description_vector=self.quantization_algo_description_vector)
            create_queries.update(
                self.gsi_util_obj.get_create_index_list(definition_list=definitions, namespace=namespace,
                                                        num_replica=self.num_index_replica,
                                                        bhive_index=self.bhive_index, defer_build=True))
            drop_queries.update(self.gsi_util_obj.get_drop_index_list(definition_list=definitions, namespace=namespace))
            build_query = self.gsi_util_obj.get_build_indexes_query(definition_list=definitions,namespace=namespace)

            self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, database=namespace, query_node=self.n1ql_node)

        timeout = 0
        with ThreadPoolExecutor() as executor:
            executor.submit(self.run_cbq_query, query=build_query, server=self.n1ql_node)
            while timeout < 360:
                index_state = self.index_rest.get_indexer_metadata()['status'][2]['status']
                if index_state == self.build_phase:
                    self.gsi_util_obj.create_gsi_indexes(create_queries=drop_queries, query_node=self.n1ql_node)
                    break
                self.sleep(1)
                timeout = timeout + 1
        if timeout > 360:
            self.fail("timeout exceeded")
        timeout = 0
        while timeout < 600:
            if 'status' not in self.index_rest.get_indexer_metadata():
                break
            self.sleep(5)
            timeout = timeout + 5

        self.assertEqual(len(self.index_rest.get_indexer_metadata()), 1, "index not dropped ")
        self.drop_index_node_resources_utilization_validations()

    def test_kill_memecached_during_index_training(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        collection_namespace = self.namespaces[0]

        vector_idx = QueryDefinition(index_name='vector', index_fields=['descriptionVector VECTOR'], dimension=384,
                                     description="IVF,PQ8x8", similarity="L2_SQUARED", is_base64=self.base64,
                                     bhive_index=self.bhive_index)
        query = vector_idx.generate_index_create_query(namespace=collection_namespace, defer_build=False,
                                                       bhive_index=self.bhive_index)
        build_query = vector_idx.generate_build_query(namespace=collection_namespace)

        self.run_cbq_query(query=query, server=self.n1ql_node)
        data_node = self.get_nodes_from_services_map(service_type="kv")

        remote_machine = RemoteMachineShellConnection(data_node)

        # for the scenario killing memecached during training phase
        timeout = 0
        with ThreadPoolExecutor() as executor:
            executor.submit(self.run_cbq_query, query=build_query)
            self.sleep(5)
            remote_machine.stop_memcached()
            self.sleep(60)
            remote_machine.start_memcached()
        meta_data = self.index_rest.get_indexer_metadata()['status']
        self.wait_until_indexes_online(timeout=120)

        for data in meta_data:
            self.assertEqual(data['status'], 'Ready', "state is errored out")
        self.item_count_related_validations()
        self.drop_index_node_resources_utilization_validations()

    def test_scalar_scans_on_vector_indexes(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        collection_namespace = self.namespaces[0]
        query_definition_1 = QueryDefinition(index_name='oneScalarOneVectorLeading',
                                             index_fields=['id', 'descriptionVector VECTOR'],
                                             dimension=384,
                                             description=f"IVF,{self.quantization_algo_description_vector}",
                                             similarity=self.similarity,
                                             scan_nprobes=self.scan_nprobes,
                                             limit=self.scan_limit, is_base64=self.base64,
                                             query_template=RANGE_SCAN_TEMPLATE.format(f"id", 'id > 1575644878'))

        create_query = query_definition_1.generate_index_create_query(namespace=collection_namespace)

        self.run_cbq_query(query=create_query, server=self.n1ql_node)
        self.item_count_related_validations()

        select_query = \
            self.gsi_util_obj.get_select_queries(definition_list=[query_definition_1], namespace=collection_namespace)[
                0]

        res = self.run_cbq_query(query=select_query, server=self.n1ql_node)

        self.assertEqual(len(res["results"]), self.scan_limit, "Expected no of results not returned")
        self.drop_index_node_resources_utilization_validations()

    def test_scans_on_different_distance_functions(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        collection_namespace = self.namespaces[0]

        desc_2 = "A BMW or Mercedes car with high safety rating and fuel efficiency"
        desc_vec2 = list(self.encoder.encode(desc_2))

        scan_desc_vec_1 = f"ANN_DISTANCE(descriptionVector, {desc_vec2}, 'L2_SQUARED', {self.scan_nprobes})"
        scan_desc_vec_2 = f"ANN_DISTANCE(descriptionVector, {desc_vec2}, 'COSINE', {self.scan_nprobes})"
        vector_index_L2 = QueryDefinition("vector_index_L2",
                                          index_fields=['rating', 'descriptionVector Vector',
                                                        'category'],
                                          dimension=384,
                                          description=f"IVF,{self.quantization_algo_description_vector}",
                                          similarity="L2_SQUARED",
                                          scan_nprobes=self.scan_nprobes,
                                          limit=self.scan_limit, is_base64=self.base64,
                                          query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format(
                                              "description, descriptionVector",
                                              "rating = 2 and "
                                              "category in ['Convertible', "
                                              "'Luxury Car', 'Supercar']",
                                              scan_desc_vec_1), bhive_index=self.bhive_index)

        vector_index_cosine = QueryDefinition("vector_index_cosine",
                                              index_fields=['rating', 'descriptionVector Vector',
                                                            'category'],
                                              dimension=384,
                                              description=f"IVF,{self.quantization_algo_description_vector}",
                                              similarity="COSINE",
                                              scan_nprobes=self.scan_nprobes,
                                              limit=self.scan_limit, is_base64=self.base64,
                                              query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format(
                                                  "description, descriptionVector",
                                                  "rating = 2 and "
                                                  "category in ['Convertible', "
                                                  "'Luxury Car', 'Supercar']",
                                                  scan_desc_vec_2), bhive_index=self.bhive_index)

        select_queries = []
        for idx in [vector_index_L2, vector_index_cosine]:
            create_query = idx.generate_index_create_query(namespace=collection_namespace)
            self.run_cbq_query(query=create_query, server=self.n1ql_node)
            select_query = self.gsi_util_obj.get_select_queries(definition_list=[idx],
                                                                namespace=collection_namespace)[0]
            select_queries.append(select_query)

        self.item_count_related_validations()

        for query in [vector_index_L2, vector_index_cosine]:
            select_query_with_explain = f"""EXPLAIN {self.gsi_util_obj.get_select_queries(definition_list=[query],
                                                                                          namespace=collection_namespace,
                                                                                          limit=self.scan_limit)[0]}"""
            index_used_select_query = \
                self.run_cbq_query(query=select_query_with_explain, server=self.n1ql_node)['results'][0]['plan']['~children'][0]['~children'][
                    0][
                    'index']
            self.log.info(index_used_select_query)
            self.assertEqual(index_used_select_query, query.index_name, 'trained index not used for scans')
        self.drop_index_node_resources_utilization_validations()

    def test_distance_projection_scans(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        collection_namespace = self.namespaces[0]

        desc_2 = "A BMW or Mercedes car with high safety rating and fuel efficiency"
        desc_vec2 = list(self.encoder.encode(desc_2))

        scan_desc_vec_1 = f"ANN_DISTANCE(descriptionVector, {desc_vec2}, '{self.similarity}', {self.scan_nprobes})"
        vector_index_L2 = QueryDefinition("vector_index_L2",
                                          index_fields=['rating', 'descriptionVector Vector',
                                                        'category'],
                                          dimension=384,
                                          description=f"IVF,{self.quantization_algo_description_vector}",
                                          similarity="L2_SQUARED",
                                          scan_nprobes=self.scan_nprobes,
                                          limit=self.scan_limit,
                                          query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format(
                                              scan_desc_vec_1,
                                              "rating = 2 and "
                                              "category in ['Convertible', "
                                              "'Luxury Car', 'Supercar']",
                                              scan_desc_vec_1), bhive_index=self.bhive_index)
        create_query = vector_index_L2.generate_index_create_query(namespace=collection_namespace)
        self.run_cbq_query(query=create_query, server=self.n1ql_node)
        self.item_count_related_validations()
        select_query = self.gsi_util_obj.get_select_queries(definition_list=[vector_index_L2],
                                                            namespace=collection_namespace)[0]
        distance_list = []
        c = self.run_cbq_query(query=select_query, server=self.n1ql_node)['results']
        for v in c:
            distance_list.append(v['$1'])

        copy_distance_list = distance_list[:]

        self.assertEqual(sorted(copy_distance_list), distance_list, "distance projection for the query is incorrect")
        self.drop_index_node_resources_utilization_validations()

    def test_scans_after_vector_field_mutations(self):
        data_node = self.get_nodes_from_services_map(service_type="kv")
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename,
                                      skip_default_scope=self.skip_default)
        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template,
                prefix='test',
                similarity=self.similarity, train_list=None,
                scan_nprobes=self.scan_nprobes,
                array_indexes=False,
                limit=self.scan_limit,
                quantization_algo_color_vector=self.quantization_algo_color_vector,
                quantization_algo_description_vector=self.quantization_algo_description_vector,
                bhive_index=self.bhive_index
            )
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions,
                                                                     namespace=namespace,
                                                                     bhive_index=self.bhive_index)
            select_queries = self.gsi_util_obj.get_select_queries(definition_list=definitions,
                                                                  namespace=namespace,
                                                                  limit=self.scan_limit)


            drop_queries = self.gsi_util_obj.get_drop_index_list(definition_list=definitions,
                                                                 namespace=namespace)
            query_stats_map = {}
            for create, select, drop in zip(create_queries, select_queries, drop_queries):
                self.run_cbq_query(query=create, server=self.n1ql_node)
                if "PRIMARY" in create:
                    continue
                query, recall, accuracy = self.validate_scans_for_recall_and_accuracy(select_query=select)
                query_stats_map[query] = [recall, accuracy]
            self.gen_table_view(query_stats_map=query_stats_map,
                                message=f"quantization value is {self.quantization_algo_description_vector}")
            self.item_count_related_validations()

            # Updating vector fields in existing documents and running scans after that
            keyspace = namespace.split(":")[-1]
            bucket, scope, collection = keyspace.split(".")
            bucket = bucket.split(':')[-1]
            self.gen_update = SDKDataLoader(num_ops=self.num_of_docs_per_collection, percent_create=0,
                                            percent_update=100, percent_delete=0, workers=16, scope=scope,
                                            collection=collection, json_template="Cars", timeout=2000,
                                            op_type="update", mutate=1, dim=384,
                                            update_start=1, update_end=self.num_of_docs_per_collection)
            task = self.cluster.async_load_gen_docs(data_node, bucket=bucket,
                                                    generator=self.gen_update,
                                                    timeout_secs=300, use_magma_loader=True)
            task.result()

            self.item_count_related_validations()

            query_stats_map = {}
            for query in select_queries:
                if "ANN_DISTANCE" not in query:
                    continue
                query, recall, accuracy = self.validate_scans_for_recall_and_accuracy(select_query=query)
                query_stats_map[query] = [recall, accuracy]
            self.gen_table_view(query_stats_map=query_stats_map,
                                message=f"quantization value is {self.quantization_algo_description_vector}")
            self.drop_index_node_resources_utilization_validations()


    def test_scans_after_adding_invalid_input_for_vector_fields_and_change_it_back(self):
        data_node = self.get_nodes_from_services_map(service_type="kv")
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename,
                                      skip_default_scope=self.skip_default)
        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template,
                prefix='test',
                similarity=self.similarity, train_list=None,
                scan_nprobes=self.scan_nprobes,
                array_indexes=False,
                limit=self.scan_limit,
                quantization_algo_color_vector=self.quantization_algo_color_vector,
                quantization_algo_description_vector=self.quantization_algo_description_vector,
                bhive_index=self.bhive_index
            )
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions,
                                                                     namespace=namespace,
                                                                     bhive_index=self.bhive_index)
            select_queries = self.gsi_util_obj.get_select_queries(definition_list=definitions,
                                                                  namespace=namespace,
                                                                  limit=self.scan_limit)

            self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, query_node=self.n1ql_node)

            self.item_count_related_validations()

            # Add invalid/scalar values for vector fields in some of the docs and run scans
            collection_namespace = self.namespaces[0]
            upsert_query = (f"update {collection_namespace} set colorRGBVector = 12 and "
                            f"descriptionVector = 'abcd' where rating > 2")
            self.run_cbq_query(query=upsert_query)

            for query in select_queries:
                self.run_cbq_query(query=query)

            # Fetch no of docs which will be mutated to validate the stats count
            select_query = f"select count(*) from {collection_namespace} where rating > 2 and colorRGBVector != [0, 0, 0]"
            result = self.run_cbq_query(query=select_query)
            doc_count = int(result["results"][0]["$1"])
            self.log.info(f'doc count {doc_count}')


            self.sleep(10)

            # change back the modified fields to vectors and re-run scans
            keyspace = namespace.split(":")[-1]
            bucket, scope, collection = keyspace.split(".")
            bucket = bucket.split(':')[-1]
            self.gen_update = SDKDataLoader(num_ops=self.num_of_docs_per_collection, percent_create=0,
                                            percent_update=100, percent_delete=0, workers=16, scope=scope,
                                            collection=collection, json_template="Cars", timeout=2000,
                                            op_type="update", dim=384, mutate=1,
                                            update_start=1, update_end=self.num_of_docs_per_collection)
            # self.load_docs_via_magma_server(server=data_node, bucket=bucket, gen=self.gen_update)
            task = self.cluster.async_load_gen_docs(data_node, bucket=bucket,
                                                    generator=self.gen_update,
                                                    timeout_secs=2000, use_magma_loader=True)
            task.result()

            query_stats_map = {}
            for query in select_queries:
                if "ANN_DISTANCE" not in query:
                    continue
                query, recall, accuracy = self.validate_scans_for_recall_and_accuracy(select_query=query)
                query_stats_map[query] = [recall, accuracy]
            self.gen_table_view(query_stats_map=query_stats_map,
                                message=f"quantization value is {self.quantization_algo_description_vector}")


            error_msg_and_doc_count = (f'"invalid_vec_type":{doc_count}')
            self.log.info(f"error msg count {error_msg_and_doc_count}")
            self.assertTrue(self.validate_error_msg_and_doc_count_in_cbcollect(self.master, error_msg_and_doc_count))
            self.drop_index_node_resources_utilization_validations()

    def test_scans_after_removing_vector_field_from_some_docs(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename,
                                      skip_default_scope=self.skip_default)
        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template,
                prefix='test',
                similarity=self.similarity, train_list=None,
                scan_nprobes=self.scan_nprobes,
                array_indexes=False,
                limit=self.scan_limit,
                quantization_algo_color_vector=self.quantization_algo_color_vector,
                quantization_algo_description_vector=self.quantization_algo_description_vector,
                bhive_index=self.bhive_index
            )
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions,
                                                                     namespace=namespace,
                                                                     bhive_index=self.bhive_index)
            select_queries = self.gsi_util_obj.get_select_queries(definition_list=definitions,
                                                                  namespace=namespace,
                                                                  limit=self.scan_limit)
            for query in create_queries:
                self.run_cbq_query(query=query)

            self.item_count_related_validations()

            # make vector field null for some of the docs and run scans
            collection_namespace = self.namespaces[0]
            upsert_query = f"update {collection_namespace} set descriptionVector = null where rating = 0"
            self.run_cbq_query(query=upsert_query)

            # Fetch no of docs which will be mutated to validate the stats count
            select_query = f"select count(*) from {collection_namespace} where rating = 0"
            result = self.run_cbq_query(query=select_query)
            doc_count = int(result["results"][0]["$1"])

            self.sleep(300)
            error_msg_and_doc_count = (f'"invalid_vec_type":{doc_count}')
            self.assertTrue(self.validate_error_msg_and_doc_count_in_cbcollect(self.master, error_msg_and_doc_count))
            self.drop_index_node_resources_utilization_validations()

    def test_scans_after_updating_dimensions_of_vector_field_and_reverting_back(self):
        data_node = self.get_nodes_from_services_map(service_type="kv")
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename,
                                      skip_default_scope=self.skip_default)
        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template,
                prefix='test',
                similarity=self.similarity, train_list=None,
                scan_nprobes=self.scan_nprobes,
                array_indexes=False,
                limit=self.scan_limit,
                quantization_algo_color_vector=self.quantization_algo_color_vector,
                quantization_algo_description_vector=self.quantization_algo_description_vector,
                bhive_index=self.bhive_index
            )
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions,
                                                                     namespace=namespace,
                                                                     bhive_index=self.bhive_index)
            select_queries = self.gsi_util_obj.get_select_queries(definition_list=definitions,
                                                                  namespace=namespace,
                                                                  limit=self.scan_limit)

            drop_queries = self.gsi_util_obj.get_drop_index_list(definition_list=definitions,
                                                                 namespace=namespace)
            query_stats_map = {}
            for create, select, drop in zip(create_queries, select_queries, drop_queries):
                self.run_cbq_query(query=create)
                if "PRIMARY" in create:
                    continue
                query, recall, accuracy = self.validate_scans_for_recall_and_accuracy(select_query=select)
                query_stats_map[query] = [recall, accuracy]
            self.gen_table_view(query_stats_map=query_stats_map,
                                message=f"quantization value is {self.quantization_algo_description_vector}")
            self.item_count_related_validations()

            # Updating dimensions of vector fields for subset of documents and running scans
            keyspace = namespace.split(":")[-1]
            bucket, scope, collection = keyspace.split(".")
            self.gen_update = SDKDataLoader(num_ops=self.num_of_docs_per_collection, percent_create=0,
                                            percent_update=100, percent_delete=0, workers=16, scope=scope,
                                            collection=collection, json_template="Cars", timeout=2000,
                                            op_type="update", mutate=1, dim=128,
                                            update_start=1, update_end=10000)
            # self.load_docs_via_magma_server(server=data_node, bucket=bucket, gen=self.gen_update)
            task = self.cluster.async_load_gen_docs(data_node, bucket=bucket,
                                                    generator=self.gen_update,
                                                    timeout_secs=2000, use_magma_loader=True)
            task.result()

            for query in select_queries:
                self.run_cbq_query(query=query)

            # Updating dimension of description field back to 384 and re-run scans
            keyspace = namespace.split(":")[-1]
            bucket, scope, collection = keyspace.split(".")
            self.gen_update = SDKDataLoader(num_ops=self.num_of_docs_per_collection, percent_create=0,
                                            percent_update=100, percent_delete=0, workers=16, scope=scope,
                                            collection=collection, json_template="Cars", timeout=2000,
                                            op_type="update", mutate=1, dim=384,
                                            update_start=1, update_end=10000)
            # self.load_docs_via_magma_server(server=data_node, bucket=bucket, gen=self.gen_update)
            task = self.cluster.async_load_gen_docs(data_node, bucket=bucket,
                                                    generator=self.gen_update,
                                                    timeout_secs=2000, use_magma_loader=True)
            task.result()

            self.item_count_related_validations()

            error_msg_and_doc_count = (f'"invalid_vec_dim":10000')
            self.assertTrue(self.validate_error_msg_and_doc_count_in_cbcollect(self.master, error_msg_and_doc_count))

            query_stats_map = {}
            for query in select_queries:
                if "ANN_DISTANCE" not in query:
                    continue
                query, recall, accuracy = self.validate_scans_for_recall_and_accuracy(select_query=query)
                query_stats_map[query] = [recall, accuracy]
            self.gen_table_view(query_stats_map=query_stats_map,
                                message=f"quantization value is {self.quantization_algo_description_vector}")
            self.drop_index_node_resources_utilization_validations()

    def test_scans_with_incremental_workload_for_composite_vector_index(self):
        data_node = self.get_nodes_from_services_map(service_type="kv")
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename,
                                      skip_default_scope=self.skip_default)
        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template,
                prefix='test',
                similarity=self.similarity, train_list=None,
                scan_nprobes=self.scan_nprobes,
                array_indexes=False,
                limit=self.scan_limit,
                quantization_algo_color_vector=self.quantization_algo_color_vector,
                quantization_algo_description_vector=self.quantization_algo_description_vector,
                bhive_index=self.bhive_index
            )
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions,
                                                                     namespace=namespace,
                                                                     bhive_index=self.bhive_index)
            select_queries = self.gsi_util_obj.get_select_queries(definition_list=definitions,
                                                                  namespace=namespace,
                                                                  limit=self.scan_limit)

            drop_queries = self.gsi_util_obj.get_drop_index_list(definition_list=definitions,
                                                                 namespace=namespace)
            query_stats_map = {}
            for create, select, drop in zip(create_queries, select_queries, drop_queries):
                self.run_cbq_query(query=create)
                if "PRIMARY" in create:
                    continue
                query, recall, accuracy = self.validate_scans_for_recall_and_accuracy(select_query=select)
                query_stats_map[query] = [recall, accuracy]
            self.gen_table_view(query_stats_map=query_stats_map,
                                message=f"quantization value is {self.quantization_algo_description_vector}")
            self.item_count_related_validations()

            # Updating vector fields in existing documents and running scans after that
            keyspace = namespace.split(":")[-1]
            bucket, scope, collection = keyspace.split(".")

            self.gen_create = SDKDataLoader(num_ops=self.num_of_docs_per_collection, percent_create=100,
                                            percent_update=0, percent_delete=0, workers=16, scope=scope,
                                            collection=collection, json_template="Cars", timeout=2000,
                                            op_type="create", dim=384,
                                            create_start=self.num_of_docs_per_collection,
                                            create_end=(self.num_of_docs_per_collection +
                                                        self.num_of_docs_per_collection // 2))
            # self.load_docs_via_magma_server(server=data_node, bucket=bucket, gen=self.gen_create)
            task = self.cluster.async_load_gen_docs(data_node, bucket=bucket,
                                                    generator=self.gen_create,
                                                    timeout_secs=2000, use_magma_loader=True)
            task.result()

            query_stats_map = {}
            for query in select_queries:
                if "ANN_DISTANCE" not in query:
                    continue
                query, recall, accuracy = self.validate_scans_for_recall_and_accuracy(select_query=query)
                query_stats_map[query] = [recall, accuracy]
            self.gen_table_view(query_stats_map=query_stats_map,
                                message=f"quantization value is {self.quantization_algo_description_vector}")

            self.item_count_related_validations()

            # verify index count matches bucket item count
            self._verify_bucket_count_with_index_count()
            self.drop_index_node_resources_utilization_validations()

    def test_scans_with_expiry_workload_for_composite_vector_index(self):
        data_node = self.get_nodes_from_services_map(service_type="kv")
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename,
                                      skip_default_scope=self.skip_default)

        # change bucket maxTTL to 120 secs to trigger expiration of docs
        self.rest.change_bucket_props(self.buckets[0], maxTTL=20)

        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(
                dataset=self.json_template,
                prefix='test',
                similarity=self.similarity, train_list=None,
                scan_nprobes=self.scan_nprobes,
                array_indexes=False,
                limit=self.scan_limit,
                quantization_algo_color_vector=self.quantization_algo_color_vector,
                quantization_algo_description_vector=self.quantization_algo_description_vector,
                bhive_index=self.bhive_index
            )
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions,
                                                                     namespace=namespace,
                                                                     bhive_index=self.bhive_index)
            select_queries = self.gsi_util_obj.get_select_queries(definition_list=definitions,
                                                                  namespace=namespace,
                                                                  limit=self.scan_limit)
            for create in create_queries:
                self.run_cbq_query(query=create)
            self.item_count_related_validations()

            with ThreadPoolExecutor() as executor:
                self.gsi_util_obj.query_event.set()
                executor.submit(self.gsi_util_obj.run_continous_query_load,
                                select_queries=select_queries, query_node=self.n1ql_node)

                # Updating vector fields in existing documents and running scans after that
                keyspace = namespace.split(":")[-1]
                bucket, scope, collection = keyspace.split(".")
                bucket = bucket.split(':')[-1]
                self.gen_update = SDKDataLoader(num_ops=self.num_of_docs_per_collection, percent_create=0,
                                                percent_update=100, percent_delete=0, workers=16, scope=scope,
                                                collection=collection, json_template="Cars", timeout=2000,
                                                op_type="update", dim=384,mutate=1,
                                                update_start=1, update_end=self.num_of_docs_per_collection)
                # self.load_docs_via_magma_server(server=data_node, bucket=bucket, gen=self.gen_update)
                task = self.cluster.async_load_gen_docs(data_node, bucket=bucket,
                                                        generator=self.gen_update,
                                                        timeout_secs=2000, use_magma_loader=True)
                task.result()
                self.gsi_util_obj.query_event.clear()

            self.drop_index_node_resources_utilization_validations()

    def test_run_scans_on_dgm(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename)
        select_queries = set()
        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                      prefix='test',
                                                                      similarity=self.similarity, train_list=None,
                                                                      scan_nprobes=self.scan_nprobes,
                                                                      array_indexes=False,
                                                                      limit=self.scan_limit,
                                                                      quantization_algo_color_vector=self.quantization_algo_color_vector,
                                                                      quantization_algo_description_vector=self.quantization_algo_description_vector)
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions, namespace=namespace,
                                                                     num_replica=self.num_index_replica)
            select_queries.update(self.gsi_util_obj.get_select_queries(definition_list=definitions,
                                                                       namespace=namespace, limit=self.scan_limit))

            self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, database=namespace,
                                                 query_node=self.n1ql_node)
        self.wait_until_indexes_online()
        self.item_count_related_validations()
        # self.sleep(300)
        self.run_continous_query = True
        self.query_node = self.n1ql_node
        query_timer = 0
        thread = Thread(target=self._run_queries_continously, args=[select_queries])
        thread.start()
        while query_timer < 300:
            query_timer += 1
            self.sleep(1)
        self.run_continous_query = False

        self.index_creation_till_rr(rr=self.desired_rr, timeout=7200)
        self.display_recall_and_accuracy_stats(select_queries=select_queries,
                                               message=f"results getting rr less than {self.desired_rr}%", similarity=self.similarity)
        self.drop_index_node_resources_utilization_validations()

    def test_partitioned_index_vector_fields_mutation(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename)
        desc_2 = "A BMW or Mercedes car with high safety rating and fuel efficiency"
        desc_vec2 = list(self.encoder.encode(desc_2))
        collection_namespace = self.namespaces[0]

        scan_desc_vec_2 = f"ANN_DISTANCE(descriptionVector, {desc_vec2}, '{self.similarity}', {self.scan_nprobes})"
        partitioned_index_description_vector = QueryDefinition("partitioned_descriptionVector",
                                                               index_fields=['rating', 'descriptionVector Vector',
                                                                             'category'],
                                                               dimension=384,
                                                               description=f"IVF,{self.quantization_algo_description_vector}",
                                                               similarity=self.similarity,
                                                               scan_nprobes=self.scan_nprobes,
                                                               limit=self.scan_limit, is_base64=self.base64,
                                                               query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format(
                                                                   "description, descriptionVector",
                                                                   "rating = 2 and "
                                                                   "category in ['Convertible', "
                                                                   "'Luxury Car', 'Supercar']",
                                                                   scan_desc_vec_2),
                                                               partition_by_fields=['meta().id']
                                                               )

        create_idx_query = partitioned_index_description_vector.generate_index_create_query(namespace=collection_namespace, num_partition=8)
        self.run_cbq_query(query=create_idx_query)
        self.item_count_related_validations()
        # The below query is to ensure that all the vector fields are made into scalar fields
        update_query = f"update {collection_namespace} set descriptionVector = 'abcd' where rating is not null"
        self.run_cbq_query(query=update_query)

        # a node is rebalanced out and all the partitions go to another indexer node and the index should not error out because it used code book from the existing instance
        if self.partitioned_index_action == "rebalance_out":
            index_node = self.get_nodes_from_services_map(service_type="index")
            #rebalancing out the above indexer node
            task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=[],
                                                to_remove=[index_node], services=['index'])
            task.result()
            rebalance_status = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")

        # a replica is added so new partitions get built and the index should not error out because it used code book from the existing instance
        elif self.partitioned_index_action == "alter_index":
            alter_index_query = f'ALTER INDEX `{partitioned_index_description_vector.index_name}` on {collection_namespace} WITH {{"action":"replica_count","num_replica": 1}}'
            try:
                self.run_cbq_query(query=alter_index_query)
            except Exception as e:
                self.fail(f"test failed with reason {e}")

        status = self.wait_until_indexes_online()
        self.assertTrue(status)

        select_query = self.gsi_util_obj.get_select_queries(definition_list=[partitioned_index_description_vector],
                                                            namespace=collection_namespace)[0]
        self.run_cbq_query(query=select_query)
        self.drop_index_node_resources_utilization_validations()

    def test_retry_index_training_and_dynamic_centroid_number(self):
        self.is_partial = self.input.param("is_partial", False)
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename)
        collection_namespace = self.namespaces[0]

        primary_index_query = f"CREATE PRIMARY INDEX `#primary` ON {collection_namespace}"
        self.run_cbq_query(query=primary_index_query)

        #query to ensure only 256 docs have vector fields
        if self.is_partial:
            modification_query = f"update {collection_namespace} SET colorRGBVector = \"hello\" WHERE rating > 3 limit {self.num_of_docs_per_collection - self.num_centroids};"
        else:
            modification_query = f"UPDATE {collection_namespace} SET descriptionVector = \"hello\" WHERE META().id NOT IN (SELECT RAW META().id FROM {collection_namespace} AS inner_coll LIMIT {self.num_centroids});"
        self.run_cbq_query(query=modification_query)
        color_vec_1 = [82.5, 106.700005, 20.9]  # camouflage green
        scan_color_vec_1 = f"ANN_DISTANCE(colorRGBVector, {color_vec_1}, '{self.similarity}', {self.scan_nprobes})"

        #creating index
        if self.is_partial:
            vector_index = QueryDefinition(index_name='PartialIndex',
                                           index_fields=['rating ', 'color', 'colorRGBVector Vector'],
                                           index_where_clause='rating > 3',
                                           dimension=3, description=f"IVF,{self.quantization_algo_color_vector}",
                                           similarity=self.similarity, scan_nprobes=self.scan_nprobes,
                                           train_list=self.trainlist, limit=self.scan_limit, is_base64=self.base64,
                                           query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format(
                                               "description, colorRGBVector",
                                               "rating = 4 and "
                                               'color like "%%blue%%" ',
                                               scan_color_vec_1))
        else:
            vector_index = QueryDefinition(index_name='vector', index_fields=['descriptionVector VECTOR'], dimension=384,
                                         description=f"IVF,{self.quantization_algo_description_vector}", similarity="L2_SQUARED",
                                         is_base64=self.base64)

        query = vector_index.generate_index_create_query(namespace=collection_namespace, defer_build=False)
        self.run_cbq_query(query=query)

        status = self.wait_until_indexes_online()
        self.assertTrue(status)
        self.drop_index_node_resources_utilization_validations()


    def test_rebalance_operation_with_errored_out_indexes(self):
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        data_node = self.get_nodes_from_services_map(service_type="kv")
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        self.collection_rest.create_scope_collection_count(scope_num=self.num_scopes, collection_num=self.num_collections,
                                                           scope_prefix=self.scope_prefix,
                                                           collection_prefix=self.collection_prefix,
                                                           bucket=self.test_bucket)
        self.sleep(10, "Allowing time after collection creation")
        # creating namespaces and loading docs
        scopes = [f'{self.scope_prefix}_{scope_num + 1}' for scope_num in range(self.num_scopes)]
        self.sleep(10, "Allowing time after collection creation")
        num_docs = 200
        for s_item in scopes:
            collections = [f'{self.collection_prefix}_{coll_num + 1}' for coll_num in range(self.num_collections)]
            for c_item in collections:
                self.namespaces.append(f'default:{self.test_bucket}.{s_item}.{c_item}')
                self.gen_create = SDKDataLoader(num_ops=num_docs, percent_create=100,
                                                percent_update=0, percent_delete=0, scope=s_item,
                                                collection=c_item, json_template=self.json_template,
                                                output=True, username=self.username, password=self.password,
                                                create_start=0, create_end=num_docs)

                # self.load_docs_via_magma_server(server=data_node, bucket=self.test_bucket, gen=self.gen_create)
                task = self.cluster.async_load_gen_docs(data_node, bucket=self.test_bucket,
                                                        generator=self.gen_create,
                                                        timeout_secs=300, use_magma_loader=True)
                task.result()
        select_queries = []
        build_queries = []
        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                      similarity=self.similarity, train_list=None,
                                                                      scan_nprobes=self.scan_nprobes,
                                                                      array_indexes=False, bhive_index=self.bhive_index,
                                                                      limit=self.scan_limit, is_base64=self.base64,
                                                                      quantization_algo_description_vector=self.quantization_algo_description_vector,
                                                                      quantization_algo_color_vector=self.quantization_algo_color_vector)
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions, namespace=namespace,
                                                                     num_replica=1, bhive_index=self.bhive_index)
            build_query = self.gsi_util_obj.get_build_indexes_query(definition_list=definitions, namespace=namespace)
            build_queries.append(build_query)
            self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, query_node=query_node)

            select_queries.extend(
                self.gsi_util_obj.get_select_queries(definition_list=definitions, namespace=namespace))

        with ThreadPoolExecutor() as executor:
            add_nodes = [self.servers[3]]
            task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=add_nodes,
                                                to_remove=[index_nodes[0]], services=['index'])
            task.result()
            rebalance_status = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")

            # Load more data into each collection so that index building does not fail this time around
            # due to insufficient training vectors
            for namespace in self.namespaces:
                bucket, scope, collection = namespace.split('.')
                bucket = bucket.split(':')[-1]
                # Load additional documents to ensure enough training vectors
                additional_docs = self.num_of_docs_per_collection  # Load same number of additional documents
                self.gen_create = SDKDataLoader(num_ops=additional_docs, percent_create=100,
                                                percent_update=0, percent_delete=0, scope=scope,
                                                collection=collection, json_template=self.json_template,
                                                output=True, username=self.username, password=self.password,
                                                create_start=0,key_prefix="doc_77",
                                                create_end=additional_docs,
                                                timeout=3000, workers=4, ops_rate=5000)
                # self.load_docs_via_magma_server(server=data_node, bucket=bucket, gen=self.gen_create)
                task = self.cluster.async_load_gen_docs(data_node, bucket=bucket,
                                                        generator=self.gen_create,
                                                        timeout_secs=3000, use_magma_loader=True)
                task.result()

            # Recreate indexes after rebalance since the removed index node may have lost some indexes
            self.log.info("Recreating indexes after rebalance...")
            for namespace in self.namespaces:
                definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                          similarity=self.similarity, train_list=None,
                                                                          scan_nprobes=self.scan_nprobes,
                                                                          array_indexes=False, bhive_index=self.bhive_index,
                                                                          limit=self.scan_limit, is_base64=self.base64,
                                                                          quantization_algo_description_vector=self.quantization_algo_description_vector,
                                                                          quantization_algo_color_vector=self.quantization_algo_color_vector)
                create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions, namespace=namespace,
                                                                         num_replica=1, bhive_index=self.bhive_index)
                self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, query_node=query_node)

            # Build all indexes using a single query per namespace
            self.log.info("Building all indexes after rebalance...")
            for namespace in self.namespaces:
                # Get all index names for this namespace
                query = f"SELECT name FROM system:indexes WHERE `using`='gsi' AND '`' || `bucket_id` || '`.`' || `scope_id` || '`.`' || `keyspace_id` || '`' = '{namespace}' AND state = 'deferred'"
                result = self.run_cbq_query(query=query, server=self.n1ql_node)

                if result['results']:
                    # Extract index names
                    index_names = [index['name'] for index in result['results']]
                    # Build all indexes for this namespace in a single query
                    build_query = f"BUILD INDEX ON {namespace}({', '.join(index_names)}) USING GSI"
                    self.log.info(f"Building indexes for {namespace}: {build_query}")
                    self.run_cbq_query(query=build_query, server=self.n1ql_node)
            self.wait_until_indexes_online()

            self.gsi_util_obj.query_event.set()
            executor.submit(self.gsi_util_obj.run_continous_query_load,
                            select_queries=select_queries, query_node=query_node)

            partial_indexes = self.get_partial_indexes_name_list()
            # perform index item count check to validate that indexer has processed all mutations
            _, stats = self._return_maps(perNode=True, map_from_index_nodes=True)
            index_item_count_map = {}
            for node in stats:
                for namespace in stats[node]:
                    for index in stats[node][namespace]:
                        if index not in index_item_count_map:
                            index_item_count_map[index] = stats[node][namespace][index]["items_count"]
                        else:
                            index_item_count_map[index] += stats[node][namespace][index]["items_count"]
            for index in index_item_count_map:
                if index in partial_indexes:
                    continue
                # Expected total: initial docs + additional docs = 2 * num_of_docs_per_collection
                expected_total = self.num_of_docs_per_collection+num_docs
                self.assertEqual(index_item_count_map[index], expected_total, f"stats {stats}")

            self.gsi_util_obj.query_event.clear()

            # Recall and accuracy check
            for select_query in select_queries:
                # Skipping validation for recall and accuracy against primary index
                if "DISTINCT" in select_query:
                    continue
                self.validate_scans_for_recall_and_accuracy(select_query=select_query)
        self.drop_index_node_resources_utilization_validations()

    def test_random_sampling_across_keyspaces(self):
        self.use_named_namespace = self.input.param("use_named_namespace", True)
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename)
        collection_namespace = self.namespaces[0]
        #required to get the ground truth data
        primary_idx = f"CREATE PRIMARY INDEX `#primary_4848` on {self.namespaces[0]};"
        self.run_cbq_query(query=primary_idx)
        desc_2 = "A red color car with 4 or 5 star safety rating"
        descVec2 = list(self.encoder.encode(desc_2))
        scan_desc_vec_2 = f"ANN_DISTANCE(descriptionVector, {descVec2}, '{self.similarity}', {self.scan_nprobes})"

        vector_index = QueryDefinition('oneScalarOneVectorLeading',
                        index_fields=['descriptionVector VECTOR', 'category'],
                        dimension=384, description=f"IVF,{self.quantization_algo_description_vector}",
                        similarity=self.similarity, scan_nprobes=self.scan_nprobes,
                        train_list=self.trainlist, limit=self.scan_limit, is_base64=self.base64,
                        query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format(f"category, descriptionVector",
                                                                           'category in ["Sedan", "Luxury Car"] ',
                                                                           scan_desc_vec_2))

        query = vector_index.generate_index_create_query(namespace=collection_namespace, defer_build=False)
        self.run_cbq_query(query=query)
        self.item_count_related_validations()
        select_query = self.gsi_util_obj.get_select_queries(definition_list=[vector_index],
                                                            namespace=collection_namespace)[0]
        _, recall_before_random_sampling, _ = self.validate_scans_for_recall_and_accuracy(select_query=select_query, similarity=self.similarity,
                                                                              scan_consitency=True)

        #loading docs with  random vector values in another namespace
        category_field = ["Sedan", "SUV", "Truck", "Coupe", "Convertible", "Hatchback", "Minivan",
            "Van", "Wagon", "Crossover", "Hybrid", "Sports Car", "Luxury Car", "Compact", "Subcompact",
            "Pickup", "Roadster", "Supercar", "Muscle Car"]

        queries = []
        if self.use_named_namespace:
            namespace = "test_bucket.test_scope_2.test_collection_2"
            self.namespaces.append(namespace)
            n1ql_node = self.get_nodes_from_services_map(service_type="n1ql")
            self.n1ql_helper.create_scope(server=n1ql_node, bucket_name=self.buckets[0].name, scope_name="test_scope_2")
            self.sleep(30)
            self.n1ql_helper.create_collection(server=n1ql_node, bucket_name=self.buckets[0].name, scope_name="test_scope_2", collection_name="test_collection_2")
        else:
            namespace = "test_bucket._default._default"
            self.namespaces.append(namespace)
        for i in range(1, 100001):
            vector = [random.randint(0, 50) for _ in range(384)]
            category = random.choice(category_field)
            query = f"INSERT INTO {self.namespaces[1]} (KEY, VALUE) VALUES (UUID(), {{ \"descriptionVector\": {vector}, \"category\": \"{category}\" }});"
            queries.append(query)
            # self.run_cbq_query(query=query)
        self.gsi_util_obj.async_create_indexes(create_queries=queries, database=self.namespaces[1])
        self.sleep(60)

        _, recall_after_random_sampling, _ = self.validate_scans_for_recall_and_accuracy(select_query=select_query,
                                                                                          similarity=self.similarity,
                                                                                          scan_consitency=True)
        self.log.info(f"recall before sampling : {recall_before_random_sampling*100}")
        self.log.info(f"recall after sampling : {recall_after_random_sampling*100}")

        self.threshold_percentage = self.input.param("threshold_percentage", 10)
        diff = recall_after_random_sampling - recall_before_random_sampling
        threshold = (self.threshold_percentage / 100) * recall_before_random_sampling
        self.assertLess(diff, threshold, f"diff {diff}, threshold {threshold}")
        self.drop_index_node_resources_utilization_validations()

    def test_mixed_dimension_data(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename)
        collection_namespace = self.namespaces[0]
        #required to get the ground truth data
        primary_idx = f"CREATE PRIMARY INDEX `#primary_4848` on {self.namespaces[0]};"
        self.run_cbq_query(query=primary_idx)
        desc_2 = "A red color car with 4 or 5 star safety rating"
        descVec2 = list(self.encoder.encode(desc_2))
        scan_desc_vec_2 = f"ANN_DISTANCE(descriptionVector, {descVec2}, '{self.similarity}', {self.scan_nprobes})"
        # query to ensure only specified num of docs have vector fields of the dimension on which the vector index created
        modification_query = f"UPDATE {collection_namespace} SET descriptionVector = [100,100,100,1000,1000] WHERE META().id NOT IN (SELECT RAW META().id FROM {collection_namespace} AS inner_coll LIMIT {self.num_centroids});"
        self.run_cbq_query(query=modification_query)

        if self.bhive_index:
            index_fields = ['descriptionVector VECTOR']
            include_fields = ['category']
        else:
            index_fields = ['descriptionVector VECTOR', 'category']
            include_fields = None

        vector_index = QueryDefinition('oneScalarOneVectorLeading',
                                       index_fields=index_fields,
                                       dimension=384, description=f"IVF,{self.quantization_algo_description_vector}",
                                       similarity=self.similarity, scan_nprobes=self.scan_nprobes,
                                       train_list=self.trainlist, limit=self.scan_limit, is_base64=self.base64,
                                       query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format(
                                           f"category, descriptionVector",
                                           'category in ["Sedan", "Luxury Car"] ',
                                           scan_desc_vec_2), include_fields=include_fields, bhive_index=self.bhive_index)

        query = vector_index.generate_index_create_query(namespace=collection_namespace, defer_build=False, bhive_index=self.bhive_index)
        self.run_cbq_query(query=query)

        #todo stats validation for no of docs skipped - https://jira.issues.couchbase.com/browse/MB-65249
        index_stats = self.index_rest.get_index_stats()
        for namespace in index_stats:
            for index in index_stats[namespace]:
                if "primary" in index:
                    continue
                else:
                    self.assertEqual(index_stats[namespace][index]['items_count'], self.num_centroids, "indexes with docs of other dimensions have been indexed")
        self.drop_index_node_resources_utilization_validations()

    def test_codebook_memory_indexer_restart(self):
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)

        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename)
        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                      similarity=self.similarity, train_list=None,
                                                                      scan_nprobes=self.scan_nprobes,
                                                                      array_indexes=False, bhive_index=self.bhive_index,
                                                                      limit=self.scan_limit, is_base64=self.base64,
                                                                      quantization_algo_description_vector=self.quantization_algo_description_vector,
                                                                      quantization_algo_color_vector=self.quantization_algo_color_vector)
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions, namespace=namespace, bhive_index=self.bhive_index)
            self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, query_node=query_node)

        self.wait_until_indexes_online()
        self.item_count_related_validations()

        code_book_memory_map_before_rebalance, aggregated_code_book_memory_before_rebalance = self.get_per_index_codebook_memory_usage()
        # restart indexer process on index_nodes
        for node in index_nodes:
            self._kill_all_processes_index(server=node)
        self.sleep(30)
        #compare the code book memory stats post indexer restart
        code_book_memory_map_after_rebalance, aggregated_code_book_memory_after_rebalance = self.get_per_index_codebook_memory_usage()
        index_names = self.get_all_indexes_in_the_cluster()
        for index in index_names:
            if "primary" in index:
                continue
            self.assertEqual(code_book_memory_map_before_rebalance[index], code_book_memory_map_after_rebalance[index],
                             f"Codebook memory has changed for index {index}")
        self.drop_index_node_resources_utilization_validations()

    def test_partition_elimination(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename)
        desc_2 = "A BMW or Mercedes car with high safety rating and fuel efficiency"
        desc_vec2 = list(self.encoder.encode(desc_2))
        collection_namespace = self.namespaces[0]

        scan_desc_vec_2 = f"ANN_DISTANCE(descriptionVector, {desc_vec2}, '{self.similarity}', {self.scan_nprobes})"

        #creating partitioned index with partition on a single key
        if self.bhive_index:
            partitioned_index_description_vector = QueryDefinition("partitioned_descriptionVector",
                                                                   index_fields=[
                                                                       '`descriptionVector` VECTOR'],
                                                                   dimension=384,
                                                                   description=f"IVF,{self.quantization_algo_description_vector}",
                                                                   similarity=self.similarity,
                                                                   scan_nprobes=self.scan_nprobes,
                                                                   limit=self.scan_limit, is_base64=self.base64,
                                                                   query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format(
                                                                       "description, descriptionVector",
                                                                       "fuel = \"LPG\"",
                                                                       scan_desc_vec_2),
                                                                   partition_by_fields=['fuel'],
                                                                   include_fields=['fuel', 'rating'],
                                                                   bhive_index=self.bhive_index
                                                                   )
        else:
            partitioned_index_description_vector = QueryDefinition("partitioned_descriptionVector",
                                                                   index_fields=['`fuel`,`rating`,`descriptionVector` VECTOR'],
                                                                   dimension=384,
                                                                   description=f"IVF,{self.quantization_algo_description_vector}",
                                                                   similarity=self.similarity,
                                                                   scan_nprobes=self.scan_nprobes,
                                                                   limit=self.scan_limit, is_base64=self.base64,
                                                                   query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format(
                                                                       "description, descriptionVector",
                                                                       "fuel = \"LPG\"",
                                                                       scan_desc_vec_2),
                                                                   partition_by_fields=['fuel'],
                                                                   bhive_index=self.bhive_index
                                                                   )
        index = partitioned_index_description_vector.generate_index_create_query(namespace=collection_namespace, num_partition=8,bhive_index=self.bhive_index)
        self.run_cbq_query(query=index)
        self.item_count_related_validations()
        select_query = self.gsi_util_obj.get_select_queries(definition_list=[partitioned_index_description_vector], namespace=collection_namespace)[0]
        self.run_cbq_query(select_query)
        drop_query = partitioned_index_description_vector.generate_index_drop_query(namespace=collection_namespace)
        stats_map = self.get_index_stats(perNode=True, partition=True)
        num_scans_list = []
        for node in stats_map:
            for keyspace in stats_map[node]:
                for index in stats_map[node][keyspace]:
                    self.log.info(f"for index {index} on node {node} num requests is {stats_map[node][keyspace][index]['num_requests']}")
                    num_scans_list.append(stats_map[node][keyspace][index]['num_requests'])
        self.assertNotEqual(num_scans_list[0], num_scans_list[1], "Partition elimination is not working")
        self.run_cbq_query(query=drop_query)
        self.sleep(30)

        # creating partitioned index with partition on a multiple keys
        if self.bhive_index:
            partitioned_index_description_vector = QueryDefinition("partitioned_descriptionVector",
                                                                   index_fields=[
                                                                       '`descriptionVector` VECTOR'],
                                                                   dimension=384,
                                                                   description=f"IVF,{self.quantization_algo_description_vector}",
                                                                   similarity=self.similarity,
                                                                   scan_nprobes=self.scan_nprobes,
                                                                   limit=self.scan_limit, is_base64=self.base64,
                                                                   query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format(
                                                                       "description, descriptionVector",
                                                                       "fuel = \"LPG\" and rating = 3",
                                                                       scan_desc_vec_2),
                                                                   partition_by_fields=['fuel, rating'],
                                                                   include_fields=['fuel', 'rating'],
                                                                   bhive_index=self.bhive_index
                                                                   )
        else:
            partitioned_index_description_vector = QueryDefinition("partitioned_descriptionVector",
                                                                   index_fields=[
                                                                       '`fuel`,`rating`,`descriptionVector` VECTOR'],
                                                                   dimension=384,
                                                                   description=f"IVF,{self.quantization_algo_description_vector}",
                                                                   similarity=self.similarity,
                                                                   scan_nprobes=self.scan_nprobes,
                                                                   limit=self.scan_limit, is_base64=self.base64,
                                                                   query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format(
                                                                       "description, descriptionVector",
                                                                       "fuel = \"LPG\" and rating = 3",
                                                                       scan_desc_vec_2),
                                                                   partition_by_fields=['fuel, rating'],
                                                                   bhive_index=self.bhive_index
                                                                   )
        index = partitioned_index_description_vector.generate_index_create_query(namespace=collection_namespace,
                                                                                 num_partition=8,bhive_index=self.bhive_index)
        self.run_cbq_query(query=index)
        self.item_count_related_validations()
        select_query = self.gsi_util_obj.get_select_queries(definition_list=[partitioned_index_description_vector], namespace=collection_namespace)[0]
        self.run_cbq_query(select_query)
        drop_query = partitioned_index_description_vector.generate_index_drop_query(namespace=collection_namespace)
        stats_map = self.get_index_stats(perNode=True, partition=True)
        num_scans_list = []
        for node in stats_map:
            for keyspace in stats_map[node]:
                for index in stats_map[node][keyspace]:
                    self.log.info(
                        f"for index {index} on node {node} num requests is {stats_map[node][keyspace][index]['num_requests']}")
                    num_scans_list.append(stats_map[node][keyspace][index]['num_requests'])
        self.assertNotEqual(num_scans_list[0], num_scans_list[1], "Partition elimination is not working")
        self.run_cbq_query(query=drop_query)
        self.sleep(30)

        # creating partitioned index with partition on scans on using IN predicate
        if self.bhive_index:
            partitioned_index_description_vector = QueryDefinition("partitioned_descriptionVector",
                                                                   index_fields=[
                                                                       '`descriptionVector` VECTOR'],
                                                                   dimension=384,
                                                                   description=f"IVF,{self.quantization_algo_description_vector}",
                                                                   similarity=self.similarity,
                                                                   scan_nprobes=self.scan_nprobes,
                                                                   limit=self.scan_limit, is_base64=self.base64,
                                                                   query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format(
                                                                       "description, descriptionVector",
                                                                       "fuel = \"LPG\" and rating = 3 and manufacturer in [\"Chevrolet\", \"Lexus\"]",
                                                                       scan_desc_vec_2),
                                                                   partition_by_fields=['fuel, rating, manufacturer'],
                                                                   include_fields=['fuel, rating, manufacturer'],
                                                                   bhive_index=self.bhive_index
                                                                   )
        else:
            partitioned_index_description_vector = QueryDefinition("partitioned_descriptionVector",
                                                                   index_fields=[
                                                                       '`fuel`,`rating`,`descriptionVector` VECTOR,`manufacturer`'],
                                                                   dimension=384,
                                                                   description=f"IVF,{self.quantization_algo_description_vector}",
                                                                   similarity=self.similarity,
                                                                   scan_nprobes=self.scan_nprobes,
                                                                   limit=self.scan_limit, is_base64=self.base64,
                                                                   query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format(
                                                                       "description, descriptionVector",
                                                                       "fuel = \"LPG\" and rating = 3 and manufacturer in [\"Chevrolet\", \"Lexus\"]",
                                                                       scan_desc_vec_2),
                                                                   partition_by_fields=['fuel, rating, manufacturer'],
                                                                   bhive_index=self.bhive_index
                                                                   )
        index = partitioned_index_description_vector.generate_index_create_query(namespace=collection_namespace,
                                                                                 num_partition=8,bhive_index=self.bhive_index)
        self.run_cbq_query(query=index)
        self.item_count_related_validations()
        select_query = self.gsi_util_obj.get_select_queries(definition_list=[partitioned_index_description_vector], namespace=collection_namespace)[0]
        self.run_cbq_query(select_query)
        drop_query = partitioned_index_description_vector.generate_index_drop_query(namespace=collection_namespace)
        stats_map = self.get_index_stats(perNode=True, partition=True)
        num_scans_list = []
        for node in stats_map:
            for keyspace in stats_map[node]:
                for index in stats_map[node][keyspace]:
                    self.log.info(
                        f"for index {index} on node {node} num requests is {stats_map[node][keyspace][index]['num_requests']}")
                    num_scans_list.append(stats_map[node][keyspace][index]['num_requests'])
        self.assertNotEqual(num_scans_list[0], num_scans_list[1], "Partition elimination is not working")
        self.run_cbq_query(query=drop_query)


    def test_coexistence_and_precedence_bhive_composite(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        collection_namespace = self.namespaces[0]

        desc_2 = "A BMW or Mercedes car with high safety rating and fuel efficiency"
        desc_vec2 = list(self.encoder.encode(desc_2))

        scan_desc_vec_1 = f"ANN_DISTANCE(descriptionVector, {desc_vec2}, 'L2_SQUARED', {self.scan_nprobes})"
        composite_vector_index = QueryDefinition(index_name='composite_index',
                            index_fields=[f'descriptionVector VECTOR'],
                            dimension=384, description=f"IVF,{self.quantization_algo_description_vector}",
                            similarity=self.similarity, scan_nprobes=self.scan_nprobes,
                            train_list=self.trainlist, limit=self.scan_limit,
                            query_template=FULL_SCAN_ORDER_BY_TEMPLATE.format(f"{desc_vec2},"
                                                                              f" {scan_desc_vec_1}",
                                                                              scan_desc_vec_1))

        bhive_index = QueryDefinition(index_name='bhive_index',
                            index_fields=[f'descriptionVector VECTOR'],
                            dimension=384, description=f"IVF,{self.quantization_algo_description_vector}",
                            similarity=self.similarity, scan_nprobes=self.scan_nprobes,
                            train_list=self.trainlist, limit=self.scan_limit,
                            query_template=FULL_SCAN_ORDER_BY_TEMPLATE.format(f"{desc_vec2},"
                                                                              f" {scan_desc_vec_1}",
                                                                              scan_desc_vec_1))

        select_queries = []
        for idx in [bhive_index, composite_vector_index]:
            create_query = idx.generate_index_create_query(namespace=collection_namespace, bhive_index=self.bhive_index)
            self.run_cbq_query(query=create_query, server=self.n1ql_node)
            select_query = self.gsi_util_obj.get_select_queries(definition_list=[idx],
                                                                namespace=collection_namespace)[0]
            select_queries.append(select_query)
            self.bhive_index = not self.bhive_index

        self.item_count_related_validations()
        for query in select_queries:
            select_query_with_explain = f"""EXPLAIN {query}"""
            res = \
                self.run_cbq_query(query=select_query_with_explain, server=self.n1ql_node)['results']
            index_used_select_query = res[0]['plan']['~children'][0]['~children'][
                    0][
                    'index']
            self.log.info(f"Result of explain is {res}")
            self.log.info(f"index used is {index_used_select_query}")
            self.assertEqual(index_used_select_query, bhive_index.index_name, 'bhive index was not the most preferred index')
        self.drop_index_node_resources_utilization_validations()

    def test_replica_repair_with_less_nodes(self):
        index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        self.enable_shard_based_rebalance()
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        select_queries = set()
        namespace_index_map = {}
        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                      prefix='test',
                                                                      similarity=self.similarity, train_list=None,
                                                                      scan_nprobes=self.scan_nprobes,
                                                                      array_indexes=False,
                                                                      limit=self.scan_limit, is_base64=self.base64,
                                                                      quantization_algo_color_vector=self.quantization_algo_color_vector,
                                                                      quantization_algo_description_vector=self.quantization_algo_description_vector,
                                                                      bhive_index=self.bhive_index)
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions, namespace=namespace,
                                                                     num_replica=self.num_index_replica,
                                                                     defer_build=True, bhive_index=self.bhive_index)
            select_queries.update(self.gsi_util_obj.get_select_queries(definition_list=definitions,
                                                                       namespace=namespace, limit=self.scan_limit))
            namespace_index_map[namespace] = definitions

            self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, database=namespace)
        self.wait_until_indexes_online(defer_build=True)

        index_metadata = self.index_rest.get_indexer_metadata()['status']
        for index in index_metadata:
            self.assertEqual(index['numReplica'], self.num_index_replica, "No. of replicas are not matching")

        for namespace in namespace_index_map:
            definition_list = namespace_index_map[namespace]
            for definitions in definition_list:
                self.alter_index_replicas(index_name=f"`{definitions.index_name}`", namespace=namespace,
                                          action='drop_replica', replica_id=1)
                self.sleep(20)
                self.wait_until_indexes_online()

        index_metadata = self.index_rest.get_indexer_metadata()['status']
        for index in index_metadata:
            self.assertTrue(index['replicaId'] != 1, f"Dropped wrong replica Id for index{index['indexName']}")

        map_before_rebalance, stats_before_rebalance = self._return_maps(perNode=True, map_from_index_nodes=True)

        # rebalancing out for replica repair
        index_node_out = index_node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [index_node_out],
                                                 services=['index'], cluster_config=self.cluster_config)
        rebalance.result()


        indexer_node = self.get_nodes_from_services_map(service_type="index")
        rest = RestConnection(indexer_node)
        index_metadata = rest.get_indexer_metadata()['status']
        for index in index_metadata:
            self.assertEqual(index['numReplica'], self.num_index_replica, "No. of replicas are not matching")

        # rebalancing in for replica repair
        index_node_in = index_node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [index_node_in], [],
                                                 services=['index'], cluster_config=self.cluster_config)
        rebalance.result()

        index_metadata = rest.get_indexer_metadata()['status']
        for index in index_metadata:
            self.assertEqual(index['numReplica'], self.num_index_replica, "No. of replicas are not matching")


        map_after_rebalance, stats_after_rebalance = self._return_maps(perNode=True, map_from_index_nodes=True)

        self.n1ql_helper.validate_item_count_data_size(map_before_rebalance=map_before_rebalance,
                                                       map_after_rebalance=map_after_rebalance,
                                                       stats_map_before_rebalance=stats_before_rebalance,
                                                       stats_map_after_rebalance=stats_after_rebalance,
                                                       item_count_increase=False,
                                                       per_node=True, skip_array_index_item_count=False)

    def test_skewed_bhive_composite_co_existence_test(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        collection_namespace = self.namespaces[0]
        select_queries = set()
        namespace_index_map = {}
        self.enable_shard_based_rebalance()
        for batch in range(0, self.num_indexes_batch):
            for namespace in self.namespaces:
                if self.bhive_index:
                    prefix = "bhive_"+''.join(random.choices(string.ascii_letters + string.digits, k=5))
                else:
                    prefix = "composite_vector_" +''.join(random.choices(string.ascii_letters + string.digits, k=5))
                definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                          prefix=prefix,
                                                                          similarity=self.similarity, train_list=None,
                                                                          scan_nprobes=self.scan_nprobes,
                                                                          array_indexes=False,
                                                                          limit=self.scan_limit, is_base64=self.base64,
                                                                          quantization_algo_color_vector=self.quantization_algo_color_vector,
                                                                          quantization_algo_description_vector=self.quantization_algo_description_vector,
                                                                          bhive_index=self.bhive_index)
                create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions, namespace=namespace,
                                                                         num_replica=self.num_index_replica,
                                                                         defer_build=False, bhive_index=self.bhive_index)
                select_queries.update(self.gsi_util_obj.get_select_queries(definition_list=definitions,
                                                                           namespace=namespace, limit=self.scan_limit))
                namespace_index_map[namespace] = definitions

                self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, database=namespace)
        self.wait_until_indexes_online()

        self.bhive_index = not self.bhive_index
        if self.bhive_index:
            prefix = "bhive"
        else:
            prefix = "composite"

        desc_2 = "A BMW or Mercedes car with high safety rating and fuel efficiency"
        desc_vec2 = list(self.encoder.encode(desc_2))

        scan_desc_vec_1 = f"APPROX_VECTOR_DISTANCE(descriptionVector, {desc_vec2}, 'L2_SQUARED', {self.scan_nprobes})"

        idx_1 = QueryDefinition(f"{prefix}_rating_desc_vector",
                                      index_fields=['rating', 'descriptionVector Vector',
                                                    'category'],
                                      dimension=384,
                                      description=f"IVF,{self.quantization_algo_description_vector}",
                                      similarity=self.similarity,
                                      scan_nprobes=self.scan_nprobes,
                                      limit=self.scan_limit, is_base64=self.base64,
                                      query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format(
                                          "description, descriptionVector",
                                          "rating = 2 and "
                                          "category in ['Convertible', "
                                          "'Luxury Car', 'Supercar']",
                                          scan_desc_vec_1), bhive_index=self.bhive_index, persist_full_vector=True)

        idx_2 = QueryDefinition(index_name=f"{prefix}_desc_vector",
                        index_fields=['descriptionVector VECTOR'],
                        dimension=384, description=f"IVF,{self.quantization_algo_description_vector}",
                        similarity=self.similarity, scan_nprobes=self.scan_nprobes,
                        train_list=self.trainlist, limit=self.scan_limit, persist_full_vector=True,
                        query_template=FULL_SCAN_ORDER_BY_TEMPLATE.format(f"{scan_desc_vec_1},"
                                                                          f" {scan_desc_vec_1}",
                                                                          scan_desc_vec_1), bhive_index=self.bhive_index)


        for idx in [idx_1, idx_2]:
            create_query = idx.generate_index_create_query(namespace=collection_namespace, bhive_index=self.bhive_index)
            self.run_cbq_query(query=create_query, server=self.n1ql_node)
            select_query = self.gsi_util_obj.get_select_queries(definition_list=[idx],
                                                                namespace=collection_namespace)[0]
            select_queries.update(select_query)

        self.item_count_related_validations()

        # validating shard seggregation
        shard_map = self.get_shards_index_map()
        self.validate_shard_seggregation(shard_index_map=shard_map)

        partial_indexes_list = self.get_partial_indexes_name_list()

        #validating item count
        _, stats = self._return_maps(perNode=True, map_from_index_nodes=True)
        index_item_count_map = {}
        for node in stats:
            for namespace in stats[node]:
                for index in stats[node][namespace]:
                    if index not in index_item_count_map:
                        index_item_count_map[index] = stats[node][namespace][index]["items_count"]
                    else:
                        index_item_count_map[index] += stats[node][namespace][index]["items_count"]
        for index in index_item_count_map:
            if index in partial_indexes_list:
                continue
            self.assertEqual(index_item_count_map[index], self.num_of_docs_per_collection, f"stats {stats}")

        #validating recall
        self.display_recall_and_accuracy_stats(select_queries=select_queries,
                                               message=f"results of skewed towards bhive {self.bhive_index}", similarity=self.similarity, stats_assertion=False)

        self.drop_index_node_resources_utilization_validations()

    def test_batch_index_create_drop_bhive_composite_combo(self):
        self.index_category_dropped = self.input.param("index_category_dropped", "composite_vector")
        self.enable_shard_based_rebalance()
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        select_queries = set()
        namespace_index_map = {}
        for index_type in ['composite_vector', 'bhive_vector']:
            namespace_index_map[index_type] = []
            if index_type == 'composite_vector':
                self.bhive_index = False
            else:
                self.bhive_index = True
            for batch in range(0, self.num_indexes_batch):
                for namespace in self.namespaces:
                    if self.bhive_index:
                        prefix = "bhive_" + ''.join(random.choices(string.ascii_letters + string.digits, k=5))
                    else:
                        prefix = "composite_vector_" + ''.join(random.choices(string.ascii_letters + string.digits, k=5))
                    definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                              prefix=prefix,
                                                                              similarity=self.similarity, train_list=None,
                                                                              scan_nprobes=self.scan_nprobes,
                                                                              array_indexes=False,
                                                                              limit=self.scan_limit, is_base64=self.base64,
                                                                              quantization_algo_color_vector=self.quantization_algo_color_vector,
                                                                              quantization_algo_description_vector=self.quantization_algo_description_vector,
                                                                              bhive_index=self.bhive_index)
                    create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions,
                                                                             namespace=namespace,
                                                                             num_replica=self.num_index_replica,
                                                                             defer_build=False, bhive_index=self.bhive_index)
                    select_queries.update(self.gsi_util_obj.get_select_queries(definition_list=definitions,
                                                                               namespace=namespace, limit=self.scan_limit))


                    namespace_index_map[index_type].append(self.gsi_util_obj.get_drop_index_list(definition_list=definitions, namespace=namespace))

                    self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, database=namespace)
        self.item_count_related_validations()

        # validating shard seggregation
        shard_map = self.get_shards_index_map()
        self.validate_shard_seggregation(shard_index_map=shard_map)

        partial_indexes_list = self.get_partial_indexes_name_list()

        # validating item count
        _, stats = self._return_maps(perNode=True, map_from_index_nodes=True)
        index_item_count_map = {}
        for node in stats:
            for namespace in stats[node]:
                for index in stats[node][namespace]:
                    if index not in index_item_count_map:
                        index_item_count_map[index] = stats[node][namespace][index]["items_count"]
                    else:
                        index_item_count_map[index] += stats[node][namespace][index]["items_count"]
        for index in index_item_count_map:
            if index in partial_indexes_list:
                continue
            self.assertEqual(index_item_count_map[index], self.num_of_docs_per_collection, f"stats {stats}")

        #dropping a particular category(bhive/composite) of indexes
        for query_list in namespace_index_map[self.index_category_dropped]:
            for drop_query in query_list:
                self.run_cbq_query(query=drop_query)

        if self.index_category_dropped == 'composite_vector':
            self.bhive_index = True
        else:
            self.bhive_index = False

        for batch in range(0, self.num_indexes_batch):
            for namespace in self.namespaces:
                if self.bhive_index:
                    prefix = "bhive_" + ''.join(random.choices(string.ascii_letters + string.digits, k=5))
                else:
                    prefix = "composite_vector_" + ''.join(random.choices(string.ascii_letters + string.digits, k=5))
                definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                          prefix=prefix,
                                                                          similarity=self.similarity, train_list=None,
                                                                          scan_nprobes=self.scan_nprobes,
                                                                          array_indexes=False,
                                                                          limit=self.scan_limit, is_base64=self.base64,
                                                                          quantization_algo_color_vector=self.quantization_algo_color_vector,
                                                                          quantization_algo_description_vector=self.quantization_algo_description_vector,
                                                                          bhive_index=self.bhive_index)
                create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions,
                                                                         namespace=namespace,
                                                                         num_replica=self.num_index_replica,
                                                                         defer_build=False, bhive_index=self.bhive_index)
                select_queries.update(self.gsi_util_obj.get_select_queries(definition_list=definitions,
                                                                           namespace=namespace, limit=self.scan_limit))


                self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, database=namespace)

        self.item_count_related_validations()

        # validating recall
        self.display_recall_and_accuracy_stats(select_queries=select_queries,
                                               message=f"results of skewed towards bhive {self.bhive_index}",
                                               similarity=self.similarity, stats_assertion=False)
        self.drop_index_node_resources_utilization_validations()