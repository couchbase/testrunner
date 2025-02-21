"""composite_vector_index.py: "This class test composite Vector indexes  for GSI"

__author__ = "Hemant Rajput"
__maintainer = "Hemant Rajput"
__email__ = "Hemant.Rajput@couchbase.com"
__git_user__ = "hrajput89"
__created_on__ = "19/06/24 03:27 pm"

"""
import datetime
import json

import requests
from concurrent.futures import ThreadPoolExecutor

from requests.auth import HTTPBasicAuth
from sentence_transformers import SentenceTransformer

from couchbase_helper.documentgenerator import SDKDataLoader
from couchbase_helper.query_definitions import QueryDefinition, FULL_SCAN_ORDER_BY_TEMPLATE, \
    RANGE_SCAN_USE_INDEX_ORDER_BY_TEMPLATE, RANGE_SCAN_TEMPLATE, RANGE_SCAN_ORDER_BY_TEMPLATE
from gsi.base_gsi import BaseSecondaryIndexingTests
from membase.api.on_prem_rest_client import RestHelper
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
        self.post_rebalance_action = self.input.param("post_rebalance_action", "data_load")
        self.partitioned_index_action = self.input.param("partitioned_index_action", "rebalance_out")
        # the below setting will be reversed post the resolving of MB-63697
        self.index_rest.set_index_settings({"indexer.plasma.mainIndex.enableInMemoryCompression": False})
        self.num_centroids = self.input.param("num_centroids", 256)

        self.log.info("==============  CompositeVectorIndex setup has ended ==============")

    def tearDown(self):
        self.log.info("==============  CompositeVectorIndex tearDown has started ==============")
        super(CompositeVectorIndex, self).tearDown()
        self.log.info("==============  CompositeVectorIndex tearDown has completed ==============")

    def suite_setUp(self):
        pass

    def suite_tearDown(self):
        pass

    def gen_table_view(self, query_stats_map, message="query stats"):
        table = TableView(self.log.info)
        table.set_headers(['Query', 'Recall', 'Accuracy'])
        for query in query_stats_map:
            table.add_row([query, query_stats_map[query][0]*100, query_stats_map[query][1]*100])
        table.display(message=message)

    def populate_vectors_in_xattr(self, bucket, scope):
        self.buckets = self.rest.get_buckets()
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
        function_name = f"GenVectors_{bucket}_{scope}"
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
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        query_stats_map = {}
        if self.xattr_indexes:
            for namespace in self.namespaces:
                bucket, scope, _ = namespace.split('.')
                self.populate_vectors_in_xattr(bucket=bucket, scope=scope)

        for similarity in ["COSINE", "L2_SQUARED", "L2", "DOT", "EUCLIDEAN_SQUARED", "EUCLIDEAN"]:
            for namespace in self.namespaces:
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
                drop_queries = self.gsi_util_obj.get_drop_index_list(definition_list=definitions,
                                                                      namespace=namespace)
                self.gsi_util_obj.async_create_indexes(create_queries=create_queries, database=namespace)
                self.wait_until_indexes_online()

                for query in select_queries:
                    # self.run_cbq_query(query=create)
                    if "DISTINCT" in query:
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

            # self.run_cbq_query(query=drop)
        self.gen_table_view(query_stats_map=query_stats_map,
                            message=f"quantization value is {self.quantization_algo_description_vector}")
        for query in query_stats_map:
            self.assertGreaterEqual(query_stats_map[query][0] * 100, 70,
                                    f"recall for query {query} is less than threshold 70")
            #uncomment the below code snippet to do assertions for accuracy
            # self.assertGreaterEqual(query_stats_map[query][1] * 100, 70,
            #                         f"accuracy for query {query} is less than threshold 70")

        self.rest.delete_bucket(bucket='metadata_bucket')

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
            err_msg = 'ErrTraining: The number of documents: 0 in keyspace:'
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
            err_msg = 'DISTINCT'
            self.assertTrue(err_msg in str(err), f"Index with INCLUDE clause is created: {err}")

        # indexes with all clause vector field
        index_gen_5 = QueryDefinition(index_name='colorRGBVector_2', is_base64=self.base64,
                                      index_fields=['ALL colorRGBVector VECTOR', 'description'],
                                      dimension=3, description="IVF,PQ3x8", similarity="L2_SQUARED")
        try:
            query = index_gen_5.generate_index_create_query(namespace=collection_namespace)
            self.run_cbq_query(query=query, server=self.n1ql_node)
        except Exception as err:
            err_msg = 'Vector index with any field having array expression is currently not supported'
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

        meta_data = self.index_rest.get_indexer_metadata()['status']
        self.assertEqual(len(meta_data), 2, f'indexes are not built metadata {meta_data}')

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

        vector_idx = QueryDefinition(index_name='vector', index_fields=['description VECTOR'], dimension=384,
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
        index_metadata = self.index_rest.get_indexer_metadata()['status']
        for idx in index_metadata:
            self.assertEqual("Error", idx['status'], 'index has been created ')

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
        meta_data = self.index_rest.get_indexer_metadata()['status']
        self.assertEqual(len(meta_data), len(self.namespaces) * 2, f"indexes are not created meta data {meta_data}")

    def test_rebalance(self):
        redistribute = {"indexer.settings.rebalance.redistribute_indexes": True}
        self.index_rest.set_index_settings(redistribute)
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
        code_book_memory_map_before_rebalance, aggregated_code_book_memory_before_rebalance = self.get_per_index_codebook_memory_usage()
        # Recall and accuracy check
        for select_query in select_queries:
            # Skipping validation for recall and accuracy against primary index
            if "DISTINCT" in select_query:
                continue
            self.validate_scans_for_recall_and_accuracy(select_query=select_query)
        with ThreadPoolExecutor() as executor:
            self.gsi_util_obj.query_event.set()
            executor.submit(self.gsi_util_obj.run_continous_query_load,
                            select_queries=select_queries, query_node=query_node)
            if self.rebalance_type == 'rebalance_in':
                add_nodes = [self.servers[3]]
                task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=add_nodes,
                                                    to_remove=[], services=['index', 'index'])
            elif self.rebalance_type == 'rebalance_swap':
                add_nodes = [self.servers[3]]
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
                self.stop_server(self.servers[self.nodes_init])
            task.result()
            rebalance_status = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")
            if self.cancel_rebalance or self.fail_rebalance:
                self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=[],
                                             to_remove=[], services=[])
                rebalance_status = RestHelper(self.rest).rebalance_reached()
                self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")
            self.gsi_util_obj.query_event.clear()
            data_nodes = self.get_nodes_from_services_map(service_type="kv")
            self.sleep(30)
            code_book_memory_map_after_rebalance, aggregated_code_book_memory_after_rebalance = self.get_per_index_codebook_memory_usage()
            index_names = self.get_all_indexes_in_the_cluster()
            for index in index_names:
                if "primary" in index:
                    continue
                self.assertEqual(code_book_memory_map_before_rebalance[index], code_book_memory_map_after_rebalance[index],
                                 f"Codebook memory has changed for index {index}")

            # Todo: Add metadata validation
            if self.post_rebalance_action == "data_load":
                for namespace in self.namespaces:
                    bucket, scope, collection = namespace.split('.')

                    self.gen_create = SDKDataLoader(num_ops=self.num_of_docs_per_collection, percent_create=100,
                                                    percent_update=0, percent_delete=0, scope=scope,
                                                    collection=collection, json_template="Cars",
                                                    output=True, username=self.username, password=self.password,
                                                    base64=self.base64,
                                                    model=self.data_model, workers=10, timeout=1500,
                                                    key_prefix="doc_77",
                                                    create_start=self.num_of_docs_per_collection,
                                                    create_end=self.num_of_docs_per_collection + 10000)
                    self.load_docs_via_magma_server(server=data_nodes.ip, bucket=bucket, gen=self.gen_create)
                self.sleep(60)
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
                    if "Partial" in index:
                        continue
                    self.assertEqual(index_item_count_map[index], self.num_of_docs_per_collection+10000, f"stats {stats}")


            if self.post_rebalance_action == "mutations":
                for namespace in self.namespaces:
                    keyspace = namespace.split(":")[-1]
                    bucket, scope, collection = keyspace.split(".")
                    self.gen_update = SDKDataLoader(num_ops=self.num_of_docs_per_collection, percent_create=0,
                                                    percent_update=100, percent_delete=0, workers=16, scope=scope,
                                                    collection=collection, json_template="Cars", timeout=2000,
                                                    op_type="update", mutate=1, dim=384,
                                                    update_start=0, update_end=self.num_of_docs_per_collection)
                    self.load_docs_via_magma_server(server=data_nodes.ip, bucket=bucket, gen=self.gen_create)
                    self.sleep(60)
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
                        self.assertEqual(index_item_count_map[index], self.num_of_docs_per_collection, f"stats {stats}")


        self.validate_scans_for_recall_and_accuracy(select_query=select_queries)

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
                        self.run_cbq_query(query=drop_query, server=self.n1ql_node)
                        break
                    self.sleep(1)
                    timeout = timeout + 1
            if timeout > 360:
                self.fail("timeout exceeded")

        self.assertEqual(len(self.index_rest.get_indexer_metadata()['status']), 0, "index not dropped ")

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
        index_metadata = self.index_rest.get_indexer_metadata()['status']
        self.assertEqual(len(index_metadata), 0, 'Indexes not dropped')

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
        self.wait_until_indexes_online()

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
        self.wait_until_indexes_online()

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
        self.wait_until_indexes_online()

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

        index_metadata = self.index_rest.get_indexer_metadata()['status']
        for index in index_metadata:
            self.assertTrue(index['replicaId'] != 1, f"Dropped wrong replica Id for index{index['indexName']}")

        map_before_rebalance, stats_before_rebalance = self._return_maps(perNode=True, map_from_index_nodes=True)

        # rebalancing in for replica repair
        index_node_in = self.servers[self.nodes_init]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [index_node_in], [],
                                                 services=['index'], cluster_config=self.cluster_config)
        rebalance.result()

        index_metadata = self.index_rest.get_indexer_metadata()['status']
        for index in index_metadata:
            self.assertEqual(index['numReplica'], self.num_index_replica, "No. of replicas are not matching")

        map_after_rebalance, stats_after_rebalance = self._return_maps(perNode=True, map_from_index_nodes=True)

        self.n1ql_helper.validate_item_count_data_size(map_before_rebalance=map_before_rebalance,
                                                       map_after_rebalance=map_after_rebalance,
                                                       stats_map_before_rebalance=stats_before_rebalance,
                                                       stats_map_after_rebalance=stats_after_rebalance,
                                                       item_count_increase=False,
                                                       per_node=True, skip_array_index_item_count=False)

        self.display_recall_and_accuracy_stats(select_queries=select_queries,
                                               message="results before after replica id", similarity=self.similarity)

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
        self.wait_until_indexes_online()

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
                self.sleep(20)
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

    def test_scan_comparison_between_trained_and_untrained_indexes(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        desc_2 = "A BMW or Mercedes car with high safety rating and fuel efficiency"
        desc_vec2 = list(self.encoder.encode(desc_2))

        scan_desc_vec_2 = f"ANN(descriptionVector, {desc_vec2}, '{self.similarity}', {self.scan_nprobes})"

        scan_color_vec_1 = f"ANN(colorRGBVector, [43.0, 133.0, 178.0], '{self.similarity}', {self.scan_nprobes})"
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
        for query in [trained_index_color_rgb_vector, trained_index_description_vector]:
            select_query_with_explain = f"EXPLAIN {self.gsi_util_obj.get_select_queries(definition_list=[query], namespace=collection_namespace, limit=self.scan_limit)[0]}"
            index_used_select_query = \
                self.run_cbq_query(query=select_query_with_explain)['results'][0]['plan']['~children'][0]['~children'][
                    0][
                    'index']
            self.assertEqual(index_used_select_query, query.index_name, 'trained index not used for scans')

    def test_compare_results_between_partitioned_and_non_partitioned_indexes(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)

        collection_namespace = self.namespaces[0]

        color_vec_2 = [90.0, 33.0, 18.0]
        scan_color_vec_2 = f"ANN(colorRGBVector, {color_vec_2}, '{self.similarity}', {self.scan_nprobes})"

        desc_2 = "A BMW or Mercedes car with high safety rating and fuel efficiency"
        desc_vec2 = list(self.encoder.encode(desc_2))

        scan_desc_vec_2 = f"ANN(descriptionVector, {desc_vec2}, '{self.similarity}', {self.scan_nprobes})"

        partitioned_index_color_rgb_vector = QueryDefinition("partitioned_colorRGBVector",
                                                             index_fields=['rating', 'colorRGBVector Vector',
                                                                           'category'],
                                                             dimension=3,
                                                             description=f"IVF,{self.quantization_algo_color_vector}",
                                                             similarity=self.similarity, scan_nprobes=self.scan_nprobes,
                                                             limit=self.scan_limit, is_base64=self.base64,
                                                             query_use_index_template=RANGE_SCAN_USE_INDEX_ORDER_BY_TEMPLATE.format(
                                                                 "color, colorRGBVector",
                                                                 "rating = 2 and "
                                                                 "category in ['Convertible', "
                                                                 "'Luxury Car', 'Supercar']",
                                                                 scan_color_vec_2),
                                                             partition_by_fields=['meta().id']
                                                             )

        non_partitioned_index_color_rgb_vector = QueryDefinition("non_partitioned_colorRGBVector",
                                                                 index_fields=['rating', 'colorRGBVector Vector',
                                                                               'category'],
                                                                 dimension=3,
                                                                 description=f"IVF,{self.quantization_algo_color_vector}",
                                                                 similarity=self.similarity,
                                                                 scan_nprobes=self.scan_nprobes,
                                                                 limit=self.scan_limit, is_base64=self.base64,
                                                                 query_use_index_template=RANGE_SCAN_USE_INDEX_ORDER_BY_TEMPLATE.format(
                                                                     "color, colorRGBVector",
                                                                     "rating = 2 and "
                                                                     "category in ['Convertible', "
                                                                     "'Luxury Car', 'Supercar']",
                                                                     scan_color_vec_2)
                                                                 )

        message = f"quantization value is {self.quantization_algo_description_vector}"
        partitioned_index_description_vector = QueryDefinition("partitioned_descriptionVector",
                                                               index_fields=['rating', 'descriptionVector Vector',
                                                                             'category'],
                                                               dimension=384,
                                                               description=f"IVF,{self.quantization_algo_description_vector}",
                                                               similarity=self.similarity,
                                                               scan_nprobes=self.scan_nprobes,
                                                               limit=self.scan_limit, is_base64=self.base64,
                                                               query_use_index_template=RANGE_SCAN_USE_INDEX_ORDER_BY_TEMPLATE.format(
                                                                   "description, descriptionVector",
                                                                   "rating = 2 and "
                                                                   "category in ['Convertible', "
                                                                   "'Luxury Car', 'Supercar']",
                                                                   scan_desc_vec_2),
                                                               partition_by_fields=['meta().id']
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
                                                                       "description, descriptionVector",
                                                                       "rating = 2 and "
                                                                       "category in ['Convertible', "
                                                                       "'Luxury Car', 'Supercar']",
                                                                       scan_desc_vec_2)
                                                                   )

        select_queries = []
        for idx in [partitioned_index_color_rgb_vector, non_partitioned_index_color_rgb_vector,
                    partitioned_index_description_vector, non_partitioned_index_description_vector]:
            create_query = idx.generate_index_create_query(namespace=collection_namespace)
            self.run_cbq_query(query=create_query, server=self.n1ql_node)
            select_query = self.gsi_util_obj.get_select_queries(definition_list=[idx],
                                                                namespace=collection_namespace,
                                                                index_name=idx.index_name)[0]
            select_queries.append(select_query)

        self.display_recall_and_accuracy_stats(select_queries=select_queries, message=message, similarity=self.similarity)

    def test_replica_repair(self):
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
                                                                      quantization_algo_description_vector=self.quantization_algo_description_vector)
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions, namespace=namespace,
                                                                     num_replica=self.num_index_replica,
                                                                     defer_build=True)
            select_queries.update(self.gsi_util_obj.get_select_queries(definition_list=definitions,
                                                                       namespace=namespace, limit=self.scan_limit))
            build_query = self.gsi_util_obj.get_build_indexes_query(definition_list=definitions, namespace=namespace)
            namespace_index_map[namespace] = definitions

            self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, database=namespace)
        self.wait_until_indexes_online(defer_build=True)

        # rebalancing out an indexer node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [index_node],
                                                 services=['index'], cluster_config=self.cluster_config)
        rebalance.result()

        self.sleep(30)

        self.run_cbq_query(query=build_query, server=self.n1ql_node)
        self.wait_until_indexes_online()

        #rebalancing in indexer node
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [index_node], [],
                                                 services=['index'], cluster_config=self.cluster_config)
        rebalance.result()

        index_metadata = self.index_rest.get_indexer_metadata()['status']
        for index in index_metadata:
            self.assertEqual(index['numReplica'], self.num_index_replica, "No. of replicas are not matching")

    def test_vector_indexes_after_bucket_flush(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename,
                                      skip_default_scope=self.skip_default)

        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        select_queries = []
        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                      similarity=self.similarity, train_list=None,
                                                                      scan_nprobes=self.scan_nprobes,
                                                                      array_indexes=False,
                                                                      limit=self.scan_limit, is_base64=self.base64,
                                                                      quantization_algo_description_vector=self.quantization_algo_description_vector,
                                                                      quantization_algo_color_vector=self.quantization_algo_color_vector)
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions, namespace=namespace,
                                                                     num_replica=1)
            self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, query_node=query_node)

            select_queries.extend(
                self.gsi_util_obj.get_select_queries(definition_list=definitions, namespace=namespace))

        for select_query in select_queries:
            if "ANN" not in select_query:
                continue
            self.validate_scans_for_recall_and_accuracy(select_query=select_query)

        for bucket in self.buckets:
            try:
                self.rest.flush_bucket(bucket=bucket)
            except Exception as ex:
                if "unable to flush bucket" not in str(ex):
                    self.fail("flushing bucket failed with unexpected error message")

        try:
            # Running the scan on an empty bucket
            for query in select_queries:
                self.run_cbq_query(query=query, server=query_node)
        except Exception as err:
            self.log.error(err)

        # restoring and running queries again to validate recall percentage
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename,
                                      skip_default_scope=self.skip_default)

        for select_query in select_queries:
            if "ANN" not in select_query:
                continue
            self.validate_scans_for_recall_and_accuracy(select_query=select_query)

    def test_recover_from_disk_snapshot(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename,
                                      skip_default_scope=self.skip_default)

        setting = {"indexer.settings.persisted_snapshot.moi.interval": 60000}
        self.index_rest.set_index_settings(setting)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
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
        index_stats = self.index_rest.get_index_stats()

        for select_query in select_queries:
            if "ANN" not in select_query:
                continue
            self.validate_scans_for_recall_and_accuracy(select_query=select_query)

        doc_count = {}
        for namespace in self.namespaces:
            count_query = f"select count(year) from {namespace} where year > 0;"
            result = self.run_cbq_query(query=count_query, server=query_node)['results'][0]["$1"]
            doc_count[namespace] = result
        # changing the interval to 10 mins
        setting = {"indexer.settings.persisted_snapshot.moi.interval": 1000000}
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
            self.gen_create = SDKDataLoader(num_ops=self.num_of_docs_per_collection, percent_create=100,
                                            percent_update=0, percent_delete=0, scope=scope,
                                            collection=collection, json_template="Cars", key_prefix="new_doc")
            task = self.cluster.async_load_gen_docs(self.master, bucket=bucket,
                                                    generator=self.gen_create, pause_secs=1,
                                                    timeout_secs=600, use_magma_loader=True)
            task.result()

        remote_client = RemoteMachineShellConnection(index_node)
        remote_client.terminate_process(process_name='indexer')

        index_stats = self.index_rest.get_index_stats()
        docs_pending = [f"{bucket}.{index}.{key}:{val}" for bucket, indexes in index_stats.items()
                        for index, stats in indexes.items()
                        for key, val in stats.items() if 'num_docs_pending' in key]
        self.log.info(f"Docs Pending after indexer kill: {docs_pending}")

        for bucket, indexes in index_stats.items():
            for index, stats in indexes.items():
                for key, val in stats.items():
                    if 'num_disk_snapshots' in key:
                        if disk_snapshots[key] != val:
                            self.fail("new snapshot is created. Adjust the stats")
        for namespace in self.namespaces:
            count_query = f"select count(year) from {namespace} where year > 0;"
            result = self.run_cbq_query(query=count_query, server=query_node)['results'][0]["$1"]
            self.assertEqual(result, doc_count[namespace] + self.num_of_docs_per_collection,
                             "Index hasn't recovered the docs within the given time")

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
        self.wait_until_indexes_online()

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

            self.gen_create = SDKDataLoader(num_ops=self.num_of_docs_per_collection, percent_create=100,
                                            percent_update=0, percent_delete=0, scope=scope,
                                            collection=collection, json_template="Cars",
                                            output=True, username=self.username, password=self.password,
                                            base64=self.base64,
                                            model=self.data_model, workers=10, timeout=1500, key_prefix="doc_77",
                                            start_seq_num=self.num_of_docs_per_collection,
                                            end=self.num_of_docs_per_collection + 10000)
            self.load_docs_via_magma_server(server=data_nodes[0], bucket=bucket, gen=self.gen_create)

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
        try:
            query = f"BUILD INDEX ON {collection_namespace}(vector) USING GSI "
            self.run_cbq_query(query=query, server=self.n1ql_node)
        except Exception as err:
            err_msg = 'are less than the minimum number of documents:'
            self.assertTrue(err_msg in str(err), f"Index with less docs are created: {err}")
        else:
            raise Exception("index has been built with less no of centroids")

        #Loading valid docs for vector indexes
        for namespace in self.namespaces:
            _, keyspace = namespace.split(':')
            bucket, scope, collection = keyspace.split('.')

            self.gen_create = SDKDataLoader(num_ops=self.num_of_docs_per_collection, percent_create=100,
                                            percent_update=0, percent_delete=0, scope=scope,
                                            collection=collection, json_template="Cars",
                                            output=True, username=self.username, password=self.password,
                                            base64=self.base64,
                                            model=self.data_model, workers=10, timeout=1500, key_prefix="doc_77",
                                            start_seq_num=self.num_of_docs_per_collection,
                                            end=self.num_of_docs_per_collection + 10000)
            self.load_docs_via_magma_server(server=data_node, bucket=bucket, gen=self.gen_create)

        self.run_cbq_query(query=query, server=self.n1ql_node)
        self.assertEqual(len(self.index_rest.get_indexer_metadata()['status']), 1,
                         "Index not created successfully")

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
                                                        bhive_index=self.bhive_index))
            drop_queries.update(self.gsi_util_obj.get_drop_index_list(definition_list=definitions, namespace=namespace))

            self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, database=namespace)

        timeout = 0
        with ThreadPoolExecutor() as executor:
            executor.submit(self.gsi_util_obj.create_gsi_indexes, create_queries=create_queries)
            self.sleep(5)
            while timeout < 360:
                index_state = self.index_rest.get_indexer_metadata()['status'][0]['status']
                if index_state == self.build_phase:
                    self.gsi_util_obj.create_gsi_indexes(create_queries=drop_queries)
                    break
                self.sleep(1)
                timeout = timeout + 1
        if timeout > 360:
            self.fail("timeout exceeded")

        self.assertEqual(len(self.index_rest.get_indexer_metadata()['status']), 0, "index not dropped ")

    def test_kill_memecached_during_index_training(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        collection_namespace = self.namespaces[0]

        vector_idx = QueryDefinition(index_name='vector', index_fields=['descriptionVector VECTOR'], dimension=384,
                                     description="IVF,PQ8x8", similarity="L2_SQUARED", is_base64=self.base64,
                                     bhive_index=self.bhive_index)
        query = vector_idx.generate_index_create_query(namespace=collection_namespace, defer_build=True,
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

        for data in meta_data:
            self.assertEqual(data['status'], 'Error', "state is not errored out")

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

        select_query = \
            self.gsi_util_obj.get_select_queries(definition_list=[query_definition_1], namespace=collection_namespace)[
                0]

        res = self.run_cbq_query(query=select_query, server=self.n1ql_node)

        self.assertEqual(len(res["results"]), self.scan_limit, "Expected no of results not returned")

    def test_scans_on_different_distance_functions(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        collection_namespace = self.namespaces[0]

        desc_2 = "A BMW or Mercedes car with high safety rating and fuel efficiency"
        desc_vec2 = list(self.encoder.encode(desc_2))

        scan_desc_vec_1 = f"ANN(descriptionVector, {desc_vec2}, 'L2_SQUARED', {self.scan_nprobes})"
        scan_desc_vec_2 = f"ANN(descriptionVector, {desc_vec2}, 'COSINE', {self.scan_nprobes})"
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
                                              scan_desc_vec_1))

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
                                                  scan_desc_vec_2))

        select_queries = []
        for idx in [vector_index_L2, vector_index_cosine]:
            create_query = idx.generate_index_create_query(namespace=collection_namespace)
            self.run_cbq_query(query=create_query, server=self.n1ql_node)
            select_query = self.gsi_util_obj.get_select_queries(definition_list=[idx],
                                                                namespace=collection_namespace)[0]
            select_queries.append(select_query)

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

    def test_distance_projection_scans(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        collection_namespace = self.namespaces[0]

        desc_2 = "A BMW or Mercedes car with high safety rating and fuel efficiency"
        desc_vec2 = list(self.encoder.encode(desc_2))

        scan_desc_vec_1 = f"ANN(descriptionVector, {desc_vec2}, '{self.similarity}', {self.scan_nprobes})"
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
                                              scan_desc_vec_1))
        create_query = vector_index_L2.generate_index_create_query(namespace=collection_namespace)
        self.run_cbq_query(query=create_query, server=self.n1ql_node)
        select_query = self.gsi_util_obj.get_select_queries(definition_list=[vector_index_L2],
                                                            namespace=collection_namespace)[0]
        distance_list = []
        c = self.run_cbq_query(query=select_query, server=self.n1ql_node)['results']
        for v in c:
            distance_list.append(v['$1'])

        copy_distance_list = distance_list[:]

        self.assertEqual(sorted(copy_distance_list), distance_list, "distance projection for the query is incorrect")


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

            # Updating vector fields in existing documents and running scans after that
            keyspace = namespace.split(":")[-1]
            bucket, scope, collection = keyspace.split(".")
            self.gen_update = SDKDataLoader(num_ops=self.num_of_docs_per_collection, percent_create=0,
                                            percent_update=100, percent_delete=0, workers=16, scope=scope,
                                            collection=collection, json_template="Cars", timeout=2000,
                                            op_type="update", mutate=1, dim=384,
                                            update_start=0, update_end=self.num_of_docs_per_collection)
            self.load_docs_via_magma_server(server=data_node, bucket=bucket, gen=self.gen_update)

            query_stats_map = {}
            for query in select_queries:
                if "ANN" not in query:
                    continue
                query, recall, accuracy = self.validate_scans_for_recall_and_accuracy(select_query=query)
                query_stats_map[query] = [recall, accuracy]
            self.gen_table_view(query_stats_map=query_stats_map,
                                message=f"quantization value is {self.quantization_algo_description_vector}")

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

            for query in create_queries:
                self.run_cbq_query(query=query)

            # Add invalid/scalar values for vector fields in some of the docs and run scans
            collection_namespace = self.namespaces[0]
            upsert_query = (f"update {collection_namespace} set colorRGBVector = 12 and "
                            f"descriptionVector = 'abcd' where rating > 2")
            self.run_cbq_query(query=upsert_query)

            for query in select_queries:
                self.run_cbq_query(query=query)

            # change back the modified fields to vectors and re-run scans
            keyspace = namespace.split(":")[-1]
            bucket, scope, collection = keyspace.split(".")
            self.gen_update = SDKDataLoader(num_ops=self.num_of_docs_per_collection, percent_create=0,
                                            percent_update=100, percent_delete=0, workers=16, scope=scope,
                                            collection=collection, json_template="Cars", timeout=2000,
                                            op_type="update", dim=384,
                                            update_start=0, update_end=self.num_of_docs_per_collection)
            self.load_docs_via_magma_server(server=data_node, bucket=bucket, gen=self.gen_update)

            query_stats_map = {}
            for query in select_queries:
                if "ANN" not in query:
                    continue
                query, recall, accuracy = self.validate_scans_for_recall_and_accuracy(select_query=query)
                query_stats_map[query] = [recall, accuracy]
            self.gen_table_view(query_stats_map=query_stats_map,
                                message=f"quantization value is {self.quantization_algo_description_vector}")

            # Fetch no of docs which will be mutated to validate the stats count
            select_query = f"select count(*) from {collection_namespace} where rating > 2"
            result = self.run_cbq_query(query=select_query)
            doc_count = int(result["results"][0]["$1"])

            error_msg_and_doc_count = (f'"The value of a VECTOR expression is expected to be an array of floats. '
                                       f'Found non-array type":{doc_count}')
            self.assertTrue(self.validate_error_msg_and_doc_count_in_cbcollect(self.master, error_msg_and_doc_count))

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

            # make vector field null for some of the docs and run scans
            collection_namespace = self.namespaces[0]
            upsert_query = f"update {collection_namespace} set descriptionVector = null where rating = 0"
            self.run_cbq_query(query=upsert_query)

            query_stats_map = {}
            for query in select_queries:
                if "ANN" not in query:
                    continue
                query, recall, accuracy = self.validate_scans_for_recall_and_accuracy(select_query=query)
                query_stats_map[query] = [recall, accuracy]
            self.gen_table_view(query_stats_map=query_stats_map,
                                message=f"quantization value is {self.quantization_algo_description_vector}")

            # Fetch no of docs which will be mutated to validate the stats count
            select_query = f"select count(*) from {collection_namespace} where rating = 0"
            result = self.run_cbq_query(query=select_query)
            doc_count = int(result["results"][0]["$1"])

            error_msg_and_doc_count = (f'"The value of a VECTOR expression is expected to be an array of floats. '
                                       f'Found non-array type":{doc_count}')
            self.assertTrue(self.validate_error_msg_and_doc_count_in_cbcollect(self.master, error_msg_and_doc_count))

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

            # Updating dimensions of vector fields for subset of documents and running scans
            keyspace = namespace.split(":")[-1]
            bucket, scope, collection = keyspace.split(".")
            self.gen_update = SDKDataLoader(num_ops=self.num_of_docs_per_collection, percent_create=0,
                                            percent_update=100, percent_delete=0, workers=16, scope=scope,
                                            collection=collection, json_template="Cars", timeout=2000,
                                            op_type="update", mutate=1, dim=128,
                                            update_start=0, update_end=10000)
            self.load_docs_via_magma_server(server=data_node, bucket=bucket, gen=self.gen_update)

            for query in select_queries:
                self.run_cbq_query(query=query)

            # Fetch no of docs which will be mutated to validate the stats count
            select_query = f"select count(*) from {namespace} where rating > 2"
            result = self.run_cbq_query(query=select_query)
            doc_count = int(result["results"][0]["$1"])

            error_msg_and_doc_count = (f'"Length of VECTOR in incoming document is different from '
                                       f'the expected dimension":{doc_count}')
            self.assertTrue(self.validate_error_msg_and_doc_count_in_cbcollect(self.master, error_msg_and_doc_count))

            # Updating dimension of description field back to 384 and re-run scans
            keyspace = namespace.split(":")[-1]
            bucket, scope, collection = keyspace.split(".")
            self.gen_update = SDKDataLoader(num_ops=self.num_of_docs_per_collection, percent_create=0,
                                            percent_update=100, percent_delete=0, workers=16, scope=scope,
                                            collection=collection, json_template="Cars", timeout=2000,
                                            op_type="update", mutate=1, dim=384,
                                            update_start=0, update_end=10000)
            self.load_docs_via_magma_server(server=data_node, bucket=bucket, gen=self.gen_update)

            query_stats_map = {}
            for query in select_queries:
                if "ANN" not in query:
                    continue
                query, recall, accuracy = self.validate_scans_for_recall_and_accuracy(select_query=query)
                query_stats_map[query] = [recall, accuracy]
            self.gen_table_view(query_stats_map=query_stats_map,
                                message=f"quantization value is {self.quantization_algo_description_vector}")

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

            # Updating vector fields in existing documents and running scans after that
            keyspace = namespace.split(":")[-1]
            bucket, scope, collection = keyspace.split(".")
            self.gen_update = SDKDataLoader(num_ops=self.num_of_docs_per_collection, percent_create=100,
                                            percent_update=0, percent_delete=0, workers=16, scope=scope,
                                            collection=collection, json_template="Cars", timeout=2000,
                                            op_type="create", dim=384,
                                            create_start=self.num_of_docs_per_collection,
                                            create_end=(self.num_of_docs_per_collection +
                                                        self.num_of_docs_per_collection // 2))
            self.load_docs_via_magma_server(server=data_node, bucket=bucket, gen=self.gen_update)

            query_stats_map = {}
            for query in select_queries:
                if "ANN" not in query:
                    continue
                query, recall, accuracy = self.validate_scans_for_recall_and_accuracy(select_query=query)
                query_stats_map[query] = [recall, accuracy]
            self.gen_table_view(query_stats_map=query_stats_map,
                                message=f"quantization value is {self.quantization_algo_description_vector}")

            # verify index count matches bucket item count
            self._verify_bucket_count_with_index_count()

    def test_scans_with_expiry_workload_for_composite_vector_index(self):
        data_node = self.get_nodes_from_services_map(service_type="kv")
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename,
                                      skip_default_scope=self.skip_default)

        # change bucket maxTTL to 120 secs to trigger expiration of docs
        self.rest.change_bucket_props(self.buckets[0], maxTTL=120)

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

            # Updating vector fields in existing documents and running scans after that
            keyspace = namespace.split(":")[-1]
            bucket, scope, collection = keyspace.split(".")
            self.gen_update = SDKDataLoader(num_ops=self.num_of_docs_per_collection, percent_create=0,
                                            percent_update=100, percent_delete=0, workers=16, scope=scope,
                                            collection=collection, json_template="Cars", timeout=2000,
                                            op_type="update", dim=384,
                                            update_start=0, update_end=self.num_of_docs_per_collection)
            self.load_docs_via_magma_server(server=data_node, bucket=bucket, gen=self.gen_update)

            # run scans in a loop while docs are getting expired
            start_time = datetime.datetime.now()
            end_time = start_time + datetime.timedelta(minutes=20)

            while datetime.datetime.now() < end_time:
                query_stats_map = {}
                for query in select_queries:
                    if "ANN" not in query:
                        continue
                    query, recall, accuracy = self.validate_scans_for_recall_and_accuracy(select_query=query)
                    query_stats_map[query] = [recall, accuracy]
                self.gen_table_view(query_stats_map=query_stats_map,
                                    message=f"quantization value is {self.quantization_algo_description_vector}")
                self.sleep(5, "waiting before re-running queries")

            # verify index count matches bucket item count
            self._verify_bucket_count_with_index_count()

            # validate that number of indexed docs are zero for all the indexes
            index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
            self.assertTrue(self.check_if_index_count_zero(definitions, index_nodes))

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

    def test_partitioned_index_vector_fields_mutation(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename)
        desc_2 = "A BMW or Mercedes car with high safety rating and fuel efficiency"
        desc_vec2 = list(self.encoder.encode(desc_2))
        collection_namespace = self.namespaces[0]

        scan_desc_vec_2 = f"ANN(descriptionVector, {desc_vec2}, '{self.similarity}', {self.scan_nprobes})"
        partitioned_index_description_vector = QueryDefinition("partitioned_descriptionVector",
                                                               index_fields=['rating', 'descriptionVector Vector',
                                                                             'category'],
                                                               dimension=384,
                                                               description=f"IVF,{self.quantization_algo_description_vector}",
                                                               similarity=self.similarity,
                                                               scan_nprobes=self.scan_nprobes,
                                                               limit=self.scan_limit, is_base64=self.base64,
                                                               query_use_index_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format(
                                                                   "description, descriptionVector",
                                                                   "rating = 2 and "
                                                                   "category in ['Convertible', "
                                                                   "'Luxury Car', 'Supercar']",
                                                                   scan_desc_vec_2),
                                                               partition_by_fields=['meta().id']
                                                               )

        create_idx_query = partitioned_index_description_vector.generate_index_create_query(namespace=collection_namespace, num_partition=8)
        self.run_cbq_query(query=create_idx_query)
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
        scan_color_vec_1 = f"ANN(colorRGBVector, {color_vec_1}, '{self.similarity}', {self.scan_nprobes})"

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
            vector_index = QueryDefinition(index_name='vector', index_fields=['description VECTOR'], dimension=384,
                                         description=f"{self.description}", similarity="L2_SQUARED",
                                         is_base64=self.base64)

        query = vector_index.generate_index_create_query(namespace=collection_namespace, defer_build=False)
        self.run_cbq_query(query=query)

        status = self.wait_until_indexes_online()
        self.assertTrue(status)


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
        #creating namespaces and loading docs

        scopes = [f'{self.scope_prefix}_{scope_num + 1}' for scope_num in range(self.num_scopes)]
        self.sleep(10, "Allowing time after collection creation")
        for s_item in scopes:
            collections = [f'{self.collection_prefix}_{coll_num + 1}' for coll_num in range(self.num_collections)]
            for c_item in collections:
                self.namespaces.append(f'default:{self.test_bucket}.{s_item}.{c_item}')
                num_docs = 200
                self.gen_create = SDKDataLoader(num_ops=num_docs, percent_create=100,
                                                percent_update=0, percent_delete=0, scope=s_item,
                                                collection=c_item, json_template=self.json_template,
                                                output=True, username=self.username, password=self.password)

                self.load_docs_via_magma_server(server=data_node, bucket=self.buckets[0], gen=self.gen_create)

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
            build_query = self.gsi_util_obj.get_build_indexes_query(definition_list=definitions, namespace=namespace)
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
                num_docs = self.num_of_docs_per_collection
                self.gen_create = SDKDataLoader(num_ops=num_docs, percent_create=100,
                                                percent_update=0, percent_delete=0, scope=scope,
                                                collection=collection, json_template=self.json_template,
                                                output=True, username=self.username, password=self.password,
                                                create_start=0,
                                                create_end=self.num_of_docs_per_collection)
                self.load_docs_via_magma_server(server=data_node, bucket=bucket, gen=self.gen_create)

            # index building should succeed this time around as there are enough qualifying documents
            # for codebook training
            self.run_cbq_query(query=build_query, server=self.n1ql_node)
            self.wait_until_indexes_online()

            self.gsi_util_obj.query_event.set()
            executor.submit(self.gsi_util_obj.run_continous_query_load,
                            select_queries=select_queries, query_node=query_node)

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
                if "Partial" in index:
                    continue
                self.assertEqual(index_item_count_map[index], self.num_of_docs_per_collection, f"stats {stats}")

            self.gsi_util_obj.query_event.clear()

            # Recall and accuracy check
            for select_query in select_queries:
                # Skipping validation for recall and accuracy against primary index
                if "DISTINCT" in select_query:
                    continue
                self.validate_scans_for_recall_and_accuracy(select_query=select_query)

    def test_random_sampling_across_keyspaces(self):
        self.use_named_namespace = self.input.param("use_named_keyspace", True)
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename)
        collection_namespace = self.namespaces[0]
        #required to get the ground truth data
        primary_idx = f"CREATE PRIMARY INDEX `#primary_4848` on {self.namespaces[0]};"
        self.run_cbq_query(query=primary_idx)
        desc_2 = "A red color car with 4 or 5 star safety rating"
        descVec2 = list(self.encoder.encode(desc_2))
        scan_desc_vec_2 = f"ANN(descriptionVector, {descVec2}, '{self.similarity}', {self.scan_nprobes})"

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

    def test_mixed_dimension_data(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename)
        collection_namespace = self.namespaces[0]
        #required to get the ground truth data
        primary_idx = f"CREATE PRIMARY INDEX `#primary_4848` on {self.namespaces[0]};"
        self.run_cbq_query(query=primary_idx)
        desc_2 = "A red color car with 4 or 5 star safety rating"
        descVec2 = list(self.encoder.encode(desc_2))
        scan_desc_vec_2 = f"ANN(descriptionVector, {descVec2}, '{self.similarity}', {self.scan_nprobes})"
        # query to ensure only 750K docs have vector fields of the dimension on which the vector index created
        modification_query = f"UPDATE {collection_namespace} SET descriptionVector = [100,100,100,1000,1000] WHERE META().id NOT IN (SELECT RAW META().id FROM {collection_namespace} AS inner_coll LIMIT {self.num_centroids});"
        self.run_cbq_query(query=modification_query)

        vector_index = QueryDefinition('oneScalarOneVectorLeading',
                                       index_fields=['descriptionVector VECTOR', 'category'],
                                       dimension=384, description=f"IVF,{self.quantization_algo_description_vector}",
                                       similarity=self.similarity, scan_nprobes=self.scan_nprobes,
                                       train_list=self.trainlist, limit=self.scan_limit, is_base64=self.base64,
                                       query_template=RANGE_SCAN_ORDER_BY_TEMPLATE.format(
                                           f"category, descriptionVector",
                                           'category in ["Sedan", "Luxury Car"] ',
                                           scan_desc_vec_2))

        query = vector_index.generate_index_create_query(namespace=collection_namespace, defer_build=False)
        self.run_cbq_query(query=query)

        #todo stats validation for no of docs skipped - https://jira.issues.couchbase.com/browse/MB-65249

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

    def test_partition_elimination(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename)
        desc_2 = "A BMW or Mercedes car with high safety rating and fuel efficiency"
        desc_vec2 = list(self.encoder.encode(desc_2))
        collection_namespace = self.namespaces[0]

        scan_desc_vec_2 = f"ANN(descriptionVector, {desc_vec2}, '{self.similarity}', {self.scan_nprobes})"

        #creating partitioned index with partition on a single key
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
                                                               partition_by_fields=['fuel']
                                                               )
        index = partitioned_index_description_vector.generate_index_create_query(namespace=collection_namespace, num_partition=8)
        self.run_cbq_query(query=index)
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
                                                               partition_by_fields=['fuel, rating']
                                                               )
        index = partitioned_index_description_vector.generate_index_create_query(namespace=collection_namespace,
                                                                                 num_partition=8)
        self.run_cbq_query(query=index)
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
                                                               partition_by_fields=['fuel, rating, manufacturer']
                                                               )
        index = partitioned_index_description_vector.generate_index_create_query(namespace=collection_namespace,
                                                                                 num_partition=8)
        self.run_cbq_query(query=index)
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








