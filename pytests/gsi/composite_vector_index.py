"""composite_vector_index.py: "This class test composite Vector indexes  for GSI"

__author__ = "Hemant Rajput"
__maintainer = "Hemant Rajput"
__email__ = "Hemant.Rajput@couchbase.com"
__git_user__ = "hrajput89"
__created_on__ = "19/06/24 03:27 pm"

"""

import ast
import os.path
import re
import subprocess
from urllib.request import urlretrieve

import numpy as np

from collection.gsi.backup_restore_utils import IndexBackupClient
from gsi.base_gsi import BaseSecondaryIndexingTests
from couchbase_helper.query_definitions import QueryDefinition
from concurrent.futures import ThreadPoolExecutor

from remote.remote_util import RemoteMachineShellConnection


class CompositeVectorIndex(BaseSecondaryIndexingTests):
    def setUp(self):
        super(CompositeVectorIndex, self).setUp()
        self.log.info("==============  CompositeVectorIndex setup has started ==============")
        self.namespaces = []
        self.dimension = self.input.param("dimension", None)
        self.trainlist = self.input.param("trainlist", None)
        self.description = self.input.param("description", None)
        self.similarity = self.input.param("similarity", "L2_SQUARED")
        self.scan_nprobes = self.input.param("scan_nprobes", 1)
        self.scan_limit = self.input.param("scan_limit", 100)
        self.multi_move = self.input.param("multi_move", False)
        self.build_phase = self.input.param("build_phase", "create")
        self.skip_default = self.input.param("skip_default", True)
        self.rest.set_internalSetting("magmaMinMemoryQuota", 1024)
        self.log.info("==============  CompositeVectorIndex setup has ended ==============")

    def tearDown(self):
        self.log.info("==============  CompositeVectorIndex tearDown has started ==============")
        super(CompositeVectorIndex, self).tearDown()
        self.log.info("==============  CompositeVectorIndex tearDown has completed ==============")

    def suite_setUp(self):
        pass

    def suite_tearDown(self):
        pass

    def restore_couchbase_bucket(self, backup_filename, skip_default_scope=True):
        filename = backup_filename.split('/')[-1]
        self.s3_utils_obj.download_file(object_name=backup_filename,
                                        filename=filename)
        remote_client = RemoteMachineShellConnection(self.master)
        remote_client.copy_file_local_to_remote(src_path=filename, des_path=filename)
        repo = filename.split('.')[0]
        remote_client.execute_command("rm -rf backup")
        backup_config_cmd = f"/opt/couchbase/bin/cbbackupmgr config --archive backup/ --repo {repo}"
        out = remote_client.execute_command(backup_config_cmd)
        self.log.info(out)
        if "failed" in out[0]:
            self.fail(out)
        self.log.info("unzip the backup repo before restoring it")
        unzip_cmd = f"unzip -o {filename}"
        remote_client.execute_command(command=unzip_cmd)
        restore_cmd = f"/opt/couchbase/bin/cbbackupmgr restore --archive backup --repo {repo} " \
                      f"--cluster couchbase://127.0.0.1 --username {self.username} --password {self.password} " \
                      f"-auto-create-buckets "
        restore_out = remote_client.execute_command(restore_cmd)
        self.log.info(restore_out)

        self.sleep(30)
        self.buckets = self.rest.get_buckets()
        self.namespaces = []
        for bucket in self.buckets:
            scopes = self.rest.get_bucket_scopes(bucket=bucket)
            for scope in scopes:
                if scope == '_default' and skip_default_scope:
                    continue
                collections = self.rest.get_scope_collections(bucket=bucket, scope=scope)
                for collection in collections:
                    self.namespaces.append(f"{bucket}.{scope}.{collection}")
        self.log.info("namespaces created sucessfully")
        self.log.info(self.namespaces)

    def validate_scans_for_recall_and_accuracy(self, select_query):
        faiss_query = self.convert_to_faiss_queries(select_query=select_query)
        vector_field, vector = self.extract_vector_field_and_query_vector(select_query)
        query_res_faiss = self.run_cbq_query(query=faiss_query)['results']
        list_of_vectors_to_be_indexed_on_faiss = []
        for v in query_res_faiss:
            list_of_vectors_to_be_indexed_on_faiss.append(v[vector_field])
        faiss_db = self.load_data_into_faiss(vectors=np.array(list_of_vectors_to_be_indexed_on_faiss, dtype="float32"))

        ann = self.search_ann_in_faiss(query=vector, faiss_db=faiss_db, limit=self.scan_limit)

        faiss_closest_vectors = []
        for idx in ann:
            # self.log.info(f'for idx {idx} vector is {list_of_vectors_to_be_indexed_on_faiss[idx]}')
            faiss_closest_vectors.append(list_of_vectors_to_be_indexed_on_faiss[idx])
        gsi_query_res = self.run_cbq_query(query=select_query)['results']
        gsi_query_vec_list = []

        for v in gsi_query_res:
            gsi_query_vec_list.append(v[vector_field])

        # self.log.info(f'len of qv list is {len(gsi_query_vec_list)}')
        # self.log.info(f'len of faiss list is {len(faiss_closest_vectors)}')

        recall = accuracy = 0

        for ele in gsi_query_vec_list:
            if ele in faiss_closest_vectors:
                recall += 1

        for qv, faiss in zip(gsi_query_vec_list, faiss_closest_vectors):
            if qv == faiss:
                accuracy += 1

        accuracy = accuracy / self.scan_limit
        recall = recall / self.scan_limit
        faiss_db.reset()



        self.log.info(f"accuracy for the query : {select_query} is {accuracy}")
        self.log.info(f"recall for the query is : {select_query} is {recall}")

    def extract_vector_field_and_query_vector(self, query):
        pattern = r"ANN\(\s*(\w+),\s*(\[(?:-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?(?:,\s*)?)+\])"
        matches = re.search(pattern, query)

        if matches:
            vector_field = matches.group(1)
            vector = matches.group(2)
            # self.log.info(f"vector_field: {vector_field}")
            self.log.info(f"Vector : {vector}")
            vector = ast.literal_eval(vector)
            return vector_field, vector
        else:
            raise Exception("no fields extracted")

    def test_create_indexes(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        self.log.info('test started')
        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                      prefix='test',
                                                                      similarity=self.similarity, train_list=None,
                                                                      scan_nprobes=self.scan_nprobes,
                                                                      array_indexes=False,
                                                                      limit=self.scan_limit, quantization_algo_color_vector=self.quantization_algo_color_vector, quantization_algo_description_vector=self.quantization_algo_description_vector)
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions, namespace=namespace)
            select_queries = self.gsi_util_obj.get_select_queries(definition_list=definitions,
                                                                  namespace=namespace, limit=self.scan_limit)

            drop_queries = self.gsi_util_obj.get_drop_index_list(definition_list=definitions, namespace=namespace)

            for create, select, drop in zip(create_queries, select_queries, drop_queries):
                self.run_cbq_query(query=create)
                if "PRIMARY" in create:
                    continue
                self.validate_scans_for_recall_and_accuracy(select_query=select)
                # todo validate indexer metadata for indexes

                self.run_cbq_query(query=drop)

    def test_create_index_negative_scenarios(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)

        collection_namespace = self.namespaces[0]
        # indexes with more than one vector field
        index_gen_1 = QueryDefinition(index_name='colorRGBVector_1',
                                      index_fields=['colorRGBVector VECTOR', 'descriptionVector VECTOR'],
                                      dimension=3, description="IVF,PQ3x8", similarity="L2_SQUARED")
        try:
            query = index_gen_1.generate_index_create_query(namespace=collection_namespace)
            self.run_cbq_query(query=query)
        except Exception as err:
            err_msg = 'Cannot have more than one vector index key'
            self.assertTrue(err_msg in str(err), f"Index with more than one vector field is created: {err}")

        # indexes with include clause for a vector field
        index_gen_2 = QueryDefinition(index_name='colorRGBVector_2',
                                      index_fields=['colorRGBVector VECTOR INCLUDE MISSING DESC', 'description'],
                                      dimension=3, description="IVF,PQ3x8", similarity="L2_SQUARED")
        try:
            query = index_gen_2.generate_index_create_query(namespace=collection_namespace)
            self.run_cbq_query(query=query)
        except Exception as err:
            err_msg = 'INCLUDE MISSING'
            self.assertTrue(err_msg in str(err), f"Index with INCLUDE clause is created: {err}")

        #indexes with empty collection defer build false
        scope = '_default'
        collection = '_default'

        self.collection_rest.create_scope_collection(bucket=self.test_bucket, scope=scope, collection=collection)
        collection_namespace = f"default:{self.test_bucket}.{scope}.{collection}"
        index_gen_3 = QueryDefinition(index_name='colorRGBVector_3',
                                      index_fields=['colorRGBVector VECTOR', 'description'],
                                      dimension=3, description="IVF,PQ3x8", similarity="L2_SQUARED")
        try:
            query = index_gen_3.generate_index_create_query(namespace=collection_namespace)
            self.run_cbq_query(query=query)
        except Exception as err:
            err_msg = 'ErrTraining: The number of documents: 0 in keyspace:'
            self.assertTrue(err_msg in str(err), f"Index with INCLUDE clause is created: {err}")

        # indexes with empty collection defer build true
        collection_namespace = f"default:{self.test_bucket}.{scope}.{collection}"
        index_gen_3 = QueryDefinition(index_name='colorRGBVector_4',
                                      index_fields=['colorRGBVector VECTOR', 'description'],
                                      dimension=3, description="IVF,PQ3x8", similarity="L2_SQUARED")

        query = index_gen_3.generate_index_create_query(namespace=collection_namespace, defer_build=True)
        self.run_cbq_query(query=query)
        self.assertEqual(len(self.index_rest.get_indexer_metadata()['status']), 1, 'defer true index not created')

    def test_build_index_scenarios(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        collection_namespace = self.namespaces[0]

        # build a scalar and vector index seqentially
        scalar_idx = QueryDefinition(index_name='scalar', index_fields=['color'])
        vector_idx = QueryDefinition(index_name='vector', index_fields=['colorRGBVector VECTOR'], dimension=3,
                                     description="IVF,PQ3x8", similarity="L2_SQUARED")

        for idx in [scalar_idx, vector_idx]:
            query = idx.generate_index_create_query(namespace=collection_namespace, defer_build=True)
            self.run_cbq_query(query=query)

        for idx in ['scalar', 'vector']:
            query = f"BUILD INDEX ON {collection_namespace}({idx}) USING GSI "
            self.run_cbq_query(query=query)
            self.sleep(5)

        meta_data = self.index_rest.get_indexer_metadata()['status']
        self.assertEqual(len(meta_data), 2 , f'indexes are not built metadata {meta_data}')

    def test_build_idx_less_than_required_centroids(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)

        collection_namespace = self.namespaces[0]

        vector_idx = QueryDefinition(index_name='vector', index_fields=['colorRGBVector VECTOR'], dimension=3,
                                     description="IVF204800,PQ3x8", similarity="L2_SQUARED")
        query = vector_idx.generate_index_create_query(namespace=collection_namespace, defer_build=True)

        self.run_cbq_query(query=query)
        # for the scenario where num docs less than centroids
        try:
            query = f"BUILD INDEX ON {collection_namespace}(vector) USING GSI "
            self.run_cbq_query(query=query)
        except Exception as err:
            err_msg = 'are less than the minimum number of documents:'
            self.assertTrue(err_msg in str(err), f"Index with less docs are created: {err}")
        else:
            raise Exception("index has been built with less no of centroids")

    def test_kill_index_process_during_training(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        collection_namespace = self.namespaces[0]

        vector_idx = QueryDefinition(index_name='vector', index_fields=['description VECTOR'], dimension=384,
                                     description="IVF1024,PQ32x8", similarity="L2_SQUARED")
        query = vector_idx.generate_index_create_query(namespace=collection_namespace, defer_build=False)
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
            self.assertEqual("Error", idx['status'],'index has been created ')

    def test_concurrent_vector_index_builds(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)

        # build a scalar and vector index parallely on different namespaces

        index_build_list = []
        for item, namespace in enumerate(self.namespaces):
            idx_name = f'idx_{item}'
            scalar_idx = QueryDefinition(index_name=idx_name + 'scalar', index_fields=['color'])
            vector_idx = QueryDefinition(index_name=idx_name + 'vector', index_fields=['colorRGBVector VECTOR'],
                                         dimension=3,
                                         description="IVF,PQ3x8", similarity="L2_SQUARED")
            query1 = scalar_idx.generate_index_create_query(namespace=namespace, defer_build=True)
            query2 = vector_idx.generate_index_create_query(namespace=namespace, defer_build=True)
            self.run_cbq_query(query=query1)
            self.run_cbq_query(query=query2)
            build_query_1 = scalar_idx.generate_build_query(namespace=namespace)
            build_query_2 = vector_idx.generate_build_query(namespace=namespace)
            index_build_list.append(build_query_1)
            index_build_list.append(build_query_2)

        with ThreadPoolExecutor() as executor:
            for query in index_build_list:
                executor.submit(self.run_cbq_query, query=query)
        self.wait_until_indexes_online(timeout=600)
        meta_data = self.index_rest.get_indexer_metadata()['status']
        self.assertEqual(len(meta_data), len(self.namespaces)*2, f"indexes are not created meta data {meta_data}")


    def test_rebalance(self):
        redistribute = {"indexer.settings.rebalance.redistribute_indexes": True}
        self.rest.set_index_settings(redistribute)
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        select_queries = []
        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                      prefix='test',
                                                                      similarity=self.similarity, train_list=None,
                                                                      scan_nprobes=self.scan_nprobes,
                                                                      array_indexes=False,
                                                                      limit=self.scan_limit, quantization_algo_color_vector=self.quantization_algo_color_vector, quantization_algo_description_vector=self.quantization_algo_description_vector)
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions, namespace=namespace,
                                                                     num_replica=1)
            self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, query_node=query_node)

            select_queries.extend(self.gsi_util_obj.get_select_queries(definition_list=definitions, namespace=namespace))
        with ThreadPoolExecutor() as executor:
            select_task = executor.submit(self.gsi_util_obj.run_continous_query_load,
                                          select_queries=select_queries, query_node=query_node)
            # if rebalance_type = 'rebalance_in':
            #     add_nodes = [self.servers[2]]
            #     task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=add_nodes,
            #                                         to_remove=[], services=['index', 'index'])

    def test_drop_build_indexes_concurrently(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        query_list = []
        collection_namespace = self.namespaces[0]
        vector_idx = QueryDefinition(index_name='vector_rgb', index_fields=['colorRGBVector VECTOR'], dimension=3,
                                     description="IVF2048,PQ3x8", similarity="L2_SQUARED")
        query = vector_idx.generate_index_create_query(namespace=collection_namespace, defer_build=True)
        self.run_cbq_query(query=query)
        build_query = vector_idx.generate_build_query(namespace=collection_namespace)
        query_list.append(build_query)

        vector_idx_2 = QueryDefinition(index_name='vector_description', index_fields=['descriptionVector VECTOR'],
                                       dimension=384,
                                       description="IVF2048,PQ32x8", similarity="L2_SQUARED")
        query = vector_idx_2.generate_index_create_query(namespace=collection_namespace, defer_build=False)
        self.run_cbq_query(query=query)
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
        vector_idx = QueryDefinition(index_name='vector_description', index_fields=['descriptionVector VECTOR'], dimension=384,
                                     description="IVF2048,PQ32x8", similarity="L2_SQUARED")
        defer_build = False
        if self.build_phase == "create":
            defer_build = True
        query = vector_idx.generate_index_create_query(namespace=collection_namespace, defer_build=defer_build)

        drop_query = vector_idx.generate_index_drop_query(namespace=collection_namespace)
        if self.build_phase == "create":
            self.run_cbq_query(query=query)
            self.wait_until_indexes_online()
            self.run_cbq_query(query=drop_query)

        else:
            timeout = 0
            with ThreadPoolExecutor() as executor:
                executor.submit(self.run_cbq_query, query=query)
                self.sleep(5)
                while timeout < 360:
                    index_state = self.index_rest.get_indexer_metadata()['status'][0]['status']
                    if index_state == self.build_phase:
                        self.run_cbq_query(query=drop_query)
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
                                         dimension=3,
                                         description="IVF,PQ3x8", similarity="L2_SQUARED")
            query1 = vector_idx.generate_index_create_query(namespace=namespace)
            self.run_cbq_query(query=query1)
            drop_query_1 = vector_idx.generate_index_drop_query(namespace=namespace)
            index_drop_list.append(drop_query_1)

        with ThreadPoolExecutor() as executor:
            for query in index_drop_list:
                executor.submit(self.run_cbq_query, query=query)
        self.wait_until_indexes_online(timeout=600)
        index_metadata = self.index_rest.get_indexer_metadata()['status']
        self.assertEqual(len(index_metadata), 0 , 'Indexes not dropped')

    def test_alter_index_alter_replica_count(self):
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if len(index_nodes) < 3:
            self.skipTest("Can't run Alter index tests with less than 2 Index nodes")
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        select_queries = set()
        namespace_index_map = {}
        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                      prefix='test',
                                                                      similarity=self.similarity, train_list=None,
                                                                      scan_nprobes=self.scan_nprobes,
                                                                      array_indexes=False,
                                                                      limit=self.scan_limit, quantization_algo_color_vector=self.quantization_algo_color_vector, quantization_algo_description_vector=self.quantization_algo_description_vector)
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions, namespace=namespace,
                                                                     num_replica=self.num_index_replicas)
            select_queries.update(self.gsi_util_obj.get_select_queries(definition_list=definitions,
                                                                       namespace=namespace, limit=self.scan_limit))
            namespace_index_map[namespace] = definitions

            self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, database=namespace)
        self.wait_until_indexes_online()

        for select_query in select_queries:
            # this is to ensure that select queries run primary indexes are not tested for recall and accuracy
            if "ANN" not in select_query:
                continue
            self.validate_scans_for_recall_and_accuracy(select_query=select_query)

        index_metadata = self.index_rest.get_indexer_metadata()['status']
        for index in index_metadata:
            self.assertEqual(index['numReplica'], self.num_index_replicas, "No. of replicas are not matching")

        for namespace in namespace_index_map:
            definition_list = namespace_index_map[namespace]
            for definitions in definition_list:
                # to reduce the no of replicas
                self.alter_index_replicas(index_name=f"`{definitions.index_name}`", namespace=namespace,
                                          action='replica_count', num_replicas=self.num_index_replicas - 1)
                self.wait_until_indexes_online()
                self.sleep(10)

        index_metadata = self.index_rest.get_indexer_metadata()['status']
        for index in index_metadata:
            self.assertEqual(index['numReplica'], self.num_index_replicas - 1,
                             "No. of replicas are not matching post alter query")

        for select_query in select_queries:
            if "ANN" not in select_query:
                continue
            self.validate_scans_for_recall_and_accuracy(select_query=select_query)

        # increasing replica count
        self.num_index_replicas = self.num_index_replicas - 1
        for namespace in namespace_index_map:
            definition_list = namespace_index_map[namespace]
            for definitions in definition_list:
                self.alter_index_replicas(index_name=f"`{definitions.index_name}`", namespace=namespace,
                                          action='replica_count', num_replicas=self.num_index_replicas + 1)
                self.wait_until_indexes_online()
                self.sleep(10)

        index_metadata = self.index_rest.get_indexer_metadata()['status']
        for index in index_metadata:
            self.assertEqual(index['numReplica'], self.num_index_replicas + 1,
                             "No. of replicas are not matching post alter query")

        for select_query in select_queries:
            if "ANN" not in select_query:
                continue
            self.validate_scans_for_recall_and_accuracy(select_query=select_query)

    def test_alter_replica_restricted_nodes(self):
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if len(index_nodes) < 3:
            self.skipTest("Can't run Alter index tests with less than  Index nodes")
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        select_queries = set()
        namespace_index_map = {}
        deploy_nodes = [f"{nodes.ip}:{self.node_port}" for nodes in index_nodes[:2]]
        num_replica = len(deploy_nodes)-1
        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                      prefix='test',
                                                                      similarity=self.similarity, train_list=None,
                                                                      scan_nprobes=self.scan_nprobes,
                                                                      array_indexes=False,
                                                                      limit=self.scan_limit, quantization_algo_color_vector=self.quantization_algo_color_vector, quantization_algo_description_vector=self.quantization_algo_description_vector)
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions, namespace=namespace, deploy_node_info=deploy_nodes, num_replica=num_replica)
            select_queries.update(self.gsi_util_obj.get_select_queries(definition_list=definitions,
                                                                       namespace=namespace, limit=self.scan_limit))
            namespace_index_map[namespace] = definitions

            self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, database=namespace)
        self.wait_until_indexes_online()

        for select_query in select_queries:
            if "ANN" not in select_query:
                continue
            self.validate_scans_for_recall_and_accuracy(select_query=select_query)

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
                self.sleep(10)
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

        for select_query in select_queries:
            if "ANN" not in select_query:
                continue
            self.validate_scans_for_recall_and_accuracy(select_query=select_query)

    def test_alter_index_alter_replica_id(self):
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if len(index_nodes) < 3:
            self.skipTest("Can't run Alter index tests with less than 2 Index nodes")
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        select_queries = set()
        namespace_index_map = {}
        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                      prefix='test',
                                                                      similarity=self.similarity, train_list=None,
                                                                      scan_nprobes=self.scan_nprobes,
                                                                      array_indexes=False,
                                                                      limit=self.scan_limit, quantization_algo_color_vector=self.quantization_algo_color_vector, quantization_algo_description_vector=self.quantization_algo_description_vector)
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions, namespace=namespace, num_replica=self.num_index_replicas)
            select_queries.update(self.gsi_util_obj.get_select_queries(definition_list=definitions,
                                                                       namespace=namespace, limit=self.scan_limit))
            namespace_index_map[namespace] = definitions

            self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, database=namespace)
        self.wait_until_indexes_online()

        for select_query in select_queries:
            if "ANN" not in select_query:
                continue
            self.validate_scans_for_recall_and_accuracy(select_query=select_query)

        index_metadata = self.index_rest.get_indexer_metadata()['status']
        for index in index_metadata:
            self.assertEqual(index['numReplica'], self.num_index_replicas, "No. of replicas are not matching")

        for namespace in namespace_index_map:
            definition_list = namespace_index_map[namespace]
            for definitions in definition_list:
                self.alter_index_replicas(index_name=f"`{definitions.index_name}`", namespace=namespace, action='drop_replica', replica_id=1)
                self.sleep(10)
                self.wait_until_indexes_online()

        index_metadata = self.index_rest.get_indexer_metadata()['status']
        for index in index_metadata:
            self.assertTrue(index['replicaId'] != 1, f"Dropped wrong replica Id for index{index['indexName']}")

        for select_query in select_queries:
            if "ANN" not in select_query:
                continue
            self.validate_scans_for_recall_and_accuracy(select_query=select_query)

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
                                                                      limit=self.scan_limit, quantization_algo_color_vector=self.quantization_algo_color_vector, quantization_algo_description_vector=self.quantization_algo_description_vector)
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions, namespace=namespace, deploy_node_info=deploy_nodes, num_replica=num_replica)
            select_queries.update(self.gsi_util_obj.get_select_queries(definition_list=definitions,
                                                                       namespace=namespace, limit=self.scan_limit))
            namespace_index_map[namespace] = definitions

            self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, database=namespace)
        self.wait_until_indexes_online()

        for select_query in select_queries:
            if "ANN" not in select_query:
                continue
            self.validate_scans_for_recall_and_accuracy(select_query=select_query)

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
                self.sleep(10)
                self.wait_until_indexes_online()


        self.wait_until_indexes_online()
        index_info = self.index_rest.get_indexer_metadata()['status']

        for idx in index_info:
            if self.multi_move:
                self.assertIn(idx['hosts'][0], nodes_targetted, f"Replica has not moved into target node meta data : {index_info}")
            else:
                self.assertEqual(index_nodes[1].ip, idx['hosts'][0].split(":")[0], f"Replica has not moved into target node meta data : {index_info}")

        for select_query in select_queries:
            if "ANN" not in select_query:
                continue
            self.validate_scans_for_recall_and_accuracy(select_query=select_query)



