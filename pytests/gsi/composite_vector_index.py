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
        self.s3_utils_obj.download_file(object_name=backup_filename,
                                        filename=backup_filename)
        copy_backup_bucket_cmd = ["scp", backup_filename, f"root@{self.master.ip}:~"]
        proc = subprocess.Popen(copy_backup_bucket_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        proc.communicate()
        if proc.returncode == 0:
            self.log.info('File successfully uploaded.')
        else:
            self.fail(f'Error uploading file: {proc.stderr}')
        repo = backup_filename.split('.')[0]
        remote_client = RemoteMachineShellConnection(self.master)
        remote_client.execute_command("rm -rf backup")
        backup_config_cmd = f"/opt/couchbase/bin/cbbackupmgr config --archive backup/ --repo {repo}"
        out = remote_client.execute_command(backup_config_cmd)
        self.log.info(out)
        self.log.info("unzip the backup repo before restoring it")
        unzip_cmd = f"unzip -o {backup_filename}"
        remote_client.execute_command(command=unzip_cmd)
        restore_cmd = f"/opt/couchbase/bin/cbbackupmgr restore --archive backup --repo {repo} " \
                      f"--cluster couchbase://127.0.0.1 --username {self.username} --password {self.password} " \
                      f"-auto-create-buckets "
        remote_client.execute_command(restore_cmd)

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
            self.log.info(f'for idx {idx} vector is {list_of_vectors_to_be_indexed_on_faiss[idx]}')
            faiss_closest_vectors.append(list_of_vectors_to_be_indexed_on_faiss[idx])
        gsi_query_res = self.run_cbq_query(query=select_query)['results']
        gsi_query_vec_list = []

        for v in gsi_query_res:
            gsi_query_vec_list.append(v[vector_field])

        self.log.info(f'len of qv list is {len(gsi_query_vec_list)}')
        self.log.info(f'len of faiss list is {len(faiss_closest_vectors)}')

        recall = accuracy = 0

        for ele in gsi_query_vec_list:
            if ele in faiss_closest_vectors:
                recall += 1

        for qv, faiss in zip(gsi_query_vec_list, faiss_closest_vectors):
            if qv == faiss:
                accuracy += 1

        accuracy = accuracy / self.scan_limit
        recall = recall / self.scan_limit

        self.log.info(f"accuracy for the query : {select_query} is {accuracy}")
        self.log.info(f"recall for the query is : {select_query} is {recall}")

    def extract_vector_field_and_query_vector(self, query):
        pattern = r"ANN\(\s*(\w+),\s*(\[(?:-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?(?:,\s*)?)+\])"
        matches = re.search(pattern, query)

        if matches:
            vector_field = matches.group(1)
            vector = matches.group(2)
            self.log.info(f"vector_field: {vector_field}")
            self.log.info(f"Vector : {vector}")
            vector = ast.literal_eval(vector)
            return vector_field, vector
        else:
            raise Exception("no fields extracted")

    def test_create_indexes(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename)

        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                      prefix='test',
                                                                      similarity=self.similarity, train_list=None,
                                                                      scan_nprobes=self.scan_nprobes,
                                                                      array_indexes=False,
                                                                      limit=self.scan_limit)
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
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        self.prepare_collection_for_indexing(json_template=self.dataset, model=self.data_model,
                                             num_of_docs_per_collection=self.num_of_docs_per_collection)
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
            self.assertTrue(err_msg in str(err), f"Index with duplicate named is create: {err}")

        # indexes with include clause for a vector field
        index_gen_2 = QueryDefinition(index_name='colorRGBVector_2',
                                      index_fields=['colorRGBVector VECTOR INCLUDE MISSING DESC', 'description'],
                                      dimension=3, description="IVF,PQ3x8", similarity="L2_SQUARED")
        try:
            query = index_gen_2.generate_index_create_query(namespace=collection_namespace)
            self.run_cbq_query(query=query)
        except Exception as err:
            err_msg = 'Cannot mix index attribute VECTOR with'
            self.assertTrue(err_msg in str(err), f"Index with INCLUDE clause is created: {err}")

        # indexes with empty collection defer build false
        scope = 'test_scope_2'
        collection = 'test_collection_1'
        self.collection_rest.create_scope_collection(bucket=self.test_bucket, scope=scope, collection=collection)
        collection_namespace = f"default:{self.test_bucket}.{scope}.{collection}"
        index_gen_3 = QueryDefinition(index_name='colorRGBVector_3',
                                      index_fields=['colorRGBVector VECTOR INCLUDE MISSING DESC', 'description'],
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
                                      index_fields=['colorRGBVector VECTOR INCLUDE MISSING DESC', 'description'],
                                      dimension=3, description="IVF,PQ3x8", similarity="L2_SQUARED")

        query = index_gen_3.generate_index_create_query(namespace=collection_namespace, defer_build=True)
        self.run_cbq_query(query=query)

    def test_build_index_scenarios(self):
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        self.prepare_collection_for_indexing(json_template=self.dataset, model=self.data_model,
                                             num_of_docs_per_collection=self.num_of_docs_per_collection)
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

        for idx in ['scalar', 'vector']:
            query = f"DROP INDEX ON {collection_namespace}({idx}) USING GSI "
            self.run_cbq_query(query=query)
            self.sleep(5)

    def test_build_idx_empty_collections(self):
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        self.prepare_collection_for_indexing(json_template=self.dataset, model=self.data_model,
                                             num_of_docs_per_collection=self.num_of_docs_per_collection)
        collection_namespace = self.namespaces[0]

        vector_idx = QueryDefinition(index_name='vector', index_fields=['colorRGBVector VECTOR'], dimension=3,
                                     description="IVF2048,PQ3x8", similarity="L2_SQUARED")
        query = vector_idx.generate_index_create_query(namespace=collection_namespace, defer_build=True)
        self.run_cbq_query(query=query)

        try:
            query = f"BUILD INDEX ON {collection_namespace}(vector) USING GSI "
            self.run_cbq_query(query=query)
        except Exception as err:
            err_msg = 'ErrTraining: The number of documents: 0 in keyspace:'
            self.assertTrue(err_msg in str(err), f"Index with INCLUDE clause is created: {err}")

    def test_concurrent_vector_index_builds(self):
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        for b in range(self.num_buckets):
            self.cluster.create_standard_bucket(name=self.test_bucket + f'_{b + 1}', port=11222,
                                                bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        for bucket in self.buckets:
            self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                                 num_of_docs_per_collection=self.num_of_docs_per_collection,
                                                 json_template=self.json_template, bucket_name=bucket.name,
                                                 model=self.data_model, base64=self.base64, load_default_coll=True)

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

    def test_rebalance(self):
        redistribute = {"indexer.settings.rebalance.redistribute_indexes": True}
        self.rest.set_index_settings(redistribute)
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        select_queries = []
        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                      prefix='test',
                                                                      similarity=self.similarity, train_list=None,
                                                                      scan_nprobes=self.scan_nprobes,
                                                                      array_indexes=False,
                                                                      limit=self.scan_limit)
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions, namespace=namespace,
                                                                     num_replica=1)
            self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, query_node=query_node)

            select_queries.extend(self.gsi_util_obj.get_select_queries(definition_list=definitions, namespace=namespace))
        with ThreadPoolExecutor() as executor:
            select_task = executor.submit(self.gsi_util_obj.run_continous_query_load,
                                          select_queries=select_queries, query_node=query_node)
            if rebalance_type = 'rebalance_in':
                add_nodes = [self.servers[2]]
                task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=add_nodes,
                                                    to_remove=[], services=['index', 'index'])
