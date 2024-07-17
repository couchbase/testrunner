"""composite_vector_index.py: "This class test composite Vector indexes  for GSI"

__author__ = "Hemant Rajput"
__maintainer = "Hemant Rajput"
__email__ = "Hemant.Rajput@couchbase.com"
__git_user__ = "hrajput89"
__created_on__ = "19/06/24 03:27 pm"

"""


import ast
import re
import numpy as np
from gsi.base_gsi import BaseSecondaryIndexingTests
from membase.api.on_prem_rest_client import RestConnection
from serverless.gsi_utils import GSIUtils
from couchbase_helper.query_definitions import QueryDefinition
from concurrent.futures import ThreadPoolExecutor



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

    def test_create_indexes(self):
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
                                             load_default_coll=True, model=self.data_model, base64=self.base64)

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
                if "PRIMARY" not in create :
                    self.validate_scans_for_recall_and_accuracy(select_query=select)
                # todo validate indexer metadata for indexes

                self.run_cbq_query(query=drop)

    def test_create_index_negetive_scenarios(self):
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        self.prepare_collection_for_indexing(json_template=self.dataset, model=self.data_model, num_of_docs_per_collection=self.num_of_docs_per_collection)
        collection_namespace = self.namespaces[0]
        # indexes with more than one vector field
        index_gen_1 = QueryDefinition(index_name='colorRGBVector_1', index_fields=['colorRGBVector VECTOR','descriptionVector VECTOR'],
                            dimension=3, description="IVF,PQ3x8", similarity="L2_SQUARED")
        try:
            query = index_gen_1.generate_index_create_query(namespace=collection_namespace)
            self.run_cbq_query(query=query)
        except Exception as err:
            err_msg = 'Cannot have more than one vector index key'
            self.assertTrue(err_msg in str(err), f"Index with duplicate named is create: {err}")

        #indexes with include clause for a vector field
        index_gen_2 = QueryDefinition(index_name='colorRGBVector_2', index_fields=['colorRGBVector VECTOR INCLUDE MISSING DESC','description'],
                            dimension=3, description="IVF,PQ3x8", similarity="L2_SQUARED")
        try:
            query = index_gen_2.generate_index_create_query(namespace=collection_namespace)
            self.run_cbq_query(query=query)
        except Exception as err:
            err_msg = 'Cannot mix index attribute VECTOR with'
            self.assertTrue(err_msg in str(err), f"Index with INCLUDE clause is created: {err}")

        #indexes with empty collection defer build false
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
        self.prepare_collection_for_indexing(json_template=self.dataset, model=self.data_model, num_of_docs_per_collection=self.num_of_docs_per_collection)
        collection_namespace = self.namespaces[0]

        #build a scalar and vector index seqentially
        scalar_idx = QueryDefinition(index_name='scalar', index_fields=['color'])
        vector_idx = QueryDefinition(index_name='vector', index_fields=['colorRGBVector VECTOR'], dimension=3, description="IVF,PQ3x8", similarity="L2_SQUARED")

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
        self.prepare_collection_for_indexing(json_template=self.dataset, model=self.data_model,num_of_docs_per_collection=self.num_of_docs_per_collection)
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
            self.cluster.create_standard_bucket(name=self.test_bucket+f'_{b+1}', port=11222,
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
            scalar_idx = QueryDefinition(index_name=idx_name+'scalar', index_fields=['color'])
            vector_idx = QueryDefinition(index_name=idx_name+'vector', index_fields=['colorRGBVector VECTOR'], dimension=3,
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


    def validate_scans_for_recall_and_accuracy_for_recall_and_accuracy(self, select_query):
        faiss_query = self.convert_to_faiss_queries(select_query=select_query)
        vectorField, vector = self.extract_vector_field_and_query_vector(select_query)
        query_res_faiss = self.run_cbq_query(query=faiss_query)['results']
        list_of_vectors_to_be_indexed_on_faiss = []
        for v in query_res_faiss:
            list_of_vectors_to_be_indexed_on_faiss.append(v[vectorField])
        faiss_db = self.load_data_into_faiss(vectors=np.array(list_of_vectors_to_be_indexed_on_faiss, dtype="float32"))

        df, ann = self.search_ann_in_faiss(query=vector, faiss_db=faiss_db, limit=self.scan_limit)
        self.log.info(f'faiss df {df}')

        faiss_closest_vectors = []
        for idx in ann:
            self.log.info(f'for idx {idx} vector is {list_of_vectors_to_be_indexed_on_faiss[idx]}')
            faiss_closest_vectors.append(list_of_vectors_to_be_indexed_on_faiss[idx])
        gsi_query_res = self.run_cbq_query(query=select_query)['results']
        gsi_query_vec_list = []

        for v in gsi_query_res:
            gsi_query_vec_list.append(v[vectorField])

        self.log.info(f'len of qv list is {len(gsi_query_vec_list)}')
        self.log.info(f'len of faiss list is {len(faiss_closest_vectors)}')

        recall = accuracy = 0

        for ele in gsi_query_vec_list:
            if ele in faiss_closest_vectors:
                recall += 1

        for qv, faiss in zip(gsi_query_vec_list, faiss_closest_vectors):
            if qv == faiss:
                accuracy += 1

        accuracy = accuracy/self.scan_limit
        recall = recall/self.scan_limit

        self.log.info(f"accuracy for the query : {select_query} is {accuracy}")
        self.log.info(f"recall for the query is : {select_query} is {recall}")

    def extract_vector_field_and_query_vector(self, query):
        pattern = r"ANN\(\s*(\w+),\s*(\[(?:-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?(?:,\s*)?)+\])"
        matches = re.search(pattern, query)

        if matches:
            vectorField = matches.group(1)
            vector = matches.group(2)
            self.log.info(f"vectorField: {vectorField}")
            self.log.info(f"Vector : {vector}")
            vector = ast.literal_eval(vector)
            return vectorField, vector
        else:
            raise Exception("no fields extracted")