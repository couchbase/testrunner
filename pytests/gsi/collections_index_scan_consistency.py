"""collections_index_scan_consistency.py: Tests to check index scan consistency

__author__ = "Hemant Rajput"
__maintainer = "Hemant Rajput"
__email__ = "Hemant.Rajput@couchbase.com"
__git_user__ = "hrajput89"
__created_on__ = "07/08/20 10:51 am"

"""

from concurrent.futures import ThreadPoolExecutor
from couchbase_helper.documentgenerator import SDKDataLoader
from couchbase_helper.query_definitions import QueryDefinition
from deepdiff import DeepDiff

from .base_gsi import BaseSecondaryIndexingTests


class CollectionsIndexScanConsistency(BaseSecondaryIndexingTests):
    def setUp(self):
        super(CollectionsIndexScanConsistency, self).setUp()
        self.log.info("==============  CollectionsIndexBasics setup has started ==============")
        self.rest.delete_all_buckets()
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        self._create_server_groups()
        self.log.info("==============  CollectionsIndexBasics setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  CollectionsIndexBasics tearDown has started ==============")
        super(CollectionsIndexScanConsistency, self).tearDown()
        self.log.info("==============  CollectionsIndexBasics tearDown has completed ==============")

    def test_request_plus_index_consistency(self):
        """
        Summary: This test validate request_plus scan consistency with flooding high data load to cluster and
         at the same instance issuing select query to fetch new docs
        """
        num_of_docs = 10 ** 5
        self.prepare_collection_for_indexing(num_of_docs_per_collection=num_of_docs)
        collection_namespace = self.namespaces[0]
        _, keyspace = collection_namespace.split(':')
        bucket, scope, collection = keyspace.split('.')
        index_gen = QueryDefinition(index_name='idx', index_fields=['age', 'country', 'city'])
        meta_index_gen = QueryDefinition(index_name='meta_idx', index_fields=['meta().id'])
        doc_body = {
            'age': 34,
            'country': 'test_country',
            'city': 'test_city',
            'filler1': ['ut', 'distinctio', 'sit', 'inventore', 'quo', 'quos', 'saepe', 'doloremque', 'sed', 'omnis'],
            'firstName': 'Mitch',
            'lastName': 'Funk',
            'streetAddress': '66877 Williamson Terrace',
            'suffix': 'V',
            'title': 'International Solutions Coordinator'
        }
        insert_query = f'INSERT INTO {collection_namespace} (KEY, VALUE) VALUES ("scan_doc_1", {doc_body})'
        query = index_gen.generate_index_create_query(namespace=collection_namespace)
        self.run_cbq_query(query=query)
        query = meta_index_gen.generate_index_create_query(namespace=collection_namespace)
        self.run_cbq_query(query=query)
        self.wait_until_indexes_online()
        gen_create = SDKDataLoader(num_ops=2 * num_of_docs, percent_create=100,
                                   percent_update=0, percent_delete=0, scope=scope,
                                   collection=collection, start_seq_num=num_of_docs + 1)
        select_query = f'Select country, city from {collection_namespace} where meta().id = "scan_doc_1"'
        count_query = f'Select count(meta().id) from {collection_namespace} where age >= 0'
        result = self.run_cbq_query(query=count_query)['results'][0]['$1']
        self.assertEqual(result, num_of_docs)
        try:
            with ThreadPoolExecutor() as executor:
                executor.submit(self._load_all_buckets, self.master, gen_create)
                executor.submit(self.run_cbq_query, query=insert_query)
                self.sleep(30, "Giving some time so the mutations start")
                select_task = executor.submit(self.run_cbq_query, query=select_query, scan_consistency='request_plus')
                count_task = executor.submit(self.run_cbq_query, query=count_query, scan_consistency='request_plus')

                result1 = select_task.result()['results'][0]
                result2 = count_task.result()['results'][0]['$1']

            self.assertEqual(result1, {'city': 'test_city', 'country': 'test_country'},
                             "scan_doc_1 which was inserted before scan request with request_plus is not in result")
            self.assertTrue(result2 > num_of_docs + 1, "request plus scan is not able to wait for new inserted docs")
        except Exception as err:
            self.fail(str(err))

    def test_at_plus_index_consistency(self):
        num_of_docs = 10 ** 3
        self.prepare_collection_for_indexing(num_of_docs_per_collection=num_of_docs, json_template="Hotel")
        collection_namespace = self.namespaces[0]
        _, keyspace = collection_namespace.split(':')
        bucket, scope, collection = keyspace.split('.')

        index_gen = QueryDefinition(index_name='idx', index_fields=['price', 'country', 'city'])
        meta_index_gen = QueryDefinition(index_name='meta_idx', index_fields=['meta().id'])
        query = index_gen.generate_index_create_query(namespace=collection_namespace)
        self.run_cbq_query(query=query)
        query = meta_index_gen.generate_index_create_query(namespace=collection_namespace)
        self.run_cbq_query(query=query)
        self.wait_until_indexes_online()

        select_query = f'Select * from {collection_namespace} where price >100 and country like "A%";'
        select_meta_id_query = f'Select * from {collection} where meta().id like "doc_100%";'
        count_query = f'Select count(*) from {collection_namespace} where price >= 0;'
        named_collection_query_context = f'default:{bucket}.{scope}'

        meta_id_result_before_new_inserts = self.run_cbq_query(query=select_meta_id_query,
                                                               query_context=named_collection_query_context)['results']
        scan_vectors_before_mutations = self.get_mutation_vectors()
        new_insert_docs_num = 2
        gen_create = SDKDataLoader(num_ops=new_insert_docs_num, percent_create=100, json_template="Hotel",
                                   percent_update=0, percent_delete=0, scope=scope,
                                   collection=collection, output=True, start_seq_num=num_of_docs + 1)
        tasks = self.data_ops_javasdk_loader_in_batches(sdk_data_loader=gen_create, batch_size=1000)
        for task in tasks:
            out = task.result()
            self.log.info(out)

        self.sleep(15, "Waiting some time before checking for mutation vectors")
        scan_vectors_after_mutations = self.get_mutation_vectors()
        new_scan_vectors = scan_vectors_after_mutations - scan_vectors_before_mutations
        scan_vector = self.convert_mutation_vector_to_scan_vector(new_scan_vectors)

        result = self.run_cbq_query(query=count_query)['results'][0]['$1']
        self.assertEqual(result, num_of_docs + new_insert_docs_num)

        try:
            # Test with inserts on named collection
            with ThreadPoolExecutor() as executor:
                select_task = executor.submit(self.run_cbq_query, query=select_query, scan_consistency='at_plus',
                                              scan_vector=scan_vector)
                meta_task = executor.submit(self.run_cbq_query, query=select_meta_id_query, scan_consistency='at_plus',
                                            scan_vector=scan_vector, query_context=named_collection_query_context)
                result = select_task.result()['results']
                meta_id_result_after_new_inserts = meta_task.result()['results']
            self.assertTrue(len(result) > 0,
                            "scan_doc_1 which was inserted before scan request with request_plus is not in result")
            self.assertEqual(len(meta_id_result_after_new_inserts),
                             len(meta_id_result_before_new_inserts) + new_insert_docs_num,
                             "request plus scan is not able to wait for new inserted docs")

            # Test with update mutation on named collection
            result1 = \
            self.run_cbq_query(query=f'Select * from {collection_namespace} where meta().id = "doc_1001"')['results'][
                0][collection]
            result2 = \
            self.run_cbq_query(query=f'Select * from {collection_namespace} where meta().id = "doc_1002"')['results'][
                0][collection]
            scan_vectors_before_mutations = self.get_mutation_vectors()
            gen_create = SDKDataLoader(num_ops=new_insert_docs_num, percent_create=0, json_template="Hotel",
                                       percent_update=100, percent_delete=0, scope=scope, fields_to_update=["price"],
                                       collection=collection, output=True, start_seq_num=num_of_docs + 1,
                                       op_type="update")
            tasks = self.data_ops_javasdk_loader_in_batches(sdk_data_loader=gen_create, batch_size=1000)
            for task in tasks:
                out = task.result()
                self.log.info(out)
            self.sleep(15, "Waiting some time before checking for mutation vectors")
            scan_vectors_after_mutations = self.get_mutation_vectors()
            new_scan_vectors = scan_vectors_after_mutations - scan_vectors_before_mutations
            scan_vector = self.convert_mutation_vector_to_scan_vector(new_scan_vectors)

            with ThreadPoolExecutor() as executor:
                select_task = executor.submit(self.run_cbq_query, query=select_query, scan_consistency='at_plus',
                                              scan_vector=scan_vector)
                meta_task = executor.submit(self.run_cbq_query, query=select_meta_id_query, scan_consistency='at_plus',
                                            scan_vector=scan_vector, query_context=named_collection_query_context)
                result = select_task.result()['results']
                meta_id_result_after_new_inserts = meta_task.result()['results']
                result3 = \
                self.run_cbq_query(query=f'Select * from {collection_namespace} where meta().id = "doc_1001"')[
                    'results'][0][collection]
                result4 = \
                self.run_cbq_query(query=f'Select * from {collection_namespace} where meta().id = "doc_1002"')[
                    'results'][0][collection]
                diff1 = DeepDiff(result1, result3, ignore_order=True)
                diff2 = DeepDiff(result2, result4, ignore_order=True)

            self.assertTrue(len(result) > 0,
                            "scan_doc_1 which was inserted before scan request with request_plus is not in result")
            self.assertEqual(len(meta_id_result_after_new_inserts),
                             len(meta_id_result_before_new_inserts) + new_insert_docs_num,
                             "request plus scan is not able to wait for new inserted docs")
            if len(diff1['values_changed']) == 1 and "root['price']" in diff1['values_changed']:
                self.log.info("Price field mutated for doc_1001")
                self.log.info(diff1)
            else:
                self.log.info(diff1)
                self.log.info(f"Before Muatation: {result1}")
                self.log.info(f"After Muatation: {result3}")
                self.fail("Unexpected Mutation found for doc_1001")
            if len(diff2['values_changed']) == 1 and "root['price']" in diff2['values_changed']:
                self.log.info("Price field mutated for doc_1002")
                self.log.info(diff2)
            else:
                self.log.info(diff1)
                self.log.info(f"Before Muatation: {result2}")
                self.log.info(f"After Muatation: {result4}")
                self.fail("Unexpected Mutation found for doc_1002")

            # Test with Delete mutation on named collection
            scan_vectors_before_mutations = self.get_mutation_vectors()
            gen_create = SDKDataLoader(num_ops=new_insert_docs_num, percent_create=0, json_template="Hotel",
                                       percent_update=0, percent_delete=100, scope=scope,
                                       collection=collection, output=True, start_seq_num=num_of_docs + 1)
            tasks = self.data_ops_javasdk_loader_in_batches(sdk_data_loader=gen_create, batch_size=1000)
            for task in tasks:
                out = task.result()
                self.log.info(out)
            self.sleep(30, "Waiting some time before checking for mutation vectors")
            scan_vectors_after_mutations = self.get_mutation_vectors()
            new_scan_vectors = scan_vectors_after_mutations - scan_vectors_before_mutations
            scan_vector = self.convert_mutation_vector_to_scan_vector(new_scan_vectors)

            with ThreadPoolExecutor() as executor:
                select_task = executor.submit(self.run_cbq_query, query=select_query, scan_consistency='at_plus',
                                              scan_vector=scan_vector)
                meta_task = executor.submit(self.run_cbq_query, query=select_meta_id_query, scan_consistency='at_plus',
                                            scan_vector=scan_vector, query_context=named_collection_query_context)
                count_task = executor.submit(self.run_cbq_query, query=count_query)
                result = select_task.result()['results']
                meta_id_result_after_new_inserts = meta_task.result()['results']
                count_result = count_task.result()['results'][0]['$1']
            self.assertTrue(len(result) > 0,
                            "scan_doc_1 which was inserted before scan request with request_plus is not in result")
            self.assertEqual(len(meta_id_result_after_new_inserts),
                             len(meta_id_result_before_new_inserts),
                             "request plus scan is not able to wait for new inserted docs")
            self.assertEqual(count_result, num_of_docs, "Docs count not matching.")

            # Test with new mutation on default collection
            select_query = f'Select * from {bucket} where price > 100 and country like "A%";'
            select_meta_id_query = f'Select meta().id,* from {bucket} where meta().id like "doc_100%";'
            count_query = f'Select count(*) from {bucket} where price >= 0;'
            named_collection_query_context = f'default:'
            scan_vectors_before_mutations = self.get_mutation_vectors()
            gen_create = SDKDataLoader(num_ops=10 ** 3, percent_create=100, json_template="Hotel",
                                       percent_update=0, percent_delete=0, scope='_default',
                                       collection='_default', output=True)
            tasks = self.data_ops_javasdk_loader_in_batches(sdk_data_loader=gen_create, batch_size=1000)
            for task in tasks:
                out = task.result()
                self.log.info(out)

            scan_vectors_after_mutations = self.get_mutation_vectors()
            new_scan_vectors = scan_vectors_after_mutations - scan_vectors_before_mutations
            scan_vector = self.convert_mutation_vector_to_scan_vector(new_scan_vectors)

            default_index_gen = QueryDefinition(index_name='default_idx', index_fields=['price', 'country', 'city'])
            default_meta_index_gen = QueryDefinition(index_name='default_meta_idx', index_fields=['meta().id'])
            query = default_index_gen.generate_index_create_query(namespace=bucket)
            self.run_cbq_query(query=query)
            query = default_meta_index_gen.generate_index_create_query(namespace=bucket)
            self.run_cbq_query(query=query)

            with ThreadPoolExecutor() as executor:
                select_task = executor.submit(self.run_cbq_query, query=select_query, scan_consistency='at_plus',
                                              scan_vector=scan_vector)
                meta_task = executor.submit(self.run_cbq_query, query=select_meta_id_query, scan_consistency='at_plus',
                                            scan_vector=scan_vector, query_context=named_collection_query_context)
                count_task = executor.submit(self.run_cbq_query, query=count_query)
                result = select_task.result()['results']
                meta_id_result_after_new_inserts = meta_task.result()['results']
                count_result = count_task.result()['results'][0]['$1']
            self.assertTrue(len(result) > 0,
                            "scan_doc_1 which was inserted before scan request with request_plus is not in result")
            self.assertEqual(len(meta_id_result_after_new_inserts), 2,
                             "request plus scan is not able to wait for new inserted docs")
            self.assertEqual(count_result, num_of_docs, "Docs count not matching.")
        except Exception as err:
            self.fail(str(err))

    def test_at_plus_index_consistency_with_multiple_buckets(self):
        """
        This test is running queries with scan vectors
        """
        num_of_docs = 10 ** 3
        self.rest.delete_all_buckets()
        bucket_1 = 'test_bucket_1'
        bucket_2 = 'test_bucket_2'
        self.cluster.create_standard_bucket(name=bucket_1, port=11222, bucket_params=self.bucket_params)
        self.cluster.create_standard_bucket(name=bucket_2, port=11222, bucket_params=self.bucket_params)
        collection_namespaces = []
        scope_prefix = 'test_scope'
        collection_prefix = 'test_collection'

        data_load_tasks = []
        for bucket in (bucket_1, bucket_2):
            for s_item in range(self.num_scopes):
                scope = f'{scope_prefix}_{s_item}'
                self.collection_rest.create_scope(bucket=bucket, scope=scope)
                for c_item in range(self.num_collections):
                    collection = f'{collection_prefix}_{c_item}'
                    self.collection_rest.create_collection(bucket=bucket, scope=scope, collection=collection)
                    self.sleep(10)
                    gen_create = SDKDataLoader(num_ops=num_of_docs, percent_create=100,
                                               percent_update=0, percent_delete=0, scope=scope,
                                               collection=collection, json_template='Hotel', output=True)
                    task = self.cluster.async_load_gen_docs(self.master, bucket, gen_create, timeout_secs=300)
                    data_load_tasks.append(task)
                    collection_namespaces.append(f'default:{bucket}.{scope}.{collection}')
        for task in data_load_tasks:
            task.result()

        for collection_namespace in collection_namespaces:
            index_gen = QueryDefinition(index_name='idx', index_fields=['price', 'country', 'city'])
            meta_index_gen = QueryDefinition(index_name='meta_idx', index_fields=['meta().id'])
            query = index_gen.generate_index_create_query(namespace=collection_namespace)
            self.run_cbq_query(query=query)
            query = meta_index_gen.generate_index_create_query(namespace=collection_namespace)
            self.run_cbq_query(query=query)
            self.wait_until_indexes_online()

        select_query1 = f'Select * from {collection_namespaces[0]} where price >100 and country like "A%";'
        select_query2 = f'Select * from {collection_namespaces[1]} where price >100 and country like "A%";'
        select_meta_id_query = f'Select * from test_collection_0 where meta().id like "doc_100%";'
        count_query1 = f'Select count(*) from {collection_namespaces[0]} where price >= 0;'
        count_query2 = f'Select count(*) from {collection_namespaces[1]} where price >= 0;'
        named_collection_query_context1 = f'default:{bucket_1}.test_scope_0'
        named_collection_query_context2 = f'default:{bucket_2}.test_scope_0'

        meta_id_result_before_inserts1 = self.run_cbq_query(query=select_meta_id_query,
                                                            query_context=named_collection_query_context1)['results']
        meta_id_result_before_inserts2 = self.run_cbq_query(query=select_meta_id_query,
                                                            query_context=named_collection_query_context2)['results']
        scan_vectors_before_mutations = self.get_mutation_vectors()
        new_insert_docs_num = 2
        gen_create = SDKDataLoader(num_ops=new_insert_docs_num, percent_create=100, json_template="Hotel",
                                   percent_update=0, percent_delete=0, scope='test_scope_0',
                                   collection='test_collection_0', output=True, start_seq_num=num_of_docs + 1)
        scan_vectors = {}
        task = self.cluster.async_load_gen_docs(self.master, bucket_1, gen_create)
        out = task.result()
        self.log.info(out)
        self.sleep(15, "Waiting some time before checking for mutation vectors")
        scan_vectors_after_mutations = self.get_mutation_vectors()
        new_scan_vectors = scan_vectors_after_mutations - scan_vectors_before_mutations
        scan_vector = self.convert_mutation_vector_to_scan_vector(new_scan_vectors)
        scan_vectors[bucket_1] = scan_vector

        scan_vectors_before_mutations = self.get_mutation_vectors()
        task = self.cluster.async_load_gen_docs(self.master, bucket_2, gen_create)
        out = task.result()
        self.log.info(out)
        self.sleep(15, "Waiting some time before checking for mutation vectors")
        scan_vectors_after_mutations = self.get_mutation_vectors()
        new_scan_vectors = scan_vectors_after_mutations - scan_vectors_before_mutations
        scan_vector = self.convert_mutation_vector_to_scan_vector(new_scan_vectors)
        scan_vectors[bucket_2] = scan_vector

        result = self.run_cbq_query(query=count_query1)['results'][0]['$1']
        self.assertEqual(result, num_of_docs + new_insert_docs_num)
        result = self.run_cbq_query(query=count_query2)['results'][0]['$1']
        self.assertEqual(result, num_of_docs + new_insert_docs_num)

        try:
            with ThreadPoolExecutor() as executor:
                select_task1 = executor.submit(self.run_cbq_query, query=select_query1, scan_consistency='at_plus',
                                               scan_vectors=scan_vectors)
                meta_task1 = executor.submit(self.run_cbq_query, query=select_meta_id_query, scan_consistency='at_plus',
                                             scan_vectors=scan_vectors, query_context=named_collection_query_context1)
                result1 = select_task1.result()['results']
                meta_id_result_after_inserts1 = meta_task1.result()['results']

                select_task2 = executor.submit(self.run_cbq_query, query=select_query2, scan_consistency='at_plus',
                                               scan_vectors=scan_vectors)
                meta_task2 = executor.submit(self.run_cbq_query, query=select_meta_id_query, scan_consistency='at_plus',
                                             scan_vectors=scan_vectors, query_context=named_collection_query_context2)
                result2 = select_task2.result()['results']
                meta_id_result_after_inserts2 = meta_task2.result()['results']
            self.assertTrue(len(result1) > 0,
                            "scan_doc_1 which was inserted before scan request with request_plus is not in result")
            self.assertEqual(len(meta_id_result_after_inserts1),
                             len(meta_id_result_before_inserts1) + new_insert_docs_num,
                             "request plus scan is not able to wait for new inserted docs")
            self.assertTrue(len(result2) > 0,
                            "scan_doc_1 which was inserted before scan request with request_plus is not in result")
            self.assertEqual(len(meta_id_result_after_inserts2),
                             len(meta_id_result_before_inserts2) + new_insert_docs_num,
                             "request plus scan is not able to wait for new inserted docs")
        except Exception as err:
            self.fail(str(err))

    def test_at_plus_scans_with_catching_up_indexer(self):
        num_of_docs = self.num_of_docs_per_collection
        scan_vectors_before_mutations = self.get_mutation_vectors()
        self.prepare_collection_for_indexing(num_of_docs_per_collection=num_of_docs, json_template="Hotel")
        collection_namespace = self.namespaces[0]
        _, keyspace = collection_namespace.split(':')
        bucket, scope, collection = keyspace.split('.')

        index_gen = QueryDefinition(index_name='idx', index_fields=['price', 'country', 'city'],
                                    partition_by_fields=['price'])
        meta_index_gen = QueryDefinition(index_name='meta_idx', index_fields=['meta().id'])
        query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=True)
        self.run_cbq_query(query=query)
        query = meta_index_gen.generate_index_create_query(namespace=collection_namespace, num_replica=1,
                                                           defer_build=True)
        self.run_cbq_query(query=query)
        scan_vectors_after_mutations = self.get_mutation_vectors()
        new_scan_vectors = sorted(list(scan_vectors_after_mutations - scan_vectors_before_mutations))
        scan_vector = self.convert_mutation_vector_to_scan_vector(new_scan_vectors)

        select_query = f'Select * from {collection_namespace} where price >= 0;'
        select_meta_id_query = f'Select meta().id,* from {collection} where meta().id like "doc_100%";'
        count_query = f'Select count(*) from {collection_namespace} where price >= 0;'
        named_collection_query_context = f'default:{bucket}.{scope}'

        gen_create = SDKDataLoader(num_ops=num_of_docs, percent_create=100, json_template="Hotel",
                                   percent_update=0, percent_delete=0, scope=scope,
                                   collection=collection, output=True, start_seq_num=num_of_docs + 1)
        try:
            with ThreadPoolExecutor() as executor:
                build_query = f"BUILD INDEX ON {collection_namespace} (idx, meta_idx)"
                tasks = self.data_ops_javasdk_loader_in_batches(sdk_data_loader=gen_create, batch_size=10000)
                executor.submit(self.run_cbq_query, query=build_query)
                self.sleep(30, "Giving some time to build indexes")

                select_task = executor.submit(self.run_cbq_query, query=select_query, scan_consistency='at_plus',
                                              scan_vector=scan_vector)
                meta_task = executor.submit(self.run_cbq_query, query=select_meta_id_query, scan_consistency='at_plus',
                                            scan_vector=scan_vector, query_context=named_collection_query_context)
                count_task = executor.submit(self.run_cbq_query, query=count_query)
                result = select_task.result()['results']
                meta_id_result_after_new_inserts = meta_task.result()['results']
                count_result = count_task.result()['results'][0]['$1']
                self.assertTrue(len(meta_id_result_after_new_inserts) > 0,
                                "at_plus didn't wait for all catchup")
                self.assertTrue(count_result > num_of_docs, "at_plus didn't wait for all catchup")
                self.assertTrue(len(result) >= num_of_docs, "at_plus didn't wait for all catchup")
                for task in tasks:
                    task.result()
        except Exception as err:
            self.fail(err)

    def test_at_plus_index_consistency_with_paused_state(self):
        if self.gsi_type != 'memory_optimized':
            self.skipTest("This test run only with GSI type MOI")
        curr_index_quota = self.rest.get_index_official_stats()['indexer']['memory_quota']
        num_of_docs = self.num_of_docs_per_collection
        self.prepare_collection_for_indexing(num_of_docs_per_collection=num_of_docs, json_template="Hotel")
        collection_namespace = self.namespaces[0]
        _, keyspace = collection_namespace.split(':')
        bucket, scope, collection = keyspace.split('.')

        index_gen = QueryDefinition(index_name='idx', index_fields=['price', 'country', 'city'],
                                    partition_by_fields=['price'])
        meta_index_gen = QueryDefinition(index_name='meta_idx', index_fields=['meta().id'])
        query = index_gen.generate_index_create_query(namespace=collection_namespace)
        self.run_cbq_query(query=query)
        query = meta_index_gen.generate_index_create_query(namespace=collection_namespace)
        self.run_cbq_query(query=query)
        is_paused = False
        new_inserts = 10 ** 4
        while not is_paused:
            gen_create = SDKDataLoader(num_ops=new_inserts, percent_create=100, json_template="Hotel",
                                       percent_update=0, percent_delete=0, scope=scope,
                                       collection=collection, output=True, start_seq_num=num_of_docs + 1)
            task = self.cluster.async_load_gen_docs(self.master, bucket, gen_create)
            task.result()
            # Updating the doc counts
            num_of_docs = num_of_docs + new_inserts
            index_metadata = self.index_rest.get_indexer_metadata()['status']
            for index in index_metadata:
                if index['status'] == 'Paused':
                    is_paused = True

        scan_vectors_before_mutations = self.get_mutation_vectors()
        # Adding more data so that indexer has to catchup after increasing indexer quota
        for i in range(5):
            gen_create = SDKDataLoader(num_ops=new_inserts, percent_create=100, json_template="Hotel",
                                       percent_update=0, percent_delete=0, scope=scope,
                                       collection=collection, output=True, start_seq_num=num_of_docs + 1)
            task = self.cluster.async_load_gen_docs(self.master, bucket, gen_create)
            task.result()
            # Updating the doc counts
            num_of_docs = num_of_docs + new_inserts

        scan_vectors_after_mutations = self.get_mutation_vectors()
        new_scan_vectors = sorted(list(scan_vectors_after_mutations - scan_vectors_before_mutations))
        scan_vector = self.convert_mutation_vector_to_scan_vector(new_scan_vectors)
        self.rest.set_service_memoryQuota(service='indexMemoryQuota',
                                          memoryQuota=int(curr_index_quota/1024/1024) + 100)
        self.sleep(60)
        index_metadata = self.index_rest.get_indexer_metadata()['status']
        for index in index_metadata:
            if index['status'] == 'Paused':
                self.fail("Indexer is still in Paused stated. Either increase memory or sleep time")
        select_query = f'Select * from {collection_namespace} where price >= 0;'
        select_meta_id_query = f'Select meta().id,* from {collection} where meta().id like "doc_100%";'
        count_query = f'Select count(*) from {collection_namespace} where price >= 0;'
        named_collection_query_context = f'default:{bucket}.{scope}'
        try:
            with ThreadPoolExecutor() as executor:
                select_task = executor.submit(self.run_cbq_query, query=select_query, scan_consistency='at_plus',
                                              scan_vector=scan_vector)
                meta_task = executor.submit(self.run_cbq_query, query=select_meta_id_query, scan_consistency='at_plus',
                                            scan_vector=scan_vector, query_context=named_collection_query_context)
                count_task = executor.submit(self.run_cbq_query, query=count_query)
                result = select_task.result()['results']
                meta_id_result_after_new_inserts = meta_task.result()['results']
                count_result = count_task.result()['results'][0]['$1']
                self.assertTrue(len(meta_id_result_after_new_inserts) > 0,
                                "at_plus didn't wait for all catchup")
                self.assertEqual(count_result, num_of_docs, "at_plus didn't wait for all catchup")
                self.assertEqual(len(result), num_of_docs, "at_plus didn't wait for all catchup")
        except Exception as err:
            self.fail(err)
