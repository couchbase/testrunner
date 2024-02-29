"""collections_indexes_with_missing_keys.py: These tests validate indexer behavior for Missing keyword

__author__ = "Hemant Rajput"
__maintainer = "Hemant Rajput"
__email__ = "Hemant.Rajput@couchbase.com"
__git_user__ = "hrajput89"
__created_on__ = "24/05/22 2:20 pm"

"""
from couchbase_helper.documentgenerator import SDKDataLoader
from couchbase_helper.query_definitions import QueryDefinition
from .base_gsi import BaseSecondaryIndexingTests


class CollectionsIndexesWithMissingKeys(BaseSecondaryIndexingTests):
    def setUp(self):
        super(CollectionsIndexesWithMissingKeys, self).setUp()
        self.log.info("==============  CollectionsIndexesWithMissingKeys setup has started ==============")
        self.rest.delete_all_buckets()
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        self.prepare_collection_for_indexing(num_of_docs_per_collection=self.num_of_docs_per_collection)
        self.index_where_clause = self.input.param('index_where_clause', None)
        self.dml_ops = self.input.param('dml_ops', False)
        self.log.info("==============  CollectionsIndexesWithMissingKeys setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  CollectionsIndexesWithMissingKeys tearDown has started ==============")
        super(CollectionsIndexesWithMissingKeys, self).tearDown()
        self.log.info("==============  CollectionsIndexesWithMissingKeys tearDown has completed ==============")

    def test_create_index_with_missing(self):
        collection_namespace = self.namespaces[0]
        index_gen_1 = QueryDefinition(index_name='idx1', index_fields=['age', 'city', 'country'])
        index_gen_2 = QueryDefinition(index_name='idx2', index_fields=['age', 'city', 'country'])

        if self.partitoned_index:
            query = index_gen_1.generate_index_create_query(namespace=collection_namespace, defer_build=False,
                                                            partition_by_fields=self.partition_fields,
                                                            num_partition=self.num_partition,
                                                            num_replica=self.num_index_replicas,
                                                            index_where_clause=self.index_where_clause)
        else:
            query = index_gen_1.generate_index_create_query(namespace=collection_namespace, defer_build=False,
                                                            num_replica=self.num_index_replicas,
                                                            index_where_clause=self.index_where_clause)
        self.run_cbq_query(query=query)
        self.wait_until_indexes_online()

        # Checking the select query runs with where clause on leading key
        select_age_query = f'select count(age) from {collection_namespace} where age >= 0'
        select_country_query = f'select count(country) from {collection_namespace} where country is not null'

        result = self.run_cbq_query(query=select_age_query)['results'][0]['$1']
        self.assertEqual(result, self.num_of_docs_per_collection, "Doc count not matching")

        # Checking for negative Syntax case for missing keys
        sytnax_err_msg = 'MISSING attribute not allowed (Only allowed with gsi leading key)'
        try:
            query = f'CREATE INDEX invalid_idx ON {collection_namespace}(age, city INCLUDE MISSING ASC, country)'
            self.run_cbq_query(query=query)
            self.fail("Create index passed for invalid syntax")
        except Exception as err:
            self.assertTrue(sytnax_err_msg in str(err), "Error message in not matching")
        try:
            query = f'CREATE INDEX invalid_idx ON {collection_namespace}(age, city, country INCLUDE MISSING DESC)'
            self.run_cbq_query(query=query)
            self.fail("Create index passed for invalid syntax")
        except Exception as err:
            self.assertTrue(sytnax_err_msg in str(err), "Error message in not matching")
        # Checking the select query runs with where clause on non-leading key
        err_msg = 'No index available on keyspace'
        try:
            self.run_cbq_query(query=select_country_query)
            self.fail(f"Query - {select_country_query} - should have failed as no primary index is available")
        except Exception as err:
            if err_msg not in str(err):
                self.fail(err)

        if self.index_where_clause:
            drop_query = index_gen_1.generate_index_drop_query(namespace=collection_namespace)
            self.run_cbq_query(drop_query)
            self.wait_until_indexes_online()
        if self.partitoned_index:
            query = index_gen_2.generate_index_create_query(namespace=collection_namespace, defer_build=False,
                                                            missing_indexes=True, num_replica=self.num_index_replicas,
                                                            missing_field_desc=self.missing_field_desc,
                                                            partition_by_fields=self.partition_fields,
                                                            num_partition=self.num_partition,
                                                            index_where_clause=self.index_where_clause)
        else:
            query = index_gen_2.generate_index_create_query(namespace=collection_namespace, defer_build=False,
                                                            missing_indexes=True, num_replica=self.num_index_replicas,
                                                            missing_field_desc=self.missing_field_desc,
                                                            index_where_clause=self.index_where_clause)
        self.run_cbq_query(query=query)
        self.wait_until_indexes_online()

        # Checking the select query runs with where clause on leading key
        result = self.run_cbq_query(query=select_age_query, scan_consistency='request_plus')['results'][0]['$1']
        self.assertEqual(result, self.num_of_docs_per_collection, "Doc count not matching")

        # Checking the select query runs with where clause on non-leading key
        try:
            result = self.run_cbq_query(query=select_country_query)['results'][0]['$1']
            self.assertEqual(result, self.num_of_docs_per_collection, "Doc count not matching")
        except Exception as err:
            if self.index_where_clause:
                if err_msg not in str(err):
                    self.fail(err)
            else:
                self.fail(err)

        if self.dml_ops:
            update_query = f"UPDATE {collection_namespace} SET age = -1 WHERE META().id = 'doc_1';"
            self.run_cbq_query(query=update_query)
            self.sleep(10)

            result = self.run_cbq_query(query=select_age_query)['results'][0]['$1']
            self.assertEqual(result, self.num_of_docs_per_collection - 1, "Doc count not matching")

            doc_value = {
                "age": 50,
                "city": "Gotham City",
                "country": "United States of America",
                "filler1": [
                    "odit",
                    "laborum",
                    "accusamus",
                    "tempore",
                    "corrupti",
                    "et",
                ],
                "firstName": "Bruce",
                "lastName": "Wayne",
                "streetAddress": "Wayne Manor",
                "suffix": "V",
                "title": "Cape Crusader"
            }
            insert_query = f'INSERT INTO {collection_namespace} (KEY, VALUE) VALUES ("new_doc_1", {doc_value})'
            self.run_cbq_query(query=insert_query)
            self.sleep(10)

            result = self.run_cbq_query(query=select_age_query)['results'][0]['$1']
            self.assertEqual(result, self.num_of_docs_per_collection, "Doc count not matching")

            merge_query = F'MERGE INTO {collection_namespace} AS target ' \
                          F'USING [{{"age": 30}}] AS source ' \
                          F'ON target.age = source.age ' \
                          F'WHEN MATCHED THEN  UPDATE SET target.country = "UNIDENTIFIED", target.age = -1;'
            self.run_cbq_query(query=merge_query)
            self.sleep(10)

            result = self.run_cbq_query(query=select_age_query)['results'][0]['$1']
            self.assertTrue(result < self.num_of_docs_per_collection, "Doc count not matching")

    def test_create_index_with_partial_on_non_leading_key(self):
        collection_namespace = self.namespaces[0]
        index_gen = QueryDefinition(index_name='idx1', index_fields=['age', 'city', 'country'])

        # Checking the select query runs with where clause on leading key
        select_age_query = f'select count(age) from {collection_namespace} where age >= 0'
        select_country_query = f'select count(country) from {collection_namespace} where country is not null'

        # Checking the select query runs with where clause on non-leading key
        err_msg = 'No index available on keyspace'
        if self.partitoned_index:
            query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=False,
                                                          missing_indexes=True, num_replica=self.num_index_replicas,
                                                          missing_field_desc=self.missing_field_desc,
                                                          partition_by_fields=self.partition_fields,
                                                          num_partition=self.num_partition,
                                                          index_where_clause=self.index_where_clause)
        else:
            query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=False,
                                                          missing_indexes=True, num_replica=self.num_index_replicas,
                                                          missing_field_desc=self.missing_field_desc,
                                                          index_where_clause=self.index_where_clause)
        self.run_cbq_query(query=query)
        self.wait_until_indexes_online()

        # Checking the select query runs with where clause on leading key
        try:
            result = self.run_cbq_query(query=select_age_query)['results'][0]['$1']
            self.assertEqual(result, self.num_of_docs_per_collection, "Doc count not matching")
        except Exception as err:
            if self.index_where_clause:
                if err_msg not in str(err):
                    self.fail(err)
            else:
                self.fail(err)

        # Checking the select query runs with where clause on non-leading key
        result = self.run_cbq_query(query=select_country_query)['results'][0]['$1']
        self.assertEqual(result, self.num_of_docs_per_collection, "Doc count not matching")

    def test_equivalent_indexes_with_missing(self):
        collection_namespace = self.namespaces[0]
        select_country_query = f'select count(country) from {collection_namespace} where country is not null'
        select_age_query = f'select count(age) from {collection_namespace} where age >= 0'
        index_gen1 = QueryDefinition(index_name='idx1', index_fields=['age'])
        query = index_gen1.generate_index_create_query(namespace=collection_namespace, defer_build=False)
        self.run_cbq_query(query=query)
        self.sleep(60, "Waiting some time before checking the index status")
        self.wait_until_indexes_online()
        index_status = self.index_rest.get_indexer_metadata()['status']
        node1, node2 = None, None
        for index in index_status:
            index_host = index['hosts'][0].split(':')[0]
            for server in self.servers:
                if index_host == server.ip and node1 is None:
                    node1 = f"{server.ip}:{self.node_port}"
                elif node2 is None:
                    node2 = f"{server.ip}:{self.node_port}"
                if node1 and node2:
                    break
        index_gen2 = QueryDefinition(index_name='idx2', index_fields=['city'])
        query = index_gen2.generate_index_create_query(namespace=collection_namespace, defer_build=False,
                                                       deploy_node_info=node2)
        self.run_cbq_query(query=query)
        self.wait_until_indexes_online()

        index_gen3 = QueryDefinition(index_name='idx3', index_fields=['country'])
        query = index_gen3.generate_index_create_query(namespace=collection_namespace, defer_build=False,
                                                       deploy_node_info=node2)
        self.run_cbq_query(query=query)
        self.wait_until_indexes_online()

        # This index should be placed on node one as it would be least loaded node and non-equivalent index
        index_gen4 = QueryDefinition(index_name='idx4', index_fields=['age'])
        query = index_gen4.generate_index_create_query(namespace=collection_namespace, defer_build=False,
                                                       missing_indexes=True, num_replica=self.num_index_replicas,
                                                       missing_field_desc=self.missing_field_desc,
                                                       index_where_clause=self.index_where_clause)
        self.run_cbq_query(query=query)
        self.wait_until_indexes_online()

        index_status = self.index_rest.get_indexer_metadata()['status']
        for index in index_status:
            index_name = index['name']
            if index_name == 'idx4':
                index_host = index['hosts'][0]
                self.assertEqual(index_host, node1)
                break
        else:
            self.fail("Index host not matching")

        drop_query = index_gen2.generate_index_drop_query(namespace=collection_namespace)
        self.run_cbq_query(query=drop_query)
        drop_query = index_gen3.generate_index_drop_query(namespace=collection_namespace)
        self.run_cbq_query(query=drop_query)

        result = self.run_cbq_query(query=select_age_query)['results'][0]['$1']
        self.assertEqual(result, self.num_of_docs_per_collection, "Doc count not matching")

        # Checking the select query runs for non-leading key without primary index
        result = self.run_cbq_query(query=select_country_query)['results'][0]['$1']
        self.assertEqual(result, self.num_of_docs_per_collection, "Doc count not matching")

    def test_covering_indexes_with_missing_keywords(self):
        collection_namespace = self.namespaces[0]
        index_gen = QueryDefinition(index_name='idx1', index_fields=['age', 'city', 'country'])

        # Checking the select query runs with where clause on leading key
        select_age_query = f'select count(age) from {collection_namespace} where age >= 0'
        select_country_query = f'select count(country) from {collection_namespace} where country is not null'

        # Checking the select query runs with where clause on non-leading key
        if self.partitoned_index:
            query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=False,
                                                          missing_indexes=True, num_replica=self.num_index_replicas,
                                                          missing_field_desc=self.missing_field_desc,
                                                          partition_by_fields=self.partition_fields,
                                                          num_partition=self.num_partition,
                                                          index_where_clause=self.index_where_clause)
        else:
            query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=False,
                                                          missing_indexes=True, num_replica=self.num_index_replicas,
                                                          missing_field_desc=self.missing_field_desc,
                                                          index_where_clause=self.index_where_clause)
        self.run_cbq_query(query=query)
        self.wait_until_indexes_online()

        # Checking the select query runs with where clause on leading key
        explain_result = self.run_cbq_query(query=f'EXPLAIN {select_age_query}')['results']
        self.assertEqual(explain_result[0]['plan']['~children'][0]['index'], 'idx1')
        result = self.run_cbq_query(query=select_age_query)['results'][0]['$1']
        self.assertEqual(result, self.num_of_docs_per_collection, "Doc count not matching")

        # Checking the select query runs with where clause on non-leading key
        explain_result = self.run_cbq_query(query=f'EXPLAIN {select_country_query}')['results']
        self.assertEqual(explain_result[0]['plan']['~children'][0]['index'], 'idx1')
        result = self.run_cbq_query(query=select_country_query)['results'][0]['$1']
        self.assertEqual(result, self.num_of_docs_per_collection, "Doc count not matching")

    def test_create_index_with_missing_with_at_plus_scan(self):
        collection_namespace = self.namespaces[0]
        index_gen = QueryDefinition(index_name='idx1', index_fields=['age', 'city', 'country'])

        # Checking the select query runs with where clause on leading key
        select_age_query = f'select count(age) from {collection_namespace} where age >= 0'
        select_country_query = f'select count(country) from {collection_namespace} where country is not null'

        # Checking the select query runs with where clause on non-leading key
        if self.partitoned_index:
            query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=False,
                                                          missing_indexes=True, num_replica=self.num_index_replicas,
                                                          missing_field_desc=self.missing_field_desc,
                                                          partition_by_fields=self.partition_fields,
                                                          num_partition=self.num_partition,
                                                          index_where_clause=self.index_where_clause)
        else:
            query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=False,
                                                          missing_indexes=True, num_replica=self.num_index_replicas,
                                                          missing_field_desc=self.missing_field_desc,
                                                          index_where_clause=self.index_where_clause)
        self.run_cbq_query(query=query)
        self.wait_until_indexes_online()

        scan_vectors_before_mutations = self.get_mutation_vectors()
        gen_create = SDKDataLoader(num_ops=self.num_of_docs_per_collection*2, percent_create=100,
                                   json_template="Person", percent_update=0, percent_delete=0, scope='test_scope_1',
                                   collection='test_collection_1', output=True)
        task = self.cluster.async_load_gen_docs(self.master, self.test_bucket, gen_create)
        out = task.result()
        self.log.info(out)
        self.sleep(15, "Waiting some time before checking for mutation vectors")
        scan_vectors_after_mutations = self.get_mutation_vectors()
        new_scan_vectors = scan_vectors_after_mutations - scan_vectors_before_mutations
        scan_vector = self.convert_mutation_vector_to_scan_vector(new_scan_vectors)
        # Checking the select query runs with where clause on leading key
        result = self.run_cbq_query(query=select_age_query, scan_consistency='at_plus', scan_vector=scan_vector)['results'][0]['$1']
        self.assertEqual(result, self.num_of_docs_per_collection*2, "Doc count not matching")

        # Checking the select query runs with where clause on non-leading key
        result = self.run_cbq_query(query=select_country_query, scan_consistency='at_plus', scan_vector=scan_vector)['results'][0]['$1']
        self.assertEqual(result, self.num_of_docs_per_collection*2, "Doc count not matching")
