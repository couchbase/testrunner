"""composite_vector_index.py: "This class test composite Vector indexes  for GSI"

__author__ = "Hemant Rajput"
__maintainer = "Hemant Rajput"
__email__ = "Hemant.Rajput@couchbase.com"
__git_user__ = "hrajput89"
__created_on__ = "19/06/24 03:27 pm"

"""




from table_view import TableView
from gsi.base_gsi import BaseSecondaryIndexingTests
from couchbase_helper.query_definitions import QueryDefinition, FULL_SCAN_ORDER_BY_TEMPLATE, RANGE_SCAN_USE_INDEX_ORDER_BY_TEMPLATE
from concurrent.futures import ThreadPoolExecutor
from membase.api.on_prem_rest_client import RestHelper


class CompositeVectorIndex(BaseSecondaryIndexingTests):
    def setUp(self):
        super(CompositeVectorIndex, self).setUp()
        self.log.info("==============  CompositeVectorIndex setup has started ==============")
        self.namespaces = []
        self.multi_move = self.input.param("multi_move", False)
        self.build_phase = self.input.param("build_phase", "create")
        self.skip_default = self.input.param("skip_default", True)
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
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
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
            query_stats_map = {}
            for create, select, drop in zip(create_queries, select_queries, drop_queries):
                self.run_cbq_query(query=create)
                if "PRIMARY" in create:
                    continue
                query, recall, accuracy = self.validate_scans_for_recall_and_accuracy(select_query=select)
                query_stats_map[query] = [recall, accuracy]
                # todo validate indexer metadata for indexes

                self.run_cbq_query(query=drop)
            self.gen_table_view(query_stats_map=query_stats_map, message=f"quantization value is {self.quantization_algo_description_vector}")
            for query in query_stats_map:
                self.assertGreaterEqual(query_stats_map[query][0] * 100, 70,
                                        f"recall for query {query} is less than threshold 70")
                self.assertGreaterEqual(query_stats_map[query][1] * 100, 70,
                                        f"accuracy for query {query} is less than threshold 70")

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
        self.index_rest.set_index_settings(redistribute)
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=False)
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        select_queries = []
        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                      similarity=self.similarity, train_list=None,
                                                                      scan_nprobes=self.scan_nprobes,
                                                                      array_indexes=False,
                                                                      limit=self.scan_limit,
                                                                      quantization_algo_description_vector=self.quantization_algo_description_vector,
                                                                      quantization_algo_color_vector=self.quantization_algo_color_vector)
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions, namespace=namespace,
                                                                     num_replica=1)
            self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, query_node=query_node)

            select_queries.extend(
                self.gsi_util_obj.get_select_queries(definition_list=definitions, namespace=namespace))

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
            # Todo: Add metadata validation
        self.validate_scans_for_recall_and_accuracy(select_query=select_queries)

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

        self.display_recall_and_accuracy_stats(select_queries=select_queries,
                                               message="results before reducing num replica count", stats_assertion=False)

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
                self.sleep(20)

        index_metadata = self.index_rest.get_indexer_metadata()['status']
        for index in index_metadata:
            self.assertEqual(index['numReplica'], self.num_index_replicas - 1,
                             "No. of replicas are not matching post alter query")

        self.display_recall_and_accuracy_stats(select_queries=select_queries,
                                               message="results after reducing num replica count")

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

        self.display_recall_and_accuracy_stats(select_queries=select_queries,
                                               message="results after increasing num replica count")

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

        self.display_recall_and_accuracy_stats(select_queries=select_queries,
                                               message="results before moving indexes to specifc node", stats_assertion=False)

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
                                               message="results after moving indexes to specifc node")

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

        self.display_recall_and_accuracy_stats(select_queries=select_queries, message="results before dropping replica id", stats_assertion=False)

        index_metadata = self.index_rest.get_indexer_metadata()['status']
        for index in index_metadata:
            self.assertEqual(index['numReplica'], self.num_index_replicas, "No. of replicas are not matching")

        for namespace in namespace_index_map:
            definition_list = namespace_index_map[namespace]
            for definitions in definition_list:
                self.alter_index_replicas(index_name=f"`{definitions.index_name}`", namespace=namespace, action='drop_replica', replica_id=1)
                self.sleep(20)
                self.wait_until_indexes_online()

        index_metadata = self.index_rest.get_indexer_metadata()['status']
        for index in index_metadata:
            self.assertTrue(index['replicaId'] != 1, f"Dropped wrong replica Id for index{index['indexName']}")

        self.display_recall_and_accuracy_stats(select_queries=select_queries,
                                               message="results before after replica id")

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

        self.display_recall_and_accuracy_stats(select_queries=select_queries, message="results before move index via alter query", stats_assertion=False)

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
                self.assertIn(idx['hosts'][0], nodes_targetted, f"Replica has not moved into target node meta data : {index_info}")
            else:
                self.assertEqual(index_nodes[1].ip, idx['hosts'][0].split(":")[0], f"Replica has not moved into target node meta data : {index_info}")

        self.display_recall_and_accuracy_stats(select_queries=select_queries, message="results after move index via alter query")
    def test_scan_comparison_between_trained_and_untrained_indexes(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        desc_2 = "A BMW or Mercedes car with high safety rating and fuel efficiency"
        descVec2 = list(self.encoder.encode(desc_2))

        scan_desc_vec_2 = f"ANN(descriptionVector, {descVec2}, '{self.similarity}', {self.scan_nprobes})"

        scan_color_vec_1 = f"ANN(colorRGBVector, [43.0, 133.0, 178.0], '{self.similarity}', {self.scan_nprobes})"
        collection_namespace = self.namespaces[0]
        # indexes with more than one vector field
        trained_index_colorRGBVector = QueryDefinition(index_name="trained_rgb", index_fields=['colorRGBVector VECTOR'],
                                        dimension=3, description=f"IVF,{self.quantization_algo_color_vector}",
                                        similarity=self.similarity, scan_nprobes=self.scan_nprobes,
                                        limit=self.scan_limit,
                                        query_template=FULL_SCAN_ORDER_BY_TEMPLATE.format(f"color, colorRGBVector,"
                                                                                          f" {scan_color_vec_1}",
                                                                                          scan_color_vec_1))

        untrained_index_colorRGBVector = QueryDefinition(index_name="untrained_rgb", index_fields=['colorRGBVector VECTOR'],
                                          dimension=3, description=f"IVF,{self.quantization_algo_color_vector}",
                                          similarity=self.similarity, scan_nprobes=self.scan_nprobes,
                                          limit=self.scan_limit,
                                          query_template=FULL_SCAN_ORDER_BY_TEMPLATE.format(
                                              f"color, colorRGBVector,"
                                              f" {scan_color_vec_1}",
                                              scan_color_vec_1))

        trained_index_descriptionVector = QueryDefinition(index_name="trained_description", index_fields=['descriptionVector VECTOR'],
                                        dimension=384, description=f"IVF,{self.quantization_algo_description_vector}",
                                        similarity=self.similarity, scan_nprobes=self.scan_nprobes,
                                        limit=self.scan_limit,
                                        query_template=FULL_SCAN_ORDER_BY_TEMPLATE.format(f"description, descriptionVector,"
                                                                                          f" {scan_desc_vec_2}",
                                                                                          scan_desc_vec_2))

        untrained_index_descriptionVector = QueryDefinition(index_name="untrained_description", index_fields=['descriptionVector VECTOR'],
                                          dimension=384, description=f"IVF,{self.quantization_algo_description_vector}",
                                          similarity=self.similarity, scan_nprobes=self.scan_nprobes,
                                          limit=self.scan_limit,
                                          query_template=FULL_SCAN_ORDER_BY_TEMPLATE.format(
                                              f"description, descriptionVector,"
                                              f" {scan_desc_vec_2}",
                                              scan_desc_vec_2))

        for idx in [trained_index_colorRGBVector, untrained_index_colorRGBVector, trained_index_descriptionVector, untrained_index_descriptionVector]:
            if "untrained" in idx.index_name:
                defer_build = True
            else:
                defer_build = False
            query = idx.generate_index_create_query(namespace=collection_namespace, defer_build=defer_build)
            self.run_cbq_query(query=query)
        for query in [trained_index_colorRGBVector, trained_index_descriptionVector]:
            select_query_with_explain = f"EXPLAIN {self.gsi_util_obj.get_select_queries(definition_list=[query],namespace=collection_namespace, limit=self.scan_limit)[0]}"
            index_used_select_query = self.run_cbq_query(query=select_query_with_explain)['results'][0]['plan']['~children'][0]['~children'][0]['index']
            self.assertEqual(index_used_select_query, query.index_name, 'trained index not used for scans')

    def test_compare_results_between_partitioned_and_non_partitioned_indexes(self):
        self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)

        collection_namespace = self.namespaces[0]

        color_vec_2 = [90.0, 33.0, 18.0]
        scan_color_vec_2 = f"ANN(colorRGBVector, {color_vec_2}, '{self.similarity}', {self.scan_nprobes})"

        desc_2 = "A BMW or Mercedes car with high safety rating and fuel efficiency"
        descVec2 = list(self.encoder.encode(desc_2))

        scan_desc_vec_2 = f"ANN(descriptionVector, {descVec2}, '{self.similarity}', {self.scan_nprobes})"


        partitioned_index_colorRGBVector = QueryDefinition("partitioned_colorRGBVector",
                                            index_fields=['rating', 'colorRGBVector Vector', 'category'],
                                            dimension=3, description=f"IVF,{self.quantization_algo_color_vector}",
                                            similarity=self.similarity, scan_nprobes=self.scan_nprobes,
                                            limit=self.scan_limit,
                                            query_use_index_template=RANGE_SCAN_USE_INDEX_ORDER_BY_TEMPLATE.format(
                                                "color, colorRGBVector",
                                                "rating = 2 and "
                                                "category in ['Convertible', "
                                                "'Luxury Car', 'Supercar']",
                                                scan_color_vec_2),
                                            partition_by_fields=['meta().id']
                                            )

        non_partitioned_index_colorRGBVector = QueryDefinition("non_partitioned_colorRGBVector",
                                                index_fields=['rating', 'colorRGBVector Vector', 'category'],
                                                dimension=3,
                                                description=f"IVF,{self.quantization_algo_color_vector}",
                                                similarity=self.similarity, scan_nprobes=self.scan_nprobes,
                                                limit=self.scan_limit,
                                                query_use_index_template=RANGE_SCAN_USE_INDEX_ORDER_BY_TEMPLATE.format(
                                                    "color, colorRGBVector",
                                                    "rating = 2 and "
                                                    "category in ['Convertible', "
                                                    "'Luxury Car', 'Supercar']",
                                                    scan_color_vec_2)
                                                )


        message = f"quantization value is {self.quantization_algo_description_vector}"
        partitioned_index_descriptionVector = QueryDefinition("partitioned_descriptionVector",
                                            index_fields=['rating', 'descriptionVector Vector', 'category'],
                                            dimension=384, description=f"IVF,{self.quantization_algo_description_vector}",
                                            similarity=self.similarity, scan_nprobes=self.scan_nprobes,
                                            limit=self.scan_limit,
                                            query_use_index_template=RANGE_SCAN_USE_INDEX_ORDER_BY_TEMPLATE.format(
                                                "description, descriptionVector",
                                                "rating = 2 and "
                                                "category in ['Convertible', "
                                                "'Luxury Car', 'Supercar']",
                                                scan_desc_vec_2),
                                            partition_by_fields=['meta().id']
                                            )

        non_partitioned_index_descriptionVector = QueryDefinition("non_partitioned_descriptionVector",
                                                index_fields=['rating', 'descriptionVector Vector', 'category'],
                                                dimension=384,
                                                description=f"IVF,{self.quantization_algo_description_vector}",
                                                similarity=self.similarity, scan_nprobes=self.scan_nprobes,
                                                limit=self.scan_limit,
                                                query_use_index_template=RANGE_SCAN_USE_INDEX_ORDER_BY_TEMPLATE.format(
                                                    "description, descriptionVector",
                                                    "rating = 2 and "
                                                    "category in ['Convertible', "
                                                    "'Luxury Car', 'Supercar']",
                                                    scan_desc_vec_2)
                                                )

        select_queries = []
        for idx in [partitioned_index_colorRGBVector, non_partitioned_index_colorRGBVector, partitioned_index_descriptionVector, non_partitioned_index_descriptionVector]:
            create_query = idx.generate_index_create_query(namespace=collection_namespace)
            self.run_cbq_query(query=create_query)
            select_query = self.gsi_util_obj.get_select_queries(definition_list=[idx],
                                                                   namespace=collection_namespace,
                                                                   index_name=idx.index_name)[0]
            select_queries.append(select_query)

        self.display_recall_and_accuracy_stats(select_queries=select_queries, message=message)

    def gen_table_view(self, query_stats_map, message="query stats"):
        table = TableView(self.log.info)
        table.set_headers(['Query', 'Recall', 'Accuracy'])
        for query in query_stats_map:
            table.add_row([query, query_stats_map[query][0], query_stats_map[query][1]])
        table.display(message=message)

    def display_recall_and_accuracy_stats(self, select_queries,message="query stats", stats_assertion=True):
        query_stats_map = {}
        for query in select_queries:
            # this is to ensure that select queries run primary indexes are not tested for recall and accuracy
            if "ANN" not in query:
                continue
            redacted_query, recall, accuracy = self.validate_scans_for_recall_and_accuracy(select_query=query)
            query_stats_map[redacted_query] = [recall, accuracy]
        self.gen_table_view(query_stats_map=query_stats_map, message=message)
        if stats_assertion:
            for query in query_stats_map:
                self.assertGreaterEqual(query_stats_map[query][0] * 100, 70,
                                        f"recall for query {query} is less than threshold 70")
                self.assertGreaterEqual(query_stats_map[query][1] * 100, 70,
                                        f"accuracy for query {query} is less than threshold 70")



