"""ephemeral_gsi.py: description
o
__author__ = "Hemant Rajput"
__maintainer = "Hemant Rajput"
__email__ = "Hemant.Rajput@couchbase.com"
__git_user__ = "hrajput89"
__created_on__ = "15/07/21 1:43 pm" 

"""

from concurrent.futures import ThreadPoolExecutor

from couchbase_helper.documentgenerator import SDKDataLoader
from couchbase_helper.query_definitions import QueryDefinition
from couchbase_helper.stats_tools import StatsCommon
from remote.remote_util import RemoteMachineShellConnection

from .base_gsi import BaseSecondaryIndexingTests


class EphemeralGSI(BaseSecondaryIndexingTests):
    def setUp(self):
        super(EphemeralGSI, self).setUp()
        self.log.info("==============  EphemeralGSI setup has started ==============")
        self.process_to_kill = self.input.param('process_to_kill', None)
        self.partition_fields = self.input.param('partition_fields', None)
        self.num_partition = self.input.param('num_partition', 8)
        self.rest.delete_all_buckets()
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type='ephemeral',
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        self.log.info("==============  EphemeralGSI setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  EphemeralGSI tearDown has started ==============")
        super(EphemeralGSI, self).tearDown()
        self.log.info("==============  EphemeralGSI tearDown has completed ==============")

    def test_gsi_on_ephemeral_bucket(self):
        self.prepare_collection_for_indexing(num_of_docs_per_collection=self.num_of_docs_per_collection)
        collection_namespace = self.namespaces[0]
        _, keyspace = collection_namespace.split(':')
        bucket, scope, collection = keyspace.split('.')
        index_gen = QueryDefinition(index_name='idx', index_fields=['age', 'country', 'city'])
        meta_index_gen = QueryDefinition(index_name='meta_idx', index_fields=['meta().id'])

        bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                   replicas=self.num_replicas, bucket_type='membase',
                                                   enable_replica_index=self.enable_replica_index,
                                                   eviction_policy='valueOnly', lww=self.lww)
        self.cluster.create_standard_bucket(name='default', port=11222,
                                            bucket_params=bucket_params)

        query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
        self.run_cbq_query(query)
        if self.defer_build:
            build_query = index_gen.generate_build_query(namespace=collection_namespace)
            self.run_cbq_query(build_query)
        self.wait_until_indexes_online()
        query = meta_index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
        self.run_cbq_query(query)
        if self.defer_build:
            build_query = meta_index_gen.generate_build_query(namespace=collection_namespace)
            self.run_cbq_query(build_query)
        self.wait_until_indexes_online()

        index_storage_mode = self.index_rest.get_index_storage_mode()
        self.assertEqual(index_storage_mode, self.gsi_type)

        select_query = f'Select * from {collection_namespace} where age >10 and country like "A%";'
        select_meta_id_query = f'Select * from {collection} where meta().id like "doc_%";'
        count_query = f'Select count(*) from {collection_namespace} where age >= 0;'
        named_collection_query_context = f'default:{bucket}.{scope}'

        with ThreadPoolExecutor() as executor:
            select_task = executor.submit(self.run_cbq_query, query=select_query)
            meta_task = executor.submit(self.run_cbq_query, query=select_meta_id_query,
                                        query_context=named_collection_query_context)
            count_task = executor.submit(self.run_cbq_query, query=count_query)
            result = select_task.result()['results']
            meta_id_result = meta_task.result()['results']
            count_result = count_task.result()['results'][0]['$1']

        self.assertTrue(len(result) > 0)
        self.assertEqual(len(meta_id_result), self.num_of_docs_per_collection)
        self.assertEqual(count_result, self.num_of_docs_per_collection)

    def test_gsi_on_ephemeral_with_partial_KV_node(self):
        kv_nodes = self.get_kv_nodes()
        index_node = self.get_nodes_from_services_map(service_type="index")
        if len(kv_nodes) < 2:
            self.fail("This test requires at least 2 KV node")

        self.prepare_collection_for_indexing(num_of_docs_per_collection=self.num_of_docs_per_collection)
        collection_namespace = self.namespaces[0]
        _, keyspace = collection_namespace.split(':')
        bucket, scope, collection = keyspace.split('.')
        index_gen = QueryDefinition(index_name='idx', index_fields=['age', 'country', 'city'])
        meta_index_gen = QueryDefinition(index_name='meta_idx', index_fields=['meta().id'])

        query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
        self.run_cbq_query(query)
        if self.defer_build:
            build_query = index_gen.generate_build_query(namespace=collection_namespace)
            self.run_cbq_query(build_query)
        self.wait_until_indexes_online()
        query = meta_index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
        self.run_cbq_query(query)
        if self.defer_build:
            build_query = meta_index_gen.generate_build_query(namespace=collection_namespace)
            self.run_cbq_query(build_query)
        self.wait_until_indexes_online()

        select_query = f'Select * from {collection_namespace} where age >10 and country like "A%";'
        select_meta_id_query = f'Select * from {collection} where meta().id like "doc_%";'
        count_query = f'Select count(*) from {collection_namespace} where age >= 0;'
        named_collection_query_context = f'default:{bucket}.{scope}'

        select_result = self.run_cbq_query(query=select_query)['results']
        meta_result = self.run_cbq_query(query=select_meta_id_query,
                                         query_context=named_collection_query_context)['results']
        count_result = self.run_cbq_query(query=count_query)['results'][0]['$1']
        self.assertTrue(len(select_result) > 0)
        self.assertEqual(len(meta_result), self.num_of_docs_per_collection)
        self.assertEqual(count_result, self.num_of_docs_per_collection)

        # Blocking communication between one KV node and index to check indexes catching with mutation from other KV
        kv_node_a, kv_node_b = kv_nodes
        try:
            self.block_incoming_network_from_node(kv_node_b, index_node)
            self.sleep(10)

            new_insert_docs_num = 10 ** 3
            gen_create = SDKDataLoader(num_ops=new_insert_docs_num, percent_create=100, json_template="Person",
                                       percent_update=0, percent_delete=0, scope=scope,
                                       collection=collection, output=True,
                                       start_seq_num=self.num_of_docs_per_collection + 1)
            tasks = self.data_ops_javasdk_loader_in_batches(sdk_data_loader=gen_create, batch_size=10000)
            self.sleep(20, "Giving some time for data insertion")
            select_result_after_kv_block = self.run_cbq_query(query=select_query)['results']
            meta_result_after_kv_block = self.run_cbq_query(query=select_meta_id_query,
                                                            query_context=named_collection_query_context)['results']
            count_result_after_kv_block = self.run_cbq_query(query=count_query)['results'][0]['$1']
            self.assertTrue(len(select_result_after_kv_block) > len(select_result),
                            "Query result not matching expected value")
            self.assertTrue(len(meta_result_after_kv_block) > len(meta_result),
                            "Query result not matching expected value")
            self.assertTrue(count_result_after_kv_block > count_result, "Query result not matching expected value")

            for task in tasks:
                out = task.result()
                self.log.info(out)
        except Exception as err:
            self.fail(err)
        finally:
            self.resume_blocked_incoming_network_from_node(kv_node_b, index_node)
            self.sleep(10)

        # Checking if indexer continue to process insertion after KV node failover
        failover_task = self.cluster.async_failover(self.servers[:self.nodes_init], failover_nodes=[kv_node_b],
                                                    graceful=self.graceful)
        failover_task.result()

        gen_create = SDKDataLoader(num_ops=new_insert_docs_num, percent_create=0, json_template="Person",
                                   percent_update=0, percent_delete=100, scope=scope,
                                   collection=collection, output=True,
                                   start_seq_num=self.num_of_docs_per_collection + 1)
        tasks = self.data_ops_javasdk_loader_in_batches(sdk_data_loader=gen_create, batch_size=10000)
        for task in tasks:
            out = task.result()
            self.log.info(out)

        select_result_after_failover = self.run_cbq_query(query=select_query)['results']
        meta_result_after_failover = self.run_cbq_query(query=select_meta_id_query,
                                                        query_context=named_collection_query_context)['results']
        count_result_after_failover = self.run_cbq_query(query=count_query)['results'][0]['$1']
        self.assertEqual(len(select_result_after_failover), len(select_result),
                         "Query result not matching expected value")
        self.assertEqual(len(meta_result_after_failover), len(meta_result), "Query result not matching expected value")
        self.assertEqual(count_result_after_failover, count_result, "Query result not matching expected value")

    def test_gsi_on_ephemeral_with_server_restart(self):
        self.prepare_collection_for_indexing(num_of_docs_per_collection=self.num_of_docs_per_collection)
        collection_namespace = self.namespaces[0]
        _, keyspace = collection_namespace.split(':')
        bucket, scope, collection = keyspace.split('.')
        index_gen = QueryDefinition(index_name='idx', index_fields=['age', 'country', 'city'])
        meta_index_gen = QueryDefinition(index_name='meta_idx', index_fields=['meta().id'])

        query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
        self.run_cbq_query(query)
        if self.defer_build:
            build_query = index_gen.generate_build_query(namespace=collection_namespace)
            self.run_cbq_query(build_query)
        self.wait_until_indexes_online()
        query = meta_index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
        self.run_cbq_query(query)
        if self.defer_build:
            build_query = meta_index_gen.generate_build_query(namespace=collection_namespace)
            self.run_cbq_query(build_query)
        self.wait_until_indexes_online()

        select_query = f'Select * from {collection_namespace} where age >10 and country like "A%";'
        select_meta_id_query = f'Select * from {collection} where meta().id like "doc_%";'
        count_query = f'Select count(*) from {collection_namespace} where age >= 0;'
        named_collection_query_context = f'default:{bucket}.{scope}'

        select_result = self.run_cbq_query(query=select_query)['results']
        meta_result = self.run_cbq_query(query=select_meta_id_query,
                                         query_context=named_collection_query_context)['results']
        count_result = self.run_cbq_query(query=count_query)['results'][0]['$1']
        self.assertTrue(len(select_result) > 0)
        self.assertEqual(len(meta_result), self.num_of_docs_per_collection)
        self.assertEqual(count_result, self.num_of_docs_per_collection)

        # restarting couchbase services
        shell = RemoteMachineShellConnection(self.master)
        shell.restart_couchbase()
        shell.disconnect()

        self.sleep(30, "Waiting for server to be up again after restart")
        select_result = self.run_cbq_query(query=select_query)['results']
        meta_result = self.run_cbq_query(query=select_meta_id_query,
                                         query_context=named_collection_query_context)['results']
        count_result = self.run_cbq_query(query=count_query)['results'][0]['$1']
        self.assertEqual(len(select_result), 0)
        self.assertEqual(len(meta_result), 0)
        self.assertEqual(count_result, 0)

    def test_gsi_on_ephemeral_with_bucket_flush(self):
        self.prepare_collection_for_indexing(num_of_docs_per_collection=self.num_of_docs_per_collection)
        collection_namespace = self.namespaces[0]
        _, keyspace = collection_namespace.split(':')
        bucket, scope, collection = keyspace.split('.')
        index_gen = QueryDefinition(index_name='idx', index_fields=['age', 'country', 'city'])
        meta_index_gen = QueryDefinition(index_name='meta_idx', index_fields=['meta().id'])

        query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
        self.run_cbq_query(query)
        if self.defer_build:
            build_query = index_gen.generate_build_query(namespace=collection_namespace)
            self.run_cbq_query(build_query)
        self.wait_until_indexes_online()
        query = meta_index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
        self.run_cbq_query(query)
        if self.defer_build:
            build_query = meta_index_gen.generate_build_query(namespace=collection_namespace)
            self.run_cbq_query(build_query)
        self.wait_until_indexes_online()

        select_query = f'Select * from {collection_namespace} where age >10 and country like "A%";'
        select_meta_id_query = f'Select * from {collection} where meta().id like "doc_%";'
        count_query = f'Select count(*) from {collection_namespace} where age >= 0;'
        named_collection_query_context = f'default:{bucket}.{scope}'

        select_result = self.run_cbq_query(query=select_query)['results']
        meta_result = self.run_cbq_query(query=select_meta_id_query,
                                         query_context=named_collection_query_context)['results']
        count_result = self.run_cbq_query(query=count_query)['results'][0]['$1']
        self.assertTrue(len(select_result) > 0)
        self.assertEqual(len(meta_result), self.num_of_docs_per_collection)
        self.assertEqual(count_result, self.num_of_docs_per_collection)

        # flushing the bucket
        task = self.cluster.async_bucket_flush(server=self.master, bucket=self.test_bucket)
        result = task.result()
        self.log.info(result)

        self.sleep(30, "Giving some time to indexer to update doc counts")
        select_result = self.run_cbq_query(query=select_query)['results']
        meta_result = self.run_cbq_query(query=select_meta_id_query,
                                         query_context=named_collection_query_context)['results']
        count_result = self.run_cbq_query(query=count_query)['results'][0]['$1']
        self.assertEqual(len(select_result), 0)
        self.assertEqual(len(meta_result), 0)
        self.assertEqual(count_result, 0)

        new_inserts = 1000
        gen_create = SDKDataLoader(num_ops=new_inserts, percent_create=100, json_template="Person",
                                   percent_update=0, percent_delete=0, scope=scope,
                                   collection=collection, output=True)
        tasks = self.data_ops_javasdk_loader_in_batches(sdk_data_loader=gen_create, batch_size=1000)
        for task in tasks:
            out = task.result()
            self.log.info(out)

        self.sleep(30)
        select_result = self.run_cbq_query(query=select_query)['results']
        meta_result = self.run_cbq_query(query=select_meta_id_query,
                                         query_context=named_collection_query_context)['results']
        count_result = self.run_cbq_query(query=count_query)['results'][0]['$1']
        self.assertTrue(len(select_result) > 0)
        self.assertEqual(len(meta_result), new_inserts)
        self.assertEqual(count_result, new_inserts)

    def test_gsi_on_ephemeral_with_process_kill(self):
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if len(index_nodes) < 2:
            self.fail("This test requires at least 2 index node")
        self.prepare_collection_for_indexing(num_of_docs_per_collection=self.num_of_docs_per_collection)
        collection_namespace = self.namespaces[0]
        _, keyspace = collection_namespace.split(':')
        bucket, scope, collection = keyspace.split('.')
        index_gen = QueryDefinition(index_name='idx', index_fields=['age', 'country', 'city'])
        meta_index_gen = QueryDefinition(index_name='meta_idx', index_fields=['meta().id'])

        partition_fields = None
        if self.partition_fields:
            partition_fields = self.partition_fields.split(',')
        query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build,
                                                      partition_by_fields=partition_fields,
                                                      num_partition=self.num_partition,
                                                      num_replica=self.num_replicas)
        self.run_cbq_query(query)
        if self.defer_build:
            build_query = index_gen.generate_build_query(namespace=collection_namespace)
            self.run_cbq_query(build_query)
        self.wait_until_indexes_online()
        self.sleep(10)
        query = meta_index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build,
                                                           partition_by_fields=partition_fields,
                                                           num_partition=self.num_partition,
                                                           num_replica=self.num_replicas)
        self.run_cbq_query(query)
        if self.defer_build:
            build_query = meta_index_gen.generate_build_query(namespace=collection_namespace)
            self.run_cbq_query(build_query)
        self.wait_until_indexes_online()

        select_query = f'Select * from {collection_namespace} where age >10 and country like "A%";'
        select_meta_id_query = f'Select * from {collection} where meta().id like "doc_%";'
        count_query = f'Select count(*) from {collection_namespace} where age >= 0;'
        named_collection_query_context = f'default:{bucket}.{scope}'

        select_result = self.run_cbq_query(query=select_query)['results']
        meta_result = self.run_cbq_query(query=select_meta_id_query,
                                         query_context=named_collection_query_context)['results']
        count_result = self.run_cbq_query(query=count_query)['results'][0]['$1']
        self.assertTrue(len(select_result) > 0)
        self.assertEqual(len(meta_result), self.num_of_docs_per_collection)
        self.assertEqual(count_result, self.num_of_docs_per_collection)

        num_rollbacks_before_kill = self.index_rest.get_num_rollback_stat(bucket=bucket)
        num_rollbacks_to_zero_before_kill = self.index_rest.get_num_rollback_stat(bucket=bucket)
        # todo: killing memcached only on indexer nodes
        for server in self.servers:
            remote = RemoteMachineShellConnection(server)
            self.log.info(f"killing {self.process_to_kill}  on {server.ip}")
            remote.terminate_process(process_name=self.process_to_kill)
        self.sleep(30, f"Sleep after killing {self.process_to_kill}")

        select_result = self.run_cbq_query(query=select_query)['results']
        meta_result = self.run_cbq_query(query=select_meta_id_query,
                                         query_context=named_collection_query_context)['results']
        count_result = self.run_cbq_query(query=count_query)['results'][0]['$1']
        num_rollbacks_after_kill = self.index_rest.get_num_rollback_stat(bucket=bucket)
        num_rollbacks_to_zero_after_kill = self.index_rest.get_num_rollback_stat(bucket=bucket)

        if self.process_to_kill == "indexer":
            self.assertTrue(len(select_result) > 0)
            self.assertEqual(len(meta_result), self.num_of_docs_per_collection)
            self.assertEqual(count_result, self.num_of_docs_per_collection)
            # todo: add assertion about num_rollback stats
            # self.assertTrue(num_rollbacks_after_kill > num_rollbacks_before_kill)
        else:
            self.assertEqual(len(select_result), 0)
            self.assertEqual(len(meta_result), 0)
            self.assertEqual(count_result, 0)

    def test_gsi_on_ephemeral_with_eviction_policy(self):
        num_of_docs = self.num_of_docs_per_collection
        self.prepare_collection_for_indexing(num_of_docs_per_collection=self.num_of_docs_per_collection)
        collection_namespace = self.namespaces[0]
        _, keyspace = collection_namespace.split(':')
        bucket, scope, collection = keyspace.split('.')
        index_gen = QueryDefinition(index_name='idx', index_fields=['age', 'country', 'city'])
        meta_index_gen = QueryDefinition(index_name='meta_idx', index_fields=['meta().id'])

        query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
        self.run_cbq_query(query)
        if self.defer_build:
            build_query = index_gen.generate_build_query(namespace=collection_namespace)
            self.run_cbq_query(build_query)
        self.wait_until_indexes_online()
        query = meta_index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=self.defer_build)
        self.run_cbq_query(query)
        if self.defer_build:
            build_query = meta_index_gen.generate_build_query(namespace=collection_namespace)
            self.run_cbq_query(build_query)
        self.wait_until_indexes_online()

        select_query = f'Select * from {collection_namespace} where age >10 and country like "A%";'
        select_meta_id_query = f'Select meta().id from {collection} where meta().id like "doc_%";'
        count_query = f'Select count(*) from {collection_namespace} where age >= 0;'
        named_collection_query_context = f'default:{bucket}.{scope}'

        select_result = self.run_cbq_query(query=select_query)['results']
        meta_result = self.run_cbq_query(query=select_meta_id_query,
                                         query_context=named_collection_query_context)['results']
        count_result = self.run_cbq_query(query=count_query)['results'][0]['$1']
        self.assertTrue(len(select_result) > 0)
        self.assertEqual(len(meta_result), self.num_of_docs_per_collection)
        self.assertEqual(count_result, self.num_of_docs_per_collection)

        new_inserts = 10 ** 4
        is_memory_full = False
        stats_all_buckets = {}
        for bucket in self.buckets:
            stats_all_buckets[bucket.name] = StatsCommon()

        threshold = 0.93
        last_memory_used_val = 0
        while not is_memory_full:
            gen_create = SDKDataLoader(num_ops=new_inserts, percent_create=100,
                                       percent_update=0, percent_delete=0, scope=scope,
                                       collection=collection, output=True, start_seq_num=num_of_docs+1)
            task = self.cluster.async_load_gen_docs(self.master, bucket, gen_create)
            task.result()
            # Updating the doc counts
            num_of_docs = num_of_docs + new_inserts
            self.sleep(30)
            memory_used = int(stats_all_buckets[bucket.name].get_stats([self.master], bucket, '',
                                                                       'mem_used')[self.master])
            self.log.info(f"Current memory usage: {memory_used}")
            if self.eviction_policy == 'noEviction':
                # memory is considered full if mem_used is at say 90% of the available memory
                if memory_used > threshold * self.bucket_size * 1000000:
                    # Just filling the leftover memory to be double sure
                    gen_create = SDKDataLoader(num_ops=new_inserts, percent_create=100,
                                               percent_update=0, percent_delete=0, scope=scope,
                                               collection=collection, output=True, start_seq_num=num_of_docs + 1)
                    task = self.cluster.async_load_gen_docs(self.master, bucket, gen_create)
                    task.result()
                    num_of_docs = num_of_docs + new_inserts
                    memory_used = int(stats_all_buckets[bucket.name].get_stats([self.master], bucket, '',
                                                                               'mem_used')[self.master])
                    self.log.info(f"Current memory usage: {memory_used}")
                    is_memory_full = True
            else:
                if memory_used < last_memory_used_val:
                    break
                last_memory_used_val = memory_used

        meta_ids = self.run_cbq_query(query=select_meta_id_query,
                                      query_context=named_collection_query_context)['results']

        ids_at_threshold = sorted([item['id'] for item in meta_ids])

        # Pushing new docs to check the eviction policy
        new_inserts = 10 ** 4
        gen_create = SDKDataLoader(num_ops=new_inserts, percent_create=100, json_template="Employee",
                                   percent_update=0, percent_delete=0, scope=scope,
                                   collection=collection, output=True, start_seq_num=num_of_docs+1)
        tasks = self.data_ops_javasdk_loader_in_batches(sdk_data_loader=gen_create, batch_size=10000)
        for task in tasks:
            out = task.result()
            self.log.info(out)

        meta_ids_with_eviction_enforced = self.run_cbq_query(query=select_meta_id_query,
                                                             query_context=named_collection_query_context)['results']
        ids_after_threshold = sorted([item['id'] for item in meta_ids_with_eviction_enforced])

        if self.eviction_policy == 'noEviction':
            self.assertEqual(len(meta_ids_with_eviction_enforced), len(meta_ids))
            self.assertEqual(ids_at_threshold, ids_after_threshold)
        else:
            self.assertTrue(len(meta_ids_with_eviction_enforced) != len(meta_ids))
            self.assertTrue(ids_after_threshold != ids_at_threshold)
