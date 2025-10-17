"""
gsi_rebalance_encryption.py: These test validate altering of node security settings during rebalance.
https://issues.couchbase.com/browse/MB-57757

__author__ = "Yash Dodderi"
__maintainer = "Yash Dodderi"
__email__ = "yash.dodderi@couchbase.com"
__git_user__ = "yash-dodderi7"
__created_on__ = 09/10/23 4:24 pm

"""

from membase.api.capella_rest_client import RestConnection as RestConnectionCapella
from membase.api.rest_client import RestConnection
from .base_gsi import BaseSecondaryIndexingTests
from couchbase_helper.cluster import Cluster
from couchbase_helper.documentgenerator import SDKDataLoader
import random
import string
from concurrent.futures import ThreadPoolExecutor
from remote.remote_util import RemoteMachineShellConnection
from threading import Thread
from random import randrange
from membase.api.rest_client import RestConnection, RestHelper


class RebalanceEncryption(BaseSecondaryIndexingTests):
    def setUp(self):
        super(RebalanceEncryption, self).setUp()
        self.rest = RestConnection(self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)[0])
        self.query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)[0]
        self.log.info("==============  RebalanceEncryption setup has started ==============")
        if self.capella_run:
            buckets = self.rest.get_buckets()
            if buckets:
                for bucket in buckets:
                    RestConnectionCapella.delete_bucket(self, bucket=bucket.name)

        else:
            self.rest.delete_all_buckets()
        self.password = self.input.membase_settings.rest_password
        self.buckets = self.rest.get_buckets()
        if not self.capella_run:
            self._create_server_groups()
            self.cb_version = float(self.cb_version.split('-')[0][0:3])
            self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                            replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                            enable_replica_index=self.enable_replica_index,
                                                            eviction_policy=self.eviction_policy, lww=self.lww)
            self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                                bucket_params=self.bucket_params)
        self.run_continous_query = False
        self.num_index_batches = self.input.param("num_index_batches", 1)
        self.state = self.input.param("state", "disable")
        self.encryption_setting = self.input.param("encryption_setting", "control")
        self.log.info("==============  RebalanceEncryption setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  RebalanceEncryption tearDown has started ==============")
        self.log.info("==============  RebalanceEncryption tearDown has completed ==============")

    def suite_tearDown(self):
        self.log.info("==============  RebalanceEncryption tearDown has started ==============")
        super(RebalanceEncryption, self).tearDown()
        self.log.info("==============  RebalanceEncryption tearDown has completed ==============")

    def suite_setUp(self):
        self.log.info("==============  RebalanceEncryption suite_setup has started ==============")
        self.log.info("==============  RebalanceEncryption suite_setup has completed ==============")

    def test_refresh_certs_during_rebalance(self):
        indexer_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        rest = RestConnection(indexer_node)
        rest.set_index_settings({"indexer.settings.enable_shard_affinity": True})
        self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                             num_of_docs_per_collection=self.num_of_docs_per_collection,
                                             json_template='Hotel')


        for namespace in self.namespaces:
            for index in range(self.num_index_batches):
                prefix = f"idx_{index}"
                query_definitions = self.gsi_util_obj.generate_hotel_data_index_definition(index_name_prefix=prefix)
                create_queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                                         namespace=namespace,
                                                                         num_replica=self.num_index_replicas)
                self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, database=namespace)
        self.wait_until_indexes_online()
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        services_in = ['index']
        self.log.info(f'nodes out list : {self.nodes_out_list}')
        
        for idx, index_node in enumerate(index_nodes):
            out_node = index_node
            self.log.info(f'node being balanced out {out_node}')
            in_node = self.servers[self.nodes_init + idx]

            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [in_node],
                                                     [out_node], services=services_in)
            while self.rest._rebalance_progress_status() != 'running':
                continue
            if RestHelper(self.rest).rebalance_reached(percentage=10):
                node_rest = RestConnection(self.master)
                status, content = node_rest.refresh_certificate()
                if not status:
                    self.fail(content)


            rebalance.result()

    def test_refresh_credentials_during_rebalance(self):
        indexer_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        rest = RestConnection(indexer_node)
        rest.set_index_settings({"indexer.settings.enable_shard_affinity": True})
        self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                             num_of_docs_per_collection=self.num_of_docs_per_collection,
                                             json_template='Hotel')


        for namespace in self.namespaces:
            for index in range(self.num_index_batches):
                prefix = f"idx_{index}"
                query_definitions = self.gsi_util_obj.generate_hotel_data_index_definition(index_name_prefix=prefix)
                create_queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                                         namespace=namespace,
                                                                         num_replica=self.num_index_replicas)
                self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, database=namespace)
        self.wait_until_indexes_online()
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        services_in = ['index']
        self.log.info(f'nodes out list : {self.nodes_out_list}')
        
        for idx, index_node in enumerate(index_nodes):
            out_node = index_node
            self.log.info(f'node being balanced out {out_node}')
            in_node = self.servers[self.nodes_init + idx]

            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [in_node],
                                                     [out_node], services=services_in)
            while self.rest._rebalance_progress_status() != 'running':
                continue
            if RestHelper(self.rest).rebalance_reached(percentage=10):
                node_rest = RestConnection(self.master)
                status, content = node_rest.refresh_credentials()
                if not status:
                    self.fail(content)


            rebalance.result()

    def test_refresh_connections_during_query_workload(self):
        '''
        This test is for MB-65153
        Steps -
        Run a continous query workload and during this workload refresh the certs
        The queries shouldn't fail and the uptime stat should not be zero
        '''
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        select_queries = []
        self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                             num_of_docs_per_collection=self.num_of_docs_per_collection,
                                             json_template='Hotel')
        for namespace in self.namespaces:
            definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template)
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions, namespace=namespace,
                                                                     num_replica=1)
            self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, query_node=query_node)

            select_queries.extend(
                self.gsi_util_obj.get_select_queries(definition_list=definitions, namespace=namespace))
        self.wait_until_indexes_online(timeout=600)
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
        # Assertions for whether queries have failed or not
        self.assertEqual(len(self.gsi_util_obj.query_errors), 0,
                         f'There are query failures {self.gsi_util_obj.query_errors}')
        for node in index_nodes:
            rest = RestConnection(node)
            stats = rest.get_all_index_stats()
            self.log.info(f"stats for uptime are {stats['uptime']}")
            self.assertIsNot(stats['uptime'], '0', f"stats for uptime are {stats['uptime']}")


    def test_change_security_settings_during_rebalance(self):
        indexer_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        rest = RestConnection(indexer_node)
        rest.set_index_settings({"indexer.settings.enable_shard_affinity": True})
        self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                             num_of_docs_per_collection=self.num_of_docs_per_collection,
                                             json_template='Hotel')


        for namespace in self.namespaces:
            for index in range(self.num_index_batches):
                prefix = f"idx_{index}"
                query_definitions = self.gsi_util_obj.generate_hotel_data_index_definition(index_name_prefix=prefix)
                create_queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                                         namespace=namespace,
                                                                         num_replica=self.num_index_replicas)
                self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, database=namespace)
        self.wait_until_indexes_online()
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        services_in = ['index']
        self.log.info(f'nodes out list : {self.nodes_out_list}')
        
        for idx, index_node in enumerate(index_nodes):
            out_node = index_node
            self.log.info(f'node being balanced out {out_node}')
            in_node = self.servers[self.nodes_init + idx]

            try:
                rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [in_node],
                                                         [out_node], services=services_in)
                while self.rest._rebalance_progress_status() != 'running':
                    continue
                if RestHelper(self.rest).rebalance_reached(percentage=10):
                    self.update_master_node()
                    node_rest = RestConnection(self.master)
                    status, content = node_rest.set_min_tls_version(version='tlsv1.3')
                    if not status:
                        self.fail(content)
            except Exception as e:
                if "TLSV1_ALERT_PROTOCOL_VERSION" in str(e):
                    self.log.info(str(e))
                    self.cluster = Cluster()
                    reconnected_node = self.get_nodes_from_services_map(get_all_nodes=False)
                    self.rest = RestConnection(self.get_nodes_from_services_map(service_type="kv", get_all_nodes=False))
                    while self.rest._rebalance_progress_status() != 'none':
                        continue
                    self.sleep(60)
                    self.log.info("Rebalance finished...")
                    self.assertTrue(RestHelper(reconnected_node).rebalance_reached(percentage=100),
                                    "rebalance failed, stuck or did not complete")
                else:
                    self.fail(str(e))


            #rebalance.result()


    def test_change_cert_settings_during_rebalance(self):
        indexer_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        rest = RestConnection(indexer_node)
        rest.set_index_settings({"indexer.settings.enable_shard_affinity": True})
        self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                             num_of_docs_per_collection=self.num_of_docs_per_collection,
                                             json_template='Hotel')


        for namespace in self.namespaces:
            for index in range(self.num_index_batches):
                prefix = f"idx_{index}"
                query_definitions = self.gsi_util_obj.generate_hotel_data_index_definition(index_name_prefix=prefix)
                create_queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                                         namespace=namespace,
                                                                         num_replica=self.num_index_replicas)
                self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, database=namespace)
        self.wait_until_indexes_online()
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        services_in = ['index']
        self.log.info(f'nodes out list : {self.nodes_out_list}')
        
        for idx, index_node in enumerate(index_nodes):
            out_node = index_node
            self.log.info(f'node being balanced out {out_node}')
            in_node = self.servers[self.nodes_init + idx]

            try:
                rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [in_node],
                                                         [out_node], services=services_in)
                while self.rest._rebalance_progress_status() != 'running':
                    continue
                if RestHelper(self.rest).rebalance_reached(percentage=10):
                    self.update_master_node()
                    node_rest = RestConnection(self.master)
                    if self.state == 'disable':
                        status, content = node_rest.client_cert_auth(state="enable", prefixes=[
                            {"delimiter": "", "prefix": "", "path": "subject.cn"}])
                        if not status:
                            self.fail(content)
                        self.sleep(2)
                        status, content = node_rest.client_cert_auth(state=self.state, prefixes=[
                            {"delimiter": "", "prefix": "", "path": "subject.cn"}])
                        if not status:
                            self.fail(content)
                    else:
                        status, content = node_rest.client_cert_auth(state=self.state, prefixes=[
                            {"delimiter": "", "prefix": "", "path": "subject.cn"}])
                        if not status:
                            self.fail(content)

            except Exception as e:
                ex = str(e)
                print(f"exception caught is {ex}")
                if "sslv3 alert handshake failure" in ex:
                    self.log.info(str(e))
                    reconnected_node = self.get_nodes_from_services_map(get_all_nodes=False)
                    self.rest = RestConnection(self.get_nodes_from_services_map(service_type="kv", get_all_nodes=False))
                    while self.rest._rebalance_progress_status() != 'none':
                        continue
                    self.log.info('rebalance done')
                    self.sleep(60)
                    self.assertTrue(RestHelper(reconnected_node).rebalance_reached(percentage=100),
                                    "rebalance failed, stuck or did not complete")
                    self.cluster = Cluster()
                else:
                    self.fail(str(e))
            else:
                while self.rest._rebalance_progress_status() != 'none':
                    continue
                self.log.info('rebalance done')

    def test_change_encryption_settings_during_rebalance(self):
        indexer_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        rest = RestConnection(indexer_node)
        rest.set_index_settings({"indexer.settings.enable_shard_affinity": True})
        self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                             num_of_docs_per_collection=self.num_of_docs_per_collection,
                                             json_template='Hotel')


        for namespace in self.namespaces:
            for index in range(self.num_index_batches):
                prefix = f"idx_{index}"
                query_definitions = self.gsi_util_obj.generate_hotel_data_index_definition(index_name_prefix=prefix)
                create_queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                                         namespace=namespace,
                                                                         num_replica=self.num_index_replicas)
                self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, database=namespace)
        self.wait_until_indexes_online()
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        services_in = ['index']
        self.log.info(f'nodes out list : {self.nodes_out_list}')
        node_rest = RestConnection(self.master)
        node_rest.update_autofailover_settings(enabled=False, timeout=120)
        if self.enforce_tls:
            cmd = f'/opt/couchbase/bin/couchbase-cli node-to-node-encryption \
                                                    -c https://{self.master.ip}:18091 \
                                                    -u Administrator \
                                                    -p password \
                                                    --enable \
                                                    --no-ssl-verify'
            conn = RemoteMachineShellConnection(self.master)
            output, error = conn.execute_command(cmd)
            self.log.info(f'output {output}, error {error}')
            if 'SUCCESS' not in output[3]:
                self.fail('node to node encryption not turned on')
        else:
            cmd = f'/opt/couchbase/bin/couchbase-cli node-to-node-encryption \
                                                                -c http://{self.master.ip}:8091 \
                                                                -u Administrator \
                                                                -p password \
                                                                --enable'
            conn = RemoteMachineShellConnection(self.master)
            output, error = conn.execute_command(cmd)
            self.log.info(f'output {output}, error {error}')
            if 'SUCCESS' not in output[2]:
                self.fail('node to node encryption not turned on')
        
        for idx, index_node in enumerate(index_nodes):
            out_node = index_node
            self.log.info(f'node being balanced out {out_node}')
            in_node = self.servers[self.nodes_init + idx]

            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [in_node],
                                                     [out_node], services=services_in)
            while self.rest._rebalance_progress_status() != 'running':
                continue
            if RestHelper(self.rest).rebalance_reached(percentage=10):
                status, content = node_rest.set_node_encryption_level(encryption_level=self.encryption_setting)
                if not status:
                    self.fail(content)
            rebalance.result()
