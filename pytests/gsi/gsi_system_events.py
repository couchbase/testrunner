"""gsi_system_events.py: These tests validate System Events for GSI

__author__ = "Hemant Rajput"
__maintainer = "Hemant Rajput"
__email__ = "Hemant.Rajput@couchbase.com"
__git_user__ = "hrajput89"
__created_on__ = "11/24/21 11:40 am"

"""
import json
from concurrent.futures import ThreadPoolExecutor

from collection.gsi.backup_restore_utils import IndexBackupClient
from couchbase_helper.query_definitions import QueryDefinition
from gsi.collections_concurrent_indexes import powerset
from lib.SystemEventLogLib.gsi_events import IndexingServiceEvents
from lib.SystemEventLogLib.Events import EventHelper
from lib import global_vars
from membase.api.rest_client import RestHelper
from remote.remote_util import RemoteMachineShellConnection

from .base_gsi import BaseSecondaryIndexingTests


class GSISystemEvents(BaseSecondaryIndexingTests):
    def setUp(self):
        super(GSISystemEvents, self).setUp()
        self.log.info("==============  GSIFreeTier setup has started ==============")
        self.rest.delete_all_buckets()
        global_vars.system_event_logs = EventHelper()
        self.system_events = global_vars.system_event_logs
        self.system_events.set_test_start_time()
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        self.prepare_collection_for_indexing(num_of_docs_per_collection=self.num_of_docs_per_collection,
                                             num_scopes=self.num_scopes, num_collections=self.num_collections)
        self.index_field_set = powerset(
            ['age', 'city', 'country', 'title', 'firstName', 'lastName', 'streetAddress',
             'suffix', 'filler1', 'phone', 'zipcode'])
        self.initial_index_num = self.input.param('initial_index_num', 2)
        self.index_drop_flag = self.input.param('index_drop_flag', False)
        self.nodes_uuids = self._get_nodes_uuids()
        self.log.info("==============  GSIFreeTier setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  GSIFreeTier tearDown has started ==============")
        super(GSISystemEvents, self).tearDown()
        self.log.info("==============  GSIFreeTier tearDown has completed ==============")

    def suite_tearDown(self):
        pass

    def suite_setUp(self):
        pass

    def _get_nodes_uuids(self):
        api = f'http://{self.master.ip}:8091/pools/default'
        status, content, header = self.rest.urllib_request(api)
        if not status:
            raise Exception(content)
        results = json.loads(content)['nodes']
        node_ip_uuid_map = {}
        for node in results:
            ip = node['hostname'].split(':')[0]
            node_ip_uuid_map[ip] = node['nodeUUID']
        return node_ip_uuid_map

    def test_gsi_update_settings_system_events(self):
        # Updating Indexer Settings and adding to system event to validate
        old_setting = {"indexer.settings.rebalance.redistribute_indexes": False,
                       "indexer.cpu.throttle.target": 0.95,
                       "indexer.allowScheduleCreate": True}
        new_setting = {"indexer.settings.rebalance.redistribute_indexes": True,
                       "indexer.cpu.throttle.target": 1.0,
                       "indexer.allowScheduleCreate": False}
        self.index_rest.set_index_settings(new_setting)
        index_node = self.index_rest.ip
        self.system_events.add_event(IndexingServiceEvents.index_settings_updated(old_setting=old_setting,
                                                                                  new_setting=new_setting,
                                                                                  node=index_node))

        # Updating Projector Settings and adding to system event to validate
        old_setting = {"projector.cpuProfile": False,
                       "projector.memProfile": False,
                       "projector.settings.log_level": "info"}
        new_setting = {"projector.cpuProfile": True,
                       "projector.memProfile": True,
                       "projector.settings.log_level": "debug"}
        self.index_rest.set_index_settings(new_setting)
        index_node = self.index_rest.ip
        self.system_events.add_event(IndexingServiceEvents.projector_settings_updated(old_setting=old_setting,
                                                                                      new_setting=new_setting,
                                                                                      node=index_node))

        # Updating Query Client Settings and adding to system event to validate
        old_setting = {"queryport.client.waitForScheduledIndex": True,
                       "queryport.client.usePlanner": True,
                       "queryport.client.log_level": "info"}
        new_setting = {"queryport.client.waitForScheduledIndex": False,
                       "queryport.client.usePlanner": False,
                       "queryport.client.log_level": "debug"}
        self.index_rest.set_index_settings(new_setting)
        index_node = self.master.ip
        self.system_events.add_event(IndexingServiceEvents.query_client_settings_updated(old_setting=old_setting,
                                                                                         new_setting=new_setting,
                                                                                         node=index_node))
        self.system_events.validate(server=self.master)

    def test_gsi_ddl_system_events(self):
        index_gens = []
        indexes_list = []
        for collection_namespace in self.namespaces:
            for item, index_field in zip(range(self.initial_index_num), self.index_field_set):
                index_name = f"idx_{item}"
                if self.partitoned_index:
                    partition_fields = index_field
                    index_gen = QueryDefinition(index_name=index_name, index_fields=index_field,
                                                partition_by_fields=partition_fields)
                else:
                    index_gen = QueryDefinition(index_name=index_name, index_fields=index_field)
                query = index_gen.generate_index_create_query(namespace=collection_namespace,
                                                              defer_build=self.defer_build,
                                                              num_replica=self.num_index_replicas)
                self.run_cbq_query(query)
                index_gens.append(index_gen)
                indexes_list.append(index_name)
        self.wait_until_indexes_online()

        indexer_metadata = self.index_rest.get_indexer_metadata()['status']
        for index in indexer_metadata:
            instance_id = index['instId']
            definition_id = index['defnId']
            replica_id = index['replicaId']

            if index['partitioned']:
                for host in index['partitionMap']:
                    for partition_id in index['partitionMap'][host]:
                        node = host.split(':')[0]
                        indexer_id = self.nodes_uuids[node]
                        self.system_events.add_event(IndexingServiceEvents.index_created(node=node,
                                                                                         definition_id=definition_id,
                                                                                         instance_id=instance_id,
                                                                                         indexer_id=indexer_id,
                                                                                         replica_id=replica_id,
                                                                                         partition_id=partition_id))
                        if not self.defer_build:
                            self.system_events.add_event(
                                IndexingServiceEvents.index_building(node=node, definition_id=definition_id,
                                                                     instance_id=instance_id, indexer_id=indexer_id,
                                                                     replica_id=replica_id, partition_id=partition_id))
                            self.system_events.add_event(
                                IndexingServiceEvents.index_online(node=node, definition_id=definition_id,
                                                                   instance_id=instance_id, indexer_id=indexer_id,
                                                                   replica_id=replica_id, partition_id=partition_id))
            else:
                node = index['hosts'][0].split(':')[0]
                indexer_id = self.nodes_uuids[node]
                self.system_events.add_event(IndexingServiceEvents.index_created(node=node,
                                                                                 definition_id=definition_id,
                                                                                 instance_id=instance_id,
                                                                                 indexer_id=indexer_id,
                                                                                 replica_id=replica_id))
                if not self.defer_build:
                    self.system_events.add_event(
                        IndexingServiceEvents.index_building(node=node, definition_id=definition_id,
                                                             instance_id=instance_id, indexer_id=indexer_id,
                                                             replica_id=replica_id))
                    self.system_events.add_event(
                        IndexingServiceEvents.index_online(node=node, definition_id=definition_id,
                                                           instance_id=instance_id, indexer_id=indexer_id,
                                                           replica_id=replica_id))

                # checking for drop and adding events before running the action as Indexer Metadata will go away
                if self.index_drop_flag:
                    self.system_events.add_event(
                        IndexingServiceEvents.index_dropped(node=node, definition_id=definition_id,
                                                            instance_id=instance_id, indexer_id=indexer_id,
                                                            replica_id=replica_id))
        if self.index_drop_flag:
            for collection_namespace in self.namespaces:
                for index_gen, index_name in zip(index_gens, indexes_list):
                    query = index_gen.generate_index_drop_query(namespace=collection_namespace)
                    self.run_cbq_query(query)

        self.system_events.validate(self.master)

    def test_gsi_ddl_system_events_with_alter_index(self):
        index_name_list = []
        for collection_namespace in self.namespaces:
            for item, index_field in zip(range(self.initial_index_num), self.index_field_set):
                index_name = f'idx_{item}'
                index_name_list.append(index_name)
                index_gen = QueryDefinition(index_name=index_name, index_fields=index_field)
                query = index_gen.generate_index_create_query(namespace=collection_namespace,
                                                              defer_build=self.defer_build,
                                                              num_replica=0)
                self.run_cbq_query(query)
                self.wait_until_indexes_online()
                alter_query = f'Alter index {index_name} on {collection_namespace} WITH' \
                              f' {{"action": "replica_count", "num_replica": 1}}'
                self.run_cbq_query(alter_query)
                self.wait_until_indexes_online()

        indexer_metadata = self.index_rest.get_indexer_metadata()['status']
        for index in indexer_metadata:
            collection_namespace = f'default:{index["bucket"]}.{index["scope"]}.{index["collection"]}'
            index_name = index['name']
            instance_id = index['instId']
            definition_id = index['defnId']
            replica_id = index['replicaId']
            node = index['hosts'][0].split(':')[0]
            indexer_id = self.nodes_uuids[node]
            self.system_events.add_event(IndexingServiceEvents.index_created(node=node,
                                                                             definition_id=definition_id,
                                                                             instance_id=instance_id,
                                                                             indexer_id=indexer_id,
                                                                             replica_id=replica_id))

            self.system_events.add_event(IndexingServiceEvents.index_building(node=node, definition_id=definition_id,
                                                                              instance_id=instance_id,
                                                                              indexer_id=indexer_id,
                                                                              replica_id=replica_id))
            self.system_events.add_event(IndexingServiceEvents.index_online(node=node, definition_id=definition_id,
                                                                            instance_id=instance_id,
                                                                            indexer_id=indexer_id,
                                                                            replica_id=replica_id))
            # Alter Index to drop a replica
            if index_name in index_name_list:
                alter_query = f'Alter index {index_name} on {collection_namespace} WITH ' \
                              f'{{"action": "drop_replica", "replicaId": {replica_id}}}'
                self.run_cbq_query(alter_query)
                self.system_events.add_event(IndexingServiceEvents.index_dropped(node=node, definition_id=definition_id,
                                                                                 instance_id=instance_id,
                                                                                 indexer_id=indexer_id,
                                                                                 replica_id=replica_id))
        self.system_events.validate(self.master)

    def test_gsi_ddl_system_events_with_rebalance(self):
        redistribute = {"indexer.settings.rebalance.redistribute_indexes": True}
        self.rest.set_index_settings(redistribute)
        index_name_list = []
        for collection_namespace in self.namespaces:
            for item, index_field in zip(range(self.initial_index_num), self.index_field_set):
                index_name = f'idx_{item}'
                index_name_list.append(index_name)
                index_gen = QueryDefinition(index_name=index_name, index_fields=index_field)
                query = index_gen.generate_index_create_query(namespace=collection_namespace,
                                                              defer_build=self.defer_build,
                                                              num_replica=self.num_index_replicas)
                self.run_cbq_query(query)
                self.wait_until_indexes_online()

        # Adding nodes and checking Index DDL
        add_nodes = [self.servers[self.nodes_init]]
        indexer_metadata_b4_reb = self.index_rest.get_indexer_metadata()['status']
        rebalance_task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=add_nodes,
                                                      to_remove=[], services=['index'])
        rebalance_task.result()
        rebalance_status = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(rebalance_status)
        indexer_metadata_aft_reb = self.index_rest.get_indexer_metadata()['status']
        self.nodes_uuids = self._get_nodes_uuids()

        new_node = self.servers[self.nodes_init].ip
        for index in indexer_metadata_aft_reb:
            instance_id = index['instId']
            definition_id = index['defnId']
            replica_id = index['replicaId']
            node = index['hosts'][0].split(':')[0]
            indexer_id = self.nodes_uuids[node]
            index_name = index['name']
            if node == new_node:
                for index_old in indexer_metadata_b4_reb:
                    if index_name == index_old['name']:
                        old_node = index_old['hosts'][0].split(':')[0]
                        old_indexer_id = self.nodes_uuids[old_node]
                        self.system_events.add_event(IndexingServiceEvents.index_created(node=old_node,
                                                                                         definition_id=definition_id,
                                                                                         instance_id=instance_id,
                                                                                         indexer_id=old_indexer_id,
                                                                                         replica_id=replica_id))

                        self.system_events.add_event(
                            IndexingServiceEvents.index_building(node=old_node, definition_id=definition_id,
                                                                 instance_id=instance_id,
                                                                 indexer_id=old_indexer_id,
                                                                 replica_id=replica_id))
                        self.system_events.add_event(
                            IndexingServiceEvents.index_online(node=old_node, definition_id=definition_id,
                                                               instance_id=instance_id,
                                                               indexer_id=old_indexer_id,
                                                               replica_id=replica_id))
                        self.system_events.add_event(
                            IndexingServiceEvents.index_dropped(node=old_node, definition_id=definition_id,
                                                                instance_id=instance_id,
                                                                indexer_id=old_indexer_id,
                                                                replica_id=replica_id))
            self.system_events.add_event(IndexingServiceEvents.index_created(node=node,
                                                                             definition_id=definition_id,
                                                                             instance_id=instance_id,
                                                                             indexer_id=indexer_id,
                                                                             replica_id=replica_id))

            self.system_events.add_event(
                IndexingServiceEvents.index_building(node=node, definition_id=definition_id,
                                                     instance_id=instance_id,
                                                     indexer_id=indexer_id,
                                                     replica_id=replica_id))
            self.system_events.add_event(IndexingServiceEvents.index_online(node=node, definition_id=definition_id,
                                                                            instance_id=instance_id,
                                                                            indexer_id=indexer_id,
                                                                            replica_id=replica_id))
        self.system_events.validate(self.master)

    def test_gsi_partition_merge_system_events(self):
        remove_nodes = [self.servers[self.nodes_init - 1]]
        index_gen = QueryDefinition(index_name='idx', index_fields=['age'], partition_by_fields=['age'])
        query = index_gen.generate_index_create_query(namespace=self.namespaces[0], defer_build=True)
        self.run_cbq_query(query)
        self.wait_until_indexes_online()

        # Adding nodes and checking Index DDL
        indexer_metadata_b4_reb = self.index_rest.get_indexer_metadata()['status']
        rebalance_task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=[],
                                                      to_remove=remove_nodes, services=['index'])
        rebalance_task.result()
        rebalance_status = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(rebalance_status)
        indexer_metadata_aft_reb = self.index_rest.get_indexer_metadata()['status']
        new_partition_map = indexer_metadata_aft_reb[0]['partitionMap']
        old_partition_map = indexer_metadata_b4_reb[0]['partitionMap']

        index = indexer_metadata_aft_reb[0]
        for node in old_partition_map:
            host = node.split(":")[0]
            instance_id = index['instId']
            definition_id = index['defnId']
            replica_id = index['replicaId']
            indexer_id = self.nodes_uuids[host]
            old_partition_list = set(old_partition_map[node])
            if node in new_partition_map:
                new_partition_list = set(new_partition_map[node])
                merged_partitions = list(new_partition_list - old_partition_list)
                dropped_parition = list(old_partition_list - new_partition_list)
                for partition in merged_partitions:
                    self.system_events.add_event(
                        IndexingServiceEvents.index_created(node=host, definition_id=definition_id,
                                                            instance_id=instance_id, indexer_id=indexer_id,
                                                            replica_id=replica_id, partition_id=partition,
                                                            is_proxy_instance=True))
                    self.system_events.add_event(
                        IndexingServiceEvents.index_partition_merged(node=host, definition_id=definition_id,
                                                                     instance_id=instance_id, indexer_id=indexer_id,
                                                                     replica_id=replica_id, partition_id=partition))
                    self.system_events.add_event(
                        IndexingServiceEvents.index_dropped(node=host, definition_id=definition_id,
                                                            instance_id=instance_id, indexer_id=indexer_id,
                                                            replica_id=replica_id, partition_id=partition,
                                                            is_proxy_instance=True))
                for partition in dropped_parition:
                    self.system_events.add_event(
                        IndexingServiceEvents.index_dropped(node=host, definition_id=definition_id,
                                                            instance_id=instance_id, indexer_id=indexer_id,
                                                            replica_id=replica_id, partition_id=partition))
            else:
                for partition in old_partition_list:
                    self.system_events.add_event(
                        IndexingServiceEvents.index_dropped(node=host, definition_id=definition_id,
                                                            instance_id=instance_id,
                                                            indexer_id=indexer_id,
                                                            replica_id=replica_id,
                                                            partition_id=partition))
        self.system_events.validate(self.master)

    def test_gsi_error_system_event(self):
        collection_namespace = self.namespaces[0]
        _, keyspace = collection_namespace.split(':')
        bucket, scope, _ = keyspace.split('.')
        self.rest.set_internalSetting('enforceLimits', True)
        self.index_rest.set_gsi_tier_limit(bucket=bucket, scope=scope, limit=5)
        query_list = []
        for item, index_field in zip(range(self.initial_index_num), self.index_field_set):
            index_name = f'idx_{item}'
            index_gen = QueryDefinition(index_name=index_name, index_fields=index_field)
            query = index_gen.generate_index_create_query(namespace=self.namespaces[0], defer_build=True,
                                                          num_replica=self.num_index_replicas)
            query_list.append(query)
        with ThreadPoolExecutor() as executor:
            tasks = []
            for query in query_list:
                task = executor.submit(self.run_cbq_query, query=query)
                tasks.append(task)

            try:
                for task in tasks:
                    task.result()
            except Exception as err:
                self.log.info(err)

        indexer_metadata = self.index_rest.get_indexer_metadata()['status']
        # Todo: Add events for the error
        self.system_events.validate(self.master)

    def test_gsi_ddl_system_events_with_backup_restore(self):
        self.audit_url = "http://%s:%s/settings/audit" % (self.master.ip, self.master.port)
        self.shell = RemoteMachineShellConnection(self.master)
        curl_output = self.shell.execute_command(f"curl -u Administrator:password -X POST "
                                                 f"-d 'auditdEnabled=true' {self.audit_url}")
        collection_namespace = self.namespaces[0]
        _, keyspace = collection_namespace.split(':')
        bucket, scope, collection = keyspace.split('.')
        drop_indexes_query = []
        for item, index_field in zip(range(self.initial_index_num), self.index_field_set):
            index_name = f'idx_{item}'
            index_gen = QueryDefinition(index_name=index_name, index_fields=index_field)
            query = index_gen.generate_index_create_query(namespace=self.namespaces[0], defer_build=True,
                                                          num_replica=self.num_index_replicas)
            self.run_cbq_query(query=query)
            drop_query = index_gen.generate_index_drop_query(namespace=collection_namespace)
            drop_indexes_query.append(drop_query)
        self.wait_until_indexes_online()
        indexer_stats_before_restore = self.rest.get_indexer_metadata()['status']
        for index in indexer_stats_before_restore:
            instance_id = index['instId']
            definition_id = index['defnId']
            replica_id = index['replicaId']
            node = index['hosts'][0].split(':')[0]
            indexer_id = self.nodes_uuids[node]
            self.system_events.add_event(IndexingServiceEvents.index_created(node=node,
                                                                             definition_id=definition_id,
                                                                             instance_id=instance_id,
                                                                             indexer_id=indexer_id,
                                                                             replica_id=replica_id))
            # Adding drop events in presumption of drop query later
            self.system_events.add_event(IndexingServiceEvents.index_dropped(node=node,
                                                                             definition_id=definition_id,
                                                                             instance_id=instance_id,
                                                                             indexer_id=indexer_id,
                                                                             replica_id=replica_id))
        backup_client = IndexBackupClient(backup_node=self.master,
                                          use_cbbackupmgr=True,
                                          backup_bucket=bucket)
        backup_result = backup_client.backup([f'{scope}.{collection}'])
        self.log.info(backup_result)

        for query in drop_indexes_query:
            self.run_cbq_query(query=query)
        restore_result = backup_client.restore()
        self.log.info(restore_result)
        indexer_stats_after_restore = self.rest.get_indexer_metadata()['status']
        backup_client.remove_backup()
        for index in indexer_stats_after_restore:
            instance_id = index['instId']
            definition_id = index['defnId']
            replica_id = index['replicaId']
            node = index['hosts'][0].split(':')[0]
            indexer_id = self.nodes_uuids[node]
            self.system_events.add_event(IndexingServiceEvents.index_created(node=node,
                                                                             definition_id=definition_id,
                                                                             instance_id=instance_id,
                                                                             indexer_id=indexer_id,
                                                                             replica_id=replica_id))
        self.system_events.validate(self.master)