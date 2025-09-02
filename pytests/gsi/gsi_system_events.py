"""gsi_system_events.py: These tests validate System Events for GSI

__author__ = "Hemant Rajput"
__maintainer = "Hemant Rajput"
__email__ = "Hemant.Rajput@couchbase.com"
__git_user__ = "hrajput89"
__created_on__ = "11/24/21 11:40 am"

"""
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
        self.log.info(f"Test Start Time: {self.system_events.test_start_time}")
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
        self.nodes_uuids = self.get_nodes_uuids()
        self.log.info("==============  GSIFreeTier setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  GSIFreeTier tearDown has started ==============")
        super(GSISystemEvents, self).tearDown()
        self.log.info("==============  GSIFreeTier tearDown has completed ==============")

    def test_gsi_update_settings_system_events(self):
        query = f'CREATE PRIMARY index on {self.namespaces[0]}'
        self.run_cbq_query(query)
        self.wait_until_indexes_online()
        # Updating Indexer Settings and adding to system event to validate
        # First get the current settings to capture the old state
        current_settings = self.index_rest.get_index_settings()
        old_setting = current_settings.copy()   
        # Define the new settings to apply
        new_setting = {"indexer.settings.rebalance.redistribute_indexes": True,
                       "indexer.cpu.throttle.target": 0.95,
                       "indexer.allowScheduleCreate": False}
        # Apply the new settings
        self.index_rest.set_index_settings(new_setting)
        # Update the old_setting to only include the settings we're actually changing
        old_setting = {k: old_setting.get(k) for k in new_setting.keys()}
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        for index_node in index_nodes:
            self.system_events.add_event(IndexingServiceEvents.index_settings_updated(old_setting=old_setting,
                                                                                      new_setting=new_setting,
                                                                                      node=index_node.ip))
        # Updating Projector Settings and adding to system event to validate
        # Get current projector settings
        current_projector_settings = self.index_rest.get_index_settings()
        old_projector_setting = current_projector_settings.copy()
        # Define the new projector settings to apply
        new_projector_setting = {"projector.cpuProfile": True,
                                "projector.memProfile": True,
                                "projector.settings.log_level": "debug"}
        # Apply the new projector settings
        self.index_rest.set_index_settings(new_projector_setting)
        # Update the old_setting to only include the projector settings we're actually changing
        old_projector_setting = {k: old_projector_setting.get(k) for k in new_projector_setting.keys()}
        index_node = self.index_rest.ip
        self.system_events.add_event(IndexingServiceEvents.projector_settings_updated(old_setting=old_projector_setting,
                                                                                      new_setting=new_projector_setting,
                                                                                      node=index_node))
        # Updating Query Client Settings and adding to system event to validate
        # Get current query client settings
        current_query_settings = self.index_rest.get_index_settings()
        old_query_setting = current_query_settings.copy()
        # Define the new query client settings to apply
        new_query_setting = {"queryport.client.waitForScheduledIndex": True,
                            "queryport.client.usePlanner": False,
                            "queryport.client.log_level": "debug"}
        # Apply the new query client settings
        self.index_rest.set_index_settings(new_query_setting)
        # Update the old_setting to only include the query client settings we're actually changing
        old_query_setting = {k: old_query_setting.get(k) for k in new_query_setting.keys()}
        index_node = self.master.ip
        self.system_events.add_event(IndexingServiceEvents.query_client_settings_updated(old_setting=old_query_setting,
                                                                                         new_setting=new_query_setting,
                                                                                         node=index_node))
        self.sleep(60)
        result = self.system_events.validate(server=self.master, ignore_order=True)
        if result:
            self.log.error(result)
            self.fail("System Event validation failed")

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
                                                              num_replica=self.num_index_replica)
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

        if self.index_drop_flag:
            for collection_namespace in self.namespaces:
                for index_gen, index_name in zip(index_gens, indexes_list):
                    query = index_gen.generate_index_drop_query(namespace=collection_namespace)
                    self.run_cbq_query(query)
            for index in indexer_metadata:
                instance_id = index['instId']
                definition_id = index['defnId']
                replica_id = index['replicaId']
                node = index['hosts'][0].split(':')[0]
                indexer_id = self.nodes_uuids[node]
                self.system_events.add_event(
                    IndexingServiceEvents.index_dropped(node=node, definition_id=definition_id,
                                                        instance_id=instance_id, indexer_id=indexer_id,
                                                        replica_id=replica_id))

        result = self.system_events.validate(server=self.master, ignore_order=True)
        if result:
            self.log.error(result)
            self.fail("System Event validation failed")

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
        result = self.system_events.validate(server=self.master, ignore_order=True)
        if result:
            self.log.error(result)
            self.fail("System Event validation failed")

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
                                                              num_replica=self.num_index_replica)
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
        self.nodes_uuids = self.get_nodes_uuids()

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
        result = self.system_events.validate(server=self.master, ignore_order=True)
        if result:
            self.log.error(result)
            self.fail("System Event validation failed")

    def test_gsi_partition_merge_system_events(self):
        remove_nodes = [self.servers[self.nodes_init - 1]]
        index_gen = QueryDefinition(index_name='idx', index_fields=['age'], partition_by_fields=['age'])
        query = index_gen.generate_index_create_query(namespace=self.namespaces[0], defer_build=True)
        self.run_cbq_query(query)
        self.wait_until_indexes_online()

        # Adding nodes and checking Index DDL
        indexer_metadata_b4_reb = self.index_rest.get_indexer_metadata()['status']
        for index in indexer_metadata_b4_reb:
            instance_id = index['instId']
            definition_id = index['defnId']
            replica_id = index['replicaId']

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
                for partition in merged_partitions:
                    self.system_events.add_event(
                        IndexingServiceEvents.index_partition_merged(node=host, definition_id=definition_id,
                                                                     instance_id=instance_id, indexer_id=indexer_id,
                                                                     replica_id=replica_id, partition_id=partition))
        result = self.system_events.validate(server=self.master, ignore_order=True)
        if result:
            self.log.error(result)
            self.fail("System Event validation failed")

    def test_gsi_schedule_creation_system_event(self):
        collection_namespace = self.namespaces[0]
        _, keyspace = collection_namespace.split(':')
        bucket, scope, _ = keyspace.split('.')
        self.index_rest.set_index_settings({"indexer.debug.enableBackgroundIndexCreation": False})
        query_list = []
        for item, index_field in zip(range(self.initial_index_num), self.index_field_set):
            index_name = f'idx_{item}'
            index_gen = QueryDefinition(index_name=index_name, index_fields=index_field)
            query = index_gen.generate_index_create_query(namespace=self.namespaces[0], defer_build=True,
                                                          num_replica=self.num_index_replica)
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
        for index in indexer_metadata:
            instance_id = index['instId']
            definition_id = index['defnId']
            replica_id = index['replicaId']
            node = index['hosts'][0].split(':')[0]
            indexer_id = self.nodes_uuids[node]
            if index['status'] == 'Scheduled for Creation':
                self.system_events.add_event(
                    IndexingServiceEvents.index_schedule_for_creation(node=node,
                                                                      definition_id=definition_id,
                                                                      indexer_id=indexer_id,
                                                                      replica_id=replica_id))
            else:
                self.system_events.add_event(IndexingServiceEvents.index_created(node=node,
                                                                                 definition_id=definition_id,
                                                                                 instance_id=instance_id,
                                                                                 indexer_id=indexer_id,
                                                                                 replica_id=replica_id))
        result = self.system_events.validate(server=self.master, ignore_order=True)
        if result:
            self.log.error(result)
            self.fail("System Event validation failed")

    def test_gsi_schedule_error_system_event(self):
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
                                                          num_replica=self.num_index_replica)
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

        self.sleep(30, "Giving some time to indexes to build or move to error state")
        indexer_metadata = self.index_rest.get_indexer_metadata()['status']
        for index in indexer_metadata:
            instance_id = index['instId']
            definition_id = index['defnId']
            replica_id = index['replicaId']
            node = index['hosts'][0].split(':')[0]
            indexer_id = self.nodes_uuids[node]
            if index['status'] == 'Error':
                err_string = 'Limit for number of indexes that can be created per scope has been reached. Limit : 5'
                self.system_events.add_event(IndexingServiceEvents.index_schedule_error(node=node,
                                                                                        definition_id=definition_id,
                                                                                        instance_id=0,
                                                                                        indexer_id=indexer_id,
                                                                                        replica_id=replica_id,
                                                                                        error_string=err_string))
            else:
                self.system_events.add_event(IndexingServiceEvents.index_created(node=node,
                                                                                 definition_id=definition_id,
                                                                                 instance_id=instance_id,
                                                                                 indexer_id=indexer_id,
                                                                                 replica_id=replica_id))
        result = self.system_events.validate(server=self.master, ignore_order=True)
        if result:
            self.log.error(result)
            self.fail("System Event validation failed")

    def test_gsi_ddl_system_events_with_backup_restore(self):
        self.audit_url = "http://%s:%s/settings/audit" % (self.master.ip, self.master.port)
        self.shell = RemoteMachineShellConnection(self.master)
        self.shell.execute_command(f"curl -u Administrator:password -X POST "
                                   f"-d 'auditdEnabled=true' {self.audit_url}")
        collection_namespace = self.namespaces[0]
        _, keyspace = collection_namespace.split(':')
        bucket, scope, collection = keyspace.split('.')
        drop_indexes_query = []
        for item, index_field in zip(range(self.initial_index_num), self.index_field_set):
            index_name = f'idx_{item}'
            index_gen = QueryDefinition(index_name=index_name, index_fields=index_field)
            query = index_gen.generate_index_create_query(namespace=self.namespaces[0], defer_build=True,
                                                          num_replica=self.num_index_replica)
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
        backup_result = backup_client.backup([f'{scope}.{collection}'], use_https=self.use_https)
        self.log.info(backup_result)

        for query in drop_indexes_query:
            self.run_cbq_query(query=query)
        restore_result = backup_client.restore(use_https=self.use_https)
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
        result = self.system_events.validate(server=self.master, ignore_order=True)
        if result:
            self.log.error(result)
            self.fail("System Event validation failed")
