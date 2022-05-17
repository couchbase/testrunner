import copy
import json
import logging
import math
import os
import random
import threading
import time

from couchbase_helper.cluster import Cluster
from couchbase_helper.query_definitions import SQLDefinitionGenerator
from couchbase_helper.tuq_generators import TuqGenerators
from lib.membase.helper.cluster_helper import ClusterOperationHelper
from lib.remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection, RestHelper
from couchbase_helper.documentgenerator import SDKDataLoader
from couchbase_helper.query_definitions import QueryDefinition
from collection.collections_rest_client import CollectionsRest
from collection.collections_stats import CollectionsStats
from collection.collections_cli_client import CollectionsCLI
from tasks.task import ConcurrentIndexCreateTask
from .newtuq import QueryTests
from tasks.task import SDKLoadDocumentsTask
from deepdiff import DeepDiff

log = logging.getLogger(__name__)


class BaseSecondaryIndexingTests(QueryTests):

    def setUp(self):
        super(BaseSecondaryIndexingTests, self).setUp()
        self.ansi_join = self.input.param("ansi_join", False)
        self.index_lost_during_move_out = []
        self.verify_using_index_status = self.input.param("verify_using_index_status", False)
        self.use_replica_when_active_down = self.input.param("use_replica_when_active_down", True)
        self.use_where_clause_in_index = self.input.param("use_where_clause_in_index", False)
        self.scan_consistency = self.input.param("scan_consistency", "request_plus")
        self.scan_vector_per_values = self.input.param("scan_vector_per_values", None)
        self.alter_index_error = self.input.param("alter_index_error", None)
        self.timeout_for_index_online = self.input.param("timeout_for_index_online", 600)
        self.verify_query_result = self.input.param("verify_query_result", True)
        self.verify_explain_result = self.input.param("verify_explain_result", True)
        self.defer_build = self.input.param("defer_build", True)
        self.build_index_after_create = self.input.param("build_index_after_create", True)
        self.run_query_with_explain = self.input.param("run_query_with_explain", True)
        self.run_query = self.input.param("run_query", True)
        self.graceful = self.input.param("graceful", False)
        self.groups = self.input.param("groups", "all").split(":")
        self.use_rest = self.input.param("use_rest", False)
        self.plasma_dgm = self.input.param("plasma_dgm", False)
        self.bucket_name = self.input.param("bucket_name", "default")
        self.num_scopes = self.input.param("num_scopes", 1)
        self.num_collections = self.input.param("num_collections", 1)
        self.item_to_delete = self.input.param('item_to_delete', None)
        self.test_bucket = self.input.param('test_bucket', 'test_bucket')
        self.wait_for_scheduled_index = self.input.param('wait_for_scheduled_index', False)
        self.scheduled_index_create_rebal = self.input.param('scheduled_index_create_rebal', True)
        self.enable_dgm = self.input.param('enable_dgm', False)
        self.rest = RestConnection(self.master)
        self.collection_rest = CollectionsRest(self.master)
        self.collection_cli = CollectionsCLI(self.master)
        self.stat = CollectionsStats(self.master)
        self.scope_prefix = self.input.param('scope_prefix', 'test_scope')
        self.collection_prefix = self.input.param('collection_prefix', 'test_collection')
        self.run_cbq_query = self.n1ql_helper.run_cbq_query
        self.num_of_docs_per_collection = self.input.param('num_of_docs_per_collection', 1000)
        self.deploy_node_info = None
        self.server_grouping = self.input.param("server_grouping", None)
        self.server_group_map = {}
        if not self.use_rest:
            query_definition_generator = SQLDefinitionGenerator()
            if self.dataset == "default" or self.dataset == "employee":
                self.query_definitions = query_definition_generator.generate_employee_data_query_definitions()
            if self.dataset == "simple":
                self.query_definitions = query_definition_generator.generate_simple_data_query_definitions()
            if self.dataset == "sabre":
                self.query_definitions = query_definition_generator.generate_sabre_data_query_definitions()
            if self.dataset == "bigdata":
                self.query_definitions = query_definition_generator.generate_big_data_query_definitions()
            if self.dataset == "array":
                self.query_definitions = query_definition_generator.generate_airlines_data_query_definitions()
            self.query_definitions = query_definition_generator.filter_by_group(self.groups, self.query_definitions)
        self.ops_map = self._create_operation_map()
        self.find_nodes_in_list()
        self.generate_map_nodes_out_dist()
        self.memory_create_list = []
        self.memory_drop_list = []
        self.skip_cleanup = self.input.param("skip_cleanup", False)
        self.index_loglevel = self.input.param("index_loglevel", None)
        if self.index_loglevel:
            self.set_indexer_logLevel(self.index_loglevel)
        if self.dgm_run:
            self._load_doc_data_all_buckets(gen_load=self.gens_load)
        self.gsi_thread = Cluster()
        self.defer_build = self.defer_build and self.use_gsi_for_secondary
        self.num_index_replicas = self.input.param("num_index_replica", 0)
        index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)[0]
        if self.use_https:
            self.node_port = '18091'
        else:
            self.node_port = '8091'
        if index_node:
            self.index_rest = RestConnection(index_node)
            # New settings for schedule Indexes
            self.index_rest.set_index_settings({"queryport.client.waitForScheduledIndex": self.wait_for_scheduled_index})
            self.index_rest.set_index_settings({"indexer.allowScheduleCreateRebal": self.scheduled_index_create_rebal})

    def tearDown(self):
        super(BaseSecondaryIndexingTests, self).tearDown()

    def _create_server_groups(self):
        if self.server_grouping:
            server_groups = self.server_grouping.split(":")
            self.log.info("server groups : %s", server_groups)

            zones = list(self.rest.get_zone_names().keys())

            # Delete Server groups
            for zone in zones:
                if zone != "Group 1":
                    nodes_in_zone = self.rest.get_nodes_in_zone(zone)
                    if nodes_in_zone:
                        self.rest.shuffle_nodes_in_zones(list(nodes_in_zone.keys()),
                                                         zone, "Group 1")
                    self.rest.delete_zone(zone)

            zones = list(self.rest.get_zone_names().keys())
            source_zone = zones[0]

            # Create Server groups
            for i in range(1, len(server_groups) + 1):
                server_grp_name = "ServerGroup_" + str(i)
                if not self.rest.is_zone_exist(server_grp_name):
                    self.rest.add_zone(server_grp_name)

            # Add nodes to Server groups
            i = 1
            for server_grp in server_groups:
                server_list = []
                server_grp_name = "ServerGroup_" + str(i)
                i += 1
                nodes_in_server_group = server_grp.split("-")
                for node in nodes_in_server_group:
                    self.rest.shuffle_nodes_in_zones(
                        [self.servers[int(node)].ip], source_zone,
                        server_grp_name)
                    server_list.append(self.servers[int(node)].ip + ":" + self.servers[int(node)].port)
                self.server_group_map[server_grp_name] = server_list

    def create_index(self, bucket, query_definition, deploy_node_info=None, desc=None):
        create_task = self.async_create_index(bucket, query_definition, deploy_node_info, desc=desc)
        create_task.result()
        if self.build_index_after_create:
            if self.defer_build:
                build_index_task = self.async_build_index(bucket, [f'`{query_definition.index_name}`'])
                build_index_task.result()
            check = self.n1ql_helper.is_index_ready_and_in_list(bucket, query_definition.index_name,
                                                                server=self.n1ql_node)
            self.assertTrue(check, "index {0} failed to be created".format(query_definition.index_name))

    def create_scope_collections(self, num_scopes=1, num_collections=1, scope_prefix=None,
                                 collection_prefix=None, test_bucket=None):
        if not collection_prefix:
            collection_prefix = self.collection_prefix
        if not scope_prefix:
            scope_prefix = self.scope_prefix
        if not test_bucket:
            test_bucket = self.test_bucket
        self.cli_rest.create_scope_collection_count(scope_num=num_scopes, collection_num=num_collections,
                                                    scope_prefix=scope_prefix,
                                                    collection_prefix=collection_prefix,
                                                    bucket=test_bucket)
        self.update_keyspace_list(test_bucket)

    def update_keyspace_list(self, bucket, default=False):
        #initialize self.keyspace before using this method
        self.keyspace = []
        scopes = self.cli_rest.get_bucket_scopes(bucket=bucket)
        if not default:
            scopes.remove('_default')
        for s_item in scopes:
            collections = self.cli_rest.get_scope_collections(bucket=bucket, scope=s_item)
            for c_item in collections:
                self.keyspace.append(f'default:{bucket}.{s_item}.{c_item}')
        self.keyspace = list(set(self.keyspace))

    def create_indexes(self, num=0, defer_build=False, itr=0, expected_failure=[], index_name_prefix="idx_",
                       query_def_group="plasma_test"):
        query_definition_generator = SQLDefinitionGenerator()
        index_create_tasks = []
        self.log.info(threading.currentThread().getName() + " Started")
        if len(self.keyspace) < num:
            num_indexes_collection = math.ceil(num / len(self.keyspace))
        else:
            num_indexes_collection = 1
        for collection_keyspace in self.keyspace:
            if self.run_tasks:
                collection_name = collection_keyspace.split('.')[-1]
                scope_name = collection_keyspace.split('.')[-2]
                query_definitions = query_definition_generator. \
                    generate_employee_data_query_definitions(index_name_prefix=index_name_prefix +
                                                                               scope_name + "_"
                                                                               + collection_name,
                                                             keyspace=collection_keyspace)
                server = random.choice(self.n1ql_nodes)
                index_create_task = ConcurrentIndexCreateTask(server, self.test_bucket, scope_name,
                                                              collection_name, query_definitions,
                                                              self.index_ops_obj, self.n1ql_helper, num_indexes_collection, defer_build,
                                                              itr, expected_failure, query_def_group)
                self.index_create_task_manager.schedule(index_create_task)
                index_create_tasks.append(index_create_task)
        self.log.info(threading.currentThread().getName() + " Completed")

        return index_create_tasks

    def load_docs(self, start_doc, key_prefix="bigkeybigkeybigkeybigkeybigkeybigkeybigkeybigkeyb_"):
        load_tasks = self.async_load_docs(start_doc, key_prefix=key_prefix)

        for task in load_tasks:
            task.result()

    def async_load_docs(self, start_doc, key_prefix="bigkeybigkeybigkeybigkeybigkeybigkeybigkeybigkeyb_", num_ops=None):
        load_tasks = []
        if not num_ops:
            num_ops = self.num_items_in_collection
        sdk_data_loader = SDKDataLoader(start_seq_num=start_doc, num_ops=self.num_items_in_collection,
                                        percent_create=self.percent_create,
                                        percent_update=self.percent_update, percent_delete=self.percent_delete,
                                        all_collections=self.all_collections, timeout=1800,
                                        json_template=self.dataset_template,
                                        key_prefix=key_prefix)
        for bucket in self.buckets:
            _task = SDKLoadDocumentsTask(self.master, bucket, sdk_data_loader)
            self.sdk_loader_manager.schedule(_task)
            load_tasks.append(_task)

        return load_tasks

    def async_create_index(self, bucket, query_definition, deploy_node_info=None, desc=None):
        index_where_clause = None
        if self.use_where_clause_in_index:
            index_where_clause = query_definition.index_where_clause
        self.query = query_definition.generate_index_create_query(namespace=bucket,
                                                                  use_gsi_for_secondary=self.use_gsi_for_secondary,
                                                                  deploy_node_info=deploy_node_info,
                                                                  defer_build=self.defer_build,
                                                                  index_where_clause=index_where_clause,
                                                                  num_replica=self.num_index_replicas, desc=desc)
        create_index_task = self.gsi_thread.async_create_index(server=self.n1ql_node, bucket=bucket,
                                                               query=self.query, n1ql_helper=self.n1ql_helper,
                                                               index_name=query_definition.index_name,
                                                               defer_build=self.defer_build)
        return create_index_task

    def create_index_using_rest(self, bucket, query_definition, exprType='N1QL', deploy_node_info=None, desc=None):
        ind_content = query_definition.generate_gsi_index_create_query_using_rest(bucket=bucket,
                                                                                  deploy_node_info=deploy_node_info,
                                                                                  defer_build=None,
                                                                                  index_where_clause=None,
                                                                                  gsi_type=self.gsi_type,
                                                                                  desc=desc)

        log.info("Creating index {0}...".format(query_definition.index_name))
        return self.rest.create_index_with_rest(ind_content)

    def async_build_index(self, bucket, index_list=None):
        if not index_list:
            index_list = []
        self.query = self.n1ql_helper.gen_build_index_query(bucket=bucket, index_list=index_list)
        self.log.info(self.query)
        build_index_task = self.gsi_thread.async_build_index(server=self.n1ql_node, bucket=bucket,
                                                             query=self.query, n1ql_helper=self.n1ql_helper)
        return build_index_task

    def async_monitor_index(self, bucket, index_name=None):
        monitor_index_task = self.gsi_thread.async_monitor_index(server=self.n1ql_node, bucket=bucket,
                                                                 n1ql_helper=self.n1ql_helper, index_name=index_name,
                                                                 timeout=self.timeout_for_index_online)
        return monitor_index_task

    def multi_create_index(self, buckets=None, query_definitions=None, deploy_node_info=None):
        if not buckets:
            buckets = self.buckets
        if not query_definitions:
            query_definitions = self.query_definitions
        for bucket in buckets:
            for query_definition in query_definitions:
                index_info = "{0}:{1}".format(bucket.name, query_definition.index_name)
                if index_info not in self.memory_create_list:
                    self.memory_create_list.append(index_info)
                    self.create_index(bucket.name, query_definition, deploy_node_info)

    def multi_create_index_using_rest(self, buckets=None, query_definitions=None, deploy_node_info=None):
        self.index_id_map = {}
        if not buckets:
            buckets = self.buckets
        if not query_definitions:
            query_definitions = self.query_definitions
        for bucket in buckets:
            if bucket not in list(self.index_id_map.keys()):
                self.index_id_map[bucket] = {}
            for query_definition in query_definitions:
                id_map = self.create_index_using_rest(bucket=bucket, query_definition=query_definition,
                                                      deploy_node_info=deploy_node_info)
                self.index_id_map[bucket][query_definition] = id_map["id"]

    def async_multi_create_index(self, buckets=None, query_definitions=None):
        if not buckets:
            buckets = self.buckets
        if not query_definitions:
            query_definitions = self.query_definitions
        create_index_tasks = []
        self.index_lost_during_move_out = []
        self.log.info(self.index_nodes_out)
        index_node_count = 0
        for query_definition in query_definitions:
            index_info = "{0}".format(query_definition.index_name)
            if index_info not in self.memory_create_list:
                self.memory_create_list.append(index_info)
                if index_node_count < len(self.index_nodes_out):
                    node_index = index_node_count
                    self.deploy_node_info = ["{0}:{1}".format(self.index_nodes_out[index_node_count].ip,
                                                              self.node_port)]
                    if query_definition.index_name not in self.index_lost_during_move_out:
                        self.index_lost_during_move_out.append(query_definition.index_name)
                    index_node_count += 1
                for bucket in buckets:
                    create_index_tasks.append(self.async_create_index(bucket.name,
                                                                      query_definition,
                                                                      deploy_node_info=self.deploy_node_info))
                # self.sleep(3)
        if self.defer_build and self.build_index_after_create:
            index_list = []
            for task in create_index_tasks:
                task.result()
            for query_definition in query_definitions:
                if query_definition.index_name not in index_list:
                    index_list.append(query_definition.index_name)
            for bucket in self.buckets:
                build_index_task = self.async_build_index(bucket, index_list)
                build_index_task.result()
            monitor_index_tasks = []
            for index_name in index_list:
                for bucket in self.buckets:
                    monitor_index_tasks.append(self.async_monitor_index(bucket.name, index_name))
            return monitor_index_tasks
        else:
            return create_index_tasks

    def multi_drop_index_using_rest(self, buckets=None, query_definitions=None):
        if not buckets:
            buckets = self.buckets
        if not query_definitions:
            query_definitions = self.query_definitions
        for bucket in buckets:
            for query_definition in query_definitions:
                self.drop_index_using_rest(bucket, query_definition)

    def multi_drop_index(self, buckets=None, query_definitions=None):
        if not buckets:
            buckets = self.buckets
        if not query_definitions:
            query_definitions = self.query_definitions
        for bucket in buckets:
            for query_definition in query_definitions:
                index_info = query_definition.generate_index_drop_query(namespace=bucket.name)
                index_create_info = "{0}:{1}".format(bucket.name, query_definition.index_name)
                if index_info not in self.memory_drop_list:
                    self.memory_drop_list.append(index_info)
                    self.drop_index(bucket.name, query_definition)
                if index_create_info in self.memory_create_list:
                    self.memory_create_list.remove(index_create_info)

    def reset_data_mount_point(self, nodes):
        for node in nodes:
            shell = RemoteMachineShellConnection(node)
            shell.execute_command('umount -l /data; '
                                  'mkdir -p /usr/disk-img; dd if=/dev/zero '
                                  'of=/usr/disk-img/disk-quota.ext4 count=10485760; '
                                  '/sbin/mkfs -t ext4 -q /usr/disk-img/disk-quota.ext4 -F; '
                                  'mount -o loop,rw,usrquota,grpquota /usr/disk-img/disk-quota.ext4 /data; '
                                  'umount -l /data; fsck.ext4 /usr/disk-img/disk-quota.ext4 -y; '
                                  'chattr +i /data; mount -o loop,rw,usrquota,grpquota /usr/disk-img/disk-quota.ext4 '
                                  '/data; rm -rf /data/*; chmod -R 777 /data')

    def get_size_of_metastore_file(self):
        indexer_nodes = self.get_nodes_from_services_map(
            service_type="index", get_all_nodes=True)
        metastore_size_on_nodes = []
        for node in indexer_nodes:
            data_dir = RestConnection(node).get_index_path()
            shell = RemoteMachineShellConnection(node)
            metastore_size, err = shell.execute_command(
                "find {0}/@2i -name MetadataStore.[0-9]* | xargs du -sb|awk '{{print $1}}'".
                    format(data_dir))

            metastore_size = metastore_size[0]
            metastore_size_on_nodes.append({"nodeip": node.ip, "metastore_size": metastore_size})

        return metastore_size_on_nodes

    def async_multi_drop_index(self, buckets=None, query_definitions=None, pre_cc=False):
        if not buckets:
            buckets = self.buckets
        if not query_definitions:
            query_definitions = self.query_definitions
        drop_index_tasks = []
        for bucket in buckets:
            for query_definition in query_definitions:
                index_info = query_definition.generate_index_drop_query(namespace=bucket.name, pre_cc=pre_cc)
                if index_info not in self.memory_drop_list:
                    self.memory_drop_list.append(index_info)
                    drop_index_tasks.append(self.async_drop_index(bucket.name, query_definition, pre_cc=pre_cc))
        return drop_index_tasks

    def alter_index_replicas(self, index_name, namespace="default", action='replica_count', num_replicas=1,
                             set_error=False, nodes=None, replica_id=1, expect_failure=False):
        error = []
        if action == 'move':
            alter_index_query = f'ALTER INDEX {index_name} on {namespace} ' \
                f'WITH {{"action": "move", "nodes":{nodes}}}'
        elif action == 'replica_count':
            if nodes is not None:
                alter_index_query = f'ALTER INDEX {index_name} on {namespace} ' \
                                    f' WITH {{"action":"replica_count","num_replica": {num_replicas}, "nodes":{nodes}}}'
            else:
                alter_index_query = f'ALTER INDEX {index_name} on {namespace} ' \
                    f' WITH {{"action":"replica_count","num_replica": {num_replicas}}}'
        elif action == 'drop_replica':
            alter_index_query = f'ALTER INDEX {index_name} ON {namespace} ' \
                f'WITH {{"action":"drop_replica","replicaId": {replica_id}}}'
        else:
            # Negative case consideration
            alter_index_query = f'ALTER INDEX {index_name} on {namespace} ' \
                ' WITH {{"action":"replica_count"}}'

        try:
            self.n1ql_helper.run_cbq_query(query=alter_index_query, server=self.n1ql_node)
        except Exception as ex:
            error.append(str(ex))
            self.log.error(str(ex))

        if error:
            if expect_failure:
                self.log.info("alter index replica count failed as expected")
                self.log.info("Error : %s", error)
                if set_error:
                    self.alter_index_error = error
            else:
                self.log.info("Error : %s", error)
                self.fail("alter index failed to change the number of replicas")
        else:
            self.log.info("alter index started successfully")
        return error

    def drop_index(self, bucket, query_definition, verify_drop=True):
        try:
            self.query = query_definition.generate_index_drop_query(namespace=bucket,
                                                                    use_gsi_for_secondary=self.use_gsi_for_secondary,
                                                                    use_gsi_for_primary=self.use_gsi_for_primary)
            actual_result = self.n1ql_helper.run_cbq_query(query=self.query, server=self.n1ql_node)
            if verify_drop:
                check = self.n1ql_helper._is_index_in_list(bucket, query_definition.index_name, server=self.n1ql_node)
                self.assertFalse(check, "index {0} failed to be deleted".format(query_definition.index_name))
        except Exception as ex:
            self.log.info(ex)
            query = "select * from system:indexes"
            actual_result = self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_node)
            self.log.info(actual_result)

    def drop_index_using_rest(self, bucket, query_definition, verify_drop=True):
        self.log.info("Dropping index: {0}...".format(query_definition.index_name))
        self.rest.drop_index_with_rest(self.index_id_map[bucket][query_definition])
        if verify_drop:
            check = self.n1ql_helper._is_index_in_list(bucket, query_definition.index_name, server=self.n1ql_node)
            self.assertFalse(check, "Index {0} failed to be deleted".format(query_definition.index_name))
            del (self.index_id_map[bucket][query_definition])

    def async_drop_index(self, bucket, query_definition, pre_cc=False):
        self.query = query_definition.generate_index_drop_query(namespace=bucket,
                                                                use_gsi_for_secondary=self.use_gsi_for_secondary,
                                                                use_gsi_for_primary=self.use_gsi_for_primary,
                                                                pre_cc=pre_cc)
        drop_index_task = self.gsi_thread.async_drop_index(server=self.n1ql_node, bucket=bucket, query=self.query,
                                                           n1ql_helper=self.n1ql_helper,
                                                           index_name=query_definition.index_name)
        return drop_index_task

    def query_using_index_with_explain(self, bucket, query_definition):
        self.query = query_definition.generate_query_with_explain(bucket=bucket)
        actual_result = self.n1ql_helper.run_cbq_query(query=self.query, server=self.n1ql_node)
        self.log.info(actual_result)
        if self.verify_explain_result:
            check = self.n1ql_helper.verify_index_with_explain(actual_result, query_definition.index_name)
            self.assertTrue(check, "Index %s not found" % (query_definition.index_name))

    def async_query_using_index_with_explain(self, bucket, query_definition):
        self.query = query_definition.generate_query_with_explain(bucket=bucket)
        query_with_index_task = self.gsi_thread.async_n1ql_query_verification(server=self.n1ql_node, bucket=bucket,
                                                                              query=self.query,
                                                                              n1ql_helper=self.n1ql_helper,
                                                                              is_explain_query=True,
                                                                              index_name=query_definition.index_name,
                                                                              verify_results=self.verify_explain_result)
        return query_with_index_task

    def multi_query_using_index_with_explain(self, buckets=None, query_definitions=None):
        if not buckets:
            buckets = self.buckets
        if not query_definitions:
            query_definitions = self.query_definitions
        for bucket in buckets:
            for query_definition in query_definitions:
                self.query_using_index_with_explain(bucket.name,
                                                    query_definition)

    def async_multi_query_using_index_with_explain(self, buckets=None, query_definitions=None):
        if not buckets:
            buckets = self.buckets
        if not query_definitions:
            query_definitions = self.query_definitions
        async_query_with_explain_tasks = []
        for bucket in buckets:
            for query_definition in query_definitions:
                async_query_with_explain_tasks.append(self.async_query_using_index_with_explain(bucket.name,
                                                                                                query_definition))
        return async_query_with_explain_tasks

    def query_using_index(self, bucket, query_definition, expected_result=None, scan_consistency=None,
                          scan_vector=None, verify_results=True):
        if not scan_consistency:
            scan_consistency = self.scan_consistency
        self.gen_results.query = query_definition.generate_query(bucket=bucket)
        if expected_result == None:
            expected_result = self.gen_results.generate_expected_result(print_expected_result=False)
        self.query = self.gen_results.query
        log.info("Query : {0}".format(self.query))
        msg, check = self.n1ql_helper.run_query_and_verify_result(query=self.query, server=self.n1ql_node, timeout=500,
                                                                  expected_result=expected_result,
                                                                  scan_consistency=scan_consistency,
                                                                  scan_vector=scan_vector,
                                                                  verify_results=verify_results)
        self.assertTrue(check, msg)

    def async_query_using_index(self, bucket, query_definition, expected_result=None, scan_consistency=None,
                                scan_vector=None):
        self.gen_results.query = query_definition.generate_query(bucket=bucket)
        self.log.info("Query : {0}".format(self.gen_results.query))
        if expected_result == None:
            expected_result = self.gen_results.generate_expected_result(print_expected_result=False)
        self.query = self.gen_results.query
        query_with_index_task = self.gsi_thread.async_n1ql_query_verification(
            server=self.n1ql_node, bucket=bucket,
            query=self.query, n1ql_helper=self.n1ql_helper,
            expected_result=expected_result, index_name=query_definition.index_name,
            scan_consistency=scan_consistency, scan_vector=scan_vector,
            verify_results=self.verify_query_result)
        return query_with_index_task

    def query_using_index_with_emptyset(self, bucket, query_definition):
        self.gen_results.query = query_definition.generate_query(bucket=bucket)
        self.log.info("Query : {0}".format(self.gen_results.query))
        self.query = self.gen_results.query
        actual_result = self.n1ql_helper.run_cbq_query(query=self.query, server=self.n1ql_node)
        self.assertTrue(len(actual_result["results"]) == 0, "Result is not empty {0}".format(actual_result["results"]))

    def multi_query_using_index_with_emptyresult(self, buckets=None, query_definitions=None):
        if not buckets:
            buckets = self.buckets
        if not query_definitions:
            query_definitions = self.query_definitions
        for bucket in buckets:
            for query_definition in query_definitions:
                self.query_using_index_with_emptyset(bucket.name, query_definition)

    def multi_query_using_index(self, buckets=None, query_definitions=None,
                                expected_results=None, scan_consistency=None,
                                scan_vectors=None, verify_results=True):
        if not buckets:
            buckets = self.buckets
        if not query_definitions:
            query_definitions = self.query_definitions
        for bucket in buckets:
            scan_vector = None
            if scan_vectors != None:
                scan_vector = scan_vectors[bucket.name]
            for query_definition in query_definitions:
                if expected_results:
                    expected_result = expected_results[query_definition.index_name]
                else:
                    expected_result = None
                self.query_using_index(bucket=bucket.name, query_definition=query_definition,
                                       expected_result=expected_result, scan_consistency=scan_consistency,
                                       scan_vector=scan_vector, verify_results=verify_results)

    def async_multi_query_using_index(self, buckets=[], query_definitions=[], expected_results={},
                                      scan_consistency=None, scan_vectors=None):
        multi_query_tasks = []
        for bucket in buckets:
            scan_vector = None
            if scan_vectors != None:
                scan_vector = scan_vectors[bucket.name]
            for query_definition in query_definitions:
                if expected_results:
                    multi_query_tasks.append(self.async_query_using_index(bucket.name, query_definition,
                                                                          expected_results[query_definition.index_name],
                                                                          scan_consistency=scan_consistency,
                                                                          scan_vector=scan_vector))
                else:
                    multi_query_tasks.append(self.async_query_using_index(bucket.name, query_definition, None,
                                                                          scan_consistency=scan_consistency,
                                                                          scan_vector=scan_vector))
        return multi_query_tasks

    def async_check_and_run_operations(self, buckets=[], initial=False, before=False, after=False,
                                       in_between=False, scan_consistency=None, scan_vectors=None):
        # self.verify_query_result = True
        # self.verify_explain_result = True
        if initial:
            self._set_query_explain_flags("initial")
            self.log.info(self.ops_map["initial"])
            return self.async_run_multi_operations(buckets=buckets,
                                                   create_index=self.ops_map["initial"]["create_index"],
                                                   drop_index=self.ops_map["initial"]["drop_index"],
                                                   query=self.ops_map["initial"]["query_ops"],
                                                   query_with_explain=self.ops_map["initial"]["query_explain_ops"],
                                                   scan_consistency=scan_consistency,
                                                   scan_vectors=scan_vectors)
        if before:
            self._set_query_explain_flags("before")
            self.log.info(self.ops_map["before"])
            return self.async_run_multi_operations(buckets=buckets,
                                                   create_index=self.ops_map["before"]["create_index"],
                                                   drop_index=self.ops_map["before"]["drop_index"],
                                                   query=self.ops_map["before"]["query_ops"],
                                                   query_with_explain=self.ops_map["before"]["query_explain_ops"],
                                                   scan_consistency=scan_consistency,
                                                   scan_vectors=scan_vectors)
        if in_between:
            self._set_query_explain_flags("in_between")
            self.log.info(self.ops_map["initial"])
            return self.async_run_multi_operations(buckets=buckets,
                                                   create_index=self.ops_map["in_between"]["create_index"],
                                                   drop_index=self.ops_map["in_between"]["drop_index"],
                                                   query=self.ops_map["in_between"]["query_ops"],
                                                   query_with_explain=self.ops_map["in_between"]["query_explain_ops"],
                                                   scan_consistency=scan_consistency,
                                                   scan_vectors=scan_vectors)
        if after:
            self._set_query_explain_flags("after")
            self.log.info(self.ops_map["initial"])
            return self.async_run_multi_operations(buckets=buckets,
                                                   create_index=self.ops_map["after"]["create_index"],
                                                   drop_index=self.ops_map["after"]["drop_index"],
                                                   query=self.ops_map["after"]["query_ops"],
                                                   query_with_explain=self.ops_map["after"]["query_explain_ops"],
                                                   scan_consistency="request_plus",
                                                   scan_vectors=scan_vectors)

    def run_multi_operations(self, buckets=[], query_definitions=[], expected_results={},
                             create_index=False, drop_index=False, query_with_explain=False, query=False,
                             scan_consistency=None, scan_vectors=None):
        try:
            if create_index:
                self.multi_create_index(buckets, query_definitions)
            if query_with_explain:
                self.multi_query_using_index_with_explain(buckets, query_definitions)
            if query:
                self.multi_query_using_index(buckets, query_definitions,
                                             expected_results, scan_consistency=scan_consistency,
                                             scan_vectors=scan_vectors)
        except Exception as ex:
            self.log.info(ex)
            raise
        finally:
            if drop_index and not self.skip_cleanup:
                self.multi_drop_index(buckets, query_definitions)

    def async_run_multi_operations(self, buckets=None, query_definitions=None, expected_results=None,
                                   create_index=False, drop_index=False, query_with_explain=False, query=False,
                                   scan_consistency=None, scan_vectors=None):
        tasks = []
        if not query_definitions:
            query_definitions = self.query_definitions
        try:
            if create_index:
                tasks += self.async_multi_create_index(buckets, query_definitions)
            if query_with_explain:
                tasks += self.async_multi_query_using_index_with_explain(buckets, query_definitions)
            if query:
                tasks += self.async_multi_query_using_index(buckets, query_definitions, expected_results,
                                                            scan_consistency=scan_consistency,
                                                            scan_vectors=scan_vectors)
            if drop_index:
                tasks += self.async_multi_drop_index(self.buckets, query_definitions)
        except Exception as ex:
            self.log.info(ex)
            raise
        return tasks

    def async_run_operations(self, phase, buckets=None, query_definitions=None, expected_results=None,
                             scan_consistency=None, scan_vectors=None):
        if not buckets:
            buckets = self.buckets
        if not query_definitions:
            query_definitions = self.query_definitions
        if not scan_consistency:
            scan_consistency = self.scan_consistency
        tasks = []
        operation_map = self.generate_operation_map(phase)
        self.log.info("=== {0}: {1} ===".format(phase.upper(), operation_map))
        nodes_out = []
        if isinstance(self.nodes_out_dist, str):
            for service in self.nodes_out_dist.split("-"):
                nodes_out.append(service.split(":")[0])
        if operation_map:
            try:
                if "create_index" in operation_map:
                    if ("index" in nodes_out or "n1ql" in nodes_out) and phase == "in_between":
                        tasks = []
                    else:
                        tasks += self.async_multi_create_index(buckets, query_definitions)
                if "query_with_explain" in operation_map:
                    if "n1ql" in nodes_out and phase == "in_between":
                        tasks = []
                    else:
                        tasks += self.async_multi_query_using_index_with_explain(buckets, query_definitions)
                if "query" in operation_map:
                    if "n1ql" in nodes_out and phase == "in_between":
                        tasks = []
                    else:
                        tasks += self.async_multi_query_using_index(buckets, query_definitions, expected_results,
                                                                    scan_consistency=scan_consistency,
                                                                    scan_vectors=scan_vectors)
                if "drop_index" in operation_map:
                    pre_cc = False
                    index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)[0]
                    cb_version = RestConnection(index_node).get_nodes_version()
                    if '7.0' not in cb_version:
                        pre_cc = True
                    if "index" in nodes_out or "n1ql" in nodes_out:
                        if phase == "in_between":
                            tasks = []
                    else:
                        tasks += self.async_multi_drop_index(self.buckets, query_definitions, pre_cc=pre_cc)
            except Exception as ex:
                log.info(ex)
                raise
        return tasks

    def run_full_table_scan_using_rest(self, bucket, query_definition, verify_result=False):
        expected_result = []
        actual_result = []
        full_scan_query = f"SELECT * FROM {bucket.name}"
        if query_definition.index_where_clause:
            full_scan_query += f" WHERE {query_definition.index_where_clause}"
        self.gen_results.query = full_scan_query
        temp = self.gen_results.generate_expected_result(print_expected_result=False)
        for item in temp:
            expected_result.append(list(item.values()))
        if self.scan_consistency == "request_plus":
            body = {"stale": "False"}
        else:
            body = {"stale": "ok"}
        content = self.rest.full_table_scan_gsi_index_with_rest(self.index_id_map[bucket][query_definition], body)
        if verify_result:
            doc_id_list = []
            for item in content:
                if item["docid"] not in doc_id_list:
                    for doc in self.full_docs_list:
                        if doc["_id"] == item["docid"]:
                            actual_result.append([doc])
                            doc_id_list.append(item["docid"])
            diffs = DeepDiff(actual_result, expected_result, ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)

            #self.assertEqual(len(sorted(actual_result)), len(sorted(expected_result)),
            #                 "Actual Items {0} are not equal to expected Items {1}".
            #                 format(len(sorted(actual_result)), len(sorted(expected_result))))
            #msg = "The number of rows match but the results mismatch, please check"
            #if sorted(actual_result) != sorted(expected_result):
            #    raise Exception(msg)

    def run_lookup_gsi_index_with_rest(self, bucket, query_definition):
        pass

    def run_range_scan_with_rest(self, bucket, query_definition):
        pass

    def gen_scan_vector(self, use_percentage=1.0, use_random=False):
        servers = self.get_kv_nodes(servers=self.servers[:self.nodes_init])
        sequence_bucket_map = self.get_vbucket_seqnos(servers, self.buckets)
        scan_vectors = {}
        if use_percentage == 1.0:
            for bucket in self.buckets:
                scan_vector = []
                self.log.info("analyzing for bucket {0}".format(bucket.name))
                map = sequence_bucket_map[bucket.name]
                for i in range(1024):
                    key = "vb_" + str(i)
                    value = [int(map[key]["abs_high_seqno"]), map[key]["uuid"]]
                    scan_vector.append(value)
                scan_vectors[bucket.name] = scan_vector
        else:
            for bucket in self.buckets:
                scan_vector = {}
                total = int(self.vbuckets * use_percentage)
                vbuckets_number_list = list(range(0, total))
                if use_random:
                    vbuckets_number_list = random.sample(range(0, self.vbuckets), total)
                self.log.info("analyzing for bucket {0}".format(bucket.name))
                map = sequence_bucket_map[bucket.name]
                for key in list(map.keys()):
                    vb = int(key.split("vb_")[1])
                    if vb in vbuckets_number_list:
                        value = [int(map[key]["abs_high_seqno"]), map[key]["uuid"]]
                        scan_vector[str(vb)] = value
                scan_vectors[bucket.name] = scan_vector
        return scan_vectors

    def check_missing_and_extra(self, actual, expected):
        missing = []
        extra = []
        for item in actual:
            if not (item in expected):
                extra.append(item)
        for item in expected:
            if not (item in actual):
                missing.append(item)
        return missing, extra

    def _verify_results(self, actual_result, expected_result, missing_count=1, extra_count=1):
        actual_result = self._gen_dict(actual_result)
        expected_result = self._gen_dict(expected_result)
        if len(actual_result) != len(expected_result):
            missing, extra = self.check_missing_and_extra(actual_result, expected_result)
            self.log.error("Missing items: %s.\n Extra items: %s" % (missing[:missing_count], extra[:extra_count]))
            self.fail("Results are incorrect.Actual num %s. Expected num: %s.\n" % (
                len(actual_result), len(expected_result)))
        if self.max_verify is not None:
            actual_result = actual_result[:self.max_verify]
            expected_result = expected_result[:self.max_verify]

        msg = "Results are incorrect.\n Actual first and last 100:  %s.\n ... \n %s" + \
              "Expected first and last 100: %s.\n  ... \n %s"
        self.assertTrue(actual_result == expected_result,
                        msg % (actual_result[:100], actual_result[-100:],
                               expected_result[:100], expected_result[-100:]))

    def verify_index_absence(self, query_definitions, buckets):
        server = self.get_nodes_from_services_map(service_type="n1ql")
        for bucket in buckets:
            for query_definition in query_definitions:
                check = self.n1ql_helper._is_index_in_list(bucket.name, query_definition.index_name, server=server)
                self.assertFalse(check, " {0} was not absent as expected".format(query_definition.index_name))

    def _gen_dict(self, result):
        result_set = []
        if result != None and len(result) > 0:
            for val in result:
                for key in list(val.keys()):
                    result_set.append(val[key])
        return result_set

    def _verify_index_map(self):
        if not self.verify_using_index_status:
            return
        index_map = self.get_index_map()
        index_bucket_map = self.n1ql_helper.gen_index_map(self.n1ql_node)
        msg = "difference in index map found, expected {0} \n actual {1}".format(index_bucket_map, index_map)
        self.assertTrue(len(list(index_map.keys())) == len(self.buckets),
                        "numer of buckets mismatch :: " + msg)
        for bucket in self.buckets:
            self.assertTrue((bucket.name in list(index_map.keys())),
                            " bucket name not present in index map {0}".format(index_map))
        for bucket_name in list(index_bucket_map.keys()):
            self.assertTrue(len(list(index_bucket_map[bucket_name].keys())) == len(list(index_map[bucket_name].keys())),
                            "number of indexes mismatch ::" + msg)
            for index_name in list(index_bucket_map[bucket_name].keys()):
                msg1 = "index_name {0} not found in {1}".format(index_name, list(index_map[bucket_name].keys()))
                self.assertTrue(index_name in list(index_map[bucket_name].keys()), msg1 + " :: " + msg)

    def _verify_primary_index_count(self):
        bucket_map = self.get_buckets_itemCount()
        count = 0
        while not self._verify_items_count() and count < 15:
            self.log.info("All Items Yet to be Indexed...")
            self.sleep(5)
            count += 1
        self.assertTrue(self._verify_items_count(), "All Items didn't get Indexed...")
        self.log.info("All the documents are indexed...")
        # self.sleep(10)
        index_bucket_map = self.n1ql_helper.get_index_count_using_primary_index(self.buckets, self.n1ql_node)
        self.log.info(bucket_map)
        self.log.info(index_bucket_map)
        for bucket_name in list(bucket_map.keys()):
            actual_item_count = index_bucket_map[bucket_name]
            expected_item_count = bucket_map[bucket_name]
            self.assertTrue(str(actual_item_count) == str(expected_item_count),
                            "Bucket {0}, mismatch in item count for index :{1} : expected {2} != actual {3} ".format
                            (bucket_name, "primary", expected_item_count, actual_item_count))

    def _verify_items_count(self):
        """
        Compares Items indexed count is sample
        as items in the bucket.
        """
        index_map = self.get_index_stats()
        for bucket_name in list(index_map.keys()):
            self.log.info("Bucket: {0}".format(bucket_name))
            for index_name, index_val in index_map[bucket_name].items():
                self.log.info("Index: {0}".format(index_name))
                self.log.info("number of docs pending: {0}".format(index_val["num_docs_pending"]))
                self.log.info("number of docs queued: {0}".format(index_val["num_docs_queued"]))
                if index_val["num_docs_pending"] and index_val["num_docs_queued"]:
                    return False
        return True

    def _verify_items_count_collections(self, index_node):
        """
        Compares Items indexed count is sample
        as items in the bucket.
        """
        index_rest = RestConnection(index_node)
        index_map = index_rest.get_index_stats_collections()
        for keyspace in list(index_map.keys()):
            self.log.info("Keyspace: {0}".format(keyspace))
            for index_name, index_val in index_map[keyspace].items():
                if index_val["index_state"] != 0:
                    self.log.info("Index: {0}".format(index_name))
                    self.log.info("number of docs pending: {0}".format(index_val["num_docs_pending"]))
                    self.log.info("number of docs queued: {0}".format(index_val["num_docs_queued"]))
                    if index_val["num_docs_pending"] or index_val["num_docs_queued"]:
                        return False
        return True

    def get_persistent_snapshot_count(self, index_node, index_name):
        """
        Returns the number of persistent snapshots for a MOI index for a specific node
        """
        index_rest = RestConnection(index_node)
        index_map = index_rest.get_index_stats_collections()
        for keyspace in list(index_map.keys()):
            self.log.info("Keyspace: {0}".format(keyspace))
            for idx_name, idx_val in index_map[keyspace].items():
                if idx_name == index_name:
                    return idx_val["num_snapshots"]


    def _get_items_count_collections(self, index_node, keyspace, index_name):
        """
        Compares Items indexed count is sample
        as items in the bucket.
        """
        try:
            index_rest = RestConnection(index_node)
            index_map = index_rest.get_index_stats_collections()
            item_count = index_map[keyspace][index_name]["items_count"]
        except Exception as e:
            self.log.info(e)
            item_count = -1
        return item_count

    def _get_index_map(self, index_node):
        """
        Compares Items indexed count is sample
        as items in the bucket.
        """
        try:
            index_rest = RestConnection(index_node)
            index_map = index_rest.get_index_stats_collections()
        except Exception as e:
            self.log.info(e)
            index_map = None
        return index_map

    def verify_type_fields(self, num_hotels=0, num_airports=0, num_airlines=0, expected_failure=False):
        retries = 3
        query = "select * from `{0}` where type = {1} order by meta().id"
        hotel_query = query.format(self.bucket_name, "'hotel'")
        hotels_validated = False
        airlines_validated = False
        airports_validated = False
        for i in range(0, retries):
            hotel_results = self.n1ql_helper.run_cbq_query(query=hotel_query, server=self.n1ql_node)
            if hotel_results['metrics']['resultCount'] == num_hotels:
                hotels_validated = True
                break
            else:
                time.sleep(1)
        self.log.info("Actual results: {0}".format(str(hotel_results['metrics']['resultCount'])))
        self.assertTrue(hotels_validated)

        airline_query = query.format(self.bucket_name, "'airline'")
        for i in range(0, retries):
            airline_results = self.n1ql_helper.run_cbq_query(query=airline_query, server=self.n1ql_node)
            if airline_results['metrics']['resultCount'] == num_airlines:
                airlines_validated = True
                break
            else:
                time.sleep(1)
        self.log.info("Actual results: {0}".format(str(airline_results['metrics']['resultCount'])))
        self.assertTrue(airlines_validated)

        airport_query = query.format(self.bucket_name, "'airport'")
        for i in range(0, retries):
            airport_results = self.n1ql_helper.run_cbq_query(query=airport_query, server=self.n1ql_node)
            if airport_results['metrics']['resultCount'] == num_airports:
                airports_validated = True
                break
            else:
                time.sleep(1)
        self.log.info("Actual results: {0}".format(str(airport_results['metrics']['resultCount'])))
        self.assertTrue(airports_validated)

    def _verify_bucket_count_with_index_count(self, query_definitions=None, buckets=None):
        """
        :param bucket:
        :param index:
        :return:
        """
        count = 0
        if not query_definitions:
            query_definitions = self.query_definitions
        if not buckets:
            buckets = self.buckets
        while not self._verify_items_count() and count < 15:
            self.log.info("All Items Yet to be Indexed...")
            self.sleep(10)
            count += 1
        if not self._verify_items_count():
            raise Exception("All Items didn't get Indexed...")
        bucket_map = self.get_buckets_itemCount()
        for bucket in buckets:
            bucket_count = bucket_map[bucket.name]
            for query in query_definitions:
                index_count = self.n1ql_helper.get_index_count_using_index(bucket,
                                                                           query.index_name, self.n1ql_node)
                self.assertTrue(int(index_count) == int(bucket_count),
                                "Bucket {0}, mismatch in item count for index :{1} : expected {2} != actual {3} ".format
                                (bucket.name, query.index_name, bucket_count, index_count))
        self.log.info("Items Indexed Verified with bucket count...")

    def verify_compression_stat(self, query_definitions):
        all_index_comp_stat_verified = False
        is_any_stat_negative = False
        indexer_nodes = self.get_nodes_from_services_map(service_type="index",
                                                         get_all_nodes=True)
        for node in indexer_nodes:
            indexer_rest = RestConnection(node)
            content = indexer_rest.get_index_storage_stats()
            for index in list(content.values()):
                for index_name, stats in index.items():
                    comp_stat_verified = False
                    is_stat_negative = False
                    for query in query_definitions:
                        if index_name == query.index_name:
                            self.log.info(f'num_rec_compressed for index {index_name} found to be '
                                          f'{stats["MainStore"]["num_rec_compressed"]} and num_rec_swapin found to be '
                                          f'{stats["MainStore"]["num_rec_swapin"]}')
                            if stats["MainStore"]["num_rec_compressed"] > 0:
                                comp_stat_verified = True
                            if stats["MainStore"]["num_rec_compressed"] < 0:
                                is_stat_negative = True
                    all_index_comp_stat_verified = all_index_comp_stat_verified or comp_stat_verified
                    is_any_stat_negative = is_any_stat_negative and is_stat_negative

            if is_any_stat_negative:
                all_index_comp_stat_verified = False
        return all_index_comp_stat_verified

    def wait_for_mutation_processing(self, index_nodes):
        count = 0
        all_nodes_mutation_processed = True
        for index_node in index_nodes:
            while not self._verify_items_count_collections(index_node) and count < 90:
                self.log.info("All Items Yet to be Indexed...")
                self.sleep(10)
                count += 1
            if not self._verify_items_count_collections(index_node):
                self.log.error("All Items didn't get Indexed...")
                mutation_processed = False
            else:
                self.log.info("All Items are indexed")
                mutation_processed = True

            all_nodes_mutation_processed = all_nodes_mutation_processed and mutation_processed
        return all_nodes_mutation_processed

    def check_if_shard_exists(self, shard_dir, index_node):
        data_dir = RestConnection(index_node).get_index_path()
        shell = RemoteMachineShellConnection(index_node)
        shard_abs_dirpath = f'{data_dir}/@2i/shards/{shard_dir}'
        return shell.check_directory_exists(shard_abs_dirpath)

    def check_if_index_recovered(self, index_list):
        indexes_not_recovered = []
        for index in index_list:
            recovered, index_count, collection_itemcount = self._verify_collection_count_with_index_count(index["query_def"])
            if not recovered:
                error_map = {"index_name": index["name"], "index_count": index_count, "bucket_count": collection_itemcount}
                indexes_not_recovered.append(error_map)
        if not indexes_not_recovered:
            self.log.info("All indexes recovered")

    def _verify_collection_count_with_index_count(self, query_def):
        recovered = False
        index_name = query_def.index_name
        keyspace = query_def.keyspace
        n1ql_node = self.get_nodes_from_services_map(service_type="n1ql")
        collection_itemcount = self.n1ql_helper.get_collection_itemCount(keyspace, n1ql_node)
        index_count = self.n1ql_helper.get_index_count_using_index_collection(keyspace, index_name, n1ql_node)
        if int(index_count) == int(collection_itemcount):
            recovered = True
            self.log.info(f'Collection item count : {collection_itemcount}, '
                          f'item count for index {index_name}: {index_count}')
        else:
            self.log.error(f'Collection item count : {collection_itemcount}, '
                           f'item count for index {index_name}: {index_count}')
        return recovered, index_count, collection_itemcount

    def _verify_index_count_less_than_collection(self, index_node, query_def):
        recovered = False
        index_name = query_def.index_name
        keyspace = query_def.keyspace
        n1ql_node = self.get_nodes_from_services_map(service_type="n1ql")
        collection_itemcount = self.n1ql_helper.get_collection_itemCount(keyspace, n1ql_node)

        index_count = self._get_items_count_collections(index_node, keyspace, index_name)
        if int(index_count) != -1 and int(index_count) < int(collection_itemcount):
            recovered = True
        elif index_count == -1 or not int(index_count):
            recovered = -1
        self.log.info(f'Collection item count : {collection_itemcount}, '
                      f'item count for index {index_name}: {index_count}')

        return recovered, index_count, collection_itemcount

    def check_index_recovered_snapshot(self, index_list, index_node, max_time=60):
        indexes_not_recovered = []
        stat_time = time.time()
        end_time = stat_time + max_time
        list_of_index = copy.deepcopy(index_list)
        while time.time() < end_time:
            if not list_of_index:
                break

            for index in list_of_index:
                snap_recovered, index_count, collection_itemcount = \
                    self._verify_index_count_less_than_collection(index_node, index["query_def"])
                if snap_recovered == -1:
                    break
                if not snap_recovered:
                    error_map = {"index_name": index["name"], "index_count": index_count, "bucket_count": collection_itemcount}
                    indexes_not_recovered.append(error_map)
                else:
                    list_of_index.remove(index)
        if indexes_not_recovered:
            self.log.info(f'Indexes {indexes_not_recovered} not recovered')
            return False
        else:
            return True

    def check_if_index_count_zero(self, index_list, index_node):
        index_count_zero = True
        index_map = None
        while not index_map:
            index_map = self._get_index_map(index_node)
        for index in index_list:
            index_count = index_map[index["query_def"].keyspace][index["query_def"].index_name]["items_count"]
            self.log.info("Index count on index {0} : {1}".format(index["query_def"].index_name, index_count))
            if index_count:
                index_count_zero = False

        return index_count_zero

    def _kill_all_processes_index(self, server):
        shell = RemoteMachineShellConnection(server)
        shell.execute_command("pkill indexer")

    def kill_loader_process(self):
        self.log.info("killing java doc loader process")
        os.system(f'pgrep -f .*{self.key_prefix}*')
        os.system(f'pkill -f .*{self.key_prefix}*')

    def _kill_all_processes_index_with_sleep(self, server, sleep_time, timeout=1200):
        self.log.info(threading.currentThread().getName() + " Started")
        endtime = time.time() + timeout
        while self.kill_index and time.time() < endtime:
            shell = RemoteMachineShellConnection(server)
            shell.execute_command("pkill indexer")
            self.sleep(sleep_time)
        self.log.info(threading.currentThread().getName() + " Completed")

    def _check_all_bucket_items_indexed(self, query_definitions=None, buckets=None):
        """
        :param bucket:
        :param index:
        :return:
        """
        count = 0
        while not self._verify_items_count() and count < 15:
            self.log.info("All Items Yet to be Indexed...")
            self.sleep(10)
            count += 1
        self.assertTrue(self._verify_items_count(), "All Items didn't get Indexed...")

    def _create_operation_map(self):
        map_initial = {"create_index": False, "query_ops": False, "query_explain_ops": False, "drop_index": False}
        map_before = {"create_index": False, "query_ops": False, "query_explain_ops": False, "drop_index": False}
        map_in_between = {"create_index": False, "query_ops": False, "query_explain_ops": False, "drop_index": False}
        map_after = {"create_index": False, "query_ops": False, "query_explain_ops": False, "drop_index": False}
        initial = self.input.param("initial", "")
        for op_type in initial.split(":"):
            if op_type != '':
                map_initial[op_type] = True
        before = self.input.param("before", "")
        for op_type in before.split(":"):
            if op_type != '':
                map_before[op_type] = True
        in_between = self.input.param("in_between", "")
        for op_type in in_between.split(":"):
            if op_type != '':
                map_in_between[op_type] = True
        after = self.input.param("after", "")
        for op_type in after.split(":"):
            if op_type != '':
                map_after[op_type] = True
        return {"initial": map_initial, "before": map_before, "in_between": map_in_between, "after": map_after}

    def generate_operation_map(self, phase):
        operation_map = []
        self.verify_query_result = False
        self.verify_explain_result = False
        ops = self.input.param(phase, "")
        for type in ops.split("-"):
            for op_type in type.split(":"):
                if "verify_query_result" in op_type:
                    self.verify_query_result = True
                    continue
                if "verify_explain_result" in op_type:
                    self.verify_explain_result = True
                    continue
                if op_type != '':
                    operation_map.append(op_type)
        return operation_map

    def _query_explain_in_async(self):
        tasks = self.async_run_multi_operations(buckets=self.buckets,
                                                query_definitions=self.query_definitions,
                                                create_index=False, drop_index=False,
                                                query_with_explain=self.run_query_with_explain,
                                                query=False, scan_consistency=self.scan_consistency)
        for task in tasks:
            task.result()
        tasks = self.async_run_multi_operations(buckets=self.buckets,
                                                query_definitions=self.query_definitions,
                                                create_index=False, drop_index=False,
                                                query_with_explain=False, query=self.run_query,
                                                scan_consistency=self.scan_consistency)
        for task in tasks:
            task.result()

    def _set_query_explain_flags(self, phase):
        if ("query_ops" in list(self.ops_map[phase].keys())) and self.ops_map[phase]["query_ops"]:
            self.ops_map[phase]["query_explain_ops"] = True
        if ("do_not_verify_query_result" in list(self.ops_map[phase].keys())) and self.ops_map[phase][
            "do_not_verify_query_result"]:
            self.verify_query_result = False
            self.ops_map[phase]["query_explain_ops"] = False
        if ("do_not_verify_explain_result" in list(self.ops_map[phase].keys())) and self.ops_map[phase][
            "do_not_verify_explain_result"]:
            self.verify_explain_result = False
            self.ops_map[phase]["query_explain_ops"] = False
        self.log.info(self.ops_map)

    def fail_if_no_buckets(self):
        buckets = False
        for a_bucket in self.buckets:
            buckets = True
        if not buckets:
            self.fail('FAIL: This test requires buckets')

    def block_incoming_network_from_node(self, node1, node2):
        shell = RemoteMachineShellConnection(node1)
        self.log.info("Adding {0} into iptables rules on {1}".format(
            node1.ip, node2.ip))
        command = "iptables -A INPUT -s {0} -j REJECT".format(node2.ip)
        shell.execute_command(command)

    def resume_blocked_incoming_network_from_node(self, node1, node2):
        shell = RemoteMachineShellConnection(node1)
        self.log.info("Removing {0} from iptables rules on {1}".format(
            node1.ip, node2.ip))
        command = "iptables -D INPUT -s {0} -j REJECT".format(node2.ip)
        shell.execute_command(command)

    def set_indexer_logLevel(self, loglevel="info"):
        """
        :param loglevel:
        Possible Values
            -- info
            -- debug
            -- warn
            -- verbose
            -- Silent
            -- Fatal
            -- Error
            -- Timing
            -- Trace
        """
        self.log.info("Setting indexer log level to {0}".format(loglevel))
        server = self.get_nodes_from_services_map(service_type="index")
        rest = RestConnection(server)
        status = rest.set_indexer_params("logLevel", loglevel)

    def wait_until_cluster_is_healthy(self):
        master_node = self.master
        if self.targetMaster:
            if len(self.servers) > 1:
                master_node = self.servers[1]
        rest = RestConnection(master_node)
        is_cluster_healthy = False
        count = 0
        while not is_cluster_healthy and count < 10:
            count += 1
            cluster_nodes = rest.node_statuses()
            for node in cluster_nodes:
                if node.status != "healthy":
                    is_cluster_healthy = False
                    log.info("Node {0} is in {1} state...".format(node.ip,
                                                                  node.status))
                    self.sleep(5)
                    break
                else:
                    is_cluster_healthy = True
        return is_cluster_healthy

    def get_server_indexes_count(self, index_nodes):
        server_index_count = {}
        for server in index_nodes:
            rest = RestConnection(server)
            server_index_count.update(rest.get_indexes_count())

        return server_index_count

    def wait_until_indexes_online(self, timeout=600, defer_build=False, check_paused_index=False, schedule_index=False):
        rest = RestConnection(self.master)
        init_time = time.time()
        check = False
        timed_out = False
        while not check:
            index_status = rest.get_index_status()
            next_time = time.time()
            if index_status == {}:
                self.log.error("No indexes are present, this check does not apply!")
                break
            else:
                for index_info in index_status.values():
                    for index_state in index_info.values():
                        if defer_build:
                            if index_state["status"] == "Ready" or index_state["status"] == "Created":
                                check = True
                            else:
                                check = False
                                time.sleep(1)
                                break
                        elif check_paused_index:
                            if index_state["status"] == "Paused" or index_state["status"] == "Ready":
                                check = True
                            else:
                                check = False
                                time.sleep(1)
                                break
                        elif schedule_index:
                            if index_state["status"] == "Ready" or index_state["status"] == "Scheduled for Creation":
                                check = True
                            else:
                                check = False
                                time.sleep(1)
                                break
                        else:
                            if index_state["status"] == "Ready":
                                check = True
                            else:
                                check = False
                                time.sleep(1)
                                break
                    if next_time - init_time > timeout:
                        timed_out = True
                        check = next_time - init_time > timeout
        if timed_out:
            check = False
        return check

    def check_if_indexes_in_dgm(self):
        def check_if_indexes_in_dgm_node(node, indexes_in_dgm):
            indexer_rest = RestConnection(node)
            node_in_dgm = False
            content = indexer_rest.get_index_storage_stats()
            try:
                tot_indexes = len(list(content.values())[0].values())
                index_in_dgm = 0
                for index in list(content.values()):
                    for key, stats in index.items():
                        stats = dict(stats)
                        rr = stats["MainStore"]["resident_ratio"]
                        if rr != 0 and rr < 1.00:
                            index_in_dgm += 1
                            if f'{node}_{index_in_dgm}' not in indexes_in_dgm:
                                indexes_in_dgm.append(f'{node}_{index_in_dgm}')
                                self.log.info("Index : {} , resident_ratio : {}".format(key, rr))
                if index_in_dgm > (tot_indexes/3):
                    node_in_dgm = True
            except IndexError as e:
                node_in_dgm = True
            return node_in_dgm

        indexes_in_dgm = []
        all_nodes_in_dgm = False
        endtime = time.time() + self.dgm_check_timeout
        while not all_nodes_in_dgm and time.time() < endtime:
            all_nodes_in_dgm = True
            for node in self.index_nodes:
                all_nodes_in_dgm = all_nodes_in_dgm and check_if_indexes_in_dgm_node(node, indexes_in_dgm)

        return all_nodes_in_dgm

    def check_if_index_created(self, index, defer_build=False):
        index_created = False
        status = None
        rest = RestConnection(self.master)
        index_status = rest.get_index_status()
        for index_info in index_status.values():
            for k, v in index_info.items():
                if k == index:
                    status = v["status"]
                    if status == "Ready" or status == "Building" or (defer_build and status == "Created"):
                        index_created = True

        return index_created, status

    def get_indexes_not_online(self):
        rest = RestConnection(self.master)
        index_list = []
        index_status = rest.get_index_status()
        for index_info in index_status.values():
            for k, v in index_info.items():
                if v["status"] != "Ready":
                    index_list.append(k)
        return index_list

    def wait_until_specific_index_online(self, index_name='', timeout=600, defer_build=False):
        rest = RestConnection(self.master)
        init_time = time.time()
        check = False
        timed_out = False
        while not check:
            index_status = rest.get_index_status()
            next_time = init_time
            for index_info in list(index_status.values()):
                for idx_name in list(index_info.keys()):
                    if idx_name == index_name:
                        for index_state in list(index_info.values()):
                            if defer_build:
                                if index_state["status"] == "Created":
                                    check = True
                                else:
                                    check = False
                                    time.sleep(1)
                                    next_time = time.time()
                                    break
                            else:
                                if index_state["status"] == "Ready":
                                    check = True
                                else:
                                    check = False
                                    time.sleep(1)
                                    next_time = time.time()
                                    break
            if next_time - init_time > timeout:
                timed_out = True
                check = next_time - init_time > timeout
        if timed_out:
            check = False
        return check

    def verify_index_in_index_map(self, index_name='', timeout=600):
        rest = RestConnection(self.master)
        init_time = time.time()
        check = False
        seen = False
        next_time = init_time
        timed_out = False
        while not check and not timed_out:
            index_status = rest.get_index_status()
            log.info(index_status)
            for index_info in list(index_status.values()):
                if seen:
                    break
                for idx_name in list(index_info.keys()):
                    if idx_name == index_name:
                        seen = True
                        break
                    else:
                        check = False
                        time.sleep(1)
                        next_time = time.time()
            if seen:
                check = True
            if next_time - init_time > timeout:
                timed_out = True
                check = False
        return check

    def get_dgm_for_plasma(self, indexer_nodes=None, memory_quota=256):
        """
        Internal Method to create OOM scenario
        :return:
        """

        def validate_disk_writes(indexer_nodes=None):
            if not indexer_nodes:
                indexer_nodes = self.get_nodes_from_services_map(
                    service_type="index", get_all_nodes=True)
            for node in indexer_nodes:
                indexer_rest = RestConnection(node)
                content = indexer_rest.get_index_storage_stats()
                for index in list(content.values()):
                    for stats in list(index.values()):
                        if stats["MainStore"]["resident_ratio"] >= 1.00:
                            return False
            return True

        def kv_mutations(self, docs=1):
            if not docs:
                docs = self.docs_per_day
            gens_load = self.generate_docs(docs)
            self.full_docs_list = self.generate_full_docs_list(gens_load)
            self.gen_results = TuqGenerators(self.log, self.full_docs_list)
            self.load(gens_load, buckets=self.buckets, flag=self.item_flag,
                      verify_data=False, batch_size=self.batch_size)

        if self.gsi_type != "plasma":
            return
        if not self.plasma_dgm:
            return
        log.info("Trying to get all indexes in DGM...")
        log.info("Setting indexer memory quota to {0} MB...".format(memory_quota))
        node = self.get_nodes_from_services_map(service_type="index")
        rest = RestConnection(node)
        rest.set_service_memoryQuota(service='indexMemoryQuota', memoryQuota=memory_quota)
        cnt = 0
        docs = 50 + self.docs_per_day
        while cnt < 100:
            if validate_disk_writes(indexer_nodes):
                log.info("========== DGM is achieved ==========")
                return True
            kv_mutations(self, docs)
            self.sleep(30)
            cnt += 1
            docs += 20
        return False

    def reboot_node(self, node):
        self.log.info("Rebooting node '{0}'....".format(node.ip))
        shell = RemoteMachineShellConnection(node)
        if shell.extract_remote_info().type.lower() == 'windows':
            o, r = shell.execute_command("shutdown -r -f -t 0")
        elif shell.extract_remote_info().type.lower() == 'linux':
            o, r = shell.execute_command("reboot")
        shell.log_command_output(o, r)
        shell.disconnect()
        # wait for restart and warmup on all node
        self.sleep(self.wait_timeout * 3)
        # disable firewall on these nodes
        self.stop_firewall_on_node(node)
        # wait till node is ready after warmup
        ClusterOperationHelper.wait_for_ns_servers_or_assert([node], self,
                                                             wait_if_warmup=True)

    def prepare_collection_for_indexing(self, num_scopes=1, num_collections=1, num_of_docs_per_collection=1000,
                                        indexes_before_load=False, json_template="Person", batch_size=10**4,
                                        bucket_name=None):
        if not bucket_name:
            bucket_name = self.test_bucket
        self.namespaces = []
        pre_load_idx_pri = None
        pre_load_idx_gsi = None
        self.collection_rest.create_scope_collection_count(scope_num=num_scopes, collection_num=num_collections,
                                                           scope_prefix=self.scope_prefix,
                                                           collection_prefix=self.collection_prefix,
                                                           bucket=bucket_name)
        scopes = [f'{self.scope_prefix}_{scope_num + 1}' for scope_num in range(num_scopes)]
        self.sleep(10, "Allowing time after collection creation")

        if num_of_docs_per_collection > 0:
            for s_item in scopes:
                collections = [f'{self.collection_prefix}_{coll_num + 1}' for coll_num in range(num_collections)]
                for c_item in collections:
                    self.namespaces.append(f'default:{bucket_name}.{s_item}.{c_item}')
                    if indexes_before_load:
                        pre_load_idx_pri = QueryDefinition(index_name='pre_load_idx_pri')
                        pre_load_idx_gsi = QueryDefinition(index_name='pre_load_idx_gsi', index_fields=['firstName'])
                        query = pre_load_idx_pri.generate_primary_index_create_query(namespace=self.namespaces[0])
                        self.run_cbq_query(query=query)
                        query = pre_load_idx_gsi.generate_index_create_query(namespace=self.namespaces[0])
                        self.run_cbq_query(query=query)
                    self.gen_create = SDKDataLoader(num_ops=num_of_docs_per_collection, percent_create=100,
                                                    percent_update=0, percent_delete=0, scope=s_item,
                                                    collection=c_item, json_template=json_template,
                                                    output=True)
                    tasks = self.data_ops_javasdk_loader_in_batches(sdk_data_loader=self.gen_create,
                                                                    batch_size=batch_size)
                    for task in tasks:
                        task.result()
        return pre_load_idx_pri, pre_load_idx_gsi

    def delete_bucket_scope_collection(self, delete_item, server, bucket, scope='test_scope_1',
                                       collection='test_collection_1', timeout=None):
        if not server:
            server = self.servers[0]
        if not bucket:
            bucket = self.test_bucket
        if not delete_item:
            delete_item = self.item_to_delete
        if delete_item == 'bucket':
            self.log.info(f"Deleting bucket: {bucket}")
            return self.cluster.bucket_delete(server=server, bucket=bucket, timeout=timeout)
        elif delete_item == 'scope':
            self.log.info(f"Deleting Scope: {scope}")
            return self.collection_rest.delete_scope(bucket=bucket, scope=scope)
        elif delete_item == 'collection':
            self.log.info(f"Deleting Collection: {collection}")
            return self.collection_rest.delete_collection(bucket=bucket, scope=scope, collection=collection)

    def get_indexer_mem_quota(self, indexer_node=None):
        """
        Get Indexer memory Quota
        :param indexer_node:
        """
        if not indexer_node:
            indexer_node = self.get_nodes_from_services_map(service_type="index")
        rest = RestConnection(indexer_node)
        content = rest.cluster_status()
        return int(content['indexMemoryQuota'])

    def get_nodes_uuids(self):
        if self.use_https:
            port = '18091'
            connection_protocol = 'https'
        else:
            port = '8091'
            connection_protocol = 'http'

        api = f'{connection_protocol}://{self.master.ip}:{port}/pools/default'
        status, content, header = self.rest.urllib_request(api)
        if not status:
            raise Exception(content)
        results = json.loads(content)['nodes']
        node_ip_uuid_map = {}
        for node in results:
            ip = node['hostname'].split(':')[0]
            node_ip_uuid_map[ip] = node['nodeUUID']
        return node_ip_uuid_map

    def validate_smart_batching_during_rebalance(self, rebalance_task):
        while self.rest._rebalance_progress() == 100 and rebalance_task.state != 'CHECKING':
            progress = self.rest._rebalance_progress()
            state = rebalance_task.state
            self.log.info(f'Progress:{progress}')
            self.log.info(f'state:{state}')
            self.sleep(5)
        self.sleep(5)
        # Validating no. of parallel concurrent build
        start_time = time.time()
        while self.rest._rebalance_progress() < 100:
            indexer_metadata = self.index_rest.get_indexer_metadata()['status']
            moving_indexes_count = 0
            # self.log.info(indexer_metadata)
            for index in indexer_metadata:
                if index['status'] == 'Moving':
                    moving_indexes_count += 1
            self.log.info(f"No. of Indexes in Moving State: {moving_indexes_count}")
            if moving_indexes_count > self.transfer_batch_size:
                self.fail("No. of parallel index builds are more than 'transfer batch size'")

            curr_time = time.time()
            if curr_time - start_time > self.rebalance_timeout:
                self.fail("Rebalance got stuck or it's taking longer than expect. Please check the logs")
            self.sleep(5)

        result = rebalance_task.result()
        self.log.info(result)
        rebalance_status = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")

class ConCurIndexOps():
    def __init__(self):
        self.all_indexes_state = {"created": [], "deleted": [], "defer_build": []}
        self.errors = []
        self.test_fail = False
        self._lock_queue = threading.Lock()
        self.errors_lock_queue = threading.Lock()
        self.create_index_lock = threading.Lock()
        self.ignore_failure = False
        self.stop_create_index = False

    def update_errors(self, error_map):
        with self.errors_lock_queue:
            if not self.ignore_failure:
                self.errors.append(error_map)

    def update_ignore_failure_flag(self, flag):
        with self.errors_lock_queue:
            self.ignore_failure = flag

    def get_ignore_failure_flag(self, flag):
        with self.errors_lock_queue:
            return self.ignore_failure

    def update_stop_create_index(self, flag):
        with self.create_index_lock:
            self.stop_create_index = flag

    def get_stop_create_index(self):
        with self.create_index_lock:
            return self.stop_create_index

    def get_errors(self):
        return self.errors

    def get_delete_index_list(self):
        return self.all_indexes_state["deleted"]

    def get_create_index_list(self):
        return self.all_indexes_state["created"]

    def get_defer_index_list(self):
        return self.all_indexes_state["defer_build"]

    def all_indexes_metadata(self, index_meta=None, operation="create", defer_build=False):
        with self._lock_queue:
            if operation == "create" and defer_build:
                self.all_indexes_state["defer_build"].append(index_meta)
            elif operation == "create":
                self.all_indexes_state["created"].append(index_meta)
            elif operation == "delete" and defer_build:
                try:
                    index = self.all_indexes_state["defer_build"]. \
                        pop(random.randrange(len(self.all_indexes_state["defer_build"])))
                    self.all_indexes_state["deleted"].append(index)
                except Exception as e:
                    index = None
                return index
            elif operation == "delete":
                try:
                    index = self.all_indexes_state["created"]. \
                        pop(random.randrange(len(self.all_indexes_state["created"])))
                    self.all_indexes_state["deleted"].append(index)
                except Exception as e:
                    index = None
                return index
            elif operation == "scan":
                try:
                    index = random.choice(self.all_indexes_state["created"])
                except Exception as e:
                    index = None
                return index
            elif operation == "build":
                try:
                    index = self.all_indexes_state["defer_build"]. \
                        pop(random.randrange(len(self.all_indexes_state["defer_build"])))
                except Exception as e:
                    index = None
                return index

    def build_complete_add_to_create(self, index):
        with self._lock_queue:
            if index is not None:
                self.all_indexes_state["created"].append(index)

    def check_if_indexes_created(self, index_node, defer_build=False):
        indexes_not_created = []
        if defer_build:
            index_list = self.all_indexes_state["defer_build"]
        else:
            index_list = self.all_indexes_state["created"]
        for index in index_list:
            index_created, status = self.check_if_index_created(index_node, index["name"], defer_build)
            if not index_created:
                indexes_not_created.append({"name": index["name"], "status": status})
        return indexes_not_created

    @staticmethod
    def check_if_index_created(index_node, index, defer_build=False):
        index_created = False
        status = None
        rest = RestConnection(index_node)
        index_status = rest.get_index_status()
        for index_info in index_status.values():
            for k, v in index_info.items():
                if k == index:
                    status = v["status"]
                    if status == "Ready" or status == "Building" or (defer_build and status == "Created"):
                        index_created = True

        return index_created, status
