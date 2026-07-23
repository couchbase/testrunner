"""
replica_repair.py: These tests validate the changes introduced in 7.2.1 for replica repair

__author__ = "Hemant Rajput"
__maintainer = "Hemant Rajput"
__email__ = "Hemant.Rajput@couchbase.com"
__git_user__ = "hrajput89"
__created_on__ = 29/06/23 12:05 pm

"""
from membase.api.capella_rest_client import RestConnection as RestConnectionCapella
from membase.api.on_prem_rest_client import RestHelper
from membase.api.rest_client import RestConnection
from .base_gsi import BaseSecondaryIndexingTests


class ReplicaRepair(BaseSecondaryIndexingTests):
    def setUp(self):
        super(ReplicaRepair, self).setUp()
        self.log.info("==============  ReplicaRepair setup has started ==============")
        if self.capella_run:
            buckets = self.rest.get_buckets()
            if buckets:
                for bucket in buckets:
                    RestConnectionCapella.delete_bucket(self, bucket=bucket.name)

        else:
            self.rest.delete_all_buckets()
        self.password = self.input.membase_settings.rest_password
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        if not self.capella_run:
            self._create_server_groups()
            self.cb_version = float(self.cb_version.split('-')[0][0:3])
        self.log.info("==============  ReplicaRepair setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  ReplicaRepair tearDown has started ==============")
        self.log.info("==============  ReplicaRepair tearDown has completed ==============")

    def suite_tearDown(self):
        self.log.info("==============  ReplicaRepair tearDown has started ==============")
        super(ReplicaRepair, self).tearDown()
        self.log.info("==============  ReplicaRepair tearDown has completed ==============")

    def suite_setUp(self):
        self.log.info("==============  ReplicaRepair suite_setup has started ==============")
        self.log.info("==============  ReplicaRepair suite_setup has completed ==============")

    def test_replica_repair_with_rebalance(self):
        self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                             num_of_docs_per_collection=self.num_of_docs_per_collection,
                                             json_template='Hotel')
        query_definitions = self.gsi_util_obj.generate_hotel_data_index_definition()
        for namespace in self.namespaces:
            queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                              namespace=namespace, num_replica=self.num_index_replica)
            self.gsi_util_obj.create_gsi_indexes(create_queries=queries, database=namespace)
        self.wait_until_indexes_online()

        indexer_metadata_before_rebalance = self.index_rest.get_indexer_metadata()['status']

        # Marking 2 index nodes for excludeNode=in and check if replica repair is happening on it
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        exclude_list = index_nodes[:-1]
        for node in exclude_list:
            rest = RestConnection(node)
            index_setting = "excludeNode=in"
            rest.set_index_planner_settings(index_setting)
            value = rest.get_exclude_node_value()
            self.log.info(f"Setting planner value on {node} to {value}")
        self.sleep(30, "Adding sleep so that indexer reads the setting changed")

        try:
            node_out = index_nodes[-1]
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [node_out],
                                                     services=['index'], cluster_config=self.cluster_config)
            self.sleep(30)
            reached = RestHelper(self.rest).rebalance_reached()
            rebalance.result()
            indexer_metadata_after_repair = self.index_rest.get_indexer_metadata()['status']
            self.assertEqual(len(indexer_metadata_after_repair), len(indexer_metadata_before_rebalance))

            # As the nodes are marked for exclude no new index creation should be allowed
            create_query = f'create index idx on {self.namespaces[0]}(age) with {{"num_replica": 1}}'
            self.run_cbq_query(query=create_query)
            self.fail("This query shouldn't run as all the index nodes are marked for exclude")
        except Exception as err:
            err_msg = 'Some indexer nodes may be marked as excluded'
            if err_msg not in str(err):
                self.fail(err)
        finally:
            # resetting the nodes exclude value
            for node in exclude_list:
                rest = RestConnection(node)
                index_setting = "excludeNode="
                rest.set_index_planner_settings(index_setting)
                value = rest.get_exclude_node_value()
                self.log.info(f"Setting planner value on {node} to {value}")

    def test_replica_repair_ignore_resource_constraint_on_empty_node(self):
        """
        MB-65821: Ignore resource constraints when an empty node is added back
        for replica repair.

        Steps:
          1. Cluster with >= 2 index nodes, indexes created with replicas,
             loaded with enough data that the planner's resource estimates are
             non-trivial.
          2. Record placement via GET /indexStatus.
          3. Failover + rebalance out one index node -> its replicas go missing.
          4. Add an empty node back and rebalance with redistributeIndexes=false.
          5. Verify GET /indexStatus shows the missing replicas rebuilt on the
             new node.
        """
        def placement(index_map):
            # {(bucket, index_name_without_replica_suffix): sorted([host_ips])}.
            # Replicas are grouped under the base name because the planner may renumber
            # them across a repair; partitioned indexes are left out since a partitioned
            # instance spans several nodes and its host list shrinks on partition
            # placement, not on replica loss.
            hosts_by_index = {}
            for bucket, indexes in index_map.items():
                for index_name, index_info in indexes.items():
                    if index_info.get('partitioned', False):
                        continue
                    hosts = index_info['hosts']
                    if isinstance(hosts, str):
                        hosts = [hosts]
                    key = (bucket, index_name.split(' (replica ')[0])
                    hosts_by_index.setdefault(key, []).extend(host.split(':')[0] for host in hosts)
            return {key: sorted(hosts) for key, hosts in hosts_by_index.items()}

        # ---- 1. Load data and create indexes with replicas ----
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        self.assertGreaterEqual(len(index_nodes), 2,
                                f"This test needs at least 2 index nodes, found {len(index_nodes)}")

        # Every index must occupy ALL index nodes, i.e. num_replica == num_index_nodes - 1.
        # Otherwise the rebalance-out itself relocates the lost replicas onto the surviving
        # nodes, nothing stays under-replicated, and the add-back rebalance has no repair
        # work left to exercise.
        num_replica = len(index_nodes) - 1
        self.log.info(f"Creating indexes with num_replica={num_replica} across {len(index_nodes)} index nodes "
                      f"so removing one node leaves the indexes under-replicated")

        self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                             num_of_docs_per_collection=self.num_of_docs_per_collection,
                                             json_template='Hotel')
        query_definitions = self.gsi_util_obj.generate_hotel_data_index_definition()
        for namespace in self.namespaces:
            queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                              namespace=namespace, num_replica=num_replica)
            self.gsi_util_obj.create_gsi_indexes(create_queries=queries, database=namespace)
        self.wait_until_indexes_online()

        # ---- 2. Record placement before the topology change ----
        placement_before = placement(self.get_index_map())
        self.log.info(f"Index placement before failover: {placement_before}")

        node_out = index_nodes[-1]
        indexes_on_node_out = [key for key, hosts in placement_before.items() if node_out.ip in hosts]
        self.assertTrue(indexes_on_node_out,
                        f"Expected some index replicas on {node_out.ip} before failover")
        self.log.info(f"Indexes with a replica on {node_out.ip} before failover: {indexes_on_node_out}")

        # ---- 3. Failover + rebalance out one index node ----
        failover_task = self.cluster.async_failover(self.servers[:self.nodes_init],
                                                    failover_nodes=[node_out], graceful=self.graceful)
        failover_task.result()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [node_out],
                                                 services=['index'], cluster_config=self.cluster_config)
        rebalance.result()
        self.assertTrue(RestHelper(self.rest).rebalance_reached(), "Rebalance-out did not complete")

        # Replicas that lived on the removed node are now missing
        placement_after_out = placement(self.get_index_map())
        self.log.info(f"Index placement after rebalance-out: {placement_after_out}")
        missing_replicas = [key for key in indexes_on_node_out
                            if len(placement_after_out.get(key, [])) < len(placement_before[key])]
        self.assertTrue(missing_replicas,
                        f"No index was left under-replicated after failover/rebalance-out of {node_out.ip} -- "
                        f"the rebalance-out relocated every replica onto the surviving nodes, so there is no "
                        f"replica repair for the add-back rebalance to perform. Placement after rebalance-out: "
                        f"{placement_after_out}")
        self.log.info(f"Under-replicated indexes after rebalance-out: {missing_replicas}")

        # ---- 4. Add the empty node back with redistributeIndexes=false ----
        # This makes the rebalance a pure replica-repair plan: numDeletedNode=0,
        # numNewNode=1, and only optional (missing replica) indexes are eligible.
        self.disable_redistribute_indexes()
        self.sleep(30, "Waiting for indexer to pick up redistribute_indexes=false")
        redistribute = self.index_rest.get_index_settings()["indexer.settings.rebalance.redistribute_indexes"]
        self.assertFalse(redistribute, "redistributeIndexes must be false for the replica-repair path")

        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [node_out], [],
                                                 services=['index'], cluster_config=self.cluster_config)
        rebalance.result()
        self.assertTrue(RestHelper(self.rest).rebalance_reached(), "Rebalance-in of empty node did not complete")
        self.wait_until_indexes_online()

        # ---- 5. Verify the missing replicas were rebuilt on the new node ----
        placement_after_repair = placement(self.get_index_map())
        self.log.info(f"Index placement after replica repair: {placement_after_repair}")

        not_repaired, not_on_new_node = [], []
        for key in missing_replicas:
            hosts = placement_after_repair.get(key, [])
            if len(hosts) < len(placement_before[key]):
                not_repaired.append((key, hosts))
            elif node_out.ip not in hosts:
                not_on_new_node.append((key, hosts))

        self.assertFalse(not_repaired,
                         f"Replica repair did not restore all replicas after adding the empty node "
                         f"back: {not_repaired}")
        self.assertFalse(not_on_new_node,
                         f"Resource constraint was NOT ignored: replicas were not placed on the incoming "
                         f"empty node {node_out.ip}: {not_on_new_node}")

        self.assertEqual(sorted(placement_after_repair.keys()), sorted(placement_before.keys()),
                         "Index instance set changed across the failover/repair cycle")
