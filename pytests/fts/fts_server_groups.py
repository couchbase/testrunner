# coding=utf-8

from .fts_base import FTSBaseTest
from lib.membase.api.rest_client import RestConnection
from lib.remote.remote_util import RemoteMachineShellConnection

class FTSServerGroups(FTSBaseTest):

    def setUp(self):
        super(FTSServerGroups, self).setUp()
        self.rest = RestConnection(self._cb_cluster.get_master_node())
        self.default_group_name = "Group 1"
        self.fts_query = {"match": "emp", "field": "type"}

        self._cleanup_server_groups()

    def tearDown(self):
        super(FTSServerGroups, self).tearDown()

    def test_nodes_ejection(self):
        eject_type = self._input.param("eject_type", None)
        initial_query_zones = self._input.param("query_zone_before_eject", None).split("|")
        post_eject_query_zones = self._input.param("query_zone_after_eject", None).split("|")

        self.build_cluster()
        self.load_data()
        idx = self.build_index()

        fts_nodes = []
        for initial_query_zone in initial_query_zones:
            fts_nodes.extend(self.get_zone_healthy_fts_nodes(zone=initial_query_zone))

        initial_hits = self.query_node(index=idx, node=fts_nodes[0])

        for node in fts_nodes[1:]:
            hits = self.query_node(index=idx, node=node)
            self.assertEqual(initial_hits, hits, "Difference in search results before node eject detected.")

        ejected_nodes = self.eject_nodes()

        post_eject_query_nodes = []
        for post_eject_query_zone in post_eject_query_zones:
            fts_nodes = self.get_zone_healthy_fts_nodes(zone=post_eject_query_zone)
            post_eject_query_nodes.extend(fts_nodes)

        try:
            for healthy_fts_node in post_eject_query_nodes:
                post_eject_hits, _, _, _ = idx.execute_query(self.fts_query, node=healthy_fts_node)
                self.assertEqual(initial_hits, post_eject_hits, "Hits are different after server groups modification!")
        finally:
            if eject_type == "shutdown":
                for ejected_node in ejected_nodes:
                    remote = RemoteMachineShellConnection(ejected_node)
                    remote.start_couchbase()

    def test_index_modification(self):
        mod_type = self._input.param("mod_type", None)
        self.build_cluster()
        self.load_data()
        idx = self.build_index()

        self.update_index(index=idx, mod_type=mod_type)

        fts_nodes = self._cb_cluster.get_fts_nodes()
        etalon_hits = self.query_node(index=idx, node=fts_nodes[0])
        for node in fts_nodes[1:]:
            hits = self.query_node(index=idx, node=node)
            self.assertEqual(etalon_hits, hits, "Found differences in fts request results between nodes after index modification")

    def test_replicas_distribution(self):
        final_replicas = self._input.param("final_replicas", 0)
        self.build_cluster()
        self.load_data()
        idx = self.build_index()

        idx.update_num_replicas(final_replicas)
        self.wait_for_indexing_complete()
        index_replica = idx.get_num_replicas()
        zones_with_replica = self.calculate_zones_with_replica(index=idx)
        self.assertEqual(index_replica + 1, zones_with_replica,
                        f"Found incorrect replicas distribution: index replicas: {index_replica}"
                        f", zones with replica count: {zones_with_replica}")

    def test_partitions_distribution(self):
        index_partitions = int(self._input.param("partitions", 1))

        self.build_cluster()
        self.load_data()
        idx = self.build_index()
        self.wait_for_indexing_complete()

        for zone in self.rest.get_zone_names():
            zone_fts_nodes = self.get_zone_healthy_fts_nodes(zone=zone)
            if len(zone_fts_nodes) > 0:
                zone_partitions_count = 0
                for node in zone_fts_nodes:
                    rest_client = RestConnection(node)
                    _, num_pindexes = rest_client.get_fts_stats(index_name=idx.name, bucket_name=idx.source_bucket.name,
                                                                stat_name="num_pindexes_actual")
                    zone_partitions_count = zone_partitions_count + num_pindexes
                self.assertEqual(zone_partitions_count, index_partitions, "Actual initial partitions distribution differs from expected.")

    def test_server_groups_modification(self):
        index_partitions = int(self._input.param("partitions", 1))
        self.build_cluster()
        available_nodes = self.rebuild_cluster_to_initial_state()
        self.load_data()
        idx = self.build_index()
        self.wait_for_indexing_complete()
        self.modify_server_groups(available_nodes=available_nodes)
        self.wait_for_indexing_complete()

        for zone in self.rest.get_zone_names():
            zone_fts_nodes = self.get_zone_healthy_fts_nodes(zone=zone)
            if len(zone_fts_nodes) > 0:
                zone_partitions_count = 0
                for node in zone_fts_nodes:
                    rest_client = RestConnection(node)
                    _, num_pindexes = rest_client.get_fts_stats(index_name=idx.name, bucket_name=idx.source_bucket.name,
                                                                stat_name="num_pindexes_actual")
                    zone_partitions_count = zone_partitions_count + num_pindexes
                self.assertEqual(zone_partitions_count, index_partitions, "Actual initial partitions distribution differs from expected.")

        fts_nodes = self._cb_cluster.get_fts_nodes()
        initial_hits = self.query_node(index=idx, node=fts_nodes[0])
        for node in fts_nodes[1:]:
            hits = self.query_node(index=idx, node=node)
            self.assertEqual(initial_hits, hits, "Difference in search results after server groups modification is detected.")

    def test_creation_order(self):
        index_partitions = int(self._input.param("partitions", 1))
        self.load_data()
        ordering = self._input.param("creation_order", None)

        if 'groups_first' == ordering:
            self.build_cluster()
            idx = self.build_index()
            self.wait_for_indexing_complete()
        else:
            idx = self.build_index()
            self.wait_for_indexing_complete()
            self.build_cluster()
            self.wait_for_indexing_complete()

        for zone in self.rest.get_zone_names():
            zone_fts_nodes = self.get_zone_healthy_fts_nodes(zone=zone)
            if len(zone_fts_nodes) > 0:
                zone_partitions_count = 0
                for node in zone_fts_nodes:
                    rest_client = RestConnection(node)
                    _, num_pindexes = rest_client.get_fts_stats(index_name=idx.name, bucket_name=idx.source_bucket.name,
                                                                stat_name="num_pindexes_actual")
                    zone_partitions_count = zone_partitions_count + num_pindexes
                self.assertEqual(zone_partitions_count, index_partitions, "Actual initial partitions distribution differs from expected.")

        fts_nodes = self._cb_cluster.get_fts_nodes()
        initial_hits = self.query_node(index=idx, node=fts_nodes[0])
        for node in fts_nodes[1:]:
            hits = self.query_node(index=idx, node=node)
            self.assertEqual(initial_hits, hits, "Difference in search results after server groups modification is detected.")

    def test_best_effort_distribution(self):
        index_partitions = int(self._input.param("partitions", 1))

        self.build_cluster()
        self.load_data()
        idx = self.build_index()
        self.wait_for_indexing_complete()

        self.eject_nodes()

        self.wait_for_indexing_complete()

        fts_nodes = self._cb_cluster.get_fts_nodes()
        initial_hits = self.query_node(index=idx, node=fts_nodes[0])
        for node in fts_nodes[1:]:
            hits = self.query_node(index=idx, node=node)
            self.assertEqual(initial_hits, hits, "Difference in search results after server group failover is detected.")

    def test_replicas_distribution_negative(self):
        self.build_cluster()
        self.load_data()
        try:
            idx = self.build_index()
            self.wait_for_indexing_complete()
            self.fail("Was able to create index having 2 replicas for a cluster containing just 2 fts nodes but 3 server groups.")
        except Exception as e:
            self.assertTrue("cluster needs 3 search nodes to support the requested replica count of 2" in str(e), "Unexpected error message while trying to create index with incorrect number of replicas.")


    def test_group_autofailover(self):
        self.build_cluster()
        self.load_data()
        idx = self.build_index()
        self.wait_for_indexing_complete()
        self.rest.update_autofailover_settings(True, 60, enableServerGroup=True)

        ejected_nodes = self.eject_nodes()
        try:
            self.sleep(120, "Waiting for server group auto failover to be started.")
            initial_hits = self.query_node(index=idx, node=self._cb_cluster.get_fts_nodes()[0])

            for zone in self.rest.get_zone_names():
                fts_nodes = self.get_zone_healthy_fts_nodes(zone=zone)
                for node in fts_nodes:
                    hits = self.query_node(index=idx, node=node)
                    self.assertEqual(initial_hits, hits, "Difference in search results after server group auto-failover is detected.")

        finally:
            for ejected_node in ejected_nodes:
                remote = RemoteMachineShellConnection(ejected_node)
                remote.start_couchbase()

    def find_max_server_group(self, idx=None):
        max_partitions_count = 0
        max_group = None
        for zone in self.rest.get_zone_names():
            zone_fts_nodes = self.get_zone_healthy_fts_nodes(zone=zone)
            if len(zone_fts_nodes) > 0:
                zone_partitions_count = 0
                for node in zone_fts_nodes:
                    rest_client = RestConnection(node)
                    _, num_pindexes = rest_client.get_fts_stats(index_name=idx.name, bucket_name=idx.source_bucket.name,
                                                                stat_name="num_pindexes_actual")
                    zone_partitions_count = zone_partitions_count + num_pindexes
                if zone_partitions_count > max_partitions_count:
                    max_partitions_count = zone_partitions_count
                    max_group = zone
        return max_group

    def modify_server_groups(self, available_nodes=None):
        operation = self._input.param("operation", None)
        if 'add_group' == operation:
            add_group = self._input.param("add_server_group", None)
            group_name = add_group.split("-")[0]
            group_nodes = add_group.split("-")[1].split(":")
            self.rest.add_zone(group_name)
            nodes_to_move = []
            for node in group_nodes:
                node_to_shuffle = available_nodes.pop(0)
                nodes_to_move.append(node_to_shuffle.ip)
                if 'D' == node:
                    self._cb_cluster.rebalance_in_node(nodes_in=[node_to_shuffle], services=['kv'], sleep_before_rebalance=0)
                elif 'F' == node:
                    self._cb_cluster.rebalance_in_node(nodes_in=[node_to_shuffle], services=['fts'],
                                                       sleep_before_rebalance=0)
                else:
                    self.fail(f"Unsupported node type found {node}!")
            self.rest.shuffle_nodes_in_zones(moved_nodes=nodes_to_move, source_zone=self.default_group_name, target_zone=group_name)
        elif 'remove_group' == operation:
            self.eject_nodes()
        elif 'add_nodes' == operation:
            extend_groups = self._input.param("groups_additions", None).split("|")
            for extended_group in extend_groups:
                group_name = extended_group.split("-")[0]
                nodes = extended_group.split("-")[1]
                nodes_to_move = []
                for node in nodes:
                    node_to_shuffle = available_nodes.pop(0)
                    if 'D' == node:
                        self._cb_cluster.rebalance_in_node(nodes_in=[node_to_shuffle], services=['kv'],
                                                           sleep_before_rebalance=0)
                    elif 'F' == node:
                        self._cb_cluster.rebalance_in_node(nodes_in=[node_to_shuffle], services=['fts'],
                                                           sleep_before_rebalance=0)
                    nodes_to_move.append(node_to_shuffle.ip)
                    self.rest.shuffle_nodes_in_zones(moved_nodes=nodes_to_move, source_zone=self.default_group_name, target_zone=group_name)
        elif 'swap_nodes' == operation:
            server_group1_fts_node = None
            server_group2_fts_node = None
            server_group1_nodes = self.rest.get_nodes_in_zone('sg1')
            server_group2_nodes = self.rest.get_nodes_in_zone('sg2')
            for key in server_group1_nodes:
                for fts_node in self._cb_cluster.get_fts_nodes():
                    if fts_node.ip == key:
                        server_group1_fts_node = fts_node
                        break
            for key in server_group2_nodes:
                for fts_node in self._cb_cluster.get_fts_nodes():
                    if fts_node.ip == key:
                        server_group2_fts_node = fts_node
                        break
            self.rest.shuffle_nodes_in_zones(moved_nodes=[server_group1_fts_node.ip], source_zone='sg1',
                                             target_zone='sg2')
            self.rest.shuffle_nodes_in_zones(moved_nodes=[server_group2_fts_node.ip], source_zone='sg2',
                                             target_zone='sg1')
        elif 'rename' == operation:
            self.rest.rename_zone('sg1', 'sg1_1')
            self.rest.rename_zone('sg2', 'sg1_2')

    def get_num_partitions_distribution(self, index=None, node=None):
        rest_client = RestConnection(node)
        _, num_pindexes = rest_client.get_fts_stats(index_name=index.name, bucket_name=index.source_bucket.name,
                                                        stat_name="num_pindexes_actual")

    def calculate_zones_with_replica(self, index=None):
        zones_list = self.rest.get_all_zones_info()
        zones_with_replica = 0
        for zone in zones_list['groups']:
            replica_found = False
            nodes = zone['nodes']
            for node in nodes:
                if replica_found:
                    break
                if 'fts' in node['services']:
                    hostname = node['hostname'][0:node['hostname'].find(":")]
                    for fts_node in self._cb_cluster.get_fts_nodes():
                        if fts_node.ip == hostname:
                            rest_client = RestConnection(fts_node)
                            _, num_pindexes = rest_client.get_fts_stats(index_name=index.name, bucket_name=index.source_bucket.name, stat_name="num_pindexes_actual")
                            if num_pindexes > 0:
                                replica_found = True
                                zones_with_replica += 1
                                break

        return zones_with_replica

    def update_index(self, index=None, mod_type=None):
        if mod_type == 'custom_mapping':
            index.index_definition['params']['doc_config'] = {}
            doc_config = {}
            doc_config['mode'] = 'type_field'
            doc_config['type_field'] = 'dept'
            index.index_definition['params']['doc_config'] = doc_config

            index.add_type_mapping_to_index_definition(type="filler",
                                                       analyzer="standard")
            index.index_definition['params']['mapping'] = {
                "default_analyzer": "standard",
                "default_datetime_parser": "dateTimeOptional",
                "default_field": "_all",
                "default_mapping": {
                    "dynamic": False,
                    "enabled": False
                },
                "default_type": "_default",
                "docvalues_dynamic": True,
                "index_dynamic": True,
                "store_dynamic": False,
                "type_field": "_type",
                "types": {
                    "Sales": {
                        "default_analyzer": "standard",
                        "dynamic": True,
                        "enabled": True,
                    }
                }
            }
            index.index_definition['uuid'] = index.get_uuid()
            index.update()
        elif "delete" == mod_type:
            self._cb_cluster.delete_fts_index(index.name)
        self.wait_for_indexing_complete()

    def rebuild_cluster_to_initial_state(self):
        cleanup_nodes = self.rest.get_nodes_in_zone(self.default_group_name)
        nodes_to_remove = []
        for key in cleanup_nodes.keys():
            node = self._cb_cluster.get_node(key, str(8091))
            nodes_to_remove.append(node)
            self._cb_cluster.rebalance_out_node(node=node, sleep_before_rebalance=0)
        return nodes_to_remove

    def build_cluster(self):
        sg_structure = self._input.param("server_groups", None)
        server_groups = sg_structure.split("|")
        available_kv_nodes = self._cb_cluster.get_kv_nodes()
        available_fts_nodes = self._cb_cluster.get_fts_nodes()

        for server_group in server_groups:
            group_name = server_group.split("-")[0]
            group_nodes = server_group.split("-")[1].split(":")
            self.rest.add_zone(group_name)
            nodes_to_move = []

            for node in group_nodes:
                if 'D' == node:
                    if len(available_kv_nodes) == 0:
                        self.fail("Cannot find any available kv node!")
                    nodes_to_move.append(available_kv_nodes.pop(0).ip)

                elif 'F' == node:
                    if len(available_fts_nodes) == 0:
                        self.fail("Cannot find any available fts node!")
                    nodes_to_move.append(available_fts_nodes.pop(0).ip)
                else:
                    self.fail(f"Unsupported node type found {node}!")
            self.rest.shuffle_nodes_in_zones(moved_nodes=nodes_to_move, source_zone=self.default_group_name, target_zone=group_name)

    def build_index(self):
        replicas = self._input.param("replicas", 0)
        partitions = self._input.param("partitions", 1)

        collection_index, _type, index_scope, index_collections = self.define_index_parameters_collection_related()
        idx = self.create_index(
            bucket=self._cb_cluster.get_bucket_by_name('default'),
            index_name="fts_idx",
            collection_index=collection_index, _type=_type,
            scope=index_scope,
            collections=index_collections)
        idx.update_index_partitions(partitions)
        idx.update_num_replicas(replicas)
        self.wait_for_indexing_complete()
        return idx

    def get_zone_healthy_fts_nodes(self, zone=None):
        zone_nodes = self.rest.get_nodes_in_zone(zone)
        healthy_fts_nodes = []
        for key in zone_nodes.keys():
            node = zone_nodes[key]
            if node["status"] == 'healthy' and 'fts' in node['services']:
                for fts_node in self._cb_cluster.get_fts_nodes():
                    if key == fts_node.ip:
                        healthy_fts_nodes.append(fts_node)
        return healthy_fts_nodes

    def query_node(self, index=None, node=None):
        hits, _, _, _ = index.execute_query(self.fts_query, node=node)
        return hits

    def eject_nodes(self):
        eject_nodes_structure = self._input.param("eject_nodes", None)
        eject_server_groups = eject_nodes_structure.split("|")
        eject_type = self._input.param("eject_type", None)
        eject_nodes = []

        for eject_server_group in eject_server_groups:
            group_name = eject_server_group.split("-")[0]
            node_types = eject_server_group.split("-")[1]
            target_zone_nodes = self.rest.get_nodes_in_zone(group_name)
            node_type_arr = node_types.split(":")
            for node_type in node_type_arr:
                if 'D' == node_type:
                    for kv_node in self._cb_cluster.get_kv_nodes():
                        if kv_node.ip in target_zone_nodes.keys():
                            if kv_node not in eject_nodes:
                                eject_nodes.append(kv_node)
                                break
                elif 'F' == node_type:
                    for fts_node in self._cb_cluster.get_fts_nodes():
                        if fts_node.ip in target_zone_nodes.keys():
                            if fts_node not in eject_nodes:
                                eject_nodes.append(fts_node)
                                break
                else:
                    self.fail("Unsupported node type found in nodes to eject.")

        for node in eject_nodes:
            if "remove" == eject_type:
                self._cb_cluster.rebalance_out_node(node=node)
            elif "failover" == eject_type:
                self._cb_cluster.failover(graceful=False, node=node)
                self._cb_cluster.rebalance_failover_nodes()
            elif "shutdown" == eject_type:
                remote = RemoteMachineShellConnection(node)
                remote.stop_couchbase()
                self._cb_cluster.failover(graceful=False, node=node)
                self._cb_cluster.rebalance_failover_nodes()
            elif "shutdown_no_rebalance" == eject_type:
                remote = RemoteMachineShellConnection(node)
                remote.stop_couchbase()

        return eject_nodes

    def create_server_group(self, group_name=None):
        self.rest.add_zone(group_name)

    def _cleanup_server_groups(self):
        curr_server_groups = self.rest.get_zone_names()
        for g in curr_server_groups.keys():
            if g != self.default_group_name:
                nodes = self.rest.get_nodes_in_zone(g)
                if nodes:
                    nodes_to_move = []
                    for key in nodes.keys():
                        nodes_to_move.append(key)
                    self.rest.shuffle_nodes_in_zones(moved_nodes=nodes_to_move, source_zone=g, target_zone=self.default_group_name)
                self.rest.delete_zone(g)
