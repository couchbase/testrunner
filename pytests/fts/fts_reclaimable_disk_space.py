# coding=utf-8

from threading import Thread

from .fts_base import FTSBaseTest
from lib.membase.api.rest_client import RestConnection
from lib.remote.remote_util import RemoteMachineShellConnection
from lib.couchbase_helper.tuq_helper import N1QLHelper


class FTSReclaimableDiskSpace(FTSBaseTest):

    def setUp(self):
        super(FTSReclaimableDiskSpace, self).setUp()

        self.default_group_name = "Group 1"
        self.n1ql = N1QLHelper(version="sherlock", shell=None,
                          item_flag=None, n1ql_port=8903,
                          full_docs_list=[], log=self.log)
        self.rest = RestConnection(self._cb_cluster.get_master_node())
        self._cleanup_server_groups()

    def tearDown(self):
        super(FTSReclaimableDiskSpace, self).tearDown()

    def test_reclaimable_disk_space(self):
        mutations_percentage = self._input.param("mutations", 40)
        replicas = self._input.param("replicas", 0)
        partitions = self._input.param("partitions", 1)

        self.load_cbworkloadgen(num_items=self._num_items)
        self.create_gsi()
        collection_index, _type, index_scope, index_collections = self.define_index_parameters_collection_related()
        idx = self.create_index(
            bucket=self._cb_cluster.get_bucket_by_name('default'),
            index_name="fts_idx",
            collection_index=collection_index, _type=_type,
            scope=index_scope,
            collections=index_collections)
        if self._input.param("custom_mapping", False):
            self.update_index(idx)
        idx.update_index_partitions(partitions)
        idx.update_num_replicas(replicas)

        self.wait_for_indexing_complete()

        self.perform_mutations(mutations_percentage=mutations_percentage)
        self.wait_for_indexing_complete()
        self.validate_feature(idx=idx)

    def test_server_groups(self):
        self.build_cluster()
        mutations_percentage = self._input.param("mutations", 40)
        replicas = self._input.param("replicas", 0)
        partitions = self._input.param("partitions", 1)

        self.load_cbworkloadgen(num_items=self._num_items)
        self.create_gsi()
        collection_index, _type, index_scope, index_collections = self.define_index_parameters_collection_related()
        idx = self.create_index(
            bucket=self._cb_cluster.get_bucket_by_name('default'),
            index_name="fts_idx",
            collection_index=collection_index, _type=_type,
            scope=index_scope,
            collections=index_collections)
        if self._input.param("custom_mapping", False):
            self.update_index(idx)
        idx.update_index_partitions(partitions)
        idx.update_num_replicas(replicas)

        self.wait_for_indexing_complete()

        self.perform_mutations(mutations_percentage=mutations_percentage)
        self.wait_for_indexing_complete()
        self.validate_feature(idx=idx)

    def test_failover(self):
        mutations_percentage = self._input.param("mutations", 40)
        replicas = self._input.param("replicas", 0)
        partitions = self._input.param("partitions", 1)

        self.load_cbworkloadgen(num_items=self._num_items)
        self.create_gsi()
        collection_index, _type, index_scope, index_collections = self.define_index_parameters_collection_related()
        idx = self.create_index(
            bucket=self._cb_cluster.get_bucket_by_name('default'),
            index_name="fts_idx",
            collection_index=collection_index, _type=_type,
            scope=index_scope,
            collections=index_collections)
        if self._input.param("custom_mapping", False):
            self.update_index(idx)
        idx.update_index_partitions(partitions)
        idx.update_num_replicas(replicas)

        self.wait_for_indexing_complete()

        self.perform_mutations(mutations_percentage=mutations_percentage)
        self.wait_for_indexing_complete()

        self._cb_cluster.failover(node=self._cb_cluster.get_random_fts_node())
        self.validate_feature(idx=idx)

    def test_failover_concurrent(self):
        mutations_percentage = self._input.param("mutations", 40)
        replicas = self._input.param("replicas", 0)
        partitions = self._input.param("partitions", 1)

        self.load_cbworkloadgen(num_items=self._num_items)
        self.create_gsi()
        collection_index, _type, index_scope, index_collections = self.define_index_parameters_collection_related()
        idx = self.create_index(
            bucket=self._cb_cluster.get_bucket_by_name('default'),
            index_name="fts_idx",
            collection_index=collection_index, _type=_type,
            scope=index_scope,
            collections=index_collections)
        if self._input.param("custom_mapping", False):
            self.update_index(idx)
        idx.update_index_partitions(partitions)
        idx.update_num_replicas(replicas)

        self.wait_for_indexing_complete()

        self.perform_mutations(mutations_percentage=mutations_percentage)
        self.wait_for_indexing_complete()

        fts_nodes = self._cb_cluster.get_fts_nodes()
        if self.is_index_on_node(index=idx, node=fts_nodes[0]):
            compaction_fts_node = fts_nodes[0]
            failover_fts_node = fts_nodes[1]
        else:
            compaction_fts_node = fts_nodes[1]
            failover_fts_node = fts_nodes[0]

        threads = []
        threads.append(Thread(target=self.perform_compaction,
                              name="compaction thread",
                              args=(compaction_fts_node, idx)))
        threads.append(Thread(target=self.parallel_failover,
                              name="failover thread",
                              args=(failover_fts_node,)))

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        reclaimable_space, num_root_filesegments = self.get_disk_usage(node=compaction_fts_node, index=idx)
        self.assertEqual(reclaimable_space, 0, "Reclaimable space is greater 0 after mutations, after a compaction call.")


    def test_concurrent_clusterops(self):
        mutations_percentage = self._input.param("mutations", 40)
        replicas = self._input.param("replicas", 0)
        partitions = self._input.param("partitions", 1)

        self.load_cbworkloadgen(num_items=self._num_items)
        self.create_gsi()
        collection_index, _type, index_scope, index_collections = self.define_index_parameters_collection_related()
        idx = self.create_index(
            bucket=self._cb_cluster.get_bucket_by_name('default'),
            index_name="fts_idx",
            collection_index=collection_index, _type=_type,
            scope=index_scope,
            collections=index_collections)
        if self._input.param("custom_mapping", False):
            self.update_index(idx)
        idx.update_index_partitions(partitions)
        idx.update_num_replicas(replicas)

        self.wait_for_indexing_complete()

        self.perform_mutations(mutations_percentage=mutations_percentage)
        self.wait_for_indexing_complete()

        operations = self._input.param("operations", None).split("|")
        compaction_happened = False

        threads = []
        if 'mutations' in operations:
            thread = Thread(target=self.perform_mutations,
                              name="mutations thread",
                              args=(mutations_percentage,))
            threads.append(thread)
        if 'compact' in operations:
            compaction_happened = True
            for fts_node in self._cb_cluster.get_fts_nodes():
                if self.is_index_on_node(index=idx, node=fts_node):
                    thread = Thread(target=self.perform_compaction,
                                     name="compact thread",
                                     args=(self._cb_cluster.get_random_fts_node(),idx))
                    threads.append(thread)
        if 'index_update_increase_partitions' in operations:
            thread = Thread(target=self.increase_index_partitons,
                              name="index_update_increase_partitions thread",
                              args=(idx,))
            threads.append(thread)
        if 'index_update_increase_replicas' == operations:
            thread = Thread(target=self.increase_index_replicas,
                              name="index_update_increase_replicas thread",
                              args=(idx,))
            threads.append(thread)

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.log.info("Going to explore reclaimable space")
        for fts_node in self._cb_cluster.get_fts_nodes():
            if self.is_index_on_node(index=idx, node=fts_node):
                self.log.info(f"Taking node {fts_node}")
                reclaimable_space, num_root_filesegments = self.get_disk_usage(node=fts_node, index=idx)
                self.log.info(f"Reclaimable space {reclaimable_space}. Compation happened? {compaction_happened}")
                if compaction_happened:
                    self.assertEqual(reclaimable_space, 0, "Reclaimable space is greater 0 after mutations, after a compaction call.")
                else:
                    self.assertGreater(reclaimable_space, 0, "Reclaimable space is 0 after mutations, before compaction.")
                self.log.info("Performing compaction")
                self.perform_compaction(server=fts_node, index=idx)
                reclaimable_space, num_root_filesegments = self.get_disk_usage(node=fts_node, index=idx)
                self.log.info(f"Reclaimable space after compaction {reclaimable_space}")
                self.assertEqual(reclaimable_space, 0, "Reclaimable space is greater than 0 after compaction call.")
                compaction_happened = True

    def validate_feature(self, idx=None, retries=10):
        self.log.info("Going to explore reclaimable space")
        for fts_node in self._cb_cluster.get_fts_nodes():
            if self.is_index_on_node(index=idx, node=fts_node):
                self.log.info(f"Taking node {fts_node}")
                compaction_retries = retries
                reclaimable_space, num_root_filesegments = self.get_disk_usage(node=fts_node, index=idx)
                retry_count = 1
                while compaction_retries > 0 and reclaimable_space > 0:
                    self.perform_compaction(server=fts_node, index=idx)
                    reclaimable_space, num_root_filesegments = self.get_disk_usage(node=fts_node, index=idx)
                    self.log.info(f"Reclaimable space is: {reclaimable_space}")
                    self.log.info(f"Root file segments number is: {num_root_filesegments}.")
                    self.log.info(f"Compaction call retry: {retry_count}")
                    self.sleep(5, "Sleep between consecutive compaction calls.")
                    retry_count = retry_count + 1
                    compaction_retries = compaction_retries - 1

        for fts_node in self._cb_cluster.get_fts_nodes():
            if self.is_index_on_node(index=idx, node=fts_node):
                reclaimable_space, num_root_filesegments = self.get_disk_usage(node=fts_node, index=idx)
                self.assertEqual(reclaimable_space, 0, "Reclaimable space is greater than 0 after compaction call.")

    def increase_index_partitons(self, index=None):
        index.update_index_partitions(20)
        self.wait_for_indexing_complete()

    def increase_index_replicas(self, index=None):
        index.update_num_replicas(1)
        self.wait_for_indexing_complete()

    def parallel_failover(self, node=None):
        self.sleep(2, "Let's parallel compaction start for sure.")
        self._cb_cluster.failover(node=node)

    def load_cbworkloadgen(self, num_items=0, is_insert=False):
        if is_insert:
            command_options = '-j --prefix inserts'
        else:
            command_options = '-j'
        shell = RemoteMachineShellConnection(self.master)
        for bucket in self._cb_cluster.get_buckets():
            shell.execute_cbworkloadgen(self.master.rest_username,
                                        self.master.rest_password,
                                        num_items,
                                        100,
                                        bucket.name,
                                        1024,
                                        command_options)

    def create_gsi(self):
        n1ql_node = self._cb_cluster.get_random_n1ql_node()
        for bucket in self._cb_cluster.get_buckets():
            self.n1ql.run_cbq_query(query=f"create primary index on {bucket.name}", server=n1ql_node)

    def perform_mutations(self, mutations_percentage=40):
        mutation_type = self._input.param("mutation_type", "update")
        n1ql_node = self._cb_cluster.get_random_n1ql_node()
        for bucket in self._cb_cluster.get_buckets():
            if "update" == mutation_type:
                query = f'update {bucket.name} set name=name||"_" where age<{mutations_percentage}'
                self.n1ql.run_cbq_query(query=query, server=n1ql_node)
            elif "delete" == mutation_type:
                query = f'delete from {bucket.name} where age<{mutations_percentage}'
                self.n1ql.run_cbq_query(query=query, server=n1ql_node)
            elif "insert" == mutation_type:
                num_items = int(self._num_items / 100 * mutations_percentage)
                self.load_cbworkloadgen(num_items=num_items, is_insert=True)

    def get_disk_usage(self, node=None, index=None):
        rest_client = RestConnection(node)
        _, reclaimable_space = rest_client.get_fts_stats(index_name=index.name, bucket_name=index.source_bucket.name,
                                                        stat_name="num_bytes_used_disk_by_root_reclaimable")
        _, num_root_filesegments = rest_client.get_fts_stats(index_name=index.name, bucket_name=index.source_bucket.name,
                                                        stat_name="num_root_filesegments")
        return reclaimable_space, num_root_filesegments

    def perform_compaction(self, server=None, index=None, timeout=120):
        import time
        rest = RestConnection(server)
        rest.start_fts_index_compaction(index_name=index.name)
        done = False
        start_time = time.time()
        while not done:
            if time.time() - start_time > timeout:
                self.fail("Compaction operation is failed due to timeout.")
            _, compactions_state_content = rest.get_fts_index_compactions(index.name)
            done = 'In progress' not in str(compactions_state_content)
            self.sleep(1)

    def update_index(self, index=None, mod_type=None):
        index.index_definition['params']['doc_config'] = {}
        doc_config = {}
        doc_config['mode'] = 'type_field'
        doc_config['type_field'] = 'name'
        index.index_definition['params']['doc_config'] = doc_config

        index.add_type_mapping_to_index_definition(type="name",
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
                "pymc100": {
                    "default_analyzer": "standard",
                    "dynamic": True,
                    "enabled": True,
                }
            }
        }

        index.index_definition['uuid'] = index.get_uuid()
        index.update()

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
                if 'D' in node:
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

    def is_index_on_node(self, index=None, node=None):
        rest_client = RestConnection(node)
        _, num_pindexes = rest_client.get_fts_stats(index_name=index.name, bucket_name=index.source_bucket.name,
                                                        stat_name="num_pindexes_actual")
        return num_pindexes > 0
