from xdcrnewbasetests import XDCRNewBaseTest, NodeHelper, FloatingServers
from couchbase_helper.documentgenerator import BlobGenerator
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from couchbase_helper.cluster import Cluster
import time

class nwusage(XDCRNewBaseTest):
    def setUp(self):
        super(nwusage, self).setUp()
        self.src_cluster = self.get_cb_cluster_by_name('C1')
        self.src_master = self.src_cluster.get_master_node()
        self.dest_cluster = self.get_cb_cluster_by_name('C2')
        self.dest_master = self.dest_cluster.get_master_node()
        self.cluster = Cluster()

    def tearDown(self):
        super(nwusage, self).tearDown()

    def _set_nwusage_limit(self, cluster, nw_limit=0):
        repl_id = cluster.get_remote_clusters()[0].get_replications()[0].get_repl_id()
        shell = RemoteMachineShellConnection(cluster.get_master_node())
        repl_id = str(repl_id).replace('/','%2F')
        base_url = "http://" + cluster.get_master_node().ip + ":8091/settings/replications/" + repl_id
        command = "curl -X POST -u Administrator:password " + base_url + " -d networkUsageLimit=" + str(nw_limit)
        output, error = shell.execute_command(command)
        shell.log_command_output(output, error)

    def _verify_bandwidth_usage(self, node, nw_limit=1, no_of_nodes=2, event_time=None,
                                nw_usage="[1-9][0-9]*", end_time=None):
        goxdcr_log = NodeHelper.get_goxdcr_log_dir(node) + '/goxdcr.log'
        nw_max = (nw_limit * 1024 * 1024)/no_of_nodes

        if event_time:
            time_to_compare = time.strptime(event_time, '%Y-%m-%dT%H:%M:%S')
        else:
            matches, _ = NodeHelper.check_goxdcr_log(node, "Success adding replication specification",
                                                 goxdcr_log, print_matches=True)
            time_to_compare_str = matches[-1].split(' ')[0].split('.')[0]
            time_to_compare = time.strptime(time_to_compare_str, '%Y-%m-%dT%H:%M:%S')

        matches, count = NodeHelper.check_goxdcr_log(node, "bandwidth_limit=" + str(nw_max) +
                                            ", bandwidth_usage=" + nw_usage, goxdcr_log, print_matches=True)
        match_count = 0
        skip_count = 0
        for item in matches:
            items = item.split(' ')
            item_time = items[0].split('.')[0]
            item_datetime = time.strptime(item_time, '%Y-%m-%dT%H:%M:%S')
            if item_datetime < time_to_compare:
                skip_count += 1
                continue
            if end_time:
                end_datetime = time.strptime(end_time, '%Y-%m-%dT%H:%M:%S')
                if item_datetime > end_datetime:
                    skip_count += 1
                    continue
            bandwidth_usage = items[-1].split('=')[-1]
            if int(bandwidth_usage) <= nw_max:
                match_count += 1
                continue
            else:
                self.fail("Bandwidth usage higher than Bandwidth limit in {0}".format(item))

        if match_count + skip_count == count:
            self.log.info("{0} stale entries skipped".format(skip_count))
            if match_count > 0:
                self.log.info("{0} entries checked - Bandwidth usage always lower than Bandwidth limit as expected".
                          format(match_count))
            else:
                if self._input.param("replication_type") == "capi":
                    self.log.info("Bandwidth Throttler not enabled on replication as expected")
                else:
                    self.fail("Bandwidth Throttler not enabled on replication")

    def test_nwusage_with_unidirection(self):
        self.setup_xdcr()
        self.sleep(60)
        nw_limit = self._input.param("nw_limit", 1)
        self._set_nwusage_limit(self.src_cluster, nw_limit)

        gen_create = BlobGenerator('nwOne', 'nwOne', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.perform_update_delete()

        self.verify_results()
        self._verify_bandwidth_usage(node=self.src_cluster.get_master_node(), nw_limit=nw_limit)

    def test_nwusage_with_bidirection(self):
        self.setup_xdcr()
        self.sleep(60)
        nw_limit = self._input.param("nw_limit", 1)
        self._set_nwusage_limit(self.src_cluster, nw_limit)
        self._set_nwusage_limit(self.dest_cluster, nw_limit)

        gen_create1 = BlobGenerator('nwOne', 'nwOne', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create1)
        gen_create2 = BlobGenerator('nwTwo', 'nwTwo', self._value_size, end=self._num_items)
        self.dest_cluster.load_all_buckets_from_generator(kv_gen=gen_create2)

        self.perform_update_delete()

        self.verify_results()
        self._verify_bandwidth_usage(node=self.src_cluster.get_master_node(), nw_limit=nw_limit)
        self._verify_bandwidth_usage(node=self.dest_cluster.get_master_node(), nw_limit=nw_limit)

    def test_nwusage_with_unidirection_pause_resume(self):
        self.setup_xdcr()

        gen_create = BlobGenerator('nwOne', 'nwOne', self._value_size, end=self._num_items)
        tasks = self.src_cluster.async_load_all_buckets_from_generator(kv_gen=gen_create)

        self.src_cluster.pause_all_replications()

        nw_limit = self._input.param("nw_limit", 1)
        self._set_nwusage_limit(self.src_cluster, nw_limit)

        self.src_cluster.resume_all_replications()

        for task in tasks:
            task.result()

        self._wait_for_replication_to_catchup()

        self.verify_results()
        self._verify_bandwidth_usage(node=self.src_cluster.get_master_node(), nw_limit=nw_limit)

    def test_nwusage_with_bidirection_pause_resume(self):
        self.setup_xdcr()

        gen_create1 = BlobGenerator('nwOne', 'nwOne', self._value_size, end=self._num_items)
        tasks = self.src_cluster.async_load_all_buckets_from_generator(kv_gen=gen_create1)
        gen_create2 = BlobGenerator('nwTwo', 'nwTwo', self._value_size, end=self._num_items)
        tasks.extend(self.dest_cluster.async_load_all_buckets_from_generator(kv_gen=gen_create2))

        self.src_cluster.pause_all_replications()
        self.dest_cluster.pause_all_replications()

        nw_limit = self._input.param("nw_limit", 1)
        self._set_nwusage_limit(self.src_cluster, nw_limit)
        self._set_nwusage_limit(self.dest_cluster, nw_limit)

        self.src_cluster.resume_all_replications()
        self.dest_cluster.resume_all_replications()

        for task in tasks:
            task.result()

        self._wait_for_replication_to_catchup()

        self.verify_results()
        self._verify_bandwidth_usage(node=self.src_cluster.get_master_node(), nw_limit=nw_limit)
        self._verify_bandwidth_usage(node=self.dest_cluster.get_master_node(), nw_limit=nw_limit)

    def test_nwusage_with_unidirection_in_parallel(self):
        self.setup_xdcr()

        gen_create = BlobGenerator('nwOne', 'nwOne', self._value_size, end=self._num_items)
        tasks = self.src_cluster.async_load_all_buckets_from_generator(kv_gen=gen_create)

        nw_limit = self._input.param("nw_limit", 1)
        self._set_nwusage_limit(self.src_cluster, nw_limit)

        for task in tasks:
            task.result()

        self._wait_for_replication_to_catchup()

        self.verify_results()
        self._verify_bandwidth_usage(node=self.src_cluster.get_master_node(), nw_limit=nw_limit)

    def test_nwusage_with_bidirection_in_parallel(self):
        self.setup_xdcr()

        gen_create1 = BlobGenerator('nwOne', 'nwOne', self._value_size, end=self._num_items)
        tasks = self.src_cluster.async_load_all_buckets_from_generator(kv_gen=gen_create1)
        gen_create2 = BlobGenerator('nwTwo', 'nwTwo', self._value_size, end=self._num_items)
        tasks.extend(self.dest_cluster.async_load_all_buckets_from_generator(kv_gen=gen_create2))

        nw_limit = self._input.param("nw_limit", 1)
        self._set_nwusage_limit(self.src_cluster, nw_limit)
        self._set_nwusage_limit(self.dest_cluster, nw_limit)

        for task in tasks:
            task.result()

        self._wait_for_replication_to_catchup()

        self.verify_results()
        self._verify_bandwidth_usage(node=self.src_cluster.get_master_node(), nw_limit=nw_limit)
        self._verify_bandwidth_usage(node=self.dest_cluster.get_master_node(), nw_limit=nw_limit)

    def test_nwusage_with_rebalance_in(self):
        self.setup_xdcr()
        self.sleep(60)
        nw_limit = self._input.param("nw_limit", 1)
        self._set_nwusage_limit(self.src_cluster, nw_limit)

        gen_create = BlobGenerator('nwOne', 'nwOne', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.src_cluster.rebalance_in()

        self.perform_update_delete()

        self.verify_results()
        self._verify_bandwidth_usage(node=self.src_cluster.get_master_node(), nw_limit=nw_limit, no_of_nodes=3)

    def test_nwusage_with_rebalance_out(self):
        self.setup_xdcr()
        self.sleep(60)
        nw_limit = self._input.param("nw_limit", 1)
        self._set_nwusage_limit(self.src_cluster, nw_limit)

        gen_create = BlobGenerator('nwOne', 'nwOne', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.src_cluster.rebalance_out()

        self.perform_update_delete()

        self.verify_results()
        self._verify_bandwidth_usage(node=self.src_cluster.get_master_node(), nw_limit=nw_limit, no_of_nodes=1)

    def test_nwusage_reset_to_zero(self):
        self.setup_xdcr()
        self.sleep(60)
        nw_limit = self._input.param("nw_limit", 1)
        self._set_nwusage_limit(self.src_cluster, nw_limit)

        gen_create = BlobGenerator('nwOne', 'nwOne', self._value_size, end=self._num_items)
        tasks = self.src_cluster.async_load_all_buckets_from_generator(kv_gen=gen_create)

        self.sleep(30)
        self._set_nwusage_limit(self.src_cluster, 0)
        event_time = time.strftime('%Y-%m-%dT%H:%M:%S')
        self.log.info("Network limit reset to 0 at {0}".format(event_time))

        for task in tasks:
            task.result()

        self.verify_results()
        self._verify_bandwidth_usage(node=self.src_cluster.get_master_node(), nw_limit=nw_limit, end_time=event_time)
        self._verify_bandwidth_usage(node=self.src_cluster.get_master_node(), nw_limit=0, no_of_nodes=2, event_time=event_time, nw_usage="0")

    def test_nwusage_with_hard_failover_and_bwthrottle_enabled(self):
        self.setup_xdcr()
        self.sleep(60)
        nw_limit = self._input.param("nw_limit", 1)
        self._set_nwusage_limit(self.src_cluster, nw_limit)

        self.src_cluster.pause_all_replications()

        gen_create = BlobGenerator('nwOne', 'nwOne', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.src_cluster.resume_all_replications()

        self.sleep(15)

        self.src_cluster.failover_and_rebalance_nodes()
        failover_time = time.strftime('%Y-%m-%dT%H:%M:%S')
        self.log.info("Node failed over at {0}".format(failover_time))

        self.sleep(15)

        self.src_cluster.rebalance_in()
        node_back_time = time.strftime('%Y-%m-%dT%H:%M:%S')
        self.log.info("Node added back at {0}".format(node_back_time))

        self._wait_for_replication_to_catchup()

        self.verify_results()
        self._verify_bandwidth_usage(node=self.src_cluster.get_master_node(), end_time=failover_time)
        self._verify_bandwidth_usage(node=self.src_cluster.get_master_node(), event_time=failover_time, end_time=node_back_time, no_of_nodes=1)
        self._verify_bandwidth_usage(node=self.src_cluster.get_master_node(), event_time=node_back_time)

    def test_nwusage_with_hard_failover_and_bwthrottle_enabled_later(self):
        self.setup_xdcr()

        self.src_cluster.pause_all_replications()

        gen_create = BlobGenerator('nwOne', 'nwOne', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.src_cluster.resume_all_replications()

        self.sleep(15)

        self.src_cluster.failover_and_rebalance_nodes()

        self.sleep(15)

        nw_limit = self._input.param("nw_limit", 1)
        self._set_nwusage_limit(self.src_cluster, nw_limit)
        bw_enable_time = time.strftime('%Y-%m-%dT%H:%M:%S')
        self.log.info("Bandwidth throttler enabled at {0}".format(bw_enable_time))

        self.sleep(60)

        self.src_cluster.rebalance_in()
        node_back_time = time.strftime('%Y-%m-%dT%H:%M:%S')
        self.log.info("Node added back at {0}".format(node_back_time))

        self._wait_for_replication_to_catchup(timeout=600)

        self.verify_results()
        self._verify_bandwidth_usage(node=self.src_cluster.get_master_node(), event_time=bw_enable_time, end_time=node_back_time, no_of_nodes=1)
        self._verify_bandwidth_usage(node=self.src_cluster.get_master_node(), event_time=node_back_time)

    def test_nwusage_with_auto_failover_and_bwthrottle_enabled(self):
        self.setup_xdcr()

        self.src_cluster.rebalance_in()

        nw_limit = self._input.param("nw_limit", 1)
        self._set_nwusage_limit(self.src_cluster, nw_limit)

        src_conn = RestConnection(self.src_cluster.get_master_node())
        src_conn.update_autofailover_settings(enabled=True, timeout=30)

        self.src_cluster.pause_all_replications()

        gen_create = BlobGenerator('nwOne', 'nwOne', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.src_cluster.resume_all_replications()

        self.sleep(15)

        shell = RemoteMachineShellConnection(self._input.servers[1])
        shell.stop_couchbase()
        self.sleep(30)
        task = self.cluster.async_rebalance(self.src_cluster.get_nodes(), [], [])
        task.result()
        failover_time = time.strftime('%Y-%m-%dT%H:%M:%S')
        self.log.info("Node auto failed over at {0}".format(failover_time))
        FloatingServers._serverlist.append(self._input.servers[1])

        self.sleep(15)

        shell.start_couchbase()
        shell.disable_firewall()
        self.sleep(45)
        self.src_cluster.rebalance_in()
        node_back_time = time.strftime('%Y-%m-%dT%H:%M:%S')
        self.log.info("Node added back at {0}".format(node_back_time))

        self._wait_for_replication_to_catchup(timeout=600)

        self.verify_results()
        self._verify_bandwidth_usage(node=self.src_cluster.get_master_node(), end_time=failover_time, no_of_nodes=3)
        self._verify_bandwidth_usage(node=self.src_cluster.get_master_node(), event_time=failover_time, end_time=node_back_time, no_of_nodes=2)
        self._verify_bandwidth_usage(node=self.src_cluster.get_master_node(), event_time=node_back_time, no_of_nodes=3)

    def test_nwusage_with_auto_failover_and_bwthrottle_enabled_later(self):
        self.setup_xdcr()

        self.src_cluster.rebalance_in()

        self.src_cluster.pause_all_replications()

        gen_create = BlobGenerator('nwOne', 'nwOne', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.src_cluster.resume_all_replications()

        self.sleep(15)

        shell = RemoteMachineShellConnection(self._input.servers[1])
        shell.stop_couchbase()
        self.sleep(45)
        task = self.cluster.async_rebalance(self.src_cluster.get_nodes(), [], [])
        task.result()
        FloatingServers._serverlist.append(self._input.servers[1])

        self.sleep(15)

        nw_limit = self._input.param("nw_limit", 1)
        self._set_nwusage_limit(self.src_cluster, nw_limit)
        bw_enable_time = time.strftime('%Y-%m-%dT%H:%M:%S')
        self.log.info("Bandwidth throttler enabled at {0}".format(bw_enable_time))

        self.sleep(60)

        shell.start_couchbase()
        shell.disable_firewall()
        self.sleep(30)
        self.src_cluster.rebalance_in()
        node_back_time = time.strftime('%Y-%m-%dT%H:%M:%S')
        self.log.info("Node added back at {0}".format(node_back_time))

        self._wait_for_replication_to_catchup(timeout=600)

        self.verify_results()
        self._verify_bandwidth_usage(node=self.src_cluster.get_master_node(), event_time=bw_enable_time, end_time=node_back_time, no_of_nodes=2)
        self._verify_bandwidth_usage(node=self.src_cluster.get_master_node(), event_time=node_back_time, no_of_nodes=3)