import datetime
import re

from couchbase_helper.cluster import Cluster
from couchbase_helper.documentgenerator import BlobGenerator
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection

from xdcrnewbasetests import XDCRNewBaseTest, NodeHelper, FloatingServers


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

    def _extract_timestamp(self, logmsg):
        #matches timestamp format : 2018-10-11T00:02:35
        timestamp_str = re.search(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', logmsg)
        timestamp = datetime.datetime.strptime(timestamp_str.group(), '%Y-%m-%dT%H:%M:%S')
        return timestamp

    def _verify_bandwidth_usage(self, node, nw_limit=1, no_of_nodes=2, event_time=None,
                                nw_usage="[1-9][0-9]*", end_time=None):
        goxdcr_log = NodeHelper.get_goxdcr_log_dir(node) + '/goxdcr.log'
        nw_max = (nw_limit * 1024 * 1024)/no_of_nodes
        if event_time:
            time_to_compare = self._extract_timestamp(event_time)
        else:
            matches, count = NodeHelper.check_goxdcr_log(node, "Success adding replication specification",
                                                 goxdcr_log, print_matches=True, timeout=60)
            #Time when replication was set up
            if count > 0:
                time_to_compare = self._extract_timestamp(matches[-1])
            else:
                self.fail("Replication not successful")
        self.sleep(60,'Waiting for bandwidth usage logs..')
        matches, count = NodeHelper.check_goxdcr_log(node, "\\\"bandwidth_usage\\\": " + nw_usage, goxdcr_log, print_matches=True, timeout=60)
        if count == 0:
            self.fail("Bandwidth usage information not found in logs!")
        match_count = 0
        skip_count = 0
        for item in matches:
            item_datetime = self._extract_timestamp(item)
            #Ignore entries that happened before the replication was set up
            if item_datetime < time_to_compare:
                skip_count += 1
                continue
            if end_time:
                end_datetime = self._extract_timestamp(end_time)
                if item_datetime > end_datetime:
                    skip_count += 1
                    continue
            bandwidth_usage = ((item.split('{"bandwidth_usage": ')[1]).split(' ')[0]).rstrip(',')
            if int(float(bandwidth_usage)) <= nw_max:
                match_count += 1
                continue
            else:
                self.fail("Bandwidth usage {0} is higher than Bandwidth limit {1} in {2}".format(bandwidth_usage,nw_max,item))

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

    def _get_current_time(self, server):
        shell = RemoteMachineShellConnection(server)
        command = "date +'%Y-%m-%dT%H:%M:%S'"
        output, error = shell.execute_command(command)
        shell.log_command_output(output, error)
        curr_time = output[0].strip()
        return curr_time

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
        event_time = self._get_current_time(self.src_cluster.get_master_node())
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
        failover_time = self._get_current_time(self.src_cluster.get_master_node())
        self.log.info("Node failed over at {0}".format(failover_time))

        self.sleep(15)

        self.src_cluster.rebalance_in()
        node_back_time = self._get_current_time(self.src_cluster.get_master_node())
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
        bw_enable_time = self._get_current_time(self.src_cluster.get_master_node())
        self.log.info("Bandwidth throttler enabled at {0}".format(bw_enable_time))

        self.sleep(60)

        self.src_cluster.rebalance_in()
        node_back_time = self._get_current_time(self.src_cluster.get_master_node())
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
        failover_time = self._get_current_time(self.src_cluster.get_master_node())
        self.log.info("Node auto failed over at {0}".format(failover_time))
        FloatingServers._serverlist.append(self._input.servers[1])

        self.sleep(15)

        shell.start_couchbase()
        shell.disable_firewall()
        self.sleep(45)
        self.src_cluster.rebalance_in()
        node_back_time = self._get_current_time(self.src_cluster.get_master_node())
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
        bw_enable_time = self._get_current_time(self.src_cluster.get_master_node())
        self.log.info("Bandwidth throttler enabled at {0}".format(bw_enable_time))

        self.sleep(60)

        shell.start_couchbase()
        shell.disable_firewall()
        self.sleep(30)
        self.src_cluster.rebalance_in()
        node_back_time = self._get_current_time(self.src_cluster.get_master_node())
        self.log.info("Node added back at {0}".format(node_back_time))

        self._wait_for_replication_to_catchup(timeout=600)

        self.verify_results()
        self._verify_bandwidth_usage(node=self.src_cluster.get_master_node(), event_time=bw_enable_time, end_time=node_back_time, no_of_nodes=2)
        self._verify_bandwidth_usage(node=self.src_cluster.get_master_node(), event_time=node_back_time, no_of_nodes=3)