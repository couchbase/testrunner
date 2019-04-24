import datetime
import random
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
        self.num_src_nodes = len(self.src_cluster.get_nodes())
        self.num_dest_nodes = len(self.dest_cluster.get_nodes())

    def tearDown(self):
        super(nwusage, self).tearDown()

    def _get_nwusage_limit(self):
        # Pick random nw_limit between 1-100 MB
        return random.randint(1, 100)

    def _set_nwusage_limit(self, cluster, nw_limit):
        repl_id = cluster.get_remote_clusters()[0].get_replications()[0].get_repl_id()
        shell = RemoteMachineShellConnection(cluster.get_master_node())
        repl_id = str(repl_id).replace('/', '%2F')
        self.log.info("Network bandwidth is throttled at {0} MB".format(nw_limit))
        base_url = "http://" + cluster.get_master_node().ip + ":8091/settings/replications/" + repl_id
        command = "curl -X POST -u Administrator:password " + base_url + " -d networkUsageLimit=" + str(nw_limit)
        output, error = shell.execute_command(command)
        shell.log_command_output(output, error)

    def _set_doc_size_num(self):
        # Weighted randomization of doc sizes ranging from 500 KB to 20 MB
        self.doc_sizes = [500] * 10 + [10 ** 3] * 30 + [10 ** 4] * 20 + [10 ** 5] * 15 + \
                         [10 ** 6] * 10 + [10 ** 7] * 10 + [2 * 10 ** 7] * 5
        self._value_size = random.choice(self.doc_sizes)
        self._num_items = 10 ** 7
        self._temp = self._value_size
        # Decrease number of docs as size increases
        while self._temp > 10:
            self._temp /= 10
            self._num_items /= 10
        self._value_size *= self.num_src_nodes
        self._num_items *= self.num_src_nodes
        self.log.info("Doc size = {0} bytes, Number of docs = {1}".format(self._value_size, self._num_items))

    def _extract_timestamp(self, logmsg):
        # matches timestamp format : 2018-10-11T00:02:35
        timestamp_str = re.search(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', logmsg)
        timestamp = datetime.datetime.strptime(timestamp_str.group(), '%Y-%m-%dT%H:%M:%S')
        return timestamp

    '''Extract current, non zero bandwidth usage stats from logs'''

    def _extract_bandwidth_usage(self, node, time_to_compare, nw_max, nw_usage, end_time):
        valid_count = 0
        skip_count = 0
        matches, count = NodeHelper.check_goxdcr_log(node, "\\\"bandwidth_usage\\\": " + nw_usage,
                                                     print_matches=True, timeout=60)
        for item in matches:
            item_datetime = self._extract_timestamp(item)
            # Ignore entries that happened before the replication was set up
            if item_datetime < time_to_compare:
                skip_count += 1
                continue
            if end_time:
                end_datetime = self._extract_timestamp(end_time)
                if item_datetime > end_datetime:
                    skip_count += 1
                    continue
            bandwidth_usage = int(float(((item.split('"bandwidth_usage": ')[1]).split(' ')[0]).rstrip(',')))
            if bandwidth_usage > nw_max:
                self.fail(
                    "Bandwidth usage {0} is higher than Bandwidth limit {1} in {2}".format(bandwidth_usage, nw_max,
                                                                                           item))
            self.log.info("BANDWIDTH_USAGE ={0}".format(bandwidth_usage))
            if nw_usage == "0" and bandwidth_usage != 0:
                self.fail(
                    "Expecting bandwidth usage to be 0 but it is {0}".format(bandwidth_usage))
            valid_count += 1
        self.log.info("Stale entries :{0}, Valid entries :{1}".format(skip_count, valid_count))
        return valid_count

    def _extract_bandwith_quota(self, node):
        matches, count = NodeHelper.check_goxdcr_log(node, "bandwidth_usage_quota=" + "[0-9][0-9]*",
                                                     print_matches=True, timeout=60)
        bandwidth_quota = int(float(((matches[-1].split('bandwidth_usage_quota=')[1]).rstrip(' '))))
        return bandwidth_quota

    def _verify_bandwidth_usage(self, node, nw_limit, no_of_nodes, event_time=None, nw_usage="[0-9][0-9]*",
                                end_time=None):
        #nw_max = (nw_limit * 1024 * 1024) / no_of_nodes
        if event_time:
            time_to_compare = self._extract_timestamp(event_time)
        else:
            matches, count = NodeHelper.check_goxdcr_log(node, "Success adding replication specification",
                                                         print_matches=True, timeout=60)
            # Time when replication was set up
            if count > 0:
                time_to_compare = self._extract_timestamp(matches[-1])
            else:
                self.fail("Replication not successful")
        nw_max = self._extract_bandwith_quota(node)
        self.sleep(60, 'Waiting for bandwidth usage logs..')
        # Try 3 times to extract current bandwidth usage from logs
        iter = 0
        while iter < 3:
            self.sleep(30, 'Waiting for bandwidth usage logs..')
            valid_count = self._extract_bandwidth_usage(node, time_to_compare, nw_max, nw_usage, end_time)
            if valid_count == 0 and self._input.param("replication_type") == "capi" or nw_limit == 0:
                self.log.info("Bandwidth Throttler not enabled on replication as expected")
                break
            if valid_count > 0:
                break
            iter += 1
        else:
            self.fail("Bandwidth Throttler not enabled!")
        # Check if large docs are not getting stuck
        matches, src_count = NodeHelper.check_goxdcr_log(self.src_master, "The connection is ruined",
                                                         print_matches=True, timeout=10)
        if src_count:
            for item in matches:
                item_datetime = self._extract_timestamp(item)
                # Ignore errors that happened before the replication was set up
                if item_datetime < time_to_compare:
                    continue
                else:
                    self.fail("Possibly hit MB-31765")

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

        self._set_doc_size_num()
        nw_limit = self._input.param("nw_limit", self._get_nwusage_limit())
        self._set_nwusage_limit(self.src_cluster, nw_limit * self.num_src_nodes)

        gen_create = BlobGenerator('nwOne', 'nwOne', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.perform_update_delete()

        self.verify_results()
        self._verify_bandwidth_usage(node=self.src_cluster.get_master_node(), nw_limit=nw_limit,
                                     no_of_nodes=self.num_src_nodes)

    def test_nwusage_with_bidirection(self):
        self.setup_xdcr()
        self.sleep(60)

        self._set_doc_size_num()
        src_nw_limit = self._input.param("nw_limit", self._get_nwusage_limit())
        self._set_nwusage_limit(self.src_cluster, src_nw_limit * self.num_src_nodes)

        self._set_doc_size_num()
        dest_nw_limit = self._input.param("nw_limit", self._get_nwusage_limit())
        self._set_nwusage_limit(self.dest_cluster, dest_nw_limit * self.num_dest_nodes)

        gen_create1 = BlobGenerator('nwOne', 'nwOne', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create1)
        gen_create2 = BlobGenerator('nwTwo', 'nwTwo', self._value_size, end=self._num_items)
        self.dest_cluster.load_all_buckets_from_generator(kv_gen=gen_create2)

        self.perform_update_delete()

        self.verify_results()
        self._verify_bandwidth_usage(node=self.src_cluster.get_master_node(), nw_limit=src_nw_limit,
                                     no_of_nodes=self.num_src_nodes)
        self._verify_bandwidth_usage(node=self.dest_cluster.get_master_node(), nw_limit=dest_nw_limit,
                                     no_of_nodes=self.num_dest_nodes)

    def test_nwusage_with_unidirection_pause_resume(self):
        self.setup_xdcr()

        gen_create = BlobGenerator('nwOne', 'nwOne', self._value_size, end=self._num_items)
        tasks = self.src_cluster.async_load_all_buckets_from_generator(kv_gen=gen_create)

        self.src_cluster.pause_all_replications()

        self._set_doc_size_num()
        nw_limit = self._input.param("nw_limit", self._get_nwusage_limit())
        self._set_nwusage_limit(self.src_cluster, nw_limit * self.num_src_nodes)

        self.src_cluster.resume_all_replications()

        for task in tasks:
            task.result()

        self._wait_for_replication_to_catchup()

        self.verify_results()
        self._verify_bandwidth_usage(node=self.src_cluster.get_master_node(), nw_limit=nw_limit,
                                     no_of_nodes=self.num_src_nodes)

    def test_nwusage_with_bidirection_pause_resume(self):
        self.setup_xdcr()

        gen_create1 = BlobGenerator('nwOne', 'nwOne', self._value_size, end=self._num_items)
        tasks = self.src_cluster.async_load_all_buckets_from_generator(kv_gen=gen_create1)
        gen_create2 = BlobGenerator('nwTwo', 'nwTwo', self._value_size, end=self._num_items)
        tasks.extend(self.dest_cluster.async_load_all_buckets_from_generator(kv_gen=gen_create2))

        self.src_cluster.pause_all_replications()
        self.dest_cluster.pause_all_replications()

        self._set_doc_size_num()
        nw_limit = self._input.param("nw_limit", self._get_nwusage_limit())
        self._set_nwusage_limit(self.src_cluster, nw_limit * self.num_src_nodes)
        self._set_nwusage_limit(self.dest_cluster, nw_limit * self.num_dest_nodes)

        self.src_cluster.resume_all_replications()
        self.dest_cluster.resume_all_replications()

        for task in tasks:
            task.result()

        self._wait_for_replication_to_catchup()

        self.verify_results()
        self._verify_bandwidth_usage(node=self.src_cluster.get_master_node(), nw_limit=nw_limit,
                                     no_of_nodes=self.num_src_nodes)
        self._verify_bandwidth_usage(node=self.dest_cluster.get_master_node(), nw_limit=nw_limit,
                                     no_of_nodes=self.num_dest_nodes)

    def test_nwusage_with_unidirection_in_parallel(self):
        self.setup_xdcr()

        gen_create = BlobGenerator('nwOne', 'nwOne', self._value_size, end=self._num_items)
        tasks = self.src_cluster.async_load_all_buckets_from_generator(kv_gen=gen_create)

        self._set_doc_size_num()
        nw_limit = self._input.param("nw_limit", self._get_nwusage_limit())
        self._set_nwusage_limit(self.src_cluster, nw_limit * self.num_src_nodes)

        for task in tasks:
            task.result()

        self._wait_for_replication_to_catchup()

        self.verify_results()
        self._verify_bandwidth_usage(node=self.src_cluster.get_master_node(), nw_limit=nw_limit,
                                     no_of_nodes=self.num_src_nodes)

    def test_nwusage_with_bidirection_in_parallel(self):
        self.setup_xdcr()

        gen_create1 = BlobGenerator('nwOne', 'nwOne', self._value_size, end=self._num_items)
        tasks = self.src_cluster.async_load_all_buckets_from_generator(kv_gen=gen_create1)
        gen_create2 = BlobGenerator('nwTwo', 'nwTwo', self._value_size, end=self._num_items)
        tasks.extend(self.dest_cluster.async_load_all_buckets_from_generator(kv_gen=gen_create2))

        self._set_doc_size_num()
        nw_limit = self._input.param("nw_limit", self._get_nwusage_limit())
        self._set_nwusage_limit(self.src_cluster, nw_limit * self.num_src_nodes)
        self._set_nwusage_limit(self.dest_cluster, nw_limit * self.num_dest_nodes)

        for task in tasks:
            task.result()

        self._wait_for_replication_to_catchup()

        self.verify_results()
        self._verify_bandwidth_usage(node=self.src_cluster.get_master_node(), nw_limit=nw_limit,
                                     no_of_nodes=self.num_src_nodes)
        self._verify_bandwidth_usage(node=self.dest_cluster.get_master_node(), nw_limit=nw_limit,
                                     no_of_nodes=self.num_dest_nodes)

    def test_nwusage_with_rebalance_in(self):
        self.setup_xdcr()
        self.sleep(60)
        no_of_nodes = self.num_src_nodes + 1
        self._set_doc_size_num()
        nw_limit = self._input.param("nw_limit", self._get_nwusage_limit())
        self._set_nwusage_limit(self.src_cluster, nw_limit * no_of_nodes)

        gen_create = BlobGenerator('nwOne', 'nwOne', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.src_cluster.rebalance_in()

        self.perform_update_delete()

        self.verify_results()
        self._verify_bandwidth_usage(node=self.src_cluster.get_master_node(), nw_limit=nw_limit,
                                     no_of_nodes=no_of_nodes)

    def test_nwusage_with_rebalance_out(self):
        self.setup_xdcr()
        self.sleep(60)
        no_of_nodes = self.num_src_nodes - 1
        self._set_doc_size_num()
        nw_limit = self._input.param("nw_limit", self._get_nwusage_limit())
        self._set_nwusage_limit(self.src_cluster, nw_limit * no_of_nodes)

        gen_create = BlobGenerator('nwOne', 'nwOne', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.src_cluster.rebalance_out()

        self.perform_update_delete()

        self.verify_results()
        self._verify_bandwidth_usage(node=self.src_cluster.get_master_node(), nw_limit=nw_limit,
                                     no_of_nodes=no_of_nodes)

    def test_nwusage_reset_to_zero(self):
        self.setup_xdcr()
        self.sleep(60)

        self._set_doc_size_num()
        nw_limit = self._input.param("nw_limit", self._get_nwusage_limit())
        self._set_nwusage_limit(self.src_cluster, nw_limit * self.num_src_nodes)

        gen_create = BlobGenerator('nwOne', 'nwOne', self._value_size, end=self._num_items)
        tasks = self.src_cluster.async_load_all_buckets_from_generator(kv_gen=gen_create)

        self.sleep(30)
        self._set_nwusage_limit(self.src_cluster, 0)
        event_time = self._get_current_time(self.src_cluster.get_master_node())
        self.log.info("Network limit reset to 0 at {0}".format(event_time))

        for task in tasks:
            task.result()

        self.verify_results()
        self._verify_bandwidth_usage(node=self.dest_cluster.get_master_node(), nw_limit=nw_limit,
                                     no_of_nodes=self.num_dest_nodes, end_time=event_time)
        self._verify_bandwidth_usage(node=self.src_cluster.get_master_node(), nw_limit=0,
                                     no_of_nodes=self.num_src_nodes,
                                     event_time=event_time, nw_usage="0")

    def test_nwusage_with_hard_failover_and_bwthrottle_enabled(self):
        self.setup_xdcr()
        self.sleep(60)
        self._set_doc_size_num()
        nw_limit = self._input.param("nw_limit", self._get_nwusage_limit())
        self._set_nwusage_limit(self.src_cluster, nw_limit * self.num_src_nodes)

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
        self._verify_bandwidth_usage(node=self.src_cluster.get_master_node(), event_time=failover_time,
                                     end_time=node_back_time, no_of_nodes=self.num_src_nodes - 1)
        self._verify_bandwidth_usage(node=self.src_cluster.get_master_node(), event_time=node_back_time,
                                     no_of_nodes=self.num_src_nodes)

    def test_nwusage_with_hard_failover_and_bwthrottle_enabled_later(self):
        self.setup_xdcr()
        self.sleep(60)
        self._set_doc_size_num()

        self.src_cluster.pause_all_replications()

        gen_create = BlobGenerator('nwOne', 'nwOne', self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.src_cluster.resume_all_replications()

        self.sleep(15)

        self.src_cluster.failover_and_rebalance_nodes()

        self.sleep(15)

        nw_limit = self._input.param("nw_limit", self._get_nwusage_limit())
        self._set_nwusage_limit(self.src_cluster, nw_limit * self.num_src_nodes)
        bw_enable_time = self._get_current_time(self.src_cluster.get_master_node())
        self.log.info("Bandwidth throttler enabled at {0}".format(bw_enable_time))

        self.sleep(60)

        self.src_cluster.rebalance_in()
        node_back_time = self._get_current_time(self.src_cluster.get_master_node())
        self.log.info("Node added back at {0}".format(node_back_time))

        self._wait_for_replication_to_catchup(timeout=600)

        self.verify_results()
        self._verify_bandwidth_usage(node=self.src_cluster.get_master_node(), event_time=bw_enable_time,
                                     end_time=node_back_time, no_of_nodes=self.num_src_nodes - 1)
        self._verify_bandwidth_usage(node=self.src_cluster.get_master_node(), no_of_nodes=self.num_src_nodes,
                                     event_time=node_back_time)

    def test_nwusage_with_auto_failover_and_bwthrottle_enabled(self):
        self.setup_xdcr()
        self.sleep(60)
        self._set_doc_size_num()
        self.src_cluster.rebalance_in()

        nw_limit = self._input.param("nw_limit", self._get_nwusage_limit())
        self._set_nwusage_limit(self.src_cluster, nw_limit * self.num_src_nodes)

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
        self._verify_bandwidth_usage(node=self.src_cluster.get_master_node(), event_time=failover_time,
                                     end_time=node_back_time, no_of_nodes=2)
        self._verify_bandwidth_usage(node=self.src_cluster.get_master_node(), event_time=node_back_time, no_of_nodes=3)

    def test_nwusage_with_auto_failover_and_bwthrottle_enabled_later(self):
        self.setup_xdcr()
        self.sleep(60)
        self._set_doc_size_num()

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

        nw_limit = self._input.param("nw_limit", self._get_nwusage_limit())
        self._set_nwusage_limit(self.src_cluster, nw_limit * self.num_src_nodes)
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
        self._verify_bandwidth_usage(node=self.src_cluster.get_master_node(), event_time=bw_enable_time,
                                     end_time=node_back_time, no_of_nodes=self.num_src_nodes)
        self._verify_bandwidth_usage(node=self.src_cluster.get_master_node(), event_time=node_back_time,
                                     no_of_nodes=self.num_src_nodes + 1)
