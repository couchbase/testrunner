from xdcr.xdcrbasetests import XDCRReplicationBaseTest
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection, RestHelper
from membase.api.exception import CBRecoveryFailedException, InvalidArgumentException, BucketCreationException
from testconstants import STANDARD_BUCKET_PORT
import time


class CBRbaseclass(XDCRReplicationBaseTest):
    def _autofail_enable(self, _rest_):
        status = _rest_.update_autofailover_settings(True, self.wait_timeout // 2)
        if not status:
            self.log.info('failed to change autofailover_settings!')
            return
        # read settings and verify
        settings = _rest_.get_autofailover_settings()
        self.assertEqual(settings.enabled, True)

    def _autofail_disable(self, _rest_):
        status = _rest_.update_autofailover_settings(False, self.wait_timeout // 2)
        if not status:
            self.log.info('failed to change autofailover_settings!')
            return
        # read settings and verify
        settings = _rest_.get_autofailover_settings()
        self.assertEqual(settings.enabled, False)

    def wait_for_failover_or_assert(self, master, autofailover_count, timeout):
        time_start = time.time()
        time_max_end = time_start + 300
        failover_count = 0
        while time.time() < time_max_end:
            failover_count = self.get_failover_count(master)
            if failover_count == autofailover_count:
                break
            self.sleep(30)

        if failover_count != autofailover_count:
            rest = RestConnection(master)
            self.log.warn("pools/default from {0} : {1}".format(master.ip, rest.cluster_status()))
            self.fail("{0} node(s) failed over, expected {1} in {2} seconds".
                            format(failover_count, autofailover_count, time.time() - time_start))
        else:
            self.log.info("{0} node(s) failed over as expected".format(failover_count))

    def get_failover_count(self, master):
        rest = RestConnection(master)
        cluster_status = rest.cluster_status()

        failover_count = 0
        # check for inactiveFailed
        for node in cluster_status['nodes']:
            self.log.info("'clusterMembership' for node {0} is {1}".format(node["otpNode"], node['clusterMembership']))
            if node['clusterMembership'] == "inactiveFailed":
                failover_count += 1

        return failover_count

    def wait_for_catchup(self, _healthy_, _compromised_, bucket):
        start = time.time()
        _flag = False
        rest1 = RestConnection(_healthy_)
        rest2 = RestConnection(_compromised_)
        while time.time() - start < 60:
            _count1 = rest1.fetch_bucket_stats(bucket=bucket)["op"]["samples"]["curr_items"][-1]
            _count2 = rest2.fetch_bucket_stats(bucket=bucket)["op"]["samples"]["curr_items"][-1]
            if _count1 == _count2:
                self.log.info("Cbrecovery caught up bucket {0}... {1} == {2}".format(bucket, _count1, _count2))
                _flag = True
                break
            self.log.warn("Waiting for cbrecovery to catch up bucket {0}... {1} != {2}".format(bucket, _count1, _count2))
            self.sleep(self.wait_timeout)
        return _flag

    def cbr_routine(self, _healthy_, _compromised_, wait_completed=True):
        tasks = []
        for bucket in self._get_cluster_buckets(_compromised_):
            tasks.append(self.cluster.async_cbrecovery(_healthy_, _compromised_, bucket_src=bucket, bucket_dest=bucket, username=_healthy_.rest_username, password=_healthy_.rest_password,
                 username_dest=_compromised_.rest_username, password_dest=_compromised_.rest_password, verbose=False, wait_completed=wait_completed))
        for task in tasks:
            task.result()

        if not wait_completed:
            return

        _check = True

        for bucket in self._get_cluster_buckets(_compromised_):
            _check += self.wait_for_catchup(_healthy_, _compromised_, bucket.name)
        if not _check:
            raise CBRecoveryFailedException("not all items were recovered. see logs above")

    def trigger_rebalance(self, rest, rebalance_reached=100):
        _nodes_ = rest.node_statuses()
        rest.rebalance(otpNodes=[node.id for node in _nodes_], ejectedNodes=[])
        reached = RestHelper(rest).rebalance_reached(rebalance_reached)
        self.assertTrue(reached, "rebalance failed, stuck or did not completed with expected progress {0}".format(rebalance_reached))

    def auto_fail_over(self, master):
        _count_ = 1
        rest = RestConnection(master)
        if "stop_server" in self.failover_reason:
            for node in self.failed_nodes:
                """
                Autofailover will not auto failover nodes, if it could
                result in data loss, so force failover
                """
                if _count_ > self._num_replicas:
                    self.sleep(10)
                    for item in rest.node_statuses():
                        if node.ip == item.ip:
                            rest.fail_over(item.id)
                            break
                    _count_ += 1
                    continue
                shell = RemoteMachineShellConnection(node)
                shell.stop_couchbase()
                shell.disconnect()
                self.wait_for_failover_or_assert(master, _count_, self.wait_timeout)
                rest.reset_autofailover()
                _count_ += 1

        elif "firewall_block" in self.failover_reason:
            for node in self.failed_nodes:
                """
                Autofailover will not auto failover nodes, if it could
                result in data loss, so force failover
                """
                if _count_ > self._num_replicas:
                    time.sleep(10)
                    for item in rest.node_statuses():
                        if node.ip == item.ip:
                            rest.fail_over(item.id)
                            break
                    _count_ += 1
                    continue
                shell = RemoteMachineShellConnection(node)
                o, r = shell.execute_command("/sbin/iptables -A INPUT -p tcp -i eth0 --dport 1000:65535 -j REJECT")
                shell.disconnect()
                self.wait_for_failover_or_assert(master, _count_, self.wait_timeout)
                rest.reset_autofailover()
                _count_ += 1

    def vbucket_map_checker(self, map_before, map_after, initial_set, final_set):
        """
        map_before: initial vbucket_map
        map_after: changed vbucket_map
        initial_set: no. of nodes in the cluster when initial vbucket_map considered
        final_set: no. of nodes in the cluster when changed vbucket_map considered
        """
        pre_dict = {}
        post_dict = {}

        """
        pre_dict and post_dict:
        Dictionaries that are to contain information about
        number of vbuckets on each node in cluster
        """
        pre_dict = dict((i, 0) for i in range(initial_set))
        post_dict = dict((i, 0) for i in range(final_set))

        for i in map_before:
            for j in range(initial_set):
                if i[0] == j:
                    pre_dict[j] += 1

        for i in map_after:
            for j in range(final_set):
                if i[0] == j:
                    post_dict[j] += 1

        if len(pre_dict) != len(post_dict):
            return False

        for i in pre_dict:
            for j in post_dict:
                if i == j:
                    if pre_dict[i] != post_dict[j]:
                        return False
        return True

# Assumption that at least 4 nodes on every cluster
class cbrecovery(CBRbaseclass, XDCRReplicationBaseTest):
    def setUp(self):
        super(cbrecovery, self).setUp()
        self._failover_count = self._input.param("fail_count", 0)
        self._add_count = self._input.param("add_count", 0)
        self.failover_reason = self._input.param("failover_reason", "stop_server")  # or firewall_block
        self.flag_val = self._input.param("setflag", 0)
        self.failed_nodes = []
        self._ifautofail = False
        for server in self._servers:
            rest = RestConnection(server)
            rest.reset_autofailover()
            shell = RemoteMachineShellConnection(server)
            o, r = shell.execute_command("iptables -F")
            shell.log_command_output(o, r)
            shell.disconnect()

    def tearDown(self):
        if self._ifautofail:
            if "stop_server" in self.failover_reason:
                for node in self.failed_nodes:
                    shell = RemoteMachineShellConnection(node)
                    shell.start_couchbase()
                    shell.disconnect()
            elif "firewall_block" in self.failover_reason:
                for node in self.failed_nodes:
                    shell = RemoteMachineShellConnection(node)
                    o, r = shell.execute_command("iptables -F")
                    shell.log_command_output(o, r)
                    o, r = shell.execute_command("/sbin/iptables -A INPUT -p tcp -i eth0 --dport 1000:65535 -j ACCEPT")
                    shell.log_command_output(o, r)
                    shell.disconnect()
            self.sleep(20)
        super(cbrecovery, self).tearDown()

    def suite_setUp(self):
        self.log.info("*** cbrecovery: suite_setUp() ***")

    def suite_tearDown(self):
        self.log.info("*** cbrecovery: suite_tearDown() ***")

    def common_preSetup(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0, flag=self.flag_val)
        tasks = []
        if self._doc_ops is not None:
            if "update" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_update, "update", self._expires))
            if "delete" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_delete, "delete", 0))
        for task in tasks:
            task.result()

        self._wait_for_replication_to_catchup()
        """
        Tracking vbucket movement just on the default bucket for now
        """
        self.vbucket_map_before = []
        self.initial_node_count = 0
        self.vbucket_map_after = []
        self.final_node_count = 0

    def common_tearDown_verification(self):
        if self._failover is not None:
            # TOVERIFY: Check if vbucket map unchanged if swap rebalance
            if self._default_bucket:
                if self._failover_count == self._add_count:
                    _flag_ = self.vbucket_map_checker(self.vbucket_map_before, self.vbucket_map_after, self.initial_node_count, self.final_node_count)
                    if _flag_:
                        self.log.info("vbucket_map same as earlier")
                    else:
                        self.log.info("vbucket_map differs from earlier")

        self.sleep(self.wait_timeout // 2)
        self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
        self.verify_results()


    def restart_cbrecover_multiple_failover_swapout_reb_routine(self):
        self.common_preSetup()
        when_step = self._input.param("when_step", "recovery_when_rebalance")
        if self._failover is not None:
            if "source" in self._failover:
                rest = RestConnection(self.src_master)
                if self._default_bucket:
                    self.initial_node_count = len(self.src_nodes)
                    self.vbucket_map_before = rest.fetch_vbucket_map()  # JUST FOR DEFAULT BUCKET AS OF NOW
                if self._failover_count >= len(self.src_nodes):
                    raise Exception("Won't failover .. count exceeds available servers on source : SKIPPING TEST")
                if len(self._floating_servers_set) < self._add_count:
                    raise Exception("Not enough spare nodes available, to match the failover count : SKIPPING TEST")
                self.log.info("Failing over {0} nodes on source ..".format(self._failover_count))
                self.failed_nodes = self.src_nodes[(len(self.src_nodes) - self._failover_count):len(self.src_nodes)]
                self.cluster.failover(self.src_nodes, self.failed_nodes)
                for node in self.failed_nodes:
                    self.src_nodes.remove(node)
                add_nodes = self._floating_servers_set[0:self._add_count]
                for node in add_nodes:
                    rest.add_node(user=node.rest_username, password=node.rest_password, remoteIp=node.ip, port=node.port)
                self.src_nodes.extend(add_nodes)
                self.sleep(self.wait_timeout // 4)
                # CALL THE CBRECOVERY ROUTINE WITHOUT WAIT FOR COMPLETED
                self.cbr_routine(self.dest_master, self.src_master, False)

                if "create_bucket_when_recovery" in when_step:
                     name = 'standard_bucket'
                     try:
                         standard_params=self._create_bucket_params(server=self.src_master, size=100, replicas=1)
                         self._create_standard_bucket(name=name, port=STANDARD_BUCKET_PORT+10,
                                                             bucket_params=standard_params)
                     except BucketCreationException as e:
                         self.log.info("bucket creation failed during cbrecovery as expected")
                     # but still able to create bucket on destination
                     standard_params = self._create_bucket_params(server=self.dest_master, size=100, replicas=1)
                     self.cluster.create_standard_bucket(name=name, port=STANDARD_BUCKET_PORT + 10,
                                                         bucket_params=standard_params)
                     # here we try to re-call cbrecovery(seems it's supported even it's still running)
                     # if recovery fast(=completed) we can get "No recovery needed"
                     self.cbr_routine(self.dest_master, self.src_master)
                elif "recovery_when_rebalance" in when_step:
                    rest.remove_all_recoveries()
                    self.trigger_rebalance(rest, 15)
                    try:
                        self.cbr_routine(self.dest_master, self.src_master)
                        self.log.exception("cbrecovery should be failed when rebalance is in progress")
                    except CBRecoveryFailedException as e:
                        self.log.info("cbrecovery failed  as expected when there are no failovered nodes")
                    reached = RestHelper(rest).rebalance_reached()
                    self.assertTrue(reached, "rebalance failed or did not completed")
                    if self._replication_direction_str == "unidirection":
                        self.log.warn("we expect data lost on source cluster with unidirection replication")
                        self.log.warn("verification data will be skipped")
                        return
                elif "recovery_when_rebalance_stopped" in when_step:
                    rest.remove_all_recoveries()
                    self.trigger_rebalance(rest, 15)
                    rest.stop_rebalance()
                    try:
                        self.cbr_routine(self.dest_master, self.src_master)
                        self.log.exception("cbrecovery should be failed when rebalance has been stopped")
                    except CBRecoveryFailedException as e:
                        self.log.info("cbrecovery failed  as expected when there are no failovered nodes")
                elif "rebalance_when_recovering" in when_step:
                    # try to call  rebalance during cbrecovery
                    try:
                        self.trigger_rebalance(rest)
                        self.log.exception("rebalance is not permitted during cbrecovery")
                    except InvalidArgumentException as e:
                        self.log.info("can't call rebalance during cbrecovery as expected")
                    # here we try to re-call cbrecovery(seems it's supported even it's still running)
                    self.cbr_routine(self.dest_master, self.src_master)
                if self._default_bucket:
                    self.vbucket_map_after = rest.fetch_vbucket_map()
                    self.final_node_count = len(self.src_nodes)

            elif "destination" in self._failover:
                rest = RestConnection(self.dest_master)
                if self._default_bucket:
                    self.initial_node_count = len(self.dest_nodes)
                    self.vbucket_map_before = rest.fetch_vbucket_map()  # JUST FOR DEFAULT BUCKET AS OF NOW
                if self._failover_count >= len(self.dest_nodes):
                    raise Exception("Won't failover .. count exceeds available servers on sink : SKIPPING TEST")
                if len(self._floating_servers_set) < self._add_count:
                    raise Exception("Not enough spare nodes available, to match the failover count : SKIPPING TEST")
                self.log.info("Failing over {0} nodes on destination ..".format(self._failover_count))
                self.failed_nodes = self.dest_nodes[(len(self.dest_nodes) - self._failover_count):len(self.dest_nodes)]
                self.cluster.failover(self.dest_nodes, self.failed_nodes)
                for node in self.failed_nodes:
                    self.dest_nodes.remove(node)
                add_nodes = self._floating_servers_set[0:self._add_count]
                for node in add_nodes:
                    rest.add_node(user=node.rest_username, password=node.rest_password, remoteIp=node.ip, port=node.port)
                self.dest_nodes.extend(add_nodes)
                self.sleep(self.wait_timeout // 4)
                # CALL THE CBRECOVERY ROUTINE
                self.cbr_routine(self.src_master, self.dest_master, False)

                if "create_bucket_when_recovery" in when_step:
                     name = 'standard_bucket'
                     try:
                         standard_params=self._create_bucket_params(server=self.dest_master, size=100, replicas=1)
                         self.cluster.create_standard_bucket(name=name, port=STANDARD_BUCKET_PORT + 10,
                                                             bucket_params=standard_params)
                     except BucketCreationException as e:
                         self.log.info("bucket creation failed during cbrecovery as expected")
                     standard_params = self._create_bucket_params(server=self.src_master, size=100, replicas=1)
                     self.cluster.create_standard_bucket(name=name, port=STANDARD_BUCKET_PORT + 10,
                                                         bucket_params=standard_params)
                     self.cbr_routine(self.src_master, self.dest_master)
                elif "recovery_when_rebalance" in when_step:
                    rest.remove_all_recoveries()
                    self.trigger_rebalance(rest, 15)
                    try:
                        self.cbr_routine(self.src_master, self.dest_master)
                        self.log.exception("cbrecovery should be failed when rebalance is in progress")
                    except CBRecoveryFailedException as e:
                        self.log.info("cbrecovery failed  as expected when there are no failovered nodes")
                    reached = RestHelper(rest).rebalance_reached()
                    self.assertTrue(reached, "rebalance failed or did not completed")
                elif "recovery_when_rebalance_stopped" in when_step:
                    rest.remove_all_recoveries()
                    self.trigger_rebalance(rest, 15)
                    rest.stop_rebalance()
                    try:
                        self.cbr_routine(self.src_master, self.dest_master)
                        self.log.exception("cbrecovery should be failed when rebalance has been stopped")
                    except CBRecoveryFailedException as e:
                        self.log.info("cbrecovery failed  as expected when there are no failovered nodes")
                elif "rebalance_when_recovering" in when_step:
                    # try to call  rebalance during cbrecovery
                    try:
                        self.trigger_rebalance(rest)
                        self.log.exception("rebalance is not permitted during cbrecovery")
                    except InvalidArgumentException as e:
                        self.log.info("can't call rebalance during cbrecovery as expected")
                    # here we try to re-call cbrecovery(seems it's supported even it's still running)
                    self.cbr_routine(self.src_master, self.dest_master)

                if self._default_bucket:
                    self.vbucket_map_after = rest.fetch_vbucket_map()
                    self.final_node_count = len(self.dest_nodes)

        self.trigger_rebalance(rest)
        self.common_tearDown_verification()


    def cbrecover_multiple_failover_swapout_reb_routine(self):
        self.common_preSetup()
        if self._failover is not None:
            if "source" in self._failover:
                rest = RestConnection(self.src_master)
                if self._default_bucket:
                    self.initial_node_count = len(self.src_nodes)
                    self.vbucket_map_before = rest.fetch_vbucket_map()  # JUST FOR DEFAULT BUCKET AS OF NOW
                if self._failover_count >= len(self.src_nodes):
                    raise Exception("Won't failover .. count exceeds available servers on source : SKIPPING TEST")
                if len(self._floating_servers_set) < self._add_count:
                    raise Exception("Not enough spare nodes available, to match the failover count : SKIPPING TEST")
                self.log.info("Failing over {0} nodes on source ..".format(self._failover_count))
                self.failed_nodes = self.src_nodes[(len(self.src_nodes) - self._failover_count):len(self.src_nodes)]
                self.cluster.failover(self.src_nodes, self.failed_nodes)
                for node in self.failed_nodes:
                    self.src_nodes.remove(node)
                add_nodes = self._floating_servers_set[0:self._add_count]
                for node in add_nodes:
                    rest.add_node(user=node.rest_username, password=node.rest_password, remoteIp=node.ip, port=node.port)
                self.src_nodes.extend(add_nodes)
                self.sleep(self.wait_timeout // 4)
                # CALL THE CBRECOVERY ROUTINE
                self.cbr_routine(self.dest_master, self.src_master)

                self.trigger_rebalance(rest)
                if self._default_bucket:
                    self.vbucket_map_after = rest.fetch_vbucket_map()
                    self.final_node_count = len(self.src_nodes)

            elif "destination" in self._failover:
                rest = RestConnection(self.dest_master)
                if self._default_bucket:
                    self.initial_node_count = len(self.dest_nodes)
                    self.vbucket_map_before = rest.fetch_vbucket_map()  # JUST FOR DEFAULT BUCKET AS OF NOW
                if self._failover_count >= len(self.dest_nodes):
                    raise Exception("Won't failover .. count exceeds available servers on sink : SKIPPING TEST")
                if len(self._floating_servers_set) < self._add_count:
                    raise Exception("Not enough spare nodes available, to match the failover count : SKIPPING TEST")
                self.log.info("Failing over {0} nodes on destination ..".format(self._failover_count))
                self.failed_nodes = self.dest_nodes[(len(self.dest_nodes) - self._failover_count):len(self.dest_nodes)]
                self.cluster.failover(self.dest_nodes, self.failed_nodes)
                for node in self.failed_nodes:
                    self.dest_nodes.remove(node)
                add_nodes = self._floating_servers_set[0:self._add_count]
                for node in add_nodes:
                    rest.add_node(user=node.rest_username, password=node.rest_password, remoteIp=node.ip, port=node.port)
                self.dest_nodes.extend(add_nodes)
                self.sleep(self.wait_timeout // 4)
                # CALL THE CBRECOVERY ROUTINE
                self.cbr_routine(self.src_master, self.dest_master)

                self.trigger_rebalance(rest)
                if self._default_bucket:
                    self.vbucket_map_after = rest.fetch_vbucket_map()
                    self.final_node_count = len(self.dest_nodes)

        self.common_tearDown_verification()


    def cbrecover_multiple_autofailover_swapout_reb_routine(self):
        self.common_preSetup()
        if self._failover is not None:
            self._ifautofail = True
            if "source" in self._failover:
                rest = RestConnection(self.src_master)
                if self._default_bucket:
                    self.initial_node_count = len(self.src_nodes)
                    self.vbucket_map_before = rest.fetch_vbucket_map()  # JUST FOR DEFAULT BUCKET AS OF NOW
                if self._failover_count >= len(self.src_nodes):
                    raise Exception("Won't failover .. count exceeds available servers on source : SKIPPING TEST")
                if len(self._floating_servers_set) < self._add_count:
                    raise Exception("Not enough spare nodes available, to match the failover count : SKIPPING TEST")

                self._autofail_enable(rest)
                self.log.info("Triggering {0} over {1} nodes on source ..".format(self.failover_reason, self._failover_count))
                self.failed_nodes = self.src_nodes[(len(self.src_nodes) - self._failover_count):len(self.src_nodes)]
                self.auto_fail_over(self.src_master)
                for node in self.failed_nodes:
                    self.src_nodes.remove(node)
                add_nodes = self._floating_servers_set[0:self._add_count]
                for node in add_nodes:
                    rest.add_node(user=node.rest_username, password=node.rest_password, remoteIp=node.ip, port=node.port)
                self.src_nodes.extend(add_nodes)
                self.sleep(self.wait_timeout // 4)
                # CALL THE CBRECOVERY ROUTINE
                self.cbr_routine(self.dest_master, self.src_master)

                self.trigger_rebalance(rest)
                if self._default_bucket:
                    self.vbucket_map_after = rest.fetch_vbucket_map()
                    self.initial_node_count = len(self.src_nodes)

                self._autofail_disable(rest)

            elif "destination" in self._failover:
                rest = RestConnection(self.dest_master)
                if self._default_bucket:
                    self.initial_node_count = len(self.dest_nodes)
                    self.vbucket_map_before = rest.fetch_vbucket_map()  # JUST FOR DEFAULT BUCKET AS OF NOW
                if self._failover_count >= len(self.dest_nodes):
                    raise Exception("Won't failover .. count exceeds available servers on source : SKIPPING TEST")
                if len(self._floating_servers_set) < self._add_count:
                    raise Exception("Not enough spare nodes available, to match the failover count : SKIPPING TEST")

                self._autofail_enable(rest)
                self.log.info("Triggering {0} over {1} nodes on destination ..".format(self.failover_reason, self._failover_count))
                self.failed_nodes = self.dest_nodes[(len(self.dest_nodes) - self._failover_count):len(self.dest_nodes)]
                self.auto_fail_over(self.dest_master)
                for node in self.failed_nodes:
                    self.dest_nodes.remove(node)
                add_nodes = self._floating_servers_set[0:self._add_count]
                for node in add_nodes:
                    rest.add_node(user=node.rest_username, password=node.rest_password, remoteIp=node.ip, port=node.port)
                self.dest_nodes.extend(add_nodes)
                self.sleep(self.wait_timeout // 4)
                # CALL THE CBRECOVERY ROUTINE
                self.cbr_routine(self.src_master, self.dest_master)

                self.trigger_rebalance(rest)
                if self._default_bucket:
                    self.vbucket_map_after = rest.fetch_vbucket_map()
                    self.final_node_count = len(self.dest_nodes)

                self._autofail_disable(rest)

        self.common_tearDown_verification()

    def cbrecover_multiple_failover_addback_routine(self):
        self.common_preSetup()
        if self._failover is not None:
            if "source" in self._failover:
                rest = RestConnection(self.src_master)
                if self._default_bucket:
                    self.initial_node_count = len(self.src_nodes)
                    self.vbucket_map_before = rest.fetch_vbucket_map()  # JUST FOR DEFAULT BUCKET AS OF NOW
                if self._failover_count >= len(self.src_nodes):
                    raise Exception("Won't failover .. count exceeds available servers on source : SKIPPING TEST")
                if len(self._floating_servers_set) < self._add_count:
                    raise Exception("Not enough spare nodes available, to match the failover count : SKIPPING TEST")
                self.log.info("Failing over {0} nodes on source ..".format(self._failover_count))
                self.failed_nodes = self.src_nodes[(len(self.src_nodes) - self._failover_count):len(self.src_nodes)]
                self.cluster.failover(self.src_nodes, self.failed_nodes)
                self.sleep(self.wait_timeout // 4)
                self.log.info("Adding back the {0} nodes that were failed over ..".format(self._failover_count))
                for node in self.failed_nodes:
                    self.adding_back_a_node(self.src_master, node)
                add_nodes = self._floating_servers_set[0:self._add_count]
                self.sleep(self.wait_timeout // 4)
                for node in add_nodes:
                    rest.add_node(user=node.rest_username, password=node.rest_password, remoteIp=node.ip, port=node.port)
                self.src_nodes.extend(add_nodes)
                # CALL THE CBRECOVERY ROUTINE
                self.cbr_routine(self.dest_master, self.src_master)

                self.trigger_rebalance(rest)
                if self._default_bucket:
                    self.vbucket_map_after = rest.fetch_vbucket_map()
                    self.final_node_count = len(self.src_nodes)

            elif "destination" in self._failover:
                rest = RestConnection(self.dest_master)
                if self._default_bucket:
                    self.initial_node_count = len(self.dest_nodes)
                    self.vbucket_map_before = rest.fetch_vbucket_map()  # JUST FOR DEFAULT BUCKET AS OF NOW
                if self._failover_count >= len(self.dest_nodes):
                    raise Exception("Won't failover .. count exceeds available servers on sink : SKIPPING TEST")
                if len(self._floating_servers_set) < self._add_count:
                    raise Exception("Not enough spare nodes available, to match the failover count : SKIPPING TEST")

                self.log.info("Failing over {0} nodes on destination ..".format(self._failover_count))
                self.failed_nodes = self.dest_nodes[(len(self.dest_nodes) - self._failover_count):len(self.dest_nodes)]
                self.cluster.failover(self.dest_nodes, self.failed_nodes)
                self.sleep(self.wait_timeout // 4)
                self.log.info("Adding back the {0} nodes that were failed over ..".format(self._failover_count))
                for node in self.failed_nodes:
                    self.adding_back_a_node(self.dest_master, node)
                add_nodes = self._floating_servers_set[0:self._add_count]
                self.sleep(self.wait_timeout // 4)
                for node in add_nodes:
                    rest.add_node(user=node.rest_username, password=node.rest_password, remoteIp=node.ip, port=node.port)
                self.dest_nodes.extend(add_nodes)
                # CALL THE CBRECOVERY ROUTINE
                self.cbr_routine(self.src_master, self.dest_master)

                self.trigger_rebalance(rest)
                if self._default_bucket:
                    self.vbucket_map_after = rest.fetch_vbucket_map()
                    self.final_node_count = len(self.dest_nodes)

        self.common_tearDown_verification()
