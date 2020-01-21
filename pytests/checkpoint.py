import re
import time
import uuid
import logger

from threading import Thread
from tasks.future import TimeoutError
from couchbase_helper.cluster import Cluster
from couchbase_helper.stats_tools import StatsCommon
from membase.api.rest_client import RestConnection
from membase.helper.cluster_helper import ClusterOperationHelper
from basetestcase import BaseTestCase
from couchbase_helper.documentgenerator import BlobGenerator
from remote.remote_util import RemoteMachineShellConnection
import testconstants

ACTIVE = "active"
REPLICA1 = "replica1"
REPLICA2 = "replica2"
REPLICA3 = "replica3"

class CheckpointTests(BaseTestCase):

    def setUp(self):
        super(CheckpointTests, self).setUp()
        self.checkpoint_size = self.input.param("checkpoint_size", 5000)
        self.value_size = self.input.param("value_size", 256)
        self.timeout = self.input.param("timeout", 60)
        servers_in = [self.servers[i + 1] for i in range(self.num_servers - 1)]
        self.cluster.rebalance(self.servers[:1], servers_in, [])
        self.bucket = self.buckets[0]
        self.master = self._get_server_by_state(self.servers[:self.num_servers], self.bucket, ACTIVE)
        if self.num_servers > 1:
            self.replica1 = self._get_server_by_state(self.servers[:self.num_servers], self.bucket, REPLICA1)
        if self.num_servers > 2:
            self.replica2 = self._get_server_by_state(self.servers[:self.num_servers], self.bucket, REPLICA2)
        if self.num_servers > 3:
            self.replica3 = self._get_server_by_state(self.servers[:self.num_servers], self.bucket, REPLICA3)

    def tearDown(self):
        super(CheckpointTests, self).tearDown()

    def checkpoint_create_items(self):
        """Load data until a new checkpoint is created on all replicas"""

        param = 'checkpoint'
        stat_key = 'vb_0:open_checkpoint_id'

        self._set_checkpoint_size(self.servers[:self.num_servers], self.bucket, str(self.checkpoint_size))
        chk_stats = StatsCommon.get_stats([self.master], self.bucket, param, stat_key)

        generate_load = BlobGenerator('nosql', 'nosql-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, generate_load, "create", 0, 1, 0, True, batch_size=self.checkpoint_size, pause_secs=5, timeout_secs=180)
        self._wait_for_stats_all_buckets(self.servers[:self.num_servers])

        self._verify_checkpoint_id(param, stat_key, chk_stats)
        self._verify_stats_all_buckets(self.servers[:self.num_servers])

    def checkpoint_create_time(self):
        """Load data, but let the timeout create a new checkpoint on all replicas"""

        param = 'checkpoint'
        stat_key = 'vb_0:open_checkpoint_id'

        self._set_checkpoint_timeout(self.servers[:self.num_servers], self.bucket, str(self.timeout))

        generate_load = BlobGenerator('nosql', 'nosql-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, generate_load, "create", 0, 1, 0, True, batch_size=self.checkpoint_size, pause_secs=5, timeout_secs=180)
        self._wait_for_stats_all_buckets(self.servers[:self.num_servers])

        chk_stats = StatsCommon.get_stats([self.master], self.bucket, param, stat_key)
        self.log.info("Sleeping for {0} seconds)".format(self.timeout + 5))
        time.sleep(self.timeout + 5)
        self._verify_checkpoint_id(param, stat_key, chk_stats)
        self._verify_stats_all_buckets(self.servers[:self.num_servers])

    def checkpoint_replication_pause(self):
        """With 3 replicas load data. pause replication to R2. Let checkpoints close on Master and R1.
        Restart replication of R2 and R3, backfill should not be seen on R1 and R2."""

        param = 'checkpoint'
        stat_key = 'vb_0:last_closed_checkpoint_id'

        self._set_checkpoint_size(self.servers[:self.num_servers], self.bucket, str(self.checkpoint_size))
        time.sleep(5)
        prev_backfill_timestamp_R1 = self._get_backfill_timestamp(self.replica1, self.replica2)
        prev_backfill_timestamp_R2 = self._get_backfill_timestamp(self.replica2, self.replica3)

        generate_load = BlobGenerator('nosql', 'nosql-', self.value_size, end=self.num_items)
        data_load_thread = Thread(target=self._load_all_buckets,
                                  name="load_data",
                                  args=(self.master, generate_load, "create", 0, 1, 0, True, self.checkpoint_size, 5, 180))
        data_load_thread.start()
        self._stop_replication(self.replica2, self.bucket)

        m_stats = StatsCommon.get_stats([self.master], self.bucket, param, stat_key)
        chk_pnt = int(m_stats[list(m_stats.keys())[0]]) + 2
        tasks = []
        tasks.append(self.cluster.async_wait_for_stats([self.master], self.bucket, param, stat_key,
                                                       '>=', chk_pnt))
        tasks.append(self.cluster.async_wait_for_stats([self.replica1], self.bucket, param, stat_key,
                                                       '>=', chk_pnt))
        for task in tasks:
            try:
                task.result(60)
            except TimeoutError:
                self.fail("Checkpoint not closed")

        data_load_thread.join()
        self._start_replication(self.replica2, self.bucket)

        self._verify_checkpoint_id(param, stat_key, m_stats)
        self._verify_stats_all_buckets(self.servers[:self.num_servers])
        self._verify_backfill_happen(self.replica1, self.replica2, prev_backfill_timestamp_R1)
        self._verify_backfill_happen(self.replica2, self.replica3, prev_backfill_timestamp_R2)

    def checkpoint_collapse(self):
        """With 3 replicas, stop replication on R2, let Master and R1 close checkpoint.
        Run load until a new checkpoint is created on Master and R1.
        Wait till checkpoints merge on R1. Restart replication of R2.
        Checkpoint should advance to the latest on R2."""

        param = 'checkpoint'
        stat_key = 'vb_0:last_closed_checkpoint_id'
        stat_chk_itms = 'vb_0:num_checkpoint_items'

        self._set_checkpoint_size(self.servers[:self.num_servers], self.bucket, str(self.checkpoint_size))
        self._stop_replication(self.replica2, self.bucket)

        generate_load = BlobGenerator('nosql', 'nosql-', self.value_size, end=self.num_items)
        data_load_thread = Thread(target=self._load_all_buckets,
                                  name="load_data",
                                  args=(self.master, generate_load, "create", 0, 1, 0, True, self.checkpoint_size, 5, 180))
        data_load_thread.start()
        m_stats = StatsCommon.get_stats([self.master], self.bucket, param, stat_key)

        tasks = []
        chk_pnt = int(m_stats[list(m_stats.keys())[0]]) + 2
        tasks.append(self.cluster.async_wait_for_stats([self.master], self.bucket, param, stat_key,
                                                       '>=', chk_pnt))
        tasks.append(self.cluster.async_wait_for_stats([self.replica1], self.bucket, param, stat_key,
                                                       '>=', chk_pnt))
        tasks.append(self.cluster.async_wait_for_stats([self.replica1], self.bucket, param,
                                                       stat_chk_itms, '>=', self.num_items))
        data_load_thread.join()
        for task in tasks:
            try:
                task.result(60)
            except TimeoutError:
                self.fail("Checkpoint not collapsed")

        tasks = []
        self._start_replication(self.replica2, self.bucket)
        tasks.append(self.cluster.async_wait_for_stats([self.replica1], self.bucket, param,
                                                       stat_chk_itms, '<', self.num_items))
        for task in tasks:
            try:
                task.result(60)
            except TimeoutError:
                self.fail("Checkpoints not replicated to replica2")

        self._verify_checkpoint_id(param, stat_key, m_stats)
        self._verify_stats_all_buckets(self.servers[:self.num_servers])

    def checkpoint_deduplication(self):
        """Disable replication of R1. Load N items to master, then mutate some of them.
        Restart replication of R1, only N items should be in stats. In this test, we can
        only load number of items <= checkpoint_size to observe deduplication"""

        param = 'checkpoint'
        stat_key = 'vb_0:num_open_checkpoint_items'
        stat_key_id = 'vb_0:open_checkpoint_id'

        self._set_checkpoint_size(self.servers[:self.num_servers], self.bucket, self.checkpoint_size)
        self._stop_replication(self.replica1, self.bucket)

        generate_load = BlobGenerator('nosql', 'nosql-', self.value_size, end=self.num_items)
        generate_update = BlobGenerator('nosql', 'sql-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, generate_load, "create", 0, 1, 0, True, batch_size=self.checkpoint_size, pause_secs=5, timeout_secs=180)
        self._wait_for_stats_all_buckets([self.master, self.replica2, self.replica3])
        m_stats = StatsCommon.get_stats([self.master], self.bucket, param, stat_key_id)
        data_load_thread = Thread(target=self._load_all_buckets,
                                  name="load_data",
                                  args=(self.master, generate_update, "update", 0, 1, 0, True, self.checkpoint_size, 5, 180))
        data_load_thread.start()
        self._start_replication(self.replica1, self.bucket)
        data_load_thread.join()

        chk_pnt = int(m_stats[list(m_stats.keys())[0]])
        timeout = 60 if (self.num_items * .001) < 60 else self.num_items * .001
        time.sleep(timeout)
        tasks = []
        tasks.append(self.cluster.async_wait_for_stats([self.master], self.bucket, param,
                                                       stat_key, '==', self.num_items))
        tasks.append(self.cluster.async_wait_for_stats([self.replica1], self.bucket, param,
                                                       stat_key, '==', self.num_items))
        tasks.append(self.cluster.async_wait_for_stats([self.master], self.bucket, param,
                                                       stat_key_id, '==', chk_pnt))
        tasks.append(self.cluster.async_wait_for_stats([self.replica1], self.bucket, param,
                                                       stat_key_id, '==', chk_pnt))

        for task in tasks:
            try:
                task.result(60)
            except TimeoutError:
                self.fail("Items weren't deduplicated")

        self._verify_stats_all_buckets(self.servers[:self.num_servers])

    def checkpoint_failover_master(self):
        """Load N items. During the load, failover Master.
        Verify backfill doesn't happen on R1, R2."""

        param = 'checkpoint'
        stat_key = 'vb_0:open_checkpoint_id'
        rest = RestConnection(self.master)
        nodes = rest.node_statuses()
        failover_node = None
        for node in nodes:
            if node.id.find(self.master.ip) >= 0:
                failover_node = node

        self._set_checkpoint_size(self.servers[:self.num_servers], self.bucket, self.checkpoint_size)
        generate_load = BlobGenerator('nosql', 'nosql-', self.value_size, end=self.num_items)
        data_load_thread = Thread(target=self._load_all_buckets,
                                  name="load_data",
                                  args=(self.master, generate_load, "create", 0, 1, 0, True, self.checkpoint_size, 5, 180))
        data_load_thread.start()
        time.sleep(5)
        prev_backfill_timestamp_R1 = self._get_backfill_timestamp(self.replica1, self.replica2)
        prev_backfill_timestamp_R2 = self._get_backfill_timestamp(self.replica2, self.replica3)

        failed_over = rest.fail_over(failover_node.id)
        if not failed_over:
            self.log.info("unable to failover the node the first time. try again in  60 seconds..")
            #try again in 60 seconds
            time.sleep(75)
            failed_over = rest.fail_over(failover_node.id)
        self.assertTrue(failed_over, "unable to failover node %s" % (self.master.ip))
        self.log.info("failed over node : {0}".format(failover_node.id))
        data_load_thread.join()

        self._verify_backfill_happen(self.replica1, self.replica2, prev_backfill_timestamp_R1)
        self._verify_backfill_happen(self.replica2, self.replica3, prev_backfill_timestamp_R2)
        self.cluster.rebalance(self.servers[:self.num_servers], [], [self.master])
        self.cluster.rebalance(self.servers[1:self.num_servers], [self.master], [])

    def checkpoint_replication_pause_failover(self):
        """Load N items. Stop replication R3. Load N' more items.
        Failover R2. When restart replication to R3, verify backfill doesn't happen on R1."""

        param = 'checkpoint'
        stat_key = 'vb_0:open_checkpoint_id'
        rest = RestConnection(self.master)
        nodes = rest.node_statuses()
        failover_node = None
        for node in nodes:
            if node.id.find(self.replica2.ip) >= 0:
                failover_node = node

        self._set_checkpoint_size(self.servers[:self.num_servers], self.bucket, self.checkpoint_size)
        generate_load_one = BlobGenerator('nosql', 'nosql-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, generate_load_one, "create", 0, 1, 0, True, batch_size=self.checkpoint_size, pause_secs=5, timeout_secs=180)
        self._wait_for_stats_all_buckets(self.servers[:self.num_servers])
        prev_backfill_timestamp_R1 = self._get_backfill_timestamp(self.replica1, self.replica2)
        m_stats = StatsCommon.get_stats([self.master], self.bucket, param, stat_key)
        self._stop_replication(self.replica3, self.bucket)

        generate_load_two = BlobGenerator('sqlite', 'sqlite-', self.value_size, end=self.num_items)
        data_load_thread = Thread(target=self._load_all_buckets,
                                          name="load_data",
                                          args=(self.master, generate_load_two, "create", 0, 1, 0, True, self.checkpoint_size, 5, 180))
        data_load_thread.start()

        failed_over = rest.fail_over(failover_node.id)
        if not failed_over:
            self.log.info("unable to failover the node the first time. try again in  60 seconds..")
            #try again in 60 seconds
            time.sleep(75)
            failed_over = rest.fail_over(failover_node.id)
        self.assertTrue(failed_over, "unable to failover node %s".format(self.replica2.ip))
        self.log.info("failed over node : {0}".format(failover_node.id))
        data_load_thread.join()
        self._start_replication(self.replica3, self.bucket)

        self.servers = []
        self.servers = [self.master, self.replica1, self.replica3]
        self.num_servers = len(self.servers)
        self._verify_checkpoint_id(param, stat_key, m_stats)
        self._verify_stats_all_buckets(self.servers[:self.num_servers])
        self._verify_backfill_happen(self.replica1, self.replica2, prev_backfill_timestamp_R1)
        self.cluster.rebalance([self.master, self.replica1, self.replica2, self.replica3], [], [self.replica2])
        self.cluster.rebalance([self.master, self.replica1, self.replica3], [self.replica2], [])

    def checkpoint_server_down(self):
        """Load N items. Shut down server R2. Then Restart R2 and
        verify backfill happens on R1 and R2."""

        param = 'checkpoint'
        stat_key = 'vb_0:open_checkpoint_id'
        rest = RestConnection(self.master)

        self._set_checkpoint_size(self.servers[:self.num_servers], self.bucket, self.checkpoint_size)
        generate_load_one = BlobGenerator('nosql', 'nosql-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, generate_load_one, "create", 0, 1, 0, True, batch_size=self.checkpoint_size, pause_secs=5, timeout_secs=180)
        self._wait_for_stats_all_buckets(self.servers[:self.num_servers])
        prev_backfill_timestamp_R1 = self._get_backfill_timestamp(self.replica1, self.replica2)
        prev_backfill_timestamp_R2 = self._get_backfill_timestamp(self.replica2, self.replica3)

        m_stats = StatsCommon.get_stats([self.master], self.bucket, param, stat_key)
        self._stop_server(self.replica2)
        time.sleep(5)
        data_load_thread = Thread(target=self._load_data_use_workloadgen, name="load_data", args=(self.master,))
        data_load_thread.start()
        data_load_thread.join()
        self._start_server(self.replica2)
        time.sleep(5)

        self._verify_checkpoint_id(param, stat_key, m_stats)
        self._verify_backfill_happen(self.replica1, self.replica2, prev_backfill_timestamp_R1, True)
        self._verify_backfill_happen(self.replica2, self.replica3, prev_backfill_timestamp_R2, True)

    def _verify_checkpoint_id(self, param, stat_key, m_stats):
        timeout = 60 if (self.num_items * .001) < 60 else self.num_items * .001

        #verify checkpiont id increases on master node
        chk_pnt = int(m_stats[list(m_stats.keys())[0]])
        tasks = []
        tasks.append(self.cluster.async_wait_for_stats([self.master], self.bucket, param, stat_key, '>', chk_pnt))
        for task in tasks:
            try:
                task.result(timeout)
            except TimeoutError:
                self.fail("New checkpoint not created")

        time.sleep(timeout / 10)
        # verify Master and all replicas are in sync with checkpoint ids
        m_stats = StatsCommon.get_stats([self.master], self.bucket, param, stat_key)
        chk_pnt = int(m_stats[list(m_stats.keys())[0]])
        tasks = []
        for server in self.servers:
            tasks.append(self.cluster.async_wait_for_stats([server], self.bucket, param, stat_key, '==', chk_pnt))
        for task in tasks:
            try:
                task.result(timeout)
            except TimeoutError:
                self.fail("Master and all replicas are NOT in sync with checkpoint ids")

    def _get_backfill_timestamp(self, server, replica_server):
        param = 'tap'
        stat_key = 'eq_tapq:replication_ns_1@%s:backfill_start_timestamp' % (replica_server.ip)
        m_stats = StatsCommon.get_stats([server], self.bucket, param, stat_key)
        self.log.info("eq_tapq:replication_ns_1@%s:backfill_start_timestamp: %s" % (replica_server.ip, m_stats[list(m_stats.keys())[0]]))
        return int(m_stats[list(m_stats.keys())[0]])

    def _verify_backfill_happen(self, server, replica_server, previous_timestamp, backfill_happen=False):
        current_timestamp = self._get_backfill_timestamp(server, replica_server)
        if (current_timestamp - previous_timestamp) < 0:
            raise Exception("cbstats tap backfill_start_timestamp doesn't work properly, which fails the test.")

        if backfill_happen:
            if (current_timestamp - previous_timestamp) == 0:
                raise Exception("Backfill doesn't happen as expected! Test fails")
        else:
            if (current_timestamp - previous_timestamp) > 0:
                raise Exception("Backfill happens unexpectedly! Test fails")

    def _set_checkpoint_size(self, servers, bucket, size):
        ClusterOperationHelper.flushctl_set(servers[0], 'chk_max_items', size, bucket)

    def _set_checkpoint_timeout(self, servers, bucket, time):
        ClusterOperationHelper.flushctl_set(servers[0], 'chk_period', time, bucket)

    def _stop_replication(self, server, bucket):
        shell = RemoteMachineShellConnection(server)
        shell.execute_cbepctl(self.bucket, "stop", "", "", 0)
        shell.execute_cbepctl(self.bucket, "", "set tap_param", "tap_throttle_queue_cap", 10)
        shell.disconnect()

    def _start_replication(self, server, bucket):
        shell = RemoteMachineShellConnection(server)
        shell.execute_cbepctl(self.bucket, "start", "", "", 0)
        shell.execute_cbepctl(self.bucket, "", "set tap_param", "tap_throttle_queue_cap", 1000000)
        shell.disconnect()

    def _stop_server(self, server):
        shell = RemoteMachineShellConnection(server)
        shell.stop_server()
        shell.disconnect()

    def _start_server(self, server):
        shell = RemoteMachineShellConnection(server)
        shell.start_server()
        shell.disconnect()

    def _load_data_use_workloadgen(self, server):
        os = "linux"
        shell = RemoteMachineShellConnection(server)
        if os == "linux":
            command = "%stools/cbworkloadgen -n %s:8091 -i %s" % (testconstants.LINUX_COUCHBASE_BIN_PATH, server.ip, self.num_items)
            output, error = shell.execute_command(command)
            shell.log_command_output(output, error)
        shell.disconnect()

    def _get_server_by_state(self, servers, bucket, vb_state):
        rest = RestConnection(servers[0])
        vbuckets = rest.get_vbuckets(bucket)[0]
        addr = None
        if vb_state == ACTIVE:
            addr = vbuckets.master
        elif vb_state == REPLICA1:
            addr = vbuckets.replica[0].encode("ascii", "ignore")
        elif vb_state == REPLICA2:
            addr = vbuckets.replica[1].encode("ascii", "ignore")
        elif vb_state == REPLICA3:
            addr = vbuckets.replica[2].encode("ascii", "ignore")
        else:
            return None

        addr = addr.split(':', 1)[0]
        for server in servers:
            if addr == server.ip:
                return server
        return None
