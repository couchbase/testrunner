from threading import Thread
import copy

from couchbase_helper.documentgenerator import BlobGenerator
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection
from memcached.helper.data_helper import LoadWithMcsoda
from .xdcrnewbasetests import XDCRNewBaseTest
from .xdcrnewbasetests import NodeHelper
from .xdcrnewbasetests import Utility, BUCKET_NAME, OPS
from scripts.install import InstallerJob
from lib.memcached.helper.data_helper import MemcachedClientHelper


# Assumption that at least 2 nodes on every cluster
class unidirectional(XDCRNewBaseTest):
    def setUp(self):
        super(unidirectional, self).setUp()
        self.src_cluster = self.get_cb_cluster_by_name('C1')
        self.src_master = self.src_cluster.get_master_node()
        self.dest_cluster = self.get_cb_cluster_by_name('C2')
        self.dest_master = self.dest_cluster.get_master_node()

    def tearDown(self):
        super(unidirectional, self).tearDown()

    """Testing Unidirectional load( Loading only at source) Verifying whether XDCR replication is successful on
    subsequent destination clusters.Create/Update/Delete operations are performed based on doc-ops specified by the user. """

    def load_with_ops(self):
        self.setup_xdcr_and_load()
        self.perform_update_delete()
        self.verify_results()

    """Testing Unidirectional load( Loading only at source) Verifying whether XDCR replication is successful on
    subsequent destination clusters. Create/Update/Delete are performed in parallel- doc-ops specified by the user. """

    def load_with_async_ops(self):
        self.setup_xdcr_and_load()
        self.async_perform_update_delete()
        self.verify_results()

    def load_with_async_ops_diff_data_size(self):
        # Load 1 item with size 1
        # 52 alphabets (small and capital letter)
        self.src_cluster.load_all_buckets(52, value_size=1)
        # Load items with below sizes of 1M
        self.src_cluster.load_all_buckets(5, value_size=1000000)
        # Load items with size 10MB
        # Getting memory issue with 20MB data on VMs.
        self.src_cluster.load_all_buckets(1, value_size=10000000)

        self.verify_results()

    """Testing Unidirectional load( Loading only at source). Failover node at Source/Destination while
    Create/Update/Delete are performed after based on doc-ops specified by the user.
    Verifying whether XDCR replication is successful on subsequent destination clusters. """

    def load_with_ops_with_warmup(self):
        self.setup_xdcr_and_load()
        warmupnodes = []
        if "C1" in self._warmup:
            warmupnodes.append(self.src_cluster.warmup_node())
        if "C2" in self._warmup:
            warmupnodes.append(self.dest_cluster.warmup_node())

        self.sleep(self._wait_timeout)
        self.perform_update_delete()
        self.sleep(self._wait_timeout // 2)

        NodeHelper.wait_warmup_completed(warmupnodes)

        self.verify_results()

    def load_with_ops_with_warmup_master(self):
        self.setup_xdcr_and_load()
        warmupnodes = []
        if "C1" in self._warmup:
            warmupnodes.append(self.src_cluster.warmup_node(master=True))
        if "C2" in self._warmup:
            warmupnodes.append(self.dest_cluster.warmup_node(master=True))

        self.sleep(self._wait_timeout)
        self.perform_update_delete()
        self.sleep(self._wait_timeout // 2)

        NodeHelper.wait_warmup_completed(warmupnodes)

        self.verify_results()

    def load_with_async_ops_with_warmup(self):
        bucket_type = self._input.param("bucket_type", "membase")
        if bucket_type == "ephemeral":
            "Test case does not apply for Ephemeral buckets"
            return
        self.setup_xdcr_and_load()
        warmupnodes = []
        if "C1" in self._warmup:
            warmupnodes.append(self.src_cluster.warmup_node())
        if "C2" in self._warmup:
            warmupnodes.append(self.dest_cluster.warmup_node())

        self.sleep(self._wait_timeout)
        self.async_perform_update_delete()
        self.sleep(self._wait_timeout // 2)

        NodeHelper.wait_warmup_completed(warmupnodes)

        self.verify_results()

    def load_with_async_ops_with_warmup_master(self):
        bucket_type = self._input.param("bucket_type", "membase")
        if bucket_type == "ephemeral":
            "Test case does not apply for Ephemeral buckets"
            return
        self.setup_xdcr_and_load()
        warmupnodes = []
        if "C1" in self._warmup:
            warmupnodes.append(self.src_cluster.warmup_node(master=True))
        if "C2" in self._warmup:
            warmupnodes.append(self.dest_cluster.warmup_node(master=True))

        self.sleep(self._wait_timeout)
        self.async_perform_update_delete()
        self.sleep(self._wait_timeout // 2)

        NodeHelper.wait_warmup_completed(warmupnodes)

        self.sleep(300)

        self.verify_results()

    def load_with_failover(self):
        self.setup_xdcr_and_load()

        if "C1" in self._failover:
            self.src_cluster.failover_and_rebalance_nodes()
        if "C2" in self._failover:
            self.dest_cluster.failover_and_rebalance_nodes()

        self.sleep(self._wait_timeout // 6)
        self.perform_update_delete()

        self.verify_results()

    def load_with_failover_then_add_back(self):

        self.setup_xdcr_and_load()

        if "C1" in self._failover:
            self.src_cluster.failover_and_rebalance_nodes(rebalance=False)
            self.src_cluster.add_back_node()
        if "C2" in self._failover:
            self.dest_cluster.failover_and_rebalance_nodes(rebalance=False)
            self.dest_cluster.add_back_node()

        self.perform_update_delete()

        self.verify_results()

    """Testing Unidirectional load( Loading only at source). Failover node at Source/Destination while
    Create/Update/Delete are performed in parallel based on doc-ops specified by the user.
    Verifying whether XDCR replication is successful on subsequent destination clusters. """

    def load_with_failover_master(self):
        self.setup_xdcr_and_load()

        if "C1" in self._failover:
            self.src_cluster.failover_and_rebalance_master()
        if "C2" in self._failover:
            self.dest_cluster.failover_and_rebalance_master()

        self.sleep(self._wait_timeout // 6)
        self.perform_update_delete()

        self.sleep(300)

        self.verify_results()

    """Testing Unidirectional load( Loading only at source). Failover node at Source/Destination while
    Create/Update/Delete are performed in parallel based on doc-ops specified by the user.
    Verifying whether XDCR replication is successful on subsequent destination clusters. """

    def load_with_async_failover(self):
        self.setup_xdcr_and_load()

        tasks = []
        if "C1" in self._failover:
            tasks.append(self.src_cluster.async_failover())
        if "C2" in self._failover:
            tasks.append(self.dest_cluster.async_failover())

        self.perform_update_delete()
        self.sleep(self._wait_timeout // 4)

        for task in tasks:
            task.result()

        if "C1" in self._failover:
            self.src_cluster.rebalance_failover_nodes()
        if "C2" in self._failover:
            self.dest_cluster.rebalance_failover_nodes()

        self.verify_results()

    """Replication with compaction ddocs and view queries on both clusters.

        This test begins by loading a given number of items on the source cluster.
        It creates num_views as development/production view with default
        map view funcs(_is_dev_ddoc = True by default) on both clusters.
        Then we disabled compaction for ddoc on src cluster. While we don't reach
        expected fragmentation for ddoc on src cluster we update docs and perform
        view queries for all views. Then we start compaction when fragmentation
        was reached fragmentation_value. When compaction was completed we perform
        a full verification: wait for the disk queues to drain
        and then verify that there has been any data loss on all clusters."""
    def replication_with_ddoc_compaction(self):
        bucket_type = self._input.param("bucket_type", "membase")
        if bucket_type == "ephemeral":
            self.log.info("Test case does not apply to ephemeral")
            return

        self.setup_xdcr_and_load()

        num_views = self._input.param("num_views", 5)
        is_dev_ddoc = self._input.param("is_dev_ddoc", True)
        fragmentation_value = self._input.param("fragmentation_value", 80)
        for bucket in self.src_cluster.get_buckets():
            views = Utility.make_default_views(bucket.name, num_views, is_dev_ddoc)

        ddoc_name = "ddoc1"
        prefix = ("", "dev_")[is_dev_ddoc]

        query = {"full_set": "true", "stale": "false"}

        tasks = self.src_cluster.async_create_views(ddoc_name, views, BUCKET_NAME.DEFAULT)
        tasks += self.dest_cluster.async_create_views(ddoc_name, views, BUCKET_NAME.DEFAULT)
        for task in tasks:
            task.result(self._poll_timeout)

        self.src_cluster.disable_compaction()
        fragmentation_monitor = self.src_cluster.async_monitor_view_fragmentation(prefix + ddoc_name, fragmentation_value, BUCKET_NAME.DEFAULT)

        # generate load until fragmentation reached
        while fragmentation_monitor.state != "FINISHED":
            # update docs to create fragmentation
            self.src_cluster.update_delete_data(OPS.UPDATE)
            for view in views:
                # run queries to create indexes
                self.src_cluster.query_view(prefix + ddoc_name, view.name, query)
        fragmentation_monitor.result()

        compaction_task = self.src_cluster.async_compact_view(prefix + ddoc_name, 'default')

        self.assertTrue(compaction_task.result())

        self.verify_results()

    """Replication with disabled/enabled ddoc compaction on source cluster.

        This test begins by loading a given number of items on the source cluster.
        Then we disabled or enabled compaction on both clusters( set via params).
        Then we mutate and delete data on the source cluster 3 times.
        After deletion we recreate deleted items. When data was changed 3 times
        we perform a full verification: wait for the disk queues to drain
        and then verify that there has been no data loss on both all clusters."""
    def replication_with_disabled_ddoc_compaction(self):
        self.setup_xdcr_and_load()

        if "C1" in self._disable_compaction:
            self.src_cluster.disable_compaction()
        if "C2" in self._disable_compaction:
            self.dest_cluster.disable_compaction()

        # perform doc's ops 3 times to increase rev number
        for _ in range(3):
            self.async_perform_update_delete()
            # restore(re-creating) deleted items
            if 'C1' in self._del_clusters:
                c1_kv_gen = self.src_cluster.get_kv_gen()
                gen_delete = copy.deepcopy(c1_kv_gen[OPS.DELETE])
                self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_delete)
                self.sleep(5)

        self.sleep(600)
        self.verify_results()

    def replication_while_rebooting_a_non_master_destination_node(self):
        self.setup_xdcr_and_load()
        self.src_cluster.set_xdcr_param("xdcrFailureRestartInterval", 1)
        self.perform_update_delete()
        self.sleep(self._wait_timeout // 2)
        rebooted_node = self.dest_cluster.reboot_one_node(self)
        NodeHelper.wait_node_restarted(rebooted_node, self, wait_time=self._wait_timeout * 4, wait_if_warmup=True)

        self.verify_results()

    def replication_with_firewall_enabled(self):
        self.src_cluster.set_xdcr_param("xdcrFailureRestartInterval", 1)
        self.setup_xdcr_and_load()
        self.perform_update_delete()

        NodeHelper.enable_firewall(self.dest_master)
        self.sleep(30)
        NodeHelper.disable_firewall(self.dest_master)
        self.verify_results()

    """Testing Unidirectional append ( Loading only at source) Verifying whether XDCR replication is successful on
    subsequent destination clusters. """

    def test_append(self):
        self.setup_xdcr_and_load()
        self.verify_results()
        loop_count = self._input.param("loop_count", 20)
        for i in range(loop_count):
            self.log.info("Append iteration # %s" % i)
            gen_append = BlobGenerator('loadOne', 'loadOne', self._value_size, end=self._num_items)
            self.src_cluster.load_all_buckets_from_generator(gen_append, ops=OPS.APPEND, batch_size=1)
            self.sleep(self._wait_timeout)
        self.verify_results()

    '''
    This method runs cbcollectinfo tool after setting up uniXDCR and check
    whether the log generated by cbcollectinfo contains xdcr log file or not.
    '''
    def collectinfotest_for_xdcr(self):
        self.load_with_ops()
        self.node_down = self._input.param("node_down", False)
        self.log_filename = self._input.param("file_name", "collectInfo")
        self.shell = RemoteMachineShellConnection(self.src_master)
        self.shell.execute_cbcollect_info("%s.zip" % (self.log_filename))
        from clitest import collectinfotest
        # HACK added self.buckets data member.
        self.buckets = self.src_cluster.get_buckets()
        collectinfotest.CollectinfoTests.verify_results(
            self, self.log_filename
        )

    """ Verify the fix for MB-9548"""
    def verify_replications_deleted_after_bucket_deletion(self):
        self.setup_xdcr_and_load()
        self.verify_results()
        rest_conn = RestConnection(self.src_master)
        replications = rest_conn.get_replications()
        self.assertTrue(replications, "Number of replications should not be 0")
        self.src_cluster.delete_all_buckets()
        self.sleep(60)
        replications = rest_conn.get_replications()
        self.log.info("Replications : %s" % replications)
        self.assertTrue(not replications, "Rest returns replication list even after source bucket is deleted ")

    """ Verify fix for MB-9862"""
    def test_verify_memcache_connections(self):
        allowed_memcached_conn = self._input.param("allowed_connections", 100)
        max_ops_per_second = self._input.param("max_ops_per_second", 2500)
        min_item_size = self._input.param("min_item_size", 128)
        num_docs = self._input.param("num_docs", 30000)
        # start load, max_ops_per_second is the combined limit for all buckets
        mcsodaLoad = LoadWithMcsoda(self.src_master, num_docs, prefix='')
        mcsodaLoad.cfg["max-ops"] = 0
        mcsodaLoad.cfg["max-ops-per-sec"] = max_ops_per_second
        mcsodaLoad.cfg["exit-after-creates"] = 1
        mcsodaLoad.cfg["min-value-size"] = min_item_size
        mcsodaLoad.cfg["json"] = 0
        mcsodaLoad.cfg["batch"] = 100
        loadDataThread = Thread(target=mcsodaLoad.load_data,
                                  name='mcloader_default')
        loadDataThread.daemon = True
        loadDataThread.start()

        src_remote_shell = RemoteMachineShellConnection(self.src_master)
        machine_type = src_remote_shell.extract_remote_info().type.lower()
        while (loadDataThread.isAlive() and machine_type == 'linux'):
            command = "netstat -lpnta | grep 11210 | grep TIME_WAIT | wc -l"
            output, _ = src_remote_shell.execute_command(command)
            if int(output[0]) > allowed_memcached_conn:
                # stop load
                mcsodaLoad.load_stop()
                loadDataThread.join()
                self.fail("Memcached connections {0} are increased above {1} \
                            on Source node".format(
                                                   allowed_memcached_conn,
                                                   int(output[0])))
            self.sleep(5)

        # stop load
        mcsodaLoad.load_stop()
        loadDataThread.join()

    # Test to verify MB-10116
    def verify_ssl_private_key_not_present_in_logs(self):
        zip_file = "%s.zip" % (self._input.param("file_name", "collectInfo"))
        try:
            self.shell = RemoteMachineShellConnection(self.src_master)
            self.load_with_ops()
            self.shell.execute_cbcollect_info(zip_file)
            if self.shell.extract_remote_info().type.lower() != "windows":
                self.shell.execute_command("apt-get install unzip")
                command = "unzip %s" % (zip_file)
                output, error = self.shell.execute_command(command)
                self.shell.log_command_output(output, error)
                if len(error) > 0:
                    raise Exception("unable to unzip the files. Check unzip command output for help")
                cmd = 'grep -R "BEGIN RSA PRIVATE KEY" cbcollect_info*/'
                output, _ = self.shell.execute_command(cmd)
            else:
                cmd = "curl -0 http://{1}:{2}@{0}:8091/diag 2>/dev/null | grep 'BEGIN RSA PRIVATE KEY'".format(
                                                    self.src_master.ip,
                                                    self.src_master.rest_username,
                                                    self.src_master.rest_password)
                output, _ = self.shell.execute_command(cmd)
            self.assertTrue(not output, "XDCR SSL Private Key is found diag logs -> %s" % output)
        finally:
            self.shell.delete_files(zip_file)
            self.shell.delete_files("cbcollect_info*")

    # Buckets States
    def delete_recreate_dest_buckets(self):
        self.setup_xdcr_and_load()

        # Remove destination buckets
        self.dest_cluster.delete_all_buckets()

        # Code for re-create_buckets
        self.create_buckets_on_cluster("C2")

        self._resetup_replication_for_recreate_buckets("C2")

        self.async_perform_update_delete()
        self.verify_results()

    def flush_dest_buckets(self):
        self.setup_xdcr_and_load()

        # flush destination buckets
        self.dest_cluster.flush_buckets()

        self.async_perform_update_delete()
        self.verify_results()

    # Nodes Crashing Scenarios
    def __kill_processes(self, crashed_nodes=[]):
        for node in crashed_nodes:
            try:
                NodeHelper.kill_erlang(node)
            except:
                self.log.info('Could not kill erlang process on node, continuing..')

    def __start_cb_server(self, node):
        shell = RemoteMachineShellConnection(node)
        shell.start_couchbase()
        shell.disconnect()

    def test_node_crash_master(self):
        self.setup_xdcr_and_load()

        crashed_nodes = []
        crash = self._input.param("crash", "").split('-')
        if "C1" in crash:
            crashed_nodes.append(self.src_master)
        if "C2" in crash:
            crashed_nodes.append(self.dest_master)

        self.__kill_processes(crashed_nodes)

        for crashed_node in crashed_nodes:
            self.__start_cb_server(crashed_node)

        bucket_type = self._input.param("bucket_type", "membase")
        if bucket_type == "ephemeral":
            self.sleep(self._wait_timeout)
        else:
            NodeHelper.wait_warmup_completed(crashed_nodes)

        self.async_perform_update_delete()
        self.verify_results()

    # Disaster at site.
    # 1. Crash Source Cluster., Sleep n second
    # 2. Crash Dest Cluster.
    # 3. Wait for Source Cluster to warmup. Load more data and perform mutations on Src.
    # 4. Wait for Dest to warmup.
    # 5. Verify data.
    def test_node_crash_cluster(self):
        self.setup_xdcr_and_load()

        crashed_nodes = []
        crash = self._input.param("crash", "").split('-')
        if "C1" in crash:
            crashed_nodes += self.src_cluster.get_nodes()
            self.__kill_processes(crashed_nodes)
            self.sleep(30)
        if "C2" in crash:
            crashed_nodes += self.dest_cluster.get_nodes()
            self.__kill_processes(crashed_nodes)

        for crashed_node in crashed_nodes:
            self.__start_cb_server(crashed_node)

        bucket_type = self._input.param("bucket_type", "membase")

        if "C1" in crash:
            if bucket_type == "ephemeral":
                self.sleep(self._wait_timeout)
            else:
                NodeHelper.wait_warmup_completed(self.src_cluster.get_nodes())
            gen_create = BlobGenerator('loadTwo', 'loadTwo', self._value_size, end=self._num_items)
            self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_create)

        self.async_perform_update_delete()

        if "C2" in crash:
            if bucket_type == "ephemeral":
                self.sleep(self._wait_timeout)
            else:
                NodeHelper.wait_warmup_completed(self.dest_cluster.get_nodes())

        self.verify_results()

    """ Test if replication restarts 60s after idle xdcr following dest bucket flush """
    def test_idle_xdcr_dest_flush(self):
        self.setup_xdcr_and_load()
        self.verify_results()

        bucket = self.dest_cluster.get_bucket_by_name(BUCKET_NAME.DEFAULT)
        self.dest_cluster.flush_buckets([bucket])

        self.sleep(self._wait_timeout)

        self.verify_results()

    """ Test if replication restarts 60s after idle xdcr following dest bucket recreate """
    def test_idle_xdcr_dest_recreate(self):
        self.setup_xdcr_and_load()
        self.verify_results()

        bucket = self.dest_cluster.get_bucket_by_name(BUCKET_NAME.DEFAULT)
        self.dest_cluster.delete_bucket(BUCKET_NAME.DEFAULT)
        bucket_params=self._create_bucket_params(size=bucket.bucket_size)
        self.dest_cluster.create_default_bucket(bucket_params)
        self.sleep(self._wait_timeout)

        self.verify_results()

    """ Test if replication restarts 60s after idle xdcr following dest failover """
    def test_idle_xdcr_dest_failover(self):
        self.setup_xdcr_and_load()
        self.verify_results()

        self.dest_cluster.failover_and_rebalance_nodes()

        self.sleep(self._wait_timeout)

        self.verify_results()

    def _disable_compression(self):
        shell = RemoteMachineShellConnection(self.src_master)
        for remote_cluster in self.src_cluster.get_remote_clusters():
            for repl in remote_cluster.get_replications():
                src_bucket_name = repl.get_src_bucket().name
                if src_bucket_name in str(repl):
                    repl_id = repl.get_repl_id()
                    repl_id = str(repl_id).replace('/', '%2F')
                    base_url = "http://" + self.src_master.ip + \
                               ":8091/settings/replications/" + repl_id
                    command = "curl -X POST -u Administrator:password " + base_url + \
                              " -d compressionType=" + "None"
                    output, error = shell.execute_command(command)
                    shell.log_command_output(output, error)
        shell.disconnect()

    def test_optimistic_replication(self):
        """Tests with 2 buckets with customized optimisic replication thresholds
           one greater than value_size, other smaller
        """
        from .xdcrnewbasetests import REPL_PARAM
        self.setup_xdcr()
        # To ensure docs size = value_size on target
        self._disable_compression()
        self.load_data_topology()
        self._wait_for_replication_to_catchup()
        for remote_cluster in self.src_cluster.get_remote_clusters():
            for replication in remote_cluster.get_replications():
                src_bucket_name = replication.get_src_bucket().name
                opt_repl_threshold = replication.get_xdcr_setting(REPL_PARAM.OPTIMISTIC_THRESHOLD)
                docs_opt_replicated_stat = 'replications/%s/docs_opt_repd' %replication.get_repl_id()
                opt_replicated = RestConnection(self.src_master).fetch_bucket_xdcr_stats(
                                        src_bucket_name
                                        )['op']['samples'][docs_opt_replicated_stat][-1]
                self.log.info("Bucket: %s, value size: %s, optimistic threshold: %s"
                              " number of mutations optimistically replicated: %s"
                                %(src_bucket_name,
                                  self._value_size,
                                  opt_repl_threshold,
                                  opt_replicated
                                ))
                if self._value_size <= opt_repl_threshold:
                    if opt_replicated == self._num_items:
                        self.log.info("SUCCESS: All keys in bucket %s were optimistically"
                                      " replicated"
                                      %(replication.get_src_bucket().name))
                    else:
                        self.fail("Value size: %s, optimistic threshold: %s,"
                                  " number of docs optimistically replicated: %s"
                          %(self._value_size, opt_repl_threshold, opt_replicated))
                else:
                    if opt_replicated == 0:
                        self.log.info("SUCCESS: No key in bucket %s was optimistically"
                                      " replicated"

                                      %(replication.get_src_bucket().name))
                    else:
                        self.fail("Partial optimistic replication detected!")

    def test_disk_full(self):
        self.setup_xdcr_and_load()
        self.verify_results()

        self.sleep(self._wait_timeout)

        zip_file = "%s.zip" % (self._input.param("file_name", "collectInfo"))
        try:
            for node in [self.src_master, self.dest_master]:
                self.shell = RemoteMachineShellConnection(node)
                self.shell.execute_cbcollect_info(zip_file)
                if self.shell.extract_remote_info().type.lower() != "windows":
                    command = "unzip %s" % (zip_file)
                    output, error = self.shell.execute_command(command)
                    self.shell.log_command_output(output, error)
                    if len(error) > 0:
                        raise Exception("unable to unzip the files. Check unzip command output for help")
                    cmd = 'grep -R "Approaching full disk warning." cbcollect_info*/'
                    output, _ = self.shell.execute_command(cmd)
                else:
                    cmd = "curl -0 http://{1}:{2}@{0}:8091/diag 2>/dev/null | grep 'Approaching full disk warning.'".format(
                                                        self.src_master.ip,
                                                        self.src_master.rest_username,
                                                        self.src_master.rest_password)
                    output, _ = self.shell.execute_command(cmd)
                self.assertNotEqual(len(output), 0, "Full disk warning not generated as expected in %s" % node.ip)
                self.log.info("Full disk warning generated as expected in %s" % node.ip)

                self.shell.delete_files(zip_file)
                self.shell.delete_files("cbcollect_info*")
        except Exception as e:
            self.log.info(e)

    def test_retry_connections_on_errors_before_restart(self):
        """
        CBQE-3373: Do not restart pipeline as soon as connection errors are
        detected, backoff and retry 5 times before trying to restart pipeline.
        """
        passed = False
        # start data load after setting up xdcr
        load_tasks = self.setup_xdcr_async_load()
        goxdcr_log = NodeHelper.get_goxdcr_log_dir(self._input.servers[0])\
                     + '/goxdcr.log*'

        # block port 11210 on target to simulate a connection error
        shell = RemoteMachineShellConnection(self.dest_master)
        out, err = shell.execute_command("/sbin/iptables -A INPUT -p tcp --dport"
                                         " 11210 -j DROP")
        shell.log_command_output(out, err)
        out, err = shell.execute_command("/sbin/iptables -L")
        shell.log_command_output(out, err)

        # complete loading
        for task in load_tasks:
            task.result()

        # wait for goxdcr to detect i/o timeout and try repairing
        self.sleep(self._wait_timeout*5)

        # unblock port 11210 so replication can continue
        out, err = shell.execute_command("/sbin/iptables -D INPUT -p tcp --dport"
                                         " 11210 -j DROP")
        shell.log_command_output(out, err)
        out, err = shell.execute_command("/sbin/iptables -L")
        shell.log_command_output(out, err)
        shell.disconnect()

        # check logs for traces of retry attempts
        for node in self.src_cluster.get_nodes():
            _, count1 = NodeHelper.check_goxdcr_log(
                            node,
                            "Failed to repair connections to target cluster",
                            goxdcr_log)
            _, count2 = NodeHelper.check_goxdcr_log(
                            node,
                            "Failed to set up connections to target cluster",
                            goxdcr_log)
            count = count1 + count2
            if count > 0:
                self.log.info('SUCCESS: We tried to repair connections before'
                              ' restarting pipeline')
                passed = True

        if not passed:
            self.fail("No attempts were made to repair connections on %s before"
                      " restarting pipeline" % self.src_cluster.get_nodes())
        self.verify_results()

    def test_verify_mb19802_1(self):
        load_tasks = self.setup_xdcr_async_load()
        goxdcr_log = NodeHelper.get_goxdcr_log_dir(self._input.servers[0])\
                     + '/goxdcr.log*'

        conn = RemoteMachineShellConnection(self.dest_cluster.get_master_node())
        conn.stop_couchbase()

        for task in load_tasks:
            task.result()

        conn.start_couchbase()
        self.sleep(300)

        for node in self.src_cluster.get_nodes():
            _, count = NodeHelper.check_goxdcr_log(
                            node,
                            "batchGetMeta received fatal error and had to abort",
                            goxdcr_log)
            self.assertEqual(count, 0, "batchGetMeta error message found in " + str(node.ip))
            self.log.info("batchGetMeta error message not found in " + str(node.ip))

        self.verify_results()

    def test_verify_mb19802_2(self):
        load_tasks = self.setup_xdcr_async_load()
        goxdcr_log = NodeHelper.get_goxdcr_log_dir(self._input.servers[0])\
                     + '/goxdcr.log*'

        self.dest_cluster.failover_and_rebalance_master()

        for task in load_tasks:
            task.result()

        for node in self.src_cluster.get_nodes():
            _, count = NodeHelper.check_goxdcr_log(
                            node,
                            "batchGetMeta received fatal error and had to abort",
                            goxdcr_log)
            self.assertEqual(count, 0, "batchGetMeta timed out error message found in " + str(node.ip))
            self.log.info("batchGetMeta error message not found in " + str(node.ip))

        self.sleep(300)
        self.verify_results()

    def test_verify_mb19697(self):
        self.setup_xdcr_and_load()
        goxdcr_log = NodeHelper.get_goxdcr_log_dir(self._input.servers[0])\
                     + '/goxdcr.log*'

        self.src_cluster.pause_all_replications()

        gen = BlobGenerator("C1-", "C1-", self._value_size, end=100000)
        self.src_cluster.load_all_buckets_from_generator(gen)

        self.src_cluster.resume_all_replications()
        self._wait_for_replication_to_catchup()

        gen = BlobGenerator("C1-", "C1-", self._value_size, end=100000)
        load_tasks = self.src_cluster.async_load_all_buckets_from_generator(gen)

        self.src_cluster.rebalance_out()

        for task in load_tasks:
            task.result()

        self._wait_for_replication_to_catchup()

        self.src_cluster.rebalance_in()

        gen = BlobGenerator("C1-", "C1-", self._value_size, end=100000)
        load_tasks = self.src_cluster.async_load_all_buckets_from_generator(gen)

        self.src_cluster.failover_and_rebalance_master()

        for task in load_tasks:
            task.result()

        self._wait_for_replication_to_catchup()

        for node in self.src_cluster.get_nodes():
            _, count = NodeHelper.check_goxdcr_log(
                            node,
                            "counter .+ goes backward, maybe due to the pipeline is restarted",
                            goxdcr_log)
            self.assertEqual(count, 0, "counter goes backward, maybe due to the pipeline is restarted "
                                        "error message found in " + str(node.ip))
            self.log.info("counter goes backward, maybe due to the pipeline is restarted "
                                        "error message not found in " + str(node.ip))

        self.sleep(300)
        self.verify_results()

    def test_verify_mb20463(self):
        src_version = NodeHelper.get_cb_version(self.src_cluster.get_master_node())
        if float(src_version[:3]) != 4.5:
            self.log.info("Source cluster has to be at 4.5 for this test")
            return

        servs = self._input.servers[2:4]
        params = {}
        params['num_nodes'] = len(servs)
        params['product'] = 'cb'
        params['version'] = '4.1.2-6088'
        params['vbuckets'] = [1024]
        self.log.info("will install {0} on {1}".format('4.1.2-6088', [s.ip for s in servs]))
        InstallerJob().parallel_install(servs, params)

        if params['product'] in ["couchbase", "couchbase-server", "cb"]:
            success = True
            for server in servs:
                success &= RemoteMachineShellConnection(server).is_couchbase_installed()
                if not success:
                    self.fail("some nodes were not installed successfully on target cluster!")

        self.log.info("4.1.2 installed successfully on target cluster")

        conn = RestConnection(self.dest_cluster.get_master_node())
        conn.add_node(user=self._input.servers[3].rest_username, password=self._input.servers[3].rest_password,
                      remoteIp=self._input.servers[3].ip)
        self.sleep(30)
        conn.rebalance(otpNodes=[node.id for node in conn.node_statuses()])
        self.sleep(30)
        conn.create_bucket(bucket='default', ramQuotaMB=512)

        tasks = self.setup_xdcr_async_load()

        self.sleep(30)

        NodeHelper.enable_firewall(self.dest_master)
        self.sleep(30)
        NodeHelper.disable_firewall(self.dest_master)

        for task in tasks:
            task.result()

        self._wait_for_replication_to_catchup(timeout=600)

        self.verify_results()

    def test_rollback(self):
        bucket = self.src_cluster.get_buckets()[0]
        nodes = self.src_cluster.get_nodes()

        # Stop Persistence on Node A & Node B
        for node in nodes:
            mem_client = MemcachedClientHelper.direct_client(node, bucket)
            mem_client.stop_persistence()

        goxdcr_log = NodeHelper.get_goxdcr_log_dir(self._input.servers[0])\
                     + '/goxdcr.log*'
        self.setup_xdcr()

        self.src_cluster.pause_all_replications()

        gen = BlobGenerator("C1-", "C1-", self._value_size, end=self._num_items)
        self.src_cluster.load_all_buckets_from_generator(gen)

        self.src_cluster.resume_all_replications()

        # Perform mutations on the bucket
        self.async_perform_update_delete()

        rest1 = RestConnection(self.src_cluster.get_master_node())
        rest2 = RestConnection(self.dest_cluster.get_master_node())

        # Fetch count of docs in src and dest cluster
        _count1 = rest1.fetch_bucket_stats(bucket=bucket.name)["op"]["samples"]["curr_items"][-1]
        _count2 = rest2.fetch_bucket_stats(bucket=bucket.name)["op"]["samples"]["curr_items"][-1]

        self.log.info("Before rollback src cluster count = {0} dest cluster count = {1}".format(_count1, _count2))

        # Kill memcached on Node A so that Node B becomes master
        shell = RemoteMachineShellConnection(self.src_cluster.get_master_node())
        shell.kill_memcached()

        # Start persistence on Node B
        mem_client = MemcachedClientHelper.direct_client(nodes[1], bucket)
        mem_client.start_persistence()

        # Failover Node B
        failover_task = self.src_cluster.async_failover()
        failover_task.result()

        # Wait for Failover & rollback to complete
        self.sleep(60)

        # Fetch count of docs in src and dest cluster
        _count1 = rest1.fetch_bucket_stats(bucket=bucket.name)["op"]["samples"]["curr_items"][-1]
        _count2 = rest2.fetch_bucket_stats(bucket=bucket.name)["op"]["samples"]["curr_items"][-1]

        self.log.info("After rollback src cluster count = {0} dest cluster count = {1}".format(_count1, _count2))

        self.assertTrue(self.src_cluster.wait_for_outbound_mutations(),
                        "Mutations in source cluster not replicated to target after rollback")
        self.log.info("Mutations in source cluster replicated to target after rollback")

        _, count = NodeHelper.check_goxdcr_log(
                        nodes[0],
                        "Received rollback from DCP stream",
                        goxdcr_log, timeout=60)
        self.assertGreater(count, 0, "rollback did not happen as expected")
        self.log.info("rollback happened as expected")

    def test_verify_mb19181(self):
        load_tasks = self.setup_xdcr_async_load()
        goxdcr_log = NodeHelper.get_goxdcr_log_dir(self._input.servers[0]) \
                     + '/goxdcr.log*'

        self.dest_cluster.failover_and_rebalance_master()

        for task in load_tasks:
            task.result()

        for node in self.src_cluster.get_nodes():
            _, count = NodeHelper.check_goxdcr_log(
                node,
                "Can't move update state from",
                goxdcr_log)
            self.assertEqual(count, 0, "Can't move update state from - error message found in " + str(node.ip))
            self.log.info("Can't move update state from - error message not found in " + str(node.ip))

        self.verify_results()

    def test_verify_mb21369(self):
        repeat = self._input.param("repeat", 5)
        load_tasks = self.setup_xdcr_async_load()

        conn = RemoteMachineShellConnection(self.src_cluster.get_master_node())
        output, error = conn.execute_command("netstat -an | grep " + self.src_cluster.get_master_node().ip
                                             + ":11210 | wc -l")
        conn.log_command_output(output, error)
        before = output[0]
        self.log.info("No. of memcached connections before: {0}".format(output[0]))

        for i in range(0, repeat):
            self.src_cluster.pause_all_replications()
            self.sleep(30)
            self.src_cluster.resume_all_replications()
            self.sleep(self._wait_timeout)
            output, error = conn.execute_command("netstat -an | grep " + self.src_cluster.get_master_node().ip
                                                 + ":11210 | wc -l")
            conn.log_command_output(output, error)
            self.log.info("No. of memcached connections in iteration {0}:  {1}".format(i+1, output[0]))
            if int(output[0]) - int(before) > 5:
                self.fail("Number of memcached connections changed beyond allowed limit")

        for task in load_tasks:
            task.result()

        self.log.info("No. of memcached connections did not increase with pausing and resuming replication multiple times")

    def test_maxttl_setting(self):
        maxttl = int(self._input.param("maxttl", None))
        self.setup_xdcr_and_load()
        self.merge_all_buckets()
        self._wait_for_replication_to_catchup()
        self.sleep(maxttl, "waiting for docs to expire per maxttl properly")
        for bucket in self.src_cluster.get_buckets():
            items = RestConnection(self.src_master).get_active_key_count(bucket)
            self.log.info("Docs in source bucket is {0} after maxttl has elapsed".format(items))
            if items != 0:
                self.fail("Docs in source bucket is not 0 after maxttl has elapsed")
        self._wait_for_replication_to_catchup()
