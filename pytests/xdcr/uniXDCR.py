import copy
import time
from random import randrange
from threading import Thread

from couchbase.documentgenerator import BlobGenerator
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection
from memcached.helper.data_helper import LoadWithMcsoda
from xdcrbasetests import XDCRReplicationBaseTest


#Assumption that at least 2 nodes on every cluster
class unidirectional(XDCRReplicationBaseTest):
    def setUp(self):
        super(unidirectional, self).setUp()

    def tearDown(self):
        super(unidirectional, self).tearDown()

    """Testing Unidirectional load( Loading only at source) Verifying whether XDCR replication is successful on
    subsequent destination clusters.Create/Update/Delete operations are performed based on doc-ops specified by the user. """

    def load_with_ops(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)

        self._modify_src_data()

        self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
        self.sleep(self.wait_timeout / 2)
        self.verify_results()

    """Testing Unidirectional load( Loading only at source) Verifying whether XDCR replication is successful on
    subsequent destination clusters. Create/Update/Delete are performed in parallel- doc-ops specified by the user. """

    def load_with_async_ops(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)

        self._async_modify_data()

        self.merge_buckets(self.src_master, self.dest_master, bidirection=False)

        self._wait_flusher_empty(self.src_master, self.src_nodes)
        self.sleep(self.wait_timeout / 2)
        self.verify_results()

    """Testing Unidirectional load( Loading only at source). Failover node at Source/Destination while
    Create/Update/Delete are performed after based on doc-ops specified by the user.
    Verifying whether XDCR replication is successful on subsequent destination clusters. """

    def load_with_ops_with_warmup(self):
        #warmup
        warmupnodes = []
        if self._warmup is not None:
            if "source" in self._warmup:
                warmupnodes.append(self.src_nodes[randrange(1, len(self.src_nodes))])
            if "destination" in self._warmup:
                warmupnodes.append(self.dest_nodes[randrange(1, len(self.dest_nodes))])
        for node in warmupnodes:
            self.do_a_warm_up(node)
        self.sleep(self.wait_timeout)

        self._modify_src_data()

        self._wait_flusher_empty(self.src_master, self.src_nodes)

        self.merge_buckets(self.src_master, self.dest_master, bidirection=False)

        self.wait_warmup_completed(warmupnodes)

        self.verify_results()

    def load_with_ops_with_warmup_master(self):
        #warmup
        warmupnodes = []
        if self._warmup is not None:
            if "source" in self._warmup:
                warmupnodes.append(self.src_master)
            if "destination" in self._warmup:
                warmupnodes.append(self.dest_master)
        for node in warmupnodes:
            self.do_a_warm_up(node)
        self.sleep(self.wait_timeout)
        self._modify_src_data()
        self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
        self.wait_warmup_completed(warmupnodes)
        self.verify_results()

    def load_with_async_ops_with_warmup(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        self.sleep(self.wait_timeout)

        #warmup
        warmupnodes = []
        if self._warmup is not None:
            if "source" in self._warmup:
                warmupnodes.append(self.src_nodes[randrange(1, len(self.src_nodes))])
            if "destination" in self._warmup:
                warmupnodes.append(self.dest_nodes[randrange(1, len(self.dest_nodes))])
        for node in warmupnodes:
            self.do_a_warm_up(node)
        self.sleep(self.wait_timeout)
        self._async_modify_data()
        self.sleep(30)
        self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
        self._wait_flusher_empty(self.src_master, self.src_nodes)
        self.wait_warmup_completed(warmupnodes)
        self.verify_results()

    def load_with_async_ops_with_warmup_master(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        self.sleep(self.wait_timeout)

        #warmup
        warmupnodes = []
        if self._warmup is not None:
            if "source" in self._warmup:
                warmupnodes.append(self.src_master)
            if "destination" in self._warmup:
                warmupnodes.append(self.dest_master)
        for node in warmupnodes:
            self.do_a_warm_up(node)
        self.sleep(self.wait_timeout)
        self._async_modify_data()
        self.sleep(self.wait_timeout / 2)
        self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
        self._wait_flusher_empty(self.dest_master, self.dest_nodes)
        self.wait_warmup_completed(warmupnodes)
        self.verify_results()

    def load_with_failover(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        self.sleep(self.wait_timeout)

        if self._failover is not None:
            if "source" in self._failover:
                if len(self.src_nodes) > 1:
                    i = len(self.src_nodes) - 1
                    self.log.info(
                            " Failing over Source Non-Master Node {0}:{1}".format(self.src_nodes[i].ip, self.src_nodes[i].port))
                    self.cluster.failover(self.src_nodes, [self.src_nodes[i]])
                    self.log.info(" Rebalance out Source Non-Master Node {0}:{1}".format(self.src_nodes[i].ip,
                                                                                          self.src_nodes[i].port))
                    self.cluster.rebalance(self.src_nodes, [], [self.src_nodes[i]])
                    self.src_nodes.remove(self.src_nodes[i])
                else:
                    self.log.info("Number of nodes {0} is less than minimum '2' needed for failover on a cluster.".format(
                                    len(self.src_nodes)))
            if "destination" in self._failover:
                if len(self.dest_nodes) > 1:
                    i = len(self.dest_nodes) - 1
                    self.log.info(" Failing over Destination Non-Master Node {0}:{1}".format(self.dest_nodes[i].ip,
                                                                                              self.dest_nodes[i].port))
                    self.cluster.failover(self.dest_nodes, [self.dest_nodes[i]])
                    self.log.info(" Rebalance out Destination Non-Master Node {0}:{1}".format(self.dest_nodes[i].ip,
                                                                                               self.dest_nodes[i].port))
                    self.cluster.rebalance(self.dest_nodes, [], [self.dest_nodes[i]])
                    self.dest_nodes.remove(self.dest_nodes[i])
                else:
                    self.log.info("Number of nodes {0} is less than minimum '2' needed for failover on a cluster.".format(
                                    len(self.dest_nodes)))

        self.sleep(self.wait_timeout / 6)
        self._async_modify_data()
        self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
        self.verify_results()

    def load_with_failover_then_add_back(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        self.sleep(self.wait_timeout)

        if self._failover is not None:
            if "source" in self._failover:
                if len(self.src_nodes) > 1:
                    i = len(self.src_nodes) - 1
                    self.log.info(
                            " Failing over Source Non-Master Node {0}:{1}".format(self.src_nodes[i].ip, self.src_nodes[i].port))
                    self.cluster.failover(self.src_nodes, [self.src_nodes[i]])
                    self.log.info(" Add back Source Non-Master Node {0}:{1}".format(self.src_nodes[i].ip,
                                                                                     self.src_nodes[i].port))
                    self.adding_back_a_node(self.src_master, self.src_nodes[i])
                    self.cluster.rebalance(self.src_nodes, [], [])
                else:
                    self.log.info("Number of nodes {0} is less than minimum '2' needed for failover on a cluster.".format(
                                    len(self.src_nodes)))
            if "destination" in self._failover:
                if len(self.dest_nodes) > 1:
                    i = len(self.dest_nodes) - 1
                    self.log.info(" Failing over Destination Non-Master Node {0}:{1}".format(self.dest_nodes[i].ip,
                                                                                              self.dest_nodes[i].port))
                    self.cluster.failover(self.dest_nodes, [self.dest_nodes[i]])
                    self.log.info(" Add back Destination Non-Master Node {0}:{1}".format(self.dest_nodes[i].ip,
                                                                                          self.dest_nodes[i].port))
                    self.adding_back_a_node(self.dest_master, self.dest_nodes[i])
                    self.cluster.rebalance(self.dest_nodes, [], [])
                else:
                    self.log.info("Number of nodes {0} is less than minimum '2' needed for failover on a cluster.".format(
                                    len(self.dest_nodes)))

        now = time.time()
        self._async_modify_data()
        self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
        #The failover/rebalance things at destination may take some time to finish.
        #During the process, each vb replicator source XDCR will probe every 30 seconds,
        #once the failover/rebalance is over and new vb map is generated, source XDCR will resume replication.
        self.sleep(max(0, 30 + now - time.time()))
        self.verify_results()

    """Testing Unidirectional load( Loading only at source). Failover node at Source/Destination while
    Create/Update/Delete are performed in parallel based on doc-ops specified by the user.
    Verifying whether XDCR replication is successful on subsequent destination clusters. """

    def load_with_failover_master(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        self.sleep(self.wait_timeout)

        if self._failover is not None:
            if "source" in self._failover:
                self.log.info(" Failing over Source Master Node {0}:{1}".format(self.src_master.ip, self.src_master.port))
                prev_master_id = RestConnection(self.src_master).get_nodes_self().id
                self.cluster.failover(self.src_nodes, [self.src_master])
                self.log.info(" Rebalance out Source Master Node {0}".format(self.src_master.ip))
                self.cluster.rebalance(self.src_nodes, [], [self.src_master])
                self.src_nodes.remove(self.src_master)
                self.src_master = self.src_nodes[0]
                rest = RestConnection(self.src_master)
                master_id = rest.get_nodes_self().id
                for bucket in self.buckets:
                    if bucket.master_id == prev_master_id:
                        bucket.master_id = master_id

            if "destination" in self._failover:
                self.log.info(" Failing over Destination Master Node {0}".format(self.dest_master.ip))
                prev_master_id = RestConnection(self.dest_master).get_nodes_self().id
                self.cluster.failover(self.dest_nodes, [self.dest_master])
                self.log.info(" Rebalance out Destination Master Node {0}".format(self.dest_master.ip))
                self.cluster.rebalance(self.dest_nodes, [], [self.dest_master])
                self.dest_nodes.remove(self.dest_master)
                self.dest_master = self.dest_nodes[0]
                rest = RestConnection(self.dest_master)
                master_id = rest.get_nodes_self().id
                for bucket in self.buckets:
                    if bucket.master_id == prev_master_id:
                        bucket.master_id = master_id

        self.sleep(self.wait_timeout / 6)
        self._async_modify_data()
        self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
        self.verify_results()

    """Testing Unidirectional load( Loading only at source). Failover node at Source/Destination while
    Create/Update/Delete are performed in parallel based on doc-ops specified by the user.
    Verifying whether XDCR replication is successful on subsequent destination clusters. """

    def load_with_async_failover(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        self.sleep(self.wait_timeout)

        tasks = []
        """Setting up failover while creates/updates/deletes at source nodes"""
        if self._failover is not None:
            if "source" in self._failover:
                i = len(self.src_nodes) - 1
                tasks.extend(self._async_failover(self.src_nodes, [self.src_nodes[i]]))
                self.log.info(" Failing over Source Node {0}".format(self.src_nodes[i].ip))
            if "destination" in self._failover:
                i = len(self.dest_nodes) - 1
                tasks.extend(self._async_failover(self.dest_nodes, [self.dest_nodes[i]]))
                self.log.info(" Failing over Destination Node {0}".format(self.dest_nodes[i].ip))

        self._async_modify_data()

        self.sleep(self.wait_timeout / 4)
        for task in tasks:
            task.result()

        self.merge_buckets(self.src_master, self.dest_master, bidirection=False)

        if self._failover is not None:
            if "source" in self._failover:
                self.log.info(" Rebalance out Source Node {0}".format(self.src_nodes[i].ip))
                self.cluster.rebalance(self.src_nodes, [], [self.src_nodes[i]])
                self.src_nodes.pop(i)

            if "destination" in self._failover:
                self.log.info(" Rebalance out Destination Node {0}".format(self.dest_nodes[i].ip))
                self.cluster.rebalance(self.dest_nodes, [], [self.dest_nodes[i]])
                self.dest_nodes.pop(i)

        self.verify_results()
            #ToDO - Failover and ADD BACK NODE

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
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        self.sleep(self.wait_timeout)

        src_buckets = self._get_cluster_buckets(self.src_master)
        for bucket in src_buckets:
            views = self.make_default_views(bucket.name, self._num_views, self._is_dev_ddoc)
        ddoc_name = "ddoc1"
        prefix = ("", "dev_")[self._is_dev_ddoc]

        query = {"full_set" : "true", "stale" : "false"}

        tasks = self.async_create_views(self.src_master, ddoc_name, views, self.default_bucket_name)
        tasks += self.async_create_views(self.dest_master, ddoc_name, views, self.default_bucket_name)
        for task in tasks:
            task.result(self._poll_timeout)

        self.disable_compaction()
        fragmentation_monitor = self.cluster.async_monitor_view_fragmentation(self.src_master,
            prefix + ddoc_name, self.fragmentation_value, "default")

        #generate load until fragmentation reached
        while fragmentation_monitor.state != "FINISHED":
            #update docs to create fragmentation
            self._load_all_buckets(self.src_master, self.gen_update, "update", self._expires)
            for view in views:
                # run queries to create indexes
                self.cluster.query_view(self.src_master, prefix + ddoc_name, view.name, query)
        fragmentation_monitor.result()

        compaction_task = self.cluster.async_compact_view(self.src_master, prefix + ddoc_name, 'default')

        result = compaction_task.result()
        self.assertTrue(result)

        self.merge_buckets(self.src_master, self.dest_master, bidirection=False)

        self.verify_results()

    def replication_with_view_queries_and_ops(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        self.sleep(self.wait_timeout)

        src_buckets = self._get_cluster_buckets(self.src_master)
        dest_buckets = self._get_cluster_buckets(self.src_master)
        for bucket in src_buckets:
            views = self.make_default_views(bucket.name, self._num_views, self._is_dev_ddoc)

        ddoc_name = "ddoc1"
        prefix = ("", "dev_")[self._is_dev_ddoc]

        query = {"full_set" : "true", "stale" : "false"}

        tasks = self.async_create_views(self.src_master, ddoc_name, views, self.default_bucket_name)
        tasks += self.async_create_views(self.dest_master, ddoc_name, views, self.default_bucket_name)
        for task in tasks:
            task.result(self._poll_timeout)

        tasks = []
        #Setting up doc-ops at source nodes
        if self._doc_ops is not None:
            # allows multiple of them but one by one on either of the clusters
            if "update" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_update, "update", self._expires))

            if "delete" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_delete, "delete", 0))

            self.sleep(5)

        while True:
            for view in views:
                self.cluster.query_view(self.src_master, prefix + ddoc_name, view.name, query)
                self.cluster.query_view(self.dest_master, prefix + ddoc_name, view.name, query)
            if set([task.state for task in tasks]) != set(["FINISHED"]):
                continue
            else:
                if "update" in self._doc_ops and self._wait_for_expiration:
                    self.sleep(self._expires)
                break

        tasks = []
        for view in views:
            tasks.append(self.cluster.async_query_view(self.src_master, prefix + ddoc_name, view.name, query, src_buckets[0].kvs[1].__len__()))
            tasks.append(self.cluster.async_query_view(self.dest_master, prefix + ddoc_name, view.name, query, dest_buckets[0].kvs[1].__len__()))

        for task in tasks:
            task.result(self._poll_timeout)

        self.merge_buckets(self.src_master, self.dest_master, bidirection=False)

        self.verify_results()

    """Replication with disabled/enabled ddoc compaction on source cluster.

        This test begins by loading a given number of items on the source cluster.
        Then we disabled or enabled compaction on both clusters( set via params).
        Then we mutate and delete data on the source cluster 3 times.
        After deletion we recreate deleted items. When data was changed 3 times
        we perform a full verification: wait for the disk queues to drain
        and then verify that there has been no data loss on both all clusters."""
    def replication_with_disabled_ddoc_compaction(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        self.sleep(self.wait_timeout)

        if self.disable_src_comp:
            self.disable_compaction(self.src_master)
        if self.disable_dest_comp:
            self.disable_compaction(self.dest_master)

        # perform doc's ops 3 times to increase rev number
        for i in range(3):
            self._async_modify_data()
            #restore deleted items
            if self._doc_ops is not None:
                if "delete" in self._doc_ops:
                    tasks = self._async_load_all_buckets(self.src_master, self.gen_delete, "create", 0)
                    self.sleep(5)
                    for task in tasks:
                        task.result()
        self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
        self.verify_results()

    def replication_while_rebooting_a_non_master_destination_node(self):
        self.set_xdcr_param("xdcrFailureRestartInterval", 1)

        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        self._async_modify_data()
        self.sleep(self.wait_timeout)

        i = len(self.dest_nodes) - 1
        shell = RemoteMachineShellConnection(self.dest_nodes[i])
        type = shell.extract_remote_info().type.lower()
        if type == 'windows':
            o, r = shell.execute_command("shutdown -r -f -t 0")
        elif type == 'linux':
            o, r = shell.execute_command("reboot")
        shell.log_command_output(o, r)
        shell.disconnect()
        self.sleep(60, "after rebooting node")
        num = 0
        while num < 10:
            try:
                shell = RemoteMachineShellConnection(self.dest_nodes[i])
            except BaseException, e:
                self.log.warn("node {0} is unreachable".format(self.dest_nodes[i].ip))
                self.sleep(60, "still can't connect to node")
                num += 1
            else:
                break
        if num == 10:
            self.fail("Can't establish SSH session after 10 minutes")
        shell.disable_firewall()
        shell.disconnect()
        self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
        self.wait_node_restarted(self.dest_nodes[i], wait_time=self.wait_timeout * 4, wait_if_warmup=True)
        self.verify_results()

    def replication_with_firewall_enabled(self):
        self.set_xdcr_param("xdcrFailureRestartInterval", 1)
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        self.sleep(self.wait_timeout / 6)
        self._async_modify_data()
        self.sleep(self.wait_timeout / 6)
        self._enable_firewall(self.dest_master)
        self.sleep(self.wait_timeout / 2)
        self._disable_firewall(self.dest_master)
        self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
        self.verify_results()

    """Testing Unidirectional append ( Loading only at source) Verifying whether XDCR replication is successful on
    subsequent destination clusters. """

    def test_append(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
        self.sleep(self.wait_timeout)
        self.verify_results()
        loop_count = self._input.param("loop_count", 20)
        for i in xrange(loop_count):
            self.log.info("Append iteration # %s" % i)
            gen = copy.deepcopy(self.gen_append)
            self._load_all_buckets(self.src_master, gen, "append", 0,
                                   batch_size=1)
            self.sleep(self.wait_timeout)
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
        collectinfotest.CollectinfoTests.verify_results(
                                                    self, self.log_filename)

    """ Verify the fix for MB-9548"""
    def test_verify_replications_stream_delete(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
        self.sleep(60)
        self.verify_results()
        src_buckets = self._get_cluster_buckets(self.src_master)
        rest_conn = RestConnection(self.src_master)
        replications = rest_conn.get_replications()
        self.assertTrue(replications, "Number of replication streams should not be 0")
        for bucket in src_buckets:
            self.cluster.bucket_delete(self.src_master, bucket.name)

        replications = rest_conn.get_replications()
        self.assertTrue(not replications, "No replication streams should exists after deleting the buckets")

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
            self.load_with_ops()
            self.shell = RemoteMachineShellConnection(self.src_master)
            self.shell.execute_cbcollect_info(zip_file)
            if self.shell.extract_remote_info().type.lower() != "windows":
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
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        self.sleep(self.wait_timeout)

        # Remove destination buckets
        for bucket in self._get_cluster_buckets(self.dest_master):
            self.cluster.bucket_delete(self.dest_master, bucket)

        # Create destination buckets
        self._create_buckets(self.dest_nodes)

        self._async_modify_data()
        self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
        self.verify_results()

    def flush_dest_buckets(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        self.sleep(self.wait_timeout)

        # flush destination buckets
        tasks = []
        for bucket in self._get_cluster_buckets(self.dest_master):
            tasks.append(self.cluster.async_bucket_flush(self.dest_master,
                                                         bucket))
        [task.result() for task in tasks]

        self._async_modify_data()
        self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
        self.verify_results()

    # Nodes Crashing Scenarios
    def __kill_processes(self, crashed_nodes=[]):
        for node in crashed_nodes:
            shell = RemoteMachineShellConnection(node)
            os_info = shell.extract_remote_info()
            shell.kill_erlang(os_info)
            shell.disconnect()

    def __start_cb_server(self, node):
        shell = RemoteMachineShellConnection(node)
        shell.start_couchbase()
        shell.disconnect()

    def test_node_crash_master(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        crashed_nodes = []
        crash = self._input.param("crash", "source").split('-')
        if "source" in crash:
            crashed_nodes.append(self.src_master)
        if "destination" in crash:
            crashed_nodes.append(self.dest_master)

        self.__kill_processes(crashed_nodes)

        for crashed_node in crashed_nodes:
            self.__start_cb_server(crashed_node)
        self.wait_warmup_completed(crashed_nodes)

        self._async_modify_data()
        self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
        self.verify_results(verify_src=True)

    # Disaster at site.
    # 1. Crash Source Cluster., Sleep n second
    # 2. Crash Dest Cluster.
    # 3. Wait for Source Cluster to warmup. Load more data and perform mutations on Src.
    # 4. Wait for Dest to warmup.
    # 5. Verify data.
    def test_node_crash_cluster(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        crashed_nodes = []
        crash = self._input.param("crash", "source").split('-')
        if "source" in crash:
            crashed_nodes += self.src_nodes
            self.__kill_processes(crashed_nodes)
            self.sleep(30)
        if "destination" in crash:
            crashed_nodes += self.dest_nodes
            self.__kill_processes(crashed_nodes)

        for crashed_node in crashed_nodes:
            self.__start_cb_server(crashed_node)

        if "source" in crash:
            self.wait_warmup_completed(self.src_nodes)
            self.gen_create2 = BlobGenerator('loadTwo', 'loadTwo', self._value_size, end=self.num_items)
            self._load_all_buckets(self.src_master, self.gen_create2, "create", 0)

        self._async_modify_data()

        if "destination" in crash:
            self.wait_warmup_completed(self.dest_nodes)

        self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
        self.verify_results(verify_src=True)
