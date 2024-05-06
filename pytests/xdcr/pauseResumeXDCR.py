from .xdcrnewbasetests import XDCRNewBaseTest, Utility, OPS
from membase.api.rest_client import RestConnection
from threading import Thread

class PauseResumeTest(XDCRNewBaseTest):
    def setUp(self):
        super(PauseResumeTest, self).setUp()
        self.src_cluster = self.get_cb_cluster_by_name('C1')
        self.src_master = self.src_cluster.get_master_node()
        self.dest_cluster = self.get_cb_cluster_by_name('C2')
        self.dest_master = self.dest_cluster.get_master_node()
        self.pause_xdcr_cluster = self._input.param("pause", "")
        self.consecutive_pause_resume = int(self._input.param("consecutive_pause_resume", 1))
        self.delete_bucket = self._input.param("delete_bucket", "")
        self.reboot = self._input.param("reboot", "")
        self.pause_wait = self._input.param("pause_wait", 5)
        self.rebalance_in = self._input.param("rebalance_in", "")
        self.rebalance_out = self._input.param("rebalance_out", "")
        self.swap_rebalance = self._input.param("swap_rebalance", "")
        self._num_rebalance = self._input.param("num_rebalance", 1)
        self._failover = self._input.param("failover", "")
        self.encrypt_after_pause = self._input.param("encrypt_after_pause", "")
        self.num_views = self._input.param("num_views", 5)
        self.is_dev_ddoc = self._input.param("is_dev_ddoc", True)

    def tearDown(self):
        super(PauseResumeTest, self).tearDown()

    def __async_load_xdcr(self):
        if self._enable_cross_cluster_versioning != None or self._version_pruning_window_hrs != None:
            for cb_cluster in self.get_cb_clusters():
                rest1 = RestConnection(cb_cluster.get_master_node())
                for bucket in cb_cluster.get_buckets():
                    self.log.info("Mobile settings: Enable cross cluster versioning: {0}, Version pruning window hours: {1}".format(self._enable_cross_cluster_versioning, self._version_pruning_window_hrs))
                    if self._enable_cross_cluster_versioning != None:
                        test = rest1.change_bucket_props(bucket, enableCrossClusterVersioning=str(self._enable_cross_cluster_versioning).lower())
                        self.log.info("{0}".format(test))
                    if self._version_pruning_window_hrs != None and self._version_pruning_window_hrs >= 24:
                        test = rest1.change_bucket_props(bucket, versionPruningWindowHrs=self._version_pruning_window_hrs)
                        self.log.info("{0}".format(test))
        self.set_xdcr_topology()
        self.setup_all_replications()
        load_tasks = self.src_cluster.async_load_all_buckets(self._num_items)
        # if this is not a bidirectional replication or
        # we plan to delete dest bucket which might result in
        # uni-directional replication in the middle of the test
        if "bidirection" in self._rdirection and self.delete_bucket != "destination":
            load_tasks += self.dest_cluster.async_load_all_buckets(self._num_items)
        #load for 20 secs before pause
        self.sleep(20)
        return load_tasks

    def pause_xdcr(self):
        self.src_cluster.pause_all_replications(verify=True)
        self.dest_cluster.pause_all_replications(verify=True)

    def resume_xdcr(self):
        self.src_cluster.resume_all_replications(verify=True)
        self.dest_cluster.resume_all_replications(verify=True)

    # Test with pause and resume
    def replication_with_pause_and_resume(self):
        count = 0
        #start loading
        load_tasks = self.__async_load_xdcr()
        tasks = []
        #are we doing consecutive pause/resume
        while count < self.consecutive_pause_resume:
            self.pause_xdcr()
            if count < 1:
                # rebalance-in?
                if self.rebalance_in != "":
                    clusters = self.rebalance_in.split('-')
                    for cluster_name in clusters:
                        cluster = self.get_cb_cluster_by_name(cluster_name)
                        tasks.append(cluster.async_rebalance_in())

                # rebalance-out/failover
                if self.rebalance_out != "" or self._failover != "":
                    clusters = self.rebalance_out.split('-')
                    for cluster_name in clusters:
                        cluster = self.get_cb_cluster_by_name(cluster_name)
                        cluster.failover_and_rebalance_nodes(graceful=True)

                 # swap rebalance?
                if self.swap_rebalance != "":
                    clusters = self.swap_rebalance.split('-')
                    for cluster_name in clusters:
                        cluster = self.get_cb_cluster_by_name(cluster_name)
                        tasks.append(cluster.async_swap_rebalance())

                if self.encrypt_after_pause != "":
                    clusters = self.encrypt_after_pause.split('-')
                    for cluster_name in clusters:
                        cluster = self.get_cb_cluster_by_name(cluster_name)
                        for remote_cluster_ref in cluster.get_remote_clusters():
                            cluster.modify_remote_cluster(remote_cluster_ref.get_name(), True)

                # delete all destination buckets and recreate them?
                if self.delete_bucket == 'destination':
                    for bucket in self.dest_cluster.get_buckets():
                        self.dest_cluster.delete_bucket(bucket.name)
                    self.create_buckets_on_cluster(self.get_cb_cluster_by_name('C2'))

                # reboot nodes?
                if self.reboot == "dest_node":
                    self.dest_cluster.reboot_one_node(self, master=True)
                if self.reboot == "dest_cluster":
                    from .xdcrnewbasetests import NodeHelper
                    threads = []
                    for node in self.dest_cluster.get_nodes():
                        threads.append(Thread(target=NodeHelper.reboot_server,
                                              args=(node, self)))
                    for thread in threads:
                        thread.start()
                    for thread in threads:
                        thread.join()

            self.sleep(self.pause_wait)

            # resume all bidirectional replications
            self.resume_xdcr()
            count += 1

        # wait for rebalance to complete
        for task in tasks:
            self.log.info("Waiting for rebalance to complete...")
            task.result()

        # wait for load to complete
        try:
            for task in load_tasks:
                self.log.info("Waiting for loading to complete...")
                task.result()
        except Exception as e:
            self.log.info(e)

        self._wait_for_replication_to_catchup(timeout=500)
        self.pause_xdcr()
        self.perform_update_delete()
        self.resume_xdcr()
        self.verify_results()

    def view_query_pause_resume(self):
        bucket_type = self._input.param("bucket_type", "membase")
        if bucket_type == "ephemeral":
            self.log.info("Test case does not apply to ephemeral")
            return

        load_tasks = self.__async_load_xdcr()
        self.pause_xdcr()

        for bucket in self.dest_cluster.get_buckets():
            views = Utility.make_default_views(bucket.name, self.num_views,
                                            self.is_dev_ddoc)
        ddoc_name = "ddoc1"
        prefix = ("", "dev_")[self.is_dev_ddoc]
        query = {"full_set": "true", "stale": "false"}
        tasks = self.dest_cluster.async_create_views(ddoc_name, views)

        [task.result(self._poll_timeout) for task in tasks]
        # Wait for load data to finish if asynchronous
        [load_task.result() for load_task in load_tasks]

        # Resume the XDCR's paused
        self.resume_xdcr()

        self.merge_all_buckets()
        tasks = []
        for view in views:
            tasks.append(self.dest_cluster.async_query_view(
                            prefix + ddoc_name, view.name, query,
                            self.dest_cluster.get_buckets()[0].kvs[1].__len__()))

        [task.result(self._poll_timeout) for task in tasks]
        self.verify_results()

    def pause_resume_single_bucket(self):
        pause_bucket_name = self._input.param("pause_bucket", "default")
        load_tasks = self.__async_load_xdcr()
        for remote_cluster_ref in self.src_cluster.get_remote_clusters():
            for repl in remote_cluster_ref.get_replications():
                if repl.get_src_bucket().name == pause_bucket_name:
                    break
        repl.pause(verify=True)
        # wait till replication is paused
        self.sleep(10)
        # check if remote cluster is still replicating
        if repl._is_cluster_replicating():
            self.log.info("Replication has been paused at source, incoming "
                          "replication for bucket {0} is not affected"
                          .format(pause_bucket_name))
        # check if pause on one bucket does not affect other replications
        if repl._is_cluster_replicating():
            self.log.info("Pausing one replication does not affect other replications")
        else:
            self.log.info("Other buckets have completed replication")
        repl.resume(verify=True)

        [task.result() for task in load_tasks]
        self.verify_results()

