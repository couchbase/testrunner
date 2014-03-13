from threading import Thread
from membase.api.rest_client import RestConnection
from pauseResumeBaseTest import PauseResumeBaseTest


class PauseResumeTest(PauseResumeBaseTest):
    def setUp(self):
        super(PauseResumeTest, self).setUp()
        self.__verify_src = False

    def tearDown(self):
        super(PauseResumeTest, self).tearDown()

    def __pause_xdcr(self, verify=True):
        # Pause the XDCR's paused
        if "source" in self.pause_xdcr_cluster or self.pause_xdcr_cluster == "both":
            self.pause_all_replication(self.src_master, verify=verify)
        if "destination" in self.pause_xdcr_cluster or self.pause_xdcr_cluster == "both":
            self.pause_all_replication(self.dest_master, verify=verify)

    def __resume_xdcr(self, verify=True):
        # Resume the XDCR's paused
        if "source" in self.pause_xdcr_cluster or self.pause_xdcr_cluster == "both":
            self.resume_all_replication(self.src_master, verify=verify)
        if "destination" in self.pause_xdcr_cluster or self.pause_xdcr_cluster == "both":
            self.resume_all_replication(self.dest_master, verify=verify)

    def __async_load_and_pause_xdcr(self):
        load_tasks = self._async_load_all_buckets(self.src_master, self.gen_create, "create", 0)
        if self._replication_direction_str in "bidirection":
            load_tasks += self._async_load_all_buckets(self.dest_master, self.gen_create2, "create", 0)

        self.__pause_xdcr()
        return load_tasks

    def __update_deletes(self):
        if self._replication_direction_str in "unidirection":
            self._async_modify_data()
        elif self._replication_direction_str in "bidirection":
            self._async_update_delete_data()
        self.sleep(self._timeout / 2)

    def __merge_buckets(self):
        if self._replication_direction_str in "unidirection":
            self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
        elif self._replication_direction_str in "bidirection":
            self.merge_buckets(self.src_master, self.dest_master, bidirection=True)
            self.__verify_src = True

    # Test with pause and resume
    def replication_with_pause_and_resume(self):
        threads = []
        count = 0
        #get input params
        consecutive_pause_resume = int(self._input.param("consecutive_pause_resume", 1))
        delete_bucket = self._input.param("delete_bucket", None)
        reboot = self._input.param("reboot", None)
        pause_wait = int(self._input.param("pause_wait", 5))
        rebalance_in = self._input.param("rebalance_in", None)
        rebalance_out = self._input.param("rebalance_out", None)

        load_tasks = self._async_load_all_buckets(self.src_master, self.gen_create, "create", 0)
        if delete_bucket != "destination":
            load_tasks += self._async_load_all_buckets(self.dest_master, self.gen_create2, "create", 0)

        #are we doing consecutive pause/resume
        while count < consecutive_pause_resume:
            # pause all bidirectional replications
            self.__pause_xdcr()
            if count < 1:
                # TODO: add all rebalance, failover, code here

                # delete all destination buckets and recreate them?
                if delete_bucket == 'destination':
                    dest_buckets = self._get_cluster_buckets(self.dest_master)
                    for bucket in dest_buckets:
                        RestConnection(self.dest_master).delete_bucket(bucket.name)
                        self.buckets.remove(bucket)
                    self._create_buckets(self.dest_nodes)

                # reboot nodes?
                if reboot == "dest_node":
                    self.reboot_node(self.dest_nodes[len(self.dest_nodes) - 1])
                elif reboot == "dest_cluster":
                    for node in self.dest_nodes:
                        threads.append(Thread(target=self.reboot_node, args=(node,)))
                    for thread in threads:
                        thread.start()
                    for thread in threads:
                        thread.join()
            self.sleep(pause_wait)
            # resume all bidirectional replications
            self.__resume_xdcr()
            count += 1
        # we're done, wait for load ops to complete
        self.log.info("Waiting for loading to complete...")
        [load_task.result() for load_task in load_tasks]
        self.__update_deletes()
        self.__merge_buckets()
        self.verify_results(verify_src=self.__verify_src)

    def rebalance_out_with_pause_resume(self):
        load_tasks = self.__async_load_and_pause_xdcr()
        # Rebalance out
        tasks = []
        if "source" in self._rebalance and self._num_rebalance < len(self.src_nodes):
            remove_nodes = self.src_nodes[len(self.src_nodes) - self._num_rebalance:]
            if "source" in self._failover:
                self.cluster.failover(self.src_nodes, remove_nodes)
            tasks.extend(self._async_rebalance(self.src_nodes, [], remove_nodes))
            remove_node_ips = [remove_node.ip for remove_node in remove_nodes]
            self.log.info(" Starting rebalance-out nodes:{0} at Source cluster {1}".
                          format(remove_node_ips, self.src_master.ip))
            map(self.src_nodes.remove, remove_nodes)
            self.__verify_src = True
        if "destination" in self._rebalance and self._num_rebalance < len(self.dest_nodes):
            remove_nodes = self.dest_nodes[len(self.dest_nodes) - self._num_rebalance:]
            if "destination" in self._failover:
                self.cluster.failover(self.dest_nodes, remove_nodes)
            tasks.extend(self._async_rebalance(self.dest_nodes, [], remove_nodes))
            remove_node_ips = [remove_node.ip for remove_node in remove_nodes]
            self.log.info(" Starting rebalance-out nodes:{0} at Destination cluster {1}".
                          format(remove_node_ips, self.dest_master.ip))
            map(self.dest_nodes.remove, remove_nodes)

        # Resume the XDCR's paused
        self.__resume_xdcr()

        # Wait for Rebalance to finish
        [task.result() for task in tasks]

        # Wait for load data to finish if asynchronous
        [load_task.result() for load_task in load_tasks]

        self.__update_deletes()
        self.__merge_buckets()
        self.verify_results(verify_src=self.__verify_src)

    def rebalance_in_with_pause_resume(self):
        load_tasks = self.__async_load_and_pause_xdcr()

        # Rebalance in
        tasks = []
        add_nodes = self._floating_servers_set[0:self._num_rebalance]
        map(self._floating_servers_set.remove, add_nodes)
        if "source" in self._rebalance:
            tasks += self._async_rebalance(self.src_nodes, add_nodes, [])
            add_nodes_ips = [node.ip for node in add_nodes]
            self.log.info(" Starting rebalance-in nodes {0} at Source cluster {1}".
                          format(add_nodes_ips, self.src_master.ip))
            map(self.src_nodes.append, add_nodes)
            self.__verify_src = True
        add_nodes = self._floating_servers_set[:self._num_rebalance]
        map(self._floating_servers_set.remove, add_nodes)
        if "destination" in self._rebalance:
            tasks += self._async_rebalance(self.dest_nodes, add_nodes, [])
            add_nodes_ips = [node.ip for node in add_nodes]
            self.log.info(" Starting rebalance-in nodes{0} at Destination cluster {1}".
                          format(add_nodes_ips, self.dest_master.ip))
            map(self.dest_nodes.append, add_nodes)
            self.sleep(self._timeout / 2)

        # Resume the XDCR's paused
        self.__resume_xdcr()

        # Wait for Rebalance to finish
        [task.result() for task in tasks]

        # Wait for load data to finish if asynchronous
        [load_task.result() for load_task in load_tasks]

        self.__update_deletes()
        self.__merge_buckets()
        self.verify_results(verify_src=self.__verify_src)

    def swap_rebalance_with_pause_resume(self):
        load_tasks = self.__async_load_and_pause_xdcr()

        # Rebalance in
        tasks = []
        for _ in range(self._num_rebalance):
            if "source" in self._rebalance and self._num_rebalance < len(self.src_nodes):
                add_node = self._floating_servers_set.pop()
                remove_node = self.src_nodes[len(self.src_nodes) - 1]
                tasks.extend(self._async_rebalance(self.src_nodes, [add_node], [remove_node]))
                self.log.info(" Starting swap-rebalance at Source cluster {0} add node {1} and remove node {2}"
                              .format(self.src_master.ip, add_node.ip, remove_node.ip))
                self.src_nodes.remove(remove_node)
                self.src_nodes.append(add_node)
                self.__verify_src = True
            self.sleep(self._timeout / 2)
            if "destination" in self._rebalance and self._num_rebalance < len(self.dest_nodes):
                add_node = self._floating_servers_set.pop()
                remove_node = self.dest_nodes[len(self.dest_nodes) - 1]
                tasks.extend(self._async_rebalance(self.dest_nodes, [add_node], [remove_node]))
                self.log.info(" Starting swap-rebalance at Destination cluster {0} add node {1} and remove node {2}"
                              .format(self.dest_master.ip, add_node.ip, remove_node.ip))
                self.dest_nodes.remove(remove_node)
                self.dest_nodes.append(add_node)

        # Resume the XDCR's paused
        self.__resume_xdcr()

        # Wait for Rebalance to finish
        [task.result() for task in tasks]

        # Wait for load data to finish if asynchronous
        [load_task.result() for load_task in load_tasks]

        self.__update_deletes()
        self.__merge_buckets()
        self.verify_results(verify_src=self.__verify_src)

    def view_query_pause_resume(self):
        load_tasks = self.__async_load_and_pause_xdcr()

        dest_buckets = self._get_cluster_buckets(self.src_master)
        for bucket in dest_buckets:
            views = self.make_default_views(bucket.name, self._num_views,
                                            self._is_dev_ddoc)
        ddoc_name = "ddoc1"
        prefix = ("", "dev_")[self._is_dev_ddoc]

        query = {"full_set": "true", "stale": "false"}
        tasks = self.async_create_views(self.dest_master, ddoc_name, views,
                                         self.default_bucket_name)

        [task.result(self._poll_timeout) for task in tasks]
        # Wait for load data to finish if asynchronous
        [load_task.result() for load_task in load_tasks]

        # Resume the XDCR's paused
        self.__resume_xdcr()

        self.__merge_buckets()
        self._verify_stats_all_buckets(self.src_nodes)
        self._verify_stats_all_buckets(self.dest_nodes)
        tasks = []
        for view in views:
            tasks.append(self.cluster.async_query_view(self.dest_master,
                                                       prefix + ddoc_name,
                                                       view.name, query,
                                                       dest_buckets[0].kvs[1].__len__()))

        [task.result(self._poll_timeout) for task in tasks]
        self.verify_results(verify_src=self.__verify_src)