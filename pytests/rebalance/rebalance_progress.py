import copy
import time
from rebalance.rebalance_base import RebalanceBaseTest
from membase.api.rest_client import RestConnection

class RebalanceProgressTests(RebalanceBaseTest):

    def setUp(self):
        super(RebalanceProgressTests, self).setUp()
        self.rest = RestConnection(self.master)
        self.num_views = self.input.param("num_views", 3)
        if self.num_views:
            self._create_indexes()

    def tearDown(self):
        super(RebalanceProgressTests, self).tearDown()

    def test_progress_rebalance_in(self):
        servers_in = self.servers[self.nodes_init : self.nodes_init + self.nodes_in]
        servers_init = self.servers[:self.nodes_init]

        rebalance = self.cluster.async_rebalance(servers_init, servers_in, [])
        self.sleep(5, "wait for rebalance start")
        previous_stats = self._get_detailed_progress()

        while rebalance.state != "FINISHED":
            new_stats = self._get_detailed_progress()
            if new_stats == {}:
                self.log.info("Got empty progress")
                break
            #vbuckets left should go decreasing
            #docsTotal and docsTransferred should be 0 in added nodes
            #docsTotal should not change
            #docsTransferred should go increasing
            self._check_stats(servers_in, previous_stats, new_stats, "outgoing",
                              docs_total=0, docs_transf=0)
            self._check_stats(servers_in, previous_stats, new_stats, "ingoing")
            self._check_stats(servers_init, previous_stats, new_stats, "ingoing",
                              docs_total=0, docs_transf=0)
            self._check_stats(servers_init, previous_stats, new_stats, "outgoing")
            #sum of sending and receiving vbuckets should coincide
            self._check_vb_sums(servers_init, servers_in, new_stats)
            previous_stats = copy.deepcopy(new_stats)
            time.sleep(10)
        rebalance.result()

    def test_progress_rebalance_out(self):
        with_failover = self.input.param("with_failover", False)
        servers_init = self.servers[:self.nodes_init]
        servers_out = self.servers[(self.nodes_init - self.nodes_out) : self.nodes_init]

        if with_failover:
            self.cluster.failover(servers_init, servers_out)
        rebalance = self.cluster.async_rebalance(servers_init, [], servers_out)
        self.sleep(5, "wait for rebalance start")
        previous_stats = self._get_detailed_progress()
        while rebalance.state != "FINISHED":
            new_stats = self._get_detailed_progress()
            if new_stats == {}:
                self.log.info("Got empty progress")
                break
            #vbuckets left should go decreasing
            #docsTotal should not change
            #docsTransferred should go increasing
            self._check_stats(servers_init, previous_stats, new_stats, "ingoing")
            self._check_stats(servers_init, previous_stats, new_stats, "outgoing")
            previous_stats = copy.deepcopy(new_stats)
            time.sleep(1)
        rebalance.result()

    def test_progress_rebalance_swap(self):
        if self.nodes_in != self.nodes_out:
            self.fail("nodes_in != nodes_out. Not a swap rebalance")
        if len(self.servers) < (self.nodes_init + self.nodes_in):
            self.log.error("Not enough VMs!")
            return
        servers_in = self.servers[self.nodes_init : self.nodes_init + self.nodes_in]
        servers_init = self.servers[:self.nodes_init]
        servers_unchanged = self.servers[:(self.nodes_init - self.nodes_out)]
        servers_out = self.servers[(self.nodes_init - self.nodes_out) : self.nodes_init]

        rebalance = self.cluster.async_rebalance(servers_init, servers_in, servers_out)
        self.sleep(5, "wait for rebalance start")
        previous_stats = self._get_detailed_progress()
        while rebalance.state != "FINISHED":
            new_stats = self._get_detailed_progress()
            if new_stats == {}:
                self.log.info("Got empty progress")
                break
            #vbuckets left should go decreasing
            #docsTotal and docsTransferred should be 0 in added nodes
            #no vbuckets moving for unchanged nodes
            #docsTotal should not change
            #docsTransferred should go increasing
            self._check_stats(servers_in, previous_stats, new_stats, "outgoing",
                              docs_total=0, docs_transf=0)
            self._check_stats(servers_in, previous_stats, new_stats, "ingoing")
            self._check_stats(servers_unchanged, previous_stats, new_stats, "ingoing",
                              active_vb=0, replica_vb=0)
            self._check_stats(servers_unchanged, previous_stats, new_stats, "outgoing",
                              active_vb=0, replica_vb=0)
            self._check_stats(servers_out, previous_stats, new_stats, "outgoing")

            #sum of sending and receiving vbuckets should coincide
            self._check_vb_sums(servers_in, servers_out, new_stats)
            previous_stats = copy.deepcopy(new_stats)
            time.sleep(1)
        rebalance.result()

    def test_progress_add_back_after_failover(self):
        servers_init = self.servers[:self.nodes_init]
        servers_failover = self.servers[(self.nodes_init - self.nodes_out) : self.nodes_init]
        servers_unchanged = self.servers[:(self.nodes_init - self.nodes_out)]
        nodes_all = self.rest.node_statuses()

        failover_nodes = []
        for failover_server in servers_failover:
            failover_nodes.extend([node for node in nodes_all if node.ip == failover_server.ip and \
                                         str(node.port) == failover_server.port])
        self.cluster.failover(servers_init, servers_failover)
        for node in failover_nodes:
            self.rest.add_back_node(node.id)

        rebalance = self.cluster.async_rebalance(servers_init, [], [])
        self.sleep(5, "wait for rebalance start")
        previous_stats = self._get_detailed_progress()
        while rebalance.state != "FINISHED":
            new_stats = self._get_detailed_progress()
            if new_stats == {}:
                self.log.info("Got empty progress")
                break
            #vbuckets left should go decreasing
            #docsTotal should not change
            #docsTransferred should go increasing
            self._check_stats(servers_unchanged, previous_stats, new_stats, "outgoing")
            self._check_stats(servers_failover, previous_stats, new_stats, "ingoing")
            previous_stats = copy.deepcopy(new_stats)
            time.sleep(1)
        rebalance.result()

    def _check_vb_sums(self, servers_ingoing, servers_outgoing, new_stats):
        active_vb_sum_1 = sum([new_stats[server.ip]["ingoing"]['activeVBucketsLeft'] for server in servers_ingoing])
        active_vb_sum_2 = sum([new_stats[server.ip]["outgoing"]['activeVBucketsLeft'] for server in servers_outgoing])
        self.assertTrue(active_vb_sum_1 == active_vb_sum_2,
                        "Active vbuckets left should be equal in servers_in and init. %s" % new_stats)

    def _check_stats(self, servers, previous_stats, new_stats, type,
                     docs_total=None, docs_transf=None,
                     active_vb=None, replica_vb=None):
        self.assertTrue(new_stats["buckets_count"] == len(self.buckets),
                        "Expected buckets %s. Actual stat %s" % (
                                len(self.buckets), new_stats))
        for server in servers:
            current_stat = new_stats[server.ip][type]
            previous_stat = previous_stats[server.ip][type]
            if new_stats["bucket"] != previous_stats["bucket"]:
                self.assertTrue(current_stat['activeVBucketsLeft'] >= previous_stat['activeVBucketsLeft'],
                            "activeVBucketsLeft for node %s increased! Previous stat %s. Actual: %s" % (
                                  server.ip, current_stat, previous_stat))
                self.assertTrue(current_stat['replicaVBucketsLeft'] >= previous_stat['replicaVBucketsLeft'],
                                "replicaVBucketsLeft for node %s increased! Previous stat %s. Actual: %s" % (
                                      server.ip, current_stat, previous_stat))
            else:
                self.assertTrue(current_stat['activeVBucketsLeft'] <= previous_stat['activeVBucketsLeft'],
                                "activeVBucketsLeft for node %s increased! Previous stat %s. Actual: %s" % (
                                      server.ip, current_stat, previous_stat))
                self.assertTrue(current_stat['replicaVBucketsLeft'] <= previous_stat['replicaVBucketsLeft'],
                                "replicaVBucketsLeft for node %s increased! Previous stat %s. Actual: %s" % (
                                      server.ip, current_stat, previous_stat))
                try:
                    if current_stat['docsTotal'] != previous_stat['docsTotal']:
                        self.log.warn("docsTotal for node %s changed! Previous stat %s. Actual: %s" % (
                                          server.ip, current_stat, previous_stat))
                except Exception as ex:
                    if previous_stat['docsTotal'] != 0 and current_stat['docsTotal'] == 0:
                        command = "sys:get_status({global, ns_rebalance_observer})."
                        self.log.info("posting: %s" % command)
                        self.rest.diag_eval(command)
                    raise ex
                self.assertTrue(current_stat['docsTransferred'] >= previous_stat['docsTransferred'],
                                "docsTransferred for node %s decreased! Previous stat %s. Actual: %s" % (
                                      server.ip, current_stat, previous_stat))
            if docs_total is not None:
                self.assertTrue(current_stat['docsTotal'] == docs_total,
                                "DocTotal for %s is %s, but should be %s. Stat %s" % (
                                        server.ip, current_stat['docsTotal'], docs_total, current_stat))
            if docs_transf is not None:
                self.assertTrue(current_stat['docsTransferred'] == docs_transf,
                                "docsTransferred for %s is %s, but should be %s. Stat %s" % (
                                        server.ip, current_stat['docsTotal'], docs_transf, current_stat))
            if active_vb is not None:
                self.assertTrue(current_stat['activeVBucketsLeft'] == active_vb,
                                "docsTransferred for %s is %s, but should be %s. Stat %s" % (
                                        server.ip, current_stat['activeVBucketsLeft'], active_vb, current_stat))
            if replica_vb is not None:
                self.assertTrue(current_stat['replicaVBucketsLeft'] == replica_vb,
                                "docsTransferred for %s is %s, but should be %s. Stat %s" % (
                                        server.ip, current_stat['activeVBucketsLeft'], active_vb, current_stat))
            self.log.info("Checked stat: %s" % new_stats)

    def _get_detailed_progress(self):
        detailed_progress = {}
        tasks = self.rest.ns_server_tasks()
        for task in tasks:
                if "detailedProgress" in task:
                    try:
                        if "perNode" in task["detailedProgress"]:
                            nodes = task["detailedProgress"]["perNode"]
                            for node in nodes:
                                detailed_progress[node.split('@')[1]] = nodes[node]
                        detailed_progress["bucket"] = task["detailedProgress"]["bucket"]
                        detailed_progress["buckets_count"] = task["detailedProgress"]["bucketsCount"]
                        break
                    except Exception as ex:
                        self.log.warn("Didn't get statistics %s" % str(ex))
        return detailed_progress

    def _create_indexes(self):
        tasks = []
        views = []
        for bucket in self.buckets:
            temp = self.make_default_views(self.default_view_name, self.num_views,
                                           False, different_map=True)
            temp_tasks = self.async_create_views(self.master, self.default_view_name,
                                                 temp, bucket)
            tasks += temp_tasks
            views += temp

        timeout = max(self.wait_timeout * 4,
                      len(self.buckets) * self.wait_timeout * self.num_items // 50000)

        for task in tasks:
            task.result(timeout)

        for bucket in self.buckets:
            for view in views:
                # run queries to create indexes
                self.cluster.query_view(self.master, self.default_view_name, view.name,
                                        {"stale" : "false", "limit" : 1000})
