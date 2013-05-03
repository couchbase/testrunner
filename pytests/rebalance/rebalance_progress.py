from rebalance.rebalance_base import RebalanceBaseTest
from membase.api.rest_client import RestConnection

class RebalanceProgressTests(RebalanceBaseTest):

    def setUp(self):
        super(RebalanceProgressTests, self).setUp()
        self.rest = RestConnection(self.master)

    def tearDown(self):
        super(RebalanceProgressTests, self).tearDown()

    def test_progress_rebalance_in(self):
        servers_in = self.servers[self.nodes_init : self.nodes_init + self.nodes_in]
        servers_init = self.servers[:self.nodes_init]

        rebalance = self.cluster.async_rebalance(servers_init, servers_in, [])

        previous_stats = self._get_detailed_progress()
        while rebalance.state != "FINISHED":
            new_stats = self._get_detailed_progress()
            #vbuckets left should go decreasing
            #docsTotal and docsTransferred should be 0 in added nodes
            #docsTotal should not change
            #docsTransferred should go increasing
            self._check_stats(servers_in, previous_stats, new_stats,
                              docs_total=0, docs_transf=0)
            self._check_stats(servers_init, previous_stats, new_stats)

            #sum of sending and receiving vbuckets should coincide
            self._check_vb_sums(servers_init, servers_in, new_stats)
            previous_stats = copy.deepcopy(new_stats)
            time.sleep(1)
        rebalance.result()

    def _check_vb_sums(self, servers1, servers2, new_stats):
        active_vb_sum_1 = sum([new_stats[server.ip]['activeVBucketsLeft'] for server in servers1])
        replica_vb_sum_1 = sum([new_stats[server.ip]['replicaVBucketsLeft'] for server in servers1])
        active_vb_sum_2 = sum([new_stats[server.ip]['activeVBucketsLeft'] for server in servers2])
        replica_vb_sum_2 = sum([new_stats[server.ip]['replicaVBucketsLeft'] for server in servers2])
        self.assertTrue(active_vb_sum_1 == active_vb_sum_2,
                        "Active vbuckets left should be equal in servers_in and init. %s" % new_stats)
        self.assertTrue(replica_vb_sum_1 == replica_vb_sum_2,
                        "Replica vbuckets left should be equal in servers_in and init. %s" % new_stats)

    def _check_stats(self, servers, previous_stats, new_stats,
                     docs_total=None, docs_transf=None):
        for server in servers:
            current_stat = new_stats[server.ip]
            previous_stat =  previous_stats[server.ip]
            self.assertTrue(current_stat['activeVBucketsLeft'] <= previous_stat['activeVBucketsLeft'],
                            "activeVBucketsLeft for node %s increased! Previous stat %s. Actual: %s" %(
                                  server.ip, current_stat, previous_stat))
            self.assertTrue(current_stat['replicaVBucketsLeft'] <= previous_stat['replicaVBucketsLeft'],
                            "replicaVBucketsLeft for node %s increased! Previous stat %s. Actual: %s" %(
                                  server.ip, current_stat, previous_stat))
            self.assertTrue(current_stat['docsTotal'] == previous_stat['docsTotal'],
                            "docsTotal for node %s changed! Previous stat %s. Actual: %s" %(
                                  server.ip, current_stat, previous_stat))
            self.assertTrue(current_stat['docsTransferred'] >= previous_stat['docsTransferred'],
                            "docsTransferred for node %s decreased! Previous stat %s. Actual: %s" %(
                                  server.ip, current_stat, previous_stat))
            if docs_total is not None:
                self.assertTrue(current_stat['docsTotal'] == docs_total,
                                "DocTotal for %s is %s, but should be %s" % (
                                        server.ip, current_stat['docsTotal'], docs_total))
            if docs_transf is not None:
                self.assertTrue(current_stat['docsTransferred'] == docs_transf,
                                "docsTransferred for %s is %s, but should be %s" % (
                                        server.ip, current_stat['docsTotal'], docs_transf)) 

    def _get_detailed_progress(self):
        detailed_progress = {}
        tasks = rest.ns_server_tasks()
        for task in tasks:
                if "detailedProgress" in task:
                    if u"perNode" in task["detailedProgress"]:
                        nodes = task["detailedProgress"]["perNode"]
                        for node in nodes:
                            detailed_progress[node.split('@')[1]] = nodes[node]
        return detailed_progress
