from couchbase_helper.document import View
from .hostnamemgmt_base import HostnameBaseTests
from membase.api.rest_client import RestConnection, RestHelper
from remote.remote_util import RemoteMachineShellConnection

class HostnameMgmtTests(HostnameBaseTests):

    def setUp(self):
        super(HostnameMgmtTests, self).setUp()

    def tearDown(self):
        super(HostnameMgmtTests, self).tearDown()

    def test_add_node(self):
        hostnames = self.rename_nodes(self.servers)
        self.verify_referenced_by_names(self.servers, hostnames)
        self._set_hostames_to_servers_objs(hostnames)
        master_rest = RestConnection(self.master)
        self.sleep(5, "Sleep to wait renaming")
        for server in self.servers[1:self.nodes_in + 1]:
            master_rest.add_node(server.rest_username, server.rest_password, hostnames[server], server.port)
        self.verify_referenced_by_names(self.servers, hostnames)

    def test_add_nodes_and_rebalance(self):
        hostnames = self.rename_nodes(self.servers)
        self.verify_referenced_by_names(self.servers, hostnames)
        self._set_hostames_to_servers_objs(hostnames)
        self.cluster.rebalance(self.servers[:1], self.servers[1:self.nodes_in + 1], [], use_hostnames=True)
        self.verify_referenced_by_names(self.servers, hostnames)

    def test_rename_twice(self):
        name = self.input.param('name', '')
        for i in range(2):
            try:
                hostnames = {}
                if name:
                    for server in self.servers[:self.nodes_init]:
                        hostnames[server] = name
                hostnames = self.rename_nodes(self.servers[:self.nodes_init], names=hostnames)
                self.verify_referenced_by_names(self.servers[:self.nodes_init], hostnames)
            except Exception as ex:
                if self.error:
                    self.assertTrue(str(ex).find(self.error) != -1,
                                    "Error expected: %s. Actual: %s" % (self.error, str(ex)))
                else:
                    raise ex
            else:
                if self.error:
                    raise Exception("Error '%s' is expected but not raised" % self.error)

    def test_rename_rebalance(self):
        if len(self.servers) < 2:
            self.fail("test require more than 1 node")
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 self.servers[self.nodes_init:self.nodes_in + self.nodes_init], [])
        self.sleep(5, 'wait for some progress in rebalance...')
        try:
            hostnames = self.rename_nodes(self.servers[:self.nodes_in + self.nodes_init])
        except Exception as ex:
            if self.error:
                self.assertTrue(str(ex).find(self.error) != -1, "Unexpected error msg")
            else:
                raise ex
        self.verify_referenced_by_ip(self.servers[:self.nodes_in + self.nodes_init])
        rebalance.result()

    def test_rename_with_index(self):
        self.defaul_map_func = "function (doc) {\n  emit(doc._id, doc);\n}"
        self.default_view_name = "default_view"
        self.default_view = View(self.default_view_name, self.defaul_map_func, None)
        num_views = self.input.param('num_views', 1)
        ddoc_name = views_prefix = 'hostname_mgmt'
        query = {'stale' : 'false'}
        views = []
        for bucket in self.buckets:
            view = self.make_default_views(views_prefix, num_views, different_map=True)
            self.create_views(self.master, ddoc_name, view, bucket)
            views += view
        for bucket in self.buckets:
            for view in views:
                self.cluster.query_view(self.master, ddoc_name, view.name, query)
        hostnames = self.rename_nodes(self.servers[:self.nodes_init])
        self.verify_referenced_by_names(self.servers[:self.nodes_init], hostnames)
        self._set_hostames_to_servers_objs(hostnames)
        query = {'stale' : 'ok'}
        for server in self.servers[:self.nodes_init]:
            for view in views:
                self.cluster.query_view(server, ddoc_name, view.name, query)

    def test_rename_with_warm_up(self):
        if len(self.servers) < 2:
            self.fail("test require more than 1 node")
        hostnames = self.rename_nodes(self.servers[:self.nodes_in + self.nodes_init])
        self._set_hostames_to_servers_objs(hostnames)
        self.cluster.rebalance(self.servers[:self.nodes_init],
                               self.servers[self.nodes_init:self.nodes_in + self.nodes_init], [], use_hostnames=True)
        self.verify_referenced_by_names(self.servers[:self.nodes_init], hostnames)
        warmup_node = self.servers[:self.nodes_in + self.nodes_init][-1]
        shell = RemoteMachineShellConnection(warmup_node)
        shell.stop_couchbase()
        self.sleep(10)
        shell.start_couchbase()
        shell.disconnect()
        self.verify_referenced_by_names(self.servers[:self.nodes_in + self.nodes_init], hostnames)

    def test_rename_negative(self):
        if len(self.servers) < 2:
            self.fail("test require more than 1 node")
        hostnames = self.rename_nodes(self.servers[:2])
        self._set_hostames_to_servers_objs(hostnames)
        self.verify_referenced_by_names(self.servers[:2], hostnames)
        try:
            self.rename_nodes(self.servers[:1], names={self.servers[0]: hostnames[self.servers[1]]})
        except Exception as  ex:
            if self.error:
                self.assertTrue(str(ex).find(self.error) != -1, "Unexpected error msg")
            else:
                raise ex

    def test_rename_negative_name_with_space(self):
        try:
            self.rename_nodes(self.servers[:1], names={self.servers[0]: ' '})
        except Exception as  ex:
            if self.error:
                self.assertTrue(str(ex).find(self.error) != -1, "Unexpected error msg")
            else:
                raise ex

    def test_rename_rebalance_start_stop(self):
        expected_progress = self.input.param('expected_progress', 30)
        if len(self.servers) < 2:
            self.fail("test require more than 1 node")
        hostnames = self.rename_nodes(self.servers[:self.nodes_in + self.nodes_init])
        self._set_hostames_to_servers_objs(hostnames)
        self.verify_referenced_by_names(self.servers[:self.nodes_in + self.nodes_init], hostnames)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 self.servers[self.nodes_init:self.nodes_in + self.nodes_init], [],
                                                 use_hostnames=True)
        self.sleep(3, 'wait for some progress in rebalance...')
        rest = RestConnection(self.master)
        reached = RestHelper(rest).rebalance_reached(expected_progress)
        self.assertTrue(reached, "rebalance failed or did not reach {0}%".format(expected_progress))
        if not RestHelper(rest).is_cluster_rebalanced():
            stopped = rest.stop_rebalance(wait_timeout=self.wait_timeout // 3)
            self.assertTrue(stopped, msg="unable to stop rebalance")
        self.verify_referenced_by_names(self.servers[:self.nodes_in + self.nodes_init], hostnames)
        self.cluster.rebalance(self.servers[:self.nodes_init + self.nodes_init],
                               [], [], use_hostnames=True)
        self.verify_referenced_by_names(self.servers[:self.nodes_in + self.nodes_init], hostnames)

    def test_rename_swap_rebalance(self):
        if len(self.servers) < 3:
            self.fail("test require more than 2 nodes")
        hostnames = self.rename_nodes(self.servers[:self.nodes_in + self.nodes_init + self.nodes_out])
        self._set_hostames_to_servers_objs(hostnames)
        self.verify_referenced_by_names(self.servers[:self.nodes_in + self.nodes_init + self.nodes_out], hostnames)
        self.cluster.rebalance(self.servers[:self.nodes_init],
                               self.servers[self.nodes_init:self.nodes_in + self.nodes_init], [],
                               use_hostnames=True)
        self.verify_referenced_by_names(self.servers[:self.nodes_in + self.nodes_init + self.nodes_out], hostnames)
        rebalance = self.cluster.rebalance(self.servers[self.nodes_init:self.nodes_in + self.nodes_init],
                                           self.servers[self.nodes_in + self.nodes_init:self.nodes_in + self.nodes_init + self.nodes_out],
                                           self.servers[self.nodes_init:self.nodes_in + self.nodes_init],
                                           use_hostnames=True)
        self.verify_referenced_by_names(self.servers[:self.nodes_in + self.nodes_init + self.nodes_out], hostnames)

    def test_rename_failover_add_back(self):
        if len(self.servers) < 2:
            self.fail("test require more than 1 node")
        failover_factor = self.input.param("failover-factor", 1)
        failover_nodes = self.servers[self.nodes_in : self.nodes_in + failover_factor + 1]
        hostnames = self.rename_nodes(self.servers[:self.nodes_in + failover_factor + 1])
        self._set_hostames_to_servers_objs(hostnames)
        self.verify_referenced_by_names(self.servers[:self.nodes_in + failover_factor + 1], hostnames)
        self.cluster.rebalance(self.servers[:self.nodes_init],
                               self.servers[self.nodes_init:self.nodes_in + failover_factor + 1], [],
                               use_hostnames=True)
        rest = RestConnection(self.master)
        nodes_all = rest.node_statuses()
        nodes = []
        for failover_node in failover_nodes:
            nodes.extend([node for node in nodes_all
                if node.ip == failover_node.hostname and str(node.port) == failover_node.port])
        self.cluster.failover(self.servers, failover_nodes, use_hostnames=True)
        self.verify_referenced_by_names(self.servers[:self.nodes_in + failover_factor + 1], hostnames)
        for node in nodes:
            rest.add_back_node(node.id)
        self.cluster.rebalance(self.servers[:self.nodes_in + failover_factor + 1], [], [], use_hostnames=True)
        self.verify_referenced_by_names(self.servers[:self.nodes_in + failover_factor + 1], hostnames)
