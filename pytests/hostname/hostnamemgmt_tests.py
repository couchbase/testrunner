from couchbase.document import View
from hostnamemgmt_base import HostnameBaseTests
from membase.api.rest_client import RestConnection
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
        for i in xrange(2):
            try:
                hostnames = {}
                if name:
                    for server in self.servers[:self.nodes_init]:
                        hostnames[server] = name
                hostnames = self.rename_nodes(self.servers[:self.nodes_init], names=hostnames)
                self.verify_referenced_by_names(self.servers[:self.nodes_init], hostnames)
            except Exception, ex:
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
        try:
            hostnames = self.rename_nodes(self.servers[:self.nodes_in + self.nodes_init])
        except Exception, ex:
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
        self.cluster.rebalance(self.servers[:self.nodes_init],
                                                 self.servers[self.nodes_in + self.nodes_init], [])
        self.verify_referenced_by_names(self.servers[:self.nodes_init], hostnames)
        self._set_hostames_to_servers_objs(hostnames)
        warmup_node = self.servers[:self.nodes_in + self.nodes_init][-1]
        shell = RemoteMachineShellConnection(warmup_node)
        shell.stop_couchbase()
        self.sleep(10)
        shell.start_couchbase()
        shell.disconnect()
        self.verify_referenced_by_names(self.servers[:self.nodes_in + self.nodes_init], hostnames)
