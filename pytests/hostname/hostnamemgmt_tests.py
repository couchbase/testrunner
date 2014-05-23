from hostnamemgmt_base import HostnameBaseTests
from membase.api.rest_client import RestConnection

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
