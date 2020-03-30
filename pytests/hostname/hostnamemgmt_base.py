from basetestcase import BaseTestCase
from lib.remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection


class HostnameBaseTests(BaseTestCase):

    def setUp(self):
        super(HostnameBaseTests, self).setUp()
        self.error = self.input.param('error', '')

    def tearDown(self):
        self._deinitialize_servers(self.servers)
        super(HostnameBaseTests, self).tearDown()

    def _deinitialize_servers(self, servers):
        for server in servers:
            rest = RestConnection(server)
            rest.force_eject_node()
        self.sleep(3, "Sleep to wait deinitialize")

    def rename_nodes(self, servers, names={}):
        print('\n\nrename names servers:', servers, ' names', names)
        hostnames={}
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            try:
                if not names:
                    hostname = shell.get_full_hostname()
                else:
                    hostname = names[server]
                rest = RestConnection(server)
                renamed, content = rest.rename_node(hostname, username=server.rest_username, password=server.rest_password)
                self.assertTrue(renamed, "Server %s is not renamed!Hostname %s. Error %s" %(
                                        server, hostname, content))
                hostnames[server] = hostname
            finally:
                shell.disconnect()
        return hostnames

    def _set_hostames_to_servers_objs(self, hostnames, server=None):
        for i in range(len(self.servers)):
            if self.servers[i] in hostnames:
                if server and server.ip != self.servers[i].ip:
                    continue
                self.servers[i].hostname = hostnames[self.servers[i]]

    def verify_referenced_by_names(self, servers, hostnames):
        for server in servers:
            rest = RestConnection(server)
            current_hostname = rest.get_nodes_self().hostname
            self.assertTrue(current_hostname.find(hostnames[server]) != -1,
                            "Server %s: Expected hostname %s, actual is %s" %(
                            server, hostnames[server], current_hostname))

    def verify_referenced_by_ip(self, servers):
        for server in servers:
            rest = RestConnection(server)
            current_hostname = rest.get_nodes_self().hostname
            self.assertTrue(current_hostname.startswith(server.ip),
                            "Server %s: Expected hostname %s, actual is %s" %(
                            server, server.ip, current_hostname))