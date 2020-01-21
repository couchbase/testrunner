import copy
from .hostnamemgmt_base import HostnameBaseTests
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from membase.api.exception import ServerAlreadyJoinedException

class HostnameMgmtMultiTests(HostnameBaseTests):

    def setUp(self):
        super(HostnameMgmtMultiTests, self).setUp()
        self.name_prefix = self.input.param('name_prefix', 'hstmgmt_')
        self.domain = self.input.param('domain', 'hq.couchbase.com')
        self.use_names = self.input.param('use_names', 1)
        self.old_files = {}
        try:
            self.__add_hosts()
        except Exception as ex:
            self.log.error(str(ex))
            self.tearDown()
            raise ex

    def tearDown(self):
        super(HostnameMgmtMultiTests, self).tearDown()
        self.__revert_hosts()

    def test_add_cluster_twice(self):
        if len(self.servers) < 2:
            self.fail("test require more than 1 node")

        print('\n\nrenaming nodes')
        hostnames = self.rename_nodes(self.servers[:self.nodes_in + self.nodes_init])
        self._set_hostames_to_servers_objs(hostnames)
        print('\n\nrebalancing...')
        self.cluster.rebalance(self.servers[:self.nodes_init],
                               self.servers[self.nodes_init:self.nodes_in + self.nodes_init], [], use_hostnames=True)
        self.verify_referenced_by_names(self.servers[:self.nodes_init], hostnames)
        remove_node = self.servers[:self.nodes_in + self.nodes_init][-1]

        print('\n\nremoving', remove_node, ' and rebalancing...;')
        self.cluster.rebalance(self.servers[:self.nodes_in + self.nodes_init],
                               [], [remove_node], use_hostnames=True)

        print('\n\nrenaming')
        new_name = self.rename_nodes([remove_node],
                                     {remove_node : self.name_prefix +\
                                       str(remove_node.ip.split('.')[-1]) + '_0' + '.' + self.domain})

        print('\n\nsetting hostnames to server objs')
        self._set_hostames_to_servers_objs(new_name, server=remove_node)
        print('\n\nrebalance again')

        self.cluster.rebalance(self.servers[:self.nodes_in + self.nodes_init - 1],
                               [remove_node], [], use_hostnames=True)
        for srv in self.servers:
            if srv.hostname in iter(new_name.values()):
                srv.hostname = ''

        print('\n\nupdating with a new name', new_name)
        hostnames.update(new_name)
        self.verify_referenced_by_names(self.servers[:self.nodes_in + self.nodes_init-1], hostnames)

    def test_add_same_node_to_cluster(self):
        self.assertTrue(len(self.servers) > 1, "test require more than 1 node")
        self.assertTrue(self.use_names > 1, "test require more than 1 use_names")
        hostnames = self.rename_nodes(self.servers[:self.nodes_in + self.nodes_init])
        self._set_hostames_to_servers_objs(hostnames)
        self.cluster.rebalance(self.servers[:self.nodes_init],
                               self.servers[self.nodes_init:self.nodes_in + self.nodes_init], [], use_hostnames=True)
        self.verify_referenced_by_names(self.servers[:self.nodes_init], hostnames)
        add_node = self.servers[:self.nodes_in + self.nodes_init][-1]
        new_name = self.name_prefix + str(add_node.ip.split('.')[-1]) + '_1' + '.' + self.domain
        master_rest = RestConnection(self.master)
        try:
            master_rest.add_node(add_node.rest_username, add_node.rest_password, new_name)
        except ServerAlreadyJoinedException:
            self.log.info('Expected exception was raised.')
        else:
            self.fail('Expected exception wasn\'t raised')

    def __add_hosts(self):
        names = {}
        for server in self.servers:
            names[server] = ' '.join([self.name_prefix + str(server.ip.split('.')[-1]) +\
                                       '_' + str(i) + '.' + self.domain
                                      for i in range(self.use_names)])
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            try:
                etc_file_dir = 'etc' if shell.extract_remote_info().type == 'Linux' else 'C:\WINDOWS\system32\drivers\etc'
                old_lines = shell.read_remote_file(etc_file_dir, 'hosts')
                self.old_files[server] = copy.deepcopy(old_lines)
                new_lines = ['%s %s\n' % (srv.ip, names[srv])
                             for srv in set(self.servers) - {server}]
                new_lines.append('127.0.0.1 %s' % names[server])
                if old_lines is not None:
                    old_lines.extend(new_lines)
                    shell.write_remote_file(etc_file_dir, 'hosts', old_lines)
            finally:
                shell.disconnect()

    def __revert_hosts(self):
        if hasattr(self, 'old_files') and self.old_files:
            for server in self.servers:
                shell = RemoteMachineShellConnection(server)
                etc_file_dir = 'etc' if shell.extract_remote_info().type == 'Linux' else 'C:\WINDOWS\system32\drivers\etc'
                try:
                    if server in self.old_files and self.old_files[server] is not None:
                       shell.write_remote_file(etc_file_dir, 'hosts', self.old_files[server])
                finally:
                    shell.disconnect()
