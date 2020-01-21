from membase.api.exception import ServerSelfJoinException, MembaseHttpExceptionTypes, ServerAlreadyJoinedException
from membase.api.rest_client import RestConnection, RestHelper
from basetestcase import BaseTestCase
from lib.remote.remote_util import RemoteMachineShellConnection, RemoteMachineHelper, RemoteUtilHelper
from lib.memcached.helper.data_helper import MemcachedClientHelper
from scripts.install import InstallerJob
from membase.api.rest_client import RestConnection
from couchbase_helper.documentgenerator import BlobGenerator
from .newupgradebasetest import NewUpgradeBaseTest
from builds.build_query import BuildQuery
import re


class HostnameTests(BaseTestCase):

    def setUp(self):
        super(HostnameTests, self).setUp()
        self.builds = self.input.param("builds", "2.2.0")
        self.product = self.input.param('product', 'couchbase-server')
        self.is_negative_test = self.input.param('is_negative_test', False)
        self.error = self.input.param('error', '')
        self.name = self.input.param('name', '')

    def tearDown(self):
        super(HostnameTests, self).tearDown()
        for server in self.servers:
            self.assertTrue(RestHelper(RestConnection(server)).is_ns_server_running(timeout_in_seconds=480),
                            msg="ns server is not running even after waiting for 6 minutes")
        self.log.info("sleep for 10 seconds to give enough time for other nodes to restart")
        self.sleep(10)

    def install_builds(self, builds, servers=None):
        params = {}
        params['product'] = 'couchbase-server'
        builds_list = builds.split(";")
        st = 0
        end = 1
        if None in servers:
            servers_to_install = self.servers
        else:
            servers_to_install = servers
        for i in builds_list:
            params['version'] = i
            InstallerJob().sequential_install(servers_to_install[st:end], params)
            st = st + 1
            end = end + 1
        self.sleep(20)
        super(HostnameTests, self).setUp()

    def rest_api_addNode(self):
        hostnames = self.convert_to_hostname(self, self.servers[0:2])
        master_rest = RestConnection(self.master)
        master_rest.add_node(self.servers[1].rest_username, self.servers[1].rest_password, hostnames[1], self.servers[1].port)
        #Now check whether the node which we added is still referred via hostname or not.
        obj = RestConnection(self.servers[1])
        var = obj.get_nodes_self().hostname
        flag = True if self.servers[1].ip in var else False
        self.assertEqual(flag, False, msg="Fail - Name of node {0} got converted to IP. Failing the test!!!".format(self.servers[1].ip))
        self.log.info("Test Passed!!")
        self.sleep(10)

#Cases 39 and 40 from hostname management test plan combined.
    def rest_api_renameNode(self):
        try:
            self.shell = RemoteMachineShellConnection(self.master)
            #com_inst_build = "cat /opt/couchbase/VERSION.txt"
            #out = self.shell.execute_command(com_inst_build.format(com_inst_build))
            self.install_builds(self.builds, self.servers[0:1])
            if self.is_negative_test:
                master_rest = RestConnection(self.master)
                self.log.info("Renaming node {0} to {1}".format(self.master, self.name))
                var = master_rest.rename_node(username=self.master.rest_username, password=self.master.rest_password,
                    port=self.master.port, hostname=self.name, is_negative_test=True)
                out = var.pop()
                self.assertEqual(out, self.error, msg="Fail to find correct error. The error should be {0}, but we got : {1}".format(self.error, out))
                self.log.info("Got correct error - {0}....Passing the test".format(out))
            else:
                self.log.info("Node {0} is referred via IP. Changing the name of the node".format(self.servers[0:1]))
                hostname = []
                info = self.shell.extract_remote_info()
                domain = ''.join(info.domain[0])
                hostname.append(info.hostname[0] + "." + domain)
                self.convert_to_hostname(self, self.servers[0:1])
                self.log.info("Calling get_node_self() to check the status of node {0}".format(self.servers[0:1]))
                obj = RestConnection(self.master)
                var = obj.get_nodes_self().hostname
                flag = True if self.master.ip in var else False
                self.assertEqual(flag, False, msg="Fail - Node {0} is still referred via IP. Should\
                                     have been referred via hostname. Failing the test!".format(self.master.ip))
                self.log.info("Name of node {0} got converted to hostname. Proceeding......!".format(self.master.ip))
                self.sleep(10)
                self.log.info("Now changing name of node {0} from hostname to IP".format(self.master.ip))
                var = obj.rename_node(username='Administrator', password='password', port='', hostname=self.master.ip)
                self.log.info("Calling get_node_self() to check the status of the node {0}".format(self.master.ip))
                var = obj.get_nodes_self().hostname
                flag = True if self.master.ip in var else False
                self.assertEqual(flag, True, msg="Fail - Node {0} is still referred via hostname. Should have been referred via IP. Failing the test!".format(self.master.ip))
                self.log.info("Node {0} referred via IP. Pass !".format(self.master.ip))
        finally:
            self.shell.disconnect()


    @staticmethod
    def convert_to_hostname(self, servers_with_hostnames, username='Administrator', password='password'):
        try:
            hostname = []
            for server in servers_with_hostnames:
                shell = RemoteMachineShellConnection(server)
                info = shell.extract_remote_info()
                domain = ''.join(info.domain[0])
                if not domain:
                    output = shell.execute_command_raw('nslookup %s' % info.hostname[0])
                    print(output)
                    self.fail("Domain is not defined, couchbase cannot be configured correctly. NOT A BUG. CONFIGURATION ISSUE")
                hostname.append(info.hostname[0] + "." + domain)
                master_rest = RestConnection(server)
                current_hostname = master_rest.get_nodes_self().hostname
                self.log.info("get_node_self function returned : {0}".format(current_hostname))
                if server.ip in current_hostname:
                    self.log.info("Node {0} is referred via IP. Need to be referred with hostname. Changing the name of the node!!".format(server.ip))
                    version = RestConnection(server).get_nodes_self().version
                    if version.startswith("1.8.1") or version.startswith("2.0.0") or version.startswith("2.0.1"):
                        RemoteUtilHelper.use_hostname_for_server_settings(server)
                        master_rest.init_cluster()
                    else:
                        master_rest.init_cluster()
                        master_rest.rename_node(username=username, password=password, port='', hostname=hostname[-1])
                else:
                    self.log.info("Node {0} already referred via hostname. No need to convert the name".format(server.ip))
        finally:
            shell.disconnect()
        return hostname
