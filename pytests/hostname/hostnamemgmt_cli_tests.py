import json
from couchbase_helper.document import View
from .hostnamemgmt_base import HostnameBaseTests
from membase.api.rest_client import RestConnection, RestHelper
from remote.remote_util import RemoteMachineShellConnection
import time

class HostnameMgmtTests(HostnameBaseTests):

    def setUp(self):
        super(HostnameMgmtTests, self).setUp()

    def tearDown(self):
        super(HostnameMgmtTests, self).tearDown()

    def test_hostname_mgmt_server_list(self):
        hostnames = self.rename_nodes(self.servers[:self.nodes_init])
        self._set_hostames_to_servers_objs(hostnames)
        self.verify_referenced_by_names(self.servers[:self.nodes_init], hostnames)
        remote = RemoteMachineShellConnection(self.master)
        cli_command = "server-list"
        output, error = remote.execute_couchbase_cli(cli_command=cli_command,
                                                     cluster_host=self.master.hostname,
                                                     user="Administrator", password="password")
        time.sleep(60)     # give time for warmup to complete
        self.assertEqual(''.join(output), "ns_1@{0} {0}:8091 healthy active".format(self.master.hostname))

    def test_hostname_mgmt_buckets_list(self):
        hostnames = self.rename_nodes(self.servers[:self.nodes_init])
        self._set_hostames_to_servers_objs(hostnames)
        self.verify_referenced_by_names(self.servers[:self.nodes_init], hostnames)
        remote_client = RemoteMachineShellConnection(self.master)
        cli_command = "bucket-list"
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command,
                                                            cluster_host=self.master.hostname,
                                                            user="Administrator", password="password")
        self.assertTrue(''.join(output).find(''.join([b.name for b in self.buckets])) != -1)
        remote_client.disconnect()

    def test_hostname_mgmt_create_bucket(self):
        hostnames = self.rename_nodes(self.servers[:self.nodes_init])
        self._set_hostames_to_servers_objs(hostnames)
        self.verify_referenced_by_names(self.servers[:self.nodes_init], hostnames)
        remote_client = RemoteMachineShellConnection(self.master)
        cli_command = "bucket-create"
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command,
                                                            cluster_host=self.master.hostname,
                                                            user="Administrator", password="password",
                                                            options=' --bucket=test --bucket-type=couchbase --bucket-port=11210 --bucket-ramsize=100 --bucket-replica=1')
        self.assertTrue(''.join(output).find(''.join([b.name for b in self.buckets])) != -1)
        remote_client.disconnect()

    def test_hostname_mgmt_remove_bucket(self):
        hostnames = self.rename_nodes(self.servers[:self.nodes_init])
        self._set_hostames_to_servers_objs(hostnames)
        self.verify_referenced_by_names(self.servers[:self.nodes_init], hostnames)
        remote_client = RemoteMachineShellConnection(self.master)
        cli_command = "bucket-delete"
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command,
                                                            cluster_host=self.master.hostname,
                                                            user="Administrator", password="password",
                                                            options='--bucket=default')
        self.assertEqual(['SUCCESS: bucket-delete'], [i for i in output if i != ''])
        remote_client.disconnect()