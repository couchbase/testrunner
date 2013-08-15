import logger
import unittest
import copy
import datetime
import time
import paramiko
import os

from couchbase.cluster import Cluster
from TestInput import TestInputSingleton
from membase.api.rest_client import RestConnection, Bucket
from couchbase.documentgenerator import BlobGenerator, DocumentGenerator
from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper

class NonRootTests(unittest.TestCase):
    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.input = TestInputSingleton.input
        self._os = self.input.param("os","null");     #Allow centos, ubuntu, windows
        self.num_items = self.input.param("items", 100000)
        self.servers = self.input.servers
        self.master = self.servers[0]
        self.clean_up()
        self.log.info("Begin setting up the couchbase on all server nodes...")
        self.non_root_install()
        self.log.info("Wait for 30 seconds after couchbase install over all servers...")
        time.sleep(30)
        self.log.info("==============  NonRootTests setUp was started ==============")

    def tearDown(self):
        """
            Delete the non-root installation
        """
        self.log.info("==============  NonRootTests tearDown was started ==============")
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            if self._os == "centos" or self._os == "ubuntu":
                command = "cd /home/{0}/opt/couchbase && ./bin/couchbase-server -k".format(server.ssh_username)
                o, e = shell.execute_non_sudo_command(command)
                shell.log_command_output(o, e)
                o, e = shell.execute_non_sudo_command("rm -rf etc/ opt/ couchbase-server-enterprise_x86_64_2.2.0-772-rel.*")
                shell.log_command_output(o, e)
            else:
                #Handling Windows?
                pass
            shell.disconnect()

    def clean_up(self):
        self.log.info("Cleaning up nodes, stopping previous couchbase instances if any ..")
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            if self._os == "centos" or self._os == "ubuntu":
                command = "cd /home/{0}/opt/couchbase && ./bin/couchbase-server -k".format(server.ssh_username)
                o, e = shell.execute_non_sudo_command(command)
                shell.log_command_output(o, e)
                o, e = shell.execute_non_sudo_command("rm -rf etc/ opt/ couchbase-server-enterprise_x86_64_2.2.0-772-rel.*")
                shell.log_command_output(o, e)
            else:
                #Handling Windows?
                pass
            shell.disconnect()

    """
        Method that sets up couchbase-server on the server list, without root privileges.
    """
    def non_root_install(self):
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            info = shell.extract_remote_info()
            ssh_client.connect(hostname=server.ip,key_filename=server.ssh_key)
            sftp_client = ssh_client.open_sftp()
            if self._os == "centos":
                command0 = "rm -rf opt/ etc/ && rm -rf couchbase-server-enterprise_x86_64_2.2.0-772-rel.rpm"
                command1 = "wget http://builds.hq.northscale.net/latestbuilds/couchbase-server-enterprise_x86_64_2.2.0-772-rel.rpm"
                command2 = "rpm2cpio couchbase-server-enterprise_x86_64_2.2.0-772-rel.rpm | cpio --extract --make-directories --no-absolute-filenames"
                command3 = "cd /home/{0}/opt/couchbase && ./bin/install/reloc.sh `pwd`".format(server.ssh_username)
                command4 = "cd /home/{0}/opt/couchbase && ./bin/couchbase-server -- -noinput -detached".format(server.ssh_username)
                command5 = "cd /home/{0}/opt/couchbase && ./bin/couchbase-server -k".format(server.ssh_username)
                o, e = shell.execute_non_sudo_command(command0)
                shell.log_command_output(o, e)
                o, e = shell.execute_non_sudo_command(command1)
                shell.log_command_output(o, e)
                o, e = shell.execute_non_sudo_command(command2)
                shell.log_command_output(o, e)
                o, e = shell.execute_non_sudo_command(command3)
                shell.log_command_output(o, e)
                self.log.info("Starting couchbase server <non-root, non-sudo> ..")
                o, e = shell.execute_non_sudo_command(command4)
                shell.log_command_output(o, e)
            elif self._os == "ubuntu":
                command0 = "rm -rf opt/ etc/ && rm -rf couchbase-server-enterprise_x86_64_2.2.0-772-rel.deb"
                command1 = "wget http://builds.hq.northscale.net/latestbuilds/couchbase-server-enterprise_x86_64_2.2.0-772-rel.deb"
                command2 = "dpkg-deb -x couchbase-server-enterprise_x86_64_2.2.0-772-rel.deb /home/{0}".format(server.ssh_username)
                command3 = "cd /home/{0}/opt/couchbase && ./bin/install/reloc.sh `pwd`".format(server.ssh_username)
                command4 = "cd /home/{0}/opt/couchbase && ./bin/couchbase-server -- -noinput -detached".format(server.ssh_username)
                command5 = "cd /home/{0}/opt/couchbase && ./bin/couchbase-server -k".format(server.ssh_username)
                o, e = shell.execute_non_sudo_command(command0)
                shell.log_command_output(o, e)
                o, e = shell.execute_non_sudo_command(command1)
                shell.log_command_output(o, e)
                o, e = shell.execute_non_sudo_command(command2)
                shell.log_command_output(o, e)
                o, e = shell.execute_non_sudo_command(command3)
                shell.log_command_output(o, e)
                self.log.info("Starting couchbase server <non-root, non-sudo> ..")
                o, e = shell.execute_non_sudo_command(command4)
                shell.log_command_output(o, e)
                self.fail("TODO: Add instructions for ubuntu")
            elif self._os == "windows":
                self.fail("TODO: Add instructions for windows")
            else:
                self.fail("Enter valid os name, options: centos, ubuntu, windows; entered name: {0} - invalid.".format(self._os))

            ssh_client.close()

    """
        Test loads a certain number of items on a standard bucket created
        using couchbase-cli and later verifies if the number matches what's expected.
    """
    def test_create_bucket_test_load(self):
        shell = RemoteMachineShellConnection(self.master)
        if self._os == "centos" or self._os == "ubuntu":
            _1 = "cd /home/{0}/opt/couchbase &&".format(self.master.ssh_username)
            _2 = " ./bin/couchbase-cli cluster-init -c localhost:8091"
            _3 = " --cluster-init-username={0} --cluster-init-password={1}".format(self.master.rest_username, self.master.rest_password)
            _4 = " --cluster-init-port=8091 --cluster-init-ramsize=1000"
            command_to_init = _1 + _2 + _3 + _4
            o, e = shell.execute_non_sudo_command(command_to_init)
            shell.log_command_output(o, e)
            time.sleep(10)
            for i in range(1, len(self.servers)):
                _1 = "cd /home/{0}/opt/couchbase &&".format(self.master.ssh_username)
                _2 = " ./bin/couchbase-cli server-add -c {1}:8091".format(self.master.ip)
                _3 = " --server-add={2}:8091".format(self.servers[i].ip)
                _4 = " --server-add-username={3}".format(self.servers[i].rest_username)
                _5 = " --server-add-password={4}".format(self.servers[i].rest_password)
                _6 = " -u {0} -p {1}".format(self.servers[i].rest_username, self.servers[i].rest_password)
                command_to_rebalance = _1 + _2 + _3 + _4 + _5 + _6
                o, e = shell.execute_non_sudo_command(command_to_rebalance)
                shell.log_command_output(o, e)
                time.sleep(10)
            if len(self.servers) < 2:
                rep_count = 0
            else:
                rep_count = 1
            self.log.info("Cluster set up, now creating a bucket ..")
            _1 = "cd /home/{0}/opt/couchbase &&".format(self.master.ssh_username)
            _2 = " ./bin/couchbase-cli bucket-create -c localhost:8091"
            _3 = " --bucket=testbucket --bucket-type=couchbase --bucket-port=11211"
            _4 = " --bucket-ramsize=500 --bucket-replica={0} --wait".format(rep_count)
            _5 = " -u {0} -p {1}".format(self.master.rest_username, self.master.rest_password)
            command_to_create_bucket = _1 + _2 + _3 + _4 + _5
            o, e = shell.execute_non_sudo_command(command_to_create_bucket)
            shell.log_command_output(o, e)
            time.sleep(30)
            self.log.info("Load {0} through cbworkloadgen ..".format(self.num_items))
            _1 = "cd /home/{0}/opt/couchbase &&".format(self.master.ssh_username)
            _2 = " ./bin/cbworkloadgen -n localhost:8091"
            _3 = " -r .8 -i {0} -s 256 -b testbucket -t 1".format(self.num_items)
            _4 = " -u {0} -p {1}".format(self.master.rest_username, self.master.rest_password)
            command_to_load = _1 + _2 + _3 + _4
            o, e = shell.execute_non_sudo_command(command_to_load)
            shell.log_command_output(o, e)
            time.sleep(20)
            rest = RestConnection(self.master)
            item_count = rest.fetch_bucket_stats(bucket="testbucket")["op"]["samples"]["curr_items"][-1]
            if (item_count == self.num_items):
                self.log.info("Item count matched, {0}={1}".format(item_count, self.num_items))
            else:
                self.fail("Item count: Not what's expected, {0}!={1}".format(item_count, self.num_items))
            self.log.info("Deleting testbucket ..");
            _1 = "cd /home/{0}/opt/couchbase &&".format(self.master.ssh_username)
            _2 = " ./bin/couchbase-cli bucket-delete -c localhost:8091"
            _3 = " --bucket=testbucket"
            _4 = " -u {0} -p {1}".format(self.master.rest_username, self.master.rest_password)
            command_to_delete_bucket = _1 + _2 + _3 + _4
            o, e = shell.execute_non_sudo_command(command_to_load)
            shell.log_command_output(o, e)
            time.sleep(10)
        elif self._os == "windows":
            # Windows support
            pass

