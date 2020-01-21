import logger
import unittest
import copy
import datetime
import time
import paramiko
import os

from couchbase_helper.cluster import Cluster
from TestInput import TestInputSingleton
from membase.api.rest_client import RestConnection, Bucket
from couchbase_helper.documentgenerator import BlobGenerator, DocumentGenerator
from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper

class NonRootTests(unittest.TestCase):
    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.input = TestInputSingleton.input
        self._os = self.input.param("os", "null");     #To allow centos, ubuntu, windows
        self.build = self.input.param("build", "couchbase-server-enterprise_2.2.0-817-rel_x86_64.rpm")
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
                command = "rm -rf etc/ opt/ usr/ {0}.*".format(self.build[:-4])
                o, e = shell.execute_non_sudo_command(command)
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
                command = "rm -rf etc/ opt/ usr/ {0}.*".format(self.build[:-4])
                o, e = shell.execute_non_sudo_command(command)
                shell.log_command_output(o, e)
                command = "rm -rf backup/"
                shell.log_command_output(o, e)
            else:
                #Handling Windows?
                pass
            shell.disconnect()

    """
        Method that sets up couchbase-server on the server list, without root privileges.
    """
    def non_root_install(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            info = shell.extract_remote_info()
            if self._os == "centos":
                command0 = "rm -rf opt/ etc/ && rm -rf {0}".format(self.build)
                command1 = "wget http://builds.hq.northscale.net/latestbuilds/{0}".format(self.build)
                command2 = "rpm2cpio {0} | cpio --extract --make-directories --no-absolute-filenames".format(self.build)
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
                command0 = "rm -rf opt/ etc/ && rm -rf {0}".format(self.build)
                command1 = "wget http://builds.hq.northscale.net/latestbuilds/{0}".format(self.build)
                command2 = "dpkg-deb -x {0} /home/{1}".format(self.build, server.ssh_username)
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
            elif self._os == "windows":
                self.fail("TODO: Add instructions for windows")
            else:
                self.fail("Enter valid os name, options: centos, ubuntu, windows; entered name: {0} - invalid.".format(self._os))


    """
        Method that initializes cluster, rebalances in nodes, and creates a standard bucket
    """
    def init_rebalance_cluster_create_testbucket(self, master, servers):
        shell = RemoteMachineShellConnection(master)
        if self._os == "centos" or self._os == "ubuntu":
            _1 = "cd /home/{0}/opt/couchbase &&".format(master.ssh_username)
            _2 = " ./bin/couchbase-cli cluster-init -c localhost:8091"
            _3 = " --cluster-username={0} --cluster-password={1}".format(master.rest_username, master.rest_password)
            _4 = " --cluster-port=8091 --cluster-ramsize=1000"
            command_to_init = _1 + _2 + _3 + _4
            o, e = shell.execute_non_sudo_command(command_to_init)
            shell.log_command_output(o, e)
            time.sleep(10)
            for i in range(1, len(servers)):
                _1 = "cd /home/{0}/opt/couchbase &&".format(master.ssh_username)
                _2 = " ./bin/couchbase-cli rebalance -c {0}:8091".format(master.ip)
                _3 = " --server-add={0}:8091".format(servers[i].ip)
                _4 = " --server-add-username={0}".format(servers[i].rest_username)
                _5 = " --server-add-password={0}".format(servers[i].rest_password)
                _6 = " -u {0} -p {1}".format(servers[i].rest_username, servers[i].rest_password)
                command_to_rebalance = _1 + _2 + _3 + _4 + _5 + _6
                o, e = shell.execute_non_sudo_command(command_to_rebalance)
                shell.log_command_output(o, e)
                time.sleep(10)
            if len(servers) < 2:
                rep_count = 0
            else:
                rep_count = 1
            self.log.info("Cluster set up, now creating a bucket ..")
            _1 = "cd /home/{0}/opt/couchbase &&".format(master.ssh_username)
            _2 = " ./bin/couchbase-cli bucket-create -c localhost:8091"
            _3 = " --bucket=testbucket --bucket-type=couchbase --bucket-port=11211"
            _4 = " --bucket-ramsize=500 --bucket-replica={0} --wait".format(rep_count)
            _5 = " -u {0} -p {1}".format(master.rest_username, master.rest_password)
            command_to_create_bucket = _1 + _2 + _3 + _4 + _5
            o, e = shell.execute_non_sudo_command(command_to_create_bucket)
            shell.log_command_output(o, e)
            time.sleep(30)
        elif self._os == "windows":
            # TODO: Windows support
            pass
        shell.disconnect()

#BEGIN TEST 1
    """
        Test loads a certain number of items on a standard bucket created
        using couchbase-cli and later verifies if the number matches what's expected.
    """
    def test_create_bucket_test_load(self):
        shell = RemoteMachineShellConnection(self.master)
        self.init_rebalance_cluster_create_testbucket(self.master, self.servers)
        if self._os == "centos" or self._os == "ubuntu":
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
            o, e = shell.execute_non_sudo_command(command_to_delete_bucket)
            shell.log_command_output(o, e)
            time.sleep(10)
        elif self._os == "windows":
            # TODO: Windows support
            self.log.info("Yet to add support for windows!")
            pass
        shell.disconnect()
#END TEST 1

#BEGIN TEST 2
    """
        Test that loads a certain number of items, backs up, deletes bucket,
        recreates bucket, restores, and verifies if count matched.
    """
    def test_bucket_backup_restore(self):
        shell = RemoteMachineShellConnection(self.master)
        self.init_rebalance_cluster_create_testbucket(self.master, self.servers)
        if self._os == "centos" or self._os == "ubuntu":
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
            ini_item_count = rest.fetch_bucket_stats(bucket="testbucket")["op"]["samples"]["curr_items"][-1]
            self.log.info("Backing up bucket 'testbucket' ..")
            _1 = "cd /home/{0}/opt/couchbase &&".format(self.master.ssh_username)
            _2 = " ./bin/cbbackup http://localhost:8091"
            _3 = " /home/{0}/backup".format(self.master.ssh_username)
            _4 = " -u {0} -p {1}".format(self.master.rest_username, self.master.rest_password)
            command_to_backup = _1 + _2 + _3 + _4
            o, e = shell.execute_non_sudo_command(command_to_backup)
            shell.log_command_output(o, e)
            time.sleep(10)
            self.log.info("Deleting bucket ..")
            _1 = "cd /home/{0}/opt/couchbase &&".format(self.master.ssh_username)
            _2 = " ./bin/couchbase-cli bucket-delete -c localhost:8091"
            _3 = " --bucket=testbucket"
            _4 = " -u {0} -p {1}".format(self.master.rest_username, self.master.rest_password)
            command_to_delete_bucket = _1 + _2 + _3 + _4
            o, e = shell.execute_non_sudo_command(command_to_delete_bucket)
            shell.log_command_output(o, e)
            time.sleep(20)
            if len(self.servers) < 2:
                rep_count = 0
            else:
                rep_count = 1
            self.log.info("Recreating bucket ..")
            _1 = "cd /home/{0}/opt/couchbase &&".format(self.master.ssh_username)
            _2 = " ./bin/couchbase-cli bucket-create -c localhost:8091"
            _3 = " --bucket=testbucket --bucket-type=couchbase --bucket-port=11211"
            _4 = " --bucket-ramsize=500 --bucket-replica={0} --wait".format(rep_count)
            _5 = " -u {0} -p {1}".format(self.master.rest_username, self.master.rest_password)
            command_to_create_bucket = _1 + _2 + _3 + _4 + _5
            o, e = shell.execute_non_sudo_command(command_to_create_bucket)
            shell.log_command_output(o, e)
            time.sleep(20)
            self.log.info("Restoring bucket 'testbucket' ..")
            _1 = "cd /home/{0}/opt/couchbase &&".format(self.master.ssh_username)
            _2 = " ./bin/cbrestore /home/{0}/backup http://localhost:8091".format(self.master.ssh_username)
            _3 = " -b testbucket -B testbucket"
            _4 = " -u {0} -p {1}".format(self.master.rest_username, self.master.rest_password)
            command_to_restore = _1 + _2 + _3 + _4
            o, e = shell.execute_non_sudo_command(command_to_restore)
            shell.log_command_output(o, e)
            time.sleep(10)
            rest = RestConnection(self.master)
            fin_item_count = rest.fetch_bucket_stats(bucket="testbucket")["op"]["samples"]["curr_items"][-1]
            self.log.info("Removing backed-up folder ..")
            command_to_remove_folder = "rm -rf /home/{0}/backup".format(self.master.ssh_username)
            o, e = shell.execute_non_sudo_command(command_to_remove_folder)
            shell.log_command_output(o, e)
            if (fin_item_count == ini_item_count):
                self.log.info("Item count before and after deleting with backup/restore matched, {0}={1}".format(
                    fin_item_count, ini_item_count))
            else:
                self.fail("Item count didnt match - backup/restore, {0}!={1}".format(fin_item_count, ini_item_count))
            self.log.info("Deleting testbucket ..");
            _1 = "cd /home/{0}/opt/couchbase &&".format(self.master.ssh_username)
            _2 = " ./bin/couchbase-cli bucket-delete -c localhost:8091"
            _3 = " --bucket=testbucket"
            _4 = " -u {0} -p {1}".format(self.master.rest_username, self.master.rest_password)
            command_to_delete_bucket = _1 + _2 + _3 + _4
            o, e = shell.execute_non_sudo_command(command_to_delete_bucket)
            shell.log_command_output(o, e)
            time.sleep(10)
        elif self._os == "windows":
            # TODO: Windows support
            self.log.info("Yet to add support for windows!")
            pass
        shell.disconnect()
#END TEST 2

    """
        Method to setup XDCR, and start replication(s)
    """
    def setup_xdcr_start_replication(self, src, dest, rep_type, bidirectional):
        shell1 = RemoteMachineShellConnection(src)
        shell2 = RemoteMachineShellConnection(dest)
        if self._os == "centos" or self._os == "ubuntu":
            self.log.info("Setting up XDCR from source cluster to destination cluster ..")
            _1 = "cd /home/{0}/opt/couchbase &&".format(src.ssh_username)
            _2 = " ./bin/couchbase-cli xdcr-setup -c localhost:8091"
            _3 = " --create --xdcr-cluster-name=_dest --xdcr-hostname={0}:8091".format(dest.ip)
            _4 = " --xdcr-username={0} --xdcr-password={1}".format(dest.rest_username, dest.rest_password)
            _5 = " -u {0} -p {1}".format(src.rest_username, src.rest_password)
            command_to_setup_xdcr = _1 + _2 + _3 + _4 + _5
            o, e = shell1.execute_non_sudo_command(command_to_setup_xdcr)
            shell1.log_command_output(o, e)
            if bidirectional:
                self.log.info("Setting up XDCR from destination cluster to source cluster ..")
                _1 = "cd /home/{0}/opt/couchbase &&".format(dest.ssh_username)
                _2 = " ./bin/couchbase-cli xdcr-setup -c localhost:8091"
                _3 = " --create --xdcr-cluster-name=_src --xdcr-hostname={0}:8091".format(src.ip)
                _4 = " --xdcr-username={0} --xdcr-password={1}".format(src.rest_username, src.rest_password)
                _5 = " -u {0} -p {1}".format(dest.rest_username, dest.rest_password)
                command_to_setup_xdcr = _1 + _2 + _3 + _4 + _5
                o, e = shell2.execute_non_sudo_command(command_to_setup_xdcr)
                shell2.log_command_output(o, e)
            time.sleep(10)
            self.log.info("Starting replication from source to destination ..")
            _1 = "cd /home/{0}/opt/couchbase &&".format(src.ssh_username)
            _2 = " ./bin/couchbase-cli xdcr-replicate -c localhost:8091"
            _3 = " --create --xdcr-cluster-name=_dest --xdcr-from-bucket=testbucket"
            _4 = " --xdcr-to-bucket=testbucket --xdcr-replication-mode={0}".format(rep_type)
            _5 = " -u {0} -p {1}".format(src.rest_username, src.rest_password)
            command_to_setup_xdcr = _1 + _2 + _3 + _4 + _5
            o, e = shell1.execute_non_sudo_command(command_to_setup_xdcr)
            shell1.log_command_output(o, e)
            if bidirectional:
                self.log.info("Starting replication from destination to source ..")
                _1 = "cd /home/{0}/opt/couchbase &&".format(dest.ssh_username)
                _2 = " ./bin/couchbase-cli xdcr-replicate -c localhost:8091"
                _3 = " --create --xdcr-cluster-name=_src --xdcr-from-bucket=testbucket"
                _4 = " --xdcr-to-bucket=testbucket --xdcr-replication-mode={0}".format(rep_type)
                _5 = " -u {0} -p {1}".format(dest.rest_username, dest.rest_password)
                command_to_setup_xdcr = _1 + _2 + _3 + _4 + _5
                o, e = shell2.execute_non_sudo_command(command_to_setup_xdcr)
                shell2.log_command_output(o, e)
            time.sleep(10)
        elif self._os == "windows":
            # TODO: WIndows support
            pass
        shell1.disconnect()
        shell2.disconnect()

    """
        Method to wait for replication to catch up
    """
    def wait_for_replication_to_catchup(self, src, dest, timeout, _str_):
        rest1 = RestConnection(src)
        rest2 = RestConnection(dest)
        # 20 minutes by default
        end_time = time.time() + timeout

        _count1 = rest1.fetch_bucket_stats(bucket="testbucket")["op"]["samples"]["curr_items"][-1]
        _count2 = rest2.fetch_bucket_stats(bucket="testbucket")["op"]["samples"]["curr_items"][-1]
        while _count1 != _count2 and (time.time() - end_time) < 0:
            self.log.info("Waiting for replication to catch up ..")
            time.sleep(60)
            _count1 = rest1.fetch_bucket_stats(bucket="testbucket")["op"]["samples"]["curr_items"][-1]
            _count2 = rest2.fetch_bucket_stats(bucket="testbucket")["op"]["samples"]["curr_items"][-1]
        if _count1 != _count2:
            self.fail("not all items replicated in {0} sec for bucket: testbucket. on source cluster:{1}, on dest:{2}".\
                                   format(timeout, _count1, _count2))
        self.log.info("Replication caught up at {0}, for testbucket".format(_str_))

#BEGIN TEST 3
    """
        Test that enagages clusters specified in XDCR,
        verifies whether data went through as expected.
    """
    def test_xdcr(self):
        _rep_type = self.input.param("replication_type", "capi")    # capi or xmem
        _bixdcr = self.input.param("bidirectional", "false")
        _clusters_dic = self.input.clusters
        _src_nodes = copy.copy(_clusters_dic[0])
        _src_master = _src_nodes[0]
        _dest_nodes = copy.copy(_clusters_dic[1])
        _dest_master = _dest_nodes[0]

        # Build source cluster
        self.init_rebalance_cluster_create_testbucket(_src_master, _src_nodes)
        # Build destination cluster
        self.init_rebalance_cluster_create_testbucket(_dest_master, _dest_nodes)

        # Setting up XDCR
        self.setup_xdcr_start_replication(_src_master, _dest_master, _rep_type, _bixdcr)

        shell1 = RemoteMachineShellConnection(_src_master)
        shell2 = RemoteMachineShellConnection(_dest_master)
        src_item_count = 0
        dest_item_count = 0
        if self._os == "centos" or self._os == "ubuntu":
            self.log.info("Load {0} through cbworkloadgen at src..".format(self.num_items))
            _1 = "cd /home/{0}/opt/couchbase &&".format(_src_master.ssh_username)
            _2 = " ./bin/cbworkloadgen -n localhost:8091 --prefix=s_"
            _3 = " -r .8 -i {0} -s 256 -b testbucket -t 1".format(self.num_items)
            _4 = " -u {0} -p {1}".format(_src_master.rest_username, _src_master.rest_password)
            command_to_load = _1 + _2 + _3 + _4
            o, e = shell1.execute_non_sudo_command(command_to_load)
            shell1.log_command_output(o, e)
            time.sleep(20)
            rest = RestConnection(_src_master)
            src_item_count = rest.fetch_bucket_stats(bucket="testbucket")["op"]["samples"]["curr_items"][-1]
            if _bixdcr:
                self.log.info("Load {0} through cbworkloadgen at src..".format(self.num_items))
                _1 = "cd /home/{0}/opt/couchbase &&".format(_dest_master.ssh_username)
                _2 = " ./bin/cbworkloadgen -n localhost:8091 --prefix=d_"
                _3 = " -r .8 -i {0} -s 256 -b testbucket -t 1".format(self.num_items)
                _4 = " -u {0} -p {1}".format(_dest_master.rest_username, _dest_master.rest_password)
                command_to_load = _1 + _2 + _3 + _4
                o, e = shell2.execute_non_sudo_command(command_to_load)
                shell2.log_command_output(o, e)
                time.sleep(20)
                rest = RestConnection(_dest_master)
                dest_item_count = rest.fetch_bucket_stats(bucket="testbucket")["op"]["samples"]["curr_items"][-1]
            self.wait_for_replication_to_catchup(_src_master, _dest_master, 1200, "destination")
            if _bixdcr:
                self.wait_for_replication_to_catchup(_dest_master, _src_master, 1200, "source")
            self.log.info("XDC REPLICATION caught up")
            rest1 = RestConnection(_src_master)
            rest2 = RestConnection(_dest_master)
            curr_count_on_src = rest1.fetch_bucket_stats(bucket="testbucket")["op"]["samples"]["curr_items"][-1]
            curr_count_on_dest = rest2.fetch_bucket_stats(bucket="testbucket")["op"]["samples"]["curr_items"][-1]
            assert(curr_count_on_src==(src_item_count + dest_item_count), "ItemCount on source not what's expected")
            assert(curr_count_on_dest==(src_item_count + dest_item_count), "ItemCount on destination not what's expected")
        elif self._os == "windows":
            # TODO: Windows support
            self.log.info("Yet to add support for windows!")
            pass
        shell1.disconnect()
        shell2.disconnect()
#END TEST 3
