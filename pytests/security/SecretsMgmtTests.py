from membase.api.rest_client import RestConnection, RestHelper
import urllib.request, urllib.parse, urllib.error
import json
from remote.remote_util import RemoteMachineShellConnection, RemoteMachineHelper
from newupgradebasetest import NewUpgradeBaseTest
from security.auditmain import audit
import subprocess
import socket
import fileinput
import sys
from subprocess import Popen, PIPE
from .SecretsMasterBase import SecretsMasterBase
from basetestcase import BaseTestCase
import _thread
from testconstants import STANDARD_BUCKET_PORT
from membase.api.rest_client import RestConnection, Bucket, RestHelper
from membase.api.exception import BucketCreationException
from membase.helper.bucket_helper import BucketOperationHelper
from couchbase_helper.documentgenerator import BlobGenerator


class SecretsMgmtTests(BaseTestCase):

    def setUp(self):
        super(SecretsMgmtTests, self).setUp()
        self.secretmgmt_base_obj = SecretsMasterBase(self.master)
        self.password = self.input.param('password', 'p@ssword')
        enable_audit = self.input.param('audit', None)
        if enable_audit:
            Audit = audit(host=self.master)
            currentState = Audit.getAuditStatus()
            self.log.info("Current status of audit on ip - {0} is {1}".format(self.master.ip, currentState))
            if not currentState:
                self.log.info("Enabling Audit ")
                Audit.setAuditEnable('true')
                self.sleep(30)

    def tearDown(self):
        self.log.info("---------------Into Teardown---------------")
        for server in self.servers:
            self.secretmgmt_base_obj = SecretsMasterBase(server)
            self.secretmgmt_base_obj.set_password(server, "")
            self.secretmgmt_base_obj.change_config_to_orginal(server, "")
            log_dir = (self.secretmgmt_base_obj.get_log_dir(server))[1:-1]
            babysitter_file = str(log_dir + "/babysitter.log")
            shell = RemoteMachineShellConnection(server)
            command = str(" mv " + babysitter_file + " " + log_dir + "/babysitterOLD.log")
            shell.execute_command(command=command)
            self.print_memcached_ip()
            shell.disconnect()
        super(SecretsMgmtTests, self).tearDown()

    def print_memcached_ip(self):
        shell = RemoteMachineShellConnection(self.master)
        o, _ = shell.execute_command("ps aux | grep 'memcached'  | awk '{print $2}'")
        if o:
            mem_pid = o[0]
        shell.disconnect()

    def test_evn_variable(self):
        self.secretmgmt_base_obj.set_password(self.master, self.password)
        self.secretmgmt_base_obj.restart_server_with_env(self.master, self.password)
        temp_return = self.secretmgmt_base_obj.check_log_files(self.master, "/babysitter.log", "Booted")
        self.assertTrue(temp_return, "Babysitter.log does not contain node initialization code")
        res, err_msg = self.secretmgmt_base_obj.check_secret_management_state(self.master, "user_configured")
        self.assertTrue(res, err_msg)

    def test_password_script(self):
        self.secretmgmt_base_obj.set_password(self.master, self.password)
        self.secretmgmt_base_obj.restart_server_with_script(self.master, self.password)
        temp_return = self.secretmgmt_base_obj.check_log_files(self.master, "/babysitter.log", "Booted")
        self.assertTrue(temp_return, "Babysitter.log does not contain node initialization code")
        res, err_msg = self.secretmgmt_base_obj.check_secret_management_state(self.master, "user_configured")
        self.assertTrue(res, err_msg)

    def test_multiple_prompt_3times(self):
        try:
            self.secretmgmt_base_obj.set_password(self.master, self.password)
            shell = RemoteMachineShellConnection(self.master)
            shell.execute_command("/opt/couchbase/etc/couchbase_init.d stop")
            self.secretmgmt_base_obj.start_server_prompt_diff_window(self.master)
            self.sleep(10)
            cmd = "/opt/couchbase/bin/couchbase-cli master-password -c localhost:8091 -u Administrator -p password --send-password"
            # self.secretmgmt_base_obj.incorrect_password(self.master,cmd="/opt/couchbase/bin/cbmaster_password")
            temp_result = self.secretmgmt_base_obj.incorrect_password(self.master, cmd=cmd)
            self.assertTrue(temp_result, "Issue with passing incorrect password 3 times")
        finally:
            for server in self.servers:
                shell = RemoteMachineShellConnection(server)
                if (RemoteMachineHelper(shell).is_process_running('memcached') is None):
                    print('Process Memcached is not running')
                    # shell.set_environment_variable("CB_MASTER_PASSWORD", self.password)
                    shell.execute_command(
                        "export CB_MASTER_PASSWORD=" + self.password + "; /opt/couchbase/etc/couchbase_init.d start")

    def test_multiple_prompt_enter_correct_2retries(self):
        try:
            self.secretmgmt_base_obj.set_password(self.master, self.password)
            shell = RemoteMachineShellConnection(self.master)
            shell.execute_command("/opt/couchbase/etc/couchbase_init.d stop")
            self.secretmgmt_base_obj.start_server_prompt_diff_window(self.master)
            self.sleep(10)
            # self.secretmgmt_base_obj.incorrect_password(self.master, cmd="/opt/couchbase/bin/cbmaster_password",
            #                                            retries_number=2,input_correct_pass=True,correct_pass=self.password)
            cmd = "/opt/couchbase/bin/couchbase-cli master-password -c localhost:8091 -u Administrator -p password --send-password"
            temp_result = self.secretmgmt_base_obj.incorrect_password(self.master, cmd=cmd,
                                                                      retries_number=2, input_correct_pass=True,
                                                                      correct_pass=self.password)
            self.assertTrue(temp_result, "Issue with incorrect password for 2 times and then correct password")

        finally:
            for server in self.servers:
                shell = RemoteMachineShellConnection(server)
                if (RemoteMachineHelper(shell).is_process_running('memcached') is None):
                    shell.set_environment_variable("CB_MASTER_PASSWORD", self.password)

    def test_multiple_prompt_enter_correct_1retries(self):
        try:
            self.secretmgmt_base_obj.set_password(self.master, self.password)
            shell = RemoteMachineShellConnection(self.master)
            shell.execute_command("/opt/couchbase/etc/couchbase_init.d stop")
            self.secretmgmt_base_obj.start_server_prompt_diff_window(self.master)
            self.sleep(10)
            # self.secretmgmt_base_obj.incorrect_password(self.master, cmd="/opt/couchbase/bin/cbmaster_password",
            #                                            retries_number=1, input_correct_pass=True, correct_pass='temp')
            cmd = "/opt/couchbase/bin/couchbase-cli master-password -c localhost:8091 -u Administrator -p password --send-password"
            temp_result = self.secretmgmt_base_obj.incorrect_password(self.master, cmd=cmd,
                                                                      retries_number=1, input_correct_pass=True,
                                                                      correct_pass=self.password)
            self.assertTrue(temp_result, "Issue with incorrect password for 1 times and then correct password")

        finally:
            for server in self.servers:
                shell = RemoteMachineShellConnection(server)
                if (RemoteMachineHelper(shell).is_process_running('memcached') is None):
                    shell.set_environment_variable("CB_MASTER_PASSWORD", self.password)

    def test_prompt_enter_correct_password(self):
        try:
            self.secretmgmt_base_obj.set_password(self.master, self.password)
            shell = RemoteMachineShellConnection(self.master)
            shell.execute_command("/opt/couchbase/etc/couchbase_init.d stop")
            shell.disconnect()
            self.secretmgmt_base_obj.start_server_prompt_diff_window(self.master)
            self.sleep(10)
            # self.secretmgmt_base_obj.incorrect_password(self.master, cmd="/opt/couchbase/bin/cbmaster_password",
            #                                            retries_number=1, input_correct_pass=True, correct_pass='temp')
            cmd = "/opt/couchbase/bin/couchbase-cli master-password -c localhost:8091 -u Administrator -p password --send-password"
            temp_result = self.secretmgmt_base_obj.correct_password_on_prompt(self.master, self.password, cmd=cmd)
            self.assertTrue(temp_result, "Issue with passing in correct password on prompt")
        finally:
            for server in self.servers:
                shell = RemoteMachineShellConnection(server)
                if (RemoteMachineHelper(shell).is_process_running('memcached') is None):
                    shell.set_environment_variable("CB_MASTER_PASSWORD", self.password)

    def test_env_variable_change_pass(self):
        new_pass = self.input.param("new_password", "new_p@ssw0rd")
        self.secretmgmt_base_obj.set_password(self.master, self.password)
        self.secretmgmt_base_obj.restart_server_with_env(self.master, self.password)
        temp_result = self.secretmgmt_base_obj.check_log_files(self.master, "/babysitter.log", "Booted")
        self.assertTrue(temp_result, "Babysitter.log does not contain node initialization code")
        self.secretmgmt_base_obj.set_password(self.master, new_pass)
        self.secretmgmt_base_obj.restart_server_with_env(self.master, new_pass)
        temp_result = self.secretmgmt_base_obj.check_log_files(self.master, "/babysitter.log", "Booted")
        self.assertTrue(temp_result, "Babysitter.log does not contain node initialization code")

    def generate_pass(self):
        type = self.input.param("type", 'char')
        pass_length = self.input.param('pass_length', 10)
        num_pass = self.input.param('num_pass', 10)
        if type in ('char', 'int', 'ext'):
            pass_list = self.secretmgmt_base_obj.generate_password_simple(type, pass_length, num_pass)
        else:
            pass_list = self.secretmgmt_base_obj.generate_password_dual(type, pass_length, num_pass)
        for item in pass_list:
            item = item.decode('ISO-8859-1').strip()
            self.secretmgmt_base_obj.set_password(self.master, item)
            self.secretmgmt_base_obj.restart_server_with_env(self.master, item)
            temp_result = self.secretmgmt_base_obj.check_log_files(self.master, "/babysitter.log",
                                                                   "Booted. Waiting for shutdown request")
            self.assertTrue(temp_result, "Babysitter.log does not contain node initialization code")

    def generate_pass_file(self):
        with open("./pytests/security/password_list.txt") as f:
            for item in f:
                item = item.decode('ISO-8859-1').strip()
                self.secretmgmt_base_obj.set_password(self.master, item)
                self.secretmgmt_base_obj.restart_server_with_env(self.master, item)
                temp_result = self.secretmgmt_base_obj.check_log_files(self.master, "/babysitter.log",
                                                                       "Booted. Waiting for shutdown request")
                self.assertTrue(temp_result, "Babysitter.log does not contain node initialization code")

    def test_cluster_rebalance_in_env_var(self):
        gen_load = BlobGenerator('buckettest', 'buckettest-', self.value_size, start=0, end=self.num_items)
        self._load_all_buckets(self.master, gen_load, "create", 0)
        servers_in = self.servers[1:]
        for servers in servers_in:
            self.secretmgmt_base_obj.setup_pass_node(servers, self.password)
        temp_result = self.cluster.rebalance(self.servers, servers_in, [])
        self.assertTrue(temp_result, "Rebalance-in did not complete with password setup node")

    def test_cluster_rebalance_out(self):
        gen_load = BlobGenerator('buckettest', 'buckettest-', self.value_size, start=0, end=self.num_items)
        self._load_all_buckets(self.master, gen_load, "create", 0)
        servers_in = self.servers[1:]
        for servers in servers_in:
            self.secretmgmt_base_obj.setup_pass_node(servers, self.password)
        self.cluster.rebalance(self.servers, servers_in, [])
        servers_out = self.servers[2:]
        temp_result = self.cluster.rebalance(self.servers, [], servers_out)
        self.assertTrue(temp_result, 'Rebalance-out did not complete with password node setup')

    def test_cluster_rebalance_in_prompt(self):
        gen_load = BlobGenerator('buckettest', 'buckettest-', self.value_size, start=0, end=self.num_items)
        self._load_all_buckets(self.master, gen_load, "create", 0)
        servers_in = self.servers[1:]
        for servers in servers_in:
            self.secretmgmt_base_obj.setup_pass_node(servers, self.password, startup_type='prompt')
        temp_result = self.cluster.rebalance(self.servers, servers_in, [])
        self.assertTrue(temp_result, 'Rebalance-in did not complete with password node setup')

    def test_cluster_rebalance_out_prompt(self):
        gen_load = BlobGenerator('buckettest', 'buckettest-', self.value_size, start=0, end=self.num_items)
        self._load_all_buckets(self.master, gen_load, "create", 0)
        servers_in = self.servers[1:]
        for servers in servers_in:
            self.secretmgmt_base_obj.setup_pass_node(servers, self.password, startup_type='prompt')
        self.cluster.rebalance(self.servers, servers_in, [])
        servers_out = self.servers[2:]
        temp_result = self.cluster.rebalance(self.servers, [], servers_out)
        self.assertTrue(temp_result, 'Rebalance-out did not complete with password node setup')

    def test_cluster_rebalance_in_diff_modes(self):
        gen_load = BlobGenerator('buckettest', 'buckettest-', self.value_size, start=0, end=self.num_items)
        self._load_all_buckets(self.master, gen_load, "create", 0)
        extra_pass = self.input.param('extra_pass', 'p@ssw0rd1')
        servers_in = self.servers[1:]
        server_env_var = servers_in[0]
        server_prompt = servers_in[1]
        server_plain = servers_in[2]
        self.secretmgmt_base_obj.setup_pass_node(server_env_var, self.password)
        self.secretmgmt_base_obj.setup_pass_node(server_prompt, extra_pass, startup_type='prompt')
        self.secretmgmt_base_obj.setup_pass_node(server_plain, startup_type='simple')
        temp_result = self.cluster.rebalance(self.servers, servers_in, [])
        self.assertTrue(temp_result, 'Rebalance-in did not complete with password node setup')

    def test_cluster_rebalance_out_diff_modes(self):
        gen_load = BlobGenerator('buckettest', 'buckettest-', self.value_size, start=0, end=self.num_items)
        self._load_all_buckets(self.master, gen_load, "create", 0)
        extra_pass = self.input.param('extra_pass', 'p@ssw0rd1')
        servers_in = self.servers[1:]
        server_env_var = servers_in[0]
        server_prompt = servers_in[1]
        server_plain = servers_in[2]
        self.secretmgmt_base_obj.setup_pass_node(server_env_var, self.password)
        self.secretmgmt_base_obj.setup_pass_node(server_prompt, extra_pass, startup_type='prompt')
        self.secretmgmt_base_obj.setup_pass_node(server_plain, startup_type='simple')
        self.cluster.rebalance(self.servers, servers_in, [])
        servers_out = self.servers[2:]
        temp_result = self.cluster.rebalance(self.servers, [], servers_out)
        self.assertTrue(temp_result, 'Rebalance-out did not complete with password node setup')

    # services_in=kv-index-n1ql,nodes_init=1,nodes_in=3
    def test_cluster_rebalance_in_env_var_services(self):
        gen_load = BlobGenerator('buckettest', 'buckettest-', self.value_size, start=0, end=self.num_items)
        self._load_all_buckets(self.master, gen_load, "create", 0)
        self.find_nodes_in_list()
        servers_in = self.servers[1:]
        for servers in servers_in:
            self.secretmgmt_base_obj.setup_pass_node(servers, self.password)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], self.nodes_in_list, [],
                                                 services=self.services_in)
        self.assertTrue(rebalance.result(), "Issue with Reablance in with different services")

    # services_in=kv-index-n1ql,nodes_init=1,nodes_in=3
    def test_cluster_rebalance_in_diff_type_var_services(self):
        gen_load = BlobGenerator('buckettest', 'buckettest-', self.value_size, start=0, end=self.num_items)
        self._load_all_buckets(self.master, gen_load, "create", 0)
        self.find_nodes_in_list()
        servers_in = self.servers[1:]
        server_env_var = servers_in[0]
        server_prompt = servers_in[1]
        server_plain = servers_in[2]
        self.secretmgmt_base_obj.setup_pass_node(server_env_var, self.password)
        self.secretmgmt_base_obj.setup_pass_node(server_prompt, self.password, startup_type='prompt')
        self.secretmgmt_base_obj.setup_pass_node(server_plain, startup_type='simple')
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], self.nodes_in_list, [],
                                                 services=self.services_in)
        self.assertTrue(rebalance.result(), "Rebalance in with different servers")

    # services_in=kv-index-n1ql,nodes_init=1,nodes_in=3
    def test_cluster_rebalance_out_env_var_services(self):
        gen_load = BlobGenerator('buckettest', 'buckettest-', self.value_size, start=0, end=self.num_items)
        self._load_all_buckets(self.master, gen_load, "create", 0)
        self.find_nodes_in_list()
        servers_in = self.servers[1:]
        for servers in servers_in:
            self.secretmgmt_base_obj.setup_pass_node(servers, self.password)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], self.nodes_in_list, [],
                                                 services=self.services_in)
        print(("result of rebalance is {0}".format(rebalance.result())))
        servers_out = self.servers[2:]
        rebalance = self.cluster.async_rebalance(self.servers, [], servers_out)
        print(("result of rebalance is {0}".format(rebalance.result())))
        self.assertTrue(rebalance.result(), "Rebalance out with different service")

    # services_in=kv-index-n1ql,nodes_init=1,nodes_in=3
    def test_cluster_rebalance_out_diff_type_var_services(self):
        extra_pass = self.input.param("extra_pass", 'p@ssw0rd01')
        gen_load = BlobGenerator('buckettest', 'buckettest-', self.value_size, start=0, end=self.num_items)
        self._load_all_buckets(self.master, gen_load, "create", 0)
        self.find_nodes_in_list()
        servers_in = self.servers[1:]
        server_env_var = servers_in[0]
        server_prompt = servers_in[1]
        server_plain = servers_in[2]
        self.secretmgmt_base_obj.setup_pass_node(server_env_var, self.password)
        self.secretmgmt_base_obj.setup_pass_node(server_prompt, extra_pass, startup_type='prompt')
        self.secretmgmt_base_obj.setup_pass_node(server_plain, startup_type='simple')
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], self.nodes_in_list, [],
                                                 services=self.services_in)
        rebalance.result()
        servers_out = self.servers[1:]
        rebalance = self.cluster.async_rebalance(self.servers, [], servers_out)
        print((rebalance.result()))
        self.assertTrue(rebalance.result(), "Rebalance in  and out with different servers")

    # services_init = kv - kv:n1ql - index - kv:index, nodes_init = 4, nodes_out = 1, nodes_out_dist = kv:1, graceful = False
    # services_init = kv - kv:n1ql - index - kv:index, nodes_init = 4, nodes_out = 1, nodes_out_dist = kv:1, graceful = False,recoveryType=delta
    def test_failover_add_back(self):
        gen_load = BlobGenerator('buckettest', 'buckettest-', self.value_size, start=0, end=self.num_items)
        self._load_all_buckets(self.master, gen_load, "create", 0)
        try:
            for servers in self.servers:
                self.secretmgmt_base_obj.setup_pass_node(servers, self.password)
            self.sleep(30)
            rest = RestConnection(self.master)
            self.graceful = self.input.param('graceful', False)
            recoveryType = self.input.param("recoveryType", "full")
            self.find_nodes_in_list()
            self.generate_map_nodes_out_dist()
            servr_out = self.nodes_out_list
            nodes_all = rest.node_statuses()
            failover_task = self.cluster.async_failover([self.master],
                                                        failover_nodes=servr_out, graceful=self.graceful)
            failover_task.result()
            nodes_all = rest.node_statuses()
            nodes = []
            if servr_out[0].ip == "127.0.0.1":
                for failover_node in servr_out:
                    nodes.extend([node for node in nodes_all
                                  if (str(node.port) == failover_node.port)])
            else:
                for failover_node in servr_out:
                    nodes.extend([node for node in nodes_all
                                  if node.ip == failover_node.ip])
            for node in nodes:
                self.log.info(node)
                rest.add_back_node(node.id)
                rest.set_recovery_type(otpNode=node.id, recoveryType=recoveryType)
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [])
            self.assertTrue(rebalance.result(), "Failover with different servers")
        except Exception as ex:
            raise

    # services_init=kv-kv-index-index:n1ql,nodes_init=4,nodes_out=1,nodes_out_dist=kv:1,graceful=True
    def test_failover(self):
        gen_load = BlobGenerator('buckettest', 'buckettest-', self.value_size, start=0, end=self.num_items)
        self._load_all_buckets(self.master, gen_load, "create", 0)
        try:
            for servers in self.servers:
                self.secretmgmt_base_obj.setup_pass_node(servers, 'temp')
            self.sleep(30)
            self.find_nodes_in_list()
            self.generate_map_nodes_out_dist()
            servr_out = self.nodes_out_list
            print(servr_out)
            self.graceful = self.input.param('graceful', False)
            failover_task = self.cluster.async_failover([self.master],
                                                        failover_nodes=servr_out, graceful=self.graceful)
            failover_task.result()
            self.log.info("Rebalance first time")
            rebalance = self.cluster.rebalance(self.servers[:self.nodes_init], [], [])
            self.log.info("Rebalance Second time")
            rebalance = self.cluster.rebalance(self.servers[:self.nodes_init], [], [])
        except Exception as ex:
            raise

    # services_init=kv-kv-index-index:n1ql,nodes_init=4,targetProcess=memcached
    # services_init=kv-kv-index-index:n1ql,nodes_init=4,targetProcess=babysitter
    def kill_process(self):
        self.targetProcess = self.input.param("targetProcess", 'memcached')
        for servers in self.servers:
            self.secretmgmt_base_obj.setup_pass_node(servers, self.password)

        for servers in self.servers:
            remote = RemoteMachineShellConnection(servers)
            if self.targetProcess == "memcached":
                remote.kill_memcached()
            else:
                remote.terminate_process(process_name=self.targetProcess)

        for servers in self.servers:
            self.secretmgmt_base_obj.restart_server_with_env(servers, self.password)
            temp_result = self.secretmgmt_base_obj.check_log_files(self.master, "/babysitter.log",
                                                                   "Booted. Waiting for shutdown request")
            self.assertTrue(temp_result, "Issue with server restart after killing of process")

    def restart_server(self):
        for servers in self.servers:
            self.secretmgmt_base_obj.setup_pass_node(servers, self.password)

        for servers in self.servers:
            self.secretmgmt_base_obj.restart_server_with_env(servers, self.password)
            temp_result = self.secretmgmt_base_obj.check_log_files(self.master, "/babysitter.log",
                                                                   "Booted. Waiting for shutdown request")
            self.assertTrue(temp_result, "Issue with server restart of server")

    # services_init=kv-kv-index-index:n1ql,nodes_init=4,default_bucket=False,bucket_type=sasl
    # services_init=kv-kv-index-index:n1ql,nodes_init=4,default_bucket=False,bucket_type=standard
    # services_init=kv-kv-index-index:n1ql,nodes_init=4,default_bucket=False,bucket_type=standard,password=a@cd#efgh@
    # services_init=kv-kv-index-index:n1ql,nodes_init=4,default_bucket=False,bucket_type=standard,password=a@cd#efgh@
    def test_bucket_create_password(self, bucket_name='secretsbucket', num_replicas=1, bucket_size=256):
        for servers in self.servers:
            self.secretmgmt_base_obj.setup_pass_node(servers, self.password)
        bucket_type = self.input.param("bucket_type", 'couchbase')
        tasks = []
        if bucket_type == 'couchbase':
            # self.cluster.create_sasl_bucket(self.master, bucket_name, self.password, num_replicas)
            rest = RestConnection(self.master)
            rest.create_bucket(bucket_name, ramQuotaMB=256)
        elif bucket_type == 'standard':
            self.cluster.create_standard_bucket(self.master, bucket_name, STANDARD_BUCKET_PORT + 1,
                                                bucket_size)
        else:
            self.log.error('Bucket type not specified')
            return
        self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(bucket_name, RestConnection(self.master)),
                        msg='failed to start up bucket with name "{0}'.format(bucket_name))
        gen_load = BlobGenerator('buckettest', 'buckettest-', self.value_size, start=0, end=self.num_items)
        self._load_all_buckets(self.master, gen_load, "create", 0)
        install_path = self.secretmgmt_base_obj._get_install_path(self.master)
        temp_result = self.secretmgmt_base_obj.check_config_files(self.master, install_path, '/config/config.dat',
                                                                  self.password)
        self.assertTrue(temp_result, "Password found in config.dat")
        temp_result = self.secretmgmt_base_obj.check_config_files(self.master, install_path, 'isasl.pw', self.password)
        self.assertTrue(temp_result, "Password found in isasl.pw")

    def test_bucket_edit_password(self, bucket_name='secretsbucket', num_replicas=1, bucket_size=100):
        updated_pass = "p@ssw0rd_updated"
        rest = RestConnection(self.master)
        for servers in self.servers:
            self.secretmgmt_base_obj.setup_pass_node(servers, self.password)
        bucket_type = self.input.param("bucket_type", 'standard')
        tasks = []
        if bucket_type == 'sasl':
            self.cluster.create_sasl_bucket(self.master, bucket_name, self.password, num_replicas, bucket_size)
            self.sleep(10)
            rest.change_bucket_props(bucket_name, saslPassword=updated_pass)
        else:
            self.log.error('Bucket type not specified')
            return
        self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(bucket_name, RestConnection(self.master)),
                        msg='failed to start up bucket with name "{0}'.format(bucket_name))
        gen_load = BlobGenerator('buckettest', 'buckettest-', self.value_size, start=0, end=self.num_items)
        self._load_all_buckets(self.master, gen_load, "create", 0)
        install_path = self.secretmgmt_base_obj._get_install_path(self.master)
        temp_result = self.secretmgmt_base_obj.check_config_files(self.master, install_path, '/config/config.dat',
                                                                  updated_pass)
        self.assertTrue(temp_result, "Password found in config.dat")
        temp_result = self.secretmgmt_base_obj.check_config_files(self.master, install_path, 'isasl.pw', updated_pass)
        self.assertTrue(temp_result, "Password found in isasl.pw")

    def test_cli_setting(self):
        temp_result = self.secretmgmt_base_obj.execute_cli(self.master, new_password=self.password)
        self.assertTrue(temp_result, "Output of the command is not correct")
        self.secretmgmt_base_obj.restart_server_with_env(self.master, self.password)
        temp_result = self.secretmgmt_base_obj.check_log_files(self.master, "/babysitter.log", "Booted. Waiting for shutdown request")
        self.assertTrue(temp_result, "Babysitter.log does not contain node initialization code")

    def test_cbcollect(self):
        rest = RestConnection(self.master)
        bucket_name = 'cbcollectbucket'
        num_replicas = 1
        bucket_size = 100
        # self.cluster.create_sasl_bucket(self.master, bucket_name, self.password, num_replicas, bucket_size)
        rest.create_bucket(bucket_name, ramQuotaMB=256)
        result = self.secretmgmt_base_obj.generate_cb_collect(self.master, "cbcollect.zip", self.password)
        self.assertTrue(result, "Bucket password appears in the cbcollect info")

    def rotate_data_key(self):
        temp_result = self.secretmgmt_base_obj.read_ns_config(self.master)
        self.assertTrue(temp_result, "Config.dat is not refereshed after data key")

    def cli_rotate_key(self):
        temp_result = self.secretmgmt_base_obj.execute_cli_rotate_key(self.master)
        self.assertTrue(temp_result, "Issue with rotate key on cli side")

    def audit_change_password(self):
        self.secretmgmt_base_obj.set_password(self.master, self.password)
        Audit = audit(eventID='8233', host=self.master)
        expectedResults = {"real_userid:source": "ns_server", "real_userid:user": "Administrator",
                           "ip": self.ipAddress, "port": 123456}
        fieldVerification, valueVerification = self.Audit.validateEvents(expectedResults)
        self.assertTrue(fieldVerification, "One of the fields is not matching")
        self.assertTrue(valueVerification, "Values for one of the fields is not matching")

    def audit_change_password(self):
        self.secretmgmt_base_obj.execute_cli_rotate_key(self.master)
        Audit = audit(eventID='8234', host=self.master)
        expectedResults = {"real_userid:source": "ns_server", "real_userid:user": "Administrator",
                           "ip": self.ipAddress, "port": 123456}
        fieldVerification, valueVerification = self.Audit.validateEvents(expectedResults)
        self.assertTrue(fieldVerification, "One of the fields is not matching")
        self.assertTrue(valueVerification, "Values for one of the fields is not matching")


class SecretsMgmtUpgrade(NewUpgradeBaseTest):

    def setUp(self):
        super(SecretsMgmtUpgrade, self).setUp()
        self.initial_version = self.input.param("initial_version", '4.1.0-5005')
        self.upgrade_version = self.input.param("upgrade_version", "4.6.0-3467")
        self.secretmgmt_base_obj = SecretsMasterBase(self.master)
        self.password = self.input.param('password', 'password')

    def tearDown(self):
        super(SecretsMgmtUpgrade, self).tearDown()

    def upgrade_all_nodes(self):
        servers_in = self.servers[1:]
        self._install(self.servers)
        self.cluster.rebalance(self.servers, servers_in, [])

        upgrade_threads = self._async_update(upgrade_version=self.upgrade_version, servers=self.servers)
        for threads in upgrade_threads:
            threads.join()

        for server in self.servers:
            self.secretmgmt_base_obj.setup_pass_node(server, self.password)
            self.secretmgmt_base_obj.restart_server_with_env(self.master, self.password)
            temp_result = self.secretmgmt_base_obj.check_log_files(self.master, "/babysitter.log", "Booted")
            self.assertTrue(temp_result, "Babysitter.log does not contain node initialization code")

        for server in self.servers:
            rest = RestConnection(server)
            temp = rest.cluster_status()
            self.log.info("Initial status of {0} cluster is {1}".format(server.ip, temp['nodes'][0]['status']))
            while (temp['nodes'][0]['status'] == 'warmup'):
                self.log.info("Waiting for cluster to become healthy")
                self.sleep(5)
                temp = rest.cluster_status()
            self.log.info("current status of {0}  is {1}".format(server.ip, temp['nodes'][0]['status']))

    def upgrade_all_nodes_post_463(self):
        servers_in = self.servers[1:]
        self._install(self.servers)
        self.cluster.rebalance(self.servers, servers_in, [])

        for server in self.servers:
            self.secretmgmt_base_obj.setup_pass_node(server, self.password)
            self.secretmgmt_base_obj.restart_server_with_env(self.master, self.password)
            temp_result = self.secretmgmt_base_obj.check_log_files(self.master, "/babysitter.log", "Booted")
            self.assertTrue(temp_result, "Babysitter.log does not contain node initialization code")

        upgrade_threads = self._async_update(upgrade_version=self.upgrade_version, servers=self.servers)
        for threads in upgrade_threads:
            threads.join()

        for server in self.servers:
            rest = RestConnection(server)
            temp = rest.cluster_status()
            self.log.info("Initial status of {0} cluster is {1}".format(server.ip, temp['nodes'][0]['status']))
            while (temp['nodes'][0]['status'] == 'warmup'):
                self.log.info("Waiting for cluster to become healthy")
                self.sleep(5)
                temp = rest.cluster_status()
            self.log.info("current status of {0}  is {1}".format(server.ip, temp['nodes'][0]['status']))


    def upgrade_half_nodes(self):
        serv_upgrade = self.servers[2:4]
        servers_in = self.servers[1:]
        self._install(self.servers)
        self.cluster.rebalance(self.servers, servers_in, [])

        upgrade_threads = self._async_update(upgrade_version=self.upgrade_version, servers=serv_upgrade)
        for threads in upgrade_threads:
            threads.join()

        for server in serv_upgrade:
            rest = RestConnection(server)
            temp = rest.cluster_status()
            self.log.info("Initial status of {0} cluster is {1}".format(server.ip, temp['nodes'][0]['status']))
            while (temp['nodes'][0]['status'] == 'warmup'):
                self.log.info("Waiting for cluster to become healthy")
                self.sleep(5)
                temp = rest.cluster_status()
            self.log.info("current status of {0}  is {1}".format(server.ip, temp['nodes'][0]['status']))
