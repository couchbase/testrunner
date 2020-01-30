import copy
import json, ast, filecmp, itertools
import os, shutil, ast
from threading import Thread
from subprocess import Popen, PIPE, check_output, STDOUT, CalledProcessError

from TestInput import TestInputSingleton, TestInputServer
from alternate_address.alternate_address_base import AltAddrBaseTest
from membase.api.rest_client import RestConnection, RestHelper
from couchbase_helper.cluster import Cluster
from remote.remote_util import RemoteMachineShellConnection
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from couchbase_helper.documentgenerator import BlobGenerator, JsonDocGenerator
from pprint import pprint
from testconstants import CLI_COMMANDS, LINUX_COUCHBASE_BIN_PATH,\
                          WIN_COUCHBASE_BIN_PATH, COUCHBASE_FROM_MAD_HATTER,\
                          WIN_TMP_PATH_RAW


class AlternateAddressTests(AltAddrBaseTest):
    def setUp(self):
        for server in TestInputSingleton.input.servers:
            remote = RemoteMachineShellConnection(server)
            remote.enable_diag_eval_on_non_local_hosts()
            remote.disconnect()
        super(AlternateAddressTests, self).setUp()
        self.remove_all_alternate_address_settings()
        self.cluster_helper = Cluster()
        self.ex_path = self.tmp_path + "export{0}/".format(self.master.ip)
        self.num_items = self.input.param("items", 1000)
        self.client_os = self.input.param("client_os", "linux")
        self.localhost = self.input.param("localhost", False)
        self.json_create_gen = JsonDocGenerator("altaddr", op_type="create",
                                       encoding="utf-8", start=0, end=self.num_items)
        self.json_delete_gen = JsonDocGenerator("imex", op_type="delete",
                                       encoding="utf-8", start=0, end=self.num_items)

    def tearDown(self):
        try:
            super(AlternateAddressTests, self).tearDown()
        except Exception as e:
            print(e)
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)
        ClusterOperationHelper.cleanup_cluster(self.servers, self.servers[0])


    def test_setting_alternate_address(self):
        server1 = self.servers[0]
        url_format = ""
        secure_port = ""
        secure_conn = ""
        self.skip_set_alt_addr = False
        shell = RemoteMachineShellConnection(server1)
        if self.secure_conn:
            cacert = self.get_cluster_certificate_info(server1)
            secure_port = "1"
            url_format = "s"
            if not self.no_cacert:
                secure_conn = "--cacert {0}".format(cacert)
            if self.no_ssl_verify:
                secure_conn = "--no-ssl-verify"
        output = self.list_alt_address(server=server1, url_format = url_format,
                                          secure_port = secure_port, secure_conn = secure_conn)
        if self._check_output(server1.ip, output):
            output, _ = self.remove_alt_address_setting(server=server1,
                                                        url_format = url_format,
                                                        secure_port = secure_port,
                                                        secure_conn = secure_conn)
            mesg = 'SUCCESS: Alternate address configuration deleted'
            if not self._check_output(mesg, output):
                self.fail("Fail to remove alternate address")
        output = self.list_alt_address(server=server1, url_format = url_format,
                                          secure_port = secure_port,
                                          secure_conn = secure_conn)
        if output and self._check_output(server1.ip, output):
            self.fail("Fail to remove alternate address with remove command")

        self.log.info("Start to set alternate address")
        internal_IP = self.get_internal_IP(server1)
        setting_cmd = "{0}couchbase-cli{1} {2}"\
                       .format(self.cli_command_path, self.cmd_ext,
                               "setting-alternate-address")
        setting_cmd += " -c http{0}://{1}:{2}{3} --username {4} --password {5} {6}"\
                       .format(url_format, internal_IP , secure_port, server1.port,
                               server1.rest_username, server1.rest_password, secure_conn)
        setting_cmd = setting_cmd + "--set --node {0} --hostname {1} "\
                                        .format(internal_IP, server1.ip)
        shell.execute_command(setting_cmd)
        output = self.list_alt_address(server=server1, url_format = url_format,
                                                     secure_port = secure_port,
                                                     secure_conn = secure_conn)
        if output and output[2]:
            if not self._check_output(server1.ip, output):
                self.fail("Fail to set correct hostname")
        else:
            self.fail("Fail to set alternate address")
        self.log.info("Start to add node to cluster use internal IP")
        services_in = self.alt_addr_services_in
        if "-" in services_in:
            set_services = services_in.split("-")
        else:
            set_services = services_in.split(",")
        i = 0
        num_hostname_add = 1
        for server in self.servers[1:]:
            add_node_IP = self.get_internal_IP(server)
            node_services = "kv"
            if len(set_services) == 1:
                node_services = set_services[0]
            elif len(set_services) > 1:
                if len(set_services) == len(self.servers[1:]):
                    node_services = set_services[i]
                    i += 1
            if self.add_hostname_node and num_hostname_add <= self.num_hostname_add:
                add_node_IP = server.ip
                num_hostname_add += 1

            try:
                shell.alt_addr_add_node(main_server=server1, internal_IP=add_node_IP,
                                        server_add=server, services=node_services,
                                        cmd_ext=self.cmd_ext)
            except Exception as e:
                if e:
                    self.fail("Error: {0}".format(e))
        rest = RestConnection(self.master)
        rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[])
        rest.monitorRebalance()
        self.log.info("Create default bucket")
        self._create_default_bucket(self.master)
        buckets = rest.get_buckets()
        status = RestHelper(rest).vbucket_map_ready(buckets[0].name)
        if not status:
            self.fail("Failed to create bucket.")

        if self.run_alt_addr_loader:
            if self.alt_addr_kv_loader:
                self.kv_loader(server1, client_os = self.client_os)
            if self.alt_addr_n1ql_query:
                self.n1ql_query(server1.ip, self.client_os,
                                create_travel_sample_bucket=True)
            if self.alt_addr_eventing_function:
                self.create_eventing_function(server1, self.client_os,
                                              create_travel_sample_bucket=True)
                self.skip_set_alt_addr = True
        alt_addr_status = []
        if not self.skip_set_alt_addr:
            for server in self.servers[1:]:
                internal_IP = self.get_internal_IP(server)
                status = self.set_alternate_address(server, url_format = url_format,
                               secure_port = secure_port, secure_conn = secure_conn,
                               internal_IP = internal_IP)
                alt_addr_status.append(status)
            if False in alt_addr_status:
                self.fail("Fail to set alt address")
            else:
                self.all_alt_addr_set = True
                if self.run_alt_addr_loader:
                    if self.alt_addr_kv_loader:
                        self.kv_loader(server1, self.client_os)
                    if self.alt_addr_n1ql_query:
                        self.n1ql_query(server1.ip, self.client_os)
        remove_node = ""
        if self.alt_addr_rebalance_out:
            internal_IP = self.get_internal_IP(self.servers[-1])
            reject_node = "ns_1@{0}".format(internal_IP)
            self.log.info("Rebalance out a node {0}".format(internal_IP))
            rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()],\
                                                     ejectedNodes=[reject_node])
            reb_status = rest.monitorRebalance()
            self.assertTrue(reb_status, "Rebalance out node {0} failed".format(internal_IP))
            remove_node = internal_IP
        if self.alt_addr_rebalance_in and self.alt_addr_rebalance_out:
            if remove_node:
                free_node = remove_node
                if self.add_hostname_node:
                    free_node = self.get_external_IP(remove_node)
                cmd = 'curl -X POST -d  "hostname={0}&user={1}&password={2}&services={3}" '\
                             .format(free_node, server1.rest_username, server1.rest_password,
                                     self.alt_addr_rebalance_in_services)
                cmd += '-u Administrator:password http://{0}:8091/controller/addNode'\
                             .format(server1.ip)
                shell.execute_command(cmd)
                rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()],\
                                                                    ejectedNodes=[])
                reb_status = rest.monitorRebalance()
                self.assertTrue(reb_status, "Rebalance back in failed")
                status = self.set_alternate_address(self.servers[-1], url_format = url_format,
                                                    secure_port = secure_port,
                                                    secure_conn = secure_conn,
                                                    internal_IP = free_node)
                if status:
                    self.all_alt_addr_set = True
                else:
                    self.all_alt_addr_set = False
            else:
                self.fail("We need a free node to add to cluster")
            if self.run_alt_addr_loader:
                if self.alt_addr_kv_loader:
                    self.kv_loader(server1, self.client_os)
                if self.alt_addr_n1ql_query:
                    self.n1ql_query(server1.ip, self.client_os)
        status = self.remove_all_alternate_address_settings()
        if not status:
            self.fail("Failed to remove all alternate address setting")

    def test_alt_addr_with_xdcr(self):
        url_format = ""
        secure_port = ""
        secure_conn = ""
        self.setup_xdcr_cluster()
        des_alt_addr_set = False

        self.log.info("Create bucket at source")
        src_master = self.clusters_dic[0][0]
        self._create_buckets(src_master)
        src_rest = RestConnection(src_master)
        src_buckets = src_rest.get_buckets()
        if src_buckets and src_buckets[0]:
            src_bucket_name = src_buckets[0].name
        else:
            self.fail("Failed to create bucket at src cluster")

        des_master = self.clusters_dic[1][0]
        self.log.info("Create bucket at destination")
        self._create_buckets(des_master)
        des_rest = RestConnection(des_master)
        des_buckets = des_rest.get_buckets()
        if des_buckets and des_buckets[0]:
            des_bucket_name = des_buckets[0].name
        else:
            self.fail("Failed to create bucket at des cluster")

        for server in self.clusters_dic[0]:
            internal_IP = self.get_internal_IP(server)
            status = self.set_alternate_address(server, url_format = url_format,
                           secure_port = secure_port, secure_conn = secure_conn,
                           internal_IP = internal_IP)
        self.all_alt_addr_set = True

        self.kv_loader(src_master, "mac")
        self.create_xdcr_reference(src_master.ip, des_master.ip)

        src_num_docs = int(src_rest.get_active_key_count(src_bucket_name))
        count = 0
        src_num_docs = int(src_rest.get_active_key_count(src_bucket_name))
        while count < 10:
            if src_num_docs < 10000:
                self.sleep(10, "wait for items written to bucket")
                src_num_docs = int(src_rest.get_active_key_count(src_bucket_name))
                count += 1
            if src_num_docs == 10000:
                self.log.info("all bucket items set")
                break
            if count == 2:
                self.fail("bucket items does not set after 30 seconds")

        self.create_xdcr_replication(src_master.ip, des_master.ip, src_bucket_name)
        self.sleep(25, "time needed for replication to be created")

        self.log.info("Reduce check point time to 30 seconds")
        self.set_xdcr_checkpoint(src_master.ip, 30)
        #self.set_xdcr_checkpoint(des_master.ip, 30)

        self.log.info("Get xdcr configs from cluster")
        shell = RemoteMachineShellConnection(self.master)
        rep_id_cmd = "curl -u Administrator:password http://{0}:8091/pools/default/remoteClusters"\
                                                                            .format(self.master.ip)
        output, error = shell.execute_command(rep_id_cmd)
        output = output[0][1:-1]
        xdcr_config = json.loads(output)

        cmd = "curl -u Administrator:password http://localhost:8091/sasl_logs/goxdcr " 
        cmd += "|  grep  'Execution timed out' | tail -n 1 "
        output, error = shell.execute_command(cmd)
        self.log.info("Verify replication timeout due to alt addr does not enable at des cluster")
        if xdcr_config["uuid"] in output[0] and "Execution timed out" in output[0]:
            self.log.info("replication failed as expected as alt addr does not enable at des")
        else:
            self.fail("Alt addr failed to disable at des cluster")

        count = 0
        des_num_docs = int(des_rest.get_active_key_count(des_bucket_name))
        while count < 6:
            if src_num_docs != des_num_docs:
                self.sleep(60, "wait for replication ...")
                des_num_docs = int(des_rest.get_active_key_count(des_bucket_name))
                count += 1
            elif src_num_docs == des_num_docs:
                self.fail("Replication should fail.  Alt addr at des does not block")
                break
            if count == 6:
                if not des_alt_addr_set:
                    self.log.info("This is expected since alt addr is not set yet")

        des_alt_addr_status =[]
        for server in self.clusters_dic[1]:
            internal_IP = self.get_internal_IP(server)
            des_alt_addr_status.append(self.set_alternate_address(server, url_format = url_format,
                           secure_port = secure_port, secure_conn = secure_conn,
                           internal_IP = internal_IP))
        if False in des_alt_addr_status:
            self.fail("Failed to set alt addr at des cluster")
        else:
            des_alt_addr_set = True

        count = 0
        self.log.info("Restart replication")
        cmd = "curl -X POST -u Administrator:password "
        cmd += "http://{0}:8091/settings/replications/{1}%2F{2}%2F{2} "\
                 .format(self.master.ip, xdcr_config["uuid"], des_bucket_name)
        cmd += "-d pauseRequested="
        try:
            check_output(cmd + "true", shell=True, stderr=STDOUT)
            self.sleep(20)
            check_output(cmd + "false", shell=True, stderr=STDOUT)
        except CalledProcessError as e:
            print("Error return code: {0}".format(e.returncode))
            if e.output:
                self.fail(e.output)
        des_rest = RestConnection(des_master)

        self.log.info("Verify docs is replicated to des cluster")
        while count < 6:
            if src_num_docs != des_num_docs:
                self.sleep(60, "wait for replication start...")
                des_num_docs = int(des_rest.get_active_key_count(des_bucket_name))
                count += 1
            elif src_num_docs == des_num_docs:
                self.log.info("Replication is complete")
                break
            if count == 6:
                if des_alt_addr_set:
                    self.fail("Replication does not complete after 6 minutes")

        self.delete_xdcr_replication(src_master.ip, xdcr_config["uuid"])

    def remove_all_alternate_address_settings(self):
        self.log.info("Remove alternate address setting in each node")
        remove_alt = []
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            cmd = "{0}couchbase-cli{1} {2} -c {3}:{4} --username {5} --password {6} {7}"\
                    .format(self.cli_command_path, self.cmd_ext,
                            "setting-alternate-address", server.ip,
                            server.port, server.rest_username, server.rest_password,
                            "--remove")
            output, error = shell.execute_command(cmd, debug=True)
            if error:
                remove_alt.append(error)
            shell.disconnect()
        if remove_alt:
            self.log.error("Remove alt address failed: {0}".format(remove_alt))
            return False
        else:
            return True

    def remove_alt_address_setting(self, server=None, url_format = "", secure_port = "",
                                   secure_conn = ""):
        sub_command = "setting-alternate-address"
        if server is None:
            server = self.master
        cmd = "{0}couchbase-cli{1} {2} -c http{3}://{4}:{5}{6} --username {7} --password {8} {9}"\
                    .format(self.cli_command_path, self.cmd_ext,
                            sub_command, url_format, server.ip, secure_port,
                            server.port, server.rest_username, server.rest_password,
                            secure_conn)
        remove_cmd = cmd + " --remove --node {0}".format(server.ip)
        shell = RemoteMachineShellConnection(server)
        output, error = shell.execute_command(remove_cmd)
        shell.disconnect()
        return output, error

    def list_alt_address(self, server=None, url_format = "", secure_port = "", secure_conn = ""):
        sub_command = "setting-alternate-address"
        if server is None:
            server = self.master
        cmd = "{0}couchbase-cli{1} {2} -c http{3}://{4}:{5}{6} --username {7} --password {8} {9}"\
                    .format(self.cli_command_path, self.cmd_ext,
                            sub_command, url_format, server.ip, secure_port,
                            server.port, server.rest_username, server.rest_password,
                            secure_conn)
        list_cmd = cmd + " --list"
        shell = RemoteMachineShellConnection(server)
        output, error = shell.execute_command(list_cmd)
        shell.disconnect()
        return output

    def set_alternate_address(self, server=None, url_format = "", secure_port = "",
                              secure_conn = "", internal_IP = ""):
        self.log.info("Start to set alternate address")
        if server is None:
            server = self.master
        shell = RemoteMachineShellConnection(server)
        internal_IP = self.get_internal_IP(server)
        setting_cmd = "{0}couchbase-cli{1} {2}"\
                       .format(self.cli_command_path, self.cmd_ext,
                               "setting-alternate-address")
        setting_cmd += " -c http{0}://{1}:{2}{3} --username {4} --password {5} {6}"\
                       .format(url_format, internal_IP , secure_port, server.port,
                               server.rest_username, server.rest_password, secure_conn)
        setting_cmd = setting_cmd + "--set --node {0} --hostname {1} "\
                                         .format(internal_IP, server.ip)
        shell.execute_command(setting_cmd)
        output = self.list_alt_address(server=server, url_format = url_format,
                                          secure_port = secure_port,
                                          secure_conn = secure_conn)
        if output:
            return True
        else:
            return False

    def get_cluster_certificate_info(self, server):
        """
            This will get certificate info from cluster
        """
        cert_file_location = self.root_path + "cert.pem"
        if self.os == "windows":
            cert_file_location = WIN_TMP_PATH_RAW + "cert.pem"
        shell = RemoteMachineShellConnection(server)
        cmd = "{0}couchbase-cli{1} ssl-manage "\
                     .format(self.cli_command_path, self.cmd_ext)
        cmd += "-c {0}:{1} -u Administrator -p password --cluster-cert-info > {2}"\
                                               .format(server.ip, server.port,
                                                       cert_file_location)
        output, _ = shell.execute_command(cmd)
        if output and "Error" in output[0]:
            self.fail("Failed to get CA certificate from cluster.")
        shell.disconnect()
        return cert_file_location

    def kv_loader(self, server = None, client_os = "linux"):
        if server is None:
            server = self.master
        buckets = RestConnection(server).get_buckets()
        base_path = "/opt/couchbase/bin/"
        if client_os == "mac":
            base_path = "/Applications/Couchbase\ Server.app/Contents/Resources/couchbase-core/bin/"
        loader_path = "{0}cbworkloadgen{1}".format(base_path, self.cmd_ext)
        cmd_load = " -n {0}:8091 -u Administrator -p password -j -b {1}"\
                                       .format(server.ip, buckets[0].name)
        error_mesg = "No alternate address information found"
        try:
            self.log.info("Load kv doc to bucket from outside network")
            output = check_output("{0} {1}".format(loader_path, cmd_load), shell=True, stderr=STDOUT)
            if output:
                self.log.info("Output from kv loader: {0}".format(output))
        except CalledProcessError as e:
            print("Error return code: ", e.returncode)
            if e.output:
                if self.all_alt_addr_set:
                    if "No alternate address information found" in e.output:
                        self.fail("Failed to set alternate address.")
                    else:
                        self.fail("Failed to load to remote cluster.{0}"\
                                  .format(e.output))
                else:
                    self.log.info("Error is expected due to alt addre not set yet")

    """ param: default_bucket=False """
    def n1ql_query(self, server_IP = None, client_os = "linux",
                   create_travel_sample_bucket=False):
        if server_IP is None:
            server_IP = self.master.ip

        self._create_travel_sample_bucket(server_IP,
                                          create_travel_sample_bucket=create_travel_sample_bucket)

        base_path = "/opt/couchbase/bin/"
        query_cmd = 'SELECT country FROM `travel-sample` WHERE name = "Excel Airways";'
        if client_os == "mac":
            base_path = "/Applications/Couchbase\ Server.app/Contents/Resources/couchbase-core/bin/"
        loader_path = "{0}cbq{1}".format(base_path, self.cmd_ext)
        cmd_load = " -u Administrator -p password -e {0} -s '{1}'"\
                      .format(server_IP, query_cmd)
        error_mesg = "No alternate address information found"
        try:
            self.log.info("Run query on travel-sample bucket from outside network")
            output = check_output("{0} {1}".format(loader_path, cmd_load), shell=True, stderr=STDOUT)
            if output:
                self.log.info("Output from n1ql query: {0}".format(output))
                if self.all_alt_addr_set:
                    if "No alternate address information found" in str(output):
                        self.fail("Failed to set alternate address.")
                    elif "Error" in str(output):
                        self.fail("Failed to find query node in port 8091.")
                    else:
                        self.fail("Failed to run query in remote cluster.{0}"\
                                  .format(output))
                else:
                    self.log.info("Error is expected due to alt addre not set yet")
        except CalledProcessError as e:
            if e.output:
                if self.all_alt_addr_set:
                    if "No alternate address information found" in e.output:
                        self.fail("Failed to set alternate address.")
                    else:
                        self.fail("Failed to run query in remote cluster.{0}"\
                                  .format(e.output))
                else:
                    self.log.info("Error is expected due to alt addre not set yet")

    def create_eventing_function(self, server = None, client_os = "linux",
                   create_travel_sample_bucket=False):
        if server is None:
            server_IP = self.master.ip
        else:
            server_IP = server.ip

        self._create_buckets(server, num_buckets=2)
        self._create_travel_sample_bucket(server)

        base_path = "/opt/couchbase/bin/"
        query_cmd = ''
        rest = RestConnection(server)
        try:
            self.log.info("Create event eventingalt from outside network")
            self._create_eventing_function(server)
            self._deploy_function(server)
            self._check_eventing_status(server)
            self._undeploy_eventing_function(server)
            self._delete_eventing_function(server)
        except CalledProcessError as e:
            if e.output:
                if self.all_alt_addr_set:
                    if "No alternate address information found" in e.output:
                        self.fail("Failed to set alternate address.")
                    else:
                        self.fail("Failed to run query in remote cluster.{0}"\
                                  .format(e.output))
                else:
                    self.log.info("Error is expected due to alt addre not set yet")

    def _create_travel_sample_bucket(self, server):
        self.log.info("Create travel-sample bucket")
        create_bucket_cmd = """curl -g -u Administrator:password \
                                   http://{0}:8091/sampleBuckets/install \
                               -d '["travel-sample"]'""".format(server.ip)
        output = check_output("{0}".format(create_bucket_cmd), shell=True,
                                               stderr=STDOUT)
        ready = RestHelper(RestConnection(server)).vbucket_map_ready("travel-sample")
        if output:
            self.log.info("Output from create travel-sample bucket: {0}"
                                                            .format(output))
        self.sleep(25, "time to load and create indexes")

    def _create_buckets(self, server, num_buckets=1):
        if server is None:
            server = self.master
        create_bucket_command = """ curl -g -u Administrator:password \
                      http://{0}:8091/pools/default/buckets \
                      -d ramQuotaMB=256 -d authType=sasl -d replicaNumber=1 """.format(server.ip)
        if num_buckets == 1:
            self.log.info("Create bucket {0} ".format("bucket_1"))
            create_bucket_command += " -d name=bucket_1 "
            output = check_output("{0}".format(create_bucket_command), shell=True,
                                               stderr=STDOUT)
            if output:
                self.log.info("Output from create bucket bucket_1")
        if num_buckets > 1:
            count = 1
            while count <= num_buckets:
                bucket_name = "bucket_{0}".format(count)
                self.log.info("Create bucket {0}".format(bucket_name))
                create_bucket = create_bucket_command
                create_bucket += " -d name={0} ".format(bucket_name)
                print("\ncreate bucket command: ", create_bucket)
                output = check_output("{0}".format(create_bucket), shell=True,
                                                   stderr=STDOUT)
                if output:
                    self.log.info("Output from create bucket {0}".format(bucket_name))
                ready = RestHelper(RestConnection(server)).vbucket_map_ready(bucket_name)
                if not ready:
                    self.fail("Could not create bucket {0}".format(bucket_name))
                count += 1
                self.sleep(5)

    def _create_default_bucket(self, server):
        if server is None:
            server = self.master
        create_bucket_command = """ curl -g -u Administrator:password \
                      http://{0}:8091/pools/default/buckets -d name=default \
                      -d ramQuotaMB=256 -d authType=sasl -d replicaNumber=1 """.format(server.ip)
        self.log.info("Create default bucket ")
        output = check_output("{0}".format(create_bucket_command), shell=True,
                                           stderr=STDOUT)
        if output:
            self.log.info("Output from create bucket default {0}".format(output))

    def _create_eventing_function(self, server):
        eventing_name = "eventingalt"
        function_body = ' {"depcfg":{"buckets":[{"alias":"travelalt","bucket_name":"bucket_2", "access":"rw"}],"metadata_bucket":"bucket_1","source_bucket":"travel-sample", '
        function_body += ' "curl":[]},"settings":{"worker_count":3,"execution_timeout":60, "user_prefix":"eventing","log_level":"INFO","dcp_stream_boundary":"everything", '
        function_body += ' "processing_status":false,"deployment_status":false,"description":"", "deadline_timeout":62}, '
        function_body += '"appname":"{0}", '.format(eventing_name)
        function_body += """ "appcode":"function OnUpdate(doc, meta) {\\n travelalt[meta.id]=doc\\n log('docId', meta.id);\\n}\\nfunction OnDelete(meta) {\\n}", """
        function_body += ' "status":"undeployed","uiState":"inactive"} '
        cmd = "curl -g -u Administrator:password http://{0}:8096/api/v1/functions/{1} -d \'{2}\'"\
                                                  .format(server.ip, eventing_name, function_body)
        output = check_output(cmd, shell=True, stderr=STDOUT)
        """ Correct output from command line
            {
             "code": 0,
             "info": {
             "status": "Stored function: 'eventingalt' in metakv",
             "warnings": null
             }
            }
        """
        if "Stored function: 'eventingalt' " not in str(output):
            self.fail("Fail to create eventing function")

    def _deploy_function(self, server):
        eventing_name = "eventingalt"
        cmd = "curl -u Administrator:password http://{0}:8096/api/v1/functions/{1}/settings"\
                                                            .format(server.ip,eventing_name)
        cmd += """ -d '{"deployment_status":true,"processing_status":true}' """
        output = check_output(cmd, shell=True, stderr=STDOUT)
        self.sleep(60, "wait for function deployed")

    def _check_eventing_status(self, server):
        eventing_name = "eventingalt"
        cmd = "curl GET -u Administrator:password http://{0}:8096/api/v1/functions/{1}/settings"\
                                                                .format(server.ip, eventing_name)
        output = check_output(cmd, shell=True, stderr=STDOUT)
        if '"deployment_status": true' in str(output):
            return True
        else:
            return False

    def _undeploy_eventing_function(self, server):
        eventing_name = "eventingalt"
        cmd = "curl -u Administrator:password http://{0}:8096/api/v1/functions/{1}/settings"\
                                                             .format(server.ip,eventing_name)
        cmd += """ -d '{"deployment_status":false,"processing_status":false}' """
        output = check_output(cmd, shell=True, stderr=STDOUT)
        self.sleep(20, "wait to undeploy function")

    def _delete_eventing_function(self, server):
        cmd = " curl -X DELETE -u Administrator:password http://{0}:8096/api/v1/functions/"\
                                                                           .format(server.ip)
        output = check_output(cmd, shell=True, stderr=STDOUT)
        if 'Function: eventingalt deleting in the background' in str(output):
            return True
        else:
            return False
