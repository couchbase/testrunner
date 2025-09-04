import json
import os
import random
import time

from lib.Cb_constants.CBServer import CbServer
from lib.membase.api.rest_client import RestConnection
from lib.remote.remote_util import RemoteMachineShellConnection
from basetestcase import BaseTestCase
from pytests.security.ntonencryptionBase import ntonencryptionBase
from pytests.security.x509_multiple_CA_util import x509main, Validation


class MultipleCA(BaseTestCase):

    def setUp(self):
        super(MultipleCA, self).setUp()
        self.standard = self.input.param("standard", "pkcs8")
        self.passphrase_type = self.input.param("passphrase_type", "script")
        self.encryption_type = self.input.param("encryption_type", "aes256")
        self.wildcard_dns = self.input.param("wildcard_dns", None)
        self.passphrase_url = self.input.param("rest_url",
                                               "https://testingsomething.free.beeceptor.com/")
        self.slave_host = self.input.param("slave_host_ip", "127.0.0.1")
        self.x509 = x509main(host=self.master, standard=self.standard,
                             encryption_type=self.encryption_type,
                             passphrase_type=self.passphrase_type,
                             wildcard_dns=self.wildcard_dns,
                             passhprase_url=self.passphrase_url,
                             slave_host_ip=self.slave_host)
        for server in self.servers:
            self.x509.delete_inbox_folder_on_server(server=server)
        sample_bucket = self.input.param("sample_bucket", "travel-sample")
        if sample_bucket is not None:
            self.load_sample_bucket(self.master, sample_bucket)
        self.buckets = RestConnection(self.master).get_buckets()
        rest = RestConnection(self.master)
        for bucket in self.buckets:
            rest.change_bucket_props(bucket, replicaNumber=self.num_replicas)
        task = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [])
        self.wait_for_rebalance_to_complete(task)
        self.n2n_encryption_level_multiple_CA = self.input.param("n2n_encryption_level_multiple_CA", None)
        if self.n2n_encryption_level_multiple_CA:
            ntonencryptionBase().setup_nton_cluster([self.master],
                                                    clusterEncryptionLevel=self.n2n_encryption_level_multiple_CA)
            CbServer.use_https = True

    def tearDown(self):
        if self.input.param("n2n_encryption_level_multiple_CA", None):
            ntonencryptionBase().disable_nton_cluster([self.master])
            CbServer.use_https = False
        self.x509 = x509main(host=self.master)
        self.x509.teardown_certs(servers=self.servers)
        super(MultipleCA, self).tearDown()

    def auth(self, client_certs=None, api=None, servers=None):
        """
        Performs client-cert and Basic authentication via:
            1. Making a rest call to servers at api
            2. Making a sdk connection and doing some creates
        :client_certs: (list) - list of tuples. Each tuple being client cert,
                                client private ket
        :api: - full url to make a rest call
        :servers: a list of servers to make sdk connection/rest connection against
        """
        if client_certs is None:
            client_certs = list()
            client_certs.append(self.x509.get_client_cert(int_ca_name="i1_r1"))
            client_certs.append(self.x509.get_client_cert(int_ca_name="iclient1_r1"))
            client_certs.append(self.x509.get_client_cert(int_ca_name="iclient1_clientroot"))
        if servers is None:
            servers = [self.servers[0]]
        for client_cert_path_tuple in client_certs:
            slave_host = self.x509.slave_host
            if slave_host.ip != '127.0.0.1':
                client_certs_to_copy = [cert for cert in client_cert_path_tuple]
                client_certs_to_copy.append(x509main.ALL_CAs_PATH + x509main.ALL_CAs_PEM_NAME)
                for client_cert_path in client_certs_to_copy:

                    # Get the directory path excluding the file
                    dir_path = os.path.dirname(client_cert_path)

                    # Check if the directory path exists
                    if not os.path.exists(dir_path):
                        # Create the entire directory path
                        os.makedirs(dir_path, exist_ok=True)
                        self.log.info(f"Created directory path: {dir_path}")
                    else:
                        self.log.info(f"Directory path already exists: {dir_path}")

                    self.x509.copy_file_from_host_to_slave(client_cert_path, client_cert_path)

            for server in servers:
                if api is None:
                    api_ep = "https://" + server.ip + ":18091/pools/default/"
                else:
                    api_ep = api
                # 1) using client auth
                self.x509_validation = Validation(server=server,
                                                  cacert=x509main.ALL_CAs_PATH + x509main.ALL_CAs_PEM_NAME,
                                                  client_cert_path_tuple=client_cert_path_tuple)
                # 1a) rest api
                status, content, response = self.x509_validation.urllib_request(api=api_ep)
                if not status:
                    self.fail("Could not login using client cert auth {0}".format(content))
                # 1b) sdk
                client = self.x509_validation.sdk_connection()
                self.x509_validation.creates_sdk(client)

                # 2) using basic auth
                self.x509_validation = Validation(server=server,
                                                  cacert=x509main.ALL_CAs_PATH + x509main.ALL_CAs_PEM_NAME,
                                                  client_cert_path_tuple=None)
                # 2a) rest api
                status, content, response = self.x509_validation.urllib_request(api=api_ep)
                if not status:
                    self.fail("Could not login using basic auth {0}".format(content))

    def wait_for_rebalance_to_complete(self, task):
        status = task.result()
        if not status:
            self.fail("rebalance/failover failed")

    def wait_for_failover_or_assert(self, expected_failover_count, timeout=180):
        time_start = time.time()
        time_max_end = time_start + timeout
        actual_failover_count = 0
        while time.time() < time_max_end:
            actual_failover_count = self.get_failover_count()
            if actual_failover_count == expected_failover_count:
                break
            time.sleep(20)
        time_end = time.time()
        self.assertTrue(actual_failover_count == expected_failover_count,
                        "{0} nodes failed over, expected : {1}".
                        format(actual_failover_count, expected_failover_count))
        self.log.info("{0} nodes failed over as expected in {1} seconds".
                      format(actual_failover_count, time_end - time_start))

    def get_failover_count(self):
        rest = RestConnection(self.master)
        cluster_status = rest.cluster_status()
        failover_count = 0
        # check for inactiveFailed
        for node in cluster_status['nodes']:
            if node['clusterMembership'] == "inactiveFailed":
                failover_count += 1
        return failover_count

    def load_sample_bucket(self, server, bucket_name="travel-sample"):
        shell = RemoteMachineShellConnection(server)
        shell.execute_command("""curl -v -u Administrator:password \
                             -X POST http://localhost:8091/sampleBuckets/install \
                          -d '["{0}"]'""".format(bucket_name))
        shell.disconnect()
        self.sleep(60)

    def test_basic_rebalance(self):
        """
        1. Init node cluster. Generate x509 certs
        2. Rebalance-in all the remaining nodes
        3. Client cert auth
        """
        self.x509.generate_multiple_x509_certs(servers=self.servers)
        self.log.info("Manifest #########\n {0}".format(json.dumps(self.x509.manifest, indent=4)))
        for server in self.servers:
            _ = self.x509.upload_root_certs(server)
        self.x509.upload_node_certs(servers=self.servers)
        self.x509.delete_unused_out_of_the_box_CAs(server=self.master)
        self.x509.upload_client_cert_settings(server=self.servers[0])
        https_val = CbServer.use_https  # so that add_node uses https
        CbServer.use_https = True
        task = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                            self.servers[self.nodes_init:], [])

        self.wait_for_rebalance_to_complete(task)
        CbServer.use_https = https_val
        self.log.info("Checking authentication ...")
        self.auth(servers=self.servers)

        content = self.x509.get_trusted_CAs()
        self.log.info("Trusted CAs: {0}".format(content))
        self.log.info("Active Root CAs names {0}".format(self.x509.root_ca_names))

    def test_rebalance_out_and_add_back(self):
        """
        1. Init node cluster. Generate x509 certs
        2. Upload root certs only to master node
        3. Rebalance-out master node
        4. Client cert auth
        5. Failover any node and recover
        6. Rebalance-in back the step3's node
        """
        self.x509.generate_multiple_x509_certs(servers=self.servers)
        self.log.info("Manifest #########\n {0}".format(json.dumps(self.x509.manifest, indent=4)))
        self.x509.upload_root_certs(self.master)
        self.x509.upload_node_certs(servers=self.servers[:self.nodes_init])
        self.x509.delete_unused_out_of_the_box_CAs(server=self.master)
        self.x509.upload_client_cert_settings(server=self.servers[0])
        self.master = self.servers[:self.nodes_init][1]
        https_val = CbServer.use_https  # so that add_node uses https
        CbServer.use_https = True
        task = self.cluster.async_rebalance(self.servers[1:self.nodes_init],
                                            [], [self.servers[0]])
        self.wait_for_rebalance_to_complete(task)
        CbServer.use_https = https_val
        self.log.info("Checking authentication ...")
        self.auth(servers=self.servers[1:self.nodes_init])
        failover_nodes = random.sample(self.servers[1:self.nodes_init], 1)
        _ = self.cluster.async_failover(self.servers[1:self.nodes_init], failover_nodes,
                                        graceful=True)
        self.wait_for_failover_or_assert(1)
        rest = RestConnection(self.master)
        for node in failover_nodes:
            rest.set_recovery_type("ns_1@" + node.ip, recoveryType="delta")
        https_val = CbServer.use_https  # so that add_node uses https
        CbServer.use_https = True
        task = self.cluster.async_rebalance(self.servers[1:self.nodes_init], [], [])
        CbServer.use_https = https_val
        self.wait_for_rebalance_to_complete(task)
        self.x509.load_trusted_CAs(server=self.servers[0])
        self.x509.reload_node_certificates(servers=[self.servers[0]])
        https_val = CbServer.use_https  # so that add_node uses https
        CbServer.use_https = True
        task = self.cluster.async_rebalance(self.servers[1:self.nodes_init], [self.servers[0]], [])
        CbServer.use_https = https_val
        self.wait_for_rebalance_to_complete(task)
        self.auth(servers=self.servers[:self.nodes_init])

    def test_failover_and_recovery(self):
        """
        1. Init node cluster. Generate x509 certs
        2. multiple node failover (graceful and hard) and recover(delta and full)
        3. Client cert auth
        """
        self.x509.generate_multiple_x509_certs(servers=self.servers[:self.nodes_init])
        self.x509.upload_root_certs(self.master)
        self.x509.upload_node_certs(servers=self.servers[:self.nodes_init])
        self.x509.delete_unused_out_of_the_box_CAs(server=self.master)
        self.x509.upload_client_cert_settings(server=self.master)
        for graceful in [True, False]:
            for recovery_type in ["delta", "full"]:
                failover_nodes = random.sample(self.servers[1:self.nodes_init], 2)
                failover_count = 0
                for node in failover_nodes:
                    _ = self.cluster.async_failover(self.servers[:self.nodes_init], [node],
                                                    graceful=graceful)
                    failover_count = failover_count + 1
                    self.wait_for_failover_or_assert(failover_count)
                rest = RestConnection(self.master)
                for node in failover_nodes:
                    rest.set_recovery_type("ns_1@" + node.ip, recoveryType=recovery_type)
                https_val = CbServer.use_https  # so that add_node uses https
                CbServer.use_https = True
                task = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [])
                CbServer.use_https = https_val
                self.wait_for_rebalance_to_complete(task)
        self.auth(servers=self.servers[:self.nodes_init])

    def test_failover_and_rebalance_out(self):
        """
        1. Init node cluster. Generate x509 certs
        2. single node failover (graceful and hard) and rebalance-out
        3. Client cert auth
        """
        self.x509.generate_multiple_x509_certs(servers=self.servers[:self.nodes_init])
        self.x509.upload_root_certs(self.master)
        self.x509.upload_node_certs(servers=self.servers[:self.nodes_init])
        self.x509.delete_unused_out_of_the_box_CAs(server=self.master)
        self.x509.upload_client_cert_settings(server=self.master)
        out_nodes = list()
        nodes_in_cluster = self.servers[:self.nodes_init]
        for graceful in [True, False]:
            failover_nodes = random.sample(nodes_in_cluster[1:], 1)
            _ = self.cluster.async_failover(nodes_in_cluster, failover_nodes,
                                            graceful=graceful)
            self.wait_for_failover_or_assert(1)
            https_val = CbServer.use_https  # so that add_node uses https
            CbServer.use_https = True
            task = self.cluster.async_rebalance(nodes_in_cluster, [], failover_nodes)
            self.wait_for_rebalance_to_complete(task)
            CbServer.use_https = https_val
            for node in failover_nodes:
                out_nodes.append(node)
            nodes_in_cluster = [node for node in self.servers[:self.nodes_init] if node not in out_nodes]
        self.auth(servers=nodes_in_cluster)

    def test_rotate_certificates(self):
        """
        1. Init node cluster. Generate x509 certs
        2. Rotate all certs
        3. Rebalance-in nodes
        4. Client cert auth
        """
        self.x509.generate_multiple_x509_certs(servers=self.servers)
        self.log.info("Manifest before rotating certs #########\n {0}".
                      format(json.dumps(self.x509.manifest, indent=4)))
        for server in self.servers[:self.nodes_init]:
            _ = self.x509.upload_root_certs(server)
        self.x509.upload_node_certs(servers=self.servers[:self.nodes_init])
        self.x509.delete_unused_out_of_the_box_CAs(server=self.master)
        self.x509.upload_client_cert_settings(server=self.servers[0])
        self.log.info("Checking authentication ...")
        self.auth(servers=self.servers[:self.nodes_init])
        self.x509.rotate_certs(self.servers, "all")
        self.log.info("Manifest after rotating certs #########\n {0}".
                      format(json.dumps(self.x509.manifest, indent=4)))

        https_val = CbServer.use_https  # so that add_node uses https
        CbServer.use_https = True
        task = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                            self.servers[self.nodes_init:], [])
        self.wait_for_rebalance_to_complete(task)
        CbServer.use_https = https_val
        self.log.info("Checking authentication ...")
        self.auth(servers=self.servers)
        content = self.x509.get_trusted_CAs()
        self.log.info("Trusted CAs: {0}".format(content))
        self.log.info("Active Root CAs names {0}".format(self.x509.root_ca_names))

    def test_cluster_works_fine_after_deleting_CA_folder(self):
        """
        1. Init node cluster. Generate x509 certs
        2. Upload root certs from any random node of the cluster
        3. Delete CA folder from that node
        4. Verify that cluster continues to operate fine by checking
            a) Failover & delta recovery of that node
            b) Failover & rebalance-out of that node
            c) Client authentication & sdk writes
        """
        self.x509.generate_multiple_x509_certs(servers=self.servers[:self.nodes_init])
        random_nodes = random.sample(self.servers[1:self.nodes_init], 1)
        self.log.info("Uploading root certs from {0}".format(random_nodes[0]))
        self.x509.upload_root_certs(random_nodes[0])
        self.x509.upload_node_certs(servers=self.servers[:self.nodes_init])
        self.x509.delete_unused_out_of_the_box_CAs(server=self.master)
        self.x509.upload_client_cert_settings(server=self.master)
        shell = RemoteMachineShellConnection(random_nodes[0])
        shell.remove_directory(self.x509.install_path + x509main.CHAINFILEPATH +
                               "/" + x509main.TRUSTEDCAPATH)
        shell.disconnect()

        failover_nodes = random_nodes
        nodes_in_cluster = self.servers[:self.nodes_init]
        for operation in ["recovery", "out"]:
            shell = RemoteMachineShellConnection(failover_nodes[0])
            shell.stop_server()
            self.cluster.async_failover(self.servers[:self.nodes_init],
                                        failover_nodes,
                                        graceful=False)
            self.wait_for_failover_or_assert(1)
            if operation == "out":
                https_val = CbServer.use_https  # so that add_node uses https
                CbServer.use_https = True
                rest = RestConnection(self.master)
                otp_nodes = []
                ejected_nodes = []
                for node in nodes_in_cluster:
                    otp_nodes.append('ns_1@'+node.ip)
                for node in failover_nodes:
                    ejected_nodes.append('ns_1@' + node.ip)
                status = rest.rebalance(otpNodes=otp_nodes, ejectedNodes=ejected_nodes)
                if not status:
                    shell.start_server(failover_nodes[0])
                    self.fail("rebalance/failover failed")
                CbServer.use_https = https_val
                nodes_in_cluster = nodes_in_cluster.remove(failover_nodes[0])
            shell.start_server(failover_nodes[0])
            if operation == "recovery":
                rest = RestConnection(self.master)
                for node in failover_nodes:
                    rest.set_recovery_type("ns_1@" + node.ip, recoveryType="delta")
                https_val = CbServer.use_https  # so that add_node uses https
                CbServer.use_https = True
                task = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [])
                self.wait_for_rebalance_to_complete(task)
                CbServer.use_https = https_val
        self.auth(servers=nodes_in_cluster)
        self.sleep(300, "Wait for sometime after rebalance operation") # Ref : MB-68356

    def test_CA_upload_from_all_nodes(self):
        """
        1. Init node cluster
        2. Upload CAs: [ca1] from inbox/CA folder of first node
        3. Upload CAs: [ca2] from inbox/CA folder of second node
        4 Upload CAs: [ca3, ca4] from inbox/CA folder of third node
        5.Verify that the net trusted CAs for the cluster is now: [ca1, ca2, ca3, ca4]
        """
        self.x509.generate_multiple_x509_certs(servers=self.servers[:self.nodes_init])
        self.x509.upload_root_certs(server=self.master, root_ca_names=[self.x509.root_ca_names[0]])
        self.x509.upload_root_certs(server=self.servers[:self.nodes_init][1],
                                    root_ca_names=[self.x509.root_ca_names[1]])
        self.x509.upload_root_certs(server=self.servers[:self.nodes_init][2],
                                    root_ca_names=self.x509.root_ca_names[2:])
        self.x509.upload_node_certs(servers=self.servers[:self.nodes_init])
        self.x509.delete_unused_out_of_the_box_CAs(server=self.master)
        self.x509.upload_client_cert_settings(server=self.master)
        self.auth(servers=self.servers[:self.nodes_init])
        content = self.x509.get_trusted_CAs()
        self.log.info("Trusted CAs: {0}".format(content))
        expected_root_ca_names = self.x509.root_ca_names
        actual_root_ca_names = list()
        for ca_dict in content:
            subject = ca_dict["subject"]
            root_ca_name = subject.split("CN=")[1]
            if "Couchbase Server" not in root_ca_name:
                actual_root_ca_names.append(root_ca_name)
        if set(actual_root_ca_names) != set(expected_root_ca_names):
            self.fail("Expected {0} Actual {1}".format(expected_root_ca_names,
                                                       actual_root_ca_names))

    def test_restart_node_with_encrypted_pkeys(self):
        """
        1. Init node cluster, with encrypted node pkeys
        2. Restart a node
        3. Failover and delta recover that node
        4. Restart the node again and rebalance-out this time
        5. Repeat steps 2 to 5 until you are left with master node
        """
        self.x509.generate_multiple_x509_certs(servers=self.servers[:self.nodes_init])
        self.x509.upload_root_certs(self.master)
        self.x509.upload_node_certs(servers=self.servers[:self.nodes_init])
        rest = RestConnection(self.master)
        nodes_in_cluster = [node for node in self.servers[:self.nodes_init]]
        for node in self.servers[1:self.nodes_init]:
            shell = RemoteMachineShellConnection(node)
            shell.restart_couchbase()
            shell.disconnect()
            self.sleep(120, "Wait after restart")
            self.cluster.async_failover(nodes_in_cluster,
                                        [node],
                                        graceful=False)
            self.wait_for_failover_or_assert(1)
            rest.set_recovery_type("ns_1@" + node.ip, recoveryType="delta")
            https_val = CbServer.use_https  # so that add_node uses https
            CbServer.use_https = True
            self.sleep(120, "Wait after restart")
            task = self.cluster.async_rebalance(nodes_in_cluster, [], [])
            CbServer.use_https = https_val
            self.wait_for_rebalance_to_complete(task)
            shell = RemoteMachineShellConnection(node)
            shell.restart_couchbase()
            shell.disconnect()
            https_val = CbServer.use_https  # so that add_node uses https
            CbServer.use_https = True
            self.sleep(120, "Wait after restart")
            task = self.cluster.async_rebalance(nodes_in_cluster,
                                                [], [node])
            self.wait_for_rebalance_to_complete(task)
            CbServer.use_https = https_val
            nodes_in_cluster.remove(node)

    def test_teardown_with_n2n_encryption(self):
        """
        Verify that regenerate, deletion of trusted CAs work
        with n2n encryption turned on
        """
        CbServer.use_https = True
        for level in ["strict", "control", "all"]:
            self.x509 = x509main(host=self.master, standard=self.standard,
                                 encryption_type=self.encryption_type,
                                 passphrase_type=self.passphrase_type,
                                 wildcard_dns=self.wildcard_dns)
            self.x509.generate_multiple_x509_certs(servers=self.servers[:self.nodes_init])
            self.x509.upload_root_certs(self.master)
            self.x509.upload_node_certs(servers=self.servers[:self.nodes_init])
            ntonencryptionBase().setup_nton_cluster([self.master], clusterEncryptionLevel=level)
            self.x509.teardown_certs(servers=self.servers)
            ntonencryptionBase().disable_nton_cluster([self.master])