import json
import random
import time

from lib.membase.api.rest_client import RestConnection
from lib.remote.remote_util import RemoteMachineShellConnection
from pytests.basetestcase import BaseTestCase
from pytests.security.x509_multiple_CA_util import x509main, Validation


class MultipleCA(BaseTestCase):

    def setUp(self):
        super(MultipleCA, self).setUp()
        self.standard = self.input.param("standard", "pkcs8")
        self.passphrase_type = self.input.param("passphrase_type", "script")
        self.encryption_type = self.input.param("encryption_type", "aes256")
        self.x509 = x509main(host=self.master, standard=self.standard,
                             encryption_type=self.encryption_type,
                             passphrase_type=self.passphrase_type)
        for server in self.servers:
            self.x509.delete_inbox_folder_on_server(server=server)
        sample_bucket = self.input.param("sample_bucket", "travel-sample")
        if sample_bucket is not None:
            self.load_sample_bucket(self.master, sample_bucket)

    def tearDown(self):
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
        self.log.info("Manifest #########\n {0}".format(json.dumps(x509main.manifest, indent=4)))
        for server in self.servers:
            _ = self.x509.upload_root_certs(server)
        self.x509.upload_node_certs(servers=self.servers)
        self.x509.upload_client_cert_settings(server=self.servers[0])
        task = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                            self.servers[self.nodes_init:], [])
        self.wait_for_rebalance_to_complete(task)
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
        self.log.info("Manifest #########\n {0}".format(json.dumps(x509main.manifest, indent=4)))
        self.x509.upload_root_certs(self.master)
        self.x509.upload_node_certs(servers=self.servers[:self.nodes_init])
        self.x509.upload_client_cert_settings(server=self.servers[0])
        self.master = self.servers[:self.nodes_init][1]
        task = self.cluster.async_rebalance(self.servers[1:self.nodes_init],
                                            [], [self.servers[0]])
        self.wait_for_rebalance_to_complete(task)
        self.log.info("Checking authentication ...")
        self.auth(servers=self.servers[1:self.nodes_init])
        failover_nodes = random.sample(self.servers[1:self.nodes_init], 1)
        _ = self.cluster.async_failover(self.servers[1:self.nodes_init], failover_nodes,
                                        graceful=True)
        self.wait_for_failover_or_assert(1)
        rest = RestConnection(self.master)
        for node in failover_nodes:
            rest.set_recovery_type("ns_1@" + node.ip, recoveryType="delta")
        task = self.cluster.async_rebalance(self.servers[1:self.nodes_init], [], [])
        self.wait_for_rebalance_to_complete(task)
        self.x509.load_trusted_CAs(server=self.servers[0])
        self.x509.reload_node_certificates(servers=[self.servers[0]])
        task = self.cluster.async_rebalance(self.servers[1:self.nodes_init], [self.servers[0]], [])
        self.wait_for_rebalance_to_complete(task)
        self.auth(servers=self.servers)

    def test_failover_and_recovery(self):
        """
        1. Init node cluster. Generate x509 certs
        2. multiple node failover (graceful and hard) and recover(delta and full)
        3. Client cert auth
        """
        self.x509.generate_multiple_x509_certs(servers=self.servers[:self.nodes_init])
        self.x509.upload_root_certs(self.master)
        self.x509.upload_node_certs(servers=self.servers[:self.nodes_init])
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
                task = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [])
                self.wait_for_rebalance_to_complete(task)
        self.auth(servers=self.servers)

    def test_failover_and_rebalance_out(self):
        """
        1. Init node cluster. Generate x509 certs
        2. single node failover (graceful and hard) and rebalance-out
        3. Client cert auth
        """
        self.x509.generate_multiple_x509_certs(servers=self.servers[:self.nodes_init])
        self.x509.upload_root_certs(self.master)
        self.x509.upload_node_certs(servers=self.servers[:self.nodes_init])
        self.x509.upload_client_cert_settings(server=self.master)
        for graceful in [True, False]:
            failover_nodes = random.sample(self.servers[1:self.nodes_init], 1)
            _ = self.cluster.async_failover(self.servers[:self.nodes_init], failover_nodes,
                                            graceful=graceful)
            self.wait_for_failover_or_assert(1)
            task = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], failover_nodes)
            self.wait_for_rebalance_to_complete(task)
        nodes_in_cluster = [node for node in self.servers[:self.nodes_init] if node not in failover_nodes]
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
                      format(json.dumps(x509main.manifest, indent=4)))
        for server in self.servers:
            _ = self.x509.upload_root_certs(server)
        self.x509.upload_node_certs(servers=self.servers)
        self.x509.upload_client_cert_settings(server=self.servers[0])
        self.log.info("Checking authentication ...")
        self.auth()
        self.x509.rotate_certs(self.servers, "all")
        self.log.info("Manifest after rotating certs #########\n {0}".
                      format(json.dumps(x509main.manifest, indent=4)))
        task = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                            self.servers[self.nodes_init:], [])
        self.wait_for_rebalance_to_complete(task)
        self.log.info("Checking authentication ...")
        self.auth(servers=self.servers)
        content = self.x509.get_trusted_CAs()
        self.log.info("Trusted CAs: {0}".format(content))
        self.log.info("Active Root CAs names {0}".format(self.x509.root_ca_names))
