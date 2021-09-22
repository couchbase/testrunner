import json

from pytests.basetestcase import BaseTestCase
from pytests.security.x509_multiple_CA_util import x509main, Validation


class MultipleCA(BaseTestCase):

    def setUp(self):
        super(MultipleCA, self).setUp()
        self.x509 = x509main(host=self.master)
        for server in self.servers:
            self.x509.delete_inbox_folder_on_server(server=server)
        self.basic_url = "https://" + self.servers[0].ip + ":18091/pools/default/"

    def tearDown(self):
        self.x509 = x509main(host=self.master)
        self.x509.teardown_certs(servers=self.servers)
        super(MultipleCA, self).tearDown()

    def test_auth(self):
        self.x509.generate_multiple_x509_certs(servers=self.servers)
        self.log.info("Manifest #########\n {0}".format(json.dumps(x509main.manifest, indent=4)))
        for server in self.servers:
            _ = self.x509.upload_root_certs(server)
        self.x509.upload_node_certs(servers=self.servers)
        self.x509.upload_client_cert_settings(self.servers[0])

        client_certs = list()
        client_certs.append(self.x509.get_client_cert(int_ca_name="i1_r1"))
        client_certs.append(self.x509.get_client_cert(int_ca_name="iclient1_r1"))
        client_certs.append(self.x509.get_client_cert(int_ca_name="iclient1_clientroot"))
        for client_cert_path_tuple in client_certs:
            # 1) using client auth
            self.x509_validation = Validation(server=self.servers[0],
                                              cacert=x509main.ALL_CAs_PATH + x509main.ALL_CAs_PEM_NAME,
                                              client_cert_path_tuple=client_cert_path_tuple)
            # 1a) rest api
            status, content, response = self.x509_validation.urllib_request(api=self.basic_url)
            if not status:
                self.fail("Could not login using client cert auth {0}".format(content))
            # 1b) sdk
            client = self.x509_validation.sdk_connection()
            self.x509_validation.creates_sdk(client)

            # 2) using basic auth
            self.x509_validation = Validation(server=self.servers[0],
                                              cacert=x509main.ALL_CAs_PATH + x509main.ALL_CAs_PEM_NAME,
                                              client_cert_path_tuple=None)
            # 2a) rest api
            status, content, response = self.x509_validation.urllib_request(api=self.basic_url)
            if not status:
                self.fail("Could not login using basic auth {0}".format(content))

    def test_basic_rebalance(self):
        """
        1. Init node cluster. Generate x509 certs
        2. Rebalance-in all the remaining nodes
        """
        self.x509.generate_multiple_x509_certs(servers=self.servers)
        self.log.info("Manifest #########\n {0}".format(json.dumps(x509main.manifest, indent=4)))
        for server in self.servers:
            _ = self.x509.upload_root_certs(server)
        self.x509.upload_node_certs(servers=self.servers)
        status = self.cluster.rebalance(self.servers[:self.nodes_init],
                                        self.servers[self.nodes_init:], [])
        if not status:
            self.fail("Rebalance-in failed")

        content = self.x509.get_trusted_CAs()
        self.log.info("Trusted CAs: {0}".format(content))
        self.log.info("Active Root CAs names {0}".format(self.x509.root_ca_names))

    def test_rotate_certificates(self):
        self.x509.generate_multiple_x509_certs(servers=self.servers)
        self.log.info("Manifest before rotating certs #########\n {0}".
                      format(json.dumps(x509main.manifest, indent=4)))
        for server in self.servers:
            _ = self.x509.upload_root_certs(server)
        self.x509.upload_node_certs(servers=self.servers)
        self.x509.rotate_certs(self.servers, "all")
        self.log.info("Manifest after rotating certs #########\n {0}".
                      format(json.dumps(x509main.manifest, indent=4)))
        status = self.cluster.rebalance(self.servers[:self.nodes_init],
                                        self.servers[self.nodes_init:], [])
        if not status:
            self.fail("Rebalance-in failed")
        content = self.x509.get_trusted_CAs()
        self.log.info("Trusted CAs: {0}".format(content))
        self.log.info("Active Root CAs names {0}".format(self.x509.root_ca_names))
