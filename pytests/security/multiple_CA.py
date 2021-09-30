import json

from pytests.basetestcase import BaseTestCase
from pytests.security.x509_multiple_CA_util import x509main, Validation


class MultipleCA(BaseTestCase):

    def setUp(self):
        super(MultipleCA, self).setUp()
        self.passphrase_type = self.input.param("passphrase_type", "script")
        self.encryption_type = self.input.param("encryption_type", "aes")
        self.x509 = x509main(host=self.master, encryption_type=self.encryption_type,
                             passphrase_type=self.passphrase_type)
        for server in self.servers:
            self.x509.delete_inbox_folder_on_server(server=server)
        self.basic_url = "https://" + self.servers[0].ip + ":18091/pools/default/"

    def tearDown(self):
        self.x509 = x509main(host=self.master)
        self.x509.teardown_certs(servers=self.servers)
        super(MultipleCA, self).tearDown()

    def auth(self, client_certs=None, api=None):
        """
        :client_certs: (list) - list of tuples. Each tuple being client cert,
                                client private ket
        :api: - full url to make a rest call
        """
        if api is None:
            api = self.basic_url
        if client_certs is None:
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
            status, content, response = self.x509_validation.urllib_request(api=api)
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
            status, content, response = self.x509_validation.urllib_request(api=api)
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
        self.x509.upload_client_cert_settings(server=self.servers[0])
        status = self.cluster.rebalance(self.servers[:self.nodes_init],
                                        self.servers[self.nodes_init:], [])
        if not status:
            self.fail("Rebalance-in failed")
        self.log.info("Checking authentication ...")
        self.auth()

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
        self.x509.upload_client_cert_settings(server=self.servers[0])
        self.log.info("Checking authentication ...")
        self.auth()
        self.x509.rotate_certs(self.servers, "all")
        self.log.info("Manifest after rotating certs #########\n {0}".
                      format(json.dumps(x509main.manifest, indent=4)))
        status = self.cluster.rebalance(self.servers[:self.nodes_init],
                                        self.servers[self.nodes_init:], [])
        if not status:
            self.fail("Rebalance-in failed")
        self.log.info("Checking authentication ...")
        self.auth()
        content = self.x509.get_trusted_CAs()
        self.log.info("Trusted CAs: {0}".format(content))
        self.log.info("Active Root CAs names {0}".format(self.x509.root_ca_names))
