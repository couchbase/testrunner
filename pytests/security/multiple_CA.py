import json

from pytests.basetestcase import BaseTestCase
from pytests.security.x509_multiple_CA_util import x509main


class MultipleCA(BaseTestCase):

    def setUp(self):
        super(MultipleCA, self).setUp()
        self.x509 = x509main(host=self.master)
        for server in self.servers:
            self.x509.delete_inbox_folder_on_server(server=server)

    def tearDown(self):
        self.x509 = x509main(host=self.master)
        self.x509.teardown_certs(servers=self.servers)
        super(MultipleCA, self).tearDown()

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



