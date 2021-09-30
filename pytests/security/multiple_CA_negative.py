import copy
import json

from pytests.basetestcase import BaseTestCase
from pytests.security.x509_multiple_CA_util import x509main, Validation


class MultipleCANegative(BaseTestCase):

    def setUp(self):
        super(MultipleCANegative, self).setUp()
        self.x509 = x509main(host=self.master)
        for server in self.servers:
            self.x509.delete_inbox_folder_on_server(server=server)
        self.basic_url = "https://" + self.servers[0].ip + ":18091/pools/default/"

    def tearDown(self):
        self.x509 = x509main(host=self.master)
        self.x509.teardown_certs(servers=self.servers)
        super(MultipleCANegative, self).tearDown()

    def test_untrusted_client_cert_fails(self):
        """
        Verify that a client cert signed by an untrusted root
        CA is not authenticated
        """
        self.x509.generate_multiple_x509_certs(servers=self.servers)
        self.log.info("Manifest #########\n {0}".format(json.dumps(x509main.manifest, indent=4)))
        cas = copy.deepcopy(x509main.root_ca_names)
        cas.remove("clientroot")  # make "clientroot" ca untrusted
        for server in self.servers[:self.nodes_init]:
            _ = self.x509.upload_root_certs(server=server, root_ca_names=cas)
        self.x509.upload_node_certs(servers=self.servers[:self.nodes_init])
        self.x509.upload_client_cert_settings(server=self.servers[0])
        client_cert_path_tuple = self.x509.get_client_cert(int_ca_name="iclient1_clientroot")
        self.x509_validation = Validation(server=self.servers[0],
                                          cacert=None,
                                          client_cert_path_tuple=client_cert_path_tuple)
        try:
            status, content, response = self.x509_validation.urllib_request(api=self.basic_url)
        except Exception as e:
            self.log.info("Rest api connection with untrusted client cert "
                          "didn't work as expected {0}".format(e))
        else:
            self.fail("Rest api connection with untrusted client cert worked")
        try:
            client = self.x509_validation.sdk_connection()
            self.x509_validation.creates_sdk(client)
        except Exception as e:
            self.log.info("SDk connection with untrusted client cert didn't work "
                          "as expected {0}".format(e))
        else:
            self.fail("SDK connection with untrusted client cert worked")