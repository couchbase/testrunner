import copy
import json
import urllib.parse

from lib.membase.api.rest_client import RestConnection
from basetestcase import BaseTestCase
from pytests.security.x509_multiple_CA_util import x509main, Validation


class MultipleCANegative(BaseTestCase):

    def setUp(self):
        super(MultipleCANegative, self).setUp()
        self.standard = self.input.param("standard", "pkcs8")
        self.passphrase_type = self.input.param("passphrase_type", "script")
        self.encryption_type = self.input.param("encryption_type", "aes256")
        self.x509 = x509main(host=self.master, standard=self.standard,
                             encryption_type=self.encryption_type,
                             passphrase_type=self.passphrase_type)
        for server in self.servers:
            self.x509.delete_inbox_folder_on_server(server=server)
        self.basic_url = "https://" + self.servers[0].ip + ":18091/pools/default/"

    def tearDown(self):
        self.x509 = x509main(host=self.master)
        self.x509.teardown_certs(servers=self.servers)
        super(MultipleCANegative, self).tearDown()

    def wait_for_rebalance_to_complete(self, task):
        status = task.result()
        if not status:
            self.fail("rebalance/failover failed")

    def test_untrusted_client_cert_fails(self):
        """
        Verify that a client cert signed by an untrusted root
        CA is not authenticated
        """
        self.x509.generate_multiple_x509_certs(servers=self.servers)
        self.log.info("Manifest #########\n {0}".format(json.dumps(self.x509.manifest, indent=4)))
        cas = copy.deepcopy(self.x509.root_ca_names)
        cas.remove("clientroot")  # make "clientroot" ca untrusted
        for server in self.servers[:self.nodes_init]:
            _ = self.x509.upload_root_certs(server=server, root_ca_names=cas)
        self.x509.upload_node_certs(servers=self.servers[:self.nodes_init])
        self.x509.upload_client_cert_settings(server=self.servers[0])
        self.x509.delete_unused_out_of_the_box_CAs(server=self.master)
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

    def test_deletion_of_active_root_cert_fails(self):
        """
        Verify that deletion of trusted CA fails if one of the nodes
        has its cert signed by it
        """
        self.x509.generate_multiple_x509_certs(servers=self.servers)
        self.x509.upload_root_certs(server=self.master)
        self.x509.upload_node_certs(servers=self.servers[:self.nodes_init])
        rest = RestConnection(self.master)
        content = self.x509.get_trusted_CAs(self.master)
        for ca_dict in content:
            if len(ca_dict["nodes"]) > 0:
                ca_id = ca_dict["id"]
                status, content, response = rest.delete_trusted_CA(ca_id)
                if not status:
                    self.log.info("Deletion of active CA failed as expected {0}".format(content))
                else:
                    self.fail("Deletion of active CA worked")

    def test_incorrect_plain_passphrase_fails(self):
        """
        Verify that an incorrect passphrase given during reload of an encrypted
        node pkey fails
        """
        self.x509.generate_multiple_x509_certs(servers=self.servers)
        self.x509.upload_root_certs(server=self.master)
        for server in self.servers[:self.nodes_init]:
            self.x509.copy_node_cert(server=server)
        params = dict()
        params["privateKeyPassphrase"] = dict()
        params["privateKeyPassphrase"]["type"] = "plain"
        params["privateKeyPassphrase"]["password"] = \
            self.x509.private_key_passphrase_map[str(self.master.ip)] + "incorrect"
        params = json.dumps(params)
        rest = RestConnection(self.master)
        status, content = rest.reload_certificate(params=params)
        if not status:
            self.log.info("Incorrect plain passphrase failed as expected {0}".format(content))
        else:
            self.fail("incorrect plain passphrase worked")

    def test_untrusted_ca_of_incoming_node_fails(self):
        """
        Verify that node addition fails (https aka tls handshake)
        if the cluster's trusted CAs does not contain the incoming node's trusted CA
        """
        self.x509.generate_multiple_x509_certs(servers=self.servers)
        self.x509.upload_root_certs(server=self.master, root_ca_names=["r1"])
        self.x509.upload_root_certs(server=self.servers[2], root_ca_names=["r1", "r2"])
        self.x509.upload_node_certs(servers=[self.master, self.servers[2]])
        rest = RestConnection(self.master)
        params = urllib.parse.urlencode({'hostname': "{0}".format(self.servers[2].ip),
                                         'user': "Administrator",
                                         'password': "password",
                                         'services': "kv"})
        api = rest.get_https_base_url() + 'controller/addNode'
        status, content, header = rest._http_request(api, 'POST', params)
        if not status:
            self.log.info("Rebalance failed as expected {0}".format(content))
        else:
            self.fail("Rebalance did not fail")

    def test_untrusted_ca_of_cluster_for_incoming_node_fails(self):
        """
        Verify that node addition fails (https aka handshake)
        if the incoming node does not have the trusted CA of the cluster
        """
        self.x509.generate_multiple_x509_certs(servers=self.servers)
        self.x509.upload_root_certs(server=self.master, root_ca_names=["r1"])
        self.x509.upload_root_certs(server=self.servers[2], root_ca_names=["r2"])
        self.x509.upload_node_certs(servers=[self.master, self.servers[2]])
        rest = RestConnection(self.master)
        params = urllib.parse.urlencode({'hostname': "{0}".format(self.servers[2].ip),
                                         'user': "Administrator",
                                         'password': "password",
                                         'services': "kv"})
        api = rest.get_https_base_url() + 'controller/addNode'
        status, content, header = rest._http_request(api, 'POST', params)
        if not status:
            self.log.info("Rebalance failed as expected {0}".format(content))
        else:
            self.fail("Rebalance did not fail")

    def test_addition_of_node_after_rebalance_out_fails(self):
        """
        verify that forgetting to upload CAs on rebalanced out node will make
        the re-addition of node back to the cluster to fail
        """
        self.x509.generate_multiple_x509_certs(servers=self.servers)
        self.x509.upload_root_certs(self.master)
        self.x509.upload_node_certs(servers=self.servers[:self.nodes_init])
        self.x509.delete_unused_out_of_the_box_CAs(server=self.master)
        out_node = self.servers[:self.nodes_init][-1]

        task = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                            [], [out_node])
        self.wait_for_rebalance_to_complete(task)

        rest = RestConnection(self.master)
        params = urllib.parse.urlencode({'hostname': "{0}".format(out_node.ip),
                                         'user': "Administrator",
                                         'password': "password",
                                         'services': "kv"})
        api = rest.get_https_base_url() + 'controller/addNode'
        status, content, header = rest._http_request(api, 'POST', params)
        if not status:
            self.log.info("Rebalance failed as expected {0}".format(content))
        else:
            self.fail("Rebalance did not fail")

    def test_incorrect_script_path_fails(self):
        """
        Verify that if the script (from which passphrase for encrypted pkey is extracted)
        is not located in /opt/couchbase/var/lib/couchbasae/scripts, it fails
        """
        self.x509.generate_multiple_x509_certs(servers=self.servers)
        self.x509.upload_root_certs(self.master)
        x509main.SCRIPTSPATH = "inbox"
        try:
            self.x509.upload_node_certs(servers=self.servers[:self.nodes_init])
        except Exception as e:
            self.log.info("Node reload failed as expected {0}".format(e))
        else:
            self.fail("Node reload worked")

    def test_wrong_rest_endpoint(self):
        """
        Verify that it fails when a wrong rest endpoint to extract pkey's
        passphrase if given
        """
        self.x509.generate_multiple_x509_certs(servers=self.servers)
        self.x509.upload_root_certs(self.master)
        self.x509.passphrase_url = "https://somewrongurl.com/"
        try:
            self.x509.upload_node_certs([self.master])
        except Exception as e:
            self.log.info("Node reload failed as expected {0}".format(e))
        else:
            self.fail("Node reload worked")
