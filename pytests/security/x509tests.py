from basetestcase import BaseTestCase
from security.x509main import x509main
# from newupgradebasetest import NewUpgradeBaseTest
from membase.api.rest_client import RestConnection, RestHelper
import subprocess
import json
import socket
from couchbase.bucket import Bucket
from threading import Thread, Event
from remote.remote_util import RemoteMachineShellConnection
from security.auditmain import audit
from security.rbac_base import RbacBase
import os, subprocess
import copy
from couchbase.cluster import Cluster
from ep_mc_bin_client import MemcachedClient
from security.ntonencryptionBase import ntonencryptionBase

from lib.Cb_constants.CBServer import CbServer


class x509tests(BaseTestCase):

    def setUp(self):
        super(x509tests, self).setUp()
        self._reset_original()
        self.ip_address = self.getLocalIPAddress()
        self.ip_address = '172.16.1.174'
        self.root_ca_path = x509main.CACERTFILEPATH + x509main.CACERTFILE
        SSLtype = self.input.param("SSLtype", "go")
        encryption_type = self.input.param('encryption_type', "")
        key_length = self.input.param("key_length", 1024)
        # Input parameters for state, path, delimeters and prefixes
        self.client_cert_state = self.input.param("client_cert_state", "disable")
        self.paths = self.input.param('paths', "subject.cn:san.dnsname:san.uri").split(":")
        self.prefixs = self.input.param('prefixs', 'www.cb-:us.:www.').split(":")
        self.delimeters = self.input.param('delimeter', '.:.:.') .split(":")
        self.setup_once = self.input.param("setup_once", False)
        self.upload_json_mode = self.input.param("upload_json_mode", 'rest')
        self.sdk_version = self.input.param('sdk_version', 'pre-vulcan')

        self.dns = self.input.param('dns', None)
        self.uri = self.input.param('uri', None)
        self.enable_nton_local = self.input.param('enable_nton_local',False)
        self.local_clusterEncryption = self.input.param('local_clusterEncryption','control')
        self.wildcard_dns = self.input.param('wildcard_dns',None)
        self.verify_ssl = None
        if SSLtype == "openssl":
            self.verify_ssl = False  # if false it does not use any cert file
        else:
            self.verify_ssl = True  # if true it uses x509Main.CERT_FILE
        copy_servers = copy.deepcopy(self.servers)

        if self.ntonencrypt == 'disable' and self.enable_nton_local:
            ntonencryptionBase().setup_nton_cluster(self.servers,'enable',self.local_clusterEncryption)
        # Generate cert and pass on the client ip for cert generation
        if (self.dns is not None) or (self.uri is not None):
            x509main(self.master)._generate_cert(copy_servers, type=SSLtype, encryption=encryption_type, key_length=key_length, client_ip=self.ip_address, alt_names='non_default', dns=self.dns, uri=self.uri,wildcard_dns=self.wildcard_dns)
        else:
            x509main(self.master)._generate_cert(copy_servers, type=SSLtype, encryption=encryption_type, key_length=key_length, client_ip=self.ip_address,wildcard_dns=self.wildcard_dns)
        self.log.info(" Path is {0} - Prefixs - {1} -- Delimeters - {2}".format(self.paths, self.prefixs, self.delimeters))

        if (self.setup_once):
            x509main(self.master).setup_master(self.client_cert_state, self.paths, self.prefixs, self.delimeters, self.upload_json_mode)
            x509main().setup_cluster_nodes_ssl(self.servers)

        # reset the severs to ipv6 if there were ipv6
        '''
        for server in self.servers:
            if server.ip.count(':') > 0:
                    # raw ipv6? enclose in square brackets
                    server.ip = '[' + server.ip + ']'
        '''

        self.log.info (" list of server {0}".format(self.servers))
        self.log.info (" list of server {0}".format(copy_servers))

        enable_audit = self.input.param('audit', None)
        if enable_audit:
            Audit = audit(host=self.master)
            currentState = Audit.getAuditStatus()
            self.log.info ("Current status of audit on ip - {0} is {1}".format(self.master.ip, currentState))
            if not currentState:
                self.log.info ("Enabling Audit ")
                Audit.setAuditEnable('true')
                self.sleep(30)
        self.protocol = "http"
        self.disable_ssl_certificate_validation = False
        self.rest_port = CbServer.port
        self.n1ql_port = CbServer.n1ql_port
        self.cbas_port = CbServer.cbas_port
        self.fts_port = CbServer.fts_port
        if CbServer.use_https:
            self.protocol = "https"
            self.disable_ssl_certificate_validation = True
            self.rest_port = CbServer.ssl_port
            self.n1ql_port = CbServer.ssl_n1ql_port
            self.cbas_port = CbServer.ssl_cbas_port
            self.fts_port = CbServer.ssl_fts_port

    def tearDown(self):
        self.log.info ("Into Teardown")
        self._reset_original()
        shell = RemoteMachineShellConnection(x509main.SLAVE_HOST)
        shell.execute_command("rm " + x509main.CACERTFILEPATH)
        shell.disconnect()
        super(x509tests, self).tearDown()

    def _reset_original(self):
        self.log.info ("Reverting to original state - regenerating certificate and removing inbox folder")
        tmp_path = "/tmp/abcd.pem"
        for servers in self.servers:
            cli_command = "ssl-manage"
            remote_client = RemoteMachineShellConnection(servers)
            options = "--regenerate-cert={0}".format(tmp_path)
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options,
                                                                cluster_host=servers.cluster_ip, user="Administrator",
                                                                password="password")
            x509main(servers)._delete_inbox_folder()

    def checkConfig(self, eventID, host, expectedResults):
        Audit = audit(eventID=eventID, host=host)
        currentState = Audit.getAuditStatus()
        self.log.info ("Current status of audit on ip - {0} is {1}".format(self.master.ip, currentState))
        if not currentState:
            self.log.info ("Enabling Audit ")
            Audit.setAuditEnable('true')
            self.sleep(30)
        fieldVerification, valueVerification = Audit.validateEvents(expectedResults)
        self.assertTrue(fieldVerification, "One of the fields is not matching")
        self.assertTrue(valueVerification, "Values for one of the fields is not matching")

    def getLocalIPAddress(self):
        '''
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('couchbase.com', 0))
        return s.getsockname()[0]
        '''
        status, ipAddress = subprocess.getstatusoutput("ifconfig en0 | grep 'inet addr:' | cut -d: -f2 |awk '{print $1}'")
        if '1' not in ipAddress:
            status, ipAddress = subprocess.getstatusoutput("ifconfig eth0 | grep  -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | awk '{print $2}'")
        return ipAddress

    def createBulkDocuments(self, client):
        start_num = 0
        end_num = 10000
        key1 = 'demo_key'
        value1 = {
          "name":"demo_value",
          "lastname":'lastname',
          "areapin":'',
          "preference":'veg',
          "type":''
        }
        for x in range (start_num, end_num):
            value = value1.copy()
            key = 'demo_key'
            key = key + str(x)
            for key1 in value:
                if value[key1] == 'type' and x % 2 == 0:
                    value['type'] = 'odd'
                else:
                    value['type'] = 'even'
                value[key1] = value[key1] + str(x)
            value['id'] = str(x)
            result = client.upsert(key, value)

    def check_rebalance_complete(self, rest):
        progress = None
        count = 0
        while (progress == 'running' or count < 10):
            progress = rest._rebalance_progress_status()
            self.sleep(10)
            count = count + 1
        if progress == 'none':
            return True
        else:
            return False

    def _extract_certs(self, raw_content):
        certs = ""
        for ca_dict in raw_content:
            certs += ca_dict["pem"]
        return certs

    def _sdk_connection(self, root_ca_path=x509main.CACERTFILEPATH + x509main.CACERTFILE, bucket='default', host_ip=None):
        self.sleep(10)
        result = False
        self.add_built_in_server_user([{'id': bucket, 'name': bucket, 'password': 'password'}], \
                                      [{'id': bucket, 'name': bucket, 'roles': 'admin'}], self.master)
        self.add_built_in_server_user([{'id': 'cbadminbucket', 'name': 'cbadminbucket', 'password': 'password'}], \
                                      [{'id': 'cbadminbucket', 'name': 'cbadminbucket', 'roles': 'admin'}], self.master)
        self.sleep(10)
        if self.sdk_version == 'pre-vulcan':
            connection_string = 'couchbases://' + host_ip + '/' + bucket + '?certpath=' + root_ca_path
            self.log.info("Connection string is -{0}".format(connection_string))
            try:
                cb = Bucket(connection_string, password='password')
                if cb is not None:
                    result = True
                    return result, cb
            except Exception as ex:
                self.log.info("Expection is  -{0}".format(ex))
        elif self.sdk_version == 'vulcan':
            key_file = x509main.CACERTFILEPATH + self.ip_address + ".key"
            chain_file = x509main.CACERTFILEPATH + "/long_chain" + self.ip_address + ".pem"
            connection_string = 'couchbases://' + host_ip + '/?ipv6=allow&certpath=' + chain_file + "&keypath=" + key_file
            self.log.info("Connection string is -{0}".format(connection_string))
            try:
                cluster = Cluster(connection_string);
                cb = cluster.open_bucket(bucket)
                if cb is not None:
                    result = True
                    self.log.info('SDK connection created successfully')
                    return result, cb
            except Exception as ex:
                self.log.info("Expection is  -{0}".format(ex))
        return result

    def test_bucket_select_audit(self):
        # security.x509tests.x509tests.test_bucket_select_audit
        eventID = 20492
        mc = MemcachedClient(self.master.ip, 11210)
        mc.sasl_auth_plain(self.master.rest_username, self.master.rest_password)
        mc.bucket_select('default')

        expectedResults = {"bucket":"default","description":"The specified bucket was selected","id":20492,"name":"select bucket" \
                           ,"peername":"127.0.0.1:46539","real_userid":{"domain":"memcached","user":"@ns_server"},"sockname":"127.0.0.1:11209"}
        Audit = audit(eventID=eventID, host=self.master)
        actualEvent = Audit.returnEvent(eventID)
        Audit.validateData(actualEvent, expectedResults)

    def test_basic_ssl_test(self):
        x509main(self.master).setup_master()
        status = x509main(self.master)._validate_ssl_login(verify=self.verify_ssl)
        self.assertEqual(status, 200, "Not able to login via SSL code")

    def test_error_without_node_chain_certificates(self):
        x509main(self.master)._upload_cluster_ca_certificate("Administrator", 'password')
        status, content = x509main(self.master)._reload_node_certificate(self.master)
        content = str(content)
        self.assertEqual(status, False, "Issue with status with node certificate are missing")
        self.assertTrue('Unable to read certificate chain file' in str(content), "Incorrect message from the system")

    def test_error_without_chain_cert(self):
        x509main(self.master)._upload_cluster_ca_certificate("Administrator", 'password')
        x509main(self.master)._setup_node_certificates(chain_cert=False)
        status, content = x509main(self.master)._reload_node_certificate(self.master)
        content = str(content)
        self.assertEqual(status, False, "Issue with status with node certificate are missing")
        self.assertTrue('Unable to read certificate chain file' in str(content) , "Incorrect message from the system")

    def test_error_without_node_key(self):
        x509main(self.master)._upload_cluster_ca_certificate("Administrator", 'password')
        x509main(self.master)._setup_node_certificates(node_key=False)
        status, content = x509main(self.master)._reload_node_certificate(self.master)
        content = str(content)
        self.assertEqual(status, False, "Issue with status with node key is missing")
        self.assertTrue('Unable to read private key file' in content, "Incorrect message from the system")

    def test_add_node_without_cert(self):
        rest = RestConnection(self.master)
        servs_inout = self.servers[1]
        x509main(self.master).setup_master()
        try:
            rest.add_node('Administrator', 'password', servs_inout.ip)
        except Exception as ex:
            ex = str(ex)
            # expected_result  = "Error adding node: " + servs_inout.ip + " to the cluster:" + self.master.ip + " - [\"Prepare join failed. Error applying node certificate. Unable to read certificate chain file\"]"
            expected_result = "Error adding node: " + servs_inout.ip + " to the cluster:" + self.master.ip
            self.assertTrue(expected_result in ex, "Incorrect Error message in exception")
            expected_result = "The certificate is issued by unknown CA or some of the intermediate certificates are missing "
            self.assertTrue(expected_result in ex, "Incorrect Error message in exception")

    def test_add_node_with_cert(self):
        servs_inout = self.servers[1:]
        rest = RestConnection(self.master)
        for node in servs_inout:
            x509main(node).setup_master()

        known_nodes = ['ns_1@' + self.master.ip]
        for server in servs_inout:
            rest.add_node('Administrator', 'password', server.ip)
            known_nodes.append('ns_1@' + server.ip)
        rest.rebalance(known_nodes)
        self.assertTrue(self.check_rebalance_complete(rest), "Issue with rebalance")

        for server in self.servers:
            if self.client_cert_state == "mandatory":
                status = x509main(server)._validate_mandatory_state_ssl_login()
                self.assertEqual(status, 200, "Not able to login via SSL code")
            else:
                status = x509main(server)._validate_ssl_login(verify=self.verify_ssl)
                self.assertEqual(status, 200, "Not able to login via SSL code")

    def test_add_remove_add_back_node_with_cert(self, rebalance=None):
        rebalance = self.input.param('rebalance', True)
        rest = RestConnection(self.master)
        servs_inout = self.servers[1:]
        serv_out = 'ns_1@' + servs_inout[0].ip
        known_nodes = ['ns_1@' + self.master.ip]
        x509main(self.master).setup_master()
        for node in servs_inout:
            x509main(node).setup_master()
        for server in servs_inout:
            rest.add_node('Administrator', 'password', server.ip)
            known_nodes.append('ns_1@' + server.ip)
        rest.rebalance(known_nodes)
        self.assertTrue(self.check_rebalance_complete(rest), "Issue with rebalance")
        for server in servs_inout:
            if self.client_cert_state == "mandatory":
                status = x509main(server)._validate_mandatory_state_ssl_login()
                self.assertEqual(status, 200, "Not able to login via SSL code")
            else:
                status = x509main(server)._validate_ssl_login(verify=self.verify_ssl)
                self.assertEqual(status, 200, "Not able to login via SSL code")
        rest.fail_over(serv_out, graceful=False)
        if (rebalance):
            rest.rebalance(known_nodes, [serv_out])
            known_nodes.remove(serv_out)
            self.assertTrue(self.check_rebalance_complete(rest), "Issue with rebalance")
            x509main(servs_inout[0]).non_local_CA_upload(server=servs_inout[0], allow=True)
            x509main(servs_inout[0])._upload_cluster_ca_certificate(self.master.rest_username,
                                                                    self.master.rest_password)
            x509main(servs_inout[0])._reload_node_certificate(host=servs_inout[0])
            rest.add_node('Administrator', 'password', serv_out)
            known_nodes.append(serv_out)
        else:
            rest.add_back_node(serv_out)
        rest.rebalance(known_nodes)
        self.assertTrue(self.check_rebalance_complete(rest), "Issue with rebalance")
        for server in servs_inout:
            if self.client_cert_state == "mandatory":
                status = x509main(server)._validate_mandatory_state_ssl_login()
                self.assertEqual(status, 200, "Not able to login via SSL code")
            else:
                status = x509main(server)._validate_ssl_login(verify=self.verify_ssl)
                self.assertEqual(status, 200, "Not able to login via SSL code")

    def test_add_remove_graceful_add_back_node_with_cert(self, recovery_type=None):
        recovery_type = self.input.param('recovery_type')
        rest = RestConnection(self.master)
        known_nodes = ['ns_1@' + self.master.ip]
        progress = None
        count = 0
        servs_inout = self.servers[1:]
        serv_out = 'ns_1@' + servs_inout[1].ip

        rest.create_bucket(bucket='default', ramQuotaMB=256)

        x509main(self.master).setup_master()
        for node in servs_inout:
            x509main(node).setup_master()
        for server in servs_inout:
            rest.add_node('Administrator', 'password', server.ip)
            known_nodes.append('ns_1@' + server.ip)

        rest.rebalance(known_nodes)
        self.assertTrue(self.check_rebalance_complete(rest), "Issue with rebalance")

        for server in servs_inout:
            if self.client_cert_state == "mandatory":
                status = x509main(server)._validate_mandatory_state_ssl_login()
                self.assertEqual(status, 200, "Not able to login via SSL code")
            else:
                status = x509main(server)._validate_ssl_login(verify=self.verify_ssl)
                self.assertEqual(status, 200, "Not able to login via SSL code")

        rest.fail_over(serv_out, graceful=True)
        self.assertTrue(self.check_rebalance_complete(rest), "Issue with rebalance")
        rest.set_recovery_type(serv_out, recovery_type)
        rest.add_back_node(serv_out)
        rest.rebalance(known_nodes)
        self.assertTrue(self.check_rebalance_complete(rest), "Issue with rebalance")

        for server in servs_inout:
            if self.client_cert_state == "mandatory":
                status = x509main(server)._validate_mandatory_state_ssl_login()
                self.assertEqual(status, 200, "Not able to login via SSL code")
            else:
                status = x509main(server)._validate_ssl_login(verify=self.verify_ssl)
                self.assertEqual(status, 200, "Not able to login via SSL code")

    def test_add_remove_autofailover(self):
        rest = RestConnection(self.master)
        serv_out = self.servers[3]
        shell = RemoteMachineShellConnection(serv_out)
        known_nodes = ['ns_1@' + self.master.ip]

        rest.create_bucket(bucket='default', ramQuotaMB=256)
        rest.update_autofailover_settings(True, 30)

        x509main(self.master).setup_master()
        for node in self.servers[1:4]:
            x509main(node).setup_master()
        for server in self.servers[1:4]:
            rest.add_node('Administrator', 'password', server.ip)
            known_nodes.append('ns_1@' + server.ip)

        rest.rebalance(known_nodes)
        self.assertTrue(self.check_rebalance_complete(rest), "Issue with rebalance")

        shell.stop_server()
        self.sleep(60)
        shell.start_server()
        shell.disconnect()
        self.sleep(30)
        for server in self.servers[1:4]:
            if self.client_cert_state == "mandatory":
                status = x509main(server)._validate_mandatory_state_ssl_login()
                self.assertEqual(status, 200, "Not able to login via SSL code")
            else:
                status = x509main(server)._validate_ssl_login(verify=self.verify_ssl)
                self.assertEqual(status, 200, "Not able to login via SSL code")

    def test_add_node_with_cert_non_master(self):
        rest = RestConnection(self.master)
        for node in self.servers[:3]:
            x509main(node).setup_master()

        servs_inout = self.servers[1]
        rest.add_node('Administrator', 'password', servs_inout.ip)
        known_nodes = ['ns_1@' + self.master.ip, 'ns_1@' + servs_inout.ip]
        rest.rebalance(known_nodes)
        self.assertTrue(self.check_rebalance_complete(rest), "Issue with rebalance")

        rest = RestConnection(self.servers[1])
        servs_inout = self.servers[2]
        rest.add_node('Administrator', 'password', servs_inout.ip)
        known_nodes = ['ns_1@' + self.master.ip, 'ns_1@' + servs_inout.ip, 'ns_1@' + self.servers[1].ip]
        rest.rebalance(known_nodes)
        self.assertTrue(self.check_rebalance_complete(rest), "Issue with rebalance")

        for server in self.servers[:3]:
            if self.client_cert_state == "mandatory":
                status = x509main(server)._validate_mandatory_state_ssl_login()
                self.assertEqual(status, 200, "Not able to login via SSL code")
            else:
                status = x509main(server)._validate_ssl_login(verify=self.verify_ssl)
                self.assertEqual(status, 200, "Not able to login via SSL code")

    # simple xdcr with ca cert
    def test_basic_xdcr_with_cert(self):

        cluster1 = self.servers[0:2]
        cluster2 = self.servers[2:4]
        remote_cluster_name = 'sslcluster'
        restCluster1 = RestConnection(cluster1[0])
        restCluster2 = RestConnection(cluster2[0])

        try:
            # Setup cluster1
            x509main(cluster1[0]).setup_master()
            x509main(cluster1[1])._setup_node_certificates(reload_cert=False)

            restCluster1.create_bucket(bucket='default', ramQuotaMB=256)
            restCluster1.remove_all_replications()
            restCluster1.remove_all_remote_clusters()

            # Setup cluster2
            x509main(cluster2[0]).setup_master()
            x509main(cluster2[1])._setup_node_certificates(reload_cert=False)
            restCluster2.create_bucket(bucket='default', ramQuotaMB=256)

            self.sleep(20)
            test = x509main.CACERTFILEPATH + x509main.CACERTFILE
            data = open(test, 'rb').read()
            restCluster1.add_remote_cluster(cluster2[0].ip, cluster2[0].port, 'Administrator', 'password', remote_cluster_name, certificate=data)
            self.sleep(20)
            replication_id = restCluster1.start_replication('continuous', 'default', remote_cluster_name)
            if replication_id is not None:
                self.assertTrue(True, "Replication was not created successfully")
        except Exception as ex:
            self.log.info("Exception is -{0}".format(ex))
        finally:
            restCluster2.delete_bucket()
            restCluster1.remove_all_replications()
            restCluster1.remove_all_remote_clusters()

    # simple xdcr with ca cert updated at source and destination, re-generate new certs
    def test_basic_xdcr_with_cert_regenerate(self):

        cluster1 = self.servers[0:2]
        cluster2 = self.servers[2:4]
        remote_cluster_name = 'sslcluster'
        restCluster1 = RestConnection(cluster1[0])
        restCluster2 = RestConnection(cluster2[0])

        try:
            # Setup cluster1
            x509main(cluster1[0]).setup_master()
            x509main(cluster1[1])._setup_node_certificates(reload_cert=False)
            restCluster1.remove_all_replications()
            restCluster1.remove_all_remote_clusters()
            restCluster1.create_bucket(bucket='default', ramQuotaMB=256)

            # Setup cluster2
            x509main(cluster2[0]).setup_master()
            x509main(cluster2[1])._setup_node_certificates(reload_cert=False)
            restCluster2.create_bucket(bucket='default', ramQuotaMB=256)

            test = x509main.CACERTFILEPATH + x509main.CACERTFILE
            data = open(test, 'rb').read()
            restCluster1.add_remote_cluster(cluster2[0].ip, cluster2[0].port, 'Administrator', 'password', remote_cluster_name, certificate=data)
            self.sleep(20)
            replication_id = restCluster1.start_replication('continuous', 'default', remote_cluster_name)

            # restCluster1.set_xdcr_param('default','default','pauseRequested',True)

            x509main(self.master)._delete_inbox_folder()
            x509main(self.master)._generate_cert(self.servers, type='openssl', root_cn="CB\ Authority", client_ip=self.ip_address)
            self.log.info ("Setting up the first cluster for new certificate")

            x509main(cluster1[0]).setup_master()
            x509main(cluster1[1])._setup_node_certificates(reload_cert=False)
            self.log.info ("Setting up the second cluster for new certificate")
            x509main(cluster2[0]).setup_master()
            x509main(cluster2[1])._setup_node_certificates(reload_cert=False)

            status = restCluster1.is_replication_paused('default', 'default')
            if not status:
                restCluster1.set_xdcr_param('default', 'default', 'pauseRequested', False)

            restCluster1.set_xdcr_param('default', 'default', 'pauseRequested', True)
            status = restCluster1.is_replication_paused('default', 'default')
            self.assertTrue(status, "Replication has not started after certificate upgrade")
        finally:
            restCluster2.delete_bucket()
            restCluster1.remove_all_replications()
            restCluster1.remove_all_remote_clusters()

    # source ca and destination self_signed
    def test_xdcr_destination_self_signed_cert(self):

        cluster1 = self.servers[0:2]
        cluster2 = self.servers[2:4]
        remote_cluster_name = 'sslcluster'
        restCluster1 = RestConnection(cluster1[0])
        restCluster2 = RestConnection(cluster2[0])
        try:
            # Setup cluster1
            for server in cluster1:
                x509main(server).setup_master()
            x509main(cluster1[1])._setup_node_certificates()

            restCluster1.add_node('Administrator', 'password', cluster1[1].ip)
            known_nodes = ['ns_1@' + cluster1[0].ip, 'ns_1@' + cluster1[1].ip]
            restCluster1.rebalance(known_nodes)
            self.assertTrue(self.check_rebalance_complete(restCluster1), "Issue with rebalance")
            restCluster1.create_bucket(bucket='default', ramQuotaMB=256)
            restCluster1.remove_all_replications()
            restCluster1.remove_all_remote_clusters()

            # Setup cluster2
            for server in cluster2:
                x509main(server).setup_master()
            x509main(cluster2[1])._setup_node_certificates()

            restCluster2.add_node('Administrator', 'password', cluster2[1].ip)
            known_nodes = ['ns_1@' + cluster2[0].ip, 'ns_1@' + cluster2[1].ip]
            restCluster2.rebalance(known_nodes)
            self.assertTrue(self.check_rebalance_complete(restCluster2), "Issue with rebalance")
            restCluster2.create_bucket(bucket='default', ramQuotaMB=256)

            self.sleep(20)
            test = x509main.CACERTFILEPATH + x509main.CACERTFILE
            data = open(test, 'rb').read()
            restCluster1.add_remote_cluster(cluster2[0].ip, cluster2[0].port,
                                            'Administrator', 'password',
                                            remote_cluster_name, certificate=data)
            self.sleep(20)
            replication_id = restCluster1.start_replication('continuous', 'default', remote_cluster_name)
            if replication_id is not None:
                self.assertTrue(True, "Cannot create a replication")

        finally:
            known_nodes = ['ns_1@' + cluster2[0].ip, 'ns_1@' + cluster2[1].ip]
            restCluster2.rebalance(known_nodes, ['ns_1@' + cluster2[1].ip])
            self.assertTrue(self.check_rebalance_complete(restCluster2), "Issue with rebalance")
            restCluster2.delete_bucket()

    def test_xdcr_remote_ref_creation(self):
        cluster1 = self.servers[0:2]
        cluster2 = self.servers[2:4]
        remote_cluster_name = 'sslcluster'
        restCluster1 = RestConnection(cluster1[0])
        restCluster2 = RestConnection(cluster2[0])
        user = self.input.param("username", "Administrator")
        password = self.input.param("password", "password")

        try:
            # Setup cluster1
            x509main(cluster1[0]).setup_master()
            x509main(cluster1[1])._setup_node_certificates(reload_cert=False)

            restCluster1.create_bucket(bucket='default', ramQuotaMB=256)
            restCluster1.remove_all_replications()
            restCluster1.remove_all_remote_clusters()

            # Setup cluster2
            x509main(cluster2[0]).setup_master()
            x509main(cluster2[1])._setup_node_certificates(reload_cert=False)

            restCluster2.create_bucket(bucket='default', ramQuotaMB=256)

            test = x509main.CACERTFILEPATH + x509main.CACERTFILE
            data = open(test, 'rb').read()

            # " -u {0}:{1}".format(user, password) + \
            cmd = "curl -k -v -X POST -u Administrator:password" \
                  " --cacert " + self.root_ca_path + \
                  " --cert-type PEM --cert " + x509main().CLIENT_CERT_PEM + \
                  " --key-type PEM --key " + x509main().CLIENT_CERT_KEY + \
                  " -d name=" + remote_cluster_name + \
                  " -d hostname=" + cluster2[0].ip + ":" + self.rest_port +\
                  " -d username=" + user + \
                  " -d password=" + password + \
                  " -d demandEncryption=1" \
                  " --data-urlencode \"certificate={0}\"".format(data) + \
                  " {0}://Administrator:password@{1}:{2}/pools/default/remoteClusters"\
                      .format(self.protocol, self.master.ip, self.rest_port)
            self.log.info("Command is {0}".format(cmd))
            shell = RemoteMachineShellConnection(x509main.SLAVE_HOST)
            output = shell.execute_command(cmd)
            shell.disconnect()
            self.sleep(10)
            '''
            data  =  open(test, 'rb').read()
            restCluster1.add_remote_cluster(cluster2[0].ip,cluster2[0].port,'Administrator','password',remote_cluster_name,certificate=data)
            '''
            replication_id = restCluster1.start_replication('continuous', 'default', remote_cluster_name)
            if replication_id is not None:
                self.assertTrue(True, "Replication was not created successfully")
        finally:
            restCluster2.delete_bucket()

    def test_basic_ssl_test_invalid_cert(self):
        x509main(self.master).setup_master()
        if self.client_cert_state == "mandatory":
            status = x509main(self.master)._validate_mandatory_state_ssl_login()
            self.assertEqual(status, 200, "Not able to login via SSL code")
        else:
            status = x509main(self.master)._validate_ssl_login(verify=self.verify_ssl)
            self.assertEqual(status, 200, "Not able to login via SSL code")

    # test sdk certs on a single node
    def test_sdk(self):
        rest = RestConnection(self.master)
        x509main(self.master).setup_master()
        rest.create_bucket(bucket='default', ramQuotaMB=256)
        result = self._sdk_connection(host_ip=self.master.ip)
        self.assertTrue(result, "Cannot create a security connection with server")

    # test with sdk cluster using ca certs
    def test_sdk_cluster(self):
        rest = RestConnection(self.master)
        x509main(self.master).setup_master()
        for node in self.servers[1:]:
            x509main(node).setup_master()
        rest.create_bucket(bucket='default', ramQuotaMB=256)

        servers_in = self.servers[1:]
        self.cluster.rebalance(self.servers, servers_in, [])

        for server in self.servers:
            result = self._sdk_connection(host_ip=server.ip)
            self.assertTrue(result, "Cannot create a security connection with server")

    def test_sdk_existing_cluster(self):
        servers_in = self.servers[1:]
        self.cluster.rebalance(self.servers, servers_in, [])

        rest = RestConnection(self.master)
        x509main(self.master).setup_master()
        x509main().setup_cluster_nodes_ssl(self.servers, reload_cert=True)
        rest.create_bucket(bucket='default', ramQuotaMB=256)

        for server in self.servers:
            result = self._sdk_connection(host_ip=server.ip)
            self.assertTrue(result, "Cannot create a security connection with server")

    # Incorrect root cert
    def test_sdk_cluster_incorrect_cert(self):
        rest = RestConnection(self.master)
        x509main(self.master).setup_master()
        for node in self.servers[1:]:
            x509main(node).setup_master()
        x509main().setup_cluster_nodes_ssl(self.servers)
        rest.create_bucket(bucket='default', ramQuotaMB=256)

        servers_in = self.servers[1:]
        self.cluster.rebalance(self.servers, servers_in, [])

        root_incorrect_ca_path = x509main.CACERTFILEPATH + x509main.INCORRECT_ROOT_CERT
        for server in self.servers:
            result = self._sdk_connection(host_ip=server.ip, root_ca_path=root_incorrect_ca_path)
            self.assertFalse(result, "Can create a security connection with incorrect root cert")

    # Changing from root to self signed certificates
    def test_sdk_change_ca_self_signed(self):
        rest = RestConnection(self.master)
        temp_file_name = x509main.CACERTFILEPATH + '/orig_cert.pem'
        x509main(self.master).setup_master()
        x509main().setup_cluster_nodes_ssl(self.servers)
        rest.create_bucket(bucket='default', ramQuotaMB=256)
        result = self._sdk_connection(host_ip=self.master.ip)
        self.assertTrue(result, "Cannot create a security connection with server")
        rest.regenerate_cluster_certificate()
        raw_content = rest.get_trusted_CAs()
        temp_cert = self._extract_certs(raw_content)
        temp_file = open(temp_file_name, 'w')
        temp_file.write(temp_cert)
        temp_file.close()

        result = self._sdk_connection(root_ca_path=temp_file_name, host_ip=self.master.ip)
        self.assertTrue(result, "Cannot create a security connection with server")

    # Changing from one root crt to another root crt when an existing connections exists
    def test_root_crt_rotate_existing_cluster(self):
        rest = RestConnection(self.master)
        x509main(self.master).setup_master()
        x509main().setup_cluster_nodes_ssl(self.servers, reload_cert=True)

        rest.create_bucket(bucket='default', ramQuotaMB=256)

        result, cb = self._sdk_connection(host_ip=self.master.ip)
        create_docs = Thread(name='create_docs', target=self.createBulkDocuments, args=(cb,))
        create_docs.start()

        x509main(self.master)._delete_inbox_folder()
        x509main(self.master)._generate_cert(self.servers, root_cn="CB\ Authority", type='openssl', client_ip=self.ip_address)
        x509main(self.master).setup_master()
        x509main().setup_cluster_nodes_ssl(self.servers, reload_cert=True)

        create_docs.join()

        result, cb = self._sdk_connection(host_ip=self.master.ip)
        self.assertTrue(result, "Cannot create a security connection with server")

    # Changing from one root crt to another root crt when an existing connections exists - cluster
    def test_root_crt_rotate_cluster(self):
        rest = RestConnection(self.master)
        x509main(self.master).setup_master()
        for node in self.servers[1:]:
            x509main(node).setup_master()
        x509main().setup_cluster_nodes_ssl(self.servers)
        rest.create_bucket(bucket='default', ramQuotaMB=256)
        self.sleep(30)
        servers_in = self.servers[1:]
        self.cluster.rebalance(self.servers, servers_in, [])

        for server in self.servers:
            result = self._sdk_connection(host_ip=server.ip)
            self.assertTrue(result, "Can create a ssl connection with correct certificate")

        result, cb = self._sdk_connection(host_ip=self.master.ip)
        create_docs = Thread(name='create_docs', target=self.createBulkDocuments, args=(cb,))
        create_docs.start()

        x509main(self.master)._delete_inbox_folder()
        x509main(self.master)._generate_cert(self.servers, root_cn="CB\ Authority", type='openssl', client_ip=self.ip_address)
        x509main(self.master).setup_master()
        x509main().setup_cluster_nodes_ssl(self.servers, reload_cert=True)

        create_docs.join()

        for server in self.servers:
            result = self._sdk_connection(host_ip=server.ip)
            self.assertTrue(result, "Can create a ssl connection with correct certificate")

    def test_root_crt_rotate_cluster_n2n(self):
        update_level = self.input.param('update_level','all')
        #ntonencryptionBase().change_cluster_encryption_cli(self.servers, 'control')
        #ntonencryptionBase().ntonencryption_cli(self.servers, 'disable')
        rest = RestConnection(self.master)
        x509main(self.master).setup_master()
        for node in self.servers[1:]:
            x509main(node).setup_master()
        x509main().setup_cluster_nodes_ssl(self.servers)
        rest.create_bucket(bucket='default', ramQuotaMB=256)
        self.sleep(30)
        servers_in = self.servers[1:]
        self.cluster.rebalance(self.servers, servers_in, [])

        for server in self.servers:
            result = self._sdk_connection(host_ip=server.ip)
            self.assertTrue(result, "Can create a ssl connection with correct certificate")

        result, cb = self._sdk_connection(host_ip=self.master.ip)
        create_docs = Thread(name='create_docs', target=self.createBulkDocuments, args=(cb,))
        create_docs.start()

        x509main(self.master)._delete_inbox_folder()
        x509main(self.master)._generate_cert(self.servers, root_cn="CB\ Authority", type='openssl', client_ip=self.ip_address)
        x509main(self.master).setup_master()
        x509main().setup_cluster_nodes_ssl(self.servers, reload_cert=True)
        create_docs.join()
        ntonencryptionBase().ntonencryption_cli(self.servers, 'enable')
        ntonencryptionBase().change_cluster_encryption_cli(self.servers, update_level)

        for server in self.servers:
            result = self._sdk_connection(host_ip=server.ip)
            self.assertTrue(result, "Can create a ssl connection with correct certificate")

    # Changing from self signed to ca signed, while there is a connection with self-signed
    def test_root_existing_connection_rotate_cert(self):
        rest = RestConnection(self.master)
        rest.create_bucket(bucket='default', ramQuotaMB=256)
        bucket = 'default'
        self.add_built_in_server_user([{'id': bucket, 'name': bucket, 'password': 'password'}], \
                                      [{'id': bucket, 'name': bucket, 'roles': 'admin'}], self.master)
        self.sleep(30)
        result = False

        connection_string = 'couchbase://' + self.master.ip + '/default'
        try:
            cb = Bucket(connection_string, password='password')
            if cb is not None:
                result = True
        except Exception as ex:
            self.log.info("Exception is -{0}".format(ex))
        self.assertTrue(result, "Cannot create a client connection with server")

        create_docs = Thread(name='create_docs', target=self.createBulkDocuments, args=(cb,))
        create_docs.start()
        x509main(self.master).setup_master()
        create_docs.join()
        result = self._sdk_connection(host_ip=self.master.ip)
        self.assertTrue(result, "Cannot create a security connection with server")

    # Audit test to test /UploadClusterCA
    def test_audit_upload_ca(self):
        x509main(self.master).setup_master()
        expectedResults = {"expires":"2049-12-31T23:59:59.000Z", "subject":"CN=Root Authority", "ip":self.ip_address, "port":57457, "source":"ns_server", \
                               "user":"Administrator"}
        self.checkConfig(8229, self.master, expectedResults)

    # Audit test for /reloadCA

    def test_audit_reload_ca(self):
        x509main(self.master).setup_master()
        expectedResults = {"expires":"2049-12-31T23:59:59.000Z", "subject":"CN=" + self.master.ip, "ip":self.ip_address, "port":57457, "source":"ns_server", \
                               "user":"Administrator"}
        self.checkConfig(8230, self.master, expectedResults)

    # Common test case for testing services and other parameter
    def test_add_node_with_cert_diff_services(self):
        if self.enable_nton_local:
            ntonencryptionBase().ntonencryption_cli(self.servers, 'enable')
            ntonencryptionBase().change_cluster_encryption_cli(self.servers, self.local_clusterEncryption)
        servs_inout = self.servers[1:4]
        rest = RestConnection(self.master)
        services_in = []
        self.log.info ("list of services to be added {0}".format(self.services_in))

        for service in self.services_in.split("-"):
            services_in.append(service.split(":")[0])
        self.log.info ("list of services to be added after formatting {0}".format(services_in))

        for node in servs_inout:
            x509main(node).setup_master(self.client_cert_state, self.paths,
                                        self.prefixs, self.delimeters,
                                        self.upload_json_mode)
        # add nodes to the cluster
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], servs_inout, [],
                                                     services=services_in)
        rebalance.result()

        self.sleep(20)

        # check for analytics services, for Vulcan check on http port
        cbas_node = self.get_nodes_from_services_map(service_type='cbas')
        if cbas_node is not None:
            self.check_analytics_service(cbas_node)

        # check if n1ql service, test it end to end
        n1ql_node = self.get_nodes_from_services_map(service_type="n1ql")
        if n1ql_node is not None:
            self.check_query_api(n1ql_node)

        # check if fts services, test it end to end
        fts_node = self.get_nodes_from_services_map(service_type='fts')
        if fts_node is not None:
            self.check_fts_service(fts_node)

        # check for kv service, test for /pools/default
        kv_node = self.get_nodes_from_services_map(service_type='kv')
        if kv_node is not None:
            self.check_ns_server_rest_api(kv_node)
            # Commenting check for views because views have been deprecated and the API call fails with 500 internal
            # self.check_views_ssl(kv_node)

    def check_ns_server_rest_api(self, host):
        rest = RestConnection(host)
        helper = RestHelper(rest)
        if not helper.bucket_exists('default'):
            rest.create_bucket(bucket='default', storageBackend="couchstore",
                               ramQuotaMB=256)
            self.sleep(10)

        if self.client_cert_state == 'enable':
            output = x509main()._execute_command_clientcert(host.ip, url='/pools/default', port=18091, headers="", client_cert=True, curl=True)
        else:
            output = x509main()._execute_command_clientcert(host.ip, url='/pools/default', port=18091, headers=' -u Administrator:password ', client_cert=False, curl=True)

        output = json.loads(output)
        self.log.info ("Print output of command is {0}".format(output))
        self.assertEqual(output['rebalanceStatus'], 'none', " The Web request has failed on port 18091 ")

        if self.client_cert_state == 'enable':
            output = x509main()._execute_command_clientcert(host.ip, url='/pools/default', port=18091, headers=None, client_cert=True, curl=True, verb='POST', data='memoryQuota=400')
        else:
            output = x509main()._execute_command_clientcert(host.ip, url='/pools/default', port=18091, headers=' -u Administrator:password ', client_cert=False, curl=True, verb='POST', data='memoryQuota=400')

        if output == "":
            self.assertTrue(True, "Issue with post on /pools/default")

        output = x509main()._execute_command_clientcert(host.ip, url='/pools/default', port=self.rest_port, headers=" -u Administrator:password ", client_cert=False, curl=True, verb='GET', plain_curl=True)
        self.assertEqual(json.loads(output)['rebalanceStatus'], 'none', " The Web request has failed on port 8091 ")

        output = x509main()._execute_command_clientcert(host.ip, url='/pools/default', port=self.rest_port, headers=" -u Administrator:password ", client_cert=True, curl=True, verb='POST', plain_curl=True, data='memoryQuota=400')
        if output == "":
            self.assertTrue(True, "Issue with post on /pools/default")

    def check_query_api(self, host):
        rest = RestConnection(self.master)
        helper = RestHelper(rest)
        if not helper.bucket_exists('default'):
            rest.create_bucket(bucket='default', storageBackend="couchstore",ramQuotaMB=256)
        self.sleep(20)

        if self.client_cert_state == 'enable':
            output = x509main()._execute_command_clientcert(host.ip, url='/query/service', port=18093, headers='', client_cert=True, curl=True, verb='GET', data="statement='create index idx1 on default(name)'")
        else:
            output = x509main()._execute_command_clientcert(host.ip, url='/query/service', port=18093, headers='-u Administrator:password ', client_cert=False, curl=True, verb='GET', data="statement='create index idx1 on default(name)'")

        self.assertEqual(json.loads(output)['status'], "success", "Create Index Failed on port 18093")

        output = x509main()._execute_command_clientcert(host.ip, url='/query/service', port=self.n1ql_port, headers='-u Administrator:password ', client_cert=False, curl=True, verb='GET', plain_curl=True, data="statement='create index idx2 on default(name)'")
        self.assertEqual(json.loads(output)['status'], "success", "Create Index Failed on port 8093")

    def check_fts_service(self, host):
        rest = RestConnection(self.master)
        helper = RestHelper(rest)
        if not helper.bucket_exists('default'):
            rest.create_bucket(bucket='default', storageBackend="couchstore",ramQuotaMB=256)
        fts_ssl_port = 18094
        self.sleep(20)
        idx = {"sourceName": "default",
                   "sourceType": "couchbase",
                   "type": "fulltext-index"}

        qry = {"indexName": "default_index_1",
                   "query": {"field": "type", "match": "emp"},
                   "size": 10000000}

        if self.client_cert_state == 'enable':
            output = x509main()._execute_command_clientcert(host.ip, url='/api/index/default_idx', port=18094, headers=" -XPUT -H \"Content-Type: application/json\" ",
                                                            client_cert=True, curl=True, verb='GET', data="'" + json.dumps(idx) + "'")
        else:
            output = x509main()._execute_command_clientcert(host.ip, url='/api/index/default_idx', port=18094, headers=" -XPUT -H \"Content-Type: application/json\" -u Administrator:password ",
                                                            client_cert=False, curl=True, verb='GET', data="'" + json.dumps(idx) + "'")
        self.assertEqual(json.loads(output)['status'], "ok", "Issue with creating FTS index with client Cert")

        output = x509main()._execute_command_clientcert(host.ip, url='/api/index/default_idx01', port=self.fts_port, headers=" -XPUT -H \"Content-Type: application/json\" -u Administrator:password ",
                                                        client_cert=False, curl=True, verb='GET', data="'" + json.dumps(idx) + "'", plain_curl=True)
        self.assertEqual(json.loads(output)['status'], "ok", "Issue with creating FTS index with client Cert")

        if self.client_cert_state == 'enable':
            output = x509main()._execute_command_clientcert(host.ip, url='/api/index/default_idx', port=18094, headers=" -H \"Content-Type: application/json\" ",
                                                            client_cert=True, curl=True, verb='DELETE')
        else:
            output = x509main()._execute_command_clientcert(host.ip, url='/api/index/default_idx', port=18094, headers=" -H \"Content-Type: application/json\" -u Administrator:password ",
                                                            client_cert=False, curl=True, verb='DELETE')
        self.assertEqual(json.loads(output)['status'], "ok", "Issue with deleteing FTS index with client Cert")

        output = x509main()._execute_command_clientcert(host.ip, url='/api/index/default_idx01', port=self.fts_port, headers=" -H \"Content-Type: application/json\" -u Administrator:password ",
                                                        client_cert=False, curl=True, verb='DELETE', plain_curl=True)
        self.assertEqual(json.loads(output)['status'], "ok", "Issue with deleteing FTS index on 8094")

        ''' - Check with FTS team on this
        if self.client_cert_state == 'enable':
            cmd = "curl -v  --cacert " + self.root_ca_path + " --cert-type PEM --cert " + self.client_cert_pem + " --key-type PEM --key " + self.client_cert_key + \
                  "  -H \"Content-Type: application/json\" " + \
                  "https://{0}:{1}/api/index/". \
                          format(host.ip, fts_ssl_port)
        else:
            cmd = "curl -v --cacert " + self.root_ca_path  + \
                      " -H \"Content-Type: application/json\" " + \
                      "  -u Administrator:password " + \
                      "https://{0}:{1}/api/index/". \
                          format(host.ip, fts_ssl_port)

        self.log.info("Running command : {0}".format(cmd))
        output = subprocess.check_output(cmd, shell=True)
        print json.loads(output)
        '''

        if self.client_cert_state == 'enable':
            output = x509main()._execute_command_clientcert(host.ip, url='/api/stats', port=18094, headers='', client_cert=True, curl=True, verb='GET')
        else:
            output = x509main()._execute_command_clientcert(host.ip, url='/api/stats', port=18094, headers=' -u Administrator:password ',
                                                            client_cert=False, curl=True, verb='GET')
        self.assertEqual(json.loads(output)['manager']['TotPlannerKickErr'], 0, "Issues with FTS Stats API")

    # Check for analytics service api, right now SSL is not supported for Analytics, hence http port
    def check_analytics_service(self, host):
        rest = RestConnection(self.master)
        helper = RestHelper(rest)
        if not helper.bucket_exists('default'):
            rest.create_bucket(bucket='default', storageBackend="couchstore",ramQuotaMB=256)

        cmd = "curl -k -v  " + \
                " -s -u Administrator:password --data pretty=true --data-urlencode 'statement=create dataset on default' " + \
                "{0}://{1}:{2}/_p/cbas/query/service ". \
                format(self.protocol, host.ip, self.rest_port)

        self.log.info("Running command : {0}".format(cmd))
        output = subprocess.check_output(cmd, shell=True)
        self.assertEqual(json.loads(output)['status'], "success", "Create CBAS Index Failed")

    def check_views_ssl(self, host):
        rest = RestConnection(self.master)
        helper = RestHelper(rest)
        if not helper.bucket_exists('default'):
            rest.create_bucket(bucket='default', ramQuotaMB=256,
                               storageBackend="couchstore")

        if self.client_cert_state == 'enable':
            output = x509main()._execute_command_clientcert(host.ip, url='/default/_design/dev_sample', port=18092, headers=' -XPUT ', client_cert=True, curl=True, verb='GET')
        else:
            output = x509main()._execute_command_clientcert(host.ip, url='/default/_design/dev_sample', port=18092, headers=' -XPUT -u Administrator:password ', client_cert=False, curl=True, verb='GET')

        if self.client_cert_state == 'enable':
            self.assertEqual(json.loads(output)['reason'], "Content is not json.", "Create View Index Failed")
        else:
            self.assertEqual(json.loads(output)['error'], "invalid_design_document", "Create Index Failed")

        # " https://{0}:{1}/default/_design/dev_sample -d '{\"views\":{\"sampleview\":{\"map\":\"function (doc, meta){emit(doc.emailId,meta.id, null);\n}\"}}, \"options\": {\"updateMinChanges\": 3, \"replicaUpdateMinChanges\": 3}}'". \

        if self.client_cert_state == 'enable':
            output = x509main()._execute_command_clientcert(host.ip, url='/default/_design/dev_sample/_view/sampleview', port=18092, headers='', client_cert=True, curl=True, verb='POST')
        else:
            output = x509main()._execute_command_clientcert(host.ip, url='/default/_design/dev_sample/_view/sampleview', port=18092, headers=' -u Administrator:password ', client_cert=False, curl=True, verb='POST')

        self.assertEqual(json.loads(output)['error'], "not_found", "Create View Index Failed")

    def test_rest_api_disable(self):
        host = self.master
        rest = RestConnection(self.master)
        rest.create_bucket(bucket='default', ramQuotaMB=256)

        status, output = x509main()._execute_command_clientcert(host.ip, url='/pools/default', port=18091, headers="", client_cert=True, curl=False)
        self.assertEqual(status, 401, "Issue with client cert with, user should not able to access via client cert")

        output = x509main()._execute_command_clientcert(host.ip, url='/pools/default', port=18091, headers=" -u Administrator:password ", client_cert=False, curl=True)
        self.assertEqual(json.loads(output)['rebalanceStatus'], 'none', " The Web request has failed on port 18091 ")

        output = x509main()._execute_command_clientcert(host.ip, url='/pools/default', port=self.rest_port, headers=" -u Administrator:password ", client_cert=False, curl=True, verb='GET', plain_curl=True)
        self.assertEqual(json.loads(output)['rebalanceStatus'], 'none', " The Web request has failed on port 18091 ")

    def test_rest_api_mandatory(self):
        host = self.master
        rest = RestConnection(self.master)
        rest.create_bucket(bucket='default', ramQuotaMB=256)

        output = x509main()._execute_command_clientcert(host.ip, url='/pools/default', port=18091, headers="", client_cert=True, curl=True)
        self.assertEqual(json.loads(output)['name'], 'default', " The Web request has failed on port 18091 ")

        cmd = "curl -v --cacert " + self.root_ca_path + \
              " -u Administrator:password  https://{0}:{1}/pools/default". \
                  format(self.master.ip, '18091')

        self.log.info("Running command : {0}".format(cmd))
        try:
            output = subprocess.check_output(cmd, shell=True)
        except:
            self.assertTrue(True, "CA Cert works with mandatory")
        if CbServer.use_https:
            plain_curl = False
        else:
            plain_curl = True
        status, output = x509main()._execute_command_clientcert(host.ip, url='/pools/default', port=self.rest_port, headers=" -u Administrator:password ", client_cert=False, curl=False, verb='GET', plain_curl=plain_curl)
        self.assertEqual(status, 401, "Invalid user gets authenticated successfully")

    def test_incorrect_user(self):
        host = self.master
        rest = RestConnection(self.master)
        rest.create_bucket(bucket='default', ramQuotaMB=256)

        status = x509main()._execute_command_clientcert(host.ip, url='/pools/default', port=18091, headers="", client_cert=True, curl=False)
        self.assertEqual(status[0], 'error' , "Invalid user gets authenticated successfully")

    def test_upload_json_tests(self):
        rest = RestConnection(self.master)
        x509main(self.master).write_client_cert_json_new(self.client_cert_state, self.paths, self.prefixs, self.delimeters)
        status, content = x509main(self.master)._upload_cluster_ca_settings("Administrator", "password")
        if "Invalid value 'subject.test' for key 'path'" in content:
            self.assertTrue(True, " Correct error message for key path")

'''
class x509_upgrade(NewUpgradeBaseTest):

    def setUp(self):
        super(x509_upgrade, self).setUp()
        self.initial_version = self.input.param("initial_version", '4.5.0-900')
        self.upgrade_version = self.input.param("upgrade_version", "4.5.0-1069")
        self.ip_address = '172.16.1.174'
        self.root_ca_path = x509main.CACERTFILEPATH + x509main.CACERTFILE
        self.client_cert_pem = x509main.CACERTFILEPATH + self.ip_address + ".pem"
        self.client_cert_key = x509main.CACERTFILEPATH + self.ip_address + ".key"
        # Input parameters for state, path, delimeters and prefixes
        self.client_cert_state = self.input.param("client_cert_state", "disable")
        self.paths = self.input.param('paths', "subject.cn:san.dnsname:san.uri").split(":")
        self.prefixs = self.input.param('prefixs', 'www.cb-:us.:www.').split(":")
        self.delimeters = self.input.param('delimeter', '.:.:.') .split(":")
        self.setup_once = self.input.param("setup_once", False)

        SSLtype = self.input.param("SSLtype", "openssl")
        encryption_type = self.input.param('encryption_type', "")
        key_length = self.input.param("key_length", 1024)

        self.dns = self.input.param('dns', None)
        self.uri = self.input.param('uri', None)

        copy_servers = copy.deepcopy(self.servers)

        self._reset_original()

        if (self.dns is not None) or (self.uri is not None):
            x509main(self.master)._generate_cert(copy_servers, type=SSLtype, encryption=encryption_type, key_length=key_length, client_ip=self.ip_address, alt_names='non_default', dns=self.dns, uri=self.uri)
        else:
            x509main(self.master)._generate_cert(copy_servers, type=SSLtype, encryption=encryption_type, key_length=key_length, client_ip=self.ip_address)
        self.log.info(" Path is {0} - Prefixs - {1} -- Delimeters - {2}".format(self.paths, self.prefixs, self.delimeters))

        if (self.setup_once):
            x509main(self.master).setup_master(self.client_cert_state, self.paths, self.prefixs, self.delimeters)
            x509main().setup_cluster_nodes_ssl(self.servers)

        enable_audit = self.input.param('audit', None)
        if enable_audit:
            Audit = audit(host=self.master)
            currentState = Audit.getAuditStatus()
            self.log.info ("Current status of audit on ip - {0} is {1}".format(self.master.ip, currentState))
            if not currentState:
                self.log.info ("Enabling Audit ")
                Audit.setAuditEnable('true')
                self.sleep(30)

    def tearDown(self):
        self._reset_original()
        super(x509_upgrade, self).tearDown()

    def _reset_original(self):
        self.log.info ("Reverting to original state - regenerating certificate and removing inbox folder")
        for servers in self.servers:
            rest = RestConnection(servers)
            rest.regenerate_cluster_certificate()
            x509main(servers)._delete_inbox_folder()

    def check_rest_api(self, host):
        rest = RestConnection(host)
        helper = RestHelper(rest)
        if not helper.bucket_exists('default'):
            rest.create_bucket(bucket='default', ramQuotaMB=256)
            self.sleep(10)

        if self.client_cert_state == 'enable':
            output = x509main()._execute_command_clientcert(host.ip, url='/pools/default', port=18091, headers="", client_cert=True, curl=True)
        else:
            output = x509main()._execute_command_clientcert(host.ip, url='/pools/default', port=18091, headers=' -u Administrator:password ', client_cert=False, curl=True)

        output = json.loads(output)
        self.log.info ("Print output of command is {0}".format(output))
        self.assertEqual(output['rebalanceStatus'], 'none', " The Web request has failed on port 18091 ")

        if self.client_cert_state == 'enable':
            output = x509main()._execute_command_clientcert(host.ip, url='/pools/default', port=18091, headers=None, client_cert=True, curl=True, verb='POST', data='memoryQuota=400')
        else:
            output = x509main()._execute_command_clientcert(host.ip, url='/pools/default', port=18091, headers=' -u Administrator:password ', client_cert=False, curl=True, verb='POST', data='memoryQuota=400')

        if output == "":
            self.assertTrue(True, "Issue with post on /pools/default")

        output = x509main()._execute_command_clientcert(host.ip, url='/pools/default', port=8091, headers=" -u Administrator:password ", client_cert=False, curl=True, verb='GET', plain_curl=True)
        self.assertEqual(json.loads(output)['rebalanceStatus'], 'none', " The Web request has failed on port 8091 ")

        output = x509main()._execute_command_clientcert(host.ip, url='/pools/default', port=8091, headers=" -u Administrator:password ", client_cert=True, curl=True, verb='POST', plain_curl=True, data='memoryQuota=400')
        if output == "":
            self.assertTrue(True, "Issue with post on /pools/default")

    def _sdk_connection(self, root_ca_path=x509main.CACERTFILEPATH + x509main.CACERTFILE, bucket='default', host_ip=None, sdk_version='pre-vulcan'):
        self.sleep(30)
        result = False
        self.add_built_in_server_user([{'id': bucket, 'name': bucket, 'password': 'password'}], \
                                      [{'id': bucket, 'name': bucket, 'roles': 'admin'}], self.master)
        if sdk_version == 'pre-vulcan':
            connection_string = 'couchbases://' + host_ip + '/' + bucket + '?certpath=' + root_ca_path
            self.log.info("Connection string is -{0}".format(connection_string))
            try:
                cb = Bucket(connection_string, password='password')
                if cb is not None:
                    result = True
                    return result, cb
            except Exception as ex:
                self.log.info("Expection is  -{0}".format(ex))
        elif sdk_version == 'vulcan':
            self.add_built_in_server_user([{'id': 'cbadminbucket', 'name': 'cbadminbucket', 'password': 'password'}], \
                                      [{'id': 'cbadminbucket', 'name': 'cbadminbucket', 'roles': 'admin'}], self.master)
            key_file = x509main.CACERTFILEPATH + self.ip_address + ".key"
            chain_file = x509main.CACERTFILEPATH + "/long_chain" + self.ip_address + ".pem"
            connection_string = 'couchbases://' + host_ip + '/?certpath=' + chain_file + "&keypath=" + key_file
            self.log.info("Connection string is -{0}".format(connection_string))
            try:
                cluster = Cluster(connection_string);
                cb = cluster.open_bucket(bucket)
                if cb is not None:
                    result = True
                    return result, cb
            except Exception as ex:
                self.log.info("Expection is  -{0}".format(ex))
        return result

    def upgrade_all_nodes(self):
        servers_in = self.servers[1:]
        self._install(self.servers)
        rest_conn = RestConnection(self.master)
        rest_conn.init_cluster(username='Administrator', password='password')
        rest_conn.create_bucket(bucket='default', ramQuotaMB=512)
        self.cluster.rebalance(self.servers, servers_in, [])

        upgrade_threads = self._async_update(upgrade_version=self.upgrade_version, servers=self.servers)
        for threads in upgrade_threads:
            threads.join()

        x509main(self.master).setup_master()
        x509main().setup_cluster_nodes_ssl(self.servers, reload_cert=True)

        for server in self.servers:
            result = self._sdk_connection(host_ip=server.ip)
            self.assertTrue(result, "Cannot create a security connection with server")
            self.check_rest_api(server)

    def upgrade_half_nodes(self):
        serv_upgrade = self.servers[2:4]
        servers_in = self.servers[1:]
        self._install(self.servers)
        rest_conn = RestConnection(self.master)
        rest_conn.init_cluster(username='Administrator', password='password')
        rest_conn.create_bucket(bucket='default', ramQuotaMB=512)
        self.cluster.rebalance(self.servers, servers_in, [])

        upgrade_threads = self._async_update(upgrade_version=self.upgrade_version, servers=serv_upgrade)
        for threads in upgrade_threads:
            threads.join()

        x509main(self.master).setup_master()
        x509main().setup_cluster_nodes_ssl(self.servers, reload_cert=True)

        for server in self.servers:
            result = self._sdk_connection(host_ip=server.ip)
            self.assertFalse(result, "Can create a security connection with server")

    def upgrade_all_nodes_4_6_3(self):
        servers_in = self.servers[1:]
        self._install(self.servers)
        rest_conn = RestConnection(self.master)
        rest_conn.init_cluster(username='Administrator', password='password')
        rest_conn.create_bucket(bucket='default', ramQuotaMB=512)
        self.cluster.rebalance(self.servers, servers_in, [])

        x509main(self.master).setup_master()
        x509main().setup_cluster_nodes_ssl(self.servers, reload_cert=True)

        upgrade_threads = self._async_update(upgrade_version=self.upgrade_version, servers=self.servers)
        for threads in upgrade_threads:
            threads.join()

        for server in self.servers:
            result = self._sdk_connection(host_ip=server.ip)
            self.assertTrue(result, "Cannot create a security connection with server")
            result = self._sdk_connection(host_ip=server.ip, sdk_version='vulcan')
            self.assertTrue(result, "Cannot create a security connection with server")
            self.check_rest_api(server)
'''
