from basetestcase import BaseTestCase
from security.x509main import x509main
from newupgradebasetest import NewUpgradeBaseTest
from membase.api.rest_client import RestConnection
import commands
import json
from couchbase.bucket import Bucket
from threading import Thread, Event
from remote.remote_util import RemoteMachineShellConnection
from security.auditmain import audit

class x509tests(BaseTestCase):

    def setUp(self):
        super(x509tests, self).setUp()
        self._reset_original()
        SSLtype = self.input.param("SSLtype","go")
        encryption_type = self.input.param('encryption_type',"")
        key_length=self.input.param("key_length",1024)
        x509main(self.master)._generate_cert(self.servers,type=SSLtype,encryption=encryption_type,key_length=key_length)
        self.ip_address = self.getLocalIPAddress()
        enable_audit=self.input.param('audit',None)
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
        super(x509tests, self).tearDown()


    def _reset_original(self):
        self.log.info ("Reverting to original state - regenerating certificate and removing inbox folder")
        for servers in self.servers:
            rest = RestConnection(servers)
            rest.regenerate_cluster_certificate()
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
        status, ipAddress = commands.getstatusoutput("ifconfig en0 | grep 'inet addr:' | cut -d: -f2 |awk '{print $1}'")
        if '1' not in ipAddress:
            status, ipAddress = commands.getstatusoutput("ifconfig eth0 | grep  -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | awk '{print $2}'")
        return ipAddress


    def createBulkDocuments(self,client):
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

    def check_rebalance_complete(self,rest):
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

    def _sdk_connection(self,root_ca_path=x509main.CACERTFILEPATH + x509main.CACERTFILE,bucket='default',host_ip=None):
        self.sleep(30)
        result = False
        connection_string = 'couchbases://'+ host_ip + '/' + bucket + '?certpath='+root_ca_path
        print connection_string
        try:
            cb = Bucket(connection_string)
            if cb is not None:
                result = True
                return result, cb
        except Exception, ex:
            print ex
            return result

    def test_basic_ssl_test(self):
        x509main(self.master).setup_master()
        status = x509main(self.master)._validate_ssl_login()
        self.assertEqual(status,200,"Not able to login via SSL code")

    def test_get_cluster_ca(self):
        x509main(self.master).setup_master()
        status, content, header = x509main(self.master)._get_cluster_ca_cert()
        content = json.loads(content)
        self.assertEqual(content['cert']['type'],"uploaded","Type of certificate is mismatch")
        #self.assertEqual(content['cert']['pem'],"uploaded","Type of certificate is mismatch")
        self.assertEqual(content['cert']['subject'],"CN=Root Authority","Common Name is incorrect")

    def test_get_cluster_ca_cluster(self):
        servs_inout = self.servers[1]
        rest = RestConnection(self.master)
        x509main(self.master).setup_master()
        x509main(servs_inout)._setup_node_certificates(reload_cert=False)
        servs_inout = self.servers[1]
        rest.add_node('Administrator','password',servs_inout.ip)
        for server in self.servers[:2]:
            status, content, header = x509main(server)._get_cluster_ca_cert()
            content = json.loads(content)
            self.assertTrue(status,"Issue while Cluster CA Cert")
            self.assertEqual(content['cert']['type'],"uploaded","Type of certificate is mismatch")
            self.assertEqual(content['cert']['subject'],"CN=Root Authority","Common Name is incorrect")

    def test_get_cluster_ca_self_signed(self):
        rest = RestConnection(self.master)
        rest.regenerate_cluster_certificate()
        status, content, header = x509main(self.master)._get_cluster_ca_cert()
        content = json.loads(content)
        self.assertTrue(status,"Issue while Cluster CA Cert")
        self.assertEqual(content['cert']['type'],"generated","Type of certificate is mismatch")
        #self.assertEqual(content['cert']['pem'],"uploaded","Type of certificate is mismatch")

    def test_error_without_node_chain_certificates(self):
        x509main(self.master)._upload_cluster_ca_certificate("Administrator",'password')
        status, content = x509main(self.master)._reload_node_certificate(self.master)
        self.assertEqual(status['status'],'400',"Issue with status with node certificate are missing")
        self.assertTrue('Unable to read certificate chain file' in content, "Incorrect message from the system")

    def test_error_without_chain_cert(self):
        x509main(self.master)._upload_cluster_ca_certificate("Administrator",'password')
        x509main(self.master)._setup_node_certificates(chain_cert=False)
        status, content = x509main(self.master)._reload_node_certificate(self.master)
        self.assertEqual(status['status'],'400',"Issue with status with node certificate are missing")
        self.assertTrue('Unable to read certificate chain file' in content, "Incorrect message from the system")

    def test_error_without_node_key(self):
        x509main(self.master)._upload_cluster_ca_certificate("Administrator",'password')
        x509main(self.master)._setup_node_certificates(node_key=False)
        status, content = x509main(self.master)._reload_node_certificate(self.master)
        self.assertEqual(status['status'],'400',"Issue with status with node key is missing")
        self.assertTrue('Unable to read private key file' in content, "Incorrect message from the system")

    def test_add_node_without_cert(self):
        rest = RestConnection(self.master)
        servs_inout = self.servers[1]
        x509main(self.master).setup_master()
        try:
            rest.add_node('Administrator','password',servs_inout.ip)
        except Exception, ex:
            ex = str(ex)
            #expected_result  = "Error adding node: " + servs_inout.ip + " to the cluster:" + self.master.ip + " - [\"Prepare join failed. Error applying node certificate. Unable to read certificate chain file\"]"
            expected_result  = "Error adding node: " + servs_inout.ip + " to the cluster:" + self.master.ip
            self.assertTrue(expected_result in ex,"Incorrect Error message in exception")
            expected_result  = "Error applying node certificate. Unable to read certificate chain file"
            self.assertTrue(expected_result in ex,"Incorrect Error message in exception")
            expected_result  = "The file does not exist."
            self.assertTrue(expected_result in ex,"Incorrect Error message in exception")


    def test_add_node_with_cert(self):
        servs_inout = self.servers[1:4]
        rest = RestConnection(self.master)
        x509main(self.master).setup_master()
        x509main().setup_cluster_nodes_ssl(servs_inout)
        known_nodes = ['ns_1@'+self.master.ip]
        for server in servs_inout:
            rest.add_node('Administrator','password',server.ip)
            known_nodes.append('ns_1@' + server.ip)
        rest.rebalance(known_nodes)
        self.assertTrue(self.check_rebalance_complete(rest),"Issue with rebalance")
        for server in self.servers:
            status = x509main(server)._validate_ssl_login()
            self.assertEqual(status,200,"Not able to login via SSL code")

    def test_add_remove_add_back_node_with_cert(self,rebalance=None):
        rebalance = self.input.param('rebalance')
        rest = RestConnection(self.master)
        servs_inout = self.servers[1:3]
        serv_out = 'ns_1@' + servs_inout[1].ip
        known_nodes = ['ns_1@'+self.master.ip]
        x509main(self.master).setup_master()
        x509main().setup_cluster_nodes_ssl(servs_inout)
        for server in servs_inout:
            rest.add_node('Administrator','password',server.ip)
            known_nodes.append('ns_1@' + server.ip)
        rest.rebalance(known_nodes)
        self.assertTrue(self.check_rebalance_complete(rest),"Issue with rebalance")
        for server in servs_inout:
            status = x509main(server)._validate_ssl_login()
            self.assertEqual(status,200,"Not able to login via SSL code")
        rest.fail_over(serv_out,graceful=False)
        if (rebalance):
            rest.rebalance(known_nodes,[serv_out])
            self.assertTrue(self.check_rebalance_complete(rest),"Issue with rebalance")
            rest.add_node('Administrator','password',servs_inout[1].ip)
        else:
            rest.add_back_node(serv_out)
        rest.rebalance(known_nodes)
        self.assertTrue(self.check_rebalance_complete(rest),"Issue with rebalance")
        for server in servs_inout:
            response = x509main(server)._validate_ssl_login()
            self.assertEqual(status,200,"Not able to login via SSL code")

    def test_add_remove_graceful_add_back_node_with_cert(self,recovery_type=None):
        recovery_type = self.input.param('recovery_type')
        rest = RestConnection(self.master)
        known_nodes = ['ns_1@'+self.master.ip]
        progress = None
        count = 0
        servs_inout = self.servers[1:]
        serv_out = 'ns_1@' + servs_inout[1].ip

        rest.create_bucket(bucket='default', ramQuotaMB=100)

        x509main(self.master).setup_master()
        x509main().setup_cluster_nodes_ssl(servs_inout)
        for server in servs_inout:
            rest.add_node('Administrator','password',server.ip)
            known_nodes.append('ns_1@' + server.ip)

        rest.rebalance(known_nodes)
        self.assertTrue(self.check_rebalance_complete(rest),"Issue with rebalance")

        for server in servs_inout:
            status = x509main(server)._validate_ssl_login()
            self.assertEqual(status,200,"Not able to login via SSL code")

        rest.fail_over(serv_out,graceful=True)
        self.assertTrue(self.check_rebalance_complete(rest),"Issue with rebalance")
        rest.set_recovery_type(serv_out,recovery_type)
        rest.add_back_node(serv_out)
        rest.rebalance(known_nodes)
        self.assertTrue(self.check_rebalance_complete(rest),"Issue with rebalance")

        for server in servs_inout:
            status = x509main(server)._validate_ssl_login()
            self.assertEqual(status,200,"Not able to login via SSL code")

    def test_add_remove_autofailover(self):
        rest = RestConnection(self.master)
        serv_out = self.servers[3]
        shell = RemoteMachineShellConnection(serv_out)
        known_nodes = ['ns_1@'+self.master.ip]

        rest.create_bucket(bucket='default', ramQuotaMB=100)
        rest.update_autofailover_settings(True,30)

        x509main(self.master).setup_master()
        x509main().setup_cluster_nodes_ssl(self.servers[1:4])
        for server in self.servers[1:4]:
            rest.add_node('Administrator','password',server.ip)
            known_nodes.append('ns_1@'+server.ip)

        rest.rebalance(known_nodes)
        self.assertTrue(self.check_rebalance_complete(rest),"Issue with rebalance")

        shell.stop_server()
        self.sleep(60)
        shell.start_server()
        self.sleep(30)
        for server in self.servers:
            status = x509main(server)._validate_ssl_login()
            self.assertEqual(status,200,"Not able to login via SSL code")

    def test_add_node_with_cert_non_master(self):
        rest = RestConnection(self.master)
        x509main(self.master).setup_master()
        x509main().setup_cluster_nodes_ssl(self.servers[1:3])

        servs_inout = self.servers[1]
        rest.add_node('Administrator','password',servs_inout.ip)
        known_nodes = ['ns_1@'+self.master.ip,'ns_1@' + servs_inout.ip]
        rest.rebalance(known_nodes)
        self.assertTrue(self.check_rebalance_complete(rest),"Issue with rebalance")

        rest = RestConnection(self.servers[1])
        servs_inout = self.servers[2]
        rest.add_node('Administrator','password',servs_inout.ip)
        known_nodes = ['ns_1@'+self.master.ip,'ns_1@' + servs_inout.ip,'ns_1@' + self.servers[1].ip]
        rest.rebalance(known_nodes)
        self.assertTrue(self.check_rebalance_complete(rest),"Issue with rebalance")

        for server in self.servers[:3]:
            status = x509main(server)._validate_ssl_login()
            self.assertEqual(status,200,"Not able to login via SSL code for ip - {0}".format(server.ip))

    #simple xdcr with ca cert
    def test_basic_xdcr_with_cert(self):

        cluster1 = self.servers[0:2]
        cluster2 = self.servers[2:4]
        remote_cluster_name = 'sslcluster'
        restCluster1 = RestConnection(cluster1[0])
        restCluster2 = RestConnection(cluster2[0])

        try:
            #Setup cluster1
            x509main(cluster1[0]).setup_master()
            x509main(cluster1[1])._setup_node_certificates(reload_cert=False)

            restCluster1.add_node('Administrator','password',cluster1[1].ip)
            known_nodes = ['ns_1@'+cluster1[0].ip,'ns_1@' + cluster1[1].ip]
            restCluster1.rebalance(known_nodes)
            self.assertTrue(self.check_rebalance_complete(restCluster1),"Issue with rebalance")
            restCluster1.create_bucket(bucket='default', ramQuotaMB=100)
            restCluster1.remove_all_replications()
            restCluster1.remove_all_remote_clusters()

            #Setup cluster2
            x509main(cluster2[0]).setup_master()
            x509main(cluster2[1])._setup_node_certificates(reload_cert=False)

            restCluster2.add_node('Administrator','password',cluster2[1].ip)
            known_nodes = ['ns_1@'+cluster2[0].ip,'ns_1@' + cluster2[1].ip]
            restCluster2.rebalance(known_nodes)
            self.assertTrue(self.check_rebalance_complete(restCluster2),"Issue with rebalance")
            restCluster2.create_bucket(bucket='default', ramQuotaMB=100)

            test = x509main.CACERTFILEPATH + x509main.CACERTFILE
            data  =  open(test, 'rb').read()
            restCluster1.add_remote_cluster(cluster2[0].ip,cluster2[0].port,'Administrator','password',remote_cluster_name,certificate=data)
            replication_id = restCluster1.start_replication('continuous','default',remote_cluster_name)
            if replication_id is not None:
                self.assertTrue(True,"Replication was not created successfully")
        finally:
            known_nodes = ['ns_1@'+cluster2[0].ip,'ns_1@'+cluster2[1].ip]
            restCluster2.rebalance(known_nodes,['ns_1@' + cluster2[1].ip])
            self.assertTrue(self.check_rebalance_complete(restCluster2),"Issue with rebalance")
            restCluster2.delete_bucket()

    #simple xdcr with ca cert updated at source and destination, re-generate new certs
    def test_basic_xdcr_with_cert_regenerate(self):

        cluster1 = self.servers[0:2]
        cluster2 = self.servers[2:4]
        remote_cluster_name = 'sslcluster'
        restCluster1 = RestConnection(cluster1[0])
        restCluster2 = RestConnection(cluster2[0])

        try:
            #Setup cluster1
            x509main(cluster1[0]).setup_master()
            x509main(cluster1[1])._setup_node_certificates(reload_cert=False)

            restCluster1.add_node('Administrator','password',cluster1[1].ip)
            known_nodes = ['ns_1@'+cluster1[0].ip,'ns_1@' + cluster1[1].ip]
            restCluster1.rebalance(known_nodes)
            self.assertTrue(self.check_rebalance_complete(restCluster1),"Issue with rebalance")
            restCluster1.create_bucket(bucket='default', ramQuotaMB=100)
            restCluster1.remove_all_replications()
            restCluster1.remove_all_remote_clusters()

            #Setup cluster2
            x509main(cluster2[0]).setup_master()
            x509main(cluster2[1])._setup_node_certificates(reload_cert=False)

            restCluster2.add_node('Administrator','password',cluster2[1].ip)
            known_nodes = ['ns_1@'+cluster2[0].ip,'ns_1@' + cluster2[1].ip]
            restCluster2.rebalance(known_nodes)
            self.assertTrue(self.check_rebalance_complete(restCluster2),"Issue with rebalance")
            restCluster2.create_bucket(bucket='default', ramQuotaMB=100)

            test = x509main.CACERTFILEPATH + x509main.CACERTFILE
            data  =  open(test, 'rb').read()
            restCluster1.add_remote_cluster(cluster2[0].ip,cluster2[0].port,'Administrator','password',remote_cluster_name,certificate=data)
            replication_id = restCluster1.start_replication('continuous','default',remote_cluster_name)

            #restCluster1.set_xdcr_param('default','default','pauseRequested',True)

            x509main(self.master)._delete_inbox_folder()
            x509main(self.master)._generate_cert(self.servers,root_cn="CB\ Authority")
            self.log.info ("Setting up the first cluster for new certificate")

            x509main(cluster1[0]).setup_master()
            x509main(cluster1[1])._setup_node_certificates(reload_cert=False)
            self.log.info ("Setting up the second cluster for new certificate")
            x509main(cluster2[0]).setup_master()
            x509main(cluster2[1])._setup_node_certificates(reload_cert=False)

            status = restCluster1.is_replication_paused('default','default')
            if not status:
                restCluster1.set_xdcr_param('default','default','pauseRequested',False)

            restCluster1.set_xdcr_param('default','default','pauseRequested',True)
            status = restCluster1.is_replication_paused('default','default')
            self.assertTrue(status,"Replication has not started after certificate upgrade")
        finally:
            known_nodes = ['ns_1@'+cluster2[0].ip,'ns_1@'+cluster2[1].ip]
            restCluster2.rebalance(known_nodes,['ns_1@' + cluster2[1].ip])
            self.assertTrue(self.check_rebalance_complete(restCluster2),"Issue with rebalance")
            restCluster2.delete_bucket()

    #source ca and destination self_signed
    def test_xdcr_destination_self_signed_cert(self):

        cluster1 = self.servers[0:2]
        cluster2 = self.servers[2:4]
        remote_cluster_name = 'sslcluster'
        restCluster1 = RestConnection(cluster1[0])
        restCluster2 = RestConnection(cluster2[0])

        try:
            #Setup cluster1
            x509main(cluster1[0])._upload_cluster_ca_certificate("Administrator",'password')
            x509main(cluster1[0])._setup_node_certificates()
            x509main(cluster1[1])._setup_node_certificates(reload_cert=False)

            restCluster1.add_node('Administrator','password',cluster1[1].ip)
            known_nodes = ['ns_1@'+cluster1[0].ip,'ns_1@' + cluster1[1].ip]
            restCluster1.rebalance(known_nodes)
            self.assertTrue(self.check_rebalance_complete(restCluster1),"Issue with rebalance")
            restCluster1.create_bucket(bucket='default', ramQuotaMB=100)
            restCluster1.remove_all_replications()
            restCluster1.remove_all_remote_clusters()


            restCluster2.add_node('Administrator','password',cluster2[1].ip)
            known_nodes = ['ns_1@'+cluster2[0].ip,'ns_1@' + cluster2[1].ip]
            restCluster2.rebalance(known_nodes)
            self.assertTrue(self.check_rebalance_complete(restCluster2),"Issue with rebalance")
            restCluster2.create_bucket(bucket='default', ramQuotaMB=100)

            test = x509main.CACERTFILEPATH + x509main.CACERTFILE
            data  =  open(test, 'rb').read()
            restCluster1.add_remote_cluster(cluster2[0].ip,cluster2[0].port,'Administrator','password',remote_cluster_name,certificate=data)
            replication_id = restCluster1.start_replication('continuous','default',remote_cluster_name)
            if replication_id is not None:
                self.assertTrue(True,"Cannot create a replication")

        finally:
            known_nodes = ['ns_1@'+cluster2[0].ip,'ns_1@'+cluster2[1].ip]
            restCluster2.rebalance(known_nodes,['ns_1@' + cluster2[1].ip])
            self.assertTrue(self.check_rebalance_complete(restCluster2),"Issue with rebalance")
            restCluster2.delete_bucket()

    def test_basic_ssl_test_invalid_cert(self):
        x509main(self.master).setup_master()
        status = x509main(self.master)._validate_ssl_login()
        self.assertEqual(status,200,"Not able to login via SSL code")

    #test sdk certs on a single node
    def test_sdk(self):
        rest = RestConnection(self.master)
        x509main(self.master).setup_master()
        rest.create_bucket(bucket='default', ramQuotaMB=100)
        result = self._sdk_connection(host_ip=self.master.ip)
        self.assertTrue(result,"Cannot create a security connection with server")

    #test with sdk cluster using ca certs
    def test_sdk_cluster(self):
        rest = RestConnection(self.master)
        x509main(self.master).setup_master()
        x509main().setup_cluster_nodes_ssl(self.servers)
        rest.create_bucket(bucket='default', ramQuotaMB=100)

        servers_in = self.servers[1:]
        self.cluster.rebalance(self.servers, servers_in, [])

        for server in self.servers:
            result = self._sdk_connection(host_ip=server.ip)
            self.assertTrue(result,"Cannot create a security connection with server")

    def test_sdk_existing_cluster(self):
        servers_in = self.servers[1:]
        self.cluster.rebalance(self.servers, servers_in, [])

        rest = RestConnection(self.master)
        x509main(self.master).setup_master()
        x509main().setup_cluster_nodes_ssl(self.servers,reload_cert=True)
        rest.create_bucket(bucket='default', ramQuotaMB=100)

        for server in self.servers:
            result = self._sdk_connection(host_ip=server.ip)
            self.assertTrue(result,"Cannot create a security connection with server")

    #Incorrect root cert
    def test_sdk_cluster_incorrect_cert(self):
        rest = RestConnection(self.master)
        x509main(self.master).setup_master()
        x509main().setup_cluster_nodes_ssl(self.servers)
        rest.create_bucket(bucket='default', ramQuotaMB=100)

        servers_in = self.servers[1:]
        self.cluster.rebalance(self.servers, servers_in, [])

        root_incorrect_ca_path = x509main.CACERTFILEPATH + x509main.INCORRECT_ROOT_CERT
        for server in self.servers:
            result = self._sdk_connection(host_ip=server.ip,root_ca_path=root_incorrect_ca_path)
            self.assertFalse(result,"Can create a security connection with incorrect root cert")

    #Changing from root to self signed certificates
    def test_sdk_change_ca_self_signed(self):
        rest = RestConnection(self.master)
        temp_file_name = '/tmp/newcerts/orig_cert.pem'
        x509main(self.master).setup_master()
        x509main().setup_cluster_nodes_ssl(self.servers)
        rest.create_bucket(bucket='default', ramQuotaMB=100)
        result = self._sdk_connection(host_ip=self.master.ip)
        self.assertTrue(result,"Cannot create a security connection with server")
        rest.regenerate_cluster_certificate()

        temp_cert = rest.get_cluster_ceritificate()
        temp_file = open(temp_file_name,'w')
        temp_file.write(temp_cert)
        temp_file.close()

        result = self._sdk_connection(root_ca_path=temp_file_name,host_ip=self.master.ip)
        self.assertTrue(result,"Cannot create a security connection with server")

    #Changing from one root crt to another root crt when an existing connections exists
    def test_root_crt_rotate_existing_cluster(self):
        rest = RestConnection(self.master)
        x509main(self.master).setup_master()
        x509main().setup_cluster_nodes_ssl(self.servers,reload_cert=True)

        rest.create_bucket(bucket='default', ramQuotaMB=100)

        result,cb  = self._sdk_connection(host_ip=self.master.ip)
        create_docs = Thread(name='create_docs', target=self.createBulkDocuments, args=(cb,))
        create_docs.start()

        x509main(self.master)._delete_inbox_folder()
        x509main(self.master)._generate_cert(self.servers,root_cn="CB\ Authority")
        x509main(self.master).setup_master()
        x509main().setup_cluster_nodes_ssl(self.servers,reload_cert=True)

        create_docs.join()

        result,cb  = self._sdk_connection(host_ip=self.master.ip)
        self.assertTrue(result,"Cannot create a security connection with server")

    #Changing from one root crt to another root crt when an existing connections exists - cluster
    def test_root_crt_rotate_cluster(self):
        rest = RestConnection(self.master)
        x509main(self.master).setup_master()
        x509main().setup_cluster_nodes_ssl(self.servers)
        rest.create_bucket(bucket='default', ramQuotaMB=100)
        self.sleep(30)
        servers_in = self.servers[1:]
        self.cluster.rebalance(self.servers, servers_in, [])

        for server in self.servers:
            result  = self._sdk_connection(host_ip=server.ip)
            self.assertTrue(result,"Can create a ssl connection with correct certificate")

        result,cb   = self._sdk_connection(host_ip=self.master.ip)
        create_docs = Thread(name='create_docs', target=self.createBulkDocuments, args=(cb,))
        create_docs.start()

        x509main(self.master)._delete_inbox_folder()
        x509main(self.master)._generate_cert(self.servers,root_cn="CB\ Authority")
        x509main(self.master).setup_master()
        x509main().setup_cluster_nodes_ssl(self.servers,reload_cert=True)


        create_docs.join()

        for server in self.servers:
            result  = self._sdk_connection(host_ip=server.ip)
            self.assertTrue(result,"Can create a ssl connection with correct certificate")

    #Changing from self signed to ca signed, while there is a connection with self-signed
    def test_root_existing_connection_rotate_cert(self):
        rest = RestConnection(self.master)
        rest.create_bucket(bucket='default', ramQuotaMB=100)
        self.sleep(30)
        result = False
        connection_string = 'couchbase://'+ self.master.ip + '/default'
        try:
            cb = Bucket(connection_string)
            if cb is not None:
                result = True
        except Exception, ex:
            print ex
        self.assertTrue(result,"Cannot create a client connection with server")

        create_docs = Thread(name='create_docs', target=self.createBulkDocuments, args=(cb,))
        create_docs.start()
        x509main(self.master).setup_master()
        create_docs.join()
        result  = self._sdk_connection(host_ip=self.master.ip)
        self.assertTrue(result,"Cannot create a security connection with server")

    #Audit test to test /UploadClusterCA
    def test_audit_upload_ca(self):
        x509main(self.master).setup_master()
        expectedResults = {"expires":"2049-12-31T23:59:59.000Z","subject":"CN=Root Authority","ip":self.ip_address, "port":57457,"source":"ns_server", \
                               "user":"Administrator"}
        self.checkConfig(8229, self.master, expectedResults)

    #Audit test for /reloadCA
    def test_audit_reload_ca(self):
        x509main(self.master).setup_master()
        expectedResults = {"expires":"2049-12-31T23:59:59.000Z","subject":"CN="+self.master.ip,"ip":self.ip_address, "port":57457,"source":"ns_server", \
                               "user":"Administrator"}
        self.checkConfig(8230, self.master, expectedResults)


class x509_upgrade(NewUpgradeBaseTest):

    def setUp(self):
        super(x509_upgrade, self).setUp()
        self.initial_version = self.input.param("initial_version",'4.5.0-900')
        self.upgrade_version = self.input.param("upgrade_version", "4.5.0-1069")
        self._reset_original()
        x509main(self.master)._generate_cert(self.servers)
        self.ip_address = self.getLocalIPAddress()
        enable_audit=self.input.param('audit',None)
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

    def _sdk_connection(self,root_ca_path=x509main.CACERTFILEPATH + x509main.CACERTFILE,bucket='default',host_ip=None):
        self.sleep(30)
        result = False
        connection_string = 'couchbases://'+ host_ip + '/' + bucket + '?certpath='+root_ca_path
        print connection_string
        try:
            cb = Bucket(connection_string)
            if cb is not None:
                result = True
                return result, cb
        except Exception, ex:
            print ex
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
        x509main().setup_cluster_nodes_ssl(self.servers,reload_cert=True)

        for server in self.servers:
            result = self._sdk_connection(host_ip=server.ip)
            self.assertTrue(result,"Cannot create a security connection with server")


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
        x509main().setup_cluster_nodes_ssl(self.servers,reload_cert=True)

        for server in self.servers:
            result = self._sdk_connection(host_ip=server.ip)
            self.assertFalse(result,"Can create a security connection with server")

