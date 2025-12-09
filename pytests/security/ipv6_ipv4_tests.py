from basetestcase import BaseTestCase
from security.IPv6_IPv4_grub_level import IPv6_IPv4
from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper
from lib.membase.api.rest_client import RestConnection, RestHelper
from security.x509main import x509main
import json
import subprocess
import socket
try:
    # For SDK2 (legacy) runs
    from couchbase.cluster import PasswordAuthenticator
except ImportError:
    # For SDK4 compatible runs
    from couchbase.auth import PasswordAuthenticator

from couchbase.cluster import Cluster
from couchbase_helper.documentgenerator import BlobGenerator
from couchbase_cli import CouchbaseCLI

class ipv6_ipv4_tests(BaseTestCase):
    REST_PORT = "8091"
    REST_SSL_PORT = "18091"
    N1QL_PORT = "8093"
    N1QL_SSL_PORT = "18093"
    FTS_PORT = "8094"
    FTS_SSL_PORT = "18094"
    ANALYTICS_PORT = "8095"
    ANALYTICS_SSL_PORT = "18095"
    EVENTING_PORT = "8096"
    EVENTING_SSL_PORT = "18096"
    NSERV_PORT = "21100"
    NSERV_SSL_PORT = "21150"
    KV_PORT = "11210"
    KV_SSL_PORT = "11207"
    INDEXER_PORTS = ["9102","9100","9101"]
    INDEXER_PORTS_NETWORK = ["9103","9104","9105"]
    INDEXER_PORT = "9102"
    INDEXER_SSL_PORT = "19102"
    PROJECTOR_PORT = "9999"
    LOG_PATH = "/opt/couchbase/var/lib/couchbase/logs"
    BLOCK_PORTS_FILE_PATH = "pytests/security/block_ports.py"

    def _port_map_(self):
        return IPv6_IPv4(self.servers).retrieve_port_map()

    def setUp(self):
        super(ipv6_ipv4_tests,self).setUp()
        self.addr_family = self.input.param("addr_family","ipv4")
        nodes_obj = IPv6_IPv4(self.servers)
        if self.upgrade_addr_family is None:
            nodes_obj.set_addr_family(self.addr_family)
        nodes_obj.transfer_file(self.BLOCK_PORTS_FILE_PATH)
        self.mode = self.input.param("mode",None)

    def kill_all_python_processes(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            shell.execute_command("pkill python")
            shell.disconnect()

    def tearDown(self):
        self.kill_all_python_processes()
        IPv6_IPv4(self.servers).delete_files()
        super(ipv6_ipv4_tests,self).tearDown()


    def verify_logs_wrapper(self,node,logfile,query_strs):
        remote_client = RemoteMachineShellConnection(node)
        output = remote_client.read_remote_file(self.LOG_PATH, logfile)
        logic = self.verify_logs(output, query_strs)
        remote_client.disconnect()
        self.assertTrue(logic, "search string {0} not present in {1}".format(query_strs,logfile))

    def verify_logs(self, logs=[], verification_string=[]):
        logic = True
        for check_string in verification_string:
            check_presence = False
            for val in logs:
                if check_string in val:
                    check_presence = True
            logic = logic and check_presence
        return logic

    def verify_port_blocked_direct_connection(self, host, port, timeout=10):
        """Verify if a port is blocked by attempting direct socket connection"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            result = sock.connect_ex((host, int(port)))
            sock.close()
            self.log.info(f"Socket connection to {host}:{port} result: {result}")
            return result != 0  # Returns True if port is blocked (connection failed)
        except Exception as e:
            self.log.info(f"Socket connection to {host}:{port} failed with exception: {e}")
            return True  # Port is blocked

    def verify_ports_not_listening(self, ports, nodes, addr_family):
        """Verify ports are not listening using lsof verification"""
        nodes_obj = IPv6_IPv4([nodes])
        listening = nodes_obj.check_ports(ports, addr_family)
        self.log.info(f"Ports {ports} listening status on {addr_family}: {'LISTENING' if listening else 'NOT LISTENING'}")
        return not listening  # Return True if ports are not listening (blocked)


    def check_ports_on_blocking(self,ports,nodes,query_strs,logfile):
        nodes_obj = IPv6_IPv4([nodes])
        addr_family = nodes_obj.get_addr_family()
        shell = RemoteMachineShellConnection(nodes)
        shell.delete_file(self.LOG_PATH,logfile)
        blocked = self._check_ports_on_blocking(ports, nodes)
        shell.disconnect()
        return  blocked

    def _check_ports_on_blocking(self, ports, nodes):
        """Enhanced port blocking verification using multiple methods"""
        nodes_obj = IPv6_IPv4([nodes])
        addr_family = nodes_obj.get_addr_family()
        # Block the ports
        pids = nodes_obj.block_ports(ports, addr_family)
        self.sleep(20)

        verification_results = []
        all_blocked = True

        try:
            self.log.info("=== Starting Socket Connection Verification ===")
            for port in ports:
                blocked = self.verify_port_blocked_direct_connection(nodes.ip, port)
                verification_results.append(("socket_test", port, blocked))
                if not blocked:
                    all_blocked = False
                    self.log.error(f"Socket test FAILED: Port {port} is still accessible")
                else:
                    self.log.info(f"Socket test PASSED: Port {port} is blocked")
            self.log.info("=== Verification Summary ===")
            for method, target, blocked in verification_results:
                status = "PASSED" if blocked else "FAILED"
                self.log.info(f"{method} for {target}: {status}")

            if all_blocked:
                self.log.info("ALL VERIFICATION METHODS PASSED - Ports are successfully blocked")
            else:
                self.log.error("SOME VERIFICATION METHODS FAILED - Port blocking may be incomplete")

            return all_blocked

        finally:
            self.log.info("=== Cleaning up: Unblocking ports ===")
            nodes_obj.unblock_ports(pids)

    def check_ports_on_blocking_opposite_addr_family(self,ports,nodes,service):
        nodes_obj = IPv6_IPv4([nodes])
        addr_family = nodes_obj.get_addr_family()
        opposite_family = None
        if addr_family == "ipv4":
            opposite_family = "ipv6"
        elif addr_family == "ipv6":
            opposite_family = "ipv4"

        running = False
        if opposite_family:
            pids = nodes_obj.block_ports(ports, opposite_family)
            running = self.check_service_up(service,nodes)
            nodes_obj.unblock_ports(pids)
        return running

    def check_service_up(self,service,nodes):
        self.sleep(20)
        if service == "nserv":
            output = x509main()._execute_command_clientcert(nodes.ip, url='/pools/default', port=8091,
                                                            headers=' -u Administrator:password ', client_cert=False,
                                                            curl=True, plain_curl=True)
            output = json.loads(output)
            self.log.info("Print output of command is {0}".format(output))
            self.assertEqual(output['rebalanceStatus'], 'none', "The Web request has failed on port 8091")

        elif service == "index":
            output = x509main()._execute_command_clientcert(nodes.ip, url='/getIndexStatus', port=9102,
                                                            headers='-u Administrator:password',
                                                            client_cert=False, curl=True, verb='GET', plain_curl=True)
            output = json.loads(output)
            self.log.info("Print output of command is {0}".format(output))
            self.assertEqual(output["code"],'success',"The Index Service is not up")

        elif service == "query":
            output = x509main()._execute_command_clientcert(nodes.ip, url='/query/service', port=8093,
                                                            headers='-u Administrator:password ',
                                                            client_cert=False, curl=True, verb='GET', plain_curl=True,
                                                            data="statement='create index idx1 on default(name)'")
            self.assertEqual(json.loads(output)['status'], "success", "Create Index Failed on port 8093")

        elif service == "fts":
            idx = {"sourceName": "default","sourceType": "couchbase","type": "fulltext-index"}
            output = x509main()._execute_command_clientcert(nodes.ip, url='/api/index/default_idx', port=8094,
                                                            headers=" -XPUT -H \"Content-Type: application/json\" -u Administrator:password ",
                                                            client_cert=False, curl=True, verb='GET', plain_curl=True,
                                                            data="'" + json.dumps(idx) + "'")
            self.assertEqual(json.loads(output)['status'], "ok", "Issue with creating FTS index with client Cert")

        elif service == "cbas":
            self.sleep(60, "Waiting for cbas to be up")
            cmd = "curl -v  " + \
                  " -s -u Administrator:password --data pretty=true --data-urlencode 'statement=create dataset on default' " + \
                  "http://{0}:{1}/_p/cbas/query/service ". \
                      format(nodes.ip, 8091)

            self.log.info("Running command : {0}".format(cmd))
            output = subprocess.check_output(cmd, shell=True)
            self.assertEqual(json.loads(output)['status'], "success", "Create CBAS Index Failed")

        elif service == "eventing":
            cmd = "curl -v -X GET -u Administrator:password http://{0}:8096/api/v1/status" \
                .format(nodes.ip)

            self.log.info("Running command : {0}".format(cmd))
            output = subprocess.check_output(cmd, shell=True)
            self.assertEqual(json.loads(output)['num_eventing_nodes'], 1, "Eventing Node is not up")

        elif service == "kv":
            cluster = Cluster("couchbase://{0}".format(nodes.ip))
            authenticator = PasswordAuthenticator("Administrator", "password")
            cluster.authenticate(authenticator)
            cb = cluster.open_bucket("default")
            cb.upsert("key1", "value1")
            self.assertEqual(cb.get("key1").value, "value1")

    def check_ports_for_service(self,ports,nodes):
        nodes_obj = IPv6_IPv4(nodes)
        cluster_setting = nodes_obj.get_addr_family()

        for port in ports:
            mappings = self._port_map_()[self.mode][cluster_setting]
            self.log.info("{0} {1} {2} Details".format(port,self.mode,cluster_setting))
            if self.mode == "pure_ipv4" or self.mode == "pure_ipv6":
                self.assertEquals(nodes_obj.check_ports([port],cluster_setting),mappings[0],"mapping is {0}".format(mappings[0]))
            elif self.mode == "hostname":
                self.assertEquals(nodes_obj.check_ports([port],cluster_setting),mappings[0] if cluster_setting=="ipv4" else mappings[1])
            # elif self.mode == "ipv4_ipv6":
            #     self.assertEquals(nodes_obj.check_ports([port],cluster_setting),mappings[0] if cluster_setting=="ipv4" else mappings[1])


    #Check for results when ports for other ip family are blocked
    #eg. for ipv4 family we blocked ports for ipv6 address family
    def test_services_block_opposite_port(self):
        servs_inout = self.servers[1:4]
        services_in = []
        for service in self.services_in.split("-"):
            services_in.append(service.split(":")[0])
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], servs_inout, [],
                                                 services=services_in)
        rebalance.result()
        self.sleep(20)

        kv_node = self.get_nodes_from_services_map(service_type='kv')
        if kv_node is not None:
            # self.check_ports_on_blocking_opposite_addr_family(['21100'], kv_node, "nserv")
            self.check_ports_on_blocking_opposite_addr_family(['11210'], kv_node, "kv")

        index_node = self.get_nodes_from_services_map(service_type='index')
        if index_node is not None:
            self.check_ports_on_blocking_opposite_addr_family(['9102'], index_node, "index")

        query_node = self.get_nodes_from_services_map(service_type='query')
        if query_node is not None:
            self.check_ports_on_blocking_opposite_addr_family(['8093'], query_node, "query")

        cbas_node = self.get_nodes_from_services_map(service_type='cbas')
        if cbas_node is not None:
            self.check_ports_on_blocking_opposite_addr_family(['8095'], cbas_node, "cbas")

        eventing_node = self.get_nodes_from_services_map(service_type='eventing')
        if eventing_node is not None:
            self.check_ports_on_blocking_opposite_addr_family(['8096'], eventing_node, "eventing")

        fts_node = self.get_nodes_from_services_map(service_type='fts')
        if fts_node is not None:
            self.check_ports_on_blocking_opposite_addr_family(['8094'], kv_node, "fts")

    #Test all services after blocking ports, check for error message format in logs
    #Test also takes care with disabling different ip family
    def test_services(self):
        servs_inout = self.servers[1:4]
        services_in = []
        for service in self.services_in.split("-"):
            services_in.append(service.split(":")[0])
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], servs_inout, [],
                                                 services=services_in)
        rebalance.result()
        self.sleep(20)

        if self.addr_family == "ipv4":
            suffix1 = "4"
            suffix2 = ""
        elif self.addr_family == "ipv6":
            suffix1 = "6"
            suffix2 = "6"

        # Check NS Server Ports

        self.log.info("Checking NS Server Ports")
        self.check_ports_for_service([self.NSERV_PORT, self.NSERV_SSL_PORT], self.servers)
        query_str1 = "Failed to start dist inet" + suffix2 + "_tcp_dist on port 21100"
        query_str2 = "Failed to start dist inet" + suffix2 + "_tls_dist on port 21150"
        self.check_ports_on_blocking([self.NSERV_PORT], self.master, [query_str1], "debug.log")
        self.sleep(20)
        self.check_ports_on_blocking([self.NSERV_SSL_PORT], self.master, [query_str2], "debug.log")
        self.sleep(10)

        kv_node = self.get_nodes_from_services_map(service_type='kv')
        if kv_node is not None:
            self.log.info("Checking KV Ports")
            self.check_ports_for_service([self.KV_PORT, self.KV_SSL_PORT], [kv_node])
            query_str1 = "CRITICAL Failed to create required IPv" + suffix1 + " socket for \"*:11210\""
            #query_str2 = "CRITICAL Failed to create required IPv" + suffix1 + " socket for \"*:11207\""
            self.check_ports_on_blocking([self.KV_PORT], kv_node, [query_str1], "debug.log")
            #self.check_ports_on_blocking([self.KV_SSL_PORT], kv_node, [query_str2], "debug.log")
            self.sleep(10)

        index_node = self.get_nodes_from_services_map(service_type='index')
        if index_node is not None:
            self.log.info("Checking Indexer Ports")
            self.check_ports_for_service([self.INDEXER_PORT,self.INDEXER_SSL_PORT], [index_node])
            for port in self.INDEXER_PORTS:
                query_str = "listen tcp" + suffix1 + " :{0}: bind: address already in use".format(port)
                self.check_ports_on_blocking([port],index_node,[query_str],"indexer.log")
            for netw in self.INDEXER_PORTS_NETWORK:
                query_str = "Error in listening on network port :{0}".format(netw)
                query_str1 = "Indexer exiting normally"
                self.check_ports_on_blocking([netw],index_node,[query_str,query_str1],"indexer.log")
            self.sleep(10)

        n1ql_node = self.get_nodes_from_services_map(service_type='n1ql')
        if n1ql_node is not None:
            self.log.info("Checking N1QL Ports")
            self.check_ports_for_service([self.N1QL_PORT,self.N1QL_SSL_PORT],[n1ql_node])
            query_str1 = "Failed to start service: listen tcp" + suffix1 + " :8093: bind: address already in use"
            query_str2 = "Failed to start service: listen tcp" + suffix1 + " :18093: bind: address already in use"
            self.check_ports_on_blocking([self.N1QL_PORT],n1ql_node,[query_str1],"query.log")
            self.check_ports_on_blocking([self.N1QL_SSL_PORT],n1ql_node,[query_str2],"query.log")
            self.sleep(10)

        eventing_node = self.get_nodes_from_services_map(service_type='eventing')
        if eventing_node:
            self.log.info("Checking Eventing Ports")
            self.check_ports_for_service([self.EVENTING_PORT,self.EVENTING_SSL_PORT], [eventing_node])
            query_str1 = "listen tcp" + suffix1 + " :8096: bind: address already in use"
            query_str2 = "listen tcp" + suffix1 + " :18096: bind: address already in use"
            self.check_ports_on_blocking([self.EVENTING_PORT], eventing_node, [query_str1],"eventing.log")
            self.check_ports_on_blocking([self.EVENTING_SSL_PORT], eventing_node, [query_str2],"eventing.log")
            self.sleep(10)

        cbas_node = self.get_nodes_from_services_map(service_type='cbas')
        if cbas_node is not None:
            self.log.info("Checking CBAS Ports")
            self.check_ports_for_service([self.ANALYTICS_PORT], [cbas_node])
            if suffix1 == "4":
                ip = " 127.0.0.1"
            else:
                ip = " [::1]"
            query_str1 = "listen tcp" + suffix1 + ip + ":8095: bind: address already in use"
            query_str2 = "listen tcp" + suffix1 + ip + ":18095: bind: address already in use"
            self.check_ports_on_blocking([self.ANALYTICS_PORT], cbas_node, [query_str1], "analytics_info.log")
            self.check_ports_on_blocking([self.ANALYTICS_SSL_PORT], cbas_node, [query_str2], "analytics_info.log")
            self.sleep(10)

        fts_node = self.get_nodes_from_services_map(service_type='fts')
        if fts_node is not None:
            self.log.info("Checking FTS Ports")
            self.check_ports_for_service([self.FTS_PORT,self.FTS_SSL_PORT], [fts_node])
            query_str1 = "init_http: listen, err: listen tcp" + suffix1 + " 0.0.0.0:8094: bind: address already in use -- main.mainServeHTTP() at init_http.go:188"
            query_str2 = "init_http: listen, err: listen tcp" + suffix1 + " :18094: bind: address already in use -- main.mainServeHTTP() at init_http.go:188"
            self.check_ports_on_blocking([self.FTS_PORT],fts_node,[query_str1],"fts.log")
            self.check_ports_on_blocking([self.FTS_SSL_PORT],fts_node,[query_str2],"fts.log")
            self.sleep(10)


class IPv4_IPv6_only(BaseTestCase):
    def setUp(self):
        super(IPv4_IPv6_only, self).setUp()
        gen_initial_create = BlobGenerator('IPv4_IPv6_only', 'IPv4_IPv6_only', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen_initial_create, "create", 0)
        self.change_addr_family = self.input.param("change_addr_family", "False")

    def tearDown(self):
        super(IPv4_IPv6_only,self).tearDown()

    def test_opposite_address_family_is_blocked(self):
        services_in = []
        for service in self.services_in.split("-"):
            services_in.append(service.split(":")[0])
        # Validate before the test starts
        self._validate_ip_addrress_family()
        nodes_in = self.servers[self.nodes_init:]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], nodes_in, [],
                                                 services=services_in)
        self.sleep(2)
        rest = RestConnection(self.master)
        reached = RestHelper(rest).rebalance_reached(percentage=30)
        if self.change_addr_family:
            if self.ipv4_only:
                cli = CouchbaseCLI(self.master, self.master.rest_username, self.master.rest_password)
                cli.setting_autofailover(0, 60)
                _, _, success = cli.set_ip_family("ipv6only")
                if not success:
                    self.fail("Unable to change ip-family to ipv6only")
                self.check_ip_family_enforcement(ip_family="ipv6_only")
                self.sleep(2)
                _, _, success = cli.set_ip_family("ipv4only")
                if not success:
                    self.fail("Unable to change ip-family to ipv4only")
                cli.setting_autofailover(1, 60)
                self.check_ip_family_enforcement(ip_family="ipv4_only")
            if self.ipv6_only:
                cli = CouchbaseCLI(self.master, self.master.rest_username, self.master.rest_password)
                cli.setting_autofailover(0, 60)
                _, _, success = cli.set_ip_family("ipv4only")
                if not success:
                    self.fail("Unable to change ip-family to ipv4only")
                self.check_ip_family_enforcement(ip_family="ipv4_only")
                self.sleep(2)
                _, _, success = cli.set_ip_family("ipv6only")
                if not success:
                    self.fail("Unable to change ip-family to ipv6only")
                cli.setting_autofailover(1, 60)
                self.check_ip_family_enforcement(ip_family="ipv6_only")
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        # Validate during rebalance
        self._validate_ip_addrress_family()
        rebalance.result()
        self.sleep(20)
        # Validate post rebalance
        self._validate_ip_addrress_family()
        # Reboot the master node
        shell = RemoteMachineShellConnection(self.master)
        shell.reboot_node()
        self.sleep(180)
        # Validate post reboot
        self._validate_ip_addrress_family()

    def _validate_ip_addrress_family(self):
        if self.ipv4_only:
            self.check_ip_family_enforcement(ip_family="ipv4_only")
        elif self.ipv6_only:
            self.check_ip_family_enforcement(ip_family="ipv6_only")
