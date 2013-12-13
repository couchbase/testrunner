import json
import time
import testconstants
from rackzone.rackzone_base import RackzoneBaseTest
from memcached.helper.data_helper import  MemcachedClientHelper
from membase.api.rest_client import RestConnection, Bucket
from membase.helper.rebalance_helper import RebalanceHelper
from remote.remote_util import RemoteMachineShellConnection



class RackzoneTests(RackzoneBaseTest):
    def setUp(self):
        super(RackzoneTests, self).setUp()
        self.command = self.input.param("command", "")
        self.zone = self.input.param("zone", 1)
        self.replica = self.input.param("replica", 1)
        self.num_items = self.input.param("items", 10000)
        self.command_options = self.input.param("command_options", '')
        self.set_get_ratio = self.input.param("set_get_ratio", 0.9)
        self.item_size = self.input.param("item_size", 128)
        self.timeout = 6000


    def tearDown(self):
        super(RackzoneTests, self).tearDown()

    def test_check_default_zone_create_by_default(self):
        zone_name = "Group 1"
        self._verify_zone(zone_name)

    def test_create_second_default_zone(self):
        zone_name = "Group 1"
        serverInfo = self.servers[0]
        rest = RestConnection(serverInfo)
        try:
            self.log.info("create additional default zone")
            rest.add_zone(zone_name)
        except Exception,e :
            print e

    def test_create_zone_with_upper_case_name(self):
        zone_name = "ALLWITHUPTERCASE"
        serverInfo = self.servers[0]
        rest = RestConnection(serverInfo)
        try:
            self.log.info("create zone {0}".format(zone_name))
            rest.add_zone(zone_name)
        except Exception,e :
            print e
        self._verify_zone(zone_name)

    def test_create_zone_with_lower_case_name(self):
        zone_name = "allwithlowercaseeeeeee"
        serverInfo = self.servers[0]
        rest = RestConnection(serverInfo)
        try:
            self.log.info("create zone {0}".format(zone_name))
            rest.add_zone(zone_name)
        except Exception,e :
            print e
        self._verify_zone(zone_name)

    def test_create_zone_with_all_number_name(self):
        zone_name = "3223345557666760"
        serverInfo = self.servers[0]
        rest = RestConnection(serverInfo)
        try:
            self.log.info("create zone {0}".format(zone_name))
            rest.add_zone(zone_name)
        except Exception,e :
            print e
        self._verify_zone(zone_name)

    def test_create_zone_with_upper_lower_number_name(self):
        zone_name = "AAABBBCCCaakkkkmmm345672"
        serverInfo = self.servers[0]
        rest = RestConnection(serverInfo)
        try:
            self.log.info("create zone {0}".format(zone_name))
            rest.add_zone(zone_name)
        except Exception,e :
            print e
        self._verify_zone(zone_name)

    def test_create_zone_with_upper_lower_number_and_space_name(self):
        zone_name = " AAAB BBCCC aakkk kmmm3 456 72 "
        serverInfo = self.servers[0]
        rest = RestConnection(serverInfo)
        try:
            self.log.info("create zone {0}".format(zone_name))
            rest.add_zone(zone_name)
        except Exception,e :
            print e
        self._verify_zone(zone_name)

    def test_create_zone_with_none_ascii_name(self):
        # zone name is limited to 64 bytes
        zone_name = "abcdGHIJKLMNOPQRSTUVWXYZ0123456789efghijklmnopqrstuvwyABCDEF_-.%"
        serverInfo = self.servers[0]
        rest = RestConnection(serverInfo)
        try:
            self.log.info("create zone {0}".format(zone_name))
            rest.add_zone(zone_name)
        except Exception,e :
            print e
        self._verify_zone(zone_name)

    def test_delete_empty_defautl_zone(self):
        zone_name ="test1"
        default_zone = "Group 1"
        moved_node = []
        serverInfo = self.servers[0]
        moved_node.append(serverInfo.ip)
        rest = RestConnection(serverInfo)
        try:
            self.log.info("create zone {0}".format(zone_name))
            rest.add_zone(zone_name)
            if rest.is_zone_exist(zone_name):
                rest.shuffle_nodes_in_zones(moved_node, default_zone, zone_name)
                rest.delete_zone(default_zone)
                if not rest.is_zone_exist(default_zone):
                    self.log.info("successful delete default zone")
                else:
                    raise Exception("Failed to delete default zone")
            rest.rename_zone(zone_name, default_zone)
        except Exception,e :
            print e

    def testrza(self):
        serverInfo = self.servers[0]
        rest = RestConnection(serverInfo)
        zones = []
        zones.append("Group 1")
        nodes_in_zone = {}
        nodes_in_zone["Group 1"] = [serverInfo.ip]
        if int(self.zone) > 1:
            for i in range(1,int(self.zone)):
                a = "Group "
                zones.append(a + str(i + 1))
                rest.add_zone(a + str(i + 1))
        servers_rebalanced = []
        self.user = serverInfo.rest_username
        self.password = serverInfo.rest_password
        k = 1
        for i in range(0, self.zone):
            if "Group 1" in zones[i]:
                total_node_per_zone = int(len(self.servers))/int(self.zone) - 1
            else:
                nodes_in_zone[zones[i]] = []
                total_node_per_zone = int(len(self.servers))/int(self.zone)
            for n in range(0,total_node_per_zone):
                nodes_in_zone[zones[i]].append(self.servers[k].ip)
                rest.add_node(user=self.user, password=self.password, remoteIp=self.servers[k].ip, port='8091', zone_name=zones[i])
                k += 1
        otpNodes = [node.id for node in rest.node_statuses()]
        started = rest.rebalance(otpNodes, [])
        if started:
            try:
                result = rest.monitorRebalance()
            except RebalanceFailedException as e:
                log.error("rebalance failed: {0}".format(e))
                return False, servers_rebalanced
            msg = "successfully rebalanced in selected nodes from the cluster ? {0}"
            self.log.info(msg.format(result))
        self._verify_replica_distribution_in_zones(nodes_in_zone, "tap")

    def _verify_zone(self, name):
        serverInfo = self.servers[0]
        rest = RestConnection(serverInfo)
        if rest.is_zone_exist(name.strip()):
            self.log.info("verified! zone '{0}' is existed".format(name.strip()))
        else:
            raise Exception("There is not zone with name: %s in cluster" % name)

    def _verify_replica_distribution_in_zones(self, nodes, commmand, saslPassword = "" ):
        cbstat_command = "%scbstats" % (testconstants.LINUX_COUCHBASE_BIN_PATH)
        shell = RemoteMachineShellConnection(self.servers[0])
        command = "tap"
        saslPassword = ''
        for group in nodes:
            for node in nodes[group]:
                commands = "%s %s:11210 %s -b %s -p \"%s\" |grep :vb_filter: |  awk '{print $1}' | xargs \
                                      | sed 's/eq_tapq:replication_ns_1@//g'  | sed 's/:vb_filter://g'  \
                               " % (cbstat_command, node, command,"default", saslPassword)
                output, error = shell.execute_command(commands)
                output = output[0].split(" ")
                if node not in output:
                    self.log.info("{0}".format(nodes))
                    self.log.info("replica of node {0} are not in its zone {1}".format(node, group))
                else:
                    raise Exception("replica of node {0} are on its own zone {1}".format(node, group))
