import json
import time
from rackzone.rackzone_base import RackzoneBaseTest
from memcached.helper.data_helper import  MemcachedClientHelper
from membase.api.rest_client import RestConnection, Bucket


class rackzoneTests(RackzoneBaseTest):
    def setUp(self):
        super(rackzoneTests, self).setUp()
        self.command = self.input.param("command", "")
        self.vbucketId = self.input.param("vbid", -1)
        self.timeout = 6000
        self.num_items = self.input.param("items", 1000)
        self.command_options = self.input.param("command_options", '')
        self.set_get_ratio = self.input.param("set_get_ratio", 0.9)
        self.item_size = self.input.param("item_size", 128)


    def tearDown(self):
        super(rackzoneTests, self).tearDown()

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
        #zone_name = "$$$&***&&%"
        #zone_name = "abcdGHIJKLMNOPQRSTUVWXYZ0123456789_-.%efghijklmnopqrstuvxyABCDE"
        zone_name = "abcdGHIJKLMNOPQRSTUVWXYZ0123456789efghijklmnopqrstuvwyABCDEF_-.%"
        serverInfo = self.servers[0]
        rest = RestConnection(serverInfo)
        try:
            self.log.info("create zone {0}".format(zone_name))
            rest.add_zone(zone_name)
            #time.sleep(10)
        except Exception,e :
            print e
        self._verify_zone(zone_name)

    def test_delete_empty_defautl_zone(self):
        zone_name ="test1"
        default_zone = "Group 1"
        moved_node = []
        serverInfo = self.servers[0]
        moved_node.append(serverInfo.ip)
        print moved_node
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


    def _verify_zone(self, name):
        serverInfo = self.servers[0]
        rest = RestConnection(serverInfo)
        if rest.is_zone_exist(name.strip()):
            self.log.info("verified! zone '{0}' is existed".format(name.strip()))
        else:
            raise Exception("There is not zone with name: %s in cluster" % name)
