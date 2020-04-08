import json
from memcached.helper.data_helper import MemcachedClientHelper
from membase.api.rest_client import RestConnection
from basetestcase import BaseTestCase
from couchbase_helper.document import View
from couchbase_helper.documentgenerator import BlobGenerator
from remote.remote_util import RemoteMachineShellConnection
from testconstants import COUCHBASE_FROM_VERSION_4, COUCHBASE_FROM_MAD_HATTER


class CCCP(BaseTestCase):

    def setUp(self):
        super(CCCP, self).setUp()
        self.map_fn = 'function (doc){emit([doc.join_yr, doc.join_mo],doc.name);}'
        self.ddoc_name = "cccp_ddoc"
        self.view_name = "cccp_view"
        self.default_view = View(self.view_name, self.map_fn, None, False)
        self.ops = self.input.param("ops", None)
        self.clients = {}
        try:
            for bucket in self.buckets:
                self.clients[bucket.name] =\
                  MemcachedClientHelper.direct_client(self.master, bucket.name)
        except:
            self.tearDown()

    def tearDown(self):
        super(CCCP, self).tearDown()

    def test_get_config_client(self):
        tasks = self.run_ops()
        for task in tasks:
            if self.ops != 'failover':
                task.result()
        for bucket in self.buckets:
            _, _, config = self.clients[bucket.name].get_config()
            self.verify_config(json.loads(config), bucket)

    def test_get_config_rest(self):
        tasks = self.run_ops()
        for task in tasks:
            if not task:
                self.fail("no task to run")
            if self.ops == 'failover':
                if not task:
                    self.fail("Ops failover failed ")
            else:
                task.result()
        for bucket in self.buckets:
            config = RestConnection(self.master).get_bucket_CCCP(bucket)
            self.verify_config(config, bucket)

    def test_set_config(self):
        """ Negative test for setting bucket config.  """
        tasks = self.run_ops()
        config_expected = 'abcabc'
        for task in tasks:
            task.result()
        for bucket in self.buckets:
            try:
                self.clients[bucket.name].set_config(config_expected)
                _, _, config = self.clients[bucket.name].get_config()
                if econfig_expected == config:
                    self.fail("It should not allow to set this format config ")
            except Exception as e:
                if e and not "Memcached error #4 'Invalid'" in str(e):
                    self.fail("ns server should not allow to set this config format")

    def test_not_my_vbucket_config(self):
        self.gen_load = BlobGenerator('cccp', 'cccp-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, self.gen_load, "create", 0)
        self.cluster.rebalance(self.servers[:self.nodes_init],
                               self.servers[self.nodes_init:self.nodes_init + 1], [])
        self.nodes_init = self.nodes_init + 1
        not_my_vbucket = False
        for bucket in self.buckets:
            while self.gen_load.has_next() and not not_my_vbucket:
                key, _ = next(self.gen_load)
                try:
                    self.clients[bucket.name].get(key)
                except Exception as ex:
                    self.log.info("Config in exception is correct. Bucket %s, key %s"\
                                                                 % (bucket.name, key))
                    config = str(ex)[str(ex).find("Not my vbucket':") \
                                                 + 16 : str(ex).find("for vbucket")]
                    if not config.endswith("}"):
                        config += "}"
                    try:
                        config = json.loads(config)
                    except Exception as e:
                        if "Expecting object" in str(e):
                            config += "}"
                            config = json.loads(config)
                    self.verify_config(config, bucket)
                    """ from watson, only the first error contains bucket details """
                    not_my_vbucket = True

    def verify_config(self, config_json, bucket):
        expected_params = ["nodeLocator", "rev", "uuid", "bucketCapabilitiesVer",
                           "bucketCapabilities"]
        for param in expected_params:
            self.assertTrue(param in config_json, "No %s in config" % param)
        self.assertTrue("name" in config_json and config_json["name"] == bucket.name,
                        "No bucket name in config")
        if self.cb_version[:5] in COUCHBASE_FROM_VERSION_4:
            self.assertTrue(len(config_json["nodesExt"]) == self.nodes_init,
                        "Number of nodes expected %s, actual %s" % (
                                        self.nodes_init, len(config_json["nodesExt"])))
        else:
            self.assertTrue(len(config_json["nodes"]) == self.nodes_init,
                        "Number of nodes expected %s, actual %s" % (
                                        self.nodes_init, len(config_json["nodes"])))
        for node in config_json["nodes"]:
            self.assertTrue("couchApiBase" in node and "hostname" in node,
                            "No hostname name in config")
            if self.cb_version[:5] in COUCHBASE_FROM_MAD_HATTER:
                """ moxi port is removed from Mad-Hatter 6.5.0 """
                self.assertTrue(node["ports"]["direct"] == 11210,
                            "ports are incorrect: %s" % node)
            else:
                self.assertTrue(node["ports"]["proxy"] == 11211 and \
                            node["ports"]["direct"] == 11210,
                            "ports are incorrect: %s" % node)
        self.assertTrue(config_json["ddocs"]["uri"] == \
                       ("/pools/default/buckets/%s/ddocs" % bucket.name),
                        "Ddocs uri is incorrect: %s "
                        % "/pools/default/buckets/default/ddocs")
        self.assertTrue(config_json["vBucketServerMap"]["numReplicas"] == self.num_replicas,
                        "Num replicas is incorrect: %s "
                        % config_json["vBucketServerMap"]["numReplicas"])
        for param in ["hashAlgorithm", "serverList", "vBucketMap"]:
            self.assertTrue(param in config_json["vBucketServerMap"],
                            "%s in vBucketServerMap" % param)
        self.log.info("Bucket %s .Config was checked" % bucket.name)

    def run_ops(self):
        tasks = []
        if not self.ops:
            return tasks
        if self.ops == 'rebalance_in':
            tasks.append(self.cluster.async_rebalance(self.servers[:self.nodes_init],
                       self.servers[self.nodes_init:self.nodes_init + self.nodes_in], []))
            self.nodes_init += self.nodes_in
        elif self.ops == 'rebalance_out':
            tasks.append(self.cluster.async_rebalance(self.servers[:self.nodes_init],
                    [], self.servers[(self.nodes_init - self.nodes_out):self.nodes_init]))
            self.nodes_init -= self.nodes_out
        elif self.ops == 'failover':
            tasks.append(self.cluster.failover(self.servers[:self.nodes_init],
                    self.servers[(self.nodes_init - self.nodes_out):self.nodes_init]))
            self.sleep(20)
            self.nodes_init -= self.nodes_out
        if self.ops == 'create_views':
            views_num = 10
            views = self.make_default_views(self.view_name, views_num, different_map=True)
            tasks.extend(self.async_create_views(self.master, self.ddoc_name, views))
        if self.ops == 'restart':
            servers_to_choose = [serv for serv in self.servers if self.master.ip != serv.ip]
            self.assertTrue(servers_to_choose, "There is only one node in cluster")
            shell = RemoteMachineShellConnection(servers_to_choose[0])
            try:
                shell.stop_couchbase()
                shell.start_couchbase()
            finally:
                shell.disconnect()
            self.sleep(5, "Server %s is starting..." % servers_to_choose[0].ip)
        return tasks
        
