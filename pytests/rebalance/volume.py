from threading import Thread
import threading
from lib.testconstants import STANDARD_BUCKET_PORT
from basetestcase import BaseTestCase
from rebalance.rebalance_base import RebalanceBaseTest
from membase.api.rest_client import RestConnection, RestHelper


class VolumeTests(BaseTestCase):
    def setUp(self):
        super(VolumeTests, self).setUp()
        self.zone = self.input.param("zone", 1)
        self.recoveryType = self.input.param("recoveryType", "full")

    def tearDown(self):
        super(VolumeTests, self).tearDown()

    def load(self, server, items, bucket,start_at=0,batch=1000):
        import subprocess
        from lib.testconstants import COUCHBASE_FROM_SPOCK
        rest = RestConnection(server)
        num_cycles = int((items / batch )) / 5
        cmd = "cbc-pillowfight -U couchbase://{0}/{3} -I {1} -m 10 -M 100 -B {2} --populate-only --start-at {4} --json".format(server.ip, items, batch,bucket,start_at)
        if rest.get_nodes_version()[:5] in COUCHBASE_FROM_SPOCK:
            cmd += " -u Administrator -P password"
        self.log.info("Executing '{0}'...".format(cmd))
        rc = subprocess.call(cmd, shell=True)
        if rc != 0:
            self.fail("Exception running cbc-pillowfight: subprocess module returned non-zero response!")

    def check_dataloss(self, server, bucket, num_items):
        from couchbase.bucket import Bucket
        from couchbase.exceptions import NotFoundError,CouchbaseError
        from lib.memcached.helper.data_helper import VBucketAwareMemcached
        self.log.info("########## validating data for bucket : {} ###########".format(bucket))
        bkt = Bucket('couchbase://{0}/{1}'.format(server.ip, bucket.name))
        rest = RestConnection(self.master)
        VBucketAware = VBucketAwareMemcached(rest, bucket.name)
        _, _, _ = VBucketAware.request_map(rest, bucket.name)
        batch_start = 0
        batch_end = 0
        batch_size = 10000
        errors = []
        while num_items > batch_end:
            batch_end = batch_start + batch_size
            keys = []
            for i in xrange(batch_start, batch_end, 1):
                keys.append(str(i).rjust(20, '0'))
            try:
                bkt.get_multi(keys)
                self.log.info("Able to fetch keys starting from {0} to {1}".format(keys[0], keys[len(keys) - 1]))
            except CouchbaseError as e:
                self.log.error(e)
                ok, fail = e.split_results()
                if fail:
                    for key in fail:
                        try:
                            bkt.get(key)
                        except NotFoundError:
                            vBucketId = VBucketAware._get_vBucket_id(key)
                            errors.append("Missing key: {0}, VBucketId: {1}".
                                          format(key, vBucketId))
            batch_start += batch_size
        self.log.info("Total missing keys:{}".format(len(errors)))
        self.log.info(errors)
        return errors


    def test_volume_with_rebalance(self):
        self.src_bucket = RestConnection(self.master).get_buckets()
        rest = RestConnection(self.master)
        bucket = rest.get_buckets()
        # for bk in bucket:
        #     rest.flush_bucket(bk)
        #self.sleep(30)
        #load initial documents
        load_thread=[]
        for b in bucket:
            load_thread.append(Thread(target=self.load, args=(self.master, self.num_items,b)))
        for t in load_thread:
            t.start()
        servers_init = self.servers[:self.nodes_init]
        new_server_list=self.servers[0:self.nodes_init]
        for t in load_thread:
            t.join()
        self.sleep(30)
        #Reload more data for mutations
        load_thread=[]
        for b in bucket:
            load_thread.append(Thread(target=self.load, args=(self.master, self.num_items,b,self.num_items)))
        for t in load_thread:
            t.start()
        # #Rebalance in 1 node
        self.log.info("==========rebalance in 1 node=========")
        servers_in=self.servers[self.nodes_init:self.nodes_init + 1]
        rebalance = self.cluster.async_rebalance(servers_init,
                                                 servers_in, [])

        rebalance.result()
        for t in load_thread:
            t.join()
        for b in bucket:
         self.check_dataloss(self.master, b,self.num_items*2)
        # load more document
        load_thread = []
        for b in bucket:
            load_thread.append(Thread(target=self.load, args=(self.master, self.num_items, b,self.num_items*2)))
        for t in load_thread:
            t.start()
        #rebalance out 1 node
        new_server_list = self.servers[0:self.nodes_init]+ servers_in
        self.log.info("==========rebalance out 1 node=========")
        servers_out=[self.servers[self.nodes_init]]
        rebalance = self.cluster.async_rebalance(servers_init,[],
                                                 servers_out)
        rebalance.result()
        for t in load_thread:
            t.join()
        for b in bucket:
         self.check_dataloss(self.master, b,self.num_items*3)
        self.sleep(30)
         # load more document
        load_thread = []
        for b in bucket:
            load_thread.append(Thread(target=self.load, args=(self.master, self.num_items, b, self.num_items*3)))
        for t in load_thread:
            t.start()
        new_server_list=list(set(new_server_list)- set(servers_out))
        #swap rebalance 1 node
        self.log.info("==========swap rebalance 1 node=========")
        servers_in = self.servers[self.nodes_init : self.nodes_init + 1]
        servers_init = self.servers[:self.nodes_init]
        servers_out = self.servers[(self.nodes_init - 1) : self.nodes_init]

        rebalance = self.cluster.async_rebalance(servers_init, servers_in, servers_out)
        rebalance.result()
        for t in load_thread:
            t.join()
        self.sleep(30)
        for b in bucket:
         self.check_dataloss(self.master, b,self.num_items*4)
        # load more document
        load_thread = []
        for b in bucket:
            load_thread.append(Thread(target=self.load, args=(self.master, self.num_items, b, self.num_items*4)))
        for t in load_thread:
            t.start()
        new_server_list=list(set(new_server_list + servers_in) - set(servers_out))
        self.log.info("==========Rebalance out of 2 nodes and Rebalance In 1 node=========")
        # Rebalance out of 2 nodes and Rebalance In 1 node
        servers_in = [list(set(self.servers) - set(new_server_list))[0]]
        servers_out = list(set(new_server_list) - set([self.master]))[-2:]
        rebalance = self.cluster.async_rebalance(servers_init, servers_in, servers_out)
        rebalance.result()
        for t in load_thread:
            t.join()
        for b in bucket:
         self.check_dataloss(self.master, b,self.num_items*5)
        self.sleep(30)
        # load more document
        load_thread = []
        for b in bucket:
            load_thread.append(Thread(target=self.load, args=(self.master, self.num_items, b, self.num_items*5)))
        for t in load_thread:
            t.start()
        new_server_list=list(set(new_server_list + servers_in) - set(servers_out))
        self.log.info("new server list:", new_server_list)
        self.log.info("==========Rebalance out of 1 nodes and Rebalance In 2 nodes=========")
        #Rebalance out of 1 nodes and Rebalance In 2 nodes
        servers_in = list(set(self.servers) - set(new_server_list))[0:2]
        servers_out = list(set(new_server_list) - set([self.master]))[0:1]
        rebalance = self.cluster.async_rebalance(servers_init, servers_in, servers_out)
        rebalance.result()
        for t in load_thread:
            t.join()
        for b in bucket:
         self.check_dataloss(self.master, b,self.num_items*6)
        self.sleep(30)
        # load more document
        load_thread = []
        for b in bucket:
            load_thread.append(Thread(target=self.load, args=(self.master, self.num_items, b, self.num_items*6)))
        for t in load_thread:
            t.start()
        new_server_list=list(set(new_server_list + servers_in) - set(servers_out))
        self.log.info("==========Rebalance in 4 nodes =========")
        #Rebalance in 4 nodes
        servers_in = list(set(self.servers) - set(new_server_list))[0:4]
        rebalance = self.cluster.async_rebalance(servers_init, servers_in, [])
        rebalance.result()
        for t in load_thread:
            t.join()
        for b in bucket:
         self.check_dataloss(self.master, b,self.num_items*7)
        self.sleep(30)
        # load more document
        load_thread = []
        for b in bucket:
            load_thread.append(Thread(target=self.load, args=(self.master, self.num_items, b, self.num_items*7)))
        for t in load_thread:
            t.start()
        new_server_list=list(set(servers_init + servers_in) - set(servers_out))
        self.log.info("==========Rebalance out 4 nodes =========")
        #Rebalance out 4 nodes
        servers_out = list(set(new_server_list) - set([self.master]))[0:4]
        rebalance = self.cluster.async_rebalance(servers_init, [], servers_out)
        rebalance.result()
        for t in load_thread:
            t.join()
        for b in bucket:
         self.check_dataloss(self.master, b,self.num_items*8)
        self.sleep(30)
        # load more document
        load_thread = []
        for b in bucket:
            load_thread.append(Thread(target=self.load, args=(self.master, self.num_items, b, self.num_items*8)))
        for t in load_thread:
            t.start()
        new_server_list = list(set(servers_init + servers_in) - set(servers_out))
        self.log.info("======Rebalance in 4 nodes (8 nodes) wait for rebalance to finish and move between server groups=========")
        #Rebalance in 4 nodes (8 nodes) wait for rebalance to finish and move between server groups
        servers_in = list(set(self.servers) - set(new_server_list))[0:4]
        rebalance = self.cluster.async_rebalance(servers_init, servers_in, [])
        rebalance.result()
        self.shuffle_nodes_between_zones_and_rebalance()
        self.log.info("======Graceful failover 1 KV node and add back(Delta and Full)=========")
        #Graceful failover 1 KV node and add back(Delta and Full)
        kv_server = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=False)
        fail_over_task = self.cluster.async_failover([self.master], failover_nodes=[kv_server], graceful=True)
        fail_over_task.result()
        self.sleep(120)
        # do a recovery and rebalance
        rest.set_recovery_type('ns_1@' + kv_server.ip, recoveryType=self.recoveryType)
        rest.add_back_node('ns_1@' + kv_server.ip)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [])
        rebalance.result()

    def shuffle_nodes_between_zones_and_rebalance(self, to_remove=None):
        """
        Shuffle the nodes present in the cluster if zone > 1. Rebalance the nodes in the end.
        Nodes are divided into groups iteratively i.e. 1st node in Group 1, 2nd in Group 2, 3rd in Group 1 and so on, when
        zone=2.
        :param to_remove: List of nodes to be removed.
        """
        if not to_remove:
            to_remove = []
        serverinfo = self.servers[0]
        rest = RestConnection(serverinfo)
        zones = ["Group 1"]
        nodes_in_zone = {"Group 1": [serverinfo.ip]}
        # Create zones, if not existing, based on params zone in test.
        # Shuffle the nodes between zones.
        if int(self.zone) > 1:
            for i in range(1, int(self.zone)):
                a = "Group "
                zones.append(a + str(i + 1))
                if not rest.is_zone_exist(zones[i]):
                    rest.add_zone(zones[i])
                nodes_in_zone[zones[i]] = []
            # Divide the nodes between zones.
            nodes_in_cluster = [node.ip for node in self.get_nodes_in_cluster()]
            nodes_to_remove = [node.ip for node in to_remove]
            for i in range(1, len(self.servers)):
                if self.servers[i].ip in nodes_in_cluster and self.servers[i].ip not in nodes_to_remove:
                    server_group = i % int(self.zone)
                    nodes_in_zone[zones[server_group]].append(self.servers[i].ip)
            # Shuffle the nodesS
            for i in range(1, self.zone):
                node_in_zone = list(set(nodes_in_zone[zones[i]]) -
                                    set([node for node in rest.get_nodes_in_zone(zones[i])]))
                rest.shuffle_nodes_in_zones(node_in_zone, zones[0], zones[i])
        otpnodes = [node.id for node in rest.node_statuses()]
        nodes_to_remove = [node.id for node in rest.node_statuses() if node.ip in [t.ip for t in to_remove]]
        # Start rebalance and monitor it.
        started = rest.rebalance(otpNodes=otpnodes, ejectedNodes=nodes_to_remove)
        if started:
            result = rest.monitorRebalance()
            msg = "successfully rebalanced cluster {0}"
            self.log.info(msg.format(result))
        # Verify replicas of one node should not be in the same zone as active vbuckets of the node.
        if self.zone > 1:
            self._verify_replica_distribution_in_zones(nodes_in_zone)