from threading import Thread
import threading
from lib.testconstants import STANDARD_BUCKET_PORT
from couchbase_helper.document import DesignDocument, View
from basetestcase import BaseTestCase
from rebalance.rebalance_base import RebalanceBaseTest
from membase.api.rest_client import RestConnection, RestHelper


class VolumeTests(BaseTestCase):
    def setUp(self):
        super(VolumeTests, self).setUp()
        self.zone = self.input.param("zone", 1)
        self.recoveryType = self.input.param("recoveryType", "full")
        self.ddocs = []
        self.default_view_name = "upgrade-test-view"
        self.ddocs_num = self.input.param("ddocs-num", 0)
        self.view_num = self.input.param("view-per-ddoc", 2)
        self.is_dev_ddoc = self.input.param("is-dev-ddoc", False)

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
                            vBucketId = VBucketAware._get_vBucket_id(key,timeout=500)
                            errors.append("Missing key: {0}, VBucketId: {1}".
                                          format(key, vBucketId))
            batch_start += batch_size
        self.log.info("Total missing keys:{}".format(len(errors)))
        self.log.info(errors)
        return errors

    def create_ddocs_and_views(self):
        self.default_view = View(self.default_view_name, None, None)
        for bucket in self.buckets:
            for i in xrange(int(self.ddocs_num)):
                views = self.make_default_views(self.default_view_name, self.view_num,
                                               self.is_dev_ddoc, different_map=True)
                ddoc = DesignDocument(self.default_view_name + str(i), views)
                self.ddocs.append(ddoc)
                for view in views:
                    self.cluster.create_view(self.master, ddoc.name, view, bucket=bucket)

    def test_volume_with_rebalance(self):
        self.src_bucket = RestConnection(self.master).get_buckets()
        rest = RestConnection(self.master)
        bucket = rest.get_buckets()
        # for bk in bucket:
        #     rest.flush_bucket(bk)
        #self.sleep(30)
        #load initial documents
        self.create_ddocs_and_views()
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
        new_server_list=list(set(new_server_list + servers_in))
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
        new_server_list = list(set(new_server_list) - set(servers_out))
        self.log.info("======Rebalance in 4 nodes (8 nodes) wait for rebalance to finish and move between server groups=========")
        #Rebalance in 4 nodes (8 nodes) wait for rebalance to finish and move between server groups
        servers_in = list(set(self.servers) - set(new_server_list))[0:4]
        rebalance = self.cluster.async_rebalance(servers_init, servers_in, [])
        rebalance.result()
        for t in load_thread:
            t.join()
        for b in bucket:
         self.check_dataloss(self.master, b,self.num_items*9)
        self.sleep(30)
        load_thread = []
        for b in bucket:
            load_thread.append(Thread(target=self.load, args=(self.master, self.num_items, b, self.num_items * 9)))
        for t in load_thread:
            t.start()
        self.shuffle_nodes_between_zones_and_rebalance()
        for t in load_thread:
            t.join()
        for b in bucket:
         self.check_dataloss(self.master, b,self.num_items*10)
        self.sleep(30)
        load_thread = []
        for b in bucket:
            load_thread.append(Thread(target=self.load, args=(self.master, self.num_items, b, self.num_items * 10)))
        for t in load_thread:
            t.start()
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
        for t in load_thread:
            t.join()
        for b in bucket:
         self.check_dataloss(self.master, b,self.num_items*11)
        self.sleep(30)

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

    def load_buckets_with_high_ops(self, server, bucket, items, batch=20000, threads=5, start_document=0, instances=1):
        import subprocess
        cmd_format = "python scripts/high_ops_doc_loader.py  --node {0} --bucket {1} --user {2} --password {3} " \
                     "--count {4} " \
                     "--batch_size {5} --threads {6} --start_document {7}"
        if instances > 1:
            cmd = cmd_format.format(server.ip, bucket.name, server.rest_username, server.rest_password,
                                    int(items) / int(instances), batch, threads, start_document)
        else:
            cmd = cmd_format.format(server.ip, bucket.name, server.rest_username, server.rest_password, items, batch,
                                    threads, start_document)
        if instances > 1:
            for i in range(1, instances):
                count = int(items) / int(instances)
                start = count * i + int(start_document)
                if i == instances - 1:
                    count = items - (count * i)
                cmd = "{} & {}".format(cmd, cmd_format.format(server.ip, bucket.name, server.rest_username,
                                                              server.rest_password, count, batch, threads, start))
        if RestConnection(server).get_nodes_version()[:5] < '5':
            cmd = cmd.replace("--user {0} --password {1} ".format(server.rest_username, server.rest_password), '')
        self.log.info("Running {}".format(cmd))
        result = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output = result.stdout.read()
        error = result.stderr.read()
        if error:
            self.log.error(error)
            self.fail("Failed to run the loadgen.")
        if output:
            loaded = output.split('\n')[:-1]
            total_loaded = 0
            for load in loaded:
                total_loaded += int(load)
            self.assertEqual(total_loaded, items,
                             "Failed to load {} items. Loaded only {} items".format(items, total_loaded))

    def load_docs(self, num_items=0, start_document=0):
        if self.loader == "pillowfight":
            load_thread = Thread(target=self.load,
                                 name="pillowfight_load",
                                 args=(self.master, self.num_items, self.batch_size, self.doc_size, self.rate_limit))
            return load_thread
        elif self.loader == "high_ops":
            if num_items == 0:
                num_items = self.num_items
            load_thread = Thread(target=self.load_buckets_with_high_ops,
                                 name="high_ops_load",
                                 args=(self.master, self.buckets[0], num_items, self.batch_size,
                                       self.threads, start_document, self.instances))
            return load_thread

    def check_data(self, server, bucket, num_items=0):
        if self.loader == "pillowfight":
            return self.check_dataloss(server, bucket,num_items)
        elif self.loader == "high_ops":
            return self.check_dataloss_for_high_ops_loader(server, bucket, num_items)

    def check_dataloss_for_high_ops_loader(self, server, bucket, num_items):
        from couchbase.cluster import Cluster, PasswordAuthenticator
        from couchbase.exceptions import NotFoundError, CouchbaseError
        from couchbase.bucket import Bucket
        from lib.memcached.helper.data_helper import VBucketAwareMemcached
        if RestConnection(server).get_nodes_version()[:5] < '5':
            bkt = Bucket('couchbase://{0}/{1}'.format(server.ip, bucket.name))
        else:
            cluster = Cluster("couchbase://{}".format(server.ip))
            auth = PasswordAuthenticator(server.rest_username, server.rest_password)
            cluster.authenticate(auth)
            bkt = cluster.open_bucket(bucket.name)
        rest = RestConnection(self.master)
        VBucketAware = VBucketAwareMemcached(rest, bucket.name)
        _, _, _ = VBucketAware.request_map(rest, bucket.name)
        batch_start = 0
        batch_end = 0
        batch_size = self.batch_size
        errors = []
        while num_items > batch_end:
            if batch_start + batch_size > num_items:
                batch_end = num_items
            else:
                batch_end = batch_start + batch_size
            keys = []
            for i in xrange(batch_start, batch_end, 1):
                key = "Key_{}".format(i)
                keys.append(key)
            try:
                result = bkt.get_multi(keys)
                self.log.info("Able to fetch keys starting from {0} to {1}".format(keys[0], keys[len(keys) - 1]))
                for i in range(batch_start, batch_end):
                    key = "Key_{}".format(i)
                    value = {'val': i}
                    if key in result:
                        val = result[key].value
                        for k in value.keys():
                            if k in val and val[k] == value[k]:
                                continue
                            else:
                                vBucketId = VBucketAware._get_vBucket_id(key)
                                errors.append(("Wrong value for key: {0}, VBucketId: {1}".format(key, vBucketId)))
                    else:
                        vBucketId = VBucketAware._get_vBucket_id(key)
                        errors.append(("Missing key: {0}, VBucketId: {1}".format(key, vBucketId)))
                self.log.info("Validated key-values starting from {0} to {1}".format(keys[0], keys[len(keys) - 1]))
            except CouchbaseError as e:
                self.log.error(e)
                ok, fail = e.split_results()
                if fail:
                    for key in fail:
                        try:
                            bkt.get(key)
                        except NotFoundError:
                            vBucketId = VBucketAware._get_vBucket_id(key)
                            errors.append("Missing key: {0}, VBucketId: {1}".format(key, vBucketId))
            batch_start += batch_size
        return errors