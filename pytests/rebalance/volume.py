from threading import Thread

from couchbase.exceptions import CouchbaseException, DocumentNotFoundException
from couchbase_helper.document import DesignDocument, View
from basetestcase import BaseTestCase
from membase.api.rest_client import RestConnection

from lib.sdk_client3 import SDKClient


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
        self.rate_limit = self.input.param("rate_limit", 100000)
        self.batch_size = self.input.param("batch_size", 10000)
        self.doc_size = self.input.param("doc_size", 100)
        self.loader = self.input.param("loader", "pillowfight")
        self.instances = self.input.param("instances", 1)
        self.node_out = self.input.param("node_out", 0)
        self.threads = self.input.param("threads", 5)
        self.use_replica_to = self.input.param("use_replica_to", False)
        self.reload_size = self.input.param("reload_size", 50000)
        self.initial_load= self.input.param("initial_load", 10000)

    def tearDown(self):
        super(VolumeTests, self).tearDown()

    def load(self, server, items, bucket,start_at=0,batch=1000):
        import subprocess
        cmd = "cbc-pillowfight -U couchbase://{0}/{3} -u Administrator -P password " \
              "-I {1} -m 10 -M 100 -B {2} --populate-only --start-at {4} --json" \
            .format(server.ip, items, batch, bucket, start_at)
        self.log.info("Executing '{0}'...".format(cmd))
        rc = subprocess.call(cmd, shell=True)
        if rc != 0:
            self.fail("Exception running cbc-pillowfight: subprocess module returned non-zero response!")

    def check_dataloss(self, server, bucket, num_items):
        from lib.memcached.helper.data_helper import VBucketAwareMemcached
        self.log.info("########## validating data for bucket : {} ###########".format(bucket))
        client = SDKClient(hosts=[server.ip], bucket=bucket.name,
                           username=server.rest_username,
                           password=server.rest_password)
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
            for i in range(batch_start, batch_end, 1):
                keys.append(str(i).rjust(20, '0'))
            try:
                client.default_collection.get_multi(keys)
                self.log.info("Able to fetch keys starting from {0} to {1}".format(keys[0], keys[len(keys) - 1]))
            except CouchbaseException as e:
                self.log.error(e)
                ok, fail = e.split_results()
                if fail:
                    for key in fail:
                        try:
                            client.default_collection.get(key)
                        except DocumentNotFoundException:
                            vBucketId = VBucketAware._get_vBucket_id(key)
                            errors.append("Missing key: {0}, VBucketId: {1}".
                                          format(key, vBucketId))
            batch_start += batch_size
        client.close()
        self.log.info("Total missing keys:{}".format(len(errors)))
        self.log.info(errors)
        return errors

    def create_ddocs_and_views(self):
        self.default_view = View(self.default_view_name, None, None)
        for bucket in self.buckets:
            for i in range(int(self.ddocs_num)):
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
        #load initial documents
        self.create_ddocs_and_views()
        load_thread=[]
        for b in bucket:
            load_thread.append(Thread(target=self.load, args=(self.master, self.num_items, b)))
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
            load_thread.append(Thread(target=self.load, args=(self.master, self.num_items, b, self.num_items)))
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
         self.check_dataloss(self.master, b, self.num_items*2)
        # load more document
        load_thread = []
        for b in bucket:
            load_thread.append(Thread(target=self.load, args=(self.master, self.num_items, b, self.num_items*2)))
        for t in load_thread:
            t.start()
        #rebalance out 1 node
        new_server_list = self.servers[0:self.nodes_init]+ servers_in
        self.log.info("==========rebalance out 1 node=========")
        servers_out=[self.servers[self.nodes_init]]
        rebalance = self.cluster.async_rebalance(servers_init, [],
                                                 servers_out)
        rebalance.result()
        for t in load_thread:
            t.join()
        for b in bucket:
         self.check_dataloss(self.master, b, self.num_items*3)
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
         self.check_dataloss(self.master, b, self.num_items*4)
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
        servers_out = list(set(new_server_list) - {self.master})[-2:]
        rebalance = self.cluster.async_rebalance(servers_init, servers_in, servers_out)
        rebalance.result()
        for t in load_thread:
            t.join()
        for b in bucket:
         self.check_dataloss(self.master, b, self.num_items*5)
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
        servers_out = list(set(new_server_list) - {self.master})[0:1]
        rebalance = self.cluster.async_rebalance(servers_init, servers_in, servers_out)
        rebalance.result()
        for t in load_thread:
            t.join()
        for b in bucket:
         self.check_dataloss(self.master, b, self.num_items*6)
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
         self.check_dataloss(self.master, b, self.num_items*7)
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
        servers_out = list(set(new_server_list) - {self.master})[0:4]
        rebalance = self.cluster.async_rebalance(servers_init, [], servers_out)
        rebalance.result()
        for t in load_thread:
            t.join()
        for b in bucket:
         self.check_dataloss(self.master, b, self.num_items*8)
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
         self.check_dataloss(self.master, b, self.num_items*9)
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
         self.check_dataloss(self.master, b, self.num_items*10)
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
         self.check_dataloss(self.master, b, self.num_items*11)
        self.sleep(30)

    def test_volume_with_high_ops(self):
        self.src_bucket = RestConnection(self.master).get_buckets()
        rest = RestConnection(self.master)
        bucket = rest.get_buckets()
        start_at=0
        total_doc=self.num_items
        #load initial documents
        self.create_ddocs_and_views()
        load_thread=[]
        for bk in bucket:
            load_thread.append(Thread(target=self.load_buckets_with_high_ops, args=(self.master, bk, self.num_items,
                                                                                    self.batch_size, self.threads, start_at, self.instances)))
        for t in load_thread:
            t.start()
        for t in load_thread:
            t.join()
        for b in bucket:
         self.check_dataloss_for_high_ops_loader(self.master, b, total_doc, batch=self.batch_size, instances=self.instances)
        self.sleep(30)
        start_at=total_doc
        #Reload more data for mutations
        load_thread=[]
        for b in bucket:
            load_thread.append(Thread(target=self.load_buckets_with_high_ops,
                                      args=(self.master, b, self.num_items,
                                                       self.batch_size, self.threads, start_at, self.instances)))
        for t in load_thread:
            t.start()
        total_doc +=self.num_items
        start_at=total_doc
        servers_init = self.servers[:self.nodes_init]
        # #Rebalance in 1 node
        self.log.info("==========rebalance in 1 node=========")
        servers_in=self.servers[self.nodes_init:self.nodes_init + 1]
        rebalance = self.cluster.async_rebalance(servers_init,
                                                 servers_in, [])
        for t in load_thread:
            t.join()
        for b in bucket:
            num_keys=rest.get_active_key_count(b)
            self.log.info("****** Number of doc in bucket : {}".format(num_keys))
        total_doc, start_at=self.load_till_rebalance_progress(rest, bucket, total_doc, start_at)
        rebalance.result()
        for b in bucket:
         self.check_dataloss_for_high_ops_loader(self.master, b, total_doc, batch=self.batch_size, instances=self.instances)
        # Reload more data for mutations
        load_thread = []
        for b in bucket:
            load_thread.append(Thread(target=self.load_buckets_with_high_ops,
                                      args=(self.master, b, self.num_items, self.batch_size, self.threads, start_at, self.instances)))
        for t in load_thread:
            t.start()
        total_doc +=self.num_items
        start_at=total_doc
        # rebalance out 1 node
        new_server_list = self.servers[0:self.nodes_init] + servers_in
        self.log.info("==========rebalance out 1 node=========")
        servers_out = [self.servers[self.nodes_init]]
        rebalance = self.cluster.async_rebalance(servers_init, [], servers_out)
        for t in load_thread:
            t.join()
        for b in bucket:
            num_keys=rest.get_active_key_count(b)
            self.log.info("****** Number of doc in bucket : {}".format(num_keys))
        total_doc, start_at=self.load_till_rebalance_progress(rest, bucket, total_doc, start_at)
        rebalance.result()
        for b in bucket:
         self.check_dataloss_for_high_ops_loader(self.master, b, total_doc, batch=self.batch_size, instances=self.instances)
        self.sleep(30)
        # Reload more data for mutations
        load_thread = []
        for b in bucket:
            load_thread.append(Thread(target=self.load_buckets_with_high_ops, args=(
            self.master, b, self.num_items, self.batch_size, self.threads, start_at, self.instances)))
        for t in load_thread:
            t.start()
        total_doc +=self.num_items
        start_at=total_doc
        new_server_list = list(set(new_server_list) - set(servers_out))
        # swap rebalance 1 node
        self.log.info("==========swap rebalance 1 node=========")
        servers_in = self.servers[self.nodes_init: self.nodes_init + 1]
        servers_init = self.servers[:self.nodes_init]
        servers_out = self.servers[(self.nodes_init - 1): self.nodes_init]
        rebalance = self.cluster.async_rebalance(servers_init, servers_in, servers_out)
        for t in load_thread:
            t.join()
        for b in bucket:
            num_keys=rest.get_active_key_count(b)
            self.log.info("****** Number of doc in bucket : {}".format(num_keys))
        total_doc, start_at=self.load_till_rebalance_progress(rest, bucket, total_doc, start_at)
        rebalance.result()
        for b in bucket:
            self.check_dataloss_for_high_ops_loader(self.master, b, total_doc, batch=self.batch_size, instances=self.instances)
        self.sleep(30)
        # load more document
        load_thread = []
        for b in bucket:
            load_thread.append(Thread(target=self.load_buckets_with_high_ops, args=(
                self.master, b, self.num_items, self.batch_size, self.threads, start_at, self.instances)))
        for t in load_thread:
            t.start()
        total_doc +=self.num_items
        start_at=total_doc
        new_server_list = list(set(new_server_list + servers_in) - set(servers_out))
        self.log.info("==========Rebalance out of 2 nodes and Rebalance In 1 node=========")
        # Rebalance out of 2 nodes and Rebalance In 1 node
        servers_in = [list(set(self.servers) - set(new_server_list))[0]]
        servers_out = list(set(new_server_list) - {self.master})[-2:]
        rebalance = self.cluster.async_rebalance(servers_init, servers_in, servers_out)
        for t in load_thread:
            t.join()
        for b in bucket:
            num_keys=rest.get_active_key_count(b)
            self.log.info("****** Number of doc in bucket : {}".format(num_keys))
        total_doc, start_at=self.load_till_rebalance_progress(rest, bucket, total_doc, start_at)
        rebalance.result()
        for b in bucket:
            self.check_dataloss_for_high_ops_loader(self.master, b, total_doc, batch=self.batch_size, instances=self.instances)
        self.sleep(30)
        # load more document
        load_thread = []
        for b in bucket:
            load_thread.append(Thread(target=self.load_buckets_with_high_ops, args=(
                self.master, b, self.num_items, self.batch_size, self.threads, start_at, self.instances)))
        for t in load_thread:
            t.start()
        total_doc += self.num_items
        start_at = total_doc
        new_server_list = list(set(new_server_list + servers_in) - set(servers_out))
        self.log.info("==========Rebalance out of 1 nodes and Rebalance In 2 nodes=========")
        # Rebalance out of 1 nodes and Rebalance In 2 nodes
        servers_in = list(set(self.servers) - set(new_server_list))[0:2]
        servers_out = list(set(new_server_list) - {self.master})[0:1]
        rebalance = self.cluster.async_rebalance(servers_init, servers_in, servers_out)
        for t in load_thread:
            t.join()
        for b in bucket:
            num_keys=rest.get_active_key_count(b)
            self.log.info("****** Number of doc in bucket : {}".format(num_keys))
        total_doc, start_at=self.load_till_rebalance_progress(rest, bucket, total_doc, start_at)
        rebalance.result()
        for b in bucket:
            self.check_dataloss_for_high_ops_loader(self.master, b, total_doc, batch=self.batch_size, instances=self.instances)
        self.sleep(30)
        # load more document
        load_thread = []
        for b in bucket:
            load_thread.append(Thread(target=self.load_buckets_with_high_ops, args=(
                self.master, b, self.num_items, self.batch_size, self.threads, start_at, self.instances)))
        for t in load_thread:
            t.start()
        total_doc += self.num_items
        start_at = total_doc
        new_server_list = list(set(new_server_list + servers_in) - set(servers_out))
        self.log.info("==========Rebalance in 4 nodes =========")
        # Rebalance in 4 nodes
        servers_in = list(set(self.servers) - set(new_server_list))[0:4]
        rebalance = self.cluster.async_rebalance(servers_init, servers_in, [])
        for t in load_thread:
            t.join()
        for b in bucket:
            num_keys=rest.get_active_key_count(b)
            self.log.info("****** Number of doc in bucket : {}".format(num_keys))
        total_doc, start_at=self.load_till_rebalance_progress(rest, bucket, total_doc, start_at)
        rebalance.result()
        for b in bucket:
            self.check_dataloss_for_high_ops_loader(self.master, b, total_doc, batch=self.batch_size, instances=self.instances)
        self.sleep(30)
        # load more document
        load_thread = []
        for b in bucket:
            load_thread.append(Thread(target=self.load_buckets_with_high_ops, args=(
                self.master, b, self.num_items, self.batch_size, self.threads, start_at, self.instances)))
        for t in load_thread:
            t.start()
        total_doc += self.num_items
        start_at = total_doc
        new_server_list = list(set(new_server_list + servers_in))
        self.log.info("==========Rebalance out 4 nodes =========")
        # Rebalance out 4 nodes
        servers_out = list(set(new_server_list) - {self.master})[0:4]
        rebalance = self.cluster.async_rebalance(servers_init, [], servers_out)
        for t in load_thread:
            t.join()
        for b in bucket:
            num_keys=rest.get_active_key_count(b)
            self.log.info("****** Number of doc in bucket : {}".format(num_keys))
        total_doc, start_at=self.load_till_rebalance_progress(rest, bucket, total_doc, start_at)
        rebalance.result()
        for b in bucket:
            self.check_dataloss_for_high_ops_loader(self.master, b, total_doc, batch=self.batch_size, instances=self.instances)
        self.sleep(30)
        # load more document
        load_thread = []
        for b in bucket:
            load_thread.append(Thread(target=self.load_buckets_with_high_ops, args=(
                self.master, b, self.num_items, self.batch_size, self.threads, start_at, self.instances)))
        for t in load_thread:
            t.start()
        total_doc += self.num_items
        start_at = total_doc
        new_server_list = list(set(new_server_list) - set(servers_out))
        self.log.info(
            "======Rebalance in 4 nodes (8 nodes) wait for rebalance to finish and move between server groups=========")
        # Rebalance in 4 nodes (8 nodes) wait for rebalance to finish and move between server groups
        servers_in = list(set(self.servers) - set(new_server_list))[0:4]
        rebalance = self.cluster.async_rebalance(servers_init, servers_in, [])
        for t in load_thread:
            t.join()
        for b in bucket:
            num_keys=rest.get_active_key_count(b)
            self.log.info("****** Number of doc in bucket : {}".format(num_keys))
        total_doc, start_at=self.load_till_rebalance_progress(rest, bucket, total_doc, start_at)
        rebalance.result()
        for b in bucket:
            self.check_dataloss_for_high_ops_loader(self.master, b, total_doc, batch=self.batch_size, instances=self.instances)
        self.sleep(30)
        # load more document
        load_thread = []
        for b in bucket:
            load_thread.append(Thread(target=self.load_buckets_with_high_ops, args=(
                self.master, b, self.num_items, self.batch_size, self.threads, start_at, self.instances)))
        for t in load_thread:
            t.start()
        total_doc += self.num_items
        start_at = total_doc
        self.log.info("####### Shuffling zones and rebalance #######")
        self.shuffle_nodes_between_zones_and_rebalance()
        for t in load_thread:
            t.join()
        for b in bucket:
            num_keys=rest.get_active_key_count(b)
            self.log.info("****** Number of doc in bucket : {}".format(num_keys))
        total_doc, start_at = self.load_till_rebalance_progress(rest, bucket, total_doc, start_at)
        for b in bucket:
            self.check_dataloss_for_high_ops_loader(self.master, b, total_doc, batch=self.batch_size, instances=self.instances)
        self.sleep(30)
        # load more document
        load_thread = []
        for b in bucket:
            load_thread.append(Thread(target=self.load_buckets_with_high_ops, args=(
                self.master, b, self.num_items, self.batch_size, self.threads, start_at, self.instances)))
        for t in load_thread:
            t.start()
        total_doc += self.num_items
        start_at = total_doc
        self.log.info("======Graceful failover 1 KV node and add back(Delta and Full)=========")
        # Graceful failover 1 KV node and add back(Delta and Full)
        kv_server = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=False)
        fail_over_task = self.cluster.async_failover([self.master], failover_nodes=[kv_server], graceful=True)
        fail_over_task.result()
        self.sleep(120)
        # do a recovery and rebalance
        rest.set_recovery_type('ns_1@' + kv_server.ip, recoveryType=self.recoveryType)
        rest.add_back_node('ns_1@' + kv_server.ip)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [])
        for t in load_thread:
            t.join()
        for b in bucket:
            num_keys=rest.get_active_key_count(b)
            self.log.info("****** Number of doc in bucket : {}".format(num_keys))
        total_doc, start_at = self.load_till_rebalance_progress(rest, bucket, total_doc, start_at)
        rebalance.result()
        for b in bucket:
            errors=self.check_dataloss_for_high_ops_loader(self.master, b, total_doc, batch=self.batch_size, instances=self.instances)
            if len(errors) > 0:
                self.fail("data is missing");
        self.sleep(30)

    def test_volume_with_high_ops_update(self):
        self.src_bucket = RestConnection(self.master).get_buckets()
        rest = RestConnection(self.master)
        bucket = rest.get_buckets()
        start_at = 0
        total_doc = self.num_items
        updated=1
        # load initial documents
        self.create_ddocs_and_views()
        load_thread = []
        for bk in bucket:
            load_thread.append(Thread(target=self.load_buckets_with_high_ops, args=(
            self.master, bk, self.num_items, self.batch_size, self.threads, start_at, self.instances)))
        for t in load_thread:
            t.start()
        for t in load_thread:
            t.join()
        for b in bucket:
            self.check_dataloss_for_high_ops_loader(self.master, b, total_doc, batch=self.batch_size, instances=self.instances)
        self.sleep(30)
        #Update all data
        load_thread=[]
        for b in bucket:
            load_thread.append(Thread(target=self.update_buckets_with_high_ops,
                                      args=(self.master, b, total_doc, total_doc,
                                                       self.batch_size, self.threads, start_at, self.instances, updated)))
        for t in load_thread:
            t.start()
        servers_init = self.servers[:self.nodes_init]
        #Rebalance in 1 node
        self.log.info("==========rebalance in 1 node=========")
        servers_in=self.servers[self.nodes_init:self.nodes_init + 1]
        rebalance = self.cluster.async_rebalance(servers_init,
                                                 servers_in, [])
        for t in load_thread:
            t.join()
        #total_doc,start_at=self.load_till_rebalance_progress(rest,bucket,total_doc,start_at)
        rebalance.result()
        for b in bucket:
            self.check_dataloss_for_high_ops_loader(self.master, b, total_doc, updated=True, ops=total_doc,
                                                    batch=self.batch_size, instances=self.instances)
        updated +=1
        #Update all data
        load_thread = []
        for b in bucket:
         load_thread.append(Thread(target=self.update_buckets_with_high_ops, args=(
         self.master, b, total_doc, total_doc, self.batch_size, self.threads, start_at, self.instances, updated)))
        for t in load_thread:
         t.start()
        # rebalance out 1 node
        new_server_list = self.servers[0:self.nodes_init] + servers_in
        self.log.info("==========rebalance out 1 node=========")
        servers_out = [self.servers[self.nodes_init]]
        rebalance = self.cluster.async_rebalance(servers_init, [], servers_out)
        rebalance.result()
        for t in load_thread:
         t.join()
        for b in bucket:
         self.check_dataloss_for_high_ops_loader(self.master, b, total_doc, updated=True,
                                                 ops=total_doc*updated, batch=self.batch_size, instances=self.instances)
        updated +=1
        #Update all data
        load_thread = []
        for b in bucket:
         load_thread.append(Thread(target=self.update_buckets_with_high_ops, args=(
         self.master, b, total_doc, total_doc, self.batch_size, self.threads, start_at, self.instances, updated)))
        for t in load_thread:
         t.start()
        new_server_list = list(set(new_server_list) - set(servers_out))
        # swap rebalance 1 node
        self.log.info("==========swap rebalance 1 node=========")
        servers_in = self.servers[self.nodes_init: self.nodes_init + 1]
        servers_init = self.servers[:self.nodes_init]
        servers_out = self.servers[(self.nodes_init - 1): self.nodes_init]
        rebalance = self.cluster.async_rebalance(servers_init, servers_in, servers_out)
        rebalance.result()
        for t in load_thread:
            t.join()
        for b in bucket:
         self.check_dataloss_for_high_ops_loader(self.master, b, total_doc, updated=True,
                                                 ops=total_doc*updated, batch=self.batch_size, instances=self.instances)
        updated += 1
        # Update all data
        load_thread = []
        for b in bucket:
            load_thread.append(Thread(target=self.update_buckets_with_high_ops, args=(
                self.master, b, total_doc, total_doc, self.batch_size, self.threads, start_at, self.instances,
                updated)))
        for t in load_thread:
            t.start()
        new_server_list = list(set(new_server_list + servers_in) - set(servers_out))
        self.log.info("==========Rebalance out of 2 nodes and Rebalance In 1 node=========")
        # Rebalance out of 2 nodes and Rebalance In 1 node
        servers_in = [list(set(self.servers) - set(new_server_list))[0]]
        servers_out = list(set(new_server_list) - {self.master})[-2:]
        rebalance = self.cluster.async_rebalance(servers_init, servers_in, servers_out)
        rebalance.result()
        for t in load_thread:
            t.join()
        for b in bucket:
            self.check_dataloss_for_high_ops_loader(self.master, b, total_doc, updated=True,
                                                    ops=total_doc * updated, batch=self.batch_size, instances=self.instances)
        updated += 1
        # Update all data
        load_thread = []
        for b in bucket:
            load_thread.append(Thread(target=self.update_buckets_with_high_ops, args=(
                self.master, b, total_doc, total_doc, self.batch_size, self.threads, start_at, self.instances,
                updated)))
        for t in load_thread:
            t.start()
        self.log.info("==========Rebalance out of 1 nodes and Rebalance In 2 nodes=========")
        new_server_list = list(set(new_server_list + servers_in) - set(servers_out))
        # Rebalance out of 1 nodes and Rebalance In 2 nodes
        servers_in = list(set(self.servers) - set(new_server_list))[0:2]
        servers_out = list(set(new_server_list) - {self.master})[0:1]
        rebalance = self.cluster.async_rebalance(servers_init, servers_in, servers_out)
        rebalance.result()
        for t in load_thread:
            t.join()
        for b in bucket:
            self.check_dataloss_for_high_ops_loader(self.master, b, total_doc, updated=True,
                                                    ops=total_doc * updated, batch=self.batch_size, instances=self.instances)
        updated += 1
        # Update all data
        load_thread = []
        for b in bucket:
            load_thread.append(Thread(target=self.update_buckets_with_high_ops, args=(
                self.master, b, total_doc, total_doc, self.batch_size, self.threads, start_at, self.instances,
                updated)))
        for t in load_thread:
            t.start()
        new_server_list = list(set(new_server_list + servers_in) - set(servers_out))
        self.log.info("==========Rebalance in 4 nodes =========")
        # Rebalance in 4 nodes
        servers_in = list(set(self.servers) - set(new_server_list))[0:4]
        rebalance = self.cluster.async_rebalance(servers_init, servers_in, [])
        rebalance.result()
        for t in load_thread:
            t.join()
        for b in bucket:
            self.check_dataloss_for_high_ops_loader(self.master, b, total_doc, updated=True,
                                                    ops=total_doc * updated, batch=self.batch_size, instances=self.instances)
        updated += 1
        # Update all data
        load_thread = []
        for b in bucket:
            load_thread.append(Thread(target=self.update_buckets_with_high_ops, args=(
                self.master, b, total_doc, total_doc, self.batch_size, self.threads, start_at, self.instances,
                updated)))
        for t in load_thread:
            t.start()
        new_server_list = list(set(new_server_list + servers_in))
        self.log.info("==========Rebalance out 4 nodes =========")
        # Rebalance out 4 nodes
        servers_out = list(set(new_server_list) - {self.master})[0:4]
        rebalance = self.cluster.async_rebalance(servers_init, [], servers_out)
        rebalance.result()
        for t in load_thread:
            t.join()
        for b in bucket:
            self.check_dataloss_for_high_ops_loader(self.master, b, total_doc,  updated=True,
                                                    ops=total_doc * updated, batch=self.batch_size,
                                                    instances=self.instances)
        updated += 1
        # Update all data
        load_thread = []
        for b in bucket:
            load_thread.append(Thread(target=self.update_buckets_with_high_ops, args=(
                self.master, b, total_doc, total_doc, self.batch_size, self.threads, start_at, self.instances,
                updated)))
        for t in load_thread:
            t.start()
        new_server_list = list(set(new_server_list) - set(servers_out))
        self.log.info(
            "======Rebalance in 4 nodes (8 nodes) wait for rebalance to finish and move between server groups=========")
        # Rebalance in 4 nodes (8 nodes) wait for rebalance to finish and move between server groups
        servers_in = list(set(self.servers) - set(new_server_list))[0:4]
        rebalance = self.cluster.async_rebalance(servers_init, servers_in, [])
        rebalance.result()
        for t in load_thread:
            t.join()
        for b in bucket:
            self.check_dataloss_for_high_ops_loader(self.master, b, total_doc, updated=True,
                                                    ops=total_doc * updated, batch=self.batch_size,
                                                    instances=self.instances)
        updated += 1
        # Update all data
        load_thread = []
        for b in bucket:
            load_thread.append(Thread(target=self.update_buckets_with_high_ops, args=(
                self.master, b, total_doc, total_doc, self.batch_size, self.threads, start_at, self.instances,
                updated)))
        for t in load_thread:
            t.start()
        self.log.info("####### Shuffling zones and rebalance #######")
        self.shuffle_nodes_between_zones_and_rebalance()
        for t in load_thread:
            t.join()
        for b in bucket:
            self.check_dataloss_for_high_ops_loader(self.master, b, total_doc, updated=True,
                                                    ops=total_doc * updated, batch=self.batch_size,
                                                    instances=self.instances)
        updated += 1
        # Update all data
        load_thread = []
        for b in bucket:
            load_thread.append(Thread(target=self.update_buckets_with_high_ops, args=(
                self.master, b, total_doc, total_doc, self.batch_size, self.threads, start_at, self.instances,
                updated)))
        for t in load_thread:
            t.start()
        self.log.info("======Graceful failover 1 KV node and add back(Delta and Full)=========")
        # Graceful failover 1 KV node and add back(Delta and Full)
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
            errors=self.check_dataloss_for_high_ops_loader(self.master, b, total_doc, updated=True,
                                                    ops=total_doc * updated, batch=self.batch_size,
                                                           instances=self.instances)
            if len(errors) > 0:
                self.fail("data is missing");

    def test_volume_with_high_ops_create_update(self):
        self.src_bucket = RestConnection(self.master).get_buckets()
        rest = RestConnection(self.master)
        bucket = rest.get_buckets()
        start_at = 0
        # load initial documents
        self.create_ddocs_and_views()
        load_thread = []
        for bk in bucket:
            load_thread.append(Thread(target=self.load_buckets_with_high_ops, args=(
            self.master, bk, self.initial_load, self.batch_size, self.threads, start_at, self.instances)))
        for t in load_thread:
            t.start()
        for t in load_thread:
            t.join()
        for b in bucket:
            self.check_dataloss_for_high_ops_loader(self.master, b, self.initial_load, batch=self.batch_size, instances=self.instances)
        self.sleep(30)
        total_doc = self.initial_load
        start_at=total_doc
        #Update initial doc and create more doc
        load_thread=[]
        create_thread=[]
        updated=1
        for b in bucket:
            load_thread.append(Thread(target=self.update_buckets_with_high_ops,
                                      args=(self.master, b, self.initial_load, self.initial_load,
                                                       self.batch_size, self.threads, 0, self.instances, updated)))
            create_thread.append(Thread(target=self.load_buckets_with_high_ops, args=(
            self.master, b, self.num_items, self.batch_size, self.threads, start_at, self.instances)))
        for t in load_thread:
            t.start()
        for th in create_thread:
            th.start()
        total_doc +=self.num_items
        start_at=total_doc
        updated +=1
        servers_init = self.servers[:self.nodes_init]
        #Rebalance in 1 node
        self.log.info("==========rebalance in 1 node=========")
        servers_in=self.servers[self.nodes_init:self.nodes_init + 1]
        rebalance = self.cluster.async_rebalance(servers_init,
                                                 servers_in, [])
        self.sleep(10)
        for t in load_thread:
            t.join()
        for th in create_thread:
            th.join()
        total_doc, start_at, updated=self.create_update_till_rebalance_progress(rest, bucket, total_doc, start_at, updated)
        rebalance.result()
        for b in bucket:
            self.check_dataloss_for_high_ops_loader(self.master, b, self.initial_load, start_document=0, updated=True,
                                                    ops=self.initial_load*(updated-1), batch=self.batch_size, instances=self.instances)
            self.check_dataloss_for_high_ops_loader(self.master, b, total_doc-self.initial_load,
                                                    start_document=self.initial_load, batch=self.batch_size, instances=self.instances)
        # Update initial doc and create more doc
        load_thread = []
        create_thread = []
        for b in bucket:
            load_thread.append(Thread(target=self.update_buckets_with_high_ops, args=(
            self.master, b, self.initial_load, self.initial_load, self.batch_size, self.threads, 0, self.instances, updated)))
            create_thread.append(Thread(target=self.load_buckets_with_high_ops, args=(
                self.master, b, self.num_items, self.batch_size, self.threads, start_at, self.instances)))
        for t in load_thread:
            t.start()
        for th in create_thread:
            th.start()
        total_doc += self.num_items
        start_at = total_doc
        updated +=1
        # rebalance out 1 node
        new_server_list = self.servers[0:self.nodes_init] + servers_in
        self.log.info("==========rebalance out 1 node=========")
        servers_out = [self.servers[self.nodes_init]]
        rebalance = self.cluster.async_rebalance(servers_init, [], servers_out)
        rebalance.result()
        self.sleep(5)
        for t in load_thread:
            t.join()
        for th in create_thread:
            th.join()
        total_doc, start_at, updated=self.create_update_till_rebalance_progress(rest, bucket, total_doc, start_at, updated)
        rebalance.result()
        for b in bucket:
            self.check_dataloss_for_high_ops_loader(self.master, b, self.initial_load, start_document=0, updated=True,
                                                    ops=self.initial_load*(updated-1),
                                                    batch=self.batch_size, instances=self.instances)
            self.check_dataloss_for_high_ops_loader(self.master, b, total_doc-self.initial_load,
                                                    start_document=self.initial_load, batch=self.batch_size,
                                                    instances=self.instances)
        # Update initial doc and create more doc
        load_thread = []
        create_thread = []
        for b in bucket:
            load_thread.append(Thread(target=self.update_buckets_with_high_ops, args=(
                self.master, b, self.initial_load, self.initial_load, self.batch_size, self.threads, 0, self.instances,
                updated)))
            create_thread.append(Thread(target=self.load_buckets_with_high_ops, args=(
                self.master, b, self.num_items, self.batch_size, self.threads, start_at, self.instances)))
        for t in load_thread:
            t.start()
        for th in create_thread:
            th.start()
        total_doc += self.num_items
        start_at = total_doc
        updated +=1
        new_server_list = list(set(new_server_list) - set(servers_out))
        # swap rebalance 1 node
        self.log.info("==========swap rebalance 1 node=========")
        servers_in = self.servers[self.nodes_init: self.nodes_init + 1]
        servers_init = self.servers[:self.nodes_init]
        servers_out = self.servers[(self.nodes_init - 1): self.nodes_init]
        rebalance = self.cluster.async_rebalance(servers_init, servers_in, servers_out)
        self.sleep(10)
        for t in load_thread:
            t.join()
        for th in create_thread:
            th.join()
        total_doc, start_at, updated = self.create_update_till_rebalance_progress(rest, bucket, total_doc, start_at,
                                                                                  updated)
        rebalance.result()
        for b in bucket:
            self.check_dataloss_for_high_ops_loader(self.master, b, self.initial_load, start_document=0, updated=True,
                                                    ops=self.initial_load*(updated-1), batch=self.batch_size,
                                                    instances=self.instances)
            self.check_dataloss_for_high_ops_loader(self.master, b, total_doc - self.initial_load,
                                                    start_document=self.initial_load, batch=self.batch_size,
                                                    instances=self.instances)
        # Update initial doc and create more doc
        load_thread = []
        create_thread = []
        for b in bucket:
            load_thread.append(Thread(target=self.update_buckets_with_high_ops, args=(
                self.master, b, self.initial_load, self.initial_load, self.batch_size, self.threads, 0,
                self.instances, updated)))
            create_thread.append(Thread(target=self.load_buckets_with_high_ops, args=(
                self.master, b, self.num_items, self.batch_size, self.threads, start_at, self.instances)))
        for t in load_thread:
            t.start()
        for th in create_thread:
            th.start()
        total_doc += self.num_items
        start_at = total_doc
        updated += 1
        new_server_list = list(set(new_server_list + servers_in) - set(servers_out))
        self.log.info("==========Rebalance out of 2 nodes and Rebalance In 1 node=========")
        # Rebalance out of 2 nodes and Rebalance In 1 node
        servers_in = [list(set(self.servers) - set(new_server_list))[0]]
        servers_out = list(set(new_server_list) - {self.master})[-2:]
        rebalance = self.cluster.async_rebalance(servers_init, servers_in, servers_out)
        self.sleep(10)
        for t in load_thread:
            t.join()
        for th in create_thread:
            th.join()
        total_doc, start_at, updated = self.create_update_till_rebalance_progress(rest, bucket, total_doc, start_at,
                                                                                  updated)
        rebalance.result()
        for b in bucket:
            self.check_dataloss_for_high_ops_loader(self.master, b, self.initial_load, start_document=0, updated=True,
                                                    ops=self.initial_load * (updated - 1),
                                                    batch=self.batch_size, instances=self.instances)
            self.check_dataloss_for_high_ops_loader(self.master, b, total_doc - self.initial_load,
                                                    start_document=self.initial_load, batch=self.batch_size,
                                                    instances=self.instances)
        # Update initial doc and create more doc
        load_thread = []
        create_thread = []
        for b in bucket:
            load_thread.append(Thread(target=self.update_buckets_with_high_ops, args=(
                self.master, b, self.initial_load, self.initial_load, self.batch_size, self.threads, 0,
                self.instances, updated)))
            create_thread.append(Thread(target=self.load_buckets_with_high_ops, args=(
                self.master, b, self.num_items, self.batch_size, self.threads, start_at, self.instances)))
        for t in load_thread:
            t.start()
        for th in create_thread:
            th.start()
        total_doc += self.num_items
        start_at = total_doc
        updated += 1
        new_server_list = list(set(new_server_list + servers_in) - set(servers_out))
        self.log.info("==========Rebalance out of 1 nodes and Rebalance In 2 nodes=========")
        # Rebalance out of 1 nodes and Rebalance In 2 nodes
        servers_in = list(set(self.servers) - set(new_server_list))[0:2]
        servers_out = list(set(new_server_list) - {self.master})[0:1]
        rebalance = self.cluster.async_rebalance(servers_init, servers_in, servers_out)
        self.sleep(10)
        for t in load_thread:
            t.join()
        for th in create_thread:
            th.join()
        total_doc, start_at, updated = self.create_update_till_rebalance_progress(rest, bucket, total_doc, start_at,
                                                                                  updated)
        rebalance.result()
        for b in bucket:
            self.check_dataloss_for_high_ops_loader(self.master, b, self.initial_load, start_document=0, updated=True,
                                                    ops=self.initial_load * (updated - 1), batch=self.batch_size,
                                                    instances=self.instances)
            self.check_dataloss_for_high_ops_loader(self.master, b, total_doc - self.initial_load,
                                                    start_document=self.initial_load, batch=self.batch_size,
                                                    instances=self.instances)
        # Update initial doc and create more doc
        load_thread = []
        create_thread = []
        for b in bucket:
            load_thread.append(Thread(target=self.update_buckets_with_high_ops, args=(
                self.master, b, self.initial_load, self.initial_load, self.batch_size, self.threads, 0,
                self.instances, updated)))
            create_thread.append(Thread(target=self.load_buckets_with_high_ops, args=(
                self.master, b, self.num_items, self.batch_size, self.threads, start_at, self.instances)))
        for t in load_thread:
            t.start()
        for th in create_thread:
            th.start()
        total_doc += self.num_items
        start_at = total_doc
        updated += 1
        new_server_list = list(set(new_server_list + servers_in) - set(servers_out))
        self.log.info("==========Rebalance in 4 nodes =========")
        # Rebalance in 4 nodes
        servers_in = list(set(self.servers) - set(new_server_list))[0:4]
        rebalance = self.cluster.async_rebalance(servers_init, servers_in, [])
        self.sleep(10)
        for t in load_thread:
            t.join()
        for th in create_thread:
            th.join()
        total_doc, start_at, updated = self.create_update_till_rebalance_progress(rest, bucket, total_doc, start_at,
                                                                                  updated)
        rebalance.result()
        for b in bucket:
            self.check_dataloss_for_high_ops_loader(self.master, b, self.initial_load, start_document=0, updated=True,
                                                    ops=self.initial_load * (updated - 1), batch=self.batch_size,
                                                    instances=self.instances)
            self.check_dataloss_for_high_ops_loader(self.master, b, total_doc - self.initial_load,
                                                    start_document=self.initial_load, batch=self.batch_size,
                                                    instances=self.instances)
        # Update initial doc and create more doc
        load_thread = []
        create_thread = []
        for b in bucket:
            load_thread.append(Thread(target=self.update_buckets_with_high_ops, args=(
                self.master, b, self.initial_load, self.initial_load, self.batch_size, self.threads, 0,
                self.instances, updated)))
            create_thread.append(Thread(target=self.load_buckets_with_high_ops, args=(
                self.master, b, self.num_items, self.batch_size, self.threads, start_at, self.instances)))
        for t in load_thread:
            t.start()
        for th in create_thread:
            th.start()
        total_doc += self.num_items
        start_at = total_doc
        updated += 1
        new_server_list = list(set(new_server_list + servers_in))
        self.log.info("==========Rebalance out 4 nodes =========")
        # Rebalance out 4 nodes
        servers_out = list(set(new_server_list) - {self.master})[0:4]
        rebalance = self.cluster.async_rebalance(servers_init, [], servers_out)
        self.sleep(10)
        for t in load_thread:
            t.join()
        for th in create_thread:
            th.join()
        total_doc, start_at, updated = self.create_update_till_rebalance_progress(rest, bucket, total_doc, start_at,
                                                                                  updated)
        rebalance.result()
        for b in bucket:
            self.check_dataloss_for_high_ops_loader(self.master, b, self.initial_load, start_document=0, updated=True,
                                                    ops=self.initial_load * (updated - 1), batch=self.batch_size,
                                                    instances=self.instances)
            self.check_dataloss_for_high_ops_loader(self.master, b, total_doc - self.initial_load,
                                                    start_document=self.initial_load, batch=self.batch_size,
                                                    instances=self.instances)
        # Update initial doc and create more doc
        load_thread = []
        create_thread = []
        for b in bucket:
            load_thread.append(Thread(target=self.update_buckets_with_high_ops, args=(
                self.master, b, self.initial_load, self.initial_load, self.batch_size, self.threads, 0,
                self.instances, updated)))
            create_thread.append(Thread(target=self.load_buckets_with_high_ops, args=(
                self.master, b, self.num_items, self.batch_size, self.threads, start_at, self.instances)))
        for t in load_thread:
            t.start()
        for th in create_thread:
            th.start()
        total_doc += self.num_items
        start_at = total_doc
        updated += 1
        new_server_list = list(set(new_server_list) - set(servers_out))
        self.log.info(
            "======Rebalance in 4 nodes (8 nodes) wait for rebalance to finish and move between server groups=========")
        # Rebalance in 4 nodes (8 nodes) wait for rebalance to finish and move between server groups
        servers_in = list(set(self.servers) - set(new_server_list))[0:4]
        rebalance = self.cluster.async_rebalance(servers_init, servers_in, [])
        self.sleep(10)
        for t in load_thread:
            t.join()
        for th in create_thread:
            th.join()
        total_doc, start_at, updated = self.create_update_till_rebalance_progress(rest, bucket, total_doc, start_at,
                                                                                  updated)
        rebalance.result()
        for b in bucket:
            self.check_dataloss_for_high_ops_loader(self.master, b, self.initial_load, start_document=0, updated=True,
                                                    ops=self.initial_load * (updated - 1), batch=self.batch_size,
                                                    instances=self.instances)
            self.check_dataloss_for_high_ops_loader(self.master, b, total_doc - self.initial_load,
                                                    start_document=self.initial_load, batch=self.batch_size,
                                                    instances=self.instances)
        # Update initial doc and create more doc
        load_thread = []
        create_thread = []
        for b in bucket:
            load_thread.append(Thread(target=self.update_buckets_with_high_ops, args=(
                self.master, b, self.initial_load, self.initial_load, self.batch_size, self.threads, 0,
                self.instances, updated)))
            create_thread.append(Thread(target=self.load_buckets_with_high_ops, args=(
                self.master, b, self.num_items, self.batch_size, self.threads, start_at, self.instances)))
        for t in load_thread:
            t.start()
        for th in create_thread:
            th.start()
        total_doc += self.num_items
        start_at = total_doc
        updated += 1
        self.log.info("======Graceful failover 1 KV node and add back(Delta and Full)=========")
        # Graceful failover 1 KV node and add back(Delta and Full)
        kv_server = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=False)
        fail_over_task = self.cluster.async_failover([self.master], failover_nodes=[kv_server], graceful=True)
        fail_over_task.result()
        self.sleep(120)
        # do a recovery and rebalance
        rest.set_recovery_type('ns_1@' + kv_server.ip, recoveryType=self.recoveryType)
        rest.add_back_node('ns_1@' + kv_server.ip)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [])
        self.sleep(10)
        for t in load_thread:
            t.join()
        for th in create_thread:
            th.join()
        total_doc, start_at, updated = self.create_update_till_rebalance_progress(rest, bucket, total_doc, start_at,
                                                                                  updated)
        rebalance.result()
        for b in bucket:
            errors1=self.check_dataloss_for_high_ops_loader(self.master, b, self.initial_load, start_document=0, updated=True,
                                                    ops=self.initial_load * (updated - 1), batch=self.batch_size,
                                                            instances=self.instances)
            errors2=self.check_dataloss_for_high_ops_loader(self.master, b, total_doc - self.initial_load,
                                                    start_document=self.initial_load, batch=self.batch_size,
                                                            instances=self.instances)
            if len(errors1) > 0 or len(errors2) > 0:
                self.fail("data is missing");


    def load_till_rebalance_progress(self, rest, bucket, total_doc, start_at):
        rebalance_status = rest._rebalance_progress_status()
        self.log.info("###### Rebalance Status:{} ######".format(rebalance_status))
        self.sleep(10)
        while rebalance_status == 'running':
            self.log.info("===== Loading {} as rebalance is going on =====".format(self.reload_size))
            load_thread = []
            for b in bucket:
                load_thread.append(Thread(target=self.load_buckets_with_high_ops, args=(self.master, b, self.reload_size,
                                                                                    self.batch_size, self.threads, start_at, self.instances)))
            for t in load_thread:
                t.start()
            for t in load_thread:
                t.join()
            rebalance_status = rest._rebalance_progress_status()
            self.log.info("###### Rebalance Status:{} ######".format(rebalance_status))
            total_doc += self.reload_size
            start_at = total_doc
        return total_doc, start_at

    def create_update_till_rebalance_progress(self, rest, bucket, total_doc, start_at, updated):
        rebalance_status = rest._rebalance_progress_status()
        self.log.info("###### Rebalance Status:{} ######".format(rebalance_status))
        self.sleep(10)
        while rebalance_status == 'running':
            self.log.info("===== Loading {} as rebalance is going on =====".format(self.reload_size))
            load_thread = []
            update_thread = []
            for b in bucket:
                load_thread.append(Thread(target=self.load_buckets_with_high_ops, args=(self.master, b, self.reload_size,
                                                                                    self.batch_size, self.threads,
                                                                                        start_at, self.threads)))
                update_thread.append(Thread(target=self.update_buckets_with_high_ops,
                                      args=(self.master, b, self.initial_load, self.initial_load,
                                                       self.batch_size, self.threads, 0, self.instances, updated)))
            for t in load_thread:
                t.start()
            for th in update_thread:
                th.start()
            for t in load_thread:
                t.join()
            for th in update_thread:
                th.join()
            rebalance_status = rest._rebalance_progress_status()
            self.log.info("###### Rebalance Status:{} ######".format(rebalance_status))
            total_doc += self.reload_size
            start_at = total_doc
            updated += 1
        return total_doc, start_at, updated

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
                                    {node for node in rest.get_nodes_in_zone(zones[i])})
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

    def update_buckets_with_high_ops(self, server, bucket, items, ops,
                                     batch=20000, threads=5, start_document=0,
                                     instances=1,update_counter=1):
        import subprocess
        #cmd_format = "python3 scripts/high_ops_doc_loader.py  --node {0} --bucket {1} --user {2} --password {3} " \
        #             "--count {4} --batch_size {5} --threads {6} --start_document {7} --cb_version {8} --instances {" \
        #             "9} --ops {10} --updates --update_counter {11}"
        cmd_format = "python3 scripts/thanosied.py  --spec couchbase://{0} --bucket {1} --user {2} --password {3} " \
                     "--count {4} --batch_size {5} --threads {6} --start_document {7} --cb_version {8} --workers {9} --rate_limit {10} " \
                     "--passes 1  --update_counter {11}"
        cb_version = RestConnection(server).get_nodes_version()[:3]
        if self.num_replicas > 1:
            cmd_format = "{} --replicate_to 1".format(cmd_format)
        cmd = cmd_format.format(server.ip, bucket.name, server.rest_username,
                                server.rest_password,
                                items, batch, threads, start_document,
                                cb_version, instances, int(ops), update_counter)
        self.log.info("Running {}".format(cmd))
        result = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE)
        output = result.stdout.read()
        error = result.stderr.read()
        if error:
            self.log.error(error)
            self.fail("Failed to run the loadgen.")
        if output:
            loaded = output.split('\n')[:-1]
            total_loaded = 0
            for load in loaded:
                total_loaded += int(load.split(':')[1].strip())
            self.assertEqual(total_loaded, ops,
                             "Failed to update {} items. Loaded only {} items".format(
                                 ops,
                                 total_loaded))

    def load_buckets_with_high_ops(self, server, bucket, items, batch=20000, threads=10, start_document=0, instances=1
                                   ,ttl=0):
        import subprocess
        #cmd_format = "python3 scripts/high_ops_doc_loader.py  --node {0} --bucket {1} --user {2} --password {3} " \
        #             "--count {4} --batch_size {5} --threads {6} --start_document {7} --cb_version {8} --instances {9} --ttl {10}"
        cmd_format = "python3 scripts/thanosied.py  --spec couchbase://{0} --bucket {1} --user {2} --password {3} " \
                     "--count {4} --batch_size {5} --threads {6} --start_document {7} --cb_version {8} --workers {9} --ttl {10}" \
                     "--passes 1"
        cb_version = RestConnection(server).get_nodes_version()[:3]
        if self.num_replicas > 0 and self.use_replica_to:
            cmd_format = "{} --replicate_to 1".format(cmd_format)
        cmd = cmd_format.format(server.ip, bucket.name, server.rest_username, server.rest_password, items, batch,
                                threads, start_document, cb_version, instances, ttl)
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
                total_loaded += int(load.split(':')[1].strip())
            self.assertEqual(total_loaded, items,
                             "Failed to load {} items. Loaded only {} items".format(items, total_loaded))

    def load_docs(self, bucket,num_items=0, start_document=0):
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
                                 args=(self.master, bucket, num_items, self.batch_size,
                                       self.threads, start_document, self.instances))
            return load_thread

    def check_data(self, server, bucket, num_items=0):
        if self.loader == "pillowfight":
            return self.check_dataloss(server, bucket, num_items)
        elif self.loader == "high_ops":
            return self.check_dataloss_for_high_ops_loader(server, bucket, num_items)

    def check_dataloss_for_high_ops_loader(self, server, bucket, items,
                                           batch=2000, threads=5,
                                           start_document=0,
                                           updated=False, ops=0,instances=1):
        import subprocess
        from lib.memcached.helper.data_helper import VBucketAwareMemcached
        #cmd_format = "python3 scripts/high_ops_doc_loader.py  --node {0} --bucket {1} --user {2} --password {3} " \
        #             "--count {4} " \
        #             "--batch_size {5} --instances {9} --threads {6} --start_document {7} --cb_version {8} --validate"
        cmd_format = "python3 scripts/thanosied.py  --spec couchbase://{0} --bucket {1} --user {2} --password {3} " \
                     "--count {4} --batch_size {5} --threads {6} --start_document {7} --cb_version {8} --workers {9} --validation 1 " \
                     "--passes 1"
        cb_version = RestConnection(server).get_nodes_version()[:3]
        if updated:
            cmd_format = "{} --updated --ops {}".format(cmd_format, int(ops))
        cmd = cmd_format.format(server.ip, bucket.name, server.rest_username,
                                server.rest_password,
                                int(items), batch, threads, start_document, cb_version, instances)
        self.log.info("Running {}".format(cmd))
        result = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE)
        output = result.stdout.read()
        error = result.stderr.read()
        errors = []
        rest = RestConnection(self.master)
        VBucketAware = VBucketAwareMemcached(rest, bucket.name)
        _, _, _ = VBucketAware.request_map(rest, bucket.name)
        if error:
            self.log.error(error)
            self.fail("Failed to run the loadgen validator.")
        if output:
            loaded = output.split('\n')[:-1]
            for load in loaded:
                if "Missing keys:" in load:
                    keys = load.split(":")[1].strip().replace('[', '').replace(']', '')
                    keys = keys.split(',')
                    for key in keys:
                        key = key.strip()
                        key = key.replace('\'', '').replace('\\', '')
                        vBucketId = VBucketAware._get_vBucket_id(key)
                        errors.append(
                            ("Missing key: {0}, VBucketId: {1}".format(key, vBucketId)))
                if "Mismatch keys: " in load:
                    keys = load.split(":")[1].strip().replace('[', '').replace(']', '')
                    keys = keys.split(',')
                    for key in keys:
                        key = key.strip()
                        key = key.replace('\'', '').replace('\\', '')
                        vBucketId = VBucketAware._get_vBucket_id(key)
                        errors.append((
                                      "Wrong value for key: {0}, VBucketId: {1}".format(
                                          key, vBucketId)))
        self.log.info("Total number of missing doc:{}".format(len(errors)))
        self.log.info("Missing/Mismatch keys:{}".format(errors))
        return errors

    def test_volume_with_high_ops_reproduce(self):
        rest = RestConnection(self.master)
        bucket = rest.get_buckets()
        start_at = 0
        # load initial documents
        self.create_ddocs_and_views()
        load_thread = []
        for bk in bucket:
            load_thread.append(Thread(target=self.load_buckets_with_high_ops, args=(
            self.master, bk, self.num_items, self.batch_size, self.threads, start_at, self.instances)))
        for t in load_thread:
            t.start()
        stats_dst = rest.get_bucket_stats()
        while stats_dst["curr_items"] < 1200000:
            self.sleep(300)
            stats_dst = rest.get_bucket_stats()
        # Rebalance in 1 node
        servers_init = self.servers[:self.nodes_init]
        # Rebalance in 1 node
        self.log.info("==========rebalance in 1 node=========")
        servers_in = self.servers[self.nodes_init:self.nodes_init + 1]
        rebalance = self.cluster.async_rebalance(servers_init, servers_in, [])
        rebalance.result()
        for t in load_thread:
            t.join()
        for b in bucket:
            errors=self.check_dataloss_for_high_ops_loader(self.master, b, self.num_items, instances=self.instances)
            if len(errors) > 0:
                self.fail("data is missing");
