import time
from eviction.evictionbase import EvictionBase

from memcached.helper.data_helper import MemcachedClientHelper
import uuid
import mc_bin_client

class EvictionKV(EvictionBase):


    def verify_all_nodes(self):
        stats_tasks = []
        for s in self.servers:
            stats_tasks.append( self.cluster.async_wait_for_stats([s], "default", "",
                                    "curr_items", "==", 0) )

        for task in stats_tasks:
            task.result(60)

    def test_verify_expiry_via_compactor_cancelled_compact(self):

        self.load_set_to_be_evicted(20, 10000)
        self.log.info("sleeping {0} seconds to allow keys to be evicted".format(self.expires + 30 ))
        time.sleep(self.expires + 30)
        self.run_expiry_pager(60*60*24)   # effectively disable it by setting it far off in the future

        compacted = self.cluster.compact_bucket(self.master, 'default')
        self.assertTrue(compacted, msg="unable compact_bucket")

        self.cluster.cancel_bucket_compaction(self.master, 'default')

        # start compaction again
        time.sleep(5)
        compacted = self.cluster.compact_bucket(self.master, 'default')
        self.assertTrue(compacted, msg="unable compact_bucket")


        self.cluster.wait_for_stats([self.master],
                                    "default", "",
                                    "curr_items", "==", 0, timeout=30)


    def test_verify_expiry_via_compactor(self):



        self.run_expiry_pager(60*60*24*7)      # effectively disable it by setting it one week ahead
        self.load_set_to_be_evicted(self.expires, self.keys_count)

        self.log.info("sleeping {0} seconds to allow keys to be evicted".format(self.expires + 30 ))
        time.sleep(self.expires + 30 )   # 30 seconds grace period


        # run the compactor which should expire the kyes
        compacted = self.cluster.compact_bucket(self.master, 'default')

        self.verify_all_nodes()






    """
    add new keys at the same rate as keys are expiring
    Here is the algorithm:
      - initally set n keys to expire in chunk, the first chunk expiring after the keys are initially set - thus
        the init time delay value added to the expiry time - this may need to be adjusted
      - in each interval set the same number of keys as is expected to expire

    Note: this us implemented with compaction doing the eviction, in the future this could be enhanced to have
    the pager expiry doing the eviction.
    """

    def test_steady_state_eviction(self):
        serverInfo = self.master
        client = MemcachedClientHelper.direct_client(serverInfo, 'default')

        expiry_time = self.input.param("expiry_time", 30)
        keys_expired_per_interval = self.input.param("keys_expired_per_interval", 100)
        key_float = self.input.param("key_float", 1000)

        # we want to have keys expire after all the key are initialized, thus the below parameter
        init_time_delay = self.input.param("init_time_delay",30)
        test_time_in_minutes = self.input.param( "test_time_in_minutes",30)



        # rampup - create a certain number of keys
        float_creation_chunks = key_float / keys_expired_per_interval
        print 'float_creation_chunks', float_creation_chunks
        for i in range( float_creation_chunks):
            #print 'setting', keys_expired_per_interval, ' keys to expire in', expiry_time * (i+1)
            for j in range(keys_expired_per_interval):
               key = str(uuid.uuid4()) + str(i) + str(j)
               client.set(key, init_time_delay + expiry_time * (i+1), 0, key)


        for i in range(test_time_in_minutes * 60/expiry_time):


            key_set_time = int( time.time())

            # ClusterOperationHelper.set_expiry_pager_sleep_time(self.master, 'default')
            testuuid = uuid.uuid4()
            keys = ["key_%s_%d" % (testuuid, i) for i in range(keys_expired_per_interval)]
            self.log.info("pushing keys with expiry set to {0}".format(expiry_time))
            for key in keys:
                try:
                    client.set(key, expiry_time + key_float/expiry_time, 0, key)
                except mc_bin_client.MemcachedError as error:
                    msg = "unable to push key : {0} to bucket : {1} error : {2}"
                    self.log.error(msg.format(key, client.vbucketId, error.status))
                    self.fail(msg.format(key, client.vbucketId, error.status))
            self.log.info("inserted {0} keys with expiry set to {1}".format(len(keys), expiry_time))
            self.log.info('sleeping {0} seconds'.format(expiry_time - (time.time()- key_set_time) ) )


            # have the compactor do the expiry
            compacted = self.cluster.compact_bucket(self.master, 'default')

            self.cluster.wait_for_stats([self.master], "default", "", "curr_items", "==", key_float, timeout=30)
            time.sleep( expiry_time - (time.time()- key_set_time))





    def test_verify_expiry(self):
        """
            heavy dgm purges expired items via compactor.
            this is a smaller test to load items below compactor
            threshold and check if items still expire via pager
        """

        self.load_set_to_be_evicted(20, 100)
        self.run_expiry_pager()
        print "sleep 40 seconds and verify all items expired"
        time.sleep(40)
        self._verify_all_buckets(self.master)
        self.cluster.wait_for_stats([self.master],
                                    "default", "",
                                    "curr_items", "==", 0, timeout=60)


    def test_eject_all_ops(self):
        """
            eject all items and ensure items can still be retrieved, deleted, and udpated
            when fulleviction enabled
        """
        self.load_ejected_set(600)
        self.load_to_dgm()

        self.ops_on_ejected_set("read",   1,   200)
        self.ops_on_ejected_set("delete", 201, 400)
        self.ops_on_ejected_set("update", 401, 600)

    def test_purge_ejected_docs(self):
        """
           Go into dgm with expired items and verify at end of load no docs expired docs remain
           and ejected docs can still be deleted resulting in 0 docs left in cluster
        """
        num_ejected = 100
        ttl = 240

        self.load_ejected_set(num_ejected)
        self.load_to_dgm(ttl=ttl)


        while ttl > 0:
            self.log.info("%s seconds until loaded docs expire" % ttl)
            time.sleep(10)
            ttl = ttl - 10


        # compact to purge expiring docs
        compacted = self.cluster.compact_bucket(self.master, 'default')
        self.assertTrue(compacted, msg="unable compact_bucket")

        iseq = self.cluster.wait_for_stats([self.master],
                                           "default", "",
                                           "curr_items", "==", num_ejected, timeout=120)
        self.assertTrue(iseq, msg="curr_items != {0}".format(num_ejected))


        # delete remaining non expiring docs
        self.ops_on_ejected_set("delete", 0, num_ejected)
        iseq = self.cluster.wait_for_stats([self.master],
                                            "default", "",
                                            "curr_items", "==", 0, timeout=30)
        self.assertTrue(iseq, msg="curr_items != {0}".format(0))



    def test_update_ejected_expiry_time(self):
        """
            eject all items set to expire
        """

        self.load_ejected_set(100)
        self.load_to_dgm(ttl=30)
        self.ops_on_ejected_set("update", ttl=30)

        # run expiry pager
        self.run_expiry_pager()

        self.cluster.wait_for_stats([self.master],
                                    "default", "",
                                    "curr_items", "==", 0, timeout=30)

