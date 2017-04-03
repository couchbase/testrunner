import time
import random
from eviction.evictionbase import EvictionBase

from memcached.helper.data_helper import MemcachedClientHelper
import uuid
import mc_bin_client

from couchbase_helper.documentgenerator import BlobGenerator, DocumentGenerator, JSONNonDocGenerator
from couchbase_helper.stats_tools import StatsCommon

from membase.api.rest_client import RestConnection
from sdk_client import SDKClient
from dcp.dcpbase import DCPBase
from dcp.constants import *
import memcacheConstants as constants

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

class EphemeralBucketsOOM(EvictionBase, DCPBase):


    KEY_ROOT = 'key-root'
    KEY_ROOT_LENGTH = len('key-root')

    def setUp(self):
        super(EphemeralBucketsOOM, self).setUp()


    def tearDown(self):
        super(EphemeralBucketsOOM, self).tearDown()

    # Ephemeral buckets tests start here


    # Ephemeral bucket configured with no eviction
    # 1. Configure an ephemeral bucket with no eviction
    # 2. Set kvs until we get OOM returned - this is expected
    # 3. Explicitly delete some KVs
    # 4. Add more kvs - should succeed
    # 5. Add keys until OOM is returned

    # Need command line parameter to specify eviction mode as no_delete or something similar


    def test_ephemeral_bucket_no_deletions(self):


        # Coarse grained load to 90%

        generate_load = BlobGenerator(EphemeralBucketsOOM.KEY_ROOT, 'param2', self.value_size, start=0, end=self.num_items)
        self._load_all_ephemeral_buckets_until_no_more_memory(self.servers[0], generate_load, "create", 0, self.num_items)


        rest = RestConnection(self.servers[0])
        itemCount = rest.get_bucket(self.buckets[0]).stats.itemCount

        # load more until we are out of memory. Note these are different than the Blob generator - is that good or bad?

        #client = SDKClient(hosts = [self.master.ip], bucket = self.buckets[0])
        mc_client = MemcachedClientHelper.direct_client(self.servers[0], self.buckets[0])


        # fine grained load until OOM
        i = itemCount
        have_available_memory = True
        while have_available_memory:
            try:
                rc = mc_client.set(EphemeralBucketsOOM.KEY_ROOT+str(i),0,0,'anyoldval'*1000)
                #rc = client.set(EphemeralBucketsOOM.KEY_ROOT+str(i), 'b'*self.value_size )
                i = i + 1
            except:
                have_available_memory = False
                self.log.info('Memory is full at {0} items'.format(i))


        stats = rest.get_bucket(self.buckets[0]).stats
        itemCountWhenOOM = stats.itemCount
        memoryWhenOOM = stats.memUsed
        self.log.info('Item count when OOM {0} and memory used {1}'.format(itemCountWhenOOM, memoryWhenOOM))


        # delete some things using the Blob deleter
        NUMBER_OF_DOCUMENTS_TO_DELETE = 10000


        generate_delete = BlobGenerator(EphemeralBucketsOOM.KEY_ROOT, 'param2', self.value_size, start=0, end=NUMBER_OF_DOCUMENTS_TO_DELETE)
        self._load_all_buckets(self.master, generate_delete, "delete", 0) #, 1, 0, True, batch_size=20000, pause_secs=5, timeout_secs=180)


        stats = rest.get_bucket(self.buckets[0]).stats
        itemCountAfterDelete = stats.itemCount
        self.log.info('After the delete, item count {0} and memory used {1}'.format(itemCountAfterDelete, stats.memUsed))



        newly_added_items = 0
        have_available_memory =  True
        i = 1
        while have_available_memory:
            try:
                rc = mc_client.set(EphemeralBucketsOOM.KEY_ROOT+str(i+itemCountWhenOOM),0,0,'c'*self.value_size)
                i = i + 1
                newly_added_items = newly_added_items + 1
            except Exception as e:
                self.log.info('While repopulating {0} got an exception {1}'.format(i,str(e)))
                have_available_memory = False

        stats = rest.get_bucket(self.buckets[0]).stats
        self.log.info('Memory is again full at {0} items and memory used is {1}'.
                      format(stats.itemCount, stats.memUsed))
        self.log.info('Compared to previous fullness, we are at {0:.1f}% items and {1:.1f}% memory'.
                    format(100*stats.itemCount/itemCountWhenOOM, 100*stats.memUsed/memoryWhenOOM) )


        self.assertTrue(newly_added_items > 0.95 * NUMBER_OF_DOCUMENTS_TO_DELETE,
                        'Deleted {0} items and were only able to add back {1} items'.format(NUMBER_OF_DOCUMENTS_TO_DELETE,
                                newly_added_items))



    """


    # NRU Eviction - in general fully populate memory and then add more kvs and see what keys are
    # evicted using a DCP connection to monitor the deletes. Also check the delete stat e.g. auto_delete_count

    # And then access some keys via a variety of means, set, upsert, incr etc and verify they are not purged.
    # Monitoring is done with a DCP stream to collect the deletes and verify the accessed keys are not part
    # of the deletes.


   """

    KEY_ROOT = 'key-root'
    KEY_ROOT_LENGTH = len('key-root')


    def test_ephemeral_bucket_NRU_eviction(self):




        rest = RestConnection(self.servers[0])
        vbuckets = rest.get_vbuckets()

        generate_load = BlobGenerator(EphemeralBucketsOOM.KEY_ROOT, 'param2', self.value_size, start=0, end=self.num_items)
        self._load_all_ephemeral_buckets_until_no_more_memory(self.servers[0], generate_load, "create", 0, self.num_items)


        self.log.info('Memory is full')

        # get the sequence numbers so far
        # This bit of code is slow with 1024 vbuckets
        pre_delete_sequence_numbers = [0] * self.vbuckets
        for v in range(self.vbuckets):
            vb_uuid, seqno, high_seqno = self.vb_info(self.master, v)
            pre_delete_sequence_numbers[v] = high_seqno





        print 'pre_delete_sequence_numbers', pre_delete_sequence_numbers

        itemCount = rest.get_bucket(self.buckets[0]).stats.itemCount

        self.log.info( 'Reached OOM, the number of items is {0}'.format( itemCount))





        # To do: access random KVs, save the keys. Access different ways, get, set, incr etc.
        keys_that_were_accessed = []


        # load some more, this should trigger some deletes

        #client = SDKClient(hosts = [self.master.ip], bucket = self.buckets[0])
        mc_client = MemcachedClientHelper.direct_client(self.servers[0], self.buckets[0])


        # figure out how many items were loaded and load a certain percentage more

        for i in range(itemCount, int(itemCount*1.2)):
            rc = mc_client.set(EphemeralBucketsOOM.KEY_ROOT+str(i), 0,0, 'a'*self.value_size )
            if i % 1000 == 0:
                pass #pdb.set_trace()



        import pdb;pdb.set_trace()
        incremental_kv_population = BlobGenerator(EphemeralBucketsOOM.KEY_ROOT, 'param2', self.value_size, start=itemCount, end=itemCount * 1.2)
        #self._load_bucket(self.buckets[0], self.master, incremental_kv_population, "create", exp=0, kv_store=1)


        deleted_keys = []


        # creating a DCP client fails, maybe an auth issue?
        dcp_client = self.dcp_client(self.servers[0], 'producer', auth_user=True)

        for vb in vbuckets[0:self.vbuckets]:
                vbucket = vb.id
                print 'vbucket', vbucket
                vb_uuid, _, high_seqno = self.vb_info(self.servers[0], vbucket, bucket = self.buckets[0])
                print vb.id, 'streaming from', pre_delete_sequence_numbers[vb.id], ' to ', high_seqno
                stream = dcp_client.stream_req(vbucket, 0, pre_delete_sequence_numbers[vb.id], high_seqno, vb_uuid)
                responses = stream.run()
                for i in responses:
                    #if 'value' in i: del i['value']
                    #print 'the response opcode is', i #['opcode'],
                    #if 'key' in i: print 'key', i['key'],
                    #print ' ',

                    if i['opcode'] == constants.CMD_DELETION: #
                        # have a delete, get the key number
                        index = int(i['key'][EphemeralBucketsOOM.KEY_ROOT_LENGTH:])
                        print 'delete:the key number is', index
                        deleted_keys.append( index )





        # verify that the intersection of deleted_keys and keys_that_were_accessed is empty

        pdb.set_trace()

