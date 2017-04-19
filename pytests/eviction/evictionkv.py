import time
import random
from eviction.evictionbase import EvictionBase
from couchbase_helper.document import View
from membase.api.exception import DesignDocCreationException
from membase.helper.bucket_helper import BucketOperationHelper

from memcached.helper.data_helper import MemcachedClientHelper
import uuid
import mc_bin_client

from couchbase_helper.documentgenerator import BlobGenerator, DocumentGenerator, JSONNonDocGenerator
from remote.remote_util import RemoteMachineShellConnection

from membase.api.rest_client import RestConnection
from dcp.dcpbase import DCPBase
import memcacheConstants as constants


class EvictionKV(EvictionBase):
    def verify_all_nodes(self):
        stats_tasks = []
        for s in self.servers:
            stats_tasks.append(self.cluster.async_wait_for_stats([s], "default", "",
                                                                 "curr_items", "==", 0))

        for task in stats_tasks:
            task.result(60)

    def test_verify_expiry_via_compactor_cancelled_compact(self):

        self.load_set_to_be_evicted(20, 10000)
        self.log.info("sleeping {0} seconds to allow keys to be evicted".format(self.expires + 30))
        time.sleep(self.expires + 30)
        self.run_expiry_pager(60 * 60 * 24)  # effectively disable it by setting it far off in the future

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

        self.run_expiry_pager(60 * 60 * 24 * 7)  # effectively disable it by setting it one week ahead
        self.load_set_to_be_evicted(self.expires, self.keys_count)

        self.log.info("sleeping {0} seconds to allow keys to be evicted".format(self.expires + 30))
        time.sleep(self.expires + 30)  # 30 seconds grace period

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
        # serverInfo = self.master
        client = MemcachedClientHelper.direct_client(self.master, 'default')

        expiry_time = self.input.param("expiry_time", 30)
        keys_expired_per_interval = self.input.param("keys_expired_per_interval", 100)
        key_float = self.input.param("key_float", 1000)

        # we want to have keys expire after all the key are initialized, thus the below parameter
        init_time_delay = self.input.param("init_time_delay", 30)
        test_time_in_minutes = self.input.param("test_time_in_minutes", 30)

        # rampup - create a certain number of keys
        float_creation_chunks = key_float / keys_expired_per_interval
        print 'float_creation_chunks', float_creation_chunks
        for i in range(float_creation_chunks):
            # print 'setting', keys_expired_per_interval, ' keys to expire in', expiry_time * (i+1)
            for j in range(keys_expired_per_interval):
                key = str(uuid.uuid4()) + str(i) + str(j)
                client.set(key, init_time_delay + expiry_time * (i + 1), 0, key)

        for i in range(test_time_in_minutes * 60 / expiry_time):

            key_set_time = int(time.time())

            # ClusterOperationHelper.set_expiry_pager_sleep_time(self.master, 'default')
            # testuuid = uuid.uuid4()
            keys = ["key_%s_%d" % (uuid.uuid4(), i) for i in range(keys_expired_per_interval)]
            self.log.info("pushing keys with expiry set to {0}".format(expiry_time))
            for key in keys:
                try:
                    client.set(key, expiry_time + key_float / expiry_time, 0, key)
                except mc_bin_client.MemcachedError as error:
                    msg = "unable to push key : {0} to bucket : {1} error : {2}"
                    self.log.error(msg.format(key, client.vbucketId, error.status))
                    self.fail(msg.format(key, client.vbucketId, error.status))
            self.log.info("inserted {0} keys with expiry set to {1}".format(len(keys), expiry_time))
            self.log.info('sleeping {0} seconds'.format(expiry_time - (time.time() - key_set_time)))

            # have the compactor do the expiry
            compacted = self.cluster.compact_bucket(self.master, 'default')

            self.cluster.wait_for_stats([self.master], "default", "", "curr_items", "==", key_float, timeout=30)
            time.sleep(expiry_time - (time.time() - key_set_time))

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
            eject all items and ensure items can still be retrieved, deleted, and udpdated
            when fulleviction enabled
        """
        self.load_ejected_set(600)
        self.load_to_dgm()

        self.ops_on_ejected_set("read", 1, 200)
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
            ttl -= 10

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

    def test_ephemeral_bucket_stats(self):
        shell = RemoteMachineShellConnection(self.master)
        rest = RestConnection(self.servers[0])

        generate_load = BlobGenerator(EphemeralBucketsOOM.KEY_ROOT, 'param2', self.value_size, start=0,
                                      end=self.num_items)
        self._load_all_ephemeral_buckets_until_no_more_memory(self.servers[0], generate_load, "create", 0,
                                                              self.num_items, percentage=0.8)

        self.log.info('Memory almost full')
        output, error = shell.execute_command("/opt/couchbase/bin/cbstats localhost:11210 -b default"
                                              " all -u Administrator -p password | grep ephemeral")
        if self.input.param('eviction_policy', 'noEviction') == 'noEviction':
            self.assertEquals([' ep_bucket_type:                                        ephemeral',
                               ' ep_ephemeral_full_policy:                              fail_new_data'], output)
        else:
            self.assertEquals([' ep_bucket_type:                                        ephemeral',
                              ' ep_ephemeral_full_policy:                              auto_delete'], output)

        output, error = shell.execute_command("/opt/couchbase/bin/cbstats localhost:11210 -b default "
                                              "vbucket-details -u Administrator -p password "
                                              "| grep seqlist_deleted_count")
        self.assertEquals(' vb_0:seqlist_deleted_count:              0', output[0])

        item_count = rest.get_bucket(self.buckets[0]).stats.itemCount
        output, error = shell.execute_command("/opt/couchbase/bin/cbstats localhost:11210 -b default all"
                                              " -u Administrator -p password | grep curr_items")
        self.assertEquals(' curr_items:                                            %s' % item_count, output[0])

        self.log.info('Reached OOM, the number of items is {0}'.format(item_count))

        # keys_that_were_accessed = []

        # load some more, this should trigger some deletes
        mc_client = MemcachedClientHelper.direct_client(self.servers[0], self.buckets[0])

        for i in xrange(200):
            key = random.randint(0, 1200)
            mc_client.get(EphemeralBucketsOOM.KEY_ROOT + str(key))
        # add ~20% of new items
        for i in range(item_count, int(item_count * 1.2)):
            mc_client.set(EphemeralBucketsOOM.KEY_ROOT + str(i), 0, 0, 'a' * self.value_size)
        self.sleep(10)
        item_count = rest.get_bucket(self.buckets[0]).stats.itemCount
        output, error = shell.execute_command("/opt/couchbase/bin/cbstats localhost:11210 -b default all"
                                              " -u Administrator -p password | grep curr_items")
        self.assertEquals(' curr_items:                                            %s' % item_count, output[0])
        output, error = shell.execute_command("/opt/couchbase/bin/cbstats localhost:11210 -b default "
                                              "vbucket-details -u Administrator -p password "
                                              "| grep seqlist_deleted_count")
        if self.input.param('eviction_policy', 'noEviction') == 'noEviction':
            self.assertEquals(' vb_0:seqlist_deleted_count:              0', output[0], 'have deleted items!')
        else:
            self.assertTrue(int(output[0].replace(' vb_0:seqlist_deleted_count:              ', '')) > 0,
                            'no deleted items!')

    #https://issues.couchbase.com/browse/MB-23988
    def test_ephemeral_bucket_views(self):
        default_map_func = "function (doc, meta) {emit(meta.id, null);}"
        default_view_name = ("xattr", "default_view")[False]
        view = View(default_view_name, default_map_func, None, False)

        ddoc_name = "ddoc1"
        tasks = self.async_create_views(self.master, ddoc_name, [view], self.buckets[0].name)
        for task in tasks:
            try:
                task.result()
                self.fail("Views not allowed for ephemeral buckets")
            except DesignDocCreationException as e:
                self.assertEquals(e._message, 'Error occured design document _design/ddoc1: {"error":"not_found","reason":"no_couchbase_bucket_exists"}\n')


class EphemeralBackupRestoreTest(EvictionBase):
    def setUp(self):
        super(EvictionBase, self).setUp()
        self.only_store_hash = False
        self.shell = RemoteMachineShellConnection(self.master)

    def tearDown(self):
        super(EvictionBase, self).tearDown()

    def _load_all_buckets(self):
        generate_load = BlobGenerator(EphemeralBucketsOOM.KEY_ROOT, 'param2', self.value_size, start=0,
                                      end=self.num_items)
        self._load_all_ephemeral_buckets_until_no_more_memory(self.servers[0], generate_load, "create", 0,
                                                              self.num_items, percentage=0.80)
    # https://issues.couchbase.com/browse/MB-23992
    def test_backup_restore(self):
        self._load_all_buckets()
        self.shell.execute_command("rm -rf /tmp/backups")
        output, error = self.shell.execute_command("/opt/couchbase/bin/cbbackupmgr config "
                                                   "--archive /tmp/backups --repo example")
        self.log.info(output)
        self.assertEquals('Backup repository `example` created successfully in archive `/tmp/backups`', output[0])
        output, error = self.shell.execute_command(
            "/opt/couchbase/bin/cbbackupmgr backup --archive /tmp/backups --repo example "
            "--cluster couchbase://127.0.0.1 --username Administrator --password password")
        self.log.info(output)
        self.assertEquals('Backup successfully completed', output[1])
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)
        imp_rest = RestConnection(self.master)
        info = imp_rest.get_nodes_self()
        if info.memoryQuota and int(info.memoryQuota) > 0:
            self.quota = info.memoryQuota
        bucket_params = self._create_bucket_params(server=self.master, size=250, bucket_type='ephemeral',
                                                   replicas=self.num_replicas,
                                                   enable_replica_index=self.enable_replica_index,
                                                   eviction_policy=self.eviction_policy)
        self.cluster.create_default_bucket(bucket_params)
        output, error = self.shell.execute_command('ls /tmp/backups/example')
        output, error = self.shell.execute_command("/opt/couchbase/bin/cbbackupmgr restore --archive /tmp/backups"
                                                   " --repo example --cluster couchbase://127.0.0.1 "
                                                   "--username Administrator --password password --start %s" % output[0])
        self.log.info(output)
        self.assertEquals('Restore completed successfully', output[1])
        self._verify_all_buckets(self.master)


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

        generate_load = BlobGenerator(EphemeralBucketsOOM.KEY_ROOT, 'param2', self.value_size, start=0,
                                      end=self.num_items)
        self._load_all_ephemeral_buckets_until_no_more_memory(self.servers[0], generate_load, "create", 0,
                                                              self.num_items)

        rest = RestConnection(self.servers[0])
        item_count = rest.get_bucket(self.buckets[0]).stats.itemCount

        # load more until we are out of memory. Note these are different than the Blob generator - is that good or bad?

        mc_client = MemcachedClientHelper.direct_client(self.servers[0], self.buckets[0])

        # fine grained load until OOM
        i = item_count
        have_available_memory = True
        while have_available_memory:
            try:
                mc_client.set(EphemeralBucketsOOM.KEY_ROOT + str(i), 0, 0, 'O' * self.value_size)
                i += 1
            except:
                have_available_memory = False
                self.log.info('Memory is full at {0} items'.format(i))

        stats = rest.get_bucket(self.buckets[0]).stats
        itemCountWhenOOM = stats.itemCount
        memoryWhenOOM = stats.memUsed
        self.log.info('Item count when OOM {0} and memory used {1}'.format(itemCountWhenOOM, memoryWhenOOM))

        # delete some things using the Blob deleter
        NUMBER_OF_DOCUMENTS_TO_DELETE = 10000

        for i in xrange(NUMBER_OF_DOCUMENTS_TO_DELETE):
            mc_client.delete(EphemeralBucketsOOM.KEY_ROOT + str(i))

        stats = rest.get_bucket(self.buckets[0]).stats
        self.log.info(
            'After the delete, item count {0} and memory used {1}'.format(stats.itemCount, stats.memUsed))

        newly_added_items = 0
        have_available_memory = True
        while have_available_memory:
            try:
                mc_client.set(EphemeralBucketsOOM.KEY_ROOT + str(newly_added_items + itemCountWhenOOM + 1), 0, 0,
                              'N' * self.value_size)
                newly_added_items += 1
            except Exception as e:
                self.log.info('While repopulating {0} got an exception {1}'.format(newly_added_items + 1, str(e)))
                have_available_memory = False

        stats = rest.get_bucket(self.buckets[0]).stats
        self.log.info('Memory is again full at {0} items and memory used is {1}'.
                      format(stats.itemCount, stats.memUsed))
        self.log.info('Compared to previous fullness, we are at {0:.1f}% items and {1:.1f}% memory'.
                      format(100 * stats.itemCount / itemCountWhenOOM, 100 * stats.memUsed / memoryWhenOOM))

        self.assertTrue(newly_added_items > 0.95 * NUMBER_OF_DOCUMENTS_TO_DELETE,
                        'Deleted {0} items and were only able to add back {1} items'.format(
                            NUMBER_OF_DOCUMENTS_TO_DELETE,
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

        generate_load = BlobGenerator(EphemeralBucketsOOM.KEY_ROOT, 'param2', self.value_size, start=0,
                                      end=self.num_items)
        self._load_all_ephemeral_buckets_until_no_more_memory(self.servers[0], generate_load, "create", 0,
                                                              self.num_items, percentage=0.8)

        self.log.info('Memory is full')

        # get the sequence numbers so far
        # This bit of code is slow with 1024 vbuckets
        pre_delete_sequence_numbers = [0] * self.vbuckets
        for v in range(self.vbuckets):
            vb_uuid, seqno, high_seqno = self.vb_info(self.master, v)
            pre_delete_sequence_numbers[v] = high_seqno

        item_count = rest.get_bucket(self.buckets[0]).stats.itemCount

        self.log.info('Reached OOM, the number of items is {0}'.format(item_count))

        keys_that_were_accessed = []

        # load some more, this should trigger some deletes
        mc_client = MemcachedClientHelper.direct_client(self.servers[0], self.buckets[0])

        # figure out how many items were loaded and load a certain percentage more

        item_count = rest.get_bucket(self.buckets[0]).stats.itemCount
        self.item_count.info('The number of items is {0}'.format(item_count))

        dcp_client = self.dcp_client(self.servers[0], 'producer')
        num = 0
        deleted_keys = []
        for vb in vbuckets[0:self.vbuckets]:
            vbucket = vb.id
            vb_uuid, _, high_seqno = self.vb_info(self.servers[0], vbucket, bucket=self.buckets[0])
            stream = dcp_client.stream_req(vbucket, 0, pre_delete_sequence_numbers[vb.id], high_seqno, vb_uuid)
            responses = stream.run()
            for i in responses:
                if i['opcode'] == constants.CMD_DELETION:  #
                    # have a delete, get the key number
                    index = int(i['key'][EphemeralBucketsOOM.KEY_ROOT_LENGTH:])
                    deleted_keys.append(index)
                    num += 1
        self.assertEquals([], deleted_keys)

        for i in xrange(200):
            key = random.randint(0, 1200)
            mc_client.get(EphemeralBucketsOOM.KEY_ROOT + str(key))
            keys_that_were_accessed.append(key)
        # add ~20% of new items
        for i in range(item_count, int(item_count * 1.2)):
            mc_client.set(EphemeralBucketsOOM.KEY_ROOT + str(i), 0, 0, 'a' * self.value_size)

        item_count = rest.get_bucket(self.buckets[0]).stats.itemCount
        self.log.info('The number of items is {0}'.format(item_count))
        deleted_keys = []

        # creating a DCP client fails, maybe an auth issue?
        dcp_client = self.dcp_client(self.servers[0], 'producer')
        num = 0
        for vb in vbuckets[0:self.vbuckets]:
            vbucket = vb.id
            vb_uuid, _, high_seqno = self.vb_info(self.servers[0], vbucket, bucket=self.buckets[0])
            stream = dcp_client.stream_req(vbucket, 0, pre_delete_sequence_numbers[vb.id], high_seqno, vb_uuid)
            responses = stream.run()
            for i in responses:
                if i['opcode'] == constants.CMD_DELETION:  #
                    # have a delete, get the key number
                    index = int(i['key'][EphemeralBucketsOOM.KEY_ROOT_LENGTH:])
                    deleted_keys.append(index)
                    num += 1
        item_count = rest.get_bucket(self.buckets[0]).stats.itemCount
        self.log.info('The number of items is {0}'.format(item_count))
        self.assertEquals(set([]), set(keys_that_were_accessed).intersection(deleted_keys))

        # one more iteration
        deleted_keys = []
        keys_that_were_accessed = []
        set(keys_that_were_accessed).intersection(deleted_keys)

        for i in xrange(200):
            key = random.randint(0, 12000)
            try:
                mc_client.get(EphemeralBucketsOOM.KEY_ROOT + str(key))
                keys_that_were_accessed.append(key)
            except mc_bin_client.MemcachedError:
                self.log.info('key %s already deleted' % key)
        # add ~10% of new items
        for i in range(int(item_count * 1.2), int(item_count * 1.4)):
            mc_client.set(EphemeralBucketsOOM.KEY_ROOT + str(i), 0, 0, 'a' * self.value_size)
            keys_that_were_accessed.append(i)

        item_count = rest.get_bucket(self.buckets[0]).stats.itemCount
        self.log.info('The number of items is {0}'.format(item_count))

        for v in range(self.vbuckets):
            vb_uuid, seqno, high_seqno = self.vb_info(self.master, v)
            pre_delete_sequence_numbers[v] = high_seqno

        # creating a DCP client fails, maybe an auth issue?
        dcp_client = self.dcp_client(self.servers[0], 'producer')
        num = 0
        for vb in vbuckets[0:self.vbuckets]:
            vbucket = vb.id
            vb_uuid, _, high_seqno = self.vb_info(self.servers[0], vbucket, bucket=self.buckets[0])
            print vb.id, 'streaming from', pre_delete_sequence_numbers[vb.id], ' to ', high_seqno
            stream = dcp_client.stream_req(vbucket, 0, pre_delete_sequence_numbers[vb.id], high_seqno, vb_uuid)
            responses = stream.run()
            for i in responses:
                if i['opcode'] == constants.CMD_DELETION:  #
                    # have a delete, get the key number
                    index = int(i['key'][EphemeralBucketsOOM.KEY_ROOT_LENGTH:])
                    deleted_keys.append(index)
                    num += 1
        item_count = rest.get_bucket(self.buckets[0]).stats.itemCount
        self.log.info('The number of items is {0}'.format(item_count))
        self.assertEquals(set([]), set(keys_that_were_accessed).intersection(deleted_keys))
