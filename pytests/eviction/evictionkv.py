import random
import time
import uuid

import mc_bin_client
import memcacheConstants as constants
from couchbase_helper.document import View
from couchbase_helper.documentgenerator import BlobGenerator
from dcp.dcpbase import DCPBase
from eviction.evictionbase import EvictionBase
from membase.api.exception import DesignDocCreationException
from membase.api.rest_client import RestConnection
from membase.helper.bucket_helper import BucketOperationHelper
from memcached.helper.data_helper import MemcachedClientHelper
from remote.remote_util import RemoteMachineShellConnection
from dcp_bin_client import DcpClient


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
        if self.bucket_type == 'ephemeral':
            self.assertFalse(compacted, msg="able compact_bucket")
            return
        else:
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

        # run the compactor which should expire the keys
        compacted = self.cluster.compact_bucket(self.master, 'default')

        self.verify_all_nodes()

    """
    add new keys at the same rate as keys are expiring
    Here is the algorithm:
      - initially set n keys to expire in chunk, the first chunk expiring after the keys are initially set - thus
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
        initial_key_set_time = []
        # rampup - create a certain number of keys
        float_creation_chunks = key_float / keys_expired_per_interval

        for i in range(float_creation_chunks):
            for j in range(keys_expired_per_interval):
                key = str(uuid.uuid4()) + str(i) + str(j)
                client.set(key, init_time_delay + expiry_time * (i + 1), 0, key)
            #pause before second chunk gets set 
            time.sleep(0.01)
            initial_key_set_time.append(time.time())
        self.sleep(init_time_delay + expiry_time - (time.time() - initial_key_set_time[0]),
                   "Waiting for first chunk to expire")
        for chunk in range(float_creation_chunks):
            key_set_time = int(time.time())

            keys = ["key_%s_%d" % (uuid.uuid4(), i) for i in range(keys_expired_per_interval)]
            self.log.info("pushing keys with expiry set to {0}".format(expiry_time * float_creation_chunks))
            for key in keys:
                try:
                    client.set(key, expiry_time * float_creation_chunks, 0, key)
                except mc_bin_client.MemcachedError as error:
                    msg = "unable to push key : {0} to bucket : {1} error : {2}"
                    self.log.error(msg.format(key, client.vbucketId, error.status))
                    self.fail(msg.format(key, client.vbucketId, error.status))
            #Buffer for the keys to get initialize
            time.sleep(5)
            self.log.info("inserted {0} keys with expiry set to {1}".format(len(keys), expiry_time * float_creation_chunks))

            compacted = self.cluster.compact_bucket(self.master, 'default')
            # have the compactor do the expiry
            if self.bucket_type == 'ephemeral':
                self.assertFalse(compacted, msg="able compact_bucket")
                return
            else:
                self.assertTrue(compacted, msg="unable compact_bucket")
            self.cluster.wait_for_stats([self.master], "default", "", "curr_items", "==", key_float, timeout=30)
            exp_time = expiry_time - (time.time() - key_set_time)
            self.sleep(exp_time)

    def test_verify_expiry(self):
        """
            heavy dgm purges expired items via compactor.
            this is a smaller test to load items below compactor
            threshold and check if items still expire via pager
        """

        self.load_set_to_be_evicted(20, 100)
        self.run_expiry_pager()
        print("sleep 40 seconds and verify all items expired")
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

        num_ejected_items = 100
        key_prefix='dgmkv'

        # load initial set of keys, then to dgm with expectation
        # that initial keys will be ejected
        self.load_ejected_set(num_ejected_items)
        total_items = self.load_to_dgm(ttl=60, prefix=key_prefix)
        self.ops_on_ejected_set("update", ttl=60)

        # run expiry pager
        self.run_expiry_pager()

        # make sure all items have expired
        time.sleep(65)

        # attempt to get expired keys.
        # this also helps drop item count down to 0
        self.verify_missing_keys(key_prefix, total_items)
        self.verify_missing_keys("ejected", num_ejected_items)


        self.cluster.wait_for_stats([self.master],
                                    "default", "",
                                    "curr_items", "==", 0, timeout=300)

    def test_ephemeral_bucket_stats(self):
        '''
        @summary: We load bucket to 85% of its capacity and then validate
        the ephemeral stats for it. We then again load bucket by 50% of 
        exiting capacity and again validate stats.
        '''
        shell = RemoteMachineShellConnection(self.master)
        rest = RestConnection(self.servers[0])

        generate_load = BlobGenerator(EphemeralBucketsOOM.KEY_ROOT, 'param2', self.value_size, start=0,
                                      end=self.num_items)
        self._load_all_ephemeral_buckets_until_no_more_memory(self.servers[0], generate_load, "create", 0,
                                                              self.num_items, percentage=0.85)

        self.log.info('Memory almost full. Getting stats...')
        time.sleep(10)
        output, error = shell.execute_command("/opt/couchbase/bin/cbstats localhost:11210 -b default"
                                              " all -u Administrator -p password | grep ephemeral")
        if self.input.param('eviction_policy', 'noEviction') == 'noEviction':
            self.assertEqual([' ep_bucket_type:                                        ephemeral',
                               ' ep_dcp_ephemeral_backfill_type:                        buffered',
                               ' ep_ephemeral_full_policy:                              fail_new_data',
                               ' ep_ephemeral_metadata_purge_age:                       259200',
                               ' ep_ephemeral_metadata_purge_chunk_duration:            20',
                               ' ep_ephemeral_metadata_purge_interval:                  60'], output)
        else:
            self.assertEqual([' ep_bucket_type:                                        ephemeral',
                               ' ep_dcp_ephemeral_backfill_type:                        buffered',
                               ' ep_ephemeral_full_policy:                              auto_delete',
                               ' ep_ephemeral_metadata_purge_age:                       259200',
                               ' ep_ephemeral_metadata_purge_chunk_duration:            20',
                               ' ep_ephemeral_metadata_purge_interval:                  60'], output)

        output, error = shell.execute_command("/opt/couchbase/bin/cbstats localhost:11210 -b default "
                                              "vbucket-details -u Administrator -p password "
                                              "| grep seqlist_deleted_count")
        self.assertEqual(' vb_0:seqlist_deleted_count:              0', output[0])

        item_count = rest.get_bucket(self.buckets[0]).stats.itemCount
        self.log.info('rest.get_bucket(self.buckets[0]).stats.itemCount: %s' % item_count)
        output, error = shell.execute_command("/opt/couchbase/bin/cbstats localhost:11210 -b default all"
                                              " -u Administrator -p password | grep curr_items")
        self.log.info(output)
        self.assertEqual(' curr_items:                                            %s' % item_count, output[0])

        self.log.info('The number of items when almost reached OOM is {0}'.format(item_count))

        mc_client = MemcachedClientHelper.direct_client(self.servers[0], self.buckets[0])

        # load some more, this should trigger some deletes
        # add ~50% of new items
        for i in range(item_count, int(item_count * 1.5)):
            try:
                mc_client.set(EphemeralBucketsOOM.KEY_ROOT + str(i), 0, 0, 'a' * self.value_size)
                if i%3000 == 0:#providing buffer for nrueviction of items
                    time.sleep(10)
            except:
                if self.input.param('eviction_policy', 'noEviction') == 'noEviction':
                    break
                else:
                    raise
        self.sleep(10)
        item_count = rest.get_bucket(self.buckets[0]).stats.itemCount
        self.log.info("items count after we tried to add +50 per : %s" % item_count)
        output, error = shell.execute_command("/opt/couchbase/bin/cbstats localhost:11210 -b default all"
                                              " -u Administrator -p password | grep curr_items")
        self.assertEqual(' curr_items:                                            %s' % item_count, output[0])

        output, error = shell.execute_command("/opt/couchbase/bin/cbstats localhost:11210 -b default "
                                              "vbucket-details -u Administrator -p password "
                                              "| grep seqlist_deleted_count")
        self.log.info(output)
        if self.input.param('eviction_policy', 'noEviction') == 'noEviction':
            self.assertEqual(' vb_0:seqlist_deleted_count:              0', output[0], 'have deleted items!')
        else:
            self.assertTrue(int(output[0].replace(' vb_0:seqlist_deleted_count:              ', '')) > 0,
                            'no deleted items!')

    # https://issues.couchbase.com/browse/MB-23988
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
                self.assertEqual(e._message,
                                  'Error occured design document _design/ddoc1: {"error":"not_found","reason":"views are supported only on couchbase buckets"}\n')


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
                                                              self.num_items, percentage=0.8)

    # https://issues.couchbase.com/browse/MB-23992
    def test_backup_restore(self):
        self._load_all_buckets()
        self.shell.execute_command("rm -rf /tmp/backups")
        output, error = self.shell.execute_command("/opt/couchbase/bin/cbbackupmgr config "
                                                   "--archive /tmp/backups --repo example")
        self.log.info(output)
        self.assertEqual('Backup repository `example` created successfully in archive `/tmp/backups`', output[0])
        output, error = self.shell.execute_command(
            "/opt/couchbase/bin/cbbackupmgr backup --archive /tmp/backups --repo example "
            "--cluster couchbase://127.0.0.1 --username Administrator --password password")
        self.log.info(output)
        self.assertEqual('Backup successfully completed', output[1])
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
                                                   "--username Administrator --password password --start %s" % output[
                                                       0])
        self.log.info(output)
        self.assertEqual('Restore completed successfully', output[1])
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
                                                              self.num_items, percentage=0.85)

        rest = RestConnection(self.servers[0])
        item_count = rest.get_bucket(self.buckets[0]).stats.itemCount
        self.log.info("completed base load with %s items" % item_count)  # we expect 2400 docs

        # load more until we are out of memory. Note these are different than the Blob generator - is that good or bad?
        self.log.info("load more until we are out of memory...")
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
                self.log.info('Memory is full at {0} items'.format(i))  # +4000 expected

        self.log.info("as a result added more %s items" % (i - item_count))

        stats = rest.get_bucket(self.buckets[0]).stats
        itemCountWhenOOM = stats.itemCount
        memoryWhenOOM = stats.memUsed
        self.log.info('Item count when OOM {0} and memory used {1}'.format(itemCountWhenOOM, memoryWhenOOM))

        NUMBER_OF_DOCUMENTS_TO_DELETE = 4000

        for i in range(item_count, item_count + NUMBER_OF_DOCUMENTS_TO_DELETE):
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
                      format(100 * stats.itemCount // itemCountWhenOOM, 100 * stats.memUsed // memoryWhenOOM))

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
                                                              self.num_items, percentage=0.80)

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
        self.assertEqual([], deleted_keys)
        
        for i in range(200):
            key = random.randint(0, 1200)
            mc_client.get(EphemeralBucketsOOM.KEY_ROOT + str(key))
            keys_that_were_accessed.append(key)
        # add ~20% of new items
        try:
            for i in range(item_count, int(item_count * 1.2)):
                mc_client.set(EphemeralBucketsOOM.KEY_ROOT + str(i), 0, 0, 'a' * self.value_size)
        except mc_bin_client.MemcachedError:
            self.log.info ("OOM Reached NRU Started")
            time.sleep(15)

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
        self.assertEqual(set([]), set(keys_that_were_accessed).intersection(deleted_keys))

        # one more iteration
        deleted_keys = []
        keys_that_were_accessed = []
        set(keys_that_were_accessed).intersection(deleted_keys)

        for i in range(200):
            key = random.randint(0, 12000)
            try:
                mc_client.get(EphemeralBucketsOOM.KEY_ROOT + str(key))
                keys_that_were_accessed.append(key)
            except mc_bin_client.MemcachedError: 
                self.log.info('key %s already deleted' % key)
        # add ~15% of new items
        try:
            for i in range(int(item_count * 1.2), int(item_count * 1.4)):
                mc_client.set(EphemeralBucketsOOM.KEY_ROOT + str(i), 0, 0, 'a' * self.value_size)
                keys_that_were_accessed.append(i)
        except mc_bin_client.MemcachedError:
            self.log.info ("OOM Reached NRU Started")
            time.sleep(15)

        item_count = rest.get_bucket(self.buckets[0]).stats.itemCount
        self.log.info('The number of items is {0}'.format(item_count))

        for v in range(self.vbuckets):
            vb_uuid, seqno, high_seqno = self.vb_info(self.master, v)
            pre_delete_sequence_numbers[v] = high_seqno

        dcp_client = self.dcp_client(self.servers[0], 'producer')
        num = 0
        for vb in vbuckets[0:self.vbuckets]:
            vbucket = vb.id
            vb_uuid, _, high_seqno = self.vb_info(self.servers[0], vbucket, bucket=self.buckets[0])
            print(vb.id, 'streaming from', pre_delete_sequence_numbers[vb.id], ' to ', high_seqno)
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
        self.assertEqual(set([]), set(keys_that_were_accessed).intersection(deleted_keys))

class EvictionDCP(EvictionBase, DCPBase):

    def setUp(self):
        super(EvictionDCP, self).setUp()
        self.dcp_client = DcpClient(self.master.ip, int(11210))
        self.dcp_client.sasl_auth_plain(self.master.rest_username, self.master.rest_password)
        self.dcp_client.bucket_select('default')
        self.dcp_client.open_producer(name='eviction', delete_times=True)

        self.rest = RestConnection(self.servers[0])
        self.client = MemcachedClientHelper.direct_client(self.master, 'default')

    def tearDown(self):
        super(EvictionDCP, self).tearDown()

    def test_stream_eviction(self):
        # eviction.evictionkv.EvictionDCP.test_stream_eviction,dgm_run=True,eviction_policy=fullEviction

        vbuckets = self.rest.get_vbuckets()

        doc_gen = BlobGenerator('dcpdata', 'dcpdata-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, doc_gen, "create", 10)
        # sleep for 10 seconds
        time.sleep(10)
        # get the item count
        item_count = self.rest.get_bucket(self.buckets[0]).stats.itemCount
        self.assertEqual(item_count, self.num_items)

        expired_keys = []
        # check if all the keys expired
        keys=[]
        for i in range(1000):
            keys.append("dcpdata" + str(i))
        time.sleep(10)
        for key in keys:
            try:
                self.client.get(key=key)
                msg = "expiry was set to {0} but key: {1} did not expire after waiting for {2}+ seconds"
                self.log.info(msg.format(10, key, 10))
            except mc_bin_client.MemcachedError as error:
                self.assertEqual(error.status, 1)

        for vb in vbuckets[0:self.vbuckets]:
            vbucket = vb.id
            vb_uuid, _, high_seqno = self.vb_info(self.servers[0], vbucket, bucket=self.buckets[0])
            stream = self.dcp_client.stream_req(vbucket, 0, 0, high_seqno, vb_uuid)
            self.dcp_client.general_control("enable_expiry_opcode", "true")

            responses = stream.run()
            for i in responses:
                if i['opcode'] == constants.CMD_EXPIRATION:
                    expired_keys.append(i['key'])


        item_count = self.rest.get_bucket(self.buckets[0]).stats.itemCount
        self.assertEqual(item_count, 0)
        check_values = set(keys).intersection(expired_keys) # check if any key is not expired
        self.assertEqual(len(check_values), self.num_items)

    def test_stream_deletioneviction(self):
        # eviction.evictionkv.EvictionDCP.test_stream_deletioneviction,dgm_run=True,eviction_policy=fullEviction
        # delete some keys and expire other keys
        vbuckets = self.rest.get_vbuckets()
        KEY_ROOT = 'dcpdata'

        doc_gen = BlobGenerator(KEY_ROOT, 'dcpdata-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, doc_gen, "create", 40)
        time.sleep(10)

        # get the item count
        item_count = self.rest.get_bucket(self.buckets[0]).stats.itemCount
        self.assertEqual(item_count, self.num_items)

        keys=[]
        for i in range(1000):
            keys.append("dcpdata" + str(i))

        # delete few keys
        keys_to_be_deleted=[]
        for i in range(100):
            int = random.randint(0, 1000)
            key = KEY_ROOT + str(int)
            try:
                self.client.delete(key)
                keys_to_be_deleted.append(key)
                keys.remove(key)
            except mc_bin_client.MemcachedError as error:
                self.assertEqual(error.status, 1) # if key is already deleted then ignore the error

        expired_keys = []
        deleted_keys = []
        time.sleep(40)

        # check if other the keys expired
        for key in keys:
            try:
                self.client.get(key=key)
                msg = "expiry was set to {0} but key: {1} did not expire after waiting for {2}+ seconds"
                self.log.info(msg.format(40, key, 40))
            except mc_bin_client.MemcachedError as error:
                self.assertEqual(error.status, 1)

        for vb in vbuckets[0:self.vbuckets]:
            vbucket = vb.id
            vb_uuid, _, high_seqno = self.vb_info(self.servers[0], vbucket, bucket=self.buckets[0])
            stream = self.dcp_client.stream_req(vbucket, 0, 0, high_seqno, vb_uuid)
            self.dcp_client.general_control("enable_expiry_opcode", "true")

            responses = stream.run()
            for i in responses:
                if i['opcode'] == constants.CMD_EXPIRATION:
                    expired_keys.append(i['key'])
                elif i['opcode'] == constants.CMD_DELETION:
                    deleted_keys.append(i['key'])


        item_count = self.rest.get_bucket(self.buckets[0]).stats.itemCount
        self.assertEqual(item_count, 0)
        self.assertEqual(len(keys_to_be_deleted), len(deleted_keys))
        self.assertEqual(len(keys), len(expired_keys))


