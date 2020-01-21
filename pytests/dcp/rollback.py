import time
import logger
from dcp.constants import *
from mc_bin_client import MemcachedError
from .dcpbase import DCPBase
from membase.api.rest_client import RestConnection, RestHelper
from couchbase_helper.documentgenerator import BlobGenerator
from remote.remote_util import RemoteMachineShellConnection

from memcached.helper.data_helper import VBucketAwareMemcached, MemcachedClientHelper
from memcacheConstants import *
import zlib
from couchbase_helper.documentgenerator import DocumentGenerator

log = logger.Logger.get_logger()


class DCPRollBack(DCPBase):

    """
    # MB-21587 rollback recover is incorrect

    The scenario is as follows:
     1. 2 node cluster
     2. populate some kvs
     3. turn off persistence on both nodes
     4. modify less than 1/2 of the keys
     5. kill memcache on node 1. When node 2 reconnects to node 1 it will ask for sequence numbers higher than
       those seen node 1 so node 1 will ask it to rollback
     6. failover to node 2
     7. Verify the keys that were modified and were active on node 1 have the values as set prior to step 4

    """

    def replicate_correct_data_after_rollback(self):
        '''
        @attention: This test case has some issue with docker runs. It
        passes without any issue on VMs.
        '''

        NUMBER_OF_DOCS = 10000


        # populate the kvs, they will look like ...
        """
        key: keyname-x
        value:
          {
          "mutated": 0,
            "_id": "keyname-x",
             "val-field-name": "serial-vals-100"
            }
        """
        vals = ['serial-vals-' + str(i) for i in range(NUMBER_OF_DOCS)]
        template = '{{ "val-field-name": "{0}"  }}'
        gen_load = DocumentGenerator('keyname', template, vals, start=0,
                                     end=NUMBER_OF_DOCS)

        rc = self.cluster.load_gen_docs(self.servers[0], self.buckets[0].name, gen_load,
                                   self.buckets[0].kvs[1], "create", exp=0, flag=0, batch_size=1000,
                                        compression=self.sdk_compression)

        # store the KVs which were modified and active on node 1
        modified_kvs_active_on_node1 = {}
        vbucket_client = VBucketAwareMemcached(RestConnection(self.master), 'default')
        client = MemcachedClientHelper.direct_client(self.servers[0], 'default')
        for i in range(NUMBER_OF_DOCS//100):
            keyname = 'keyname-' + str(i)
            vbId = ((zlib.crc32(keyname) >> 16) & 0x7fff) & (self.vbuckets- 1)
            if vbucket_client.vBucketMap[vbId].split(':')[0] == self.servers[0].ip:
                rc = client.get( keyname )
                modified_kvs_active_on_node1[ keyname ] = rc[2]

        # stop persistence
        for bucket in self.buckets:
            for s in self.servers[:self.nodes_init]:
                client = MemcachedClientHelper.direct_client(s, bucket)
                try:
                    client.stop_persistence()
                except MemcachedError as e:
                    if self.bucket_type == 'ephemeral':
                        self.assertTrue(
                            "Memcached error #4 'Invalid':  Flusher not running. for vbucket :0 to mc " in str(e))
                        return
                    else:
                        raise

        # modify less than 1/2 of the keys
        vals = ['modified-serial-vals-' + str(i) for i in range(NUMBER_OF_DOCS//100)]
        template = '{{ "val-field-name": "{0}"  }}'
        gen_load = DocumentGenerator('keyname', template, vals, start=0,
                                     end=NUMBER_OF_DOCS//100)
        rc = self.cluster.load_gen_docs(self.servers[0], self.buckets[0].name, gen_load,
                                   self.buckets[0].kvs[1], "create", exp=0, flag=0, batch_size=1000,
                                        compression=self.sdk_compression)

        # kill memcached, when it comes back because persistence is disabled it will have lost the second set of mutations
        shell = RemoteMachineShellConnection(self.servers[0])
        shell.kill_memcached()
        time.sleep(10)

        # start persistence on the second node
        client = MemcachedClientHelper.direct_client(self.servers[1], 'default')
        client.start_persistence()

        time.sleep(5)

        # failover to the second node
        rc = self.cluster.failover(self.servers, self.servers[1:2], graceful=True)
        time.sleep(30)     # give time for the failover to complete

        # check the values, they should be what they were prior to the second update
        client = MemcachedClientHelper.direct_client(self.servers[0], 'default')
        for k, v  in modified_kvs_active_on_node1.items():
            rc = client.get( k )
            self.assertTrue( v == rc[2], 'Expected {0}, actual {1}'.format(v, rc[2]))

        # need to rebalance the node back into the cluster
        # def rebalance(self, servers, to_add, to_remove, timeout=None, use_hostnames=False, services = None):

        rest_obj = RestConnection(self.servers[0])
        nodes_all = rest_obj.node_statuses()
        for node in nodes_all:
            if node.ip == self.servers[1].ip:
                break

        node_id_for_recovery = node.id
        status = rest_obj.add_back_node(node_id_for_recovery)
        if status:
            rest_obj.set_recovery_type(node_id_for_recovery,
                                       recoveryType='delta')
        rc = self.cluster.rebalance(self.servers[:self.nodes_init], [], [])

    """
    # MB-21568 sequence number is incorrect during a race between persistence and failover.

    The scenario is as follows:
    1. Two node cluster
    2. Set 1000 KVs
    3. Stop persistence
    4. Set another 1000 non-intersecting KVs
    5. Kill memcached on node 1
    6. Verify that the number of items on both nodes is the same

    """
    def test_rollback_and_persistence_race_condition(self):

        nodeA = self.servers[0]
        vbucket_client = VBucketAwareMemcached(RestConnection(self.master), 'default')
        gen_create = BlobGenerator('dcp', 'dcp-', 64, start=0, end=self.num_items)
        self._load_all_buckets(nodeA, gen_create, "create", 0)

        # stop persistence
        for bucket in self.buckets:
            for s in self.servers[:self.nodes_init]:
                client = MemcachedClientHelper.direct_client(s, bucket)
                try:
                    client.stop_persistence()
                except MemcachedError as e:
                    if self.bucket_type == 'ephemeral':
                        self.assertTrue("Memcached error #4 'Invalid':  Flusher not running. for vbucket :0 to mc " in str(e))
                        return
                    else:
                        raise

        vb_uuid, seqno, high_seqno = self.vb_info(self.servers[0], 5)

        time.sleep(10)

        # more (non-intersecting) load
        gen_create = BlobGenerator('dcp-secondgroup', 'dcpsecondgroup-', 64, start=0, end=self.num_items)
        self._load_all_buckets(nodeA, gen_create, "create", 0)

        shell = RemoteMachineShellConnection(self.servers[0])
        shell.kill_memcached()

        time.sleep(10)

        mc1 = MemcachedClientHelper.direct_client(self.servers[0], "default")
        mc2 = MemcachedClientHelper.direct_client(self.servers[1], "default")

        node1_items = mc1.stats()["curr_items_tot"]
        node2_items = mc2.stats()["curr_items_tot"]

        self.assertTrue(node1_items == node2_items,
                        'Node items not equal. Node 1:{0}, node 2:{1}'.format(node1_items, node2_items))
