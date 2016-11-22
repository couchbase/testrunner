import time
import logger
from dcp.constants import *
from dcpbase import DCPBase
from membase.api.rest_client import RestConnection, RestHelper
from couchbase_helper.documentgenerator import BlobGenerator
from remote.remote_util import RemoteMachineShellConnection
from lib.cluster_run_manager  import CRManager

from memcached.helper.data_helper import VBucketAwareMemcached, MemcachedClientHelper
from memcacheConstants import *
import zlib

log = logger.Logger.get_logger()

class DCPRollBack(DCPBase):



    # MB-21568 sequence number is incorrect during a race between persistence and failover
    def test_rollback_and_persistence_race_condition(self):

        nodeA = self.servers[0]
        vbucket_client = VBucketAwareMemcached(RestConnection(self.master), 'default')
        gen_create = BlobGenerator('dcp', 'dcp-', 64, start=0, end=self.num_items)


        #def _load_all_buckets(self, server, kv_gen, op_type, exp, kv_store=1, flag=0,
        #                  only_store_hash=True, batch_size=1000, pause_secs=1, timeout_secs=30,
        #                  proxy_client=None):
        self._load_all_buckets(nodeA, gen_create, "create", 0)

        # stop persistence
        for bucket in self.buckets:
            for s in self.servers:
                client = MemcachedClientHelper.direct_client(s, bucket)
                client.stop_persistence()

        vb_uuid, seqno, high_seqno = self.vb_info(self.servers[0], 5)

        #dcp_client = self.dcp_client(self.master, FLAG_OPEN_PRODUCER, 5, name='testDCPconn')
        """
            def stream_req(self, vbucket, takeover, start_seqno, end_seqno,
                       vb_uuid, snap_start = None, snap_end = None):
        """
        #stream = dcp_client.stream_req(  5, 0, 2*high_seqno, 3*high_seqno, vb_uuid)

        time.sleep(10)

        # more (non-intersecting) load
        gen_create = BlobGenerator('dcp-secondgroup', 'dcpsecondgroup-', 64, start=0, end=self.num_items)
        self._load_all_buckets(nodeA, gen_create, "create", 0)


        """

        # find which keys are active on the first node.
        active_on_first_node = []
        for i in range(self.num_items):
            # map the key to the vbucket to the node
            vbId = (((zlib.crc32('dcp-secondgroup' + str(i))) >> 16) & 0x7fff) & (self.vbuckets- 1)
            #print 'key', 'dcp-secondgroup' + str(i), 'is on vbid', vbId, 'and the server is', vbucket_client.vBucketMap[vbId].split(':')[0]
            if vbucket_client.vBucketMap[vbId].split(':')[0] == self.servers[0].ip:
                #print 'adding key ', 'dcp-secondgroup' + str(i)
                active_on_first_node.append('dcp-secondgroup' + str(i) )
        """



        #import pdb;pdb.set_trace()
        shell = RemoteMachineShellConnection(self.servers[0])
        shell.kill_memcached()
        #shell.execute_command("kill -9 $(pgrep -l -f memcached|cut -d ' ' -f 1)")


        # start persistence on the second node

        client = MemcachedClientHelper.direct_client(self.servers[1], bucket)
        client.start_persistence()

        # the second node should have the same number of items at this point
        # some kind of check can go here for curr_items_tot


        # failover ...
        #tasks = self.cluster.failover(self.servers, self.servers[0:1], graceful=True)
        #for t in tasks:
            #result = t.result()


        #   def rebalance(self, servers, to_add, to_remove, timeout=None, use_hostnames=False, services = None):


        #self.cluster.rebalance(self.servers, [], self.servers[0:1])


        # and then check on the newly active, check a key that was previously a replica and should have been rolled
        # back ie. should not exist but in the presence of this bug it will exist

        #vbucket_client = VBucketAwareMemcached(RestConnection(self.servers[1]), 'default')


        """
        client = MemcachedClientHelper.direct_client(self.servers[1], 'default')

        for i in active_on_first_node:
            rc = client.get( i )
            print 'the rc from the get is', rc
        print 'done'
        """

