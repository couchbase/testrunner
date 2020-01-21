import time
import logger
from dcp.constants import VBSEQNO_STAT, PRODUCER
from .dcpbase import DCPBase
from membase.api.rest_client import RestConnection, RestHelper
from mc_bin_client import MemcachedClient, MemcachedError
from couchbase_helper.documentgenerator import BlobGenerator

log = logger.Logger.get_logger()


class DCPRebalanceTests(DCPBase):

    def test_mutations_during_rebalance(self):

        # start rebalance
        task = self.cluster.async_rebalance(
            [self.master],
            self.servers[1:], [])

        # load some data
        vbucket = 0
        self.load_docs(self.master, vbucket, self.num_items)
        vb_uuid, seqno, high_seqno = self.vb_info(self.master,
                                                  vbucket)
        # stream
        log.info("streaming vb {0} to seqno {1}".format(
            vbucket, high_seqno))
        self.assertEqual(high_seqno, self.num_items)

        dcp_client = self.dcp_client(self.master, PRODUCER, vbucket)
        stream = dcp_client.stream_req(
            vbucket, 0, 0,
            high_seqno, vb_uuid)

        stream.run()
        last_seqno = stream.last_by_seqno
        assert last_seqno == high_seqno, last_seqno

        # verify rebalance
        assert task.result()

    def test_failover_swap_rebalance(self):
        """ add and failover node then perform swap rebalance """

        assert len(self.servers) > 2, "not enough servers"
        nodeA = self.servers[0]
        nodeB = self.servers[1]
        nodeC = self.servers[2]

        gen_create = BlobGenerator('dcp', 'dcp-', 64, start=0, end=self.num_items)
        self._load_all_buckets(nodeA, gen_create, "create", 0)

        vbucket = 0
        vb_uuid, seqno, high_seqno = self.vb_info(nodeA, vbucket)

        # rebalance in nodeB
        assert self.cluster.rebalance([nodeA], [nodeB], [])

        # add nodeC
        rest = RestConnection(nodeB)
        rest.add_node(user=nodeC.rest_username,
                      password=nodeC.rest_password,
                      remoteIp=nodeC.ip,
                      port=nodeC.port)

        # stop and failover nodeA
        assert self.stop_node(0)
        self.stopped_nodes.append(0)
        self.master = nodeB

        assert self.cluster.failover([nodeB], [nodeA])
        try:
            assert self.cluster.rebalance([nodeB], [], [])
        except:
            pass
        self.add_built_in_server_user()
        # verify seqnos and stream mutations
        rest = RestConnection(nodeB)
        vbuckets = rest.get_vbuckets()
        total_mutations = 0

        for vb in vbuckets:
            mcd_client = self.mcd_client(nodeB, auth_user=True)
            stats = mcd_client.stats(VBSEQNO_STAT)
            vbucket = vb.id
            key = 'vb_{0}:high_seqno'.format(vbucket)
            total_mutations += int(stats[key])

        assert total_mutations == self.num_items #/ 2   # divide by because the items are split between 2 servers
        task = self.cluster.async_rebalance([nodeB], [], [nodeC])
        task.result()

    def test_stream_req_during_failover(self):
        """stream_req mutations before and after failover from state-changing vbucket"""

        # start rebalance
        try:
            self.cluster.rebalance([self.master], self.servers[1:], [])
        except:
            pass

        vbucket = 0
        mcd_client = self.mcd_client(self.master, vbucket, auth_user=True)
        mcd_client.set('key1', 0, 0, 'value', vbucket)

        # failover node where key was set
        rest = RestConnection(self.master)
        index = self.vbucket_host_index(rest, vbucket)
        fail_n = self.servers[index]
        ready_n = [n for n in self.servers if n.ip != fail_n.ip or n.port != fail_n.port]

        assert self.stop_node(index)
        self.stopped_nodes.append(index)
        assert self.cluster.failover(ready_n, [fail_n])
        rebalance_t = self.cluster.async_rebalance(ready_n, [], [])

        # vbucket has moved, set another key in new location
        rest = RestConnection(ready_n[0])
        index = self.vbucket_host_index(rest, vbucket)
        new_master = ready_n[0]
        mcd_client = self.mcd_client(new_master, auth_user=True)
        try:
            mcd_client.set('key2', 0, 0, 'value', vbucket)
        except MemcachedError:
            self.sleep(10)
            mcd_client = self.mcd_client(new_master, auth_user=True)
            mcd_client.set('key2', 0, 0, 'value', vbucket)

        # stream mutation
        dcp_client = self.dcp_client(new_master, PRODUCER, vbucket)
        stream = dcp_client.stream_req(vbucket, 0, 0, 2, 0)

        while stream.has_response():

            response = stream.next_response()

            assert response is not None,\
                 "Timeout reading stream after failover"

            if 'key' in response:
                if response['by_seqno'] == 1:
                    assert response['key'] == 'key1'
                elif response['by_seqno'] == 2:
                    assert response['key'] == 'key2'
                else:
                    assert False, "received unexpected mutation"
            if response['opcode'] == 0x55: # end
                break

        assert stream.last_by_seqno == 2
        assert rebalance_t.result()
        self.cluster.rebalance([new_master], [], ready_n[1:])

    def test_failover_log_table_updated(self):
        """Verifies failover table entries are updated when vbucket ownership changes"""

        # rebalance in nodeB
        nodeA = self.servers[0]
        nodeB = self.servers[1]

        # load nodeA only
        rest = RestConnection(nodeA)
        vbuckets = rest.get_vbuckets()
        for vb_info in vbuckets[0:4]:
            vbucket = vb_info.id
            self.load_docs(nodeA, vbucket, self.num_items)

        # get original failover table
        mcd_client = self.mcd_client(nodeA, auth_user=True)
        orig_table = mcd_client.stats('failovers')

        # add nodeB
        self.cluster.rebalance([nodeA], [nodeB], [])

        # stop nodeA and failover
        assert self.stop_node(0)
        self.stopped_nodes.append(0)
        self.master = nodeB
        assert self.cluster.failover([nodeB], [nodeA])
        assert self.cluster.rebalance([nodeB], [], [])

        # load nodeB only
        rest = RestConnection(nodeB)
        vbuckets = rest.get_vbuckets()
        for vb_info in vbuckets[0:4]:
            vbucket = vb_info.id
            self.load_docs(nodeB, vbucket, self.num_items)

        # add nodeA back
        assert self.start_node(0)
        del self.stopped_nodes[0]
        rest = RestHelper(RestConnection(nodeA))
        assert rest.is_ns_server_running()
        time.sleep(10)
        self.cluster.rebalance([nodeB], [nodeA], [])

        # stop nodeB and failover
        assert self.stop_node(1)
        self.master = nodeA
        self.stopped_nodes.append(1)
        assert self.cluster.failover([nodeA], [nodeB])
        assert self.cluster.rebalance([nodeA], [], [])

        # load nodeA only
        rest = RestConnection(nodeA)
        vbuckets = rest.get_vbuckets()
        for vb_info in vbuckets[0:4]:
            vbucket = vb_info.id
            self.load_docs(nodeA, vbucket, self.num_items)

        # check failover table entries
        mcd_client = self.mcd_client(nodeA, auth_user=True)
        stats = mcd_client.stats('failovers')
        for vb_info in vbuckets[0:4]:
            vb = vb_info.id
            assert int(stats['vb_'+str(vb)+':num_entries']) == 2

            vb_uuid, _, _= self.vb_info(nodeA,
                                                      vb)
            dcp_client = self.dcp_client(nodeA, PRODUCER)
            stream = dcp_client.stream_req(
                vb, 0, 0,
                self.num_items*3, vb_uuid)

            mutations = stream.run()
            assert stream.last_by_seqno == self.num_items*3, stream.last_by_seqno