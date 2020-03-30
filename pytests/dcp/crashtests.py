import time
import logger
from dcp.constants import *
from .dcpbase import DCPBase
from membase.api.rest_client import RestConnection, RestHelper
from couchbase_helper.documentgenerator import BlobGenerator
from remote.remote_util import RemoteMachineShellConnection
from lib.cluster_run_manager  import CRManager

log = logger.Logger.get_logger()


class DCPCrashTests(DCPBase):

    def test_stream_after_n_crashes(self):

        crashes = 5
        vbucket = 0

        # load some data
        nodeA = self.servers[0]
        rest = RestHelper(RestConnection(nodeA))
        for i in range(crashes):
            self.load_docs(nodeA, vbucket, self.num_items)
            assert self.stop_node(0)
            time.sleep(5)
            assert self.start_node(0)
            assert rest.is_ns_server_running()
            time.sleep(2)

            vb_uuid, _, high_seqno = self.vb_info(nodeA, vbucket)
            dcp_client = self.dcp_client(nodeA, PRODUCER)
            stream = dcp_client.stream_req(
                vbucket, 0, 0,
                high_seqno, vb_uuid)
            stream.run()

            assert stream.last_by_seqno == high_seqno

    def test_crash_while_streaming(self):

        vbucket = 0
        nodeA = self.servers[0]
        n = 10000
        self.load_docs(nodeA, vbucket, n)

        dcp_client = self.dcp_client(nodeA, PRODUCER)
        stream = dcp_client.stream_req(vbucket, 0, 0, 2*n, 0)
        self.load_docs(nodeA, vbucket, n)
        assert self.stop_node(0)
        time.sleep(2)
        assert self.start_node(0)
        rest = RestHelper(RestConnection(nodeA))
        assert rest.is_ns_server_running()
        time.sleep(30)

        _, _, high_seqno = self.vb_info(nodeA, vbucket)
        dcp_client = self.dcp_client(nodeA, PRODUCER)
        stream = dcp_client.stream_req(vbucket, 0, 0, high_seqno, 0)
        stream.run()
        assert stream.last_by_seqno == high_seqno

    def test_crash_entire_cluster(self):

        self.cluster.rebalance(
            [self.master],
            self.servers[1:], [])

        vbucket = 0
        nodeA = self.servers[0]
        n = 10000
        self.load_docs(nodeA, vbucket, n)

        dcp_client = self.dcp_client(nodeA, PRODUCER)
        stream = dcp_client.stream_req(vbucket, 0, 0, 2*n, 0)
        self.load_docs(nodeA, vbucket, n)

        # stop all nodes
        node_range = list(range(len(self.servers)))
        for i in node_range:
            assert self.stop_node(i)
        time.sleep(2)

        # start all nodes in reverse order
        node_range.reverse()
        for i in node_range:
            assert self.start_node(i)

        rest = RestHelper(RestConnection(nodeA))
        assert rest.is_ns_server_running()

        _, _, high_seqno = self.vb_info(nodeA, vbucket)
        dcp_client = self.dcp_client(nodeA, PRODUCER)
        stream = dcp_client.stream_req(vbucket, 0, 0, high_seqno, 0)
        stream.run()
        assert stream.last_by_seqno == high_seqno
