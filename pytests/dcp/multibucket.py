import time
import logger
from dcp.constants import *
from .dcpbase import DCPBase
from membase.api.rest_client import RestConnection, RestHelper
from couchbase_helper.documentgenerator import BlobGenerator

log = logger.Logger.get_logger()

class DCPMultiBucket(DCPBase):

    def test_stream_all_buckets(self):
        doc_gen = BlobGenerator(
            'dcpdata', 'dcpdata-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, doc_gen, "create", 0)

        user_name = self.input.param("user_name", None)
        password = self.input.param("password", None)
        nodeA = self.servers[0]
        rest = RestConnection(nodeA)
        vbuckets = rest.get_vbuckets()

        buckets = ['default']
        for i in range(self.standard_buckets):
            buckets.append('standard_bucket'+str(i))

        for bucket in buckets:
            if user_name is not None:
                self.add_built_in_server_user([{'id': user_name, 'name': user_name, 'password': password}], \
                                              [{'id': user_name, 'name': user_name, 'roles': 'data_dcp_reader[default]'}], self.master)
                dcp_client = self.dcp_client(nodeA, PRODUCER, bucket_name=bucket, auth_user=user_name, auth_password=password)
            else:
                dcp_client = self.dcp_client(nodeA, PRODUCER, bucket_name=bucket)

            for vb in vbuckets[0:16]:
                vbucket = vb.id
                vb_uuid, _, high_seqno = self.vb_info(nodeA, vbucket, bucket = bucket)
                stream = dcp_client.stream_req(vbucket, 0, 0, high_seqno, vb_uuid)
                responses = stream.run()
                assert high_seqno == stream.last_by_seqno

    def test_stream_after_warmup(self):

        nodeA = self.servers[0]
        bucket = 'standard_bucket'+str(self.standard_buckets-1)
        originalVbInfo = self.all_vb_info(nodeA, bucket = bucket)
        expectedVbSeqno = {}

        # load all buckets
        doc_gen = BlobGenerator(
            'dcpdata', 'dcpdata-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, doc_gen, "create", 0)
        self._wait_for_stats_all_buckets([nodeA])

        # store expected vb seqnos
        originalVbInfo = self.all_vb_info(nodeA, bucket = bucket)


        # restart node
        assert self.stop_node(0)
        time.sleep(5)
        assert self.start_node(0)
        rest = RestHelper(RestConnection(nodeA))
        assert  rest.is_ns_server_running()
        time.sleep(2)

        # verify original vbInfo can be streamed
        dcp_client = self.dcp_client(nodeA, PRODUCER, bucket_name=bucket)
        for vbucket in originalVbInfo:
            vb_uuid, _, high_seqno = originalVbInfo[vbucket]
            stream = dcp_client.stream_req(vbucket, 0, 0, high_seqno, vb_uuid)
            responses = stream.run()
            assert high_seqno == stream.last_by_seqno

