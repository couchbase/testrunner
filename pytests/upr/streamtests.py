from upr.constants import *
from upr.uprbase import UPRBase


class UPRStreamTests(UPRBase):

    def test_stream_request(self):

        # selective load into vbucket 0
        vbucket = 0
        self.load_docs(self.master, vbucket, self.num_items)

        # stream known mutations
        start_seqno = 0
        vb_uuid, _, end_seqno = self.vb_info(self.master, vbucket)

        upr_client = self.upr_client(self.master, PRODUCER)
        stream = upr_client.stream_req(vbucket, 0, start_seqno,
                                         self.num_items, 0)
        stream.status == SUCCESS
        stream.run()
        assert stream.last_by_seqno == self.num_items
        upr_client.close()


    def test_stream_request_flow_control(self):

        vbucket = 0
        self.load_docs(self.master, vbucket, self.num_items)

        # stream known mutations
        start_seqno = 0
        vb_uuid, _, end_seqno = self.vb_info(self.master, vbucket)

        upr_client = self.upr_client(self.master, PRODUCER)
        upr_client.flow_control(128)
        stream = upr_client.stream_req(vbucket, 0, start_seqno,
                                       self.num_items, end_seqno)
        stream.status == SUCCESS

        while stream.has_response():
            msg = stream.next_response()
            _, _, unacked = self.flow_control_info(self.master)
            if unacked > 0:
                upr_client.ack(unacked)
            if 'by_seqno' in msg:
                last_mutation = msg['by_seqno']

        assert stream.last_by_seqno == self.num_items
        upr_client.close()
