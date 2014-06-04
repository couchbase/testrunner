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
        response = upr_client.stream_request(vbucket, start_seqno,
                                             self.num_items, end_seqno)
        assert response['status'] == SUCCESS,\
                        "{0}:{1}".format(EINV_RESPONSE, response)
        stream = upr_client.get_stream(vbucket)

        last_mutation = 0
        while stream.has_message():
            msg = stream.next_message()
            if 'by_seqno' in msg:
                last_mutation = msg['by_seqno']

        assert last_mutation == self.num_items


    def test_stream_request_flow_control(self):

        vbucket = 0
        self.load_docs(self.master, vbucket, self.num_items)

        # stream known mutations
        start_seqno = 0
        vb_uuid, _, end_seqno = self.vb_info(self.master, vbucket)

        upr_client = self.upr_client(self.master, PRODUCER)
        upr_client.flow_control(128)
        response = upr_client.stream_request(vbucket, start_seqno,
                                             self.num_items, end_seqno)
        assert response['status'] == SUCCESS,\
                        "{0}:{1}".format(EINV_RESPONSE, response)
        stream = upr_client.get_stream(vbucket)

        last_mutation = 0
        while stream.has_message():
            msg = stream.next_message()
            _, _, unacked = self.flow_control_info(self.master)
            if unacked > 0:
                upr_client.ack(unacked)
            if 'by_seqno' in msg:
                last_mutation = msg['by_seqno']

        assert last_mutation == self.num_items
