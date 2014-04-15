import random
from mc_bin_client import MemcachedClient
from memcacheConstants import *
import threading
import Queue
import time


class UprClient(MemcachedClient):
    """ UprClient implements upr protocol using mc_bin_client as base
        for sending and receiving commands """

    def __init__(self, host='127.0.0.1', port=11210, timeout=5):
        super(UprClient, self).__init__(host, port, timeout)

        # recv timeout
        self.timeout = timeout

        # map of uprstreams.  key = vbucket, val = UprStream
        self.streams = {}

        # inflight ops.  key = opaque, val = Operation
        self.ops = {}

        # start reader thread
        self.conn = UprClient.Reader(self)
        self.conn.start()

    def _restart_reader(self):
        """ restart reader thread """
        self.conn = UprClient.Reader(self)
        self.conn.start()


    def open_consumer(self, name):
        """ opens an upr consumer connection """

        op = OpenConsumer(name)
        return self._handle_op(op)

    def open_producer(self, name):
        """ opens an upr producer connection """

        op = OpenProducer(name)
        return self._handle_op(op)

    def open_notifier(self, name):
        """ opens an upr notifier connection """

        op = OpenNotifier(name)
        return self._handle_op(op)

    def add_stream(self, vbucket, takeover = 0):
        """ sent to upr-consumer to add stream on a particular vbucket.
            the takeover flag is there for completeness and is used by
            ns_server during vbucket move """

        op = AddStream(vbucket, takeover)
        return self._handle_op(op)

    def stream_request(self, vbucket, start_seqno, end_seqno,
                       vb_uuid, high_seqno, takeover = 0):
        """" sent to upr-producer to stream mutations from
             a particular vbucket.

             upon sucessful stream request an UprStream object is created
             that can be used by the client to receive mutations.

             start_seqno = seqno to begin streaming
             end_seqno = seqno to specify end of stream
             vb_uuid = vbucket uuid as specified in failoverlog
             high_seqno = high seqno at time vbucket was generated"""

        op = StreamRequest(vbucket, start_seqno, end_seqno,
                           vb_uuid, high_seqno, takeover)

        response = self._handle_op(op)

        if response['status'] == 0:

            # upon successful stream request create stream object
            stream = UprStream(op)
            self.streams[vbucket] = stream

        return response

    def get_stream(self, vbucket):
        """ for use by external clients to get stream
            associated with a particular vbucket """
        return self.streams.get(vbucket)

    def _handle_op(self, op):
        """ sends op to mcd. Then it recvs response

            if the received response is for another op then it will
            attempt to get the next response and retry 5 times."""

        self.send_op(op)
        self.ops[op.opaque] = op

        if not self.conn.isAlive():
            self._restart_reader()

        try:
            return op.queue.get(timeout=30)
        except Queue.Empty:
            assert False,\
                "ERROR: Never received response for op: %s" % op.opcode


    def send_op(self, op):
        """ sends op details to mcd client for lowlevel packet assembly """

        self.vbucketId = op.vbucket
        self._sendCmd(op.opcode,
                      op.key,
                      op.value,
                      op.opaque,
                      op.extras)


    class Reader(threading.Thread):
        """ Reader class receievs upr response and pairs op with opaque.
            the op is used to created a formated response
            which is then put on the op queue to be consumed
            by the caller """

        def __init__(self, client):
            threading.Thread.__init__(self)
            self.client = client

        def run(self):

            while True:
                try:
                    opcode, status, opaque, cas, keylen, extlen, body =\
                         self.client._recvMsg()

                    op = self.client.ops.get(opaque)
                    if op:
                        response = op.formated_response(opcode, keylen,
                                                        extlen, status,
                                                        cas, body, opaque)
                        op.queue.put(response)

                    elif opcode == CMD_STREAM_REQ:
                        # stream_req ops received during add_stream request
                        self.ack_stream_req(opaque)

                except EOFError: # reached end of socket
                    break

    def ack_stream_req(self, opaque):
        body   = struct.pack("<QQ", 123456, 0)
        header = struct.pack(REQ_PKT_FMT,
                             RES_MAGIC_BYTE,
                             CMD_STREAM_REQ,
                             0, 0, 0, 0,
                             len(body), opaque, 0)
        self.client.s.send(header + body)

class UprStream(object):
    """ UprStream class manages the responses received from producers
       during active stream requests """

    def __init__(self, op):
        self.op = op
        self.vbucket = op.vbucket
        self.last_by_seqno = 0
        self._ended = False

    @property
    def ended(self):
        return self._ended

    def has_message(self):
        """ true if END has not been received or timeout waiting for items in recv q """
        return not self._ended

    def next_message(self):
        """ Fetch next message from op queue. verify that mutations
            are recieved in expected order.
            Deactivate if end_stream command received """

        msg = None

        if self.has_message():

            try:
                msg = self.op.queue.get(timeout=15)
            except Queue.Empty:
                err_msg = "Timeout receiving msg from stream. last_by_seqno = %s, end_seqno = %s" %\
                                (self.last_by_seqno, self.op.end_seqno)
                self._ended = True
                raise Exception(err_msg)

            # mutation response verification
            if msg['opcode'] == CMD_MUTATION:
                assert 'by_seqno' in msg,\
                     "ERROR: vbucket(%s) received mutation without seqno: %s"\
                         % (self.vbucket, msg)
                assert msg['by_seqno'] > self.last_by_seqno,\
                     "ERROR: Out of order response on vbucket %s: %s"\
                         % (self.vbucket, msg)
                self.last_by_seqno = msg['by_seqno']

            if msg['opcode'] == CMD_STREAM_END:
                self._ended = True

        return msg

class Operation(object):
    """ Operation Class generically represents any upr operation providing
        default values for attributes common to each operation """

    def __init__(self, opcode, key='', value='',
                 extras='', vbucket=0, opaque = None):
        self.opcode = opcode
        self.key = key
        self.value = value
        self.extras = extras
        self.vbucket = vbucket
        self.opaque = opaque or random.Random().randint(0, 2 ** 32)
        self.queue = Queue.Queue()

    def formated_response(self, opcode, keylen, extlen, status, cas, body, opaque):
        return { 'opcode' : opcode,
                 'status' : status,
                 'body'   : body }

class Open(Operation):
    """ Open connection base class """

    def __init__(self, name, flag):
        opcode = CMD_OPEN
        key = name
        extras = struct.pack(">iI", 0, flag)
        Operation.__init__(self, opcode, key, extras = extras)

class OpenConsumer(Open):
    """ Open consumer spec """
    def __init__(self, name):
        Open.__init__(self, name, FLAG_OPEN_CONSUMER)

class OpenProducer(Open):
    """ Open producer spec """
    def __init__(self, name):
        Open.__init__(self, name, FLAG_OPEN_PRODUCER)

class OpenNotifier(Open):
    """ Open notifier spec """
    def __init__(self, name):
        Open.__init__(self, name, FLAG_OPEN_NOTIFIER)


class AddStream(Operation):
    """ AddStream spec """

    def __init__(self, vbucket, takeover = 0):
        opcode = CMD_ADD_STREAM
        extras = struct.pack(">I", takeover)
        Operation.__init__(self, opcode,
                           extras = extras,
                           vbucket = vbucket)

    def formated_response(self, opcode, keylen, extlen, status, cas, body, opaque):
        response = { 'opcode'        : opcode,
                     'status'        : status,
                     'extlen'        : extlen,
                     'value'         : body}
        return response



class StreamRequest(Operation):
    """ StreamRequest spec """

    def __init__(self, vbucket, start_seqno, end_seqno,
                 vb_uuid, high_seqno, takeover = 0):

        opcode = CMD_STREAM_REQ
        extras = struct.pack(">IIQQQQ", takeover, 0,
                             start_seqno,
                             end_seqno,
                             vb_uuid,
                             high_seqno)

        Operation.__init__(self, opcode,
                           extras = extras,
                           vbucket = vbucket)
        self.start_seqno = start_seqno
        self.end_seqno = end_seqno
        self.vb_uuid = vb_uuid
        self.high_seqno = high_seqno
        self.takeover = takeover

    def formated_response(self, opcode, keylen, extlen, status, cas, body, opaque):
        if opcode == CMD_STREAM_REQ:

            response = { 'opcode' : opcode,
                         'status' : status }

            if status == 0:
                assert (len(body) % 16) == 0
                response['failover_log'] = []

                pos = 0
                bodylen = len(body)
                while bodylen > pos:
                    vb_uuid, seqno = struct.unpack(">QQ", body[pos:pos+16])
                    response['failover_log'].append((vb_uuid, seqno))
                    pos += 16
            else:
                response['err_msg'] = body

        elif opcode == CMD_STREAM_END:
            flags = struct.unpack(">I", body[0:4])[0]
            response = { 'opcode'  : opcode,
                         'vbucket' : status,
                         'flags'   : flags }

        elif opcode == CMD_MUTATION:
            by_seqno, rev_seqno, flags, exp, lock_time, ext_meta_len, nru = \
                struct.unpack(">QQIIIHB", body[0:31])
            key = body[31:31+keylen]
            value = body[31+keylen:]
            response = { 'opcode'     : opcode,
                         'vbucket'    : status,
                         'by_seqno'   : by_seqno,
                         'rev_seqno'  : rev_seqno,
                         'flags'      : flags,
                         'expiration' : exp,
                         'lock_time'  : lock_time,
                         'nru'        : nru,
                         'key'        : key,
                         'value'      : value }

        elif opcode == CMD_DELETION:
            by_seqno, rev_seqno, ext_meta_len = \
                struct.unpack(">QQH", body[0:18])
            key = body[18:18+keylen]
            response = { 'opcode'     : opcode,
                         'vbucket'    : status,
                         'by_seqno'   : by_seqno,
                         'rev_seqno'  : rev_seqno,
                         'key'        : key }

        elif opcode == CMD_SNAPSHOT_MARKER:
            response = { 'opcode'     : opcode,
                         'vbucket'    : status }
        else:
            response = { 'err_msg' : "(Stream Request) Unknown response",
                         'opcode'  :  opcode,
                         'status'  : -1 }

        return response
