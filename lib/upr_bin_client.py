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

        self.dead = False

    def _restart_reader(self):
        """ restart reader thread """
        self.conn = UprClient.Reader(self)
        self.conn.start()


    def _open(self, op):

        if not self.conn.isAlive():
            self.reconnect()
            self._restart_reader()
            self.dead = False

        return self._handle_op(op)

    def close(self):
        super(UprClient, self).close()
        self.dead = True

    def open_consumer(self, name):
        """ opens an upr consumer connection """

        op = OpenConsumer(name)
        return self._open(op)

    def open_producer(self, name):
        """ opens an upr producer connection """

        op = OpenProducer(name)
        return self._open(op)

    def open_notifier(self, name):
        """ opens an upr notifier connection """

        op = OpenNotifier(name)
        return self._open(op)

    def get_failover_log(self, vbucket):
        """ get upr failover log """

        op = GetFailoverLog(vbucket)
        return self._handle_op(op)

    def flow_control(self, buffer_size):
        """ sent to notify producer how much data a client is able to receive
            while streaming mutations"""

        op = FlowControl(buffer_size)
        return self._handle_op(op)

    def ack(self, nbytes):
        """ sent to notify producer number of bytes client has received"""

        op = Ack(nbytes)
        return self._handle_op(op)

    def quit(self):
        """ send quit command to mc - when response is recieved quit reader """
        op = Quit()
        r = {'opcode': op.opcode}
        if not self.dead:
            r = self._handle_op(op)
            if r['status'] == 0:
                self.dead = True
        else:
            r['status'] = 0xff

        return r

    def add_stream(self, vbucket, takeover = 0):
        """ sent to upr-consumer to add stream on a particular vbucket.
            the takeover flag is there for completeness and is used by
            ns_server during vbucket move """

        op = AddStream(vbucket, takeover)
        return self._handle_op(op)

    def close_stream(self, vbucket):
        """ sent either to producer or consumer to close stream openned by
            this clients connection on specified vbucket """

        op = CloseStream(vbucket)
        return self._handle_op(op)

    def stream_req(self, vbucket, takeover, start_seqno, end_seqno,
                       vb_uuid, snap_start = None, snap_end = None):
        """" sent to upr-producer to stream mutations from
             a particular vbucket.

             upon sucessful stream request an UprStream object is created
             that can be used by the client to receive mutations.

             vbucket = vbucket number to strem mutations from
             takeover = specify takeover flag 0|1
             start_seqno = seqno to begin streaming
             end_seqno = seqno to specify end of stream
             vb_uuid = vbucket uuid as specified in failoverlog
             snapt_start = start seqno of snapshot
             snap_end = end seqno of snapshot """

        op = StreamRequest(vbucket, takeover, start_seqno, end_seqno,
                                        vb_uuid, snap_start, snap_end)

        response = self._handle_op(op)

        stream = UprStream(op, response)

        if stream.status  == 0:
            # upon successful stream request associate stream object
            # with vbucket
            self.streams[vbucket] = stream

        return stream

    def get_stream(self, vbucket):
        """ for use by external clients to get stream
            associated with a particular vbucket """
        return self.streams.get(vbucket)

    def _handle_op(self, op):
        """ sends op to mcd. Then it recvs response

            if the received response is for another op then it will
            attempt to get the next response and retry 5 times."""

        if not self.conn.isAlive():
            self._restart_reader()

        self.ops[op.opaque] = op
        self.send_op(op)


        # poll op queue for a response
        wait = 30
        resp = None
        while wait > 0 and resp is None:
            try:
                resp = op.queue.get(timeout=1)
            except Queue.Empty:
                pass

            wait -= 1

            if not self.conn.isAlive():
                # op caused ClientError

                if resp is None:
                    # client died without sending response
                    resp = {'opcode'  : op.opcode,
                            'status'  : 0xff}
                break

        assert resp is not None,\
            "ERROR: Never received response for op: %s" % op.opcode

        return resp

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

                except Exception as ex:
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

    def __init__(self, op, response):

        self.op = op
        self.opcode = op.opcode
        self.vbucket = op.vbucket
        self.last_by_seqno = 0

        self.failover_log = response.get('failover_log')
        self.err_msg = response.get('err_msg')
        self.status = response.get('status')
        self.rollback = response.get('rollback')
        self.rollback_seqno = response.get('seqno')

        self._ended = False
        if self.status != 0:
            self._ended = True

    @property
    def ended(self):
        return self._ended

    def has_response(self):
        """ true if END has not been received or timeout waiting for items in recv q """
        return not self._ended

    def next_response(self, timeout = 5):
        """ Fetch next message from op queue. verify that mutations
            are recieved in expected order.
            Deactivate if end_stream command received """

        msg = None

        if self.has_response():

            try:
                msg = self.op.queue.get(timeout = timeout)
            except Queue.Empty:
                return None

            # mutation response verification
            if msg['opcode'] in (CMD_MUTATION, CMD_DELETION):

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

    def run(self, end=None):
        """ return the responses from the stream up to seqno = end

            Note: all this data has already been sent over the wire
            and this method is merely reading what's been received
            off the queue. """

        mutations = []
        while self.has_response():
            response = self.next_response()
            if response is None:
                break
            mutations.append(response)
            if end and self.last_by_seqno >= end:
                break

        return mutations

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

class CloseStream(Operation):
    """ CloseStream spec """

    def __init__(self, vbucket, takeover = 0):
        opcode = CMD_CLOSE_STREAM
        Operation.__init__(self, opcode,
                           vbucket = vbucket)

    def formated_response(self, opcode, keylen, extlen, status, cas, body, opaque):
        response = { 'opcode'        : opcode,
                     'status'        : status,
                     'value'         : body}
        return response

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

    def __init__(self, vbucket, takeover, start_seqno, end_seqno,
                 vb_uuid, snap_start = None, snap_end = None):

        if snap_start is None:
            snap_start = start_seqno
        if snap_end is None:
            snap_end = start_seqno

        opcode = CMD_STREAM_REQ
        extras = struct.pack(">IIQQQQQ", takeover, 0,
                             start_seqno,
                             end_seqno,
                             vb_uuid,
                             snap_start,
                             snap_end)

        Operation.__init__(self, opcode,
                           extras = extras,
                           vbucket = vbucket)
        self.start_seqno = start_seqno
        self.end_seqno = end_seqno
        self.vb_uuid = vb_uuid
        self.takeover = takeover
        self.snap_start = snap_start
        self.snap_end = snap_end


    def formated_response(self, opcode, keylen, extlen, status, cas, body, opaque):
        if opcode == CMD_STREAM_REQ:

            response = { 'opcode' : opcode,
                         'status' : status,
                         'failover_log' : [],
                         'err_msg'   : None}

            if status == 0:
                assert (len(body) % 16) == 0
                response['failover_log'] = []

                pos = 0
                bodylen = len(body)
                while bodylen > pos:
                    vb_uuid, seqno = struct.unpack(">QQ", body[pos:pos+16])
                    response['failover_log'].append((vb_uuid, seqno))
                    pos += 16
            elif status == 35:

                seqno = struct.unpack(">II",body)
                response['seqno'] = seqno[0]
                response['rollback'] = seqno[1]

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

            assert len(body) == 20
            snap_start, snap_end, flag =\
                struct.unpack(">QQI", body)
            assert flag in (0,1) , "Invalid snapshot flag: %s" % flag
            assert snap_start <= snap_end, "Snapshot start: %s > end: %s" %\
                                                (snap_start, snap_end)
            flag = ('memory','disk')[flag]

            response = { 'opcode'     : opcode,
                         'vbucket'    : status,
                         'snap_start_seqno' : snap_start,
                         'snap_end_seqno'   : snap_end,
                         'flag'   : flag }

        else:
            response = { 'err_msg' : "(Stream Request) Unknown response",
                         'opcode'  :  opcode,
                         'status'  : -1 }

        return response

class GetFailoverLog(Operation):
    """ GetFailoverLog spec """

    def __init__(self, vbucket):
        opcode = CMD_GET_FAILOVER_LOG
        Operation.__init__(self, opcode,
                           vbucket = vbucket)

    def formated_response(self, opcode, keylen, extlen, status, cas, body, opaque):

        failover_log = []

        if status == 0:
            assert len(body) % 16 == 0
            pos = 0
            bodylen = len(body)
            while bodylen > pos:
                vb_uuid, seqno = struct.unpack(">QQ", body[pos:pos+16])
                failover_log.append((vb_uuid, seqno))
                pos += 16

        response = { 'opcode'        : opcode,
                     'status'        : status,
                     'value'         : failover_log}
        return response

class FlowControl(Operation):
    """ FlowControl spec """

    def __init__(self, buffer_size):
        opcode = CMD_FLOW_CONTROL
        Operation.__init__(self, opcode,
                           key = "connection_buffer_size",
                           value = str(buffer_size))

    def formated_response(self, opcode, keylen, extlen, status, cas, body, opaque):
        response = { 'opcode'        : opcode,
                     'status'        : status,
                     'body'          : body}
        return response

class Ack(Operation):
    """ Ack spec """

    def __init__(self, nbytes):
        opcode = CMD_UPR_ACK
        self.nbytes = nbytes
        acked = struct.pack(">L", self.nbytes)
        Operation.__init__(self, opcode,
                           value = acked)

    def formated_response(self, opcode, keylen, extlen, status, cas, body, opaque):
        response = { 'opcode'        : opcode,
                     'status'        : status,
                     'body'          : body}
        return response

class Quit(Operation):

    def __init__(self):
        opcode = CMD_QUIT
        Operation.__init__(self, opcode)

    def formated_response(self, opcode, keylen, extlen, status, cas, body, opaque):
        response = { 'opcode'        : opcode,
                     'status'        : status}
        return response
