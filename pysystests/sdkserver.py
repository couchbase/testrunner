import eventlet
import json
import random
import string
import json
import sys
from couchbase.couchbaseclient import CouchbaseClient
from multiprocessing import Process
import testcfg as cfg
pool = eventlet.GreenPool(1000000)



class SysCouchClient(CouchbaseClient):
    def __init__(self, url, bucket, cred):
        self.pending_get_msgs = 0
        super(SysCouchClient, self).__init__(url, bucket, cred)

    def incr_pending_get_msgs(self, val):
        self.pending_get_msgs =  \
            self.pending_get_msgs + val

class CouchClientManager():
    def __init__(self):
        self.client_map = {}

    def add_bucket_client(self, bucket = "default"):
        ip = cfg.COUCHBASE_IP
        port = cfg.COUCHBASE_PORT
        url = "http://%s:%s/pools/default/"  % (ip, port)
        client = SysCouchClient(url, bucket,"")
        self.client_map[bucket] = client

    def get_bucket_client(self, bucket):
        if bucket not in self.client_map:
            self.add_bucket_client(bucket)

        return self.client_map[bucket]

    def requestHandler(self, client_sock):
        c = ''
        while True:
            _recv = client_sock.recv(1024)
            if not _recv:
                break
            else:
                c = c + _recv
        try:
            data = json.loads(c)
            res = self.exec_request(data)
        except ValueError as ex:
            print ex


    def exec_request(self, data):
        if data['command'] == 'set':
            return self.do_set(data)

        if data['command'] == 'setq':
            return self.do_setq(data)

        if data['command'] == 'mset':
            return self.do_mset(data)

        if data['command'] == 'mdelete':
            return self.do_mdelete(data)

        if data['command'] == 'get':
            return self.do_get(data)

        if data['command'] == 'mget':
            return self.do_mget(data)

        if data['command'] == 'delete':
            return self.do_delete(data)

        if data['command'] == 'query':
            return self.do_query(data)


    def do_mset(self, data):

        bucket = data["bucket"]
        keys = data['args']
        template = data['template']
        kv = template['kv']
        ttl = 0
        flags = 0

        if "ttl" in template:
            ttl = int(template['ttl'])
        if "flags" in template:
            flags = template['flags']
        if template["size"] is not None:
            size = int(template['size'])
            kv_size = sys.getsizeof(kv)/8
            if  kv_size < size:
                padding = _random_string(size - kv_size)
                kv.update({"padding" : padding})

        client = self.get_bucket_client(bucket)

        # noop every 1k keys
        cnt = 0
        for key in keys:
            doc = {"args" : [key, ttl, flags, kv]}
            try:
                self.do_setq(doc, client)
            except Exception as ex:
                print ex
            cnt = cnt + 1
        client.recv_bulk_responses()

        return True


    def _get_set_args(self, data):
        key = str(data['args'][0])
        exp = int(data['args'][1])
        flags = int(data['args'][2])
        value = json.dumps(data['args'][3])

        return key, exp, flags, value

    def do_setq(self, args, client):
        key, exp, flags, value = self._get_set_args(args)
        client.setq(key,exp, flags, value)
        return True

    def do_set(self, data):
        key, exp, flags, value = self._get_set_args(data)
        client = self.get_bucket_client(data["bucket"])
        return client.set(key,exp, flags, value)

    def do_get(self, data):
        key = str(data['args'][0])
        client = self.get_bucket_client(data["bucket"])
        client.get(key)
        return key


    def do_mget(self, data):
        keys = data['args']
        client = self.get_bucket_client(data["bucket"])
        for key in keys:
            key = str(key)
            client.getq(key)

        # increment getq count
        client.incr_pending_get_msgs(len(keys))

        if client.pending_get_msgs > 400:
            rc = client.recv_bulk_responses()
            client.pending_get_msgs = 0
        else:
            client.noop()
        return True


    def do_delete(self, data):

        key = str(data['args'][0])
        client = self.get_bucket_client(data["bucket"])
        res = client.delete(key)
        return res

    def do_mdelete(self, data):
        keys = data['args']
        results = []
        client = self.get_bucket_client(data["bucket"])
        for key in keys:
            key = str(key)
            client.deleteq(key)

        return True


def _random_string(length):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(length))



def start_listener(port):
    print "Python sdk starting on port %s" % port
    server = eventlet.listen(('127.0.0.1', port))
    client_mgr = CouchClientManager()
    while True:
        new_sock, address = server.accept()
        pool.spawn_n(client_mgr.requestHandler, new_sock)


if __name__ == '__main__':
    for port in xrange(50008, 50012):
        p = Process(target=start_listener, args=(port,))
        p.start()
