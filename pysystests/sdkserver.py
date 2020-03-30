import eventlet
import json
import random
import string
import json
import sys
import time
from couchbase.couchbaseclient import CouchbaseClient
from multiprocessing import Process
import testcfg as cfg
pool = eventlet.GreenPool(1000000)
processMap = {}



class SysCouchClient(CouchbaseClient):
    def __init__(self, url, bucket, cred, accport):
        self.pending_get_msgs = 0
        self.bucket = bucket
        self.url = url
        self.cred = cred
        self.ready = True
        self.accport = accport
        try:
            super(SysCouchClient, self).__init__(url, bucket, cred)
            print("sdk_%s: connected to %s => %s" % (accport, url, bucket))
        except Exception as ex:
            print("sdk_%s: unable to establish connection to %s => %s, %s " %\
                (accport, url, bucket, ex))
            self.ready = False

    def incr_pending_get_msgs(self, val):
        self.pending_get_msgs =  \
            self.pending_get_msgs + val

class CouchClientManager():
    def __init__(self, accport):
        self.client_map = {}
        self.accport = accport

    def add_bucket_client(self, bucket = "default",
                          password = "",
                          ip = cfg.COUCHBASE_IP,
                          port = cfg.COUCHBASE_PORT):

        url = "http://%s:%s/pools/default"  % (ip, port)
        client = SysCouchClient(url, bucket, password, self.accport)
        if client.ready == True:
            self.client_map[bucket] = client

    def get_bucket_client(self, bucket,
                          password = "",
                          ip = cfg.COUCHBASE_IP,
                          port = cfg.COUCHBASE_PORT):

        if bucket not in self.client_map:
            self.add_bucket_client(bucket, password, ip, port)

        return self.client_map[bucket]

    def requestHandler(self, client_sock):
        c = ''
        respond = False
        while True:
            _recv = client_sock.recv(1024)
            if not _recv:
                break
            if _recv == '\0':
                respond = True
                break
            else:
                c = c + _recv
        rc = self._requestHandler(c)

        if respond:
            self.sendClientResponse(client_sock, rc)

        client_sock.close()

    def sendClientResponse(self, client_sock, rc):
        msg = ""
        if rc is not None:
            try:
                msg = json.dumps(rc)
            except Exception:
                pass
        client_sock.send(msg)


    def _requestHandler(self, c, retries = 0):
        try:
            data = json.loads(c)
            self.client_from_req(data)
            res = self.exec_request(data)
            return res
        except ValueError as ex:
            print(ex)
            print("unable to decode json: %s" % c)
        except Exception as ex:
            processMap[self.accport]["connected"] = False
            print("Error: %s" % ex)

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

        if data['command'] == 'latency':
            return self.get_op_latency(data)

    def do_mset(self, data):

        keys = data['args']
        template = data['template']
        kv = template['kv']
        ttl = 0
        flags = 0

        if "ttl" in template:
            ttl = int(template['ttl'])
        if "flags" in template:
            flags = template['flags']

        client = self.client_from_req(data)

        for key in keys:
            doc = {"args" : [key, ttl, flags, kv]}
            try:
                self.do_setq(doc, client)
            except Exception as ex:
                raise Exception(ex)

        rc = client.recv_bulk_responses()
        if len(rc) > 0:
            for msg in rc:
                if isinstance(msg, dict) and 'error' in msg and int(msg["error"]) != 0:
                   if int(msg["error"]) == 7:
                       client.reconfig_vbucket_map(forward=True)
                   if int(msg["error"]) == 134:
                       pass # temp failure (oom) ignore
                   else:
                       ts = time.localtime()
                       ts_string = "%s/%s/%s %s:%s:%s" %\
                           (ts.tm_year, ts.tm_mon, ts.tm_mday, ts.tm_hour, ts.tm_min, ts.tm_sec)
                       print("%s:set MemcachedError%d: %s" % (ts_string, msg["error"], msg["rv"]))

        return True


    def _get_set_args(self, data):
        key = str(data['args'][0])
        exp = int(data['args'][1])
        flags = int(data['args'][2])
        value = json.dumps(data['args'][3])

        return key, exp, flags, value

    def do_setq(self, args, client):
        key, exp, flags, value = self._get_set_args(args)
        client.setq(key, exp, flags, value)
        return True

    def get_op_latency(self, data):

        # retrieve instance of sdk client
        client = self.client_from_req(data)

        # op_args pass in as tuple, i.e
        # set => ('key', 0, 0, 'val')
        op_args = data['args']

        # select op
        op = data['op']
        if op == 'set':
            op_args[3] = json.dumps(op_args[3])
            func = client.set
        if op == 'get':
            func = client.get
        if op == 'delete':
            func = client.delete

        # timed wrapper
        start = time.time()
        rc = func(*op_args)  # exec
        end = time.time()

        latency = end - start
        return latency


    def do_set(self, data):
        key, exp, flags, value = self._get_set_args(data)
        client = self.client_from_req(data)
        return client.set(key, exp, flags, value)

    def do_get(self, data):
        key = str(data['args'][0])
        client = self.client_from_req(data)
        client.get(key)
        return key

    def do_mget(self, data):
        keys = data['args']
        client = self.client_from_req(data)
        for key in keys:
            key = str(key)
            client.getq(key)

        # increment getq count
        client.incr_pending_get_msgs(len(keys))

        if client.pending_get_msgs > 400:
            rc = client.recv_bulk_responses()
            if len(rc) > 0:
                for msg in rc:
                    if isinstance(msg, dict) and 'error' in msg and int(msg["error"]) != 0:
                        if int(msg["error"]) == 7:
                            client.reconfig_vbucket_map(forward=True)
                        if int(msg["error"]) == 134:
                            pass # temp failure (oom) ignore
                        else:
                            ts = time.localtime()
                            ts_string = "%s/%s/%s %s:%s:%s" %\
                                (ts.tm_year, ts.tm_mon, ts.tm_mday, ts.tm_hour, ts.tm_min, ts.tm_sec)
                            print("%s:get MemcachedError%d: %s" % (ts_string, msg["error"], msg["rv"]))


            client.pending_get_msgs = 0
        else:
            client.noop()
        return True


    def do_delete(self, data):

        key = str(data['args'][0])
        client = self.client_from_req(data)
        res = client.delete(key)
        return res

    def do_mdelete(self, data):
        keys = data['args']
        results = []
        client = self.client_from_req(data)
        for key in keys:
            key = str(key)
            client.deleteq(key)

        return True


    def client_from_req(self, data, password = ""):
        bucket = str(data["bucket"])
        if "password" in data:
            password = str(data["password"])
        ip = data['cb_ip']
        port = data['cb_port']
        client = self.get_bucket_client(bucket, password, ip, port)
        return client


def monitorSubprocesses():
    # when any subprocess ends, attempt to restart
    while True:
        for port in processMap:
            if not processMap[port]['process'].is_alive():
                restart_listener(port)
        time.sleep(1)

def restart_listener(port):
    stop_listener(port)
    start_listener(port)

def stop_listener(port):
    process = processMap[port]["process"]
    try:
        print("sdk_%s: exiting" % (port))
        process.terminate()
    except Exception as ex:
        print("sdk_%s: error occured termination %s" % (port, ex))

def start_listener(port):
    p = Process(target=_run, args=(port,))
    processMap[port] = {"process" : p,
                        "connected" : True,
                        "alt_nodes" : []}

    print("sdk_%s: starting" % port)
    p.start()

def _run(port):
    server = eventlet.listen(('127.0.0.1', port))
    client_mgr = CouchClientManager(port)
    while processMap[port]["connected"]:
        new_sock, address = server.accept()
        pool.spawn_n(client_mgr.requestHandler, new_sock)

if __name__ == '__main__':
    for port in range(50008, 50012):
        start_listener(port)
    monitorSubprocesses()
