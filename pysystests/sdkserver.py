import eventlet
import json
import random
import string
import yajl
import sys
from couchbase.couchbaseclient import CouchbaseClient

#default_client = CouchbaseClient("http://localhost:9000/pools/default","default","")
client_map = {}
pool = eventlet.GreenPool(1000)


def requestHandler(client_sock):
    c = '' 
    while True: 
        _recv = client_sock.recv(1024)
        if not _recv:
            break
        else:
            c = c + _recv 
    try:
        data = yajl.loads(c)

        #multi-bucket support
        bucket = data["bucket"]
        if bucket not in client_map:
            url = "http://%s:%s/pools/default/"  % (data["cb_ip"], data["cb_port"])
            new_client = CouchbaseClient(url, bucket,"")
            client_map[data["bucket"]] = new_client

        res = exec_request(data)
        client_sock.sendall(str(res))
    except ValueError as ex:
        print ex 
        client_sock.sendall(ex)

 
def exec_request( data):
    if data['command'] == 'set':
        return do_set(data)

    if data['command'] == 'setq':
        return do_setq(data)

    if data['command'] == 'mset':
        return do_mset(data)

    if data['command'] == 'mdelete':
        return do_mdelete(data)

    if data['command'] == 'get':
        return do_get(data)

    if data['command'] == 'mget':
        return do_mget(data)

    if data['command'] == 'delete':
        return do_delete(data)

    if data['command'] == 'query':
        return do_query(data)


def do_mset(data):
    keys = data['args']
    template = data['template']
    kv = template['kv']
    ttl = 0
    flags = 0
    
    if "ttl" in template:
        ttl = int(template['ttl'])
    if "flags" in template:
        flags = template['flags']
    if "size" in template:
        size = int(template['size'])
        kv_size = sys.getsizeof(kv)/8
        if  kv_size < size:
            padding = _random_string(size - kv_size)
            kv.update({"padding" : padding})

    client = client_map[data["bucket"]]
    for key in keys:
        doc = {"args" : [key, ttl, flags, kv]}
        try:
            do_setq(doc, client)
        except Exception as ex:
            print ex

def _get_set_args( data):
    key = str(data['args'][0]) 
    exp = int(data['args'][1])
    flags = int(data['args'][2]) 
    value = yajl.dumps(data['args'][3])

    return key, exp, flags, value

def do_setq(args, client):
    key, exp, flags, value = _get_set_args(args)
    client.setq(key,exp, flags, value)
    return True
 
def do_set(data):
    key, exp, flags, value = _get_set_args(data)
    client = client_map[data["bucket"]]
    return client.set(key,exp, flags, value)

def do_get( data):
    key = str(data['args'][0])
    client = client_map[data["bucket"]]
    client.get(key)
    return key


def do_mget( data):
    keys = data['args']
    results = []
    client = client_map[data["bucket"]]
    for key in keys:
        key = str(key)
        client.getq(key)
    return results


def do_delete( data):

    key = str(data['args'][0])
    client = client_map[data["bucket"]]
    res = client.delete(key)
    return res

def do_query( data):
    design_doc_name = data['args'][0]
    view_name = data['args'][1]
    bucket = data['args'][2]
    params = data['args'][3]
    res = rest.query_view(design_doc_name, view_name, bucket, params)
    return res

def _random_string(length):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(length))


print "Python sdk starting on port 50008"
server = eventlet.listen(('127.0.0.1', 50008))
while True:
    new_sock, address = server.accept()
    pool.spawn_n(requestHandler, new_sock)


