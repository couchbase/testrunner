import time
import logger
from threading import Thread
from exception import TimeoutException
from membase.api.rest_client import RestConnection
from membase.api.exception import BucketCreationException
from membase.helper.bucket_helper import BucketOperationHelper
from memcached.helper.data_helper import KVStoreAwareSmartClient
from mc_bin_client import MemcachedError
import copy
import json

class Task():
    def __init__(self, name):
        self.log = logger.Logger.get_logger()
        self.name = name
        self.cancelled = False
        self.retries = 0
        self.res = None

    def cancel(self):
        self.cancelled = True

    def is_timed_out(self):
        if self.res is not None and self.res['status'] == 'timed_out':
            return True
        return False

    def set_result(self, result):
        self.res = result

    def result(self, retries=0):
        while self.res is None:
            if retries == 0 or self.retries < retries:
                time.sleep(1)
            else:
                self.res = {"status": "timed_out", "value": None}
        if self.res['status'] == 'error':
            raise Exception(self.res['value'])
        if self.res['status'] == 'timed_out':
            raise TimeoutException("task {0} timed out, tried {1} times".format(self.name,
                                                                                self.retries))
        return self.res['value']

class NodeInitializeTask(Task):
    def __init__(self, server):
        Task.__init__(self, "node_init_task")
        self.state = "initializing"
        self.server = server

    def step(self, task_manager):
        if self.cancelled:
            self.result = self.set_result({"status": "cancelled", "value": None})
        elif self.is_timed_out():
            return
        elif self.state == "initializing":
            self.state = "node init"
            task_manager.schedule(self)
        elif self.state == "node init":
            self.init_node()
            self.state = "cluster init"
            task_manager.schedule(self)
        elif self.state == "cluster init":
            self.init_node_memory()
        else:
            raise Exception("Bad State in NodeInitializationTask")

    def init_node(self):
        rest = RestConnection(self.server)
        rest.init_cluster(self.server.rest_username, self.server.rest_password)

    def init_node_memory(self):
        rest = RestConnection(self.server)
        info = rest.get_nodes_self()
        quota = int(info.mcdMemoryReserved * 2 / 3)
        rest.init_cluster_memoryQuota(self.server.rest_username, self.server.rest_password, quota)
        self.state = "finished"
        self.set_result({"status": "success", "value": quota})

class BucketCreateTask(Task):
    def __init__(self, server, bucket='default', replicas=1, port=11210, size=0, password=None):
        Task.__init__(self, "bucket_create_task")
        self.server = server
        self.bucket = bucket
        self.replicas = replicas
        self.port = port
        self.size = size
        self.password = password
        self.state = "initializing"

    def step(self, task_manager):
        if self.cancelled:
            self.result = self.set_result({"status": "cancelled", "value": None})
        elif self.is_timed_out():
            return
        elif self.state == "initializing":
            self.state = "creating"
            task_manager.schedule(self)
        elif self.state == "creating":
            self.create_bucket(task_manager)
        elif self.state == "checking":
            self.check_bucket_ready(task_manager)
        else:
            raise Exception("Bad State in BucketCreateTask")

    def create_bucket(self, task_manager):
        rest = RestConnection(self.server)
        if self.size <= 0:
            info = rest.get_nodes_self()
            size = info.memoryQuota * 2 / 3

        authType = 'none' if self.password is None else 'sasl'

        try:
            rest.create_bucket(bucket=self.bucket,
                               ramQuotaMB=self.size,
                               replicaNumber=self.replicas,
                               proxyPort=self.port,
                               authType=authType,
                               saslPassword=self.password)
            self.state = "checking"
            task_manager.schedule(self)
        except BucketCreationException:
            self.state = "finished"
            self.set_result({"status": "error",
                             "value": "Failed to create bucket {0}".format(self.bucket)})

    def check_bucket_ready(self, task_manager):
        try:
            if BucketOperationHelper.wait_for_memcached(self.server, self.bucket):
                self.set_result({"status": "success", "value": None})
                self.state == "finished"
                return
            else:
                self.log.info("vbucket map not ready after try {0}".format(self.retries))
        except Exception as e:
            self.log.info("vbucket map not ready after try {0}".format(self.retries))
        self.retries = self.retries + 1
        task_manager.schedule(self)

class BucketDeleteTask(Task):
    def __init__(self, server, bucket):
        Task.__init__(self, "bucket_delete_task")
        self.server = server
        self.bucket = bucket
        self.state = "initializing"

    def step(self, task_manager):
        if self.cancelled:
            self.result = self.set_result({"status": "cancelled", "value": None})
        elif self.is_timed_out():
            return
        elif self.state == "initializing":
            self.state = "creating"
            task_manager.schedule(self)
        elif self.state == "creating":
            self.delete_bucket(task_manager)
        elif self.state == "checking":
            self.check_bucket_deleted()
        else:
            raise Exception("Bad State in BucketDeleteTask")

    def delete_bucket(self, task_manager):
        rest = RestConnection(self.server)
        if rest.delete_bucket(self.bucket):
            self.state = "checking"
            task_manager.schedule(self)
        else:
            self.state = "finished"
            self.set_result({"status": "error",
                             "value": "Failed to delete bucket {0}".format(self.bucket)})

    def check_bucket_deleted(self):
        rest = RestConnection(self.server)
        if BucketOperationHelper.wait_for_bucket_deletion(self.bucket, rest, 200):
            self.set_result({"status": "success", "value": None})
        else:
            self.set_result({"status": "error",
                             "value": "{0} bucket took too long to delete".format(self.bucket)})
        self.state = "finished"

class RebalanceTask(Task):
    def __init__(self, servers, to_add=[], to_remove=[]):
        Task.__init__(self, "rebalance_task")
        self.servers = servers
        self.to_add = to_add
        self.to_remove = to_remove
        self.state = "initializing"

    def step(self, task_manager):
        if self.cancelled:
            self.result = self.set_result({"status": "cancelled", "value": None})
        elif self.is_timed_out():
            return
        elif self.state == "initializing":
            self.state = "add_nodes"
            task_manager.schedule(self)
        elif self.state == "add_nodes":
            self.add_nodes(task_manager)
        elif self.state == "start_rebalance":
            self.start_rebalance(task_manager)
        elif self.state == "rebalancing":
            self.rebalancing(task_manager)
        else:
            raise Exception("Bad State in RebalanceTask: {0}".format(self.state))

    def add_nodes(self, task_manager):
        master = self.servers[0]
        rest = RestConnection(master)
        try:
            for node in self.to_add:
                print node
                self.log.info("adding node {0}:{1} to cluster".format(node.ip, node.port))
                added = rest.add_node(master.rest_username, master.rest_password,
                                      node.ip, node.port)
            self.state = "start_rebalance"
            task_manager.schedule(self)
        except Exception as e:
            self.state = "finished"
            self.set_result({"status": "error", "value": e})

    def start_rebalance(self, task_manager):
        rest = RestConnection(self.servers[0])
        nodes = rest.node_statuses()
        ejectedNodes = []
        for node in self.to_remove:
            ejectedNodes.append(node.id)
        try:
            rest.rebalance(otpNodes=[node.id for node in nodes], ejectedNodes=ejectedNodes)
            self.state = "rebalancing"
            task_manager.schedule(self)
        except Exception as e:
            self.state = "finishing"
            self.set_result({"status": "error", "value": e})

    def rebalancing(self, task_manager):
        rest = RestConnection(self.servers[0])
        progress = rest._rebalance_progress()
        if progress is not -1 and progress is not 100:
            task_manager.schedule(self, 10)
        else:
            self.state = "finishing"
            self.set_result({"status": "success", "value": None})


class GenericLoadingTask(Thread, Task):
    def __init__(self, rest, bucket, info = None, kv_store = None, store_enabled = True):
        Thread.__init__(self)
        Task.__init__(self, "load_gen_task")
        self.rest = rest
        self.client = KVStoreAwareSmartClient(rest, bucket, kv_store, info, store_enabled)
        self.state = "pending"
        self.doc_ids = []

    def cancel(self):
        self.cancelled = True
        self.join()
        self.log.info("cancelling LoadDocGeneratorTask")

    def step(self, task_manager):
        if self.state == "pending":
            self.state = "running"
            task_manager.schedule(self)
        elif self.state == "running":
            self.start()
            self.state = "finished"
        else:
            raise Exception("Bad State in DocumentGeneratorTask")

    def run(self):
        while self.has_next() and not self.cancelled:
            op, key, value, exp = self.next()
            if not self._try_operation(op, key, value, exp):
                self.state = "finished"
                self.set_result({"status": "error", "value": "Failed op on {0}".format(key)})
            self.doc_ids.append(key)
        self.state = "finished"
        self.set_result({"status": "success", "value": self.doc_ids})

    def _try_operation(self, op, key, value, exp):
        retry_count = 0
        value = value.replace(" ", "")
        while retry_count < 5:
            try:
                if op == "set":
                    self.client.set(key, value, exp)
                elif op == "get":
                    self.client.mc_get(key)
                elif op == "delete":
                    self.client.delete(key)
                return True
            except MemcachedError as error:
                if error.status == 134:
                    client.reset_vbucket(self.rest, key)
                retry_count += 1
        return False

    def has_next():
        raise NotImplementedError

    def next():
        raise NotImplementedError

class LoadDocGeneratorTask(GenericLoadingTask):
    def __init__(self, rest, doc_generator, bucket, expiration, loop=False):
        GenericLoadingTask.__init__(self, rest, bucket)
        self.doc_generator = doc_generator
        self.expiration = expiration
        self.loop = loop

    def has_next(self):
        if not self.doc_generator.has_next():
            if self.loop:
                self.doc_generator.reset()
            else:
                return False
        return True

    def next(self):
        _value = self.doc_generator.next().encode("ascii", "ignore")
        _json = json.loads(_value, encoding="utf-8")
        _id = _json["_id"].encode("ascii", "ignore")
        return "set", _id, _value, self.expiration

class DocumentMutateTask(GenericLoadingTask):
    def __init__(self, docs, rest, bucket, kv_store=None, info=None, store_enabled=True,
                 expiration=0):
        GenericLoadingTask.__init__(self, rest, bucket, kv_store, info, store_enabled)
        self.docs = docs
        self.expiration = expiration
        self.itr = 0

    def has_next(self):
        return len(self.docs) > self.itr

    def next(self):
        _doc_body = self.docs[self.itr].encode("ascii", "ignore")
        _json = json.loads(_doc_body, encoding="utf-8")
        _id = _json["_id"].encode("ascii", "ignore")
        self.itr = self.itr + 1
        return "set", _id, _doc_body, self.expiration

class DocumentAccessTask(GenericLoadingTask):
    def __init__(self, docs, rest, bucket, info=None):
        GenericLoadingTask.__init__(self, rest, bucket, info = info)
        self.docs = docs
        self.itr = 0

    def has_next(self):
        return len(self.docs) > self.itr

    def next(self):
        _doc_body = self.docs[self.itr].encode("ascii", "ignore")
        _json = json.loads(_doc_body, encoding="utf-8")
        _id = _json["_id"].encode("ascii", "ignore")
        self.itr = self.itr + 1
        return "get", _id, _doc_body, 0

class DocumentDeleteTask(GenericLoadingTask):
    def __init__(self, docs, rest, bucket, info=None, kv_store=None, store_enabled=True):
        GenericLoadingTask.__init__(self, rest, bucket, kv_store, info, store_enabled)
        self.docs = docs
        self.itr = 0

    def has_next(self):
        return len(self.docs) > self.itr

    def next(self):
        _doc_body = self.docs[self.itr].encode("ascii", "ignore")
        _json = json.loads(_doc_body, encoding="utf-8")
        _id = _json["_id"].encode("ascii", "ignore")
        self.itr = self.itr + 1
        return "delete", _id, "", 0
