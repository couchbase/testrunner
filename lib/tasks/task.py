import time
import logger
from threading import Thread
from membase.api.rest_client import RestConnection
from membase.api.exception import BucketCreationException
from membase.helper.bucket_helper import BucketOperationHelper
from memcached.helper.data_helper import KVStoreAwareSmartClient, MemcachedClientHelper
from mc_bin_client import MemcachedError
from tasks.future import Future
import copy
import json

PENDING='PENDING'
EXECUTING='EXECUTING'
CHECKING='CHECKING'
FINISHED='FINISHED'

class Task(Future):
    def __init__(self, name):
        Future.__init__(self)
        self.log = logger.Logger.get_logger()
        self.state = PENDING
        self.name = name
        self.cancelled = False
        self.retries = 0
        self.res = None

    def step(self, task_manager):
        if not self.done():
            if self.state == PENDING:
                self.state = EXECUTING
                task_manager.schedule(self)
            elif self.state == EXECUTING:
                self.execute(task_manager)
            elif self.state == CHECKING:
                self.check(task_manager)
            elif self.state != FINISHED:
                raise Exception("Bad State in {0}: {1}".format(self.name, self.state))

    def execute(self, task_manager):
        raise NotImplementedError

    def check(self, task_manager):
        raise NotImplementedError

class NodeInitializeTask(Task):
    def __init__(self, server):
        Task.__init__(self, "node_init_task")
        self.server = server
        self.quota = 0

    def execute(self, task_manager):
        rest = RestConnection(self.server)
        username = self.server.rest_username
        password = self.server.rest_password
        rest.init_cluster(username, password)
        info = rest.get_nodes_self()
        self.quota = int(info.mcdMemoryReserved * 2 / 3)
        rest.init_cluster_memoryQuota(username, password, self.quota)
        self.state = CHECKING
        task_manager.schedule(self)

    def check(self, task_manager):
        self.state = FINISHED
        self.set_result(self.quota)

class BucketCreateTask(Task):
    def __init__(self, server, bucket='default', replicas=1, port=11210, size=0,
                 password=None):
        Task.__init__(self, "bucket_create_task")
        self.server = server
        self.bucket = bucket
        self.replicas = replicas
        self.port = port
        self.size = size
        self.password = password

    def execute(self, task_manager):
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
            self.state = CHECKING
            task_manager.schedule(self)
        except BucketCreationException as e:
            self.state = FINISHED
            self.set_exception(e)

    def check(self, task_manager):
        try:
            if BucketOperationHelper.wait_for_memcached(self.server, self.bucket):
                self.set_result(True)
                self.state == FINISHED
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

    def execute(self, task_manager):
        rest = RestConnection(self.server)
        if rest.delete_bucket(self.bucket):
            self.state = CHECKING
            task_manager.schedule(self)
        else:
            self.state = FINISHED
            self.set_result(False)

    def check(self, task_manager):
        rest = RestConnection(self.server)
        if BucketOperationHelper.wait_for_bucket_deletion(self.bucket, rest, 200):
            self.set_result(True)
        else:
            self.set_result(False)
        self.state = FINISHED

class RebalanceTask(Task):
    def __init__(self, servers, to_add=[], to_remove=[]):
        Task.__init__(self, "rebalance_task")
        self.servers = servers
        self.to_add = to_add
        self.to_remove = to_remove

    def execute(self, task_manager):
        try:
            self.add_nodes(task_manager)
            self.start_rebalance(task_manager)
            self.state = CHECKING
            task_manager.schedule(self)
        except Exception as e:
            self.state = FINISHED
            self.set_exception(e)

    def add_nodes(self, task_manager):
        master = self.servers[0]
        rest = RestConnection(master)
        for node in self.to_add:
            self.log.info("adding node {0}:{1} to cluster".format(node.ip, node.port))
            added = rest.add_node(master.rest_username, master.rest_password,
                                  node.ip, node.port)

    def start_rebalance(self, task_manager):
        rest = RestConnection(self.servers[0])
        nodes = rest.node_statuses()
        ejectedNodes = []
        for server in self.to_remove:
            for node in nodes:
                if server.ip == node.ip and server.port == node.port:
                    ejectedNodes.append(node.id)
        rest.rebalance(otpNodes=[node.id for node in nodes], ejectedNodes=ejectedNodes)

    def check(self, task_manager):
        rest = RestConnection(self.servers[0])
        progress = rest._rebalance_progress()
        if progress is not -1 and progress is not 100:
            task_manager.schedule(self, 10)
        else:
            self.state = FINISHED
            self.set_result(True)

class StatsWaitTask(Task):
    EQUAL='=='
    NOT_EQUAL='!='
    LESS_THAN='<'
    LESS_THAN_EQ='<='
    GREATER_THAN='>'
    GREATER_THAN_EQ='>='

    def __init__(self, stats, bucket):
        Task.__init__(self, "stats_wait_task")
        self.bucket = bucket
        self.stats = stats
        self.conns = {}

    def execute(self, task_manager):
        self.state = CHECKING
        task_manager.schedule(self)

    def check(self, task_manager):
        for node in self.stats:
            client = self._get_connection(node['server'])
            for param, stats_list in node['stats'].items():
                stats = client.stats(param)
                for k, v in stats_list.items():
                    if not stats.has_key(k):
                        self.state = FINISHED
                        self.set_exception(Exception("Stat {0} not found".format(k)))
                        return
                    if not self._compare(v['compare'], stats[k], v['value']):
                        task_manager.schedule(self, 5)
                        return
        for server, conn in self.conns.items():
            conn.close()
        self.state = FINISHED
        self.set_result(True)

    def _get_connection(self, server):
        if not self.conns.has_key(server):
            self.conns[server] = MemcachedClientHelper.direct_client(server, self.bucket)
        return self.conns[server]

    def _compare(self, cmp_type, a, b):
        if isinstance(b, (int, long)) and a.isdigit():
            a = int(a)
        elif isinstance(b, (int, long)) and not a.isdigit():
                return False
        if (cmp_type == StatsWaitTask.EQUAL and a == b) or\
            (cmp_type == StatsWaitTask.NOT_EQUAL and a != b) or\
            (cmp_type == StatsWaitTask.LESS_THAN_EQ and a <= b) or\
            (cmp_type == StatsWaitTask.GREATER_THAN_EQ and a >= b) or\
            (cmp_type == StatsWaitTask.LESS_THAN and a < b) or\
            (cmp_type == StatsWaitTask.GREATER_THAN and a > b):
            return True
        return False

class GenericLoadingTask(Thread, Task):
    def __init__(self, rest, bucket, info = None, kv_store = None, store_enabled = True):
        Thread.__init__(self)
        Task.__init__(self, "load_gen_task")
        self.rest = rest
        self.client = KVStoreAwareSmartClient(rest, bucket, kv_store, info, store_enabled)
        self.doc_ids = []

    def cancel(self):
        self.cancelled = True
        self.join()
        self.log.info("cancelling LoadDocGeneratorTask")

    def execute(self, task_manager):
        self.start()
        self.state = FINISHED

    def check(self, task_manager):
        pass

    def run(self):
        while self.has_next() and not self.cancelled:
            op, key, value, exp = self.next()
            if not self._try_operation(op, key, value, exp):
                self.state = FINISHED
                self.set_result(Exception("Operation failed"))
            self.doc_ids.append(key)
        self.state = FINISHED
        self.set_result(self.doc_ids)

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
