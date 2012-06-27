import logger
import random
import string
from threading import Thread
from memcacheConstants import ERR_NOT_FOUND
from membase.api.rest_client import RestConnection
from membase.api.exception import BucketCreationException
from membase.helper.bucket_helper import BucketOperationHelper
from memcached.helper.data_helper import VBucketAwareMemcached, MemcachedClientHelper
from couchbase.document import DesignDocument, View
from mc_bin_client import MemcachedError
from tasks.future import Future
import json
from membase.api.exception import DesignDocCreationException, QueryViewException, ReadDocumentException, RebalanceFailedException

#TODO: Setup stacktracer
#TODO: Needs "easy_install pygments"
#import stacktracer
#stacktracer.trace_start("trace.html",interval=30,auto=True) # Set auto flag to always update file!


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
    def __init__(self, server, bucket='default', replicas=1, size=0, port=11211,
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
            self.size = info.memoryQuota * 2 / 3

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
                self.log.info("bucket '{0}' was created with per node RAM quota: {1}".format(self.bucket, self.size))
                self.set_result(True)
                self.state = FINISHED
                return
            else:
                self.log.info("vbucket map not ready after try {0}".format(self.retries))
        except Exception:
            self.log.info("vbucket map not ready after try {0}".format(self.retries))
        self.retries = self.retries + 1
        task_manager.schedule(self)


class BucketDeleteTask(Task):
    def __init__(self, server, bucket="default"):
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
    def __init__(self, servers, to_add=[], to_remove=[], do_stop=False, progress=30):
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
            rest.add_node(master.rest_username, master.rest_password,
                          node.ip, node.port)

    def start_rebalance(self, task_manager):
        rest = RestConnection(self.servers[0])
        nodes = rest.node_statuses()
        ejectedNodes = []
        for server in self.to_remove:
            for node in nodes:
                if server.ip == node.ip and int(server.port) == int(node.port):
                    ejectedNodes.append(node.id)
        rest.rebalance(otpNodes=[node.id for node in nodes], ejectedNodes=ejectedNodes)

    def check(self, task_manager):
        rest = RestConnection(self.servers[0])
        try:
            progress = rest._rebalance_progress()
        except RebalanceFailedException as ex:
            self.state = FINISHED
            self.set_exception(ex)
        if progress != -1 and progress != 100:
            task_manager.schedule(self, 10)
        else:
            self.log.info("rebalancing was completed with progress: {0}%".format(progress))
            self.state = FINISHED
            self.set_result(True)

class StatsWaitTask(Task):
    EQUAL='=='
    NOT_EQUAL='!='
    LESS_THAN='<'
    LESS_THAN_EQ='<='
    GREATER_THAN='>'
    GREATER_THAN_EQ='>='

    def __init__(self, servers, bucket, param, stat, comparison, value):
        Task.__init__(self, "stats_wait_task")
        self.servers = servers
        self.bucket = bucket
        self.param = param
        self.stat = stat
        self.comparison = comparison
        self.value = value
        self.conns = {}

    def execute(self, task_manager):
        self.state = CHECKING
        task_manager.schedule(self)

    def check(self, task_manager):
        stat_result = 0
        for server in self.servers:
            client = self._get_connection(server)
            try:
                stats = client.stats(self.param)
                if not stats.has_key(self.stat):
                    self.state = FINISHED
                    self.set_exception(Exception("Stat {0} not found".format(self.stat)))
                    return
                if stats[self.stat].isdigit():
                    stat_result += long(stats[self.stat])
                else:
                    stat_result = stats[self.stat]
            except EOFError as ex:
                self.state = FINISHED
                self.set_exception(ex)

        if not self._compare(self.comparison, str(stat_result), self.value):
            self.log.info("Not Ready: %s %s %s %s expected on %s" % (self.stat, stat_result,
                      self.comparison, self.value, self._stringify_servers()))
            task_manager.schedule(self, 5)
            return
        self.log.info("Saw %s %s %s %s expected on %s" % (self.stat, stat_result,
                      self.comparison, self.value, self._stringify_servers()))

        for server, conn in self.conns.items():
            conn.close()
        self.state = FINISHED
        self.set_result(True)

    def _stringify_servers(self):
        return ''.join([`server.ip` for server in self.servers])

    def _get_connection(self, server):
        if not self.conns.has_key(server):
            self.conns[server] = MemcachedClientHelper.direct_client(server, self.bucket)
        return self.conns[server]

    def _compare(self, cmp_type, a, b):
        if isinstance(b, (int, long)) and a.isdigit():
            a = long(a)
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
    def __init__(self, server, bucket, kv_store):
        Thread.__init__(self)
        Task.__init__(self, "load_gen_task")
        self.kv_store = kv_store
        self.client = VBucketAwareMemcached(RestConnection(server), bucket)

    def execute(self, task_manager):
        self.start()
        self.state = EXECUTING

    def check(self, task_manager):
        pass

    def run(self):
        while self.has_next() and not self.done():
            self.next()
        self.state = FINISHED
        self.set_result(True)

    def has_next(self):
        raise NotImplementedError

    def next(self):
        raise NotImplementedError

    def _unlocked_create(self, partition, key, value):
        try:
            value_json = json.loads(value)
            value_json['mutated'] = 0
            value = json.dumps(value_json)
        except ValueError:
            index = random.choice(range(len(value)))
            value = value[0:index] + random.choice(string.ascii_uppercase) + value[index+1:]

        try:
            self.client.set(key, self.exp, 0, value)
            partition.set(key, value, self.exp)
        except MemcachedError as error:
            self.state = FINISHED
            self.set_exception(error)

    def _unlocked_read(self, partition, key):
        try:
            o, c, d = self.client.get(key)
        except MemcachedError as error:
            if error.status == ERR_NOT_FOUND and partition.get_valid(key) is None:
                pass
            else:
                self.state = FINISHED
                self.set_exception(error)

    def _unlocked_update(self, partition, key):
        value = partition.get_valid(key)
        if value is None:
            return
        try:
            value_json = json.loads(value)
            value_json['mutated'] += 1
            value = json.dumps(value_json)
        except ValueError:
            index = random.choice(range(len(value)))
            value = value[0:index] + random.choice(string.ascii_uppercase) + value[index+1:]

        try:
            self.client.set(key, self.exp, 0, value)
            partition.set(key, value, self.exp)
        except MemcachedError as error:
            self.state = FINISHED
            self.set_exception(error)

    def _unlocked_delete(self, partition, key):
        try:
            self.client.delete(key)
            partition.delete(key)
        except MemcachedError as error:
            if error.status == ERR_NOT_FOUND and partition.get_valid(key) is None:
                pass
            else:
                self.state = FINISHED
                self.set_exception(error)

class LoadDocumentsTask(GenericLoadingTask):
    def __init__(self, server, bucket, generator, kv_store, op_type, exp):
        GenericLoadingTask.__init__(self, server, bucket, kv_store)
        self.generator = generator
        self.op_type = op_type
        self.exp = exp

    def has_next(self):
        return self.generator.has_next()

    def next(self):
        key, value = self.generator.next()
        partition = self.kv_store.acquire_partition(key)
        if self.op_type == 'create':
            self._unlocked_create(partition, key, value)
        elif self.op_type == 'read':
            self._unlocked_read(partition, key)
        elif self.op_type == 'update':
            self._unlocked_update(partition, key)
        elif self.op_type == 'delete':
            self._unlocked_delete(partition, key)
        else:
            self.state = FINISHED
            self.set_exception(Exception("Bad operation type: %s" % self.op_type))
        self.kv_store.release_partition(key)

class WorkloadTask(GenericLoadingTask):
    def __init__(self, server, bucket, kv_store, num_ops, create, read, update, delete, exp):
        GenericLoadingTask.__init__(self, server, bucket, kv_store)
        self.itr = 0
        self.num_ops = num_ops
        self.create = create
        self.read = create + read
        self.update = create + read + update
        self.delete = create + read + update + delete
        self.exp = exp

    def has_next(self):
        if self.num_ops == 0 or self.itr < self.num_ops:
            return True
        return False

    def next(self):
        self.itr += 1
        rand = random.randint(1,  self.delete)
        if rand > 0 and rand <= self.create:
            self._create_random_key()
        elif rand > self.create and rand <= self.read:
            self._get_random_key()
        elif rand > self.read and rand <= self.update:
            self._update_random_key()
        elif rand > self.update and rand <= self.delete:
            self._delete_random_key()

    def _get_random_key(self):
        partition, part_num = self.kv_store.acquire_random_partition()
        if partition is None:
            return

        key = partition.get_random_valid_key()
        if key is None:
            self.kv_store.release_partition(part_num)
            return

        self._unlocked_read(partition, key)
        self.kv_store.release_partition(part_num)

    def _create_random_key(self):
        partition, part_num = self.kv_store.acquire_random_partition(False)
        if partition is None:
            return

        key = partition.get_random_deleted_key()
        if key is None:
            self.kv_store.release_partition(part_num)
            return

        value = partition.get_deleted(key)
        if value is None:
            self.kv_store.release_partition(part_num)
            return

        self._unlocked_create(partition, key, value)
        self.kv_store.release_partition(part_num)

    def _update_random_key(self):
        partition, part_num = self.kv_store.acquire_random_partition()
        if partition is None:
            return

        key = partition.get_random_valid_key()
        if key is None:
            self.kv_store.release_partition(part_num)
            return

        self._unlocked_update(partition, key)
        self.kv_store.release_partition(part_num)

    def _delete_random_key(self):
        partition, part_num = self.kv_store.acquire_random_partition()
        if partition is None:
            return

        key = partition.get_random_valid_key()
        if key is None:
            self.kv_store.release_partition(part_num)
            return

        self._unlocked_delete(partition, key)
        self.kv_store.release_partition(part_num)

class ValidateDataTask(GenericLoadingTask):
    def __init__(self, server, bucket, kv_store):
        GenericLoadingTask.__init__(self, server, bucket, kv_store)
        self.valid_keys, self.deleted_keys = kv_store.key_set()
        self.num_valid_keys = len(self.valid_keys)
        self.num_deleted_keys = len(self.deleted_keys)
        self.itr = 0

    def has_next(self):
        if self.itr < (self.num_valid_keys + self.num_deleted_keys):
            return True
        return False

    def next(self):
        if self.itr < self.num_valid_keys:
            self._check_valid_key(self.valid_keys[self.itr])
        else:
            self._check_deleted_key(self.deleted_keys[self.itr - self.num_valid_keys])
        self.itr += 1

    def _check_valid_key(self, key):
        partition = self.kv_store.acquire_partition(key)

        value = partition.get_valid(key)
        if value is None:
            self.kv_store.release_partition(key)
            return
        value = json.dumps(value)

        try:
            o, c, d = self.client.get(key)
            if d != json.loads(value):
                self.state = FINISHED
                self.set_exception(Exception('Bad result: %s != %s' % (json.dumps(d), value)))
        except MemcachedError as error:
            if error.status == ERR_NOT_FOUND and partition.get_valid(key) is None:
                pass
            else:
                self.state = FINISHED
                self.set_exception(error)
        self.kv_store.release_partition(key)

    def _check_deleted_key(self, key):
        partition = self.kv_store.acquire_partition(key)

        try:
            o, c, d = self.client.delete(key)
            if partition.get_valid(key) is not None:
                self.state = FINISHED
                self.set_exception(Exception('Not Deletes: %s' % (key)))
        except MemcachedError as error:
            if error.status == ERR_NOT_FOUND:
                pass
            else:
                self.state = FINISHED
                self.set_exception(error)
        self.kv_store.release_partition(key)

class ViewCreateTask(Task):
    def __init__(self, server, design_doc_name, view, bucket = "default"):
        Task.__init__(self, "create_view_task")
        self.server = server
        self.bucket = bucket
        self.view = view
        prefix = ("","dev_")[self.view.dev_view]
        self.design_doc_name = prefix + design_doc_name
        self.ddoc_rev_no = 0

    def execute(self, task_manager):

        rest = RestConnection(self.server)

        try:
            # appending view to existing design doc
            content = rest.get_ddoc(self.bucket, self.design_doc_name)
            ddoc = DesignDocument._init_from_json(self.design_doc_name, content)
            ddoc.add_view(self.view)
            self.ddoc_rev_no = self._parse_revision(content['_rev'])

        except ReadDocumentException:
            # creating first view in design doc
            ddoc = DesignDocument(self.design_doc_name, [self.view])

        try:
            rest.create_design_document(self.bucket, ddoc)
            self.state = CHECKING
            task_manager.schedule(self)

        except DesignDocCreationException as e:
            self.state = FINISHED
            self.set_exception(e)

    def check(self, task_manager):
        rest = RestConnection(self.server)

        try:
            query = {"stale" : "ok"}
            content = \
                rest.query_view(self.design_doc_name, self.view.name, self.bucket, query)
            self.log.info("view : {0} was created successfully in ddoc: {1}".format(self.view.name, self.design_doc_name))
            self.state = FINISHED
            if self._check_ddoc_revision():
                self.set_result(self.ddoc_rev_no)
            else:
                self.set_exception(Exception("failed to update design document"))
        except QueryViewException as e:
            task_manager.schedule(self, 2)


    def _check_ddoc_revision(self):
        valid = False
        rest = RestConnection(self.server)

        try:
            content = rest.get_ddoc(self.bucket, self.design_doc_name)
            new_rev_id = self._parse_revision(content['_rev'])
            if new_rev_id != self.ddoc_rev_no:
                self.ddoc_rev_no = new_rev_id
                valid = True
        except ReadDocumentException:
            pass

        return valid

    def _parse_revision(self, rev_string):
        return int(rev_string.split('-')[0])

class ViewDeleteTask(Task):
    def __init__(self, server, design_doc_name, view, bucket = "default"):
        Task.__init__(self, "delete_view_task")
        self.server = server
        self.bucket = bucket
        self.view = view
        prefix = ("","dev_")[self.view.dev_view]
        self.design_doc_name = prefix + design_doc_name

    def execute(self, task_manager):

        rest = RestConnection(self.server)

        try:
            # remove view from existing design doc
            content = rest.get_ddoc(self.bucket, self.design_doc_name)
            ddoc = DesignDocument._init_from_json(self.design_doc_name, content)
            status = ddoc.delete_view(self.view)
            if not status:
                self.state = FINISHED
                self.set_exception(Exception('View does not exist! %s' % (self.view.name)))

            # update design doc
            rest.create_design_document(self.bucket, ddoc)
            self.state = CHECKING
            task_manager.schedule(self, 2)

        except (ValueError, ReadDocumentException, DesignDocCreationException) as e:
            self.state = FINISHED
            self.set_exception(e)

    def check(self, task_manager):
        rest = RestConnection(self.server)

        try:
            # make sure view was deleted
            query = {"stale" : "ok"}
            content = \
                rest.query_view(self.design_doc_name, self.view.name, self.bucket, query)
            self.state = FINISHED
            self.set_result(False)
        except QueryViewException as e:
            self.log.info("view : {0} was successfully deleted in ddoc: {1}".format(self.view.name, self.design_doc_name))
            self.state = FINISHED
            self.set_result(True)

class ViewQueryTask(Task):
    def __init__(self, server, design_doc_name, view_name,
                 query, expected_rows = None,
                 bucket = "default", retry_time = 2):
        Task.__init__(self, "query_view_task")
        self.server = server
        self.bucket = bucket
        self.view_name = view_name
        self.design_doc_name = design_doc_name
        self.query = query
        self.expected_rows = expected_rows
        self.retry_time = retry_time

    def execute(self, task_manager):

        rest = RestConnection(self.server)

        try:
            # make sure view can be queried
            content =\
                rest.query_view(self.design_doc_name, self.view_name, self.bucket, self.query)

            if self.expected_rows is None:
                # no verification
                self.state = FINISHED
                self.set_result(content)
            else:
                self.state = CHECKING
                task_manager.schedule(self)

        except QueryViewException as e:
            # initial query failed, try again
            task_manager.schedule(self, self.retry_time)

    def check(self, task_manager):
        rest = RestConnection(self.server)

        try:
            # query and verify expected num of rows returned
            content = \
                rest.query_view(self.design_doc_name, self.view_name, self.bucket, self.query)
            if content['total_rows'] == self.expected_rows:
                self.state = FINISHED
                self.set_result(content)
            else:
                # retry until expected results or task times out
                task_manager.schedule(self, self.retry_time)

        except QueryViewException as e:
            # subsequent query failed! exit
            self.state = FINISHED
            self.set_exception(e)

