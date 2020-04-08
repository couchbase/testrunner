
from app.celery import celery
from celery.task.sets import TaskSet
import app.postcondition_handlers as phandler
import app.sdk_client_tasks as client
import json
import uuid
import time
import copy
import re
from rabbit_helper import PersistedMQ, RabbitHelper
from celery import current_task
from celery import Task
from cache import ObjCacher, CacheHelper
from app.query import updateQueryBuilders
from app.rest_client_tasks import create_rest, http_ping
import random
import testcfg as cfg
from celery.exceptions import TimeoutError
from celery.signals import task_postrun
from celery.utils.log import get_task_logger
from membase.helper.cluster_helper import ClusterOperationHelper

logger = get_task_logger(__name__)


EXCHANGE = cfg.CB_CLUSTER_TAG+"consumers"
if cfg.SERIESLY_IP != '':
    from seriesly import Seriesly



"""Monitors the workload queue for new messages sent from clients.
When a message is received it is caached and sent to sysTestRunner for processing
"""
@celery.task(base = PersistedMQ, ignore_result = True)
def workloadConsumer(workloadQueue = "workload_default", templateQueue = "workload_template_default"):


    rabbitHelper = workloadConsumer.rabbitHelper

    templateMsg = None
    workloadMsg = None

    try:
        templateQueueSize = rabbitHelper.qsize(templateQueue)
        if templateQueueSize > 0:
            templateMsg = rabbitHelper.getJsonMsg(templateQueue)
            try:
                template = Template(templateMsg)
                if 'rcq' in templateMsg:
                    rabbitHelper.putMsg(templateMsg['rcq'], "Stored Template: "+template.name)
            except KeyError:
                logger.info("Ignoring malformated msg: %s" % templateMsg)

    except ValueError as ex:
        logger.error("Error parsing template msg %s: " % templateMsg)
        logger.error(ex)
    except Exception as ex:
        logger.error(ex)


    try:
        workloadQueueSize = rabbitHelper.qsize(workloadQueue)
        if workloadQueueSize > 0:
            workloadMsg = rabbitHelper.getJsonMsg(workloadQueue)
            try:
                workload = Workload(workloadMsg)
                # launch kvworkload
                sysTestRunner.delay(workload)
                if 'rcq' in workloadMsg:
                    rabbitHelper.putMsg(workloadMsg['rcq'], "Started workload id: "+workload.id)
            except KeyError:
                logger.info("Ignoring malformated msg: %s" % workloadMsg)

    except ValueError as ex:
        logger.error("Error parsing workloadMsg %s: " % workloadMsg)
        logger.error(ex)
    except Exception as ex:
        logger.error(ex)




"""Runs the provided workload against configured bucket.  If previous workload has
postcondition dependencies then bucket will be set to blocking mode, meaning workloads
cannot overwrite each other.  Note, if postcondition of previous workload never
finishes you will have to manually kill task via cbsystest script.
"""
@celery.task(ignore_result = True)
def sysTestRunner(workload):


    bucket = str(workload.bucket)
    prevWorkload = None

    bucketStatus = BucketStatus.from_cache(bucket)

    if bucketStatus is not None:
        prevWorkload = bucketStatus.latestWorkload(bucket)
    else:
        bucketStatus = BucketStatus(bucket)


    # make this the latest taskid against this bucket
    bucketStatus.addTask(bucket, current_task.request.id, workload)

    if workload.wait is not None:
        # wait before processing
        time.sleep(workload.wait)

    if bucketStatus.mode(bucket) == "blocking":
        while Cache().retrieve(prevWorkload.id) is not None:
                time.sleep(2)

    elif bucketStatus.mode(bucket) == "nonblocking":
        if prevWorkload is not None:
            # disable previously running
            # workload if bucket in nonblocking mode.
            # if current workload has no preconditions
            # it's not allowed to override previous workload
            if workload.preconditions is None:
                prevWorkload.active = False

    # print out workload params
    logger.error(workload.params)

   #DEPRECIATED
   #if workload.miss_perc > 0:
   #     setupCacheMissQueues(workload)


    run(workload)

def run(workload):

    workload.active = True
    rabbitHelper = RabbitHelper()
    sdk_queue_key = "sdk_consumer.*"

    # read doc template
    template = Template.from_cache(str(workload.template))
    if template is None:
        logger.error("no doc template imported")
        return

    consumer_template = copy.deepcopy(template)
    bucket = str(workload.bucket)
    password = str(workload.password)

    active_hosts = None
    clusterStatus = CacheHelper.clusterstatus(cfg.CB_CLUSTER_TAG+"_status")
    if clusterStatus is not None:
        active_hosts = clusterStatus.get_all_hosts()


    if workload.cc_queues is not None:
        # override template attribute with workload
        consumer_template.cc_queues = workload.cc_queues

    if len(workload.indexed_keys) > 0:
        template.indexed_keys = workload.indexed_keys


    ops_sec = workload.ops_per_sec

    # modify ops by number of consumers
    num_consumers = rabbitHelper.numExchangeQueues(cfg.CB_CLUSTER_TAG, EXCHANGE)

    if num_consumers == 0:
        logger.error("No sdkclients running")
        return

    ops_sec = int(ops_sec)/num_consumers
    create_count = int(ops_sec *  workload.create_perc/100)
    update_count = int(ops_sec *  workload.update_perc/100)
    get_count = int(ops_sec *  workload.get_perc/100)
    del_count = int(ops_sec *  workload.del_perc/100)
    exp_count = int(ops_sec *  workload.exp_perc/100)
    consume_queue =  workload.consume_queue

    ttl = workload.ttl
    miss_queue = workload.miss_queue
    miss_perc = workload.miss_perc

    # broadcast to sdk_consumers
    msg = {'bucket' : bucket,
           'id' : workload.id,
           'password' : password,
           'template' : consumer_template.__dict__,
           'ops_sec' : ops_sec,
           'create_count' : create_count,
           'update_count' : update_count,
           'get_count' : get_count,
           'del_count' : del_count,
           'exp_count' : exp_count,
           'consume_queue' : consume_queue,
           'ttl' : ttl,
           'miss_perc' : miss_perc,
           'active' : True,
           'active_hosts' : active_hosts}

    rabbitHelper.putMsg('', json.dumps(msg), EXCHANGE)
    logger.error("start task sent to %s consumers" % num_consumers)



@celery.task(base = PersistedMQ, ignore_result = True)
def task_postrun_handler(sender=None, task_id=None, task=None, args=None, kwargs=None,
                         state = None, signal = None, retval = None):

    rabbitHelper = task_postrun_handler.rabbitHelper
#
#   TODO: do in garbage collection task
#   if sender == run:
#       # cleanup workload after handled by test runner
#       if isinstance(retval, Workload):
#           workload = retval
#           try:
#               rabbitHelper.delete(workload.task_queue)
#           except:
#               pass # queue already deleted
#       else:
#           logger.error(retval)
#
    if sender == client.mset:

        if isinstance(retval, tuple):
            isupdate = args[3]
            if isupdate == False:

                # allow multi set keys to be consumed
                keys = retval[0]
                template = retval[1]
                bucket = args[2]

                indexed_keys = template['indexed_keys']

                if len(indexed_keys) > 0:
                   if keys is not None and len(keys) > 0:
                       updateQueryBuilders.apply_async(args=[template, bucket, keys[0]])

                # put created item into specified cc_queues (if specified)
                # and item is not set to expire
                if template["cc_queues"] is not None and template["ttl"] == 0:
                    for queue in template["cc_queues"]:
                        queue = str(queue)
                        rabbitHelper.declare(queue)
                        if keys is not None and len(keys) > 0:
                            rabbitHelper.putMsg(queue, json.dumps(keys))
        else:
            logger.error("Error during multi set")

task_postrun.connect(task_postrun_handler, weak=False)

"""
Generates list of tasks to run based on params passed in to workload
"""
@celery.task(base = PersistedMQ)
def queue_op_cycles(workload):


    # read doc template
    template = Template.from_cache(str(workload.template))
    if template is None:
        logger.error("no doc template imported")
        return

    rabbitHelper = queue_op_cycles.rabbitHelper
    bucket = str(workload.bucket)
    task_queue = workload.task_queue

    active_hosts = None
    clusterStatus = CacheHelper.clusterstatus(cfg.CB_CLUSTER_TAG+"_status")
    if clusterStatus is not None:
        active_hosts = clusterStatus.get_all_hosts()

    # create 30 op cycles
    for i in range(20):

        if workload.cc_queues is not None:
            # override template attribute with workload
            template.cc_queues = workload.cc_queues

        if len(workload.indexed_keys) > 0:
            template.indexed_keys = workload.indexed_keys

        # read  workload settings
        bucketInfo = {"bucket" : workload.bucket,
                      "password" : workload.password}

        ops_sec = workload.ops_per_sec

        create_count = int(ops_sec *  workload.create_perc/100)
        update_count = int(ops_sec *  workload.update_perc/100)
        get_count = int(ops_sec *  workload.get_perc/100)
        del_count = int(ops_sec *  workload.del_perc/100)
        exp_count = int(ops_sec *  workload.exp_perc/100)
        consume_queue =  workload.consume_queue

        ttl = workload.ttl
        miss_queue = workload.miss_queue
        miss_perc = workload.miss_perc

        generate_pending_tasks(task_queue, template, bucketInfo, create_count,
                               update_count, get_count, del_count, exp_count,
                               consume_queue, ttl, miss_perc, miss_queue, active_hosts)


def setupCacheMissQueues(workload):
    """ assuming misses will come from keys in
        consume_queue or cc_queue.
        so make location where keys were going
        to be read the miss queue and set
        consume queue to location where new keys are
        being generated.

        Only required that at least a cc_queue
        with miss items is provided"""

    # make another cc_queue to put only hot items
    new_cc_queue = None
    if workload.cc_queues is None:

        new_cc_queue = workload.id + "__hot__"
        workload.cc_queues = [new_cc_queue]

        # delete new cc_queue if it exists
        try:
            rabbitHelper.delete(new_cc_queue)
        except:
            pass # queue already deleted
    else:
        new_cc_queue = workload.cc_queues[0]


    # move old consume queue to miss queue
    if workload.consume_queue is not None:
        workload.miss_queue = workload.consume_queue

    # make new cc_queue the consume queue
    workload.consume_queue = new_cc_queue

    # save changes
    ObjCacher().store(CacheHelper.WORKLOADCACHEKEY, workload)

@celery.task(ignore_result = True)
def task_prerun_handler(workload, prevWorkload):

    # WARNING PRECONDITIONS ARE DEPRECIATED
    # this method is used to disable a previously running workload
    if workload.preconditions is not None:

        # block tasks against bucket until pre-conditions met
        bs = BucketStatus.from_cache(bucket)
        bs.block(bucket)


        while not stat_checker.check(workload.preconditions):
            time.sleep(1)

        if prevWorkload is not None:
            prevWorkload.active = False

        bs = BucketStatus.from_cache(bucket)
        bs.unblock(bucket)




"""Retrieve all pending tasks from running workloads and distributes to workers
"""
@celery.task(base = PersistedMQ, ignore_result = True)
def taskScheduler():
    pass

   #workloads = CacheHelper.workloads()

   #rabbitHelper = taskScheduler.rabbitHelper
   #tasks = []

   #for workload in workloads:
   #    if workload.active:

   #        task_queue = workload.task_queue
   #        num_ready_tasks = rabbitHelper.qsize(task_queue)
   #        # dequeue subtasks
   #        if num_ready_tasks > 0:
   #            tasks = rabbitHelper.getJsonMsg(task_queue)
   #            if tasks is not None and len(tasks) > 0:

   #                # apply async
   #                result = TaskSet(tasks = tasks).apply_async()


   #        # check if more subtasks need to be queued
   #        if num_ready_tasks < 10:
   #            queue_op_cycles.delay(workload)


""" scans active workloads for postcondition flags and
runs checks against bucket stats.  If postcondition
is met, the workload is deactivated and bucket put
back into nonblocking mode
"""
@celery.task(ignore_result = True)
def postcondition_handler():

    workloads = CacheHelper.workloads()
    for workload in workloads:
        if workload.postcondition_handler and workload.active:
            bucket = workload.bucket
            bs = BucketStatus.from_cache(bucket)
            bs.block(bucket)
            status = True

            try:
                postcondition_handler = \
                    getattr(phandler,
                            workload.postcondition_handler)

                status = postcondition_handler(workload)

            except AttributeError:
                logger.error("Postcondition method %s doesn't exist" \
                             % workload.postcondition_handler)
                workload.postcondition = None
                workload.postcondition_handler = None


            if status == True:
                # unblock bucket and deactivate workload
                bs = BucketStatus.from_cache(bucket)
                bs.unblock(bucket)
                workload.active = False

@celery.task(base = PersistedMQ, ignore_result = True)
def generate_pending_tasks(task_queue, template, bucketInfo, create_count,
                           update_count, get_count, del_count,
                           exp_count, consume_queue, ttl = 0,
                           miss_perc = 0, miss_queue = None, active_hosts = None):

    rabbitHelper = generate_pending_tasks.rabbitHelper
    bucket = bucketInfo['bucket']
    password = bucketInfo['password']

    create_tasks, update_tasks, get_tasks, del_tasks = ([], [], [], [])
    if create_count > 0:
        set_template = copy.deepcopy(template)
        set_template.ttl = 0 # override template level ttl
        #rabbitHelper.putMsg('gvset', str(create_count))
        create_tasks = generate_set_tasks(set_template, create_count, bucket, password = password, hosts = active_hosts)

    if update_count > 0:

        update_tasks = generate_update_tasks(template, update_count, consume_queue, bucket, password = password, hosts = active_hosts)

    if get_count > 0:

        if miss_queue is not None:
            # generate miss tasks
            miss_count = int(miss_perc/float(100) * get_count)
            get_tasks = generate_get_tasks(miss_count, miss_queue, bucket, password = password, hosts = active_hosts)
            get_count = get_count - miss_count

        get_tasks = get_tasks + generate_get_tasks(get_count, consume_queue, bucket, password = password, hosts = active_hosts)

    if del_count > 0:

        del_tasks = generate_delete_tasks(del_count, consume_queue, bucket, password = password, hosts = active_hosts)

    if exp_count > 0:
        # set ttl from workload level ttl
        # otherwise template level ttl will be used
        exp_template = copy.deepcopy(template)
        if ttl > 0:
            exp_template.ttl = ttl
        create_tasks = create_tasks + \
            generate_set_tasks(exp_template, exp_count, bucket, password = password, hosts = active_hosts)

    pending_tasks = create_tasks + update_tasks + get_tasks + del_tasks
    pending_tasks = json.dumps(pending_tasks)
    #rabbitHelper.putMsg(task_queue, pending_tasks)

    rabbitHelper.putMsg('gvset', pending_tasks)

def _random_string(length):
    return (("%%0%dX" % (length * 2)) % random.getrandbits(length * 8)).encode("ascii")

def generate_set_tasks(template, count, bucket = "default", password = "", hosts = None, batch_size = 1000):


    if batch_size > count:
        batch_size = count

    tasks = []
    batch_counter = 0
    i = 0
    while i < count:

        key_batch = []
        end_cursor = i + batch_size
        if end_cursor > count:
            batch_size = count - i

        while batch_counter < batch_size:
            # create doc keys
            key = _random_string(12)
            key_batch.append(key)
            batch_counter = batch_counter + 1
            i = i + 1

        tasks.append(client.mset.s(key_batch, template.__dict__, bucket, False, password, hosts))
        batch_counter = 0

    return tasks

@celery.task(base = PersistedMQ, ignore_result = True)
def generate_get_tasks(count, docs_queue, bucket="default", password = "", hosts = None):

    rabbitHelper = generate_get_tasks.rabbitHelper

    tasks = []
    keys_retrieved = 0

    while keys_retrieved < count:

        if rabbitHelper.qsize(docs_queue) == 0:
            msg = ("%s keys retrieved, Requested %s ") % (keys_retrieved, count)
            logger.info(msg)
            break

        keys = rabbitHelper.getJsonMsg(docs_queue, requeue = True)
        keys_retrieved = keys_retrieved + len(keys)

        if len(keys) > 0:
            if keys_retrieved > count:
                end_idx = keys_retrieved - count
                keys = keys[:-end_idx]
            tasks.append(client.mget.s(keys, bucket, password, hosts))

    return tasks


@celery.task(base = PersistedMQ, ignore_result = True)
def generate_update_tasks(template, count, docs_queue, bucket = "default", password = "", hosts = None):

    rabbitHelper = generate_update_tasks.rabbitHelper
    val = json.dumps(template.kv)

    tasks = []
    keys_updated = 0

    while keys_updated < count:
        if rabbitHelper.qsize(docs_queue) == 0:
            msg = ("Error: %s keys updated, Requested %s ") % (keys_updated, count)
            logger.info(msg)
            break

        keys = rabbitHelper.getJsonMsg(docs_queue, requeue = True)
        keys_updated = keys_updated + len(keys)

        if len(keys) > 0:
            if keys_updated > count:
                end_idx = keys_updated - count
                keys = keys[:-end_idx]
            tasks.append(client.mset.s(keys, template.__dict__, bucket, True, password = "", hosts = hosts))

    return tasks


@celery.task(base = PersistedMQ, ignore_result = True)
def generate_delete_tasks(count, docs_queue, bucket = "default", password = "", hosts = None):


    rabbitHelper = generate_delete_tasks.rabbitHelper

    tasks = []
    keys_deleted = 0

    while keys_deleted < count:

        if rabbitHelper.qsize(docs_queue) == 0:
            msg = ("%s keys deleted, Requested %s ") % (keys_deleted, count)
            logger.info(msg)
            break

        keys = rabbitHelper.getJsonMsg(docs_queue)
        keys_deleted = keys_deleted + len(keys)

        if len(keys) > 0:
            if keys_deleted > count:
                end_idx = keys_deleted - count
                keys = keys[:-end_idx]
            tasks.append(client.mdelete.s(keys, bucket, password, hosts = hosts))


    return tasks

@celery.task(base = PersistedMQ, ignore_result = True)
def report_kv_latency(bucket = "default"):

    if cfg.SERIESLY_IP == '':
        # seriesly not configured
        return

    rabbitHelper = report_kv_latency.rabbitHelper
    clusterStatus = CacheHelper.clusterstatus(cfg.CB_CLUSTER_TAG+"_status") or\
        ClusterStatus()

    host = clusterStatus.get_random_host()
    if host is None: return

    ip, port = host.split(':')

    workloads = CacheHelper.workloads()
    for workload in workloads:
        if workload.active and workload.bucket == bucket:

            # read workload params
            bucket = str(workload.bucket)
            password = str(workload.password)

            # read template from active workload
            template = Template.from_cache(str(workload.template))
            template = template.__dict__
            client.decodeMajgicStrings(template)

            # setup key/val to use for timing
            key = _random_string(12)
            value = json.dumps(template['kv'])
            get_key = key


            # for get op, try to pull from consume_queue
            # so that we can calc impact of dgm
            consume_queue = workload.consume_queue
            if consume_queue is not None:
                keys = rabbitHelper.getJsonMsg(str(consume_queue), requeue = True)
                if len(keys) > 0:
                    get_key = str(keys['start'])

            # collect op latency
            set_latency = client.mc_op_latency('set', key, value, ip, port, bucket, password)
            get_latency = client.mc_op_latency('get', get_key, value, ip, port, bucket, password)
            delete_latency = client.mc_op_latency('delete', key, value, ip, port, bucket, password)


            # report to seriessly
            seriesly = Seriesly(cfg.SERIESLY_IP, 3133)
            db = None
            if 'fast' in seriesly.list_dbs():
                db='fast'
            else:
                bucketStatus = BucketStatus.from_cache(bucket) or BucketStatus(bucket)
                db = bucketStatus.latency_db
                if db not in seriesly.list_dbs():
                    seriesly.create_db(db)

            if db is not None:
                seriesly[db].append({'set_latency' : set_latency,
                                     'get_latency' : get_latency,
                                     'delete_latency' : delete_latency})



@celery.task(base = PersistedMQ, ignore_result = True)
def kv_ops_manager(max_msgs = 1000):

    rabbitHelper = kv_ops_manager.rabbitHelper

    isovercommited = False

    get_q = "get_"+cfg.CB_CLUSTER_TAG
    set_q = "set_"+cfg.CB_CLUSTER_TAG
    del_q = "delete_"+cfg.CB_CLUSTER_TAG
    kv_queues = [get_q, set_q, del_q]


    # check set/get/delete queues
    for queue in kv_queues:
        if rabbitHelper.qsize(queue) > max_msgs:
            # purge tasks in this queue
            rabbitHelper.purge(queue)
            isovercommited = True

    if isovercommited:
        throttle_kv_ops()

def throttle_kv_ops(isovercommited=True):

    rabbitHelper = kv_ops_manager.rabbitHelper

    workloads = CacheHelper.workloads()
    for workload in workloads:
       if workload.active:
           if isovercommited:
               # clear pending task_queue
               rabbitHelper.purge(workload.task_queue)

               # reduce ops by 10%
               new_ops_per_sec = workload.ops_per_sec*0.90
               if new_ops_per_sec > 5000:
                   workload.ops_per_sec = workload.ops_per_sec*0.90
                   logger.error("Cluster Overcommited: reduced ops to (%s)" % workload.ops_per_sec)


@celery.task(ignore_result = True)
def updateClusterStatus(ignore_result = True):

    done = False

    clusterStatus = CacheHelper.clusterstatus(cfg.CB_CLUSTER_TAG+"_status") or\
        ClusterStatus()

    # check cluster nodes
    cached_nodes = clusterStatus.nodes
    new_cached_nodes = []

    for node in cached_nodes:

        # get an active node
        if clusterStatus.http_ping_node(node) is not None:

            # get remaining nodes
            active_nodes = clusterStatus.get_cluster_nodes(node)

            # populate cache with healthy nodes
            for active_node in active_nodes:
                if active_node.status == 'healthy':
                    new_cached_nodes.append(active_node)

            break



    if len(new_cached_nodes) > 0:

        # check for update
        new_node_list = ["%s:%s" % (n.ip, n.port) for n in new_cached_nodes]

        if len(new_node_list) != len(cached_nodes) or\
            len(set(clusterStatus.get_all_hosts()).intersection(new_node_list)) !=\
                len(cached_nodes):
            clusterStatus.nodes = new_cached_nodes
            clusterStatus.update_orchestrator()
    else:
        clusterStatus.orchestrator = None
        ObjCacher().delete(CacheHelper.CLUSTERSTATUSKEY, clusterStatus)

def getClusterStat(bucket, stat):

    val = 0
    clusterStatus = CacheHelper.clusterstatus(cfg.CB_CLUSTER_TAG+"_status") or\
        ClusterStatus()
    host = clusterStatus.get_random_host()
    stat_checker = phandler.BucketStatChecker(bucket, addr = host)
    stats = stat_checker.get_stats()
    if len(stats) > 0:
        if stat in stats:
            val = stats[stat]

    return val

def replace_magic_vars(str_):
    ref = re.match(r".*\$(?P<var>\w+)", str_).group('var')
    ref = str(ref.strip())
    value = CacheHelper.getPhaseVar(ref) or 0
    str_  = str_.replace("$"+ref, str(value))
    return str_

"""
" object used to keep track of active nodes in a cluster
"""
class ClusterStatus(object):
    def __init__(self, id = cfg.CB_CLUSTER_TAG+"_status"):
        self.initialized = False

        self.id = id
        self.orchestrator = None
        self.nodes = self.get_cluster_nodes() or []
        self.all_available_hosts = self.get_available_hosts() or []
        self.rebalancing = False

        if len(self.nodes) > 0:
            self.update_orchestrator()

        self.initialized = True

    def get_all_hosts(self):
        return ["%s:%s" % (node.ip, node.port) for node in self.nodes]

    def get_available_hosts(self):
        if len(cfg.CLUSTER_IPS) > 0:
            return ["%s:%s" % (ip.split(':')[0], ip.split(':')[1] if len(ip.split(':'))>1
                               else cfg.COUCHBASE_PORT) for ip in cfg.CLUSTER_IPS]
        return []

    def get_random_host(self):
        all_hosts = self.get_all_hosts()
        if len(all_hosts) > 0:
            return all_hosts[random.randint(0, len(all_hosts) - 1)]

    def http_ping_node(self, node = None):
        if node:
            ip, port = node.ip, node.port
        else:
            ip, port = self.orchestrator.ip, self.orchestrator.port

        return http_ping(ip, port)

    def get_cluster_nodes(self, node = None):
        rest = self.node_rest(node)
        if rest is not None:
            return rest.node_statuses()

    def update_orchestrator(self, ref_node = None, retry = 5):

        if len(self.nodes) > 0:

            if ref_node is None:
                ref_node = self.nodes[0]

            address = {'server_ip' : ref_node.ip, 'port' : ref_node.port}
            rest = create_rest(**address)

            status, content = ClusterOperationHelper.find_orchestrator_with_rest(rest)

            if status == True:
                content = re.sub(r".*@", "", content).strip("'").split(':')
                orchestrator_ip, orchestrator_port = \
                    content[0], content[1] if len(content) > 1 else cfg.COUCHBASE_PORT

                # look up matching node in self nodes
                for node in self.nodes:
                    if node.ip == orchestrator_ip and \
                        int(node.port) == int(orchestrator_port):
                            self.orchestrator = node
                            break
            elif retry > 0:
               # wait
               time.sleep(5)

               # select random node and retry
               ref_node = self.nodes[random.randint(0, len(self.nodes) - 1)]
               retry = retry - 1
               return self.update_orchestrator(ref_node, retry)


    def node_rest(self, node = None):
        rest = None
        args = {'username' : cfg.COUCHBASE_USER,
                'password' : cfg.COUCHBASE_PWD}

        if self.orchestrator is None:
            ip, port = cfg.COUCHBASE_IP, cfg.COUCHBASE_PORT
        elif node is None:
            ip, port = self.orchestrator.ip, self.orchestrator.port
        else:
            ip, port = node.ip, node.port

        args['server_ip'] = ip
        args['port'] = port

        rest = create_rest(**args)

        return rest

    def __setattr__(self, name, value):

        # auto cache changes made to this object
        super(ClusterStatus, self).__setattr__(name, value)
        if self.initialized:
            ObjCacher().store(CacheHelper.CLUSTERSTATUSKEY, self)

class Workload(object):
    AUTOCACHEKEYS = ['active',
                     'ops_per_sec',
                     'postconditions',
                     'postcondition_handler',
                     'initialized']

    def __init__(self, params):
        self.initialized = False
        self.id = "workload_"+str(uuid.uuid4())[:7]
        self.params = params
        self.bucket = str(params["bucket"])
        self.password = str(params["password"])
        self.task_queue = "%s_%s" % (self.bucket, self.id)
        self.template = params["template"]
        self.ops_per_sec = params["ops_per_sec"]
        self.create_perc = int(params["create_perc"])
        self.update_perc = int(params["update_perc"])
        self.del_perc = int(params["del_perc"])
        self.get_perc = int(params["get_perc"])
        self.exp_perc = params["exp_perc"]
        self.miss_perc = params["miss_perc"]
        self.preconditions = params["preconditions"]
        self.postconditions = params["postconditions"]
        self.postcondition_handler = None
        self.active = False
        self.consume_queue = params["consume_queue"]
        self.cc_queues = params["cc_queues"]
        self.miss_queue = None # internal use only
        self.wait = params["wait"]
        self.ttl = int(params["ttl"])
        self.indexed_keys = []

        # consume from cc_queue by default if not specified
        if self.cc_queues != None:
            if self.consume_queue == None:
                self.consume_queue = self.cc_queues[0]

        # setup postcondition handler
        self.setupPostconditionHandler()
        self.initialized = True

    def setupPostconditionHandler(self):

        if self.postconditions:

            stat, cmp_type, value = \
                phandler.default_condition_params(self.postconditions)

            if stat == 'count':
                stat_checker = phandler.BucketStatChecker(self.bucket)
                curr_items = stat_checker.get_curr_items()
                value = int(value) + int(curr_items)
                self.postconditions = "curr_items >= %s" % value

            # setup postcondition hander
            self.postcondition_handler =\
                phandler.getPostConditionMethod(self)


    @staticmethod
    def defaultSpec():
        return {'update_perc': 0,
                'postconditions': None,
                'postcondition_handler': None,
                'del_perc': 0,
                'create_perc': 0,
                'exp_perc': 0,
                'miss_perc': 0,
                'ttl': 15,
                'bucket': 'default',
                'password': '',
                'ops_per_sec': 0,
                'consume_queue': None,
                'preconditions': None,
                'template': 'default',
                'cc_queues': None,
                'miss_queue': None,
                'get_perc': 0,
                'wait': None,
                'indexed_keys' : []}


    def updateIndexKeys(self, key):

        template = Template.from_cache(str(self.template))

        # update workload with information about which keys being index
        if key is not None:

            # when indexed key does not exist in kv pair do not update
            if key in template.kv:

                # do not update if we are already traking index key
                if key not in self.indexed_keys:

                    # update and cache workload object
                    self.indexed_keys.append(key)
                    ObjCacher().store(CacheHelper.WORKLOADCACHEKEY, self)
            else:
                logger.error("key: '%s' does not exist in kvpair.  Smart querying disabled" % key)


    def __setattr__(self, name, value):
        super(Workload, self).__setattr__(name, value)

        # auto cache workload when certain attributes change
        # if object has been fully setup
        if name in Workload.AUTOCACHEKEYS and self.initialized:
            ObjCacher().store(CacheHelper.WORKLOADCACHEKEY, self)

        # check if workload is being deactivated
        if name == "active" and self.active == False and self.initialized:
            msg = {'active' : False,
                   'id' : self.id}
            RabbitHelper().putMsg('', json.dumps(msg), EXCHANGE)
            logger.error("kill task %s" % self.id)

            #self.requeueNonDeletedKeys()

    def requeueNonDeletedKeys(self):
        rabbitHelper = RabbitHelper()
        task_type = 'app.sdk_client_tasks.mdelete'

        # requeue pending delete keys so that they may be deleted in another workload
        while rabbitHelper.qsize(self.task_queue) > 0:
            task_set = rabbitHelper.getJsonMsg(self.task_queue)

            if len(task_set) > 0:
                keys = [task['args'] for task in task_set \
                            if task['task'] == task_type]
                if len(keys) > 0:
                    # put back on to consume_queue
                    msg = json.dumps(keys[0][0])
                    rabbitHelper.putMsg(self.consume_queue, msg)

        try:
            # delete task queue
            rabbitHelper.delete(self.task_queue)

            # delete consume queue if it was a miss_queue
            if self.miss_queue is not None and self.consume_queue is not None:
                rabbitHelper.delete(self.consume_queue)
        except:
            pass


    @staticmethod
    def from_cache(id_):
        return ObjCacher().instance(CacheHelper.WORKLOADCACHEKEY, id_)

class Template(object):
    def __init__(self, params):
        logger.error(params)
        self.name = params["name"]
        self.id = self.name
        self.ttl = params["ttl"]
        self.flags = params["flags"]
        self.cc_queues = params["cc_queues"]
        self.kv = params["kv"]
        self.size = params.get("size") or ['128']
        self.size = list(map(int, self.size))
        self.indexed_keys = []

        # cache
        ObjCacher().store(CacheHelper.TEMPLATECACHEKEY, self)

    @staticmethod
    def from_cache(id_):
        return ObjCacher().instance(CacheHelper.TEMPLATECACHEKEY, id_)

class BucketStatus(object):

    def __init__(self, id_):
        self.id = id_
        self.history = {}
        self.latency_db = id_ + "latency"

    def addTask(self, bucket, taskid, workload):
        newPair = (taskid, workload)
        if bucket not in self.history:
            self.history[bucket] = {"tasks" : [newPair]}
        else:
            self.history[bucket]["tasks"].append(newPair)
        ObjCacher().store(CacheHelper.BUCKETSTATUSCACHEKEY, self)

    def latestWorkload(self, bucket):
        workload = None
        if len(self.history) > 0 and bucket in self.history:
            taskId, workload = self.history[bucket]["tasks"][-1]

        # retrieve latest workload in case it has changed since last cached
        workload = Workload.from_cache(workload.id)
        return workload

    def mode(self, bucket):
        mode = "nonblocking"
        if "mode" in self.history[bucket]:
            mode = self.history[bucket]["mode"]
        return mode

    def block(self, bucket):
        self._set_mode(bucket, "blocking")

    def unblock(self, bucket):
        self._set_mode(bucket, "nonblocking")

    def _set_mode(self, bucket, mode):
        self.history[bucket]["mode"] = mode


    def __setattr__(self, name, value):
        super(BucketStatus, self).__setattr__(name, value)
        ObjCacher().store(CacheHelper.BUCKETSTATUSCACHEKEY, self)

    @staticmethod
    def from_cache(id_):
        return ObjCacher().instance(CacheHelper.BUCKETSTATUSCACHEKEY, id_)
