from __future__ import absolute_import
from app.celery import celery
from celery.task.sets import TaskSet
from celery.task.control import revoke
from datetime import timedelta
from app.stats import StatChecker
import app.sdk_client_tasks as client
import yajl
import uuid
import time
import Queue
from rabbit_helper import PersistedMQ
from celery import current_task
from celery import Task
from celery.task.control import inspect
from threading import Event
from cache import Cache, WorkloadCacher, BucketStatusCacher, TemplateCacher
import random
import testcfg as cfg
from celery.exceptions import TimeoutError
from celery.signals import task_prerun, task_postrun
from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


"""Monitors the workload queue for new messages sent from clients.
When a message is received it is caached and sent to sysTestRunner for processing 
"""
@celery.task(base = PersistedMQ)
def workloadConsumer(workloadQueue = "workload", templateQueue = "workload_template"):

    rabbitHelper = workloadConsumer.rabbitHelper 
    templateMsg = None
    workloadMsg = None

    try:
        templateQueueSize = rabbitHelper.qsize(templateQueue)
        if templateQueueSize > 0:
            templateMsg = rabbitHelper.getMsg(templateQueue)
            templateMsg = yajl.loads(templateMsg)
            logger.error(templateMsg)
            template = Template(templateMsg)
            TemplateCacher().store(template)
    except ValueError as ex:
        logger.error("Error parsing template msg %s: " % templateMsg)
        logger.error(ex)
    except Exception as ex:
        logger.error(ex)


    try:
        workloadQueueSize = rabbitHelper.qsize(workloadQueue)
        if workloadQueueSize > 0:
            workloadMsg = rabbitHelper.getMsg(workloadQueue)
            workloadMsg = yajl.loads(workloadMsg)
            workload = Workload(workloadMsg)
            WorkloadCacher().store(workload)

            # launch workload
            sysTestRunner.delay(workload)
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
@celery.task(base = PersistedMQ)
def sysTestRunner(workload):
   

    bucket = str(workload.bucket)
    latestWorkloadTask = None
    prevWorkload = None

    cache = BucketStatusCacher()
    bucketStatus = cache.bucketstatus(bucket)
    
    if bucketStatus is not None:
        latestWorkloadTask, prevWorkload  = bucketStatus.latestWorkloadTask(bucket)
    else:
        bucketStatus = BucketStatus(bucket)


    # make this the latest taskid against this bucket
    bucketStatus.addTask(bucket, current_task.request.id, workload)
    cache.store(bucketStatus)

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
                WorkloadCacher().store(prevWorkload)

    
    runTask = run.apply_async(args=[workload, prevWorkload], expires = workload.expires)
    return runTask.get()


def task_postrun_handler(sender=None, task_id=None, task=None, args=None, kwargs=None,
                         state = None, signal = None, retval = None):

    rabbitHelper = sysTestRunner.rabbitHelper
    if sender == sysTestRunner:
        # cleanup workload after handled by test runner
        workload = retval 
        rabbitHelper.purge(workload.task_queue)
        WorkloadCacher().delete(workload)

    if sender == client.mset:
        # allow multi set keys to be consumed
        if isinstance(retval,list):
            keys = retval 

            # note template was converted to dict for mset
            template = args[1]  

            if template["cc_queues"] is not None:
                for queue in template["cc_queues"]:
                    queue = str(queue)
                    rabbitHelper.declare(queue)
                    if keys is not None and len(keys) > 0:
                        rabbitHelper.putMsg(queue, yajl.dumps(keys))             
        else:
            logger.error("Error during multi set")
            logger.error(retval)

task_postrun.connect(task_postrun_handler)

"""Generates list of tasks to run based on params passed in to workload
until post conditions(if any) are hit
"""
@celery.task(base = PersistedMQ, name = "run")
def run(workload, prevWorkload = None):
    rabbitHelper = run.rabbitHelper

    cache = WorkloadCacher()
    workload.active = True
    cache.store(workload)

    bucket = str(workload.bucket)
    task_queue = workload.task_queue

    inflight = 0

    while workload.active:
        
        if inflight < 20:

            # read doc template 
            template = TemplateCacher().template(str(workload.template))
            

            if template != None:

                if workload.cc_queues is not None:
                    # override template attribute with workload
                    template.cc_queues = workload.cc_queues

                # read  workload settings
                bucket = workload.bucket
                ops_sec = workload.ops_per_sec

                create_count = int(ops_sec *  workload.create_perc/100)
                update_count = int(ops_sec *  workload.update_perc/100)
                get_count = int(ops_sec *  workload.get_perc/100)
                del_count = int(ops_sec *  workload.del_perc/100)

                consume_queue =  workload.consume_queue


                generate_pending_tasks.delay(task_queue, template, bucket, create_count,
                                              update_count, get_count, del_count, consume_queue)
                inflight = inflight + 1

        else:
            inflight = rabbitHelper.qsize(task_queue)
            time.sleep(1)

        workload = cache.workload(workload.id)

    return workload

def task_prerun_handler(sender=None, task_id=None, task=None, args=None, kwargs=None, signal = None):
    if sender == run:
        workload = args[0]
        prevWorkload = args[1] 
        if workload.preconditions is not None:
            # block tasks against bucket until pre-conditions met
            bucket = str(workload.bucket)
            bs = BucketStatusCacher().bucketstatus(bucket)
            bs.block(bucket)
            BucketStatusCacher().store(bs)

            stat_checker = StatChecker(cfg.COUCHBASE_IP +":"+cfg.COUCHBASE_PORT,
                                       bucket = bucket,
                                       username = cfg.COUCHBASE_USER,
                                       password = cfg.COUCHBASE_PWD)
            while not stat_checker.check(workload.preconditions):
                time.sleep(1)
            prevWorkload.active = False
            WorkloadCacher().store(prevWorkload)
            bs = BucketStatusCacher().bucketstatus(bucket)
            bs.unblock(bucket)
            BucketStatusCacher().store(bs)
            

task_prerun.connect(task_prerun_handler)



"""Retrieve all pending tasks from running workloads and distributes to workers
"""
@celery.task(base = PersistedMQ)
def taskScheduler():

    cache = WorkloadCacher()
    workloads = cache.workloads

    rabbitHelper = taskScheduler.rabbitHelper 

    for workload in workloads:
        if workload.active:    
            task_queue = workload.task_queue
            # dequeue subtasks
            if rabbitHelper.qsize(task_queue) > 0:
                tasks = rabbitHelper.getMsg(task_queue)

                if tasks is not None and len(tasks) > 0:
                    tasks = yajl.loads(tasks)

                    # apply async
                    result = TaskSet(tasks = tasks).apply_async()
                    try:
                        res = result.join(timeout = 1)
                    except TimeoutError:
                        pass


""" scans active workloads for postcondition flags and 
runs checks against bucket stats.  If postcondition
is met, the workload is deactivated and bucket put
back into nonblocking mode
"""
@celery.task(base = PersistedMQ)
def postcondition_handler():

    cache = WorkloadCacher()

    for workload in  cache.workloads:
        if workload.postconditions and workload.active:
            bucket = workload.bucket
            bs = BucketStatusCacher().bucketstatus(bucket)
            bs.block(bucket)
            BucketStatusCacher().store(bs)

            stat_checker = StatChecker(cfg.COUCHBASE_IP +":"+cfg.COUCHBASE_PORT,
                                       bucket = bucket,
                                       username = cfg.COUCHBASE_USER,
                                       password = cfg.COUCHBASE_PWD)
            if stat_checker.check(workload.postconditions):
                bs = BucketStatusCacher().bucketstatus(bucket)
                bs.unblock(bucket)
                BucketStatusCacher().store(bs)
                workload.active = False
                cache.store(workload)


@celery.task(base = PersistedMQ)
def generate_pending_tasks(task_queue, template, bucket, create_count,
                           update_count, get_count, del_count,
                           consume_queue):

    rabbitHelper = generate_pending_tasks.rabbitHelper
    create_tasks , update_tasks , get_tasks , del_tasks = ([],[],[],[])
    if create_count > 0:
        create_tasks = generate_set_tasks(template, create_count, bucket)
    if update_count > 0:
        update_tasks = generate_update_tasks(template, update_count, consume_queue, bucket)
    if get_count > 0:
        get_tasks = generate_get_tasks(get_count, consume_queue, bucket)
    if del_count > 0:
        del_tasks = generate_del_tasks(del_count, consume_queue, bucket)

    pending_tasks = create_tasks + update_tasks + get_tasks + del_tasks 
    pending_tasks = yajl.dumps(pending_tasks)

    rabbitHelper.putMsg(task_queue, pending_tasks)


def _random_string(length):
    return (("%%0%dX" % (length * 2)) % random.getrandbits(length * 8)).encode("ascii")

@celery.task(base = PersistedMQ)
def generate_set_tasks(template, count, bucket = "default", batch_size = 100):

    rabbitHelper = generate_set_tasks.rabbitHelper 

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

        tasks.append(client.mset.s(key_batch, template.__dict__, bucket))
        batch_counter = 0

    return tasks

@celery.task(base = PersistedMQ)
def generate_get_tasks(count, docs_queue, bucket="default", batch_size = 100):
    rabbitHelper = generate_get_tasks.rabbitHelper 

    tasks = [] 

    if batch_size > count:
        batch_size = count

    if rabbitHelper.qsize(docs_queue) > 0:

        keys = rabbitHelper.getMsg(docs_queue, requeue = True)
        keys = yajl.loads(keys)

        # keys are stored in batch but sometimes we need to get more
        while len(keys) < count:
            if rabbitHelper.qsize(docs_queue) == 0: break 
                
            ex_keys = rabbitHelper.getMsg(docs_queue,  requeue = True)
            if ex_keys is not None and len(ex_keys) > 0:
                keys = keys + yajl.loads(ex_keys)
       
        if len(keys) > 0:

            # generate tasks
            i = 0
            while count > 0:
                tasks.append(client.mget.s(keys[i:i+batch_size], bucket))
                i = i + batch_size
                count = count - batch_size 
    else:
        # warning there are no documents to consume
        pass

    return tasks 

    
@celery.task(base = PersistedMQ)
def generate_update_tasks(template, update_count, docs_queue, bucket = "default"):

    rabbitHelper = generate_update_tasks.rabbitHelper 

    tasks = [] 

    if rabbitHelper.qsize(docs_queue) > 0:

        keys = rabbitHelper.getMsg(docs_queue, requeue = True)
        keys = yajl.loads(keys)

        # keys are stored in batch but sometimes we need to get more
        while len(keys) < update_count:

            if rabbitHelper.qsize(docs_queue) == 0: break 

            ex_keys = rabbitHelper.getMsg(docs_queue, requeue = True)
            keys = keys + yajl.loads(ex_keys)
        

        if len(keys) > 0:

            # generate tasks
            val = yajl.dumps(template["kv"])
            tasks = [client.set.s(key, val, bucket) for key in keys[0:update_count]]

    return tasks 



@celery.task(base = PersistedMQ)
def generate_del_tasks(del_count, docs_queue, bucket = "default"):

    rabbitHelper = generate_del_tasks.rabbitHelper 
    tasks = [] 

    if rabbitHelper.qsize(docs_queue) > 0:

        keys = rabbitHelper.getMsg(docs_queue)
        keys = yajl.loads(keys)

        # keys are stored in batch but sometimes we need to get more
        while len(keys) < del_count:

            if rabbitHelper.qsize(docs_queue) == 0: break 

            ex_keys = rabbitHelper.getMsg(docs_queue)
            if ex_keys is not None and len(ex_keys) > 0:
                keys = keys + yajl.loads(ex_keys)
        

        if len(keys) > 0:

            # requeue undeleted keys 
            msg = yajl.dumps(keys[del_count:])
            rabbitHelper.putMsg(docs_queue, msg)

            # generate tasks
            tasks = [client.delete.s(key, bucket) for key in keys[0:del_count]]

    return tasks


class Workload(object):
    def __init__(self, params):
        self.id = "workload_"+str(uuid.uuid4())[:7]
        self.params = params        
        self.bucket = str(params["bucket"])
        self.task_queue = "%s_%s" % (self.bucket, self.id)
        self.template = params["template"]
        self.ops_per_sec = params["ops_per_sec"]
        self.create_perc = int(params["create_perc"])
        self.update_perc = int(params["update_perc"])
        self.del_perc = int(params["del_perc"])
        self.get_perc = int(params["get_perc"])
        self.preconditions = params["preconditions"]
        self.postconditions = params["postconditions"]
        self.active = False 
        self.consume_queue = params["consume_queue"] 
        self.cc_queues = params["cc_queues"]
        self.wait = params["wait"]
        self.expires = params["expires"]

        # consume from cc_queue by default if not specified
        if self.cc_queues != None:
            if self.consume_queue == None:
                self.consume_queue = self.cc_queues[0]

class Template(object):
    def __init__(self, params):
        self.name = params["name"]
        self.ttl = params["ttl"]
        self.flags = params["flags"]
        self.cc_queues = params["cc_queues"]
        self.kv = params["kv"]

class BucketStatus(object):

    def __init__(self, id_):
        self.id = id_ 
        self.history = {}

    def addTask(self, bucket, taskid, workload):
        newPair = (taskid, workload)

        if bucket not in self.history:
            self.history[bucket] = {"tasks" : [newPair]}
        else:
            self.history[bucket]["tasks"].append(newPair)
            
    def latestWorkloadTask(self, bucket):
        taskId = None
        if len(self.history) > 0 and bucket in self.history:
            taskId = self.history[bucket]["tasks"][-1]

        return taskId

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


