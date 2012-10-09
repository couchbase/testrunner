from __future__ import absolute_import
import uuid
import json
import time
from app.celery import celery
from celery import Task
from celery.task.sets import TaskSet
from rabbit_helper import PersistedMQ
from cache import ObjCacher, CacheHelper
from app.rest_client_tasks import multi_query
from celery.exceptions import TimeoutError

from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)

@celery.task(base = PersistedMQ)
def activeQueryWorkload(workloadMsg):

    rabbitHelper = activeQueryWorkload.rabbitHelper

    prevWorkload = CacheHelper.active_query()
    if prevWorkload is not None:
        prevWorkload.active = False

    workload = QueryWorkload(workloadMsg)
    task_queue = workload.task_queue

    workload.active = True


@celery.task(base = PersistedMQ)
def queryScheduler(batch_size = 100):

    rabbitHelper = queryScheduler.rabbitHelper

    active_query = CacheHelper.active_query()
    if active_query is not None:
        count = int(active_query.qps)
        params = {"stale" : "update_after"}
        multi_query.delay(count,
                          active_query.ddoc,
                          active_query.view,
                          params,
                          active_query.bucket)

class QueryWorkload(object):
    def __init__(self, params):
        self.id = "query_workload_"+str(uuid.uuid4())[:7]
        self.active = False
        self.qps = params["queries_per_sec"]
        self.ddoc = params["ddoc"]
        self.view = params["view"]
        self.bucket = params["bucket"]
        self.password = params["password"]
        self.task_queue = "%s_%s" % (self.bucket, self.id)

    def __setattr__(self, name, value):
        super(QueryWorkload, self).__setattr__(name, value)

        # cache when active key mutated
        if name == 'active':
            ObjCacher().store(CacheHelper.QUERYCACHEKEY, self)

    @staticmethod
    def from_cache(id_):
        return ObjCacher().instance(CacheHelper.QUERYCACHEKEY, id_)

