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

"""
Monitors the query queue for new messages sent from clients.
When a message is received it is activated for detection
in the queryRunner task
"""
@celery.task(base = PersistedMQ)
def queryConsumer(queryQueue = "query_default"):

    rabbitHelper = queryConsumer.rabbitHelper
    queryQueueSize = rabbitHelper.qsize(queryQueue)

    # for cli retreive currently active query workload
    # since multi-query is not supported here
    active_query = None
    all_queries = CacheHelper.active_queries()
    if len(all_queries) > 0:
        active_query = all_queries[0]

    if queryQueueSize> 0:

        # setup new query workload from queued message
        queryMsg = rabbitHelper.getJsonMsg(queryQueue)
        logger.error(queryMsg)
        try:
            queryWorkload = QueryWorkload(queryMsg)

            # deactivate old query workload
            if active_query is not None:
                active_query.active = False


            # activate new query workload
            # to be detected in queryRunner task
            queryWorkload.active = True

            if 'rcq' in queryMsg:
                rabbitHelper.putMsg(queryMsg['rcq'], "Started Querying: %s/%s" % \
                    (queryWorkload.ddoc, queryWorkload.view))

        except KeyError:
            logger.info("Invalid query workload message: %s" % queryMsg)


"""
Looks for active query workloads in the cache and runs them
"""
@celery.task
def queryRunner():

    # retreive all active query workloads
    queries = CacheHelper.active_queries()
    for query in queries:

        count = int(query.qps)
        params = {"stale" : "update_after"}
        multi_query.delay(count,
                          query.ddoc,
                          query.view,
                          params,
                          query.bucket)


class QueryWorkload(object):
    def __init__(self, params):
        self.id = "query_workload_"+str(uuid.uuid4())[:7]
        self.active = False
        self.qps = params["queries_per_sec"]
        self.ddoc = params["ddoc"]
        self.view = params["view"]
        self.bucket = params.get("bucket")
        self.password = params.get("password")
        self.task_queue = "%s_%s" % (self.bucket, self.id)

    def __setattr__(self, name, value):
        super(QueryWorkload, self).__setattr__(name, value)

        # cache when active key mutated
        if name == 'active':
            ObjCacher().store(CacheHelper.QUERYCACHEKEY, self)

    @staticmethod
    def from_cache(id_):
        return ObjCacher().instance(CacheHelper.QUERYCACHEKEY, id_)

