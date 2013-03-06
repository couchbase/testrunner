from __future__ import absolute_import
import uuid
import json
import time
import urllib
from app.celery import celery
from celery import Task
from celery.task.sets import TaskSet
from rabbit_helper import PersistedMQ
from cache import ObjCacher, CacheHelper
from app.rest_client_tasks import multi_query
from app.sdk_client_tasks import _int_float_str_gen
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
        filters = list(set(query.include_filters) -\
                       set(query.exclude_filters))
        params = generateQueryParams(query.template,
                                     query.bucket,
                                     filters,
                                     query.limit,
                                     query.startkey,
                                     query.endkey,
                                     query.startkey_docid,
                                     query.endkey_docid)
        multi_query.delay(count,
                          query.ddoc,
                          query.view,
                          params,
                          query.bucket,
                          query.password)


class QueryWorkload(object):
    def __init__(self, params):
        self.id = "query_workload_"+str(uuid.uuid4())[:7]
        self.template = params["template"]
        self.active = False
        self.qps = params["queries_per_sec"]
        self.ddoc = params["ddoc"]
        self.view = params["view"]
        self.bucket = params.get("bucket")
        self.password = params.get("password")
        self.include_filters =\
            self.uniq_include_filters(params.get("include_filters"))
        self.exclude_filters = params.get("exclude_filters")
        self.startkey = params.get("startkey")
        self.endkey = params.get("endkey")
        self.startkey_docid = params.get("startkey_docid")
        self.endkey_docid = params.get("endkey_docid")
        self.limit = params.get("limit")
        self.task_queue = "%s_%s" % (self.bucket, self.id)

    @staticmethod
    def defaultSpec():
        return {"template" : "default",
                "queries_per_sec" : 0,
                "ddoc" : None,
                "view" : None,
                "bucket" : "default",
                "password" : None,
                "include_filters" : ["startkey", "endkey", "limit"],
                "exclude_filters" : [],
                "startkey" : None,
                "endkey" : None,
                "startkey_docid" : None,
                "endkey_docid" : None,
                "limit" : 10}

    def uniq_include_filters(self, ex_filters):
        include = ["startkey","endkey","limit"]
        [include.append(f_) for f_ in ex_filters]
        logger.error(include)
        include = list(set(include))
        return include

    def __setattr__(self, name, value):
        super(QueryWorkload, self).__setattr__(name, value)

        # cache when active key mutated
        if name == 'active':
            ObjCacher().store(CacheHelper.QUERYCACHEKEY, self)

    @staticmethod
    def from_cache(id_):
        return ObjCacher().instance(CacheHelper.QUERYCACHEKEY, id_)

def updateQueryBuilder(template, bucket, recent_id):
    id_ = "%s_%s" % (template['name'], bucket)
    qbuilder = QueryBuilder.from_cache(id_)
    if qbuilder is None:
        qbuilder = QueryBuilder(template['name'], bucket)
    qbuilder.update(template, recent_id)


def generateQueryParams(template_name,
                        bucket,
                        filters,
                        limit = 10,
                        startkey = None,
                        endkey = None,
                        startkey_docid = None,
                        endkey_docid = None):

    id_ = "%s_%s" % (template_name, bucket)
    qbuilder = QueryBuilder.from_cache(id_)
    params = {}

    if 'limit' in filters:
        params = {'limit' : limit}

    if qbuilder is not None:
        if 'startkey' in filters:
            # if startkey filter selected without explicit value
            # use most recent value
            startkey = startkey or qbuilder.recentkey
            startkey = typecast(startkey)
            params.update({'startkey' : startkey})
        if 'endkey' in filters:
            endkey = endkey or qbuilder.endkey
            endkey = typecast(endkey)
            params.update({'endkey' : endkey})
        if 'startkey_docid' in filters:
            startkey_docid = startkey_docid or qbuilder.recentkey_docid
            startkey_docid = typecast(startkey_docid)
            params.update({'startkey_docid' : qbuilder.recentkey_docid})
        if 'endkey_docid' in filters:
            endkey_docid = endkey_docid or qbuilder.endkey_docid
            endkey_docid = typecast(endkey_docid)
            params.update({'endkey_docid' : qbuilder.endkey_docid})

        if startkey > endkey:
            params.update({'descending' : True})

    return params

def typecast(str_):
    val = None

    try: #Int
        val = int(str_)

    except ValueError: #String

        # decode magic value if specified
        if str_.find('$') == 0:
            val = _int_float_str_gen(str_)
            return typecast(val)

        val = '"%s"' % str_
    except TypeError: #None
        val = ""


    return val

class QueryBuilder(object):
    def __init__(self, template_name, bucket = "default"):
        self.id = "%s_%s" % (template_name, bucket)
        self.startkey = None
        self.startkey_docid = None
        self.endkey = None
        self.endkey_docid = None
        self.recentkey = None
        self.recentkey_docid = None
        self.params = {}

    def update(self, template, recent_id):

        kv = template['kv']
        indexed_key = template['indexed_key']
        key = kv[indexed_key]

        if self.startkey is None or key < self.startkey:
            self.startkey = key
            self.startkey_docid = recent_id
        if self.endkey is None or key > self.endkey:
            self.endkey = key
            self.endkey_docid = recent_id

        self.recentkey = key
        self.recentkey_docid = recent_id
        ObjCacher().store(CacheHelper.QBUILDCACHEKEY, self)



    @staticmethod
    def from_cache(id_):
        return ObjCacher().instance(CacheHelper.QBUILDCACHEKEY, id_)


