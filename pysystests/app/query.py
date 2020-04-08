
import uuid
import json
import time
import urllib.request, urllib.parse, urllib.error
from app.celery import celery
from celery import Task
from celery.task.sets import TaskSet
from rabbit_helper import PersistedMQ
from cache import ObjCacher, CacheHelper
from app.rest_client_tasks import multi_query
from app.sdk_client_tasks import _int_float_str_gen
from celery.exceptions import TimeoutError
import testcfg as cfg

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
@celery.task(base = PersistedMQ, ignore_result=True)
def queryRunner(max_msgs = 10):

    rabbitHelper = queryRunner.rabbitHelper

    # check queue with pending http requests
    pending_http_requests = "query_multi_"+cfg.CB_CLUSTER_TAG
    if rabbitHelper.qsize(pending_http_requests) > max_msgs:

        # purge waiting tasks
        rabbitHelper.purge(pending_http_requests)
        query_ops_manager(max_msgs, True)

    else:

        hosts = None
        clusterStatus = CacheHelper.clusterstatus(cfg.CB_CLUSTER_TAG+"_status")

        if clusterStatus:
            hosts = clusterStatus.get_all_hosts()

        # retreive all active query workloads
        queries = CacheHelper.active_queries()
        for query in queries:

            # async update query workload object
            updateQueryWorkload.apply_async(args=[query])

            count = int(query.qps)
            filters = list(set(query.include_filters) -\
                           set(query.exclude_filters))
            params = generateQueryParams(query.indexed_key,
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
                              query.password,
                              hosts = hosts)


class QueryWorkload(object):
    def __init__(self, params):
        self.id = "query_workload_"+str(uuid.uuid4())[:7]
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
        self.indexed_key = params.get("indexed_key")

    @staticmethod
    def defaultSpec():
        return {"queries_per_sec" : 0,
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
                "limit" : 10,
                "indexed_key" : None}


    def uniq_include_filters(self, ex_filters):
        include = ["startkey", "endkey", "limit"]
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

"""
" if any kv workload is running against the bucket
" that is being queried, this method will update
" its workload to include index information
" so that we can track work being done in the kv workload
"""
@celery.task
def updateQueryWorkload(query):
    workloads = CacheHelper.workloads()

    for workload in workloads:
        if workload.active and workload.bucket == query.bucket:
            key = query.indexed_key
            workload.updateIndexKeys(key)

@celery.task
def updateQueryBuilders(template, bucket, recent_id):

    for key in template['indexed_keys']:

        id_ = "%s_%s" % (key, bucket)
        qbuilder = QueryBuilder.from_cache(id_)
        if qbuilder is None:
            qbuilder = QueryBuilder(key, bucket)

        new_id = template['kv'][key]
        qbuilder.update(new_id, recent_id)


def generateQueryParams(indexed_key,
                        bucket,
                        filters,
                        limit = 10,
                        startkey = None,
                        endkey = None,
                        startkey_docid = None,
                        endkey_docid = None):

    id_ = "%s_%s" % (indexed_key, bucket)
    qbuilder = QueryBuilder.from_cache(id_)
    params = {}

    if 'limit' in filters:
        params = {'limit' : limit}

    if 'startkey' in filters:

        # try to use startkey from query builder
        # only if user startkey not supplied
        if qbuilder and startkey is None:
            startkey = qbuilder.recentkey

        # user supplied startkey
        if startkey:
            startkey = typecast(startkey)
            params.update({'startkey' : startkey})

    if 'endkey' in filters:
        if qbuilder and endkey is None:
            endkey = qbuilder.endkey

        if endkey:
            endkey = typecast(endkey)
            params.update({'endkey' : endkey})

    if 'startkey_docid' in filters:
        if qbuilder and startkey_docid is None:
            startkey_docid = qbuilder.recentkey_docid

        if startkey_docid:
            startkey_docid = typecast(startkey_docid)
            params.update({'startkey_docid' : startkey_docid})

    if 'endkey_docid' in filters:
        if qbuilder and endkey_docid is None:
            endkey_docid = endkey_docid or qbuilder.endkey_docid

        if endkey_docid:
            endkey_docid = typecast(endkey_docid)
            params.update({'endkey_docid' : endkey_docid})

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
    def __init__(self, indexed_key, bucket = "default"):
        self.id = "%s_%s" % (indexed_key, bucket)
        self.startkey = None
        self.startkey_docid = None
        self.endkey = None
        self.endkey_docid = None
        self.recentkey = None
        self.recentkey_docid = None
        self.params = {}

    def update(self, recent_key, recent_id):


        if self.startkey is None or recent_key < self.startkey:
            self.startkey = recent_key
            self.startkey_docid = recent_id
        if self.endkey is None or recent_key > self.endkey:
            self.endkey = recent_key
            self.endkey_docid = recent_id

        self.recentkey = recent_key
        self.recentkey_docid = recent_id

        ObjCacher().store(CacheHelper.QBUILDCACHEKEY, self)



    @staticmethod
    def from_cache(id_):
        return ObjCacher().instance(CacheHelper.QBUILDCACHEKEY, id_)


@celery.task(base = PersistedMQ, ignore_result=True)
def query_ops_manager(max_msgs = 10, isovercommited = False):

    rabbitHelper = query_ops_manager.rabbitHelper

    # retreive all active query workloads
    queries = CacheHelper.active_queries()
    for query in queries:

        # check if query tasks are overloaded
        if rabbitHelper.qsize(query.task_queue) > max_msgs or isovercommited:

            # purge waiting tasks
            rabbitHelper.purge(query.task_queue)

            # throttle down ops by 10%
            new_queries_per_sec = query.qps*0.90

            # cannot reduce below 10 qps
            if new_queries_per_sec > 10:
                query.qps = new_queries_per_sec
                logger.error("Cluster Overcommited: reduced queries/sec to (%s)" %\
                             query.qps)




