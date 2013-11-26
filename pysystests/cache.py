import json
import time
import pylibmc
import hashlib
from celery.utils.log import get_task_logger
import testcfg as cfg

logger = get_task_logger(__name__)

class Cache(object):
    def __init__(self):
        self._mc = pylibmc.Client([cfg.OBJECT_CACHE_IP+":"+cfg.OBJECT_CACHE_PORT], binary=True)
        self.logger = get_task_logger(__name__)

    def store(self, key, data, collectionKey):
        if isinstance(data, dict):
            data = json.dumps(data)

        self._mc[key] = data

        # update collection index
        keyList = self.retrieve(collectionKey)
        if keyList is None:
            keyList = [key]
        elif key not in keyList:
            keyList.append(key)

        self._mc[collectionKey] = keyList

    def fetchCollection(self, collectionKey):
        keys = self.retrieve(collectionKey)
        data = []
        if keys is not None:
            for key in keys:
                val = self.retrieve(key)
                if val is not None:
                    data.append(val)
        return data

    def retrieve(self, key, retry_count = 5):
        data = None

        try:
            data = self._mc[key]
        except KeyError as ex:
            self.logger.info("fail attempt to retrieve key %s" % ex)
        except (ValueError, SyntaxError) as ex:
            pass # non json
        except Exception as ex:
            if retry_count > 0:
                self.logger.info("error occured in mc protocol fetching %s: %s" % (key,ex))
                self.logger.info("retry attempt: %s" % retry_count)
                time.sleep(2)
                cnt = retry_count - 1
                self.retrieve(key, cnt)

        return data

    def delete(self, key, collectionKey):
        del self._mc[key]

        # remove index
        keys = self.retrieve(collectionKey)
        data = []
        try:
            idx = keys.index(key)
            del keys[idx]
            self._mc[collectionKey] = keys
        except ValueError:
            pass

    def clear(self, collectionKey):
        keyList = self.retrieve(collectionKey)
        if keyList is not None:
            for key in keyList:
                if key is not None:
                    try:
                        del self._mc[key]
                    except KeyError as ex:
                        self.logger.error("error clearing key %s"  % ex)
        try:
            del self._mc[collectionKey]
        except KeyError:
            pass # index already deleted


class ObjCacher(Cache):

    def allinstances(self, cachekey):
        return self.fetchCollection(cachekey)

    def store(self, cachekey, obj):
        id_ = getContextKey(cachekey, obj.id)
        super(ObjCacher, self).store(id_, obj, cachekey)

    def instance(self, cachekey, id_):
        key = getContextKey(cachekey, id_)
        return self.retrieve(key)

    def delete(self, cachekey, obj):
        id_ = getContextKey(cachekey, obj.id)
        super(ObjCacher, self).delete(id_, cachekey)

    def clear(self, cachekey):
        super(ObjCacher, self).clear(cachekey)


class CacheHelper():

    WORKLOADCACHEKEY = "WORKLOADCACHEKEY"
    TEMPLATECACHEKEY = "TEMPLATECACHEKEY"
    BUCKETSTATUSCACHEKEY = "BUCKETSTATUSCACHEKEY"
    QUERYCACHEKEY = "QUERYCACHEKEY"
    QBUILDCACHEKEY = "QBUILDCACHEKEY"
    VARCACHEKEY = "VARCACHEKEY"
    CLUSTERSTATUSKEY = "CLUSTERSTATUSKEY"
    ACTIVETASKCACHEKEY = "ACTIVETASKCACHEKEY"

    @staticmethod
    def workloads():
        return ObjCacher().allinstances(CacheHelper.WORKLOADCACHEKEY)

    @staticmethod
    def templates():
        return ObjCacher().allinstances(CacheHelper.TEMPLATECACHEKEY)

    @staticmethod
    def queries():
        return ObjCacher().allinstances(CacheHelper.QUERYCACHEKEY)

    @staticmethod
    def qbuilders():
        return ObjCacher().allinstances(CacheHelper.QBUILDCACHEKEY)

    @staticmethod
    def clusterstatus(_id):
        return ObjCacher().instance(CacheHelper.CLUSTERSTATUSKEY, _id)

    @staticmethod
    def active_queries():
        active = []
        for query in CacheHelper.queries():
            if query.active:
                active.append(query)
        return active

    @staticmethod
    def cc_queues():
        queues = []
        for workload in CacheHelper.workloads():
            if workload.cc_queues is not None:
                [queues.append(q) for q in workload.cc_queues]
        return queues

    @staticmethod
    def consume_queues():
        queues = []
        for workload in CacheHelper.workloads():
            if workload.consume_queue is not None:
                queues.append(workload.consume_queue)
        return queues

    @staticmethod
    def miss_queues():
        queues = []
        for workload in CacheHelper.workloads():
            if workload.miss_queue is not None and workload.consume_queue is not None:
                queues.append(workload.consume_queue)
        return queues

    @staticmethod
    def task_queues():
        kv = [workload.task_queue for workload in CacheHelper.workloads()]
        query = [workload.task_queue for workload in CacheHelper.queries()]
        return kv + query

    @staticmethod
    def queues():
        return CacheHelper.task_queues() +\
            CacheHelper.cc_queues() +\
            CacheHelper.consume_queues() +\
            CacheHelper.miss_queues()

    @staticmethod
    def cachePhaseVar(key, value):
        Cache().store(key, value, CacheHelper.VARCACHEKEY)

    @staticmethod
    def getPhaseVar(key):
        return Cache().retrieve(key)

    @staticmethod
    def cacheClean():
        objCacheKeys = [CacheHelper.WORKLOADCACHEKEY,
                        CacheHelper.BUCKETSTATUSCACHEKEY,
                        CacheHelper.QUERYCACHEKEY,
                        CacheHelper.ACTIVETASKCACHEKEY,
                        CacheHelper.CLUSTERSTATUSKEY]

        for cacheKey in objCacheKeys:
            ObjCacher().clear(cacheKey)


# ensure no collisions during object caching
def getContextKey(collectionKey, id_):
   m = hashlib.md5()
   m.update(collectionKey)
   m.update(id_)
   return m.hexdigest()
