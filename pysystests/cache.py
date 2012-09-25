import json
import pylibmc
import hashlib
from celery.utils.log import get_task_logger
import testcfg as cfg

WORKLOADCACHEKEY = "WORKLOADCACHEKEY"
TEMPLATECACHEKEY = "TEMPLATECACHEKEY"
BUCKETSTATUSCACHEKEY = "BUCKETSTATUSCACHEKEY"
NODESTATSCACHEKEY = "NODESTATSCACHEKEY"

class Cache(object):
    def __init__(self):
        self._mc = pylibmc.Client([cfg.OBJECT_CACHE_IP+":"+cfg.OBJECT_CACHE_PORT], binary=True)
        self.logger = get_task_logger(__name__)

    def store(self, key, data, collectionKey):
#TODO: this instead key = getContextKey(collectionKey, key)

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

    def retrieve(self, key):
        data = None

        try:
            data = self._mc[key]
        except KeyError as ex:
            self.logger.info("fail attempt to retrieve key %s" % ex)
        except (ValueError, SyntaxError) as ex:
            pass # non json

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


class WorkloadCacher(Cache):
    @property
    def workloads(self):
        return self.fetchCollection(WORKLOADCACHEKEY)

    def store(self, workload):
        id_ = getContextKey(WORKLOADCACHEKEY, workload.id)
        super(WorkloadCacher, self).store(id_, workload, WORKLOADCACHEKEY)

    def workload(self, key):
        id_ = getContextKey(WORKLOADCACHEKEY, key)
        return self.retrieve(id_)

    def delete(self, workload):
        id_ = getContextKey(WORKLOADCACHEKEY, workload.id)
        super(WorkloadCacher, self).delete(id_, WORKLOADCACHEKEY)
         
    def clear(self):
        super(WorkloadCacher, self).clear(WORKLOADCACHEKEY)

    @property
    def queues(self):
        return self.task_queues + self.cc_queues + self.consume_queues

    @property
    def task_queues(self):
        return [workload.task_queue for workload in self.workloads]

    @property
    def cc_queues(self):
        queues = []
        for workload in self.workloads:
            if workload.cc_queues is not None:
                [queues.append(q) for q in workload.cc_queues]
        return queues

    @property
    def consume_queues(self):
        queues = []
        for workload in self.workloads:
            if workload.consume_queue is not None:
                queues.append(workload.consume_queue)
        return queues

            

class TemplateCacher(Cache):

    def store(self, template):
        id_ = getContextKey(TEMPLATECACHEKEY, template.name)
        super(TemplateCacher, self).store(id_, template, TEMPLATECACHEKEY)

    @property
    def templates(self):
        return self.fetchCollection(TEMPLATECACHEKEY)

    def template(self, name):
        id_ = getContextKey(TEMPLATECACHEKEY, name)
        return self.retrieve(id_)

    def clear(self):
        super(TemplateCacher, self).clear(TEMPLATECACHEKEY)

    @property
    def cc_queues(self):
        queues = []
        for template in self.templates:
            if template.cc_queues is not None:
                [queues.append(q) for q in template.cc_queues]
        return queues



class BucketStatusCacher(Cache):

    @property
    def bucketstatuses(self):
        return self.fetchCollection(BUCKETSTATUSCACHEKEY)

    def store(self, bucketstatus):
        id_ = getContextKey(BUCKETSTATUSCACHEKEY, bucketstatus.id)
        super(BucketStatusCacher, self).store(id_, bucketstatus, BUCKETSTATUSCACHEKEY)

    def bucketstatus(self, key):
        id_ = getContextKey(BUCKETSTATUSCACHEKEY, key)
        return self.retrieve(id_)

    def clear(self):
        super(BucketStatusCacher, self).clear(BUCKETSTATUSCACHEKEY)

class NodeStatsCacher(Cache):

    @property
    def allnodestats(self):
        return self.fetchCollection(NODESTATSCACHEKEY)

    def store(self, nodestats):
        id_ = getContextKey(NODESTATSCACHEKEY, nodestats.id)
        super(NodeStatsCacher, self).store(id_, nodestats, NODESTATSCACHEKEY)

    def nodestats(self, key):
        id_ = getContextKey(NODESTATSCACHEKEY, key)
        return self.retrieve(id_)

    def clear(self):
        super(NodeStatsCacher, self).clear(NODESTATSCACHEKEY)



def cacheClean():
    for cacheKey in [WORKLOADCACHEKEY, BUCKETSTATUSCACHEKEY, NODESTATSCACHEKEY]:
        Cache().clear(cacheKey)


# ensure no collisions during object caching
def getContextKey(collectionKey, id_):
   m = hashlib.md5()
   m.update(collectionKey)
   m.update(id_)
   return m.hexdigest()
