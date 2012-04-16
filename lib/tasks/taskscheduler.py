from tasks.future import Future
from tasks.task import *

class TaskScheduler():

    @staticmethod
    def async_bucket_create(tm, server, bucket='default', replicas=1, port=11210, size=0,
                           password=None):
        _task = BucketCreateTask(server, bucket, replicas, port, size, password)
        tm.schedule(_task)
        return _task

    @staticmethod
    def async_bucket_delete(tm, server, bucket='default'):
        _task = BucketDeleteTask(server, bucket)
        tm.schedule(_task)
        return _task

    @staticmethod
    def async_init_node(tm, server):
        _task = NodeInitializeTask(server)
        tm.schedule(_task)
        return _task

    @staticmethod
    def async_load_gen_docs(tm, rest, generator, bucket, expiration, loop=False):
        _task = LoadDocGeneratorTask(rest, generator, bucket, expiration, loop)
        tm.schedule(_task)
        return _task

    @staticmethod
    def async_mutate_docs(tm, docs, rest, bucket, kv_store=None, info=None, store_enabled=True,
                          expiration=0):
        _task = DocumentMutateTask(docs, rest, bucket, kv_store, info, store_enabled, expiration)
        tm.schedule(_task)
        return _task

    @staticmethod
    def async_access_docs(tm, docs, rest, bucket, info=None):
        _task = DocumentAccessTask(docs, rest, bucket, info)
        tm.schedule(_task)
        return _task

    @staticmethod
    def async_delete_docs(tm, docs, rest, bucket, info=None, kv_store=None, store_enabled=True):
        _task = DocumentDeleteTask(docs, rest, bucket, info, kv_store, store_enabled)
        tm.schedule(_task)
        return _task

    @staticmethod
    def async_rebalance(tm, servers, to_add, to_remove):
        _task = RebalanceTask(servers, to_add, to_remove)
        tm.schedule(_task)
        return _task

    @staticmethod
    def bucket_create(tm, server, bucket='default', replicas=1, port=11210, size=0, password=None):
        _task = TaskScheduler.async_bucket_create(tm, server, bucket, replicas, port, size,
                                                  password)
        return _task.result()

    @staticmethod
    def bucket_delete(tm, server, bucket='default'):
        _task = TaskScheduler.async_bucket_delete(tm, server, bucket)
        return _task.result()

    @staticmethod
    def init_node(tm, server):
        _task = TaskScheduler.async_init_node(tm, server)
        return _task.result()

    @staticmethod
    def rebalance(tm, servers, to_add, to_remove):
        _task = TaskScheduler.async_rebalance(tm, servers, to_add, to_remove)
        return _task.result()

    @staticmethod
    def load_gen_docs(tm, rest, generator, bucket, expiration, loop=False):
        _task = TaskScheduler.async_load_gen_docs(tm, rest, generator, bucket, expiration, loop)
        return _task.result()

    @staticmethod
    def mutate_docs(tm, docs, rest, bucket, kv_store=None, info=None, store_enabled=True,
                    expiration=0):
        _task = TaskScheduler.async_mutate_docs(tm, docs, rest, bucket, kv_store, info,
                                                store_enabled, expiration)
        return _task.result()

    @staticmethod
    def access_docs(tm, docs, rest, bucket, info=None):
        _task = TaskScheduler.async_access_docs(tm, docs, rest, bucket, info)
        return _task.result()

    @staticmethod
    def delete_docs(tm, docs, rest, bucket, info=None, kv_store=None, store_enabled=True):
        _task = TaskScheduler.async_delete_docs(tm, docs, rest, bucket, info, kv_store,
                                                store_enabled)
        return _task.result()
