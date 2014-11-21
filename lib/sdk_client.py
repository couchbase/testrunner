#!/usr/bin/env python
"""
Python based SDK client interface

"""

from couchbase.bucket import Bucket as CouchbaseBucket
from couchbase.exceptions import CouchbaseError,BucketNotFoundError

class SDKClient(object):
    """Python SDK Client Implementation for testrunner"""

    def __init__(self, bucket, hosts = ["localhost"] , scheme = "couchbase",
                 ssl_path = None, uhm_options = None, password=None,
                 quiet=False, certpath = None, transcoder = None):
        self.connection_string = \
            self._createString(scheme = scheme, bucket = bucket, hosts = hosts,
                               certpath = certpath, uhm_options = uhm_options)
        self.password = password
        self.quiet = quiet
        self.transcoder = transcoder
        print self.connection_string
        self._createConn()

    def _createString(self, scheme ="couchbase", bucket = None, hosts = ["localhost"], certpath = None, uhm_options = ""):
        connection_string = "{0}://{1}".format(scheme, ", ".join(hosts).replace(" ",""))
        if bucket != None:
            connection_string = "{0}/{1}".format(connection_string, bucket)
        if uhm_options != None:
            connection_string = "{0}?{1}".format(connection_string, uhm_options)
        if scheme == "couchbases":
            if "?" in connection_string:
                connection_string = "{0},certpath={1}".format(connection_string, certpath)
            else:
                connection_string = "{0}?certpath={1}".format(connection_string, certpath)
        return connection_string

    def _createConn(self):
        try:
            self.cb = CouchbaseBucket(self.connection_string, password = self.password,
                                  quiet = self.quiet, transcoder = self.transcoder)
        except BucketNotFoundError as e:
             raise

    def reconnect(self):
        self.cb.close()
        self._createConn()

    def close(self):
        self.cb._close()

    def append(self, key, value, cas=0, format=None, persist_to=0, replicate_to=0):
        try:
            self.cb.append(key, value, cas=cas, format=format, persist_to=persist_to, replicate_to=replicate_to)
        except CouchbaseError as e:
            raise

    def append_multi(self, keys, cas=0, format=None, persist_to=0, replicate_to=0):
        try:
            self.cb.append_multi(keys, cas=cas, format=format, persist_to=persist_to, replicate_to=replicate_to)
        except CouchbaseError as e:
            raise

    def prepend(self, key, value, cas=0, format=None, persist_to=0, replicate_to=0):
        try:
            self.cb.prepend(key, value, cas=cas, format=format, persist_to=persist_to, replicate_to=replicate_to)
        except CouchbaseError as e:
            raise

    def prepend_multi(self, keys, cas=0, format=None, persist_to=0, replicate_to=0):
        try:
            self.cb.prepend_multi(keys, cas=cas, format=format, persist_to=persist_to, replicate_to=replicate_to)
        except CouchbaseError as e:
            raise

    def replace(self, key, value, cas=0, ttl=0, format=None, persist_to=0, replicate_to=0):
        try:
           self.cb.replace( key, value, cas=cas, ttl=ttl, format=format,
                                    persist_to=persist_to, replicate_to=replicate_to)
        except CouchbaseError as e:
            raise

    def replace_multi(self, keys, cas=0, ttl=0, format=None, persist_to=0, replicate_to=0):
        try:
            self.cb.replace_multi( keys, cas=cas, ttl=ttl, format=format, persist_to=persist_to, replicate_to=replicate_to)
        except CouchbaseError as e:
            raise

    def cas(self, key, value, cas=0, ttl=0, format=None):
        return self.cb.replace(key, value, cas=cas,format=format)

    def remove(self,key, cas=0, quiet=None, persist_to=0, replicate_to=0):
        try:
            self.cb.remove(key, cas=cas, quiet=quiet, persist_to=persist_to, replicate_to=replicate_to)
        except CouchbaseError as e:
            raise

    def remove_multi(self, keys, quiet=None, persist_to=0, replicate_to=0):
        try:
            self.cb.remove_multi(keys, quiet=quiet, persist_to=persist_to, replicate_to=replicate_to)
        except CouchbaseError as e:
            raise

    def upsert(self, key, value, cas=0, ttl=0, format=None, persist_to=0, replicate_to=0):
        try:
            self.cb.upsert(key, value, cas, ttl, format, persist_to, replicate_to)
        except CouchbaseError as e:
            raise

    def upsert_multi(self, keys, ttl=0, format=None, persist_to=0, replicate_to=0):
        try:
            self.cb.upsert_multi(keys, ttl=ttl, format=format, persist_to=persist_to, replicate_to=replicate_to)
        except CouchbaseError as e:
            raise

    def insert(self, key, value, ttl=0, format=None, persist_to=0, replicate_to=0):
        try:
            self.cb.insert(key, value, ttl=ttl, format=format, persist_to=persist_to, replicate_to=replicate_to)
        except CouchbaseError as e:
            raise

    def insert_multi(self, keys,  ttl=0, format=None, persist_to=0, replicate_to=0):
        try:
            self.cb.insert_multi(keys, ttl=ttl, format=format, persist_to=persist_to, replicate_to=replicate_to)
        except CouchbaseError as e:
            raise

    def touch(self, key, ttl = 0):
        try:
            self.cb.touch(key, ttl=ttl)
        except CouchbaseError as e:
            raise

    def touch_multi(self, keys, ttl = 0):
        try:
            self.cb.touch_multi(keys, ttl=ttl)
        except CouchbaseError as e:
            raise

    def decr(self, key, delta=1, initial=None, ttl=0):
        self.counter(key, delta=-delta, initial=initial, ttl=ttl)


    def decr_multi(self, keys, delta=1, initial=None, ttl=0):
        self.counter_multi(keys, delta=-delta, initial=initial, ttl=ttl)

    def incr(self, key, delta=1, initial=None, ttl=0):
        self.counter(key, delta=delta, initial=initial, ttl=ttl)


    def incr_multi(self, keys, delta=1, initial=None, ttl=0):
        self.counter_multi(keys, delta=delta, initial=initial, ttl=ttl)

    def counter(self, key, delta=1, initial=None, ttl=0):
        try:
            self.cb.counter(key, delta=delta, initial=initial, ttl=ttl)
        except CouchbaseError as e:
            raise

    def counter_multi(self, keys, delta=1, initial=None, ttl=0):
        try:
            self.cb.counter_multi(keys, delta=delta, initial=initial, ttl=ttl)
        except CouchbaseError as e:
            raise

    def get(self, key, ttl=0, quiet=None, replica=False, no_format=False):
        try:
            rv = self.cb.get(key, ttl=ttl, quiet=quiet, replica=replica, no_format=no_format)
            return self.__translate_get(rv)
        except CouchbaseError as e:
            raise

    def rget(self, key, replica_index=None, quiet=None):
        try:
            data  = self.get(key, replica_index=None, quiet=None)
            return self.__translate_get(data)
        except CouchbaseError as e:
            raise

    def get_multi(self, keys, ttl=0, quiet=None, replica=False, no_format=False):
        try:
            data = self.cb.get_multi(keys, ttl=ttl, quiet=quiet, replica=replica, no_format=no_format)
            return self.__translate_get_multi(data)
        except CouchbaseError as e:
            raise

    def rget_multi(self, key, replica_index=None, quiet=None):
        try:
            data = self.cb.rget_multi(key, replica_index=None, quiet=None)
            return self.__translate_get(data)
        except CouchbaseError as e:
            raise

    def stats(self, keys=None):
        try:
            stat_map = self.cb.stats(keys = keys)
            return stat_map
        except CouchbaseError as e:
            raise

    def errors(self, clear_existing=True):
        try:
            rv = self.cb.errors(clear_existing = clear_existing)
            return rv
        except CouchbaseError as e:
            raise

    def observe(self, key, master_only=False):
        try:
            data = self.cb.observe(key, master_only = master_only)
            return self.__translate_observe(data)
        except CouchbaseError as e:
            raise

    def observe_multi(self, keys, master_only=False):
        try:
            data = self.cb.observe_multi(keys, master_only = master_only)
            return self.__translate_observe_multi(data)
        except CouchbaseError as e:
            raise

    def endure(self, key, persist_to=-1, replicate_to=-1, cas=0, check_removed=False, timeout=5.0, interval=0.010):
        try:
            self.cb.endure(key, persist_to=persist_to, replicate_to=replicate_to,
                           cas=cas, check_removed=check_removed, timeout=timeout, interval=interval)
        except CouchbaseError as e:
            raise

    def endure_multi(self, keys, persist_to=-1, replicate_to=-1, cas=0, check_removed=False, timeout=5.0, interval=0.010):
        try:
            self.cb.endure(keys, persist_to=persist_to, replicate_to=replicate_to,
                           cas=cas, check_removed=check_removed, timeout=timeout, interval=interval)
        except CouchbaseError as e:
            raise

    def lock(self, key, ttl=0):
        try:
            data = self.cb.lock(key, ttl = ttl)
            return self.__translate_get(data)
        except CouchbaseError as e:
            raise

    def lock_multi(self, keys, ttl=0):
        try:
            data = self.cb.lock_multi(keys, ttl = ttl)
            return self.__translate_get_multi(data)
        except CouchbaseError as e:
            raise

    def unlock(self, key, ttl=0):
        try:
            return self.cb.unlock(key)
        except CouchbaseError as e:
            raise

    def unlock_multi(self, keys):
        try:
            return self.cb.unlock_multi(keys)
        except CouchbaseError as e:
            raise

    def __translate_get_multi(self, data):
        map = {}
        if data == None:
            return map
        for key, result in data.items():
            map[key] = [result.value,result.cas,result.flags]
        return map

    def __translate_get(self, data):
        return data.value, data.flags, data.cas

    def __translate_observe(self, data):
        return data

    def __translate_observe_multi(self, data):
        map = {}
        if data == None:
            return map
        for key, result in data.items():
            map[key] = result.value
        return map

    def __translate_upsert_multi(self, data):
        map = {}
        if data == None:
            return map
        for key, result in data.items():
            map[key] = result
        return map

    def __translate_upsert_op(self, data):
        return data.rc, data.success, data.errstr, data.key

