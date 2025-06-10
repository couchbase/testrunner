#!/usr/bin/env python
"""
Python based SDK client interface

"""
import crc32
import time
from couchbase import FMT_AUTO
from memcached.helper.old_kvstore import ClientKeyValueStore
from couchbase.bucket import Bucket as CouchbaseBucket
from couchbase.cluster import Cluster, ClassicAuthenticator, PasswordAuthenticator
from couchbase.exceptions import CouchbaseError, BucketNotFoundError, AuthError
from mc_bin_client import MemcachedError
from couchbase.n1ql import N1QLQuery, N1QLRequest

import couchbase
import json

from lib.Cb_constants.CBServer import CbServer


class SDKClient(object):
    """Python SDK Client Implementation for testrunner - master branch Implementation"""

    def __init__(self, bucket, hosts=["localhost"], scheme="couchbase",
                 ssl_path=None, uhm_options=None, password=None,
                 quiet=True, certpath=None, transcoder=None, ipv6=False, compression=True):

        if CbServer.use_https:
            scheme = "couchbases"
        self.connection_string = \
            self._createString(scheme=scheme, bucket=bucket, hosts=hosts,
                               certpath=certpath, uhm_options=uhm_options, ipv6=ipv6, compression=compression)
        self.bucket = bucket
        self.password = password
        self.quiet = quiet
        self.transcoder = transcoder
        self.default_timeout = 1
        self._createConn()
        couchbase.set_json_converters(json.dumps, json.loads)

    def _createString(self, scheme="couchbase", bucket=None, hosts=["localhost"], certpath=None,
                      uhm_options="", ipv6=False, compression=True):

        connection_string = "{0}://{1}".format(scheme, ", ".join(hosts).replace(" ", ""))
        # if bucket != None:
        #     connection_string = "{0}/{1}".format(connection_string, bucket)
        if uhm_options != None:
            connection_string = "{0}?{1}".format(connection_string, uhm_options)
        if ipv6 == True:
            if "?" in connection_string:
                connection_string = "{0},ipv6=allow".format(connection_string)
            else:
                connection_string = "{0}?ipv6=allow".format(connection_string)
        if compression == True:
            if "?" in connection_string:
                connection_string = "{0},compression=on".format(connection_string)
            else:
                connection_string = "{0}?compression=on".format(connection_string)
        else:
            if "?" in connection_string:
                connection_string = "{0},compression=off".format(connection_string)
            else:
                connection_string = "{0}?compression=off".format(connection_string)
        if scheme == "couchbases":
            if "?" in connection_string:
                if not certpath:
                    connection_string = "{0}&ssl=no_verify".format(connection_string)
                else:
                    connection_string = "{0}&certpath={1}".format(connection_string, certpath)
            else:
                if not certpath:
                    connection_string = "{0}?ssl=no_verify".format(connection_string)
                else:
                    connection_string = "{0}?certpath={1}".format(connection_string, certpath)

        return connection_string

    def _createConn(self):
        try:
            cluster = Cluster(self.connection_string, bucket_class=CouchbaseBucket)
            cluster.authenticate(PasswordAuthenticator(self.bucket, 'password'))
            self.cb = cluster.open_bucket(self.bucket)
        except BucketNotFoundError:
            raise
        except AuthError:
            # Try using default user created by the tests, if any, in case there is no user with bucket name in the
            # cluster.
            try:
                cluster = Cluster(self.connection_string, bucket_class=CouchbaseBucket)
                username = CbServer.rest_username
                password = CbServer.rest_password
                cluster.authenticate(PasswordAuthenticator(username, password))
                self.cb = cluster.open_bucket(self.bucket)
            except AuthError:
                raise

    def reconnect(self):
        self.cb.close()
        self._createConn()

    def close(self):
        self.cb._close()

    def counter_in(self, key, path, delta, create_parents=True, cas=0, ttl=0, persist_to=0, replicate_to=0,
                   collection=None):

        try:
            return self.cb.counter_in(key, path, delta, create_parents=create_parents, cas=cas, ttl=ttl,
                                      persist_to=persist_to, replicate_to=replicate_to)
        except CouchbaseError as e:
            raise

    def arrayappend_in(self, key, path, value, create_parents=True, cas=0, ttl=0, persist_to=0, replicate_to=0,
                       collection=None):

        try:
            return self.cb.arrayappend_in(key, path, value, create_parents=create_parents, cas=cas, ttl=ttl,
                                          persist_to=persist_to, replicate_to=replicate_to)
        except CouchbaseError as e:
            raise

    def arrayprepend_in(self, key, path, value, create_parents=True, cas=0, ttl=0, persist_to=0, replicate_to=0,
                        collection=None):

        try:
            return self.cb.arrayprepend_in(key, path, value, create_parents=create_parents, cas=cas, ttl=ttl,
                                           persist_to=persist_to, replicate_to=replicate_to)
        except CouchbaseError as e:
            raise

    def arrayaddunique_in(self, key, path, value, create_parents=True, cas=0, ttl=0, persist_to=0, replicate_to=0,
                          collection=None):

        try:
            return self.cb.addunique_in(key, path, value, create_parents=create_parents, cas=cas, ttl=ttl,
                                        persist_to=persist_to, replicate_to=replicate_to)
        except CouchbaseError as e:
            raise

    def arrayinsert_in(self, key, path, value, cas=0, ttl=0, persist_to=0, replicate_to=0, collection=None):

        try:
            return self.cb.arrayinsert_in(key, path, value, cas=cas, ttl=ttl, persist_to=persist_to,
                                          replicate_to=replicate_to)
        except CouchbaseError as e:
            raise

    def remove_in(self, key, path, cas=0, ttl=0, collection=None):

        try:
            self.cb.remove_in(key, path, cas=cas, ttl=ttl)
        except CouchbaseError as e:
            raise

    def mutate_in(self, key, *specs, **kwargs):

        try:
            self.cb.mutate_in(key, *specs, **kwargs)
        except CouchbaseError as e:
            raise

    def lookup_in(self, key, *specs, **kwargs):

        try:
            self.cb.lookup_in(key, *specs, **kwargs)
        except CouchbaseError as e:
            raise

    def get_in(self, key, path, collection=None):

        try:
            result = self.cb.get_in(key, path)
            return self.__translate_get(result)
        except CouchbaseError as e:
            raise

    def exists_in(self, key, path, collection=None):

        try:
            self.cb.exists_in(key, path)
        except CouchbaseError as e:
            raise

    def replace_in(self, key, path, value, cas=0, ttl=0, persist_to=0, replicate_to=0, collection=None):

        try:
            return self.cb.replace_in(key, path, value, cas=cas, ttl=ttl, persist_to=persist_to,
                                      replicate_to=replicate_to)
        except CouchbaseError as e:
            raise

    def insert_in(self, key, path, value, create_parents=True, cas=0, ttl=0, persist_to=0, replicate_to=0,
                  collection=None):

        try:
            return self.cb.insert_in(key, path, value, create_parents=create_parents, cas=cas, ttl=ttl,
                                     persist_to=persist_to, replicate_to=replicate_to)
        except CouchbaseError as e:
            raise

    def upsert_in(self, key, path, value, create_parents=True, cas=0, ttl=0, persist_to=0, replicate_to=0,
                  collection=None):

        try:
            return self.cb.upsert_in(key, path, value, create_parents=create_parents, cas=cas, ttl=ttl,
                                     persist_to=persist_to, replicate_to=replicate_to)
        except CouchbaseError as e:
            raise

    def append(self, key, value, cas=0, format=None, persist_to=0, replicate_to=0, collection=None):

        try:
            self.cb.append(key, value, cas=cas, format=format, persist_to=persist_to, replicate_to=replicate_to)
        except CouchbaseError as e:
            try:
                time.sleep(10)
                self.cb.append(key, value, cas=cas, format=format, persist_to=persist_to, replicate_to=replicate_to)
            except CouchbaseError as e:
                raise

    def append_multi(self, keys, cas=0, format=None, persist_to=0, replicate_to=0, collection=None):

        try:
            self.cb.append_multi(keys, cas=cas, format=format, persist_to=persist_to, replicate_to=replicate_to)
        except CouchbaseError as e:
            try:
                time.sleep(10)
                self.cb.append_multi(keys, cas=cas, format=format, persist_to=persist_to, replicate_to=replicate_to)
            except CouchbaseError as e:
                raise

    def prepend(self, key, value, cas=0, format=None, persist_to=0, replicate_to=0, collection=None):

        try:
            self.cb.prepend(key, value, cas=cas, format=format, persist_to=persist_to, replicate_to=replicate_to)
        except CouchbaseError as e:
            try:
                self.cb.prepend(key, value, cas=cas, format=format, persist_to=persist_to, replicate_to=replicate_to)
            except CouchbaseError as e:
                raise

    def prepend_multi(self, keys, cas=0, format=None, persist_to=0, replicate_to=0, collection=None):

        try:
            self.cb.prepend_multi(keys, cas=cas, format=format, persist_to=persist_to, replicate_to=replicate_to)
        except CouchbaseError as e:
            try:
                time.sleep(10)
                self.cb.prepend_multi(keys, cas=cas, format=format, persist_to=persist_to, replicate_to=replicate_to)
            except CouchbaseError as e:
                raise

    def replace(self, key, value, cas=0, ttl=0, format=None, persist_to=0, replicate_to=0, collection=None):

        try:
            self.cb.replace(key, value, cas=cas, ttl=ttl, format=format,
                            persist_to=persist_to, replicate_to=replicate_to)
        except CouchbaseError as e:
            try:
                time.sleep(10)
                self.cb.replace(key, value, cas=cas, ttl=ttl, format=format,
                                persist_to=persist_to, replicate_to=replicate_to)
            except CouchbaseError as e:
                raise

    def replace_multi(self, keys, cas=0, ttl=0, format=None, persist_to=0, replicate_to=0, collection=None):

        try:
            self.cb.replace_multi(keys, cas=cas, ttl=ttl, format=format, persist_to=persist_to,
                                  replicate_to=replicate_to)
        except CouchbaseError as e:
            try:
                time.sleep(10)
                self.cb.replace_multi(keys, cas=cas, ttl=ttl, format=format, persist_to=persist_to,
                                      replicate_to=replicate_to)
            except CouchbaseError as e:
                raise

    def cas(self, key, value, cas=0, ttl=0, format=None, collection=None):

        return self.cb.replace(key, value, cas=cas, format=format)

    def delete(self, key, cas=0, quiet=True, persist_to=0, replicate_to=0, collection=None):

        self.remove(key, cas=cas, quiet=quiet, persist_to=persist_to, replicate_to=replicate_to)

    def remove(self, key, cas=0, quiet=True, persist_to=0, replicate_to=0, collection=None):

        try:
            return self.cb.remove(key, cas=cas, quiet=quiet, persist_to=persist_to, replicate_to=replicate_to)
        except CouchbaseError as e:
            try:
                time.sleep(10)
                return self.cb.remove(key, cas=cas, quiet=quiet, persist_to=persist_to, replicate_to=replicate_to)
            except CouchbaseError as e:
                raise

    def remove_multi(self, keys, quiet=True, persist_to=0, replicate_to=0, collection=None):

        try:
            self.cb.remove_multi(keys, quiet=quiet, persist_to=persist_to, replicate_to=replicate_to)
        except CouchbaseError as e:
            try:
                time.sleep(10)
                self.cb.remove_multi(keys, quiet=quiet, persist_to=persist_to, replicate_to=replicate_to)
            except CouchbaseError as e:
                raise

    def set(self, key, value, cas=0, ttl=0, format=None, persist_to=0, replicate_to=0, collection=None):

        return self.upsert(key, value, cas=cas, ttl=ttl, format=format, persist_to=persist_to,
                           replicate_to=replicate_to)

    def upsert(self, key, value, cas=0, ttl=0, format=None, persist_to=0, replicate_to=0, collection=None):

        try:
            return self.cb.upsert(key, value, cas, ttl, format, persist_to, replicate_to)
        except CouchbaseError as e:
            try:
                time.sleep(10)
                return self.cb.upsert(key, value, cas, ttl, format, persist_to, replicate_to)
            except CouchbaseError as e:
                raise

    def set_multi(self, keys, ttl=0, format=None, persist_to=0, replicate_to=0, collection=None):

        return self.upsert_multi(keys, ttl=ttl, format=format, persist_to=persist_to, replicate_to=replicate_to)

    def upsert_multi(self, keys, ttl=0, format=None, persist_to=0, replicate_to=0, collection=None):

        try:
            self.cb.upsert_multi(keys, ttl=ttl, format=format, persist_to=persist_to, replicate_to=replicate_to)
        except CouchbaseError as e:
            try:
                time.sleep(120)
                self.cb.upsert_multi(keys, ttl=ttl, format=format, persist_to=persist_to, replicate_to=replicate_to)
            except CouchbaseError as e:
                raise

    def insert(self, key, value, ttl=0, format=None, persist_to=0, replicate_to=0, collection=None):

        try:
            self.cb.insert(key, value, ttl=ttl, format=format, persist_to=persist_to, replicate_to=replicate_to)
        except CouchbaseError as e:
            try:
                time.sleep(10)
                self.cb.insert(key, value, ttl=ttl, format=format, persist_to=persist_to, replicate_to=replicate_to)
            except CouchbaseError as e:
                raise

    def insert_multi(self, keys, ttl=0, format=None, persist_to=0, replicate_to=0, collection=None):

        try:
            self.cb.insert_multi(keys, ttl=ttl, format=format, persist_to=persist_to, replicate_to=replicate_to)
        except CouchbaseError as e:
            try:
                time.sleep(10)
                self.cb.insert_multi(keys, ttl=ttl, format=format, persist_to=persist_to, replicate_to=replicate_to)
            except CouchbaseError as e:
                raise

    def touch(self, key, ttl=0, collection=None):

        try:
            self.cb.touch(key, ttl=ttl)
        except CouchbaseError as e:
            try:
                time.sleep(10)
                self.cb.touch(key, ttl=ttl)
            except CouchbaseError as e:
                raise

    def touch_multi(self, keys, ttl=0, collection=None):

        try:
            self.cb.touch_multi(keys, ttl=ttl)
        except CouchbaseError as e:
            try:
                time.sleep(10)
                self.cb.touch_multi(keys, ttl=ttl)
            except CouchbaseError as e:
                raise

    def decr(self, key, delta=1, initial=None, ttl=0, collection=None):

        self.counter(key, delta=-delta, initial=initial, ttl=ttl)

    def decr_multi(self, keys, delta=1, initial=None, ttl=0, collection=None):

        self.counter_multi(keys, delta=-delta, initial=initial, ttl=ttl)

    def incr(self, key, delta=1, initial=None, ttl=0, collection=None):

        self.counter(key, delta=delta, initial=initial, ttl=ttl)

    def incr_multi(self, keys, delta=1, initial=None, ttl=0, collection=None):

        self.counter_multi(keys, delta=delta, initial=initial, ttl=ttl)

    def counter(self, key, delta=1, initial=None, ttl=0, collection=None):

        try:
            self.cb.counter(key, delta=delta, initial=initial, ttl=ttl)
        except CouchbaseError as e:
            try:
                time.sleep(10)
                self.cb.counter(key, delta=delta, initial=initial, ttl=ttl)
            except CouchbaseError as e:
                raise

    def counter_multi(self, keys, delta=1, initial=None, ttl=0, collection=None):

        try:
            self.cb.counter_multi(keys, delta=delta, initial=initial, ttl=ttl)
        except CouchbaseError as e:
            try:
                time.sleep(10)
                self.cb.counter_multi(keys, delta=delta, initial=initial, ttl=ttl)
            except CouchbaseError as e:
                raise

    def get(self, key, ttl=0, quiet=True, replica=False, no_format=False, collection=None):

        try:
            rv = self.cb.get(key, ttl=ttl, quiet=quiet, replica=replica, no_format=no_format)
            return self.__translate_get(rv)
        except CouchbaseError as e:
            try:
                time.sleep(10)
                rv = self.cb.get(key, ttl=ttl, quiet=quiet, replica=replica, no_format=no_format)
                return self.__translate_get(rv)
            except CouchbaseError as e:
                raise

    def rget(self, key, replica_index=None, quiet=True, collection=None):

        try:
            data = self.rget(key, replica_index=replica_index, quiet=None)
            return self.__translate_get(data)
        except CouchbaseError as e:
            try:
                time.sleep(10)
                data = self.rget(key, replica_index=replica_index, quiet=None)
                return self.__translate_get(data)
            except CouchbaseError as e:
                raise

    def get_multi(self, keys, ttl=0, quiet=True, replica=False, no_format=False, collection=None):

        try:
            data = self.cb.get_multi(keys, ttl=ttl, quiet=quiet, replica=replica, no_format=no_format)
            return self.__translate_get_multi(data)
        except CouchbaseError as e:
            try:
                time.sleep(10)
                data = self.cb.get_multi(keys, ttl=ttl, quiet=quiet, replica=replica, no_format=no_format)
                return self.__translate_get_multi(data)
            except CouchbaseError as e:
                raise

    def rget_multi(self, key, replica_index=None, quiet=True, collection=None):

        try:
            data = self.cb.rget_multi(key, replica_index=None, quiet=quiet)
            return self.__translate_get_multi(data)
        except CouchbaseError as e:
            try:
                time.sleep(10)
                data = self.cb.rget_multi(key, replica_index=None, quiet=quiet)
                return self.__translate_get_multi(data)
            except CouchbaseError as e:
                raise

    def stats(self, keys=None):

        try:
            stat_map = self.cb.stats(keys=keys)
            return stat_map
        except CouchbaseError as e:
            try:
                time.sleep(10)
                return self.cb.stats(keys=keys)
            except CouchbaseError as e:
                raise

    def errors(self, clear_existing=True):

        try:
            rv = self.cb.errors(clear_existing=clear_existing)
            return rv
        except CouchbaseError as e:
            raise

    def observe(self, key, master_only=False, collection=None):

        try:
            return self.cb.observe(key, master_only=master_only)
        except CouchbaseError as e:
            try:
                time.sleep(10)
                return self.cb.observe(key, master_only=master_only)
            except CouchbaseError as e:
                raise

    def observe_multi(self, keys, master_only=False, collection=None):

        try:
            data = self.cb.observe_multi(keys, master_only=master_only)
            return self.__translate_observe_multi(data)
        except CouchbaseError as e:
            try:
                time.sleep(10)
                data = self.cb.observe_multi(keys, master_only=master_only)
                return self.__translate_observe_multi(data)
            except CouchbaseError as e:
                raise

    def endure(self, key, persist_to=-1, replicate_to=-1, cas=0, check_removed=False, timeout=5.0, interval=0.010,
               collection=None):

        try:
            self.cb.endure(key, persist_to=persist_to, replicate_to=replicate_to,
                           cas=cas, check_removed=check_removed, timeout=timeout, interval=interval)
        except CouchbaseError as e:
            try:
                time.sleep(10)
                self.cb.endure(key, persist_to=persist_to, replicate_to=replicate_to,
                               cas=cas, check_removed=check_removed, timeout=timeout, interval=interval)
            except CouchbaseError as e:
                raise

    def endure_multi(self, keys, persist_to=-1, replicate_to=-1, cas=0, check_removed=False, timeout=5.0,
                     interval=0.010, collection=None):

        try:
            self.cb.endure(keys, persist_to=persist_to, replicate_to=replicate_to,
                           cas=cas, check_removed=check_removed, timeout=timeout, interval=interval)
        except CouchbaseError as e:
            try:
                time.sleep(10)
                self.cb.endure(keys, persist_to=persist_to, replicate_to=replicate_to,
                               cas=cas, check_removed=check_removed, timeout=timeout, interval=interval)
            except CouchbaseError as e:
                raise

    def lock(self, key, ttl=0, collection=None):

        try:
            data = self.cb.lock(key, ttl=ttl)
            return self.__translate_get(data)
        except CouchbaseError as e:
            try:
                time.sleep(10)
                data = self.cb.lock(key, ttl=ttl)
                return self.__translate_get(data)
            except CouchbaseError as e:
                raise

    def lock_multi(self, keys, ttl=0, collection=None):

        try:
            data = self.cb.lock_multi(keys, ttl=ttl)
            return self.__translate_get_multi(data)
        except CouchbaseError as e:
            try:
                time.sleep(10)
                data = self.cb.lock_multi(keys, ttl=ttl)
                return self.__translate_get_multi(data)
            except CouchbaseError as e:
                raise

    def unlock(self, key, ttl=0, collection=None):

        try:
            return self.cb.unlock(key)
        except CouchbaseError as e:
            try:
                time.sleep(10)
                return self.cb.unlock(key)
            except CouchbaseError as e:
                raise

    def unlock_multi(self, keys, collection=None):

        try:
            return self.cb.unlock_multi(keys)
        except CouchbaseError as e:
            try:
                time.sleep(10)
                return self.cb.unlock_multi(keys)
            except CouchbaseError as e:
                raise

    def n1ql_query(self, statement, prepared=False):

        try:
            return N1QLQuery(statement, prepared)
        except CouchbaseError as e:
            raise

    def n1ql_request(self, query):

        try:
            return N1QLRequest(query, self.cb)
        except CouchbaseError as e:
            raise

    def __translate_get_multi(self, data):

        map = {}
        if data == None:
            return map
        for key, result in list(data.items()):
            map[key] = [result.flags, result.cas, result.value]
        return map

    def __translate_get(self, data):

        return data.flags, data.cas, data.value

    def __translate_delete(self, data):

        return data

    def __translate_observe(self, data):

        return data

    def __translate_observe_multi(self, data):

        map = {}
        if data == None:
            return map
        for key, result in list(data.items()):
            map[key] = result.value
        return map

    def __translate_upsert_multi(self, data):

        map = {}
        if data == None:
            return map
        for key, result in list(data.items()):
            map[key] = result
        return map

    def __translate_upsert_op(self, data):

        return data.rc, data.success, data.errstr, data.key


class SDKSmartClient(object):
    def __init__(self, rest, bucket, compression=True, info=None):

        self.rest = rest
        if hasattr(bucket, 'name'):
            self.bucket = bucket.name
        else:
            self.bucket = bucket

        if rest.ip == "127.0.0.1":
            self.host = "{0}:{1}".format(rest.ip, rest.port)
            self.scheme = "http"
        else:
            self.host = rest.ip
            self.scheme = "couchbase"
            if CbServer.use_https:
                self.scheme = "couchbases"
        self.client = SDKClient(self.bucket, hosts=[self.host], scheme=self.scheme,
                                compression=compression)

    def reset(self, compression=True, rest=None):

        self.client = SDKClient(self.bucket, hosts=[self.host], scheme=self.scheme,
                                compression=compression)

    def memcached(self, key):

        return self.client

    def set(self, key, exp, flags, value, format=FMT_AUTO, scope=None, collection=None):

        rc = self.client.set(key, value, ttl=exp, format=format)

    def append(self, key, value, format=FMT_AUTO, scope=None, collection=None):

        return self.client.set(key, value, format=format)

    def observe(self, key, collection=None):

        return self.client.observe(key)

    def get(self, key, scope=None, collection=None):

        return self.client.get(key)

    def getr(self, key, replica_index=0, scope=None, collection=None):

        return self.client.rget(key, replica_index=replica_index)

    def setMulti(self, exp, flags, key_val_dic, pause=None, timeout=5.0, parallel=None, format=FMT_AUTO,
                 scope=None, collection=None):

        try:
            self.client.cb.timeout = timeout
            return self.client.upsert_multi(key_val_dic, ttl=exp, format=format)
        finally:
            self.client.cb.timeout = self.client.default_timeout

    def getMulti(self, keys_lst, pause=None, timeout_sec=5.0, parallel=None, scope=None, collection=None):

        try:
            self.client.cb.timeout = timeout_sec
            map = self.client.get_multi(keys_lst)
            return map
        finally:
            self.client.cb.timeout = self.client.default_timeout

    def getrMulti(self, keys_lst, replica_index=None, pause=None, timeout_sec=5.0, parallel=None, collection=None):

        try:
            self.client.cb.timeout = timeout_sec
            map = self.client.rget_multi(keys_lst, replica_index=replica_index)
            return map
        finally:
            self.client.cb.timeout = self.client.default_timeout

    def delete(self, key, scope=None, collection=None):

        return self.client.remove(key)


class SDKBasedKVStoreAwareSmartClient(SDKSmartClient):
    def __init__(self, rest, bucket, kv_store=None, info=None, store_enabled=True):

        SDKSmartClient.__init__(self, rest, bucket, info)
        self.kv_store = kv_store or ClientKeyValueStore()
        self.store_enabled = store_enabled
        self._rlock = threading.Lock()

    def set(self, key, value, ttl=-1, collection=None):

        self._rlock.acquire()
        try:
            if ttl >= 0:
                self.memcached(key).set(key, ttl, 0, value)
            else:
                self.memcached(key).set(key, 0, 0, value)

            if self.store_enabled:
                self.kv_store.write(key, hashlib.md5(value).digest(), ttl)
        except MemcachedError as e:
            self._rlock.release()
            raise MemcachedError(e.status, e.msg)
        except AssertionError:
            self._rlock.release()
            raise AssertionError
        except:
            self._rlock.release()
            raise Exception("General Exception from KVStoreAwareSmartClient.set()")

        self._rlock.release()

    """
    " retrieve meta data of document from disk
    """

    def get_doc_metadata(self, num_vbuckets, key):

        vid = crc32.crc32_hash(key) & (num_vbuckets - 1)

        mc = self.memcached(key)
        metadatastats = None

        try:
            metadatastats = mc.stats("vkey {0} {1}".format(key, vid))
        except MemcachedError:
            msg = "key {0} doesn't exist in memcached".format(key)
            self.log.info(msg)

        return metadatastats

    def delete(self, key, collection=None):

        try:
            self._rlock.acquire()
            opaque, cas, data = self.memcached(key).delete(key)
            if self.store_enabled:
                self.kv_store.delete(key)
            self._rlock.release()
            if cas == 0:
                raise MemcachedError(7, "Invalid cas value")
        except Exception as e:
            self._rlock.release()
            raise MemcachedError(7, str(e))

    def get_valid_key(self, key):

        return self.get_key_check_status(key, "valid")

    def get_deleted_key(self, key):

        return self.get_key_check_status(key, "deleted")

    def get_expired_key(self, key):

        return self.get_key_check_status(key, "expired")

    def get_all_keys(self):

        return list(self.kv_store.keys())

    def get_all_valid_items(self):

        return self.kv_store.valid_items()

    def get_all_deleted_items(self):

        return self.kv_store.deleted_items()

    def get_all_expired_items(self):

        return self.kv_store.expired_items()

    def get_key_check_status(self, key, status):

        item = self.kv_get(key)
        if (item is not None and item["status"] == status):
            return item
        else:
            msg = "key {0} is not valid".format(key)
            self.log.info(msg)
            return None

    # safe kvstore retrieval
    # return dict of {key,status,value,ttl}
    # or None if not found
    def kv_get(self, key):

        item = None
        try:
            item = self.kv_store.read(key)
        except KeyError:
            msg = "key {0} doesn't exist in store".format(key)
            # self.log.info(msg)

        return item

    # safe memcached retrieval
    # return dict of {key, flags, seq, value}
    # or None if not found
    def mc_get(self, key):

        item = self.mc_get_full(key)
        if item is not None:
            item["value"] = hashlib.md5(item["value"]).digest()
        return item

    # unhashed value
    def mc_get_full(self, key):

        item = None
        try:
            x, y, value = self.memcached(key).get(key)
            item = {}
            item["key"] = key
            item["flags"] = x
            item["seq"] = y
            item["value"] = value
        except MemcachedError:
            msg = "key {0} doesn't exist in memcached".format(key)

        return item

    def kv_mc_sync_get(self, key, status):

        self._rlock.acquire()
        kv_item = self.get_key_check_status(key, status)
        mc_item = self.mc_get(key)
        self._rlock.release()

        return kv_item, mc_item
