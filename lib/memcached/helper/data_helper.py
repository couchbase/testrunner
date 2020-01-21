import copy
import socket
from multiprocessing.process import BaseProcess as Process
from multiprocessing.queues import Queue
import random
import time
from random import Random
import uuid
from TestInput import TestInputServer
from TestInput import TestInputSingleton
import logger
import zlib
import crc32
import hashlib
import threading
from mc_bin_client import MemcachedClient, MemcachedError
from mc_ascii_client import MemcachedAsciiClient
from memcached.helper.old_kvstore import ClientKeyValueStore
from membase.api.rest_client import RestConnection, RestHelper, Bucket, vBucket
from memcacheConstants import ERR_NOT_FOUND, ERR_NOT_MY_VBUCKET, ERR_ETMPFAIL, ERR_EINVAL, ERR_2BIG
import json
import sys
from perf_engines import mcsoda
import memcacheConstants

from queue import Queue
from threading import Thread

log = logger.Logger.get_logger()
try:
    import concurrent.futures
except ImportError:
    log.warn("{0} {1}".format("Can not import concurrent module.",
                              "Data for each server will be loaded/retrieved sequentially"))

class MemcachedClientHelperExcetion(Exception):
    def __init__(self, errorcode, message):
        Exception.__init__(self, errorcode, message)
        self._message = message
        self.errorcode = errorcode
        self._args = (errorcode, message)


class MemcachedClientHelper(object):
    # value_sizes {10:0.1,20:0.2:40:0.8}

    @staticmethod
    def create_threads(servers=None,
                       name='default',
                       ram_load_ratio=-1,
                       number_of_items=-1,
                       value_size_distribution=None,
                       number_of_threads=50,
                       override_vBucketId=-1,
                       write_only=False,
                       moxi=True,
                       async_write=False,
                       delete_ratio=0,
                       expiry_ratio=0,
                       collection=None):
        log = logger.Logger.get_logger()
        if not servers:
            raise MemcachedClientHelperExcetion(errorcode='invalid_argument',
                                                message="servers is not set")
        if ram_load_ratio < 0 and number_of_items < 0:
            raise MemcachedClientHelperExcetion(errorcode='invalid_argument',
                                                message="ram_load_ratio or number_of_items must be specified")
        if not value_size_distribution:
            value_size_distribution = {16: 0.25, 128: 0.25, 512: 0.25, 1024: 0.25}

        list = []

        if ram_load_ratio >= 0:
            info = RestConnection(servers[0]).get_bucket(name)
            emptySpace = info.stats.ram - info.stats.memUsed
            space_to_fill = (int((emptySpace * ram_load_ratio) / 100.0))
            log.info('space_to_fill : {0}, emptySpace : {1}'.format(space_to_fill, emptySpace))
            for size, probability in list(value_size_distribution.items()):
                how_many = int(space_to_fill / (size + 250) * probability)
                payload_generator = DocumentGenerator.make_docs(number_of_items,
                        {"name": "user-${prefix}", "payload": "memcached-json-${prefix}-${padding}",
                         "size": size, "seed": str(uuid.uuid4())})
                list.append({'size': size, 'value': payload_generator, 'how_many': how_many})
        else:
            for size, probability in value_size_distribution.items():
                how_many = ((number_of_items // number_of_threads) * probability)
                payload_generator = DocumentGenerator.make_docs(number_of_items,
                        {"name": "user-${prefix}", "payload": "memcached-json-${prefix}-${padding}",
                         "size": size, "seed": str(uuid.uuid4())})
                list.append({'size': size, 'value': payload_generator, 'how_many': how_many})

        for item in list:
            item['how_many'] //= int(number_of_threads)
            # at least one element for each value size
            if item['how_many'] < 1:
                item['how_many'] = 1
            msg = "each thread will send {0} items with value of size : {1}"
            log.info(msg.format(item['how_many'], item['size']))

        threads = []
        for i in range(0, int(number_of_threads)):
            # choose one of the servers random
            thread = WorkerThread(serverInfo=MemcachedClientHelper.random_pick(servers),
                                  name=name,
                                  values_list=list,
                                  override_vBucketId=override_vBucketId,
                                  write_only=write_only,
                                  moxi=moxi,
                                  async_write=async_write,
                                  delete_ratio=delete_ratio,
                                  expiry_ratio=expiry_ratio,
                                  collection=collection)
            threads.append(thread)

        return threads

    @staticmethod
    def create_threads_for_load_bucket(serverInfo=None,
                                       name='default',
                                       ram_load_ratio=-1,
                                       number_of_items=-1,
                                       value_size_distribution=None,
                                       number_of_threads=50,
                                       override_vBucketId=-1,
                                       write_only=False,
                                       moxi=True,
                                       delete_ratio=0,
                                       expiry_ratio=0,
                                       collection=None):
        log = logger.Logger.get_logger()
        if not serverInfo:
            raise MemcachedClientHelperExcetion(errorcode='invalid_argument',
                                                message="serverInfo is not set")
        if ram_load_ratio < 0 and number_of_items < 0:
            raise MemcachedClientHelperExcetion(errorcode='invalid_argument',
                                                message="ram_load_ratio or number_of_items must be specified")
        if not value_size_distribution:
            value_size_distribution = {16: 0.33, 128: 0.33, 1024: 0.33}

        list = []

        if ram_load_ratio >= 0:
            info = RestConnection(serverInfo).get_bucket(name)
            emptySpace = info.stats.ram - info.stats.memUsed
            space_to_fill = (int((emptySpace * ram_load_ratio) / 100.0))
            log.info('space_to_fill : {0}, emptySpace : {1}'.format(space_to_fill, emptySpace))
            for size, probability in list(value_size_distribution.items()):
                # let's assume overhead per key is 64 bytes ?
                how_many = int(space_to_fill / (size + 250) * probability)
                payload = MemcachedClientHelper.create_value('*', size)
                list.append({'size': size, 'value': payload, 'how_many': how_many})
        else:
            for size, probability in list(value_size_distribution.items()):
                how_many = (number_of_items * probability)
                payload = MemcachedClientHelper.create_value('*', size)
                list.append({'size': size, 'value': payload, 'how_many': how_many})

        for item in list:
            item['how_many'] //= int(number_of_threads)
            # at least one element for each value size
            if item['how_many'] < 1:
                item['how_many'] = 1
            msg = "each thread will send {0} items with value of size : {1}"
            log.info(msg.format(item['how_many'], item['size']))

        threads = []
        for i in range(0, int(number_of_threads)):
            thread = WorkerThread(serverInfo=serverInfo,
                                  name=name,
                                  values_list=list,
                                  override_vBucketId=override_vBucketId,
                                  write_only=write_only,
                                  moxi=moxi,
                                  delete_ratio=delete_ratio,
                                  expiry_ratio=expiry_ratio,
                                  collection=collection)
            threads.append(thread)

        return threads

    @staticmethod
    def load_bucket_and_return_the_keys(servers=None,
                                        name='default',
                                        ram_load_ratio=-1,
                                        number_of_items=-1,
                                        value_size_distribution=None,
                                        number_of_threads=50,
                                        override_vBucketId=-1,
                                        write_only=False,
                                        moxi=True,
                                        delete_ratio=0,
                                        expiry_ratio=0,
                                        collection=None):
        inserted_keys = []
        rejected_keys = []
        log = logger.Logger.get_logger()
        threads = MemcachedClientHelper.create_threads(servers,
                                                       name,
                                                       ram_load_ratio,
                                                       number_of_items,
                                                       value_size_distribution,
                                                       number_of_threads,
                                                       override_vBucketId,
                                                       write_only=write_only,
                                                       moxi=moxi,
                                                       delete_ratio=delete_ratio,
                                                       expiry_ratio=expiry_ratio,
                                                       collection=collection)

        # we can start them!
        for thread in threads:
            thread.start()
        log.info("waiting for all worker thread to finish their work...")
        [thread.join() for thread in threads]
        log.info("worker threads are done...")

        inserted_count = 0
        rejected_count = 0
        deleted_count = 0
        expired_count = 0
        for thread in threads:
            t_inserted, t_rejected = thread.keys_set()
            inserted_count += thread.inserted_keys_count()
            rejected_count += thread.rejected_keys_count()
            deleted_count += thread._delete_count
            expired_count += thread._expiry_count
            inserted_keys.extend(t_inserted)
            rejected_keys.extend(t_rejected)
        msg = "inserted keys count : {0} , rejected keys count : {1}"
        log.info(msg.format(inserted_count, rejected_count))
        msg = "deleted keys count : {0} , expired keys count : {1}"
        log.info(msg.format(deleted_count, expired_count))
        return inserted_keys, rejected_keys

    @staticmethod
    def load_bucket(servers,
                    name='default',
                    ram_load_ratio=-1,
                    number_of_items=-1,
                    value_size_distribution=None,
                    number_of_threads=50,
                    override_vBucketId=-1,
                    write_only=False,
                    moxi=True,
                    collection=None):
        inserted_keys_count = 0
        rejected_keys_count = 0
        log = logger.Logger.get_logger()
        threads = MemcachedClientHelper.create_threads(servers,
                                                       name,
                                                       ram_load_ratio,
                                                       number_of_items,
                                                       value_size_distribution,
                                                       number_of_threads,
                                                       override_vBucketId,
                                                       write_only,
                                                       moxi,
                                                       collection=collection)
        # we can start them!
        for thread in threads:
            thread.start()
        log.info("waiting for all worker thread to finish their work...")
        [thread.join() for thread in threads]
        log.info("worker threads are done...")
        for thread in threads:
            inserted_keys_count += thread.inserted_keys_count()
            rejected_keys_count += thread.rejected_keys_count()
        msg = "inserted keys count : {0} , rejected keys count : {1}"
        log.info(msg.format(inserted_keys_count, rejected_keys_count))
        return inserted_keys_count, rejected_keys_count

    @staticmethod
    def create_value(pattern, size):
        return (pattern * (size // len(pattern))) + pattern[0:(size % len(pattern))]

    @staticmethod
    def random_pick(list):
        if list:
            if len(list) > 1:
                return list[Random().randint(0, len(list) - 1)]
            return list[0]
            # raise array empty ?
        return None

    @staticmethod
    def direct_client(server, bucket, timeout=30, admin_user='cbadminbucket',admin_pass='password'):
        log = logger.Logger.get_logger()
        rest = RestConnection(server)
        node = None
        try:
            node = rest.get_nodes_self()
        except ValueError as e:
            log.info("could not connect to server {0}, will try scanning all nodes".format(server))
        if not node:
            nodes = rest.get_nodes()
            for n in nodes:
                if n.ip == server.ip and n.port == server.port:
                    node = n

        if isinstance(server, dict):
            log.info("dict:{0}".format(server))
            log.info("creating direct client {0}:{1} {2}".format(server["ip"], node.memcached, bucket))
        else:
            log.info("creating direct client {0}:{1} {2}".format(server.ip, node.memcached, bucket))
        RestHelper(rest).vbucket_map_ready(bucket, 60)
        vBuckets = RestConnection(server).get_vbuckets(bucket)
        if isinstance(server, dict):
            client = MemcachedClient(server["ip"], node.memcached, timeout=timeout)
        else:
            client = MemcachedClient(server.ip, node.memcached, timeout=timeout)
        if vBuckets != None:
            client.vbucket_count = len(vBuckets)
        else:
            client.vbucket_count = 0
        bucket_info = rest.get_bucket(bucket)
        # todo raise exception for not bucket_info

        cluster_compatibility = rest.check_cluster_compatibility("5.0")
        if cluster_compatibility is None:
            pre_spock = True
        else:
            pre_spock = not cluster_compatibility
        if pre_spock:
            log.info("Atleast 1 of the server is on pre-spock "
                     "version. Using the old ssl auth to connect to "
                     "bucket.")
            client.sasl_auth_plain(bucket_info.name.encode('ascii'),
                                    bucket_info.saslPassword.encode('ascii'))
        else:
            if isinstance(bucket, Bucket):
                bucket = bucket.name
            bucket = bucket.encode('ascii')
            client.sasl_auth_plain(admin_user, admin_pass)
            client.bucket_select(bucket)

        return client

    @staticmethod
    def proxy_client(server, bucket, timeout=30, force_ascii=False, standalone_moxi_port=None):
        # for this bucket on this node what is the proxy ?
        rest = RestConnection(server)
        log = logger.Logger.get_logger()
        bucket_info = rest.get_bucket(bucket)
        nodes = bucket_info.nodes

        if (TestInputSingleton.input and "ascii" in TestInputSingleton.input.test_params \
            and TestInputSingleton.input.test_params["ascii"].lower() == "true")\
            or force_ascii:
            ascii = True
        else:
            ascii = False
        for node in nodes:
            RestHelper(rest).vbucket_map_ready(bucket, 60)
            vBuckets = rest.get_vbuckets(bucket)
            port_moxi = standalone_moxi_port or node.moxi
            if ascii:
                log = logger.Logger.get_logger()
                log.info("creating ascii client {0}:{1} {2}".format(server.ip, port_moxi, bucket))
                client = MemcachedAsciiClient(server.ip, port_moxi, timeout=timeout)
            else:
                log = logger.Logger.get_logger()
                if isinstance(server, dict):
                    log.info("creating proxy client {0}:{1} {2}".format(server["ip"], port_moxi, bucket))
                    client = MemcachedClient(server["ip"], port_moxi, timeout=timeout)
                else:
                    log.info("creating proxy client {0}:{1} {2}".format(server.ip, port_moxi, bucket))
                    client = MemcachedClient(server.ip, port_moxi, timeout=timeout)
                client.vbucket_count = len(vBuckets)
                if bucket_info.authType == "sasl":
                    client.sasl_auth_plain(bucket_info.name,
                                           bucket_info.saslPassword)
            return client
        if isinstance(server, dict):
            raise Exception("unable to find {0} in get_nodes()".format(server["ip"]))
        else:
            raise Exception("unable to find {0} in get_nodes()".format(server.ip))

    @staticmethod
    def standalone_moxi_client(server, bucket, timeout=30, moxi_port=None):
        log = logger.Logger.get_logger()
        if isinstance(server, dict):
            log.info("creating proxy client {0}:{1} {2}".format(server["ip"], moxi_port, bucket.name))
            client = MemcachedClient(server["ip"], moxi_port, timeout=timeout)
        else:
            log.info("creating proxy client {0}:{1} {2}".format(server.ip, moxi_port, bucket.name))
            client = MemcachedClient(server.ip, moxi_port, timeout=timeout)
        if bucket.name != 'default' and bucket.authType == "sasl":
            client.sasl_auth_plain(bucket.name.encode('ascii'),
                                   bucket.saslPassword.encode('ascii'))
        return client
        if isinstance(server, dict):
            raise Exception("unable to find {0} in get_nodes()".format(server["ip"]))
        else:
            raise Exception("unable to find {0} in get_nodes()".format(server.ip))

    @staticmethod
    def flush_bucket(server, bucket, admin_user='cbadminbucket',admin_pass='password'):
        # if memcached throws OOM error try again ?
        log = logger.Logger.get_logger()
        client = MemcachedClientHelper.direct_client(server, bucket, admin_user=admin_user, admin_pass=admin_pass)
        retry_attempt = 5
        while retry_attempt > 0:
            try:
                client.flush()
                log.info('flushed bucket {0}...'.format(bucket))
                break
            except MemcachedError:
                retry_attempt -= 1
                log = logger.Logger.get_logger()
                log.info('flush raised memcached error trying again in 5 seconds...')
                time.sleep(5)
        client.close()
        return


class MutationThread(threading.Thread):
    def run(self, collection=None):
        values = DocumentGenerator.make_docs(len(self.keys),
                {"name": "user-${prefix}", "payload": "memcached-json-${prefix}-${padding}",
                 "size": 1024, "seed": self.seed})
        client = MemcachedClientHelper.proxy_client(self.serverInfo, self.name)
        counter = 0
        for value in values:
            try:
                if self.op == "set":
                    client.set(self.keys[counter], 0, 0, value, collection=collection)
                    self._mutated_count += 1
            except MemcachedError:
                self._rejected_count += 1
                self._rejected_keys.append({"key": self.keys[counter], "value": value})
            except Exception as e:
                self.log.info("unable to mutate {0} due to {1}".format(self.keys[counter], e))
                self._rejected_count += 1
                self._rejected_keys.append({"key": self.keys[counter], "value": value})
                client.close()
                client = MemcachedClientHelper.proxy_client(self.serverInfo, self.name)
            counter = counter + 1
        self.log.info("mutation failed {0} times".format(self._rejected_count))
        client.close()

    def __init__(self, serverInfo,
                 keys,
                 op,
                 seed,
                 name='default',
                 collection=None):
        threading.Thread.__init__(self)
        self.log = logger.Logger.get_logger()
        self.serverInfo = serverInfo
        self.name = name
        self.collection=collection
        self.keys = keys
        self.op = op
        self.seed = seed
        self._mutated_count = 0
        self._rejected_count = 0
        self._rejected_keys = []


class ReaderThread(object):
    def __init__(self, info, keyset, queue, collection=None):
        self.info = info
        self.log = logger.Logger.get_logger()
        self.error_seen = 0
        self.keyset = keyset
        self.aborted = False
        self.queue = queue
        self.collection=collection

    def abort(self):
        self.aborted = True

    def _saw_error(self, key):
    #        error_msg = "unable to get key {0}"
        self.error_seen += 1

    #        if self.error_seen < 500:
    #            self.log.error(error_msg.format(key))

    def start(self):
        client = MemcachedClientHelper.direct_client(self.info["server"], self.info['name'], admin_user='cbadminbucket',
                                                     admin_pass='password')
        time.sleep(5)
        while self.queue.empty() and self.keyset:
            selected = MemcachedClientHelper.random_pick(self.keyset)
            selected['how_many'] -= 1
            if selected['how_many'] < 1:
                self.keyset.remove(selected)
            key = "{0}-{1}-{2}".format(self.info['baseuuid'],
                                       selected['size'],
                                       int(selected['how_many']))
            try:
                client.send_get(key, self.collection)
            except Exception:
                self._saw_error(key)
                #        self.log.warn("attempted to get {0} keys before they are set".format(self.error_seen))
        client.close()


# mutation ? let' do two cycles , first run and then try to mutate all those itesm
# and return
class WorkerThread(threading.Thread):
    # too flags : stop after x errors
    # slow down after every seeing y errors
    # value_list is a list of document generators
    def __init__(self,
                 serverInfo,
                 name,
                 values_list,
                 ignore_how_many_errors=5000,
                 override_vBucketId=-1,
                 terminate_in_minutes=120,
                 write_only=False,
                 moxi=True,
                 async_write=False,
                 delete_ratio=0,
                 expiry_ratio=0,
                 collection=None):
        threading.Thread.__init__(self)
        self.log = logger.Logger.get_logger()
        self.serverInfo = serverInfo
        self.name = name
        self.collection=collection
        self.values_list = []
        self.values_list.extend(copy.deepcopy(values_list))
        self._value_list_copy = []
        self._value_list_copy.extend(copy.deepcopy(values_list))
        self._inserted_keys_count = 0
        self._rejected_keys = []
        self._rejected_keys_count = 0
        self._delete_ratio = delete_ratio
        self._expiry_ratio = expiry_ratio
        self._delete_count = 0
        self._expiry_count = 0
        self._delete = []
        self.ignore_how_many_errors = ignore_how_many_errors
        self.override_vBucketId = override_vBucketId
        self.terminate_in_minutes = terminate_in_minutes
        self._base_uuid = uuid.uuid4()
        self.queue = Queue()
        self.moxi = moxi
        # let's create a read_thread
        self.info = {'server': serverInfo,
                     'name': self.name,
                     'baseuuid': self._base_uuid,
                     'collection': self.collection}
        self.write_only = write_only
        self.aborted = False
        self.async_write = async_write

    def inserted_keys_count(self):
        return self._inserted_keys_count

    def rejected_keys_count(self):
        return self._rejected_keys_count

    # smart functin that gives you sth you can use to
    # get inserted keys
    # we should just expose an iterator instead which
    # generates the key,values on fly
    def keys_set(self):
        # let's construct the inserted keys set
        # TODO: hard limit , let's only populated up to 1 million keys
        inserted_keys = []
        for item in self._value_list_copy:
            for i in range(0, (int(item['how_many']))):
                key = "{0}-{1}-{2}".format(self._base_uuid, item['size'], i)
                if key not in self._rejected_keys:
                    inserted_keys.append(key)
                if len(inserted_keys) > 2 * 1024 * 1024:
                    break
        return inserted_keys, self._rejected_keys

    def run(self):
        msg = "starting a thread to set keys mixed set-get ? {0} and using async_set ? {1}"
        msg += " with moxi ? {2}"
        msg = msg.format(self.write_only, self.async_write, self.moxi)
        self.log.info(msg)
        try:
            awareness = VBucketAwareMemcached(RestConnection(self.serverInfo), self.name)
            client = None
            if self.moxi:
                client = MemcachedClientHelper.proxy_client(self.serverInfo, self.name)
        except Exception as ex:
            self.log.info("unable to create memcached client due to {0}. stop thread...".format(ex))
            import traceback
            traceback.print_exc()
            return
        # keeping keys in the memory is not such a good idea because
        # we run out of memory so best is to just keep a counter ?
        # if someone asks for the keys we can give them the formula which is
        # baseuuid-{0}-{1} , size and counter , which is between n-0 except those
        # keys which were rejected
        # let's print out some status every 5 minutes..

        if not self.write_only:
            self.reader = Process(target=start_reader_process, args=(self.info, self._value_list_copy, self.queue))
            self.reader.start()
        start_time = time.time()
        last_reported = start_time
        backoff_count = 0
        while len(self.values_list) > 0 and not self.aborted:
            selected = MemcachedClientHelper.random_pick(self.values_list)
            selected['how_many'] -= 1
            if selected['how_many'] < 1:
                self.values_list.remove(selected)
            if (time.time() - start_time) > self.terminate_in_minutes * 60:
                self.log.info("its been more than {0} minutes loading data. stopping the process..".format(
                    self.terminate_in_minutes))
                break
            else:
                # every two minutes print the status
                if time.time() - last_reported > 2 * 60:
                    if not self.moxi:
                        awareness.done()
                        try:
                            awareness = VBucketAwareMemcached(RestConnection(self.serverInfo), self.name)
                        except Exception:
                            # vbucket map is changing . sleep 5 seconds
                            time.sleep(5)
                            awareness = VBucketAwareMemcached(RestConnection(self.serverInfo), self.name)
                        self.log.info("now connected to {0} memcacheds".format(len(awareness.memcacheds)))
                    last_reported = time.time()
                    for item in self.values_list:
                        self.log.info(
                            '{0} keys (each {1} bytes) more to send...'.format(item['how_many'], item['size']))

            key = "{0}-{1}-{2}".format(self._base_uuid,
                                       selected['size'],
                                       int(selected['how_many']))
            if not self.moxi:
                client = awareness.memcached(key)
                if not client:
                    self.log.error("client should not be null")
            value = "*"
            try:
                value = next(selected["value"])
            except StopIteration:
                pass
            try:
                if self.override_vBucketId >= 0:
                    client.vbucketId = self.override_vBucketId
                if self.async_write:
                    client.send_set(key, 0, 0, value, self.collection)
                else:
                    client.set(key, 0, 0, value, self.collection)
                self._inserted_keys_count += 1
                backoff_count = 0
                # do expiry sets, 30 second expiry time
                if Random().random() < self._expiry_ratio:
                    client.set(key + "-exp", 30, 0, value, self.collection)
                    self._expiry_count += 1
                    # do deletes if we have 100 pending
                # at the end delete the remaining
                if len(self._delete) >= 100:
                #                    self.log.info("deleting {0} keys".format(len(self._delete)))
                    for key_del in self._delete:
                        client.delete(key_del, self.collection)
                    self._delete = []
                    # do delete sets
                if Random().random() < self._delete_ratio:
                    client.set(key + "-del", 0, 0, value, self.collection)
                    self._delete.append(key + "-del")
                    self._delete_count += 1
            except MemcachedError as error:
                if not self.moxi:
                    awareness.done()
                    try:
                        awareness = VBucketAwareMemcached(RestConnection(self.serverInfo), self.name)
                    except Exception:
                        # vbucket map is changing . sleep 5 seconds
                        time.sleep(5)
                        awareness = VBucketAwareMemcached(RestConnection(self.serverInfo), self.name)
                    self.log.info("now connected to {0} memcacheds".format(len(awareness.memcacheds)))
                    if isinstance(self.serverInfo, dict):
                        self.log.error(
                            "memcached error {0} {1} from {2}".format(error.status, error.msg, self.serverInfo["ip"]))
                    else:
                        self.log.error(
                            "memcached error {0} {1} from {2}".format(error.status, error.msg, self.serverInfo.ip))
                if error.status == 134:
                    backoff_count += 1
                    if backoff_count < 5:
                        backoff_seconds = 15 * backoff_count
                    else:
                        backoff_seconds = 2 * backoff_count
                    self.log.info("received error # 134. backing off for {0} sec".format(backoff_seconds))
                    time.sleep(backoff_seconds)

                self._rejected_keys_count += 1
                self._rejected_keys.append({"key": key, "value": value})
                if len(self._rejected_keys) > self.ignore_how_many_errors:
                    break
            except Exception as ex:
                if not self.moxi:
                    awareness.done()
                    try:
                        awareness = VBucketAwareMemcached(RestConnection(self.serverInfo), self.name)
                    except Exception:
                        awareness = VBucketAwareMemcached(RestConnection(self.serverInfo), self.name)
                    self.log.info("now connected to {0} memcacheds".format(len(awareness.memcacheds)))
                if isinstance(self.serverInfo, dict):
                    self.log.error("error {0} from {1}".format(ex, self.serverInfo["ip"]))
                    import  traceback
                    traceback.print_exc()
                else:
                    self.log.error("error {0} from {1}".format(ex, self.serverInfo.ip))
                self._rejected_keys_count += 1
                self._rejected_keys.append({"key": key, "value": value})
                if len(self._rejected_keys) > self.ignore_how_many_errors:
                    break

                    # before closing the session let's try sending those items again
        retry = 3
        while retry > 0 and self._rejected_keys_count > 0:
            rejected_after_retry = []
            self._rejected_keys_count = 0
            for item in self._rejected_keys:
                try:
                    if self.override_vBucketId >= 0:
                        client.vbucketId = self.override_vBucketId
                    if self.async_write:
                        client.send_set(item["key"], 0, 0, item["value"], self.collection)
                    else:
                        client.set(item["key"], 0, 0, item["value"], self.collection)
                    self._inserted_keys_count += 1
                except MemcachedError:
                    self._rejected_keys_count += 1
                    rejected_after_retry.append({"key": item["key"], "value": item["value"]})
                    if len(rejected_after_retry) > self.ignore_how_many_errors:
                        break
            self._rejected_keys = rejected_after_retry
            retry = -1
            # clean up the rest of the deleted keys
            if len(self._delete) > 0:
            #                self.log.info("deleting {0} keys".format(len(self._delete)))
                for key_del in self._delete:
                    client.delete(key_del, self.collection)
                self._delete = []

            self.log.info("deleted {0} keys".format(self._delete_count))
            self.log.info("expiry {0} keys".format(self._expiry_count))
            #        client.close()
        awareness.done()
        if not self.write_only:
            self.queue.put_nowait("stop")
            self.reader.join()

    def _initialize_memcached(self):
        pass

    def _set(self):
        pass

    def _handle_error(self):
        pass
        # if error is memcached error oom related let's do a sleep

    def _time_to_stop(self):
        return self.aborted or len(self._rejected_keys) > self.ignore_how_many_errors


class VBucketAwareMemcached(object):
    def __init__(self, rest, bucket, info=None, collection=None):
        self.log = logger.Logger.get_logger()
        self.info = info
        self.bucket = bucket
        if isinstance(bucket, Bucket):
            self.bucket = bucket.name
        self.memcacheds = {}
        self.vBucketMap = {}
        self.vBucketMapReplica = {}
        self.rest = rest
        self.reset(rest)
        self.collections=collection

    def reset(self, rest=None):
        if not rest:
            self.rest = RestConnection(self.info)
        m, v, r = self.request_map(self.rest, self.bucket)
        self.memcacheds = m
        self.vBucketMap = v
        self.vBucketMapReplica = r

    def reset_vbuckets(self, rest, vbucketids_set, forward_map=None, admin_user='cbadminbucket',admin_pass='password'):
        if not forward_map:
            forward_map = rest.get_bucket(self.bucket, num_attempt=2).forward_map
            if not forward_map:
                self.reset(rest)
                forward_map = rest.get_vbuckets(self.bucket)
        nodes = rest.get_nodes()
        for vBucket in forward_map:
            if vBucket.id in vbucketids_set:
                self.vBucketMap[vBucket.id] = vBucket.master
                masterIp = vBucket.master.rsplit(":", 1)[0]
                masterPort = int(vBucket.master.rsplit(":", 1)[1])
                if self.vBucketMap[vBucket.id] not in self.memcacheds:
                    server = TestInputServer()
                    server.rest_username = rest.username
                    server.rest_password = rest.password
                    for node in nodes:
                        if node.ip == masterIp and node.memcached == masterPort:
                            server.port = node.port
                    server.ip = masterIp
                    self.log.info("Received forward map, reset vbucket map, new direct_client")
                    self.memcacheds[vBucket.master] = MemcachedClientHelper.direct_client(server, self.bucket,
                                                                    admin_user=admin_user, admin_pass=admin_pass)
                # if no one is using that memcached connection anymore just close the connection
                used_nodes = {self.vBucketMap[vb_name] for vb_name in self.vBucketMap}
                rm_clients = []
                for memcache_con in self.memcacheds:
                    if memcache_con not in used_nodes:
                        rm_clients.append(memcache_con)
                for rm_cl in rm_clients:
                    self.memcacheds[rm_cl].close()
                    del self.memcacheds[rm_cl]
                self.vBucketMapReplica[vBucket.id] = vBucket.replica
                for replica in vBucket.replica:
                    self.add_memcached(replica, self.memcacheds, self.rest, self.bucket)
        return True

    def request_map(self, rest, bucket):
        memcacheds = {}
        vBucketMap = {}
        vBucketMapReplica = {}
        vb_ready = RestHelper(rest).vbucket_map_ready(bucket, 60)
        if not vb_ready:
            raise Exception("vbucket map is not ready for bucket {0}".format(bucket))
        vBuckets = rest.get_vbuckets(bucket)
        for vBucket in vBuckets:
            vBucketMap[vBucket.id] = vBucket.master
            self.add_memcached(vBucket.master, memcacheds, rest, bucket)

            vBucketMapReplica[vBucket.id] = vBucket.replica
            for replica in vBucket.replica:
                self.add_memcached(replica, memcacheds, rest, bucket)
        return memcacheds, vBucketMap, vBucketMapReplica

    def add_memcached(self, server_str, memcacheds, rest, bucket, admin_user='cbadminbucket', admin_pass='password'):
        if not server_str in memcacheds:
            serverIp = server_str.rsplit(":", 1)[0]
            serverPort = int(server_str.rsplit(":", 1)[1])
            nodes = rest.get_nodes()
            server = TestInputServer()
            server.ip = serverIp
            server.port = rest.port
            server.rest_username = rest.username
            server.rest_password = rest.password
            try:
                for node in nodes:
                    if node.ip == serverIp and node.memcached == serverPort:
                        if server_str not in memcacheds:
                            server.port = node.port
                            memcacheds[server_str] = \
                                MemcachedClientHelper.direct_client(server, bucket, admin_user=admin_user,
                                                                    admin_pass=admin_pass)
                            #self.enable_collection(memcacheds[server_str])
                        break
            except Exception as ex:
                msg = "unable to establish connection to {0}. cleanup open connections"
                self.log.warn(msg.format(serverIp))
                self.done()
                raise ex


    def memcached(self, key, replica_index=None):
        vBucketId = self._get_vBucket_id(key)
        if replica_index is None:
            return self.memcached_for_vbucket(vBucketId)
        else:
            return self.memcached_for_replica_vbucket(vBucketId, replica_index)

    def memcached_for_vbucket(self, vBucketId):
        if vBucketId not in self.vBucketMap:
            msg = "vbucket map does not have an entry for vb : {0}"
            raise Exception(msg.format(vBucketId))
        if self.vBucketMap[vBucketId] not in self.memcacheds:
            msg = "moxi does not have a mc connection for server : {0}"
            raise Exception(msg.format(self.vBucketMap[vBucketId]))
        return self.memcacheds[self.vBucketMap[vBucketId]]

    def memcached_for_replica_vbucket(self, vBucketId, replica_index=0, log_on = False):
        if vBucketId not in self.vBucketMapReplica:
            msg = "replica vbucket map does not have an entry for vb : {0}"
            raise Exception(msg.format(vBucketId))
        if log_on:
          self.log.info("replica vbucket: vBucketId {0}, server{1}".format(vBucketId, self.vBucketMapReplica[vBucketId][replica_index]))
        if self.vBucketMapReplica[vBucketId][replica_index] not in self.memcacheds:
            msg = "moxi does not have a mc connection for server : {0}"
            raise Exception(msg.format(self.vBucketMapReplica[vBucketId][replica_index]))
        return self.memcacheds[self.vBucketMapReplica[vBucketId][replica_index]]

    def not_my_vbucket_memcached(self, key, collection=None):
        vBucketId = self._get_vBucket_id(key)
        which_mc = self.vBucketMap[vBucketId]
        for server in self.memcacheds:
            if server != which_mc:
                return self.memcacheds[server]

# DECORATOR
    def aware_call(func):
      def new_func(self, key, *args, **keyargs):
        vb_error = 0
        while True:
            try:
                return func(self, key, *args, **keyargs)
            except MemcachedError as error:
                if error.status == ERR_NOT_MY_VBUCKET and vb_error < 5:
                    self.reset_vbuckets(self.rest, {self._get_vBucket_id(key)},
                                        forward_map=self._parse_not_my_vbucket_error(error))
                    vb_error += 1
                else:
                    raise error
            except (EOFError, socket.error) as error:
                if "Got empty data (remote died?)" in error.message or \
                   "Timeout waiting for socket" in error.message or \
                   "Broken pipe" in error.message or "Connection reset by peer" in error.message \
                    and vb_error < 5:
                    self.reset_vbuckets(self.rest, {self._get_vBucket_id(key)})
                    vb_error += 1
                else:
                    raise error
            except BaseException as error:
                if vb_error < 5:
                    self.reset_vbuckets(self.rest, {self._get_vBucket_id(key)})
                    vb_error += 1
                else:
                    raise error
      return new_func

# SUBDOCS

    @aware_call
    def counter_sd(self, key, path, value, expiry=0, opaque=0, cas=0, create=False, collection=None):
        return self._send_op(self.memcached(key).counter_sd, key, path, value, expiry=expiry, opaque=opaque, cas=cas, create=create, collection=collection)

    @aware_call
    def array_add_insert_sd(self, key, path, value, expiry=0, opaque=0, cas=0, create=False, collection=None):
        return self._send_op(self.memcached(key).array_add_insert_sd, key, path, value, expiry=expiry, opaque=opaque, cas=cas, create=create, collection=collection)

    @aware_call
    def array_add_unique_sd(self, key, path, value, expiry=0, opaque=0, cas=0, create=False, collection=None):
        return self._send_op(self.memcached(key).array_add_unique_sd, key, path, value, expiry=expiry, opaque=opaque, cas=cas, create=create, collection=collection)

    @aware_call
    def array_push_first_sd(self, key, path, value, expiry=0, opaque=0, cas=0, create=False, collection=None):
        return self._send_op(self.memcached(key).array_push_first_sd, key, path, value, expiry=expiry, opaque=opaque, cas=cas, create=create, collection=collection)

    @aware_call
    def array_push_last_sd(self, key, path, value, expiry=0, opaque=0, cas=0, create=False, collection=None):
        return self._send_op(self.memcached(key).array_push_last_sd, key, path, value, expiry=expiry, opaque=opaque, cas=cas, create=create, collection=collection)

    @aware_call
    def replace_sd(self, key, path, value, expiry=0, opaque=0, cas=0, create=False, collection=None):
        return self._send_op(self.memcached(key).replace_sd, key, path, value, expiry=expiry, opaque=opaque, cas=cas, create=create, collection=collection)

    @aware_call
    def delete_sd(self, key, path, opaque=0, cas=0, collection=None):
        return self._send_op(self.memcached(key).delete_sd, key, path, opaque=opaque, cas=cas, collection=collection)

    @aware_call
    def dict_upsert_sd(self, key, path, value, expiry=0, opaque=0, cas=0, create=False, collection=None):
        return self._send_op(self.memcached(key).dict_upsert_sd, key, path, value, expiry=expiry, opaque=opaque, cas=cas, create=create, collection=collection)

    @aware_call
    def dict_add_sd(self, key, path, value, expiry=0, opaque=0, cas=0, create=False, collection=None):
        return self._send_op(self.memcached(key).dict_add_sd, key, path, value, expiry=expiry, opaque=opaque, cas=cas, create=create, collection=collection)

    @aware_call
    def exists_sd(self, key, path, cas=0, collection=None):
        return self._send_op(self.memcached(key).exists_sd, key, path, cas=cas, collection=collection)

    @aware_call
    def get_sd(self, key, path, cas=0, collection=None):
        return self._send_op(self.memcached(key).get_sd, key, path, cas=cas, collection=collection)

    @aware_call
    def set(self, key, exp, flags, value, collection=None):
        return self._send_op(self.memcached(key).set, key, exp, flags, value, collection=collection)

    @aware_call
    def append(self, key, value, collection=None):
        return self._send_op(self.memcached(key).append, key, value, collection=collection)

    @aware_call
    def observe(self, key, collection=None):
        return self._send_op(self.memcached(key).observe, key, collection=collection)

    @aware_call
    def observe_seqno(self, key, vbucket_uuid, collection=None):
        return self._send_op(self.memcached(key).observe_seqno, key, vbucket_uuid, collection=collection)

    # This saves a lot of repeated code - the func is the mc bin client function

    def generic_request(self, func, *args):
        key = args[0]
        vb_error = 0
        while True:
            try:
                return self._send_op(func, *args)
            except MemcachedError as error:
                if error.status == ERR_NOT_MY_VBUCKET and vb_error < 5:
                    self.reset_vbuckets(self.rest, {self._get_vBucket_id(key)},
                                        forward_map=self._parse_not_my_vbucket_error(error))
                    vb_error += 1
                else:
                    raise error
            except (EOFError, socket.error) as error:
                if "Got empty data (remote died?)" in error.message or \
                   "Timeout waiting for socket" in error.message or \
                   "Broken pipe" in error.message or "Connection reset by peer" in error.message \
                    and vb_error < 5:
                    self.reset_vbuckets(self.rest, {self._get_vBucket_id(key)})
                    vb_error += 1
                    if vb_error >= 5:
                        raise error
                else:
                    raise error
            except BaseException as error:
                if vb_error < 5:
                    self.reset_vbuckets(self.rest, {self._get_vBucket_id(key)})
                    self.log.info("***************resetting vbucket id***********")
                    vb_error += 1
                else:
                    raise error

    def get(self, key, collection=None):
        vb_error = 0
        while True:
            try:
                return self._send_op(self.memcached(key).get, key, collection=collection)
            except MemcachedError as error:
                if error.status == ERR_NOT_MY_VBUCKET and vb_error < 5:
                    self.reset_vbuckets(self.rest, {self._get_vBucket_id(key)},
                                        forward_map=self._parse_not_my_vbucket_error(error))
                    vb_error += 1
                else:
                    raise error
            except (EOFError, socket.error) as error:
                if "Got empty data (remote died?)" in error.message or \
                   "Timeout waiting for socket" in error.message or\
                   "Broken pipe" in error.message or "Connection reset by peer" in error.message \
                    and vb_error < 5:
                    self.reset_vbuckets(self.rest, {self._get_vBucket_id(key)})
                    vb_error += 1
                else:
                    raise error
            except BaseException as error:
                if vb_error < 5:
                    self.reset_vbuckets(self.rest, {self._get_vBucket_id(key)})
                    vb_error += 1
                else:
                    raise error

    def getr(self, key, replica_index=0, collection=None):
        vb_error = 0
        while True:
            try:
                vBucketId = self._get_vBucket_id(key)
                return self._send_op(self.memcached(key, replica_index=replica_index).getr, key, collection=collection)
            except MemcachedError as error:
                if error.status == ERR_NOT_MY_VBUCKET and vb_error < 5:
                    self.reset_vbuckets(self.rest, {self._get_vBucket_id(key)},
                                        forward_map=self._parse_not_my_vbucket_error(error))
                    vb_error += 1
                else:
                    raise error
            except (EOFError, socket.error) as error:
                if "Got empty data (remote died?)" in error.message or \
                   "Timeout waiting for socket" in error.message or\
                   "Broken pipe" in error.message or "Connection reset by peer" in error.message \
                    and vb_error < 5:
                    self.reset_vbuckets(self.rest, {self._get_vBucket_id(key)})
                    vb_error += 1
                else:
                    raise error
            except BaseException as error:
                if vb_error < 5:
                    self.reset_vbuckets(self.rest, {self._get_vBucket_id(key)})
                    vb_error += 1
                else:
                    raise error

    def setMulti(self, exp, flags, key_val_dic, pause_sec=1, timeout_sec=5, parallel=False, collection=None):

        if parallel:
            try:
                import concurrent.futures
                self._setMulti_parallel(exp, flags, key_val_dic, pause_sec, timeout_sec, collection=collection)
            except ImportError:
                self._setMulti_seq(exp, flags, key_val_dic, pause_sec, timeout_sec, collection=collection)
        else:
            self._setMulti_seq(exp, flags, key_val_dic, pause_sec, timeout_sec, collection=collection)


    def _setMulti_seq(self, exp, flags, key_val_dic, pause_sec=1, timeout_sec=5, collection=None):
        # set keys in their respective vbuckets and identify the server for each vBucketId

        server_keyval = self._get_server_keyval_dic(key_val_dic)

        # get memcached client against each server and multi set
        for server_str, keyval in list(server_keyval.items()):
            #if the server has been removed after server_keyval has been gotten
            if server_str not in self.memcacheds:
                self._setMulti_seq(exp, flags, key_val_dic, pause_sec, timeout_sec, collection=collection)
            else:

                mc = self.memcacheds[server_str]

                errors = self._setMulti_rec(mc, exp, flags, keyval, pause_sec,
                                            timeout_sec, self._setMulti_seq, collection=collection)
                if errors:
                    self.log.error(list(set(str(error) for error in errors)), exc_info=1)
                    raise errors[0]

    def _setMulti_parallel(self, exp, flags, key_val_dic, pause_sec=1, timeout_sec=5, collection=None):
        # set keys in their respective vbuckets and identify the server for each vBucketId
        server_keyval = self._get_server_keyval_dic(key_val_dic)
        # get memcached client against each server and multi set
        tasks = []
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(server_keyval)) as executor:
            for server_str, keyval in list(server_keyval.items()) :
                mc = self.memcacheds[server_str]
                tasks.append(executor.submit(self._setMulti_rec, mc, exp, flags, keyval, pause_sec, timeout_sec, collection, self._setMulti_parallel))
            errors = []
            now = time.time()
            for future in concurrent.futures.as_completed(tasks, timeout_sec):
                if future.exception() is not None:
                    self.log.error("exception in {0} sec".format(time.time() - now))
                    raise future.exception()
                errors.extend(future.result())

            if errors:
                self.log.error(list(set(str(error) for error in errors)), exc_info=1)
                raise errors[0]

    def enable_collection(self, memcached_client,bucket="default"):
        memcached_client.bucket_select(bucket)
        memcached_client.enable_collections()
        memcached_client.hello(memcacheConstants.FEATURE_COLLECTIONS)
        memcached_client.get_collections(True)

    def _setMulti_rec(self, memcached_client, exp, flags, keyval, pause, timeout, rec_caller_fn, collection=None):
        try:
            errors = memcached_client.setMulti(exp, flags, keyval, collection=collection)

            if not errors:
                return []
            elif timeout <= 0:
                return errors
            else:
                time.sleep(pause)
                self.reset_vbuckets(self.rest, self._get_vBucket_ids(list(keyval.keys())))
                try:
                    rec_caller_fn(exp, flags, keyval, pause, timeout - pause, collection=collection)  # Start all over again for these key vals.
                except MemcachedError as error:
                    if error.status == ERR_2BIG:
                        self.log.info("<MemcachedError #%d ``%s''>" % (error.status, error.msg))
                        return []
                    else:
                        return [error]
                return []  # Note: If used for async,too many recursive threads could get spawn here.
        except (EOFError, socket.error) as error:
            try:
                if "Got empty data (remote died?)" in error.strerror or \
                   "Timeout waiting for socket" in error.strerror or \
                   "Broken pipe" in error.strerror or \
                   "Connection reset by peer" in error.strerror\
                    and timeout > 0:
                    time.sleep(pause)
                    self.reset_vbuckets(self.rest, self._get_vBucket_ids(list(keyval.keys())))
                    rec_caller_fn(exp, flags, keyval, pause, timeout - pause)
                    return []
                else:
                    return [error]
            except AttributeError:
                # noinspection PyPackageRequirements
                if "Got empty data (remote died?)" in error.message or \
                   "Timeout waiting for socket" in error.message or \
                   "Broken pipe" in error.message or \
                   "Connection reset by peer" in error.message\
                    and timeout > 0:
                    time.sleep(pause)
                    self.reset_vbuckets(self.rest, self._get_vBucket_ids(list(keyval.keys())))
                    rec_caller_fn(exp, flags, keyval, pause, timeout - pause)
                    return []
                else:
                    return [error]

        except BaseException as error:
            if timeout <= 0:
                return [error]
            else:
                time.sleep(pause)
                self.reset_vbuckets(self.rest, self._get_vBucket_ids(list(keyval.keys())))
                rec_caller_fn(exp, flags, keyval, pause, timeout - pause, collection=collection)  # Please refer above for comments.
                return []

    def _get_server_keyval_dic(self, key_val_dic):
        server_keyval = {}
        for key, val in list(key_val_dic.items()):
            vBucketId = self._get_vBucket_id(key)
            server_str = self.vBucketMap[vBucketId]
            if server_str not in server_keyval :
                server_keyval[server_str] = {}
            server_keyval[server_str][key] = val
        return server_keyval


    def getMulti(self, keys_lst, pause_sec=1, timeout_sec=5, parallel=True,collection=None):
        if parallel:
            try:

                import concurrent.futures
                return self._getMulti_parallel(keys_lst, pause_sec, timeout_sec, collection=collection)
            except ImportError:
                return self._getMulti_seq(keys_lst, pause_sec, timeout_sec, collection=collection)
        else:
            return self._getMulti_seq(keys_lst, pause_sec, timeout_sec, collection=collection)


    def _getMulti_seq(self, keys_lst, pause_sec=1, timeout_sec=5, collection=None):
        server_keys = self._get_server_keys_dic(keys_lst)  # set keys in their respective vbuckets and identify the server for each vBucketId
        keys_vals = {}
        for server_str, keys in list(server_keys.items()) :  # get memcached client against each server and multi get
            mc = self.memcacheds[server_str]
            keys_vals.update(self._getMulti_from_mc(mc, keys, pause_sec, timeout_sec,  self._getMulti_seq, collection=collection))
        if len(keys_lst) != len(keys_vals):
            raise ValueError("Not able to get values for following keys - {0}".format(set(keys_lst).difference(list(keys_vals.keys()))))
        return keys_vals


    def _getMulti_parallel(self, keys_lst, pause_sec=1, timeout_sec=5, collection=None):
        server_keys = self._get_server_keys_dic(keys_lst)
        tasks = []
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(server_keys)) as executor:
            for server_str, keys in list(server_keys.items()) :
                mc = self.memcacheds[server_str]
                tasks.append(executor.submit(self._getMulti_from_mc, mc, keys, pause_sec, timeout_sec, self._getMulti_parallel, collection=collection))
            keys_vals = self._reduce_getMulti_values(tasks, pause_sec, timeout_sec)
            if len(set(keys_lst)) != len(keys_vals):
                raise ValueError("Not able to get values for following keys - {0}".format(set(keys_lst).difference(list(keys_vals[collection].keys()))))

            return keys_vals


    def _getMulti_from_mc(self, memcached_client, keys, pause, timeout, rec_caller_fn, collection=None):
        try:
            return memcached_client.getMulti(keys, collection=collection)

        except (EOFError, socket.error) as error:
            if "Got empty data (remote died?)" in error.message or \
               "Timeout waiting for socket" in error.message or \
               "Broken pipe" in error.message or "Connection reset by peer" in error.message \
                and timeout > 0:
                time.sleep(pause)
                self.reset_vbuckets(self.rest, self._get_vBucket_ids(keys))
                return rec_caller_fn(keys, pause, timeout - pause, collection=collection)
            else:
                raise error
        except BaseException as error:
            if timeout <= 0:
                raise error
            time.sleep(pause)
            self.reset_vbuckets(self.rest, self._get_vBucket_ids(keys))
            return rec_caller_fn(keys, pause, timeout - pause)

    def _reduce_getMulti_values(self, tasks, pause, timeout):
        keys_vals = {}
        import concurrent.futures
        now = time.time()
        for future in concurrent.futures.as_completed(tasks, timeout):
            if future.exception() is not None:
                self.log.error("exception in {0} sec".format(time.time() - now))
                raise future.exception()
            keys_vals.update(future.result())
        return keys_vals

    def _get_server_keys_dic(self, keys):
        server_keys = {}
        for key in keys:
            vBucketId = self._get_vBucket_id(key)
            server_str = self.vBucketMap[vBucketId]
            if server_str not in server_keys :
                server_keys[server_str] = []
            server_keys[server_str].append(key)
        return server_keys

    def _get_vBucket_ids(self, keys, collection=None):
        return {self._get_vBucket_id(key) for key in keys}


    def _get_vBucket_id(self, key, collection=None):
        return (zlib.crc32(key.encode()) >> 16) & (len(self.vBucketMap) - 1)


    def delete(self, key, collection=None):
        vb_error = 0
        while True:
            try:
                return self._send_op(self.memcached(key).delete, key, collection=collection)
            except MemcachedError as error:
                if error.status in [ERR_NOT_MY_VBUCKET, ERR_EINVAL] and vb_error < 5:
                    self.reset_vbuckets(self.rest, {self._get_vBucket_id(key)})
                    vb_error += 1
                else:
                    raise error
            except (EOFError, socket.error) as error:
                if "Got empty data (remote died?)" in error.message or \
                   "Timeout waiting for socket" in error.message or \
                   "Broken pipe" in error.message or "Connection reset by peer" in error.message \
                    and vb_error < 5:
                    self.reset_vbuckets(self.rest, set([key], collection=collection))
                    vb_error += 1
                else:
                    raise error
            except BaseException as error:
                if vb_error < 5:
                    self.reset_vbuckets(self.rest, {self._get_vBucket_id(key)})
                    vb_error += 1
                else:
                    raise error

    def _send_op(self, func, *args, **kargs):
        backoff = .001
        while True:
            try:
                return func(*args, **kargs)
            except MemcachedError as error:
                if error.status == ERR_ETMPFAIL and backoff < .5:
                    time.sleep(backoff)
                    backoff *= 2
                else:
                    raise error
            except (EOFError, IOError, socket.error) as error:
                raise MemcachedError(ERR_NOT_MY_VBUCKET, "Connection reset with error: {0}".format(error))

    def done(self):
        [self.memcacheds[ip].close() for ip in self.memcacheds]

         # This saves a lot of repeated code - the func is the mc bin client function

    def generic_request(self, func, *args):
        key = args[0]
        vb_error = 0
        while True:
            try:
                return self._send_op(func, *args)
            except MemcachedError as error:
                if error.status == ERR_NOT_MY_VBUCKET and vb_error < 5:
                    self.reset_vbuckets(self.rest, {self._get_vBucket_id(key)},
                                        forward_map=self._parse_not_my_vbucket_error(error))
                    vb_error += 1
                else:
                    raise error
            except (EOFError, socket.error) as error:
                if "Got empty data (remote died?)" in error.message or \
                   "Timeout waiting for socket" in error.message or \
                   "Broken pipe" in error.message or "Connection reset by peer" in error.message \
                    and vb_error < 5:
                    self.reset_vbuckets(self.rest, {self._get_vBucket_id(key)})
                    vb_error += 1
                    if vb_error >= 5:
                        raise error
                else:
                    raise error
            except BaseException as error:
                if vb_error < 5:
                    self.reset_vbuckets(self.rest, {self._get_vBucket_id(key)})
                    self.log.info("***************resetting vbucket id***********")
                    vb_error += 1
                else:
                    raise error



    def _parse_not_my_vbucket_error(self, error):
        error_msg = error.msg
        if "Connection reset with error:" in error_msg:
            self.log.error("{0} while _send_op, server is alive?".format(error_msg))
            return None
        vbuckets = []
        try:
            error_json = json.loads(error_msg[error_msg.find('{'):error_msg.rfind('}') + 1])
        except:
            self.log.error("Error while getting CCCP from not_my_vbucket...\n %s" % error_msg)
            return None
        if 'vBucketMapForward' in error_json['vBucketServerMap']:
            vBucketMap = error_json['vBucketServerMap']['vBucketMapForward']
        else:
            vBucketMap = error_json['vBucketServerMap']['vBucketMap']
        serverList = error_json['vBucketServerMap']['serverList']
        if not self.rest:
            self.rest = RestConnection(self.info)
        serverList = [server.replace("$HOST", str(self.rest.ip))
                  if server.find("$HOST") != -1 else server for server in serverList]
        counter = 0
        for vbucket in vBucketMap:
            vbucketInfo = vBucket()
            vbucketInfo.master = serverList[vbucket[0]]
            if vbucket:
                for i in range(1, len(vbucket)):
                    if vbucket[i] != -1:
                        vbucketInfo.replica.append(serverList[vbucket[i]])
            vbucketInfo.id = counter
            counter += 1
            vbuckets.append(vbucketInfo)
        return vbuckets



    def sendHellos(self, feature_flag ):
        for m in self.memcacheds:
            self.memcacheds[ m ].hello( feature_flag )



class KVStoreAwareSmartClient(VBucketAwareMemcached):
    def __init__(self, rest, bucket, kv_store=None, info=None, store_enabled=True, collection=None):
        VBucketAwareMemcached.__init__(self, rest, bucket, info, collection=collection)
        self.kv_store = kv_store or ClientKeyValueStore()
        self.store_enabled = store_enabled
        self._rlock = threading.Lock()

    def set(self, key, value, ttl=-1, flag=0, collection=None):
        self._rlock.acquire()
        try:
            if ttl >= 0:
                self.memcached(key).set(key, ttl, 0, value, collection=collection)
            else:
                self.memcached(key).set(key, 0, 0, value, collection=collection)

            if self.store_enabled:
                self.kv_store.write(key, hashlib.md5(value.encode()).digest(), ttl)

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
    def get_doc_metadata(self, num_vbuckets, key, collection=None):
        vid = crc32.crc32_hash(key) & (num_vbuckets - 1)

        mc = self.memcached(key, collection=collection)
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
            opaque, cas, data = self.memcached(key).delete(key, collection=collection)
            if self.store_enabled:
                self.kv_store.delete(key, collection=collection)
            self._rlock.release()
            if cas == 0:
                raise MemcachedError(7, "Invalid cas value")
        except Exception as e:
            self._rlock.release()
            raise MemcachedError(7, str(e))

    def get_valid_key(self, key, collection=None):
        return self.get_key_check_status(key, "valid", collection=collection)

    def get_deleted_key(self, key, collection=None):
        return self.get_key_check_status(key, "deleted", collection=collection)

    def get_expired_key(self, key, collection=None):
        return self.get_key_check_status(key, "expired", collection=collection)

    def get_all_keys(self, collection=None):
        return self.kv_store.keys(collection=collection)

    def get_all_valid_items(self, collection=None):
        return self.kv_store.valid_items(collection=collection)

    def get_all_deleted_items(self, collection=None):
        return self.kv_store.deleted_items(collection=collection)

    def get_all_expired_items(self,collection=None):
        return self.kv_store.expired_items(collection=collection)

    def get_key_check_status(self, key, status,collection=None):
        item = self.kv_get(key, collection=collection)
        if(item is not None  and item["status"] == status):
            return item
        else:
            msg = "key {0} is not valid".format(key)
            self.log.info(msg)
            return None

    # safe kvstore retrieval
    # return dict of {key,status,value,ttl}
    # or None if not found
    def kv_get(self, key,collection=None):
        item = None
        try:
            item = self.kv_store.read(key, collection=collection)
        except KeyError:
            msg = "key {0} doesn't exist in store".format(key)
            # self.log.info(msg)

        return item

    # safe memcached retrieval
    # return dict of {key, flags, seq, value}
    # or None if not found
    def mc_get(self, key, collection=None):
        item = self.mc_get_full(key, collection=collection)
        if item is not None:
            item["value"] = hashlib.md5(item["value"]).digest()
        return item

    # unhashed value
    def mc_get_full(self, key, collection=None):
        item = None
        try:
            x, y, value = self.memcached(key).get(key, collection=collection)
            item = {}
            item["key"] = key
            item["flags"] = x
            item["seq"] = y
            item["value"] = value
        except MemcachedError:
            msg = "key {0} doesn't exist in memcached".format(key)

        return item

    def kv_mc_sync_get(self, key, status, collection=None):
        self._rlock.acquire()
        kv_item = self.get_key_check_status(key, status, collection=collection)
        mc_item = self.mc_get(key, collection=collection)
        self._rlock.release()

        return kv_item, mc_item


class KVStoreSmartClientHelper(object):

    @staticmethod
    def do_verification(client, collection=None):
        keys = client.get_all_keys(collection=collection)
        validation_failures = {}
        for k in keys:
            m, valid = KVStoreSmartClientHelper.verify_key(client, k, collection=collection)
            if(valid == False):
                validation_failures[k] = m

        return validation_failures

    @staticmethod
    def verify_key(client, key, collection=None):
        status = False
        msg = ""
        item = client.kv_get(key, collection=collection)
        if item is not None:
            if item["status"] == "deleted":
                msg, status = \
                    KVStoreSmartClientHelper.verify_delete(client, key, collection=collection)

            elif item["status"] == "expired":
                msg, status = \
                    KVStoreSmartClientHelper.verify_expired(client, key, collection=collection)

            elif item["status"] == "valid":
                msg, status = \
                    KVStoreSmartClientHelper.verify_set(client, key, collection=collection)

        return msg, status

    # verify kvstore contains key with valid status
    # and that key also exists in memcached with
    # expected value
    @staticmethod
    def verify_set(client, key, collection=None):

        kv_item = client.get_valid_key(key, collection=collection)
        mc_item = client.mc_get(key, collection=collection)
        status = False
        msg = ""

        if(kv_item is not None and mc_item is not None):
            # compare values
            if kv_item["value"] == mc_item["value"]:
                status = True
            else:
                msg = "kvstore and memcached values mismatch"
        elif(kv_item is None):
            msg = "valid status not set in kv_store"
        elif(mc_item is None):
            msg = "key missing from memcached"

        return msg, status


    # verify kvstore contains key with deleted status
    # and that it does not exist in memcached
    @staticmethod
    def verify_delete(client, key, collection=None):
        deleted_kv_item = client.get_deleted_key(key, collection=collection)
        mc_item = client.mc_get(key, collection=collection)
        status = False
        msg = ""

        if(deleted_kv_item is not None and mc_item is None):
            status = True
        elif(deleted_kv_item is None):
            msg = "delete status not set in kv_store"
        elif(mc_item is not None):
            msg = "key still exists in memcached"

        return msg, status


    # verify kvstore contains key with expired status
    # and that key has also expired in memcached
    @staticmethod
    def verify_expired(client, key, collection=None):
        expired_kv_item = client.get_expired_key(key, collection=collection)
        mc_item = client.mc_get(key, collection=collection)
        status = False
        msg = ""

        if(expired_kv_item is not None and mc_item is None):
            status = True
        elif(expired_kv_item is None):
            msg = "exp. status not set in kv_store"
        elif(mc_item is not None):
            msg = "key still exists in memcached"
        return msg, status

def start_reader_process(info, keyset, queue):
    ReaderThread(info, keyset, queue).start()


class GeneratedDocuments(object):
    def __init__(self, items, kv_template, options=dict(size=1024)):
        self._items = items
        self._kv_template = kv_template
        self._options = options
        self._pointer = 0
        if "padding" in options:
            self._pad = options["padding"]
        else:
           self._pad = DocumentGenerator._random_string(options["size"])

    # Required for the for-in syntax
    def __iter__(self):
        return self

    def __len__(self):
        return self._items

    def reset(self):
        self._pointer = 0

    def has_next(self):
        return self._pointer != self._items

    # Returns the next value of the iterator
    def __next__(self):
        if self._pointer == self._items:
            raise StopIteration
        else:
            i = self._pointer
            doc = {"meta":{"id": "{0}-{1}".format(i, self._options["seed"])}, "json":{}}
            for k in self._kv_template:
                v = self._kv_template[k]
                if isinstance(v, str) and v.find("${prefix}") != -1:
                    v = v.replace("${prefix}", "{0}".format(i))
                    # how about the value size
                if isinstance(v, str) and v.find("${padding}") != -1:
                    v = v.replace("${padding}", self._pad)
                if isinstance(v, str) and v.find("${seed}") != -1:
                    v = v.replace("${seed}", "{0}".format(self._options["seed"]))
                doc["json"][k] = v
        self._pointer += 1
        return json.dumps(doc)


class DocumentGenerator(object):
    # will loop over all values in props and replace ${prefix} with ${i}
    @staticmethod
    def make_docs(items, kv_template, options=dict(size=1024, seed=str(uuid.uuid4()))):
        return GeneratedDocuments(items, kv_template, options)

    @staticmethod
    def _random_string(length):
        return (("%%0%dX" % (length * 2)) % random.getrandbits(length * 8)).encode("ascii")

    @staticmethod
    def create_value(pattern, size):
        return (pattern * (size // len(pattern))) + pattern[0:(size % len(pattern))]

    @staticmethod
    def get_doc_generators(count, kv_template=None, seed=None, sizes=None):

        seed = seed or str(uuid.uuid4())[0:7]
        sizes = sizes or [128]

        doc_gen_iterators = []

        if kv_template is None:
            kv_template = {"name": "doc-${prefix}-${seed}",
                           "sequence": "${seed}",
                           "email": "${prefix}@couchbase.com"}
        for size in sizes:
            options = {"size": size, "seed": seed}
            docs = DocumentGenerator.make_docs(count // len(sizes),
                                               kv_template, options)
            doc_gen_iterators.append(docs)

        return doc_gen_iterators

    @staticmethod
    def get_doc_generators_by_load_ratio(rest,
                                         bucket='default',
                                         ram_load_ratio=1,
                                         value_size_distribution=None,
                                         seed=None):

        log = logger.Logger.get_logger()

        if ram_load_ratio < 0 :
            raise MemcachedClientHelperExcetion(errorcode='invalid_argument',
                                                message="ram_load_ratio")
        if not value_size_distribution:
            value_size_distribution = {16: 0.25, 128: 0.25, 512: 0.25, 1024: 0.25}

        list = []


        info = rest.get_bucket(bucket)
        emptySpace = info.stats.ram - info.stats.memUsed
        space_to_fill = (int((emptySpace * ram_load_ratio) / 100.0))
        log.info('space_to_fill : {0}, emptySpace : {1}'.format(space_to_fill, emptySpace))
        for size, probability in list(value_size_distribution.items()):
            how_many = int(space_to_fill / (size + 250) * probability)
            doc_seed = seed or str(uuid.uuid4())
            kv_template = {"name": "user-${prefix}", "payload": "memcached-json-${prefix}-${padding}",
                     "size": size, "seed": doc_seed}
            options = {"size": size, "seed": doc_seed}
            payload_generator = DocumentGenerator.make_docs(how_many, kv_template, options)
            list.append({'size': size, 'value': payload_generator, 'how_many': how_many, 'seed' : doc_seed})

        return list

#        docs = DocumentGenerator.make_docs(number_of_items,
#                {"name": "user-${prefix}", "payload": "payload-${prefix}-${padding}"},
#                {"size": 1024, "seed": str(uuid.uuid4())})

# Format of the json documents that mcsoda uses.
# JSON BODY
# {
# "key":"%s",
# "key_num":%s,
# "name":"%s",
# "email":"%s",
# "city":"%s",
# "country":"%s",
# "realm":"%s",
# "coins":%s,
# "achievements":%s
# }

class LoadWithMcsoda(object):

    def __init__(self, master, num_docs, prefix='', bucket='default', rest_user='Administrator',
                 rest_password="password", protocol='membase-binary', port=11211):

        rest = RestConnection(master)
        self.bucket = bucket
        vBuckets = rest.get_vbuckets(self.bucket)
        self.vbucket_count = len(vBuckets)

        self.cfg = {
                'max-items': num_docs,
                'max-creates': num_docs,
                'min-value-size': 128,
                'exit-after-creates': 1,
                'ratio-sets': 1,
                'ratio-misses': 0,
                'ratio-creates': 1,
                'ratio-deletes': 0,
                'ratio-hot': 0,
                'ratio-hot-sets': 1,
                'ratio-hot-gets': 0,
                'ratio-expirations': 0,
                'expiration': 0,
                'threads': 1,
                'json': 1,
                'batch': 10,
                'vbuckets': self.vbucket_count,
                'doc-cache': 0,
                'doc-gen': 0,
                'prefix': prefix,
                'socket-timeout': 60,
        }

        self.protocol = protocol
        self.rest_user = rest_user
        self.rest_password = rest_password

        if protocol == 'membase-binary':
            self.host_port = "{0}:{1}:{2}".format(master.ip, master.port, port)

        elif protocol == 'memcached-binary':
            self.host_port = "{0}:{1}:{1}".format(master.ip, port)

        self.ctl = { 'run_ok': True }

    def protocol_parse(self, protocol_in):
        if protocol_in.find('://') >= 0:
            protocol = \
                '-'.join(((["membase"] + \
                               protocol_in.split("://"))[-2] + "-binary").split('-')[0:2])
            host_port = ('@' + protocol_in.split("://")[-1]).split('@')[-1] + ":8091"
            user, pswd = (('@' + protocol_in.split("://")[-1]).split('@')[-2] + ":").split(':')[0:2]

        return protocol, host_port, user, pswd

    def get_cfg(self):
        return self.cfg

    def load_data(self, collection=None):
        cur, start_time, end_time = mcsoda.run(self.cfg, {}, self.protocol, self.host_port, self.rest_user, \
            self.rest_password, ctl=self.ctl, bucket=self.bucket)
        return cur

    def load_stop(self):
        self.ctl['run_ok'] = False
