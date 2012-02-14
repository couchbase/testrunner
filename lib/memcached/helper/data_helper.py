import copy
from multiprocessing.process import Process
from multiprocessing.queues import Queue
import random
import time
from random import Random
import uuid
from TestInput import TestInputServer
from TestInput import TestInputSingleton
import logger
import crc32
import threading
from mc_bin_client import MemcachedClient, MemcachedError
from mc_ascii_client import MemcachedAsciiClient
from membase.api.rest_client import RestConnection, RestHelper
import json
import sys
sys.path.append('./pytests/performance/')
import mcsoda


class MemcachedClientHelperExcetion(Exception):
    def __init__(self, errorcode, message):
        Exception.__init__(self, errorcode, message)
        self._message = message
        self.errorcode = errorcode
        self._args = (errorcode, message)


class MemcachedClientHelper(object):
    #value_sizes {10:0.1,20:0.2:40:0.8}

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
                       expiry_ratio=0):
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
            for size, probability in value_size_distribution.items():
                how_many = int(space_to_fill / (size + 250) * probability)
                payload_generator = DocumentGenerator.make_docs(number_of_items,
                        {"name": "user-${prefix}", "payload": "memcached-json-${prefix}-${padding}",
                         "size": size, "seed": str(uuid.uuid4())})
                list.append({'size': size, 'value': payload_generator, 'how_many': how_many})
        else:
            for size, probability in value_size_distribution.items():
                how_many = ((number_of_items / number_of_threads)* probability)
                payload_generator = DocumentGenerator.make_docs(number_of_items,
                        {"name": "user-${prefix}", "payload": "memcached-json-${prefix}-${padding}",
                         "size": size, "seed": str(uuid.uuid4())})
                list.append({'size': size, 'value': payload_generator, 'how_many': how_many})

        for item in list:
            item['how_many'] /= int(number_of_threads)
            #at least one element for each value size
            if item['how_many'] < 1:
                item['how_many'] = 1
            msg = "each thread will send {0} items with value of size : {1}"
            log.info(msg.format(item['how_many'], item['size']))

        threads = []
        for i in range(0, int(number_of_threads)):
            #choose one of the servers random
            thread = WorkerThread(serverInfo=MemcachedClientHelper.random_pick(servers),
                                  name=name,
                                  values_list=list,
                                  override_vBucketId=override_vBucketId,
                                  write_only=write_only,
                                  moxi=moxi,
                                  async_write=async_write,
                                  delete_ratio=delete_ratio,
                                  expiry_ratio=expiry_ratio)
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
                                       expiry_ratio=0):
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
            for size, probability in value_size_distribution.items():
                #let's assume overhead per key is 64 bytes ?
                how_many = int(space_to_fill / (size + 250) * probability)
                payload = MemcachedClientHelper.create_value('*', size)
                list.append({'size': size, 'value': payload, 'how_many': how_many})
        else:
            for size, probability in value_size_distribution.items():
                how_many = (number_of_items * probability)
                payload = MemcachedClientHelper.create_value('*', size)
                list.append({'size': size, 'value': payload, 'how_many': how_many})

        for item in list:
            item['how_many'] /= int(number_of_threads)
            #at least one element for each value size
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
                                  expiry_ratio=expiry_ratio)
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
                                        expiry_ratio=0):
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
                                                       expiry_ratio=expiry_ratio)

        #we can start them!
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
                    moxi=True):
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
                                                       moxi)
        #we can start them!
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
        return (pattern * (size / len(pattern))) + pattern[0:(size % len(pattern))]

    @staticmethod
    def random_pick(list):
        if list:
            if len(list) > 1:
                return list[Random().randint(0, len(list) - 1)]
            return list[0]
            #raise array empty ?
        return None

    @staticmethod
    def direct_client(server, bucket, timeout=30):
        rest = RestConnection(server)
        node = rest.get_nodes_self()
        log = logger.Logger.get_logger()
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
        client.vbucket_count = len(vBuckets)
        bucket_info = rest.get_bucket(bucket)
        #todo raise exception for not bucket_info
        client.sasl_auth_plain(bucket_info.name.encode('ascii'),
                               bucket_info.saslPassword.encode('ascii'))
        return client

    @staticmethod
    def proxy_client(server, bucket, timeout=30, force_ascii=False):
        #for this bucket on this node what is the proxy ?
        rest = RestConnection(server)
        log = logger.Logger.get_logger()
        bucket_info = rest.get_bucket(bucket)
        nodes = bucket_info.nodes

        if (TestInputSingleton.input and "ascii" in TestInputSingleton.input.test_params\
            and TestInputSingleton.input.test_params["ascii"].lower() == "true")\
            or force_ascii:
            ascii = True
        else:
            ascii = False
        for node in nodes:
            RestHelper(rest).vbucket_map_ready(bucket, 60)
            vBuckets = rest.get_vbuckets(bucket)
            if ascii:
                log = logger.Logger.get_logger()
                log.info("creating ascii client {0}:{1} {2}".format(server.ip, bucket_info.port, bucket))
                client = MemcachedAsciiClient(server.ip, bucket_info.port, timeout=timeout)
            else:
                log = logger.Logger.get_logger()
                if isinstance(server, dict):
                    log.info("creating proxy client {0}:{1} {2}".format(server["ip"], node.moxi, bucket))
                    client = MemcachedClient(server["ip"], node.moxi, timeout=timeout)
                else:
                    log.info("creating proxy client {0}:{1} {2}".format(server.ip, node.moxi, bucket))
                    client = MemcachedClient(server.ip, node.moxi, timeout=timeout)
                client.vbucket_count = len(vBuckets)
                if bucket_info.authType == "sasl":
                    client.sasl_auth_plain(bucket_info.name.encode('ascii'),
                                           bucket_info.saslPassword.encode('ascii'))
            return client
        if isinstance(server, dict):
            raise Exception("unable to find {0} in get_nodes()".format(server["ip"]))
        else:
            raise Exception("unable to find {0} in get_nodes()".format(server.ip))

    @staticmethod
    def flush_bucket(server, bucket):
        #if memcached throws OOM error try again ?
        log = logger.Logger.get_logger()
        client = MemcachedClientHelper.direct_client(server, bucket)
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
    def run(self):
        values = DocumentGenerator.make_docs(len(self.keys),
                {"name": "user-${prefix}", "payload": "memcached-json-${prefix}-${padding}",
                 "size": 1024, "seed": self.seed})
        client = MemcachedClientHelper.proxy_client(self.serverInfo, self.name)
        counter = 0
        for value in values:
            try:
                if self.op == "set":
                    client.set(self.keys[counter], 0, 0, value)
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
                 name='default'):
        threading.Thread.__init__(self)
        self.log = logger.Logger.get_logger()
        self.serverInfo = serverInfo
        self.name = name
        self.keys = keys
        self.op = op
        self.seed = seed
        self._mutated_count = 0
        self._rejected_count = 0
        self._rejected_keys = []


class ReaderThread(object):
    def __init__(self, info, keyset, queue):
        self.info = info
        self.log = logger.Logger.get_logger()
        self.error_seen = 0
        self.keyset = keyset
        self.aborted = False
        self.queue = queue

    def abort(self):
        self.aborted = True

    def _saw_error(self, key):
    #        error_msg = "unable to get key {0}"
        self.error_seen += 1

    #        if self.error_seen < 500:
    #            self.log.error(error_msg.format(key))

    def start(self):
        client = MemcachedClientHelper.direct_client(self.info["server"], self.info['name'])
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
                client.send_get(key)
            except Exception:
                self._saw_error(key)
                #        self.log.warn("attempted to get {0} keys before they are set".format(self.error_seen))
        client.close()


#mutation ? let' do two cycles , first run and then try to mutate all those itesm
#and return
class WorkerThread(threading.Thread):
    #too flags : stop after x errors
    #slow down after every seeing y errors
    #value_list is a list of document generators
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
                 expiry_ratio=0):
        threading.Thread.__init__(self)
        self.log = logger.Logger.get_logger()
        self.serverInfo = serverInfo
        self.name = name
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
        #let's create a read_thread
        self.info = {'server': serverInfo,
                     'name': self.name,
                     'baseuuid': self._base_uuid}
        self.write_only = write_only
        self.aborted = False
        self.async_write = async_write

    def inserted_keys_count(self):
        return self._inserted_keys_count

    def rejected_keys_count(self):
        return self._rejected_keys_count

    #smart functin that gives you sth you can use to
    #get inserted keys
    #we should just expose an iterator instead which
    #generates the key,values on fly
    def keys_set(self):
        #let's construct the inserted keys set
        #TODO: hard limit , let's only populated up to 1 million keys
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
        awareness = VBucketAwareMemcached(RestConnection(self.serverInfo), self.name)
        client = None
        if self.moxi:
            try:
                client = MemcachedClientHelper.proxy_client(self.serverInfo, self.name)
            except Exception as ex:
                self.log.info("unable to create memcached client due to {0}. stop thread...".format(ex))
                import traceback

                traceback.print_exc()
                return
                #keeping keys in the memory is not such a good idea because
                #we run out of memory so best is to just keep a counter ?
                #if someone asks for the keys we can give them the formula which is
            # baseuuid-{0}-{1} , size and counter , which is between n-0 except those
        #keys which were rejected
        #let's print out some status every 5 minutes..

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
                #every two minutes print the status
                if time.time() - last_reported > 2 * 60:
                    if not self.moxi:
                        awareness.done()
                        try:
                            awareness = VBucketAwareMemcached(RestConnection(self.serverInfo), self.name)
                        except Exception:
                            #vbucket map is changing . sleep 5 seconds
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
                value = selected["value"].next()
            except StopIteration:
                pass
            try:
                if self.override_vBucketId >= 0:
                    client.vbucketId = self.override_vBucketId
                if self.async_write:
                    client.send_set(key, 0, 0, value)
                else:
                    client.set(key, 0, 0, value)
                self._inserted_keys_count += 1
                backoff_count = 0
                # do expiry sets, 30 second expiry time
                if Random().random() < self._expiry_ratio:
                    client.set(key + "-exp", 30, 0, value)
                    self._expiry_count += 1
                    # do deletes if we have 100 pending
                # at the end delete the remaining
                if len(self._delete) >= 100:
                #                    self.log.info("deleting {0} keys".format(len(self._delete)))
                    for key_del in self._delete:
                        client.delete(key_del)
                    self._delete = []
                    # do delete sets
                if Random().random() < self._delete_ratio:
                    client.set(key + "-del", 0, 0, value)
                    self._delete.append(key + "-del")
                    self._delete_count += 1
            except MemcachedError as error:
                if not self.moxi:
                    awareness.done()
                    try:
                        awareness = VBucketAwareMemcached(RestConnection(self.serverInfo), self.name)
                    except Exception:
                        #vbucket map is changing . sleep 5 seconds
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

                    #before closing the session let's try sending those items again
        retry = 3
        while retry > 0 and self._rejected_keys_count > 0:
            rejected_after_retry = []
            self._rejected_keys_count = 0
            for item in self._rejected_keys:
                try:
                    if self.override_vBucketId >= 0:
                        client.vbucketId = self.override_vBucketId
                    if self.async_write:
                        client.send_set(item["key"], 0, 0, item["value"])
                    else:
                        client.set(item["key"], 0, 0, item["value"])
                    self._inserted_keys_count += 1
                except MemcachedError:
                    self._rejected_keys_count += 1
                    rejected_after_retry.append({"key": item["key"], "value": item["value"]})
                    if len(rejected_after_retry) > self.ignore_how_many_errors:
                        break
            self._rejected_keys = rejected_after_retry
            retry = - 1
            # clean up the rest of the deleted keys
            if len(self._delete) > 0:
            #                self.log.info("deleting {0} keys".format(len(self._delete)))
                for key_del in self._delete:
                    client.delete(key_del)
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
        #if error is memcached error oom related let's do a sleep

    def _time_to_stop(self):
        return self.aborted or len(self._rejected_keys) > self.ignore_how_many_errors


class VBucketAwareMemcached(object):
    def __init__(self, rest, bucket, info=None):
        self.log = logger.Logger.get_logger()
        self.info = info
        self.bucket = bucket
        self.memcacheds = {}
        self.vBucketMap = {}
        self.reset(rest)

    def reset(self, rest=None):
        m, v = self.request_map(rest or RestConnection(self.info), self.bucket)
        self.memcacheds = m
        self.vBucketMap = v

    def request_map(self, rest, bucket):
        memcacheds = {}
        vBucketMap = {}
        vb_ready = RestHelper(rest).vbucket_map_ready(bucket, 60)
        if not vb_ready:
            raise Exception("vbucket map is not ready for bucket {0}".format(bucket))
        vBuckets = rest.get_vbuckets(bucket)
        nodes = rest.get_nodes()
        for vBucket in vBuckets:
            masterIp = vBucket.master.split(":")[0]
            masterPort = int(vBucket.master.split(":")[1])
            vBucketMap[vBucket.id] = vBucket.master
            if not vBucket.master in memcacheds:
                server = TestInputServer()
                server.ip = masterIp
                server.port = rest.port
                server.rest_username = rest.username
                server.rest_password = rest.password
                try:
                    for node in nodes:
                        if node.ip == masterIp and node.memcached == masterPort:
                            server.port = node.port
                            memcacheds[vBucket.master] =\
                            MemcachedClientHelper.direct_client(server, bucket)
                            break
                except Exception as ex:
                    msg = "unable to establish connection to {0}.cleanup open connections"
                    self.log.warn(msg.format(masterIp))
                    self.done()
                    raise ex
        return memcacheds, vBucketMap

    def memcached(self, key):
        vBucketId = crc32.crc32_hash(key) & (len(self.vBucketMap) - 1)
        return self.memcached_for_vbucket(vBucketId)

    def memcached_for_vbucket(self, vBucketId):
        if vBucketId not in self.vBucketMap:
            msg = "vbucket map does not have an entry for vb : {0}"
            raise Exception(msg.format(vBucketId))
        if self.vBucketMap[vBucketId] not in self.memcacheds:
            msg = "poxi does not have a mc connection for server : {0}"
            raise Exception(msg.format(self.vBucketMap[vBucketId]))
        return self.memcacheds[self.vBucketMap[vBucketId]]

    def not_my_vbucket_memcached(self, key):
        vBucketId = crc32.crc32_hash(key) & (len(self.vBucketMap) - 1)
        which_mc = self.vBucketMap[vBucketId]
        for server in self.memcacheds:
            if server != which_mc:
                return self.memcacheds[server]

    def done(self):
        [self.memcacheds[ip].close() for ip in self.memcacheds]


def start_reader_process(info, keyset, queue):
    ReaderThread(info, keyset, queue).start()


class GeneratedDocuments(object):
    def __init__(self, items, kv_template, options=dict(size=1024)):
        self._items = items
        self._kv_template = kv_template
        self._options = options
        self._pointer = 0
        self._pad = DocumentGenerator._random_string(options["size"])

    # Required for the for-in syntax
    def __iter__(self):
        return self

    def __len__(self):
        return self._items

    # Returns the next value of the iterator
    def next(self):
        if self._pointer == self._items:
            raise StopIteration
        else:
            i = self._pointer
            doc = {"_id": "{0}-{1}".format(i, self._options["seed"])}
            for k in self._kv_template:
                v = self._kv_template[k]
                if isinstance(v, str) and v.find("${prefix}") != -1:
                    v = v.replace("${prefix}", "{0}".format(i))
                    #how about the value size
                if isinstance(v, str) and v.find("${padding}") != -1:
                    v = v.replace("${padding}", self._pad)
                if isinstance(v, str) and v.find("${seed}") != -1:
                    v = v.replace("${seed}", "{0}".format(self._options["seed"]))
                doc[k] = v
        self._pointer += 1
        return json.dumps(doc)


class DocumentGenerator(object):
    #will loop over all values in props and replace ${prefix} with ${i}
    @staticmethod
    def make_docs(items, kv_template, options=dict(size=1024, seed=str(uuid.uuid4()))):
        return GeneratedDocuments(items, kv_template, options)

    @staticmethod
    def _random_string(length):
        return (("%%0%dX" % (length * 2)) % random.getrandbits(length * 8)).encode("ascii")

    @staticmethod
    def create_value(pattern, size):
        return (pattern * (size / len(pattern))) + pattern[0:(size % len(pattern))]

#        docs = DocumentGenerator.make_docs(number_of_items,
#                {"name": "user-${prefix}", "payload": "payload-${prefix}-${padding}"},
#                {"size": 1024, "seed": str(uuid.uuid4())})

#Format of the json documents that mcsoda uses.
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

    def __init__(self, master, num_docs, bucket='default',prefix=''):

        rest = RestConnection(master)
        vBuckets = rest.get_vbuckets(bucket)
        self.vbucket_count = len(vBuckets)

        self.cfg = {
                'max-items': num_docs,
                'max-creates': num_docs,
                'min-value-size': 512,
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
                'batch': 1000,
                'vbuckets': self.vbucket_count,
                'doc-cache': 0,
                'prefix': prefix,
        }

        # Right now only supports membase binary and 'default' bucket
        protocol="membase-binary://{0}:8091".format(master.ip)

        self.protocol, self.host_port, self.user, self.pswd = self.protocol_parse(protocol)

    def protocol_parse(self, protocol_in, use_direct=False):
        if protocol_in.find('://') >= 0:
            protocol = \
                '-'.join(((["membase"] + \
                               protocol_in.split("://"))[-2] + "-binary").split('-')[0:2])
            host_port = ('@' + protocol_in.split("://")[-1]).split('@')[-1] + ":8091"
            user, pswd = (('@' + protocol_in.split("://")[-1]).split('@')[-2] + ":").split(':')[0:2]

        return protocol, host_port, user, pswd

    def get_cfg(self):
        return self.cfg

    def load_data(self):
        cur, start_time, end_time = mcsoda.run(self.cfg, {}, self.protocol, self.host_port, self.user, self.pswd)
        return cur
