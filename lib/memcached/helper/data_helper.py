import copy
from multiprocessing.process import Process
from multiprocessing.queues import Queue
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
from threading import Thread


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
                       async_write=False):
        log = logger.Logger.get_logger()
        if not servers:
            raise MemcachedClientHelperExcetion(errorcode='invalid_argument',
                                                message="servers is not set")
        if ram_load_ratio < 0 and number_of_items < 0:
            raise MemcachedClientHelperExcetion(errorcode='invalid_argument',
                                                message="ram_load_ration or number_of_items must be specified")
        if not value_size_distribution:
            value_size_distribution = {16: 0.25, 128: 0.25, 512: 0.25, 1024: 0.25}

        list = []

        if ram_load_ratio >= 0:
            info = RestConnection(servers[0]).get_bucket(name)
            emptySpace = info.stats.ram - info.stats.memUsed
            space_to_fill = (int((emptySpace * ram_load_ratio) / 100.0))
            log.info('space_to_fill : {0}, emptySpace : {1}'.format(space_to_fill, emptySpace))
            for size, probability in value_size_distribution.items():
                #let's assume overhead per key is 64 bytes ?
                how_many = int(space_to_fill / (size + 250) * probability)
                payload = MemcachedClientHelper.create_value('*', size)
                log.info("payload size {0}".format(len(payload)))
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
            #choose one of the servers random
            thread = WorkerThread(serverInfo=MemcachedClientHelper.random_pick(servers),
                                  name=name,
                                  values_list=list,
                                  override_vBucketId=override_vBucketId,
                                  write_only=write_only,
                                  moxi=moxi,
                                  async_write=async_write)
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
                                       moxi=True):
        log = logger.Logger.get_logger()
        if not serverInfo:
            raise MemcachedClientHelperExcetion(errorcode='invalid_argument',
                                                message="serverInfo is not set")
        if ram_load_ratio < 0 and number_of_items < 0:
            raise MemcachedClientHelperExcetion(errorcode='invalid_argument',
                                                message="ram_load_ration or number_of_items must be specified")
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
                                  moxi=moxi)
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
                                        moxi=True):
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
                                                       moxi=moxi)

        #we can start them!
        for thread in threads:
            thread.start()
        log.info("waiting for all worker thread to finish their work...")
        [thread.join() for thread in threads]
        log.info("worker threads are done...")

        inserted_count = 0
        rejected_count = 0
        for thread in threads:
            t_inserted, t_rejected = thread.keys_set()
            inserted_count += thread.inserted_keys_count()
            rejected_count += thread.rejected_keys_count()
            inserted_keys.extend(t_inserted)
            rejected_keys.extend(t_rejected)
        msg = "inserted keys count : {0} , rejected keys count : {1}"
        log.info(msg.format(inserted_count, rejected_count))
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
        RestHelper(rest).vbucket_map_ready(bucket, 60)
        vBuckets = RestConnection(server).get_vbuckets(bucket)
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
        bucket_info = rest.get_bucket(bucket)
        nodes = bucket_info.nodes
        if ("ascii" in TestInputSingleton.input.test_params \
                and TestInputSingleton.input.test_params["ascii"].lower() == "true") \
                or force_ascii:
            ascii = True
        else:
            ascii = False
        for node in nodes:
            RestHelper(rest).vbucket_map_ready(bucket, 60)
            vBuckets = rest.get_vbuckets(bucket)
            if ascii:
                client = MemcachedAsciiClient(server.ip, node.moxi, timeout=timeout)
            else:
                client = MemcachedClient(server.ip, node.moxi, timeout=timeout)
                client.vbucket_count = len(vBuckets)
                if bucket_info.authType == "sasl":
                    client.sasl_auth_plain(bucket_info.name.encode('ascii'),
                                           bucket_info.saslPassword.encode('ascii'))
            return client
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
        client = MemcachedClientHelper.proxy_client(self.serverInfo, self.name)
        vbucket_count = len(RestConnection(self.serverInfo).get_vbuckets(self.name))
        for key in self.keys:
            #every two minutes print the status
            vId = crc32.crc32_hash(key) & (vbucket_count - 1)
            client.vbucketId = vId
            try:
                if self.op == "set":
                    client.set(key, 0, 0, self.seed)
                    self._mutated_count += 1
            except MemcachedError:
                self._rejected_count += 1
                self._rejected_keys.append(key)
            #                self.log.info("mutation failed for {0},{1}".format(key, self.seed))
            except Exception as e:
                self.log.info(e)
                self._rejected_count += 1
                self._rejected_keys.append(key)
                #                self.log.info("mutation failed for {0},{1}".format(key, self.seed))
                client.close()
                client = MemcachedClientHelper.proxy_client(self.serverInfo, self.name)
        self.log.info("mutation failed {0} times".format(self._rejected_count))
        #print some of those rejected keys...
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
    def __init__(self,
                 serverInfo,
                 name,
                 values_list,
                 ignore_how_many_errors=5000,
                 override_vBucketId=-1,
                 terminate_in_minutes=120,
                 write_only=False,
                 moxi=True,
                 async_write=False):
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
        vbucket_count = len(RestConnection(self.serverInfo).get_vbuckets(self.name))
        client = None
        if self.moxi:
            try:
                client = MemcachedClientHelper.proxy_client(self.serverInfo, self.name)
            except Exception:
                self.log.info("unable to create memcached client. stop thread...")
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
            vId = crc32.crc32_hash(key) & (vbucket_count - 1)
            client.vbucketId = vId
            try:
                if self.override_vBucketId >= 0:
                    client.vbucketId = self.override_vBucketId
                if self.async_write:
                    client.send_set(key, 0, 0, selected['value'])
                else:
                    client.set(key, 0, 0, selected['value'])
                self._inserted_keys_count += 1
                backoff_count = 0
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
                self.log.error("memcached error {0} {1} from {2}".format(error.status, error.msg, self.serverInfo.ip))
                if error.status == 134:
                    backoff_count += 1
                    if backoff_count < 5:
                        backoff_seconds = 15 * backoff_count
                    else:
                        backoff_seconds = 2 * backoff_count
                    self.log.info("received error # 134. backing off for {0} sec".format(backoff_seconds))
                    time.sleep(backoff_seconds)

                self._rejected_keys_count += 1
                self._rejected_keys.append(key)
                if len(self._rejected_keys) > self.ignore_how_many_errors:
                    break
            except Exception as ex:
                if not self.moxi:
                    awareness.done()
                    try:
                        awareness = VBucketAwareMemcached(RestConnection(self.serverInfo), self.name)
                    except Exception:
                        #vbucket map is changing . sleep 5 seconds
                        time.sleep(5)
                        awareness = VBucketAwareMemcached(RestConnection(self.serverInfo), self.name)
                    self.log.info("now connected to {0} memcacheds".format(len(awareness.memcacheds)))
                self.log.error("error {0} from {1}".format(ex, self.serverInfo.ip))
                self._rejected_keys_count += 1
                self._rejected_keys.append(key)
                if len(self._rejected_keys) > self.ignore_how_many_errors:
                    break

                    #before closing the session let's try sending those items again
        retry = 3
        while retry > 0 and self._rejected_keys_count > 0:
            rejected_after_retry = []
            self._rejected_keys_count = 0
            for key in self._rejected_keys:
                vId = crc32.crc32_hash(key) & (vbucket_count - 1)
                client.vbucketId = vId
                try:
                    if self.override_vBucketId >= 0:
                        client.vbucketId = self.override_vBucketId
                    if self.async_write:
                        client.send_set(key, 0, 0, selected['value'])
                    else:
                        client.set(key, 0, 0, selected['value'])
                    self._inserted_keys_count += 1
                except MemcachedError:
                    self._rejected_keys_count += 1
                    rejected_after_retry.append(key)
                    if len(rejected_after_retry) > self.ignore_how_many_errors:
                        break
            self._rejected_keys = rejected_after_retry
            retry = - 1
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
    def __init__(self, rest, bucket):
        self.log = logger.Logger.get_logger()
        self.memcacheds = {}
        self.vBucketMap = {}
        vb_ready = RestHelper(rest).vbucket_map_ready(bucket, 60)
        if not vb_ready:
            raise Exception("vbucket map is not ready for bucket {0}".format(bucket))
        vBuckets = rest.get_vbuckets(bucket)
        nodes = rest.get_nodes()
        for vBucket in vBuckets:
            masterIp = vBucket.master.split(":")[0]
            masterPort = int(vBucket.master.split(":")[1])
            self.vBucketMap[vBucket.id] = vBucket.master
            if not vBucket.master in self.memcacheds:
                server = TestInputServer()
                server.ip = masterIp
                server.port = rest.port
                server.rest_username = rest.username
                server.rest_password = rest.password
                try:
                    for node in nodes:
                        if node.ip == masterIp and node.memcached == masterPort:
                            self.memcacheds[vBucket.master] =\
                            MemcachedClientHelper.direct_client(server, bucket)
                            break
                except Exception as ex:
                    msg = "unable to establish connection to {0}.cleanup open connections"
                    self.log.warn(msg.format(masterIp))
                    self.done()
                    raise ex

    def memcached(self, key):
        vBucketId = crc32.crc32_hash(key) & (len(self.vBucketMap) - 1)
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
