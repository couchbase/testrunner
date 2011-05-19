import copy
import time
from random import Random
import uuid
import logger
import crc32
import threading
from mc_bin_client import MemcachedClient, MemcachedError
from membase.api.rest_client import RestConnection

class MemcachedClientHelperExcetion(Exception):
    def __init__(self, errorcode, message):
        self._message = message
        self.errorcode = errorcode
        self._args = (errorcode, message)


class MemcachedClientHelper(object):
    #value_sizes {10:0.1,20:0.2:40:0.8}

    @staticmethod
    def create_threads_for_load_bucket(serverInfo=None,
                                       name='default',
                                       port=11211,
                                       ram_load_ratio=-1,
                                       number_of_items=-1,
                                       value_size_distribution=None,
                                       number_of_threads=50,
                                       override_vBucketId=-1):
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
            item['how_many'] /= number_of_threads
            #at least one element for each value size
            if item['how_many'] < 1:
                item['how_many'] = 1
            msg = "each thread will send {0} items with value of size : {1}"
            log.info(msg.format(item['how_many'], item['size']))

        threads = []
        for i in range(0, number_of_threads):
            thread = WorkerThread(serverInfo=serverInfo,
                                  name=name,
                                  port=port,
                                  password='password',
                                  values_list=list,
                                  ignore_how_many_errors=5000,
                                  override_vBucketId=override_vBucketId)
            threads.append(thread)

        return threads

    @staticmethod
    def load_bucket_and_return_the_keys(serverInfo=None,
                                        name='default',
                                        port=11211,
                                        ram_load_ratio=-1,
                                        number_of_items=-1,
                                        value_size_distribution=None,
                                        number_of_threads=50,
                                        override_vBucketId=-1):
        inserted_keys = []
        rejected_keys = []
        log = logger.Logger.get_logger()
        threads = MemcachedClientHelper.create_threads_for_load_bucket(serverInfo,
                                                                       name,
                                                                       port,
                                                                       ram_load_ratio,
                                                                       number_of_items,
                                                                       value_size_distribution,
                                                                       number_of_threads,
                                                                       override_vBucketId)
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
    def load_bucket(serverInfo=None,
                    name='default',
                    port=11211,
                    ram_load_ratio=-1,
                    number_of_items=-1,
                    value_size_distribution=None,
                    number_of_threads=50,
                    override_vBucketId=-1):
        inserted_keys_count = 0
        rejected_keys_count = 0
        log = logger.Logger.get_logger()
        threads = MemcachedClientHelper.create_threads_for_load_bucket(serverInfo,
                                                                       name,
                                                                       port,
                                                                       ram_load_ratio,
                                                                       number_of_items,
                                                                       value_size_distribution,
                                                                       number_of_threads,
                                                                       override_vBucketId)
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
    def create_memcached_client(ip, bucket='default', port=11211, password='password'):
        client = MemcachedClient(ip, port)
        if port == 11210:
            if bucket == 'default':
                client.sasl_auth_plain(bucket, '')
            else:
                client.sasl_auth_plain(bucket, password)
        if bucket != 'default' and port == 11211:
            client.sasl_auth_plain(bucket, password)
        return client

        #let's divide this and each thread will take care of 1/10th of the load

    @staticmethod
    def flush_bucket(ip, bucket='default', port=11211, password='password'):
        #if memcached throws OOM error try again ?
        log = logger.Logger.get_logger()
        client = MemcachedClient(ip, port)
        if bucket != 'default' and port == 11211:
            client.sasl_auth_plain(bucket, password)
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
        client = MemcachedClientHelper.create_memcached_client(self.serverInfo.ip,
                                                               self.name,
                                                               self.port,
                                                               self.password)
        for key in self.keys:
            #every two minutes print the status
            vId = crc32.crc32_hash(key) & 1023
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
                client = MemcachedClientHelper.create_memcached_client(self.serverInfo.ip,
                                                               self.name,
                                                               self.port,
                                                               self.password)

        self.log.info("mutation failed {0} times".format(self._rejected_count))
        #print some of those rejected keys...
        client.close()


    def __init__(self,serverInfo,
                 keys,
                 op,
                 seed,
                 name='default',
                 port=11211,
                 password='password'):
        threading.Thread.__init__(self)
        self.log = logger.Logger.get_logger()
        self.serverInfo = serverInfo
        self.name = name
        self.port = port
        self.password = password
        self.keys = keys
        self.op = op
        self.seed = seed
        self._mutated_count = 0
        self._rejected_count = 0
        self._rejected_keys = []


#mutation ? let' do two cycles , first run and then try to mutate all those itesm
#and return
class WorkerThread(threading.Thread):
    #too flags : stop after x errors
    #slow down after every seeing y errors
    def __init__(self,
                 serverInfo,
                 name,
                 port,
                 password,
                 values_list,
                 ignore_how_many_errors=5000,
                 override_vBucketId=-1,
                 terminate_in_minutes=60):
        threading.Thread.__init__(self)
        self.log = logger.Logger.get_logger()
        self.serverInfo = serverInfo
        self.name = name
        self.port = port
        self.password = password
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
        client = MemcachedClientHelper.create_memcached_client(self.serverInfo.ip,
                                                               self.name,
                                                               self.port,
                                                               self.password)
        #keeping keys in the memory is not such a good idea because
        #we run out of memory so best is to just keep a counter ?
        #if someone asks for the keys we can give them the formula which is
        # baseuuid-{0}-{1} , size and counter , which is between n-0 except those
        #keys which were rejected
        #let's print out some status every 5 minutes..

        start_time = time.time()
        last_reported = start_time
        while len(self.values_list) > 0:
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
                    last_reported = time.time()
                    for item in self.values_list:
                        self.log.info(
                            '{0} keys (each {1} bytes) more to send...'.format(item['how_many'], item['size']))

            key = "{0}-{1}-{2}".format(self._base_uuid,
                                       selected['size'],
                                       int(selected['how_many']))
            vId = crc32.crc32_hash(key) & 1023
            client.vbucketId = vId
            try:
                if self.override_vBucketId >= 0:
                    client.vbucketId = self.override_vBucketId
                client.set(key, 0, 0, selected['value'])
                self._inserted_keys_count += 1
            except MemcachedError as error:
                self.log.info(error.status)
                if error.status == 134:
                    self.log.info("received error # 134. backing off for 1 sec")
                    time.sleep(1.0)
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
                vId = crc32.crc32_hash(key) & 1023
                client.vbucketId = vId
                try:
                    if self.override_vBucketId >= 0:
                        client.vbucketId = self.override_vBucketId
                    client.set(key, 0, 0, key)
                    self._inserted_keys_count += 1
                except MemcachedError:
                    self._rejected_keys_count += 1
                    rejected_after_retry.append(key)
                    if len(rejected_after_retry) > self.ignore_how_many_errors:
                        break
            self._rejected_keys = rejected_after_retry
            retry =- 1
        client.close()