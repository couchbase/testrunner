import copy
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
    def load_bucket(serverInfo=None,
                    name='default',
                    port=11211,
                    ram_load_ratio=0.1,
                    value_size_distribution=None,
                    number_of_threads=50):
        inserted_keys = []
        rejected_keys = []
        log = logger.Logger.get_logger()

        if not serverInfo:
            raise MemcachedClientHelperExcetion(errorcode='invalid_argument',
                                                message="serverInfo is not set")
        if not value_size_distribution:
            value_size_distribution = {16: 0.33, 128: 0.33, 1024: 0.33}

        info = RestConnection(serverInfo).get_bucket(name)
        emptySpace = info.stats.ram - info.stats.memUsed
        space_to_fill = (int((emptySpace * ram_load_ratio) / 100.0))

        log.info('space_to_fill : {0}, emptySpace : {1}'.format(space_to_fill, emptySpace))
        #every key also uses 32 bytes + and some overhead per key?

        # dict : value_size -> ( 'porbability':0.33,'how_many':1000,'value':'***'
        list = []
        for size, probability in value_size_distribution.items():
            #let's assume overhead per key is 64 bytes ?
            how_many = int(space_to_fill / (size + 64) * probability)
            list.append({'size': size,
                         'value': MemcachedClientHelper.create_value('*', size),
                         'how_many': how_many})

        #divide between threads
        for item in list:
            if item['how_many'] > number_of_threads:
                item['how_many'] /= number_of_threads
            else:
                item['how_many'] = 1
            log.info("each thread will send {0} items with value of size : {1}".format(item['how_many'], item['size']))

        #let's divide this load in 10 threads
        threads = []
        for i in range(0, number_of_threads):
            #ignore should be relative to the number of keys to be inserted ?
            thread = WorkerThread(serverInfo, name, port, 'password', list, ignore_how_many_errors=5000)
            thread.start()
            threads.append(thread)

        #now wait for them to finish
        #lets estimate too ?
        log.info("waiting for all worker thread to finish their work...")
        [thread.join() for thread in threads]
        log.info("worker threads are done...")

        for thread in threads:
            inserted_keys.extend(thread.inserted_keys)
            rejected_keys.extend(thread.rejected_keys)
            #now get all key stuff

        log.info("inserted keys count : {0} , rejected keys count : {1}".format(
            len(inserted_keys), len(rejected_keys)))
        return inserted_keys, rejected_keys


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
        if bucket != 'default' and port == 11211:
            client.sasl_auth_plain(bucket, password)
        return client

        #let's divide this and each thread will take care of 1/10th of the load


class WorkerThread(threading.Thread):
    #too flags : stop after x errors
    #slow down after every seeing y errors
    def __init__(self, serverInfo, name, port, password, values_list, ignore_how_many_errors=5000):
        threading.Thread.__init__(self)
        self.log = logger.Logger.get_logger()
        self.serverInfo = serverInfo
        self.name = name
        self.port = port
        self.password = password
        self.values_list = []
        self.values_list.extend(copy.deepcopy(values_list))
        self.inserted_keys = []
        self.rejected_keys = []
        self.ignore_how_many_errors = ignore_how_many_errors

    def run(self):
        client = MemcachedClientHelper.create_memcached_client(self.serverInfo.ip,
                                                               self.name,
                                                               self.port,
                                                               self.password)
        while len(self.values_list) > 0:
            selected = MemcachedClientHelper.random_pick(self.values_list)
            selected['how_many'] -= 1
            if selected['how_many'] < 1:
                self.values_list.remove(selected)
            key = "{0}".format(uuid.uuid4())
            vId = crc32.crc32_hash(key) & 1023
            client.vbucketId = vId
            try:
                client.set(key, 0, 0, selected['value'])
                self.inserted_keys.append(key)
            except MemcachedError:
                #if its 130 , then
                #slow down until the ram usage goes under water mark?
                #or quit after errors
                self.rejected_keys.append(key)
                if len(self.rejected_keys) > self.ignore_how_many_errors:
                    break

        client.close()
        if len(self.rejected_keys) > 0:
            self.log.error("unable to push {0} keys".format(len(self.rejected_keys)))
