import threading
import time
import fractions
import uuid
import logger
from TestInput import TestInputServer

import mc_bin_client
from membase.api.rest_client import RestConnection
from memcached.helper.data_helper import VBucketAwareMemcached


class FakeMemcachedClient(object):
    def __init__(self, *args):
        self.db = {}
    def set(self, key, exp, flags, val):
        self.db[key] = val
    def get(self, key):
        return (0,0,self.db[key])
    def add(self, key, exp, flags, val):
        self.db[key] = val
    def delete(self, key):
        del self.db[key]
    def sasl_auth_plain(self, *args):
        pass


class LoadThread(threading.Thread):
    def __init__(self, load_info, server_index):
        threading.Thread.__init__(self)
        self.daemon = True

        self.log = logger.Logger()
        # thread state info
        self.stopped = False
        self.paused = True

        self.mutation_index = 0
        self.get_index = 0
        self.value_failures = 0
        self.backoff = 0

        self.mutation_index_max = 0
        self.items = 0
        self.operations = 0
        self.time = 0
        self.size = 0
        self.operation_rate = 0

        threads = int(load_info['operation_info'].get('threads', 0))

        # cache info
        self.cache_data = load_info['operation_info'].get('cache_data', False)
        self.data_cache = {}

        # server info
        self.server_ip = load_info['server_info'][server_index].ip
        self.bucket_name = load_info['memcached_info'].get('bucket_name', '')
        self.bucket_password = load_info['memcached_info'].get('bucket_password', '')

        # operation info
        self.create = load_info['operation_info']['create_percent'] / fractions.gcd(load_info['operation_info']['create_percent'], 100 - load_info['operation_info']['create_percent'])
        self.nocreate = (100 - load_info['operation_info']['create_percent']) / fractions.gcd(load_info['operation_info']['create_percent'], 100 - load_info['operation_info']['create_percent'])
        self.operation_sequence = []
        for op in load_info['operation_info']['operation_distribution']:
            for i in range(load_info['operation_info']['operation_distribution'][op]):
                self.operation_sequence.append(op)

        self.valuesize_sequence = []
        for op in load_info['operation_info']['valuesize_distribution']:
            for i in range(load_info['operation_info']['valuesize_distribution'][op]):
                self.valuesize_sequence.append(op)

        self.max_operation_rate = int(load_info['operation_info'].get('operation_rate', 0) / threads)

        self.uuid = uuid.uuid4()
        self.name = str(self.uuid) + self.name

        # limit info
        # all but time needs to be divided equally amongst threads
        self.limit_items = int(load_info['limit_info'].get('items', 0) / threads)
        self.limit_operations = int(load_info['limit_info'].get('operations', 0) / threads)
        self.limit_time = int(load_info['limit_info'].get('time', 0))
        self.limit_size = int(load_info['limit_info'].get('size', 0) / threads)
        self.poxi = self._poxi()
        print "now connected to {0} memcacheds".format(len(self.poxi.memcacheds))
        # connect
        #self.server should be vbucketaware memcached
#        self.server = mc_bin_client.MemcachedClient(self.server_ip, self.server_port)
#        if self.bucket_name or self.bucket_password:
#            self.server.sasl_auth_plain(self.bucket_name,self.bucket_password)


    def _poxi(self):
        tServer = TestInputServer()
        tServer.ip = self.server_ip
        tServer.rest_username = "Administrator"
        tServer.rest_password = "password"
        tServer.port = 8091
        rest = RestConnection(tServer)
        return VBucketAwareMemcached(rest, self.bucket_name)

    def run(self):
        while True:
            # handle pause/stop
            while self.paused:
                if self.stopped:
                    return
                time.sleep(1)
            if self.stopped:
                return

            start_time = time.time()

            # stop thread if we hit a limit (first limit we hit ends the thread)
            if self.limit_items and self.items > self.limit_items:
                self.log.info("items count limit reached")
                return
            if self.limit_operations and self.operations > self.limit_operations:
                self.log.info("operations limit reached")
                return
            if self.limit_time and self.time > self.limit_time:
                self.log.info("time limit reached")
                return
            if self.limit_size and self.size > self.limit_size:
                self.log.info("size limit reached")
                return

            # rate limit if we need to
            if self.max_operation_rate and self.operation_rate > self.max_operation_rate:
                time.sleep(1.0/float(self.max_operation_rate))

            # do the actual work
            operation = self.get_operation()
            if operation == 'set':
                key = self.name + '_' + `self.get_mutation_key()`
                try:
#                    print `self.mutation_index` + " : " + `self.get_mutation_key()`
                    self.poxi.memcached(key).set(key, 0, 0, self.get_data())
                    self.operations += 1
                    self.backoff -= 1

                    # update number of items
                    # for now the only time we have new items is with ever increasing mutation key indexes
                    if self.get_mutation_key() > self.mutation_index_max:
                        self.mutation_index_max = self.get_mutation_key()
                        # looks like this will miss the first mutation
                        self.items += 1

                    # TODO: verify that this works, we may need to take the second to max index
                    # update the size of all data (values, not keys) that is in the system
                    # this can be either from new keys or overwriting old keys
                    prev_indexes = self.get_mutation_indexes(self.get_mutation_key())
                    prev_size = 0
                    if prev_indexes:
                        prev_size = self.get_data_size(max(prev_indexes))
                    self.size += self.get_data_size() - prev_size

                    self.mutation_index += 1
                except mc_bin_client.MemcachedError as e:
                    self.poxi.done()
                    self.poxi = self._poxi()
                    print "now connected to {0} memcacheds".format(len(self.poxi.memcacheds))
                    if self.backoff < 0:
                        self.backoff = 0
                    if self.backoff > 30:
                        self.backoff = 30
                    self.backoff += 1
                    # temporary error
#                    if e.status == 134:
#                        time.sleep(self.backoff)
#                    else:
#                        print `time.time()` + ' ' + self.name + ' set(' + `self.backoff` + ') ',
#                        print e
#                        time.sleep(self.backoff)
            elif operation == 'get':
                key = self.name + '_' + `self.get_get_key()`
                try:
                    vdata = self.poxi.memcached(key).get(key)
                    self.operations += 1
                    self.backoff -= 1
                    data = vdata[2]
                    try:
                        data_expected = self.get_data(max(self.get_mutation_indexes(self.get_get_key())))
                        if data != data_expected:
                            self.value_failures += 1
                            raise
                    except Exception as e:
                        print e
                        print "create: " + `self.create`
                        print "nocreate: " + `self.nocreate`
                        print "get_index: " + `self.get_index`
                        print "get_key: " + `self.get_get_key()`
                        print "mutation_index_max: " + `self.mutation_index_max`
                        print "mutation_indexes: " + `self.get_mutation_indexes(self.get_get_key())`
                        print "getting data for mutation index: " + `max(self.get_mutation_indexes(self.get_get_key()))`
                        print "got:      \'" + data + "\'"
                        raise ValueError
                    self.get_index += 1
                except mc_bin_client.MemcachedError as e:
                    if self.backoff < 0:
                        self.backoff = 0
                    if self.backoff > 30:
                        self.backoff = 30
                    self.backoff += 1
                    print `time.time()` + ' ' + self.name + ' get(' + `self.backoff` + ') ',
                    print e
                    time.sleep(self.backoff)

            end_time = time.time()
            self.time += (end_time-start_time)
            self.operation_rate = (float(self.operations) / self.time)

    # get the current operation based on the get and mutation indexes
    def get_operation(self):
        return self.operation_sequence[(self.mutation_index + self.get_index) % len(self.operation_sequence)]

    # mutation_index -> mutation_key : based on create/nocreate
    def get_mutation_key(self, index=None):
        if index == None:
            index = self.mutation_index
        return index-self.nocreate*(index/(self.create+self.nocreate))

    # get_index -> get_key : based on get_index % mutation_index
    def get_get_key(self):
        return self.get_index % self.get_mutation_key(self.mutation_index)

    # key -> mutation_indexes
    def get_mutation_indexes(self, key):
        # starting point to find an index
        s=key*(self.nocreate+self.create)/self.create

        mutation_indexes=[]

        # for now we will scan all possible gcs even though we knows the step size
        # if we could guarentee that our calculated s was actually a gc it would be faster
        # scan a range (right now nocreate^2) incrementing by 1
        #  once we find a valid index, increment by nocreate
        index = s-(self.nocreate*self.nocreate)
        if index < 0:
            index = 0
        index_max = s+(self.nocreate*self.nocreate)
        incr = 1
        while index <= index_max:
            if index >= 0 and index <= self.mutation_index and self.get_mutation_key(index) == key:
                incr = self.nocreate
                mutation_indexes.append(index)
            index += incr
        return mutation_indexes

    # mutation_index -> mutation_data : based on create/nocreate
    def get_data(self, index=None):
        if index == None:
            index = self.mutation_index

        valuesize = self.valuesize_sequence[index % len(self.valuesize_sequence)]
        if self.cache_data:
            if not valuesize in self.data_cache:
                self.data_cache[valuesize] = (str(uuid.uuid3(self.uuid,`index`)) * (1+valuesize/36))[:valuesize]
            return `index` + self.data_cache[valuesize]
        else:
            return (str(uuid.uuid3(self.uuid,`index`)) * (1+valuesize/36))[:valuesize]

    # mutation_index -> mutation_data : based on create/nocreate
    # shortcut for getting the expected size of a mutation without generating the data
    def get_data_size(self, index=None):
        if index == None:
            index = self.mutation_index

        valuesize = self.valuesize_sequence[index % len(self.valuesize_sequence)]
        return valuesize


class LoadRunner(object):
    def __init__(self, load_info, dryrun=False):
        if dryrun:
            mc_bin_client.MemcachedClient = FakeMemcachedClient

        self.stopped = False
        self.paused = True

        self.threads = []
        self.num_servers = len(load_info['server_info'])
        self.num_threads = int(load_info['operation_info']['threads'])
        for i in range(self.num_threads):
            t = LoadThread(load_info, i % self.num_servers)
            t.start()
            self.threads.append(t)

    # start running load against server
    # this is run in a seperate thread(s)
    def start(self):
        self.stopped = False
        self.paused = False
        for t in self.threads:
            t.stopped = False
            t.paused = False
        
    # pause load but keep track of where we are
    def pause(self):
        self.paused = True
        for t in self.threads:
            t.paused = True

    # stop load and reset back to start
    def stop(self):
        self.stopped = True
        for t in self.threads:
            t.stopped = True

    # dump entire sequence of operations to a file
    def dump(self):
        pass

    # verify entire dataset is correct in membase at our current position
    def verify(self):
        pass

    # get the current state (num ops, percent complete, time elapsed)
    # also get the number of failed ops and failed add/set (failed adds due to item existing don't count)
    # return total ops, op breakdown (gets, sets, etc), total ops/s and ops/s breakdown (gets/s, sets/s, etc)
    def query(self):
        for t in self.threads:
            t.join(0)
        self.threads = [t for t in self.threads if t.isAlive()]

        if not len(self.threads):
            self.stopped = True

        if self.stopped:
            state = 'stopped'
        elif self.paused:
            state = 'paused'
        else:
            state = 'running'

        return {
            'ops':0,
            'time':0,
            'keys':0,
            'state':state,
        }

    # block till condition
    def wait(self, time_limit=None):
        if time_limit == None:
            for t in self.threads:
                t.join()
        else:
            start_time = time.time()
            while len(self.threads) > 0:
                for t in self.threads:
                    if time.time() - start_time > time_limit:
                        return
                    t.join(1)
                self.threads = [t for t in self.threads if t.isAlive()]
