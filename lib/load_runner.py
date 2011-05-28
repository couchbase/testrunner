import threading
import time
import fractions
import uuid

import mc_bin_client


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

        # thread state info
        self.stopped = False
        self.paused = True

        self.mutation_index = 0
        self.get_index = 0
        self.mutation_max = 0
        self.value_failures = 0

        # server info
        self.server_ip = load_info['server_info'][server_index].ip
        self.server_port = int(load_info['memcached_info'].get('bucket_port', 11211))
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

        self.uuid = uuid.uuid4()

        # connect
        self.server = mc_bin_client.MemcachedClient(self.server_ip, self.server_port)
        if self.bucket_name or self.bucket_password:
            self.server.sasl_auth_plain(self.bucket_name,self.bucket_password)


    def run(self):
        while True:
            # handle pause/stop
            while self.paused:
                if self.stopped:
                    return
                time.sleep(1)
            if self.stopped:
                return

            # do the actual work
            operation = self.get_operation()
            if operation == 'set':
                key = self.name + '_' + `self.get_mutation_key()`
                try:
#                    print `self.mutation_index` + " : " + `self.get_mutation_key()`
                    self.server.set(key, 0, 0, self.get_data())
                    if self.get_mutation_key() > self.mutation_max:
                        self.mutation_max = self.get_mutation_key()
                    self.mutation_index += 1
                except mc_bin_client.MemcachedError as e:
                    # temporary error
                    if e.status == 134:
                        time.sleep(1)
                    else:
                        print 'set',
                        print e
                        time.sleep(1)
            elif operation == 'get':
                key = self.name + '_' + `self.get_get_key()`
                try:
                    vdata = self.server.get(key)
                    data = vdata[2]
                    try:
                        data_expected = self.get_data(max(self.get_mutation_indexes(self.get_get_key())))
                        if data != data_expected:
                            self.value_failures += 1
                            raise
                    except:
                        print e
#                        print self.server.db
#                        print "create: " + `self.create`
#                        print "nocreate: " + `self.nocreate`
#                        print "get_index: " + `self.get_index`
#                        print "get_key: " + `self.get_get_key()`
#                        print "mutation_max: " + `self.mutation_max`
#                        print "mutation_indexes: " + `self.get_mutation_indexes(self.get_get_key())`
#                        print "getting data for mutation index: " + `max(self.get_mutation_indexes(self.get_get_key()))`
#                        print "got:      \'" + data + "\'"
#                        print "expected: \'" + data_expected + "\'"
#                        raise ValueError
                    self.get_index += 1
                except mc_bin_client.MemcachedError as e:
                    print 'get',
                    print e
                    time.sleep(1)


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
        return (str(uuid.uuid3(self.uuid,`index`)) * (1+valuesize/36))[:valuesize]



class LoadRunner(object):
    def __init__(self, load_info, dryrun=False):
        if dryrun:
            mc_bin_client.MemcachedClient = FakeMemcachedClient

        self.stopped = False
        self.paused = True

        self.threads = []
        self.num_servers = len(load_info['server_info'])
        self.num_threads = load_info['operation_info']['threads']
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

        if len(self.threads) == 0:
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
    def wait(self, time_limit=0):
        if time_limit == 0:
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
