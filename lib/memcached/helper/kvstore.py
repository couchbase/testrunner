import threading
import random
import zlib
import time
import copy
import collections


class KVStore(object):
    def __init__(self, num_locks=16):
        self.num_locks = num_locks
        self.reset()
        self.acquire_lock = threading.Lock()     # needed for deadlocks where different threads grab some of the partitions

    def reset(self):
        self.cache = {}
        for itr in range(self.num_locks):
            self.cache[itr] = {"lock": threading.Lock(),
                               "partition": Partition(itr)}

    def acquire_partition(self, key):
        partition = self.cache[self._hash(key)]
        return partition

    def acquire_partitions(self, keys):
        part_obj_keys = collections.defaultdict(list)
        for key in keys:
            partition = self.cache[self._hash(key)]
            '''
            frozenset because dict is mutable , cant be hashed
            frozenset converts a dict to immutable object
            '''
            part_obj_keys[frozenset(partition.items())].append(key)
        return part_obj_keys

    def acquire_random_partition(self, has_valid=True):
        seed = random.choice(range(self.num_locks))
        for itr in range(self.num_locks):
            part_num = (seed + itr) % self.num_locks
            if has_valid and self.cache[part_num]["partition"].has_valid_keys():
                return self.cache[part_num]["partition"], part_num
            if not has_valid and self.cache[part_num]["partition"].has_deleted_keys():
                return self.cache[part_num]["partition"], part_num
        return None, None

    def key_set(self):
        valid_keys = []
        deleted_keys = []
        for itr in range(self.num_locks):
            self.cache[itr]["lock"].acquire()
            partition = self.cache[itr]["partition"]
            valid_keys.extend(partition.valid_key_set())
            deleted_keys.extend(partition.deleted_key_set())
        return valid_keys, deleted_keys

    def __len__(self):
        return sum([len(self.cache[itr]["partition"]) for itr in range(self.num_locks)])

    def _hash(self, key):
        return zlib.crc32(key) % self.num_locks


class Partition(object):
    def __init__(self, part_id):
        self.part_id = part_id
        self.__valid = {}
        self.__deleted = {}
        self.__timestamp = {}
        self.__expired_keys = []

    def get_all_keys(self, valid = False, deleted = False, expired = False):
        '''
        for debugging
        :return:
        '''
        if valid:
            return self.__valid
        if deleted:
            return self.__deleted
        if expired:
            return self.__expired_keys

    def set(self, key, value, exp=0, flag=0):
        if key in self.__deleted:
            del self.__deleted[key]
        if key in self.__expired_keys:
            self.__expired_keys.remove(key)
        if exp != 0:
            exp = (time.time() + exp)
        self.__valid[key] = {"value": value,
                           "expires": exp,
                           "flag": flag}
        self.__timestamp[key] = time.time()

    def delete(self, key):
        if key in self.__valid:
            self.__deleted[key] = self.__valid[key]["value"]
            self.__timestamp[key] = time.time()
            del self.__valid[key]

    def get_timestamp(self, key):
        return self.__timestamp.get(key, 0)

    def get_key(self, key):
        return self.__valid.get(key)

    def get_valid(self, key):
        self.__expire_key(key)
        if key in self.__valid:
            return self.__valid[key]["value"]
        return None

    def get_deleted(self, key):
        self.__expire_key(key)
        return self.__deleted.get(key)

    def get_random_valid_key(self):
        try:
            key = random.choice(self.valid_key_set())
            return key
        except IndexError:
            return None

    def get_random_deleted_key(self):
        try:
            return random.choice(self.__deleted.keys())
        except IndexError:
            return None

    def get_flag(self, key):
        self.__expire_key(key)
        if key in self.__valid:
            return self.__valid[key]["flag"]
        return None

    def valid_key_set(self):
        valid_keys = copy.copy(self.__valid.keys())
        [self.__expire_key(key) for key in valid_keys]
        return self.__valid.keys()

    def deleted_key_set(self):
        valid_keys = copy.copy(self.__valid.keys())
        [self.__expire_key(key) for key in valid_keys]
        return self.__deleted.keys()

    def expired_key_set(self):
        valid_keys = copy.copy(self.__valid.keys())
        [self.__expire_key(key) for key in valid_keys]
        return self.__expired_keys

    def has_valid_keys(self):
        return len(self.__valid) > 0

    def has_deleted_keys(self):
        return len(self.__deleted) > 0

    def __expire_key(self, key):
        if key in self.__valid:
            if self.__valid[key]["expires"] != 0 and self.__valid[key]["expires"] < time.time():
                self.__deleted[key] = self.__valid[key]["value"]
                self.__expired_keys.append(key)
                del self.__valid[key]

    def expired(self, key):
        if key not in self.__valid and key not in self.__deleted:
            raise Exception("Key: %s is not a valid key" % key)
        self.__expire_key(key)
        return key in self.__expired_keys

    def __len__(self):
        [self.__expire_key(key) for key in self.__valid.keys()]
        return len(self.__valid.keys())

    def __eq__(self, other):
        if isinstance(other, Partition):
            return self.part_id == other.part_id
        return False

    def __hash__(self):
        return self.part_id.__hash__()


class Synchronized(object):
    def __init__(self, partition):
        self.partition = partition

    def __enter__(self):
        self.partition["lock"].acquire()
        return self.partition["partition"]

    def __exit__(self, exc_type, exc_value, traceback):
        self.partition["lock"].release()
