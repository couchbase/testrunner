import threading
import random
import zlib
import time

class KVStore(object):
    def __init__(self, num_locks=16):
        self.num_locks = num_locks
        self.reset()

    def reset(self):
        self.cache = {}
        for itr in range(self.num_locks):
            self.cache[itr] = {"lock"   : threading.Lock(),
                               "partition"  : Partition()}

    def acquire_partition(self, key):
        partition = self.cache[self._hash(key)]
        partition["lock"].acquire()
        return partition["partition"]

    def release_partition(self, key):
        if isinstance(key, str):
            self.cache[self._hash(key)]["lock"].release()
        elif isinstance(key, int):
            self.cache[key]["lock"].release()
        else:
            raise(Exception("Bad key"))

    def acquire_random_partition(self, has_valid = True):
        seed = random.choice(range(self.num_locks))
        for itr in range(self.num_locks):
            part_num = (seed + itr) % self.num_locks
            self.cache[part_num]["lock"].acquire()
            if has_valid and self.cache[part_num]["partition"].has_valid_keys():
                return self.cache[part_num]["partition"], part_num
            if not has_valid and self.cache[part_num]["partition"].has_deleted_keys():
                return self.cache[part_num]["partition"], part_num
            self.cache[part_num]["lock"].release()
        return None, None

    def key_set(self):
        valid_keys = []
        deleted_keys = []
        for itr in range(self.num_locks):
            self.cache[itr]["lock"].acquire()
            partition = self.cache[itr]["partition"]
            valid_keys.extend(partition.valid_key_set())
            deleted_keys.extend(partition.deleted_key_set())
            self.cache[itr]["lock"].release()
        return valid_keys, deleted_keys

    def __len__(self):
        return sum([len(self.cache[itr]["partition"]) for itr in range(self.num_locks)])

    def _hash(self, key):
        return zlib.crc32(key) % self.num_locks

class Partition(object):
    def __init__(self):
        self.valid = {}
        self.deleted = {}

    def set(self, key, value, exp=0):
        if key in self.deleted:
            del self.deleted[key]
        if exp != 0:
            exp = (time.time() + exp)
        self.valid[key] = {"value"   : value,
                           "expires" : exp}

    def delete(self, key):
        if key in self.valid:
            self.deleted[key] = self.valid[key]["value"]
            del self.valid[key]

    def get_valid(self, key):
        if key in self.valid:
            if not self._expired(key):
                return self.valid[key]["value"]
        return None

    def get_deleted(self, key):
        if key in self.deleted:
            return self.deleted[key]
        if self._expired(key):
            return self.deleted[key]
        return None

    def get_random_valid_key(self):
        try:
            while True:
                key = random.choice(self.valid.keys())
                if not self._expired(key):
                    return key
        except IndexError:
            return None

    def get_random_deleted_key(self):
        try:
            return random.choice(self.deleted.keys())
        except IndexError:
            return None

    def valid_key_set(self):
        key_set = []
        for key in self.valid:
            key_set.append(key)
        return key_set

    def deleted_key_set(self):
        key_set = []
        for key in self.deleted:
            key_set.append(key)
        return key_set

    def has_valid_keys(self):
        if len(self.valid) > 0:
            return True
        return False

    def has_deleted_keys(self):
        if len(self.deleted) > 0:
            return True
        return False

    def _expired(self, key):
        if key in self.valid:
            if self.valid[key]["expires"] == 0:
                return False
            if self.valid[key]["expires"] < time.time():
                self.deleted[key] = self.valid[key]["value"]
                del self.valid[key]
                return True
        return False

    def __len__(self):
        length = 0
        for key in self.valid.keys():
            if not self._expired(key):
                length += 1
        return length
