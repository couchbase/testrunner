import threading
import random
import zlib
import time
import copy


class KVStore(object):
    def __init__(self, num_locks=16):
        self.num_locks = num_locks
        self.reset()

    def reset(self):
        self.cache = {}
        for itr in range(self.num_locks):
            self.cache[itr] = {"lock": threading.Lock(),
                               "partition": Partition(itr)}

    def acquire_partition(self, key):
        partition = self.cache[self._hash(key)]
        partition["lock"].acquire()
        return partition["partition"]

    def acquire_partitions(self, keys):
        part_obj_keys = {}
        for key in keys:
            partition = self.cache[self._hash(key)]
            partition_obj = partition["partition"]
            if partition_obj not in part_obj_keys:
                partition["lock"].acquire()
                part_obj_keys[partition_obj] = []
            part_obj_keys[partition_obj].append(key)
        return part_obj_keys

    def release_partitions(self, partition_objs):
        for partition_obj in partition_objs:
            partition = self.cache[partition_obj.part_id]
            partition["lock"].release()

    def release_partition(self, key):
        if isinstance(key, str):
            self.cache[self._hash(key)]["lock"].release()
        elif isinstance(key, int):
            self.cache[key]["lock"].release()
        else:
            raise(Exception("Bad key"))

    def acquire_random_partition(self, has_valid=True):
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
    def __init__(self, part_id):
        self.part_id = part_id
        self.__valid = {}
        self.__deleted = {}
        self.__timestamp = {}
        self.__expired_keys = []

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
