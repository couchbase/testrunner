#!/usr/bin/env python

from crc32 import crc32_hash

try:
    import threading as _threading
except ImportError:
    import dummy_threading as _threading

class VbucketHelper:

    @staticmethod
    def get_vbucket_id(key, num_vbuckets):
        vbucketId = 0
        if num_vbuckets > 0:
            vbucketId = crc32_hash(key) & (num_vbuckets - 1)
        return vbucketId

class SyncDict:
    """
    Synchronized dict wrapper with basic atomic operations.
    """
    def __init__(self):
        self.mutex = _threading.Lock()
        self.dict = {}

    def __repr__(self):
        return "<%s> dict: %s" % (self.__class__.__name__, self.dict)

    def put(self, key, value):
        with self.mutex:
            self.dict[key] = value

    def get(self, key):
        with self.mutex:
            try:
                return self.dict[key]
            except KeyError as e:
                print "<%s> failed to get key %s : %s"\
                    % (self.__class__.__name__, key, e)
        return None

    def pop(self, key):
        with self.mutex:
            try:
                return self.dict.pop(key)
            except KeyError as e:
                print "<%s> failed to pop key %s : %s"\
                    % (self.__class__.__name__, key, e)
        return None

    def remove(self, key):
        with self.mutex:
            try:
                del self.dict[key]
            except KeyError as e:
                print "<%s> failed to delete key %s : %s"\
                    % (self.__class__.__name__, key, e)
                return False
        return True

    def clear(self):
        with self.mutex:
            self.dict.clear()

    def empty(self):
        with self.mutex:
            return not self._size()

    def size(self):
        with self.mutex:
            return self._size()

    def _size(self):
        return len(self.dict)