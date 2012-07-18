#!/usr/bin/env python

from crc32 import crc32_hash
from functools import wraps

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
        self.dict = {}
        self.mutex = _threading.RLock()
        self.not_empty = _threading.Condition(self.mutex)

    def __repr__(self):
        return "<%s> dict: %s" % (self.__class__.__name__, self.dict)

    def put(self, key, value):
        with self.mutex:
            self.dict[key] = value
            self.not_empty.notify()

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

    def wait_not_empty(self):
        """
        Wait until an item becomes available.

        @caution: Not a traditional pro/con call.
                  To minimize locking scope,
                  items won't be consumed here.
        """
        with self.mutex:
            while not self._size():
                self.not_empty.wait()

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

def synchronized(lock_name):
    """synchronized access to class method"""
    def _outer(func):
        @wraps(func)
        def _inner(self, *args, **kwargs):
            try:
                lock = self.__getattribute__(lock_name)
            except AttributeError:
                print "<synchronized> cannot find lock: %s" % lock_name
                return _inner
            with lock:
                return func(self, *args, **kwargs)
        return _inner
    return _outer
