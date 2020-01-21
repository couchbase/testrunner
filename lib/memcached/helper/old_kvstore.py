import threading
import time
import copy
class ClientKeyValueStore(object):

    def __init__(self):
        self._rlock = threading.Lock()
        self._wlock = threading.Lock()
        self._cache = {}

        #dict : {"key","value","status"} status could be deleted , expired

    def write(self, key, value, ttl= -1):
        self._rlock.acquire()
        self._wlock.acquire()
        if ttl >= 0:
            self._cache[key] = {"key":key, "value":value, "ttl":(time.time() + ttl), "status":"valid"}
        else:
            self._cache[key] = {"key":key, "value":value, "ttl":-1, "status":"valid"}
        self._rlock.release()
        self._wlock.release()

    def delete(self, key):
        self._rlock.acquire()
        self._wlock.acquire()
        if key in self._cache:
            self._cache[key]["status"] = "deleted"
        else:
            self._cache[key] = {"key":key, "value":"N/A", "ttl":-1, "status":"delete"}
        self._rlock.release()
        self._wlock.release()

    def read(self, key):
        item = {}
        self._rlock.acquire()
        if key in self._cache:
            item = self._cache[key]
        self._rlock.release()
        if item["ttl"] >= 0 and item["ttl"] < time.time():
            item["status"] = "expired"
        return item


    def keys(self):
        #dump all the keys
        keys = []
        self._rlock.acquire()
        keys =  copy.deepcopy(list(self._cache.keys()))
        self._rlock.release()
        return keys

    def valid_items(self):
        keys = list(self.keys())
        valid_keys = []
        for k in keys:
            item = self.read(k)
            if item["status"] == "valid":
                valid_keys.append(k)
        return valid_keys