#!/usr/bin/env python

from Queue import Queue

from obs_def import ObserveKeyState

class Observer:
    #TODO: logging

    _keys = Queue()     # keys to be observed, key = [:string]

    def _observe_blocking(self, key_state):
        if self._keys.empty():
            print "<%s> empty keys" % self.__class__.__name__
            return

        print "<%s> self._keys %s" % (self.__class__.__name__, self._keys.queue)

        self._send()
        responses = self._recv()
        if responses:
            self.cmp_rm_keys(responses, key_state)

    def observe(self, key_state=ObserveKeyState.OBS_PERSISITED, blocking=True):
        if blocking:
            self._observe_blocking(key_state)
        else:
            raise NotImplementedError("<%s> unblocking observe has not been implemented"
                % self.__class__.__name__ )

    def load_keys(self, keys):
        if not keys:
            print "<%s> invalid argument key: %s" % (self.__class__.__name__, keys)
            return

        if self._keys.empty():
            print "<%s> load_keys = %s" % (self.__class__.__name__, keys)
            self._keys.put(keys)

    def clear_keys(self):
        with self._keys.mutex:
            self._keys.queue.clear()

    def cmp_rm_keys(self, responses, key_state):
        """
        remove keys based on (@param: responses),
        using key_state as filter
        """
        if not responses:
            print "<%s> empty responses" % self.__class__.__name__
            return True

        for res in responses:
            if res.__class__.__name__ != "ObserveResponse":
                print "<%s> invalid response" % self.__class__.__name__
                return False

            if self._keys.empty():
                return True

            for obs_key in res.keys:
                if obs_key.key_state == key_state and\
                   obs_key.key in self._keys.queue:
                    with self._keys.mutex:
                        self._keys.queue.remove(obs_key.key)

        return True

    def build_conns(self):
        raise NotImplementedError(
            "<%s> build_conns() has not been implemented" % self.__class__.__name__)

    def _send(self):
        """send packets to servers"""
        raise NotImplementedError(
            "<%s> send() has not been implemented" % self.__class__.__name__)

    def _recv(self):
        """receive packets from servers"""
        raise NotImplementedError(
            "<%s> recv() has not been implemented" % self.__class__.__name__)

