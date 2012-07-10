#!/usr/bin/env python

import time

from obs_def import ObserveKeyState, ObserveStatus
from obs_helper import SyncDict

class Observable:
    key = ""
    cas = 0x0000000000000000
    status = ObserveStatus.OBS_UNKNOWN
    start_time = 0
    end_time = 0

    def __init__(self, key, cas, start_time=0):
        self.key = key
        self.cas = cas
        if start_time:
            self.start_time = start_time
        else:
            self.start_time = time.time()

    def __repr__(self):
        return "<%s> key: %s, cas: %x, status: %x, "\
               "start_time: %d, end_time: %d \n" %\
               (self.__class__.__name__, self.key, self.cas,
                self.status, self.start_time, self.end_time)

    def __str__(self):
        return self.__repr__()

class Observer:
    #TODO: logging

    # {key_str: Observable}
    _observables = SyncDict()

    def _observe_blocking(self, key_state):
        self._observables.wait_not_empty()

        print "<%s> self._observables %s" % (self.__class__.__name__, self._observables)

        self._send()
        responses = self._recv()
        if responses:
            self.update_observables(responses, key_state)

    def observe(self, key_state=ObserveKeyState.OBS_PERSISITED, blocking=True):
        if blocking:
            self._observe_blocking(key_state)
        else:
            raise NotImplementedError("<%s> unblocking observe has not been implemented"
                % self.__class__.__name__ )

    def load_observables(self, observables):
        """
        Load observables into cache for observation.
        @param observables must be a iterable collections of Observable
        """
        if not observables:
            print "<%s> invalid argument observables: %s" \
                    % (self.__class__.__name__, observables)
            return

        if self._observables.empty():
            print "<%s> load observables = %s" % (self.__class__.__name__, observables)
            for obs in observables:
                self._observables.put(obs.key, obs)

    def clear_observables(self):
        print "clear observables : %s" % self._observables
        self._observables.clear()

    def update_observables(self, responses, key_state):
        """
        Update observables based on (@param: responses),
        using key_state as filter
        """
        if not responses:
            print "<%s> empty responses" % self.__class__.__name__
            return True

        if self._observables.empty():
            return True

        res_keys = self.reskey_generator(responses)

        for res_key in res_keys:
            obs = self._observables.get(res_key.key)
            if not obs:
                continue
            elif obs.cas == res_key.cas:
                # TODO: race cond?
                if res_key.key_state == key_state :
                    obs.status = ObserveStatus.OBS_SUCCESS
            else:
                obs.status = ObserveStatus.OBS_MODIFIED
            obs.end_time = time.time()
            self._observables.put(obs.key, obs)

        return True

    def num_observables(self):
        return self._observables.size()

    def reskey_generator(self, responses):
        for res in responses:
            for res_key in res.keys:
                if res.__class__.__name__ == "ObserveResponse":
                    yield res_key

    def observable_filter(self, status):
        """
        Generate observables, using status as filter
        not thread safe.
        """
        for obs in self._observables.dict.itervalues():
            if obs.status == status:
                yield obs

    def _build_conns(self):
        raise NotImplementedError(
            "<%s> _build_conns() has not been implemented" % self.__class__.__name__)

    def _send(self):
        """send packets to servers"""
        raise NotImplementedError(
            "<%s> _send() has not been implemented" % self.__class__.__name__)

    def _recv(self):
        """receive packets from servers"""
        raise NotImplementedError(
            "<%s> _recv() has not been implemented" % self.__class__.__name__)

