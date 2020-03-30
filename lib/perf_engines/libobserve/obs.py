#!/usr/bin/env python

import time

from obs_def import ObserveKeyState, ObserveStatus
from obs_helper import SyncDict

class Observable:

    def __init__(self, key="", cas=0x0000000000000000,
                 start_time=0, persist_count=1, repl_count=1):
        self.key = key
        self.cas = cas
        self.status = ObserveStatus.OBS_UNKNOWN
        if start_time:
            self.start_time = start_time
        else:
            self.start_time = time.time()
        self.persist_end_time = 0
        self.persist_count = int(persist_count)
        self.persist_servers = set()
        self.repl_end_time = 0
        self.repl_count = int(repl_count)
        self.repl_servers = set()

    def remove_server(self, server, repl=False, stats=True):
        """ remove server from checklist, decrement counter
            and record finish time if necessary
        """
        if repl:
            if server in self.repl_servers:
                self.repl_servers.remove(server)
                self.repl_count -= 1
                if stats and not self.repl_count:
                    self.repl_end_time = time.time()
        else:
            if server in self.persist_servers:
                self.persist_servers.remove(server)
                self.persist_count -= 1
                if stats and not self.persist_count:
                    self.persist_end_time = time.time()

    def __repr__(self):
        return "<%s> key: %s, cas: %x, status: %x, start_time: %d, "\
               "persist_end_time: %d ,repl_end_time: %d, "\
               "persist_count = %d, persist_servers = %s, "\
               "repl_count: %d, repl_servers: %s\n" %\
               (self.__class__.__name__, self.key, self.cas, self.status,
                self.start_time, self.persist_end_time,self.repl_end_time,
                self.persist_count, self.persist_servers,
                self.repl_count, self.repl_servers)

    def __str__(self):
        return self.__repr__()

class Observer:
    #TODO: logging

    # {key_str: Observable}
    _observables = SyncDict()

    def _observe_blocking(self):
        self._observables.wait_not_empty()

        print "<%s> self._observables %s" % (self.__class__.__name__, self._observables)

        if not self._send():
            return False

        responses = self._recv()
        if responses:
            self.update_observables(responses)

    def observe(self, blocking=True):
        if blocking:
            self._observe_blocking()
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

    def update_observables(self, responses):
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

        for server, res_key in res_keys:
            obs = self._observables.get(res_key.key)

            if not obs:
                continue

            if server in obs.persist_servers:
                if obs.cas == res_key.cas:
                    if res_key.key_state == ObserveKeyState.OBS_PERSISITED:
                        obs.remove_server(server, repl=False)
                else:
                    obs.status = ObserveStatus.OBS_MODIFIED
            elif server in obs.repl_servers:
                if res_key.key_state in \
                    [ObserveKeyState.OBS_PERSISITED, ObserveKeyState.OBS_FOUND]:
                        obs.remove_server(server, repl=True,
                                          stats=(obs.cas == res_key.cas))
                elif res_key.key_state == ObserveKeyState.OBS_IMPOSSIBLE:
                    print "<%s> invalid key_state %x from repl sever" \
                        % (self.__class__.__name__, server)
                    obs.remove_server(server, repl=True, stats=False)
            else:
                print "<%s> invalid server: %s" \
                    % (self.__class__.__name__, server)
                obs.status = ObserveStatus.OBS_ERROR

            if obs.persist_count <= 0 and \
                obs.repl_count <= 0:
                obs.status = ObserveStatus.OBS_SUCCESS

            self._observables.put(obs.key, obs)
        return True

    def num_observables(self):
        return self._observables.size()

    def reskey_generator(self, responses):
        for server, reses in responses.iteritems():
            for res in reses:
                if res.__class__.__name__ == "ObserveResponse":
                    for res_key in res.keys:
                        yield (server, res_key)

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

