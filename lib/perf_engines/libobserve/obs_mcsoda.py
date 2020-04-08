#!/usr/bin/env python
from threading import Thread, RLock
from time import sleep
import socket

from lib.mc_bin_client import MemcachedClient
from lib.perf_engines.sys_helper import SocketHelper, synchronized

from .obs import Observer
from .obs_req import ObserveRequestKey, ObserveRequest
from .obs_res import ObserveResponse
from .obs_def import ObservePktFmt, ObserveStatus, ObserveKeyState
from .obs_helper import VbucketHelper

BACKOFF = 0.2
MAX_BACKOFF = 1

class McsodaObserver(Observer, Thread):

    ctl = None
    cfg = None
    store = None
    awareness = None
    conns = {}
    obs_keys = {}   # {server: [keys]}
    callback = None

    #TODO: handle persist_count != 1
    #TODO: socket timeout, fine-grained exceptions
    #TODO: network helper
    #TODO: wait call timeout

    def __init__(self, ctl, cfg, store, callback):
        self.ctl = ctl
        self.cfg = cfg
        self.store = store
        self.callback = callback
        self.conn_lock = RLock()
        self._build_conns()
        self.backoff = self.cfg.get('obs-backoff', BACKOFF)
        self.max_backoff = self.cfg.get('obs-max-backoff', MAX_BACKOFF)
        super(McsodaObserver, self).__init__()

    def run(self):
        while self.ctl['run_ok']:
            self.observe()
            try:
                next(self.observable_filter(ObserveStatus.OBS_UNKNOWN))
                print("<%s> sleep for %f seconds" % (self.__class__.__name__, self.backoff))
                sleep(self.backoff)
                self.backoff = min(self.backoff * 2, self.max_backoff)
            except StopIteration:
                self.measure_client_latency()
                self.clear_observables()
                if self.callback:
                    self.callback(self.store)
                self.backoff = self.cfg.get('obs-backoff', BACKOFF)
        print("<%s> stopped running" % (self.__class__.__name__))

    @synchronized("conn_lock")
    def _build_conns(self):
        """build separate connections based on store"""
        if not self.store:
            print("<%s> failed to build connections, invalid store object"\
                % self.__class__.__name__)
            return False

        if self.store.__class__.__name__ == "StoreMemcachedBinary":
            conn = MemcachedClient(self.store.conn.host, self.store.conn.port)
            server_str = "{0}:{1}".format(self.store.conn.host, self.store.conn.port)
            self.conns[server_str] = conn
        elif self.store.__class__.__name__ == "StoreMembaseBinary":
            for memcached in self.store.awareness.memcacheds.values():
                conn = MemcachedClient(memcached.host, memcached.port)
                server_str = "{0}:{1}".format(conn.host, conn.port)
                self.conns[server_str] = conn
            self.awareness = self.store.awareness
        else:
            print("<%s> error: unsupported store object %s" %\
                  (self.__class__.__name__, store.__class__.__name__))
            return False

        return True

    @synchronized("conn_lock")
    def _refresh_conns(self):
        """blocking call to refresh connections based on topology change"""
        if not self.store:
            print("<%s> failed to refresh connections, invalid store object"\
                % self.__class__.__name__)
            return False

        print("<%s> refreshing connections" % self.__class__.__name__)

        if self.store.__class__.__name__ == "StoreMembaseBinary":
            old_keys = set(self.conns)
            new_keys = set(self.store.awareness.memcacheds)

            for del_server in old_keys.difference(new_keys):
                print("<%s> _refresh_conns: delete server: %s" \
                    % (self.__class__.__name__, del_server))
                del self.conns[del_server]

            for add_server in new_keys.difference(old_keys):
                print("<%s> _refresh_conns: add server: %s" \
                    % (self.__class__.__name__, add_server))
                self._add_conn(add_server)

            self.awareness = self.store.awareness

        return True

    @synchronized("conn_lock")
    def _add_conn(self, server):
        if not self.store:
            print("<%s> failed to add conn, invalid store object"\
                % self.__class__.__name__)
            return False

        if self.store.__class__.__name__ == "StoreMembaseBinary":
            print("<%s> _add_conn: %s"\
                % (self.__class__.__name__, server))
            host, port = server.split(":")
            conn = MemcachedClient(host, int(port))
            self.conns[server] = conn

        return True

    @synchronized("conn_lock")
    def _reconnect(self, conn):
        if not conn or\
            conn.__class__.__name__ != "MemcachedClient":
            print("<%s> failed to reconnect, invalid connection object"\
                % self.__class__.__name__)
            return False

        return conn.reconnect()

    def _send(self):
        self.obs_keys.clear()   # {server: [keys]}

        observables = self.observable_filter(ObserveStatus.OBS_UNKNOWN)
        with self._observables.mutex:
            for obs in observables:
                vbucketid = VbucketHelper.get_vbucket_id(obs.key, self.cfg.get("vbuckets", 0))
                obs_key = ObserveRequestKey(obs.key, vbucketid)
                if obs.persist_count > 0:
                    persist_server = self._get_server_str(vbucketid)
                    vals = self.obs_keys.get(persist_server, [])
                    vals.append(obs_key)
                    self.obs_keys[persist_server] = vals
                    if not obs.persist_servers:
                        obs.persist_servers.add(persist_server)
                        self._observables.put(obs.key, obs)
                if obs.repl_count > 0:
                    repl_servers = self._get_server_str(vbucketid, repl=True)
                    if len(repl_servers) < obs.repl_count:
                        print("<%s> not enough number of replication servers to observe"\
                            % self.__class__.__name__)
                        obs.status = ObserveStatus.OBS_ERROR # mark out this key
                        self._observables.put(obs.key, obs)
                        continue
                    if not obs.repl_servers:
                        obs.repl_servers.update(repl_servers)
                        self._observables.put(obs.key, obs)
                    for server in obs.repl_servers:
                        vals = self.obs_keys.get(server, [])
                        vals.append(obs_key)
                        self.obs_keys[server] = vals

        reqs = []
        for server, keys in self.obs_keys.items():
            req = ObserveRequest(keys)
            pkt = req.pack()
            try:
                self.conns[server].s.send(pkt)
            except KeyError as e:
                print("<%s> failed to send observe pkt : %s" % (self.__class__.__name__, e))
                self._add_conn(server)
                return None
            except Exception as e:
                print("<%s> failed to send observe pkt : %s" % (self.__class__.__name__, e))
                self._refresh_conns()
                return None
            reqs.append(req)

        print("reqs::")
        print(reqs)
        return reqs

    def _recv(self):

        responses = {}      # {server: [responses]}
        for server in self.obs_keys.keys():
            hdr = ''
            while len(hdr) < ObservePktFmt.OBS_RES_HDR_LEN:
                try:
                    hdr += self.conns[server].s.recv(ObservePktFmt.OBS_RES_HDR_LEN)
                except KeyError as e:
                    print("<%s> failed to recv observe pkt : %s" % (self.__class__.__name__, e))
                    self._add_conn(server)
                    return None
                except Exception as e:
                    print("<%s> failed to recv observe pkt: %s" % (self.__class__.__name__, e))
                    self._refresh_conns()
                    return None
            res = ObserveResponse()

            if not res.unpack_hdr(hdr):
                if res.status == ERR_NOT_MY_VBUCKET:
                    self._refresh_conns()
                return None

            body = ''
            while len(body) < res.body_len:
                body += self.conns[server].s.recv(res.body_len)
            res.unpack_body(body)

            # TODO: error check

            self.save_latency_stats(res.persist_stat//1000)

            print("res::<%s>" % server)
            print(res)
            vals = responses.get(server, [])
            vals.append(res)
            responses[server] = vals

        return responses

    def _get_server_str(self, vbucketid, repl=False):
        """retrieve server string {ip:port} based on vbucketid"""
        if self.awareness:
            if repl:
                server = self.awareness.vBucketMapReplica[vbucketid]
            else:
                server = self.awareness.vBucketMap[vbucketid]
            return server
        elif len(self.conns) and not repl:
            return next(iter(self.conns.keys()))

        return None

    def block_for_persistence(self, key, cas, server="", timeout=0):
        """
        observe a key until it has been persisted
        """
        self.backoff = self.cfg.get('obs-backoff', BACKOFF)

        while True:

            status, new_cas = self.observe_single(key, server, timeout)

            if status < 0:
                return False

            if new_cas != cas:
                print("<%s> block_for_persistence: key: %s, "\
                    "cas: %s has been modified"\
                    % (self.__class__.__name__, key, cas))
                return False

            elif status == ObserveKeyState.OBS_PERSISITED:
                return True
            elif status == ObserveKeyState.OBS_FOUND:
                sleep(self.backoff)
                self.backoff = min(self.backoff * 2, self.max_backoff)
                continue
            elif status == ObserveKeyState.OBS_NOT_FOUND:
                print("<%s> block_for_persistence: key: %s, cas: %s does not" \
                    " exist any more" % (self.__class__.__name__, key, cas))
                return False
            else:
                print("<%s> block_for_persistence: invalid key state: %x" \
                    % (self.__class__.__name__, res_key.key_state))
                return False

        return False # unreachable

    def block_for_replication(self, key, cas, num=1, timeout=0, persist=False):
        """
        observe a key until it has been replicated to @param num of servers

        @param persist : block until item has been persisted to disk
        """
        if not isinstance(num, int) or num <= 0:
            print("<%s> block_for_replication: invalid num %s" \
                % (self.__class__.__name__, num))
            return False

        vbucketid = \
            VbucketHelper.get_vbucket_id(key, self.cfg.get("vbuckets", 0))

        repl_servers = self._get_server_str(vbucketid, repl=True)

        if persist and not self.block_for_persistence(key, cas):
            return False

        self.backoff = self.cfg.get('obs-backoff', BACKOFF)

        print("<%s> block_for_replication: repl_servers: %s,"\
            " key: %s, cas: %s, vbucketid: %s" \
            % (self.__class__.__name__, repl_servers, key, cas, vbucketid))

        while len(repl_servers) >= num > 0:

            for server in repl_servers:

                if num == 0:
                    break

                status, new_cas = self.observe_single(key, server, timeout)

                if status < 0:
                    repl_servers.remove(server)
                    continue

                if new_cas and new_cas != cas:
                    # Due to the current protocol limitations,
                    # assume key is unique and new, skip this server
                    repl_servers.remove(server)
                    continue

                if status == ObserveKeyState.OBS_PERSISITED:
                    num -= 1
                    repl_servers.remove(server)
                    continue
                elif status == ObserveKeyState.OBS_FOUND:
                    if not persist:
                        num -= 1
                        repl_servers.remove(server)
                        continue
                elif status == ObserveKeyState.OBS_NOT_FOUND:
                    pass

                if len(repl_servers) == 1:
                    sleep(self.backoff)
                    self.backoff = min(self.backoff * 2, self.max_backoff)

        if num > 0:
            return False

        return True

    def observe_single(self, key, server="", timeout=0):
        """
        send an observe command and get the response back

        parse the response afterwards

        @return (status, cas)

        @status -1 : network error
        @status -2 : protocol error
        @status ObserveKeyState
        """
        cas = ""
        if not key:
            print("<%s> observe_single: invalid key" % self.__class__.__name__)
            return -1

        vbucketid = \
            VbucketHelper.get_vbucket_id(key, self.cfg.get("vbuckets", 0))
        if not server:
            server = self._get_server_str(vbucketid)
        req_key = ObserveRequestKey(key, vbucketid)

        req = ObserveRequest([req_key])
        pkt = req.pack()

        try:
            skt = self.conns[server].s
        except KeyError:
            print("<%s> observe_single: KeyError: %s" \
                % (self.__class__.__name__, server))
            self._add_conn(server)
            return -1, cas

        try:
            SocketHelper.send_bytes(skt, pkt, timeout)
        except IOError:
            print("<%s> observe_single: IOError: " \
                  "failed to send observe pkt : %s" \
                  % (self.__class__.__name__, pkt))
            self._reconnect(self.conns[server])
            self._refresh_conns()
            return -1, cas
        except socket.timeout:
            print("<%s> observe_single: timeout: " \
                "failed to send observe pkt : %s" \
                % (self.__class__.__name__, pkt))
            return -1, cas
        except Exception as e:
            print("<%s> observe_single: failed to send observe pkt : %s" \
                % (self.__class__.__name__, e))
            return -1, cas

        try:
            hdr = SocketHelper.recv_bytes(
                skt, ObservePktFmt.OBS_RES_HDR_LEN, timeout)
            res = ObserveResponse()
            if not res.unpack_hdr(hdr):
                if res.status == ERR_NOT_MY_VBUCKET:
                    self._refresh_conns()
                return -1, cas
            body = SocketHelper.recv_bytes(skt, res.body_len, timeout)
            res.unpack_body(body)
        except IOError:
            print("<%s> observe_single: IOError: failed to recv observe pkt" \
                % self.__class__.__name__)
            self._reconnect(self.conns[server])
            self._refresh_conns()
            return -1, cas
        except socket.timeout:
            print("<%s> observe_single: timeout: failed to recv observe pkt" \
                % self.__class__.__name__)
            return -1, cas
        except Exception as e:
            print("<%s> observe_single: failed to recv observe pkt : %s" \
                % (self.__class__.__name__, e))
            return -1, cas

        if not res:
            print("<%s> observe_single: empty response" \
                % self.__class__.__name__)
            return -1, cas

        key_len = len(res.keys)
        if key_len != 1:
            # we are not supposed to receive responses for more than one key,
            # otherwise, it's a server side protocol error
            print("<%s> observe_single: invalid number of keys in response: %d"\
                    % (self.s.__name__, key_len))
            return -2, cas

        res_key = res.keys[0]
        cas = res_key.cas

        if res_key.key != key:
            print("<%s> observe_single: invalid key %s in response"\
                % self.__class__.__name__)
            return -2, cas

        return res_key.key_state, cas

    def measure_client_latency(self):
        observables = self.observable_filter(ObserveStatus.OBS_SUCCESS)
        for obs in observables:
            persist_dur = obs.persist_end_time - obs.start_time
            repl_dur = obs.repl_end_time - obs.start_time
            print("<%s> saving client latency, "\
                  "key: %s, cas: %s, persist_dur: %f, repl_dur: %f"\
                  % (self.__class__.__name__, obs.key, obs.cas,
                     persist_dur, repl_dur))
            if persist_dur > 0:
                self.save_latency_stats(persist_dur, obs.start_time, False)
            if repl_dur > 0:
                self.save_latency_stats(persist_dur, obs.start_time,
                                        server=False, repl=True)

    def save_latency_stats(self, latency, time=0, server=True, repl=False):
        if not latency:
            return False    # TODO: simply skip 0

        if server:
            self.store.add_timing_sample("obs-persist-server", float(latency))
        else:
            if repl:
                cmd = "obs-repl-client" # TODO: # of replicas
            else:
                cmd = "obs-persist-client"
            self.store.add_timing_sample(cmd, float(latency))

        if self.store.sc:
            self.store.save_stats(time)

        return True
