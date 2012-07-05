#!/usr/bin/env python

from threading import Thread
from time import sleep

from mc_bin_client import MemcachedClient
from obs import Observer
from obs_req import ObserveRequestKey, ObserveRequest
from obs_res import ObserveResponse
from obs_def import ObservePktFmt
from obs_helper import VbucketHelper

class McsodaObserver(Observer, Thread):

    ctl = None
    cfg = None
    store = None
    awareness = None
    conns = {}
    freq = 3
    obs_keys = {}   # {server: [keys]}

    #TODO: topology change
    #TODO: socket timeout, fine-grained exceptions
    #TODO: network helper
    #TODO: remove hard-coded freq
    #TODO: CAS

    def __init__(self, ctl, cfg, store):
        self.ctl = ctl
        self.cfg = cfg
        self.store = store
        self._build_conns()
        super(McsodaObserver, self).__init__()

    def run(self):
        while self.ctl['run_ok']:
            self.observe()
            print "<%s> sleep for %d seconds" % (self.__class__.__name__, self.freq)
            sleep(self.freq)
        print "<%s> stopped running" % (self.__class__.__name__)

    def _build_conns(self):
        """build separate connections based on store"""
        if not self.store:
            print "<%s> failed to build connections, invalid store object"\
            % self.__class__.__name__
            return False

        if self.store.__class__.__name__ == "StoreMemcachedBinary":
            conn = MemcachedClient(self.store.conn.host, self.store.conn.port)
            server_str = "{0}:{1}".format(self.store.conn.host, self.store.conn.port)
            self.conns[server_str] = conn
        elif self.store.__class__.__name__ == "StoreMembaseBinary":
            for memcached in self.store.awareness.memcacheds.itervalues():
                conn = MemcachedClient(memcached.host, memcached.port)
                server_str = "{0}:{1}".format(conn.host, conn.port)
                self.conns[server_str] = conn
            self.awareness = self.store.awareness
        else:
            print "<%s> error: unsupported store object %s" %\
                  (self.__class__.__name__, store.__class__.__name__)
            return False

        return True

    def _send(self):
        self.obs_keys.clear()   # {server: [keys]}

        with self._keys.mutex:
            for key in self._keys.queue:
                vbucketid = VbucketHelper.get_vbucket_id(key, self.cfg.get("vbuckets", 0))
                obs_key = ObserveRequestKey(key, vbucketid)
                server = self._get_server_str(vbucketid)
                vals = self.obs_keys.get(server, [])
                vals.append(obs_key)
                self.obs_keys[server] = vals

        reqs = []
        for server, keys in self.obs_keys.iteritems():
            req = ObserveRequest(keys)
            pkt = req.pack()
            try:
                self.conns[server].s.send(pkt)
            except Exception as e:
                print "<%s> failed to send observe pkt : %s" % (self.__class__.__name__, e)
                return None
            reqs.append(req)

        print "reqs::"
        print reqs
        return reqs

    def _recv(self):
        print "<%s> observe receive responses" % self.__class__.__name__

        responses = []
        for server in self.obs_keys.iterkeys():
            hdr = ''
            while len(hdr) < ObservePktFmt.OBS_RES_HDR_LEN:
                try:
                    hdr += self.conns[server].s.recv(ObservePktFmt.OBS_RES_HDR_LEN)
                except Exception as e:
                    print "<%s> failed to recv observe pkt: %s" % (self.__class__.__name__, e)
                    return None
            res = ObserveResponse()

            if not res.unpack_hdr(hdr):
                return None

            # TODO measure server/client side latency

            body = ''
            while len(body) < res.body_len:
                body += self.conns[server].s.recv(res.body_len)
            res.unpack_body(body)

            # TODO: error check

            print "res::<%s>" % server
            print res
            responses.append(res)

        return responses

    def _get_server_str(self, vbucketid):
        """retrieve server string {ip:port} based on vbucketid"""
        if self.awareness:
            server = self.awareness.vBucketMap[vbucketid]
            return server
        elif len(self.conns):
            return self.conns.iterkeys().next()

        return None

    def _reconn(self):
        pass

        #    def save_obs_latency(self, latency):
        #        if not latency:
        #            return
        #        print " persist msec: {0}".format(latency)
        #        self.store.add_timing_sample("observe", float(latency))
        #        if self.store.sc:
        #            self.store.save_stats()