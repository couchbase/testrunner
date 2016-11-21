#!/usr/bin/env python
"""
Ascii memcached test client.
"""

import socket
import select
import exceptions

import memcacheConstants


class MemcachedError(exceptions.Exception):
    """Error raised when a command fails."""

    def __init__(self, status, msg):
        supermsg = 'Memcached error #' + status
        if msg:
            supermsg += ":  " + msg
        exceptions.Exception.__init__(self, supermsg)

        self.status = status
        self.msg = msg

    def __repr__(self):
        return "<MemcachedError #%d ``%s''>" % (self.status, self.msg)


class MemcachedAsciiClient(object):
    """Simple ascii memcached client."""

    def __init__(self, host='127.0.0.1', port=11211, timeout=30):
        self.host = host
        self.port = port
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.timeout = timeout
        self.s.connect_ex((host, port))

    def close(self):
        self.s.close()

    def __del__(self):
        self.close()

    def _send_msg(self, cmd):
        _, w, _ = select.select([], [self.s], [], self.timeout)
        if w:
            self.s.send(cmd)
        else:
            raise exceptions.EOFError(
                "Timeout waiting for socket send. from {0}".format(self.host))

    def _recv_msg(self):
        r, _, _ = select.select([self.s], [], [], self.timeout)
        if r:
            response = ""
            while not response.endswith("\r\n"):
                data = self.s.recv(1)
                if data == '':
                    raise exceptions.EOFError(
                        "Got empty data (remote died?). from {0}".format(
                            self.host))
                response += data
            return response[:-2]
        else:
            raise exceptions.EOFError(
                "Timeout waiting for socket recv. from {0}".format(self.host))

    def _recv_data(self, length):
        r, _, _ = select.select([self.s], [], [], self.timeout)
        if r:
            response = ""
            while len(response) < length + 2:
                data = self.s.recv((length + 2) - len(response))
                if data == '':
                    raise exceptions.EOFError(
                        "Got empty data (remote died?). from {0}".format(
                            self.host))
                response += data
            return response[:-2]
        else:
            raise exceptions.EOFError(
                "Timeout waiting for socket recv. from {0}".format(self.host))

    def _do_store(self, cmd):
        """Send a command and await its response."""
        self._send_msg(cmd)
        return self._recv_msg()

    def _do_retrieve(self, cmd):
        """Send a command and await its response."""
        self._send_msg(cmd)
        msg = self._recv_msg()
        result = {}
        error = ""
        while msg.split(" ")[0] == "VALUE":
            key = msg.split(" ")[1]
            flags = int(msg.split(" ")[2]) % 2 ** 32
            length = int(msg.split(" ")[3])
            cas = int(msg.split(" ")[4])
            data = self._recv_data(length)
            result[key] = (flags, cas, data)
            msg = self._recv_msg()
        if msg != "END":
            error = msg
        return result, error

    def _do_stats_version(self, cmd):
        """Send a command and await its response."""
        self._send_msg(cmd)
        msg = self._recv_msg()
        result = {}
        error = ""
        while msg.split(" ")[0] == "STAT" or msg.split(" ")[0] == "VERSION":
            print "msg:", msg
            kind = msg.split(" ")[0]
            key = msg.split(" ")[1]
            if kind == "VERSION":
                return key, ""
            value = msg.split(" ")[2]
            result[key] = value
            msg = self._recv_msg()
        if msg != "END":
            error = msg
        return result, error

    def _do_incr_decr(self, cmd):
        """Send a command and await its response."""
        self._send_msg(cmd)
        msg = self._recv_msg()
        try:
            # asci incr/decr doesn't give us the cas
            return (int(msg), 0), ""
        except ValueError:
            return None, msg

    def append(self, key, value, cas=0):
        response = self._do_store(
            "append {0} 0 0 {1} {2}\r\n{3}\r\n".format(key, len(value), cas,
                                                       value))
        if response != "STORED":
            raise MemcachedError(-1, response)

    def prepend(self, key, value, cas=0):
        response = self._do_store(
            "prepend {0} 0 0 {1} {2}\r\n{3}\r\n".format(key, len(value), cas,
                                                        value))
        if response != "STORED":
            raise MemcachedError(-1, response)

    def incr(self, key, amt=1, init=0, exp=0):
        """Increment or create the named counter."""
        response, error = self._do_incr_decr("incr {0} {1}\r\n".format(key,
                                                                       amt))
        if error:
            raise MemcachedError(-1, error)
        return response

    def decr(self, key, amt=1, init=0, exp=0):
        """Decrement or create the named counter."""
        response, error = self._do_incr_decr("decr {0} {1}\r\n".format(key,
                                                                       amt))
        if error:
            raise MemcachedError(-1, error)
        return response

    def set(self, key, exp, flags, val):
        """Set a value in the memcached server."""
        response = self._do_store(
            "set {0} {1} {2} {3}\r\n{4}\r\n".format(key, flags, exp, len(val),
                                                    val))
        if response != "STORED":
            raise MemcachedError(-1, response)

    def add(self, key, exp, flags, val):
        """Add a value in the memcached server iff it doesn't already exist."""
        response = self._do_store(
            "add {0} {1} {2} {3}\r\n{4}\r\n".format(key, flags, exp, len(val),
                                                    val))
        if response != "STORED":
            raise MemcachedError(-1, response)

    def replace(self, key, exp, flags, val):
        """Replace a value in the memcached server iff it already exists."""
        response = self._do_store(
            "replace {0} {1} {2} {3}\r\n{4}\r\n".format(key, flags, exp,
                                                        len(val), val))
        if response != "STORED":
            raise MemcachedError(-1, response)

    def get(self, key):
        """Get the value for a given key within the memcached server."""
        response, error = self._do_retrieve("gets {0}\r\n".format(key))
        if error:
            raise MemcachedError(-1, error)
        return response.items()[0][1]

    def getl(self, key, exp=15):
        """Get the value for a given key within the memcached server."""
        response, error = self._do_retrieve("getl {0} {1}\r\n".format(key,
                                                                      exp))
        if error:
            raise MemcachedError(-1, error)
        return response.items()[0][1]

    def cas(self, key, exp, flags, old_val, val):
        """CAS in a new value for the given key and comparison value."""
        response = self._do_store(
            "cas {0} {1} {2} {3} {4}\r\n{5}\r\n".format(key, flags, exp,
                                                        len(val), old_val,
                                                        val))
        if response != "STORED":
            raise MemcachedError(-1, response)

    def touch(self, key, exp):
        """Touch a key in the memcached server."""
        response = self._do_store("touch {0} {1}\r\n".format(key, exp))
        if response != "STORED":
            raise MemcachedError(-1, response)

    def gat(self, key, exp):
        """Get the value for a given key and touch it within the memcached
        server."""
        response, error = self._do_retrieve("gat {0} {1}\r\n".format(key, exp))
        if error:
            raise MemcachedError(-1, error)
        return response.items()[0][1]

    def version(self):
        """Get the value for a given key within the memcached server."""
        response, error = self._do_stats_version("version\r\n")
        if error:
            raise MemcachedError(-1, error)
        return response

    def get_multi(self, keys):
        """Get values for any available keys in the given iterable.
        Returns a dict of matched keys to their values."""

        cmd = "gets"
        for key in keys:
            cmd += " " + key
        cmd += "\r\n"
        response, error = self._do_retrieve(cmd)
        if error:
            raise MemcachedError(-1, error)
        return response

    def stats(self, sub=''):
        """Get stats."""
        if sub:
            sub = " " + sub
        response, error = self._do_stats_version("stats{0}\r\n".format(sub))
        if error:
            raise MemcachedError(-1, error)
        return response

    def delete(self, key, cas=0):
        """Delete the value for a given key within the memcached server."""
        response = self._do_store("delete {0} {1}\r\n".format(key, cas))
        if response != "DELETED":
            raise MemcachedError(-1, response)

    def flush(self, timebomb=0):
        """Flush all storage in a memcached instance."""
        return self._doCmd(memcacheConstants.CMD_FLUSH, '', '',
                           memcacheConstants.struct.pack(
                               memcacheConstants.FLUSH_PKT_FMT,
                               timebomb))
