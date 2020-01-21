#!/usr/bin/env python

import logging
from functools import wraps

class SocketHelper:

    @staticmethod
    def send_bytes(skt, buf, timeout=0):
        """
        Send bytes to the socket
        @raise ValueError, IOError, socket.timeout, Exception
        """
        if not buf or not skt:
            raise ValueError("<send_bytes> invalid socket descriptor or buf")

        if timeout:
            skt.settimeout(timeout)
        else:
            skt.settimeout(None)

        length = len(buf)
        sent_total = 0

        while sent_total < length:
            sent = skt.send(buf)
            if not sent:
                raise IOError("<send_bytes> connection broken")
            sent_total += sent
            buf = buf[sent:]

        return sent_total

    @staticmethod
    def recv_bytes(skt, length, timeout=0):
        """
        Receive bytes from the socket
        @raise ValueError, IOError, socket.timeout, Exception
        """
        if not skt or length < 0:
            raise ValueError("<recv_bytes> invalid socket descriptor or length")

        if timeout:
            skt.settimeout(timeout)
        else:
            skt.settimeout(None)

        buf = ''
        while len(buf) < length:
            data = skt.recv(length - len(buf))
            if not data:
                raise IOError("<recv_bytes> connection broken")
            buf += data

        return buf

    @staticmethod
    def send_bytes_ex(skt, buf, timeout=0):
        """
        Send bytes to the socket. Swallow exceptions.
        """
        try:
            return SocketHelper.send_bytes(skt, buf, timeout)
        except Exception as e:
            logging.error(e)

        return -1

    @staticmethod
    def recv_bytes_ex(skt, length, timeout=0):
        """
        Receive bytes to the socket. Swallow exceptions.
        """
        try:
            return SocketHelper.recv_bytes(skt, length, timeout)
        except Exception as e:
            logging.error(e)

        return None

def synchronized(lock_name):
    """synchronized access to class method"""
    def _outer(func):
        @wraps(func)
        def _inner(self, *args, **kwargs):
            try:
                lock = self.__getattribute__(lock_name)
            except AttributeError:
                logging.error("<synchronized> cannot find lock: %s", lock_name)
                return _inner
            with lock:
                return func(self, *args, **kwargs)
        return _inner
    return _outer