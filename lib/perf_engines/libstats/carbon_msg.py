#!/usr/bin/env python

import time

class CarbonMessage:

    def __init__(self, key, value, timestamp=0):
        self.key = key
        self.value = value
        if timestamp:
            self.timestamp = time
        else:
            self.timestamp = time.time()

    def pack(self):
        return "%s %s %d\n" % (self.key, self.value, self.timestamp)

    def __repr__(self):
        return "<CarbonMessage> key: %s, value: %s, timestamp: %d" \
            % (self.key, self.value, self.timestamp)

    def __str__(self):
        return self.__repr__()