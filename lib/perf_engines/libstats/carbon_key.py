#!/usr/bin/env python

class CarbonKey:
    """
    Optionally used by {@class: CarbonMessage}
    """
    def __init__(self, src, server, metric,
                 cluster="", bucket="", vbucket=""):
        self.src = src
        self.server = server
        self.metric = metric
        self.cluster = cluster
        self.bucket = bucket
        self.vbucket = str(vbucket)

    def pack(self):
        buf = self.src + "."
        buf += self.server.replace('.', '-') + "."
        if self.cluster:
            buf += "cluster-" + self.cluster + "."
        if self.bucket:
            buf += "bucket-" + self.bucket + "."
        if self.vbucket:
            buf += "vbucket-" + self.vbucket + "."
        buf += self.metric
        return buf

    def __repr__(self):
        return self.pack()

    def __str__(self):
        return self.pack()
