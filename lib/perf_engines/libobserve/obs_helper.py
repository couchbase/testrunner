#!/usr/bin/env python

from crc32 import crc32_hash

class VbucketHelper:

    @staticmethod
    def get_vbucket_id(key, num_vbuckets):
        vbucketId = 0
        if num_vbuckets > 0:
            vbucketId = crc32_hash(key) & (num_vbuckets - 1)
        return vbucketId