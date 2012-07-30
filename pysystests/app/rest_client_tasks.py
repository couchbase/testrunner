##!/usr/bin/env python
"""

rest tasks 

"""

from __future__ import absolute_import
from app.celery import celery

import json
import eventlet

###
SDK_IP = '127.0.0.1'
SDK_PORT = 50008
###


@celery.task
def query_view(design_doc_name, view_name, bucket = "default", params = None):

    if params == None:
        params = {"stale" : "update_after"}

    message = {"command" : "query",
               "args" : [design_doc_name, view_name, bucket, params]}

    return  _send_msg(message)

def _send_msg(message):
    sdk_client = eventlet.connect((SDK_IP, SDK_PORT))
    sdk_client.sendall(json.dumps(message))

