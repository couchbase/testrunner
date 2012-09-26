##!/usr/bin/env python
"""

rest tasks 

"""
from __future__ import absolute_import
import sys
sys.path=["../lib"] + sys.path
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from app.celery import celery
import testcfg as cfg
import collections
import json
import eventlet
from rabbit_helper import PersistedMQ
from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)

###
SDK_IP = '127.0.0.1'
SDK_PORT = 50008
###

@celery.task(base = PersistedMQ)
def conn():
    pass


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

"""
"""

def perform_admin_tasks(adminMsg):
    rest = create_rest()

    # Add nodes
    servers = adminMsg["rebalance_in"]
    if servers:
        for server in servers.split():
            logger.error("Adding node %s" % server)
            ip,port = parse_server_arg(server)
            rest.add_node(cfg.COUCHBASE_USER, cfg.COUCHBASE_PWD, ip, port)

    allNodes = []
    for node in rest.node_statuses():
        allNodes.append(node.id)

    # Remove nodes
    servers = adminMsg["rebalance_out"]
    toBeEjectedNodes = []
    if servers :
        for server in servers.split():
            for node in rest.node_statuses():
                if "%s" % node.ip == "%s" % server:
                    logger.error("Removing node %s" % node.id)
                    toBeEjectedNodes.append(node.id)

    # Failover Node
    failover_servers = adminMsg["failover"]
    only_failover = adminMsg["only_failover"]
    if failover_servers:
        for server in failover_servers.split():
            for node in rest.node_statuses():
                if "%s" % node.ip == "%s" % server:
                    logger.error("Failing node %s" % node.id)
                    rest.fail_over(node.id)
                    if not only_failover:
                        toBeEjectedNodes.append(node.id)

    # SoftRestart a node
    servers = adminMsg["soft_restart"]
    if servers:
        logger.error('Soft Restart')
        restart(servers)
    # HardRestart a node
    servers = adminMsg["hard_restart"]
    if servers:
        logger.error('Hard Restart')
        restart(servers, type='hard')

    if len(allNodes) > 0 or len(toBeEjectedNodes) > 0:
        logger.error("Rebalance")
        rest.rebalance(otpNodes=allNodes, ejectedNodes=toBeEjectedNodes)

def parse_server_arg(server):
    ip = server
    port = 8091
    addr = server.split(":")
    if len(addr) > 1:
        ip = addr[0]
        port = addr[1]
    return ip, port

def _dict_to_obj(dict_):
    return type('OBJ', (object,), dict_)

def restart(servers='', type='soft'):

    for server in servers.split():
        node_ssh, node = create_ssh_conn(server)
        if type is not 'soft':
            cmd = "reboot"
        else:
            cmd = "/etc/init.d/couchbase-server restart"

        logger.error(cmd)
        result = node_ssh.execute_command(cmd, node)
        logger.error(result)

def create_rest(server_ip=cfg.COUCHBASE_IP, port=cfg.COUCHBASE_PORT,
                username=cfg.COUCHBASE_USER, password=cfg.COUCHBASE_PWD):
    serverInfo = { "ip" : server_ip,
                   "port" : port,
                   "rest_username" : username,
                   "rest_password" :  password
                }
    print serverInfo
    node = _dict_to_obj(serverInfo)
    return RestConnection(node)

def create_ssh_conn(server_ip = '', port=22, username = cfg.SSH_USER,
               password = cfg.SSH_PASSWORD, os='linux'):
    if isinstance(server_ip, unicode):
        server_ip = str(server_ip)

    serverInfo = {"ip" : server_ip,
                  "port" : port,
                  "ssh_username" : username,
                  "ssh_password" : password,
                  "ssh_key": '',
                  "type": os
                }

    node = _dict_to_obj(serverInfo)
    shell = RemoteMachineShellConnection(node)
    return shell, node
