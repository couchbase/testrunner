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
import json
import eventlet
from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)

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

@celery.task
def perform_admin_tasks(adminMsg):
    rest = create_rest()

    # Add nodes
    servers = adminMsg["rebalance_in"]
    add_nodes(rest, servers)

    # Get all nodes
    allNodes = []
    for node in rest.node_statuses():
        allNodes.append(node.id)

    # Remove nodes
    servers = adminMsg["rebalance_out"]
    toBeEjectedNodes  = remove_nodes(rest, servers)

    # Failover Node
    servers = adminMsg["failover"]
    only_failover = adminMsg["only_failover"]
    toBeEjectedNodes.extend(failover_nodes(rest, servers, only_failover))

    # SoftRestart a node
    servers = adminMsg["soft_restart"]
    restart(servers)

    # HardRestart a node
    servers = adminMsg["hard_restart"]
    restart(servers, type='hard')

    if not only_failover and (len(allNodes) > 0 or len(toBeEjectedNodes) > 0):
        logger.error("Rebalance")
        logger.error(allNodes)
        logger.error(toBeEjectedNodes)
        rest.rebalance(otpNodes=allNodes, ejectedNodes=toBeEjectedNodes)

@celery.task
def perform_xdcr_tasks(xdcrMsg):
    logger.error(xdcrMsg)
    src_master = create_server_obj()
    dest_master = create_server_obj(server_ip=xdcrMsg['dest_cluster_ip'], username=xdcrMsg['dest_cluster_rest_username'],
                                    password=xdcrMsg['dest_cluster_rest_pwd'])
    dest_cluster_name = xdcrMsg['dest_cluster_name']
    xdcr_link_cluster(src_master, dest_master, dest_cluster_name)
    xdcr_start_replication(src_master, dest_cluster_name)

    if xdcrMsg['replication_type'] == "bidirection":
        src_cluster_name = dest_cluster_name + "_temp"
        xdcr_link_cluster(dest_master, src_master, src_cluster_name)
        xdcr_start_replication(dest_master, src_cluster_name)

def xdcr_link_cluster(src_master, dest_master, dest_cluster_name):
    rest_conn_src = RestConnection(src_master)
    rest_conn_src.add_remote_cluster(dest_master.ip, dest_master.port,
                                 dest_master.rest_username,
                                 dest_master.rest_password, dest_cluster_name)

def xdcr_start_replication(src_master, dest_cluster_name):
        rest_conn_src = RestConnection(src_master)
        for bucket in rest_conn_src.get_buckets():
            (rep_database, rep_id) = rest_conn_src.start_replication("continuous",
                                                                     bucket.name, dest_cluster_name)
            logger.error("rep_database: %s rep_id: %s" % (rep_database, rep_id))

def add_nodes(rest, servers=''):
    for server in servers.split():
        logger.error("Adding node %s" % server)
        ip, port = parse_server_arg(server)
        rest.add_node(cfg.COUCHBASE_USER, cfg.COUCHBASE_PWD, ip, port)

def remove_nodes(rest, servers=''):
    toBeEjectedNodes = []
    for server in servers.split():
        for node in rest.node_statuses():
            if "%s" % node.ip == "%s" % server:
                logger.error("Removing node %s" % node.id)
                toBeEjectedNodes.append(node.id)

    return toBeEjectedNodes

def failover_nodes(rest, servers='', only_failover=False):
    toBeEjectedNodes = []
    for server in servers.split():
        for node in rest.node_statuses():
            if "%s" % node.ip == "%s" % server:
                logger.error("Failing node %s" % node.id)
                rest.fail_over(node.id)
                if not only_failover:
                    toBeEjectedNodes.append(node.id)
    return toBeEjectedNodes


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
            logger.error('Hard Restart')
            cmd = "reboot"
        else:
            logger.error('Soft Restart')
            cmd = "/etc/init.d/couchbase-server restart"

        logger.error(cmd)
        result = node_ssh.execute_command(cmd, node)
        logger.error(result)

def create_server_obj(server_ip=cfg.COUCHBASE_IP, port=cfg.COUCHBASE_PORT,
                      username=cfg.COUCHBASE_USER, password=cfg.COUCHBASE_PWD):
    serverInfo = { "ip" : server_ip,
                   "port" : port,
                   "rest_username" : username,
                   "rest_password" :  password
    }
    node = _dict_to_obj(serverInfo)
    return node

def create_rest(server_ip=cfg.COUCHBASE_IP, port=cfg.COUCHBASE_PORT,
                username=cfg.COUCHBASE_USER, password=cfg.COUCHBASE_PWD):
    return RestConnection(create_server_obj(server_ip, port, username, password))

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
