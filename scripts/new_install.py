#!/usr/bin/env python

import sys

sys.path = [".", "lib"] + sys.path
import threading
import queue
import time
import random
import install_utils, install_constants
import logging.config
import os
from membase.api.exception import InstallException
import traceback

logging.config.fileConfig("scripts.logging.conf")
log = logging.getLogger()
q = queue.Queue()


def node_installer(node, install_tasks):
    while True:
        install_task = install_tasks.get()
        if install_task is None:
            break
        else:
            do_install_task(install_task, node)
            install_tasks.task_done()


def on_install_error(install_task, node, e):
    node.queue.empty()
    log.error("Error {0}:{1} occurred on {2} during {3}".format(repr(e), e, node.ip, install_task))


def do_install_task(task, node):
    try:
        if task == "uninstall":
            node.uninstall_cb()
        elif task == "install":
            node.install_cb()
        elif task == "init":
            node.init_cb()
        elif task == "cleanup":
            node.cleanup_cb()
        log.info("Done with %s on %s." % (task, node.ip))
    except Exception as e:
        on_install_error(task, node, e)
        traceback.print_exc()


def validate_install(params):
    log.info("-" * 100)
    cluster_nodes = {}
    for node in install_utils.NodeHelpers:
        version = params["version"]
        if node.install_success is None:
            node.install_success = False
            if params["cluster_version"]:
                if node.ip != params["bkrs_client"].ip:
                    version = params["cluster_version"]
            if node.rest:
                try:
                    node_status = node.rest.cluster_status()["nodes"]
                except:
                    continue
                for item in node_status:
                    hostname = item["hostname"]
                    if "alternateAddresses" in item and "external" in item["alternateAddresses"]:
                        hostname = item["alternateAddresses"]["external"]["hostname"]
                    if node.ip not in hostname:
                        continue
                    if version in item['version'] and item['status'] == "healthy":
                        node.install_success = True

                    if node.enable_ipv6 and not item["addressFamily"] == "inet6":
                        node.install_success = False

                    afamily = "Unknown"
                    if 'addressFamily' in list(item.keys()):
                        afamily = item['addressFamily']

                    cluster_nodes[node.ip] = {
                        "hostname": item["hostname"],
                        "version": item["version"],
                        "afamily": afamily,
                        "services": item["services"]
                    }

                # check cluster has correct number of nodes
                if params.get("init_clusters", False):
                    selected_cluster = None
                    for cluster in params["clusters"].values():
                        for server in cluster:
                            if server.ip == node.ip:
                                selected_cluster = cluster
                    if selected_cluster is not None:
                        if len(node_status) != len(selected_cluster):
                            node.install_success = False

    clusters = []

    if params.get("init_clusters", False):
        for cluster in params["clusters"].values():
            nodes = { node.ip: cluster_nodes[node.ip] for node in cluster}
            for [ip, node] in nodes.items():
                del cluster_nodes[ip]
            clusters.append(list(nodes.values()))

    for node in cluster_nodes.values():
        clusters.append([node])

    for [i, cluster] in enumerate(clusters):
        for node in cluster:
            log.info("cluster:C{0}\tnode:{1}\tversion:{2}\taFamily:{3}\tservices:{4}".format(i + 1, node['hostname'],
                                                                                    node['version'],
                                                                                    node['afamily'],
                                                                                    node['services']))
    install_utils.print_result_and_exit()

def do_install(params):
    # Per node, spawn one thread, which will process a queue of install tasks
    for server in params["servers"]:
        node_helper = install_utils.get_node_helper(server.ip)
        install_tasks = params["install_tasks"]
        q = queue.Queue()
        for _ in install_tasks:
            q.put(_)
        t = threading.Thread(target=node_installer, args=(node_helper, q))
        t.daemon = True
        t.start()
        node_helper.queue = q
        node_helper.thread = t

    force_stop = start_time + params["timeout"]
    for node in install_utils.NodeHelpers:
        try:
            while node.queue.unfinished_tasks and time.time() < force_stop:
                time.sleep(install_constants.INSTALL_POLL_INTERVAL)
            else:
                raise InstallException
        except InstallException:
            if time.time() >= force_stop:
                log.error("INSTALL TIMED OUT AFTER {0}s.VALIDATING..".format(params["timeout"]))
                break
    if "init" in params["install_tasks"]:
        if params.get("init_clusters", False) and len(params["clusters"]) > 0:
            timeout = force_stop - time.time()
            install_utils.init_clusters(timeout)
        validate_install(params)


def do_uninstall(params):
    # Per node, spawn one thread, which will process a queue of
    # uninstall tasks
    for server in params["servers"]:
        node_helper = install_utils.get_node_helper(server.ip)
        install_tasks = ['uninstall']
        q = queue.Queue()
        for _ in install_tasks:
            q.put(_)
        t = threading.Thread(target=node_installer,
                             args=(node_helper, q))
        t.daemon = True
        t.start()
        node_helper.queue = q
        node_helper.thread = t
    force_stop = start_time + params["timeout"]
    for node in install_utils.NodeHelpers:
        try:
            while node.queue.unfinished_tasks and time.time() < \
                    force_stop:
                time.sleep(install_constants.INSTALL_POLL_INTERVAL)
            else:
                raise InstallException
        except InstallException:
            if time.time() >= force_stop:
                log.error(
                    "Uninstall TIMED OUT AFTER {0}s. "
                    "VALIDATING..".format(
                        params["timeout"]))
                break

def main():
    params = install_utils.process_user_input()
    install_utils.pre_install_steps()
    if 'uninstall' in params['install_tasks']:
        # Do uninstallation of products first before downloading the
        # builds.
        do_uninstall(params)
        params['install_tasks'].remove('uninstall')

    if 'install' in params['install_tasks']:
        install_utils.download_build()
    if 'tools' in params['install_tasks']:
        install_utils.install_tools()
    do_install(params)




if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    log.info("TOTAL INSTALL TIME = {0} seconds".format(round(end_time - start_time)))
    sys.exit(0)
