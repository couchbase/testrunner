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
from membase.api.rest_client import RestConnection
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


def version_check_worker(node_helper, version_check_params):
    """Worker function to check version and reset node state for a single node."""
    try:
        install_version = node_helper.cb_version or version_check_params["version"]
        cb_edition = version_check_params["cb_edition"]
        cluster_profile = version_check_params.get("cluster_profile", None)
        is_installed = install_utils.check_if_version_already_installed(
            node_helper.node, install_version, cb_edition, cluster_profile
        )
        if is_installed:
            log.info("Version already installed on {0}. Skipping download, uninstall, install tasks for this node.".format(node_helper.ip))
            # Reset node state to pre-init before running init task
            install_utils.reset_node_state_to_presetup(node_helper)
            # Remove tasks that are not needed when version is already installed on this node
            for task_to_remove in ["download_build", "uninstall", "install"]:
                if task_to_remove in node_helper.install_tasks:
                    node_helper.install_tasks.remove(task_to_remove)
    except Exception as e:
        log.error("Error during version check on {0}: {1}".format(node_helper.ip, e))


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

def validate_columnar_install(params):
    cluster_nodes = []
    for node in install_utils.NodeHelpers:
        if not (params["columnar"] or node.profile == "columnar"):
            # Continue if this node is not columnar profile
            continue

        version = node.cb_version or params["version"]
        if node.install_success is None:
            node.install_success = False
            node.rest = RestConnection(node.node, check_connectivity=False)
            try:
                pools_status = node.rest.get_pools_info()
                node_status = node.rest.get_nodes_status_from_self()
            except:
                continue

            if pools_status is None and node_status is None:
                continue

            node.install_success = True
            if not pools_status["isEnterprise"]:
                node.install_success = False
            if pools_status["configProfile"] != "columnar":
                node.install_success = False
            # Cleanup next few lines post resolution of MB-60871
            if "7.6.100" not in pools_status["implementationVersion"] and version not in pools_status["implementationVersion"]:
                node.install_success = False
            # Cleanup next few lines post resolution of MB-60871
            if "7.6.100" not in node_status["version"] and version not in node_status["version"]:
                node.install_success = False
            if node_status["status"] != "healthy":
                node.install_success = False

            cluster_nodes.append({
                "ipaddr": node.ip,
                "status": node_status["status"],
                "version": node_status["version"],
                "implementedVersion": pools_status["implementationVersion"],
                "profile": pools_status["configProfile"],
                "isEnterprise": pools_status["isEnterprise"]
            })

    for [i, node] in enumerate(cluster_nodes):
        log.info("cluster:C{0}\tnode:{1}\tversion:{2}\tprofile:{3}\tstatus:{4}".format(i + 1,
                                                                                     node['ipaddr'],
                                                                                     node['version'],
                                                                                     node['profile'],
                                                                                     node['status']))

def validate_install(params):
    cluster_nodes = {}
    for node in install_utils.NodeHelpers:
        if node.profile == "columnar" or params["columnar"]:
            # Continue if this node is columnar profile
            continue
        version = node.cb_version or params["version"]
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


def do_install(params, install_tasks):
    # Per node, spawn one thread, which will process a queue of install tasks
    for server in params["servers"]:
        node_helper = install_utils.get_node_helper(server.ip)
        q = queue.Queue()
        for _ in install_tasks[server]:
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

    # Check if ANY node needs init
    any_node_needs_init = any("init" in node.install_tasks for node in install_utils.NodeHelpers)
    if any_node_needs_init:
        if params.get("init_clusters", False) and len(params["clusters"]) > 0:
            timeout = force_stop - time.time()
            install_utils.init_clusters(timeout)

    # Common validate for both regular and columnar install (useful in mixed profiles)
    log.info("-" * 100)
    # Check if ANY node needs install or init
    any_node_needs_install_or_init = any(
        "install" in node.install_tasks or "init" in node.install_tasks
        for node in install_utils.NodeHelpers
    )
    if any_node_needs_install_or_init:
        validate_install(params)
        validate_columnar_install(params)
    install_utils.print_result_and_exit()


def do_uninstall(params, install_tasks):
    # Per node, spawn one thread, which will process a queue of
    # uninstall tasks (same pattern as do_install for consistency)
    for server, tasks in install_tasks.items():
        node_helper = install_utils.get_node_helper(server.ip)
        q = queue.Queue()
        for task_to_add in tasks:
            if task_to_add == 'uninstall':
                q.put(task_to_add)
        t = threading.Thread(target=node_installer, args=(node_helper, q))
        t.daemon = True
        t.start()
        node_helper.queue = q
        node_helper.thread = t
    force_stop = start_time + params["timeout"]
    for node_helper in install_utils.NodeHelpers:
        try:
            while node_helper.queue.unfinished_tasks and time.time() < \
                    force_stop:
                time.sleep(install_constants.INSTALL_POLL_INTERVAL)
            else:
                raise InstallException
        except InstallException:
            if time.time() >= force_stop:
                log.error("Uninstall TIMED OUT AFTER {0}s. VALIDATING.."
                          .format(params["timeout"]))
                break


def main():
    install_tasks = dict()
    node_helpers_to_install = list()

    params = install_utils.process_user_input()

    for server in params["servers"]:
        install_tasks[server] = params['install_tasks']
        node_helpers_to_install.append(
            install_utils.get_node_helper(server.ip))

    # Check if target version is already installed on each node individually
    # This allows per-node installation customization (some nodes may already have the version)
    # Only run this check if force_reinstall is False (default: True/full reinstall)
    if not params["force_reinstall"]:
        log.info("Checking if version is already installed on each node to optimize installation...")
        # Spawn threads for parallel version checking and node state reset
        version_check_threads = []
        for node_helper in node_helpers_to_install:
            t = threading.Thread(target=version_check_worker, args=(node_helper, params))
            t.daemon = True
            t.start()
            version_check_threads.append(t)
        # Wait for all version check threads to complete
        for t in version_check_threads:
            t.join()
    else:
        log.info("Skipping version check (force_reinstall=True). All nodes will follow the full installation process.")

    install_utils.pre_install_steps(node_helpers_to_install)

    # Build install_tasks dict from individual node_helper install_tasks
    for node_helper in node_helpers_to_install:
        # Make a copy to avoid modifying node_helper.install_tasks
        install_tasks[node_helper.node] = list(node_helper.install_tasks)

    # Check if ANY node needs uninstall
    any_node_needs_uninstall = any("uninstall" in node.install_tasks for node in node_helpers_to_install)
    if any_node_needs_uninstall:
        # Do uninstallation of products first before downloading the builds.
        do_uninstall(params, install_tasks)

    for server, install_task in install_tasks.items():
        if 'uninstall' in install_task:
            install_task.remove('uninstall')

    # Check if ANY node needs install or download_build
    any_node_needs_install_or_download = any(
        "install" in node.install_tasks or "download_build" in node.install_tasks
        for node in node_helpers_to_install
    )
    if any_node_needs_install_or_download:
        install_utils.download_build(node_helpers_to_install)

    # Check if ANY node needs tools
    any_node_needs_tools = any("tools" in node.install_tasks for node in node_helpers_to_install)
    if any_node_needs_tools:
        install_utils.install_tools(node_helpers_to_install)

    do_install(params, install_tasks)


if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    log.info("TOTAL INSTALL TIME = {0} seconds".format(round(end_time - start_time)))
    sys.exit(0)
