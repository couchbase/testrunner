#!/usr/bin/env python

import sys

sys.path = [".", "lib"] + sys.path
import threading
import Queue
import time
import random
import install_utils, install_constants
import logging.config
import os
from membase.api.exception import InstallException

logging.config.fileConfig("scripts.logging.conf")
log = logging.getLogger()
q = Queue.Queue()


def node_installer(node, install_tasks):
    #force_stop = time.time() + install_constants.INSTALL_TIMEOUT
    while True:
        #if time.time() < force_stop: # and not node.halt_thread:
        install_task = install_tasks.get()
        do_install_task(install_task, node)
        install_tasks.task_done()
        #else:
        #    break


def on_install_error(install_task, node, err):
    node.queue.empty()
    node.halt_thread = True
    install_utils.print_error_and_exit("Error occurred on {0} during {1}".format(node.ip, install_task))


def do_install_task(task, node):
    # try:
    if task == "uninstall":
        node.uninstall_cb()
    elif task == "install":
        node.install_cb()
    elif task == "init":
        node.init_cb()
    elif task == "cleanup":
        node.cleanup_cb()
    log.debug("Done with %s on %s." % (task, node.ip))
    # except Exception as e:
    #    on_install_error(task, node, e.message)


def validate_install(version):
    for node in install_utils.NodeHelpers:
        node.install_success = False
        if node.rest:
            node_status = node.rest.cluster_status()["nodes"]
            for item in node_status:
                if version in item['version'] and item['status'] == "healthy":
                    node.install_success = True
                    log.debug("node:{0}\tversion:{1}\tstatus:{2}\tservices:{3}".format(item['hostname'],
                                                                                     item['version'],
                                                                                     item['status'],
                                                                                     item['services']))
    print_result()


def print_result():
    success = []
    fail = []
    for node in install_utils.NodeHelpers:
        if node.install_success:
            success.append(node.ip)
        elif not node.install_success:
            fail.append(node.ip)
    log.info("-" * 100)
    for _ in fail:
        log.error("INSTALL FAILED ON: \t{0}".format(_))
    log.info("-" * 100)

    for _ in success:
        log.info("INSTALL COMPLETED ON: \t{0}".format(_))
    log.info("-" * 100)


def do_install(params):
    # Per node, spawn one thread, which will process a queue of install tasks
    for server in params["servers"]:
        node_helper = install_utils.get_node_helper(server.ip)
        install_tasks = ["uninstall", "install", "init", "cleanup"]
        q = Queue.Queue()
        for _ in install_tasks:
            q.put(_)
        t = threading.Thread(target=node_installer, args=(node_helper, q))
        t.daemon = True
        t.start()
        node_helper.queue = q
        node_helper.thread = t

    for node in install_utils.NodeHelpers:
        force_stop = time.time() + install_constants.INSTALL_TIMEOUT
        try:
            while time.time() < force_stop:  # and not node.halt_thread:
                time.sleep(install_constants.INSTALL_POLL_INTERVAL)
            else:
                raise InstallException
        except InstallException as e:
            log.error("INSTALL TIMED OUT AFTER {0}s.VALIDATING..".format(install_constants.INSTALL_TIMEOUT))
            validate_install(params["version"])
            sys.exit(e)
        node.thread.join()
        node.queue.join()
        validate_install(params["version"])


def main():
    params = install_utils.process_user_input()
    install_utils.pre_install_steps()
    do_install(params)


if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    log.info("TOTAL INSTALL TIME = {0} seconds".format(round(end_time - start_time)))
    sys.exit()
