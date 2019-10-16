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

logging.config.fileConfig("scripts.logging.conf")
log = logging.getLogger()
q = Queue.Queue()


def node_installer(node, install_tasks):
    while True:
        if not node.halt_thread:
            install_task = install_tasks.get()
            do_install_task(install_task, node)
            install_tasks.task_done()
        else:
            break

def on_install_error(install_task, node, err):
    node.queue.empty()
    node.halt_thread = True
    install_utils.print_error_and_exit("Error occurred on {0} during {1}".format(node.ip, install_task))


def do_install_task(task, node):
    #try:
    time.sleep(random.randint(5, 7))
    if task == "uninstall":
        node.uninstall_cb()
    elif task == "install":
        node.install_cb()
    elif task == "init":
        node.init_cb()
    elif task == "cleanup":
        node.cleanup_cb()
    log.debug("Done with %s on %s." % (task, node.ip))
    #except Exception as e:
    #    on_install_error(task, node, e.message)


def validate_install():
    for node in install_utils.NodeHelpers:
        log.info("-"*100)
        if node.state == "end-init":
            log.info("INSTALL COMPLETED ON {0}".format(node.ip))
        else:
            log.error("INSTALL FAILED ON {0} at {1} stage".format(node.ip, node.state))
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

    for node in install_utils.NodeHelpers:
        node.queue.join()

def main():
    params = install_utils.process_user_input()
    install_utils.pre_install_steps()
    do_install(params)
    validate_install()

if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    log.info("Total install time = {0} seconds".format(round(end_time - start_time)))
    sys.exit()

