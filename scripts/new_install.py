#!/usr/bin/env python

import sys

sys.path = [".", "lib"] + sys.path
import threading
import Queue
import time
import random
import install_utils
import logging.config

logging.config.fileConfig("scripts.logging.conf")
log = logging.getLogger()
q = Queue.Queue()


def node_installer(node, install_tasks):
    while True:
        install_task = install_tasks.get()
        do_install_task(install_task, node)
        install_tasks.task_done()


def on_install_error(install_task, node, err):
    log.error("Install failed on %s during %s due to %s" % (node.ip, install_task, err))
    q.empty()
    node.cleanup(err)


def do_install_task(task, node):
    try:
        time.sleep(random.randint(5, 7))
        if "_uninstall" in task:
            node.uninstall_cb()
        elif "_install" in task:
            node.install_cb()
        elif "_init" in task:
            node.init_cb()
        log.info("Done with %s on %s." % (task, node.ip))
    except Exception as e:
        on_install_error(task, node, e.message)


def validate_install():
    return True


def do_install(params):
    # Per node, spawn one thread, which will process a queue of install tasks
    queues = []
    for server in params["servers"]:
        node_helper = install_utils.get_node_helper(server.ip)
        if "centos" in node_helper.get_os():
            install_tasks = ["rpm_uninstall", "rpm_install", "rpm_init"]
        if "ubuntu" in node_helper.get_os():
            install_tasks = ["deb_uninstall", "deb_install", "deb_init"]
        q = Queue.Queue()
        for _ in install_tasks:
            q.put(_)
        t = threading.Thread(target=node_installer, args=(node_helper, q))
        t.daemon = True
        t.start()
        queues.append(q)

    for q in queues:
        q.join()

def main():
    params = install_utils.process_user_input()
    install_utils.pre_install_steps()
    do_install(params)
    success = validate_install()
    # Check for installation success
    if not success:
        log.error("Installation FAILED")
        sys.exit()

if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    log.info("Total install time = {0} seconds".format(round(end_time - start_time)))
