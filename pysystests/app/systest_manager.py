from __future__ import absolute_import
from app.celery import celery
import json
import time
import datetime
from celery.task.control import revoke
import testcfg as cfg
from rabbit_helper import PersistedMQ
from app.workload_manager import Workload, run

from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)



"""Monitors the systest_manager queue for new test case messages sent from clients.
"""
@celery.task(base = PersistedMQ)
def systestManager(testQueue = "systest_manager_default"):

    rabbitHelper = systestManager.rabbitHelper

    try:
        testQueueSize = rabbitHelper.qsize(testQueue)

        if testQueueSize > 0:
            testMsg = rabbitHelper.getJsonMsg(testQueue)
            try:
                launchSystest(testMsg)
            except KeyError:
                logger.info("Ignoring malformated msg: %s" % testMsg)

    except ValueError as ex:
        logger.error("Error parsing test msg %s: " % testMsg)
        logger.error(ex)
    except Exception as ex:
        logger.error(ex)


def launchSystest(testMsg):

    name = "<test name>"
    desc = "<test description>"

    if "name" in testMsg:
        name = testMsg["name"]
    if "desc" in testMsg:
        desc = testMsg["desc"]

    logger.error('\n')
    logger.error('###################################')
    logger.error('Starting Test: %s (%s)' % (name, desc))
    logger.error('###################################')

    # retrieve phase keys and make sure they are ordered
    phases = testMsg['phases']
    keys = phases.keys()
    keys.sort()

    for phase_key in keys:

        # run phase
        phase = testMsg['phases'][phase_key]

        runPhase(name, phase)

    if 'loop' in testMsg and testMsg['loop']:
        launchSystest(testMsg)

    logger.error('\n')
    logger.error('###### Test Complete  ######')
    # TODO, some kind of pass/fail and/or stat info

def runPhase(name, phase):

    workload = admin = query = None
    docTemplate = "default"
    runTime = 10
    name = "<phase name>"
    desc = "<phase description>"

    if 'name' in phase:
        name = phase['name']
    if 'desc' in phase:
        desc = phase['desc']
    if 'admin' in phase:
        admin = phase['admin']
    if 'workload' in phase:
        workload = phase['workload']
    if 'query' in phase:
        query = phase['query']
    if 'runtime' in phase:
        runTime = int(phase['runtime'])

    logger.error('\n')
    logger.error("Running Phase: %s (%s)" % (name, desc))

    if admin is not None:
        # send admin msg
        pass

    if workload is not None:
        workloadRunnable = createWorkload(workload)
        logger.error("Starting workload %s" % workloadRunnable.id)
        run.delay(workloadRunnable)

    if query is not None:
        # send query
        pass

    # monitor phase
    monitorPhase(runTime, workloadRunnable.id)

    # phase complete: #TODO stat report
    time.sleep(5)

def monitorPhase(runTime, workloadId):

    # monitor rebalance
    # monitor pre/post conditions lala

    end_time = time.time() + int(runTime)

    while True:

        if time.time() > end_time:
           # kill any running tasks
            if workloadId is not None:
                logger.error("Stopping workload %s" % workloadId)
                workload = Workload.from_cache(workloadId)
                workload.active = False
                break
        else:
            time.sleep(2)


def createWorkload(workload_str):

    params = workload_str.split(",")
    workloadSpec = Workload.defaultSpec()

    for op in params:
        key,val = op.split(':')
        if key == 's':
            workloadSpec['create_perc'] = int(val)
        if key == 'g':
            workloadSpec['get_perc'] = int(val)
        if key == 'u':
            workloadSpec['update_perc'] = int(val)
        if key == 'd':
            workloadSpec['del_perc'] = int(val)
        if key == 'cc':
            workloadSpec['cc_queues'] = [val]
        if key == 't':
            workloadSpec['template'] = str(val)
        if key == 'ops':
            workloadSpec['ops_per_sec'] = int(val)

    workload = Workload(workloadSpec)
    return workload

