from __future__ import absolute_import
from app.celery import celery
import json
import time
import datetime
from celery.task.control import revoke
import testcfg as cfg
from rabbit_helper import PersistedMQ
from app.workload_manager import Workload, sysTestRunner

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
    name = "<phase name>"
    desc = "<phase description>"

    # default time a workload is run without any conditions in seconds
    runTime = 10

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
        sysTestRunner.delay(workloadRunnable)

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

           # kill any running tasks that do not have conditions
            if workloadId is not None:
                workload = Workload.from_cache(workloadId)

                if workload is not None:
                    if workload.postconditions is not None:
                        # see if workload condition handler has stopped load
                        if workload.active == False:
                            logger.error("Postconditions met %s" % workload.postconditions)
                            logger.error("Stopping workload %s" % workloadId)
                            break
                    else:
                        logger.error("Runtime conditions met: %s's" % runTime)
                        logger.error("Stopping workload %s" % workloadId)
                        workload.active = False
                        break
                else:
                    logger.error("Unable to fetch workload...cache down?")
                    break

        else:
            time.sleep(2)


def createWorkload(workload):

    params = None
    workloadSpec = Workload.defaultSpec()

    if isinstance(workload, dict):
        params = workload['spec'].split(",")

        # parse ex args
        if 'bucket' in workload:
            workloadSpec['bucket'] = str(workload['bucket'])

        if 'template' in workload:
            workloadSpec['template'] = str(workload['template'])

        if 'conditions' in workload:
            for condition in workload['conditions'].split(','):
                stage, equality = condition.split(':')
                if stage == "pre":
                    workloadSpec['preconditions'] = equality
                if stage == "post":
                    workloadSpec['postconditions'] = equality

    else:
        # simple spec
        params = workload.split(",")
        logger.error(params)



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
        if key == 'ccq':
            workloadSpec['cc_queues'] = [str(val)]
        if key == 'coq':
            workloadSpec['consume_queue'] = str(val)
        if key == 't':
            workloadSpec['template'] = str(val)
        if key == 'ops':
            workloadSpec['ops_per_sec'] = int(val)

    workloadRunnable = Workload(workloadSpec)
    return workloadRunnable
