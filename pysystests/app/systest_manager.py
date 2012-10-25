from __future__ import absolute_import
from app.celery import celery
import json
import time
import datetime
from celery.task.control import revoke
import testcfg as cfg
from rabbit_helper import PersistedMQ
from app.workload_manager import Workload, sysTestRunner
from app.query import QueryWorkload
from app.rest_client_tasks import perform_admin_tasks, perform_xdcr_tasks, create_ssh_conn, monitorRebalance

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
            suffix = testMsg['suffix']

            try:
                if "localtestname" in testMsg:
                    # read test from local worker filesystem
                    testMsg = loadTestFromFile(testMsg["localtestname"], suffix)

                    if "runlist" in testMsg:

                        # handle runlist
                        for test in testMsg['runlist']:
                            testMsg = loadTestFromFile(test, suffix)
                            testMsg['loop'] = False
                            launchSystest(testMsg)

                    elif testMsg is not None:

                        # run local standalone
                        launchSystest(testMsg)
                else:
                    # run remote standalone
                    launchSystest(testMsg)

            except KeyError:
                logger.info("Ignoring malformated msg: %s" % testMsg)

    except ValueError as ex:
        logger.error("Error parsing test msg %s: " % testMsg)
        logger.error(ex)
    except Exception as ex:
        logger.error(ex)

def loadTestFromFile(name, suffix="js"):
    testMsg = None

    try:
        fname = "tests/%s.%s" % (name,suffix)
        json_data = open(fname)
        testMsg = json.load(json_data)
    except Exception as ex:
        logger.error("Error loading test %s: %s" % (fname, ex))

    return testMsg

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
    logger.error('###### Test Complete!  ######')
    # TODO, some kind of pass/fail and/or stat info

def runPhase(name, phase):

    workload = workloadId = cluster = query = queryIds = None
    docTemplate = "default"
    rebalance_required = False

    name = "<phase name>"
    desc = "<phase description>"

    # default time a workload is run without any conditions in seconds
    runTime = 10

    if 'name' in phase:
        name = phase['name']
    if 'desc' in phase:
        desc = phase['desc']
    if 'cluster' in phase:
        cluster = phase['cluster']
    if 'workload' in phase:
        workload = phase['workload']
    if 'query' in phase:
        query = phase['query']
    if 'runtime' in phase:
        runTime = int(phase['runtime'])

    logger.error('\n')
    logger.error("Running Phase: %s (%s)" % (name, desc))

    if cluster is not None:

        clusterMsg = parseClusterReq(cluster)
        perform_admin_tasks(clusterMsg)
        rebalance_required = clusterMsg['rebalance_required']

    if workload is not None:
        workloadRunnable = createWorkload(workload)
        workloadId = workloadRunnable.id
        logger.error("Starting workload %s" % workloadId)
        sysTestRunner.delay(workloadRunnable)

    if query is not None:
        queryIds = activateQueries(query)

    # monitor phase
    monitorPhase(runTime, workloadId, rebalance_required, queryIds)

    # phase complete: #TODO stat report
    time.sleep(5)

def activateQueries(query):

    queryIds = []

    if isinstance(query, list):
        # multi-query support
        for paramStr in query:
            params = parseQueryStr(paramStr)
            qid =_activateQueries(params)
            queryIds.append(qid)
    else:
        params = parseQueryStr(query)
        qid = _activateQueries(params)
        queryIds.append(qid)

    return queryIds

def _activateQueries(params):
    queryRunnable = QueryWorkload(params)
    logger.error("Starting queries: %s" % params)
    queryRunnable.active = True
    return queryRunnable.id

def parseQueryStr(query):

    params = {"bucket" : "default"}

    for op in query.split(','):
        key, val = op.split(':')
        if key == "qps":
            params['queries_per_sec'] = int(val)
        if key == 'ddoc':
            params['ddoc'] = str(val)
        if key == 'view':
            params['view'] = str(val)
        if key == 'bucket':
            params['bucket'] = str(val)
        if key == 'password':
            params['password'] = str(val)

    return params

def parseClusterReq(cluster):

    clusterMsg = {'failover': '',
                  'hard_restart': '',
                  'rebalance_out': '',
                  'only_failover': False,
                  'soft_restart': '',
                  'rebalance_in': ''}


    rebalance_required = True

    if 'add' in cluster:
        clusterMsg['rebalance_in'] = cluster['add']

    if 'rm' in cluster:
        clusterMsg['rebalance_out'] = cluster['rm']

    clusterMsg['rebalance_required'] = rebalance_required
    return clusterMsg

def monitorPhase(runTime, workloadId, rebalancing = False, queryIds = None):

    # monitor rebalance
    # monitor pre/post conditions lala

    running = True
    end_time = time.time() + int(runTime)

    while running:

        if time.time() > end_time:

            if rebalancing:
                monitorRebalance()
                rebalancing = False
            elif workloadId is not None:
                running = getWorkloadStatus(workloadId)
            else:
                running = False

        else:
            time.sleep(2)

    if queryIds != None:
        # stop queries
        for qid in queryIds:
            QueryWorkload.from_cache(qid).active = False

def getWorkloadStatus(workloadId):

        running = True

        workload = Workload.from_cache(workloadId)

        # stop running tasks that do not have conditions
        if workload is not None:
            if workload.postconditions is not None:

                # see if workload condition handler has stopped load
                if workload.active == False:
                    logger.error("Postconditions met %s" % workload.postconditions)
                    logger.error("Stopping workload %s" % workloadId)
                    running = False
            else:
                logger.error("Stopping workload %s" % workloadId)
                workload.active = False
                running = False
        else:
            logger.error("Unable to fetch workload...cache down?")
            running = False

        return running

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
