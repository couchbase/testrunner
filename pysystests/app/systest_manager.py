from __future__ import absolute_import
from app.celery import celery
import json
import time
import datetime
from celery.task.control import revoke
import testcfg as cfg
from cache import ObjCacher, CacheHelper
from rabbit_helper import PersistedMQ
from app.workload_manager import Workload, sysTestRunner
from app.query import QueryWorkload
from app.rest_client_tasks import perform_admin_tasks, perform_xdcr_tasks, create_ssh_conn, monitorRebalance, perform_bucket_create_tasks, perform_view_tasks
from celery.utils.log import get_task_logger
if cfg.SERIESLY_IP != '':
    from seriesly import Seriesly

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

            if 'rcq' in testMsg:
                rabbitHelper.putMsg(testMsg['rcq'], "Starting test: %s" % (testMsg))


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
    # convert key type to int to be sorted correctly
    keys = phases.keys()
    keys = [int(k) for k in keys]
    keys.sort()

    for phase_key in keys:

        # run phase
        phase = testMsg['phases'][str(phase_key)]
        add_phase_to_db(phase, phase_key, name, desc)
        phase_status = runPhase(name, phase)

        if phase_status == False:
            break

    if 'loop' in testMsg and testMsg['loop']:
        launchSystest(testMsg)

    logger.error('\n')
    logger.error('###### Test Complete!  ######')
    # TODO, some kind of pass/fail and/or stat info

def add_phase_to_db(phase, phase_key, name, desc):

    if cfg.SERIESLY_IP != '':
        seriesly = Seriesly(cfg.SERIESLY_IP, 3133)
        seriesly.event.append({str(phase_key): {str(phase['name']): str(time.time()), 'run_id': name+ '-' + desc}})

def runPhase(name, phase):

    ddocs = workload = workloadIds = cluster = query = queryIds = buckets = None
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
    if 'buckets' in phase:
        buckets = phase['buckets']
    if 'ddocs' in phase:
        ddocs = phase['ddocs']

    logger.error('\n')
    logger.error("Running Phase: %s (%s)" % (name, desc))

    if buckets is not None:
        perform_bucket_create_tasks(buckets)

    if ddocs is not None:
        perform_view_tasks(ddocs)

    if cluster is not None:

        clusterMsg = parseClusterReq(cluster)
        perform_admin_tasks(clusterMsg)
        rebalance_required = clusterMsg['rebalance_required']

    if workload is not None:
        workloadIds = activateWorkloads(workload)

    if query is not None:
        queryIds = activateQueries(query)

    # monitor phase
    phase_status = monitorPhase(runTime, workloadIds, rebalance_required, queryIds)

    # phase complete: #TODO stat report
    time.sleep(5)

    return phase_status

def activateWorkloads(workload):

    workloadIds = []

    if isinstance(workload, list):
        # multi bucket workload support
        for workloadDefn in workload:
            workloadId = _activateWorkloads(workloadDefn)
            workloadIds.append(workloadId)
            time.sleep(2)
    else:
        workloadId = _activateWorkloads(workload)
        workloadIds.append(workloadId)

    return workloadIds

def _activateWorkloads(workloadDefn):
    workloadRunnable = createWorkload(workloadDefn)
    sysTestRunner.delay(workloadRunnable)
    workloadId = workloadRunnable.id
    logger.error("Started workload %s" % workloadId)
    return workloadId

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

    params = QueryWorkload.defaultSpec()

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
        if key == 'include':
            val = [str(v) for v in val.split(' ')]
            params['include_filters'] = val
        if key == 'exclude':
            params['exclude_filters'] = str(val)
        if key == 'idx':
            params['indexed_key'] = str(val)
        if key == 'start':
            params['startkey'] = str(val)
        if key == 'end':
            params['endkey'] = str(val)
        if key == 'limit':
            params['limit'] = str(val)
        if key == 'startid':
            params['startkey_docid'] = str(val)
        if key == 'endid':
            params['endkey_docid'] = str(val)

    return params

def parseClusterReq(cluster):

    clusterMsg = {'failover': '',
        'hard_restart': '',
        'rebalance_out': '',
        'only_failover': False,
        'soft_restart': '',
        'rebalance_in': '',
        'add_back': '',
        'auto_failover': ''}


    rebalance_required = True

    if 'add' in cluster:
        clusterMsg['rebalance_in'] = cluster['add']

    if 'rm' in cluster:
        clusterMsg['rebalance_out'] = cluster['rm']

    if 'failover' in cluster:
        clusterMsg['failover'] = cluster['failover']

    if 'hard_restart' in cluster:
        clusterMsg['hard_restart'] = cluster['hard_restart']

    if 'only_failover' in cluster:
        if cluster['only_failover'] == "True":
            clusterMsg['only_failover'] = True

    if 'soft_restart' in cluster:
        clusterMsg['soft_restart'] = cluster['soft_restart']

    if 'auto_failover' in cluster:
        clusterMsg['auto_failover'] = cluster['auto_failover']

    if 'add_back' in cluster:
        clusterMsg['add_back'] = cluster['add_back']

    clusterMsg['rebalance_required'] = rebalance_required
    return clusterMsg

def monitorPhase(runTime, workloadIds, rebalancing = False, queryIds = None):

    # monitor rebalance
    # monitor pre/post conditions lala

    running = True
    phase_status = True
    end_time = time.time() + int(runTime)

    while running:

        if time.time() > end_time:

            if rebalancing:
                phase_status = monitorRebalance()
                rebalancing = False
            elif workloadIds is not None and len(workloadIds) > 0:
                for workloadId in workloadIds:
                    _status = getWorkloadStatus(workloadId)
                    if _status == False:
                        # remove stopped workload from list to stop polling
                        index = workloadIds.index(workloadId)
                        workloadIds.pop(index)
            else:
                running = False

        else:
            time.sleep(2)

    if queryIds != None:
        # stop queries
        for qid in queryIds:
            QueryWorkload.from_cache(qid).active = False

    return phase_status

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
        if key == 'b':
            workloadSpec['bucket'] = str(val)
        if key == 'pwd':
            workloadSpec['password'] = str(val)
        if key == 'g':
            workloadSpec['get_perc'] = int(val)
        if key == 'u':
            workloadSpec['update_perc'] = int(val)
        if key == 'd':
            workloadSpec['del_perc'] = int(val)
        if key == 'e':
            workloadSpec['exp_perc'] = int(val)
        if key == 'm':
            workloadSpec['miss_perc'] = int(val)
        if key == 'ttl':
            workloadSpec['ttl'] = int(val)
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
