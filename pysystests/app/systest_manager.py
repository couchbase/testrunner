
from app.celery import celery
import json
import time
import copy
import datetime
from celery.task.control import inspect
import testcfg as cfg
from cache import ObjCacher, CacheHelper
from rabbit_helper import PersistedMQ, RabbitHelper, rawTaskPublisher
from app.workload_manager import Workload, sysTestRunner, getClusterStat, replace_magic_vars

from app.query import QueryWorkload
from app.rest_client_tasks import perform_admin_tasks, perform_xdcr_tasks, create_ssh_conn, monitorRebalance, perform_bucket_create_tasks, perform_view_tasks, perform_xdcr_tasks, perform_teardown_tasks, perform_cli_task
from celery.utils.log import get_task_logger
from cbsystest import getResponseQueue

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
                        testMsg['loop'] = ''
                        launchSystest(testMsg)

                elif testMsg is not None:

                    # run local standalone
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
        fname = "tests/%s.%s" % (name, suffix)
        json_data = open(fname)
        testMsg = json.load(json_data)
    except Exception as ex:
        logger.error("Error loading test %s: %s" % (fname, ex))

    return testMsg

def parsePhaseRangeInfo(range):
    range = range.replace("[", "")
    range = range.replace("]", "")
    str_len = len(range)
    loop_start = None
    loop_end = None

    if range != '':
        if range.find(":") == -1:
            loop_start = int(range)
            loop_end = int(range)
        elif range.find(":") == str_len - 1:
            loop_start = int(range.split(':')[0])
        elif range.find(":") == 0:
            loop_end = int(range.split(':')[1])
        else:
            loop_start = int(range.split(':')[0])
            loop_end = int(range.split(':')[1])

    return loop_start, loop_end


def launchSystest(testMsg, loop=False):

    name = "<test name>"
    desc = "<test description>"
    phases_to_loop = ''

    if "name" in testMsg:
        name = testMsg["name"]
    if "desc" in testMsg:
        desc = testMsg["desc"]
    if "loop" in testMsg:
        phases_to_loop = testMsg["loop"]

    logger.error('\n')
    logger.error('###################################')
    logger.error('Starting Test: %s (%s)' % (name, desc))
    logger.error('###################################')

    # retrieve phase keys and make sure they are ordered
    phases = testMsg['phases']
    # convert key type to int to be sorted correctly
    keys = list(phases.keys())
    keys = sorted([int(k) for k in keys])

    loop_start, loop_end = parsePhaseRangeInfo(phases_to_loop)
    if loop:
        if loop_start is None and loop_end is not None:
            keys = keys[:loop_end]
        elif loop_end is None and loop_start is not None:
            keys = keys[loop_start:]
        elif loop_start is not None and loop_end is not None:
            if loop_start == loop_end:
                keys = [keys[loop_start]]
            else:
                keys = keys[loop_start:loop_end]

    for phase_key in keys:

        phase = testMsg['phases'][str(phase_key)]
        add_phase_to_db(phase, phase_key, name, desc)

        # parse remote phases from local phase
        remote_phases, new_local_phase = parseRemotePhases(phase)

        remotePhaseIds = []
        if len(remote_phases) > 0:

            # run remote phases
            remotePhaseIds = runRemotePhases(remote_phases)

        # run local phase
        phase_status = runPhase(new_local_phase)
        if phase_status == False:
            break

        # monitor remote phases
        for remoteIP, taskID in remotePhaseIds:
            remote_running = get_remote_phase_status(remoteIP, taskID)
            while remote_running != False:
                time.sleep(2)
                remote_running = get_remote_phase_status(remoteIP, taskID)

        if 'cache' in phase:
            cacheVariable(phase['cache'])


    if phases_to_loop != '':
        launchSystest(testMsg, True)

    logger.error('\n')
    logger.error('###### Test Complete!  ######')
    # TODO, some kind of pass/fail and/or stat info


"""
" given a phase definition this method will extract
" tasks that should be ran on remote clusters
" and create additional remote phases
"""
def parseRemotePhases(phase):
    remotePhaseMap = {}
    newLocalPhase = copy.deepcopy(phase)

    for task in phase:


        if isinstance(phase[task], dict) and 'remote' in phase[task]:
            remoteRef = str(phase[task]['remote'])
            remoteIP = cfg.REMOTE_SITES[remoteRef]["RABBITMQ_IP"]

            # create remote if remote phase spec if necessary
            if  remoteIP not in remotePhaseMap:
                remotePhase = { 'name' : 'remote_' + phase.get('name') or 'remote_phase',
                                'desc' : 'remote_' + phase.get('desc') or 'remote_phase_description',
                                'runtime' : phase.get('runtime') or 10}
            else:
                remotePhase = remotePhaseMap[remoteIP]

            remotePhaseMap[remoteIP] = remotePhase

            # delete the 'remote' tag from this task
            del phase[task]['remote']

            # replace any magic variables from condition phases
            if 'conditions' in phase[task] and phase[task]['conditions'].find("$") >= 0:
                phase[task]['conditions'] = replace_magic_vars(phase[task]['conditions'])

            # add task to new remote phase
            remotePhaseMap[remoteIP].update({task : phase[task]})

            # remote this task from new local phase
            del newLocalPhase[task]

        # mutli-bucket workload/query support
        elif isinstance(phase[task], list):
            idx = 0
            updated_local_phase = False
            for phase_task_spec in phase[task]:

                if 'remote' in phase_task_spec:
                    remoteRef = str(phase_task_spec['remote'])
                    remoteIP = cfg.REMOTE_SITES[remoteRef]["RABBITMQ_IP"]

                    if  remoteIP not in remotePhaseMap:
                        remotePhase = { 'name' : 'remote_' + phase.get('name') or\
                                                 'remote_phase',
                                        'desc' : 'remote_' + phase.get('desc') or\
                                                 'remote_phase_description',
                                        'runtime' : phase.get('runtime') or 10,
                                        task : []}
                    else:
                        remotePhase = remotePhaseMap[remoteIP]
                        if task not in remotePhase:
                            remotePhase[task] = []

                    remotePhaseMap[remoteIP] = remotePhase

                    # delete the 'remote' tag from this task spec
                    del phase_task_spec['remote']

                    # add task to new remote phase
                    remotePhaseMap[remoteIP][task].append(phase_task_spec)

                    # replace any magic variables from condition phases
                    for workload in remotePhaseMap[remoteIP][task]:
                        if 'conditions' in workload and workload['conditions'].find("$") >= 0:

                            # number of workloads defined in thisremote  phase
                            num_workloads = len(remotePhaseMap[remoteIP][task])

                            # for each workload containing postcondition key using
                            # a magic variable replace it with actual value
                            for i in range(0, num_workloads):
                                remotePhaseMap[remoteIP][task][i]['conditions'] =\
                                    replace_magic_vars(workload['conditions'])

                    # remote this task from new local phase
                    del newLocalPhase[task][idx]
                    updated_local_phase = True

                # move to nex index
                if not updated_local_phase:
                    idx = idx + 1

    return remotePhaseMap, newLocalPhase


"""
" runRemotePhases: for each remote_phase this method will make a
"                  call to runPhase method on remote broker.
"                  Each remote phase returns it's taskid into a
"                  response queue for later monitoring
"""
def runRemotePhases(remote_phases, retry = 5):

    taskIds = []

    for remoteIP in remote_phases:

        # get handler to remote broker
        rabbitHelper = RabbitHelper(mq_server = remoteIP)
        rcq = getResponseQueue(rabbitHelper)
        args = (remote_phases[remoteIP], rcq)

        # call runPhase on remoteIP
        rawTaskPublisher("app.systest_manager.runPhase",
                         args,
                         "run_phase_"+cfg.CB_CLUSTER_TAG,
                         broker = remoteIP,
                         exchange = "default",
                         routing_key = cfg.CB_CLUSTER_TAG+".run.phase")

        # get taskID of phase running on remote broker
        taskId = None
        while taskId is None and retry > 0:
            time.sleep(2)
            taskId = rabbitHelper.getMsg(rcq)
            taskIds.append((remoteIP, taskId))
            retry = retry - 1

    return taskIds

def add_phase_to_db(phase, phase_key, name, desc):

    if cfg.SERIESLY_IP != '':
        seriesly = Seriesly(cfg.SERIESLY_IP, 3133)
        seriesly.event.append({str(phase_key): {str(phase['name']): str(time.time()), 'name': name, 'desc': desc}})

@celery.task(base = PersistedMQ, ignore_result=True)
def runPhase(phase, rcq = None):

    # if this task is run by a remote client return
    # task id for monitoring
    if rcq is not None:
        runPhase.rabbitHelper.putMsg(rcq, runPhase.request.id)

    ddocs = workload = workloadIds = cluster = query = queryIds =\
        buckets = xdcr = teardown = ssh = None
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
    if 'xdcr' in phase:
        xdcr = phase['xdcr']
    if 'teardown' in phase:
        teardown = phase['teardown']
    if 'ssh' in phase:
        ssh = phase['ssh']

    logger.error('\n')
    logger.error("Running Phase: %s (%s)" % (name, desc))

    if buckets is not None:
        perform_bucket_create_tasks(buckets)

    if ddocs is not None:
        perform_view_tasks(ddocs)

    if xdcr is not None:
        perform_xdcr_tasks(xdcr)

    if ssh is not None:
        perform_cli_task(ssh)

    if cluster is not None:
        if isinstance(cluster, list):
            cluster = cluster[0] # there can only be one cluster task-per-site

        clusterMsg = parseClusterReq(cluster)
	logger.error("{0}".format(clusterMsg))
        perform_admin_tasks(clusterMsg)
        rebalance_required = clusterMsg['rebalance_required']

    if workload is not None:
        workloadIds = activateWorkloads(workload)

    if query is not None:
        queryIds = activateQueries(query)

    if teardown is not None:
        perform_teardown_tasks(teardown)

    # monitor phase
    phase_status = monitorPhase(runTime, workloadIds, rebalance_required, queryIds)

    # phase complete: #TODO stat report
    time.sleep(5)

    return phase_status

"""
" cacheVariable: stores some variable or state into the object cache
"                for later use in other phases
"""
def cacheVariable(cacheMsg):
    bucket = cacheMsg.get("bucket") or "default"
    ref = str(cacheMsg.get("reference") or "default_key")
    stat = cacheMsg.get("stat") or "curr_items"
    value = getClusterStat(bucket, stat)
    CacheHelper.cachePhaseVar(ref, value)

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

    if isinstance(query, dict):
        query = query['spec']

    if isinstance(query, list):
        # multi-query support
        for paramStr in query:

            if isinstance(paramStr, dict):
                paramStr = paramStr['spec']

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
        'services' : None,
        'auto_failover': '',
        'involve_orchestrator': False}


    rebalance_required = True
    if 'group' in cluster:
        clusterMsg['group'] = cluster['group']
    else:
        clusterMsg['group'] = "Group 1"

    if 'add' in cluster:
        clusterMsg['rebalance_in'] = cluster['add']

    if 'services' in cluster:
        clusterMsg['services'] = cluster['services']

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

    if 'orchestrator' in cluster:
        if cluster['orchestrator'] == "True":
            clusterMsg['involve_orchestrator'] = True

    if clusterMsg['soft_restart'] != '' or clusterMsg['hard_restart'] != '' \
        or clusterMsg['only_failover'] == True:
        rebalance_required = False

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
            if isinstance(workload['conditions'], dict):
                if "post" in workload['conditions']:
                    workloadSpec['postconditions'] = workload['conditions']['post']
            else:
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
        key, val = op.split(':')
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

"""
" get_remote_phase_status: this method will call the get_phase_status task
"                          running on a worker in the remote cluster.
"""
def get_remote_phase_status(remoteIP, taskID, retry = 5):

    # assemble a request to remoteIP phase_status method
    rabbitHelper = RabbitHelper(mq_server = remoteIP)
    rcq = getResponseQueue(rabbitHelper)
    task_method = "app.systest_manager.get_phase_status"
    task_queue = "phase_status_"+cfg.CB_CLUSTER_TAG
    task_routing_queue = cfg.CB_CLUSTER_TAG+".phase.status"
    args = (taskID, rcq)

    # call phase_status task
    rawTaskPublisher(task_method,
                     args,
                     task_queue,
                     broker = remoteIP,
                     exchange="default",
                     routing_key = task_routing_queue)

    # retrieve status of phase
    rc = None
    while rc is None and retry > 0:
        rc = rabbitHelper.getMsg(rcq)
        time.sleep(2)
        retry = retry - 1

    rabbitHelper.delete(rcq)
    return rc == 'True'

"""
" get_phase_status: retreives the status of any phase currently running in
"                   the context of local worker(s).  Uses celery's inspect
"                   object to query the runtime status.
"""
@celery.task(base = PersistedMQ, ignore_result = True)
def get_phase_status(taskId, rcq = None):

    running = False

    try:
        # instantiate inspect object and get active tasks
        inspector = inspect()
        active_task_info = inspector.active()

        # search for runPhase method among active tasks
        for host in active_task_info:
            for active_task in active_task_info[host]:
                if active_task['name'] == 'app.systest_manager.runPhase':
                    if active_task['id'] == taskId:
                        running = True  # phase running

    except Exception as ex:
        print(ex)
        logger.error(ex)

    # put result into response queue for any remote client requesting this status
    if rcq is not None:
        get_phase_status.rabbitHelper.putMsg(rcq, str(running))

