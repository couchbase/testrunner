import argparse
import json
from rabbit_helper import RabbitHelper
import uuid
import time
import copy

parser = argparse.ArgumentParser(description='CB System Test Tool')
subparser = parser.add_subparsers(dest="subparsers")

def add_modifier_args(parser):
    parser.add_argument("--cc_queues",    nargs='+', help="queues to copy created keys into")
    parser.add_argument("--consume_queue", help="queue with keys to get/update/delete")
    parser.add_argument("--precondition", help="required stat or cluster state required before running workload")
    parser.add_argument("--postcondition", help="required stat or cluster state required to complete workload")
    parser.add_argument("--wait",  nargs=3,  help="time to wait before starting workload: <hour> <min> <sec>", metavar = ('HOUR', 'MIN', 'SEC'), type=int)
#depreciated
#parser.add_argument("--expires",nargs=3,  help="time to wait before terminating workload: <hour> <min> <sec>", metavar = ('HOUR','MIN','SEC'), type=int)

def add_broker_arg(parser):
    parser.add_argument("--broker", help="ip address of broker used to consume options")
    parser.add_argument("--cluster", default="deafult", help="queue suffix within broker used to consume option")

def add_template_parser(parent):
    parser = parent.add_parser("template")

    add_broker_arg(parser)
    parser.add_argument("--name",     help="template name", required = True)
    parser.add_argument("--ttl",      default=0, help="document expires time")
    parser.add_argument("--flags",    default=0, help="document create flags")
    parser.add_argument("--cc_queues", nargs='+', help="queues to copy created keys into")
    parser.add_argument("--kvpairs",   nargs='+', help="list of kv items i.e=> state:ca,age:28,company:cb")
    parser.add_argument("--type",    help="json/non-json default is json", default="json")
    parser.add_argument("--size", nargs='+',    help="size of documents. padding is used if necessary")

#TODO    parser.add_argument("--blobs",   nargs='+', help="data strings for non-json docs")
    parser.set_defaults(handler=import_template)

#TODO    parser.add_argument("--blobs",   nargs='+', help="data strings for non-json docs")
    parser.set_defaults(handler=import_template)


def add_workload_parser(parent):
    parser = parent.add_parser("workload")

    add_broker_arg(parser)
    parser.add_argument("--name",    help="predefind workload", default="default")
    parser.add_argument("--bucket",  help="bucket", default="default")
    parser.add_argument("--password", help="password", default="")
    parser.add_argument("--ops",     help="ops per sec", default=0, type=int)
    parser.add_argument("--create",  help="percentage of creates 0-100", default=0, type=int)
    parser.add_argument("--update",  help="percentage of updates 0-100", default=0, type=int)
    parser.add_argument("--get",     help="percentage of gets 0-100", default=0, type=int)
    parser.add_argument("--miss",    help="percentage of misses 0-100", default=0, type=int)
    parser.add_argument("--expire",  help="percentage of expirations 0-100", default=0, type=int)
    parser.add_argument("--ttl",      default=15, help="document expires time to use when expirations set")
    parser.add_argument("--delete",  help="percentage of deletes 0-100", default=0, type=int)
    parser.add_argument("--template", help="predefined template to use", default="default")
    parser.add_argument("--standalone", help="run without broker", action='store_true')
    parser.add_argument("--hosts",  default=["127.0.0.1"],  nargs='+', help="couchbase hosts for use with standalone")
    parser.add_argument("--padding",  default="", help="you can put a custom string here when using standalone loader")
    add_modifier_args(parser)

    parser.set_defaults(handler=run_workload)

def add_admin_parser(parent):
    parser = parent.add_parser("admin")

    add_broker_arg(parser)
    parser.add_argument("--rebalance_in", help="rebalance_in", default='', type=str)
    parser.add_argument("--rebalance_out", help="rebalance_out", default='', type=str)
    parser.add_argument("--failover", help="failover", default='', type=str)
    parser.add_argument("--only_failover", help="only_failover", default=False, action='store_true')
    parser.add_argument("--soft_restart", help="soft_restart", default='', type=str)
    parser.add_argument("--hard_restart", help="hard_restart", default='', type=str)

    parser.set_defaults(handler=perform_admin_tasks)

def add_xdcr_parser(parent):
    parser = parent.add_parser("xdcr")

    add_broker_arg(parser)
    parser.add_argument("--dest_cluster_ip", help="Dest. cluster ip", default='', type=str)
    parser.add_argument("--dest_cluster_username", help="Dest. cluster rest username", default='Administrator', type=str)
    parser.add_argument("--dest_cluster_pwd", help="Dest. cluster rest pwd", default='password', type=str)
    parser.add_argument("--dest_cluster_name", help="Dest. cluster name", default='', type=str)
    parser.add_argument("--replication_type", help="unidirection or bidirection", default='unidirection', type=str)
    parser.set_defaults(handler=perform_xdcr_tasks)

def add_query_parser(parent):
    parser = parent.add_parser("query")

    add_broker_arg(parser)
    parser.add_argument("--ddoc", help="Design Document", required = True, type=str)
    parser.add_argument("--view", help="Name of view", required = True, type=str)
    parser.add_argument("--bucket", help="Bucket with documents to query", default="default", type=str)
    parser.add_argument("--password", help="Sasl password of bucket", default="", type=str)
    parser.add_argument("--queries_per_sec", help="Queries per second", default=1, type=int, metavar = 'N')
    parser.add_argument("--indexed_key",   help="the key from kvpair being indexed in this query", metavar = "KEY")
    parser.add_argument("--include_filters", help="<startkey_docid, endkey_docid, descending, stale_ok, stale_false>", default=["startkey", "endkey", "limit"], nargs='+', metavar="")
    parser.add_argument("--exclude_filters", help="<startkey, endkey, limit>", default=[], nargs='+', metavar="")
    parser.add_argument("--startkey", help="manually specify value for startkey <default=auto>", type=str)
    parser.add_argument("--endkey", help="manually specify value for endkey <default=auto>", type=str)
    parser.add_argument("--startkey_docid", help="manually specify value for startkey_docid <default=auto>", type=str)
    parser.add_argument("--endkey_docid", help="manually specify value for endkey_docid <default=auto>", type=str)
    parser.add_argument("--limit", help="number of rows in query results", type=int, default=10)
    parser.set_defaults(handler=perform_query_tasks)

def add_test_parser(parent):
    parser = parent.add_parser("test")

    add_broker_arg(parser)
    parser.add_argument("--name", help="name of remote test or runlist to start, found in <pysystests>/tests/<name>.js directory")
    parser.add_argument("--fromfile",  help="launch a test from local file configuration")
    parser.add_argument("--filesuffix", default="js", help="suffix appened to file when using 'name' arg", metavar = 'js')


    parser.set_defaults(handler=run_systemtest)

def setup_run_parser():
    run_parser = subparser.add_parser('run')
    subparser_ = run_parser.add_subparsers()
    add_workload_parser(subparser_)
    add_admin_parser(subparser_)
    add_xdcr_parser(subparser_)
    add_query_parser(subparser_)
    add_test_parser(subparser_)

def setup_import_parser():
    import_parser = subparser.add_parser('import')
    subparser_ = import_parser.add_subparsers()
    add_template_parser(subparser_)


def setup_list_parser():
    list_parser = subparser.add_parser('list')
    list_parser.add_argument('workloads', help='list pre-defined workloads')
    list_parser.add_argument('templates', help='list pre-defined document templates')
    list_parser.add_argument('tests', help='list pre-defined tests')

def conv_to_secs(list_):
    return list_[0]*60*60 + list_[1]*60 + list_[2]

def getResponseQueue(handler):
    rc_queue = "rc_"+str(uuid.uuid4())[:7]
    handler.declare(rc_queue)
    return rc_queue

def receiveResponse(handle, rc_queue, tries = 5):
    while tries > 0:
        rc = handle.getMsg(rc_queue)
        if rc is not None:
            print(rc)
            handle.delete(rc_queue)
            return
        tries = tries - 1
        time.sleep(1)
    print("no response received from broker")
    handle.delete(rc_queue)

def run_workload(args):

    workload = {}

    if args.name != None:
        # TODO: read in workload params from saved store
        # workload.update(cached_workload)
        pass

    if args.wait is not None:
        args.wait = conv_to_secs(args.wait)

    workload = { "bucket"      : args.bucket,
                 "password"    : args.password,
                 "ops_per_sec" : args.ops,
                 "create_perc" : args.create,
                 "update_perc" : args.update,
                 "get_perc"    : args.get,
                 "del_perc"    : args.delete,
                 "exp_perc"    : args.expire,
                 "miss_perc"   : args.miss,
                 "ttl"         : args.ttl,
                 "cc_queues"   : args.cc_queues,
                 "consume_queue" : args.consume_queue,
                 "postconditions" : args.postcondition,
                 "preconditions" : args.precondition,
                 "wait"  : args.wait,
                 "template"  : args.template}
    cluster = args.cluster

    if args.standalone:
        from consumer import start_client_processes
        task = argsToTask(args)
        start_client_processes(task, standalone = True)
    else:
        rabbitHelper = RabbitHelper(args.broker, cluster)
        workload['rcq'] = getResponseQueue(rabbitHelper)
        rabbitHelper.putMsg("workload_"+cluster, json.dumps(workload))
        receiveResponse(rabbitHelper, workload['rcq'])

def argsToTask(args):

    bucket = args.bucket
    password = args.password
    active_hosts = args.hosts
    ops_sec = args.ops
    num_consumers = 1

    ops_sec = int(ops_sec)//num_consumers
    create_count = int(ops_sec *  args.create/100)
    update_count = int(ops_sec *  args.update/100)
    get_count = int(ops_sec *  args.get/100)
    del_count = int(ops_sec *  args.delete/100)
    exp_count = int(ops_sec *  args.expire/100)

    ttl = args.ttl
    miss_perc = args.miss

    # broadcast to sdk_consumers
    msg = {'bucket' : bucket,
           'id' : bucket,
           'password' : password,
           'ops_sec' : ops_sec,
           'create_count' : create_count,
           'update_count' : update_count,
           'get_count' : get_count,
           'del_count' : del_count,
           'exp_count' : exp_count,
           'cc_queues' : None,
           'consume_queue' : None,
           'ttl' : ttl,
           'miss_perc' : miss_perc,
           'active' : True,
           'active_hosts' : active_hosts}

    # set doc-template to this message
    msg_copy = copy.deepcopy(msg)
    msg_copy['template'] = {}
    msg_copy['template']['cc_queues'] = None
    msg_copy['template']['kv'] = msg

    return msg_copy


def import_template(args):

    val = None

    if args.type == "json":
        json_val = {}
        for kv in args.kvpairs:
            pair = '{%s}' % kv
            try:
                pair = json.loads(pair)
                json_val.update(pair)
            except ValueError as ex:
                print("ERROR: Unable to encode as valid json: %s " % kv)
                print("make sure strings surrounded by double quotes")
                return
        val = json_val

    #TODO binary blobs

    template = {"name" : args.name,
                "ttl" : args.ttl,
                "flags" : args.flags,
                "cc_queues" : args.cc_queues,
                "size" : args.size,
                "kv" : val}
    cluster = args.cluster

    rabbitHelper = RabbitHelper(args.broker, cluster)
    template['rcq'] = getResponseQueue(rabbitHelper)
    rabbitHelper.putMsg("workload_template_"+cluster, json.dumps(template))
    receiveResponse(rabbitHelper, template['rcq'])

def perform_admin_tasks(args):

    actions = {'rebalance_in': args.rebalance_in,
               'rebalance_out': args.rebalance_out,
               'failover': args.failover,
               'soft_restart': args.soft_restart,
               'hard_restart': args.hard_restart,
               'only_failover': args.only_failover
              }
    cluster = args.cluster

    #TODO: Validate the user inputs, before passing to rabbit
    rabbitHelper = RabbitHelper(args.broker, cluster)
    actions['rcq'] = getResponseQueue(rabbitHelper)
    rabbitHelper.putMsg("admin_"+cluster, json.dumps(actions))
    receiveResponse(rabbitHelper, actions['rcq'])

def perform_xdcr_tasks(args):

    xdcrMsg = {'dest_cluster_ip': args.dest_cluster_ip,
               'dest_cluster_rest_username': args.dest_cluster_username,
               'dest_cluster_rest_pwd':  args.dest_cluster_pwd,
               'dest_cluster_name': args.dest_cluster_name,
               'replication_type': args.replication_type,
    }
    cluster = args.cluster

    #TODO: Validate the user inputs, before passing to rabbit
    print(xdcrMsg)
    rabbitHelper = RabbitHelper(args.broker, cluster)
    xdcrMsg['rcq'] = getResponseQueue(rabbitHelper)
    rabbitHelper.putMsg("xdcr_"+cluster, json.dumps(xdcrMsg))
    receiveResponse(rabbitHelper, xdcrMsg['rcq'])

def perform_query_tasks(args):
    queryMsg = {'queries_per_sec' : args.queries_per_sec,
                'ddoc' : args.ddoc,
                'view' : args.view,
                'bucket' : args.bucket,
                'password' : args.password,
                'include_filters' : args.include_filters,
                'exclude_filters' : args.exclude_filters,
                'startkey' : args.startkey,
                'endkey' : args.endkey,
                'startkey_docid' : args.startkey_docid,
                'endkey_docid' : args.endkey_docid,
                'limit' : args.limit,
                'indexed_key' : args.indexed_key}

    cluster = args.cluster

    rabbitHelper = RabbitHelper(args.broker, cluster)
    queryMsg['rcq'] = getResponseQueue(rabbitHelper)
    rabbitHelper.putMsg('query_'+cluster, json.dumps(queryMsg))
    receiveResponse(rabbitHelper, queryMsg['rcq'])

def run_systemtest(args):

    cluster = args.cluster
    rabbitHelper = RabbitHelper(args.broker, cluster)

    test = {'suffix' : args.filesuffix}

    if args.fromfile is not None:

        # load json config
        json_data = open(args.fromfile)
        msg = json.load(json_data)

    elif args.name is not None:
        msg = { "localtestname" : args.name }

    test.update(msg)
    test['rcq'] = getResponseQueue(rabbitHelper)
    rabbitHelper.putMsg('systest_manager_'+cluster, json.dumps(test))
    receiveResponse(rabbitHelper, test['rcq'])


if __name__ == "__main__":

    ### setup main arg parsers
    setup_run_parser()
    setup_list_parser()
    setup_import_parser()

    ## PARSE ARGS ##
    args = parser.parse_args()

    # setup parser callbacks
    if args.subparsers == "run" or\
        args.subparsers == "import":
        args.handler(args)

