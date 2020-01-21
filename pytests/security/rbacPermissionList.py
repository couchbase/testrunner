from membase.api.rest_client import RestConnection
from membase.helper.cluster_helper import ClusterOperationHelper
import urllib.request, urllib.parse, urllib.error
import json
import httplib2
from tasks.future import Future
from tasks.taskmanager import TaskManager
from tasks.task import *
import types
from couchbase_helper.document import View
import base64
import time
import logger
log = logger.Logger.get_logger()

class rbacPermissionList():

    def _create_headers(self,username,password,capi=False):
        authorization = base64.encodestring(('%s:%s' % (username, password)).encode()).decode()
        if capi:
            return {'Content-Type': 'application/json',
                    'Authorization': 'Basic %s' % authorization,
                    'Accept': '*/*'}
        else:
            return {'Content-Type': 'application/x-www-form-urlencoded',
                    'Authorization': 'Basic %s' % authorization,
                    'Accept': '*/*'}

    def check_rebalance_complete(self, rest):
        progress = None
        count = 0
        while (progress == 'running' or count < 10):
            progress = rest._rebalance_progress_status()
            time.sleep(10)
            count = count + 1
        if progress == 'none':
            return True
        else:
            return False

    def _rest_client_wrapper(self,url,method,params="",username='Administrator',password='password',restClient=None,capi=False):
        api = restClient.baseUrl + url
        headers = self._create_headers(username, password, capi=capi)
        status, content, header = restClient._http_request(api, method=method, params=params, headers=headers)
        return header['status']

    def _return_http_code(self,permission,username,password,host,httpCode,user_role,port=None,capi=False):
        log.info(" ----- Permission set is ------------{0}".format(permission))
        flag = True
        test = {}
        list_return = []
        return_list = []
        for per in permission:
            values = permission[per]
            temp = values.split(";")
            url = temp[0]
            method = temp[1]
            if len(temp) == 3:
                params = temp[2]
                params = params.replace("?", ";")
                json_acceptable_string = params.replace("'", "\"")
                if capi:
                    params = json_acceptable_string
                    print(params)
                else:
                    params = json.loads(json_acceptable_string)
                    params = urllib.parse.urlencode(params)
            else:
                params = ""
            restClient = RestConnection(host)
            result = self._rest_client_wrapper(url, method, params=params, username=username, password=password, restClient=restClient, capi=capi)
            test['url'] = str(url)
            test['result'] = result
            test['perName'] = str(per)
            list_return.append(test)
            test={}

        for list in list_return:
            if int(list['result']) not in httpCode:
                print("Return HTTP Code does not match with expected -- {0} and actual - {1}".format(list['result'], httpCode))
                return_list.append(list)
                flag = 'False'
                return_value = {
                        "final_result":flag,
                        "failed_result":return_list
                }
                with open(user_role, "a") as myfile:
                    myfile.write(str(return_value) + "\n")
        return flag


    def cluster_admin_diag_read(self,username,password,host,port=8091, servers=None,cluster=None,httpCode=None,user_role=None):
        _cluster_admin_diag_read = {
            "getDiag": "diag;GET",
            #"getDiagVbuckets":"/diag/vbuckets:GET",
            #"getDiagAle":"diag/ale;GET",
            #"getDiagMasterEvents":"/diag/masterEvents:GET",
        }
        result = self._return_http_code(_cluster_admin_diag_read, username, password, host=host, port=port, httpCode=httpCode, user_role=user_role)

    def cluster_admin_diag_write(self,username,password,host,port=8091, servers=None,cluster=None,httpCode=None,user_role=None):
        _cluster_admin_diag_write = {
            "eval":"/diag/eval;POST;{'ale':'set_loglevel(ns_server,error).'}"
        }

        result = self._return_http_code(_cluster_admin_diag_write, username, password, host=host, port=port, httpCode=httpCode, user_role=user_role)


    def cluster_pools_read(self,username,password,host,port=8091, servers=None,cluster=None,httpCode=None,user_role=None):
        _cluster_pools_read = {
            "Pools": "/pools;GET",
            "PoolsDefault": "/pools/default;GET",
            #"poolsStreaming":"/poolsStreaming/default:GET"
            "poolsNode": "/pools/nodes;GET",
            "nodeServices": "/pools/default/nodeServices;GET",
            #"nodeServiceStreaming":"/pools/default/nodeServicesStreaming;GET"
        }
        result = self._return_http_code(_cluster_pools_read, username, password, host=host, port=port, httpCode=httpCode, user_role=user_role)


    def cluster_nodes_read(self,username,password,host,port=8091, servers=None,cluster=None,httpCode=None,user_role=None):
        _cluster_nodes_read = {
            "pools_nodeStatus": "/nodeStatuses;GET",
            "pools_nodes": "/pools/nodes;GET",
        }

        result = self._return_http_code(_cluster_nodes_read, username, password, host=host, port=port, httpCode=httpCode, user_role=user_role)

    def cluster_samples_read(self,username,password,host,port=8091, servers=None,cluster=None,httpCode=None,user_role=None):
        _cluster_sample_read = {
            "getSampleBuckets":"/sampleBuckets;GET"
        }
        result = self._return_http_code(_cluster_sample_read, username, password, host=host, port=port, httpCode=httpCode, user_role=user_role)


    def cluster_settings_read(self,username,password,host,port=8091, servers=None,cluster=None,httpCode=None,user_role=None):
        _cluster_settings_read = {
            "settingsWeb":"/settings/web;GET",
            "settingsAlert":"/settings/alerts;GET",
            "settingsStat":"/settings/stats;GET",
            "settingsAutoFailover":"/settings/autoFailover;GET",
            "settingsAutoCompaction":"/settings/autoCompaction;GET"
        }
        result = self._return_http_code(_cluster_settings_read, username, password, host=host, port=port, httpCode=httpCode, user_role=user_role)


    def get_cluster_tasks_read(self,username,password,host,port=8091, servers=None,cluster=None,httpCode=None,user_role=None):
        _cluster_tasks_read = {
            "TasksRebalanceProg":"/pools/default/rebalanceProgress;GET",
            "Tasks":"/pools/default/tasks;GET"
        }
        result = self._return_http_code(_cluster_tasks_read, username, password, host=host, port=port, httpCode=httpCode, user_role=user_role)


    def get_cluster_pools_write(self,username,password,host,port=8091, servers=None,cluster=None,httpCode=None,user_role=None):
        _cluster_pools_write = {
            "poolsDefault":"/pools/default;POST;{'indexMemoryQuota': '300'}"
        }
        result = self._return_http_code(_cluster_pools_write, username, password, host=host, port=port, httpCode=httpCode, user_role=user_role)


    def get_cluster_settings_write(self,username,password,host,port=8091, servers=None,cluster=None,httpCode=None,user_role=None):
        _cluster_settings_write = {
            "alertsWrite":"settings/alerts;POST;{'enabled': 'true','recipients': 'ritam@couchbase.com','sender': 'ritam@northscale.com','emailUser': 'ritam',\
                                   'emailPass': 'password','emailHost': 'couchbase.com','emailPort': '25','emailEncrypt': 'false','alerts': 'auto_failover_node,auto_failover_maximum_reached'}",
            "testEmailWrite":"settings/alerts/testEmail;POST;{'enabled': 'true','recipients': 'ritam@couchbase.com','sender': 'ritam@northscale.com','emailUser': 'ritam',\
                                   'emailPass': 'password','emailHost': 'couchbase.com','emailPort': '25','emailEncrypt': 'false','alerts': 'auto_failover_node,auto_failover_maximum_reached',\
                                   'body':'This email was sent to you to test the email alert email server settings.','subject':'Test email from Couchbase Server'}",
            #POST /settings/stats (enable sending stats to remote server) is it even used?
            "autoFailoverWrite":"settings/autoFailover;POST;{'enabled': 'true','timeout':30}",
            "resetCountWrite":"settings/autoFailover/resetCount;POST",
            "autoCompactionWrite":"controller/setAutoCompaction;POST;{'parallelDBAndViewCompaction':'false'}",
            "resetAlerts":"controller/resetAlerts;POST"
        }
        result = self._return_http_code(_cluster_settings_write, username, password, host=host, port=port, httpCode=httpCode, user_role=user_role)

    def cluster_nodes_write(self,username,password,host,port=8091, servers=None,cluster=None,httpCode=None,user_role=None):
        try:
            _cluster_nodes_write = {
                "ejectNode": "/controller/ejectNode;POST",
                #"addNode":"/controller/addNode;POST",
                #"addNodeV2":"/controller/addNodeV2;POST",
                #"uuidAddNode":"pools/default/serverGroups/<uuid>/addNode;POST",
                #"uiidAddNodev1":"/pools/default/serverGroups/<uuid>/addNodeV2;POST",
                #"failover":"/controller/failOver;POST",
                #"graceFullFailover":"/controller/startGracefulFailover;POST",
                #"rebalance":"/controller/rebalance;POST",
                #"reAddNode":"/controller/reAddNode;POST",
                #"reFailover":"/controller/reFailOver;POST",
                #"stopRebalance":"/controller/stopRebalance;POST",
                #"setRecoveryType":"/controller/setRecoveryType;POST"
            }

            rest = RestConnection(servers[0])
            known_nodes = []


            #Add Node
            params = {'hostname': servers[1].ip,'user': 'Administrator','password': 'password'}
            add_node = {"addNode":"controller/addNode;POST;" + str(params)}
            result = self._return_http_code(add_node, username, password, host=host, port=port, httpCode=httpCode, user_role=user_role)

            #cluster.rebalance(servers,servers[1],[])
            rest.eject_node("Administrator", "password", 'ns_1@'+servers[1].ip)

            #cluster.rebalance(servers,[],servers[1:])

            #time.sleep(30)
            #params = {'hostname': servers[1].ip,'user': 'Administrator','password': 'password'}
            #add_node = {"addNode":"controller/addNodeV2;POST;" + str(params)}
            #result = self._return_http_code(add_node,username,password,host=host,port=port, httpCode=httpCode, user_role=user_role)

            #cluster.rebalance(servers,[],servers[1:])

            time.sleep(30)
            cluster.rebalance(servers, servers[1:], [])
            params = {'otpNode': "ns_1@"+servers[1].ip}
            failover_node = {"failover":"controller/failOver;POST;"+str(params)}
            result = self._return_http_code(failover_node, username, password, host=host, port=port, httpCode=httpCode, user_role=user_role)
            time.sleep(30)
            cluster.rebalance(servers, [], servers[1:])

            time.sleep(15)
            cluster.rebalance(servers, servers[1:], [])
            time.sleep(15)
            params = {'otpNode': "ns_1@"+servers[1].ip}
            grace_failover = {"grace_failover":"controller/startGracefulFailover;POST;"+str(params)}
            result = self._return_http_code(grace_failover, username, password, host=host, port=port, httpCode=httpCode, user_role=user_role)
            time.sleep(60)
            rest.set_recovery_type("ns_1@"+servers[1].ip, 'delta')
            time.sleep(30)

            rest.add_back_node("ns_1@"+servers[1].ip)

            time.sleep(30)
            serv_out = 'ns_1@' + servers[2].ip
            rest.fail_over(serv_out, graceful=False)
            time.sleep(15)
            params = {'otpNode': "ns_1@"+servers[2].ip}
            radd_node = {"reAddNode":"controller/reAddNode;POST;"+ str(params)}
            result = self._return_http_code(radd_node, username, password, host=host, port=port, httpCode=httpCode, user_role=user_role)


            time.sleep(30)

            #serv_out = 'ns_1@' + servers[3].ip
            #rest.fail_over(serv_out,graceful=False)
            #params = {'otpNode': "ns_1@"+servers[3].ip}
            #radd_node = {"reFailOver":"controller/reFailOver;POST;"+ str(params)}
            #result = self._return_http_code(radd_node,username,password,host=host,port=port, httpCode=httpCode, user_role=user_role)

            cluster.rebalance(servers, [], servers[1:])

            time.sleep(30)
            cluster.rebalance(servers, servers[1:], [])
            time.sleep(30)
            serv_out = 'ns_1@' + servers[1].ip
            rest.fail_over(serv_out, graceful=True)
            time.sleep(60)
            params = {'otpNode': 'ns_1@'+servers[1].ip,'recoveryType': 'delta'}
            recovery_type = {"setRecoveryType":"controller/setRecoveryType;POST;"+ str(params)}
            result = self._return_http_code(recovery_type, username, password, host=host, port=port, httpCode=httpCode, user_role=user_role)
            cluster.rebalance(servers)
        except:
            log.info ("Issue with rebalance, going to next test case")
            cluster.rebalance(servers, [], servers[1:])
            for server in servers:
                rest = RestConnection(server)
                rest.init_cluster(username='Administrator', password='password')
                rest.init_cluster_memoryQuota(memoryQuota=rest.get_nodes_self().mcdMemoryReserved)

    def cluster_bucket_all_create(self,username,password,host,port=8091, servers=None,cluster=None,httpCode=None,user_role=None):
        _cluster_bucket_all_create = {
            "sampleInstall":"/sampleBuckets/install;POST",
            "Buckets":"/pools/default/buckets;POST"
        }
        try:
            params = {'name': 'default1',
                        'authType': 'sasl',
                        'saslPassword': '',
                        'ramQuotaMB': 100,
                        'replicaNumber': 1,
                        'proxyPort': 1121,
                        'bucketType': 'membase',
                        'replicaIndex': 1,
                        'threadsNumber': 2,
                        'flushEnabled': 1,
                        'evictionPolicy': 'valueOnly'
                      }

            create_bucket = {"Buckets":"/pools/default/buckets;POST;"+ str(params)}
            if user_role in ['admin', 'cluster_admin']:
                result = self._return_http_code(create_bucket, username, password, host=host, port=port, httpCode=202, user_role=user_role)
            else:
                result = self._return_http_code(create_bucket, username, password, host=host, port=port, httpCode=httpCode, user_role=user_role)

            #sampleInstall = {"sampleInstall":"/sampleBuckets/install;POST;{['beer-sample']"}
            #result = self._return_http_code(sampleInstall,username,password,host=host,port=port, httpCode=httpCode, user_role=user_role)
        except:
            log.info ("Issue with create bucket, going to next test case")


    def cluster_bucket_settings_read(self,username,password,host,port=8091, servers=None,cluster=None,httpCode=None,user_role=None):
        _cluster_bucket_settings_read = {
            "default": "/pools/default/b/<bucket-name>;GET",
            #"defaultBS":"/pools/default/bs/" + bucket + ":GET",
            "bucketNodes": "/pools/default/buckets/<bucket-name>/nodes;GET",
            "bucketNodeID": "/pools/default/buckets/<bucket-name>/nodes/<node_id>:8091;GET",
            "bucketDot": "/dot/<bucket-name>;GET",
            "bucketsvg": "/dotsvg/<bucket-name>;GET",
            "bucketPools": "/pools/default/buckets;GET",
            "bucketName": "/pools/default/buckets/<bucket-name>;GET",
            #"bucketStreaming":"/pools/default/bucketsStreaming/" + bucket + ":GET"
        }

        try:
            for perm in _cluster_bucket_settings_read:
                temp = _cluster_bucket_settings_read[perm]
                temp = temp.replace('<bucket-name>', 'default')
                temp = temp.replace('<node_id>', host.ip)
                _cluster_bucket_settings_read[perm] = temp

            result = self._return_http_code(_cluster_bucket_settings_read, username, password, host=host, port=port, httpCode=httpCode, user_role=user_role)
        except:
            log.info("Issue with bucket setting read, going to next test case")


    def cluster_bucket_settings_write(self,username,password,host,port=8091, servers=None,cluster=None,httpCode=None,user_role=None):
        _cluster_bucket_settings_write ={
            "bucketSettingsWrite":"/pools/default/buckets/<bucket_name>;POST;{'ramQuotaMB'=200}"
        }
        try:
            params = {'ramQuotaMB': 100}

            bucket_write = {"bucketSettingsWrite":"/pools/default/buckets/default;POST;" + str (params)}
            result = self._return_http_code(bucket_write, username, password, host=host, port=port, httpCode=httpCode, user_role=user_role)
        except:
            log.info("Issue with bucket setting write, going to next test case")

    def cluster_bucket_flush(self,username,password,host,port=8091, servers=None,cluster=None,httpCode=None,user_role=None):
        _cluster_bucket_flush ={
            "bucketSettingsWriteFlush":"/pools/default/buckets/<bucket_name>/controller/doFlush;POST"
        }

        for perm in _cluster_bucket_flush:
            temp = _cluster_bucket_flush[perm]
            temp = temp.replace('<bucket-name>', 'default')
            _cluster_bucket_flush[perm] = temp

        result = self._return_http_code(_cluster_bucket_flush, username, password, host=host, port=port, httpCode=httpCode, user_role=user_role)


    def cluster_bucket_delete(self,username,password,host,port=8091, servers=None,cluster=None,httpCode=None,user_role=None):
        _cluster_bucket_delete ={
            "unsafePurgeBuckett":"pools/default/buckets/<bucket_name>/controller/unsafePurgeBucket;POST",
            "delete_bucket":"pools/default/buckets/<bucket_name>;DELETE"
        }
        try:

            bucket_write = {"unsafePurgeBuckett":"pools/default/buckets/default/controller/unsafePurgeBucket;POST"}
            result = self._return_http_code(bucket_write, username, password, host=host, port=port, httpCode=httpCode, user_role=user_role)
            bucket_write = {"delete_bucket":"pools/default/buckets/default;DELETE"}
            result = self._return_http_code(bucket_write, username, password, host=host, port=port, httpCode=httpCode, user_role=user_role)
        except:
            log.info ("Issue with bucket delete, going to next test case")


    def cluster_bucket_compact(self,username,password,host,port=8091, servers=None,cluster=None,httpCode=None,user_role=None):
        _cluster_bucket_compact = {
            "compact_bucket":"/pools/default/buckets/<bucket_name>/controller/compactBucket;POST",
            "cancel_compaction":"/pools/default/buckets/<bucket_name>/controller/cancelBucketCompaction;POST",
            "compact_database":"/pools/default/buckets/<bucket_name>/controller/compactDatabases;POST",
            "cancel_db_compact":"/pools/default/buckets/<bucket_name>/controller/cancelDatabasesCompaction;POST"
        }


        for perm in _cluster_bucket_compact:
            temp = _cluster_bucket_compact[perm]
            temp = temp.replace('<bucket_name>', 'default')
            _cluster_bucket_compact[perm] = temp

        result = self._return_http_code(_cluster_bucket_compact, username, password, host=host, port=port, httpCode=httpCode, user_role=user_role)

    #--------------PENDING -------------------------###
    def cluster_bucket_data_write(self,username,password,host,port=8091, servers=None,cluster=None,httpCode=None,user_role=None):


        _cluster_bucket_data_write = {
            "create_doc_id":"pools/default/buckets/<bucket_name>/docs/<doc_id>;POST",
            "delete_doc_id":"pools/default/buckets/<bucket_name>/docs/<doc_id>;DELETE"
        }
        cluster.create_view(host, "Doc1", View('abcd', 'function (doc) { emit(doc.age, doc.first_name);}', None), 'default', 180, with_query=False)
        #doc_id = "_design/dev_Doc1"
        doc_id = '1234'

        for perm in _cluster_bucket_data_write:
            temp = _cluster_bucket_data_write[perm]
            temp = temp.replace('<bucket_name>', 'default')
            temp = temp.replace('<doc_id>', doc_id)
            _cluster_bucket_data_write[perm] = temp

        bucket_data_write = {"create_doc_id":"pools/default/buckets/default/docs/1234;POST;{'click':'to edit','with JSON':'there are no reserved field names'}"}
        result = self._return_http_code(bucket_data_write, username, password, host=host, port=8091, httpCode=httpCode, user_role=user_role)

        bucket_data_write = {"create_doc_id":"pools/default/buckets/default/docs/1234;DELETE"}
        result = self._return_http_code(bucket_data_write, username, password, host=host, port=8091, httpCode=httpCode, user_role=user_role)


    def cluster_bucket_recovery_read(self,username,password,host,port=8091, servers=None,cluster=None,httpCode=None,user_role=None):
        _cluster_bucket_recovery_read = {
            "recovery_read":"/pools/default/buckets/<name>/recoveryStatus;GET"
        }

        for perm in _cluster_bucket_recovery_read:
            temp = _cluster_bucket_recovery_read[perm]
            temp = temp.replace('<name>', 'default')
            _cluster_bucket_recovery_read[perm] = temp

        #result = self._return_http_code(_cluster_bucket_recovery_read,username,password,host=host,port=8092, httpCode=httpCode, user_role=user_role)

    #--------------PENDING -------------------------#
    def cluster_bucket_recovery_write(self,username,password,host,port=8091, servers=None,cluster=None,httpCode=None,user_role=None):
        _cluster_bucket_recovery_read = {
            "recovery_start":"pools/default/buckets/<bucket_name>/controller/startRecovery;POST",
            "recovery_stop":"pools/default/buckets/<bucket_name>/controller/stopRecovery;POST",
            "covevery_commit":"pools/default/buckets/<bucket_name>/controller/commitVBucket;POST"
        }

        for perm in _cluster_bucket_recovery_read:
            temp = _cluster_bucket_recovery_read[perm]
            temp = temp.replace('<bucket_name>', 'default')
            _cluster_bucket_recovery_read[perm] = temp

        #result = self._return_http_code(_cluster_bucket_recovery_read,username,password,host=host,port=8092, httpCode=httpCode, user_role=user_role)


    def cluster_stats_read(self,username,password,host,port=8091, servers=None,cluster=None,httpCode=None,user_role=None):
        _cluster_stats_read = {
            #"overviewStats":"pools/default/overviewStats;GET",
            #"stats_direcotry":"pools/default/buckets/<name>/statsDirectory;GET",
            #"query_stats":"pools/default/buckets/@query/stats;GET",
            #"xdcr_stat":"pools/default/buckets/@xdcr-<bucket_name>/stats;GET",
            #"index_stat":"pools/default/buckets/@index-<bucket_name>/stats;GET",
            #"node_stat":"pools/default/buckets/@query/nodes/<node_id>/stats;GET",
            #"xdcr_node_bucket":"pools/default/buckets/@xdcr-<bucket_name>/nodes/<node_id>/stats;GET",
            #"bucket_node":"pools/default/buckets/@index-<bucket_name>/nodes/<node_id>/stats;GET"
        }

        for perm in _cluster_stats_read:
            temp = _cluster_stats_read[perm]
            temp = temp.replace('<bucket_name>', 'default')
            temp = temp.replace('<name>', 'default')
            temp = temp.replace('<node_id>', host.ip)
            _cluster_stats_read[perm] = temp

        #result = self._return_http_code(_cluster_stats_read,username,password,host=host,port=8091, httpCode=httpCode, user_role=user_role)


    def cluster_bucket_stats_read(self,username,password,host,port=8091, servers=None,cluster=None,httpCode=None,user_role=None):
        _cluster_bucket_stats_read = {
            "stats_directory": "pools/default/buckets/<name>/statsDirectory;GET",
            #"statname":"pools/default/buckets/<bucket_id>/stats/<query_selects>;GET",
            #"stats":"/pools/default/buckets/<name>/stats;GET",
            #"node_stat":"pools/default/buckets/<bucket_id>/nodes/<node_id>/stats;GET"
        }

        for perm in _cluster_bucket_stats_read:
            temp = _cluster_bucket_stats_read[perm]
            temp = temp.replace('<bucket_name>', 'default')
            temp = temp.replace('<name>', 'default')
            temp = temp.replace('<node_id>', host.ip)
            _cluster_bucket_stats_read[perm] = temp

        result = self._return_http_code(_cluster_bucket_stats_read, username, password, host=host, port=8091, httpCode=httpCode, user_role=user_role)


    #----Add new security REST API ------------------#
    def cluster_admin_security_read(self,username,password,host,port=8091, servers=None,cluster=None,httpCode=None,user_role=None):
        rest = RestConnection(host)
        rest.create_ro_user('ritam1234', 'password')

        _cluster_admin_security_read ={
            "readOnlyAdmin":"/settings/readOnlyAdminName;GET",
            "saslauth":"settings/saslauthdAuth;GET",
            "audit":"/settings/audit;GET"
        }

        result = self._return_http_code(_cluster_admin_security_read, username, password, host=host, port=8091, httpCode=httpCode, user_role=user_role)

    def cluster_admin_security_write(self,username,password,host,port=8091, servers=None,cluster=None,httpCode=None,user_role=None):

        _cluster_admin_security_write = {
            "change_pass":"/settings/readOnlyUser;PUT",
            "create_readonly":"/settings/readOnlyUser;POST",
            "delete_readonly":"settings/readOnlyUser;DELETE",
            "saslauthd":"/settings/saslauthdAuth;POST",
            "audit":"settings/audit;POST",
            #"validateCredentails":"validateCredentials;POST",
            "regenerateCert":"controller/regenerateCertificate;POST"
        }

        rest = RestConnection(host)
        rest.create_ro_user('ritam1', 'password')
        _cluster_admin_security_write = {"change_pass":"/settings/readOnlyUser;PUT;{'password':'passowrd123'}"}
        result = self._return_http_code(_cluster_admin_security_write, username, password, host=host, port=port, httpCode=httpCode, user_role=user_role)

        _cluster_admin_security_write = {"create_readonly":"/settings/readOnlyUser;POST;{'username':'ritam1123','password':'passowrd123'}"}
        result = self._return_http_code(_cluster_admin_security_write, username, password, host=host, port=port, httpCode=httpCode, user_role=user_role)

        _cluster_admin_security_write = {"delete_readonly":"settings/readOnlyUser;DELETE"}
        result = self._return_http_code(_cluster_admin_security_write, username, password, host=host, port=port, httpCode=httpCode, user_role=user_role)

        _cluster_admin_security_write = {"saslauthd":"/settings/saslauthdAuth;POST;{'enabled':'true','admins':[],'roAdmins':[]}"}
        result = self._return_http_code(_cluster_admin_security_write, username, password, host=host, port=port, httpCode=httpCode, user_role=user_role)

        _cluster_admin_security_write = {"audit":"settings/audit;POST;{'auditdEnabled':'true'}"}
        result = self._return_http_code(_cluster_admin_security_write, username, password, host=host, port=port, httpCode=httpCode, user_role=user_role)

        #_cluster_admin_security_write = {"validateCredentails":"validateCredentials;POST;{'user':'foo','password':'bar'}"}
        #result = self._return_http_code(_cluster_admin_security_write,username,password,host=host,port=port, httpCode=httpCode, user_role=user_role)

        _cluster_admin_security_write = {"regenerateCert":"controller/regenerateCertificate;POST"}
        result = self._return_http_code(_cluster_admin_security_write, username, password, host=host, port=port, httpCode=httpCode, user_role=user_role)


    def cluster_logs_read(self,username,password,host,port=8091, servers=None,cluster=None,httpCode=None,user_role=None):

        _cluster_logs_read ={
            "getLogs":"/logs;GET"
        }

        result = self._return_http_code(_cluster_logs_read, username, password, host=host, port=8091, httpCode=httpCode, user_role=user_role)

    def cluster_admin_logs_read(self,username,password,host,port=8091, servers=None,cluster=None,httpCode=None,user_role=None):

        _cluster_admin_logs_read ={
            "sasl_logs":"sasl_logs;GET",
            #"GET /sasl_logs/<log_name>",
            #"startlog":"controller/startLogsCollection;POST;{'nodes'='','from'=''}",
            "cancellogcollection":"controller/cancelLogsCollection;POST"
        }

        result = self._return_http_code(_cluster_admin_logs_read, username, password, host=host, port=8091, httpCode=httpCode, user_role=user_role)



    def  cluster_bucket_views_read(self,username,password,host,port=8091, servers=None,cluster=None,httpCode=None,user_role=None):

        cluster.create_view(host, "Doc1", View('abcd', 'function (doc) { emit(doc.age, doc.first_name);}', None), 'default', 180, with_query=False)
        doc_id = "_design/dev_Doc1"

        _cluster_bucket_views_read ={
            "getview":"pools/default/buckets/<bucket_name>/ddocs;GET"
        }

        for perm in _cluster_bucket_views_read:
            temp = _cluster_bucket_views_read[perm]
            temp = temp.replace('<bucket_name>', 'default')
            _cluster_bucket_views_read[perm] = temp

        result = self._return_http_code(_cluster_bucket_views_read, username, password, host=host, port=8092, httpCode=httpCode, user_role=user_role)



    def cluster_bucket_views_compact(self,username,password,host,port=8091, servers=None,cluster=None,httpCode=None,user_role=None):

        cluster.create_view(host, "Doc1", View('abcd', 'function (doc) { emit(doc.age, doc.first_name);}', None), 'default', 180, with_query=False)
        doc_id = "_design%2Fdev_Doc1"

        _cluster_bucket_views_compact ={
            "compact_view":"pools/default/buckets/<bucket_name>/ddocs/<doc_id>/controller/compactView;POST",
            "cancel_compact":"pools/default/buckets/<bucket_name>/ddocs/<doc_id>/controller/cancelViewCompaction;POST"
        }

        for perm in _cluster_bucket_views_compact:
            temp = _cluster_bucket_views_compact[perm]
            temp = temp.replace('<bucket_name>', 'default')
            temp = temp.replace('<doc_id>', doc_id)
            _cluster_bucket_views_compact[perm] = temp

        result = self._return_http_code(_cluster_bucket_views_compact, username, password, host=host, port=8092, httpCode=httpCode, user_role=user_role)


    def cluster_bucket_views_write(self,username,password,host,port=8091, servers=None,cluster=None,httpCode=None,user_role=None):
        '''
        cluster.create_view(host,"Doc1",View('abcd', 'function (doc) { emit(doc.age, doc.first_name);}', None),'default',180,with_query=False)
        doc_id = "_design%2Fdev_Doc1"

        '''

        _cluster_bucket_views_write ={
            "view_write":"couchBase/default/_design/dev_1234;PUT;{'views':{'1234':{'map':'function(doc, meta){emit(meta.id,null)?}'}}}"
        }

        result = self._return_http_code(_cluster_bucket_views_write, username, password, host=host, port=8091, httpCode=httpCode, user_role=user_role, capi=True)


    def cluster_server_groups_read(self,username,password,host,port=8091, servers=None,cluster=None,httpCode=None,user_role=None):
        _cluster_server_groups_read = {
            "get_server_groups":"pools/default/serverGroups;GET"
        }

        result = self._return_http_code(_cluster_server_groups_read, username, password, host=host, port=port, httpCode=httpCode, user_role=user_role)


    def cluster_server_groups_write(self,username,password,host,port=8091, servers=None,cluster=None,httpCode=None,user_role=None):
        _cluster_server_groups_write = {
            "create_server_group":"pools/default/serverGroups;POST",
            "delete_server_group":"pools/default/serverGroups/<uuid>;DELETE",
            "edit_server_group":"pools/default/serverGroups;PUT",
            "edit_server_group_id":"pools/default/serverGroups/<uuid>;PUT"
        }

        rest = RestConnection(servers[0])
        try:
            rest.delete_zone('rbacNewGroup')
            rest.delete_zone('rbackGroup')
            rest.delete_zone('rbacNewGroupUpdated')

            create_server_group = {"create_server_group":"pools/default/serverGroups;POST;" + "{'name':'rbacGroup'}"}
            result = self._return_http_code(create_server_group, username, password, host=host, port=port, httpCode=httpCode, user_role=user_role)

            zones = rest.get_zone_names()
            create_server_group = {"delete_server_group":"pools/default/serverGroups/" + zones['rbacGroup'] + ";DELETE"}
            result = self._return_http_code(create_server_group, username, password, host=host, port=port, httpCode=httpCode, user_role=user_role)

            rest.add_zone('rbacNewGroup')
            zones = rest.get_zone_names()
            create_server_group = {"create_server_group":"pools/default/serverGroups/" + zones['rbacNewGroup'] + ";PUT;" + "{'name':'rbacNewGroupUpdated'}"}
            result = self._return_http_code(create_server_group, username, password, host=host, port=port, httpCode=httpCode, user_role=user_role)

        except:
            print("Issues with Server Group add test case")




    def cluster_indexes_read(self,username,password,host,port=8091, servers=None,cluster=None,httpCode=None,user_role=None):
        _cluster_indexes_read = {
            "maxParalledIndexer":"settings/maxParallelIndexers;GET",
            "view_updated":"settings/viewUpdateDaemon;GET",
            #"view_index_stauts":"indexStatus;GET",
            "view_index_settings":"settings/indexes;GET"
        }

        result = self._return_http_code(_cluster_indexes_read, username, password, host=host, port=port, httpCode=httpCode, user_role=user_role)


    def cluster_indexes_write(self,username,password,host,port=8091,servers=None,cluster=None,httpCode=None,user_role=None):
        _cluster_indexes_write = {
            "max_paralled_index":"settings/maxParallelIndexers;POST;{'globalValue':'8'}",
            #"view_update_daemon":"settings/viewUpdateDaemon;POST",
            "indexes":"settings/indexes;POST;{'indexerThreads':5}"
        }

        result = self._return_http_code(_cluster_indexes_write, username, password, host=host, port=port, httpCode=httpCode, user_role=user_role)


    def cluster_bucket_xdcr_write(self,username,password,host,port=8091,servers=None,cluster=None,httpCode=None,user_role=None):
        _cluster_xdcr_settings_read = {
            "create_replication":"controller/createReplication;POST",
            "cancel_XDCR":"controller/cancelXDCR/<xid>;POST",
            "delete_XDCR":"controller/cancelXDCR/<xid>;DELETE"
        }

        rest = RestConnection(servers[0])
        rest.remove_all_replications()
        rest.remove_all_remote_clusters()

        remote_cluster_name = 'rbac_cluster'
        rest = RestConnection(servers[0])
        remote_server01 = servers[1]
        remote_server02 = servers[2]
        rest_remote01 = RestConnection(remote_server01)
        rest_remote01.delete_bucket()
        rest_remote01.create_bucket(bucket='default1', ramQuotaMB=100, proxyPort=11252)
        rest_remote02 = RestConnection(remote_server02)
        remote_id = rest.add_remote_cluster(remote_server01.ip, 8091, 'Administrator', 'password', remote_cluster_name)
        time.sleep(10)
        #replication_id = rest.start_replication('continuous','default',remote_cluster_name)

        param_map = {'replicationType': 'continuous','toBucket': 'default1','fromBucket': 'default','toCluster': remote_cluster_name,
                     'type': 'capi'}
        create_replication = {"create_replication":"controller/createReplication;POST;"+str(param_map)}
        result = self._return_http_code(create_replication, username, password, host=host, port=port, httpCode=httpCode, user_role=user_role)


        rest.remove_all_replications()
        rest.remove_all_remote_clusters()

        remote_id = rest.add_remote_cluster(remote_server01.ip, 8091, 'Administrator', 'password', remote_cluster_name)
        time.sleep(20)
        replication_id = rest.start_replication('continuous', fromBucket='default', toCluster=remote_cluster_name, toBucket='default1')
        replication_id = replication_id.replace("/", "%2F")

        cancel_replication = {"cancel_XDCR":"controller/cancelXDCR/" + replication_id + ";POST"}
        result = self._return_http_code(cancel_replication, username, password, host=host, port=port, httpCode=httpCode, user_role=user_role)

        rest.remove_all_replications()
        rest.remove_all_remote_clusters()
        remote_id = rest.add_remote_cluster(remote_server01.ip, 8091, 'Administrator', 'password', remote_cluster_name)
        time.sleep(20)
        replication_id = rest.start_replication('continuous', fromBucket='default', toCluster=remote_cluster_name, toBucket='default1')
        replication_id = replication_id.replace("/", "%2F")

        cancel_replication = {"cancel_XDCR":"controller/cancelXDCR/" + replication_id + ";DELETE"}
        result = self._return_http_code(cancel_replication, username, password, host=host, port=port, httpCode=httpCode, user_role=user_role)

        rest.remove_all_replications()
        rest.remove_all_remote_clusters()
        rest_remote01.delete_bucket('default1')


    def cluster_bucket_xdcr_execute(self,username,password,host,port=8091,servers=None,cluster=None,httpCode=None,user_role=None):
        _cluster_bucket_xdcr_execute = {
            "replication_settings":"settings/replications/<id>;GET"
        }

        rest = RestConnection(servers[0])
        rest.remove_all_replications()
        rest.remove_all_remote_clusters()

        remote_cluster_name = 'rbac_cluster'
        rest = RestConnection(servers[0])
        remote_server01 = servers[1]
        remote_server02 = servers[2]
        rest_remote01 = RestConnection(remote_server01)
        rest_remote01.delete_bucket()
        rest_remote01.create_bucket(bucket='default', ramQuotaMB=100)
        rest_remote02 = RestConnection(remote_server02)
        remote_id = rest.add_remote_cluster(remote_server01.ip, 8091, 'Administrator', 'password', remote_cluster_name)
        time.sleep(20)
        replication_id = rest.start_replication('continuous', 'default', remote_cluster_name)
        replication_id = replication_id.replace("/", "%2F")

        cluster_bucket_xdcr_execute = {"replication_settings":"settings/replications/" + replication_id + ";GET"}
        result = self._return_http_code(cluster_bucket_xdcr_execute, username, password, host=host, port=port, httpCode=httpCode, user_role=user_role)

        rest.remove_all_replications()
        rest.remove_all_remote_clusters()
        rest_remote01.delete_bucket()


    def cluster_bucket_xdcr_read(self,username,password,host,port=8091,servers=None,cluster=None,httpCode=None,user_role=None):
        _cluster_bucket_xdcr_read = {
            "replication_settings":"settings/replications/<id>;GET"
        }

        rest = RestConnection(servers[0])
        rest.remove_all_replications()
        rest.remove_all_remote_clusters()

        remote_cluster_name = 'rbac_cluster'
        rest = RestConnection(servers[0])
        remote_server01 = servers[1]
        remote_server02 = servers[2]
        rest_remote01 = RestConnection(remote_server01)
        rest_remote01.delete_bucket()
        rest_remote01.create_bucket(bucket='default', ramQuotaMB=100)
        rest_remote02 = RestConnection(remote_server02)
        remote_id = rest.add_remote_cluster(remote_server01.ip, 8091, 'Administrator', 'password', remote_cluster_name)
        time.sleep(20)
        replication_id = rest.start_replication('continuous', 'default', remote_cluster_name)
        replication_id = replication_id.replace("/", "%2F")

        bucket_xdcr_read = {"replication_settings":"settings/replications/" + replication_id + ";GET"}
        result = self._return_http_code(bucket_xdcr_read, username, password, host=host, port=port, httpCode=httpCode, user_role=user_role)

        rest.remove_all_replications()
        rest.remove_all_remote_clusters()
        rest_remote01.delete_bucket()


    def cluster_bucket_password_read(self,username,password,host,port=8091,servers=None,cluster=None,httpCode=None,user_role=None):
        _cluster_bucket_xdcr_read = {
            "replication_settings":"settings/replications/<id>;GET"
        }


    def cluster_bucket_data_read(self,username,password,host,port=8091,servers=None,cluster=None,httpCode=None,user_role=None):
        log.info ("Into Cluster Bucket read data")


    def cluster_admin_internal_all(self,username,password,host,port=8091,servers=None,cluster=None,httpCode=None,user_role=None):
        _cluster_bucket_xdcr_read = {
            "replication_settings":"settings/replications/<id>;GET"
        }



    def cluster_xdcr_remote_clusters_read(self,username,password,host,port=8091, servers=None,cluster=None,httpCode=None,user_role=None):
        remote_cluster_name = 'rbac_cluster'
        rest = RestConnection(servers[0])
        remote_server01 = servers[1]
        remote_server02 = servers[2]
        rest_remote01 = RestConnection(remote_server01)
        rest_remote01.delete_bucket()
        rest_remote01.create_bucket(bucket='default', ramQuotaMB=100)
        rest_remote02 = RestConnection(remote_server02)

        #------ First Test the Get Requests for XDCR --------------#

        #Remove all remote cluster references
        rest.remove_all_replications()
        rest.remove_all_remote_clusters()

        #Add remote cluster reference and replications
        rest.add_remote_cluster(remote_server01.ip, 8091, 'Administrator', 'password', remote_cluster_name)
        time.sleep(20)
        replication_id = rest.start_replication('continuous', 'default', remote_cluster_name)

        _cluster_xdcr_remote_clusters_read ={
            "remove_cluser_read": "/pools/default/remoteClusters;GET",
        }

        result = self._return_http_code(_cluster_xdcr_remote_clusters_read, username, password, host=host, port=8091, httpCode=httpCode, user_role=user_role)

    def cluster_xdcr_remote_clusters_write(self,username,password,host,port=8091, servers=None,cluster=None,httpCode=None,user_role=None):

        rest = RestConnection(servers[0])
        rest.remove_all_replications()
        rest.remove_all_remote_clusters()

        _cluster_xdcr_remove_cluster_write = {
            "remoteClusters":"pools/default/remoteClusters;POST",
            "remote_cluster_id":"pools/default/remoteClusters/<id>;PUT",
            "delete_remote":"pools/default/remoteClusters/<id>;DELETE"
        }

        params = {'hostname': "{0}:{1}".format(servers[1].ip, servers[1].port),'username': 'Administrator','password': 'password','name':'rbac_remote01'}
        add_node = {"remoteClusters":"pools/default/remoteClusters;POST;" + str(params)}
        result = self._return_http_code(add_node, username, password, host=host, port=port, httpCode=httpCode, user_role=user_role)

        rest.remove_all_replications()
        rest.remove_all_remote_clusters()

        remote_cluster_name = 'rbac_cluster'
        rest = RestConnection(servers[0])
        remote_server01 = servers[1]
        remote_server02 = servers[2]
        rest_remote01 = RestConnection(remote_server01)
        rest_remote01.delete_bucket()
        rest_remote01.create_bucket(bucket='default', ramQuotaMB=100)
        rest_remote02 = RestConnection(remote_server02)

        rest.remove_all_replications()
        rest.remove_all_remote_clusters()
        remote_id = rest.add_remote_cluster(remote_server01.ip, 8091, 'Administrator', 'password', remote_cluster_name)
        time.sleep(20)
        delete_remote = {"delete_remote":"pools/default/remoteClusters/" + str(remote_cluster_name) + ";DELETE"}
        result = self._return_http_code(delete_remote, username, password, host=host, port=port, httpCode=httpCode, user_role=user_role)


    def cluster_xdcr_settings_read(self,username,password,host,port=8091,servers=None,cluster=None,httpCode=None,user_role=None):

        _cluster_xdcr_settings_read = {
            "replication_settings":"settings/replications;GET"
        }

        rest = RestConnection(servers[0])
        rest.remove_all_replications()
        rest.remove_all_remote_clusters()

        remote_cluster_name = 'rbac_cluster'
        rest = RestConnection(servers[0])
        remote_server01 = servers[1]
        remote_server02 = servers[2]
        rest_remote01 = RestConnection(remote_server01)
        rest_remote01.delete_bucket()
        rest_remote01.create_bucket(bucket='default', ramQuotaMB=100)
        rest_remote02 = RestConnection(remote_server02)
        remote_id = rest.add_remote_cluster(remote_server01.ip, 8091, 'Administrator', 'password', remote_cluster_name)
        time.sleep(20)
        replication_id = rest.start_replication('continuous', 'default', remote_cluster_name)
        result = self._return_http_code(_cluster_xdcr_settings_read, username, password, host=host, port=port, httpCode=httpCode, user_role=user_role)
        rest.remove_all_replications()
        rest.remove_all_remote_clusters()
        rest_remote01.delete_bucket()



    def cluster_xdcr_settings_write(self,username,password,host,port=8091,servers=None,cluster=None,httpCode=None,user_role=None):

        _cluster_xdcr_settings_read = {
            "replication_settings":"settings/replications;POST;{'httpConnections': 20}"
        }

        rest = RestConnection(servers[0])
        rest.remove_all_replications()
        rest.remove_all_remote_clusters()

        remote_cluster_name = 'rbac_cluster'
        rest = RestConnection(servers[0])
        remote_server01 = servers[1]
        remote_server02 = servers[2]
        rest_remote01 = RestConnection(remote_server01)
        rest_remote01.delete_bucket()
        rest_remote01.create_bucket(bucket='default', ramQuotaMB=100)
        rest_remote02 = RestConnection(remote_server02)
        rest_remote02.delete_bucket()
        remote_id = rest.add_remote_cluster(remote_server01.ip, 8091, 'Administrator', 'password', remote_cluster_name)
        time.sleep(20)
        replication_id = rest.start_replication('continuous', 'default', remote_cluster_name)

        result = self._return_http_code(_cluster_xdcr_settings_read, username, password, host=host, port=port, httpCode=httpCode, user_role=user_role)

        rest.remove_all_replications()
        rest.remove_all_remote_clusters()
        rest_remote01.delete_bucket()


    def cluster_admin_setup_write(self,username,password,host,port=8091, servers=None,cluster=None,httpCode=None,user_role=None):
        '''
        rest = RestConnection(servers[0])
        known_nodes = []
        known_nodes.append('ns_1@'+servers[0].ip)
        serv_out = 'ns_1@' + servers[1].ip
        for server in servers[1:]:
            rest.add_node('Administrator','password',server.ip)
            known_nodes.append('ns_1@' + server.ip)

        rest.rebalance(known_nodes)
        self.check_rebalance_complete(rest)
        rest.fail_over(serv_out,graceful=False)
        rest.rebalance(known_nodes,[serv_out])
        self.check_rebalance_complete(rest)
        '''

        _cluster_admin_setup_write = {
            #"engageCluster":"/engageCluster2;POST",
            #"completeJoin":"/completeJoin;POST",
            #"doJoinCluster":"/node/controller/doJoinCluster;POST;{'clusterMemberHostIp':192.168.46.101,'clusterMemberPort':8091,'user':'Administrator,'password':'password'}",

            #"doJoinClusterV2":"/node/controller/doJoinClusterV2;POST;{'hostname':'server02','user':user,'password':password}",
            ##"rename":"/node/controller/rename:POST;{'hostname':'server02','user':user,'password':password}",
            #"settings":"POST /nodes/controller/settings;POST;{'index_path':'/tmp/index'}",
            #"setupServices":"/node/controller/setupServices;POST;{'services':'','user':user,'password':password}",
            #"settings":"/settings/web;POST;{'username':'Administrator','password':'password','port':'8091'}"
        }

        '''
        for perm in _cluster_admin_setup_write:
            temp = _cluster_admin_setup_write[perm]
            temp = temp.replace('server01',servers[1].ip + ":" + servers[1].port)
            temp = temp.replace('server02',servers[2].ip + ":" + servers[2].port)
            temp = temp.replace('user',username)
            temp = temp.replace('password',password)
            _cluster_admin_setup_write[perm] = temp
        '''

        #result = self._return_http_code(_cluster_admin_setup_write,username,password,host=servers[1],port=port, httpCode=httpCode, user_role=user_role)
