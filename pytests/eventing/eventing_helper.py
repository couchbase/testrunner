import logging,os,json

from TestInput import TestInputServer
from eventing.eventing_constants import EXPORTED_FUNCTION
from lib.membase.api.rest_client import RestConnection
import time

from remote.remote_util import RemoteMachineShellConnection

log = logging.getLogger()

class EventingHelper:
    '''
    __author__      = "vikas chaudhary"
    __email__       = "vikas.chaudhary@couchbase.com"

    The Eventing class to call when trying to anything with eventing
    outside the purview of EventingBaseTest class

    Prerequisites:
    Following buckets needs to be present to deploy handlers
    src_bucket,metadata,dst_bucket,dst_bucket1,source_bucket_mutation,dst_bucket_curl

    Sample usage:

        from eventing_helper import EventingHelper
        event=EventingHelper(servers=self.servers,master=self.master)
        event.deploy_bucket_op_function()
        event.verify_documents_in_destination_bucket('test_import_function_1',1,'dst_bucket')
        event.undeploy_bucket_op_function()
        event.deploy_curl_function()
        event.verify_documents_in_destination_bucket('bucket_op_curl', 1, 'dst_bucket_curl')
        event.undeploy_curl_function()
        event.deploy_sbm_function()
        event.verify_documents_in_destination_bucket('bucket_op_sbm', 1, 'source_bucket_mutation')
        event.undeploy_sbm_function()
    '''
    def __init__(self,servers,master):
        if servers == None or master == None:
            return
        self.servers=servers
        self.master=master
        self.eventing_nodes= self.get_nodes_from_services_map(service_type="eventing",servers=servers,master=master,get_all_nodes=True)
        self.eventing_rest=RestConnection(self.eventing_nodes[0])

    def get_handler_json(self,function):
        # read the exported function
        script_dir = os.path.dirname(__file__)
        abs_file_path = os.path.join(script_dir, function)
        fh = open(abs_file_path, "r")
        body = json.loads(fh.read())
        return body

    '''
        deploy any handler passed as json sample handler pytests/eventing/exported_functions/bucket_op.json
    '''
    def __deploy_function(self,function):
        body = self.get_handler_json(function)
        self.deploy_function(body)
        self.name=body['appname']
        self.wait_for_handler_state(self.name,"deployed")

    '''
        undeploy handler , accept handler name which need
        Parameters
        ----------
        name : str
            The name of the handler
    '''
    def __undeploy_function(self,name,wait_for_undeployment=True):
        self.eventing_rest.undeploy_function(name)
        log.info("Undeploy Application : {0}".format(name))
        if wait_for_undeployment:
            self.wait_for_handler_state(name, "undeployed")

    '''
        delete handler , accept handler name which need
        Parameters
        ----------
        name : str
            The name of the handler
    '''
    def delete_funtion(self,name):
        content1 = self.eventing_rest.delete_single_function(name)
        log.info("Delete Application : {0}".format(name))


    '''
        Wrapper method to deploy bucket op
        Needs 3 buckets 'src_bucket','metadata','dst_bucket'
        Implemented in Vulcan(> 5.5)
    '''
    def deploy_bucket_op_function(self):
        bucket=['src_bucket','metadata','dst_bucket']
        self.check_for_buckets(bucket)
        self.__deploy_function(EXPORTED_FUNCTION.BUCKET_OP)

    '''
        Wrapper method to undeploy bucket op
    '''
    def undeploy_bucket_op_function(self):
        self.__undeploy_function('test_import_function_1')

    '''
        Wrapper method to deploy bucket op with timers
        Needs 3 buckets 'src_bucket','metadata','dst_bucket1'
        Implemented in Vulcan(> 6.0)
    '''
    def deploy_timer_function(self):
        bucket = ["src_bucket", "metadata", "dst_bucket1"]
        self.check_for_buckets(bucket)
        self.__deploy_function(EXPORTED_FUNCTION.BUCKET_OP_WITH_TIMER)

    '''
        Wrapper method to undeploy bucket op with timers
    '''
    def undeploy_timer_function(self):
        self.__undeploy_function('test_import_function_2')

    '''
        Wrapper method to deploy bucket op with timers
        Needs 3 buckets 'source_bucket_mutation','metadata'
        Implemented in Vulcan(> 6.5)
    '''
    def deploy_sbm_function(self):
        bucket = ["source_bucket_mutation", "metadata"]
        self.check_for_buckets(bucket)
        self.__deploy_function(EXPORTED_FUNCTION.SBM_BUCKET_OP)

    '''
        Wrapper method to undeploy source bucket op
    '''
    def undeploy_sbm_function(self):
        self.__undeploy_function('bucket_op_sbm')

    '''
        Wrapper method to deploy curl handler
        Needs 3 buckets 'src_bucket','metadata','dst_bucket_curl'
        Implemented in Vulcan(> 6.5)
    '''
    def deploy_curl_function(self):
        bucket = ["src_bucket", "metadata", "dst_bucket_curl"]
        self.check_for_buckets(bucket)
        self.__deploy_function(EXPORTED_FUNCTION.CURL_BUCKET_OP)

    '''
        Wrapper method to undeploy curl handler
    '''
    def undeploy_curl_function(self):
        self.__undeploy_function('bucket_op_curl')

    def check_for_buckets(self,bucket):
        buckets=self.eventing_rest.get_buckets()
        bucket_list=[]
        for b in buckets:
            bucket_list.append(b.name)
        for exp_bucket in bucket:
            if exp_bucket in bucket_list:
                pass
            else:
                raise Exception("Following buckets {} not in the cluster {}".format(exp_bucket,bucket_list))


    '''
        Verify method to check number of documents in destination bucket
        Destination bucket for various handlers
        BUCKET_OP : dst_bucket
        BUCKET_OP_WITH_TIMER : dst_bucket1
        CURL_BUCKET_OP : dst_bucket_curl
        SBM_BUCKET_OP : source_bucket_mutation
        
        Parameters
        ----------
        name : str
            The name of handler deployed
        expected_dcp_mutations : int
            Number of documents needs to be in destination bucket(equals to the number of documents in source bucket
            but double for source bucket handler)
        bucket : str
            Destination bucket for handlers 
            Mapping:
                BUCKET_OP : dst_bucket
                BUCKET_OP_WITH_TIMER : dst_bucket1
                CURL_BUCKET_OP : dst_bucket_curl
                SBM_BUCKET_OP : source_bucket_mutation
        
    '''
    def verify_documents_in_destination_bucket(self,name,expected_dcp_mutations,bucket,timeout=600):
        # wait for bucket operations to complete and verify it went through successfully
        count = 0
        stats_dst = self.eventing_rest.get_bucket_stats(bucket)
        if bucket == "source_bucket_mutation":
            expected_dcp_mutations=expected_dcp_mutations*2
        while stats_dst["curr_items"] != expected_dcp_mutations and count < 20:
            message = "Waiting for handler code {2} to complete bucket operations... Current : {0} Expected : {1}".\
                      format(stats_dst["curr_items"], expected_dcp_mutations,name)
            self.sleep(timeout/20, message=message)
            curr_items=stats_dst["curr_items"]
            stats_dst = self.eventing_rest.get_bucket_stats(bucket)
            if curr_items == stats_dst["curr_items"]:
                count += 1
        if stats_dst["curr_items"] != expected_dcp_mutations:
            total_dcp_backlog = 0
            timers_in_past = 0
            lcb = {}
            # TODO : Use the following stats in a meaningful way going forward. Just printing them for debugging.
            for eventing_node in self.eventing_nodes:
                rest_conn = RestConnection(eventing_node)
                out = rest_conn.get_all_eventing_stats()
                total_dcp_backlog += out[0]["events_remaining"]["dcp_backlog"]
                if "TIMERS_IN_PAST" in out[0]["event_processing_stats"]:
                    timers_in_past += out[0]["event_processing_stats"]["TIMERS_IN_PAST"]
                total_lcb_exceptions= out[0]["lcb_exception_stats"]
                host=eventing_node.ip
                lcb[host]=total_lcb_exceptions
                full_out = rest_conn.get_all_eventing_stats(seqs_processed=True)
                log.info("Stats for Node {0} is \n{1} ".format(eventing_node.ip, json.dumps(out, sort_keys=True,
                                                                                          indent=4)))
                log.debug("Full Stats for Node {0} is \n{1} ".format(eventing_node.ip, json.dumps(full_out,
                                                                                                sort_keys=True,
                                                                                                indent=4)))
            raise Exception(
                "Bucket operations from handler code took lot of time to complete or didn't go through. Current : {0} "
                "Expected : {1}  dcp_backlog : {2}  TIMERS_IN_PAST : {3} lcb_exceptions : {4}".format(stats_dst["curr_items"],
                                                                                 expected_dcp_mutations,
                                                                                 total_dcp_backlog,
                                                                                 timers_in_past,lcb))
        log.info("Final docs count... Current : {0} Expected : {1}".
                 format(stats_dst["curr_items"], expected_dcp_mutations))

    def deploy_function(self, body, deployment_fail=False, wait_for_bootstrap=True,print_eventing_handler_code_in_logs=False):
        body['settings']['deployment_status'] = True
        body['settings']['processing_status'] = True
        if print_eventing_handler_code_in_logs:
            log.info("Deploying the following handler code : {0} with {1}".format(body['appname'],body['depcfg']))
            log.info("\n{0}".format(body['appcode']))
        content1 = self.eventing_rest.create_function(body['appname'], body)
        log.info("deploy Application : {0}".format(content1))
        if deployment_fail:
            res = json.loads(content1)
            if not res["compile_success"]:
                return
            else:
                raise Exception("Deployment is expected to be failed but no message of failure")
        if wait_for_bootstrap:
            # wait for the function to come out of bootstrap state
            self.wait_for_handler_state(body['appname'], "deployed")

    def wait_for_handler_state(self, name,status,iterations=20):
        self.sleep(20, message="Waiting for {} to {}...".format(name,status))
        result = self.eventing_rest.get_composite_eventing_status()
        count = 0
        composite_status = None
        while composite_status != status and count < iterations:
            self.sleep(20,"Waiting for {} to {}...".format(name,status))
            result = self.eventing_rest.get_composite_eventing_status()
            for i in range(len(result['apps'])):
                if result['apps'][i]['name'] == name:
                    composite_status = result['apps'][i]['composite_status']
            count+=1
        if count == iterations:
            raise Exception('Eventing took lot of time for handler {} to {}'.format(name,status))

    def undeploy_function(self, name,wait_for_undeployment=True):
        content = self.eventing_rest.undeploy_function(name)
        log.info("Undeploy Application : {0}".format(name))
        if wait_for_undeployment:
            self.wait_for_handler_state(name,"undeployed")
        return content

    def sleep(self, timeout=15, message=""):
        log.info("sleep for {0} secs. {1} ...".format(timeout, message))
        time.sleep(timeout)

    def pause_function(self, body,wait_for_pause=True):
        body['settings']['deployment_status'] = True
        body['settings']['processing_status'] = False
        content1 = self.eventing_rest.set_settings_for_function(body['appname'], body['settings'])
        log.info("Pause Application : {0}".format(body['appname']))
        if wait_for_pause:
            self.wait_for_handler_state(body['appname'], "paused")

    def resume_function(self, body,wait_for_resume=True):
        body['settings']['deployment_status'] = True
        body['settings']['processing_status'] = True
        if "dcp_stream_boundary" in body['settings']:
            body['settings'].pop('dcp_stream_boundary')
        log.info("Settings after deleting dcp_stream_boundary : {0}".format(body['settings']))
        # undeploy the function
        content1 = self.eventing_rest.set_settings_for_function(body['appname'], body['settings'])
        log.info("Resume Application : {0}".format(body['appname']))
        if wait_for_resume:
            self.wait_for_handler_state(body['appname'], "deployed")

    def is_function_deployed(self,handler_name):
        result = self.eventing_rest.get_composite_eventing_status()
        if handler_name in result:
            return
        else:
            raise Exception("{} not deployed".format(handler_name))


    def get_nodes_from_services_map(self, service_type="n1ql", get_all_nodes=False,
                                    servers=None, master=None):
        if not servers:
            servers = self.servers
        if not master:
            master = self.master
        self.get_services_map(master=master)
        if (service_type not in self.services_map):
            log.info("cannot find service node {0} in cluster " \
                          .format(service_type))
        else:
            list = []
            for server_info in self.services_map[service_type]:
                tokens = server_info.rsplit(":", 1)
                ip = tokens[0]
                port = int(tokens[1])
                for server in servers:
                    """ In tests use hostname, if IP in ini file use IP, we need
                        to convert it to hostname to compare it with hostname
                        in cluster """
                    if "couchbase.com" in ip and "couchbase.com" not in server.ip:
                        shell = RemoteMachineShellConnection(server)
                        hostname = shell.get_full_hostname()
                        log.info("convert IP: {0} to hostname: {1}" \
                                      .format(server.ip, hostname))
                        server.ip = hostname
                        shell.disconnect()
                    elif ip.endswith(".svc"):
                        from kubernetes import client as kubeClient, config as kubeConfig
                        currNamespace = ip.split('.')[2]
                        kubeConfig.load_incluster_config()
                        v1 = kubeClient.CoreV1Api()
                        nodeList = v1.list_pod_for_all_namespaces(watch=False)

                        for node in nodeList.items:
                            if node.metadata.namespace == currNamespace and \
                               node.status.pod_ip == server.ip:
                                ip = node.status.pod_ip
                                break
                    elif "couchbase.com" in server.ip and "couchbase.com" not in ip:
                        node = TestInputServer()
                        node.ip = ip
                        """ match node.ip to server in ini file to get correct credential """
                        for server in servers:
                            shell = RemoteMachineShellConnection(server)
                            ips = shell.get_ip_address()
                            if node.ip in ips:
                                node.ssh_username = server.ssh_username
                                node.ssh_password = server.ssh_password
                                break

                        shell = RemoteMachineShellConnection(node)
                        hostname = shell.get_full_hostname()
                        log.info("convert IP: {0} to hostname: {1}" \
                                      .format(ip, hostname))
                        ip = hostname
                        shell.disconnect()
                    if (port != 8091 and port == int(server.port)) or \
                            (port == 8091 and server.ip.lower() == ip.lower()):
                        list.append(server)
            log.info("list of {0} nodes in cluster: {1}".format(service_type, list))
            if get_all_nodes:
                return list
            else:
                try:
                    if len(list)==0:
                        list.append(servers[0])
                    return list[0]
                except IndexError as e:
                    log.info(self.services_map)
                    raise e


    def get_services_map(self, reset=True, master=None):
        if not reset:
            return
        else:
            self.services_map = {}
        if not master:
            master = self.master
        rest = RestConnection(master)
        map = rest.get_nodes_services()
        for key, val in map.iteritems():
            for service in val:
                if service not in self.services_map.keys():
                    self.services_map[service] = []
                self.services_map[service].append(key)