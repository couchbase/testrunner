from membase.api.rest_client import RestConnection
import logger
log = logger.Logger.get_logger()
import time
from subprocess import Popen, PIPE
import json
import urllib2
import os
import stat
import subprocess
from remote.remote_util import RemoteMachineShellConnection
import socket
import ssl

class ntonencryptionBase:
    
    TEST_SSL_FILENAME = '/tmp/testssl.sh'
    OUTPUT_FILE = '/tmp/output.json'
    OUTPUT_FILE_LOG = '/tmp/output.log'
    PORTS_NSSERVER_NONSSL = [21100,21200,21300]
    PORTS_NSSERVER_SSL =[21150]
    PORTS_INDEXER = [9100,9101]
    #9102,9103,9104,9105
    PORTS_ANALYTICS = [9110]
    #9120,9110,9112,9111,9113,9115,9116,9117,9118,9119,9121
    PORTS_QUERY = [18093]
    PORTS_FTS = [18094]
    
    
    def ntonencryption_cli(self,servers,status,update_to_all=False):
        """
        Work on node-to-node-encryption cli
        -- enable - Enable node-to-node-encryption
        --disable - Disable node-to-node-encryption
        --get - Get settings for node-to-node-encryption
        """
        log.info('Changing node-to-node-encryption to {0}'.format(status))
        cli_command = 'node-to-node-encryption'
        options = ''
        if status == 'enable':
            options = '--enable'
        elif status == 'disable':
            options = '--disable'
        elif status == 'get':
            options == '--get'
        
        if update_to_all == False:
            remote_client = RemoteMachineShellConnection(servers[0])
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                        options=options, cluster_host="localhost", user='Administrator', password='password')
        else:
            for server in servers:
                remote_client = RemoteMachineShellConnection(server)
                output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                            options=options, cluster_host="localhost", user='Administrator', password='password')
        log.info("Output of node-to-node-encryption command is {0}".format(output))
        log.info("Error of node-to-node-encryption command is {0}".format(error))
        #To check for wildcard dns - Unable to switch on n2n if retries exceeded error occurs
        try:
            if("retries exceeded" in output[0]):
                return False
            else:
                return True
        except:
            pass
    def change_cluster_encryption_cli(self, servers, clusterEncryptionLevel,get_settings=False, update_to_all=False):
        """
        Work on setting-security for changing clsuterEncryption Level
        Change on 1 node or entire cluster
        """
        log.info ('Changing encryption Level - clusterEncryptionLevel = {0}'.format(clusterEncryptionLevel))
        options = ''
        cli_command = 'setting-security'
        if clusterEncryptionLevel == 'all':
            options = '--set --cluster-encryption-level all'
        elif clusterEncryptionLevel == 'control':
            options = '--set --cluster-encryption-level control'
        
        if get_settings == True:
            options = "--get"
        
        if update_to_all == False:
            remote_client = RemoteMachineShellConnection(servers[0])
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                        options=options, cluster_host="localhost", user='Administrator', password='password')
        else:
            for server in servers:
                remote_client = RemoteMachineShellConnection(server)
                output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                            options=options, cluster_host="localhost", user='Administrator', password='password')
        log.info("Output of setting-security command is {0}".format(output))
        log.info("Error of setting-security command is {0}".format(error))


    def setup_nton_cluster(self,servers,ntonStatus='enable',clusterEncryptionLevel='control'):
        """
        Main function to setup node to node encryption and also ClusterEncyprtionLevel
        """
        log.info ('Setting up node to node encryption - status = {0} and clusterEncryptionLevel = {1}'.format(ntonStatus,clusterEncryptionLevel))
        self.disable_autofailover(servers)
        result = self.ntonencryption_cli(servers,ntonStatus)
        self.change_cluster_encryption_cli(servers,clusterEncryptionLevel)
        return result
    def disable_nton_cluster(self,servers):
        """
        Main function to disable node to node encryption
        """
        log.info ('Disable up node to node encryption - status = disable and clusterEncryptionLevel = control')
        self.disable_autofailover(servers)
        self.change_cluster_encryption_cli(servers,'control',update_to_all=True)
        self.ntonencryption_cli(servers,'disable',update_to_all=True)
    
    '''def get_ntonencryption_status(self,servers):'''
    
    def setup_hostname(self,servers):
        cli_command = 'node-init'
        if self.hostname:
            for server in servers:
                options = '--node-init-hostname ' + sever.ip
                remote_client = RemoteMachineShellConnection(server)
                output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, \
                        options=options, cluster_host="localhost", user='Administrator', password='password') 
            
    
    def check_server_ports(self,servers):
        overall_result = []
        for node in servers:
            log.info("{0}".format(node))
            node_info = "{0}:{1}".format(node.ip, node.port)
            rest = RestConnection(node)
            #return_map = rest.get_nodes_services()
            node_services_list = rest.get_nodes_services()[node_info]
            
            for service in node_services_list:
                if service == 'kv':
                    node_ports = self.PORTS_NSSERVER_SSL
                elif service == 'cbas':
                    node_ports = self.PORTS_ANALYTICS
                elif service == 'index':
                    node_ports = self.PORTS_INDEXER
                elif service == 'n1ql':
                    node_ports = self.PORTS_QUERY
                elif service == 'fts':
                    node_ports = self.PORTS_FTS
            
                for node_port in node_ports:
                    #result = self.check_ssl_enabled(node.ip,node_port)
                    result = self.check_ssl_enabled_port(node.ip,node_port)
                    log.info(" --- Port being Tested --{0} ---- and service is ---{1} ------and result is -----{2}".format(node_port,service,result))
                    overall_result.append({'port':node_port,'result':result,'service':service, 'node':node.ip})
        return overall_result
                
    def disable_autofailover(self, servers):
        for server in servers:
            rest = RestConnection(server)
            rest.update_autofailover_settings(False, 120)
    
    def get_the_testssl_script(self, testssl_file_name):
            # get the testssl script
        attempts = 0
        #print 'getting test ssl', testssl_file_name

        while attempts < 1:
            try:
                response = urllib2.urlopen("http://testssl.sh/testssl.sh", timeout = 5)
                content = response.read()
                f = open(testssl_file_name, 'w' )
                f.write( content )
                f.close()
                break
            except urllib2.URLError as e:
                attempts += 1
                log.info("have an exception getting testssl {0}".format(e))


        st = os.stat(testssl_file_name)
        os.chmod(testssl_file_name, st.st_mode | stat.S_IEXEC | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    
    
    def check_ssl_enabled(self, host, port):
        
        try:
            self.get_the_testssl_script(self.TEST_SSL_FILENAME)
            
            p = subprocess.Popen('rm -rf ' + self.OUTPUT_FILE + " " + self.OUTPUT_FILE_LOG, shell=True)
            p.wait()
            cmd = self.TEST_SSL_FILENAME + " --warnings off --jsonfile-pretty " + self.OUTPUT_FILE + " --logfile " + self.OUTPUT_FILE_LOG + " " + host + ":" + str(port)
            p = subprocess.Popen(cmd, shell=True)
            p.wait()
            with open(self.OUTPUT_FILE) as json_file:
                data = json.load(json_file)
                for p in data['scanResult']:
                    id = p['serverDefaults'][0]['id']
                    value = p['serverDefaults'][0]['finding']
            
            if (id == 'TLS_extensions')and (value == '(none)') and "doesn't seem to be a TLS/SSL enabled server" in open(self.OUTPUT_FILE_LOG).read():
                return False
            else:
                return True
        except:
            log.info ("Issue with file parsing")
            return False
        
        '''
        if "doesn't seem to be a TLS/SSL enabled server" in open(self.OUTPUT_FILE_LOG).read():
            print "Is not a SSL enabled"
        else:
                print "SSl Enabled"
        
                print id
        print value
        if value == ('(none)'):
            print 'value is none'
        '''
    
    def check_ssl_enabled_port(self,host,port,ssl_enabled=True):
        result = None
        try:
            cert = ssl.get_server_certificate((host,port))
            result = True
        except:
            result = False
        
        if result == False and ssl_enabled == False:
            result = True
        
        return result
            
    def validate_results(self, servers, results, clusterLevelEncryption,ntonStatus='enable'):
        finalResult = True
        for node in servers:
            node_info = "{0}:{1}".format(node.ip, node.port)
            log.info(node_info)
            log.info(ntonStatus)
            rest = RestConnection(node)
            #return_map = rest.get_nodes_services()
            node_services_list = rest.get_nodes_services()[node_info]
            
            for service in node_services_list:
                if service == 'kv':
                    node_ports = self.PORTS_NSSERVER_SSL
                elif service == 'cbas':
                    node_ports = self.PORTS_ANALYTICS
                elif service == 'index':
                    node_ports = self.PORTS_INDEXER
                elif service == 'n1ql':
                    node_ports = self.PORTS_QUERY
                elif service == 'fts':
                    node_ports = self.PORTS_FTS
            
                if ntonStatus == 'enable':
                    for node_port in node_ports:
                        for result in results:
                            if node_port == result['port']:
                                if clusterLevelEncryption == 'all' and result['service'] in ['cbas','kv','index','n1ql','fts']:
                                    if result['result'] == False:
                                        finalResult = False
                                elif clusterLevelEncryption == 'control' and result['service'] in ['cbas','index']:
                                    if result['result'] == True:
                                        finalResult = False
                                elif clusterLevelEncryption == 'control' and result['service'] in ['n1ql','fts']:
                                    if result['result'] == False:
                                        finalResult = False
                                elif clusterLevelEncryption == 'control' and result['service'] in ['kv']:
                                    if result['result'] == False:
                                        finalResult = False
                            log.info("Value of {0} result {1} finalResult {2} port {3}".format(result['service'],result['result'],finalResult,result['port']))
                else:
                    for node_port in node_ports:
                        for result in results:
                            if node_port == result['port']:
                                if result['service'] in ['cbas','kv','index']:
                                    if result['result'] is not False:
                                        finalResult = False
                                elif result['service'] in ['n1ql','fts']:
                                    if result['result'] is False:
                                        finalResult = False
                                log.info("Value of {0} result {1} finalResult {2} port {3}".format(result['service'],result['result'],finalResult,result['port']))
        return finalResult