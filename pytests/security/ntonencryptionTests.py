from membase.api.rest_client import RestConnection, RestHelper
import urllib.request, urllib.parse, urllib.error
import json
from remote.remote_util import RemoteMachineShellConnection
import subprocess
import socket
import fileinput
import sys
from subprocess import Popen, PIPE
from basetestcase import BaseTestCase
from lib.testconstants import STANDARD_BUCKET_PORT
from couchbase_helper.documentgenerator import BlobGenerator
import time
import time, os
from security.ntonencryptionBase import ntonencryptionBase
from lib.couchbase_helper.tuq_helper import N1QLHelper
from cbas.cbas_base import *
# from fts.fts_callable import *
import logger
from .x509main import x509main
from remote.remote_util import RemoteMachineShellConnection


class ntonencryptionTest(BaseTestCase):

    def setUp(self):
        super(ntonencryptionTest, self).setUp()
        self.bucket_list = RestConnection(self.master).get_buckets()
        self.shell = RemoteMachineShellConnection(self.master)
        self.item_flag = self.input.param("item_flag", 4042322160)
        self.full_docs_list = ''
        self.n1ql_port = 8093
        self.enable_nton_local = self.input.param('enable_nton_local',False)
        self.local_clusterEncryption = self.input.param('local_clusterEncryption','control')
        self.hostname = self.input.param('hostname',False)
        self.x509enable = self.input.param("x509enable",False)
        self.wildcard_dns = self.input.param("wildcard_dns",None)
        if self.ntonencrypt == 'enable' and not self.x509enable:
            ntonencryptionBase().setup_nton_cluster(self.servers,clusterEncryptionLevel=self.ntonencrypt_level)
    
    def tearDown(self):
        super(ntonencryptionTest, self).tearDown()
        ntonencryptionBase().disable_nton_cluster(self.servers)
        self._reset_original()

    def _reset_original(self, servers=None):
        if servers is None:
            servers = self.servers
        self.log.info ("Reverting to original state - regenerating certificate and removing inbox folder")
        tmp_path = "/tmp/abcd.pem"
        for servers in servers:
            cli_command = "ssl-manage"
            remote_client = RemoteMachineShellConnection(servers)
            options = "--regenerate-cert={0}".format(tmp_path)
            output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options,
                                                                cluster_host=servers.ip, user="Administrator",
                                                                password="password")
            x509main(servers)._delete_inbox_folder()
        
    
    def perform_doc_ops_in_all_cb_buckets(self, num_items, operation, start_key=0, end_key=1000):
        """
        Create/Update/Delete docs in all cb buckets
        :param num_items: No. of items to be created/deleted/updated
        :param operation: String - "create","update","delete"
        :param start_key: Doc Key to start the operation with
        :param end_key: Doc Key to end the operation with
        :return:
        """
        try:
            age = list(range(70))
            first = ['james', 'sharon', 'dave', 'bill', 'mike', 'steve']
            template = '{{ "number": {0}, "first_name": "{1}" , "mutated":0}}'
            gen_load = DocumentGenerator('test_docs', template, age, first,
                                         start=start_key, end=end_key)
            self.log.info("%s %s documents..." % (operation, num_items))
            try:
                self._load_all_buckets(self.master, gen_load, operation, 0)
                #self._verify_stats_all_buckets(self.input.servers)
            except Exception as e:
                self.log.info("Exception is {0}".format(e))
        except:
            raise Exception("Error while loading document")
    
    def create_fts_index_query_compare(self, index_queue=None):
        """
        Call before upgrade
        1. creates a default index, one per bucket
        2. Loads fts json data
        3. Runs queries and compares the results against ElasticSearch
        """
        self.log.info("Create FTS index query")
        try:
            self.fts_obj = FTSCallable(nodes=self.servers, es_validate=False)
            for bucket in self.buckets:
                self.fts_obj.create_default_index(
                    index_name="index_{0}".format(bucket.name),
                    bucket_name=bucket.name)
            self.fts_obj.load_data(self.num_items)
            self.fts_obj.wait_for_indexing_complete()
            for index in self.fts_obj.fts_indexes:
                self.fts_obj.run_query_and_compare(index=index, num_queries=20)
            return self.fts_obj
        except Exception as ex:
            self.log.info("Exception is {0}".format(ex))
    
    def create_cbas_index_query(self):
        '''
        1. Create CBAS index and query the statement'
        '''
        
        try:
            self.sleep(10)
            self.log.info("Creating CBAS Index")
            self.cbas_node = self.get_nodes_from_services_map(service_type="cbas")
            self.log.info( "Creating CBAS Index")
            cbas_rest = RestConnection(self.cbas_node)       
            self.log.info( "Creating CBAS Index")
            dataset_stmt = 'create dataset dataset_1 on default'
            content = cbas_rest.execute_statement_on_cbas(dataset_stmt,None)
            self.log.info( "Creating CBAS Index")
            connect_stmt = 'CONNECT LINK Local;'
            content = cbas_rest.execute_statement_on_cbas(connect_stmt,None)
            self.log.info( "Creating CBAS Index")
            query_stmt = 'SELECT VALUE COUNT(*) FROM dataset_1;'
            content = cbas_rest.execute_statement_on_cbas(query_stmt,None)
            self.log.info( "Creating CBAS Index")
        except Exception as ex:
            self.log.info("Exception is {0}".format(ex))
            raise Exception ("Exception in CBAS node")
            
    
    def create_secondary_index_query(self, index_field, index_query=None,step='before'):
        '''
        1. Create a index
        2. Run query for the index
        '''
        
        try:
            self.n1ql_node = self.get_nodes_from_services_map(service_type="n1ql")
            if self.n1ql_node is not None:
                self.n1ql_helper = N1QLHelper(shell=self.shell,
                                                  max_verify=self.max_verify,
                                                  buckets=self.buckets,
                                                  item_flag=self.item_flag,
                                                  n1ql_port=self.n1ql_port,
                                                  full_docs_list=self.full_docs_list,
                                                  log=self.log, input=self.input,
                                                  master=self.n1ql_node,
                                                  use_rest=True
                                                  )
                query = "Create index " + index_field + " on default(" + index_field + ")"
                self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_node)
                if step == 'before':
                    query = 'create primary index on default'
                    self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_node)
                self.perform_doc_ops_in_all_cb_buckets(2000,'create',end_key=2000)
                self.sleep(10)
                query = "select * from default where " + index_field + " is not NULL"
                self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_node)
                if index_query is not None:
                    query = "select * from default where " + index_query + " is not NULL"
                    self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_node)
                    query = "select * from default"
                    self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_node)
        except:
            raise Exception("Error while creating index/n1ql setup")
    
    def check_all_services(self, servers):
        
        cbas_node = self.get_nodes_from_services_map(service_type="cbas")
        if cbas_node is not None:
            self.create_cbas_index_query()
        
        if self.ntonencrypt == 'disable' and self.enable_nton_local == True:
            self.create_secondary_index_query('id')
            self.create_secondary_index_query('name','id','after')
        else:
            self.create_secondary_index_query('name')
        #ntonencryptionBase().get_ntonencryption_status(self.servers)
        
        if self.get_nodes_from_services_map(service_type="fts") is not None:
            self.create_fts_index_query_compare()
        
        self.perform_doc_ops_in_all_cb_buckets(2000,'create',end_key=2000)
        
    
    def data_rebalance_in(self):
        '''
        1. Enable encryption on all nodes 
        2. Add node to current cluster
        '''
        services_in = []
        self.perform_doc_ops_in_all_cb_buckets(100,'create')
        time.sleep(30)

        for service in self.services_in.split("-"):
            services_in.append(service.split(":")[0])

        servs_inout =  self.servers[self.nodes_init:]
        self.log.info("{0}".format(servs_inout))
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], servs_inout, [],
                                                 services=services_in)
        rebalance.result()
        if self.ntonencrypt == 'disable' and self.enable_nton_local == True:
            ntonencryptionBase().setup_nton_cluster(self.servers,'enable',self.local_clusterEncryption)
        #ntonencryptionBase().    (self.servers)
        final_result = ntonencryptionBase().check_server_ports(self.servers)
        result = ntonencryptionBase().validate_results(self.servers, final_result, self.ntonencrypt_level)
        self.log.info ("Final result is {0}".format(final_result))
        self.assertTrue(result)
     
    def index_rebalance_in(self):
        self.perform_doc_ops_in_all_cb_buckets(1000,'create',end_key=1000)
        time.sleep(10)
        services_in = []
        self.log.info ("list of services to be added {0}".format(self.services_in))
        
        for service in self.services_in.split("-"):
            services_in.append(service.split(":")[0])
        self.log.info ("list of services to be added after formatting {0}".format(services_in))
        
        # add nodes to the cluster
        servs_inout =  self.servers[self.nodes_init:]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], servs_inout, [],
                                                     services=services_in)
        rebalance.result()
        if self.ntonencrypt == 'disable' and self.enable_nton_local == True:
            self.create_secondary_index_query('id')
            ntonencryptionBase().setup_nton_cluster(self.servers,'enable',self.local_clusterEncryption)
            time.sleep(10)
            self.create_secondary_index_query('name','id','after')
        else:
            self.create_secondary_index_query('name')
        #ntonencryptionBase().get_ntonencryption_status(self.servers)
        self.perform_doc_ops_in_all_cb_buckets(2000,'create',end_key=2000)
        final_result = ntonencryptionBase().check_server_ports(self.servers)
        if self.ntonencrypt == 'disable' and self.enable_nton_local == True:
            result = ntonencryptionBase().validate_results(self.servers, final_result, self.local_clusterEncryption)
        else:
            result = ntonencryptionBase().validate_results(self.servers, final_result, self.ntonencrypt_level)
        self.log.info ("Final result is {0}".format(final_result))
        self.assertTrue(result)
    
    def cbas_rebalance_in(self):
        services_in = []
        self.perform_doc_ops_in_all_cb_buckets(10000,'create',end_key=10000)
        #time.sleep(30)
        
        for service in self.services_in.split("-"):
            services_in.append(service.split(":")[0])
        self.log.info ("list of services to be added after formatting {0}".format(services_in))
        
        servs_inout = self.servers[self.nodes_init:]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], servs_inout, [],
                                                 services=services_in)
        rebalance.result()
        if self.ntonencrypt == 'disable' and self.enable_nton_local == True:
            ntonencryptionBase().setup_nton_cluster(self.servers,'enable',self.local_clusterEncryption)
        self.create_cbas_index_query()
        self.perform_doc_ops_in_all_cb_buckets(2000,'create',end_key=2000)
        final_result = ntonencryptionBase().check_server_ports(self.servers)
        if self.ntonencrypt == 'disable' and self.enable_nton_local == True:
            result = ntonencryptionBase().validate_results(self.servers, final_result, self.local_clusterEncryption)
        else:
            result = ntonencryptionBase().validate_results(self.servers, final_result, self.ntonencrypt_level)
        self.log.info ("Final result is {0}".format(final_result))
        self.assertTrue(result)
            
              
    def fts_rebalance_in(self):
        services_in = []
        self.perform_doc_ops_in_all_cb_buckets(10000,'create')
        time.sleep(30)
        
        for service in self.services_in.split("-"):
            services_in.append(service.split(":")[0])
        self.log.info ("list of services to be added after formatting {0}".format(services_in))
        
        servs_inout = self.servers[self.nodes_init:]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], servs_inout, [],
                                                 services=services_in)
        rebalance.result()
        if self.ntonencrypt == 'disable' and self.enable_nton_local == True:
            ntonencryptionBase().setup_nton_cluster(self.servers,'enable',self.local_clusterEncryption)
        self.create_fts_index_query_compare()
        self.sleep(10)
        #ntonencryptionBase().get_ntonencryption_status(self.servers)
        final_result = ntonencryptionBase().check_server_ports(self.servers)
        if self.ntonencrypt == 'disable' and self.enable_nton_local == True:
            result = ntonencryptionBase().validate_results(self.servers, final_result, self.local_clusterEncryption)
        else:
            result = ntonencryptionBase().validate_results(self.servers, final_result, self.ntonencrypt_level)
        self.log.info ("Final result is {0}".format(final_result))
        self.assertTrue(result)
        
        
    def all_services_rebalance_in(self):
        self.perform_doc_ops_in_all_cb_buckets(1000,'create',end_key=1000)
        time.sleep(30)
        services_in = []
        self.log.info ("list of services to be added {0}".format(self.services_in))
        
        for service in self.services_in.split("-"):
            services_in.append(service.split(":")[0])
        self.log.info ("list of services to be added after formatting {0}".format(services_in))
        
        # add nodes to the cluster
        servs_inout =  self.servers[self.nodes_init:]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], servs_inout, [],
                                                     services=services_in)
        rebalance.result()
        if self.ntonencrypt == 'disable' and self.enable_nton_local == True:
            ntonencryptionBase().setup_nton_cluster(self.servers,'enable',self.local_clusterEncryption)

        #ntonencryptionBase().get_ntonencryption_status(self.servers)
        cbas_node = self.get_nodes_from_services_map(service_type="cbas")
        if cbas_node is not None:
            self.create_cbas_index_query()
        
        if self.ntonencrypt == 'disable' and self.enable_nton_local == True:
            self.create_secondary_index_query('id')
            self.create_secondary_index_query('name','id','after')
        else:
            self.create_secondary_index_query('name')
        #ntonencryptionBase().get_ntonencryption_status(self.servers)
        
        if self.get_nodes_from_services_map(service_type="fts") is not None:
            self.create_fts_index_query_compare()
        
        self.perform_doc_ops_in_all_cb_buckets(2000,'create',end_key=2000)
        
        #ntonencryptionBase().get_ntonencryption_status(self.servers)
        final_result = ntonencryptionBase().check_server_ports(self.servers)
        if self.ntonencrypt == 'disable' and self.enable_nton_local == True:
            result = ntonencryptionBase().validate_results(self.servers, final_result, self.local_clusterEncryption)
        else:
            result = ntonencryptionBase().validate_results(self.servers, final_result, self.ntonencrypt_level)
        self.log.info ("Final result is {0}".format(final_result))
        self.assertTrue(result)
    
    
    def all_rebalance_in_disable(self):
        self.perform_doc_ops_in_all_cb_buckets(10000,'create')
        time.sleep(30)
        servs_inout = self.servers[self.nodes_init:]
        services_in = []
        for service in self.services_in.split("-"):
            services_in.append(service.split(":")[0])
        self.log.info ("list of services to be added after formatting {0}".format(services_in))
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], servs_inout, [],
                                                 services=services_in)
        rebalance.result()
        if self.ntonencrypt == 'disable' and self.enable_nton_local == True:
            ntonencryptionBase().setup_nton_cluster(self.servers,'enable',self.local_clusterEncryption)
        #ntonencryptionBase().get_cluster_nton_status(self.master)   
        final_result = ntonencryptionBase().check_server_ports(self.servers)
        ntonencryptionBase().ntonencryption_cli(self.servers, 'disable')
        final_result = ntonencryptionBase().check_server_ports(self.servers)
        if self.ntonencrypt == 'disable' and self.enable_nton_local == True:
            result = ntonencryptionBase().validate_results(self.servers, final_result, self.local_clusterEncryption,'disable')
        else:
            result = ntonencryptionBase().validate_results(self.servers, final_result, self.ntonencrypt_level,'disable')
        self.log.info ("Final result is {0}".format(final_result))
        self.assertTrue(result)
    
    def test_add_nodes_x509_rebalance(self):
        servs_inout = self.servers[self.nodes_init:]
        services_in = []
        rest = RestConnection(self.master)
        copy_servers = copy.deepcopy(self.servers)
        self.log.info( 'before cert generate')
        x509main(self.master)._generate_cert(copy_servers, type='openssl', encryption='', key_length=1024, client_ip='172.16.1.174', alt_names='non_default', dns=None, uri=None,wildcard_dns=self.wildcard_dns)
        x509main(self.master).setup_master()
        for server in servs_inout:
            x509main(server).setup_master()
        x509main().setup_cluster_nodes_ssl(servs_inout, reload_cert=True)



        for service in self.services_in.split("-"):
            services_in.append(service.split(":")[0])
        self.log.info ("list of services to be added after formatting {0}".format(services_in))
        
        # add nodes to the cluster
        servs_inout = self.servers[1:4]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], servs_inout, [],
                                                     services=services_in)
        rebalance.result()
        #Check if n2n can be enabled after adding x509 certificates
        if self.x509enable and self.ntonencrypt=='enable':
            encryption_result = ntonencryptionBase().setup_nton_cluster(self.servers,'enable',self.ntonencrypt_level)
            self.assertTrue(encryption_result,"Retries Exceeded. Cannot enable n2n encryption")
        self.check_all_services(self.servers)
        
        #ntonencryptionBase().get_ntonencryption_status(self.servers)
        #ntonencryptionBase().get_cluster_nton_status(self.master)
        final_result = ntonencryptionBase().check_server_ports(self.servers)
        result = ntonencryptionBase().validate_results(self.servers, final_result, self.ntonencrypt_level)
        self.assertTrue(result,'Issue with results with x509 enable for sets')
        ntonencryptionBase().change_cluster_encryption_cli(self.servers, 'control')
        ntonencryptionBase().ntonencryption_cli(self.servers, 'disable')
        final_result_disable = ntonencryptionBase().check_server_ports(self.servers)
        self.log.info("{0}".format(final_result_disable))
        result = ntonencryptionBase().validate_results(self.servers, final_result_disable, self.ntonencrypt_level,'disable')
        self.assertTrue(result,'Issue with results with x509 disable for sets')
        
        
    
    def test_init_nodes_x509(self):
        servs_inout = self.servers[1:4]
        rest = RestConnection(self.master)
        copy_servers = copy.deepcopy(self.servers)
        self.log.info( 'before cert generate')
        if self.x509enable:
            x509main(self.master)._generate_cert(copy_servers, type='openssl', encryption='', key_length=1024, client_ip='172.16.1.174', alt_names='non_default', dns=None, uri=None,wildcard_dns=self.wildcard_dns)
            x509main(self.master).setup_master()
            x509main().setup_cluster_nodes_ssl(servs_inout,True)
        
        if self.ntonencrypt == 'disable' and self.enable_nton_local == True:
            ntonencryptionBase().setup_nton_cluster(self.servers,'enable',self.local_clusterEncryption)
        # Check if n2n can be enabled after adding x509 certificates
        elif self.x509enable and self.ntonencrypt=='enable':
            encryption_result = ntonencryptionBase().setup_nton_cluster(self.servers,'enable',self.ntonencrypt_level)
            self.assertTrue(encryption_result,"Retries Exceeded. Cannot enable n2n encryption")
        self.check_all_services(self.servers)
        
        #ntonencryptionBase().get_ntonencryption_status(self.servers)
        #ntonencryptionBase().get_cluster_nton_status(self.master)
        final_result = ntonencryptionBase().check_server_ports(self.servers)
        self.log.info("{0}".format(final_result))
        result = ntonencryptionBase().validate_results(self.servers, final_result, self.ntonencrypt_level)
        self.assertTrue(result,'Issue with results with x509 enable for sets')
        ntonencryptionBase().change_cluster_encryption_cli(self.servers, 'control')
        ntonencryptionBase().ntonencryption_cli(self.servers, 'disable')
        final_result_disable = ntonencryptionBase().check_server_ports(self.servers)
        result = ntonencryptionBase().validate_results(self.servers, final_result_disable, self.ntonencrypt_level,'disable')
        self.assertTrue(result,'Issue with results with x509 disable for sets')

    
    def test_add_nodes_x509_rebalance_rotate(self):
        servs_inout = self.servers[self.nodes_init:]
        services_in = []
        rest = RestConnection(self.master)
        copy_servers = copy.deepcopy(self.servers)
        self.log.info( 'before cert generate')
        x509main(self.master)._generate_cert(copy_servers, type='openssl', encryption='', key_length=1024, client_ip='172.16.1.174', alt_names='non_default', dns=None, uri=None,wildcard_dns=self.wildcard_dns)
        x509main(self.master).setup_master()
        for server in servs_inout:
            x509main(server).setup_master()
        x509main().setup_cluster_nodes_ssl(servs_inout, reload_cert=True)
        for service in self.services_in.split("-"):
            services_in.append(service.split(":")[0])
        self.log.info ("list of services to be added after formatting {0}".format(services_in))

        # add nodes to the cluster
        servs_inout = self.servers[1:4]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], servs_inout, [],
                                                     services=services_in)
        rebalance.result()

        #Check if n2n can be enabled after adding x509 certificates
        if self.x509enable and self.ntonencrypt == 'enable':
            encryption_result = ntonencryptionBase().setup_nton_cluster(self.servers, 'enable', self.ntonencrypt_level)
            self.assertTrue(encryption_result,"Retries Exceeded. Cannot enable n2n encryption")


        x509main(self.master)._delete_inbox_folder()
        x509main(self.master)._generate_cert(self.servers, root_cn="CB\ Authority", type='openssl', client_ip='172.16.1.174',wildcard_dns=self.wildcard_dns)
        ntonencryptionBase().change_cluster_encryption_cli(self.servers, 'control')
        ntonencryptionBase().ntonencryption_cli(self.servers, 'disable')
        x509main(self.master).setup_master()
        x509main().setup_cluster_nodes_ssl(self.servers, reload_cert=True)
        ntonencryptionBase().ntonencryption_cli(self.servers, 'enable')
        ntonencryptionBase().change_cluster_encryption_cli(self.servers,self.ntonencrypt_level)
        
        
        self.check_all_services(self.servers)
        
        #ntonencryptionBase().get_ntonencryption_status(self.servers)
        #ntonencryptionBase().get_cluster_nton_status(self.master)
        final_result = ntonencryptionBase().check_server_ports(self.servers)
        result = ntonencryptionBase().validate_results(self.servers, final_result, self.ntonencrypt_level)
        self.assertTrue(result,'Issue with results with x509 enable for sets')
        ntonencryptionBase().change_cluster_encryption_cli(self.servers, 'control')
        ntonencryptionBase().ntonencryption_cli(self.servers, 'disable')
        final_result_disable = ntonencryptionBase().check_server_ports(self.servers)
        self.log.info("{0}".format(final_result_disable))
        result = ntonencryptionBase().validate_results(self.servers, final_result_disable, self.ntonencrypt_level,'disable')
        self.assertTrue(result,'Issue with results with x509 disable for sets')
        
    
    def test_add_nodes_x509_rebalance_rotate_disable(self):
        servs_inout = self.servers[self.nodes_init:]
        services_in = []
        rest = RestConnection(self.master)
        copy_servers = copy.deepcopy(self.servers)
        self.log.info( 'before cert generate')
        x509main(self.master)._generate_cert(copy_servers, type='openssl', encryption='', key_length=1024, client_ip='172.16.1.174', alt_names='non_default', dns=None, uri=None,wildcard_dns=self.wildcard_dns)
        x509main(self.master).setup_master()
        for server in servs_inout:
            x509main(server).setup_master()
        x509main().setup_cluster_nodes_ssl(servs_inout, reload_cert=True)
        for service in self.services_in.split("-"):
            services_in.append(service.split(":")[0])
        self.log.info ("list of services to be added after formatting {0}".format(services_in))
        
        # add nodes to the cluster
        servs_inout = self.servers[1:4]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], servs_inout, [],
                                                     services=services_in)
        rebalance.result()

        #Check if n2n can be enabled after adding x509 certificates
        if self.x509enable and self.ntonencrypt == 'enable':
            encryption_result = ntonencryptionBase().setup_nton_cluster(self.servers, 'enable', self.ntonencrypt_level)
            self.assertTrue(encryption_result,"Retries Exceeded. Cannot enable n2n encryption")

        ntonencryptionBase().change_cluster_encryption_cli(self.servers, 'control')
        ntonencryptionBase().ntonencryption_cli(self.servers, 'disable')
        
        x509main(self.master)._delete_inbox_folder()
        x509main(self.master)._generate_cert(self.servers, root_cn="CB\ Authority", type='openssl', client_ip='172.16.1.174',wildcard_dns=self.wildcard_dns)
        x509main(self.master).setup_master()
        x509main().setup_cluster_nodes_ssl(self.servers, reload_cert=True)
        
        ntonencryptionBase().ntonencryption_cli(self.servers, 'enable')
        ntonencryptionBase().change_cluster_encryption_cli(self.servers, self.ntonencrypt_level)
        
        self.check_all_services(self.servers)
        
        #ntonencryptionBase().get_ntonencryption_status(self.servers)
        #ntonencryptionBase().get_cluster_nton_status(self.master)
        final_result = ntonencryptionBase().check_server_ports(self.servers)
        result = ntonencryptionBase().validate_results(self.servers, final_result, self.ntonencrypt_level)
        self.assertTrue(result,'Issue with results with x509 enable for sets')
        ntonencryptionBase().change_cluster_encryption_cli(self.servers, 'control')
        ntonencryptionBase().ntonencryption_cli(self.servers, 'disable')
        final_result_disable = ntonencryptionBase().check_server_ports(self.servers)
        self.log.info("{0}".format(final_result_disable))
        result = ntonencryptionBase().validate_results(self.servers, final_result_disable, self.ntonencrypt_level,'disable')
        self.assertTrue(result,'Issue with results with x509 disable for sets')
