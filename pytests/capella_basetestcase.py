from couchbase_helper.data_analysis_helper import DataAnalysisResultAnalyzer, DataAnalyzer, DataCollector
from lib.Cb_constants.CBServer import CbServer
from basetestcase import OnPremBaseTestCase
import copy
from cluster_config import ClusterConfig
from membase.helper.bucket_helper import BucketOperationHelper
from lib.capella.internal_api import capella_utils as CapellaAPI
from lib.capella.internal_api import pod, tenant
from TestInput import TestInputServer, TestInputSingleton
from couchbase_helper.cluster import Cluster
import logger


class BaseTestCase(OnPremBaseTestCase):
    def setUp(self):
        self.input = TestInputSingleton.input
        self.parse_params()
        self.nodes_init = self.input.param("nodes_init", 3)

        self.log = logger.Logger.get_logger()
        if self.log_level:
            self.log.setLevel(level=self.log_level)
    
        self.use_https = True
        CbServer.use_https = True
        if not hasattr(self, 'cluster'):
            self.cluster = Cluster()
        self.buckets = []
        self.collection_name = {}
        CbServer.rest_username = self.input.membase_settings.rest_username
        CbServer.rest_password = self.input.membase_settings.rest_password
        self.data_collector = DataCollector()
        self.data_analyzer = DataAnalyzer()
        self.result_analyzer = DataAnalysisResultAnalyzer()
        self.set_testrunner_client()

        self.pod = pod(self.input.capella["pod"])
        self.tenant = tenant(
            self.input.capella["tenant_id"],
            self.input.capella["capella_user"],
            self.input.capella["capella_pwd"],
        )
        self.project_id = self.input.capella["project_id"]
        self.tenant.project_id = self.project_id
        
        cluster_details = self.create_capella_config()

        cluster_id, _, _ = CapellaAPI.create_cluster(self.pod, self.tenant, cluster_details)

        self.cluster_id = cluster_id
        CbServer.cluster_id = cluster_id

        CapellaAPI.add_allowed_ip(self.pod, self.tenant, self.cluster_id)
        CapellaAPI.create_db_user(self.pod, self.tenant, self.cluster_id, self.input.membase_settings.rest_username, self.input.membase_settings.rest_password)

        CbServer.pod = self.pod
        CbServer.tenant = self.tenant
        CbServer.cluster_id = self.cluster_id

        servers = CapellaAPI.get_nodes_formatted(
            self.pod, self.tenant, self.cluster_id, self.input.membase_settings.rest_username, self.input.membase_settings.rest_password)

        self.cluster_config = ClusterConfig(self.input.membase_settings.rest_username, self.input.membase_settings.rest_password, self.create_input_servers())
        self.cluster_config.update_servers(servers)

        if not self.skip_buckets_handle:
            BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)

        self.bucket_base_params = {'membase': {}}
    
        shared_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                   replicas=self.num_replicas,
                                                   enable_replica_index=self.enable_replica_index,
                                                   eviction_policy=self.eviction_policy, bucket_priority=None,
                                                   lww=self.lww, maxttl=self.maxttl,
                                                   compression_mode=self.compression_mode)

        membase_params = copy.deepcopy(shared_params)
        membase_params['bucket_type'] = 'membase'
        self.bucket_base_params['membase']['non_ephemeral'] = membase_params

        membase_ephemeral_params = copy.deepcopy(shared_params)
        membase_ephemeral_params['bucket_type'] = 'ephemeral'
        self.bucket_base_params['membase']['ephemeral'] = membase_ephemeral_params

        self.bucket_base_params['membase']['non_ephemeral']['size'] = self.bucket_size
        self.bucket_base_params['membase']['ephemeral']['size'] = self.bucket_size

        self._bucket_creation()

    def tearDown(self):
        # cluster was created during setup so destroy it
        CapellaAPI.destroy_cluster(self.pod, self.tenant, self.cluster_id)

    def create_capella_config(self):
        services_count = {}
        for service in self.services_init.split("-"):
            service = ",".join(sorted(service.split(":")))
            if service in services_count:
                services_count[service] += 1
            else:
                services_count[service] = 1

        config = CapellaAPI.create_capella_config(self.input, services_count)

        return config

    # Use services_init and services_in to create a list of servers
    # Fallback to self.input.servers if services_init is not specified
    def create_input_servers(self):
        servers = []
        if self.services_init:
            for services in self.services_init.split("-"):
                services = ",".join(services.split(":"))
                server = TestInputServer()
                server.services = services
                servers.append(server)
            if self.services_in:
                for services in self.services_in.split("-"):
                    services = ",".join(services.split(":"))
                    server = TestInputServer()
                    server.services = services
                    servers.append(server)
        else:
            servers = self.input.servers
        return servers

    @property
    def servers(self):
        return self.cluster_config.servers

    @property
    def master(self):
        return self.cluster_config.master

    @servers.setter
    def servers(self, servers):
        self.cluster_config.servers = servers

    @master.setter
    def master(self, master):
        self.cluster_config.master = master

    @property
    def cbas_node(self):
        return self.cluster_config.cbas_node

    @cbas_node.setter
    def cbas_node(self, cbas_node):
        self.cluster_config.cbas_node = cbas_node