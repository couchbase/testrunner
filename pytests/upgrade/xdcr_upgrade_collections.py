import queue
from .newupgradebasetest import NewUpgradeBaseTest
from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper
from couchbase_helper.documentgenerator import BlobGenerator
from membase.api.rest_client import RestConnection, RestHelper
from security.rbac_base import RbacBase
from threading import Thread
from collection.collections_cli_client import CollectionsCLI
from collection.collections_rest_client import CollectionsRest
from collection.collections_stats import CollectionsStats


class XDCRUpgradeCollectionsTests(NewUpgradeBaseTest):
    def setUp(self):
        super(XDCRUpgradeCollectionsTests, self).setUp()
        self.nodes_init = self.input.param('nodes_init', 2)
        self.queue = queue.Queue()
        self.rate_limit = self.input.param("rate_limit", 100000)
        self.batch_size = self.input.param("batch_size", 1000)
        self.doc_size = self.input.param("doc_size", 100)
        self.loader = self.input.param("loader", "high_doc_ops")
        self.instances = self.input.param("instances", 4)
        self.threads = self.input.param("threads", 5)
        self.use_replica_to = self.input.param("use_replica_to", False)
        self.index_name_prefix = None
        self.rest_src = RestConnection(self.servers[0])

    def tearDown(self):
        super(XDCRUpgradeCollectionsTests, self).tearDown()

    def enable_migration_mode(self, src_bucket, dest_bucket):
        setting_val_map = {"collectionsMigrationMode": "true",
                           "colMappingRules": '{"REGEXP_CONTAINS(META().id,\'0$\')":"scope1.mycollection_scope1"}'
                           }
        self.rest_src.set_xdcr_params(src_bucket, dest_bucket, setting_val_map)

    def verify_doc_counts(self):
        des_master = self.servers[self.nodes_init]
        src_cbver = RestConnection(self.master).get_nodes_version()
        des_cbver = RestConnection(des_master).get_nodes_version()
        src_items = RestConnection(self.master).get_buckets_itemCount()
        des_items = RestConnection(des_master).get_buckets_itemCount()
        if src_cbver[:3] < "7.0" and des_cbver[:3] >= "7.0":
            des_items = self.get_col_item_count(des_master, "default", "_default",
                                                "_default", self.des_stat_col)
            if src_items["default"] != des_items:
                self.fail("items do not match. src: {0} != des: {1}"
                          .format(src_items["default"], des_items))
        elif src_cbver[:3] >= "7.0" and des_cbver[:3] < "7.0":
            src_items = self.get_col_item_count(self.master, "default", "_default",
                                                "_default", self.stat_col)
            if src_items != des_items["default"]:
                self.fail("items do not match. src: {0} != des: {1}"
                          .format(src_items, des_items["default"]))
        elif src_cbver[:3] >= "7.0" and des_cbver[:3] >= "7.0":
            if src_items["default"] != des_items["default"]:
                self.fail("items do not match. src: {0} != des: {1}"
                          .format(src_items["default"], des_items["default"]))

    def test_xdcr_upgrade_with_services(self):
        after_upgrade_services_in = self.input.param("after_upgrade_services_in", False)
        after_upgrade_buckets_in = self.input.param("after_upgrade_buckets_in", False)
        after_upgrade_buckets_out = self.input.param("after_upgrade_buckets_out", False)
        after_upgrade_buckets_flush = self.input.param("after_upgrade_buckets_flush", False)

        # Install initial version on the specified nodes
        self._install(self.servers[:self.nodes_init])
        # Configure the nodes with services on cluster1
        self.operations(self.servers[:self.nodes_init], services="kv,kv")
        # get the n1ql node which will be used in pre,during and post upgrade for running n1ql commands
        self.n1ql_server = self.get_nodes_from_services_map(service_type="n1ql")
        # Run the pre upgrade operations, typically creating index
        self.pre_upgrade(self.servers[:self.nodes_init])
        if self.input.param("ddocs_num", 0) > 0:
            self.create_ddocs_and_views()

        self._install(self.servers[self.nodes_init:self.num_servers])
        self.master = self.servers[self.nodes_init]
        # Configure the nodes with services on the other cluster2
        try:
            self.operations(self.servers[self.nodes_init:self.num_servers], services="kv,kv")
            self.sleep(timeout=10)
        except Exception as ex:
            if ex:
                print("error: ", str(ex))
            self.log.info("bucket is created")

        # create a xdcr relationship between cluster1 and cluster2
        self.rest_src.add_remote_cluster(self.servers[self.nodes_init].ip,
                                    self.servers[self.nodes_init].port,
                                    'Administrator', 'password', "C2")

        repl_id = self.rest_src.start_replication('continuous', 'default', "C2")
        if repl_id is not None:
            self.log.info("Replication created successfully")
        # Run the post_upgrade operations
        self._create_ephemeral_buckets()
        self.post_upgrade(self.servers[:self.nodes_init])
        # Add new services after the upgrade
        for upgrade_version in self.upgrade_versions:
            src_nodes = self.servers[:self.nodes_init]
            for server in src_nodes:
                remote = RemoteMachineShellConnection(server)
                remote.stop_server()
            src_upgrade_threads = self._async_update(upgrade_version, src_nodes)

            for upgrade_thread in src_upgrade_threads:
                upgrade_thread.join()
            src_success_upgrade = True
            while not self.queue.empty():
                src_success_upgrade &= self.queue.get()
            if not src_success_upgrade:
                self.fail("Upgrade failed in source cluster. See logs above!")
            else:
                self.log.info("Upgrade source cluster success")

            des_nodes = self.servers[self.nodes_init:self.num_servers]
            self.master = self.servers[self.nodes_init]
            for server in des_nodes:
                remote = RemoteMachineShellConnection(server)
                remote.stop_server()
            des_upgrade_threads = self._async_update(upgrade_version, des_nodes)
            for upgrade_thread in des_upgrade_threads:
                upgrade_thread.join()
            des_success_upgrade = True
            while not self.queue.empty():
                des_success_upgrade &= self.queue.get()
            if not des_success_upgrade:
                self.fail("Upgrade failed in des cluster. See logs above!")
            else:
                self.log.info("Upgrade des cluster success")

        self.master = self.servers[0]
        self.rest = RestConnection(self.master)
        self.rest_col = CollectionsRest(self.master)
        self.cli_col = CollectionsCLI(self.master)
        self.stat_col = CollectionsStats(self.master)
        self.log.info("Create scope collection at src cluster")
        #self.rest_col.create_scope_collection_count()
        self.sleep(10)

        self.des_rest = RestConnection(self.servers[self.nodes_init])
        self.des_rest_col = CollectionsRest(self.servers[self.nodes_init])
        self.des_cli_col = CollectionsCLI(self.servers[self.nodes_init])
        self.des_stat_col = CollectionsStats(self.servers[self.nodes_init])
        self.log.info("Create scope collection at des cluster")
        self.buckets = RestConnection(self.servers[self.nodes_init]).get_buckets()
        self._create_scope_collection(self.rest_col, self.cli_col, self.buckets[0].name)
        self._create_scope_collection(self.des_rest_col, self.des_cli_col, self.buckets[0].name)
        self.load_to_collections_bucket()

        self.enable_migration = self.input.param("enable_migration", False)
        if self.enable_migration:
            self.enable_migration_mode(self.buckets[0].name, self.buckets[0].name)
        self.verify_doc_counts()

        if after_upgrade_buckets_in is not False:
            self.bucket_size = 256
            self._create_sasl_buckets(self.master, 1)
            self._create_standard_buckets(self.master, 1)
            if self.input.param("ddocs_num", 0) > 0:
                self.create_ddocs_and_views()
                gen_load = BlobGenerator('upgrade', 'upgrade-', self.value_size,
                                         end=self.num_items)
                self._load_all_buckets(self.master, gen_load, "create", self.expire_time,
                                       flag=self.item_flag)

        # deleting buckets after upgrade
        if after_upgrade_buckets_out is not False:
            self._all_buckets_delete(self.master)
        # flushing buckets after upgrade
        if after_upgrade_buckets_flush is not False:
            self._all_buckets_flush()

    def run_view_queries(self):
        view_query_thread = Thread(target=self.view_queries, name="run_queries",
                                   args=(self.run_view_query_iterations,))
        return view_query_thread

    def view_queries(self, iterations):
        query = {"connectionTimeout": 60000}
        for count in range(iterations):
            for i in range(self.view_num):
                self.cluster.query_view(self.master, self.ddocs[0].name,
                                        self.default_view_name + str(i), query,
                                        expected_rows=None, bucket="default", retry_time=2)

    def create_user(self, node):
        self.log.info("inside create_user")
        testuser = [{'id': 'cbadminbucket', 'name': 'cbadminbucket',
                     'password': 'password'}]
        rolelist = [{'id': 'cbadminbucket', 'name': 'cbadminbucket',
                     'roles': 'admin'}]
        self.log.info("before create_user_source")
        RbacBase().create_user_source(testuser, 'builtin', node)
        self.log.info("before add_user_role")
        RbacBase().add_user_role(rolelist, RestConnection(node), 'builtin')
