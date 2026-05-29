import json, logging, os, queue, time
from threading import Thread

from eventing.eventing_base import EventingBaseTest
from membase.api.rest_client import RestHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from lib.testconstants import STANDARD_BUCKET_PORT
from lib.membase.api.rest_client import RestConnection
from membase.api.exception import RebalanceFailedException
from pytests.security.ntonencryptionBase import ntonencryptionBase
from pytests.security.jwt_utils import JWTUtils
from pytests.fts.fts_callable import FTSCallable
from lib.Cb_constants.CBServer import CbServer

from upgrade.newupgradebasetest import NewUpgradeBaseTest

log = logging.getLogger()

class EventingUpgrade(NewUpgradeBaseTest, EventingBaseTest):
    def setUp(self):
        """Initialize upgrade test: REST connections, bucket names, handler tracking, FTS setup."""
        log.info("==============  EventingUpgrade setup has started ==============")
        super(EventingUpgrade, self).setUp()
        self.init_nodes = False
        self.rest = RestConnection(self.master)
        self.server = self.master
        self.queue = queue.Queue()
        self.src_bucket_name = self.input.param('src_bucket_name', 'src_bucket')
        self.eventing_log_level = self.input.param('eventing_log_level', 'INFO')
        self.dst_bucket_name = self.input.param('dst_bucket_name', 'dst_bucket')
        self.dst_bucket_name1 = self.input.param('dst_bucket_name1', 'dst_bucket1')
        self.dst_bucket_curl = self.input.param('dst_bucket_curl', 'dst_bucket_curl')
        self.source_bucket_mutation = self.input.param('source_bucket_mutation', 'source_bucket_mutation')
        self.metadata_bucket_name = self.input.param('metadata_bucket_name', 'metadata')
        self.n1ql_op_dst = self.input.param('n1ql_op_dst', 'n1ql_op_dst')
        self.analytics_dst = self.input.param('analytics_dst', 'analytics_dst')
        self.fts_dst = self.input.param('fts_dst', 'fts_dst')
        self.during_upgrade_src_bucket_name = self.input.param('during_upgrade_src_bucket_name', 'during_upgrade_src_bucket')
        self.during_upgrade_dst_bucket_name = self.input.param('during_upgrade_dst_bucket_name', 'during_upgrade_dst_bucket')
        self.gens_load = self.generate_docs(self.docs_per_day)
        self.upgrade_version = self.input.param("upgrade_version")
        #self.exported_handler_version = self.input.param("exported_handler_version", '6.6.1')
        self.enable_n2n_encryption_and_tls = self.input.param("enable_n2n_encryption_and_tls", False)
        self.failover_type = self.input.param("failover_type", "hard")
        self.recovery_type = self.input.param("recovery_type", "delta")
        self.rebalance_settle_time = self.input.param("sleep_before_rebalance", 60)
        # FTS setup
        self.fts_index_name = self.input.param("fts_index_name", "travel_sample_test")
        self.fts_doc_count = self.input.param("fts_doc_count", 31500)
        self.fts_index = None
        self.fts_callable = FTSCallable(nodes=self.servers, es_validate=False)
        # Handler tracking — populated during deploy, used by pause/resume helpers
        self.all_handler_names = []
        log.info("==============  EventingUpgrade setup has completed ==============")

    def tearDown(self):
        """Clean up FTS index, analytics link, then delegate to parent tearDown."""
        log.info("==============  EventingUpgrade tearDown has started ==============")
        try:
            if getattr(self, 'fts_index', None):
                self.fts_callable.delete_fts_index(self.fts_index_name)
                self.fts_index = None
        except Exception:
            log.exception("FTS index cleanup failed")
        # Clean up analytics link
        try:
            cbas_node = self.get_nodes_from_services_map(service_type="cbas")
            if cbas_node:
                cbas_rest = RestConnection(cbas_node)
                cbas_rest.execute_statement_on_cbas("DISCONNECT LINK Local", None)
        except Exception:
            log.exception("Analytics teardown cleanup failed")
        super(EventingUpgrade, self).tearDown()
        log.info("==============  EventingUpgrade tearDown has completed ==============")

    ###########################################################################
    # Offline upgrade test
    ###########################################################################

    def test_offline_upgrade_with_eventing(self):
        """
        Offline upgrade: stop all nodes, install new version in-place, restart, verify eventing.
        """
        # Pre-upgrade: install old version, set up cluster, deploy handlers
        self._install_and_setup_cluster()
        self._create_upgrade_buckets()
        self._load_pre_upgrade_data()
        self._deploy_pre_upgrade_handlers()
        self.print_eventing_stats_from_all_eventing_nodes()
        # Upgrade all nodes offline
        self._perform_offline_upgrade()
        self._verify_cluster_services_after_upgrade()
        # Post-upgrade infra
        self._enable_tls_if_configured()
        self._verify_pre_upgrade_handlers_survived()
        # Re-assert memory quotas after upgrade — new version may auto-detect different values
        self._set_memory_quotas()
        # Post-upgrade features: collections, FTS, analytics, new handlers
        self._create_collections_on_all_buckets()
        self._setup_fts_index()
        self._setup_analytics()
        self._deploy_post_upgrade_handlers()
        # Validation cycles
        self._run_full_mutation_cycle()
        self._run_pause_resume_cycle()
        self._verify_jwt_auth()
        self._verify_rbac()
        self.print_eventing_stats_from_all_eventing_nodes()
        self.skip_metabucket_check = True

    ###########################################################################
    # Online upgrade test — regular rebalance
    ###########################################################################

    def test_online_upgrade_with_regular_rebalance_with_eventing(self):
        """Online upgrade via regular rebalance: swap old nodes for new-version nodes, verify eventing."""
        # Pre-upgrade: install old + new versions, set up cluster, deploy handlers
        self._install_and_setup_online_cluster()
        self._install_travel_sample()
        self._create_upgrade_buckets()
        self._load_pre_upgrade_data()
        self._deploy_pre_upgrade_handlers()
        self.print_eventing_stats_from_all_eventing_nodes()
        # Online upgrade: rebalance in new nodes, rebalance out old nodes
        self._perform_online_upgrade_regular_rebalance()
        # Post-upgrade infra
        self._enable_tls_if_configured()
        self._verify_pre_upgrade_handlers_survived()
        # Re-assert memory quotas after upgrade — new nodes may have different auto-detected values
        self._set_memory_quotas()
        # Post-upgrade features: collections, FTS, analytics, new handlers
        self._create_collections_on_all_buckets()
        self._setup_fts_index()
        self._setup_analytics()
        self._deploy_post_upgrade_handlers()
        # Validation cycles
        self._run_full_mutation_cycle()
        self._run_pause_resume_cycle()
        self._verify_jwt_auth()
        self._verify_rbac()
        self.print_eventing_stats_from_all_eventing_nodes()
        self.skip_metabucket_check = True

    ###########################################################################
    # Online upgrade test — swap rebalance
    ###########################################################################

    def test_online_upgrade_with_swap_rebalance_with_eventing(self):
        """Online upgrade via swap rebalance: replace nodes one-by-one, verify eventing."""
        # Pre-upgrade: install old + new versions, set up cluster, deploy handlers
        self._install_and_setup_online_cluster()
        self._create_upgrade_buckets()
        self._load_pre_upgrade_data()
        self._deploy_pre_upgrade_handlers()
        self.print_eventing_stats_from_all_eventing_nodes()
        # Online upgrade: swap rebalance nodes one at a time
        self._perform_online_upgrade_swap_rebalance()
        # Post-upgrade infra
        self._enable_tls_if_configured()
        self._verify_pre_upgrade_handlers_survived()
        # Re-assert memory quotas after upgrade — swapped nodes may have different auto-detected values
        self._set_memory_quotas()
        # Post-upgrade features: collections, travel-sample, FTS, analytics, new handlers
        self._create_collections_on_all_buckets()
        self._load_travel_sample_post_upgrade()
        self._setup_fts_index()
        self._setup_analytics()
        self._deploy_post_upgrade_handlers()
        # Validation cycles
        self._run_full_mutation_cycle()
        self._run_pause_resume_cycle()
        self._verify_jwt_auth()
        self._verify_rbac()
        self.print_eventing_stats_from_all_eventing_nodes()
        self.skip_metabucket_check = True

    ###########################################################################
    # Online upgrade test — failover + rebalance / recovery
    ###########################################################################

    def test_online_upgrade_with_failover_recovery_rebalance_with_eventing(self):
        """Online upgrade that fails the eventing node over, per recovery_type:

        - "out": rebalances the failed node OUT and a new-version node IN
          (plain failover, no recovery).
        - "delta"/"full": adds the node back with delta/full recovery, then
          swaps it for a new-version node.

        Fails over per failover_type (hard/graceful) in all cases.
        """
        # Pre-upgrade: install old + new versions, set up cluster, deploy handlers
        self._install_and_setup_online_cluster()
        self._create_upgrade_buckets()
        self._load_pre_upgrade_data()
        self._deploy_pre_upgrade_handlers()
        self.print_eventing_stats_from_all_eventing_nodes()
        # Online upgrade: failover eventing node, recover it, then swap to new version
        self._perform_online_upgrade_with_failover(recovery_type=self.recovery_type)
        # Post-upgrade infra
        self._enable_tls_if_configured()
        self._verify_pre_upgrade_handlers_survived()
        self._set_memory_quotas()
        # Post-upgrade features: collections, travel-sample, FTS, analytics, new handlers
        self._create_collections_on_all_buckets()
        self._load_travel_sample_post_upgrade()
        self._setup_fts_index()
        self._setup_analytics()
        self._deploy_post_upgrade_handlers()
        # Validation cycles
        self._run_full_mutation_cycle()
        self._run_pause_resume_cycle()
        self._verify_jwt_auth()
        self._verify_rbac()
        self.print_eventing_stats_from_all_eventing_nodes()
        self.skip_metabucket_check = True

    ###########################################################################
    # Common Helper Methods — shared by offline and online upgrade tests
    ###########################################################################

    def _set_memory_quotas(self):
        """Set explicit service memory quotas right after cluster setup, before
        any buckets exist, so auto-detected quotas don't over-commit RAM on small
        test machines (KV=1500, Index=256, FTS=3000, CBAS=1024, Eventing=512)."""
        rest = RestConnection(self.master)
        # Delete default bucket if it exists (e.g., when run without default_bucket=False)
        try:
            if rest.get_bucket_by_name("default"):
                rest.delete_bucket("default")
                self.sleep(5, "Waiting for default bucket deletion before setting memory quota")
        except Exception:
            pass
        quotas = {
            'memoryQuota': 1500,
            'indexMemoryQuota': 256,
            'ftsMemoryQuota': 3000,
            'cbasMemoryQuota': 1024,
            'eventingMemoryQuota': 512,
        }
        for service, mb in quotas.items():
            log.info("Setting {0} = {1}MB".format(service, mb))
            ok = rest.set_service_memoryQuota(service=service, memoryQuota=mb)
            if not ok:
                log.error("FAILED to set {0} to {1}MB — API returned False".format(service, mb))
            else:
                log.info("Successfully set {0} = {1}MB".format(service, mb))
        log.info("Service memory quotas set: KV=1500, Index=256, FTS=3000, CBAS=1024, Eventing=512")

    def _create_upgrade_buckets(self):
        """Create all buckets needed by the upgrade test (quotas already set)."""
        self.bucket_size = 100
        log.info("Creating the required buckets in the initial version")
        bucket_params = self._create_bucket_params(server=self.server, size=self.bucket_size,
                                                   replicas=self.num_replicas,
                                                   bucket_storage='couchstore')
        bucket_names = [
            self.src_bucket_name,
            self.dst_bucket_name,
            self.metadata_bucket_name,
            self.dst_bucket_name1,
            self.dst_bucket_curl,
            self.source_bucket_mutation,
            self.n1ql_op_dst,
            self.analytics_dst,
            self.fts_dst,
            self.during_upgrade_src_bucket_name,
            self.during_upgrade_dst_bucket_name,
        ]
        for i, name in enumerate(bucket_names):
            self.cluster.create_standard_bucket(name=name, port=STANDARD_BUCKET_PORT + 1 + i,
                                                bucket_params=bucket_params)
            self.sleep(30)
        self.buckets = RestConnection(self.master).get_buckets()
        self.src_bucket = [b for b in self.buckets if b.name == self.src_bucket_name]
        if not self.src_bucket:
            self.fail("src_bucket '{0}' not found in cluster buckets: {1}".format(
                self.src_bucket_name, [b.name for b in self.buckets]))
        self.during_upgrade_src_bucket = [b for b in self.buckets if b.name == self.during_upgrade_src_bucket_name]
        if not self.during_upgrade_src_bucket:
            self.fail("during_upgrade_src_bucket '{0}' not found in cluster buckets: {1}".format(
                self.during_upgrade_src_bucket_name, [b.name for b in self.buckets]))

    def _load_pre_upgrade_data(self):
        """Load generated docs into source bucket before upgrade."""
        self.load(self.gens_load, buckets=self.src_bucket, verify_data=False)

    def _create_pre_upgrade_handler(self, appname, appcode, meta_bucket="metadata",
                                    src_bucket="src_bucket",
                                    bucket_bindings=None,
                                    dcp_stream_boundary="everything"):
        """Create a handler with bucket-level bindings for pre-upgrade (old version without collections)."""
        if bucket_bindings is None:
            bucket_bindings = ["dst_bucket.dst_bucket.rw"]
        body = {}
        body['appname'] = appname
        script_dir = os.path.dirname(__file__)
        abs_file_path = os.path.join(script_dir, appcode)
        with open(abs_file_path, "r") as fh:
            body['appcode'] = fh.read()
        body['depcfg'] = {}
        body['depcfg']['metadata_bucket'] = meta_bucket
        body['depcfg']['source_bucket'] = src_bucket
        body['depcfg']['curl'] = []
        body['depcfg']['buckets'] = []
        body['settings'] = {}
        body['settings']['dcp_stream_boundary'] = dcp_stream_boundary
        body['settings']['deployment_status'] = False
        body['settings']['processing_status'] = False
        body['settings']['log_level'] = self.eventing_log_level
        body['function_scope'] = self.function_scope
        for binding in bucket_bindings:
            bind_map = binding.split(".")
            if len(bind_map) < 3:
                raise Exception("Binding {} doesn't have all the fields".format(binding))
            body['depcfg']['buckets'].append(
                {"alias": bind_map[0], "bucket_name": bind_map[1], "access": bind_map[2]})
        self.rest.create_function(body['appname'], body)
        log.info("Saved pre-upgrade function: {}".format(body['appname']))
        return body

    def _deploy_pre_upgrade_handlers(self):
        """Deploy handlers on the old version before upgrade."""
        self.restServer = self.get_nodes_from_services_map(service_type="eventing")
        self.rest = RestConnection(self.restServer)
        log.info("Deploying pre-upgrade handlers on initial version")
        self._create_pre_upgrade_handler("bucket_op", "handler_code/delete_doc_bucket_op.js")
        self._create_pre_upgrade_handler("timers", "handler_code/bucket_op_with_timers_upgrade.js", bucket_bindings=["dst_bucket.dst_bucket1.rw"])
        self._create_pre_upgrade_handler("during_upgrade_handler", "handler_code/delete_doc_bucket_op.js",
            src_bucket=self.during_upgrade_src_bucket_name,
            bucket_bindings=["dst_bucket.{0}.rw".format(self.during_upgrade_dst_bucket_name)])
        self.sleep(20)
        self.deploy_handler_by_name("timers")
        self.deploy_handler_by_name("during_upgrade_handler")
        self.all_handler_names.extend(["bucket_op", "timers", "during_upgrade_handler"])

    def _install_travel_sample(self):
        log.info("Loading travel-sample bucket on pre-upgrade cluster")
        EventingBaseTest.load_sample_buckets(self, self.master, "travel-sample")
        self.sleep(120, "Waiting for travel-sample bucket to load")

    def _load_travel_sample_post_upgrade(self):
        """Load travel-sample on the upgraded cluster via the sample loader.

        Re-lowers magmaMinMemoryQuota (the upgrade resets it) so the loader's
        bucket clears magma's 1024MB minimum; the bucket must not be pre-created.
        """
        log.info("Lowering magmaMinMemoryQuota to 100MB on upgraded cluster")
        rest = RestConnection(self.master)
        rest.set_internalSetting("magmaMinMemoryQuota", 100)
        log.info("Running sample loader to load travel-sample")
        rest.load_sample("travel-sample")
        self.assertTrue(self._wait_for_travel_sample(rest), "travel-sample did not finish loading post-upgrade.")

    def _wait_for_travel_sample(self, rest, timeout=300):
        """Poll until travel-sample exists with a stable item count.
        Returns True on success, False on timeout."""
        bucket_api = '{0}pools/default/buckets/travel-sample'.format(rest.baseUrl)
        end_time = time.time() + timeout
        previous_count = -1
        stable_polls = 0
        while time.time() < end_time:
            self.sleep(10)
            try:
                status, content, _ = rest._http_request(bucket_api, method='GET')
                if not status:
                    continue  # bucket not created yet
                current_count = int(json.loads(content)["basicStats"]["itemCount"])
                log.info("travel-sample item count: {0}".format(current_count))
                if current_count > 0 and current_count == previous_count:
                    stable_polls += 1
                    if stable_polls >= 2:
                        log.info("travel-sample loading complete with {0} items".format(current_count))
                        return True
                else:
                    stable_polls = 0
                previous_count = current_count
            except (json.JSONDecodeError, ValueError, KeyError, TypeError):
                pass  # transient/incomplete response while bucket is still initializing

        log.error("travel-sample bucket did not appear/stabilize within {0}s".format(timeout))
        return False

    def _enable_tls_if_configured(self):
        """Enable N2N encryption and TLS strict mode if configured."""
        if self.enable_n2n_encryption_and_tls:
            ntonencryptionBase().setup_nton_cluster([self.master], clusterEncryptionLevel="strict")
            CbServer.use_https = True
            # Re-establish REST connection to eventing node over TLS
            self.restServer = self.get_nodes_from_services_map(service_type="eventing")
            self.rest = RestConnection(self.restServer)
            # Bounce the timers handler to pick up TLS config
            self.wait_for_handler_state("timers", "deployed")
            self.pause_handler_by_name("timers")
            self.resume_handler_by_name("timers")

    def _verify_pre_upgrade_handlers_survived(self):
        """Verify pre-upgrade handlers survived the upgrade and data is intact."""
        self.wait_for_handler_state("bucket_op", "undeployed")
        self.wait_for_handler_state("timers", "deployed")
        self.deploy_handler_by_name("bucket_op")
        self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day * 2016)
        self.verify_doc_count_collections("dst_bucket1._default._default", self.docs_per_day * 2016)

    def _create_collections_on_all_buckets(self):
        buckets = RestConnection(self.master).get_buckets()
        for bucket in buckets:
            self.create_scope_collection(bucket.name, "event", "coll_0")

    def _setup_fts_index(self):
        """Create the FTS index on travel-sample and wait for it to ingest.

        Uses FTSCallable (matching the eventing FTS suite) so the index is the
        default index the fts_query handler queries by its bare name.
        """
        self.fts_index = self.fts_callable.create_default_index(
            index_name=self.fts_index_name,
            bucket_name="travel-sample",
            plan_params={"indexPartitions": 1, "numReplicas": 0})
        self.fts_callable.wait_for_indexing_complete(
            item_count=self.fts_doc_count, idx=self.fts_index)
        self.sleep(30, "Waiting for FTS indexing to complete")

    def _setup_analytics(self):
        """Create the analytics dataverse/collection, connect the link, and wait
        for ingestion. Statements are retried since CBAS can be unstable post-upgrade."""
        log.info("Setting up analytics for upgrade test")
        cbas_node = self.get_nodes_from_services_map(service_type="cbas")
        cbas_rest = RestConnection(cbas_node)
        # Helper: execute a CBAS statement with retries
        def _cbas_exec_with_retry(statement, retries=3, delay=30):
            for attempt in range(1, retries + 1):
                try:
                    return cbas_rest.execute_statement_on_cbas(statement, None)
                except Exception as e:
                    if attempt < retries:
                        log.warning("CBAS statement attempt {0}/{1} failed: {2}. "
                                    "Retrying after {3}s...".format(attempt, retries, e, delay))
                        self.sleep(delay, "Waiting for CBAS to stabilise before retry")
                    else:
                        raise
        _cbas_exec_with_retry("CREATE DATAVERSE `travel-sample`.`inventory`")
        _cbas_exec_with_retry(
            "CREATE ANALYTICS COLLECTION `travel-sample`.`inventory`.`airline` "
            "ON `travel-sample`.`inventory`.`airline`")
        _cbas_exec_with_retry("CONNECT LINK Local")
        # Poll until analytics ingests data
        count = 0
        poll_count = 0
        while count == 0 and poll_count < 20:
            self.sleep(15, "Waiting for analytics to ingest travel-sample data")
            try:
                result = cbas_rest.execute_statement_on_cbas(
                    "SELECT COUNT(*) AS cnt FROM `travel-sample`.`inventory`.`airline`", None)
                if isinstance(result, bytes):
                    result = result.decode("utf-8")
                parsed = json.loads(result)
                count = parsed["results"][0]["cnt"]
            except Exception as e:
                log.warning("Analytics count poll failed: {0}".format(e))
            poll_count += 1
        log.info("Analytics airline collection has {} docs".format(count))
        if count == 0:
            self.fail("Analytics airline collection has 0 docs — ingestion failed")

    def _deploy_post_upgrade_handlers(self, include_fts_analytics=True):
        """Deploy post-upgrade (collection-scoped) handlers; pass
        include_fts_analytics=False to skip the travel-sample-dependent ones."""
        self.add_built_in_server_user()
        self.restServer = self.get_nodes_from_services_map(service_type="eventing")
        self.rest = RestConnection(self.restServer)
        # Core handlers: sbm, curl, n1ql, on_deploy
        self.create_function_with_collection(
            "sbm", "handler_code/ABO/insert_sbm.js",
            src_namespace="source_bucket_mutation.event.coll_0",
            collection_bindings=["src_bucket.source_bucket_mutation.event.coll_0.rw"])
        self.hostname = "http://qa.sc.couchbase.com/"
        self.create_function_with_collection(
            "curl", "handler_code/ABO/curl_get.js",
            src_namespace="src_bucket.event.coll_0",
            collection_bindings=["dst_bucket.dst_bucket_curl.event.coll_0.rw"],
            is_curl=True)
        self.create_function_with_collection(
            "n1ql", "handler_code/collections/n1ql_insert_update.js",
            src_namespace="src_bucket.event.coll_0")
        self.create_function_with_collection(
            "on_deploy", "handler_code/ondeploy_basic_bucket_op.js",
            src_namespace="src_bucket.event.coll_0",
            collection_bindings=["dst_bucket.dst_bucket.event.coll_0.rw"])
        post_upgrade_handlers = ["sbm", "curl", "n1ql", "on_deploy"]
        # Optional: analytics and FTS handlers (require travel-sample)
        if include_fts_analytics:
            self.create_function_with_collection(
                "analytics", "handler_code/analytics/analytics_basic_select.js",
                src_namespace="src_bucket.event.coll_0",
                collection_bindings=["dst_bucket.analytics_dst.event.coll_0.rw"])
            self.create_function_with_collection(
                "fts_query", "handler_code/fts_query_support/match_query.js",
                src_namespace="src_bucket.event.coll_0",
                collection_bindings=["dst_bucket.fts_dst.event.coll_0.rw"])
            post_upgrade_handlers.extend(["analytics", "fts_query"])
        # Deploy all handlers
        for handler in post_upgrade_handlers:
            self.deploy_handler_by_name(handler)
        self.all_handler_names.extend(post_upgrade_handlers)

    def _run_full_mutation_cycle(self, include_fts_analytics=True):
        """Load data, verify all handlers processed, delete data, verify cleanup."""
        doc_count = self.docs_per_day * 2016
        # Load data into collection sources
        self.load_data_to_collection(doc_count, namespace="source_bucket_mutation.event.coll_0")
        self.load_data_to_collection(doc_count, namespace="src_bucket.event.coll_0")
        # Verify existing handlers
        self.verify_doc_count_collections("dst_bucket_curl.event.coll_0", doc_count)
        self.verify_doc_count_collections("n1ql_op_dst.event.coll_0", doc_count)
        self.verify_doc_count_collections("source_bucket_mutation.event.coll_0", doc_count * 2)
        # Verify FTS/analytics handlers if deployed
        if include_fts_analytics:
            self.verify_doc_count_collections("analytics_dst.event.coll_0", doc_count)
            self.verify_doc_count_collections("fts_dst.event.coll_0", doc_count)
        # Delete data from all sources
        self.load_data_to_collection(doc_count, namespace="source_bucket_mutation.event.coll_0", is_delete=True)
        self.load_data_to_collection(doc_count, namespace="src_bucket.event.coll_0", is_delete=True)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False, batch_size=self.batch_size, op_type='delete')
        rest = RestConnection(self.master)
        dst_cleanup = rest.get_bucket_by_name(self.dst_bucket_name) + \
                      rest.get_bucket_by_name(self.dst_bucket_name1)
        if not dst_cleanup:
            log.warning("dst_cleanup is empty — skipping destination bucket delete")
        else:
            self.load(self.gens_load, buckets=dst_cleanup, verify_data=False, batch_size=self.batch_size, op_type='delete')
        # Verify all destinations are empty
        self.verify_doc_count_collections("dst_bucket_curl.event.coll_0", 0)
        self.verify_doc_count_collections("n1ql_op_dst.event.coll_0", 0)
        self.verify_doc_count_collections("source_bucket_mutation.event.coll_0", doc_count)
        self.verify_doc_count_collections("dst_bucket._default._default", 0)
        self.verify_doc_count_collections("dst_bucket1._default._default", 0)
        if include_fts_analytics:
            self.verify_doc_count_collections("analytics_dst.event.coll_0", 0)
            self.verify_doc_count_collections("fts_dst.event.coll_0", 0)

    def _pause_all_handlers(self):
        for name in self.all_handler_names:
            self.pause_handler_by_name(name)

    def _resume_all_handlers(self):
        for name in self.all_handler_names:
            self.resume_handler_by_name(name)

    def _run_pause_resume_cycle(self, include_fts_analytics=True):
        """Pause all handlers, load data while paused, resume, verify processing."""
        doc_count = self.docs_per_day * 2016
        self._pause_all_handlers()
        # Load data while handlers are paused
        self.load_data_to_collection(doc_count, namespace="source_bucket_mutation.event.coll_0")
        self.load_data_to_collection(doc_count, namespace="src_bucket.event.coll_0")
        self.load_data_to_collection(doc_count, namespace="src_bucket._default._default")
        # Resume all handlers
        self._resume_all_handlers()
        # Verify handlers processed the mutations after resume
        self.verify_doc_count_collections("dst_bucket_curl.event.coll_0", doc_count)
        self.verify_doc_count_collections("n1ql_op_dst.event.coll_0", doc_count)
        self.verify_doc_count_collections("source_bucket_mutation.event.coll_0", doc_count * 3)
        self.verify_doc_count_collections("dst_bucket._default._default", doc_count)
        if include_fts_analytics:
            self.verify_doc_count_collections("analytics_dst.event.coll_0", doc_count)
            self.verify_doc_count_collections("fts_dst.event.coll_0", doc_count)
        self.sleep(300)  # wait for timers to fire
        self.verify_doc_count_collections("dst_bucket1._default._default", doc_count)

    def _verify_jwt_auth(self):
        """Verify eventing handler lifecycle operations work with JWT authentication."""
        log.info("Verifying JWT authentication for eventing operations")
        jwt_utils = JWTUtils(log=log)
        private_key, public_key = jwt_utils.generate_key_pair("ES256", key_size=2048)
        # Create group with eventing permissions
        status, content = self.rest.add_group_role(
            group_name="jwt_upgrade_group",
            description="Group for upgrade JWT test",
            roles="admin"
        )
        if not status:
            self.fail("Failed to create JWT group: {}".format(content))
        # Create external user
        import urllib.parse
        payload = urllib.parse.urlencode({
            "name": "jwt_upgrade_user",
            "groups": "jwt_upgrade_group"
        })
        self.rest.add_external_user("jwt_upgrade_user", payload)
        # Configure JWT on cluster
        jwt_config = jwt_utils.get_jwt_config(
            issuer_name="upgrade-test-issuer",
            algorithm="ES256",
            pub_key=public_key,
            token_audience=["cb-cluster"],
            token_group_matching_rule=["^.jwt_upgrade_user$ jwt_upgrade_group"],
            jit_provisioning=False
        )
        status, content, _ = self.rest.create_jwt_with_config(jwt_config)
        if not status:
            self.fail("Failed to configure JWT: {}".format(content))
        # Generate JWT token
        jwt_token = jwt_utils.create_token(
            issuer_name="upgrade-test-issuer",
            user_name="jwt_upgrade_user",
            algorithm="ES256",
            private_key=private_key,
            token_audience=["cb-cluster"],
            user_groups=["jwt_upgrade_group"],
            ttl=3600
        )
        # Test handler lifecycle with JWT: create, deploy, pause, resume, undeploy, delete
        body = self.create_function_with_collection(
            "jwt_test", "handler_code/no_op.js",
            src_namespace="src_bucket.event.coll_0",
            collection_bindings=["dst_bucket.dst_bucket.event.coll_0.rw"])
        self.deploy_function(body, jwt_token=jwt_token)
        self.pause_function(body, jwt_token=jwt_token)
        self.resume_function(body, jwt_token=jwt_token)
        self.undeploy_function(body, jwt_token=jwt_token)
        self.delete_function(body, jwt_token=jwt_token)
        log.info("JWT authentication verification passed")

    def _verify_rbac(self):
        """Verify eventing_manage_functions role works after upgrade."""
        payload = ("name=john&roles=data_reader[metadata],data_writer[metadata],"
                   "data_writer[dst_bucket],data_dcp_reader[src_bucket],"
                   "eventing_manage_functions[src_bucket:_default]&password=asdasd")
        self.rest.add_set_builtin_user(user_id="john", payload=payload)
        self._create_pre_upgrade_handler("test", "handler_code/no_op.js")
        self.deploy_handler_by_name("test")
        self.pause_handler_by_name("test")
        self.resume_handler_by_name("test")
        self.undeploy_function_by_name("test")
        self.rest.delete_single_function("test", self.function_scope)

    ###########################################################################
    # Private helpers — offline upgrade only
    ###########################################################################

    def _install_and_setup_cluster(self):
        """Install the old version on all cluster nodes and form the initial cluster."""
        self._install(self.servers[:self.nodes_init])
        self.operations(self.servers[:self.nodes_init], services="kv,index,n1ql-kv,eventing-kv,fts,cbas")
        self._set_memory_quotas()
        self._install_travel_sample()
        self._capture_pre_upgrade_node_services()

    def _capture_pre_upgrade_node_services(self):
        """Snapshot each node's service assignment before upgrade, so
        _verify_cluster_services_after_upgrade can check the actual topology
        survived instead of comparing against a hardcoded service list."""
        rest = RestConnection(self.master)
        self.pre_upgrade_node_services = {
            node.ip: set(getattr(node, 'services', []) or [])
            for node in rest.get_nodes()
        }
        log.info("Captured pre-upgrade node services: {0}".format(
            {ip: sorted(s) for ip, s in self.pre_upgrade_node_services.items()}))

    def _perform_offline_upgrade(self):
        upgrade_threads = self._async_update(self.upgrade_version, self.servers[:self.nodes_init])
        for upgrade_thread in upgrade_threads:
            upgrade_thread.join()
        self.sleep(120)
        success_upgrade = True
        while not self.queue.empty():
            success_upgrade &= self.queue.get()
        if not success_upgrade:
            self.fail("Offline upgrade failed!")

    def _verify_cluster_services_after_upgrade(self):
        """
        Verify every node retained the exact service assignment it had before
        upgrade (against the snapshot from _capture_pre_upgrade_node_services).
        Offline upgrade can drop node service assignments (esp. index/n1ql on
        master); fail fast here instead of hours of silent N1QL/FTS failures.
        """
        rest = RestConnection(self.master)
        nodes = rest.get_nodes()
        log.info("=== Post-upgrade service verification ===")
        post_upgrade_node_services = {}
        for node in nodes:
            node_services = set(getattr(node, 'services', []) or [])
            log.info("Node {0}: services = {1}".format(
                getattr(node, 'hostname', 'unknown'), sorted(node_services)))
            post_upgrade_node_services[node.ip] = node_services
        mismatches = {}
        for ip, pre_services in self.pre_upgrade_node_services.items():
            missing = pre_services - post_upgrade_node_services.get(ip, set())
            if missing:
                mismatches[ip] = sorted(missing)
        if mismatches:
            self.fail("Node service assignments lost after offline upgrade: {0} "
                      "(pre-upgrade: {1}, post-upgrade: {2})".format(
                mismatches,
                {ip: sorted(s) for ip, s in self.pre_upgrade_node_services.items()},
                {ip: sorted(s) for ip, s in post_upgrade_node_services.items()}))
        log.info("All pre-upgrade node service assignments preserved after upgrade: {0}".format(
            {ip: sorted(s) for ip, s in post_upgrade_node_services.items()}))

    ###########################################################################
    # Private helpers — online upgrade only
    ###########################################################################

    def _install_and_setup_online_cluster(self):
        """Install old version on first N nodes, new version on next N spare nodes, form cluster."""
        self._install(self.servers[:self.nodes_init])
        self.initial_version = self.upgrade_version
        self._install(self.servers[self.nodes_init:self.num_servers])
        self.operations(self.servers[:self.nodes_init],
                        services="kv,index,n1ql-kv,eventing-kv,fts,cbas")
        self._set_memory_quotas()

    def _eventing_background_load_loop(self, duration_secs=900, sleep_secs=30):
        bucket = self.during_upgrade_src_bucket[0]
        end_time = time.time() + duration_secs
        iteration = 0
        while time.time() < end_time:
            gens_load = self.generate_docs((iteration + 1) * self.docs_per_day,
                                           start=iteration * self.docs_per_day)
            try:
                task = self.cluster.async_load_gen_docs(
                    self.master, bucket.name, gens_load, bucket.kvs[1], 'create',
                    compression=self.sdk_compression)
                task.result()
                self.during_upgrade_total_docs += self.docs_per_day * 2016
                iteration += 1
                log.info("During-upgrade load: iteration {0} complete, {1} total docs "
                         "loaded into {2} so far".format(iteration, self.during_upgrade_total_docs, bucket.name))
            except Exception as e:
                log.warning("During-upgrade load: iteration {0} failed, will retry "
                           "same batch next pass: {1}".format(iteration + 1, e))
            if time.time() < end_time:
                self.sleep(sleep_secs)

    def _start_eventing_background_load(self):
        """Load docs so that there are mutations to process for the eventing service while upgrade is in progress."""
        self.during_upgrade_total_docs = 0
        thread = Thread(target=self._eventing_background_load_loop,
                        name="during_upgrade_load_loop")
        thread.start()
        return thread

    def _finish_eventing_background_load(self, thread):
        """verify doc count right after upgarde finishes."""
        thread.join()
        src_count = self.get_item_count(self.master, self.during_upgrade_src_bucket_name)
        log.info("Background load loop into during_upgrade_src_bucket completed: "
                 "{0} docs counted by loader, {1} docs actually in source".format(
                     self.during_upgrade_total_docs, src_count))
        self.verify_doc_count_collections(
            "{0}._default._default".format(self.during_upgrade_dst_bucket_name),
            src_count)

    def _perform_online_upgrade_regular_rebalance(self):
        """Rebalance in new-version nodes while removing old-version nodes, then update master."""
        load_thread = self._start_eventing_background_load()
        self.online_upgrade(services=["kv,index,n1ql", "kv,eventing", "kv,fts,cbas"])
        self._finish_eventing_background_load(load_thread)
        self._refresh_after_online_upgrade()

    def _perform_online_upgrade_swap_rebalance(self):
        """Swap rebalance nodes one at a time to new version, then update master."""
        load_thread = self._start_eventing_background_load()
        self.online_upgrade_swap_rebalance(services=["kv,index,n1ql", "kv,eventing", "kv,fts,cbas"])
        self._finish_eventing_background_load(load_thread)
        self._refresh_after_online_upgrade()

    def _perform_online_upgrade_with_failover(self, recovery_type="out"):
        """Failover the eventing node during upgrade, then recover or rebalance out."""
        load_thread = self._start_eventing_background_load()
        self.online_upgrade_with_failover(
            services=["kv,index,n1ql", "kv,eventing", "kv,fts,cbas"],
            recovery_type=recovery_type)
        self._finish_eventing_background_load(load_thread)
        self._refresh_after_online_upgrade()

    def online_upgrade(self, services=None):
        """Rebalance in new-version nodes and out old-version nodes in one step, then find new master."""
        servers_in = self.servers[self.nodes_init:self.num_servers]
        servers_out = self.servers[:self.nodes_init]
        self.cluster.rebalance(self.servers[:self.nodes_init], servers_in, servers_out, services=services,
                               sleep_before_rebalance=self.rebalance_settle_time)
        self.sleep(300)
        self.log.info("Rebalance in all {0} nodes".format(self.input.param("upgrade_version", "")))
        self.log.info("Rebalanced out all old version nodes")
        status, content = ClusterOperationHelper.find_orchestrator(servers_in[0])
        self.assertTrue(status, msg="Unable to find orchestrator: {0}:{1}".format(status, content))
        for new_server in servers_in:
            if content.find(new_server.ip) >= 0:
                self._switch_master(new_server)
                self.log.info("%s node %s becomes the master"
                              % (self.input.param("upgrade_version", ""), new_server.ip))
                self.master = new_server
                break

    def online_upgrade_swap_rebalance(self, services=None):
        """Replace nodes one at a time via swap rebalance, swapping the master last."""
        servers_in = self.servers[self.nodes_init:self.num_servers]
        self.sleep(self.sleep_time)
        status, content = ClusterOperationHelper.find_orchestrator(self.master)
        self.assertTrue(status, msg="Unable to find orchestrator: {0}:{1}".format(status, content))
        # Track which nodes are currently in the cluster.
        active_nodes = list(self.servers[:self.nodes_init])
        # Swap non-master nodes first (index 1, 2 → eventing, fts/cbas).
        i = 1
        for server_in, service_in in zip(servers_in[1:], services[1:]):
            log.info("Swap rebalance nodes : server_in: {0} service_in:{1} service_out:{2}".format(
                server_in, service_in, self.servers[i]))
            self.cluster.rebalance(active_nodes, [server_in], [self.servers[i]],
                                   services=[service_in],
                                   sleep_before_rebalance=self.rebalance_settle_time)
            # Update active list: remove old, add new
            active_nodes.remove(self.servers[i])
            active_nodes.append(server_in)
            i += 1
        # Switch master to a new-version node BEFORE swapping the old master out.
        self._switch_master(servers_in[1])
        # Swap the old master (servers[0]) last
        self.cluster.rebalance(active_nodes, [servers_in[0]], [self.servers[0]],
                               services=[services[0]],
                               sleep_before_rebalance=self.rebalance_settle_time)
        active_nodes.remove(self.servers[0])
        active_nodes.append(servers_in[0])
        # Discover the actual orchestrator on the fully-upgraded cluster
        status, content = ClusterOperationHelper.find_orchestrator(servers_in[0])
        self.assertTrue(status, msg="Unable to find orchestrator after swap rebalance: {0}:{1}".format(
            status, content))
        for node in active_nodes:
            if content.find(node.ip) >= 0:
                self._switch_master(node)
                self.log.info("Swap rebalance complete — orchestrator is {0}".format(node.ip))
                break

    def _get_otp_node(self, rest, server_ip):
        for node in rest.node_statuses():
            if node.ip == server_ip:
                return node.id
        return None

    def _wait_for_failover_complete(self, rest, server_ip, timeout=300):
        """Poll until server_ip reaches 'inactiveFailed'.
        Graceful failover is async (cluster.failover() returns before it
        finishes), so recovery must wait or set_recovery_type is rejected.
        """
        end = time.time() + timeout
        while time.time() < end:
            for node in rest.get_nodes(get_all_nodes=True):
                if node.ip == server_ip:
                    if node.clusterMembership == "inactiveFailed":
                        log.info("Node {0} is now inactiveFailed".format(server_ip))
                        return True
                    break
            self.sleep(10, "Waiting for {0} to reach inactiveFailed".format(server_ip))
        self.fail("Node {0} did not reach inactiveFailed within {1}s".format(server_ip, timeout))

    def _rebalance_eventing_retry(self, active_nodes, to_in, to_out, services=None, retries=4):
        """Rebalance, retrying the eventing 'deploying or resuming' guard.
        After add-back a function auto-resumes and ns_server rejects the eventing
        rebalance; the failed attempt rolls back and lets it settle, so retry.
        """
        for attempt in range(1, retries + 1):
            try:
                self.cluster.rebalance(active_nodes, to_in, to_out, services=services,
                                       sleep_before_rebalance=self.rebalance_settle_time if to_in else None)
                return
            except Exception as e:
                retryable = isinstance(e, RebalanceFailedException) or "deploying or resuming" in str(e)
                if retryable and attempt < retries:
                    self.sleep(60, "Eventing rebalance failed (likely an app still "
                                   "deploying/resuming); letting it settle and retrying "
                                   "(attempt {0}/{1})".format(attempt, retries))
                    continue
                raise

    def online_upgrade_with_failover(self, services=None, recovery_type="out"):
        """Upgrade the cluster but remove the eventing node via FAILOVER.
        Swap the fts/cbas node mixed-version, fail the eventing node over, then
        per recovery_type rebalance it out or add it back (delta/full); master last.
        """
        servers_in = self.servers[self.nodes_init:self.num_servers]
        old_master, old_eventing, old_ftscbas = (self.servers[0], self.servers[1], self.servers[2])
        new_master, new_eventing, new_ftscbas = (servers_in[0], servers_in[1], servers_in[2])
        svc_master, svc_eventing, svc_ftscbas = (services[0], services[1], services[2])

        graceful = (self.failover_type == "graceful")
        self.assertIn(self.failover_type, ("hard", "graceful"),
                      "failover_type must be 'hard' or 'graceful', got {0}".format(self.failover_type))
        if recovery_type not in ("out", "delta", "full"):
            self.fail("recovery_type must be 'out', 'delta' or 'full', got {0}".format(recovery_type))

        self.sleep(self.sleep_time)
        status, content = ClusterOperationHelper.find_orchestrator(self.master)
        self.assertTrue(status, msg="Unable to find orchestrator: {0}:{1}".format(status, content))

        active_nodes = list(self.servers[:self.nodes_init])

        # --- Step 1: swap fts/cbas node so the cluster becomes mixed-version ---
        log.info("Failover upgrade: swap rebalance fts/cbas {0} -> {1}".format(
            old_ftscbas.ip, new_ftscbas.ip))
        self.cluster.rebalance(active_nodes, [new_ftscbas], [old_ftscbas], services=[svc_ftscbas],
                               sleep_before_rebalance=self.rebalance_settle_time)
        active_nodes.remove(old_ftscbas)
        active_nodes.append(new_ftscbas)

        # --- Step 2: failover the eventing node ---
        log.info("Failover upgrade: {0} failover of eventing node {1}".format(
            self.failover_type, old_eventing.ip))
        self.cluster.failover(active_nodes, failover_nodes=[old_eventing], graceful=graceful)
        rest = RestConnection(self.master)
        if graceful:
            # Graceful failover runs as an async rebalance; cluster.failover()
            # returns once the request is accepted, not once it completes. Wait
            # for the rebalance to finish before attempting recovery.
            rest.monitorRebalance()
        # Confirm the node is actually failed over before recovery / rebalance-out.
        self._wait_for_failover_complete(rest, old_eventing.ip)
        self.sleep(30, "Settling after failover of eventing node")

        # --- Step 3: recover the node or rebalance it out ---
        if recovery_type in ("delta", "full"):
            otp = self._get_otp_node(rest, old_eventing.ip)
            self.assertTrue(otp, "Could not find otpNode for failed-over node {0}".format(old_eventing.ip))
            log.info("Failover upgrade: {0} recovery + add-back of {1}".format(
                recovery_type, old_eventing.ip))
            rest.set_recovery_type(otpNode=otp, recoveryType=recovery_type)
            rest.add_back_node(otpNode=otp)
            #On add-back eventing auto-resumes the deployed function &ns server rejects the eventing rebalance which is expected while the resume is in flight.
            self.sleep(60, "Letting eventing auto-resume settle after add-back")
            # Rebalance to finalize the recovery; the node rejoins (still old version).
            self._rebalance_eventing_retry(active_nodes, [], [])
            # Now swap the recovered old-version eventing node for the new-version one.
            log.info("Failover upgrade: swap recovered eventing node {0} -> {1}".format(
                old_eventing.ip, new_eventing.ip))
            self._rebalance_eventing_retry(active_nodes, [new_eventing], [old_eventing], services=[svc_eventing])
            active_nodes.remove(old_eventing)
            active_nodes.append(new_eventing)
        else:
            log.info("Failover upgrade: rebalance out failed eventing node {0}".format(old_eventing.ip))
            self._rebalance_eventing_retry(active_nodes, [], [old_eventing])
            active_nodes.remove(old_eventing)
            log.info("Failover upgrade: rebalance in new-version eventing node {0}".format(new_eventing.ip))
            self._rebalance_eventing_retry(active_nodes, [new_eventing], [], services=[svc_eventing])
            active_nodes.append(new_eventing)

        # --- Step 4: swap the old master out last ---
        # Move the test's orchestrator handle off the old master before removing it.
        self._switch_master(new_eventing)
        log.info("Failover upgrade: swap master node {0} -> {1}".format(old_master.ip, new_master.ip))
        self._rebalance_eventing_retry(active_nodes, [new_master], [old_master], services=[svc_master])
        active_nodes.remove(old_master)
        active_nodes.append(new_master)

        # Discover the actual orchestrator on the fully-upgraded cluster
        status, content = ClusterOperationHelper.find_orchestrator(new_master)
        self.assertTrue(status, msg="Unable to find orchestrator after failover upgrade: {0}:{1}".format(
            status, content))
        for node in active_nodes:
            if content.find(node.ip) >= 0:
                self._switch_master(node)
                self.log.info("Failover upgrade complete — orchestrator is {0}".format(node.ip))
                break

    def _switch_master(self, server):
        """Point self.master and REST connections to a new orchestrator node."""
        self.master = server
        self.rest = RestConnection(self.master)
        self.rest_helper = RestHelper(self.rest)

    def _refresh_after_online_upgrade(self):
        """Re-point cached handles (REST, server, fts_callable, buckets) at the
        new cluster after an online upgrade removes the old nodes."""
        # REST & eventing connections
        self.restServer = self.get_nodes_from_services_map(service_type="eventing")
        self.rest = RestConnection(self.restServer)
        self.add_built_in_server_user()
        # self.server is used by _create_bucket_params / load_sample_buckets
        self.server = self.master
        # Re-create FTSCallable so its internal CouchbaseCluster.__master_node
        # points to a live node, not the old master that was rebalanced out.
        active_servers = self.servers[self.nodes_init:self.num_servers]
        self.fts_callable = FTSCallable(nodes=active_servers, es_validate=False)
        # Refresh bucket objects so KV operations use metadata from the new cluster.
        self.buckets = RestConnection(self.master).get_buckets()
        self.src_bucket = [b for b in self.buckets if b.name == self.src_bucket_name]
        if not self.src_bucket:
            self.fail("src_bucket '{0}' not found after online upgrade: {1}".format(
                self.src_bucket_name, [b.name for b in self.buckets]))

# TO-DO: pending patches to track for this suite
#   - Chaos Tests from Morpheus
#   - Import/Export Handlers
#   - Base64/XATTRS
#   - OnDeploy
