import logging
import random
import datetime
import os
from TestInput import TestInputSingleton
from lib.membase.api.rest_client import RestConnection
from pytests.basetestcase import BaseTestCase
from testconstants import INDEX_QUOTA, MIN_KV_QUOTA, EVENTING_QUOTA
from pytests.query_tests_helper import QueryHelperTests

log = logging.getLogger(__name__)


class EventingBaseTest(QueryHelperTests, BaseTestCase):
    def setUp(self):
        if self._testMethodDoc:
            log.info("\n\nStarting Test: %s \n%s" % (self._testMethodName, self._testMethodDoc))
        else:
            log.info("\n\nStarting Test: %s" % (self._testMethodName))
        self.input = TestInputSingleton.input
        self.input.test_params.update({"default_bucket": False})
        super(EventingBaseTest, self).setUp()
        self.server = self.master
        self.restServer = self.get_nodes_from_services_map(service_type="eventing")
        self.rest = RestConnection(self.restServer)
        self.log.info(
            "Setting the min possible memory quota so that adding mode nodes to the cluster wouldn't be a problem.")
        self.rest.set_service_memoryQuota(service='memoryQuota', memoryQuota=330)
        self.rest.set_service_memoryQuota(service='indexMemoryQuota', memoryQuota=INDEX_QUOTA)
        # self.rest.set_service_memoryQuota(service='eventingMemoryQuota', memoryQuota=EVENTING_QUOTA)
        self.src_bucket_name = self.input.param('src_bucket_name', 'src_bucket')
        self.dst_bucket_name = self.input.param('dst_bucket_name', 'dst_bucket')
        self.metadata_bucket_name = self.input.param('metadata_bucket_name', 'metadata')
        self.create_functions_buckets = self.input.param('create_functions_buckets', True)
        self.docs_per_day = self.input.param("doc-per-day", 1)
        random.seed(datetime.time)
        self.function_name = "Function_{0}_{1}".format(random.randint(1, 1000000000), self._testMethodName)

    def tearDown(self):
        super(EventingBaseTest, self).tearDown()

    def create_save_function_body(self, appname, appcode, description="Sample Description",
                                  checkpoint_interval=10000, cleanup_timers=False,
                                  dcp_stream_boundary="everything", deployment_status=True, log_level="TRACE",
                                  rbacpass="password", rbacrole="admin", rbacuser="cbadminbucket",
                                  skip_timer_threshold=86400,
                                  sock_batch_size=1, tick_duration=5000, timer_processing_tick_interval=500,
                                  timer_worker_pool_size=3, worker_count=1, processing_status=True,
                                  cpp_worker_thread_count=1):
        body = {}
        body['appname'] = appname
        script_dir = os.path.dirname(__file__)
        abs_file_path = os.path.join(script_dir, appcode)
        fh = open(abs_file_path, "r")
        body['appcode'] = fh.read()
        fh.close()
        body['depcfg'] = {}
        body['depcfg']['buckets'] = []
        body['depcfg']['buckets'].append({"alias": self.dst_bucket_name, "bucket_name": self.dst_bucket_name})
        body['depcfg']['metadata_bucket'] = self.metadata_bucket_name
        body['depcfg']['source_bucket'] = self.src_bucket_name
        body['settings'] = {}
        body['settings']['checkpoint_interval'] = checkpoint_interval
        body['settings']['cleanup_timers'] = cleanup_timers
        body['settings']['dcp_stream_boundary'] = dcp_stream_boundary
        body['settings']['deployment_status'] = deployment_status
        body['settings']['description'] = description
        body['settings']['log_level'] = log_level
        body['settings']['rbacpass'] = rbacpass
        body['settings']['rbacrole'] = rbacrole
        body['settings']['rbacuser'] = rbacuser
        body['settings']['skip_timer_threshold'] = skip_timer_threshold
        body['settings']['sock_batch_size'] = sock_batch_size
        body['settings']['tick_duration'] = tick_duration
        body['settings']['timer_processing_tick_interval'] = timer_processing_tick_interval
        body['settings']['timer_worker_pool_size'] = timer_worker_pool_size
        body['settings']['worker_count'] = worker_count
        body['settings']['processing_status'] = processing_status
        body['settings']['cpp_worker_thread_count'] = cpp_worker_thread_count
        return body

    def wait_for_bootstrap_to_complete(self, name):
        result = self.rest.get_deployed_eventing_apps()
        count = 0
        while name not in result and count < 20:
            self.sleep(30, message="Waiting for eventing node to come out of bootstrap state...")
            count += 1
            result = self.rest.get_deployed_eventing_apps()
        if count == 20:
            raise Exception(
                'Eventing took lot of time to come out of bootstrap state or did not successfully bootstrap')

    def verify_eventing_results(self, name, expected_dcp_mutations, doc_timer_events=False, on_delete=False):
        # we can't rely on DCP_MUTATION stats when doc timers events are set.
        # TODO : add this back when getEventProcessingStats works reliably for doc timer events as well
        if not doc_timer_events:
            count = 0
            stats = self.rest.get_eventing_stats(name)
            if on_delete:
                mutation_type = "DCP_DELETION"
            else:
                mutation_type = "DCP_MUTATION"
            actual_dcp_mutations = stats[mutation_type]
            # wait for eventing node to process dcp mutations
            log.info("Number of {0} processed till now : {1}".format(mutation_type, actual_dcp_mutations))
            while actual_dcp_mutations != expected_dcp_mutations and count < 20:
                self.sleep(30, message="Waiting for eventing to process all dcp mutations...")
                count += 1
                stats = self.rest.get_eventing_stats(name)
                actual_dcp_mutations = stats[mutation_type]
                log.info("Number of {0} processed till now : {1}".format(mutation_type, actual_dcp_mutations))
            if count == 20:
                raise Exception(
                    "Eventing has not processed all the {0}. Current : {1} Expected : {2}".format(mutation_type,
                                                                                                  actual_dcp_mutations,
                                                                                                  expected_dcp_mutations
                                                                                                  ))
        # wait for bucket operations to complete and verify it went through successfully
        count = 0
        stats_dst = self.rest.get_bucket_stats(bucket=self.dst_bucket_name)
        while stats_dst["curr_items"] != expected_dcp_mutations and count < 20:
            self.sleep(30, message="Waiting for handler code to complete all bucket operations...")
            count += 1
            stats_dst = self.rest.get_bucket_stats(bucket=self.dst_bucket_name)
        self.assertEqual(expected_dcp_mutations, stats_dst["curr_items"],
                         "Bucket operations from handler code took lot of time to complete or didn't go through")

    def deploy_function(self, body):
        body['settings']['deployment_status'] = True
        body['settings']['processing_status'] = True
        # save the function so that it appears in UI
        content = self.rest.save_function(self.function_name, body)
        log.info("saveApp API : {0}".format(content))
        # deploy the function
        log.info("Deploying the following handler code")
        log.info("\n{0}".format(body['appcode']))
        content = self.rest.deploy_function(self.function_name, body)
        log.info("deployApp API : {0}".format(content))
        # wait for the function to come out of bootstrap state
        self.wait_for_bootstrap_to_complete(self.function_name)

    def undeploy_and_delete_function(self, body):
        self.undeploy_function(body)
        self.delete_function()

    def undeploy_function(self, body):
        body['settings']['deployment_status'] = False
        body['settings']['processing_status'] = False
        # save the function so that it disappears from UI
        content = self.rest.save_function(self.function_name, body)
        log.info("saveApp API : {0}".format(content))
        # undeploy the function
        content = self.rest.undeploy_function(self.function_name, body['settings'])
        log.info("Undeploy Application : {0}".format(content))

    def delete_function(self):
        # delete the function from the UI and backend
        self.rest.delete_function_from_temp_store(self.function_name)
        self.rest.delete_function(self.function_name)
        log.info("Delete Application : {0}".format(self.function_name))

