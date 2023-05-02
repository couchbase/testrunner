from lib.membase.api.rest_client import RestConnection
from lib.testconstants import STANDARD_BUCKET_PORT
from pytests.eventing.eventing_base import EventingBaseTest
from lib.SystemEventLogLib.eventing_service_events import EventingServiceEvents
from lib import global_vars
import logging, os

log = logging.getLogger()


class EventingSystemEvents(EventingBaseTest):
    def setUp(self):
        super(EventingSystemEvents, self).setUp()
        self.rest.set_service_memoryQuota(service='memoryQuota',
                                          memoryQuota=700)
        if self.create_functions_buckets:
            self.bucket_size = 256
            log.info(self.bucket_size)
            bucket_params = self._create_bucket_params(server=self.server,
                                                       size=self.bucket_size,
                                                       replicas=self.num_replicas)
            self.cluster.create_standard_bucket(name=self.src_bucket_name,
                                                port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
            self.src_bucket = RestConnection(self.master).get_buckets()
            self.cluster.create_standard_bucket(name=self.dst_bucket_name,
                                                port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
            self.cluster.create_standard_bucket(name=self.metadata_bucket_name,
                                                port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
            self.buckets = RestConnection(self.master).get_buckets()
        handler_code = self.input.param('handler_code', 'bucket_op')
        if handler_code == 'bucket_op':
            self.handler_code = "handler_code/ABO/insert_rebalance.js"
        self.eventing_node = self.get_nodes_from_services_map(
            service_type="eventing")

    def tearDown(self):
        super(EventingSystemEvents, self).tearDown()

    def test_eventing_processes_system_events(self):
        global_vars.system_event_logs.add_event(
            EventingServiceEvents.producer_startup(self.eventing_node))
        self.system_events.validate(server=self.master)
        body = self.create_save_function_body(self.function_name,
                                              self.handler_code)
        self.deploy_function(body)
        global_vars.system_event_logs.add_event(
            EventingServiceEvents.consumer_startup(self.eventing_node))
        self.kill_consumer(self.eventing_node)
        global_vars.system_event_logs.add_event(
            EventingServiceEvents.consumer_crash(self.eventing_node))
        self.wait_for_handler_state(body['appname'], "deployed")
        self.undeploy_and_delete_function(body)
        self.system_events.validate(server=self.master)

    def test_debugger_and_tracing_start_stop_system_events(self):
        body = self.create_save_function_body(self.function_name,
                                              self.handler_code)
        self.deploy_function(body)
        self.rest.enable_eventing_debugger()
        self.rest.start_eventing_debugger(self.function_name,
                                          self.function_scope)
        global_vars.system_event_logs.add_event(
            EventingServiceEvents.start_debugger(self.eventing_node,
                                                 self.function_name))
        self.rest.stop_eventing_debugger(self.function_name,
                                         self.function_scope)
        global_vars.system_event_logs.add_event(
            EventingServiceEvents.stop_debugger(self.eventing_node,
                                                self.function_name))
        os.system("""curl -v -u Administrator:password -XGET \
                 http://{0}:8096/startTracing >/dev/null 2>&1 &"""
                  .format(self.eventing_node.ip))
        global_vars.system_event_logs.add_event(
            EventingServiceEvents.start_tracing(self.eventing_node))
        self.rest.stop_tracing()
        global_vars.system_event_logs.add_event(
            EventingServiceEvents.stop_tracing(self.eventing_node))
        self.undeploy_and_delete_function(body)
        self.system_events.validate(server=self.master)

    def test_create_delete_import_export_eventing_functions_system_events(self):
        body = self.create_save_function_body(self.function_name,
                                              self.handler_code)
        global_vars.system_event_logs.add_event(
            EventingServiceEvents.create_function(self.eventing_node,
                                                  self.function_name))
        self.rest.export_function(self.function_name, self.function_scope)
        global_vars.system_event_logs.add_event(
            EventingServiceEvents.export_functions(self.eventing_node))
        self.import_function_from_directory("exported_functions/bucket_op.json")
        global_vars.system_event_logs.add_event(
            EventingServiceEvents.import_functions(self.eventing_node))
        self.delete_function(body)
        global_vars.system_event_logs.add_event(
            EventingServiceEvents.delete_function(self.eventing_node,
                                                  self.function_name))
        self.system_events.validate(server=self.master)

    def test_lifecycle_operations_system_events(self):
        body = self.create_save_function_body(self.function_name,
                                              self.handler_code)
        self.deploy_function(body)
        global_vars.system_event_logs.add_event(
            EventingServiceEvents.deploy_function(self.eventing_node,
                                                  self.function_name))
        self.pause_function(body)
        global_vars.system_event_logs.add_event(
            EventingServiceEvents.pause_function(self.eventing_node,
                                                 self.function_name))
        self.resume_function(body)
        global_vars.system_event_logs.add_event(
            EventingServiceEvents.resume_function(self.eventing_node,
                                                  self.function_name))
        self.undeploy_and_delete_function(body)
        global_vars.system_event_logs.add_event(
            EventingServiceEvents.undeploy_function(self.eventing_node,
                                                    self.function_name))
        self.system_events.validate(server=self.master)