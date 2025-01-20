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
