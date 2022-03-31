from SystemEventLogLib.SystemEventOperations import SystemEventRestHelper
from remote.remote_util import RemoteMachineShellConnection
import datetime
import json
from .tuq import QueryTests


class QuerySystemEventLogs(QueryTests):
    def setUp(self):
        super(QuerySystemEventLogs, self).setUp()
        self.shell = RemoteMachineShellConnection(self.master)
        self.info = self.shell.extract_remote_info()
        if self.info.type.lower() == 'windows':
            self.curl_path = f"{self.path}curl"
        else:
            self.curl_path = "curl"
        self.event_rest = SystemEventRestHelper([self.master])

    def tearDown(self):
        super(QuerySystemEventLogs, self).tearDown()

    def test_memory_quota(self):
        event_seen = False
        log_fields = ["uuid", "component", "event_id", "description", "severity", "timestamp", "extra_attributes", "node"]
        query = "'statement=select (select * from `default`)&memory_quota=1'"
        curl_output = self.shell.execute_command(
            f"{self.curl_path} -X POST -u {self.rest.username}:{self.rest.password} http://{self.master.ip}:{self.n1ql_port}/query/service -d {query}")
        self.log.info(curl_output)
        output = self.convert_list_to_json(curl_output[0])
        requestID = output['requestID']
        events = self.event_rest.get_events(server=self.master, events_count=-1)["events"]
        for event in events:
            if event['event_id'] == 1026:
                event_seen = True
                for field in log_fields:
                    self.assertTrue(field in event.keys(), f"Field {field} is not in the event and it should be, please check the event {event}")
                    self.assertEqual(event['component'], "query")
                    self.assertEqual(event['description'], "Request memory quota exceeded")
                    self.assertEqual(event['severity'], "info")
                    self.assertEqual(event['node'], self.master.ip)
                    self.assertEqual(event['extra_attributes']['request-id'], requestID)

        self.assertTrue(event_seen, f"We did not see the event id we were looking for: {events}")

    def test_change_settings(self):
        event_seen = False
        log_fields = ["uuid", "component", "event_id", "description", "severity", "timestamp", "extra_attributes",
                      "node"]
        settings = '{"auto-prepare":true,"completed-limit":5000,"controls":false}'
        curl_output = self.shell.execute_command(
            f"{self.curl_path} -X POST -u {self.rest.username}:{self.rest.password} http://{self.servers[1].ip}:{self.n1ql_port}/admin/settings -d '{settings}'")
        self.log.info(curl_output)
        events = self.event_rest.get_events(server=self.master, events_count=-1)["events"]
        for event in events:
            if event['event_id'] == 1025:
                if "None" in str(event):
                    pass
                else:
                    for field in log_fields:
                        self.assertTrue(field in event.keys(),
                                        f"Field {field} is not in the event and it should be, please check the event {event}")
                    self.assertEqual(event['component'], "query")
                    self.assertEqual(event['description'], "Configuration changed")
                    self.assertEqual(event['severity'], "info")
                    settings_changed = event['extra_attributes']
                    # Now check the event we generated
                    event_seen = True
                    for setting in settings_changed:
                        if setting == "auto-prepare":
                            expected_setting = {'from': False, 'to': True}
                        elif setting == "completed-limit":
                            expected_setting = {'from': 4000, 'to': 5000}
                        else:
                            self.fail(f"Unrecognized setting {setting} which should not have been changed! Please check the event {event}")
                        self.assertEqual(event['extra_attributes'][setting], expected_setting)
                        self.assertEqual(event['node'], self.servers[1].ip)

        self.assertTrue(event_seen, f"We did not see the event id we were looking for: {events}")