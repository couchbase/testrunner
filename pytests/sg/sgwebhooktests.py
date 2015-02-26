from sg.sg_base import GatewayBaseTest
from sg.sg_webhook_base import GatewayWebhookBaseTest
from remote.remote_util import RemoteMachineShellConnection

class SGWebHookTest(GatewayWebhookBaseTest):
    def setUp(self):
        super(SGWebHookTest, self).setUp()
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            shell.copy_files_local_to_remote('pytests/sg/resources', '/root')

    def tearDown(self):
        super(SGWebHookTest, self).tearDown()

    def webHookBasic(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            self.start_sync_gateway(shell, self.configfile)
            success, revision, status = self.create_doc(shell, self.doc_id, self.doc_content)
            self.assertTrue(success)
            doc_content = '{"class":"Math", "student":"John", "grade":"C", "count":99}'
            success, revision, status = self.update_doc(shell, self.doc_id, self.doc_content, revision)
            self.assertTrue(success)
            doc_content = '{"class":"Math", "student":"John", "grade":"A", "count":99}'
            success, revision, status = self.update_attachment(shell, self.doc_id, self.doc_content,
                                                               'gateway_config.json', revision)
            self.assertTrue(success)
            success, revision, status = self.delete_doc(shell, self.doc_id, revision)
            self.assertTrue(success)
            shell.disconnect()

    def webHookMutipleWebHooks(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            self.start_sync_gateway(shell, self.configfile)
            success, revision, status = self.create_doc_logfiles(shell, self.doc_id, self.doc_content,
                                        ['/opt/gocode/simpleServe.txt', '/opt/gocode/simpleServe2.txt'])
            self.assertTrue(success)
            shell.disconnect()

    def webHookMutipleWebHooksNegative(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            self.start_sync_gateway(shell, self.configfile)
            success, revision, status = self.create_doc(shell, self.doc_id, self.doc_content)
            self.assertTrue(success)
            self.assertTrue(self.check_message_in_gatewaylog(shell,
                            'Error attempting to post to url http://localhost:9999'))
            shell.disconnect()

    def webHookFilter(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            self.start_sync_gateway(shell, self.configfile)
            if self.extra_param:
                success, revision, status = self.create_doc(shell, self.doc_id, self.doc_content)
            else:
                success, revision, status = self.create_doc_no_post(shell, self.doc_id, self.doc_content)
            shell.disconnect()

    def webHookFilterAlwaysTrue(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            self.start_sync_gateway(shell, self.configfile)
            success, revision, status = self.create_doc(shell, self.doc_id, self.doc_content)
            self.assertTrue(success)
            shell.disconnect()

    def webHookFilterAlwaysFalse(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            self.start_sync_gateway(shell, self.configfile)
            success, revision, status = self.create_doc_no_post(shell, self.doc_id, self.doc_content)
            self.assertTrue(success)
            shell.disconnect()

    def webHookFilterNoReturn(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            self.start_sync_gateway(shell, self.configfile)
            success, revision, status = self.create_doc_no_post(shell, self.doc_id, self.doc_content)
            self.assertTrue(success)
            self.assertTrue(self.check_message_in_gatewaylog(shell,
                            'Error calling webhook filter function: Validate function returned non-boolean value'))
            shell.disconnect()

    def webHookFilterPartNoReturn(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            self.start_sync_gateway(shell, self.configfile)
            if self.extra_param:
                success, revision, status = self.create_doc(shell, self.doc_id, self.doc_content)
            else:
                success, revision, status = self.create_doc_no_post(shell, self.doc_id, self.doc_content)
            self.assertTrue(success)
            shell.disconnect()

    def webHookFilterPartNoReturn(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            self.start_sync_gateway(shell, self.configfile)
            if self.extra_param:
                success, revision, status = self.create_doc(shell, self.doc_id, self.doc_content)
            else:
                success, revision, status = self.create_doc_no_post(shell, self.doc_id, self.doc_content)
            self.assertTrue(success)
            shell.disconnect()

    def webHookFilterBadReturn(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            self.start_sync_gateway(shell, self.configfile)
            if self.extra_param:
                success, revision, status = self.create_doc(shell, self.doc_id, self.doc_content)
            else:
                success, revision, status = self.create_doc_no_post(shell, self.doc_id, self.doc_content)
            self.assertTrue(success)
            shell.disconnect()

    def webHookFilterBadFilter(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            self.start_sync_gateway(shell, self.configfile)
            success, revision, status = self.create_doc_no_post(shell, self.doc_id, self.doc_content)
            self.assertTrue(success)
            self.assertTrue(self.check_message_in_gatewaylog(shell,
                            'Event queue worker sending event Document change event for doc id'))
            self.assertTrue(self.check_message_in_gatewaylog(shell,
                            'Error calling webhook filter function: ReferenceError: doc2 is not defined'))
            shell.disconnect()

    def webHookFilterBadFilter2(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            self.start_sync_gateway(shell, self.configfile)
            success, revision, status = self.create_doc_no_post(shell, self.doc_id, self.doc_content)
            self.assertTrue(success)
            self.assertTrue(self.check_message_in_gatewaylog(shell,
                            'Event queue worker sending event Document change event for doc id'))
            self.assertTrue(self.check_message_in_gatewaylog(shell,
                            'function processing aborted: SyntaxError: Unexpected token'))
            shell.disconnect()

    def webHookBadEvent(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            self.start_sync_gateway(shell, self.configfile)
            success, revision, status = self.create_doc_no_post(shell, self.doc_id, self.doc_content)
            self.assertTrue(success)
            self.assertFalse(self.check_message_in_gatewaylog(shell,
                            'Event queue worker sending event Document change event for doc id'))
            self.assertTrue(self.check_message_in_gatewaylog(shell,
                            'FATAL:'))
            shell.disconnect()

    def webHookBadEventHandlers(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            self.start_sync_gateway(shell, self.configfile)
            success, revision, status = self.create_doc_no_post(shell, self.doc_id, self.doc_content)
            self.assertTrue(success)
            self.assertFalse(self.check_message_in_gatewaylog(shell,
                            'Event queue worker sending event Document change event for doc id'))
            self.assertTrue(self.check_message_in_gatewaylog(shell,
                            'FATAL:'))
            shell.disconnect()

    def webHookBadEventHandler(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            self.start_sync_gateway(shell, self.configfile)
            success, revision, status = self.create_doc_no_post(shell, self.doc_id, self.doc_content)
            self.assertFalse(success)
            self.assertFalse(self.check_message_in_gatewaylog(shell,
                            'Event queue worker sending event Document change event for doc id'))
            self.assertTrue(self.check_message_in_gatewaylog(shell,
                            'FATAL: Error opening database: Unknown event handler type webhookkkkkkkkkkk'))
            shell.disconnect()

    def webHookBadUrlProtocol(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            self.start_sync_gateway(shell, self.configfile)
            success, revision, status = self.create_doc_no_post(shell, self.doc_id, self.doc_content)
            self.assertTrue(success)
            self.assertFalse(self.check_message_in_gatewaylog(shell,
                            'Event queue worker sending event Document change event for doc id'))
            self.assertTrue(self.check_message_in_gatewaylog(shell,
                           'unsupported protocol scheme'))
            shell.disconnect()

    def webHookBadUrl(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            self.start_sync_gateway(shell, self.configfile)
            success, revision, status = self.create_doc_no_post(shell, self.doc_id, self.doc_content)
            self.assertTrue(success)
            self.assertFalse(self.check_message_in_gatewaylog(shell,
                            'Event queue worker sending event Document change event for doc id'))
            self.assertTrue(self.check_message_in_gatewaylog(shell,
                            'FATAL:'))
            shell.disconnect()

    def webHookNoUrl(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            self.start_sync_gateway(shell, self.configfile)
            success, revision, status = self.create_doc_no_post(shell, self.doc_id, self.doc_content)
            self.assertTrue(success)
            self.assertFalse(self.check_message_in_gatewaylog(shell,
                            'Event queue worker sending event Document change event for doc id'))
            self.assertTrue(self.check_message_in_gatewaylog(shell,
                            'FATAL:'))
            shell.disconnect()

    def webHookTimeoutDefault(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            self.start_sync_gateway(shell, self.configfile)
            success, revision, status = self.create_doc_delay(shell, self.doc_id, self.doc_content, 60)
            # Issue 679
            self.assertFalse(success)
            shell.disconnect()

    def webHookTimeout5Seconds(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            self.start_sync_gateway(shell, self.configfile)
            success, revision, status = self.create_doc_delay(shell, self.doc_id, self.doc_content, 5)
            # Issue 679
            self.assertFalse(success)
            shell.disconnect()

    def webHookTimeoutNegative(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            self.start_sync_gateway(shell, self.configfile)
            success, revision, status = self.create_doc_delay(shell, self.doc_id, self.doc_content, 60)
            self.assertFalse(success)
            self.assertTrue(self.check_message_in_gatewaylog(shell,
                            'cannot unmarshal number -1 into Go value of type uint6'))
            shell.disconnect()

    def webHookTimeoutBadValue(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            self.start_sync_gateway(shell, self.configfile)
            success, revision, status = self.create_doc_delay(shell, self.doc_id, self.doc_content, 60)
            self.assertFalse(success)
            self.assertTrue(self.check_message_in_gatewaylog(shell,
                            'cannot unmarshal string into Go value of type uint64'))
            shell.disconnect()

    def webHookProcesses(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            self.start_sync_gateway(shell, self.configfile)
            success, revision, status = self.create_doc(shell, self.doc_id, self.doc_content)
            self.assertTrue(success)
            self.assertTrue(self.check_message_in_gatewaylog(shell,
                            'Starting event manager with max processes:1000, wait time:2 ms'))
            shell.disconnect()

    def webHookProcessesBadMax(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            self.start_sync_gateway(shell, self.configfile)
            success, revision, status = self.create_doc_no_post(shell, self.doc_id, self.doc_content)
            self.assertFalse(success)
            self.assertTrue(self.check_message_in_gatewaylog(shell,
                            'cannot unmarshal string into Go value of type uint'))
            shell.disconnect()

    def webHookProcessesBadWait(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            self.start_sync_gateway(shell, self.configfile)
            success, revision, status = self.create_doc(shell, self.doc_id, self.doc_content)
            self.assertTrue(success)
            self.assertTrue(self.check_message_in_gatewaylog(shell,
                            'Error parsing wait_for_process from config, using default strconv.ParseInt: parsing "zzzzz": invalid syntax'))
            self.assertTrue(self.check_message_in_gatewaylog(shell,
                            'Events: Starting event manager with max processes:500, wait time:5 ms'))
            shell.disconnect()

    def webHookProcessesLarge(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            self.start_sync_gateway(shell, self.configfile)
            long_str = "x" * 950
            content = '{0}"a":"{1}" {2}'.format("{", long_str, "}")
            for i in range(20):
                success, revision, status = self.create_doc_silent(shell, str(i), content)
                if not success:
                    self.assertTrue(self.check_message_in_gatewaylog(shell,
                                    'Event queue full - discarding event'))
                else:
                    self.assertTrue(success)
            shell.disconnect()
