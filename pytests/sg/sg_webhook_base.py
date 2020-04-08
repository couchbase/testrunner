import logger
import json
from TestInput import TestInputSingleton
from remote.remote_util import RemoteMachineShellConnection, RemoteMachineHelper
import re
import time
import os
from pytests.sg.sg_base import GatewayBaseTest


class GatewayWebhookBaseTest(GatewayBaseTest):
    def setUp(self):
        super(GatewayWebhookBaseTest, self).setUp()
        self.log = logger.Logger.get_logger()
        self.input = TestInputSingleton.input
        self.extra_param = self.input.param("extra_param", "")
        self.configfile = self.input.param("config", "config_webhook_basic.json")
        self.doc_id = self.input.param("doc_id", "doc1")
        self.doc_content = self.input.param("doc_content", "{'a':1}")
        self.expected_error = self.input.param("expected_error", "")
        self.servers = self.input.servers
        self.master = self.servers[0]
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            if self.case_number == 1:
                shell.execute_command("rm -rf {0}/tmp/*".format(self.folder_prefix))
                # will install sg only the first time
                self.install(shell)
                pid = self.is_sync_gateway_process_running(shell)
                self.assertNotEqual(pid, 0)
                exist = shell.file_exists('{0}/tmp/'.format(self.folder_prefix), 'gateway.log')
                self.assertTrue(exist)
                shell.copy_files_local_to_remote('pytests/sg/resources', '/tmp')
            self.start_simpleServe(shell)
            shell.disconnect()

    def start_sync_gateway(self, shell, config_filename):
        self.kill_processes_gateway(shell)
        self.log.info('=== start_sync_gateway with config file {0}.'.format(config_filename))
        output, error = shell.execute_command('cat {0}/tmp/{1}'.format(self.folder_prefix, config_filename))
        shell.log_command_output(output, error)
        type = shell.extract_remote_info().type.lower()
        if type == 'windows':
            output, error = shell.execute_command_raw('{0}/sync_gateway.exe'
                                                      ' c:/tmp/{1} > {2}/tmp/gateway.log 2>&1 &'.
                                                      format(self.installed_folder, config_filename,
                                                             self.folder_prefix))
        else:
            output, error = shell.execute_command_raw('nohup /opt/couchbase-sync-gateway/bin/sync_gateway'
                                                      ' /tmp/{0} >/tmp/gateway.log 2>&1 &'.format(config_filename))
        shell.log_command_output(output, error)
        obj = RemoteMachineHelper(shell).is_process_running('sync_gateway')
        if obj and obj.pid:
            self.log.info('start_sync_gateway - Sync Gateway is running with pid of {0}'.format(obj.pid))
            if not shell.file_exists('{0}/tmp/'.format(self.folder_prefix), 'gateway.log'):
                self.log.info('start_sync_gateway - Fail to find gateway.log')
                return False
            else:
                return True
        else:
            self.log.info('start_sync_gateway - Sync Gateway is NOT running')
            output, error = shell.execute_command('cat {0}/tmp/gateway.log'.format(self.folder_prefix))
            shell.log_command_output(output, error)
            return False

    def send_request(self, shell, method, doc_name, content):
        self.info = shell.extract_remote_info()
        cmd = 'curl -X {0} http://{1}:4984/db/{2}{3}'.format(method, self.info.ip, doc_name, content)
        type =shell.extract_remote_info().type.lower()
        if type == 'windows':
            cmd = "/cygdrive/c/cygwin64/bin/curl.exe -X {0} http://{1}:4984/db/{2}{3}".format(method, self.info.ip, doc_name, content)
        output, error = shell.execute_command_raw(cmd)
        shell.log_command_output(output, error)
        if not output:
            self.log.info('No output from issuing {0}'.format(cmd))
            return None
        else:
            self.log.info('Output - {0}'.format(output[-1]))
            dic = json.loads(output[-1])
            return dic

    def check_post_contents(self, doc_content, post_dic):
        doc_content_dic = json.loads(doc_content)
        if not doc_content_dic:
            return True
        for key, value in list(doc_content_dic.items()):
            if not post_dic[key]:
                self.log.info('check_post_contents found missing key {0}'.format(key))
                return False
            else:
                if post_dic[key] != doc_content_dic[key]:
                    self.log.info('check_post_contents found unmatched value - post_dic({0}), doc_content_dic({1})'
                                  .format(post_dic[key], doc_content_dic[key]))
                    return False
                else:
                    return True

    def check_post_attachment(self, attachment_filename, post_dic):
        if not post_dic['_attachments']:
            self.log.info('check_post_attachment found missing key - \'_attachments\'')
            return False
        else:
            if not post_dic['_attachments'][attachment_filename]:
                self.log.info('check_post_attachment does not have the attachment for {0}'
                              .format(attachment_filename))
                return False
            else:
                return True

    def check_post_deleted(self, post_dic):
        if not post_dic['_deleted']:
            self.log.info('check_post_deleted found missing key - \'_deleted\'')
            return False
        else:
            return True

    def check_http_server(self, shell, doc_id, revision, doc_content, attachment_filename, deleted, go_logfiles,
                          silent):
        if not go_logfiles:
            go_logfiles = ['{0}/tmp/simpleServe.txt'.format(self.folder_prefix)]
        for go_logfile in go_logfiles:
            head, tail = os.path.split(go_logfile)
            logs = shell.read_remote_file(head, tail)[-1:]  # last line
            if not silent:
                self.log.info("check_http_server - checking {0} - {1}".format(go_logfile, logs[0]))
            else:
                self.log.info("check_http_server - checking {0} - {1} ...".format(go_logfile, logs[0][:10]))
            simple_string = re.split(' ', logs[0])
            try:
                post_dic = json.loads(simple_string[2])
            except ValueError:
                self.log.info("check_http_server - log({0}) does not have a valid json format- {1}"
                              .format(go_logfile, simple_string[2]))
                return False
            if not post_dic:
                self.log.info('check_http_server - log({0}) indicates did not get the post for rev '
                              .format(go_logfile, revision))
                return False
            elif post_dic['_rev'] != revision:
                self.log.info('check_http_server - log({0}) indicates post revision is {1}, but expecting {2}'
                              .format(go_logfile, post_dic['_rev'], revision))
                return False
            elif post_dic['_id'] != doc_id:
                self.log.info('check_http_server - log({0}) indicates post id is {1}, but expecting {2}'
                              .format(go_logfile, post_dic['_id'], doc_id))
                return False
            elif doc_content and (not self.check_post_contents(doc_content, post_dic)):
                self.log.info('check_http_server - log({0}) indicates post content is {1}, '
                              'but expecting {2}'
                              .format(go_logfile, post_dic, doc_content))
                return False
            elif attachment_filename and (not self.check_post_attachment(attachment_filename, post_dic)):
                self.log.info('check_http_server - log({0}) indicates post content is {1}, '
                              'but expecting {2}'
                              .format(go_logfile, post_dic, doc_content))
                return False
            elif deleted and (not self.check_post_deleted(post_dic)):
                self.log.info('check_http_server - log({0}) indicates post content does not have '
                              'correct value for _deleted {1}'
                              .format(go_logfile, post_dic))
                return False
        return True

    def create_doc_internal(self, shell, doc_id, doc_content, go_logfiles, should_post, sleep_seconds,
                            expect_status, silent):
        if not silent:
            self.log.info('=== Creating a doc({0}) - {1}'.format(doc_id, doc_content))
        else:
            self.log.info('=== Creating a doc({0}) - {1}...'.format(doc_id, doc_content[:5]))
        send_requst_dic = self.send_request(shell, 'PUT', doc_id, ' -d \'{0}\' -H "Content-Type: application/json"'
                                            .format(doc_content))
        if not send_requst_dic or not send_requst_dic['rev']:
            self.log.info('create_doc - create_doc failed - {0}'.format(send_requst_dic))
            return False, '', ''
        else:
            revision = send_requst_dic['rev']
            if not revision:
                self.log.info('create_doc - create_doc failed - {0}'.format(send_requst_dic))
                return False, '', ''
            else:
                if sleep_seconds:
                    self.log.info('create_doc - start sleep for {0} seconds'.format(sleep_seconds))
                    time.sleep(sleep_seconds)
                    self.log.info('create_doc - end sleep for {0} seconds'.format(sleep_seconds))
                status = self.check_status_in_gateway_log(shell)
                if expect_status and status != '200' and should_post:
                    self.log.info('create_doc - gateway log shows status code of {0}, but expecting status code of 200 '
                                  .format(status))
                    return False, revision, status
                elif not self.check_http_server(shell, doc_id, revision, doc_content, None, False, go_logfiles, silent):
                    if should_post:
                        self.log.info('create_doc - Document {0} is created successfully with rev = {1} '
                                      'but NOT posted as expected'.format(doc_id, revision))
                        return False, revision, status
                    else:
                        self.log.info('create_doc - Document {0} is created successfully with rev = {1} '
                                      'and not posted as expected'.format(doc_id, revision))
                        return True, revision, status
                elif should_post:
                    self.log.info('create_doc - Document {0} is created successfully with rev = {1} '
                                  'and posted as expected'.format(doc_id, revision))
                    return True, revision, status
                else:
                    self.log.info('create_doc - Document {0} is created successfully with rev = {1} '
                                  'but is posted when it is NOT supposed to be posted'.format(doc_id, revision))
                    return False, revision, status

    def create_doc(self, shell, doc_id, doc_content):
        self.log.info('=== create_doc id({0}), content({1})'.format(doc_id, doc_content))
        return self.create_doc_internal(shell, doc_id, doc_content, None, True, None, True, False)

    def create_doc_logfiles(self, shell, doc_id, doc_content, go_logfiles):
        self.log.info('=== create_doc_logfile_to_check id({0}), content({1}) logs({2})'
                      .format(doc_id, doc_content, go_logfiles))
        return self.create_doc_internal(shell, doc_id, doc_content, go_logfiles, True, None, True, False)

    def create_doc_no_post(self, shell, doc_id, doc_content):
        self.log.info('=== create_doc_logfile_to_check id({0}), content({1})'
                      .format(doc_id, doc_content))
        return self.create_doc_internal(shell, doc_id, doc_content, None, False, None, True, False)

    def create_doc_delay(self, shell, doc_id, doc_content, timeout):
        self.log.info('=== create_doc_delay id({0}), content({1})'
                      .format(doc_id, doc_content))
        a = doc_content.split(":")
        delay = int(a[1][:-1])
        if delay <= timeout:
            self.log.info('create_doc_delay http server response before timeout and gateload log status should be ok')
            return self.create_doc_internal(shell, doc_id, doc_content, None, True, delay, True, False)
        else:
            self.log.info('create_doc_delay http server response pass timeout and gateload log status should not be ok')
            return self.create_doc_internal(shell, doc_id, doc_content, None, True, delay, False, False)

    def create_doc_silent(self, shell, doc_id, doc_content):
        self.log.info('=== create_doc_silent id({0}), content({1}...)'.format(doc_id, doc_content[:10]))
        return self.create_doc_internal(shell, doc_id, doc_content, None, True, None, True, True)

    def update_doc(self, shell, doc_id, doc_content, revision):
        self.log.info('=== Updating a doc({0}) - {1}'.format(doc_id, doc_content))
        send_requst_dic = self.send_request(shell, 'PUT', doc_id, '?rev=\'{0}\' -d \'{1}\' -H "Content-Type: '
                                                                  'application/json"'.format(revision, doc_content))
        revision = send_requst_dic['rev']
        if not revision:
            self.log.info('update_doc failed - {0}'.format(send_requst_dic))
            return False, '', ''
        else:
            status = self.check_status_in_gateway_log(shell)
            if status != '200':
                self.log.info('update_doc - gateway log shows status code of {0}, but expecting status code of 200 '
                              .format(status))
                return False, revision, status
            else:
                if not self.check_http_server(shell, doc_id, revision, doc_content, None, False, None, False):
                    return False, revision, status
                else:
                    self.log.info('update_doc - Document {0} is updated successfully with rev = {1}'
                                  .format(doc_id, revision))
                    return True, revision, status

    def update_attachment(self, shell, doc_id, doc_content, attachment_filename, revision):
        self.log.info('=== update_doc_with_attachment a doc({0}) - {1}'.format(doc_id, attachment_filename))
        send_requst_dic = self.send_request(shell, 'PUT', doc_id, '/{0}?rev={1}'
                                            .format(attachment_filename, revision, ))
        revision = send_requst_dic['rev']
        if not revision:
            self.log.info('update_doc_with_attachment failed - {0}'.format(send_requst_dic))
            return False, '', ''
        else:
            status = self.check_status_in_gateway_log(shell)
            if status != '200':
                self.log.info('update_doc_with_attachment - gateway log shows status code of {0}, but expecting '
                              'status code of 200 '
                              .format(status))
                return False, revision, status
            else:
                if not self.check_http_server(shell, doc_id, revision, doc_content, attachment_filename,
                                              False, None, False):
                    return False, revision, status
                else:
                    self.log.info('update_doc_with_attachment - Document {0} is updated successfully with rev = {1}'
                                  .format(doc_id, revision))
                    return True, revision, status

    def delete_doc(self, shell, doc_id, revision):
        self.log.info('=== delete_doc a doc({0})'.format(doc_id))
        send_requst_dic = self.send_request(shell, 'DELETE', doc_id, '?rev={0}'.format(revision))
        revision = send_requst_dic['rev']
        if not revision:
            self.log.info('delete_doc failed - {0}'.format(send_requst_dic))
            return False, '', ''
        else:
            status = self.check_status_in_gateway_log(shell)
            if status != '200':
                self.log.info('delete_doc - gateway log shows status code of {0}, but expecting status code of 200 '
                              .format(status))
                return False, revision, status
            elif not self.check_http_server(shell, doc_id, revision, None, None, True, None, False):
                return False, revision, status
            else:
                self.log.info('delete_doc - Document {0} is updated successfully with rev = {1}'
                              .format(doc_id, revision))
                return True, revision, status
