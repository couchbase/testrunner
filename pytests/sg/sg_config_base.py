from sg.sg_base import GatewayBaseTest
import json
import urllib.request, urllib.parse, urllib.error
try:
    from jinja2 import Environment, FileSystemLoader
except ImportError as e:
    print('please install required modules:', e)
    raise
from remote.remote_util import RemoteMachineShellConnection, RemoteMachineHelper


class GatewayConfigBaseTest(GatewayBaseTest):

    def setUp(self):
        super(GatewayConfigBaseTest, self).setUp()
        self.template = self.input.param("template", "")
        self.feedtype = self.input.param("feedtype", "tap")
        self.config = self.input.param("config", "gateway_config.json")
        self.doc_id = self.input.param("doc_id", "doc1")
        self.doc_content = self.input.param("doc_content", '"a":1')
        self.expected_error = self.input.param("expected_error", "")
        self.expected_stdout = self.input.param("expected_stdout", "")
        self.not_expected_log = self.input.param("not_expected_log", "")
        self.bucket = self.input.param("bucket", "default")
        self.password = self.input.param("password", "")
        self.port = self.input.param("port", "8091")
        self.db_name = self.input.param("db_name", "db")
        self.sync_port = self.input.param("sync_port", "4984")
        self.admin_port = self.input.param("admin_port", "4985")
        self.dbs = self.input.clusters
        self.db_master = self.dbs[0]
        # parameters for accounts
        self.admin_channels = self.input.param("admin_channels", "")
        self.admin_roles = self.input.param("admin_roles", "")
        self.all_channels = self.input.param("all_channels", "")
        self.doc_channels = self.input.param("doc_channels", "")
        self.disabled = self.input.param("disabled", "")
        self.email = self.input.param("email", "")
        self.user_name = self.input.param("user_name", "")
        self.role_name = self.input.param("role_name", "")
        self.role_channels = self.input.param("role_channels", "")
        self.roles = self.input.param("roles", "")
        self.expect_channels = self.input.param("expect_channels", "")
        self.param = self.input.param("param", "").replace("LOCAL_IP", self.master.ip)
        self.expected_log = self.input.param("expected_log", "").replace("LOCAL_IP", self.master.ip)

    def generate_sync_gateways_config(self, template_filename, output_config_file):
        loader = FileSystemLoader('pytests/sg/resources')
        env = Environment(loader=loader)
        template = env.get_template('{0}'.format(template_filename))
        if self.password:
            password_str = urllib.parse.quote('{0}'.format(self.bucket)) +':{0}@'.format(self.password)
        else:
            password_str = ''
        if template_filename == 'gateway_config_template.json':
            with open(output_config_file, 'w') as fh:
                fh.write(template.render(
                    password=password_str,
                    port=self.port,
                    bucket=self.bucket,
                    feedtype=self.feedtype,
                    db_ip=self.db_master[0].ip
                ))
        elif template_filename == 'gateway_config_template_nobucket.json':
            with open(output_config_file, 'w') as fh:
                fh.write(template.render(
                    password=password_str,
                    port=self.port,
                    db_ip=self.db_master[0].ip
                ))
        else:
            with open(output_config_file, 'w') as fh:
                fh.write(template.render(
                    password=password_str,
                    port=self.port,
                    db_ip=self.db_master[0].ip
                ))

    def start_sync_gateway(self, shell):
        self.log.info('=== start_sync_gateway_internal')
        self.kill_processes_gateway(shell)
        output, error = shell.execute_command_raw('ps -ef | grep sync_gateway')
        shell.log_command_output(output, error)
        if self.config != '':
            self.config = '/tmp/{0}'.format(self.config)
            output, error = shell.execute_command('cat {0}'.format(self.config))
            shell.log_command_output(output, error)
        output, error = shell.execute_command(
                'nohup /opt/couchbase-sync-gateway/bin/sync_gateway {0} {1} >/tmp/gateway.log 2>&1 &'
                .format(self.param, self.config))
        shell.log_command_output(output, error)
        if not self.expected_error:
            obj = RemoteMachineHelper(shell).is_process_running('sync_gateway')
            if obj and obj.pid:
                self.log.info('Sync Gateway is running with pid of {0}'.format(obj.pid))
                if not shell.file_exists('/tmp/', 'gateway.log'):
                    self.log.info('Fail to find gateway.log')
                else:
                    return True
            else:
                self.log.info('Sync Gateway is NOT running')
                output, error = shell.execute_command_raw('cat /tmp/gateway.log')
                shell.log_command_output(output, error)
            return False
        else:
            return self.check_message_in_gatewaylog(shell, self.expected_error)

    def start_sync_gateway_template(self, shell, template):
        self.log.info('=== start_sync_gateway_template - template file {0}'.format(template))
        if template:
            self.config_file = 'gateway_config_test.json'
            self.generate_sync_gateways_config(template, 'pytests/sg/resources/{0}'.format(self.config))
            shell.copy_files_local_to_remote('pytests/sg/resources', '/tmp')
        else:
            self.config_file = ''
        return self.start_sync_gateway(shell)

    def send_request(self, shell, cmd):
        output, error = shell.execute_command_raw(cmd)
        if not output:
            shell.log_command_output(output, error)
            self.log.info('No output from issuing {0}'.format(cmd))
            return None, error
        else:
            output_str = ''.join(output)
            self.log.info('Output - {0}'.format(output_str))
            return output_str, error

    def create_doc(self, shell):
        self.log.info('=== create_doc({0}) - {1}'.format(self.doc_id, self.doc_content))
        self.info = shell.extract_remote_info()
        doc_content_str = '{{"channels":[{0}], {1}}}'.format(self.doc_channels, self.doc_content) \
                          if self.doc_channels else '{{{0}}}'.format(self.doc_content)
        cmd = 'curl -X PUT http://{0}:{1}/{2}/{3} -d \'{4}\' -H "Content-Type: application/json"'\
              .format(self.info.ip, self.sync_port, self.db_name, self.doc_id, doc_content_str)
        send_requst_ret, errors = self.send_request(shell, cmd)
        if not send_requst_ret:
             self.log.info('create_doc does not have output')
             return False, ''
        elif 'error' in send_requst_ret:
             self.log.info('create_doc return error - {0}'.format(send_requst_ret))
             return False, ''
        elif errors and 'curl: (3)' in errors[1]:
            return False, errors
        else:
            send_requst_dic = json.loads(send_requst_ret)
            revision = send_requst_dic['rev']
            if not revision:
                self.log.info('create_doc failed - {0}'.format(send_requst_dic))
                return False, ''
            else:
                return True, revision

    def get_all_docs(self, shell):
        self.log.info('=== get_all_docs')
        self.info = shell.extract_remote_info()
        cmd = 'curl -X GET http://{0}:{1}/{2}/_all_docs?channels=true'\
              .format(self.info.ip, self.sync_port, self.db_name)
        send_requst_ret, _ = self.send_request(shell, cmd)
        if not send_requst_ret:
             self.log.info('get_all_docs does not have output')
             return False
        elif 'error' in send_requst_ret:
             self.log.info('get_all_docs return error - {0}'.format(send_requst_ret))
             return False
        else:
            send_requst_dic = json.loads(send_requst_ret)
            rows = send_requst_dic['rows']
            row1 = rows[0]
            channels = row1['value']['channels']
            input_list = self.doc_channels.split(',')
            if len(channels) != len(input_list):
                self.log.info('get_all_docs number of items in channels({0}) is different from input doc_channels({1})'
                              .format(len(channels), len(input_list)))
                return False
            for item in channels:
                if item.encode('ascii', 'ignore') not in self.doc_channels:
                    self.log.info('get_all_docs channels missing item - {0}'.format(item.encode('ascii', 'ignore')))
                    return False
            if not channels:
                self.log.info('get_all_docs does not return channels - {0}'.format(send_requst_dic))
                return False
            else:
                return True

    def delete_doc(self, shell, revision):
        self.log.info('=== delete_doc a doc({0})'.format(self.doc_id))
        self.info = shell.extract_remote_info()
        cmd = 'curl -X DELETE http://{0}:{1}/{2}/{3}?rev={4}'.format(self.info.ip, self.sync_port, self.db_name, self.doc_id, revision)
        send_requst_ret, _ = self.send_request(shell, cmd)
        if send_requst_ret and 'error' in send_requst_ret:
            self.log.info('delete_doc failed - {0}'.format(send_requst_ret))
            return False
        else:
            self.log.info('delete_doc - Document {0} is updated successfully with rev = {1}'
                          .format(self.doc_id, revision))
            return True

    def create_user(self, shell):
        self.log.info('=== create_user({0}) - admin_channels({1}) all_channels({2}) disabled({3}) password({4})'
                      ' admin_roles({5}) roles({6})'.format(self.user_name, self.admin_channels, self.all_channels, self.disabled,
                                                        self.password, self.admin_roles, self.roles))
        self.info = shell.extract_remote_info()
        disabled_str = ''
        if self.disabled:
            if self.disabled == True:
                disabled_str = '"disabled":true,'
            elif self.disabled == False:
                disabled_str = '"disabled":false,'
            else:
                disabled_str = '"disabled":"{0}",'.format(self.disabled)
        admin_channels_str = '"admin_channels":[{0}],'.format(self.admin_channels) if self.admin_channels else ''
        all_channels_str = '"all_channels":[{0}],'.format(self.all_channels) if self.all_channels else ''
        password_str = '"password":"{0}",'.format(self.password) if self.password else ''
        admin_roles_str = '"admin_roles":[{0}],'.format(self.admin_roles) if self.admin_roles else ''
        roles_str = '"roles":[{0}],'.format(self.roles) if self.roles else ''
        email_str = '"email":"{0}"'.format(self.email) if self.email else ''
        str = '{0}{1}{2}{3}{4}{5}{6}'.format(admin_channels_str, all_channels_str, disabled_str,
                                                   password_str, admin_roles_str, roles_str, email_str )
        if str and str[-1] == ',': str = str[:-1]
        cmd = "curl -X PUT http://{0}:{1}/{2}/_user/{3} --data '{{{4}}}'"\
              .format(self.info.ip, self.admin_port, self.db_name, self.user_name, str)
        send_requst_ret, _ = self.send_request(shell, cmd)
        if self.expected_stdout:
            if send_requst_ret and self.expected_stdout in send_requst_ret:
                return True
            else:
                self.log.info('create_user expected_stdout({0}), but get {1}'.format(self.expected_stdout, send_requst_ret))
                return False
        else:
            if send_requst_ret:
                self.log.info('create_user got error - {0}'.format(send_requst_ret))
                return False
            else:
                return True

    def get_users(self, shell):
        self.log.info('=== get_users')
        self.info = shell.extract_remote_info()
        cmd = 'curl -X GET http://{0}:{1}/{2}/_user/'.format(self.info.ip, self.admin_port, self.db_name)
        send_requst_ret, _ = self.send_request(shell, cmd)
        if send_requst_ret and 'error' in send_requst_ret:
            self.log.info('get_users failed - {0}'.format(send_requst_ret))
            return False
        else:
            return True

    def get_user(self, shell):
        self.log.info('=== get_user ({0})'.format(self.user_name))
        self.info = shell.extract_remote_info()
        cmd = 'curl -X GET http://{0}:{1}/{2}/_user/{3}'.\
            format(self.info.ip, self.admin_port, self.db_name, self.user_name)
        send_requst_ret, _ = self.send_request(shell, cmd)
        if send_requst_ret and 'error' in send_requst_ret:
            self.log.info('get_user failed - {0}'.format(send_requst_ret))
            return False
        else:
            send_requst_dic = json.loads(send_requst_ret)
            if self.admin_channels and 'admin_channels' in send_requst_dic:
                input_list = self.admin_channels.split(',')
                if len(send_requst_dic['admin_channels']) != len(input_list):
                    self.log.info('get_user number of items in admin_channels({0}) is different from input({1})'
                                  .format(len(send_requst_dic['admin_channels']), len(input_list)))
                    return False
                for item in send_requst_dic['admin_channels']:
                    if item.encode('ascii', 'ignore') not in self.admin_channels:
                        self.log.info('get_user admin_channels missing item - {0}'.format(item.encode('ascii', 'ignore')))
                        return False
            if self.admin_roles and 'admin_roles' in send_requst_dic and send_requst_dic['admin_roles'] != self.admin_roles:
                input_list = self.admin_roles.split(',')
                if len(send_requst_dic['admin_roles']) != len(input_list):
                    self.log.info('get_user number of items in admin_roles({0}) is different from input({1})'
                                  .format(len(send_requst_dic['admin_roles']), len(input_list)))
                    return False
                for item in send_requst_dic['admin_roles']:
                    if item.encode('ascii', 'ignore') not in self.admin_roles:
                        self.log.info('get_user admin_roles missing item - {0}'.format(item.encode('ascii', 'ignore')))
                        return False
                if len(send_requst_dic['roles']) != len(input_list):
                    self.log.info('get_user number of items in roles({0}) is different from admin_roles input({1})'
                                  .format(len(send_requst_dic['admin_roles']), len(input_list)))
                    return False
                for item in send_requst_dic['roles']:
                    if item.encode('ascii', 'ignore') not in self.admin_roles:
                        self.log.info('get_user roles missing item set by admin_roles - {0}'.format(item.encode('ascii', 'ignore')))
                        return False
            if self.expect_channels:
                input_list = self.expect_channels.split(',')
                if len(send_requst_dic['all_channels']) != len(input_list) + 1:
                    self.log.info('get_user number of items in all_channels({0}) is different from expect_channels({1}+1)'
                                  .format(len(send_requst_dic['all_channels']), len(input_list)))
                    return False
                for item in send_requst_dic['all_channels']:
                    if item.encode('ascii', 'ignore') not in self.expect_channels and item.encode('ascii', 'ignore') != '!':
                        self.log.info('get_user missing channel from self.expect_channels({0})'.format(item.encode('ascii', 'ignore')))
                        return False
            if self.roles and 'roles' in send_requst_dic and send_requst_dic['roles'] != self.roles:
                for item in send_requst_dic['roles']:
                    if item.encode('ascii', 'ignore') in self.roles:
                        self.log.info('get_user roles setting {0} should have no effect'.format(item.encode('ascii', 'ignore')))
                        return False
            if self.email:
                if '@' in self.email and '.' in self.email:
                    if send_requst_dic['email'] != self.email:
                        self.log.info('get_user found unexpected value for email - {0}, expecting {1}'
                              .format(send_requst_dic['email'], self.email))
                        return False
                else:
                    if 'email' in send_requst_dic:
                        self.log.info('get_user found email - {0}, but the input email({1}) should not be accepted'
                              .format(send_requst_dic['email'], self.email))
                        return False
            if 'password' in send_requst_ret:
                self.log.info('Password should not be returned.')
                return False
            if self.disabled and send_requst_dic['disabled'] != self.disabled:
                self.log.info('get_user found unexpected value for disabled - {0}, expecting {1}'
                              .format(send_requst_dic['disabled'], self.disabled))
            return True

    def delete_user(self, shell):
        self.log.info('=== delete_user ({0})'.format(self.user_name))
        self.info = shell.extract_remote_info()
        cmd = 'curl -X DELETE http://{0}:{1}/{2}/_user/{3}'.\
            format(self.info.ip, self.admin_port, self.db_name, self.user_name)
        send_requst_ret, _ = self.send_request(shell, cmd)
        if send_requst_ret and 'error' in send_requst_ret:
            self.log.info('delete_user failed - {0}'.format(send_requst_ret))
            return False
        else:
            return True


    def create_role(self, shell, role_name, admin_channels):
        self.log.info('=== create_role({0}) - admin_channels({1})'.format(role_name, admin_channels))
        self.info = shell.extract_remote_info()
        admin_channels_str = '"admin_channels":[{0}]'.format(admin_channels) if admin_channels else ''
        cmd = "curl -X PUT http://{0}:{1}/{2}/_role/{3} --data '{{{4}}}'"\
              .format(self.info.ip, self.admin_port, self.db_name, role_name, admin_channels_str)
        send_requst_ret, _ = self.send_request(shell, cmd)
        if send_requst_ret:
            self.log.info('create_role got error - {0}'.format(send_requst_ret))
            return False
        else:
            return True

    def parse_input_create_roles(self, shell):
        if self.role_channels:
            roles = self.role_channels.split(';')
            for index, role in enumerate(roles):
                role_channels = role.split('$')
                channels = ''
                for index, item in enumerate(role_channels):
                    if index == 0:
                        role_name = item
                    else:
                        channels = '{0}, "{1}"'.format(channels, item) if channels else '"{0}"'.format(item)
                if not self.create_role(shell, role_name, channels):
                    return False
            return True

    def get_roles(self, shell):
        self.log.info('=== get_roles')
        self.info = shell.extract_remote_info()
        cmd = 'curl -X GET http://{0}:{1}/{2}/_role/'.format(self.info.ip, self.admin_port, self.db_name)
        send_requst_ret, _ = self.send_request(shell, cmd)
        if send_requst_ret and 'error' in send_requst_ret:
            self.log.info('get_roles failed - {0}'.format(send_requst_ret))
            return False
        else:
            return True

    def get_role(self, shell):
        self.log.info('=== get_role ({0})'.format(self.role_name))
        self.info = shell.extract_remote_info()
        cmd = 'curl -X GET http://{0}:{1}/{2}/_role/{3}'.\
            format(self.info.ip, self.admin_port, self.db_name, self.role_name)
        send_requst_ret, _ = self.send_request(shell, cmd)
        if self.expected_stdout:
            if send_requst_ret and self.expected_stdout in send_requst_ret:
                return True
            else:
                self.log.info('get_role expected_stdout({0}), but get {1}'.format(self.expected_stdout, send_requst_ret))
                return False
        else:
            if send_requst_ret and 'error' in send_requst_ret:
                self.log.info('get_role failed - {0}'.format(send_requst_ret))
                return False
            else:
                send_requst_dic = json.loads(send_requst_ret)
                if self.admin_channels and 'admin_channels' in send_requst_dic:
                    input_list = self.admin_channels.split(',')
                    if len(send_requst_dic['admin_channels']) != len(input_list):
                        self.log.info('get_role number of items in admin_channels{0} is different from input{1}'
                                      .format(len(send_requst_dic['admin_channels']), len(input_list)))
                        return False
                    for item in send_requst_dic['admin_channels']:
                        if item.encode('ascii', 'ignore') not in self.admin_channels:
                            self.log.info('get_role admin_channels missing item - {0}'.format(item.encode('ascii', 'ignore')))
                            return False
                if self.admin_roles and 'admin_roles' in send_requst_dic and send_requst_dic['admin_roles'] != self.admin_roles:
                    input_list = self.admin_roles.split(',')
                    if len(send_requst_dic['admin_roles']) != len(input_list):
                        self.log.info('get_role number of items in admin_roles{0} is different from input{1}'
                                      .format(len(send_requst_dic['admin_roles']), len(input_list)))
                        return False
                    for item in send_requst_dic['admin_roles']:
                        if item.encode('ascii', 'ignore') not in self.admin_roles:
                            self.log.info('get_role admin_roles missing item - {0}'.format(item.encode('ascii', 'ignore')))
                            return False
                return True


    def delete_role(self, shell):
        self.log.info('=== delete_role ({0})'.format(self.role_name))
        self.info = shell.extract_remote_info()
        cmd = 'curl -X DELETE http://{0}:{1}/{2}/_role/{3}'.\
            format(self.info.ip, self.admin_port, self.db_name, self.role_name)
        send_requst_ret, _ = self.send_request(shell, cmd)
        if send_requst_ret and 'error' in send_requst_ret:
            self.log.info('delete_role failed - {0}'.format(send_requst_ret))
            return False
        else:
            return True