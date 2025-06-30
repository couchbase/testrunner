import json
import time

import logger
import random

from lib.membase.api.rest_client import RestConnection
from lib.remote.remote_util import RemoteMachineShellConnection
from lib import testconstants


class IndexBackupClient(object):
    """
    A class to represent an Indexer Backup.

    Attributes
    ----------
    rest : membase.api.rest_client.RestConnection
        Object that establishes the rest connection to the indexer node
    node : TestInput.TestInputServer
        Object that represents the indexer node
    bucket : membase.api.rest_client.Bucket
        Object that represents the bucket on which the back is to be taken
    is_backup_exists : boolean
        variable to check whether backup exists
    backup : dict
        result of the backup api

    Methods
    -------
    backup(namespaces=[], include=True):
        Takes the backup of the current indexes.

    restore(namespaces=[], include=True):
        Restores the stored backup to the node.
    """

    def __init__(self, backup_node, use_cbbackupmgr, backup_bucket,
                 restore_bucket=None, restore_node=None, disabled_services="--disable-ft-indexes --disable-views --disable-data --disable-ft-alias --disable-eventing --disable-analytics"):
        self.use_cbbackupmgr = use_cbbackupmgr
        self.set_backup_node(backup_node)
        if not self.use_cbbackupmgr and isinstance(backup_bucket, list):
            backup_bucket = backup_bucket[0]
        if restore_node:
            self.set_restore_node(restore_node)
        else:
            self.set_restore_node(backup_node)
        self.set_backup_bucket(backup_bucket)
        if restore_bucket:
            self.set_restore_bucket(restore_bucket)
        else:
            self.set_restore_bucket(backup_bucket)
        self.rand = random.randint(1, 1000000000)
        self.log = logger.Logger.get_logger()
        self.disabled_services = disabled_services

    def __repr__(self):
        obj = """backup node:{0}
        backup bucket:{1}
        restore node:{2}
        restore bucket:{3}
        """.format(self.backup_node, self.backup_bucket, self.restore_node,
                   self.restore_buckets)
        return obj

    def _backup_with_tool(self, namespaces, include, config_args, backup_args, use_https=False, use_certs=False):
        remote_client = RemoteMachineShellConnection(self.backup_node)
        os_platform = remote_client.extract_remote_info().type.lower()
        if os_platform == 'linux':
            if remote_client.nonroot:
                self.cli_command_location = testconstants.LINUX_NONROOT_CB_BIN_PATH
            else:
                self.cli_command_location = testconstants.LINUX_COUCHBASE_BIN_PATH
            self.backup_path = testconstants.LINUX_BACKUP_PATH
        elif os_platform == 'windows':
            self.cmd_ext = ".exe"
            self.cli_command_location = \
                testconstants.WIN_COUCHBASE_BIN_PATH_RAW
            self.backup_path = testconstants.WIN_BACKUP_C_PATH
        elif os_platform == 'mac':
            self.cli_command_location = testconstants.MAC_COUCHBASE_BIN_PATH
            self.backup_path = testconstants.LINUX_BACKUP_PATH
        else:
            raise Exception("OS not supported.")
        command = self.cli_command_location + "cbbackupmgr config --archive {0} --repo backup_{1} {2}".format(
            self.backup_path, self.rand, self.disabled_services)
        if not config_args:
            config_args = "--include-data " if include else " --exclude-data "
            if namespaces:
                namespaces = ["{0}.{1}".format(self.backup_bucket, namespace)
                              for namespace in namespaces]
                config_args += ",".join(namespaces)
            else:
                if isinstance(self.backup_bucket, list):
                    config_args += ",".join(self.backup_bucket)
                else:
                    config_args += self.backup_bucket
        command += " {0}".format(config_args)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        if error or not [
            x for x in output
            if 'created successfully in archive' in x] and not [
            x for x in output
            if "`backup_" + str(self.rand) + "` exists" in x]:
            self.is_backup_exists = False
            return self.is_backup_exists, output
        if use_https and not use_certs:
            cmd = f"cbbackupmgr backup --archive {self.backup_path} --repo backup_{self.rand} " \
                  f"--cluster couchbases://{self.backup_node.ip} --username {self.backup_node.rest_username} " \
                  f"--password {self.backup_node.rest_password} --no-ssl-verify"
        elif use_certs:
            output, error = remote_client.execute_command(command="find /opt/couchbase/var/lib/couchbase/inbox/CA* -maxdepth 1 -type d")
            self.log.info(f"output is {output}")
            certpath = output[0]
            cmd = f"cbbackupmgr backup --archive {self.backup_path} --repo backup_{self.rand} " \
                  f"--cluster couchbases://{self.backup_node.ip} --username {self.backup_node.rest_username} " \
                  f"--password {self.backup_node.rest_password} --cacert {certpath}/ca.pem"
            self.log.info(f"cmd is {cmd}")

        else:
            cmd = f"cbbackupmgr backup --archive {self.backup_path} --repo backup_{self.rand} " \
                  f"--cluster couchbase://{self.backup_node.ip} --username {self.backup_node.rest_username} " \
                  f"--password {self.backup_node.rest_password}"
        command = "{0}{1}".format(self.cli_command_location, cmd)
        if backup_args:
            command += " {0}".format(backup_args)
        self.log.info(f"Backing-up using command: {command}")
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        if error or not [x for x in output if 'Backup successfully completed'
                                              in x or 'Backup completed successfully' in x]:
            self.is_backup_exists = False
            return self.is_backup_exists, output
        self.is_backup_exists = True
        return self.is_backup_exists, output

    def _restore_with_tool(self, mappings, namespaces, include, restore_args, use_https=False, use_certs=False):
        if not self.is_backup_exists:
            return self.is_backup_exists, "Backup not found"
        remote_client = RemoteMachineShellConnection(self.backup_node)
        if use_https and not use_certs:
            command = "{0}cbbackupmgr restore --archive {1} --repo backup_{2} --cluster couchbases://{3}" \
                      " --username {4} --password {5} --force-updates {6} --no-ssl-verify".format(
                self.cli_command_location, self.backup_path, self.rand,
                self.restore_node.ip, self.restore_node.rest_username,
                self.restore_node.rest_password, self.disabled_services)
        elif use_certs:
            output, error = remote_client.execute_command(
                command="find /opt/couchbase/var/lib/couchbase/inbox/CA* -maxdepth 1 -type d")
            self.log.info(f"output is {output}")
            certpath = output[0]
            command = "{0}cbbackupmgr restore --archive {1} --repo backup_{2} --cluster couchbases://{3}" \
                      " --username {4} --password {5} --force-updates {6} --cacert {7}/ca.pem".format(
                self.cli_command_location, self.backup_path, self.rand,
                self.restore_node.ip, self.restore_node.rest_username,
                self.restore_node.rest_password, self.disabled_services, certpath)
        else:
            command = "{0}cbbackupmgr restore --archive {1} --repo backup_{2} --cluster couchbase://{3}" \
                      " --username {4} --password {5} --force-updates {6}".format(
                self.cli_command_location, self.backup_path, self.rand,
                self.restore_node.ip, self.restore_node.rest_username,
                self.restore_node.rest_password, self.disabled_services)
        if not restore_args:
            mapping_args = ""
            if mappings:
                mapping_args += "--map-data "
                remappings = []
                if self.backup_bucket != self.restore_bucket:
                    bucket_mapping = "{0}={1}".format(
                        self.backup_bucket, self.restore_bucket)
                    remappings.append(bucket_mapping)
                for mapping in mappings:
                    mapping_tokens = mapping.split(":")
                    src = "{0}.{1}".format(
                        self.backup_bucket, mapping_tokens[0])
                    tgt = "{0}.{1}".format(
                        self.restore_bucket, mapping_tokens[1])
                    remappings.append("{0}={1}".format(src, tgt))
                mapping_args += ",".join(remappings)
            elif self.backup_bucket != self.restore_bucket:
                mapping_args += "--map-data "
                mapping_args += "{0}={1}".format(
                    self.backup_bucket, self.restore_bucket)
            config_args = "--include-data " if include else "--exclude-data "
            if namespaces:
                namespaces = ["{0}.{1}".format(self.backup_bucket, namespace)
                              for namespace in namespaces]
                config_args += ",".join(namespaces)
            else:
                config_args += self.backup_bucket
            restore_args = "{0} {1}".format(mapping_args, config_args)
        command = "{0} {1}".format(command, restore_args)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        if error or not [x for x in output if 'Restore completed successfully'
                                              in x]:
            self.log.error(output)
            return False, output
        return True, output

    def backup(
            self, namespaces=[], include=True, config_args="", backup_args="", use_https=False, use_certs=False):
        if self.use_cbbackupmgr:
            return self._backup_with_tool(
                namespaces, include, config_args, backup_args, use_https, use_certs)
        return self._backup_with_rest(namespaces, include, config_args)

    def restore(
            self, mappings=[], namespaces=[], include=True, restore_args="", use_https=False, use_cert=False):
        if self.use_cbbackupmgr:
            return self._restore_with_tool(
                mappings, namespaces, include, restore_args, use_https, use_cert)
        return self._restore_with_rest(
            mappings, namespaces, include, restore_args)

    def remove_backup(self):
        remote_client = RemoteMachineShellConnection(self.backup_node)
        cmd = "cbbackupmgr remove --archive {0} --repo backup_{1}".format(
            self.backup_path, self.rand)
        command = "{0}{1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        if error:
            raise Exception("Backup not removed successfully")
        self.is_backup_exists = False

    def remove_backup_repo(self):
        remote_client = RemoteMachineShellConnection(self.backup_node)
        output, error = remote_client.execute_command("ls " + self.backup_path)
        if not error:
            command = "rm -rf {0}".format(self.backup_path)
            output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)

    def _backup_with_rest(self, namespaces, include, query_params):
        """Takes the backup of the current indexes.
        Parameters:
            namespaces -- list of namespaces that are to be included in or
            excluded from backup. Namespaces should follow the syntax
            described in docs (default [])
            include -- :boolean: if False then exclude (default True)
        Returns (status, content) tuple
        """
        if not query_params and namespaces:
            query_params = "?include=" if include else "?exclude="
            query_params += ",".join(namespaces)
        api = self.backup_api.format(self.backup_bucket, query_params)
        status, content, _ = self.backup_rest._http_request(api=api)
        if status:
            self.backup_data = json.loads(content)['result']
            self.is_backup_exists = True
        return status, content

    def _restore_with_rest(self, mappings, namespaces, include, query_params):
        """Restores the stored backup to the node
        Parameters:
            mappings -- list of mappings should follow the syntax
            described in docs (default [])
        Returns (status, content) tuple
        """
        if not self.is_backup_exists or not self.backup_data:
            raise Exception("Backup not found")
        if not query_params:
            if mappings:
                query_params = "?remap={0}".format(",".join(mappings))
            if namespaces:
                if query_params:
                    query_params += "&"
                else:
                    query_params += "?"
                query_params += "include=" if include else "exclude="
                query_params += ",".join(namespaces)
        headers = self.restore_rest._create_capi_headers()
        body = json.dumps(self.backup_data)
        api = self.restore_api.format(self.restore_bucket, query_params)
        status, content, _ = self.restore_rest._http_request(
            api=api, method="POST", params=body, headers=headers)
        json_response = json.loads(content)
        if json_response['code'] == "success":
            return True, json_response
        return False, json_response

    def set_restore_bucket(self, restore_bucket):
        self.restore_bucket = restore_bucket
        if self.is_backup_exists and self.backup_data:
            for metadata in self.backup_data['metadata']:
                for topology in metadata['topologies']:
                    topology['bucket'] = restore_bucket
                    for definition in topology['definitions']:
                        definition['bucket'] = restore_bucket
                for definition in metadata['definitions']:
                    definition['bucket'] = restore_bucket

    def set_backup_bucket(self, backup_bucket):
        self.backup_bucket = backup_bucket
        self.is_backup_exists = False
        self.backup_data = {}

    def set_backup_node(self, backup_node):
        self.backup_rest = RestConnection(backup_node)
        self.backup_node = backup_node
        self.backup_api = \
            self.backup_rest.index_baseUrl + "api/v1/bucket/{0}/backup{1}"
        self.is_backup_exists = False
        self.backup_data = {}
        shell = RemoteMachineShellConnection(backup_node)
        info = shell.extract_remote_info().type.lower()
        if info == 'linux':
            self.cli_command_location = testconstants.LINUX_COUCHBASE_BIN_PATH
            self.backup_path = testconstants.LINUX_BACKUP_PATH
        elif info == 'windows':
            self.cmd_ext = ".exe"
            self.cli_command_location = \
                testconstants.WIN_COUCHBASE_BIN_PATH_RAW
            self.backup_path = testconstants.WIN_BACKUP_C_PATH
        elif info == 'mac':
            self.cli_command_location = testconstants.MAC_COUCHBASE_BIN_PATH
            self.backup_path = testconstants.LINUX_BACKUP_PATH
        else:
            raise Exception("OS not supported.")

    def set_restore_node(self, restore_node):
        self.restore_rest = RestConnection(restore_node)
        self.restore_node = restore_node
        self.restore_api = \
            self.restore_rest.index_baseUrl + "api/v1/bucket/{0}/backup{1}"
