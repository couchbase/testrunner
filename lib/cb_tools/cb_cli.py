import json

from cb_tools.cb_tools_base import CbCmdBase
from lib.Cb_constants.CBServer import CbServer


class CbCli(CbCmdBase):
    def __init__(self, shell_conn, no_ssl_verify=None):
        CbCmdBase.__init__(self, shell_conn, "couchbase-cli")
        if no_ssl_verify is None:
            no_ssl_verify = CbServer.use_https
        self.cli_flags = ""
        if no_ssl_verify:
            self.cli_flags += " --no-ssl-verify"

    def delete_bucket(self, bucket_name):
        cmd = "%s bucket-delete -c %s:%s -u %s -p %s --bucket %s" \
              % (self.cbstatCmd, "localhost", self.port,
                 self.username, self.password, bucket_name)
        cmd += self.cli_flags
        output, error = self._execute_cmd(cmd)
        if len(error) != 0:
            raise Exception(str(error))
        return output

    def enable_dp(self):
        """
        Method to enable developer-preview

        Raise:
        Exception(if any) during command execution
        """
        cmd = "echo 'y' | %s enable-developer-preview --enable " \
              "-c %s:%s -u %s -p %s" \
              % (self.cbstatCmd, "localhost", self.port,
                 self.username, self.password)
        cmd += self.cli_flags
        output, error = self._execute_cmd(cmd)
        if len(error) != 0:
            raise Exception("\n".join(error))
        if "SUCCESS: Cluster is in developer preview mode" not in str(output):
            raise Exception("Expected output not seen: %s" % output)

    def enable_n2n_encryption(self):
        cmd = "%s node-to-node-encryption -c %s:%s -u %s -p %s --enable" \
             % (self.cbstatCmd, "localhost", self.port,
                self.username, self.password)
        cmd += self.cli_flags
        output, error = self._execute_cmd(cmd)
        if len(error) != 0:
            raise Exception(str(error))
        CbServer.n2n_encryption = True
        CbServer.use_https = False
        return output

    def disable_n2n_encryption(self):
        cmd = "%s node-to-node-encryption -c %s:%s -u %s -p %s --disable" \
              % (self.cbstatCmd, "localhost", self.port,
                 self.username, self.password)
        cmd += self.cli_flags
        output, error = self._execute_cmd(cmd)
        if len(error) != 0:
            raise Exception(str(error))
        CbServer.n2n_encryption = False
        CbServer.use_https = False
        return output

    def set_n2n_encryption_level(self, level="all"):
        cmd = "%s setting-security -c %s:%s -u %s -p %s --set --cluster-encryption-level %s" \
              % (self.cbstatCmd, "localhost", self.port,
                 self.username, self.password, level)
        cmd += self.cli_flags
        output, error = self._execute_cmd(cmd)
        if len(error) != 0:
            raise Exception(str(error))
        if level != "strict":
            CbServer.use_https = False
        else:
            CbServer.use_https = True
        return output

    def get_n2n_encryption_level(self):
        cmd = "%s setting-security -c %s:%s -u %s -p %s --get" \
              % (self.cbstatCmd, "localhost", self.port,
                 self.username, self.password)
        cmd += self.cli_flags
        output, error = self._execute_cmd(cmd)
        if len(error) != 0:
            raise Exception(str(error))
        json_acceptable_string = output[0].replace("'", "\"")
        security_dict = json.loads(json_acceptable_string)
        if "clusterEncryptionLevel" in security_dict:
            return security_dict["clusterEncryptionLevel"]
        else:
            return None

