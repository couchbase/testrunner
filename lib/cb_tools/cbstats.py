import re
import zlib
import json

from cb_tools.cb_tools_base import CbCmdBase

from TestInput import TestInputSingleton


class Cbstats(CbCmdBase):
    def __init__(self, shell_conn):
        CbCmdBase.__init__(self, shell_conn, "cbstats")

    def __calculate_vbucket_num(self, doc_key, total_vbuckets):
        """
        Calculates vbucket number based on the document's key

        Argument:
        :doc_key        - Document's key
        :total_vbuckets - Total vbuckets present in the bucket

        Returns:
        :vbucket_number calculated based on the 'doc_key'
        """
        return (((zlib.crc32(doc_key)) >> 16) & 0x7fff) & (total_vbuckets-1)

    def get_scopes(self, bucket):
        """
        Fetches list of scopes for the particular bucket
        Uses command:
          cbstats localhost:port scopes

        Arguments:
        :bucket - Bucket object to fetch the name

        Returns:
        :scope_data - Dict containing the scopes stat values
        """
        scope_data = dict()
        cmd = "%s localhost:%s -u %s -p %s -b %s scopes" \
              % (self.cbstatCmd, self.mc_port, self.username, self.password,
                 bucket.name)

        output, error = self._execute_cmd(cmd)
        if len(error) != 0:
            raise Exception("\n".join(error))

        output = str(output)

        # Fetch general scope stats
        manifest_uid_pattern = "[ \t]*uid:[ \t]+([0-9]+)"
        manifest_uid_pattern = re.compile(manifest_uid_pattern)
        manifest_uid = manifest_uid_pattern.findall(str(output))[0]
        scope_data["manifest_uid"] = int(manifest_uid)

        scope_names_pattern = "[ \t]+([0-9xa-f]+):name:" \
                              "[ \t]+([0-9A-Za-z_%-]+)"
        col_count_pattern = "[ \t]*%s:collections:[ \t]+([0-9]+)"
        items_count_pattern = "[ \t]*%s:items:[ \t]+([0-9]+)"

        scope_names_pattern = re.compile(scope_names_pattern)
        scope_names = scope_names_pattern.findall(output)

        # Populate specific scope stats
        scope_data["count"] = 0
        for s_name_match in scope_names:
            s_id = s_name_match[0]
            s_name = s_name_match[1]

            collections_count_pattern = col_count_pattern % s_id
            s_items_count_pattern = items_count_pattern % s_id

            collections_count_pattern = re.compile(collections_count_pattern)
            s_items_count_pattern = re.compile(s_items_count_pattern)

            collection_count = collections_count_pattern.findall(output)[0]
            items_count = s_items_count_pattern.findall(output)[0]

            scope_data[s_name] = dict()
            scope_data[s_name]["id"] = s_id
            scope_data[s_name]["collections"] = int(collection_count)
            scope_data[s_name]["num_items"] = int(items_count)
            scope_data["count"] += 1

        return scope_data

    def get_scope_details(self, bucket_name):
        """
        Fetches scopes-details status from the server
        Uses command:
          cbstats localhost:port scopes-details

        Arguments:
        :bucket_name - Name of the bucket to get the stats

        Returns:
        :scope_data - Dict containing the scopes_details stat values
        """
        scope_data = dict()
        cmd = "%s localhost:%s -u %s -p %s -b %s scopes-details" \
              % (self.cbstatCmd, self.mc_port, self.username, self.password,
                 bucket_name)

        output, error = self._execute_cmd(cmd)
        if len(error) != 0:
            raise Exception("\n".join(error))

        pattern = "[ \t]*manifest:scopes:([0-9xa-h]+):collections:" \
                  "[ \t]+(a-zA-Z_0-9%-)+"
        regexp = re.compile(pattern)
        scope_name_match = regexp.findall(str(output))
        for scope in scope_name_match:
            scope_name = scope.group(1)
            scope_data[scope_name] = dict()
            scope_data[scope_name]["id"] = scope.group(0)

        # Cluster_run case
        if type(output) is str:
            output = output.split("\n")

        for line in output:
            match_result = regexp.match(line)
            if match_result:
                scope_data = match_result.group(1)
                break

        return scope_data

    def get_collections(self, bucket):
        """
        Fetches list of collections from the server
        Uses command:
          cbstats localhost:port collections

        Arguments:
        :bucket - Bucket object to fetch the name

        Returns:
        :collection_data - Dict containing the collections stat values
        """
        collection_data = dict()
        scope_id_mapping = dict()

        # Fetch scope_data before fetching collections
        # scope_data = self.get_scopes(bucket)

        cmd = "%s localhost:%s -u %s -p %s -b %s collections" \
              % (self.cbstatCmd, self.mc_port, self.username, self.password,
                 bucket.name)

        output, error = self._execute_cmd(cmd)
        if len(error) != 0:
            raise Exception("\n".join(error))

        manifest_uid_pattern = "[ \t]*manifest_uid:[ \t]+([0-9]+)"

        scope_name_pattern = "[ \t]*([0-9xa-f]+):([0-9xa-f]+):scope_name:" \
                             "[ \t]+([a-zA-Z_0-9%-]+)"
        col_name_pattern = "[ \t]*([0-9xa-f]+):([0-9xa-f]+):name:" \
                           "[ \t]+([a-zA-Z_0-9%-]+)"
        collection_stat_pattern = \
            "[ \t]*%s:%s:([0-9A-Za-z_]+):[ \t]+([0-9]+)"

        manifest_uid_pattern = re.compile(manifest_uid_pattern)
        scope_name_pattern = re.compile(scope_name_pattern)
        col_name_pattern = re.compile(col_name_pattern)

        # Populate generic manifest stats
        manifest_uid = manifest_uid_pattern.findall(str(output))[0]
        collection_data["count"] = 0
        collection_data["manifest_uid"] = int(manifest_uid)

        # Fetch all available collection names
        scope_names = scope_name_pattern.findall(str(output))
        collection_names = col_name_pattern.findall(str(output))

        for scope_name_match in scope_names:
            scope_id_mapping[scope_name_match[0]] = scope_name_match[2]

        # Populate collection specific data
        for c_name_match in collection_names:
            s_id = c_name_match[0]
            c_id = c_name_match[1]

            scope_name = scope_id_mapping[s_id]
            c_name = c_name_match[2]

            if scope_name not in collection_data:
                collection_data[scope_name] = dict()

            curr_col_stats_pattern = collection_stat_pattern % (s_id, c_id)
            curr_col_stats_pattern = re.compile(curr_col_stats_pattern)
            pattern_match = curr_col_stats_pattern.findall(str(output))

            collection_data[scope_name][c_name] = dict()
            collection_data[scope_name][c_name]["id"] = c_id
            for match in pattern_match:
                try:
                    collection_data[scope_name][c_name][match[0]] \
                        = int(match[1])
                except ValueError:
                    collection_data[scope_name][c_name][match[0]] = match[1]
            collection_data["count"] += 1

        return collection_data

    def get_collection_details(self, bucket_name):
        """
        Fetches collections_details from the server
        Uses command:
          cbstats localhost:port collections-details

        Arguments:
        :bucket_name - Name of the bucket to get the stats

        Returns:
        :collection_data - Dict containing the collections stat values
        """
        collection_data = dict()
        cmd = "%s localhost:%s -u %s -p %s -b %s collections-details" \
              % (self.cbstatCmd, self.mc_port, self.username, self.password,
                 bucket_name)

        output, error = self._execute_cmd(cmd)
        if len(error) != 0:
            raise Exception("\n".join(error))

        output = str(output)
        bucket_uid_pattern = "[ \t]*uid[ \t:]+([0-9]+)"
        bucket_uid_pattern = re.compile(bucket_uid_pattern)
        bucket_uid = bucket_uid_pattern.findall(output)
        collection_data["uid"] = int(bucket_uid[0])
        return collection_data

    def get_stats(self, bucket_name, stat_name, field_to_grep=None):
        """
        Fetches stats using cbstat and greps for specific line.
        Uses command:
          cbstats localhost:port 'stat_name' | grep 'field_to_grep'

        Note: Function calling this API should take care of validating
        the outputs and handling the errors/warnings from execution.

        Arguments:
        :bucket_name   - Name of the bucket to get the stats
        :stat_name     - Any valid stat_command accepted by cbstats
        :field_to_grep - Target stat name string to grep.
                         Default=None, means fetch all data

        Returns:
        :output - Output for the cbstats command
        :error  - Buffer containing warnings/errors from the execution
        """

        cmd = "%s localhost:%s -u %s -p '%s' -b %s %s" \
              % (self.cbstatCmd, self.mc_port, self.username, self.password,
                 bucket_name, stat_name)

        if field_to_grep:
            cmd = "%s | grep %s" % (cmd, field_to_grep)
        return self._execute_cmd(cmd)

    def get_timings(self, bucket_name, command="raw"):
        """
        Fetches timings stat
        :param bucket_name: Name of the bucket to get timings
        :param command: default="raw", to print in readable format
        :return output: Output for the cbstats command
        :return error:  Buffer containing warnings/errors from the execution
        """

        cmd = "%s localhost:%s %s -u %s -p %s -b %s timings" % (self.cbstatCmd,
                                                                self.mc_port,
                                                                command,
                                                                self.username,
                                                                self.password,
                                                                bucket_name)
        return self._execute_cmd(cmd)

    def get_kvtimings(self, command="raw"):
        """
        Fetches kvtiming using cbstats

        :param command: default="raw", to print in readable format
        :return output: Output for the cbstats command
        :return error:  Buffer containing warnings/errors from the execution
        """
        cmd = "%s localhost:%s %s -u %s -p %s kvtimings" % (self.cbstatCmd,
                                                            self.mc_port,
                                                            command,
                                                            self.username,
                                                            self.password)

        return self._execute_cmd(cmd)

    def get_vbucket_stats(self, bucket_name, stat_name, vbucket_num,
                          field_to_grep=None):
        """
        Fetches failovers stats for specified vbucket
        and greps for specific stat.
        Uses command:
          cbstats localhost:port failovers '[vbucket_num]' | \
            grep '[field_to_grep]'

        Note: Function calling this API should take care of validating
        the outputs and handling the errors/warnings from execution.

        Arguments:
        :bucket_name   - Name of the bucket to get the stats
        :stat_name     - Any valid stat_command accepted by cbstats
        :vbucket_num   - Target vbucket number to fetch the stats
        :field_to_grep - Target stat name string to grep.
                         Default=None, means to fetch all stats related to
                         the selected vbucket stat

        Returns:
        :output - Output for the cbstats command
        :error  - Buffer containing warnings/errors from the execution
        """

        cmd = "%s localhost:%s -u %s -p %s -b %s %s %s" \
              % (self.cbstatCmd, self.mc_port, self.username, self.password,
                 bucket_name, stat_name, vbucket_num)

        if field_to_grep:
            cmd = "%s | grep %s" % (cmd, field_to_grep)

        return self._execute_cmd(cmd)

    # Below are wrapper functions for above command executor APIs
    def all_stats(self, bucket_name, stat_name="all"):
        """
        Get a particular value of stat from the command,
          cbstats localhost:port all
        Arguments:
        :bucket_name   - Name of the bucket to get the stats
        :stat_name     - Any valid stat_command accepted by cbstats
        Returns:
        :result - Dict of stat_key:value
        Raise:
        :Exception returned from command line execution (if any)
        """
        if stat_name == '':
            stat_name = "all"
        result = dict()
        output, error = self.get_stats(bucket_name, stat_name)
        if len(error) != 0:
            raise Exception("\n".join(error))

        if type(output) is str:
            output = output.split("\n")
        elif type(output) is not list:
            output = str(output, 'utf-8')
            output = output.split("\n")

        pattern = "[ \t]*([0-9A-Za-z_]+)[ \t]*:[ \t]+([a-zA-Z0-9\-\.\:_ ]+)"
        pattern = re.compile(pattern)
        for line in output:
            match_result = pattern.match(line)
            if match_result:
                result[match_result.group(1)] = match_result.group(2)
        return result

    def checkpoint_stats(self, bucket_name):
        """
        Fetches checkpoint stats for the given bucket_name

        :param bucket_name: Bucket name to fetch the stats for
        :return result: Dictionary map of vb_num as inner dict

        Raise:
        :Exception returned from the command line execution (if any)
        """
        result = dict()
        output, error = self.get_stats(bucket_name, "checkpoint")
        if len(error) != 0:
            raise Exception("\n".join(error))

        pattern = \
            "[\t ]*vb_([0-9]+):([a-zA-Z0-9@.->:_]+):[\t ]+([0-9A-Za-z_]+)"
        pattern = re.compile(pattern)
        for line in output:
            match_result = pattern.match(line)
            if match_result:
                vb_num = int(match_result.group(1))
                stat_name = match_result.group(2)
                stat_value = match_result.group(3)
                try:
                    stat_value = int(stat_value)
                except ValueError:
                    pass
                if vb_num not in result:
                    result[vb_num] = dict()
                result[vb_num][stat_name] = stat_value
        return result

    def magma_stats(self, bucket_name, field_to_grep=None,
                    stat_name="kvstore"):
        """
        Get a particular value of "kvstore" stat from the command,
        cbstats localhost:port kvstore
        kvstore stats are the stats for magma

        Arguments:
        :bucket_name   - Name of the bucket to get the stats
        :stat_name     - Any valid stat_command accepted by cbstats
        :field_to_grep - Target stat name string to grep.

        Returns:
        :result - Return a dict with key as  'field_to_grep' or an empty dict
                  if field_to_grep does not exist or invalid

        Raise:
        :Exception returned from command line execution (if any)
        """

        result = dict()
        output, error = self.get_stats(bucket_name, stat_name,
                                       field_to_grep=field_to_grep)
        if len(error) != 0:
            raise Exception("\n".join(error))
        pattern = ":[ \t]+"
        pattern_for_key = "^[ \s]+"
        bucket_absent_error = "No access to bucket:%s - permission \
                               denied or bucket does not exist" % bucket_name
        if type(output) is not list:
            output = [output]
        if field_to_grep is None and bucket_absent_error in output[0]:
            raise Exception("\n", bucket_absent_error)
        for ele in output:
            result[re.sub(pattern_for_key, "", re.split(pattern, ele)[0])] = \
                json.loads(re.split(pattern, ele)[1])
        return result

    def vbucket_list(self, bucket_name, vbucket_type="active"):
        """
        Get list of vbucket numbers as list.
        Uses command:
          cbstats localhost:port vbuckets

        Arguments:
        :bucket_name  - Name of the bucket to get the stats
        :vbucket_type - Type of vbucket (active/replica)
                        Default="active"

        Returns:
        :vb_list - List containing list of vbucket numbers matching
                   the :vbucket_type:

        Raise:
        :Exception returned from command line execution (if any)
        """

        vb_list = list()
        cmd = "%s localhost:%s -u %s -p %s -b %s vbucket" \
              % (self.cbstatCmd, self.mc_port, self.username, self.password,
                 bucket_name)
        output, error = self._execute_cmd(cmd)
        if len(error) != 0:
            raise Exception("\n".join(error))

        if type(output) is str:
            output = output.split("\n")
        elif type(output) is not list:
            output = str(output, 'utf-8')
            output = output.split("\n")

        pattern = "[ \t]*vb_([0-9]+)[ \t]*:[ \t]+([a-zA-Z]+)"
        regexp = re.compile(pattern)
        for line in output:
            match_result = regexp.match(line)
            if match_result:
                curr_vb_type = match_result.group(2)
                if curr_vb_type == vbucket_type:
                    vb_num = match_result.group(1)
                    vb_list.append(int(vb_num))

        return vb_list

    def vbucket_details(self, bucket_name):
        """
        Get a particular value of stat from the command,
          cbstats localhost:port vbucket-details

        :param bucket_name:   - Name of the bucket to get the stats
        :returns vb_details:  - Dictionary of format dict[vb][stat_name]=value

        Raise:
        :Exception returned from command line execution (if any)
        """

        stats = dict()
        cmd = "%s localhost:%s -u %s -p %s -b %s vbucket-details" \
              % (self.cbstatCmd, self.mc_port, self.username, self.password,
                 bucket_name)

        output, error = self._execute_cmd(cmd)
        if len(error) != 0:
            raise Exception("\n".join(error))
        # In case of cluster_run, output is plain string due to direct exec
        if type(output) is str:
            output = output.split("\n")
        elif type(output) is not list:
            output = str(output, 'utf-8')
            output = output.split("\n")

        pattern = "[ \t]*vb_([0-9]+):([0-9a-zA-Z_]*):?[ \t]+([0-9A-Za-z\-\.\:\",_\[\]]+)"
        regexp = re.compile(pattern)

        for line in output:
            if line.strip() == '':
                continue
            match_result = regexp.match(line)
            vb_num = match_result.group(1)
            stat_name = match_result.group(2)
            stat_value = match_result.group(3)

            if stat_name == "":
                stat_name = "type"

            # Create a sub_dict to state vbucket level stats
            if vb_num not in stats:
                stats[vb_num] = dict()
            # Populate the values to the stats dictionary
            stats[vb_num][stat_name] = stat_value

        return stats

    def vkey_stat(self, bucket_name, doc_key,
                  vbucket_num=None, total_vbuckets=1024):
        """
        Get vkey stats from the command,
          cbstats localhost:port -b 'bucket_name' vkey 'doc_id' 'vbucket_num'

        Arguments:
        :bucket_name    - Name of the bucket to get the stats
        :doc_key        - Document key to validate for
        :vbucket_num    - Target vbucket_number to fetch the stats.
                          If 'None', calculate the vbucket_num locally
        :total_vbuckets - Total vbuckets configured for the bucket.
                          Default=1024

        Returns:
        :result - Dictionary of stat:values

        Raise:
        :Exception returned from command line execution (if any)
        """

        result = dict()
        if vbucket_num is None:
            vbucket_num = self.__calculate_vbucket_num(doc_key, total_vbuckets)

        cmd = "%s localhost:%s -u %s -p %s -b %s vkey %s %s" \
              % (self.cbstatCmd, self.mc_port, self.username, self.password,
                 bucket_name, doc_key, vbucket_num)

        output, error = self._execute_cmd(cmd)
        if len(error) != 0:
            raise Exception("\n".join(error))

        # In case of cluster_run, output is plain string due to direct exec
        if type(output) is str:
            output = output.split("\n")

        pattern = "[ \t]*key_([a-zA-Z_]+)[: \t]+([0-9A-Za-z]+)"
        pattern = re.compile(pattern)
        for line in output:
            match_result = pattern.match(line)
            if match_result:
                result[match_result.group(1)] = match_result.group(2)

        return result

    def vbucket_seqno(self, bucket_name):
        """
        Get a particular value of stat from the command,
          cbstats localhost:port vbucket-seqno

        Arguments:
        :bucket_name   - Name of the bucket to get the stats

        Returns:
        :result - Dictionary of format stats["vb_num"]["stat_name"] = value

        Raise:
        :Exception returned from command line execution (if any)
        """

        stats = dict()
        output, error = self.get_stats(bucket_name, "vbucket-seqno")
        if len(error) != 0:
            raise Exception("\n".join(error))

        if type(output) is str:
            output = output.split("\n")
        elif type(output) is not list:
            output = str(output, 'utf-8')
            output = output.split("\n")

        pattern = "[ \t]*vb_([0-9]+):([0-9a-zA-Z_]+):[ \t]+([0-9]+)"
        regexp = re.compile(pattern)
        for line in output:
            match_result = regexp.match(line)
            if match_result:
                vb_num = match_result.group(1)
                stat_name = match_result.group(2)
                stat_value = match_result.group(3)

                # Create a sub_dict to state vbucket level stats
                if vb_num not in stats:
                    stats[vb_num] = dict()
                # Populate the values to the stats dictionary
                stats[vb_num][stat_name] = stat_value

        return stats

    def failover_stats(self, bucket_name):
        """
        Gets vbucket's failover stats. Uses command,
          cbstats localhost:port -b 'bucket_name' failovers

        Arguments:
        :bucket_name    - Name of the bucket to get the stats
        :field_to_grep  - Target stat name string to grep.
                          Default=None means fetch all stats for the vbucket

        Returns:
        :stats - Dictionary of format stats["vb_num"]["stat_name"] = value

        Raise:
        :Exception returned from command line execution (if any)
        """

        stats = dict()
        output, error = self.get_stats(bucket_name, "failovers")
        if len(error) != 0:
            raise Exception("\n".join(error))

        if type(output) is str:
            output = output.split("\n")
        elif type(output) is not list:
            output = str(output, 'utf-8')
            output = output.split("\n")

        pattern = "[ \t]vb_([0-9]+):([0-9A-Za-z:_]+):[ \t]+([0-9]+)"
        regexp = re.compile(pattern)

        for line in output:
            # Match the regexp to the line and populate the values
            match_result = regexp.match(line)
            vb_num = match_result.group(1)
            stat_name = match_result.group(2)
            stat_value = match_result.group(3)

            # Create a sub_dict to state vbucket level stats
            if vb_num not in stats:
                stats[vb_num] = dict()
            # Populate the values to the stats dictionary
            stats[vb_num][stat_name] = stat_value

        return stats

    def verify_failovers_field_stat(self, bucket_name, field_to_grep,
                                    expected_value, vbuckets_list=None):
        """
        Verifies the given value against the failovers stats

        Arguments:
        :bucket_name    - Name of the bucket to get the stats
        :field_to_grep  - Target stat name string to grep
        :expected_value - Expected value against which the verification
                          needs to be done
        :vbuckets_list  - List of vbuckets to verify the values

        Returns:
        :is_stat_ok  - Boolean value saying whether it is okay or not

        Raise:
        :Exception returned from command line execution (if any)
        """
        # Local function to parse and verify the output lines
        def parse_failover_logs(output, error):
            is_ok = True
            if len(error) != 0:
                raise Exception("\n".join(error))

            pattern = "[ \t]vb_[0-9]+:{0}:[ \t]+([0-9]+)".format(field_to_grep)
            regexp = re.compile(pattern)
            for line in output:
                match_result = regexp.match(line)
                if match_result is None:
                    is_ok = False
                    break
                else:
                    if match_result.group(1) != expected_value:
                        is_ok = False
                        break
            return is_ok

        is_stat_ok = True
        if vbuckets_list is None:
            output, error = self.get_stats(
                bucket_name, "failovers", field_to_grep=field_to_grep)
            try:
                is_stat_ok = parse_failover_logs(output, error)
            except Exception as err:
                raise Exception(err)
        else:
            for tem_vb in vbuckets_list:
                output, error = self.get_vbucket_stats(
                    bucket_name, "failovers", vbucket_num=tem_vb,
                    field_to_grep=field_to_grep)
                try:
                    is_stat_ok = parse_failover_logs(output, error)
                    if not is_stat_ok:
                        break
                except Exception as err:
                    raise Exception(err)
        return is_stat_ok

    def dcp_stats(self, bucket_name):
        stats = dict()
        output, error = self.get_stats(bucket_name, "dcp")
        if len(error) != 0:
            raise Exception("\n".join(error))
        for line in output:
            line = line.split()
            # TODO: Remove try..catch once MB-46630 is fixed
            try:
                stat = line[0].rstrip(":")
                stats[stat] = line[1].strip()
            except IndexError:
                pass
        return stats

    def dcp_vbtakeover(self, bucket_name, vb_num, key):
        """
        Fetches dcp-vbtakeover stats for the target vb,key
          cbstats localhost:port -b bucket_name dcp-vbtakeover vb_num key

        Arguments:
        :bucket_name - Name of the bucket
        :vb_num - Target vBucket
        :key - Document key

        Returns:
        :stats - Dictionary of stats field::value

        Raise:
        :Exception returned from command line execution (if any)
        """

        stats = dict()
        cmd = "%s localhost:%s -u %s -p %s -b %s dcp-vbtakeover %s %s" \
              % (self.cbstatCmd, self.mc_port, self.username, self.password,
                 bucket_name, vb_num, key)

        output, error = self._execute_cmd(cmd)
        if len(error) != 0:
            raise Exception("\n".join(error))

        for line in output:
            line = line.strip()
            list_all = line.rsplit(":", 1)
            stat = list_all[0]
            val = list_all[1].strip()
            try:
                val = int(val)
            except ValueError:
                pass
            stats[stat] = val

        return stats

    def warmup_stats(self, bucket_name):
        """
        Fetches warmup stats for the requested bucket.
        :param bucket_name: Name of the bucket to fetch the stats
        :return stats: Dict values of warmup stats
        """
        stats = dict()
        output, error = self.get_stats(bucket_name, "warmup")
        if len(error) != 0:
            raise Exception("\n".join(error))

        for line in output:
            line = line.strip()
            list_all = line.rsplit(":", 1)
            stat = list_all[0]
            val = list_all[1].strip()
            try:
                val = int(val)
            except ValueError:
                pass
            stats[stat] = val
        return stats
