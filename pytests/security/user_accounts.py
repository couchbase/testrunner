import os
import re
import json
import time
import requests

from datetime import datetime

from lib import testconstants
from couchbase.bucket import Bucket
from couchbase.exceptions import CouchbaseException as CBException
from basetestcase import BaseTestCase
from lib.membase.api.rest_client import RestConnection
from lib.remote.remote_util import RemoteMachineShellConnection


class UserAccounts(BaseTestCase):
    def suite_setUp(self):
        super(UserAccounts, self).suite_setUp()
        self.log.info("Suite Setup Start and End")

    def setUp(self):
        super(UserAccounts, self).setUp()
        self.log.info("Setup Start")
        self.rest = RestConnection(self.master)
        self.remote_connection = RemoteMachineShellConnection(self.master)
        self.test_user = self.input.param("test_user", "test_user")
        self.test_user_password = self.input.param("test_user_password", "password")
        self.test_user_new_password = self.input.param("test_user_new_password", "password2")
        self.log.info("Adding an internal user")
        payload = "name={0}&roles={1}&password={2}".format(self.test_user, "admin", self.test_user_password)
        self.rest.add_set_builtin_user(self.test_user, payload)
        output, error = self.remote_connection.execute_command("""curl -g -v -u Administrator:password -X POST http://{0}:8091/sampleBuckets/install -d  '["travel-sample"]'""".format(self.master.ip))
        self.log.info("Setup End")

    def suite_tearDown(self):
        # self.log.info("Suite Teardown Start")
        # self.log.info("Suite Teardown End")
        # delete all users
        pass

    def teardown(self):
        # self.log.info("Teardown Start")
        # self.log.info("Teardown End")
        pass


    def check_ui_login(self, cluster, user_id, password):
        # is it present in lib???
        self.log.info("Logging in to the UI")
        url = "http://" + cluster.ip + ":8091" + "/uilogin?use_cert_for_auth=0"
        self.log.info(url)
        data = {
            'user': user_id,
            'password': password,
        }
        response = requests.post(url, data=data)
        # Print the response to check if the login was successful
        self.log.info(f"Status Code: {response.status_code}")
        self.log.info(f"Response Text: {response.text}")
        if response.status_code == 200:
            return True
        else:
            return False

    def test_manual_lock_unlock(self):
        """
        lock
        check access
        unlock
        check access
        """
        self.log.info("Locking user account: {0}".format(self.test_user))
        self.rest.set_unset_user_lock(self.test_user, "true")

        assert not self.check_ui_login(self.master, self.test_user, self.test_user_password)

        self.log.info("Unlocking user account: {0}".format(self.test_user))
        self.rest.set_unset_user_lock(self.test_user, "false")

        assert self.check_ui_login(self.master, self.test_user, self.test_user_password)

    def test_password_reset(self):
        """
        add user, check password reset
        access pools default/ui
        change password
            last one
            new one
        access pools default/ui
        """
        self.log.info("Adding an internal user while enforcing password change")
        payload = "name={0}&roles={1}&password={2}&temporaryPassword=true".format("user1", "admin", "password1")
        self.rest.add_set_builtin_user("user1", payload)

        assert not self.check_ui_login(self.master, "user1", "password1")

        status, content, header = self.rest.change_password_user("user1", "password1", "password1")
        if status:
            self.fail("Reuse of last password should not be allowed")
        else:
            self.log.info("Failed as expected. Can't reuse the last password")

        status, content, header = self.rest.change_password_user("user1", "password1", "password2")
        if status:
            self.log.info("Password changed after the first logon")
        else:
            self.fail("Password change failed with error: {0}.".format(content))

        assert self.check_ui_login(self.master, "user1", "password2")

    def test_last_usage_date(self):
        """
        check no last date
        enable tracking
        sleep
        check last date and verify date
        """
        users = self.rest.retrieve_user_roles()
        for user in users:
            if user["id"] == self.test_user:
                if user.get("last_activity_time"):
                    self.fail("Last activity time should not be tracked")
                else:
                    self.log.info("Last activity time is not tracked as expected")
        self.rest.configure_user_activity({"enabled": "true"})
        assert self.check_ui_login(self.master, self.test_user, self.test_user_password)
        assert self.check_ui_login(self.master, self.test_user, self.test_user_password)
        time.sleep(60 * 15)  # takes minimum 15 minutes
        users = self.rest.retrieve_user_roles()
        for user in users:
            if user["id"] == self.test_user:
                if user.get("last_activity_time"):
                    self.log.info("Last activity time is shown as follows: {0}".format(user["last_activity_time"]))
                else:
                    self.fail("Last activity time is not being tracked, takes time")

        self.rest.configure_user_activity({"enabled": "false"})
        assert self.check_ui_login(self.master, self.test_user, self.test_user_password)
        assert self.check_ui_login(self.master, self.test_user, self.test_user_password)
        time.sleep(60 * 15)  # takes minimum 15 minutes
        users = self.rest.retrieve_user_roles()
        for user in users:
            if user["id"] == self.test_user:
                if user.get("last_activity_time"):
                    self.fail("Last activity time should not be tracked")
                else:
                    self.log.info("Last activity time is not tracked as expected")

    def test_non_existing_user(self):
        self.log.info("Locking user account: {0}".format("notReal"))
        status, _, _ = self.rest.set_unset_user_lock("notReal", "true")
        if status:
            self.fail("Lock atttempt on a non existing user was sucessful")

    def test_user_accounts_unauthz(self):
        """
        create less permission users
        check for locking and unlocking users
        check for managing user activity
        """

        # Include few non-admin roles
        self.log.info("Add few non-admin users")
        roles = ['cluster_admin', 'ro_admin', 'user_admin_external']
        for role in roles:
            user_name = "user_" + role
            role = role
            password = "password"
            payload = "name={0}&roles={1}&password={2}".format(user_name, role, password)
            self.log.info("User name -- {0} :: role -- {1}".format(user_name, role))
            self.rest.add_set_builtin_user(user_name, payload)

            serverinfo_dict = {"ip": self.master.ip, "port": self.master.port,
                               "username": user_name, "password": password}
            rest_non_admin = RestConnection(serverinfo_dict)

            self.log.info("Locking user account: {0}".format(self.test_user))
            status, content, header = rest_non_admin.set_unset_user_lock(self.test_user, "true")
            if not status:
                self.log.info("Locking user failed as expected")
            else:
                self.fail("Locking user should have failed for unauthorised user")

            self.log.info("Managing user account activity")
            status, content, header = rest_non_admin.configure_user_activity({"enabled": "true"})
            if not status:
                self.log.info("Managing user account activity failed as expected")
            else:
                self.fail("Managing user account activity should have failed for unauthorised user")

    def test_user_backup(self):
        """
        create users
        backup
        check fields
            locked
            expiry
        """
        user_name1 = "user1"
        payload = "name={0}&roles={1}&password={2}".format(user_name1, "admin", "password")
        self.log.info("User name -- {0} :: role -- {1}".format(user_name1, "admin"))
        self.rest.add_set_builtin_user(user_name1, payload)

        self.rest.set_unset_user_lock(user_name1, "true")

        user_name2 = "user2"
        payload = "name={0}&roles={1}&password={2}&temporaryPassword=true".format(user_name2, "admin", "password")
        self.log.info("User name -- {0} :: role -- {1}".format(user_name2, "admin"))
        self.rest.add_set_builtin_user(user_name2, payload)

        status, content, header = self.rest.backup_rbac_users()
        users = json.loads(content)["users"]
        for user in users:
            if user["id"] == user_name1:
                if user.get("locked"):
                    self.log.info("Locked state backed up for the given user")
                    assert user["locked"]
                else:
                    self.fail("Locked state not backed up for the given user")

            if user["id"] == user_name2:
                if "expiry" in user["auth"]:
                    self.log.info("Password logon state backed up for the given user")
                    assert user["auth"]["expiry"] == 0
                else:
                    self.fail("Password logon state not backed up for the given user")

    def test_admin_user_lock(self):
        """
        lock
        check
        unlock
        check
        """
        cmd = "/opt/couchbase/bin/couchbase-cli admin-manage --lock"
        self.remote_connection.execute_command(cmd)

        assert not self.check_ui_login(self.master, "Administrator", "password")

        cmd = "/opt/couchbase/bin/couchbase-cli admin-manage --unlock"
        self.remote_connection.execute_command(cmd)

        assert self.check_ui_login(self.master, "Administrator", "password")

    def validate_logs_lock_status(self, start_time, end_time):
        """
        {"description":"User was either locked or unlocked","id":8276,"identity":{"domain":"local","user":"user2"},
        "local":{"ip":"172.23.216.134","port":8091},"locked":true,"name":"user locked/unlocked",
        "real_userid":{"domain":"builtin","user":"Administrator"},"remote":{"ip":"172.16.1.94","port":63426},
        "sessionid":"6349f33a377f0dcea1115fef1aa776ce3bc1dd55","timestamp":"2025-01-16T12:51:19.300+05:30"}
        """
        logs_dir = testconstants.LINUX_COUCHBASE_LOGS_PATH

        def check_logs(grep_output_list):
            for line in grep_output_list:
                # eg: 2021-07-12T04:03:45
                timestamp_regex = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+")
                match_obj = timestamp_regex.search(line)
                if not match_obj:
                    self.log.info("%s does not match any timestamp" % line)
                    return True
                timestamp = match_obj.group()
                timestamp = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f")
                if start_time <= timestamp <= end_time:
                    return True
            return False

        log_files = self.remote_connection.execute_command(
            "ls " + os.path.join(logs_dir, "current-audit.log"))[0]

        log_file = log_files[0]
        log_file = log_file.strip("\n")
        grep_for_str = "User was either locked or unlocked"
        cmd_to_run = "grep -r '%s' %s" \
                     % (grep_for_str, log_file)
        grep_output = self.remote_connection.execute_command(cmd_to_run)[0]
        return check_logs(grep_output)

    def validate_logs_activity_config(self, start_time, end_time):
        logs_dir = testconstants.LINUX_COUCHBASE_LOGS_PATH

        def check_logs(grep_output_list):
            for line in grep_output_list:
                # eg: 2021-07-12T04:03:45
                timestamp_regex = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+")
                match_obj = timestamp_regex.search(line)
                if not match_obj:
                    self.log.info("%s does not match any timestamp" % line)
                    return True
                timestamp = match_obj.group()
                timestamp = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f")
                if start_time <= timestamp <= end_time:
                    return True
            return False

        log_files = self.remote_connection.execute_command(
            "ls " + os.path.join(logs_dir, "current-audit.log"))[0]

        log_file = log_files[0]
        log_file = log_file.strip("\n")
        grep_for_str = "User activity"
        cmd_to_run = "grep -r '%s' %s" \
                     % (grep_for_str, log_file)
        grep_output = self.remote_connection.execute_command(cmd_to_run)[0]
        return check_logs(grep_output)

    def validate_logs_temporary_password(self, start_time, end_time):
        """
        {"description":"User was added or updated","full_name":"","groups":[],"id":8232,
        "identity":{"domain":"local","user":"user3"},"local":{"ip":"172.23.216.134","port":8091},"locked":false,
        "name":"set user","real_userid":{"domain":"builtin","user":"Administrator"},"reason":"updated",
        "remote":{"ip":"172.16.1.94","port":64216},"roles":["admin"],
        "sessionid":"c1624df19f0dc312ca260bfe534e51faa80da218","temporary_password":true,
        "timestamp":"2025-01-16T12:53:46.570+05:30"}
        """
        logs_dir = testconstants.LINUX_COUCHBASE_LOGS_PATH

        def check_logs(grep_output_list):
            for line in grep_output_list:
                # eg: 2021-07-12T04:03:45
                timestamp_regex = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+")
                match_obj = timestamp_regex.search(line)
                if not match_obj:
                    self.log.info("%s does not match any timestamp" % line)
                    return True
                timestamp = match_obj.group()
                timestamp = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f")
                if start_time <= timestamp <= end_time:
                    return True
            return False

        log_files = self.remote_connection.execute_command(
            "ls " + os.path.join(logs_dir, "current-audit.log"))[0]

        log_file = log_files[0]
        log_file = log_file.strip("\n")
        grep_for_str1 = "User was added or updated"
        grep_for_str2 = "temporary_password"
        cmd_to_run = "grep -r '%s' %s | grep %s" \
                     % (grep_for_str1, log_file, grep_for_str2)
        grep_output = self.remote_connection.execute_command(cmd_to_run)[0]
        return check_logs(grep_output)

    def test_log(self):
        """
        - Lock unlock should be logged
        - Tracking user activity should be logged
        - Logs should not leak any sensitive information

        - Logged in audit.log
        - "id": xxxx
        - "name": "xxx"
        - "description": "xxx"
        """
        # Enable auditing
        self.rest.setAuditSettings()

        # Lock unlock users
        start_time = datetime.now()
        self.rest.set_unset_user_lock(self.test_user, "true")
        self.sleep(5)
        end_time = datetime.now()
        self.log.info("Start time: {0}".format(start_time))
        self.log.info("End time: {0}".format(end_time))
        validate_result = self.validate_logs_lock_status(start_time, end_time)
        self.log.info("Validation result: {}".format(validate_result))
        if not validate_result:
            self.fail("Did not find any logs in the specified interval")
        else:
            self.log.info("Log found")

        # Track user activity
        start_time = datetime.now()
        self.rest.configure_user_activity({"enabled": "true"})
        time.sleep(10)
        end_time = datetime.now()
        self.log.info("Start time: {0}".format(start_time))
        self.log.info("End time: {0}".format(end_time))
        validate_result = self.validate_logs_activity_config(start_time, end_time)
        self.log.info("Validation result: {}".format(validate_result))
        if not validate_result:
            self.fail("Did not find any logs in the specified interval")
        else:
            self.log.info("Log found")

        # Set temporary password
        start_time = datetime.now()
        payload = "name={0}&roles={1}&password={2}&temporaryPassword=true".format("log_test_user", "admin",
                                                                                  "password")
        self.rest.add_set_builtin_user("log_test_user", payload)
        time.sleep(10)
        end_time = datetime.now()
        self.log.info("Start time: {0}".format(start_time))
        self.log.info("End time: {0}".format(end_time))
        validate_result = self.validate_logs_temporary_password(start_time, end_time)
        self.log.info("Validation result: {}".format(validate_result))
        if not validate_result:
            self.fail("Did not find any logs in the specified interval")
        else:
            self.log.info("Log found")

    def test_group_activity(self):
        """
        Two groups
            grp_pos
            grp_neg
        Two users
            user_grp_pos
            user_grp_neg
        enable user activity
            grp_pos
        verify
            user activity shown for user_grp_pos
            user activity not shown for user_grp_neg
        """
        self.rest.add_group_role("grp_pos", "", "analytics_manager[*]")
        self.rest.add_group_role("grp_neg", "", "admin")
        self.rest.get_group_list()

        payload = "name={0}&groups={1}&password={2}".format("user_grp_pos", "grp_pos", "password")
        self.rest.add_set_builtin_user("user_grp_pos", payload)
        payload = "name={0}&groups={1}&password={2}".format("user_grp_neg", "grp_neg", "password")
        self.rest.add_set_builtin_user("user_grp_neg", payload)

        self.rest.retrieve_user_roles()

        param = {
            "enabled": "true",
            "trackedGroups": ["grp_pos"],
            "trackedRoles": []
        }
        self.rest.configure_user_activity(param)
        assert self.check_ui_login(self.master, "user_grp_pos", self.test_user_password)
        assert self.check_ui_login(self.master, "user_grp_neg", self.test_user_password)
        time.sleep(60 * 15)  # takes minimum 15 minutes

        users = self.rest.retrieve_user_roles()

        for user in users:
            if user["id"] == "user_grp_neg":
                if user.get("last_activity_time"):
                    self.fail("Last activity time should not be tracked")
                else:
                    self.log.info("Last activity time is not tracked as expected")

            if user["id"] == "user_grp_pos":
                if user.get("last_activity_time"):
                    self.log.info("Last activity time is shown as follows: {0}".format(user["last_activity_time"]))
                else:
                    self.fail("Last activity time is not being tracked, takes time")

    def test_roles_activity(self):
        """
        Two roles
            analytics_manager[*]
            ro_admin
        Two users
            user_role_pos
            user_role_neg
        enable user activity
            analytics_manager[*]
        verify
            user activity shown for user_role_pos
            user activity not shown for user_role_neg
        """

        payload = "name={0}&roles={1}&password={2}".format("user_role_pos", "analytics_manager[*]", "password")
        self.rest.add_set_builtin_user("user_role_pos", payload)
        payload = "name={0}&roles={1}&password={2}".format("user_role_neg", "ro_admin", "password")
        self.rest.add_set_builtin_user("user_role_neg", payload)

        param = {
            "enabled": "true",
            "trackedGroups": [],
            "trackedRoles": ["analytics_manager"]
        }
        self.rest.configure_user_activity(param)
        assert self.check_ui_login(self.master, "user_role_pos", self.test_user_password)
        assert self.check_ui_login(self.master, "user_role_neg", self.test_user_password)
        self.log.info("Going to sleep for 15 minutes as it takes 15 minutes, minimum")
        time.sleep(60 * 15)  # takes minimum 15 minutes

        users = self.rest.retrieve_user_roles()

        for user in users:
            if user["id"] == "user_role_neg":
                if user.get("last_activity_time"):
                    self.fail("Last activity time should not be tracked")
                else:
                    self.log.info("Last activity time is not tracked as expected")

            if user["id"] == "user_role_pos":
                if user.get("last_activity_time"):
                    self.log.info("Last activity time is shown as follows: {0}".format(user["last_activity_time"]))
                else:
                    self.fail("Last activity time is not being tracked, takes time")

    def test_volume(self):
        """
        Test locking and unlocking all user accounts in bulk.
        """
        self.user_count = 10  # Number of users for volume testing
        self.test_users = [
            {
                "username": "vol_user" + str(i),
                "password": "password" + str(i),
                "roles": "admin",
            }
            for i in range(self.user_count)
        ]

        # Create users in bulk
        for user in self.test_users:
            payload = "name={0}&roles={1}&password={2}".format(user["username"], user["roles"], user["password"])
            self.rest.add_set_builtin_user(user["username"], payload)

        for user in self.test_users:
            self.log.info("User: {0}".format(user))
            # Lock the user
            self.rest.set_unset_user_lock(user["username"], "true")

            self.sleep(10, "Waiting for user to be locked")

            # Verify the user is locked
            assert not self.check_ui_login(self.master, user["username"], user["password"])

            # Unlock the user
            self.rest.set_unset_user_lock(user["username"], "false")

            self.sleep(10, "Waiting for user to be unlocked")

            # Verify the user is unlocked
            assert self.check_ui_login(self.master, user["username"], user["password"])

    def test_sdk_lock(self):
        """
        - Lock/unlock
        """
        """
        self.log.info("Locking user account: {0}".format(self.test_user))
        self.rest.set_unset_user_lock(self.test_user, "true")

        bucket_name = "travel-sample"
        ip = self.master.ip
        username = self.test_user
        password = self.test_user_password
        url = 'couchbase://{ip}/{name}'.format(ip=ip, name=bucket_name)
        bucket = Bucket(url, username=username, password=password)
        data = bucket.get("airline_10")
        print(json.dumps(data.value, indent=4))

        self.log.info("Unlocking user account: {0}".format(self.test_user))
        self.rest.set_unset_user_lock(self.test_user, "false")

        bucket_name = "travel-sample"
        ip = self.master.ip
        username = self.test_user
        password = self.test_user_password
        url = 'couchbase://{ip}/{name}'.format(ip=ip, name=bucket_name)
        bucket = Bucket(url, username=username, password=password)
        data = bucket.get("airline_10")
        print(json.dumps(data.value, indent=4))
        """

        self.sleep(300, "Wait for sample bucket to come up")
        buckets = RestConnection(self.master).get_buckets()
        found_bucket = False
        for bucket in buckets:
            if bucket.name == "travel-sample":
                found_bucket = True
                continue
        if not found_bucket:
            self.fail("travel-sample bucket not created")

        self.log.info("Locking user account: {0}".format(self.test_user))
        self.log.info("Attempting to lock user account: {}".format(self.test_user))
        self.rest.set_unset_user_lock(self.test_user, "true")
        self.log.info("User {} locked successfully.".format(self.test_user))

        bucket_name = "travel-sample"
        ip = self.master.ip
        username = self.test_user
        password = self.test_user_password
        self.log.info("Bucket Name: {}, IP: {}, Username: {}".format(bucket_name, ip, username))

        url = 'couchbase://{}/{}'.format(ip, bucket_name)
        self.log.info("Connecting to Couchbase at: {}".format(url))

        try:
            Bucket(url, username=username, password=password)
            self.log.info("Couchbase bucket connection established.")
        except Exception as e:
            self.log.info(e)
        else:
            self.fail("Authentication should have failed as the user is locked")

        self.log.info("Unlocking user account: {0}".format(self.test_user))
        self.log.info("Attempting to unlock user account: {}".format(self.test_user))
        self.rest.set_unset_user_lock(self.test_user, "false")
        self.log.info("User {} unlocked successfully.".format(self.test_user))

        self.sleep(10, "Wait for some time before connecting to the bucket again")

        self.log.info("Bucket Name: {}, IP: {}, Username: {}".format(bucket_name, ip, username))

        url = 'couchbase://{}/{}'.format(ip, bucket_name)
        self.log.info("Connecting to Couchbase at: {}".format(url))

        bucket = Bucket(url, username=username, password=password)
        self.log.info("Couchbase bucket connection established again.")

        data = bucket.get("airline_10")
        self.log.info("Data retrieved successfully from bucket after unlocking.")
        self.log.info(json.dumps(data.value, indent=4))

    def test_sdk_temp_password(self):
        """
        - temp password
        """
        self.log.info("Sleeping for few seconds before fetching buckets")
        time.sleep(3)
        buckets = RestConnection(self.master).get_buckets()
        found_bucket = False
        for bucket in buckets:
            if bucket.name == "travel-sample":
                found_bucket = True
                continue
        if not found_bucket:
            self.fail("travel-sample bucket not created")

        self.log.info("Adding an internal user while enforcing password change")
        payload = "name={0}&roles={1}&password={2}&temporaryPassword=true".format("user1", "admin", self.test_user_password)
        self.rest.add_set_builtin_user("user1", payload)

        bucket_name = "travel-sample"
        ip = self.master.ip
        username = "user1"
        password = self.test_user_password
        url = 'couchbase://{ip}/{name}'.format(ip=ip, name=bucket_name)

        try:
            Bucket(url, username=username, password=password)
            self.log.info("Couchbase bucket connection established.")
            self.fail("Authentication should have failed as the user has temporary password")
        except CBException as e:
            self.log.info(e)
            self.log.info("Expected auth failure as the user had temporary password")
        except Exception as e:
            self.fail(f"Failing due to exception: {e}")

        o, err = RemoteMachineShellConnection(self.master).change_user_password(username, self.test_user_password, self.test_user_new_password)
        if err:
            self.fail("Failed to change password. Exiting test.....")

        self.sleep(10, "Wait for some time before connecting to the bucket again")

        url = 'couchbase://{}/{}'.format(ip, bucket_name)
        self.log.info("Connecting to Couchbase at: {}".format(url))

        bucket = Bucket(url, username=username, password=self.test_user_new_password)
        self.log.info("Couchbase bucket connection established again.")

        data = bucket.get("airline_10")
        self.log.info("Data retrieved successfully from bucket after resetting the password.")
        self.log.info(json.dumps(data.value, indent=4))
