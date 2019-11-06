import time, os, json

from threading import Thread
import threading
import random
from basetestcase import BaseTestCase
from rebalance.rebalance_base import RebalanceBaseTest
from membase.api.exception import RebalanceFailedException
from membase.api.rest_client import RestConnection, RestHelper
from couchbase_helper.documentgenerator import BlobGenerator
from membase.helper.rebalance_helper import RebalanceHelper
from remote.remote_util import RemoteMachineShellConnection
from membase.helper.cluster_helper import ClusterOperationHelper
from membase.helper.bucket_helper import BucketOperationHelper


class AutoRetryFailedRebalance(RebalanceBaseTest):

    def setUp(self):
        super(AutoRetryFailedRebalance, self).setUp()
        self.rest = RestConnection(self.servers[0])
        self.sleep_time = self.input.param("sleep_time", 15)
        self.enabled = self.input.param("enabled", True)
        self.afterTimePeriod = self.input.param("afterTimePeriod", 300)
        self.maxAttempts = self.input.param("maxAttempts", 1)
        self.log.info("Changing the retry rebalance settings ....")
        self.change_retry_rebalance_settings(enabled=self.enabled, afterTimePeriod=self.afterTimePeriod,
                                             maxAttempts=self.maxAttempts)
        self.rebalance_operation = self.input.param("rebalance_operation", "rebalance_out")
        self.disable_auto_failover = self.input.param("disable_auto_failover", True)
        self.auto_failover_timeout = self.input.param("auto_failover_timeout", 120)
        if self.disable_auto_failover:
            self.rest.update_autofailover_settings(False, 120)
        else:
            self.rest.update_autofailover_settings(True, self.auto_failover_timeout)

    def tearDown(self):
        self.reset_retry_rebalance_settings()
        # Reset to default value
        super(AutoRetryFailedRebalance, self).tearDown()
        rest = RestConnection(self.servers[0])
        zones = rest.get_zone_names()
        for zone in zones:
            if zone != "Group 1":
                rest.delete_zone(zone)

    def test_auto_retry_of_failed_rebalance_where_failure_happens_before_rebalance(self):
        before_rebalance_failure = self.input.param("before_rebalance_failure", "stop_server")
        # induce the failure before the rebalance starts
        self._induce_error(before_rebalance_failure)
        self.sleep(self.sleep_time)
        try:
            operation = self._rebalance_operation(self.rebalance_operation)
            operation.result()
        except Exception as e:
            self.log.info("Rebalance failed with : {0}".format(str(e)))
            # Recover from the error
            self._recover_from_error(before_rebalance_failure)
            self.check_retry_rebalance_succeeded()
        else:
            self.fail("Rebalance did not fail as expected. Hence could not validate auto-retry feature..")
        finally:
            if self.disable_auto_failover:
                self.rest.update_autofailover_settings(True, 120)
            self.start_server(self.servers[1])
            self.stop_firewall_on_node(self.servers[1])

    def test_auto_retry_of_failed_rebalance_where_failure_happens_during_rebalance(self):
        during_rebalance_failure = self.input.param("during_rebalance_failure", "stop_server")
        try:
            operation = self._rebalance_operation(self.rebalance_operation)
            self.sleep(self.sleep_time)
            # induce the failure during the rebalance
            self._induce_error(during_rebalance_failure)
            operation.result()
        except Exception as e:
            self.log.info("Rebalance failed with : {0}".format(str(e)))
            # Recover from the error
            self._recover_from_error(during_rebalance_failure)
            self.check_retry_rebalance_succeeded()
        else:
            # This is added as the failover task is not throwing exception
            if self.rebalance_operation == "graceful_failover":
                # Recover from the error
                self._recover_from_error(during_rebalance_failure)
                self.check_retry_rebalance_succeeded()
            else:
                self.fail("Rebalance did not fail as expected. Hence could not validate auto-retry feature..")
        finally:
            if self.disable_auto_failover:
                self.rest.update_autofailover_settings(True, 120)
            self.start_server(self.servers[1])
            self.stop_firewall_on_node(self.servers[1])

    def test_auto_retry_of_failed_rebalance_does_not_get_triggered_when_rebalance_is_stopped(self):
        operation = self._rebalance_operation(self.rebalance_operation)
        reached = RestHelper(self.rest).rebalance_reached(30)
        self.assertTrue(reached, "Rebalance failed or did not reach {0}%".format(30))
        self.rest.stop_rebalance(wait_timeout=self.sleep_time)
        result = json.loads(self.rest.get_pending_rebalance_info())
        self.log.info(result)
        retry_rebalance = result["retry_rebalance"]
        if retry_rebalance != "not_pending":
            self.fail("Auto-retry succeeded even when Rebalance was stopped by user")

    def test_negative_auto_retry_of_failed_rebalance_where_rebalance_will_be_cancelled(self):
        during_rebalance_failure = self.input.param("during_rebalance_failure", "stop_server")
        post_failure_operation = self.input.param("post_failure_operation", "cancel_pending_rebalance")
        try:
            operation = self._rebalance_operation(self.rebalance_operation)
            self.sleep(self.sleep_time)
            # induce the failure during the rebalance
            self._induce_error(during_rebalance_failure)
            operation.result()
        except Exception as e:
            self.log.info("Rebalance failed with : {0}".format(str(e)))
            # Recover from the error
            self._recover_from_error(during_rebalance_failure)
            result = json.loads(self.rest.get_pending_rebalance_info())
            self.log.info(result)
            retry_rebalance = result["retry_rebalance"]
            rebalance_id = result["rebalance_id"]
            if retry_rebalance != "pending":
                self.fail("Auto-retry of failed rebalance is not triggered")
            if post_failure_operation == "cancel_pending_rebalance":
                # cancel pending rebalance
                self.log.info("Cancelling rebalance Id: {0}".format(rebalance_id))
                self.rest.cancel_pending_rebalance(rebalance_id)
            elif post_failure_operation == "disable_auto_retry":
                # disable the auto retry of the failed rebalance
                self.log.info("Disable the the auto retry of the failed rebalance")
                self.change_retry_rebalance_settings(enabled=False)
            elif post_failure_operation == "retry_failed_rebalance_manually":
                # retry failed rebalance manually
                self.log.info("Retrying failed rebalance Id: {0}".format(rebalance_id))
                self.cluster.rebalance(self.servers[:self.nodes_init], [], [])
            else:
                self.fail("Invalid post_failure_operation option")
            # Now check and ensure retry won't happen
            result = json.loads(self.rest.get_pending_rebalance_info())
            self.log.info(result)
            retry_rebalance = result["retry_rebalance"]
            if retry_rebalance != "not_pending":
                self.fail("Auto-retry of failed rebalance is not cancelled")
        else:
            self.fail("Rebalance did not fail as expected. Hence could not validate auto-retry feature..")
        finally:
            if self.disable_auto_failover:
                self.rest.update_autofailover_settings(True, 120)
            self.start_server(self.servers[1])
            self.stop_firewall_on_node(self.servers[1])

    def test_negative_auto_retry_of_failed_rebalance_where_rebalance_will_not_be_cancelled(self):
        during_rebalance_failure = self.input.param("during_rebalance_failure", "stop_server")
        post_failure_operation = self.input.param("post_failure_operation", "create_delete_buckets")
        zone_name = "Group_{0}_{1}".format(random.randint(1, 1000000000), self._testMethodName)
        zone_name = zone_name[0:60]
        default_zone = "Group 1"
        moved_node = []
        moved_node.append(self.servers[1].ip)
        try:
            operation = self._rebalance_operation(self.rebalance_operation)
            self.sleep(self.sleep_time)
            # induce the failure during the rebalance
            self._induce_error(during_rebalance_failure)
            operation.result()
        except Exception as e:
            self.log.info("Rebalance failed with : {0}".format(str(e)))
            # Recover from the error
            self._recover_from_error(during_rebalance_failure)
            result = json.loads(self.rest.get_pending_rebalance_info())
            self.log.info(result)
            retry_rebalance = result["retry_rebalance"]
            if retry_rebalance != "pending":
                self.fail("Auto-retry of failed rebalance is not triggered")
            if post_failure_operation == "create_delete_buckets":
                # delete buckets and create new one
                BucketOperationHelper.delete_all_buckets_or_assert(servers=self.servers, test_case=self)
                self.sleep(self.sleep_time)
                BucketOperationHelper.create_bucket(self.master, test_case=self)
            elif post_failure_operation == "change_replica_count":
                # change replica count
                self.log.info("Changing replica count of buckets")
                for bucket in self.buckets:
                    self.rest.change_bucket_props(bucket, replicaNumber=2)
            elif post_failure_operation == "change_server_group":
                # change server group
                self.log.info("Creating new zone " + zone_name)
                self.rest.add_zone(zone_name)
                self.log.info("Moving {0} to new zone {1}".format(moved_node, zone_name))
                status = self.rest.shuffle_nodes_in_zones(moved_node, default_zone, zone_name)
            else:
                self.fail("Invalid post_failure_operation option")
            # In these failure scenarios while the retry is pending, then the retry will be attempted but fail
            try:
                self.check_retry_rebalance_succeeded()
            except Exception as e:
                self.log.info(e)
                if "Retrying of rebalance still did not help. All the retries exhausted" not in str(e):
                    self.fail("Auto retry of failed rebalance succeeded when it was expected to fail")
        else:
            self.fail("Rebalance did not fail as expected. Hence could not validate auto-retry feature..")
        finally:
            if post_failure_operation == "change_server_group":
                status = self.rest.shuffle_nodes_in_zones(moved_node, zone_name, default_zone)
                self.log.info("Shuffle the node back to default group . Status : {0}".format(status))
                self.sleep(self.sleep_time)
                self.log.info("Deleting new zone " + zone_name)
                try:
                    self.rest.delete_zone(zone_name)
                except:
                    self.log.info("Errors in deleting zone")
            if self.disable_auto_failover:
                self.rest.update_autofailover_settings(True, 120)
            self.start_server(self.servers[1])
            self.stop_firewall_on_node(self.servers[1])

    def test_auto_retry_of_failed_rebalance_with_rebalance_test_conditions(self):
        test_failure_condition = self.input.param("test_failure_condition")
        # induce the failure before the rebalance starts
        self._induce_rebalance_test_condition(test_failure_condition)
        self.sleep(self.sleep_time)
        try:
            operation = self._rebalance_operation(self.rebalance_operation)
            operation.result()
        except Exception as e:
            self.log.info("Rebalance failed with : {0}".format(str(e)))
            # Delete the rebalance test condition so that we recover from the error
            self._delete_rebalance_test_condition(test_failure_condition)
            self.check_retry_rebalance_succeeded()
        else:
            self.fail("Rebalance did not fail as expected. Hence could not validate auto-retry feature..")
        finally:
            if self.disable_auto_failover:
                self.rest.update_autofailover_settings(True, 120)
            self._delete_rebalance_test_condition(test_failure_condition)

    def test_auto_retry_of_failed_rebalance_with_autofailvoer_enabled(self):
        before_rebalance_failure = self.input.param("before_rebalance_failure", "stop_server")
        # induce the failure before the rebalance starts
        self._induce_error(before_rebalance_failure)
        try:
            operation = self._rebalance_operation(self.rebalance_operation)
            operation.result()
        except Exception as e:
            self.log.info("Rebalance failed with : {0}".format(str(e)))
            if self.auto_failover_timeout < self.afterTimePeriod:
                self.sleep(self.auto_failover_timeout)
                result = json.loads(self.rest.get_pending_rebalance_info())
                self.log.info(result)
                retry_rebalance = result["retry_rebalance"]
                if retry_rebalance != "not_pending":
                    self.fail("Auto-failover did not cancel pending retry of the failed rebalance")
            else:
                try:
                    self.check_retry_rebalance_succeeded()
                except Exception as e:
                    if "Retrying of rebalance still did not help" not in str(e):
                        self.fail("retry rebalance succeeded even without failover")
                    self.sleep(self.auto_failover_timeout)
                    self.cluster.rebalance(self.servers[:self.nodes_init], [], [])
        else:
            self.fail("Rebalance did not fail as expected. Hence could not validate auto-retry feature..")
        finally:
            if self.disable_auto_failover:
                self.rest.update_autofailover_settings(True, 120)
            self.start_server(self.servers[1])
            self.stop_firewall_on_node(self.servers[1])

    def _rebalance_operation(self, rebalance_operation):
        self.log.info("Starting rebalance operation of type : {0}".format(rebalance_operation))
        if rebalance_operation == "rebalance_out":
            operation = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], self.servers[1:])
        elif rebalance_operation == "rebalance_in":
            operation = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                     [self.servers[self.nodes_init]], [])
        elif rebalance_operation == "swap_rebalance":
            self.rest.add_node(self.master.rest_username, self.master.rest_password,
                               self.servers[self.nodes_init].ip, self.servers[self.nodes_init].port)
            operation = self.cluster.async_rebalance(self.servers[:self.nodes_init], []
                                                     , [self.servers[self.nodes_init - 1]])
        elif rebalance_operation == "graceful_failover":
            # TODO : retry for graceful failover is not yet implemented
            operation = self.cluster.async_failover([self.master], failover_nodes=[self.servers[1]],
                                                    graceful=True, wait_for_pending=120)
        return operation

    def _induce_error(self, error_condition):
        if error_condition == "stop_server":
            self.stop_server(self.servers[1])
        elif error_condition == "enable_firewall":
            self.start_firewall_on_node(self.servers[1])
        elif error_condition == "kill_memcached":
            self.kill_server_memcached(self.servers[1])
        elif error_condition == "reboot_server":
            shell = RemoteMachineShellConnection(self.servers[1])
            shell.reboot_node()
        elif error_condition == "kill_erlang":
            shell = RemoteMachineShellConnection(self.servers[1])
            shell.kill_erlang()
            self.sleep(self.sleep_time * 3)
        else:
            self.fail("Invalid error induce option")

    def _recover_from_error(self, error_condition):
        if error_condition == "stop_server" or error_condition == "kill_erlang":
            self.start_server(self.servers[1])
            self.sleep(self.sleep_time * 4)
        elif error_condition == "enable_firewall":
            self.stop_firewall_on_node(self.servers[1])
        elif error_condition == "reboot_server":
            self.sleep(self.sleep_time * 4)
            # wait till node is ready after warmup
            ClusterOperationHelper.wait_for_ns_servers_or_assert([self.servers[1]], self, wait_if_warmup=True)

    def _induce_rebalance_test_condition(self, test_failure_condition):
        if test_failure_condition == "verify_replication":
            set_command = "testconditions:set(verify_replication, {fail, \"" + "default" + "\"})"
        elif test_failure_condition == "backfill_done":
            set_command = "testconditions:set(backfill_done, {for_vb_move, \"" + "default\", 1 , " + "fail})"
        else:
            set_command = "testconditions:set({0}, fail)".format(test_failure_condition)
        get_command = "testconditions:get({0})".format(test_failure_condition)
        for server in self.servers:
            rest = RestConnection(server)
            _, content = rest.diag_eval(set_command)
            self.log.info("Command : {0} Return : {1}".format(set_command, content))

        for server in self.servers:
            rest = RestConnection(server)
            _, content = rest.diag_eval(get_command)
            self.log.info("Command : {0} Return : {1}".format(get_command, content))

    def _delete_rebalance_test_condition(self, test_failure_condition):
        delete_command = "testconditions:delete({0})".format(test_failure_condition)
        get_command = "testconditions:get({0})".format(test_failure_condition)
        for server in self.servers:
            rest = RestConnection(server)
            _, content = rest.diag_eval(delete_command)
            self.log.info("Command : {0} Return : {1}".format(delete_command, content))

        for server in self.servers:
            rest = RestConnection(server)
            _, content = rest.diag_eval(get_command)
            self.log.info("Command : {0} Return : {1}".format(get_command, content))
