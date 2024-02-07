import time
import threading
import json
from security.auditmain import audit
from tuqquery.tuq_join import JoinTests
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection
from membase.helper.cluster_helper import ClusterOperationHelper
from backuptests import BackupHelper
from .tuq_sanity import QuerySanityTests


class QueriesOpsTests(QuerySanityTests):
    def setUp(self):
        self.cluster_ops = True
        super(QueriesOpsTests, self).setUp()
        self.setup_query_nodes = self.input.param("setup_query_nodes", False)
        self.query_params = {'scan_consistency': 'statement_plus'}
        if self.nodes_init > 1 and not self._testMethodName == 'suite_setUp':
            if self.setup_query_nodes:
                self.cluster.rebalance(self.servers[:1], self.servers[1:self.nodes_init], [], services=['n1ql'])
            else:
                self.cluster.rebalance(self.servers[:1], self.servers[1:self.nodes_init], [])
        self.indx_type = self.input.param("indx_type", 'GSI')
        self.stop_server = self.input.param("stop_server", False)
        self.stop_source = self.input.param("stop_source", False)
        self.network_partition = self.input.param("network_partition", False)
        self.rebalance_index = self.input.param("rebalance_index", False)
        self.rebalance_n1ql = self.input.param("rebalance_n1ql", False)
        self.retry_time = self.input.param("retry_time", 300)
        self.rebalance_out = self.input.param("rebalance_out", False)
        self.num_retries = self.input.param("num_retries", 1)
        self.fail_status = False
        self.sleep_time = self.input.param("sleep_time", 10)
        self.query_buckets = self.get_query_buckets(check_all_buckets=True)


    def suite_setUp(self):
        super(QueriesOpsTests, self).suite_setUp()

    def tearDown(self):
        rest = RestConnection(self.master)
        if rest._rebalance_progress_status() == 'running':
            self.log.warning("rebalancing is still running, test should be verified")
            stopped = rest.stop_rebalance()
            self.assertTrue(stopped, msg="unable to stop rebalance")
        try:
            super(QueriesOpsTests, self).tearDown()
        except:
            pass
        try:
            ClusterOperationHelper.cleanup_cluster(self.servers, master=self.master)
            self.sleep(1)
        except:
            for server in set(self.servers) - {self.master}:
                try:
                    rest = RestConnection(server)
                    rest.force_eject_node()
                    time.sleep(1)
                except BaseException as e:
                    self.fail(e)

    def suite_tearDown(self):
        super(QueriesOpsTests, self).suite_tearDown()

    def test_incr_rebalance_in(self):
        self.assertTrue(len(self.servers) >= self.nodes_in + 1, "Servers are not enough")
        index_field = self.input.param("index_field", 'job_title')
        indexes = []
        try:
            indexes = self._create_multiple_indexes(index_field)
            self.test_min()
            for i in range(1, self.nodes_in + 1):
                rebalance = self.cluster.async_rebalance(self.servers[:i],
                                                         self.servers[i:i + 1], [])
                self.test_min()
                self.sleep(60)
                rebalance.result()
                self.test_min()
        finally:
            self._delete_multiple_indexes(indexes)

    def test_incr_rebalance_out(self):
        self.assertTrue(len(self.servers[:self.nodes_init]) > self.nodes_out,
                        "Servers are not enough")
        index_field = self.input.param("index_field", 'job_title')
        indexes = []
        try:
            indexes = self._create_multiple_indexes(index_field)
            self.test_min()
            for i in range(1, self.nodes_out + 1):
                rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init - (i - 1)],
                                                         [],
                                                         self.servers[self.nodes_init - i:self.nodes_init - (i - 1)])
                self.test_min()
                self.sleep(60)
                rebalance.result()
                self.test_min()
        finally:
            self._delete_multiple_indexes(indexes)

    def test_swap_rebalance(self):
        self.assertTrue(len(self.servers) >= self.nodes_init + self.nodes_in,
                        "Servers are not enough")
        index_field = self.input.param("index_field", 'name')
        indexes = []
        try:
            indexes = self._create_multiple_indexes(index_field)
            self.test_array_append()
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                     self.servers[self.nodes_init:self.nodes_init + self.nodes_in],
                                                     self.servers[self.nodes_init - self.nodes_out:self.nodes_init])
            self.test_array_append()
            self.sleep(60)
            rebalance.result()
            self.test_array_append()
        finally:
            self._delete_multiple_indexes(indexes)

    def test_rebalance_with_server_crash(self):
        servr_in = self.servers[self.nodes_init:self.nodes_init + self.nodes_in]
        servr_out = self.servers[self.nodes_init - self.nodes_out:self.nodes_init]
        self.test_case()
        for i in range(3):
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                     servr_in, servr_out)
            self.sleep(5, "Wait some time for rebalance process and then kill memcached")
            remote = RemoteMachineShellConnection(self.servers[self.nodes_init - 1])
            remote.terminate_process(process_name='memcached')
            self.test_case()
            try:
                rebalance.result()
            except:
                pass
        self.cluster.rebalance(self.servers[:self.nodes_init], servr_in, servr_out)
        self.test_case()

    def test_failover(self):
        index_field = self.input.param("index_field", 'name')
        indexes = []
        try:
            indexes = self._create_multiple_indexes(index_field)
            servr_out = self.servers[self.nodes_init - self.nodes_out:self.nodes_init]
            self.test_union()
            self.cluster.failover(self.servers[:self.nodes_init], servr_out)
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                     [], servr_out)
            self.test_union()
            self.sleep(60)
            rebalance.result()
            self.test_union()
        finally:
            self._delete_multiple_indexes(indexes)

    def test_failover_add_back(self):
        index_field = self.input.param("index_field", 'name')
        indexes = []
        try:
            indexes = self._create_multiple_indexes(index_field)
            servr_out = self.servers[self.nodes_init - self.nodes_out:self.nodes_init]
            self.test_union()

            nodes_all = RestConnection(self.master).node_statuses()
            nodes = []
            for failover_node in servr_out:
                nodes.extend([node for node in nodes_all
                              if node.ip == failover_node.ip and str(node.port) == failover_node.port])
            self.cluster.failover(self.servers[:self.nodes_init], servr_out)
            self.sleep(60)
            for node in nodes:
                RestConnection(self.master).add_back_node(node.id)
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [])
            self.test_union()
            self.sleep(60)
            rebalance.result()
            self.test_union()
        finally:
            self._delete_multiple_indexes(indexes)

    def test_autofailover(self):
        autofailover_timeout = 30
        status = RestConnection(self.master).update_autofailover_settings(True, autofailover_timeout)
        self.assertTrue(status, 'failed to change autofailover_settings!')
        servr_out = self.servers[self.nodes_init - self.nodes_out:self.nodes_init]
        self.test_union()
        remote = RemoteMachineShellConnection(self.servers[self.nodes_init - 1])
        try:
            remote.stop_server()
            self.sleep(autofailover_timeout + 10, "Wait for autofailover")
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                     [], servr_out)
            self.test_union()
            self.sleep(60)
            rebalance.result()
            self.test_union()
        finally:
            remote.start_server()

    def test_cancel_query_mb_9223(self):
        for query_bucket in self.query_buckets:
            self.query = 'SELECT tasks_points.task1 AS points FROM %s AS test ' % query_bucket + \
                         'GROUP BY test.tasks_points.task1 ORDER BY points'
            self.log.info("run query to cancel")
            try:
                RestConnection(self.master).query_tool(self.query, timeout=5)
            except:
                self.log.info("query is cancelled")
            full_list = self.generate_full_docs_list(self.gens_load)
            expected_result = [{"points": doc["tasks_points"]["task1"]} for doc in full_list]
            expected_result = [dict(y) for y in set(tuple(x.items()) for x in expected_result)]
            expected_result = sorted(expected_result, key=lambda doc: doc['points'])
            actual_result = self.run_cbq_query()
            self._verify_results(actual_result['results'], expected_result)

    def test_failover_with_server_crash(self):
        servr_out = self.servers[self.nodes_init - self.nodes_out:self.nodes_init]
        self.test_union()
        self.cluster.failover(self.servers[:self.nodes_init], servr_out)
        for i in range(3):
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                     [], servr_out)
            self.sleep(5, "Wait some time for rebalance process and then kill memcached")
            self.shell.terminate_process(process_name='memcached')
            self.test_union()
            try:
                rebalance.result()
            except:
                pass
        self.cluster.rebalance(self.servers[:self.nodes_init], [], servr_out)
        self.test_union()

    def test_warmup(self):
        index_field = self.input.param("index_field", 'name')
        indexes = []
        try:
            indexes = self._create_multiple_indexes(index_field)
            num_srv_warm_up = self.input.param("srv_warm_up", 1)
            if self.input.tuq_client is None:
                self.fail("For this test external tuq server is requiered. " + \
                          "Please specify one in conf")
            self.test_union_all()
            for server in self.servers[self.nodes_init - num_srv_warm_up:self.nodes_init]:
                remote = RemoteMachineShellConnection(server)
                remote.stop_server()
                remote.start_server()
                remote.disconnect()
            # run query, result may not be as expected, but tuq shouldn't fail
            try:
                self.test_union_all()
            except:
                pass
            ClusterOperationHelper.wait_for_ns_servers_or_assert(self.servers, self, wait_if_warmup=True)
            self.verify_cluster_stats(self.servers[:self.nodes_init])
            self.sleep(50)
            self.verify_cluster_stats(self.servers[:self.nodes_init])
            self.test_union_all()
        finally:
            self._delete_multiple_indexes(indexes)

    def test_with_backup(self):
        tmp_folder = "/tmp/backup"
        try:
            self.shell.create_directory(tmp_folder)
            node = RestConnection(self.master).get_nodes_self()
            self.is_membase = False
            BackupHelper(self.master, self).backup('default', node, tmp_folder)
            self.verify_cluster_stats(self.servers[:self.nodes_init])
            self.test_union_all()
        finally:
            self.shell.delete_files(tmp_folder)

    def test_queries_after_backup_restore(self):
        method_name = self.input.param('to_run', 'test_any')
        self.couchbase_login_info = "%s:%s" % (self.input.membase_settings.rest_username,
                                               self.input.membase_settings.rest_password)
        self.backup_location = self.input.param("backup_location", "/tmp/backup")
        self.command_options = self.input.param("command_options", '')
        shell = RemoteMachineShellConnection(self.master)
        fn = getattr(self, method_name)
        fn()
        self.shell.execute_cluster_backup(self.couchbase_login_info, self.backup_location, self.command_options)
        fn = getattr(self, method_name)
        fn()
        for bucket in self.buckets:
            self.cluster.bucket_flush(self.master, bucket=bucket)
        # Adding sleep after flushing buckets (see CBQE-5838)
        self.sleep(210)

        shell.restore_backupFile(self.couchbase_login_info, self.backup_location,
                                 [bucket.name for bucket in self.buckets])
        fn = getattr(self, method_name)
        fn()

    def test_queries_after_backup_with_view(self):
        index_name = "Automation_backup_index"
        method_name = self.input.param('to_run', 'test_any')
        self.couchbase_login_info = "%s:%s" % (self.input.membase_settings.rest_username,
                                               self.input.membase_settings.rest_password)
        self.backup_location = self.input.param("backup_location", "/tmp/backup")
        self.command_options = self.input.param("command_options", '')
        index_field = self.input.param("index_field", '')
        self.assertTrue(index_field, "Index field should be provided")
        for query_bucket in self.query_buckets:
            self.run_cbq_query(query="CREATE INDEX %s ON %s(%s)" % (index_name, query_bucket,
                                                                    ','.join(index_field.split(';'))))
        try:
            shell = RemoteMachineShellConnection(self.master)
            fn = getattr(self, method_name)
            fn()
            self.shell.execute_cluster_backup(self.couchbase_login_info, self.backup_location, self.command_options)
            fn = getattr(self, method_name)
            fn()
            for bucket in self.buckets:
                self.cluster.bucket_flush(self.master, bucket=bucket)
            # Adding sleep after flushing buckets (see CBQE-5838)
            self.sleep(210)
            shell.restore_backupFile(self.couchbase_login_info, self.backup_location,
                                     [bucket.name for bucket in self.buckets])
            fn = getattr(self, method_name)
            fn()
        finally:
            for query_bucket in self.query_buckets:
                self.run_cbq_query(query="DROP INDEX %s ON %s" % (index_name, query_bucket))

    def test_queries_after_backup_with_gsi(self):
        index_name = "Automation_backup_index"
        method_name = self.input.param('to_run', 'test_any')
        self.couchbase_login_info = "%s:%s" % (self.input.membase_settings.rest_username,
                                               self.input.membase_settings.rest_password)
        self.backup_location = self.input.param("backup_location", "/tmp/backup")
        self.command_options = self.input.param("command_options", '')
        index_field = self.input.param("index_field", '')
        self.assertTrue(index_field, "Index field should be provided")
        for query_bucket in self.query_buckets:
            query = "CREATE INDEX %s ON %s(%s) USING GSI" % (index_name, query_bucket, ','.join(index_field.split(';')))
            # if self.gsi_type:
            #     query += "  WITH {'index_type': 'memdb'}"
            self.run_cbq_query(query=query)
        try:
            shell = RemoteMachineShellConnection(self.master)
            fn = getattr(self, method_name)
            fn()
            self.shell.execute_cluster_backup(self.couchbase_login_info, self.backup_location, self.command_options)
            fn = getattr(self, method_name)
            fn()
            for bucket in self.buckets:
                self.cluster.bucket_flush(self.master, bucket=bucket)
            # Adding sleep after flushing buckets (see CBQE-5838)
            self.sleep(210)
            shell.restore_backupFile(self.couchbase_login_info, self.backup_location,
                                     [bucket.name for bucket in self.buckets])
            fn = getattr(self, method_name)
            fn()
        finally:
            for query_bucket in self.query_buckets:
                self.run_cbq_query(query="DROP INDEX %s ON %s" % (index_name, query_bucket))

    def test_audit_add_node(self):
        event_id = 8196  # add node
        server = self.master
        if self.input.tuq_client and "client" in self.input.tuq_client:
            server = self.input.tuq_client["client"]
        index_field = self.input.param("index_field", 'job_title')
        indexes = []
        try:
            audit_reb_in = audit(eventID=event_id, host=server)
            indexes = self._create_multiple_indexes(index_field)
            servers_in = self.servers[1:self.nodes_in]
            rebalance = self.cluster.async_rebalance(self.servers[:1],
                                                     servers_in, [], services=self.services_in)
            expected_result = {"services": self.services_in, 'port': 8091, 'hostname': servers_in[0].ip,
                               'groupUUID': "0",
                               'node': 'ns_1@' + servers_in[0].ip, 'source': 'ns_server',
                               'user': self.master.rest_username,
                               "ip": self.getLocalIPAddress(), "port": 57457}
            self.test_min()
            audit_reb_in.checkConfig(expected_result)
            rebalance.result()
        finally:
            for query_bucket in self.query_buckets:
                for index_name in set(indexes):
                    self.run_cbq_query(query="DROP INDEX %s ON %s" % (index_name, query_bucket))

    def test_audit_rm_node(self):
        eventID = 8197  # add node
        server = self.master
        if self.input.tuq_client and "client" in self.input.tuq_client:
            server = self.input.tuq_client["client"]
        index_field = self.input.param("index_field", 'job_title')
        indexes = []
        try:
            audit_reb_out = audit(eventID=eventID, host=server)
            indexes = self._create_multiple_indexes(index_field)
            servers_in = self.servers[1:self.nodes_in]
            self.cluster.rebalance(self.servers[:1], servers_in, [], services=self.services_in)
            rebalance = self.cluster.rebalance(self.servers[:1], [], servers_in)
            expected_result = {"services": self.services_in, 'port': 8091, 'hostname': servers_in[0].ip,
                               'groupUUID': "0",
                               'node': 'ns_1@' + servers_in[0].ip, 'source': 'ns_server',
                               'user': self.master.rest_username,
                               "ip": self.getLocalIPAddress(), "port": 57457}
            self.test_min()
            audit_reb_out.checkConfig(expected_result)
            rebalance.result()
        finally:
            for query_bucket in self.query_buckets:
                for index_name in set(indexes):
                    self.run_cbq_query(query="DROP INDEX %s ON %s" % (index_name, query_bucket))

    def test_rebalance_in_stop_node(self):
        self.assertTrue(len(self.servers) >= self.nodes_in + 1, "Servers are not enough")
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 self.servers[1:(self.nodes_init + 1)], self.servers[:1],
                                                 services=['kv,index'])
        rebalance.result()
        rebalance = self.cluster.async_rebalance(self.servers[1:self.nodes_init + 1],
                                                 self.servers[:1], [], services=['n1ql'])
        rebalance.result()
        try:
            thread1 = threading.Thread(name='run_query', target=self.run_queries_until_timeout)
            thread1.start()
            if self.stop_source:
                remote = RemoteMachineShellConnection(self.servers[0])
                remote.stop_server()
            self.sleep(60)
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1],
                                                     self.servers[2:(self.nodes_init + 2)], [], services=['n1ql'])
            rebalance.result()
            if self.stop_server:
                self.log.info("Stopping server %s" % self.servers[2])
                remote = RemoteMachineShellConnection(self.servers[2])
                remote.stop_server()
                self.sleep(60)
            elif self.network_partition:
                self.start_firewall_on_node(self.servers[2])
                self.sleep(10)
            else:
                self.sleep(60)
                self.cluster.failover(self.servers[:self.nodes_init + 2], self.servers[2:self.nodes_init + 2])
            thread1.join()
            self.assertFalse(self.fail_status, "Queries did not recover")
        except Exception as e:
            self.assertFalse(True, str(e))
        finally:
            if self.stop_source or self.stop_server:
                remote.start_server()
            elif self.network_partition:
                self.stop_firewall_on_node(self.servers[self.nodes_init])
                self.sleep(300)
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1],
                                                     [], [self.servers[0]])
            rebalance.result()
            self.sleep(60)
            rebalance = self.cluster.async_rebalance(self.servers[1:self.nodes_init + 1],
                                                     [self.servers[0]], [], services=['kv,n1ql,index'])
            rebalance.result()

    def test_rebalance_in_failure(self):
        self.assertTrue(len(self.servers) >= self.nodes_in + 1, "Servers are not enough")
        try:
            thread1 = threading.Thread(name='run_query', target=self.run_queries_until_timeout)
            thread1.start()
            self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                         self.servers[1:(self.nodes_init + 1)], [], services=['n1ql'])
            time.sleep(1)
            self.start_firewall_on_node(self.servers[(self.nodes_init)])
            self.sleep(120)
            thread1.join()
            self.assertFalse(self.fail_status, "Queries did not recover")
        except Exception as e:
            self.assertFalse(True, str(e))
        finally:
            self.stop_firewall_on_node(self.servers[(self.nodes_init)])
            self.sleep(300)

    def test_rebalance_failure_retry(self):
        self.assertTrue(len(self.servers) >= self.nodes_in + 1, "Servers are not enough")
        body = {"enabled": "true", "afterTimePeriod": self.retry_time, "maxAttempts": self.num_retries}
        rest = RestConnection(self.master)
        rest.set_retry_rebalance_settings(body)
        result = rest.get_retry_rebalance_settings()
        if self.rebalance_out:
            rebalance_server = self.servers[(self.nodes_init - 1)]
        else:
            rebalance_server = self.servers[(self.nodes_init)]
        self.log.info("Retry Rebalance settings changed to : {0}"
                      .format(json.loads(result)))
        try:
            if self.rebalance_out:
                rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                         [], [self.servers[self.nodes_init - 1]])
            else:
                rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                         self.servers[1:(self.nodes_init + 1)], [], services=['n1ql'])
            time.sleep(1)
            self.start_firewall_on_node(rebalance_server)
            rebalance.result()
            self.fail("Rebalance did not fail")
        except Exception as e:
            self.log.error(str(e))
            if self.num_retries > 1:
                time.sleep(self.retry_time + 30)
            self.stop_firewall_on_node(rebalance_server)
            time.sleep(10)
            self.check_retry_rebalance_succeeded()
        finally:
            self.stop_firewall_on_node(rebalance_server)
            body = {"enabled": "false"}
            rest.set_retry_rebalance_settings(body)
            self.sleep(60)

    def test_rebalance_out_query_node(self):
        self.assertTrue(len(self.servers) >= self.nodes_in + 1, "Servers are not enough")
        try:
            thread1 = threading.Thread(name='run_query', target=self.run_queries_until_timeout)
            thread1.start()
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                     [], [self.servers[self.nodes_init - 1]])
            rebalance.result()
            if self.stop_server:
                self.shell.execute_command("killall -9 cbq-engine")
            thread1.join()
            self.assertFalse(self.fail_status, "Queries failed")
        except Exception as e:
            self.assertFalse(True, str(e))

    def test_swap_rebalance_nodes(self):
        self.assertTrue(len(self.servers) >= self.nodes_in + 1, "Servers are not enough")
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 self.servers[self.nodes_init:self.nodes_init + 1],
                                                 [], services=["kv"])
        rebalance.result()
        rebalance = self.cluster.async_rebalance(self.servers[:(self.nodes_init + 1)],
                                                 self.servers[self.nodes_init + 1:self.nodes_init + 2],
                                                 self.servers[:1], services=["index"])
        rebalance.result()
        rebalance = self.cluster.async_rebalance(self.servers[1:(self.nodes_init + 3)],
                                                 self.servers[self.nodes_init + 2:self.nodes_init + 3],
                                                 [], services=["n1ql"])
        rebalance.result()
        try:
            thread1 = threading.Thread(name='run_query', target=self.run_queries_until_timeout)
            thread1.start()
            if self.rebalance_index:
                rebalance = self.cluster.async_rebalance(self.servers[1:self.nodes_init + 3],
                                                         self.servers[:1],
                                                         self.servers[self.nodes_init + 1:self.nodes_init + 2],
                                                         services=["kv,n1ql,index"])
            elif self.rebalance_n1ql:
                rebalance = self.cluster.async_rebalance(self.servers[1:self.nodes_init + 3],
                                                         self.servers[:1],
                                                         self.servers[self.nodes_init + 2:self.nodes_init + 3],
                                                         services=["kv,n1ql,index"])
            else:
                rebalance = self.cluster.async_rebalance(self.servers[1:self.nodes_init + 3],
                                                         self.servers[:1],
                                                         self.servers[self.nodes_init:self.nodes_init + 1],
                                                         services=["kv,n1ql,index"])
            rebalance.result()
            thread1.join()
            self.assertFalse(self.fail_status, "Queries failed")
        except Exception as e:
            self.assertFalse(True, str(e))

    def test_swap_rebalance_kv_n1ql_index(self):
        self.assertTrue(len(self.servers) >= self.nodes_in + 1, "Servers are not enough")
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 self.servers[self.nodes_init:self.nodes_init + 1],
                                                 [], services=["kv,n1ql,index"])
        rebalance.result()
        try:
            thread1 = threading.Thread(name='run_query', target=self.run_queries_until_timeout)
            thread1.start()
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1],
                                                     self.servers[self.nodes_init + 1:self.nodes_init + 2],
                                                     self.servers[self.nodes_init:self.nodes_init + 1],
                                                     services=["kv,index,n1ql"])
            rebalance.result()
            thread1.join()
            self.assertFalse(self.fail_status, "Queries failed")
        except Exception as e:
            self.assertFalse(True, str(e))

    ###########################################################################################################
    def test_prepared_with_incr_rebalance_in(self):
        self.assertTrue(len(self.servers) >= self.nodes_in + 1, "Servers are not enough")
        try:
            for i in range(1, self.nodes_in + 1):
                rebalance = self.cluster.async_rebalance(self.servers[:i],
                                                         self.servers[i:i + 1], [], services=['n1ql'])
                rebalance.result()
                self.log.info("-" * 100)
                self.log.info("Querying alternate query node to test the encoded_prepare ....")
                self.test_prepared_between()
                self.log.info("-" * 100)
        finally:
            self.log.info("Done with encoded_prepare ....")

    def test_prepared_with_incr_rebalance_out(self):
        self.assertTrue(len(self.servers[:self.nodes_init]) > self.nodes_out,
                        "Servers are not enough")
        try:
            for i in range(1, self.nodes_out + 1):
                rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init - (i - 1)],
                                                         [],
                                                         self.servers[1])
                rebalance.result()
                self.log.info("-" * 100)
                self.log.info("Querying alternate query node to test the encoded_prepare ....")
                self.test_prepared_union()
                self.log.info("-" * 100)
        finally:
            self.log.info("Done with encoded_prepare ....")

    def test_prepared_with_swap_rebalance(self):
        self.assertTrue(len(self.servers) >= self.nodes_init + self.nodes_in,
                        "Servers are not enough")
        try:
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                     self.servers[self.nodes_init:self.nodes_init + self.nodes_in],
                                                     self.servers[self.nodes_init - self.nodes_out:self.nodes_init])
            rebalance.result()
            self.log.info("-" * 100)
            self.log.info("Querying alternate query node to test the encoded_prepare ....")
            self.test_prepared_union()
            self.log.info("-" * 100)
        finally:
            self.log.info("Done with encoded_prepare ....")

    def test_prepared_with_rebalance_with_server_crash(self):
        servr_in = self.servers[self.nodes_init:self.nodes_init + self.nodes_in]
        servr_out = self.servers[self.nodes_init - self.nodes_out:self.nodes_init]
        self.test_case()
        for i in range(3):
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                     servr_in, servr_out)
            self.sleep(5, "Wait some time for rebalance process and then kill memcached")
            remote = RemoteMachineShellConnection(self.servers[self.nodes_init - 1])
            remote.terminate_process(process_name='memcached')
            try:
                rebalance.result()
                # self.log.info("-"*100)
                # self.log.info("Querying alternate query node to test the encoded_prepare ....")
                # self.test_prepared_unnest()
                # self.log.info("-"*100)
            except:
                pass
        self.cluster.rebalance(self.servers[:self.nodes_init], servr_in, servr_out)
        self.log.info("-" * 100)
        self.log.info("Querying alternate query node to test the encoded_prepare ....")
        self.test_prepared_within_list_object()
        self.log.info("-" * 100)

    def test_prepared_with_failover(self):
        try:
            servr_out = self.servers[self.nodes_init - self.nodes_out:self.nodes_init]
            self.cluster.failover(self.servers[:self.nodes_init], servr_out)
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                     [], servr_out)
            rebalance.result()
            self.log.info("-" * 100)
            self.log.info("Querying alternate query node to test the encoded_prepare ....")
            self.test_prepared_within_list_object()
            self.log.info("-" * 100)
        finally:
            self.log.info("Done with encoded_prepare ....")

    def test_prepared_with_failover_add_back(self):
        try:
            servr_out = self.servers[self.nodes_init - self.nodes_out:self.nodes_init]

            nodes_all = RestConnection(self.master).node_statuses()
            nodes = []
            for failover_node in servr_out:
                nodes.extend([node for node in nodes_all
                              if node.ip == failover_node.ip and str(node.port) == failover_node.port])
            self.cluster.failover(self.servers[:self.nodes_init], servr_out)
            for node in nodes:
                RestConnection(self.master).add_back_node(node.id)
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [])
            rebalance.result()
            self.log.info("-" * 100)
            self.log.info("Querying alternate query node to test the encoded_prepare ....")
            self.test_prepared_within_list_object()
            self.log.info("-" * 100)
        finally:
            self.log.info("Done with encoded_prepare ....")

    def test_prepared_with_autofailover(self):
        autofailover_timeout = 30
        status = RestConnection(self.master).update_autofailover_settings(True, autofailover_timeout)
        self.assertTrue(status, 'failed to change autofailover_settings!')
        servr_out = self.servers[self.nodes_init - self.nodes_out:self.nodes_init]
        remote = RemoteMachineShellConnection(self.servers[self.nodes_init - 1])
        try:
            remote.stop_server()
            self.sleep(autofailover_timeout + 10, "Wait for autofailover")
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                     [], servr_out)
            rebalance.result()
            self.log.info("-" * 100)
            self.log.info("Querying alternate query node to test the encoded_prepare ....")
            self.test_prepared_union()
            self.log.info("-" * 100)
        finally:
            remote.start_server()
            self.log.info("Done with encoded_prepare ....")

    def test_prepared_with_warmup(self):
        try:
            num_srv_warm_up = self.input.param("srv_warm_up", 1)
            if self.input.tuq_client is None:
                self.fail("For this test external tuq server is requiered. " + "Please specify one in conf")
            self.test_union_all()
            for server in self.servers[self.nodes_init - num_srv_warm_up:self.nodes_init]:
                remote = RemoteMachineShellConnection(server)
                remote.stop_server()
                remote.start_server()
                remote.disconnect()
                # run query, result may not be as expected, but tuq shouldn't fail
            try:
                self.test_union_all()
            except:
                pass
            ClusterOperationHelper.wait_for_ns_servers_or_assert(self.servers, self, wait_if_warmup=True)
            self.verify_cluster_stats(self.servers[:self.nodes_init])
            self.sleep(50)
            self.verify_cluster_stats(self.servers[:self.nodes_init])
            self.log.info("-" * 100)
            self.log.info("Querying alternate query node to test the encoded_prepare ....")
            self.test_prepared_union()
            self.log.info("-" * 100)
        finally:
            self.log.info("Done with encoded_prepare ....")


class QueriesOpsJoinsTests(JoinTests):
    def setUp(self):
        super(QueriesOpsJoinsTests, self).setUp()
        if self.nodes_init > 1 and not self._testMethodName == 'suite_setUp':
            self.cluster.rebalance(self.servers[:1], self.servers[1:self.nodes_init], [])
        self.test_to_run = self.input.param("test_to_run", "test_join_several_keys")

    def suite_setUp(self):
        super(QueriesOpsJoinsTests, self).suite_setUp()

    def tearDown(self):
        rest = RestConnection(self.master)
        if rest._rebalance_progress_status() == 'running':
            self.log.warning("rebalancing is still running, test should be verified")
            stopped = rest.stop_rebalance()
            self.assertTrue(stopped, msg="unable to stop rebalance")
        try:
            super(QueriesOpsJoinsTests, self).tearDown()
        except:
            pass
        ClusterOperationHelper.cleanup_cluster(self.servers)
        self.sleep(10)

    def suite_tearDown(self):
        super(QueriesOpsJoinsTests, self).suite_tearDown()

    def test_incr_rebalance_in(self):
        self.assertTrue(len(self.servers) >= self.nodes_in + 1, "Servers are not enough")
        fn = getattr(self, self.test_to_run)
        fn()
        for i in range(1, self.nodes_in + 1):
            rebalance = self.cluster.async_rebalance(self.servers[:i],
                                                     self.servers[i:i + 1], [])
            fn()
            rebalance.result()
            fn()

    def test_run_queries_all_rebalance_long(self):
        timeout = self.input.param("wait_timeout", 900)
        self.assertTrue(len(self.servers) >= self.nodes_in + 1, "Servers are not enough")
        fn = getattr(self, self.test_to_run)
        fn()
        rebalance = self.cluster.async_rebalance(self.servers[:1], self.servers[1:self.nodes_in + 1], [])
        i = 0
        end_time = time.time() + timeout
        while rebalance.state != "FINISHED" or time.time() > end_time:
            i += 1
            self.log.info('ITERATION %s') % i
            fn()
        rebalance.result()

    def test_incr_rebalance_out(self):
        self.assertTrue(len(self.servers[:self.nodes_init]) > self.nodes_out,
                        "Servers are not enough")
        fn = getattr(self, self.test_to_run)
        fn()
        for i in range(1, self.nodes_out + 1):
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init - (i - 1)],
                                                     [],
                                                     self.servers[self.nodes_init - i:self.nodes_init - (i - 1)])
            fn()
            rebalance.result()
            fn()

    def test_swap_rebalance(self):
        self.assertTrue(len(self.servers) >= self.nodes_init + self.nodes_in,
                        "Servers are not enough")
        fn = getattr(self, self.test_to_run)
        fn()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 self.servers[self.nodes_init:self.nodes_init + self.nodes_in],
                                                 self.servers[self.nodes_init - self.nodes_out:self.nodes_init])
        fn()
        rebalance.result()
        fn()

    def test_failover(self):
        servr_out = self.servers[self.nodes_init - self.nodes_out:self.nodes_init]
        fn = getattr(self, self.test_to_run)
        fn()
        self.cluster.failover(self.servers[:self.nodes_init], servr_out)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], servr_out)
        fn()
        rebalance.result()
        fn()

    def test_failover_add_back(self):
        servr_out = self.servers[self.nodes_init - self.nodes_out:self.nodes_init]
        fn = getattr(self, self.test_to_run)
        fn()

        nodes_all = RestConnection(self.master).node_statuses()
        nodes = []
        for failover_node in servr_out:
            nodes.extend([node for node in nodes_all
                          if node.ip != failover_node.ip or str(node.port) != failover_node.port])
        self.cluster.failover(self.servers[:self.nodes_init], servr_out)
        for node in nodes:
            RestConnection(self.master).add_back_node(node.id)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [])
        fn()
        rebalance.result()
        fn()

    def test_warmup(self):
        num_srv_warm_up = self.input.param("srv_warm_up", self.nodes_init)
        if self.input.tuq_client is None:
            self.fail("For this test external tuq server is requiered. " + "Please specify one in conf")
        fn = getattr(self, self.test_to_run)
        fn()
        for server in self.servers[self.nodes_init - num_srv_warm_up:self.nodes_init]:
            remote = RemoteMachineShellConnection(server)
            remote.stop_server()
            remote.start_server()
            remote.disconnect()
        # run query, result may not be as expected, but tuq shouldn't fail
        try:
            fn()
        except:
            pass
        ClusterOperationHelper.wait_for_ns_servers_or_assert(self.servers, self)
        fn()
