from couchbase_helper.query_definitions import QueryDefinition
from .base_gsi import BaseSecondaryIndexingTests
from membase.api.on_prem_rest_client import RestHelper, RestConnection
from serverless.gsi_utils import GSIUtils
import random
import string
from remote.remote_util import RemoteMachineShellConnection


class Pause_Resume_GSI(BaseSecondaryIndexingTests):
    def setUp(self):
        super(Pause_Resume_GSI, self).setUp()
        self.log.info("==============  CollectionsIndexBasics setup has started ==============")
        self.rest.delete_all_buckets()
        self.query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)[0]
        self._create_server_groups()
        self.create_S3_config()
        self.run_hibernation_first = self.input.param('run_hibernation_first', False)
        self.gsi_util_obj = GSIUtils(self.run_cbq_query)
        self.log.info("==============  CollectionsIndexBasics setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  CollectionsIndexBasics tearDown has started ==============")
        super(Pause_Resume_GSI, self).tearDown()
        self.log.info("==============  CollectionsIndexBasics tearDown has completed ==============")

    def get_sub_cluster_list(self):
        idx_host_list_dict = self.get_host_list()
        sub_cluster_list = list()
        for bucket in idx_host_list_dict:
            host_list = sorted(list(idx_host_list_dict[bucket]))
            if host_list not in sub_cluster_list:
                sub_cluster_list.append(host_list)
        return sub_cluster_list

    def get_host_list(self):
        indexer_metadata = self.index_rest.get_indexer_metadata()['status']
        idx_host_list_dict = {str(bucket): set() for bucket in self.buckets}

        for idx in indexer_metadata:
            idx_bucket = idx['bucket']
            idx_host = idx['hosts'][0]
            idx_host_list_dict[idx_bucket].add(idx_host)
        return idx_host_list_dict

    def get_server_group(self, node_ip):
        # Find Server group of removing nodes
        zone_info = self.rest.get_zone_and_nodes()
        for zone_name, node_list in zone_info.items():
            if node_ip in node_list:
                return zone_name
        return

    def verify_post_resume(self, indexer_meta_data_before_pause, indexer_meta_data_after_resume,
                           definition_list_before_pause, index_stats_before_pause, index_stats_after_resume):

        self.assertEqual(len(indexer_meta_data_before_pause), len(indexer_meta_data_after_resume),
                         'No of indexes before pause and after resume do not match')
        index_namespace_keys = list(index_stats_before_pause)[1:]
        for key in index_namespace_keys:
            self.assertEqual(index_stats_before_pause[key]['items_count'],
                             index_stats_after_resume[key]['items_count'],
                             'No of indexes before pause and after resume do not match')
        select_queries = self.gsi_util_obj.get_select_queries(definition_list=definition_list_before_pause,
                                                              namespace=self.namespaces[0])

        drop_queries = self.gsi_util_obj.get_drop_index_list(definition_list=definition_list_before_pause,namespace=self.namespaces[0])
        for query in select_queries:
            self.run_cbq_query(query=query)
            self.sleep(5)

        for query in drop_queries:
            self.run_cbq_query(query=query)
            self.sleep(5)

        # Creating new indexes,running select queries and dropping them post resume
        definition_list = []
        for namespace in self.namespaces:
            prefix = f'idx_{"".join(random.choices(string.ascii_uppercase + string.digits, k=10))}' \
                     f'_batch_1_'
            definition_list = self.gsi_util_obj.get_index_definition_list(dataset='Person', prefix=prefix)
            create_list = self.gsi_util_obj.get_create_index_list(definition_list=definition_list, namespace=namespace,
                                                                  defer_build_mix=False)
            self.log.info(f"Create index list: {create_list}")
            self.gsi_util_obj.create_gsi_indexes(create_queries=create_list, query_node=self.query_node)
        select_queries = self.gsi_util_obj.get_select_queries(definition_list=definition_list,
                                                                               namespace=self.namespaces[0])

        drop_queries = self.gsi_util_obj.get_drop_index_list(definition_list=definition_list_before_pause,namespace=self.namespaces[0])
        for query in select_queries:
            self.run_cbq_query(query=query)
            self.sleep(5)

        for query in drop_queries:
            self.run_cbq_query(query=query)
            self.sleep(5)

    def test_basic_pause_resume(self):
        self.prepare_tenants(random_bucket_name='hibernation', index_creations=False)
        definition_list = []
        for namespace in self.namespaces:
            prefix = f'idx_{"".join(random.choices(string.ascii_uppercase + string.digits, k=10))}' \
                     f'_batch_1_'
            definition_list = self.gsi_util_obj.get_index_definition_list(dataset='Employee', prefix=prefix)
            create_list = self.gsi_util_obj.get_create_index_list(definition_list=definition_list, namespace=namespace,
                                                                  defer_build_mix=False)
            self.log.info(f"Create index list: {create_list}")
            self.gsi_util_obj.create_gsi_indexes(create_queries=create_list, query_node=self.query_node)
        self.sleep(410, 'Waiting to clear ddl tokens')
        self.buckets = self.rest.get_buckets()
        self.s3_utils_obj.delete_s3_folder(folder=self.buckets[0].name)
        indexder_metadata_before_pause = self.index_rest.get_indexer_metadata()['status']
        indexder_stats_before_pause = self.index_rest.get_index_official_stats()
        self.log.info(indexder_metadata_before_pause)

        self.log.info('Pausing bucket')
        self.rest.pause_operation(bucket_name=self.buckets[0].name, blob_region=self.region, s3_bucket=self.s3_bucket)
        self.sleep(15, 'Sleep post pause')

        self.log.info('Resuming bucket')
        self.rest.resume_operation(bucket_name=self.buckets[0].name, blob_region=self.region, s3_bucket=self.s3_bucket)
        self.sleep(15, 'Sleep post resume')
        indexder_metadata_after_resume = self.index_rest.get_indexer_metadata()['status']
        indexder_stats_after_resume = self.index_rest.get_index_official_stats()
        self.log.info(indexder_metadata_after_resume)
        self.verify_post_resume(indexer_meta_data_before_pause=indexder_metadata_before_pause,
                                indexer_meta_data_after_resume=indexder_metadata_after_resume,
                                definition_list_before_pause=definition_list,
                                index_stats_before_pause=indexder_stats_before_pause,
                                index_stats_after_resume=indexder_stats_after_resume)
        self.s3_utils_obj.delete_s3_folder(folder=self.bucket_name)

    def test_stop_pause(self):
        self.prepare_tenants(random_bucket_name='hibernation', index_creations=False)
        definition_list = []
        for namespace in self.namespaces:
            prefix = f'idx_{"".join(random.choices(string.ascii_uppercase + string.digits, k=10))}' \
                     f'_batch_1_'
            definition_list = self.gsi_util_obj.get_index_definition_list(dataset='Employee', prefix=prefix)
            create_list = self.gsi_util_obj.get_create_index_list(definition_list=definition_list, namespace=namespace,
                                                                  defer_build_mix=False)
            self.log.info(f"Create index list: {create_list}")
            self.gsi_util_obj.create_gsi_indexes(create_queries=create_list, query_node=self.query_node)
        self.sleep(410, 'Waiting to clear ddl tokens')
        self.buckets = self.rest.get_buckets()
        self.s3_utils_obj.delete_s3_folder(folder=self.buckets[0].name)
        indexder_metadata_before_pause = self.index_rest.get_indexer_metadata()['status']
        indexder_stats_before_pause = self.index_rest.get_index_official_stats()
        self.log.info(indexder_metadata_before_pause)
        self.log.info('Pausing bucket')
        self.rest.pause_operation(bucket_name=self.buckets[0].name, blob_region=self.region, s3_bucket=self.s3_bucket,
                                  pause_complete=False)
        self.log.info(f"Stopping pause on bucket {self.buckets[0].name}")
        self.rest.stop_pause(bucket=self.buckets[0].name)
        self.sleep(10)
        self.rest.wait_bucket_hibernation(task='pause_bucket', operation='stopped')
        self.sleep(10)
        indexder_metadata_after_resume = self.index_rest.get_indexer_metadata()['status']
        indexder_stats_after_resume = self.index_rest.get_index_official_stats()
        self.log.info(indexder_metadata_after_resume)
        self.verify_post_resume(indexer_meta_data_before_pause=indexder_metadata_before_pause,
                                indexer_meta_data_after_resume=indexder_metadata_after_resume,
                                definition_list_before_pause=definition_list,
                                index_stats_before_pause=indexder_stats_before_pause,
                                index_stats_after_resume=indexder_stats_after_resume)
        self.s3_utils_obj.delete_s3_folder(folder=self.bucket_name)

    def test_stop_resume(self):
        self.prepare_tenants(random_bucket_name='hibernation')
        self.sleep(410, 'Waiting to clear ddl tokens')
        self.buckets = self.rest.get_buckets()
        self.s3_utils_obj.delete_s3_folder(folder=self.buckets[0].name)
        self.log.info('Pausing bucket')
        self.rest.pause_operation(bucket_name=self.buckets[0].name, blob_region=self.region, s3_bucket=self.s3_bucket)

        self.log.info('Resuming bucket')
        self.rest.resume_operation(bucket_name=self.buckets[0].name, blob_region=self.region, s3_bucket=self.s3_bucket,
                                   resume_complete=False)

        self.rest.stop_resume(bucket=self.buckets[0].name)
        self.sleep(10)
        self.rest.wait_bucket_hibernation(task='resume_bucket', operation='stopped')
        self.sleep(10)
        self.assertEqual([], self.rest.get_buckets(),
                         f"Indexer meta data before pause and after resume not the same.")

    def test_pause_with_nodes_failover(self):
        self.prepare_tenants(random_bucket_name='hibernation')
        self.sleep(410, 'Waiting to clear ddl tokens')
        self.buckets = self.rest.get_buckets()
        self.s3_utils_obj.delete_s3_folder(folder=self.buckets[0].name)
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        error_msg = None
        if self.run_hibernation_first:
            self.log.info('Pausing bucket')
            self.rest.pause_operation(bucket_name=self.buckets[0].name, blob_region=self.region,
                                      s3_bucket=self.s3_bucket,
                                      pause_complete=False)

            self.sleep(5, 'Time for pause to start')
        try:
            failover_task = self.cluster.async_failover([self.master], failover_nodes=[index_server], graceful=True)
            failover_task.result()
        except Exception as exception:
            self.log.info(error_msg)
            error_msg = str(exception)

        if self.run_hibernation_first:
            if error_msg:
                expected_error = "Failover Node failed :b'Unexpected server error: in_bucket_hibernation' "
                self.assertEqual(error_msg, expected_error, f"Incorrect exception: {error_msg}")
                if not self.rest.wait_bucket_hibernation(task='pause_bucket', operation='completed'):
                    self.fail('Pause failed post failure of failover')
            else:
                self.fail("Node failover did not fail as expected")

        else:
            if error_msg:
                self.fail(error_msg)

            try:
                self.log.info('Pausing bucket')
                self.rest.pause_operation(bucket_name=self.buckets[0].name, blob_region=self.region,
                                          s3_bucket=self.s3_bucket, pause_complete=False)
                self.sleep(5, 'Time for pause to start')

            except Exception as exception:
                error_msg = str(exception)

            if error_msg:
                self.assertEqual(error_msg, 'b\'{"error":"rebalance_running"}\'',
                                 f"Did not get expected error for pause, Expected: rebalance_running, Actual: {error_msg}")
            else:
                self.fail('Pause did not fail as expected')

    def test_resume_with_nodes_failover(self):
        self.prepare_tenants(random_bucket_name='hibernation')
        self.sleep(410, 'Waiting to clear ddl tokens')
        self.buckets = self.rest.get_buckets()
        self.s3_utils_obj.delete_s3_folder(folder=self.buckets[0].name)
        index_server = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        error_msg = None
        self.log.info('Pausing bucket')
        self.rest.pause_operation(bucket_name=self.buckets[0].name, blob_region=self.region, s3_bucket=self.s3_bucket)

        if self.run_hibernation_first:
            self.log.info('Resuming bucket')
            self.rest.resume_operation(bucket_name=self.buckets[0].name, blob_region=self.region,
                                       s3_bucket=self.s3_bucket,
                                       resume_complete=False)
            self.sleep(5, 'Time for resume to start')
        try:
            failover_task = self.cluster.async_failover([self.master], failover_nodes=[index_server], graceful=True)
            failover_task.result()
        except Exception as exception:
            error_msg = str(exception)

        if self.run_hibernation_first:
            if error_msg:
                expected_error = "Unexpected server error: in_bucket_hibernation"
                error_result = (expected_error in error_msg)
                self.assertTrue(error_result, f"Unexpected exception: {error_msg}")
                if not self.rest.wait_bucket_hibernation(task='resume_bucket', operation='completed'):
                    self.fail('Resume failed post failure of failover')
            else:
                self.fail("Node failover did not fail as expected")

        else:
            if error_msg:
                self.fail(error_msg)

            try:
                self.log.info('Resuming bucket')
                self.rest.resume_operation(bucket_name=self.buckets[0].name, blob_region=self.region,
                                           s3_bucket=self.s3_bucket, resume_complete=False)
                self.sleep(5, 'Time for resume to start')

            except Exception as exception:
                error_msg = str(exception)

            if error_msg:
                self.assertEqual(error_msg, 'b\'{"error":"rebalance_running"}\'', f"Did not get expected error \
                                                for pause, Expected: requires_rebalance, Acutal: {error_msg}")
            else:
                self.fail('Resume did not fail as expected')

    def test_rebalance_pause(self):
        self.prepare_tenants(random_bucket_name='hibernation')
        self.sleep(410, 'Waiting to clear ddl tokens')
        self.s3_utils_obj.delete_s3_folder(folder=self.buckets[0].name)
        sub_clusters = self.get_sub_cluster_list()
        remove_node_ips = [sub_cluster[0].split(':')[0] for sub_cluster in sub_clusters]
        node_zone_dict = {}

        remove_nodes = []
        for remove_node_ip in remove_node_ips:
            for server in self.servers:
                if remove_node_ip == server.ip:
                    remove_nodes.append(server)
                    break

        for node in remove_node_ips:
            node_zone_dict[node] = self.get_server_group(node)

        for counter, server in enumerate(node_zone_dict):
            self.rest.add_node(user=self.rest.username, password=self.rest.password,
                               remoteIp=self.servers[self.nodes_init + counter].ip,
                               zone_name=node_zone_dict[server],
                               services=['index', 'kv', 'n1ql'])
        if self.run_hibernation_first:
            self.log.info('Pausing bucket')
            self.rest.pause_operation(bucket_name=self.buckets[0].name, blob_region=self.region,
                                      s3_bucket=self.s3_bucket,
                                      pause_complete=False)
            self.sleep(5, "Wait for pause operation to start")

        try:
            rebalance_task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init],
                                                          to_add=[],
                                                          to_remove=remove_nodes,
                                                          services=['index'] * len(remove_nodes))
            self.sleep(5, "Rebalance started")
        except Exception as exception:
            expected_error = 'Cannot rebalance when another bucket is pausing/resuming.'
            self.assertIn(expected_error, exception, 'Rebalance should fail')

        # if self.run_hibernation_first:
        #     rebalance_status = RestHelper(self.rest).rebalance_reached()
        #     self.assertFalse(rebalance_status, "rebalance failed, stuck or did not complete")

        if not self.run_hibernation_first:
            try:
                self.log.info('Pausing bucket')
                self.rest.pause_operation(bucket_name=self.buckets[0].name, blob_region=self.region,
                                          s3_bucket=self.s3_bucket, pause_complete=False)
                self.sleep(5, "Wait for pause operation to start")
            except Exception as exception:
                expected_error_msg = str(exception)
                self.assertIn("rebalance_running", expected_error_msg, 'Pause did not fail as expected')

    def test_rebalance_resume(self):
        self.prepare_tenants(random_bucket_name='hibernation')
        self.sleep(410, 'Waiting to clear ddl tokens')
        self.s3_utils_obj.delete_s3_folder(folder=self.buckets[0].name)
        sub_clusters = self.get_sub_cluster_list()
        remove_node_ips = [sub_cluster[0].split(':')[0] for sub_cluster in sub_clusters]
        node_zone_dict = {}

        remove_nodes = []
        for remove_node_ip in remove_node_ips:
            for server in self.servers:
                if remove_node_ip == server.ip:
                    remove_nodes.append(server)
                    break

        for node in remove_node_ips:
            node_zone_dict[node] = self.get_server_group(node)

        for counter, server in enumerate(node_zone_dict):
            self.rest.add_node(user=self.rest.username, password=self.rest.password,
                               remoteIp=self.servers[self.nodes_init + counter].ip,
                               zone_name=node_zone_dict[server],
                               services=['index', 'kv', 'n1ql'])

        self.log.info('Pausing bucket')
        self.rest.pause_operation(bucket_name=self.buckets[0].name, blob_region=self.region, s3_bucket=self.s3_bucket,
                                  pause_complete=True)
        self.sleep(5, "Wait for pause operation to complete")

        if self.run_hibernation_first:
            self.log.info('Resuming bucket')
            self.rest.resume_operation(bucket_name=self.buckets[0].name, blob_region=self.region,
                                       s3_bucket=self.s3_bucket,
                                       resume_complete=False)
            self.sleep(5, "Wait for resume operation to start")

        try:
            rebalance_task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init],
                                                          to_add=[],
                                                          to_remove=remove_nodes,
                                                          services=['index'] * len(remove_nodes))
            self.sleep(5, "Rebalance started")
        except Exception as exception:
            expected_error = 'Cannot rebalance when another bucket is pausing/resuming.'
            self.assertIn(expected_error, exception, 'Rebalance should fail')

        if not self.run_hibernation_first:
            try:
                self.log.info('Resuming bucket')
                self.rest.resume_operation(bucket_name=self.buckets[0].name, blob_region=self.region,
                                           s3_bucket=self.s3_bucket, resume_complete=False)
                self.sleep(5, "Wait for resume operation to start")
            except Exception as exception:
                expected_error_msg = str(exception)
                self.assertIn("rebalance_running", expected_error_msg, 'Resume did not fail as expected')

    def test_concurrent_pause(self):
        self.prepare_tenants(random_bucket_name='hibernation')
        self.buckets = self.rest.get_buckets()
        for buckets in self.buckets:
            self.s3_utils_obj.delete_s3_folder(folder=buckets.name)
        self.sleep(310, 'Waiting to clear ddl tokens')
        indexder_metadata_before_pause = self.index_rest.get_indexer_metadata()['status']
        indexder_stats_before_pause = self.index_rest.get_index_official_stats()
        self.log.info(indexder_metadata_before_pause)

        self.log.info('Pausing bucket')
        try:
            for bucket in self.buckets:
                _, content = self.rest.pause_operation(bucket_name=bucket.name, blob_region=self.region,
                                          s3_bucket=self.s3_bucket, pause_complete=False)
        except Exception as e:
            self.log.info(str(e))
            self.assertIn('Cannot pause/resume bucket, while another bucket is being paused/resumed', str(e), 'Error msg incorrect')
            self.assertNotEqual(self.buckets[0].name, self.rest.get_buckets()[0].name,
                             f'Bucket {self.buckets[0].name} has not paused sucessfully')
            self.assertNotEqual(len(self.buckets), len(self.rest.get_buckets()), 'Pause failed even for the first bucket')
            indexder_metadata_post_concurrent_pause_attempt = self.index_rest.get_indexer_metadata()['status']
            self.assertEqual(len(indexder_metadata_post_concurrent_pause_attempt), 28)

        self.sleep(15, 'Sleep post pause')

    def test_concurrent_resume(self):
        self.prepare_tenants(random_bucket_name='hibernation')
        self.buckets = self.rest.get_buckets()
        for buckets in self.buckets:
            self.s3_utils_obj.delete_s3_folder(folder=buckets.name)
        self.sleep(310, 'Waiting to clear ddl tokens')
        indexder_metadata_before_pause = self.index_rest.get_indexer_metadata()['status']
        indexder_stats_before_pause = self.index_rest.get_index_official_stats()
        self.log.info(indexder_metadata_before_pause)

        self.log.info('Pausing bucket')
        for bucket in self.buckets:
            self.rest.pause_operation(bucket_name=bucket.name, blob_region=self.region,
                                                   s3_bucket=self.s3_bucket)
        self.sleep(30, 'Sleep post sucessful pause of all the buckets')
        try:
            for bucket in self.buckets:
                self.rest.resume_operation(bucket_name=bucket.name, blob_region=self.region, s3_bucket=self.s3_bucket)

        except Exception as e:
            self.log.info(str(e))
            self.assertIn('Cannot pause/resume bucket, while another bucket is being paused/resumed', str(e), 'Error msg incorrect')
            self.assertEqual(self.buckets[0].name, self.rest.get_buckets()[0].name, f'Bucket {self.buckets[0].name} has not resumed sucessfully')
            self.assertNotEqual(len(self.buckets), len(self.rest.get_buckets()), 'Pause failed even for the first bucket')
            indexder_metadata_post_concurrent_resume_attempt = self.index_rest.get_indexer_metadata()['status']
            self.assertEqual(len(indexder_metadata_post_concurrent_resume_attempt), 14)

    def test_kill_indexing_service_during_pause(self):
        self.prepare_tenants(random_bucket_name='hibernation')
        self.buckets = self.rest.get_buckets()
        self.s3_utils_obj.delete_s3_folder(folder=self.buckets[0].name)
        self.sleep(310, 'Waiting to clear ddl tokens')
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if self.run_hibernation_first:
            self.log.info('Pausing bucket')
            self.rest.pause_operation(bucket_name=self.buckets[0].name, blob_region=self.region,
                                      s3_bucket=self.s3_bucket,
                                      pause_complete=False)

            self.sleep(5, 'Time for pause to start')

        remote = RemoteMachineShellConnection(index_nodes[0])
        if not self.run_hibernation_first:
            remote.stop_indexer()

        else:
            remote.terminate_process(process_name="indexer")

        if self.run_hibernation_first:
            try:
                self.rest.wait_bucket_hibernation(task='pause_bucket', operation='completed')
            except Exception as e:
                error_msg = str(e)
                self.log.error(str(e))
                self.assertEqual('Operation completed failed', error_msg, 'Failure did not come')
            else:
                self.fail('Pause operation did not fail as expected')
        if not self.run_hibernation_first:
            try:
                self.log.info('Pausing bucket')
                self.rest.pause_operation(bucket_name=self.buckets[0].name, blob_region=self.region,
                                          s3_bucket=self.s3_bucket,
                                          pause_complete=True)

                #self.sleep(5, 'Time for pause to start')
            except Exception as e:
                error_msg = str(e)
                self.assertEqual(error_msg,'Operation completed failed','Operation did not fail as expected')
            else:
                self.fail('Pause operation did not fail as expected')


    def test_kill_indexing_service_during_resume(self):
        self.prepare_tenants(random_bucket_name='hibernation')
        self.buckets = self.rest.get_buckets()
        self.s3_utils_obj.delete_s3_folder(folder=self.buckets[0].name)
        self.sleep(310, 'Waiting to clear ddl tokens')
        self.log.info('Starting the pause operation')
        self.rest.pause_operation(bucket_name=self.buckets[0].name, blob_region=self.region,
                                  s3_bucket=self.s3_bucket,
                                  pause_complete=True)
        self.sleep(30, 'Sleeping post sucessful pause')
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if self.run_hibernation_first:
            self.log.info('Resuming bucket')
            self.rest.resume_operation(bucket_name=self.buckets[0].name, blob_region=self.region,
                                      s3_bucket=self.s3_bucket,
                                      pause_complete=False)

            self.sleep(5, 'Time for pause to start')

        remote = RemoteMachineShellConnection(index_nodes[0])
        if not self.run_hibernation_first:
            remote.stop_indexer()

        else:
            remote.terminate_process(process_name="indexer")

        if self.run_hibernation_first:
            try:
                self.rest.wait_bucket_hibernation(task='resume_bucket', operation='completed')
            except Exception as e:
                error_msg = str(e)
                self.log.error(str(e))
                self.assertEqual('Operation completed failed', error_msg, 'Failure did not come')
            else:
                self.fail('Pause operation did not fail as expected')
        if not self.run_hibernation_first:
            try:
                self.log.info('Pausing bucket')
                self.rest.resume_operation(bucket_name=self.buckets[0].name, blob_region=self.region,
                                          s3_bucket=self.s3_bucket,
                                          pause_complete=True)

                #self.sleep(5, 'Time for pause to start')
            except Exception as e:
                error_msg = str(e)
                self.assertEqual(error_msg, 'Operation completed failed','Operation did not fail as expected')
            else:
                self.fail('Resume operation did not fail as expected')

    def test_resume_post_clearing_s3_data(self):
        self.prepare_tenants(random_bucket_name='hibernation')
        self.buckets = self.rest.get_buckets()
        self.s3_utils_obj.delete_s3_folder(folder=self.buckets[0].name)
        self.sleep(310, 'Waiting to clear ddl tokens')
        self.log.info('Starting the pause operation')
        self.rest.pause_operation(bucket_name=self.buckets[0].name, blob_region=self.region,
                                  s3_bucket=self.s3_bucket,
                                  pause_complete=True)
        self.sleep(30, 'Sleeping post sucessful pause')
        self.s3_utils_obj.delete_s3_folder(folder=self.buckets[0].name)
        try:
            self.rest.resume_operation(bucket_name=self.buckets[0].name,blob_region=self.region,s3_bucket=self.s3_bucket)
        except Exception as e:
            self.log.info(str(e))
            self.assertIn('s3_get_failure', str(e), "S3 faliure did not occur as expected")

    def test_resume_during_ongoing_pause(self):
        self.prepare_tenants(random_bucket_name='hibernation')
        self.buckets = self.rest.get_buckets()
        self.s3_utils_obj.delete_s3_folder(folder=self.buckets[0].name)
        self.sleep(310, 'Waiting to clear ddl tokens')
        self.log.info('Starting the pause operation')
        self.rest.pause_operation(bucket_name=self.buckets[0].name, blob_region=self.region,
                                  s3_bucket=self.s3_bucket,
                                  pause_complete=False)
        self.sleep(5, 'Sleeping post triggering pause')
        try:
            self.rest.resume_operation(bucket_name=self.buckets[0].name, blob_region=self.region,
                                       s3_bucket=self.s3_bucket,
                                       resume_complete=True)
        except Exception as error_msg:
            self.log.info(str(error_msg))
            self.assertIn('Cannot pause/resume bucket, while another bucket is being paused/resumed', str(e),
                          'Error msg incorrect')
        else:
            self.fail('Resume did not fail as expected')


    def test_resume_during_ongoing_resume(self):
        self.prepare_tenants(random_bucket_name='hibernation')
        self.buckets = self.rest.get_buckets()
        self.s3_utils_obj.delete_s3_folder(folder=self.buckets[0].name)
        self.sleep(310, 'Waiting to clear ddl tokens')
        self.log.info('Starting the pause operation')
        self.rest.pause_operation(bucket_name=self.buckets[0].name, blob_region=self.region,
                                  s3_bucket=self.s3_bucket,
                                  pause_complete=True)
        self.sleep(5, 'Sleeping post triggering pause')
        self.log.info('Starting resume')
        self.rest.resume_operation(bucket_name=self.buckets[0].name, blob_region=self.region,
                                  s3_bucket=self.s3_bucket,
                                  resume_complete=False)
        try:
            self.rest.resume_operation(bucket_name=self.buckets[0].name, blob_region=self.region,
                                       s3_bucket=self.s3_bucket,
                                       resume_complete=True)
        except Exception as e:
            error_msg = str(e)
            self.log.info(str(error_msg))
            self.assertIn('Cannot pause/resume bucket, while another bucket is being paused/resumed', str(error_msg),
                          'Error msg incorrect')
            self.rest.wait_bucket_hibernation(task='resume_bucket', operation='completed')
        else:
            self.fail('Resume did not fail as expected')


    def test_abort_pause_and_pause(self):
        self.prepare_tenants(random_bucket_name='hibernation')
        self.buckets = self.rest.get_buckets()
        self.s3_utils_obj.delete_s3_folder(folder=self.buckets[0].name)
        self.sleep(310, 'Waiting to clear ddl tokens')
        self.log.info('Starting the pause operation')
        self.rest.pause_operation(bucket_name=self.buckets[0].name, blob_region=self.region,
                                  s3_bucket=self.s3_bucket,
                                  pause_complete=False)
        self.sleep(5, 'Sleeping post triggering pause')
        self.log.info('Stopping pause')
        self.rest.stop_pause(bucket=self.buckets[0].name)
        self.sleep(5, 'Sleeping post stopping pause')
        self.rest.pause_operation(bucket_name=self.buckets[0].name, blob_region=self.region,
                                  s3_bucket=self.s3_bucket,
                                  pause_complete=True)
