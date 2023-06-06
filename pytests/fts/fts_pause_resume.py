from .fts_base import FTSBaseTest
from TestInput import TestInputSingleton
from lib.membase.api.rest_client import RestConnection, RestHelper
import time
import pprint
import threading

class PauseResume(FTSBaseTest):

    def setUp(self):
        super(PauseResume, self).setUp()
        self._create_server_groups()
        self.create_S3_config()
        self.bucket_params = {}
        self.rest = RestConnection(self._cb_cluster.get_master_node())
        self.result_holder = {}

    def tearDown(self):
        super(PauseResume, self).tearDown()

    def _decode_containers(self, encoded_containers=[]):
        decoded_containers = {}
        if len(encoded_containers) > 0:
            decoded_containers["buckets"] = []
            for container in encoded_containers:
                container_path = container.split(".")
                if len(container_path) > 0:
                    bucket = container_path[0]
                    bucket_already_decoded = False
                    for b in decoded_containers["buckets"]:
                        if b["name"] == bucket:
                            bucket_already_decoded = True
                            break
                    if not bucket_already_decoded:
                        decoded_containers["buckets"].append({"name": bucket, "scopes": []})
                if len(container_path) > 1:
                    bucket = container_path[0]
                    scope = container_path[1]
                    scope_already_decoded = False
                    for b in decoded_containers["buckets"]:
                        if b["name"] == bucket:
                            for s in b["scopes"]:
                                if s["name"] == scope:
                                    scope_already_decoded = True
                                    break
                            if not scope_already_decoded:
                                b["scopes"].append({"name": scope, "collections": []})
                if len(container_path) > 2:
                    bucket = container_path[0]
                    scope = container_path[1]
                    collection = container_path[2]
                    collection_already_decoded = False
                    for b in decoded_containers["buckets"]:
                        if b["name"] == bucket:
                            for s in b["scopes"]:
                                if s["name"] == scope:
                                    for c in s["collections"]:
                                        if c["name"] == collection:
                                            collection_already_decoded = True
                                            break
                                    if not collection_already_decoded:
                                        s["collections"].append({"name": collection})
        return decoded_containers

    def _create_kv(self):
        containers = eval(TestInputSingleton.input.param("kv", "{}"))
        decoded_containers = self._decode_containers(containers)
        test_objects_created, error = self._cb_cluster.create_bucket_scope_collection_multi_structure(
            existing_buckets=None, bucket_params=self.bucket_params, data_structure=decoded_containers,
            cli_client=self.cli_client)
        time.sleep(10)

    def fts_indexes_functionality_status(self, indexes):
        """
            Returns True iff
                1. We are able to fetch index definition for given index
                2. We are able to run FTS query on given index
        """
        functionality_works = False
        if not isinstance(indexes, list):
            indexes = [indexes]

        for index in indexes:
            status, resp = index.get_index_defn()
            if status:
                functionality_works = True
            else:
                self.log.error(
                    f"Not able to fetch index definition for {index} \n Status : {status} \n Response {resp}")
            try:
                hits, _, _, _ = index.execute_query(self.sample_query,
                                                    zero_results_ok=False)
            except Exception as err:
                functionality_works = False
                self.log.error(f"FTS query functionality failure {err}")

        return functionality_works

    def print_ns_server_task(self):
        task = self.rest.ns_server_tasks()
        self.log.info("##### TASK LIST #####")
        pprint.PrettyPrinter(width=20).pprint(task)

    def verify_fts_functionality(self, paused_indexes=None, resumed_indexes=None, active_indexes=None):
        if paused_indexes is not None:
            if not isinstance(paused_indexes, list):
                paused_indexes = [paused_indexes]
            for index in paused_indexes:
                fts_functionality_status = self.fts_indexes_functionality_status(index)
                if fts_functionality_status:
                    self.fail(f"FTS functionality working even after pause for index {index.name}")
                else:
                    self.log.info(
                        f"FTS functionality for index {index.name} not working since it's source bucket was paused")

        if resumed_indexes is not None:
            if not isinstance(resumed_indexes, list):
                resumed_indexes = [resumed_indexes]
            for index in resumed_indexes:
                fts_functionality_status = self.fts_indexes_functionality_status(index)
                if not fts_functionality_status:
                    self.fail(f"FTS functionality not working for index {index.name} after resume")
                else:
                    self.log.info("FTS functionality working after resume operation")

        if active_indexes is not None:
            if not isinstance(active_indexes, list):
                active_indexes = [active_indexes]
            for index in active_indexes:
                fts_functionality_status = self.fts_indexes_functionality_status(index)
                if fts_functionality_status:
                    self.log.info("FTS functionality working for active indexes")
                else:
                    self.fail("FTS functionality not working for active indexes after pausing some bucket")

    def pause_concurrency_helper_function(self, thread_name):
        try:
            self.rest.pause_operation(bucket_name=thread_name, blob_region=self.region, s3_bucket=self.s3_bucket,
                                      pause_complete=False)
            # Store the result in the shared variable
            self.result_holder[thread_name] = "Success"
        except Exception as e:
            # Store the exception in the shared variable
            self.result_holder[thread_name] = e
            self.assertIn('Cannot pause/resume bucket, while another bucket is being paused/resumed', str(e), 'Error msg incorrect')

    def resume_concurrency_helper_function(self, thread_name):
        try:
            self.rest.resume_operation(bucket_name=thread_name, blob_region=self.region, s3_bucket=self.s3_bucket,
                                       resume_complete=False)
            # Store the result in the shared variable
            self.result_holder[thread_name] = "Success"
        except Exception as e:
            # Store the exception in the shared variable
            self.result_holder[thread_name] = e
            self.assertIn('Cannot pause/resume bucket, while another bucket is being paused/resumed', str(e), 'Error msg incorrect')


    def test_basic_pause_resume_fts(self):
        # create required buckets, collections and then load data
        self._create_kv()
        self.load_data()

        # Create Indexes
        index1 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket1'), "index1")
        index2 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket2'), "index2")

        self.wait_for_indexing_complete()
        self.buckets = self.rest.get_buckets()

        # pause a bucket
        self.s3_utils_obj.delete_s3_folder(folder=self.buckets[0].name)
        self.log.info('Pausing bucket')
        self.rest.pause_operation(bucket_name=self.buckets[0].name, blob_region=self.region, s3_bucket=self.s3_bucket)
        self.verify_fts_functionality(paused_indexes=[index1], active_indexes=[index2])

        self.sleep(10, "Sleeping 10 seconds for bucket hibernation to complete")

        # resume the bucket
        self.rest.resume_operation(bucket_name=self.buckets[0].name, blob_region=self.region, s3_bucket=self.s3_bucket)
        self.verify_fts_functionality(active_indexes=[index2], resumed_indexes=[index1])

        # delete s3 folder
        self.s3_utils_obj.delete_s3_folder(folder=self.buckets[0].name)

    def pause_and_abort(self):
        # create required buckets, collections and then load data
        self._create_kv()
        self.load_data()

        # Create Indexes
        index1 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket1'), "index1")
        index2 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket2'), "index2")

        self.wait_for_indexing_complete()
        self.buckets = self.rest.get_buckets()

        # pause a bucket
        self.s3_utils_obj.delete_s3_folder(folder=self.buckets[0].name)
        self.log.info('Pausing bucket')
        self.rest.pause_operation(bucket_name=self.buckets[0].name, blob_region=self.region, s3_bucket=self.s3_bucket,
                                  pause_complete=False)

        # abort pause operation
        self.rest.stop_pause_operation(bucket=self.buckets[0].name)

        # verify operations post abort pause
        self.verify_fts_functionality(active_indexes=[index1, index2])

        # delete s3 folder
        self.s3_utils_obj.delete_s3_folder(folder=self.buckets[0].name)

    def pause_abort_pause(self):
        # create required buckets, collections and then load data
        self._create_kv()
        self.load_data()

        # Create Indexes
        index1 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket1'), "index1")
        index2 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket2'), "index2")

        self.wait_for_indexing_complete()
        self.buckets = self.rest.get_buckets()

        # pause a bucket
        self.s3_utils_obj.delete_s3_folder(folder=self.buckets[0].name)
        self.log.info('Pausing bucket')
        self.rest.pause_operation(bucket_name=self.buckets[0].name, blob_region=self.region, s3_bucket=self.s3_bucket,
                                  pause_complete=False)

        # abort pause operation
        self.rest.stop_pause(bucket=self.buckets[0].name)

        # pause bucket again
        self.s3_utils_obj.delete_s3_folder(folder=self.buckets[0].name)
        self.log.info('Pausing bucket again')
        self.rest.pause_operation(bucket_name=self.buckets[0].name, blob_region=self.region, s3_bucket=self.s3_bucket)

        self.verify_fts_functionality(paused_indexes=[index1], active_indexes=[index2])

        # delete s3 folder
        self.s3_utils_obj.delete_s3_folder(folder=self.buckets[0].name)

    def pause_and_resume_bucket_with_max_indexes(self):
        # create required buckets, collections and then load data
        self._create_kv()
        self.load_data()

        # Create Indexes
        index_arr = []
        for i in range(20):
            index = self.create_index(self._cb_cluster.get_bucket_by_name('bucket1'), f"index_{i}")
            index_arr.append(index)
        index2 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket2'), "index2")
        self.wait_for_indexing_complete()

        self.buckets = self.rest.get_buckets()

        # pause a bucket
        self.s3_utils_obj.delete_s3_folder(folder="bucket1")
        self.log.info('Pausing bucket')
        self.rest.pause_operation(bucket_name="bucket1", blob_region=self.region, s3_bucket=self.s3_bucket)

        self.verify_fts_functionality(paused_indexes=index_arr, active_indexes=index2)

        self.sleep(10, "Sleeping 10 seconds for bucket hibernation to complete")

        # resume the bucket
        self.rest.resume_operation(bucket_name="bucket1", blob_region=self.region, s3_bucket=self.s3_bucket)

        self.verify_fts_functionality(resumed_indexes=index_arr, active_indexes=index2)

        # delete s3 folder
        self.s3_utils_obj.delete_s3_folder(folder=self.buckets[0].name)

    def pause_multiple_buckets_concurrently(self):
        # create required buckets, collections and then load data
        self._create_kv()
        self.load_data()

        # Create Indexes
        index1 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket1'), "index1")
        index2 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket2'), "index2")
        self.wait_for_indexing_complete()

        self.buckets = self.rest.get_buckets()
        for i in range(2):
            self.s3_utils_obj.delete_s3_folder(folder=f"bucket{i + 1}")

        # pause buckets concurrently
        self.log.info('Pausing buckets')
        thread_arr = []
        for i in range(2):
            thread = threading.Thread(target=self.pause_concurrency_helper_function,
                                      kwargs={"thread_name": f"bucket{i+1}"})
            thread.start()
            thread_arr.append(thread)

        for thread in thread_arr:
                thread.join()

        paused_bucket = None
        count = 0
        for thread, result in self.result_holder.items():
            if isinstance(result, Exception):
                count += 1
                paused_bucket = thread
        print("Failed bucket:", paused_bucket)
        self.assertEqual(count, 1)

        paused_index = None
        active_index = None
        if paused_bucket == "bucket1":
            paused_index = index1
            active_index = index2
        else:
            paused_index = index2
            active_index = index1

        self.verify_fts_functionality(paused_indexes=[paused_index], active_indexes=[active_index])

        self.sleep(10, "Sleeping 10 seconds for bucket hibernation to complete")

        # resume the bucket
        self.rest.resume_operation(bucket_name=paused_bucket, blob_region=self.region, s3_bucket=self.s3_bucket)

        self.verify_fts_functionality(resumed_indexes=[paused_index], active_indexes=[active_index])

        # delete s3 folder
        for i in range(2):
            self.s3_utils_obj.delete_s3_folder(folder=f"bucket{i + 1}")

    def pause_bucket_currently_pausing(self):
        # create required buckets, collections and then load data
        self._create_kv()
        self.load_data()

        # Create Indexes
        index1 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket1'), "index1")
        index2 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket2'), "index2")
        self.wait_for_indexing_complete()

        self.buckets = self.rest.get_buckets()
        # pause a bucket
        self.s3_utils_obj.delete_s3_folder(folder=self.buckets[0].name)
        self.log.info('Pausing bucket')
        self.rest.pause_operation(bucket_name=self.buckets[0].name, blob_region=self.region, s3_bucket=self.s3_bucket,
                                  pause_complete=False)

        # pause bucket again
        try:
            self.rest.pause_operation(bucket_name=self.buckets[0].name, blob_region=self.region,
                                      s3_bucket=self.s3_bucket, pause_complete=True)
        except Exception as e:
            print(f"Error : {str(e)}")
            self.assertIn('Cannot pause/resume bucket, while another bucket is being paused/resumed', str(e),
                          'Error msg incorrect')

        self.verify_fts_functionality(active_indexes=[index1, index2])

        # delete s3 folder
        self.s3_utils_obj.delete_s3_folder(folder=self.buckets[0].name)

    def pause_and_resume_multiple_buckets_sequentially(self):
        # create required buckets, collections and then load data
        self._create_kv()
        self.load_data()

        # Create Indexes
        index1 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket1'), "index1")
        index2 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket2'), "index2")

        self.wait_for_indexing_complete()
        self.buckets = self.rest.get_buckets()

        # pause a bucket 1
        self.s3_utils_obj.delete_s3_folder(folder="bucket1")
        self.log.info('Pausing bucket 1')
        self.rest.pause_operation(bucket_name="bucket1", blob_region=self.region, s3_bucket=self.s3_bucket)

        # pause a bucket 2
        self.s3_utils_obj.delete_s3_folder(folder="bucket2")
        self.log.info('Pausing bucket 2')
        self.rest.pause_operation(bucket_name="bucket2", blob_region=self.region, s3_bucket=self.s3_bucket)

        self.verify_fts_functionality(paused_indexes=[index1, index2])

        self.sleep(10, "Sleeping 10 seconds for bucket hibernation to complete")

        # resume the buckets
        self.log.info('Resuming buckets')
        self.rest.resume_operation(bucket_name="bucket1", blob_region=self.region, s3_bucket=self.s3_bucket)
        self.rest.resume_operation(bucket_name="bucket2", blob_region=self.region, s3_bucket=self.s3_bucket)

        self.verify_fts_functionality(resumed_indexes=[index1, index2])

        # delete s3 folder
        self.s3_utils_obj.delete_s3_folder(folder=self.buckets[0].name)

    def run_queries_during_pausing_and_resuming_state(self):
        # create required buckets, collections and then load data
        self._create_kv()
        self.load_data()

        # Create Indexes
        index1 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket1'), "index1")
        index2 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket2'), "index2")

        self.wait_for_indexing_complete()
        self.buckets = self.rest.get_buckets()

        # pause a bucket
        self.s3_utils_obj.delete_s3_folder(folder="bucket1")
        self.log.info('Pausing bucket')
        self.rest.pause_operation(bucket_name="bucket1", blob_region=self.region, s3_bucket=self.s3_bucket,
                                  pause_complete=False)

        start_time = time.time()
        while time.time() - start_time < 30:
            hits, _, _, _ = index1.execute_query(self.sample_query, zero_results_ok=True)
            if self.rest.wait_bucket_hibernation(task='pause_bucket', operation='completed'):
                break

        self.verify_fts_functionality(paused_indexes=[index1], active_indexes=[index2])

        self.sleep(10, "Sleeping 10 seconds for bucket hibernation to complete")

        # resume the buckets
        self.log.info('Resuming buckets')
        self.rest.resume_operation(bucket_name="bucket1", blob_region=self.region, s3_bucket=self.s3_bucket,
                                   resume_complete=False)
        start_time = time.time()
        while time.time() - start_time < 30:
            hits, _, _, _ = index1.execute_query(self.sample_query, zero_results_ok=True)
            if self.rest.wait_bucket_hibernation(task='resume_bucket', operation='completed'):
                break

        self.verify_fts_functionality(resumed_indexes=[index1], active_indexes=[index2])
        # delete s3 folder
        self.s3_utils_obj.delete_s3_folder(folder=self.buckets[0].name)

    def pause_bucket_currently_resuming(self):
        # create required buckets, collections and then load data
        self._create_kv()
        self.load_data()

        # Create Indexes
        index1 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket1'), "index1")
        index2 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket2'), "index2")

        self.wait_for_indexing_complete()
        self.buckets = self.rest.get_buckets()

        # pause a bucket
        self.s3_utils_obj.delete_s3_folder(folder=self.buckets[0].name)
        self.log.info('Pausing bucket')
        self.rest.pause_operation(bucket_name=self.buckets[0].name, blob_region=self.region, s3_bucket=self.s3_bucket)
        self.verify_fts_functionality(paused_indexes=[index1], active_indexes=[index2])
        self.sleep(10, "Sleeping 10 seconds for bucket hibernation to complete")
        # resume the bucket
        self.rest.resume_operation(bucket_name=self.buckets[0].name, blob_region=self.region, s3_bucket=self.s3_bucket,
                                   resume_complete=False)

        # pause bucket while resuming
        self.log.info('Pausing bucket again')
        try:
            self.rest.pause_operation(bucket_name=self.buckets[0].name, blob_region=self.region,
                                      s3_bucket=self.s3_bucket)
            self.s3_utils_obj.delete_s3_folder(folder=self.buckets[0].name)
            self.fail("Pause operation passed even though the resume wasn't completed")
        except Exception as e:
            print(f"Error : {str(e)}")
            self.assertIn('Cannot pause/resume bucket, while another bucket is being paused/resumed', str(e),
                          'Error msg incorrect')

        self.rest.wait_bucket_hibernation(task='resume_bucket', operation='completed')
        self.verify_fts_functionality(resumed_indexes=[index1], active_indexes=[index2])

        # delete s3 folder
        self.s3_utils_obj.delete_s3_folder(folder=self.buckets[0].name)

    def resume_non_paused_bucket(self):
        # create required buckets, collections and then load data
        self._create_kv()
        self.load_data()

        # Create Indexes
        index1 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket1'), "index1")
        index2 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket2'), "index2")

        self.wait_for_indexing_complete()
        self.buckets = self.rest.get_buckets()

        # Resume an operation
        try:
            self.rest.resume_operation(bucket_name=self.buckets[0].name, blob_region=self.region, s3_bucket=self.s3_bucket)
            self.fail("Resuming a non paused bucked was successful")
        except:
            self.log.info("Success - Cannot resume a non paused bucket")

        self.verify_fts_functionality(active_indexes=[index1, index2])
        # delete s3 folder
        self.s3_utils_obj.delete_s3_folder(folder=self.buckets[0].name)

    def resume_and_abort(self):
        # create required buckets, collections and then load data
        self._create_kv()
        self.load_data()

        # Create Indexes
        index1 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket1'), "index1")
        index2 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket2'), "index2")

        self.wait_for_indexing_complete()
        self.buckets = self.rest.get_buckets()

        # pause a bucket
        self.s3_utils_obj.delete_s3_folder(folder=self.buckets[0].name)
        self.log.info('Pausing bucket')
        self.rest.pause_operation(bucket_name=self.buckets[0].name, blob_region=self.region, s3_bucket=self.s3_bucket)
        self.verify_fts_functionality(paused_indexes=[index1], active_indexes=[index2])

        self.sleep(10, "Sleeping 10 seconds for bucket hibernation to complete")

        # resume the bucket
        self.rest.resume_operation(bucket_name=self.buckets[0].name, blob_region=self.region, s3_bucket=self.s3_bucket,
                                   resume_complete=False)

        # abort resume
        self.rest.stop_resume_operation(bucket=self.buckets[0].name)
        self.verify_fts_functionality(paused_indexes=[index1], active_indexes=[index2])

        # delete s3 folder
        self.s3_utils_obj.delete_s3_folder(folder=self.buckets[0].name)

    def resume_abort_resume(self):
        # create required buckets, collections and then load data
        self._create_kv()
        self.load_data()

        # Create Indexes
        index1 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket1'), "index1")
        index2 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket2'), "index2")

        self.wait_for_indexing_complete()
        self.buckets = self.rest.get_buckets()

        # pause a bucket
        self.s3_utils_obj.delete_s3_folder(folder=self.buckets[0].name)
        self.log.info('Pausing bucket')
        self.rest.pause_operation(bucket_name=self.buckets[0].name, blob_region=self.region, s3_bucket=self.s3_bucket)
        self.verify_fts_functionality(paused_indexes=[index1], active_indexes=[index2])

        self.sleep(10, "Sleeping 10 seconds for bucket hibernation to complete")

        # resume the bucket
        self.rest.resume_operation(bucket_name=self.buckets[0].name, blob_region=self.region, s3_bucket=self.s3_bucket,
                                   resume_complete=False)

        # abort resume
        self.rest.stop_resume_operation(bucket=self.buckets[0].name)

        # resume the bucket
        self.rest.resume_operation(bucket_name=self.buckets[0].name, blob_region=self.region, s3_bucket=self.s3_bucket)

        self.verify_fts_functionality(resumed_indexes=[index1], active_indexes=[index2])

        # delete s3 folder
        self.s3_utils_obj.delete_s3_folder(folder=self.buckets[0].name)

    def resume_multiple_buckets_concurrently(self):
        # create required buckets, collections and then load data
        self._create_kv()
        self.load_data()

        # Create Indexes
        index1 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket1'), "index1")
        index2 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket2'), "index2")
        self.wait_for_indexing_complete()

        self.buckets = self.rest.get_buckets()
        for i in range(2):
            self.s3_utils_obj.delete_s3_folder(folder=f"bucket{i + 1}")

        # pause a bucket 1
        self.s3_utils_obj.delete_s3_folder(folder="bucket1")
        self.log.info('Pausing bucket 1')
        self.rest.pause_operation(bucket_name="bucket1", blob_region=self.region, s3_bucket=self.s3_bucket)

        # pause a bucket 2
        self.s3_utils_obj.delete_s3_folder(folder="bucket2")
        self.log.info('Pausing bucket 2')
        self.rest.pause_operation(bucket_name="bucket2", blob_region=self.region, s3_bucket=self.s3_bucket)

        self.verify_fts_functionality(paused_indexes=[index1, index2])

        self.sleep(10, "Sleeping 10 seconds for bucket hibernation to complete")

        # resume buckets concurrently
        self.log.info('Resuming buckets')
        thread_arr = []
        for i in range(2):
            thread = threading.Thread(target=self.resume_concurrency_helper_function,
                                      kwargs={"thread_name":f"bucket{i+1}"})
            thread.start()
            thread_arr.append(thread)

        for thread in thread_arr:
            thread.join()

        non_resumed_bucket = None
        count = 0
        for thread, result in self.result_holder.items():
            if isinstance(result, Exception):
                count += 1
                non_resumed_bucket = thread
        print("Bucket that failed to resume:", non_resumed_bucket)
        self.assertEqual(count, 1)

        resumed_index = None
        active_index = None
        if non_resumed_bucket == "bucket1":
            resumed_index = index2
            active_index = index1
        else:
            resumed_index = index1
            active_index = index2

        self.verify_fts_functionality(resumed_indexes=[resumed_index], active_indexes=[active_index])

        # delete s3 folder
        self.s3_utils_obj.delete_s3_folder(folder=self.buckets[0].name)

    def resume_bucket_currently_resuming(self):
        # create required buckets, collections and then load data
        self._create_kv()
        self.load_data()

        # Create Indexes
        index1 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket1'), "index1")
        index2 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket2'), "index2")
        self.wait_for_indexing_complete()

        self.buckets = self.rest.get_buckets()
        # pause a bucket
        self.s3_utils_obj.delete_s3_folder(folder=self.buckets[0].name)
        self.log.info('Pausing bucket')
        self.rest.pause_operation(bucket_name=self.buckets[0].name, blob_region=self.region, s3_bucket=self.s3_bucket)

        self.verify_fts_functionality(paused_indexes=index1, active_indexes=index2)
        self.sleep(10, "Sleeping 10 seconds for bucket hibernation to complete")
        self.rest.resume_operation(bucket_name=self.buckets[0].name, blob_region=self.region, s3_bucket=self.s3_bucket,
                                   resume_complete=False)

        # resume bucket again
        try:
            self.rest.resume_operation(bucket_name=self.buckets[0].name, blob_region=self.region,
                                       s3_bucket=self.s3_bucket)
        except Exception as e:
            print(f"Error : {str(e)}")
            # add assertion here like bucket which is currently pausing cannot be paused again

        # delete s3 folder
        self.s3_utils_obj.delete_s3_folder(folder=self.buckets[0].name)

    def resume_bucket_currently_pausing(self):
        # create required buckets, collections and then load data
        self._create_kv()
        self.load_data()

        # Create Indexes
        index1 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket1'), "index1")
        index2 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket2'), "index2")
        self.wait_for_indexing_complete()

        self.buckets = self.rest.get_buckets()
        # pause a bucket
        self.s3_utils_obj.delete_s3_folder(folder=self.buckets[0].name)
        self.log.info('Pausing bucket')
        self.rest.pause_operation(bucket_name=self.buckets[0].name, blob_region=self.region, s3_bucket=self.s3_bucket,
                                  pause_complete=False)
        try:
            self.rest.resume_operation(bucket_name=self.buckets[0].name, blob_region=self.region,
                                       s3_bucket=self.s3_bucket, resume_complete=False)
        except Exception as e:
            print(f"Error : {str(e)}")

        self.sleep(10, "waiting for bucket to be paused completely")
        self.verify_fts_functionality(paused_indexes=index1, active_indexes=index2)

        # delete s3 folder
        self.s3_utils_obj.delete_s3_folder(folder=self.buckets[0].name)

    def pause_bucket_during_failover(self):
        # create required buckets, collections and then load data
        self._create_kv()
        self.load_data()

        # Create Indexes
        index1 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket1'), "index1")
        index2 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket2'), "index2")
        self.wait_for_indexing_complete()

        self.buckets = self.rest.get_buckets()
        self.log.info("Initiating node failover")
        self._cb_cluster.failover()

        self.log.info('Pausing bucket')
        try:
            self.rest.pause_operation(bucket_name=self.buckets[0].name, blob_region=self.region, s3_bucket=self.s3_bucket,
                                      pause_complete=False)
        except Exception as e:
            self.log.error(f"Error : {e}")

        self.verify_fts_functionality(active_indexes=[index1, index2])

        self._cb_cluster.async_failover_add_back_node()
        self.sleep(30, "waiting for 30 seconds for the cluster to finish add back")

        start_time = time.time()

        while time.time() - start_time < 30:
            self.print_ns_server_task()
            time.sleep(10)
            # <Check for pause operation>
            # assert pause to be started

        self.verify_fts_functionality(paused_indexes=index1, active_indexes=index2)


    def resume_bucket_during_failover(self):
        # create required buckets, collections and then load data
        self._create_kv()
        self.load_data()

        # Create Indexes
        index1 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket1'), "index1")
        index2 = self.create_index(self._cb_cluster.get_bucket_by_name('bucket2'), "index2")
        self.wait_for_indexing_complete()

        self.buckets = self.rest.get_buckets()

        self.log.info('Pausing bucket')
        self.rest.pause_operation(bucket_name=self.buckets[0].name, blob_region=self.region, s3_bucket=self.s3_bucket,
                                      pause_complete=False)
        self.verify_fts_functionality(paused_indexes=[index1], active_indexes=[index2])
        self.sleep(10, "Sleeping 10 seconds for bucket hibernation to complete")
        self.log.info("Initiating node failover")
        self._cb_cluster.failover()
        try:
            self.rest.resume_operation(bucket_name=self.buckets[0].name, blob_region=self.region,
                                       s3_bucket=self.s3_bucket, resume_complete=False)
        except Exception as e:
            self.log.error(f"Error {e}")


        self._cb_cluster.async_failover_add_back_node()
        self.sleep(30, "waiting for 30 seconds for the cluster to finish add back")
        self.verify_fts_functionality(paused_indexes=[index1], active_indexes=[index2])