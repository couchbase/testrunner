import json
import os
import logging
import re

from lib.membase.api.rest_client import RestConnection
from lib.testconstants import STANDARD_BUCKET_PORT
from pytests.eventing.eventing_constants import HANDLER_CODE, EXPORTED_FUNCTION, HANDLER_CODE_CURL
from pytests.eventing.eventing_base import EventingBaseTest, log

log = logging.getLogger()


class EventingLifeCycle(EventingBaseTest):
    def setUp(self):
        super(EventingLifeCycle, self).setUp()
        self.src_bucket = self.rest.get_bucket_by_name(self.src_bucket_name)
        self.gens_load = self.generate_docs(self.docs_per_day)

    def tearDown(self):
        super(EventingLifeCycle, self).tearDown()

    def test_function_deploy_undeploy_in_a_loop_for_bucket_operations(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE)
        for i in range(1, 5):
            self.cluster.bucket_flush(self.master, self.dst_bucket_name)
            self.deploy_function(body)
            self.undeploy_function(body)
        self.sleep(30)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the create mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        self.undeploy_and_delete_function(body)

    def test_function_deploy_undeploy_in_a_loop_for_n1ql_operations(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.N1QL_INSERT_ON_UPDATE)
        for i in range(1, 5):
            self.cluster.bucket_flush(self.master, self.dst_bucket_name)
            self.deploy_function(body)
            self.undeploy_function(body)
        self.sleep(30)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the create mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        self.undeploy_and_delete_function(body)

    def test_function_deploy_undeploy_in_a_loop_for_doc_timers(self):
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_WITH_DOC_TIMER)
        for i in range(1, 5):
            self.cluster.bucket_flush(self.master, self.src_bucket_name)
            self.cluster.bucket_flush(self.master, self.dst_bucket_name)
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size)
            self.deploy_function(body)
            self.undeploy_function(body)
        self.sleep(30)
        # doc timers wont process the same docs again if there is no update
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='update')
        self.deploy_function(body)
        # Wait for eventing to catch up with all the create mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_function_pause_resume_in_a_loop_for_bucket_operations(self):
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE)
        self.deploy_function(body)
        # load some data
        task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                self.buckets[0].kvs[1], 'create', compression=self.sdk_compression)
        for i in range(1, 10):
            self.pause_function(body)
            # This sleep in intentionally put in a function
            self.sleep(5, "sleep for some seconds after pausing the function")
            self.resume_function(body)
        task.result()
        self.sleep(30)
        # Wait for eventing to catch up with all the create mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_function_pause_resume_in_a_loop_for_n1ql_operations(self):
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.N1QL_INSERT_ON_UPDATE)
        self.deploy_function(body)
        # load some data
        task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                self.buckets[0].kvs[1], 'create', compression=self.sdk_compression)
        for i in range(1, 10):
            self.pause_function(body)
            # This sleep in intentionally put in a function
            self.sleep(5, "sleep for some seconds after pausing the function")
            self.resume_function(body)
        task.result()
        self.sleep(30)
        # Wait for eventing to catch up with all the create mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_function_pause_resume_in_a_loop_for_doc_timers(self):
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_WITH_DOC_TIMER)
        self.deploy_function(body)
        # load some data
        task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                self.buckets[0].kvs[1], 'create', compression=self.sdk_compression)
        for i in range(1, 10):
            self.pause_function(body)
            # This sleep in intentionally put in a function
            self.sleep(5, "sleep for some seconds after pausing the function")
            self.resume_function(body)
        task.result()
        self.sleep(30)
        # Wait for eventing to catch up with all the create mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_export_function(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_WITH_CRON_TIMER)
        self.deploy_function(body)
        # export the function that we have created
        output = self.rest.export_function(self.function_name, self.function_scope)
        # Wait for eventing to catch up with all the create mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        log.info("exported function")
        log.info(output["settings"])
        log.info("imported function")
        log.info(body["settings"])
        # Validate that exported function data matches with the function that we created
        self.assertTrue(output["appname"] == self.function_name, msg="Function name mismatch from the exported function")
        self.assertTrue(output["appcode"] == body["appcode"], msg="Handler code mismatch from the exported function")
        # Looks like exported functions add few more settings. So it will not be the same anymore
        # self.assertTrue(cmp(output["settings"], body["settings"]) == 0,
        #                 msg="Settings mismatch from the exported function")
        self.undeploy_and_delete_function(body)

    def test_import_function(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.import_function_from_directory(EXPORTED_FUNCTION.N1QL_INSERT_ON_UPDATE_WITH_CRON_TIMER)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the create mutations and verify results
        self.verify_eventing_results(body['appname'], self.docs_per_day * 2016)
        self.undeploy_delete_all_functions()

    def test_eventing_debugger(self):
        count = 0
        match = False
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE)
        self.deploy_function(body)
        #enable debugger
        self.rest.enable_eventing_debugger()
        # Start eventing debugger
        out1 = self.rest.start_eventing_debugger(self.function_name, self.function_scope)
        log.info(" Started eventing debugger : {0}".format(out1))
        # do some mutations
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        eventing_ip = self.get_nodes_from_services_map(service_type="eventing")
        # get debugger url
        pattern = re.compile(eventing_ip.ip+':9140(.*)')
        while count < 10:
            out2 = self.rest.get_eventing_debugger_url(self.function_name, self.function_scope)
            self.log.info("url generated {}".format(out2))
            url=json.loads(out2)
            matched = re.match(pattern, url["websocket"])
            if matched:
                log.info("Got debugger url : {0}{1}".format(matched.group(0), matched.group(1)))
                match = True
                break
            count += 1
            self.sleep(30)
        if not match:
            self.fail("Debugger url was not generated even after waiting for 300 secs...    ")
        # stop debugger
        self.rest.stop_eventing_debugger(self.function_name, self.function_scope)
        # undeploy and delete the function
        self.undeploy_and_delete_function(body)

    #MB-30847
    def test_eventing_debugger_default_settings(self):
        count = 0
        match = False
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE)
        self.deploy_function(body)
        try:
            out1 = self.rest.start_eventing_debugger(self.function_name, self.function_scope)
        except Exception as e:
            msg=json.dumps(str(e))
            assert "ERR_DEBUGGER_DISABLED" in msg
        #enable debugger
        self.rest.enable_eventing_debugger()
        #start debugger
        out1 = self.rest.start_eventing_debugger(self.function_name, self.function_scope)
        log.info(" Started eventing debugger : {0}".format(out1))
        # do some mutations
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        eventing_ip = self.get_nodes_from_services_map(service_type="eventing")
        # get debugger url
        pattern = re.compile(eventing_ip.ip + ':9140(.*)')
        while count < 10:
            out2 = self.rest.get_eventing_debugger_url(self.function_name, self.function_scope)
            url = json.loads(out2)
            matched = re.match(pattern, url["websocket"])
            if matched:
                log.info("Got debugger url : {0}{1}".format(matched.group(0), matched.group(1)))
                match = True
                break
            count += 1
            self.sleep(30)
        if not match:
            self.fail("Debugger url was not generated even after waiting for 300 secs...    ")
        # stop debugger
        self.rest.stop_eventing_debugger(self.function_name, self.function_scope)
        # disable debugger
        self.rest.disable_eventing_debugger()
        try:
            out1 = self.rest.start_eventing_debugger(self.function_name, self.function_scope)
        except Exception as e:
            msg=json.dumps(str(e))
            assert "ERR_DEBUGGER_DISABLED" in msg
        # undeploy and delete the function
        self.undeploy_and_delete_function(body)

    def test_undeploying_functions_when_timers_are_getting_fired(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_WITH_DOC_TIMER,
                                              worker_count=3)
        self.deploy_function(body)
        # undeploy a function when timers are getting fired
        try:
            self.undeploy_function(body)
        except Exception as ex:
            # Sometimes when we undeploy, it fails with "No JSON object could be decoded"
            pass
        self.sleep(120)
        self.wait_for_handler_state(body['appname'], "undeployed")
        # Check to ensure metada bucket does not have any documents after undeploy
        stats_meta = self.rest.get_bucket_stats(self.metadata_bucket_name)
        if stats_meta["curr_items"] != 0:
            self.fail("Metadata bucket still has some docs left after undeploy : {0}".format(stats_meta["curr_items"]))

    def test_eventing_debugger_source_bucket_mutation(self):
        count = 0
        match = False
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OP_WITH_SOURCE_BUCKET_MUTATION)
        self.deploy_function(body)
        #enable debugger
        self.rest.enable_eventing_debugger()
        # Start eventing debugger
        out1 = self.rest.start_eventing_debugger(self.function_name, self.function_scope)
        log.info(" Started eventing debugger : {0}".format(out1))
        # do some mutations
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        # get debugger url
        eventing_ip = self.get_nodes_from_services_map(service_type="eventing")
        # get debugger url
        pattern = re.compile(eventing_ip.ip + ':9140(.*)')
        while count < 10:
            out2 = self.rest.get_eventing_debugger_url(self.function_name, self.function_scope)
            url = json.loads(out2)
            matched = re.match(pattern, url["websocket"])
            if matched:
                log.info("Got debugger url : {0}{1}".format(matched.group(0), matched.group(1)))
                match = True
                break
            count += 1
            self.sleep(30)
        if not match:
            self.fail("Debugger url was not generated even after waiting for 300 secs...    ")
        # stop debugger
        self.rest.stop_eventing_debugger(self.function_name, self.function_scope)
        # undeploy and delete the function
        self.undeploy_and_delete_function(body)

    def test_pause_resume_undeploy_delete(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OP_WITH_SOURCE_BUCKET_MUTATION)
        self.deploy_function(body)
        self.pause_function(body)
        self.resume_function(body)
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016 * 2, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_pause_undeploy_delete(self):
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE)
        self.deploy_function(body)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        self.pause_function(body)
        self.undeploy_and_delete_function(body)

    def test_eventing_debugger_pause_resume(self):
        count = 0
        match = False
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE)
        self.deploy_function(body)
        self.pause_function(body)
        #enable debugger
        self.rest.enable_eventing_debugger()
        # Start eventing debugger
        out1 = self.rest.start_eventing_debugger(self.function_name, self.function_scope)
        log.info(" Started eventing debugger : {0}".format(out1))
        # do some mutations
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        eventing_ip = self.get_nodes_from_services_map(service_type="eventing")
        # get debugger url
        pattern = re.compile(eventing_ip.ip + ':9140(.*)')
        while count < 10:
            out2 = self.rest.get_eventing_debugger_url(self.function_name, self.function_scope)
            url = json.loads(out2)
            matched = re.match(pattern, url["websocket"])
            if matched:
                log.info("Got debugger url : {0}{1}".format(matched.group(0), matched.group(1)))
                match = True
                break
            count += 1
            self.sleep(30)
        if not match:
            self.fail("Debugger url was not generated even after waiting for 300 secs...    ")
        # stop debugger
        self.rest.stop_eventing_debugger(self.function_name, self.function_scope)
        # undeploy and delete the function
        self.undeploy_and_delete_function(body)

    def test_eventing_debugger_curl(self):
        count = 0
        match = False
        body = self.create_save_function_body(self.function_name, HANDLER_CODE_CURL.BUCKET_OP_WITH_CURL_GET)
        body['depcfg']['curl'] = []
        body['depcfg']['curl'].append(
            {"hostname": self.hostname, "value": "server", "auth_type": self.auth_type, "username": self.curl_username,
             "password": self.curl_password, "cookies": self.cookies})
        self.rest.create_function(body['appname'], body, self.function_scope)
        self.deploy_function(body)
        #enable debugger
        self.rest.enable_eventing_debugger()
        # Start eventing debugger
        out1 = self.rest.start_eventing_debugger(self.function_name, self.function_scope)
        log.info(" Started eventing debugger : {0}".format(out1))
        # do some mutations
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        eventing_ip = self.get_nodes_from_services_map(service_type="eventing")
        # get debugger url
        pattern = re.compile(eventing_ip.ip + ':9140(.*)')
        while count < 10:
            out2 = self.rest.get_eventing_debugger_url(self.function_name, self.function_scope)
            url = json.loads(out2)
            matched = re.match(pattern, url["websocket"])
            if matched:
                log.info("Got debugger url : {0}{1}".format(matched.group(0), matched.group(1)))
                match = True
                break
            count += 1
            self.sleep(30)
        if not match:
            self.fail("Debugger url was not generated even after waiting for 300 secs...    ")
        # stop debugger
        self.rest.stop_eventing_debugger(self.function_name, self.function_scope)
        # undeploy and delete the function
        self.undeploy_and_delete_function(body)

    def test_export_credentials(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE_CURL.BUCKET_OP_WITH_CURL_GET)
        body['depcfg']['curl'] = []
        body['depcfg']['curl'].append(
            {"hostname": self.hostname, "value": "server", "auth_type": self.auth_type, "username": self.curl_username,
             "password": self.curl_password, "cookies": self.cookies})
        self.rest.create_function(body['appname'],body)
        self.deploy_function(body)
        # export the function that we have created
        output = self.rest.export_function(self.function_name, self.function_scope)
        # Wait for eventing to catch up with all the create mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        log.info("exported function")
        log.info(output)
        log.info("imported function")
        log.info(body)
        # Validate that exported function data matches with the function that we created
        self.assertTrue(output['depcfg']['curl'][0]['password'] == "", msg="password is not empty")
        self.assertTrue(output['depcfg']['curl'][0]['username'] == "", msg="username is not empty")
        self.undeploy_and_delete_function(body)

    def test_eventing_debugger_ABO(self):
        count = 0
        match = False
        body = self.create_save_function_body(self.function_name, "handler_code/ABO/insert.js")
        self.deploy_function(body)
        #enable debugger
        self.rest.enable_eventing_debugger()
        # Start eventing debugger
        out1 = self.rest.start_eventing_debugger(self.function_name, self.function_scope)
        log.info(" Started eventing debugger : {0}".format(out1))
        # do some mutations
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        # get debugger url
        eventing_ip = self.get_nodes_from_services_map(service_type="eventing")
        # get debugger url
        pattern = re.compile(eventing_ip.ip + ':9140(.*)')
        while count < 10:
            out2 = self.rest.get_eventing_debugger_url(self.function_name, self.function_scope)
            url = json.loads(out2)
            matched = re.match(pattern, url["websocket"])
            if matched:
                log.info("Got debugger url : {0}{1}".format(matched.group(0), matched.group(1)))
                match = True
                break
            count += 1
            self.sleep(30)
        if not match:
            self.fail("Debugger url was not generated even after waiting for 300 secs...    ")
        # stop debugger
        self.rest.stop_eventing_debugger(self.function_name, self.function_scope)
        # undeploy and delete the function
        self.undeploy_and_delete_function(body)

    # MB-42177
    def test_single_function_filter(self):
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE, worker_count=3)
        function_details = self.rest.get_function_details(body['appname'], self.function_scope)
        body1 = json.loads(function_details)
        assert body1['appname'] == self.function_name, True

    # MB-40502
    def same_api_call_multiple_times(self):
        try:
            body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE, worker_count=3)
            self.deploy_function(body)
            self.deploy_function(body)
        except Exception as e:
            self.log.info(e)
            assert "ERR_APP_ALREADY_DEPLOYED" in str(e), True
        finally:
            self.undeploy_and_delete_function(body)

    # MB-40423
    def test_error_deploy_after_pause(self):
        try:
            body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE, worker_count=3)
            self.deploy_function(body)
            self.pause_function(body)
            self.deploy_function(body)
        except Exception as e:
            self.log.info(e)
            assert "ERR_APP_PAUSED" in str(e), True
        finally:
            self.undeploy_and_delete_function(body)

    def test_update_appcode_when_handler_is_paused(self):
        bucket_params = self._create_bucket_params(server=self.server, size=self.bucket_size,
                                                   replicas=self.num_replicas)
        self.cluster.create_standard_bucket(name=self.dst_bucket_name1, port=STANDARD_BUCKET_PORT + 1,
                                            bucket_params=bucket_params)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, "handler_code/appcode_before_update.js")
        body1 = self.create_save_function_body(self.function_name + "1", "handler_code/appcode_after_update.js")
        body['depcfg']['buckets'].append(
            {"alias": self.dst_bucket_name1, "bucket_name": self.dst_bucket_name1, "access": "rw"})
        self.rest.create_function(body['appname'], body, self.function_scope)
        self.deploy_function(body)
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        self.pause_function(body)
        self.rest.update_function_appcode(body1['appcode'], self.function_name, self.function_scope)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        self.resume_function(body)
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True, bucket=self.dst_bucket_name1)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                          batch_size=self.batch_size, op_type='delete')
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True, bucket=self.dst_bucket_name1)
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        self.undeploy_and_delete_function(body)
        self.delete_function(body1)
