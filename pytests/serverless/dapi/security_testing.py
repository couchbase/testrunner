from pytests.serverless.serverless_basetestcase import ServerlessBaseTestCase
from lib.serverlessLib.dapi.dapi import RestfulDAPI
from TestInput import TestInputSingleton

import json
import random
import time
import string
import requests
import threading


class DataAPITests(ServerlessBaseTestCase):

    def setUp(self):
        super().setUp()
        self.num_buckets = self.input.param("num_buckets", 1)
        
        self.create_database(self.num_buckets)
        self.value_size = self.input.param("value_size", 255)
        self.key_size = self.input.param("key_size", 8)
        self.number_of_docs = self.input.param("number_of_docs", 1)
        self.randomize_value = self.input.param("randomize_value", False)
        self.randomize_doc_size = self.input.param("randomize_doc_size", False)
        self.tenant_id = self.input.capella.get("tenant_id")
        self.project_id = self.input.capella.get("project_id")
        self.pod = self.input.capella.get("pod")
        self.user = self.input.capella.get("capella_user")
        self.pwd = self.input.capella.get("capella_pwd")
        self.internal_url = self.pod.replace("cloud", "", 1)
        
        self.rest_username = \
            TestInputSingleton.input.membase_settings.rest_username
        self.rest_password = \
            TestInputSingleton.input.membase_settings.rest_password

    def tearDown(self):
        return super().tearDown()

    def create_database(self, num_of_database):
        tasks = list()
        dapi_list = list()
        for i in range(0,num_of_database):
            seed = "dapi-" + str(i)
            task = self.create_database_async(seed)
            tasks.append(task)
        for task in tasks:
            task.result()
        for database in self.databases.values():
            dapi_info = dict()
            dapi_info["dapi_endpoint"] = database.data_api
            dapi_info["access_token"] = database.access_key
            dapi_info["access_secret"] = database.secret_key
            dapi_info["database_id"] = database.id
            dapi_list.append(dapi_info)
        time.sleep(70)
        self.dapi_info_list = dapi_list


    def test_create_scope_with_incorrect_access_token(self):
        for dapi_info in self.dapi_info_list:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": dapi_info["dapi_endpoint"],
                                          "access_token": "00000000000000000000000",
                                          "access_secret": dapi_info["access_secret"]})
            scope_name = "testScope"
            response = self.rest_dapi.create_scope({"scopeName": scope_name})
            self.log.info("Response Received, status ->".format(response.status_code))
            self.assertEqual(response.status_code,401)

    def test_create_scope_with_incorrect_access_secret(self):
        for dapi_info in self.dapi_info_list:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": dapi_info["dapi_endpoint"],
                                          "access_token": dapi_info["access_token"],
                                          "access_secret": "0000000000000000000000000000"})
            scope_name = "testScope"
            response = self.rest_dapi.create_scope({"scopeName": scope_name})
            self.log.info("Response Received, status ->".format(response.status_code))
            self.assertEqual(response.status_code,401)

    def test_brute_force_access_secret_60_seconds(self):
        initial_time = time.time()
        length_access_secret = 36
        chars_allowed = list(string.ascii_letters) + (list(string.digits))
        while time.time() < initial_time + 60:
            access_secret = ''.join(
                random.choice(chars_allowed)
                for i in range(length_access_secret))
            for dapi_info in self.dapi_info_list:
                self.rest_dapi = RestfulDAPI({"dapi_endpoint": dapi_info["dapi_endpoint"],
                                          "access_token": dapi_info["access_token"],
                                          "access_secret": access_secret})
                scope_name = "testScope"
                response = self.rest_dapi.create_scope(
                    {"scopeName": scope_name})
                self.log.info(response.status_code)
                self.assertEqual(response.status_code,401)

    def test_brute_force_access_token_60_seconds(self):
        initial_time = time.time()
        length_access_token = 15
        chars_allowed = list(string.ascii_letters) + (list(string.digits))
        while time.time() < initial_time + 60:
            access_token = ''.join(
                random.choice(chars_allowed)
                for i in range(length_access_token))
            for dapi_info in self.dapi_info_list:
                self.rest_dapi = RestfulDAPI({"dapi_endpoint": dapi_info["dapi_endpoint"],
                                          "access_token": access_token,
                                          "access_secret": dapi_info["access_secret"]})
                scope_name = "testScope"
                response = self.rest_dapi.create_scope(
                    {"scopeName": scope_name})
                self.log.info(response.status_code)
                self.assertEqual(response.status_code,401)

    def test_sql_injection_attack_doc_key(self):
       for dapi_info in self.dapi_info_list:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": dapi_info["dapi_endpoint"],
                                          "access_token": dapi_info["access_token"],
                                          "access_secret": dapi_info["access_secret"]})
            key = "abcdefgh123456"
            doc = {"body":"aaaaabbbbbbbcccccccc"}
            response = self.rest_dapi.insert_doc(key + " or 1=1", doc,
                                                 '_default', '_default')

            # Adding a stray character to the key and verify that it still does not work
            response = self.rest_dapi.get_doc(key + "a or 1=1", "_default",
                                              "_default")
            self.assertEqual(response.status_code,404)

    def test_sql_injection_attack_doc_body(self):
       for dapi_info in self.dapi_info_list:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": dapi_info["dapi_endpoint"],
                                          "access_token": dapi_info["access_token"],
                                          "access_secret": dapi_info["access_secret"]})
            key = "abcdefgh123456"
            doc = {"body":"aaaaabbbbbbbcccccccc"}
            doc["body"] += " or 1=1"

            response = self.rest_dapi.insert_doc(key, doc, '_default',
                                                 '_default')
            time.sleep(2.0)
            response = self.rest_dapi.get_doc(key, "_default", "_default")

            # This verifies that a command like 1=1 is taken just as a string and is not running
            self.assertEqual(response.json()["doc"]["body"][-7:],' or 1=1')

    def test_sql_injection_attack_access_token(self):
        for dapi_info in self.dapi_info_list:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": dapi_info["dapi_endpoint"],
                                          "access_token": dapi_info["access_token"]+" or 1=1",
                                          "access_secret": dapi_info["access_secret"]})
            scope_name = "testScope"
            response = self.rest_dapi.create_scope(
                {"scopeName": scope_name})
            self.log.info(response.status_code)
            # Must be unauthorized as token has changed
            self.assertEqual(response.status_code,401)

    def test_sql_injection_attack_access_secret(self):
        for dapi_info in self.dapi_info_list:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": dapi_info["dapi_endpoint"],
                                          "access_token": dapi_info["access_token"],
                                          "access_secret": dapi_info["access_secret"]+" or 1=1"})
            scope_name = "testScope"
            response = self.rest_dapi.create_scope(
                {"scopeName": scope_name})
            self.log.info(response.status_code)
            # Must be unauthorized as secret has changed
            self.assertEqual(response.status_code ,401)

    def test_sql_injection_attack_endpoint(self):
        for dapi_info in self.dapi_info_list:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": dapi_info["dapi_endpoint"]+ '?parameter=x|1=1',
                                          "access_token": dapi_info["access_token"],
                                          "access_secret": dapi_info["access_secret"]})
            scope_name = "testScope"
            response = self.rest_dapi.create_scope(
                {"scopeName": scope_name})
            self.log.info(response.status_code)
            # It should not show any data, instead it should just give not found
            self.assertEqual(response.status_code,404)

    def test_command_injection_attack_endpoint(self):
        for dapi_info in self.dapi_info_list:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": dapi_info["dapi_endpoint"]+ '?parameter=x||ping+-c+10+127.0.0.1||',
                                          "access_token": dapi_info["access_token"],
                                          "access_secret": dapi_info["access_secret"]})
            scope_name = "testScope"
            response = self.rest_dapi.create_scope(
                {"scopeName": scope_name})
            self.log.info(response.status_code)
            # There should not be extra time wasted, instead it should just give not found
            self.assertEqual(response.status_code,404)

    def test_after_allowing_current_ip(self):
        for dapi_info in self.dapi_info_list:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": dapi_info["dapi_endpoint"],
                                          "access_token": dapi_info["access_token"],
                                          "access_secret": dapi_info["access_secret"]})
            scope_name = "testScope"
            response = self.rest_dapi.create_scope(
                {"scopeName": scope_name})
            self.log.info(response.status_code)
            # It should be authorized
            self.assertEqual(response.status_code,200)

    def test_after_deleting_current_ip(self):
        time.sleep(10)
        for dapi_info in self.dapi_info_list:
            url = '{}/v2/organizations/{}/projects/{}/clusters/{}/allowlists?page=1&perPage=100&sortBy=cidr&sortDirection=asc' \
                .format(self.internal_url, self.tenant_id, self.project_id, dapi_info["database_id"])
            # Get Jwt
            jwtUrl = '{}/sessions'.format(self.internal_url)

            resp = requests.post(jwtUrl, auth=(self.user, self.pwd))

            jwt = resp.json()["jwt"]

            # Get all IPs Allowed
            headers = {"Authorization": "Bearer "+jwt}
            resp = requests.get(url, headers=headers)
            ip_address_id = resp.json()["data"][0]["data"]["id"]

            # Delete the given IP from allowed lists
            url = '{}/v2/organizations/{}/projects/{}/clusters/{}/allowlists-bulk' \
                .format(self.internal_url, self.tenant_id, self.project_id, dapi_info["database_id"])
            data_send = {
                "delete": [
                    ip_address_id
                ]
            }
            resp = requests.post(url, data=json.dumps(data_send), headers=headers)

            self.rest_dapi = RestfulDAPI({"dapi_endpoint": dapi_info["dapi_endpoint"],
                                          "access_token": dapi_info["access_token"],
                                          "access_secret": dapi_info["access_secret"]})
            scope_name = "testScope"
            response = self.rest_dapi.create_scope(
                {"scopeName": scope_name})
            self.log.info(response.status_code)
            # It should be unauthorized
            self.assertEqual(response.status_code, 401)

    def test_wrong_payload(self):
        for dapi_info in self.dapi_info_list:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": dapi_info["dapi_endpoint"],
                                          "access_token": dapi_info["access_token"],
                                          "access_secret": dapi_info["access_secret"]})
            scope_name = "testScope"
            response = self.rest_dapi.create_scope(
                {
                    "params": "1=1"
                })
            self.log.info(response.status_code)
            # It should not show any data or create the requested, instead it should just give bad request or internal server error
            self.assertEqual(response.status_code,500)

    def dos_task(self):
        scope_name = "testScope" + str(random.randint(1,1000000000))
        response = self.rest_dapi.create_scope({"scopeName": scope_name})
        if response == None or response.status_code == None or response.status_code != 200:
            self.lock.acquire()
            self.failure_counter += 1
            self.lock.release()

    def incorrect_creds_task(self):
        for dapi_info in self.dapi_info_list:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": dapi_info["dapi_endpoint"],
                                          "access_token":"000000000000000000000",
                                          "access_secret": dapi_info["access_secret"]})

            scope_name = "testScope" + str(random.randint(1,10000000))
            response = self.rest_dapi.create_scope({"scopeName": scope_name})
            if response == None or response.status_code == None or response.status_code != 401:
                self.lock.acquire()
                self.failure_counter += 1
                self.lock.release()

    def sql_injection_task(self):
        for dapi_info in self.dapi_info_list:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": dapi_info["dapi_endpoint"],
                                          "access_token": dapi_info["access_token"],
                                          "access_secret": dapi_info["access_secret"]+"?1=1"})
            scope_name = "testScope"+ str(random.randint(1,10000000))
            response = self.rest_dapi.create_scope(
                {"scopeName": scope_name})
            
            if response == None or response.status_code == None or response.status_code != 401:
                self.lock.acquire()
                self.failure_counter += 1
                self.lock.release()
    
    def add_scope_success_task(self):
        for dapi_info in self.dapi_info_list:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": dapi_info["dapi_endpoint"],
                                          "access_token": dapi_info["access_token"],
                                          "access_secret": dapi_info["access_secret"]})
            scope_name = "testScope"+ str(random.randint(1,10000000))
            response = self.rest_dapi.create_scope(
                {"scopeName": scope_name})
            if response == None or response.status_code == None or response.status_code != 200:
                self.lock.acquire()
                self.failure_counter += 1
                self.lock.release()

    def wrong_payload_task(self):
        for dapi_info in self.dapi_info_list:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": dapi_info["dapi_endpoint"],
                                          "access_token": dapi_info["access_token"],
                                          "access_secret": dapi_info["access_secret"]})
            scope_name = "testScope"
            response = self.rest_dapi.create_scope(
                {
                    "params": "1=1"
                })
            if response == None or response.status_code == None or response.status_code != 500:
                self.lock.acquire()
                self.failure_counter += 1
                self.lock.release()

    def test_multi_functions_attack(self):
        time.sleep(10)
        threads = 200
        self.lock = threading.Lock()
        processes = []
        self.failure_counter = 0
        for i in range(threads):
            curr_target = self.dos_task
            if i%4 == 0:
                curr_target = self.incorrect_creds_task
            elif i%4 == 1:
                curr_target = self.sql_injection_task
            elif i%4 == 2:
                curr_target = self.add_scope_success_task
            elif i%4 == 3:
                curr_target = self.wrong_payload_task

            processes.append(threading.Thread(target=curr_target))
        for i in range(threads):
            processes[i].start()
        time.sleep(100)
        self.log.info("failiures received ", self.failure_counter)
        self.assertLess(self.failure_counter, threads*0.2)