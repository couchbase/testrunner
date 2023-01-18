from pytests.serverless.serverless_basetestcase import ServerlessBaseTestCase
from lib.serverlessLib.dapi.dapi import RestfulDAPI
import json


class RestfulDAPITest(ServerlessBaseTestCase):
    def setUp(self):
        super().setUp()
        self.num_buckets = self.input.param("num_buckets", 1)
        self.create_database(self.num_buckets)

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
        self.dapi_info_list = dapi_list


    def test_dapi_health(self):
        for dapi_info in self.dapi_info_list:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": dapi_info["dapi_endpoint"],
                                          "access_token": dapi_info["access_token"],
                                          "access_secret": dapi_info["access_secret"]})

            self.log.info("Checking DAPI health for DB: {}".format(dapi_info["database_id"]))
            response = self.rest_dapi.check_dapi_health()
            self.assertTrue(response.status_code == 200,
                            "DAPI is not healthy for database: {}".format(dapi_info["database_id"]))
            self.log.info(json.loads(response.content)["health"])
            self.assertTrue(json.loads(response.content)["health"].lower() == "ok",
                            "DAPI health is not OK")
