from pytests.serverless.serverless_basetestcase import ServerlessBaseTestCase
from lib.couchbase_helper.documentgenerator import BatchedDocumentGenerator
from lib.serverlessLib.dapi.dapi import RestfulDAPI
from pytests.serverless.dapi.dapi_helper import doc_generator
import json
import threading
import time
import pytz
import datetime

class RestfulDAPITest(ServerlessBaseTestCase):
    def setUp(self):
        super().setUp()
        self.num_buckets = self.input.param("num_buckets", 1)
        self.create_database(self.num_buckets)
        self.value_size = self.input.param("value_size", 255)
        self.key_size = self.input.param("key_size", 8)
        self.number_of_docs = self.input.param("number_of_docs", 10)
        self.randomize_value = self.input.param("randomize_value", False)
        self.mixed_key = self.input.param("mixed_key", False)
        self.number_of_collections = self.input.param("number_of_collection", 10)
        self.number_of_scopes = self.input.param("number_of_scope", 10)
        self.number_of_threads = self.input.param("number_of_threads", 1)
        self.batch_size = self.input.param("batch_size",10)
        self.error_message = self.input.param("error_msg", None)
        self.mutate_type = self.input.param("mutate_type", "insert")

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
        time.sleep(60)
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

    def test_mutate_dapi_crud(self):
        self.result = True
        def mutate_dapi_thread(mutate_type, key_prefix, document_per_thread, bucket_name):
            doc_gen = doc_generator(key_prefix, self.key_size, self.value_size,
                                    document_per_thread, self.randomize_value,
                                    self.mixed_key)

            while(doc_gen.has_next()):
                key, doc = doc_gen.__next__()
                doc = json.loads(doc)
                # insert doc
                response = self.rest_dapi.insert_doc(key, doc, "_default", "_default")
                if self.error_message is not None:
                    if response is None or response.status_code != 409:
                        self.log.critical("Negative test failed with unsupported "
                                        "key/value for database: {}".format(bucket_name))
                        self.result = False
                        return
                    error_msg = json.loads(response.content)["error"]["message"]
                    if error_msg != "An unknown KV error occured":
                        self.log.critical("Wrong error msg for unsupported key/value doc: {}".format(error_msg))
                        self.result = False
                        return
                    return

                if response is None or response.status_code != 201:
                    self.result = False
                    self.log.critical(response)
                    if response is not None:
                        self.log.critical("Insert doc failed for {}:"
                                        "response: {}".format(bucket_name, response.status_code))
                    return

                # upsert doc
                if mutate_type == "upsert":
                    doc['updated'] = True
                    response = self.rest_dapi.upsert_doc(key, doc, "_default", "_default")
                    if response is None or response.status_code != 200:
                        self.resut = False
                        self.log.critical(response)
                        if response is not None:
                            self.log.critical("Upsert doc failed for {}: Response {}".format(
                                bucket_name, response.status_code))
                        return

                # delete doc
                if mutate_type == "delete":
                    response = self.rest_dapi.delete_doc(key, "_default", "_default")
                    if response is None or response.status_code != 200:
                        self.resut = False
                        self.log.critical(response)
                        if response is not None:
                            self.log.critical("delete doc failed for {}: Response {}".format(
                                bucket_name, response.status_code))
                        self.result = False
                        return

                # check doc exists
                response = self.rest_dapi.check_doc_exists(key, "_default", "_default")
                if mutate_type == "delete":
                    if response is None or response.status_code != 404:
                        self.result = False
                        self.log.critical(response)
                        self.log.critical("Check doc exists failed: {}"
                                           "for database: {}".format(bucket_name, response.status_code))
                        return
                else:
                    if response is None or response.status_code != 204:
                        self.result = False
                        self.log.critical(response)
                        self.log.critical("Check doc exists failed: {}"
                                           "for datbase: {}".format(bucket_name, response.status_code))
                        return

                # Get doc
                response = self.rest_dapi.get_doc(key, "_default", "_default")
                if mutate_type == "delete":
                    if response is None or response.status_code != 404:
                        self.result = False
                        self.log.critical(response)
                        self.log.critical("Get doc for deleted doc"
                                        "failed: {} response: {}".format(bucket_name, response.status_code))
                        return
                    continue

                if response is None or response.status_code != 200:
                    self.result = False
                    self.log.critical(response)
                    self.log.critical("Get doc failed for {}: Response {}".format(
                        bucket_name, response.status_code))
                    return

                # check retrieved doc is same with upserted doc
                doc_retrieved = list(json.loads(response.content).values())[0]
                if doc_retrieved != doc:
                    self.log.critical("value mismatch with inserted doc")
                    self.result = False
                    return

        for dapi_info in self.dapi_info_list:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": dapi_info["dapi_endpoint"],
                                            "access_token": dapi_info["access_token"],
                                            "access_secret": dapi_info["access_secret"]})

            self.log.info("{} document for database: {}".format(self.mutate_type, dapi_info["database_id"]))

            document_per_thread = self.number_of_docs // self.number_of_threads
            remaining_doc = self.number_of_docs % self.number_of_threads

            thread_list, key_prefix = list(), "key"
            for i in range(self.number_of_threads):
                key_prefix = key_prefix + str(i)
                if remaining_doc:
                    thread = threading.Thread(target=mutate_dapi_thread,
                                    args=(self.mutate_type, key_prefix,
                                          document_per_thread + 1, dapi_info["database_id"]))
                    remaining_doc -= 1
                else:
                    thread = threading.Thread(target=mutate_dapi_thread,
                                    args=(self.mutate_type, key_prefix,
                                          document_per_thread, dapi_info["database_id"]))
                thread_list.append(thread)
            for thread in thread_list:
                thread.start()

            for thread in thread_list:
                thread.join()

            self.assertTrue(self.result, "insert doc failed, check logs..............")
            #negative tests
            self.log.info("Running negative tests.........")
            # upsert a doc which is not inserted yet
            key, doc = "key", {"inserted": True}
            response = self.rest_dapi.upsert_doc(key, doc, "_default", "_default")
            self.log.info("Response code for upsertion of doc: {}".format(response.status_code))
            self.assertTrue(response.status_code == 404, "Upsert failed not inserted"
                            "doc for database: {}".format(dapi_info["database_id"]))
            # insert a already inserted doc
            response = self.rest_dapi.insert_doc(key, doc, "_default", "_default", "?upsert=true")
            self.log.info("Response code for insertion of doc: {}".format(response.status_code))
            self.assertTrue(response.status_code == 201, "Insert doc failed")
            response = self.rest_dapi.insert_doc(key, doc, "_default", "_default")
            self.log.info("Response code for insert of already inserted doc is: {}".format(response.status_code))
            self.assertTrue(response.status_code == 409, "Insert succees for already inserted doc")
            # delete unavailable doc
            key = "monkey"
            response = self.rest_dapi.delete_doc(key, "_default", "_default")
            self.log.info("response code for deletion of unavailable doc:{}".format(response.status_code))
            self.assertTrue(response.status_code == 404, "Delete doc failed for unavailable"
                            "doc for database: {}".format(dapi_info["database_id"]))


    def test_get_scopes(self):
        for dapi_info in self.dapi_info_list:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": dapi_info["dapi_endpoint"],
                                          "access_token": dapi_info["access_token"],
                                          "access_secret": dapi_info["access_secret"],
                                          "test": "scopes"})
            self.log.info("To get list of all scopes for DB: {}".format(dapi_info["database_id"]))
            self.log.info(dapi_info["dapi_endpoint"])

            scope_name , scope_suffix = "scope", 0
            scope_name_list = ["_default", "_system"]
            for i in range(self.number_of_scopes):
                scope_suffix += 1
                scope_name = "scope" + str(scope_suffix)
                scope_name_list.append(scope_name)
                response = self.rest_dapi.create_scope({"scopeName": scope_name})
                self.log.info("response for creation of scope: {}".format(response.status_code))
                self.assertTrue(response.status_code == 200,
                                "Creation of scope failed for database {}".format(dapi_info["database_id"]))

            response = self.rest_dapi.get_scope_list()
            self.log.info("status code for getting list of scope: {}".format(response.status_code))
            self.assertTrue(response.status_code == 200,
                            "Getting list of scopes failed for database {}".format(dapi_info["database_id"]))

            response_dict = json.loads(response.content)
            response_list = response_dict["scopes"]
            scope_list = []
            for scope in response_list:
                scope_list.append(scope["Name"])

            scope_list.sort()
            scope_name_list.sort()
            self.assertTrue(scope_list == scope_name_list,
                            "Wrong scopes received for database {}".format(dapi_info["database_id"]))

    def test_get_collections(self):
        for dapi_info in self.dapi_info_list:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": dapi_info["dapi_endpoint"],
                                          "access_token": dapi_info["access_token"],
                                          "access_secret": dapi_info["access_secret"]})

            self.log.info("To get list of all collections within a scope in database {}".format(dapi_info["database_id"]))
            self.log.info(dapi_info["dapi_endpoint"])

            scope = "testScope"
            response = self.rest_dapi.create_scope({"scopeName": scope})
            self.log.info(response.status_code)
            self.assertTrue(response.status_code == 200,
                            "Creation of scope failed for database {}".format(dapi_info["database_id"]))

            collection_name, collection_suffix = "collection", 0
            collection_name_list = []
            for i in range(self.number_of_collections):
                collection_suffix += 1
                collection_name = "collection" + str(collection_suffix)
                collection_name_list.append(collection_name)
                response = self.rest_dapi.create_collection(scope, {"name": collection_name})
                self.log.info("Response code for creation of collection: {}".format(response.status_code))
                self.assertTrue(response.status_code == 200,
                                "Creation of collection failed for database {}".format(dapi_info["database_id"]))

            response = self.rest_dapi.get_collection_list(scope)
            self.log.info("Response code for getting list of collection {}".format(response.status_code))

            self.assertTrue(response.status_code == 200,
                            "Getting list of collections failed for database {}".format(dapi_info["database_id"]))

            collection_list = json.loads(response.content)
            collection_list = collection_list["collections"]
            self.assertTrue(len(collection_list) == self.number_of_collections,
                            "Getting all collections failed for testscope for database {}".format(dapi_info["database_id"]))

            temp_collection_list = []
            for collection in collection_list:
                temp_collection_list.append(collection["Name"])

            temp_collection_list.sort()
            collection_name_list.sort()
            self.assertTrue(temp_collection_list == collection_name_list,
                            "Wrong collection/s received for database {}".format(dapi_info["database_id"]))

    def test_get_documents(self):
        for dapi_info in self.dapi_info_list:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": dapi_info["dapi_endpoint"],
                                          "access_token": dapi_info["access_token"],
                                          "access_secret": dapi_info["access_secret"]})

            self.log.info("Get list of all documents within a collection in database {}".format(dapi_info["database_id"]))
            self.log.info(dapi_info["dapi_endpoint"])

            length_of_list, key, content = 20, "key", "content"
            keys_inserted = list()
            key_value, content_value = 0, 0

            for i in range(length_of_list):
                key_value += 1
                content_value += 1
                key = "key" + str(key_value)
                content = "content" + str(content_value)
                keys_inserted.append(key)
                response = self.rest_dapi.insert_doc(key, {"content": content}, "_default", "_default")
                self.assertTrue(response.status_code == 201,
                                "Insertion failed for database {}".format(dapi_info["database_id"]))

            response = self.rest_dapi.get_document_list("_default", "_default")
            self.assertTrue(response.status_code == 200,
                            "Getting list of documents failed for database {}".format(dapi_info["database_id"]))

            self.log.info("Response code for getting list of documents {}".format(response.status_code))

            response_dict = json.loads(response.content)
            document_list = response_dict["docs"]
            keys_retreived = []
            for document in document_list:
                keys_retreived.append(document['id'])
            keys_retreived.sort()
            keys_inserted.sort()
            self.assertTrue(keys_inserted == keys_retreived,
                            "Wrong document received for database {}".format(dapi_info["database_id"]))

    def test_get_subdocument(self):
        for dapi_info in self.dapi_info_list:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": dapi_info["dapi_endpoint"],
                                          "access_token": dapi_info["access_token"],
                                          "access_secret": dapi_info["access_secret"]})

            self.log.info("Get list of all sub-documents within a document in database {}".format(dapi_info["database_id"]))
            self.log.info(dapi_info["dapi_endpoint"])

            response = self.rest_dapi.insert_doc("k", {"inserted": True}, "_default", "_default")
            self.log.info("Response code for insertion of doc {}".format(response.status_code))

            self.assertTrue(response.status_code == 201,
                            "Insertion failed for database {}".format(dapi_info["database_id"]))

            response = self.rest_dapi.get_doc("k", "_default", "_default")
            self.log.info("Response code to get document {}".format(response.status_code))

            self.assertTrue(response.status_code == 200,
                            "Getting document failed for database {}".format(dapi_info["database_id"]))

            response = self.rest_dapi.insert_subdoc("k",
                                                    [{"type": "insert", "path": "counter3", "value": 4},
                                                     {"type": "insert", "path": "counter1", "value": 10}],
                                                    "_default", "_default")
            self.log.info("Response code for insertion of subdocument {}".format(response.status_code))

            self.assertTrue(response.status_code == 200,
                            "Sub doc insertion failed for database {}".format(dapi_info["database_id"]))

            response = self.rest_dapi.get_subdoc("k",
                                                 [{"type": "get", "path": "counter3"}],
                                                 "_default", "_default")
            self.log.info("printing status code for getting subdoc: {}".format(response.status_code))
            if response.status_code == 200:
                self.log.info("printing response content for getting subbdoc {}".format(response.content))

            self.assertTrue(response.status_code == 200,
                            "Getting subdocs failed for database {}".format(dapi_info["database_id"]))

    def test_insert_subdocument(self):
        for dapi_info in self.dapi_info_list:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": dapi_info["dapi_endpoint"],
                                          "access_token": dapi_info["access_token"],
                                          "access_secret": dapi_info["access_secret"]})

            self.log.info("Mutating subdocument inside a document in database {}".format(dapi_info["database_id"]))
            self.log.info(dapi_info["dapi_endpoint"])

            response = self.rest_dapi.insert_doc("k", {"inserted": True}, "_default", "_default")
            self.log.info("Response code for insertion of document {}".format(response.status_code))

            self.assertTrue(response.status_code == 201,
                            "Insertion failed for database {}".format(dapi_info["database_id"]))

            response = self.rest_dapi.get_doc("k", "_default", "_default")
            self.log.info("Response code to get document {}".format(response.status_code))

            self.assertTrue(response.status_code == 200,
                            "Getting document failed for database {}".format(dapi_info["database_id"]))

            response = self.rest_dapi.insert_subdoc("k",
                                                    [{"type": "insert", "path": "counter3", "value": 4},
                                                     {"type": "insert", "path": "counter1", "value": 10}],
                                                    "_default", "_default")
            self.log.info("Response code for insertion of subdocument {}".format(response.status_code))

            self.assertTrue(response.status_code == 200,
                            "Sub doc insertion failed for database {}".format(dapi_info["database_id"]))

            response = self.rest_dapi.get_subdoc("k", [{"type": "get", "path": "counter3"}], "_default", "_default")
            self.log.info("printing response status code for getting subdoc {}".format(response.status_code))
            self.assertTrue(response.status_code == 200, "Getting subdoc failed for database {}".format(dapi_info["database_id"]))

    def test_create_scope(self):
        for dapi_info in self.dapi_info_list:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": dapi_info["dapi_endpoint"],
                                          "access_token": dapi_info["access_token"],
                                          "access_secret": dapi_info["access_secret"]})
            self.log.info("Creation of scope for database {}".format(dapi_info["database_id"]))

            # negative tests - create scopes with pre-existing scope name
            response = self.rest_dapi.create_scope({"scopeName": "_default"})
            self.log.info(response.status_code)
            self.assertTrue(response.status_code == 500, "Scope got created with pre-existing scope name")
            # negative tests - get scope detail for not existing scope
            response = self.rest_dapi.get_scope_detail("_asdfljasdf")
            self.log.info(response.status_code)
            self.assertTrue(response.status_code == 404, "Get datail of unexisting scope is successful")
            # negative with bad scope name
            response = self.rest_dapi.create_scope({"scopeName": ".......'..,,**&&&^!@#~~@!#~~!@*:::"})
            self.log.info(response.status_code)
            self.assertTrue(response.status_code == 500, "Scope got created with pre-existing scope name")
            # negative test with malformed request syntax
            response = self.rest_dapi.create_scope({"asdfasdf": "/Saurabh"})
            self.log.info(response.status_code)
            self.assertTrue(response.status_code == 500, "Scope got created with pre-existing scope name")

            scope_name, scope_suffix = "scope", 0
            scope_name_list = ["_default", "_system"]
            for i in range(self.number_of_scopes):
                scope_suffix += 1
                scope_name = "scope" + str(scope_suffix)
                scope_name_list.append(scope_name)
                response = self.rest_dapi.create_scope({"scopeName": scope_name})
                self.log.info("response for creation of scope: {}".format(response.status_code))
                self.assertTrue(response.status_code == 200,
                                "Creation of scope failed for database {}".format(dapi_info["database_id"]))

            response = self.rest_dapi.get_scope_list()
            self.log.info("status code for getting list of scope: {}".format(response.status_code))
            self.assertTrue(response.status_code == 200,
                            "Getting list of scopes failed for database {}".format(dapi_info["database_id"]))

            response_dict = json.loads(response.content)
            response_list = response_dict["scopes"]
            scope_list = []
            for scope in response_list:
                scope_list.append(scope["Name"])

            scope_list.sort()
            scope_name_list.sort()
            self.assertTrue(scope_list == scope_name_list,
                            "Wrong scopes received for database {}".format(dapi_info["database_id"]))

    def test_create_collection(self):
        for dapi_info in self.dapi_info_list:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": dapi_info["dapi_endpoint"],
                                          "access_token": dapi_info["access_token"],
                                          "access_secret": dapi_info["access_secret"]})
            self.log.info("Creation of collection for database {}".format(dapi_info["database_id"]))

            scope = "testScope"
            response = self.rest_dapi.create_scope({"scopeName": scope})
            self.log.info("Response code for creation of scope {}".format(response.status_code))
            self.assertTrue(response.status_code == 200,
                            "Creation of scope failed for database {}".format(dapi_info["database_id"]))

            # negative tests - create scopes with pre-existing scope name
            response = self.rest_dapi.create_collection(scope, {"name": "_default"})
            self.log.info(response.status_code)
            self.assertTrue(response.status_code == 500, "Collection got created with pre-existing scope name")
            # negative tests - get collection detail for not existing collection
            response = self.rest_dapi.get_collection_detail(scope, "_asdfljasdf")
            self.log.info(response.status_code)
            self.assertTrue(response.status_code == 404, "Get datail of unexisting collection is successful")
            # negative with bad collection name
            response = self.rest_dapi.create_collection(scope, {"name": ".......'..,,**&&&^!@#~~@!#~~!@*:::"})
            self.log.info(response.status_code)
            self.assertTrue(response.status_code == 500, "Scope got created with pre-existing scope name")
            # negative test with malformed request syntax
            response = self.rest_dapi.create_collection(scope, {"asdfasdf": "/Saurabh"})
            self.log.info(response.status_code)
            self.assertTrue(response.status_code == 500, "collection got created with pre-existing scope name")

            collection_name, collection_suffix = "collection", 0
            collection_name_list = []
            for i in range(self.number_of_collections):
                collection_suffix += 1
                collection_name = "collection" + str(collection_suffix)
                collection_name_list.append(collection_name)
                response = self.rest_dapi.create_collection(scope, {"name": collection_name})
                self.log.info("Response code for creation of collection: {}".format(response.status_code))
                self.assertTrue(response.status_code == 200,
                                "Creation of collection failed for database {}".format(dapi_info["database_id"]))

            response = self.rest_dapi.get_collection_list(scope)

            self.log.info("Response code for get list of collection {}".format(response.status_code))

            self.assertTrue(response.status_code == 200,
                            "Getting list of collections failed for database {}".format(dapi_info["database_id"]))

            collection_list = json.loads(response.content)
            collection_list = collection_list["collections"]

            temp_collection_list = []
            for collection in collection_list:
                temp_collection_list.append(collection["Name"])

            temp_collection_list.sort()
            collection_name_list.sort()
            self.assertTrue(temp_collection_list == collection_name_list,
                            "Wrong collection/s received for database {}".format(dapi_info["database_id"]))

    def test_delete_collection(self):
        for dapi_info in self.dapi_info_list:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": dapi_info["dapi_endpoint"],
                                          "access_token": dapi_info["access_token"],
                                          "access_secret": dapi_info["access_secret"]})
            self.log.info("Deletion of collection for database {}".format(dapi_info["database_id"]))

            scope = "_default"
            response = self.rest_dapi.delete_collection(scope, "_asdfasdfasdf__sadfasdf")
            self.log.info(response.status_code)
            self.assertTrue(response.status_code == 404, "Collection got deleted with non-existing scope name")

            collection_name = "testCollection"
            response = self.rest_dapi.create_collection("_default", {"name": collection_name})
            self.log.info("Response code for creation of collection {}".format(response.status_code))
            self.assertTrue(response.status_code == 200,
                            "Create collection for database {}".format(dapi_info["database_id"]))

            response = self.rest_dapi.delete_collection("_default", collection_name)
            self.log.info("Response code for deletion of collection {}".format(response.status_code))
            self.assertTrue(response.status_code == 200,
                            "Deletion of collection failed for database {}".format(dapi_info["database_id"]))

            response = self.rest_dapi.get_collection_list("_default")
            self.log.info("Response code to get list of collection {}".format(response.status_code))
            collection_list = json.loads(response.content)
            collection_list = collection_list["collections"]

            collection_name_list = []
            for collection in collection_list:
                collection_name_list.append(collection["Name"])

            for collection in collection_name_list:
                self.assertTrue(collection != collection_name,
                                "Getting delete collection: {} for database {}".format(collection_name, dapi_info["database_id"]))
            # negative test case
            response = self.rest_dapi.get_document_list("_default", collection_name)
            self.log.info("Response code to get list of documents {}".format(response.status_code))
            self.assertTrue(response.status_code == 404,
                            "Getting empty list for deleted collection for database {}".format(dapi_info["database_id"]))

    def test_delete_scope(self):
        for dapi_info in self.dapi_info_list:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": dapi_info["dapi_endpoint"],
                                          "access_token": dapi_info["access_token"],
                                          "access_secret": dapi_info["access_secret"]})
            self.log.info("Deletion of scope for database {}".format(dapi_info["database_id"]))

            # negative tests - create scopes with pre-existing scope name
            response = self.rest_dapi.delete_scope("_Dafasdfasdfasd_asdfasdf")
            self.log.info(response.status_code)
            self.assertTrue(response.status_code == 404, "Scope got deleted with non-existing scope name")

            scope_name = "testScope"
            response = self.rest_dapi.create_scope({"scopeName": scope_name})
            self.log.info("Response code for creation of scope {}".format(response.status_code))
            self.assertTrue(response.status_code == 200,
                            "Create scope {} failed for database {}".format(scope_name, dapi_info["database_id"]))

            response = self.rest_dapi.delete_scope(scope_name)
            self.log.info("Response code for deletion of scope {}".format(response.status_code))
            self.assertTrue(response.status_code == 200,
                            "Deletion of scope {} failed for database {}".format(scope_name, dapi_info["database_id"]))

            response = self.rest_dapi.get_scope_list()
            self.log.info("Response code to get list of scope {}".format(response.status_code))
            scope_list = json.loads(response.content)
            scope_list = scope_list["scopes"]
            scope_name_list = []
            for scope in scope_list:
                scope_name_list.append(scope["Name"])

            for scope in scope_name_list:
                self.assertTrue(scope != scope_name,
                                "Getting deleted scope: {} for database {}".format(scope_name, dapi_info["database_id"]))
            # negative test case
            response = self.rest_dapi.get_collection_list(scope_name)
            self.log.info("response code for getting collections for deleted scope {}".format(response.status_code))
            self.assertTrue(response.status_code == 404,
                            "Getting empty list for deleted scope for database {}".format(dapi_info["database_id"]))

    def test_execute_query(self):
        for dapi_info in self.dapi_info_list:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": dapi_info["dapi_endpoint"],
                                          "access_token": dapi_info["access_token"],
                                          "access_secret": dapi_info["access_secret"]})
            self.log.info("Execute query for database {}".format(dapi_info["database_id"]))

            # create collection query
            query = {"query": "CREATE COLLECTION country"}
            response = self.rest_dapi.execute_query(query, "_default")
            self.log.info("Response code for create collection query: {}".format(response.status_code))
            self.assertTrue(response.status_code == 200,
                            "Create collection query failed for database {}".format(dapi_info["database_id"]))
            response = self.rest_dapi.get_collection_list("_default")
            self.assertTrue(response.status_code == 200,
                            "Get list of collection failed for database {}".format(dapi_info["database_id"]))
            if "country" not in [collection["Name"] for collection in
                                    (json.loads(response.content)["collections"])]:
                self.log.critical("Collection does not get created with query")
            # Insert query
            query = {"query": 'INSERT INTO _default '
                              '(KEY, VALUE) VALUES ("key1", { "type" : "hotel", "name" : "new hotel" });'}
            response = self.rest_dapi.execute_query(query, "_default")
            self.log.info("Response code for execution of query {}".format(response.status_code))
            self.assertTrue(response.status_code == 200,
                            "Query Execution failed for database {}".format(dapi_info["database_id"]))
            response = self.rest_dapi.get_doc("key1", "_default", "_default",)
            self.assertTrue(response.status_code == 200,
                            "Get document failed for database {}".format(dapi_info["database_id"]))

            # select query
            query = {"query": "SELECT type, name FROM _default"}
            response = self.rest_dapi.execute_query(query, "_default")
            self.log.info("Response status code for execute query: {}".format(response.status_code))
            self.assertTrue(response.status_code == 200, "Query execution failed for database {}".format(dapi_info["database_id"]))
            # create primary index
            query = {"query": "CREATE PRIMARY INDEX idx_default_primary ON `_default` USING GSI"}
            response = self.rest_dapi.execute_query(query, "_default")
            self.log.info("Response code for creation of primary index: {}".format(response.status_code))
            # update query
            query = {"query": "UPDATE _default SET type = 'hostel' where name = 'new hostel'"}
            response = self.rest_dapi.execute_query(query, "_default")
            self.log.info("Response code for update query: {}".format(response.status_code))
            self.assertTrue(response.status_code == 200,
                            "Update query failed for database {}".format(dapi_info["database_id"]))
            # delete query
            query = {"query": "DELETE from _default"}
            response = self.rest_dapi.execute_query(query, "_default")
            self.log.info("Response code for delete doc query: {}".format(response.status_code))
            self.assertTrue(response.status_code == 200,
                            "Delete query failed for database {}".format(dapi_info["database_id"]))
            response = self.rest_dapi.get_doc("key1", "_default", "_default")
            self.log.info("Response code to get deleted doc: {}".format(response.status_code))
            self.assertTrue(response.status_code == 404,
                            "Getting deleted document for database {}".format(dapi_info["database_id"]))
            # drop collection query
            query = {"query": "DROP COLLECTION country"}
            response = self.rest_dapi.execute_query(query, "_default")
            self.log.info("Response code for drop of collection: {}".format(response.status_code))
            self.assertTrue(response.status_code == 200,
                            "Drop collection query failed for database {}".format(dapi_info["database_id"]))
            response = self.rest_dapi.get_collection_list("_default")
            self.assertTrue(response.status_code == 200,
                            "Get list of collection failed for database {}".format(dapi_info["database_id"]))
            if "country" in [collection["Name"] for collection in
                                    (json.loads(response.content)["collections"])]:
                self.log.critical("Drop collection query failed for database {}".format(dapi_info["database_id"]))
            # Invalid Query
            query = {"query": "SLECT city, body FROM _default WHERE age=$age LIMIT 5",
                     "parameters": {"age": 5}}
            response = self.rest_dapi.execute_query(query, "_default")
            self.log.info("Response code for execution of invalid query: {}".format(response.status_code))
            self.assertTrue(response.status_code == 400,
                            "Query Execution passed with invalid query for database: {}".format(dapi_info["database_id"]))
            val = json.loads(response.content)["error"]["message"]
            self.log.info(val)


    def test_get_bulk_doc(self):
        self.result = True

        def bulk_get_thread(key_prefix, document_per_thread, bucket_name):

            doc_gen = doc_generator(key_prefix, self.key_size, self.value_size,
                                    document_per_thread, self.randomize_value,
                                    self.mixed_key)

            batched_gen_obj = BatchedDocumentGenerator(doc_gen, self.batch_size)
            # insertion of documents
            while(batched_gen_obj.has_next()):
                kv_dapi = []
                kv = {}
                next_batch = batched_gen_obj.next_batch()
                for key in next_batch:
                    doc = next_batch[key]
                    doc = json.loads(doc)
                    kv[key] = doc
                    kv_dapi.append({"id": key, "value": doc})

                # insert bulk document
                response = self.rest_dapi.insert_bulk_document("_default",
                                                               "_default",
                                                               kv_dapi)
                if response is None or response.status_code != 200:
                    self.result = False
                    self.log.critical(response)
                    self.log.critical("Bulk insert failed for {}: Response: {}".format(
                        bucket_name, response.status_code))
                    return
                # bulk keys get in response after insertion of bulk document
                keys = json.loads(response.content).get("docs", [])
                # compare both the list
                if [key["id"] for key in keys].sort() != [kv.keys()].sort():
                    self.log.critical("Ambiguous keys get inserted for database {}".format(bucket_name))
                    self.log.critical("Response: ".format(response))
                    self.result = False
                    return

                # Get bulk document
                response = self.rest_dapi.get_bulk_document("_default", "_default", tuple(kv.keys()))
                if response is None or response.status_code != 200:
                    self.result = False
                    self.log.critical("Response: ".format(response))
                    self.log.critical("Bulk get failed for {}: Response: ".format(
                        bucket_name, response.status_code))
                    return
                # list of key document pair get from response
                bulk_key_document = json.loads(response.content).get("docs", [])

                for key_doc in bulk_key_document:
                    if key_doc.get("doc") != kv.get(key_doc["id"]):
                        self.log.critical("{}: Value mismatch for key: "
                                          "{}. Actual {}, Expected {}".
                                          format(bucket_name, key,
                                                 key_doc.get("doc"),
                                                 kv.get(key_doc["id"])))
                        self.log.critical("Response: ".format(response))
                        self.result = False
                        return

        for dapi_info in self.dapi_info_list:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": dapi_info["dapi_endpoint"],
                                          "access_token": dapi_info["access_token"],
                                          "access_secret": dapi_info["access_secret"]})
            self.log.info("Insert bulk document for database {}".format(dapi_info["database_id"]))

            document_per_thread = self.number_of_docs // self.number_of_threads
            remaining_doc = self.number_of_docs % self.number_of_threads

            thread_list, key_prefix = [], "key"
            for i in range(self.number_of_threads):
                key_prefix = key_prefix + str(i)
                if remaining_doc:
                    thread = threading.Thread(target=bulk_get_thread,
                                              args=(key_prefix, document_per_thread + 1,
                                                    dapi_info["database_id"]))
                    remaining_doc =- 1
                else:
                    thread = threading.Thread(target=bulk_get_thread,
                                              args=(key_prefix, document_per_thread,
                                                    dapi_info["database_id"]))
                thread_list.append(thread)

            for thread in thread_list:
                thread.start()

            for thread in thread_list:
                thread.join()

            self.assertTrue(self.result, "Check the test logs...")


    def test_delete_bulk_doc(self):
        self.result = True

        def bulk_delete_thread(key_prefix, document_per_thread, bucket_name):

            # insertion of documents
            doc_gen = doc_generator(key_prefix, self.key_size, self.value_size,
                                    document_per_thread, self.randomize_value,
                                    self.mixed_key)

            batched_gen_obj = BatchedDocumentGenerator(doc_gen, self.batch_size)
            # insertion of documents
            while(batched_gen_obj.has_next()):
                kv_dapi = []
                kv = {}
                next_batch = batched_gen_obj.next_batch()
                for key in next_batch:
                    doc = next_batch[key]
                    doc = json.loads(doc)
                    kv[key] = doc
                    kv_dapi.append({"id": key, "value": doc})

                # insert bulk document
                response = self.rest_dapi.insert_bulk_document("_default",
                                                               "_default",
                                                               kv_dapi)
                if response is None or response.status_code != 200:
                    self.result = False
                    self.log.critical(response)
                    self.log.critical("Bulk insert failed for {}: Response: {}".format(
                        bucket_name, response.status_code))
                    return
                # bulk keys get in response after insertion of bulk document
                keys = json.loads(response.content).get("docs", [])
                # compare both the list
                if [key["id"] for key in keys].sort() != [kv.keys()].sort():
                    self.log.critical("Ambiguous keys get inserted for database {}".format(bucket_name))
                    self.log.critical("Response: ".format(response))
                    self.result = False
                    return

                # Delete bulk document
                response = self.rest_dapi.delete_bulk_document("_default",
                                                               "_default",
                                                               tuple(kv.keys()))

                if response is None or response.status_code != 200:
                    self.result = False
                    self.log.critical(response)
                    self.log.critical("Bulk deletion failed for {}: Response: {}".format(
                        bucket_name, response.status_code))
                    return

                keys = json.loads(response.content).get("docs", [])

                if [key["id"] for key in keys].sort() != [kv.keys()].sort():
                    self.log.critical("Ambiguous keys get deleted for database {}".format(bucket_name))
                    self.log.critical("Response: ".format(response))
                    self.result = False
                    return

        for dapi_info in self.dapi_info_list:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": dapi_info["dapi_endpoint"],
                                          "access_token": dapi_info["access_token"],
                                          "access_secret": dapi_info["access_secret"]})
            self.log.info("Delete bulk document for database {}".format(dapi_info["database_id"]))

            document_per_thread = self.number_of_docs // self.number_of_threads
            remaining_doc = self.number_of_docs % self.number_of_threads

            thread_list, key_prefix = [], "key"
            for i in range(self.number_of_threads):
                key_prefix = key_prefix + str(i)
                if remaining_doc:
                    thread = threading.Thread(target=bulk_delete_thread,
                                              args=(key_prefix, document_per_thread + 1,
                                                    dapi_info["database_id"]))
                    remaining_doc =- 1
                else:
                    thread = threading.Thread(target=bulk_delete_thread,
                                              args=(key_prefix, document_per_thread,
                                                    dapi_info["database_id"]))
                thread_list.append(thread)

            for thread in thread_list:
                thread.start()

            for thread in thread_list:
                thread.join()

            self.assertTrue(self.result, "Check the test logs...")


    def test_update_bulk_doc(self):
        self.result = True

        def bulk_upsert_thread(key_prefix, document_per_thread, bucket_name):

            # insertion of documents
            doc_gen = doc_generator(key_prefix, self.key_size, self.value_size,
                                    document_per_thread, self.randomize_value,
                                    self.mixed_key)

            batched_gen_obj = BatchedDocumentGenerator(doc_gen, self.batch_size)
            # insertion of documents
            while(batched_gen_obj.has_next()):
                kv_dapi = []
                kv = {}
                next_batch = batched_gen_obj.next_batch()
                for key in next_batch:
                    doc = next_batch[key]
                    doc = json.loads(doc)
                    kv[key] = doc
                    kv_dapi.append({"id": key, "value": doc})

                # insert bulk document
                response = self.rest_dapi.insert_bulk_document("_default",
                                                               "_default",
                                                               kv_dapi)
                if response is None or response.status_code != 200:
                    self.result = False
                    self.log.critical(response)
                    self.log.critical("Bulk insert failed for {}: Response: {}".format(
                        bucket_name, response.status_code))
                    return
                # bulk keys get in response after insertion of bulk document
                keys = json.loads(response.content).get("docs", [])
                # compare both the list
                if [key["id"] for key in keys].sort() != [kv.keys()].sort():
                    self.log.critical("Ambiguous keys get inserted for database {}".format(bucket_name))
                    self.log.critical("Response: ".format(response))
                    self.result = False
                    return

                # updation of kv_dapi
                for key_value in kv_dapi:
                    key_value['value']['update'] = True

                # update bulk document
                response = self.rest_dapi.update_bulk_document("_default",
                                                               "_default",
                                                               kv_dapi)
                if response is None or response.status_code != 200:
                    self.result = False
                    self.log.critical(response)
                    self.log.critical("Bulk update failed for {}: Response: {}".format(
                        bucket_name, response.status_code))
                    return

                # bulk keys get in response after insertion of bulk document
                keys = json.loads(response.content).get("docs", [])
                # compare both the list
                if [key["id"] for key in keys].sort() != [kv.keys()].sort():
                    self.log.critical("Ambiguous keys get updated for database {}".format(bucket_name))
                    self.log.critical("Response: ".format(response))
                    self.result = False
                    return

        for dapi_info in self.dapi_info_list:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": dapi_info["dapi_endpoint"],
                                          "access_token": dapi_info["access_token"],
                                          "access_secret": dapi_info["access_secret"]})
            self.log.info("Upsert bulk document for database {}".format(dapi_info["database_id"]))

            document_per_thread = self.number_of_docs // self.number_of_threads
            remaining_doc = self.number_of_docs % self.number_of_threads

            thread_list, key_prefix = [], "key"
            for i in range(self.number_of_threads):
                key_prefix = key_prefix + str(i)
                if remaining_doc:
                    thread = threading.Thread(target=bulk_upsert_thread,
                                              args=(key_prefix, document_per_thread + 1,
                                                    dapi_info["database_id"]))
                    remaining_doc =- 1
                else:
                    thread = threading.Thread(target=bulk_upsert_thread,
                                              args=(key_prefix, document_per_thread,
                                                    dapi_info["database_id"]))
                thread_list.append(thread)

            for thread in thread_list:
                thread.start()

            for thread in thread_list:
                thread.join()

            self.assertTrue(self.result, "Check the test logs...")


    def test_get_bucket_list(self):
        for dapi_info in self.dapi_info_list:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": dapi_info["dapi_endpoint"],
                                          "access_token": dapi_info["access_token"],
                                          "access_secret": dapi_info["access_secret"]})
            self.log.info("GET bucket info for database: {}".format(dapi_info["database_id"]))

            response = self.rest_dapi.get_bucket_info(dapi_info["database_id"])
            self.assertTrue(response.status_code == 200,
                            "GET bucket info failed for database: {}".format(dapi_info["database_id"]))

            bucket = json.loads(response.content)['bucket']

            bucket_list = list()
            bucket_list.append(bucket)

            self.assertTrue(bucket['Name'] == dapi_info["database_id"],
                            "wrong database received for database: {}".format(dapi_info["database_id"]))

            self.assertTrue(response.status_code == 200,
                            "get bucket info failed for database: {}".format(dapi_info["database_id"]))

            response = self.rest_dapi.get_bucket_list()
            self.assertTrue(response.status_code == 200,
                            "Get list of buckets failed for database: {}".format(dapi_info["database_id"]))
            bucket_list_retrieved = json.loads(response.content)['buckets']

            self.assertTrue(bucket_list_retrieved == bucket_list,
                            "Wrong bucket list retrieved for database: {}".format(dapi_info["database_id"]))


    def test_million_doc_mutate(self):
        self.result = True
        self.counter = 0
        def doc_mutate_thread(lock, key_prefix, document_per_thread, database):

            doc_gen = doc_generator(key_prefix, self.key_size, self.value_size,
                                    document_per_thread, self.randomize_value,
                                    self.mixed_key)

            batched_gen_obj = BatchedDocumentGenerator(doc_gen, self.batch_size)

            while(batched_gen_obj.has_next()):
                kv_dapi = []
                kv = {}
                next_batch = batched_gen_obj.next_batch()
                for key in next_batch:
                    doc = next_batch[key]
                    doc = json.loads(doc)
                    kv[key] = doc
                    kv_dapi.append({"id": key, "value": doc})

                # insert bulk document
                response = self.rest_dapi.insert_bulk_document("_default",
                                                               "_default",
                                                               kv_dapi)

                if response is None or response.status_code != 200:
                    self.result = False
                    self.log.critical(response)
                    self.log.critical("Bulk insert failed for {}: Response: {}".format(database.id, response.status_code))
                    return

            doc_gen.reset()
            batched_gen_obj = BatchedDocumentGenerator(doc_gen, self.batch_size)

            while(batched_gen_obj.has_next()):
                kv = {}
                next_batch = batched_gen_obj.next_batch()

                for key in next_batch:
                    doc = next_batch[key]
                    doc = json.loads(doc)
                    kv[key] = doc

                response = self.rest_dapi.get_bulk_document("_default", "_default", tuple(kv.keys()))
                if response is None or response.status_code != 200:
                    self.result = False
                    self.log.critical("Response: ".format(response))
                    self.log.critical("Bulk get failed for {}: Response: ".format(
                        database.id, response.status_code))
                    return

                bulk_key_document = json.loads(response.content).get("docs", [])

                for key_doc in bulk_key_document:
                    if key_doc.get("doc") != kv.get(key_doc["id"]):
                        self.log.critical("{}: Value mismatch for key: "
                                          "{}. Actual {}, Expected {}".
                                          format(database.id, key,
                                                 key_doc.get("doc"),
                                                 kv.get(key_doc["id"])))
                        self.log.critical("Response: ".format(response))
                        self.result = False
                        return


        for database in self.databases.values():

            self.rest_dapi = RestfulDAPI({"dapi_endpoint": database.data_api,
                                        "access_token": database.access_key,
                                        "access_secret": database.secret_key})


            self.log.info("Insert bulk document for database {}".format(database.id))

            document_per_thread = self.number_of_docs // self.number_of_threads
            remaining_doc = self.number_of_docs % self.number_of_threads
            response = self.rest_dapi.get_bucket_info(database.id)
            self.log.info(response.content)
            thread_list, key_prefix = [], "key"
            test_start = time.perf_counter()
            for i in range(self.number_of_threads):
                key_prefix = key_prefix + str(i)
                lock = threading.Lock()
                if remaining_doc:
                    self.log.info("have remaining doc")
                    thread = threading.Thread(target=doc_mutate_thread, args=(lock, key_prefix, document_per_thread + 1,database))
                    remaining_doc =- 1
                else:
                    thread = threading.Thread(target=doc_mutate_thread,
                                            args=(lock, key_prefix, document_per_thread,database))
                thread_list.append(thread)

            for thread in thread_list:
                thread.start()

            for thread in thread_list:
                thread.join()

            test_end = time.perf_counter()

            self.log.info("Time took to complete test: {}".format((test_end - test_start)))

            self.assertTrue(self.result, "Check the test logs...")


    def test_crud_drop_bucket(self):
        self.result = True

        def test_crud_thread():
            response = self.rest_dapi.insert_doc("key1", {"inserted": True}, "_default", "_default")
            if response is None or response.status_code != 500:
                self.result = False
                self.log.critical(response)
                self.log.critical("Ambiguous response for insertion of doc "
                                  "while drop bucket: {}".format(response.status_code))
                return

            response = self.rest_dapi.get_doc("key1", "_default", "_default")
            if response is None or response.status_code != 500:
                self.result = False
                self.log.critical(response)
                self.log.critical("Ambiguous response for getting doc "
                                  "while drop bucket: {}".format(response.status_code))
                return

            response = self.rest_dapi.upsert_doc("key1", {"updated": True}, "_default", "_default")
            if response is None or response.status_code != 500:
                self.result = False
                self.log.critical(response)
                self.log.critical("Ambiguous response for updation of  doc "
                                  "while drop bucket: {}".format(response.status_code))
                return

            response = self.rest_dapi.delete_doc("key1",  "_default", "_default")

            if response is None or response.status_code != 500:
                self.result = False
                self.log.critical(response)
                self.log.critical("Ambiguous response for deletion of doc "
                                  "while drop bucket: {}".format(response.status_code))
                return

        def drop_bucket_thread():
            self.tearDown()

        for database in self.databases.values():

            self.rest_dapi = RestfulDAPI({"dapi_endpoint": database.data_api,
                                        "access_token": database.access_key,
                                        "access_secret": database.secret_key})

            self.log.info("Test crud and drop bucket simultaneously for database {}".format(database.id))

            crud_thread = threading.Thread(target=test_crud_thread)
            bucket_drop_thread = threading.Thread(target=drop_bucket_thread)

            bucket_drop_thread.start()
            bucket_drop_thread.join()

            crud_thread.start()
            crud_thread.join()

            self.assertTrue(self.result, "Check the test logs...")


    def test_query_parameters_get_docs(self):
        for database in self.databases.values():
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": database.data_api,
                                        "access_token": database.access_key,
                                        "access_secret": database.secret_key})

            # insert doc
            doc = dict()
            doc['name'] = "a" * 1024 * 1024 * 10
            response = self.rest_dapi.insert_doc("key", doc, "_default", "_default")
            self.assertTrue(response.status_code == 201, "Insert doc failed for database: {}".format(database.id))
            # test timeout parameter
            response = self.rest_dapi.get_doc("key", "_default", "_default", "?timeout=0.00001s&meta=true")
            self.assertTrue(response.status_code == 200, "Get doc failed with timeout parameter")
            error_msg = json.loads(response.content)["error"]["message"]
            self.assertTrue(error_msg == self.error_message, "Got wrong error message for timeout parametere: {}".format(error_msg))
            # test meta parameter
            response = self.rest_dapi.get_doc("key", "_default", "_default", "?meta=true")
            self.assertTrue(response.status_code == 200, "Get doc failed for database: {}".format(database.id))
            content = json.loads(response.content)
            flag = True
            if 'meta' not in content:
                flag = False
                self.log.critical("Not getting meta data for Get doc api: {}".format(database.id))

            self.assertTrue(flag == True, "Get meta data failed for GET doc api for database: {}".format(database.id))

            # replica is still pending

    def test_query_parameter_check_doc(self):
        for database in self.databases.values():
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": database.data_api,
                                        "access_token": database.access_key,
                                        "access_secret": database.secret_key})
            self.log.info("test query parameters for check doc exists api for database: ".format(database.id))

            #insert doc
            doc = dict()
            doc['name'] = "a" * 1024 * 1024 * 19
            response = self.rest_dapi.insert_doc("key", doc, "_default", "_default")
            self.assertTrue(response.status_code == 201, "Insert doc failed for database: {}".format(database.id))
            # test timeout
            response = self.rest_dapi.check_doc_exists("key", "_default", "_default", "?timeout=0.000001s&meta=true")
            self.assertTrue(response.status_code == 200, "Check doc failed with timeout parameter")
            # error_msg = json.loads(response.content)["error"]["message"]
            # self.assertTrue(error_msg == self.error_message, "Got wrong error message for timeout parametere: {}".format(error_msg))


    def test_query_paramter_insert_doc(self):
        for database in self.databases.values():
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": database.data_api,
                                        "access_token": database.access_key,
                                        "access_secret": database.secret_key})
            self.log.info("Test query parameters for insert doc api for database: {}".format(database.id))

            # insert doc with parameters
            doc = dict()
            doc['name'] = "a" * 1024 * 1024 * 19
            response = self.rest_dapi.insert_doc("key", doc, "_default", "_default",
                                                "?timeout=0.001s&meta=true")
            self.log.info("Response code for insetion of doc: {}".format(response.status_code))
            self.assertTrue(response.status_code == 200, "Insert doc failed with timeout parameter")
            error_msg = json.loads(response.content)["error"]["message"]
            self.assertTrue(error_msg == self.error_message, "Got wrong error message for timeout parametere: {}".format(error_msg))


            # test expiry
            response = self.rest_dapi.insert_doc("key1", {"inserted": True}, "_default", "_default", "?expiry=60s&meta=true")
            self.assertTrue(response.status_code == 201, "Insert doc failed for database: {}".format(database.id))
            response = self.rest_dapi.get_doc("key1", "_default", "_default", "?meta=true&withExpiry=true")
            self.assertTrue(response.status_code == 200, "Get doc failed for database: {}".format(database.id))
            expiryTime = json.loads(response.content)['meta']["api"]["expiryTime"]
            expiryTime = datetime.datetime.strptime(expiryTime, '%Y-%m-%dT%H:%M:%SZ')
            expiryTime = expiryTime.strftime("%Y-%m-%d %H:%M:%S")

            UTC = pytz.utc
            now = datetime.datetime.now(UTC)
            now = now.strftime("%Y-%m-%d %H:%M:%S")
            self.assertTrue(now < expiryTime, "Unexpected expiry time for database: {}".format(database.id))

            time.sleep(70)
            response = self.rest_dapi.get_doc("key1", "_default", "_default")
            self.assertTrue(response.status_code == 404, "Get for expired doc is"
                            "successful for database: {}".format(database.id))

            # test preserveExpiry parameter
            response = self.rest_dapi.insert_doc("key1", {"inserted": True}, "_default", "_default",
                                                 "?expiry=60s&meta=true")
            self.assertTrue(response.status_code == 201, "Insert doc failed for database: {}".format(database.id))
            response = self.rest_dapi.upsert_doc("key1", {"upserted": True}, "_default", "_default",
                                                 "?preserveExpiry=true")
            self.log.info("Response code for upsertion of document is: {}".format(response.status_code))
            self.assertTrue(response.status_code == 200, "Upsert doc with preserve expiry query"
                            "parameter failed for database: {}".format(database.id))
            time.sleep(60)
            response = self.rest_dapi.get_doc("key1", "_default", "_default")
            self.log.info("response code for get doc: {}".format(response.status_code))
            self.assertTrue(response.status_code == 404, "Doc is still present after putting expiry time and"
                            "keeping preserver expiry for database: {}".format(database.id))
            # durability is pending


    def test_query_parameter_upsert_doc(self):
        for database in self.databases.values():
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": database.data_api,
                                        "access_token": database.access_key,
                                        "access_secret": database.secret_key})
            self.log.info("Test query parameters for upsert doc for database: {}".format(database.id))

            # insert a doc
            doc = dict()
            doc['age'] = 10
            temp_doc = doc.copy()
            temp_doc['name'] = "a" * 1024 * 1024 * 19
            response = self.rest_dapi.insert_doc("key", doc, "_default", "_default")
            self.assertTrue(response.status_code == 201, "Insert doc failed for database: {}".format(database.id))

            # test timeout for upsert
            response = self.rest_dapi.upsert_doc("key", temp_doc, "_default", "_default",
                                                 "?timeout=0.0001s&meta=true")

            self.assertTrue(response.status_code == 200, "Upsert doc failed for database: {}".format(database.id))
            error_msg = json.loads(response.content)["error"]["message"]
            self.assertTrue(error_msg == self.error_message, "Got wrong error message for timeout parametere: {}".format(error_msg))

            # test expiry
            temp_doc['upserted'] = True
            response = self.rest_dapi.upsert_doc("key", temp_doc, "_default", "_default", "?expiry=60s&meta=true")
            self.assertTrue(response.status_code == 200, "Upsert doc failed for database: {}".format(database.id))
            response = self.rest_dapi.get_doc("key", "_default", "_default", "?meta=true&withExpiry=true")
            self.assertTrue(response.status_code == 200, "Get doc failed for database: {}".format(database.id))
            expiryTime = json.loads(response.content)['meta']["api"]["expiryTime"]
            expiryTime = datetime.datetime.strptime(expiryTime, '%Y-%m-%dT%H:%M:%SZ')
            expiryTime = expiryTime.strftime("%Y-%m-%d %H:%M:%S")

            UTC = pytz.utc
            now = datetime.datetime.now(UTC)
            now = now.strftime("%Y-%m-%d %H:%M:%S")
            self.assertTrue(now < expiryTime, "Unexpected expiry time for database: {}".format(database.id))

            # sleep for 60 second so that doc get expired

            time.sleep(60)
            response = self.rest_dapi.get_doc("key", "_default", "_default")
            self.assertTrue(response.status_code == 404, "doc is still present after the"
                            "expiry time for database: {}".format(database.id))

            # test preserve expiry

            response = self.rest_dapi.insert_doc("key", {"inserted": True}, "_default", "_default", "?expiry=60s")
            self.assertTrue(response.status_code == 201, "Insert doc failed for database: {}".format(database.id))

            response = self.rest_dapi.upsert_doc("key", {"upserted": True}, "_default", "_default", "?preserveExpiry=true")
            self.assertTrue(response.status_code == 200, "Upsert doc failed for database: {}".format(database.id))

            time.sleep(60)
            response = self.rest_dapi.get_doc("key", "_default", "_default")
            self.assertTrue(response.status_code == 404, "Doc is still present after adding expiry time of 60s")

            # test locktime parameter
            doc = dict()
            doc["inserted"] = True
            response = self.rest_dapi.insert_doc("key1", doc, "_default", "_default")
            self.assertTrue(response.status_code == 201, "Insert doc failed for database: {}".format(response.status_code))

            response = self.rest_dapi.upsert_doc("key1", doc, "_default", "_default", "?lockTime=60s")

            temp_doc = doc.copy()
            temp_doc["upserted"] = True

            response = self.rest_dapi.upsert_doc("key1", temp_doc, "_default", "_default")
            self.assertTrue(response.status_code == 200, "Upsertion of doc failed for database: {}".format(database.id))

            response = self.rest_dapi.get_doc("key1", "_default", "_default")
            self.assertTrue(response.status_code == 200, "Get doc failed for database: {}".format(database.id))
            doc_retrieved = list(json.loads(response.content).values())[0]
            self.assertTrue(doc_retrieved == doc, "doc is getting upaded instead putting locktime parameter of 60 seconds")

            time.sleep(70)

            response = self.rest_dapi.upsert_doc("key1", temp_doc, "_default", "_default")
            self.assertTrue(response.status_code == 200, "Upsert of doc failed for database: {}".format(database.id))

            response = self.rest_dapi.get_doc("key1", "_default", "_default")
            self.assertTrue(response.status_code == 200, "Get doc failed for database: {}".format(database.id))
            doc_retrieved = list(json.loads(response.content).values())[0]
            self.assertTrue(doc_retrieved == temp_doc, "doc is not getting upaded after the locktime is over")

            #test duruability and consistency parameter is still pending

    def test_query_paramete_delete_doc(self):
        for database in self.databases.values():
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": database.data_api,
                                        "access_token": database.access_key,
                                        "access_secret": database.secret_key})
            self.log.info("Test query parameters for delete doc api for database: {}".format(database.id))

            response = self.rest_dapi.insert_doc("key", {"inserted": True}, "_default", "_default")
            self.assertTrue(response.status_code == 201, "Insert doc failed for databbase: {}".format(database.id))

            response = self.rest_dapi.delete_doc("key", "_default", "_default", "?timeout=0.00000001s&meta=true")

            self.assertTrue(response.status_code == 200, "Delete doc failed for database: {}".format(database.id))
            error_msg = json.loads(response.content)["error"]["message"]
            self.assertTrue(error_msg == self.error_message, "Got wrong error message for timeout parametere: {}".format(error_msg))
            # durability and consistency parameter is pending


    def test_query_parameter_max_page_size(self):
        for database in self.databases.values():
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": database.data_api,
                                        "access_token": database.access_key,
                                        "access_secret": database.secret_key})

            self.log.info("Test query parametere max page size ie 10000 for database: {}".format(database.id))
            self.log.info("Batch size is: {}".format(self.batch_size))
            doc_gen = doc_generator("key", self.key_size, self.value_size,
                                    self.number_of_docs, self.randomize_value,
                                    self.mixed_key)
            batched_gen_obj = BatchedDocumentGenerator(doc_gen, self.batch_size)
            while batched_gen_obj.has_next():
                kv_dapi = []
                next_batch = batched_gen_obj.next_batch()
                for key in next_batch:
                    doc = next_batch[key]
                    doc = json.loads(doc)
                    kv_dapi.append({"id": key, "value": doc})

                response = self.rest_dapi.insert_bulk_document("_default", "_default", kv_dapi)
                self.log.info("Response code for bulk insert doc is: {}".format(response.status_code))

            response = self.rest_dapi.get_document_list("_default", "_default", "?pagesize=10001")
            self.assertTrue(response.status_code == 500, "Negative test for pagesize greater than 10000 failed")
            error_msg = json.loads(response.content)["error"]["message"]
            self.assertTrue(self.error_message == error_msg, "Ambigous error message got for pagesize greater than 10000")

            response = self.rest_dapi.get_document_list("_default", "_default", "?limit=10001")
            self.assertTrue(response.status_code == 500, "Negative test for limit greater than 10000 failed")
            error_msg = json.loads(response.content)["error"]["message"]
            self.assertTrue(self.error_message == error_msg, "Ambigous error message got for limit greater than 10000")


    def test_query_parameter_get_doc_list(self):
        for database in self.databases.values():
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": database.data_api,
                                        "access_token": database.access_key,
                                        "access_secret": database.secret_key})

            self.log.info("Test query parameters for bulk doc get api for database: {}".format(database.id))

            self.log.info("Batch size is: {}".format(self.batch_size))
            doc_gen = doc_generator("key", self.key_size, self.value_size,
                                    self.number_of_docs, self.randomize_value,
                                    self.mixed_key)
            batched_gen_obj = BatchedDocumentGenerator(doc_gen, self.batch_size)
            key_doc = dict()
            while batched_gen_obj.has_next():
                kv_dapi = []
                next_batch = batched_gen_obj.next_batch()
                for key in next_batch:
                    doc = next_batch[key]
                    doc = json.loads(doc)
                    key_doc[key] = doc
                    kv_dapi.append({"id": key, "value": doc})

                response = self.rest_dapi.insert_bulk_document("_default", "_default", kv_dapi)
                self.log.info("Response code for bulk insert doc is: {}".format(response.status_code))

            # test very less timeout parameter
            response = self.rest_dapi.get_document_list("_default", "_default", "?timeout=0.05s&meta=true")
            self.assertTrue(response.status_code == 400, "Get doc list with less timeout parameter for database: {}".format(database.id))
            error_msg = json.loads(response.content)["error"]["message"]
            self.assertTrue(error_msg == self.error_message, "Wrong error message received")

            # test pagesize=0 parameter
            response = self.rest_dapi.get_document_list("_default", "_default", "?pagesize=0&pretty=true")
            self.assertTrue(response.status_code == 200, "Get document list failef with pagesize=0 parametere: {}".format(response.status_code))
            doc_list = json.loads(response.content)["docs"]
            keys_inserted = list(key_doc.keys())
            keys_retrieved = list(doc["id"] for doc in doc_list)
            self.assertTrue(len(keys_retrieved) == 20, "Get doc list does not have length equals 20")
            validate_keys = next((keys for keys in keys_retrieved if keys not in keys_inserted), True)
            self.assertTrue(validate_keys == True, "Get unambigous keys while fetching list of docs"
                            "key: {} for database: {}".format(validate_keys, database.id))

            # test pagesize > 0 parameter
            response = self.rest_dapi.get_document_list("_default", "_default", "?pagesize=10")
            self.assertTrue(response.status_code == 200, "Getting doc list failed with pagesize parameter: {}".format(response.status_code))
            doc_list = json.loads(response.content)["docs"]
            keys_retrieved = list(doc["id"] for doc in doc_list)
            self.assertTrue(len(keys_retrieved) == 10, "Get doc list does not have length equals 10")
            validate_keys = next((keys for keys in keys_retrieved if keys not in keys_inserted), True)
            self.assertTrue(validate_keys == True, "Get unambigous keys while fetching list of docs"
                            "key: {} for database: {}".format(validate_keys, database.id))

            # test limit parameter wiht 0 value
            response = self.rest_dapi.get_document_list("_default", "_default", "?limit=0")
            self.assertTrue(response.status_code == 200, "Get doc list failed with limit parametere: {}".format(response.status_code))
            doc_list = json.loads(response.content)["docs"]
            keys_retrieved = list(doc["id"] for doc in doc_list)
            self.assertTrue(len(keys_retrieved) == 20, "Get doc list does not have length equals 20")
            validate_keys = next((keys for keys in keys_retrieved if keys not in keys_inserted), True)
            self.assertTrue(validate_keys == True, "Get unambigous keys while fetching list of docs"
                            "key: {} for database: {}".format(validate_keys, database.id))

            # test limit parameter for greater than 0
            response = self.rest_dapi.get_document_list("_default", "_default", "?limit=10")
            self.assertTrue(response.status_code == 200, "Getting doc list failed with limit parameter: {}".format(response.status_code))
            doc_list = json.loads(response.content)["docs"]
            keys_retrieved = list(doc["id"] for doc in doc_list)
            self.assertTrue(len(keys_retrieved) == 10, "Get doc list does not have length equals 10")
            validate_keys = next((keys for keys in keys_retrieved if keys not in keys_inserted), True)
            self.assertTrue(validate_keys == True, "Get unambigous keys while fetching list of docs"
                            "key: {} for database: {}".format(validate_keys, database.id))

            # test pass limit and pagesize parameter together, pagesize should override limit parameter
            response = self.rest_dapi.get_document_list("_default", "_default", "?limit=10&pagesize=20")
            self.assertTrue(response.status_code == 200, "Get doc list failed with limit and"
                            "pagesize parametere: {}".format(response.status_code))
            doc_list = json.loads(response.content)["docs"]
            keys_retrieved = list(doc["id"] for doc in doc_list)
            self.assertTrue(len(keys_retrieved) == 20, "Get doc list does not have length equals 20")
            validate_keys = next((keys for keys in keys_retrieved if keys not in keys_inserted), True)
            self.assertTrue(validate_keys == True, "Get unambigous keys while fetching list of docs"
                            "key: {} for database: {}".format(validate_keys, database.id))

            # test page parameter
            mark = dict()
            for i in range(1, 11):
                query_param = "?orderby=META().id&order=ASC&pagesize=10&page=" + str(i)
                response = self.rest_dapi.get_document_list("_default", "_default", query_param)
                self.assertTrue(response.status_code == 200, "Get document list failed with pagesize=10 and"
                                "page = {} for database: {}".format(i, database.id))
                doc_list = json.loads(response.content)["docs"]
                keys_retrieved = list(doc["id"] for doc in doc_list)
                # check if the same documents comes in two different pages
                validate_flag = True
                for key in keys_retrieved:
                    self.log.info(key)

                for key in keys_retrieved:
                    if key in list(mark.keys()):
                        validate_flag = False
                        break
                    mark[key] = True
                self.assertTrue(validate_flag == True, "Getting the same document in two different pages")

            # test offset parameter
            response = self.rest_dapi.get_document_list("_default", "_default", "?offset=30&pagesize={}".format(self.number_of_docs))
            self.assertTrue(response.status_code == 200, "Get doc list failed for database: {}".format(database.id))
            document_list = json.loads(response.content)["docs"]
            self.assertTrue(len(document_list) == (self.number_of_docs - 30),
                            "Offset parameter is not working properly for database: {}".format(database.id))

            # test offset parameter along with pagesize and page
            response = self.rest_dapi.get_document_list("_default", "_default", "?offset=40&pagesize=80&page=0")
            self.assertTrue(response.status_code == 200, "Get doc list failed with offset, pagesize and page parameter")
            document_list = json.loads(response.content)["docs"]
            self.assertTrue(len(document_list) == 60, "Length of document list is as expected")
