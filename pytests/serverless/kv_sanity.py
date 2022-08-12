from serverless.serverless_basetestcase import ServerlessBaseTestCase


class KVSanity(ServerlessBaseTestCase):
    def setUp(self):
        return super().setUp()

    def tearDown(self):
        return super().tearDown()

    def test_kv_sanity(self):
        tasks = []
        for _ in range(0, 3):
            task = self.create_database_async()
            tasks.append(task)
        for task in tasks:
            task.result()
        for database in self.databases.values():
            bucket = self.get_sdk_bucket(database.id)
            collection = bucket.default_collection()
            collection.upsert("key", {"key": "value"})
            res = collection.get("key")
            self.assertEqual(res.value["key"], "value")
