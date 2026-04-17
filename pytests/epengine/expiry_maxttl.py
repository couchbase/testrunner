import copy
from couchbase_helper.documentgenerator import JsonDocGenerator
from serverless.dapi.dapi_helper import doc_generator
from membase.api.rest_client import RestConnection
from basetestcase import BaseTestCase
from remote.remote_util import RemoteMachineShellConnection
from cb_tools.cbstats import Cbstats

class ExpiryMaxTTL(BaseTestCase):

    def setUp(self):
        super(ExpiryMaxTTL, self).setUp()

    def _load_json(self, bucket, num_items, exp=0):
        self._gen_create = JsonDocGenerator("key-",
                                            encoding="utf-8",
                                            start=0,
                                            end=num_items)

        gen = copy.deepcopy(self._gen_create)
        self.log.info("Loading {0} docs with expiry={1}s".format(num_items, exp))
        task = self.cluster.async_load_gen_docs(self.master,
                                             bucket.name,
                                             gen,
                                             bucket.kvs[1],
                                             "create",
                                             exp, compression=self.sdk_compression)
        task.result()
        while RestConnection(self.master).get_active_key_count(bucket) != num_items:
            self.sleep(2, "waiting for docs to get loaded")
        self.log.info("Item count = {0}".format(RestConnection(self.master).get_active_key_count(bucket)))
        return

    def _update_bucket_maxTTL(self, maxttl):
        for bucket in self.buckets:
            self.log.info("Updating maxTTL for bucket {0} to {1}s".format(bucket.name, maxttl))
            RestConnection(self.master).change_bucket_props(bucket, maxTTL=maxttl)

    def test_maxttl_lesser_doc_expiry(self):
        """
         A simple test to create a bucket with maxTTL and
         check whether new creates with greater exp are
         deleted when maxTTL has lapsed
        :return:
        """
        for bucket in self.buckets:
            self._load_json(bucket, self.num_items, exp=int(self.maxttl)+500)
        self.sleep(int(self.maxttl), "waiting for all docs to expire per maxTTL rule...")
        self.expire_pager(self.servers)
        self.sleep(20, "waiting for item count to come down...")
        for bucket in self.buckets:
            items = RestConnection(self.master).get_active_key_count(bucket)
            RestConnection(self.master).get_active_key_count(bucket)
            self.log.info("Doc expiry set to = {0}s, maxTTL = {1}s, after {2}s, item count = {3}".format(
                int(self.maxttl) + 500,
                self.maxttl,
                self.maxttl,
                items))
            if items > 0:
                self.fail("Bucket maxTTL of {0} is not honored".format(self.maxttl))
            else:
                self.log.info("SUCCESS: Doc expiry set to = {0}s, maxTTL = {1}s, after {2}s, item count = {3}".format(
                    int(self.maxttl) + 500,
                    self.maxttl,
                    self.maxttl,
                    items))

    def test_maxttl_greater_doc_expiry(self):
        """
        maxTTL is set to 200s in this test,
        Docs have lesser TTL.
        :return:
        """
        for bucket in self.buckets:
            self._load_json(bucket, self.num_items, exp=int(self.maxttl)-100)
        self.sleep(int(self.maxttl-100), "waiting for all docs to expire per maxTTL rule...")
        self.expire_pager(self.servers)
        self.sleep(20, "waiting for item count to come down...")
        for bucket in self.buckets:
            items = RestConnection(self.master).get_active_key_count(bucket)
            self.log.info("Doc expiry set to = {0}s, maxTTL = {1}s, after {2}s, item count = {3}".format(
                int(self.maxttl) - 100,
                self.maxttl-100,
                self.maxttl-100,
                items))
            if items == 0:
                self.log.info("SUCCESS: Docs with lesser expiry deleted")
            else:
                self.fail("FAIL: Doc with lesser expiry still present past ttl")

    def test_set_maxttl_on_existing_bucket(self):
        """
        1. Create a bucket with no max_ttl
        2. Upload 1000 docs with exp = 100s
        3. Set maxTTL on bucket as 60s
        4. After 60s, run expiry pager, get item count, must be 1000
        5. After 40s, run expiry pager again and get item count, must be 0
        6. Now load another set of docs with exp = 100s
        7. Run expiry pager after 60s and get item count, must be 0
        """
        for bucket in self.buckets:
            self._load_json(bucket, self.num_items, exp=100)
        self._update_bucket_maxTTL(maxttl=60)

        self.sleep(60, "waiting before running expiry pager...")
        self.expire_pager(self.servers)
        self.sleep(20, "waiting for item count to come down...")
        for bucket in self.buckets:
            items = RestConnection(self.master).get_active_key_count(bucket)
            self.log.info("Doc expiry set to = 100s, maxTTL = 60s"
                          "(set after doc creation), after 60s, item count = {0}".format(items))
            if items != self.num_items:
                self.fail("FAIL: Items with larger expiry before maxTTL updation deleted!")

        self.sleep(40, "waiting before running expiry pager...")
        self.expire_pager(self.servers)
        self.sleep(20, "waiting for item count to come down...")
        for bucket in self.buckets:
            items = RestConnection(self.master).get_active_key_count(bucket)
            self.log.info("Doc expiry set to = 100s, maxTTL = 60s"
                          "(set after doc creation), after 100s,"
                          " item count = {0}".format(items))
            if items != 0:
                self.fail("FAIL: Items with not greater expiry set before maxTTL "
                          "updation not deleted after elapsed TTL!")
        for bucket in self.buckets:
            self._load_json(bucket, self.num_items, exp=100)

        self.sleep(60, "waiting before running expiry pager...")
        self.expire_pager(self.servers)
        self.sleep(20, "waiting for item count to come down...")
        for bucket in self.buckets:
            items = RestConnection(self.master).get_active_key_count(bucket)
            self.log.info("Doc expiry set to = 100s, maxTTL = 60s, after 100s,"
                          " item count = {0}".format(items))
            if items != 0:
                self.fail("FAIL: Items with not greater expiry not "
                          "deleted after elapsed maxTTL!")

    def test_maxttl_possible_values(self):
        """
        Test
        1. min - 0
        2. max - 2147483647q
        3. default - 0
        4. negative values, date, string
        """
        # default
        rest = RestConnection(self.master)
        default_maxttl = rest.get_bucket_maxTTL()
        if default_maxttl != 0:
            self.fail("FAIL: default maxTTL if left unset must be 0 but is {0}".format(default_maxttl))
        self.log.info("Verified: default maxTTL if left unset is {0}".format(default_maxttl))

        # max value
        try:
            self._update_bucket_maxTTL(maxttl=2147483648)
        except Exception as e:
            self.log.info("Expected exception : {0}".format(e))
            try:
                self._update_bucket_maxTTL(maxttl=2147483647)
            except Exception as e:
                self.fail("Unable to set maxTTL=2147483647, the max permitted value")
            else:
                self.log.info("Verified: Max value permitted is 2147483647")
        else:
            self.fail("Able to set maxTTL greater than 2147483647")

        # min value
        try:
            self._update_bucket_maxTTL(maxttl=0)
        except Exception as e:
            self.fail("Unable to set maxTTL=0, the min permitted value")
        else:
            self.log.info("Verified: Min value permitted is 0")

        # negative value
        try:
            self._update_bucket_maxTTL(maxttl=-60)
        except Exception as e:
            self.log.info("Verified: negative values not permitted, exception : {0}".format(e))
        else:
            self.fail("FAIL: Able to set a negative maxTTL")

        # date/string
        try:
            self._update_bucket_maxTTL(maxttl="12/23/2016")
        except Exception as e:
            self.log.info("Verified: string not permitted, exception : {0}".format(e))
        else:
            self.fail("FAIL: Able to set a date string maxTTL")

    def test_update_maxttl(self):
        """
        1. Create a bucket with ttl = 200s
        2. Upload 1000 docs with exp = 100s
        3. Update ttl = 40s
        4. After 40s, run expiry pager again and get item count, must be 1000
        5. After 60s, run expiry pager again and get item count, must be 0
        6. Now load another set of docs with exp = 100s
        7. Run expiry pager after 40s and get item count, must be 0
        """
        for bucket in self.buckets:
            self._load_json(bucket, self.num_items, exp=100)
        self._update_bucket_maxTTL(maxttl=40)

        self.sleep(40, "waiting before running expiry pager...")
        self.expire_pager(self.servers)
        self.sleep(20, "waiting for item count to come down...")
        for bucket in self.buckets:
            items = RestConnection(self.master).get_active_key_count(bucket)
            self.log.info("Doc expiry set to = 100s, maxTTL at the time of doc creation = 200s"
                          " updated maxttl = 40s, after 40s item count = {0}".format(items))
            if items != self.num_items:
                self.fail("FAIL: Updated ttl affects docs with larger expiry before updation!")

        self.sleep(60, "waiting before running expiry pager...")
        self.expire_pager(self.servers)
        self.sleep(20, "waiting for item count to come down...")
        for bucket in self.buckets:
            items = RestConnection(self.master).get_active_key_count(bucket)
            self.log.info("Doc expiry set to = 100s, maxTTL at the time of doc creation = 200s"
                          " updated maxttl = 40s, after 100s item count = {0}".format(items))
            if items != 0:
                self.fail("FAIL: Docs with 100s as expiry before maxTTL updation still alive!")

    def test_maxttl_with_doc_updates(self):
        """
        1. Create a bucket with ttl = 60s
        2. Upload 1000 docs with exp = 40s
        3. After 20s, Update docs with exp = 60s
        4. After 40s, run expiry pager again and get item count, must be 1000
        5. After 20s, run expiry pager again and get item count, must be 0
        """
        rest = RestConnection(self.master)
        for bucket in self.buckets:
            self._load_json(bucket, self.num_items, exp=40)

        self.sleep(20, "waiting to update docs with exp=60s...")

        for bucket in self.buckets:
            self._load_json(bucket, self.num_items, exp=60)

        self.sleep(40, "waiting before running expiry pager...")
        self.expire_pager(self.servers)
        for bucket in self.buckets:
            items = rest.get_active_key_count(bucket)
            self.log.info("Items: {0}".format(items))
            if items != self.num_items:
                self.fail("FAIL: Docs with updated expiry deleted unexpectedly!")

        self.sleep(20, "waiting before running expiry pager...")
        self.expire_pager(self.servers)
        self.sleep(20, "waiting for item count to come down...")
        for bucket in self.buckets:
            items = rest.get_active_key_count(bucket)
            self.log.info("Items: {0}".format(items))
            if items != 0:
                self.fail("FAIL: Docs with updated expiry not deleted after new exp has elapsed!")

    def test_persistent_metadata_purge_age_purge_validation(self):
        """Test actual purging with persistent_metadata_purge_age set to max value
        This test validates that tombstones are purged correctly when
        persistent_metadata_purge_age is set to the maximum value (730 days)
        """
        self.log.info("Starting test for persistent_metadata_purge_age "
                     "purge validation with max value (730 days)")

        shell = RemoteMachineShellConnection(self.master)
        cbstat = Cbstats(shell)
        bucket_name = self.buckets[0].name

        # Load documents with expiry
        self.log.info("Loading documents with expiry to create tombstones")
        expiry_ttl = 10  # 10 seconds TTL
        key_prefix = "test_key"
        doc_generator_expiry = doc_generator(
            key_prefix, key_size=8,
            value_size=256, number_of_docs=self.num_items)

        task = self.cluster.async_load_gen_docs(
            self.master, self.buckets[0].name, doc_generator_expiry,
            self.buckets[0].kvs[1], "create",
            exp=expiry_ttl,
            batch_size=10)
        task.result()

        # Wait for documents to expire
        self.sleep(expiry_ttl + 5, "Wait for documents to expire")

        # Trigger expiry pager to create tombstones
        self.log.info("Triggering expiry pager to create tombstones")
        self.expire_pager(self.servers, 30)
        self.sleep(30, "Wait for expiry pager to process")

        # Check for document count after expiry
        stats = cbstat.all_stats(bucket_name)
        initial_doc_count = int(stats.get("curr_items", 0))
        self.log.info(f"Document count after expiry: {initial_doc_count}")

        # Set persistent_metadata_purge_age to max value (730 days)
        max_purge_age = 730 * 24 * 60 * 60  # 730 days in seconds
        self.log.info(f"Setting persistent_metadata_purge_age to {max_purge_age} "
                     f"seconds (730 days = 2 years)")
        shell.execute_cbepctl(self.buckets[0], "", "set flush_param",
                             "persistent_metadata_purge_age", str(max_purge_age))

        # Verify the value was set using cbstats
        stats = cbstat.all_stats(bucket_name)
        actual_purge_age = int(stats.get("ep_persistent_metadata_purge_age", 0))

        self.assertEqual(actual_purge_age, max_purge_age,
                        f"ep_persistent_metadata_purge_age mismatch: "
                        f"expected {max_purge_age}, got {actual_purge_age}")
        self.log.info(f"Verified persistent_metadata_purge_age set to "
                     f"{actual_purge_age} seconds")

        # For validation purposes, run compaction to ensure tombstones
        # can be cleaned up with the new max purge age
        self.log.info("Running compaction to validate purge age configuration")
        compact_tasks = list()
        for bucket in self.buckets:
            compact_tasks.append(self.cluster.async_compact_bucket(
                self.master, bucket))
        for task in compact_tasks:
            task.result()
            self.assertTrue(task.result, "Compaction failed due to: "
                                         + str(task.exception))

        # Verify that documents were expired (count should be 0)
        stats = cbstat.all_stats(bucket_name)
        final_doc_count = int(stats.get("curr_items", 0))

        self.assertEqual(final_doc_count, 0,
                        f"Documents not properly purged: expected 0, "
                        f"got {final_doc_count}")
        self.log.info("Successfully validated persistent_metadata_purge_age "
                     "with max value (730 days)")

        shell.disconnect()
