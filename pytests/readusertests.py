import logger

from membase.api.rest_client import RestConnection
from basetestcase import BaseTestCase
from membase.api.exception import BucketCreationException, \
            DesignDocCreationException, AddNodeException, \
            FailoverFailedException
from couchbase_helper.document import DesignDocument, View

class ROUserTests(BaseTestCase):

    def setUp(self):
        super(ROUserTests, self).setUp()
        self.username = self.input.param('username', 'myrouser')
        self.password = self.input.param('password', 'myropass')
        self.admin_user = self.master.rest_username
        self.admin_pass = self.master.rest_password

    def tearDown(self):
        if hasattr(self, 'admin_user'):
            self.master.rest_username = self.admin_user
            self.master.rest_password = self.admin_pass
        super(ROUserTests, self).tearDown()
        RestConnection(self.master).delete_ro_user()

    def create_user_test_bucket_check(self):
        rest = RestConnection(self.master)

        rest.create_ro_user(username=self.username, password=self.password)
        self.master.rest_username = self.username
        self.master.rest_password = self.password
        rest = RestConnection(self.master)

        self.log.info("Try to edit bucket")
        try:
            rest.create_bucket(bucket='bucket0', ramQuotaMB=100,
                               authType='sasl', saslPassword='sasl')
        except BucketCreationException as e:
            self.log.info("Unable to create the bucket. Expected")
        else:
            self.fail("Created bucket. But user is read only")
        self.log.info("Try to delete bucket")
        self.assertFalse(rest.delete_bucket(self.buckets[0]),
                         "Deleted bucket. But user is read only")
        self.log.info("Unable to delete bucket. Expected")

    def create_user_test_ddoc_check(self):
        rest = RestConnection(self.master)
        ddoc = DesignDocument("ddoc_ro_0", [View("ro_view",
                            "function (doc) {\n  emit(doc._id, doc);\n}",
                            dev_view=False)])
        rest.create_design_document(self.buckets[0], ddoc)

        rest.create_ro_user(username=self.username, password=self.password)
        self.master.rest_username = self.username
        self.master.rest_password = self.password
        rest = RestConnection(self.master)

        self.log.info("Try to delete ddoc")
        self.buckets[0].authType = ""
        try:
            rest.delete_view(self.buckets[0], ddoc.views[0])
        except Exception as ex:
            self.log.info("Unable to delete ddoc. Expected")
            self.buckets[0].authType = "sasl"
        else:
            self.buckets[0].authType = "sasl"
            self.fail("Able to delete ddoc")

    def negative_create_user_test(self):
        self.log.info("try to create user %s, pass %s" % (self.username, self.password))
        rest = RestConnection(self.master)
        self.assertFalse(rest.create_ro_user(username=self.username, password=self.password),
                         "No error appeared")
        self.log.info("Error appears as expected")
