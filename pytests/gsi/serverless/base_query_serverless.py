import requests

from pytests.serverless.serverless_basetestcase import ServerlessBaseTestCase


class QueryBaseServerless(ServerlessBaseTestCase):
    def setUp(self):
        super(QueryBaseServerless, self).setUp()

    def tearDown(self):
        super(QueryBaseServerless, self).tearDown()