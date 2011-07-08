import random
import unittest
import uuid
from TestInput import TestInputSingleton
from couchdb import client
import logger

class AllDocsTests(unittest.TestCase):
    cleanup_dbs = []

    def setUp(self):
        self.log = logger.Logger.get_logger()
        input = TestInputSingleton.input
        node = input.servers[0]
        url = "http://{0}:{1}/".format(node.ip, node.port)
        self.log.info("connecting to couchdb @ {0}".format(url))
        self.server = client.Server(url, full_commit=False)

    def tearDown(self):
        for db in self.cleanup_dbs:
            try:
                self.server.delete(db)
            except Exception:
                pass

        all_dbs = [db for db in self.server]
        for db in all_dbs:
            if db.find("doctest") != -1:
                self.server.delete(db)
        self.cleanup_dbs = []

    def _get_db_name(self):
        name = "doctests-{0}".format(str(uuid.uuid4())[:6])
        self.cleanup_dbs.append(name)
        return name

    def _random_doc(self, howmany=1):
        docs = []
        for i in range(0, howmany):
            id = "{0}".format(i)
            k1 = "a"
            v1 = random.randint(0, 10000)
            k2 = "b"
            v2 = random.randint(0, 10000)
            #have random key-values here ?
            doc = {"_id": id, k1: v1, k2: v2}
            docs.append(doc)
        return docs

    def test_multipledocs(self):
        db_name = self._get_db_name()
        self.server.create(db_name)
        docs = self._random_doc(10)
        for doc in docs:
            self.server[db_name].save(doc)
        self.assertTrue(self.server[db_name].info()['doc_count'] == len(docs))