import random
import unittest
import uuid
from TestInput import TestInputSingleton
from couchdb import client
import logger

class BasicTests(unittest.TestCase):

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

    def test_create(self):
        db_name = self._get_db_name()
        self.server.create(db_name)

    def test_doccount(self):
        db_name = self._get_db_name()
        self.server.create(db_name)
        self.assertTrue(self.server[db_name].info()['doc_count'] == 0)

    def test_docsave(self):
        db_name = self._get_db_name()
        self.server.create(db_name)
        doc = {"_id":"0","a":1,"b":1}
        id, rev = self.server[db_name].save(doc)
        self.assertEquals(id,"0")
        self.assertTrue(rev)

    def test_docdelete(self):
        db_name = self._get_db_name()
        self.server.create(db_name)
        doc = {"_id":"0","a":1,"b":1}
        id, rev = self.server[db_name].save(doc)
        fetched = self.server[db_name].get(id)
        self.server[db_name].delete(fetched)
        self.assertFalse(self.server[db_name].get(id), msg="doc not found")

    def test_delete(self):
        db_name = self._get_db_name()
        self.server.create(db_name)
        self.server.resource.delete(path="{0}".format(db_name))

    def test_multiplecreate(self):
        db_names = []
        for i in range(20):
            db_names.append(self._get_db_name())
        for db in db_names:
            self.server.create(db)
        #verify all dbs
        all_dbs = [db for db in self.server]
        for db_name in db_names:
            exist = False
            for couchdb_db in all_dbs:
                if couchdb_db == db_name:
                    exist = True
                    break
            if not exist:
                self.fail("db {0} was created but not listed in couchdb.all_dbs".format(db_name))

    def _random_doc(self, howmany=1):
        docs = []
        for i in range(howmany):
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

    def test_maponekey(self):
        query = """function(doc) {
            if(doc.a == 4) {
                emit(null, doc.b);
            }
        }"""
        db_name = self._get_db_name()
        doc1 = {"_id": "0", "a": 4, "b": 4}
        doc2 = {"_id": "1", "a": 10, "b": 10}
        self.server.create(db_name)
        self.server[db_name].save(doc1)
        self.server[db_name].save(doc2)
        results = self.server[db_name].query(query)
        self.assertEquals(results.total_rows, 1)
        self.assertEquals(results.rows[0].value, doc1["b"])
        print results



    def test_mapqueryafterupdate(self):
        query = """function(doc) {
            if(doc.a == 4) {
                emit(null, doc.b);
            }
        }"""
        db_name = self._get_db_name()
        doc1 = {"_id": "0", "a": 4, "b": 4}
        doc2 = {"_id": "1", "a": 10, "b": 10}
        self.server.create(db_name)
        self.server[db_name].save(doc1)
        self.server[db_name].save(doc2)
        results = self.server[db_name].query(query)
        self.assertEquals(results.total_rows, 1)
        fetched = self.server[db_name].get("1")
        fetched["a"] = 4
        self.server[db_name].save(fetched)
        results = self.server[db_name].query(query)
        self.assertEquals(results.total_rows, 2)
        doc3 = {"_id": "2", "a": 4, "b": 4}
        doc4 = {"_id": "3", "a": 5, "b": 5}
        doc5 = {"_id": "4", "a": 6, "b": 6}
        doc6 = {"_id": "5", "a": 7, "b": 7}
        self.server[db_name].update([doc3,doc4,doc5,doc6])
        results = self.server[db_name].query(query)
        self.assertEquals(results.total_rows, 3)
        print results.total_rows

    def test_mapqueryafterdelete(self):
        query = """function(doc) {
            if(doc.a == 4) {
                emit(null, doc.b);
            }
        }"""
        db_name = self._get_db_name()
        doc1 = {"_id": "0", "a": 4, "b": 4}
        doc2 = {"_id": "1", "a": 10, "b": 10}
        self.server.create(db_name)
        self.server[db_name].save(doc1)
        self.server[db_name].save(doc2)
        results = self.server[db_name].query(query)
        self.assertEquals(results.total_rows, 1)
        fetched = self.server[db_name].get("1")
        fetched["a"] = 4
        self.server[db_name].save(fetched)
        results = self.server[db_name].query(query)
        self.assertEquals(results.total_rows, 2)
        doc3 = {"_id": "2", "a": 4, "b": 4}
        doc4 = {"_id": "3", "a": 5, "b": 5}
        doc5 = {"_id": "4", "a": 6, "b": 6}
        doc6 = {"_id": "5", "a": 7, "b": 7}
        self.server[db_name].update([doc3,doc4,doc5,doc6])
        results = self.server[db_name].query(query)
        self.assertEquals(results.total_rows, 3)
        #now delete doc3
        fetched = self.server[db_name].get(doc3["_id"])
        self.server[db_name].delete(fetched)
        results = self.server[db_name].query(query)
        self.assertEquals(results.total_rows, 2)

    def test_reduceonekey(self):
        map = """function(doc) {
            if(doc.a == 4) {
                emit(null, doc.b);
            }
        }"""
        reduce = """function(keys, values) {
            return sum(values);
        }"""
        db_name = self._get_db_name()
        doc1 = {"_id": "0", "a": 4, "b": 4}
        doc2 = {"_id": "1", "a": 10, "b": 10}
        doc3 = {"_id": "2", "a": 4, "b": 40}
        self.server.create(db_name)
        self.server[db_name].save(doc1)
        self.server[db_name].save(doc2)
        self.server[db_name].save(doc3)
        results = self.server[db_name].query(map, reduce)
        self.assertEquals(results.total_rows, 1)
        self.assertEquals(results.rows[0].value, doc1["b"] + doc3["b"])

    def test_get(self):
        db_name = self._get_db_name()
        self.server.create(db_name)
        doc = {"_id":"0","a":1,"b":1}
        id, rev = self.server[db_name].save(doc)
        fetched = self.server[db_name].get(id)
        self._doc_equals(doc,fetched)

    def test_update(self):
        db_name = self._get_db_name()
        self.server.create(db_name)
        doc = {"_id":"0","a":1,"b":1}
        id, rev = self.server[db_name].save(doc)
        fetched = self.server[db_name].get(id)
        self._doc_equals(doc,fetched)
        doc["a"] = 10000
        id, rev = self.server[db_name].save(doc)
        self.assertEquals(id,"0")
        self.assertTrue(rev)
        fetched = self.server[db_name].get(id)
        self._doc_equals(doc,fetched)

    def _doc_equals(self, doc1, doc2):
        #two way equal ?
        ok = True
        for k in doc1:
            if k != "rev":
                ok = k in doc2 and doc1[k] == doc2[k]
            if not ok:
                break
        if ok:
            for k in doc2:
                if k != "rev":
                    ok = k in doc1 and doc1[k] == doc2[k]
                if not ok:
                    break
        return ok

    def test_baddocs(self):
        bad_docs = []
        bad_docs.append(["goldfish", {"_zing": 4}])
        bad_docs.append(["zebrafish", {"_zoom": "hello"}])
        bad_docs.append(["mudfish", {"zane": "goldfish", "_fan": "something smells delicious"}])
        bad_docs.append(["tastyfish", {"_bing": {"wha?": "soda can"}}])
        db_name = self._get_db_name()
        self.server.create(db_name)
        for bad in bad_docs:
            a,b,c = self.server.resource.put(path="{0}/{1}".format(db_name, bad[0]), body=bad)
            print a,b,c