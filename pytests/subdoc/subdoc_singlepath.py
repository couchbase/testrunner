from .subdoc_base import SubdocBaseTest
from lib.mc_bin_client import MemcachedClient, MemcachedError
from lib.memcacheConstants import *
import copy, json
from couchbase_helper.documentgenerator import SubdocDocumentGenerator
from basetestcase import BaseTestCase
from random import randint

class SubdocSinglePathTests(SubdocBaseTest):
    def setUp(self):
        super(SubdocSinglePathTests, self).setUp()
        self.client = MemcachedClient(host=self.server.ip)
        self.jsonSchema = {
            "id" : "0",
            "number" : 0,
            "array" : [],
            "child" : {},
            "isDict" : True,
            "padding": None
        }

    def tearDown(self):
        super(SubdocSinglePathTests, self).tearDown()

    def insertBinaryDocument(self, key):
        pass

    def _createNestedJson(self, key, dict):
        if dict['levels'] == 0:
            return
        if dict['doc'] == {}:
            dict['doc'] = copy.copy(self.jsonSchema)
        else:
            dict['doc']['child'] = copy.copy(self.jsonSchema)
            dict['doc']['child']['array'] = []
            dict['doc'] = dict['doc']['child']

        dict['doc']['id'] = key
        dict['doc']['number'] = dict['levels']

        for level in range(0, dict['levels']):
            dict['doc']['array'].append(level)
        return self._createNestedJson(key, {'doc': dict['doc'], 'levels': dict['levels']-1})

    def insertJsonDocument(self, key, levels, expiry, size=512):
        dict = {'doc' : {}, 'levels' : levels }
        self._createNestedJson(key, dict)
        jsonDump = json.dumps(dict['doc'])
        self.client.set(key, expiry, 0, jsonDump)

    def deleteDoc(self, key):
        self.client.delete(key)

    def _load_all_docs(self, key=None, levels=0, num_items=0):
        dict = {'doc' : {}, 'levels' : levels }
        self._createNestedJson(key, dict)
        template = dict['doc']
        gen_load = SubdocDocumentGenerator(key, template, start=0, end=num_items)

        print("Inserting json data into bucket")
        self._load_all_buckets(self.server, gen_load, "create", 0)
        self._wait_for_stats_all_buckets([self.server])

class SubDocHelper(SubdocSinglePathTests):
    ''' add helper functions for extending the existing get/replace/insert/counter functions below
    '''

    def update_docs(self):
        pass

    def get_docs(self):
        pass

    def remove_docs(self):
        pass

    def insert_docs(self):
        pass

    def validate_results(self):
        pass

class SanityTests(SubdocSinglePathTests):
    basicDocKey = 'basicDocKey'
    deepNestedDocKey =  'deepNestedDocKey'
    deepNestedGreaterThanAllowedDocKey = 'deepNestedGreaterThanAllowedDocKey'

    def setUp(self):
        super(SanityTests, self).setUp()

        '''change this to configurable, currently set as 10000 documents'''
        self._load_all_docs(self.basicDocKey, 16, 10000)
        self._load_all_docs(self.deepNestedDocKey, 30, 1000)
        self._load_all_docs(self.deepNestedGreaterThanAllowedDocKey, 64, 100)

        ''' Issue w/ gets, retain below until fixed '''
        self.insertJsonDocument(self.basicDocKey, 16, 0)
        self.insertJsonDocument(self.deepNestedDocKey, 30, 0)
        self.insertJsonDocument(self.deepNestedGreaterThanAllowedDocKey, 64, 0)

    def tearDown(self):
        super(SanityTests, self).tearDown()

    def getsInDictionaryValue(self):
        opaque, cas, data = self.client.get_in(self.basicDocKey, 'child.child.child.child.child.isDict')
        assert data == 'true'

    def getsInDictionaryValueNull(self):
        opaque, cas, data = self.client.get_in(self.basicDocKey, 'child.child.child.child.child.padding')
        assert data == 'null'

    def getsInArrayValue(self):
        opaque, cas, data = self.client.get_in(self.basicDocKey, 'child.child.child.child.child.array[0]')
        assert data == '0'

    def getsInArrayNegativeIndex(self):
        opaque, cas, data = self.client.get_in(self.basicDocKey, 'array[-1]')
        assert data == '15'

    def getsInMismatchPath(self):
        try:
            self.client.get_in(self.basicDocKey, 'child[0]')
        except MemcachedError as error:
            assert error.status == ERR_SUBDOC_PATH_MISMATCH

    def getsInMissingPath(self):
        try:
            self.client.get_in(self.basicDocKey, 'child.random')
        except MemcachedError as error:
            assert error.status == ERR_SUBDOC_PATH_ENOENT

    def getsInIncorrectSyntax(self):
        try:
            self.client.get_in(self.basicDocKey, 'array[[1]]')
        except MemcachedError as error:
            assert error.status == ERR_SUBDOC_PATH_EINVAL

    def getsInNonJsonDocument(self):
        pass

    def getsInNestedDoc(self):
        opaque, cas, data = self.client.get_in(self.deepNestedDocKey, 'child.isDict')
        assert data == 'true'

    def getsInDocTooDeep(self):
        try:
            opaque, cas, data = self.client.get_in(self.deepNestedGreaterThanAllowedDocKey, 'child.isDict')
        except MemcachedError as error:
            assert error.status == ERR_SUBDOC_DOC_E2DEEP

    def getsInPathTooDeep(self):
        pass

    ''' Start of Sample Structure Functions'''
    def removeInArrayValue(self):
        try:
            opaque, cas, data = self.client.remove_in(self.basicDocKey, 'child.child.child.child.child.array[0]')
        except Exception as exception:
            raise exception

    def removeInNestedDoc(self):
        try:
            opaque, cas, data = self.client.remove_in(self.deepNestedDocKey, 'child.child.child.child.child.array[0]')
        except Exception as exception:
            raise exception

    def removeInArrayValueNull(self):
        try:
            opaque, cas, data = self.client.remove_in(self.basicDocKey, 'child.child.child.child.child.padding')
        except Exception as exception:
            raise exception

    def removeInArrayNegativeIndex(self):
        try:
            opaque, cas, data = self.client.remove_in(self.basicDocKey, 'array[-1]')
        except Exception as exception:
            raise exception

    ''' Does not work - Need to add type '''
    def counterInNestedDoc(self):
        try:
            opaque, cas, data = self.client.counter_in(self.basicDocKey, 'child.child.child.child.child.array[0]', '-5', 0)
        except Exception as exception:
            raise exception
        finally:
            opaque, cas, data = self.client.get_in(self.basicDocKey, 'child.child.child.child.child.array[0]')

    def insert_inInArrayValue(self):
        try:
            random_string = '"123"'
            opaque, cas, data = self.client.insert_in(self.basicDocKey, 'child.child.child.child.child.array[0]', random_string)
        except Exception as exception:
            raise exception

    def insert_inLongValue(self):
        try:
            long_string = ''.join(chr(97 + randint(0, 25)) for i in range(10000))
            long_string = "'" + long_string + "'"
            opaque, cas, data = self.client.insert_in(self.basicDocKey, 'child.child.child.child.child.array[0]', long_string)
        except Exception as exception:
            raise exception

    def insert_inNestedDoc(self):
        try:
            long_string = ''.join(chr(97 + randint(0, 25)) for i in range(10000))
            long_string = "'" + long_string + "'"
            opaque, cas, data = self.client.insert_in(self.deepNestedDocKey, 'child.isDict', long_string)
        except Exception as exception:
            raise exception

    def replace_inInArrayValue(self):
        try:
            random_string = '"hello"'
            opaque, cas, data = self.client.replace_in(self.basicDocKey, 'child.child.child.child.child.array[0]', random_string)
        except Exception as exception:
            raise exception

    def replace_inLongValue(self):
        try:
            long_string = ''.join(chr(97 + randint(0, 25)) for i in range(10000))
            long_string = "'" + long_string + "'"
            opaque, cas, data = self.client.replace_in(self.basicDocKey, 'child.child.child.child.child.child.child.child.array[0]', long_string)
        except Exception as exception:
            raise exception

    def replace_inNullValue(self):
        try:
            empty_string = "'   '"
            opaque, cas, data = self.client.replace_in(self.basicDocKey, 'child.child.child.child.child.child.child.child.array[0]', empty_string)
        except Exception as exception:
            raise exception

    def exists_inInArrayValue(self):
        try:
            opaque, cas, data = self.client.exists_in(self.deepNestedDocKey, 'child.child.child.child.child.array[0]')
        except Exception as exception:
            raise exception

    def exists_inInArrayNegativeIndex(self):
        try:
            opaque, cas, data = self.client.exists_in(self.deepNestedDocKey, 'child.child.child.child.child.array[-1]')
        except Exception as exception:
            raise exception

    def exists_Missing(self):
        try:
            opaque, cas, data = self.client.exists_in(self.deepNestedDocKey, 'child.child.child.child.child.array.child')
        except MemcachedError as error:
            assert error.status == ERR_SUBDOC_PATH_MISMATCH

    ''' Error- Path Mismatch'''
    def append_inInArrayValue(self):
        try:
            random_string = '"hello"'
            blank_string = '" "'
            opaque, cas, data = self.client.append_in(self.basicDocKey, 'child.child.child.child.child.array[0]', 1)
        except Exception as exception:
            raise exception

    ''' Error- Path Mismatch'''
    def prepend_inInArrayValue(self):
        try:
            random_string = '"hello"'
            blank_string = '" "'
            opaque, cas, data = self.client.prepend_in(self.basicDocKey, 'child.child.child.child.child.array[0]', random_string)
        except Exception as exception:
            raise exception

    ''' End of Sample Structure Functions'''
