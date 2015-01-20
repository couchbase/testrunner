import logger
import json
import uuid
import copy
import math
import re

import testconstants
import datetime
import time
from datetime import date
from couchbase_helper.tuq_generators import TuqGenerators
from couchbase_helper.tuq_generators import JsonGenerator
from remote.remote_util import RemoteMachineShellConnection
from basetestcase import BaseTestCase
from couchbase_helper.documentgenerator import DocumentGenerator
from couchbase_helper.tuq_helper import N1QLHelper
from membase.api.exception import CBQError, ReadDocumentException
from membase.api.rest_client import RestConnection
from memcached.helper.data_helper import MemcachedClientHelper

class QueryTests(BaseTestCase):
    def setUp(self):
        super(QueryTests, self).setUp()
        self.version = self.input.param("cbq_version", "git_repo")
        if self.input.tuq_client and "client" in self.input.tuq_client:
            self.shell = RemoteMachineShellConnection(self.input.tuq_client["client"])
        else:
            self.shell = RemoteMachineShellConnection(self.master)
        self.use_gsi_for_primary = self.input.param("use_gsi_for_primary", True)
        self.use_gsi_for_secondary = self.input.param("use_gsi_for_secondary", True)
        self.create_primary_index = self.input.param("create_primary_index", True)
        self.use_rest = self.input.param("use_rest", True)
        self.max_verify = self.input.param("max_verify", None)
        self.buckets = RestConnection(self.master).get_buckets()
        self.docs_per_day = self.input.param("doc-per-day", 49)
        self.item_flag = self.input.param("item_flag", 4042322160)
        self.n1ql_port = self.input.param("n1ql_port", 8093)
        self.dataset = self.input.param("dataset", "default")
        self.gens_load = self.generate_docs(self.docs_per_day)
        if self.input.param("gomaxprocs", None):
            self.n1ql_helper.configure_gomaxprocs()
        self.full_docs_list = self.generate_full_docs_list(self.gens_load)
        self.gen_results = TuqGenerators(self.log, self.generate_full_docs_list(self.gens_load))
        self.load(self.gens_load, flag=self.item_flag)
        # Define Helper Method which will be used for running n1ql queries, create index, drop index
        self.n1ql_helper = N1QLHelper(version = self.version, shell = self.shell,
            use_rest = self.use_rest, max_verify = self.max_verify,
            buckets = self.buckets, item_flag = self.item_flag,
            n1ql_port = self.n1ql_port, full_docs_list = self.full_docs_list,
            log = self.log, input = self.input, master = self.master)
        self.n1ql_helper._start_command_line_query(self.master)
        if self.create_primary_index:
            try:
                self.n1ql_helper.create_primary_index_for_3_0_and_greater(using_gsi = self.use_gsi_for_primary)
            except Exception, ex:
                self.log.info(ex)

    def tearDown(self):
        if hasattr(self, 'shell'):
            self.n1ql_helper._restart_indexer()
            self.n1ql_helper.killall_tuq_process()
        super(QueryTests, self).tearDown()

    def generate_docs(self, num_items, start=0):
        try:
            if self.dataset == "simple":
                return self.generate_docs_simple(num_items, start)
            if self.dataset == "sales":
                return self.generate_docs_sales(num_items, start)
            if self.dataset == "bigdata":
                return self.generate_docs_bigdata(num_items, start)
            return getattr(self, 'generate_docs_' + self.dataset)(num_items, start)
        except Exception, ex:
            self.log.info(ex)
            self.fail("There is no dataset %s, please enter a valid one" % self.dataset)

    def generate_docs_default(self, docs_per_day, start=0):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_employee(docs_per_day, start)

    def generate_docs_sabre(self, docs_per_day, start=0):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_sabre(docs_per_day, start)

    def generate_docs_employee(self, docs_per_day, start=0):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_employee(docs_per_day = docs_per_day, start = start)

    def generate_docs_simple(self, docs_per_day, start=0):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_simple(start = start, docs_per_day = docs_per_day)

    def generate_docs_sales(self, docs_per_day, start=0):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_sales(docs_per_day = docs_per_day, start = start)

    def generate_docs_bigdata(self, docs_per_day, start=0):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_employee_bigdata(docs_per_day = docs_per_day,
            start = start, value_size = self.value_size)