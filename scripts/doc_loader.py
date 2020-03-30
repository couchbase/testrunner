#!/usr/bin/env python
import getopt
import sys

sys.path.extend(('.', 'lib'))
from lib.remote.remote_util import RemoteMachineShellConnection
from couchbase_helper.documentgenerator import DocumentGenerator
from couchbase_helper.cluster import Cluster
from membase.api.rest_client import Bucket
from couchbase_helper.documentgenerator import Base64Generator
from couchbase_helper.documentgenerator import JSONNonDocGenerator
from couchbase_helper.tuq_generators import JsonGenerator
import TestInput
import logger
import json
import logging.config
import os
import uuid
import copy


def usage(error=None):
    print("""\
Syntax: doc_loader.py [options]
will create documents like:
{
  "tasks_points": {
    "task1": 1,
    "task2": 1
  },
  "name": "employee-4",
  "mutated": 0,
  "skills": [
    "skill2010",
    "skill2011"
  ],
  "join_day": 4,
  "join_mo": 1,
  "test_rate": 1.1,
  "join_yr": 2010,
  "_id": "query-test1b32d75-0",
  "email": "4-mail@couchbase.com",
  "job_title": "Engineer",
  "VMs": [
    {
      "RAM": 1,
      "os": "ubuntu",
      "name": "vm_1",
      "memory": 1
    },
    {
      "RAM": 1,
      "os": "windows",
      "name": "vm_2",
      "memory": 1
    }
  ]
}

Options
 -i <file>        Path to .ini file containing cluster information.
 -p <key=val,...> Comma-separated key=value info.

Available keys:
 bucket_name= bucket to load (default by default)
 bucket_port=dedicated bucket port if any
 bucket_sasl_pass=sasl password of bucket
 doc_per_day=documents to load per <one day>. 49 by default
 years=number of years. 2 by default
 flags=flags of items
 to_dir=a directory to create json docs

Example:
 doc_loader.py -i cluster.ini -p bucket_name=default,doc_per_day=1
 doc_loader.py -i cluster.ini -p doc_per_day=1,bucket_name=sasl,bucket_sasl_pass=pass
 doc_loader.py -i cluster.ini -p doc_per_day=1,to_dir=/tmp/my_bucket
""")
    sys.exit(error)

class DocLoader(object):
    def __init__(self):
        self.log = logger.Logger.get_logger()

    def generate_docs(self, docs_per_day, years):
        generators = []
        types = ["Engineer", "Sales", "Support"]
        join_yr = [2010, 2011]
        join_mo = range(1, 12 + 1)
        join_day = range(1, 28 + 1)
        template = '{{ "name":"{0}", "join_yr":{1}, "join_mo":{2}, "join_day":{3},'
        template += ' "email":"{4}", "job_title":"{5}", "test_rate":{8}, "skills":{9},'
        template += '"VMs": {10},'
        template += ' "tasks_points" : {{"task1" : {6}, "task2" : {7}}}}}'
        for info in types:
            for year in join_yr:
                for month in join_mo:
                    for day in join_day:
                        prefix = str(uuid.uuid4())[:7]
                        name = ["employee-%s" % (str(day))]
                        email = ["%s-mail@couchbase.com" % (str(day))]
                        vms = [{"RAM": month, "os": "ubuntu",
                                "name": "vm_%s" % month, "memory": month},
                               {"RAM": month, "os": "windows",
                                "name": "vm_%s"% (month + 1), "memory": month}]
                        generators.append(DocumentGenerator("query-test" + prefix,
                                               template,
                                               name, [year], [month], [day],
                                               email, [info], list(range(1, 10)), list(range(1, 10)),
                                               [float("%s.%s" % (month, month))],
                                               [["skill%s" % y for y in join_yr]], [vms],
                                               start=0, end=docs_per_day))
        self.log.info("Docs are generated.")
        return generators


class DocLoaderCouchbase(DocLoader):
    def __init__(self, servers, cluster):
        super(DocLoaderCouchbase, self).__init__()
        self.servers = servers
        self.master = self.servers[0]
        self.cluster = cluster

    def load(self, generators_load, bucket, exp=0, flag=0,
             kv_store=1, only_store_hash=True, batch_size=1, pause_secs=1,
             timeout_secs=30, op_type='create'):
        gens_load = []
        for generator_load in generators_load:
            gens_load.append(copy.deepcopy(generator_load))
        tasks = []
        items = 0
        for gen_load in gens_load:
                items += (gen_load.end - gen_load.start)

        self.log.info("%s %s to %s documents..." % (op_type, items, bucket.name))
        tasks.append(self.cluster.async_load_gen_docs(self.master, bucket.name,
                                             gens_load,
                                             bucket.kvs[kv_store], op_type, exp, flag,
                                             only_store_hash, batch_size, pause_secs,
                                             timeout_secs))
        for task in tasks:
            task.result()
        self.log.info("LOAD IS FINISHED")

class JoinDocLoader(DocLoaderCouchbase):
    def __init__(self, servers, cluster):
        super(JoinDocLoader, self).__init__(servers, cluster)

    def generate_docs(self, docs_per_day, years):
        generators = []
        start = 0
        types = ['Engineer', 'Sales', 'Support']
        join_yr = [2010, 2011]
        join_mo = range(1, 12 + 1)
        join_day = range(1, 28 + 1)
        template = '{{ "name":"{0}", "join_yr":{1}, "join_mo":{2}, "join_day":{3},'
        template += ' "job_title":"{4}", "tasks_ids":{5}}}'
        for info in types:
            for year in join_yr:
                for month in join_mo:
                    for day in join_day:
                        name = ["employee-%s" % (str(day))]
                        tasks_ids = ["test_task-%s" % day, "test_task-%s" % (day + 1)]
                        generators.append(DocumentGenerator("query-test-%s-%s-%s-%s" % (info, year, month, day),
                                               template,
                                               name, [year], [month], [day],
                                               [info], [tasks_ids],
                                               start=start, end=docs_per_day))
        start, end = 0, (28 + 1)
        template = '{{ "task_name":"{0}", "project": "{1}"}}'
        generators.append(DocumentGenerator("test_task", template,
                                            ["test_task-%s" % i for i in range(0, 10)],
                                            ["CB"],
                                            start=start, end=10))
        generators.append(DocumentGenerator("test_task", template,
                                            ["test_task-%s" % i for i in range(10, 20)],
                                            ["MB"],
                                            start=10, end=20))
        generators.append(DocumentGenerator("test_task", template,
                                            ["test_task-%s" % i for i in range(20, end)],
                                            ["IT"],
                                            start=20, end=end))
        return generators

class SabreDocLoader(DocLoaderCouchbase):
    def __init__(self, servers, cluster):
        super(SabreDocLoader, self).__init__(servers, cluster)

    def generate_docs(self, docs_per_day, years):
        return JsonGenerator().generate_docs_sabre(docs_per_day=docs_per_day, years=years)


class Base64DocLoader(DocLoaderCouchbase):
    def __init__(self, servers, cluster):
        super(Base64DocLoader, self).__init__(servers, cluster)

    def generate_docs(self, docs_per_day, years):
        values = ['Engineer', 'Sales', 'Support']
        generators = [Base64Generator('title-', values, start=0, end=docs_per_day)]
        return generators


class NonDocLoader(DocLoaderCouchbase):
    def __init__(self, servers, cluster):
        super(NonDocLoader, self).__init__(servers, cluster)

    def generate_docs(self, docs_per_day, years):
        values = ['Engineer', 'Sales', 'Support']
        generators = [JSONNonDocGenerator('nondoc-', values, start=0, end=docs_per_day)]
        return generators


def initialize_bucket(name, port=None, saslPassword=None):
    if saslPassword:
       return Bucket(name=name, authType="sasl", saslPassword=saslPassword)
    elif port:
       return Bucket(name=name, authType=None, saslPassword=None, port=port)
    else:
       return Bucket(name=name, authType="sasl", saslPassword=None)

class DocLoaderDirectory(DocLoader):
    def __init__(self, server, directory, bucket_name):
        super(DocLoaderDirectory, self).__init__()
        self.directory = directory
        self.server = server
        self.bucket_name = bucket_name

    def load(self, generators_load):
        gens_load = []
        for generator_load in generators_load:
            gens_load.append(copy.deepcopy(generator_load))
        items = 0
        for gen_load in gens_load:
            items += (gen_load.end - gen_load.start)
        shell = RemoteMachineShellConnection(self.server)
        try:
            self.log.info("Delete directory's content %s/data/default/%s ..." % (self.directory, self.bucket_name))
            shell.execute_command('rm -rf %s/data/default/*' % self.directory)
            self.log.info("Create directory %s/data/default/%s..." % (self.directory, self.bucket_name))
            shell.execute_command('mkdir -p %s/data/default/%s' % (self.directory, self.bucket_name))
            self.log.info("Load %s documents to %s/data/default/%s..." % (items, self.directory, self.bucket_name))
            for gen_load in gens_load:
                for i in range(gen_load.end):
                    key, value = next(gen_load)
                    out = shell.execute_command("echo '%s' > %s/data/default/%s/%s.json" % (value, self.directory,
                                                                                            self.bucket_name, key))
            self.log.info("LOAD IS FINISHED")
        finally:
            shell.disconnect()

def main():
    try:
        (opts, args) = getopt.getopt(sys.argv[1:], 'hi:p', [])
        for o, a in opts:
            if o == "-h":
                usage()

        input = TestInput.TestInputParser.get_test_input(sys.argv)
        if not input.servers:
            usage("ERROR: no servers specified. Please use the -i parameter.")
    except IndexError:
        usage()
    except getopt.GetoptError as error:
        usage("ERROR: " + str(error))

    docs_per_day = input.param("doc_per_day", 49)
    years = input.param("years", 2)
    bucket_name = input.param("bucket_name", "default")
    bucket_port = input.param("bucket_port", None)
    bucket_sasl_pass = input.param("bucket_sasl_pass", None)
    flag = input.param("flags", 0)
    to_directory = input.param("to_dir", '')
    loader_type = input.param("loader_type", 'default')

    if to_directory:
        loader = DocLoaderDirectory(input.servers[0], to_directory, bucket_name)
        generators_load = loader.generate_docs(docs_per_day, years)
        loader.load(generators_load)
    else:
        cluster = Cluster()
        try:
            bucket = initialize_bucket(bucket_name, bucket_port, bucket_sasl_pass)
            if loader_type == 'default':
                loader = DocLoaderCouchbase(input.servers, cluster)
            elif loader_type == 'join':
                loader = JoinDocLoader(input.servers, cluster)
            elif loader_type == 'sabre':
                loader = SabreDocLoader(input.servers, cluster)
            elif loader_type == 'base64':
                loader = Base64DocLoader(input.servers, cluster)
            elif loader_type== 'nondoc':
                loader = NonDocLoader(input.servers, cluster)
            generators_load = loader.generate_docs(docs_per_day, years)
            loader.load(generators_load, bucket, flag=flag)
        finally:
            cluster.shutdown()

if __name__ == "__main__":
    main()
