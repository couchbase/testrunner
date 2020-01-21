import time
import logger

from basetestcase import BaseTestCase
from memcacheConstants import ERR_NOT_FOUND
from castest.cas_base import CasBaseTest
from couchbase_helper.documentgenerator import BlobGenerator
from mc_bin_client import MemcachedError

from membase.api.rest_client import RestConnection, RestHelper
from memcached.helper.data_helper import VBucketAwareMemcached, MemcachedClientHelper
from ep_mc_bin_client import MemcachedClient, MemcachedError
import json
from lib.couchbase_helper.tuq_generators import JsonGenerator

from remote.remote_util import RemoteMachineShellConnection


"""

Capture basic get, set operations, also the meta operations. This is based on some 4.1.1 test which had separate
bugs with incr and delete with meta and I didn't see an obvious home for them.

This is small now but we will reactively add things

These may be parameterized by:
   - full and value eviction
   - DGM and non-DGM



"""



class basic_ops(BaseTestCase):

    def setUp(self):
        super(basic_ops, self).setUp()
        self.src_bucket = RestConnection(self.master).get_buckets()

    def tearDown(self):
        super(basic_ops, self).tearDown()

    def do_basic_ops(self):

        KEY_NAME = 'key1'
        KEY_NAME2 = 'key2'
        CAS = 1234
        self.log.info('Starting basic ops')


        rest = RestConnection(self.master)
        client = VBucketAwareMemcached(rest, 'default')
        mcd = client.memcached(KEY_NAME)

        # MB-17231 - incr with full eviction
        rc = mcd.incr(KEY_NAME, 1)
        print('rc for incr', rc)



        # MB-17289 del with meta


        rc = mcd.set(KEY_NAME, 0, 0, json.dumps({'value':'value2'}))
        print('set is', rc)
        cas = rc[1]


        # wait for it to persist
        persisted = 0
        while persisted == 0:
                opaque, rep_time, persist_time, persisted, cas = client.observe(KEY_NAME)


        try:
            rc = mcd.evict_key(KEY_NAME)

        except MemcachedError as exp:
            self.fail("Exception with evict meta - {0}".format(exp) )

        CAS = 0xabcd
        # key, value, exp, flags, seqno, remote_cas

        try:
            #key, exp, flags, seqno, cas
            rc = mcd.del_with_meta(KEY_NAME2, 0, 0, 2, CAS)



        except MemcachedError as exp:
            self.fail("Exception with del_with meta - {0}".format(exp) )

    # Reproduce test case for MB-28078
    def do_setWithMeta_twice(self):

        mc = MemcachedClient(self.master.ip, 11210)
        mc.sasl_auth_plain(self.master.rest_username, self.master.rest_password)
        mc.bucket_select('default')

        try:
            mc.setWithMeta('1', '{"Hello":"World"}', 3600, 0, 1, 0x1512a3186faa0000)
        except MemcachedError as error:
            self.log.info("<MemcachedError #%d ``%s''>" % (error.status, error.message))
            self.fail("Error on First setWithMeta()")

        stats = mc.stats()
        self.log.info('curr_items: {} and curr_temp_items:{}'.format(stats['curr_items'], stats['curr_temp_items']))
        self.log.info("Sleeping for 5 and checking stats again")
        time.sleep(5)
        stats = mc.stats()
        self.log.info('curr_items: {} and curr_temp_items:{}'.format(stats['curr_items'], stats['curr_temp_items']))

        try:
            mc.setWithMeta('1', '{"Hello":"World"}', 3600, 0, 1, 0x1512a3186faa0000)
        except MemcachedError as error:
            stats = mc.stats()
            self.log.info('After 2nd setWithMeta(), curr_items: {} and curr_temp_items:{}'.format(stats['curr_items'], stats['curr_temp_items']))
            if int(stats['curr_temp_items']) == 1:
                self.fail("Error on second setWithMeta(), expected curr_temp_items to be 0")
            else:
                self.log.info("<MemcachedError #%d ``%s''>" % (error.status, error.message))

    def generate_docs_bigdata(self, docs_per_day, start=0, document_size=1024000):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_bigdata(end=docs_per_day, start=start, value_size=document_size)

    def test_large_doc_size_1MB(self):
        # bucket size =256MB, when Bucket gets filled 236MB then the test starts failing
        # document size =2MB, No of docs = 221 , load 250 docs
        # epengine.basic_ops.basic_ops.test_large_doc_size_1MB,skip_cleanup=True,document_size=1024000,dgm_run=True
        docs_per_day = 10
        document_size = self.input.param('document_size')
        # generate docs with size >=  1MB , See MB-29333
        gens_load = self.generate_docs_bigdata(docs_per_day=(25 * docs_per_day), document_size=document_size)
        self.load(gens_load, buckets=self.src_bucket, verify_data=False, batch_size=10)

        # check if all the documents(250) are loaded else the test has failed with "Memcached Error 134"
        mc = MemcachedClient(self.master.ip, 11210)
        mc.sasl_auth_plain(self.master.rest_username, self.master.rest_password)
        mc.bucket_select('default')
        stats = mc.stats()
        self.assertEqual(int(stats['curr_items']), 250)


    def test_large_doc_size_2MB(self):
        # bucket size =256MB, when Bucket gets filled 236MB then the test starts failing
        # document size =2MB, No of docs = 108 , load 150 docs
        # epengine.basic_ops.basic_ops.test_large_doc_size_2MB,skip_cleanup = True,document_size=2048000,dgm_run=True
        docs_per_day = 5
        document_size = self.input.param('document_size')
        # generate docs with size >=  1MB , See MB-29333
        gens_load = self.generate_docs_bigdata(docs_per_day=(25 *docs_per_day), document_size=document_size)
        self.load(gens_load, buckets=self.src_bucket, verify_data=False, batch_size=10)

        # check if all the documents(125) are loaded else the test has failed with "Memcached Error 134"
        mc = MemcachedClient(self.master.ip, 11210)
        mc.sasl_auth_plain(self.master.rest_username, self.master.rest_password)
        mc.bucket_select('default')
        stats = mc.stats()
        self.assertEqual(int(stats['curr_items']), 125)

    def test_large_doc_20MB(self):
        # test reproducer for MB-29258,
        # Load a doc which is greater than 20 MB with compression enabled and check if it fails
        # check with compression_mode as active, passive and off
        document_size= self.input.param('document_size', 20)
        gens_load = self.generate_docs_bigdata(docs_per_day=1, document_size=(document_size * 1024000))
        self.load(gens_load, buckets=self.src_bucket, verify_data=False, batch_size=10)

        mc = MemcachedClient(self.master.ip, 11210)
        mc.sasl_auth_plain(self.master.rest_username, self.master.rest_password)
        mc.bucket_select('default')
        stats = mc.stats()
        if (document_size > 20):
            self.assertEqual(int(stats['curr_items']), 0) # failed with error "Data Too Big" when document size > 20MB
        else:
            self.assertEqual(int(stats['curr_items']), 1)
            gens_update = self.generate_docs_bigdata(docs_per_day=1, document_size=(21 * 1024000))
            self.load(gens_update, buckets=self.src_bucket, verify_data=False, batch_size=10)
            stats = mc.stats()
            self.assertEqual(int(stats['curr_items']), 1)

    def test_diag_eval_curl(self):
        # Check if diag/eval can be done only by local host
        # epengine.basic_ops.basic_ops.test_diag_eval_curl,disable_diag_eval_non_local=True

        port = self.master.port

        # check if local host can work fine
        cmd=[]
        cmd_base = 'curl http://{0}:{1}@localhost:{2}/diag/eval '.format(self.master.rest_username,
                                                                         self.master.rest_password, port)
        command = cmd_base + '-X POST -d \'os:cmd("env")\''
        cmd.append(command)
        command = cmd_base + '-X POST -d \'case file:read_file("/etc/passwd") of {ok, B} -> io:format("~p~n", [binary_to_term(B)]) end.\''
        cmd.append(command)

        shell = RemoteMachineShellConnection(self.master)
        for command in cmd:
            output, error = shell.execute_command(command)
            self.assertNotEqual("API is accessible from localhost only", output[0])

        # Disable allow_nonlocal_eval
        if self.disable_diag_eval_on_non_local_host:
            command = cmd_base + '-X POST -d \'ns_config:set(allow_nonlocal_eval, false).\''
            output, error = shell.execute_command(command)

        # check ip address on diag/eval will not work fine when allow_nonlocal_eval is disabled
        cmd=[]
        cmd_base = 'curl http://{0}:{1}@{2}:{3}/diag/eval '.format(self.master.rest_username,
                                                                   self.master.rest_password, self.master.ip, port)
        command = cmd_base + '-X POST -d \'os:cmd("env")\''
        cmd.append(command)
        command = cmd_base + '-X POST -d \'case file:read_file("/etc/passwd") of {ok, B} -> io:format("~p~n", [binary_to_term(B)]) end.\''
        cmd.append(command)

        for command in cmd:
            output, error = shell.execute_command(command)
            if self.disable_diag_eval_on_non_local_host:
                self.assertEqual("API is accessible from localhost only", output[0])
            else:
                self.assertNotEqual("API is accessible from localhost only", output[0])

    def verify_stat(self,items, value="active" ):
        mc = MemcachedClient(self.master.ip, 11210)
        mc.sasl_auth_plain(self.master.rest_username, self.master.rest_password)
        mc.bucket_select('default')
        stats = mc.stats()
        self.assertEqual(stats['ep_compression_mode'], value)
        self.assertEqual(int(stats['ep_item_compressor_num_compressed']), items)
        self.assertNotEqual(int(stats['vb_active_itm_memory']), int(stats['vb_active_itm_memory_uncompressed']))

    def test_compression_active_and_off(self):
        '''
        test reproducer for MB-29272,
        Load some documents with compression mode set to active
        get the cbstats
        change compression mode to off and wait for minimum 250ms
        Load some more documents and check the compression is not done
        epengine.basic_ops.basic_ops.test_compression_active_and_off,items=10000,compression_mode=active

        :return:
        '''
        # Load some documents with compression mode as active
        gen_create = BlobGenerator('eviction', 'eviction-', self.value_size, end=self.num_items)
        gen_create2 = BlobGenerator('eviction2', 'eviction2-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen_create, "create", 0)
        self._wait_for_stats_all_buckets(self.servers[:self.nodes_init])
        self._verify_stats_all_buckets(self.servers[:self.nodes_init])

        remote = RemoteMachineShellConnection(self.master)
        for bucket in self.buckets:
            # Verify the stat or compression_mode active and compressed items should be 10000
            self.verify_stat(items=self.num_items)

            # change compression mode to off
            output, _ = remote.execute_couchbase_cli(cli_command='bucket-edit',
                                                     cluster_host="localhost:8091",
                                                     user=self.master.rest_username,
                                                     password=self.master.rest_password,
                                                     options='--bucket=%s --compression-mode off' % bucket.name)
            self.assertTrue(' '.join(output).find('SUCCESS') != -1, 'compression mode set to off')

            # sleep for 10 sec (minimum 250sec)
            time.sleep(10)

        # Load data and check stats to see compression is not done for newly added data
        self._load_all_buckets(self.master, gen_create2, "create", 0)
        self._wait_for_stats_all_buckets(self.servers[:self.nodes_init])
        self._verify_stats_all_buckets(self.servers[:self.nodes_init])

        for bucket in self.buckets:
            # Verify the stat for compression_mode off and compressed items should be still 10000
            self.verify_stat(items=self.num_items, value="off")


    def do_get_random_key(self):
        # MB-31548, get_Random key gets hung sometimes.
        mc = MemcachedClient(self.master.ip, 11210)
        mc.sasl_auth_plain(self.master.rest_username, self.master.rest_password)
        mc.bucket_select('default')

        count = 0
        while (count < 1000000):
            count = count +1
            try:
                mc.get_random_key()
            except MemcachedError as error:
                self.fail("<MemcachedError #%d ``%s''>" % (error.status, error.message))
            if count % 1000 == 0:
                self.log.info('The number of iteration is {}'.format(count))

