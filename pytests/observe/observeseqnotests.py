import copy
import json

from basetestcase import BaseTestCase
from membase.helper.cluster_helper import ClusterOperationHelper
from mc_bin_client import MemcachedClient, MemcachedError


from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.rebalance_helper import RebalanceHelper
from memcached.helper.data_helper import VBucketAwareMemcached, MemcachedClientHelper

import memcacheConstants
import struct
import time
import traceback





class ObserveSeqNoTests(BaseTestCase):
    def setUp(self):
        self._cleanup_nodes = []
        self._failed_nodes = []
        super(ObserveSeqNoTests, self).setUp()



    def observe_seqno_response_to_dict(self, resp ):
        format_type = 'no_failover' if resp[1] == 0 else 'hard_failover'
        return {'format_type':format_type, 'vbucket_id':resp[2], 'new_vbucket_uuid':resp[3], 'last_persisted_seqno':resp[4],
                 'current_seqno':resp[5], 'old_vbucket_uuid':resp[6] ,'last_seqno_received':resp[7]}





    def extract_vbucket_uuid_and_seqno(self,  results):

        vbucket_uuid = struct.unpack('>Q', results[2][0:8])[0]
        seqno = struct.unpack('>Q', results[2][8:16])[0]
        return {'vbucket_uuid':vbucket_uuid, 'seqno':seqno}


    """ check_results - verify the failover type, verify the sequence number and persisted sequence numbers match,
    and depending on the failover type verify the vbucket_uuid

    """

    def check_results(self, op_data, actual_data):


        self.assertTrue(op_data['format_type'] == actual_data['format_type'],
           msg='Failover type does not match. Expected: {0}. Actual {1}'.format(
                    op_data['format_type'], actual_data['format_type']) )


        if op_data['format_type'] == 'no_failover':
            self.assertTrue(op_data['vbucket_uuid'] == actual_data['new_vbucket_uuid'],
               msg='Observe Vbucket uuid does not match. Expected: {0}. Actual {1}'.format(
                        hex(op_data['vbucket_uuid']), hex(actual_data['new_vbucket_uuid'])) )
        else:
            self.assertTrue(op_data['vbucket_uuid'] == actual_data['old_vbucket_uuid'],
               msg='Observe Vbucket uuid does not match. Expected: {0}. Actual {1}'.format(
                        hex(op_data['vbucket_uuid']), hex(actual_data['old_vbucket_uuid'])) )

        self.assertTrue(op_data['seqno'] == actual_data['current_seqno'],
           msg='Observe seqno does not match. Expected: {0}. Actual {1}'.format(
                    op_data['seqno'], actual_data['current_seqno']) )


        self.assertTrue(op_data['seqno'] == actual_data['last_persisted_seqno'],
           msg='Persisted seqno does not match. Expected: {0}. Actual {1}'.format(
                    op_data['seqno'], actual_data['last_persisted_seqno']) )



    def verify_vbucket_and_seqno(self, first_resp, second_resp, op ):

        self.assertTrue( first_resp['vbucket_uuid'] == second_resp['vbucket_uuid'],
                         msg='For ' + op + ' the vbucket uuid did not match. Expected {0}, actual {1}'.format(
                             hex( first_resp['vbucket_uuid']), hex(second_resp['vbucket_uuid']) ) )

        self.assertTrue( first_resp['seqno'] < second_resp['seqno'],
                         msg='For ' + op + ' the seqno did not increase. First {0}, second {1}'.format(
                             first_resp['seqno'], second_resp['seqno'] ) )




    # Verify that the set, add, delete, replace, append, prepend, incr, decr, setWithMeta and delWithMeta
    # commands all return a vbucket uuid and seq no when the feature is enabled. Verify that the
    # vbucket_uuid is the same and that the sequence number is increasing
    # Note that when the Python SDK has these calls implemented I will use it but for now I am
    # using the mc_bin_client

    def test_new_response_fields(self):

        self.log.info('\n\nStarting test_new_response_fields')

        client = VBucketAwareMemcached(RestConnection(self.master), 'default')


        h = client.sendHellos( memcacheConstants.PROTOCOL_BINARY_FEATURE_MUTATION_SEQNO );



        set_resp = self.extract_vbucket_uuid_and_seqno( client.set('test1key', 0, 0, '123456789') )

        # test the inplace operations
        test = client.generic_request(client.memcached('test1key').set, 'test1key', 0, 0, 'totally new value')
        replace_resp = self.extract_vbucket_uuid_and_seqno(
            client.generic_request( client.memcached('test1key').replace,  'test1key', 0, 0, 'totally new value') )
        self.verify_vbucket_and_seqno( set_resp, replace_resp, 'replace')

        append_resp = self.extract_vbucket_uuid_and_seqno(
            client.generic_request( client.memcached('test1key').append, 'test1key', 'appended data') )
        self.verify_vbucket_and_seqno(replace_resp, append_resp, 'append')

        prepend_resp = self.extract_vbucket_uuid_and_seqno(
            client.generic_request( client.memcached('test1key').prepend, 'test1key', 'prepended data') )
        self.verify_vbucket_and_seqno(append_resp, prepend_resp, 'prepend')


        # and finally do the delete
        delete_resp = self.extract_vbucket_uuid_and_seqno(
            client.generic_request( client.memcached('test1key').delete, 'test1key') )
        self.verify_vbucket_and_seqno( set_resp, delete_resp, 'delete')


        #meta commands under construction
        # test the 'meta' commands
        TEST_SEQNO = 123
        TEST_CAS = 456

        set_with_meta_resp = client.generic_request(
            client.memcached('test1keyformeta').set_with_meta, 'test1keyformeta', 0, 0, TEST_SEQNO, TEST_CAS, '123456789')
        set_meta_vbucket_uuid, set_meta_seqno = struct.unpack('>QQ', set_with_meta_resp[2])
        set_with_meta_dict = {'vbucket_uuid':set_meta_vbucket_uuid, 'seqno': set_meta_seqno}



        get_meta_resp = client.generic_request(client.memcached( 'test1keyformeta').getMeta, 'test1keyformeta')
        self.assertTrue(TEST_SEQNO == get_meta_resp[3], \
               msg='get meta seqno does not match as set. Expected {0}, actual {1}'.format(TEST_SEQNO, get_meta_resp[3]) )
        self.assertTrue(TEST_CAS == get_meta_resp[4], \
               msg='get meta cas does not match as set. Expected {0}, actual {1}'.format(TEST_CAS, get_meta_resp[4]) )


        #   def del_with_meta(self, key, exp, flags, seqno, old_cas, new_cas, vbucket= -1):
        del_with_meta_resp = client.generic_request(
            client.memcached('test1keyformeta').del_with_meta, 'test1keyformeta', 0, 0, TEST_SEQNO, TEST_CAS, TEST_CAS+1)
        vbucket_uuid, seqno = struct.unpack('>QQ', del_with_meta_resp[2])
        del_with_meta_dict = {'vbucket_uuid':vbucket_uuid, 'seqno': seqno}

        self.verify_vbucket_and_seqno( set_with_meta_dict, del_with_meta_dict, 'set/del with meta')





        #  do some integer operations
        set_resp = self.extract_vbucket_uuid_and_seqno( client.set('key-for-integer-value', 0, 0, '123') )
        incr_resp = client.generic_request(client.memcached('key-for-integer-value').incr, 'key-for-integer-value')
        incr_resp_dict = {'vbucket_uuid':incr_resp[2], 'seqno':incr_resp[3]}
        self.verify_vbucket_and_seqno(set_resp, incr_resp_dict, 'incr')


        decr_resp = client.generic_request(client.memcached('key-for-integer-value').decr, 'key-for-integer-value')
        decr_resp_dict = {'vbucket_uuid':decr_resp[2], 'seqno':decr_resp[3]}
        self.verify_vbucket_and_seqno(incr_resp_dict, decr_resp_dict, 'decr')


        add_resp = self.extract_vbucket_uuid_and_seqno(
            client.generic_request( client.memcached('totally new key').add, 'totally new key', 0, 0, 'totally new value') )

        self.assertTrue( add_resp['vbucket_uuid'] > 0, msg='Add request vbucket uuid is zero')

        self.log.info('\n\nComplete test_new_response_fields\n\n')


    """
     test_basic_operations - persisted values are correct, error detection
    """

    def test_basic_operations(self):
        self.log.info('\n\nStarting test_basic_operations')


        client = VBucketAwareMemcached(RestConnection(self.master), 'default')
        h = client.sendHellos( memcacheConstants.PROTOCOL_BINARY_FEATURE_MUTATION_SEQNO );

        all_clients = []
        for s in self.servers:
            all_clients.append( MemcachedClientHelper.direct_client(s, self.default_bucket_name))


        # basic case
        op_data = self.extract_vbucket_uuid_and_seqno( client.set('test1key', 0, 0, 'test1value') )
        op_data['format_type'] = 'no_failover'


        for s in self.servers:
           RebalanceHelper.wait_for_persistence(s, self.default_bucket_name)

        o = client.observe_seqno('test1key', op_data['vbucket_uuid'])
        results = self.observe_seqno_response_to_dict( o )
        self.check_results( op_data, results)



         # 2. Disable persistence, Set a key, observe seqno, should not be persisted,
         #    enable and wait for persistence, observe seqno and check everything


        self.log.info('\n\nVerify responses are correct when keys are not persisted')
        for i in all_clients:
            i.stop_persistence()
        mc = MemcachedClientHelper.direct_client(self.master, "default")


        self.log.info('setting the kv')
        op_data = self.extract_vbucket_uuid_and_seqno( client.set('test2key', 0, 0, 'test2value') )
        op_data['format_type'] = 'no_failover'

        self.log.info('calling observe seq no')
        o = client.observe_seqno('test2key', op_data['vbucket_uuid'])
        results = self.observe_seqno_response_to_dict( o )
        # can't use check results because persisted is
        self.assertTrue(op_data['vbucket_uuid'] == results['new_vbucket_uuid'],
           msg='Observe Vbucket uuid does not match. Expected: {0}. Actual {1}'.format(
                    hex(op_data['vbucket_uuid']), hex(results['new_vbucket_uuid'])) )

        self.assertTrue(op_data['seqno'] == results['current_seqno'],
           msg='Observe seqno does not match. Expected: {0}. Actual {1}'.format(
                    op_data['seqno'], results['current_seqno']) )


        self.assertTrue(op_data['seqno'] > results['last_persisted_seqno'],
           msg='Persisted seqno is too big. Expected: {0}. Actual {1}'.format(
                    op_data['seqno'], results['last_persisted_seqno']) )



        self.log.info('starting persistence')

        for s in all_clients:
            s.start_persistence()

        for s in self.servers:
            RebalanceHelper.wait_for_persistence(s, self.default_bucket_name)

        results = self.observe_seqno_response_to_dict( client.observe_seqno('test2key', op_data['vbucket_uuid']) )
        self.check_results( op_data, results)



        # error case - broken
        """
        mc.set('badbuckettestkey', 0, 0, 'testvalue',1)
        try:
           o = client.observe_seqno('badbuckettestkey', 2)
           self.fail('bucket is incorrect, should have returned an error')
        except AssertionError, ex:    # this is to catch the above fail, should it ever happen
            raise
        except Exception, ex:
            traceback.print_exc()
            if ex.status != memcacheConstants.ERR_NOT_FOUND:
                self.log.info('Observe seqno incorrect error code for invalid bucket. Expected: {0}. Actual {1}'.format(
                  memcacheConstants.ERR_NOT_FOUND, ex.status))
                raise Exception(ex)
        """



        self.log.info('\n\nComplete test_basic_operations')


    """ failover testing
    """

    def test_failover(self):

        self.log.info('\n\nStarting test_failover')

        client = VBucketAwareMemcached(RestConnection(self.master), 'default')
        h = client.sendHellos( memcacheConstants.PROTOCOL_BINARY_FEATURE_MUTATION_SEQNO );


        self.log.info('\n\nVerify responses are correct after graceful failover')

        op_data = self.extract_vbucket_uuid_and_seqno( client.set('failoverkey', 0, 0, 'failovervalue') )
        op_data['format_type'] = 'no_failover'



        # don't really need to do this so it is commented
        #pre_failover_results = self.observe_seqno_response_to_dict( client.observe_seqno('failoverkey', vbucket_uuid) )




        # which server did the key go to and gracefully fail that server

        self.log.info('\n\nstarting graceful failover scenario')
        server_with_key = client.memcached( 'failoverkey').host
        self.log.info('\n\nserver {0} has the key and it will be failed over'.format(server_with_key))


        RebalanceHelper.wait_for_persistence(self.master, self.default_bucket_name)

        # now failover
        RestConnection(self.master).fail_over(otpNode = 'ns_1@' + server_with_key, graceful=True)

        if server_with_key in self.servers:
            self.servers.remove(server_with_key)



        self.log.info('server should be failed over now')

        time.sleep(5)
        # reinstantiate the client so we get the new view of the world
        client = VBucketAwareMemcached(RestConnection(self.master), 'default')
        server_with_key = client.memcached( 'failoverkey').host
        self.log.info('\n\nkey is now on server {0}'.format(server_with_key))

        after_failover_results = self.observe_seqno_response_to_dict(
            client.observe_seqno('failoverkey', op_data['vbucket_uuid']) )


        # verify: no (hard) failover, everything else as before
        self.check_results( op_data, after_failover_results)
        self.log.info('Test complete')






        # now do a hard failover

        # which server did the key go to and gracefully fail that server

        time.sleep(30)
        self.log.info('\n\nstarting hard failover scenario')

        client.sendHellos( memcacheConstants.PROTOCOL_BINARY_FEATURE_MUTATION_SEQNO );
        op_data = self.extract_vbucket_uuid_and_seqno( client.set('hardfailoverkey', 0, 0, 'failovervalue') )
        op_data['format_type'] = 'hard_failover'


        server_with_key = client.memcached( 'hardfailoverkey').host
        self.log.info('\n\nserver {0} has the key and it will be hard failed over'.format(server_with_key))


        # now failover
        RestConnection(self.master).fail_over(otpNode = 'ns_1@' + server_with_key, graceful=False)

        if server_with_key in self.servers:
            self.servers.remove(server_with_key)



        self.log.info('\n\nserver should be failed over now')

        time.sleep(10)
        # reinstantiate the client so we get the new view of the world
        client = VBucketAwareMemcached(RestConnection(self.master), 'default')
        server_with_key = client.memcached( 'hardfailoverkey').host
        self.log.info('\n\nkey is now on server {0}'.format(server_with_key))

        time.sleep(10)

        after_failover_results = self.observe_seqno_response_to_dict(
            client.observe_seqno('hardfailoverkey', op_data['vbucket_uuid']) )

        self.check_results( op_data, after_failover_results)

        self.log.info('Test complete')

    def test_CASnotzero(self):
        # MB-31149
        # observe.observeseqnotests.ObserveSeqNoTests.test_CASnotzero
        # set value, append and check CAS value
        self.log.info('Starting test_CASnotzero')

        # without hello(mutationseqencenumber)
        client = VBucketAwareMemcached(RestConnection(self.master), 'default')
        KEY_NAME = "test1key"
        client.set(KEY_NAME, 0, 0, json.dumps({'value':'value2'}))
        client.generic_request(client.memcached(KEY_NAME).append, 'test1key', 'appended data')
        get_meta_resp = client.generic_request(client.memcached(KEY_NAME).getMeta, 'test1key')
        self.log.info('the CAS value without hello(mutationseqencenumber): {} '.format(get_meta_resp[4]))
        self.assertNotEqual(get_meta_resp[4], 0)
        
        # with hello(mutationseqencenumber)
        KEY_NAME = "test2key"
        client.set(KEY_NAME, 0, 0, json.dumps({'value':'value1'}))
        h = client.sendHellos(memcacheConstants.PROTOCOL_BINARY_FEATURE_MUTATION_SEQNO)
        client.generic_request(client.memcached(KEY_NAME).append, 'test2key', 'appended data456')

        get_meta_resp = client.generic_request(client.memcached(KEY_NAME).getMeta, 'test2key')
        self.log.info('the CAS value with hello(mutationseqencenumber): {} '.format(get_meta_resp[4]))
        self.assertNotEqual(get_meta_resp[4], 0)

    def test_appendprepend(self):
        # MB-32078, Append with CAS=0 can return ENGINE_KEY_EEXISTS
        # observe.observeseqnotests.ObserveSeqNoTests.test_appendprepend
        self.log.info('Starting test_appendprepend')
        TEST_SEQNO = 123
        TEST_CAS = 456
        KEY_NAME='test_appendprepend'

        client = VBucketAwareMemcached(RestConnection(self.master), 'default')
        # create a value with CAS
        client.generic_request(
            client.memcached(KEY_NAME).set_with_meta, KEY_NAME, 0, 0, TEST_SEQNO, TEST_CAS, '123456789')
        get_resp = client.generic_request(client.memcached(KEY_NAME).get, KEY_NAME)
        self.assertEqual(get_resp[2], '123456789')

        # check if the changing the value fails
        try:
            client.generic_request(
            client.memcached(KEY_NAME).set_with_meta, KEY_NAME, 0, 0, 0, 0, 'value')
            self.fail('Expected to fail but passed')
        except MemcachedError as exp:
            self.assertEqual(int(exp.status), 2)

        # check if append works fine
        client.generic_request(client.memcached(KEY_NAME).append, KEY_NAME, 'appended data')
        get_resp = client.generic_request(client.memcached(KEY_NAME).get, KEY_NAME)
        self.assertEqual(get_resp[2], '123456789appended data')

        # check if prepend works fine and verify the data
        client.generic_request(client.memcached(KEY_NAME).prepend, KEY_NAME, 'prepended data')
        get_resp = client.generic_request(client.memcached(KEY_NAME).get, KEY_NAME)
        self.assertEqual(get_resp[2], 'prepended data123456789appended data')






