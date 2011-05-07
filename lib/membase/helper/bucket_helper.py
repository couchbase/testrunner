import time
import uuid
import zlib
import logger
import mc_bin_client
import crc32
import socket
import ctypes
from membase.api.rest_client import RestConnection, RestHelper
from memcached.helper.data_helper import MemcachedClientHelper


class BucketOperationHelper():

    #this function will assert
    @staticmethod
    def create_default_buckets(servers,number_of_replicas=1,assert_on_test = None):
        log = logger.Logger.get_logger()
        for serverInfo in servers:
            ip_rest = RestConnection(serverInfo)
            ip_rest.create_bucket(bucket='default',
                               ramQuotaMB=256,
                               replicaNumber=number_of_replicas,
                               proxyPort=11220)
            msg = 'create_bucket succeeded but bucket "default" does not exist'
            removed_all_buckets = BucketOperationHelper.wait_for_bucket_creation('default', ip_rest)
            if not removed_all_buckets:
                log.error(msg)
                if assert_on_test:
                    assert_on_test.fail(msg=msg)


    @staticmethod
    def delete_all_buckets_or_assert(servers, test_case):
        log = logger.Logger.get_logger()
        log.info('deleting existing buckets on {0}'.format(servers))
        for serverInfo in servers:
            rest = RestConnection(serverInfo)
            buckets = []
            try:
                buckets = rest.get_buckets()
            except:
                log.info('15 seconds sleep before calling get_buckets again...')
                time.sleep(15)
                buckets = rest.get_buckets()
            for bucket in buckets:
                print bucket.name
                rest.delete_bucket(bucket.name)
                log.info('deleted bucket : {0} from {1}'.format(bucket.name,serverInfo.ip))
                msg = 'bucket "{0}" was not deleted even after waiting for two minutes'.format(bucket.name)
                test_case.assertTrue(BucketOperationHelper.wait_for_bucket_deletion(bucket.name, rest, 200)
                                     , msg=msg)
        log.info('sleeping for 10 seconds because we want to :)')
        time.sleep(10)

    #TODO: TRY TO USE MEMCACHED TO VERIFY BUCKET DELETION BECAUSE
    # BUCKET DELETION IS A SYNC CALL W.R.T MEMCACHED
    @staticmethod
    def wait_for_bucket_deletion(bucket,
                                 rest,
                                 timeout_in_seconds=120):
        log = logger.Logger.get_logger()
        log.info('waiting for bucket deletion to complete....')
        start = time.time()
        helper = RestHelper(rest)
        while (time.time() - start) <= timeout_in_seconds:
            if not helper.bucket_exists(bucket):
                return True
            else:
                time.sleep(2)
        return False

    @staticmethod
    def wait_for_bucket_creation(bucket,
                                 rest,
                                 timeout_in_seconds=120):
        log = logger.Logger.get_logger()
        log.info('waiting for bucket creation to complete....')
        start = time.time()
        helper = RestHelper(rest)
        while (time.time() - start) <= timeout_in_seconds:
            if helper.bucket_exists(bucket):
                return True
            else:
                time.sleep(2)
        return False

    #try to insert key in all vbuckets before returning from this function
    @staticmethod
    def wait_till_memcached_is_ready_or_assert(servers,
                                               bucket_port,
                                               test,
                                               bucket_name = 'default',
                                               bucket_password='password'):
        log = logger.Logger.get_logger()

        for serverInfo in servers:
            msg = "waiting for memcached bucket : {0} in {1}:{2} to accept set ops"
            log.info(msg.format(bucket_name, serverInfo.ip, bucket_port))
            inserted_keys = []
            start_time = time.time()
            warmed_up_vBuckets = []
            client = None
            while time.time() <= (start_time + 120):
                key = '{0}'.format(uuid.uuid4())
                vBucketId = crc32.crc32_hash(key) & 1023 # or & 0x3FF
                try:
                    client = MemcachedClientHelper.create_memcached_client(serverInfo.ip,
                                                                           bucket_name,
                                                                           bucket_port,
                                                                           bucket_password)
                    client.vbucketId = vBucketId
                    client.set(key, 0, 0, key)
                    inserted_keys.append(key)
                    if not vBucketId in warmed_up_vBuckets:
                        warmed_up_vBuckets.append(vBucketId)
                    if len(warmed_up_vBuckets) == 1024:
                        break
                except mc_bin_client.MemcachedError as error:
                    msg = "memcached not ready yet .. (memcachedError : {0}) - unable to push key : {1} to bucket : {2}"
                    log.error(msg.format(error.status, key, vBucketId))
                    time.sleep(3)
                except Exception as ex:
                    log.error("general error : {0} while setting key ".format(ex))
                    time.sleep(3)
            if client:
                client.flush()
                client.close()
            if len(warmed_up_vBuckets) < 1:
                test.fail('memcached not ready for {0} after waiting for 5 minutes'.format(serverInfo.ip))
            else:
                log.info("inserted {0} keys to all {1} vBuckets".format(len(inserted_keys),len(warmed_up_vBuckets)))


    @staticmethod
    def verify_data(ip, keys, value_equal_to_key,verify_flags, port, test):
        log = logger.Logger.get_logger()
        #verify all the keys
        client = mc_bin_client.MemcachedClient(ip, port)
        #populate key
        index = 0
        all_verified = True
        keys_failed = []
        for key in keys:
            try:
                index += 1
                vbucketId = crc32.crc32_hash(key) & 1023 # or & 0x3FF
                client.vbucketId = vbucketId
                flag, keyx, value = client.get(key=key)
                if value_equal_to_key:
                    test.assertEquals(value, key, msg='values dont match')
                if verify_flags:
                    actual_flag = socket.ntohl(flag)
                    expected_flag = ctypes.c_uint32(zlib.adler32(value)).value
                    test.assertEquals(actual_flag, expected_flag, msg='flags dont match')
                log.info("verified key #{0} : {1}".format(index, key))
            except mc_bin_client.MemcachedError as error:
                log.error(error)
                log.error("memcachedError : {0} - unable to get a pre-inserted key : {0}".format(error.status, key))
                keys_failed.append(key)
                all_verified = False
        client.close()
        log.error('unable to verify #{0} keys'.format(len(keys_failed)))
        return all_verified

    @staticmethod
    def keys_dont_exist(keys,ip,port,test):
        log = logger.Logger.get_logger()
        #verify all the keys
        client = mc_bin_client.MemcachedClient(ip, port)
        #populate key
        for key in keys:
            try:
                vbucketId = crc32.crc32_hash(key) & 1023 # or & 0x3FF
                client.vbucketId = vbucketId
                client.get(key=key)
                client.close()
                log.error('key {0} should not exist in the bucket'.format(key))
                return False
            except mc_bin_client.MemcachedError as error:
                log.error(error)
                log.error("expected memcachedError : {0} - unable to get a pre-inserted key : {1}".format(error.status, key))
        client.close()
        return True

    @staticmethod
    def keys_exist_or_assert(keys,ip,name,port,password,test):
        log = logger.Logger.get_logger()
        #verify all the keys
        client = MemcachedClientHelper.create_memcached_client(ip,
                                                               name,
                                                               port,
                                                               password)
        #populate key
        for key in keys:
            try:
                vbucketId = crc32.crc32_hash(key) & 1023 # or & 0x3FF
                client.vbucketId = vbucketId
                client.get(key=key)
            except mc_bin_client.MemcachedError as error:
                log.error(error)
                if test:
                    test.fail("key {0} does not exist".format(key))
                else:
                    log.error("key {0} does not exist".format(key))
        client.close()
        return True


    @staticmethod
    def load_data_or_assert(serverInfo,
                   fill_ram_percentage=10.0,
                   bucket_name = 'default',
                   port=11211,
                   test = None):
        log = logger.Logger.get_logger()
        if fill_ram_percentage <= 0.0:
            fill_ram_percentage = 5.0
        client = mc_bin_client.MemcachedClient(serverInfo.ip, port)
        #populate key
        rest = RestConnection(serverInfo)
        testuuid = uuid.uuid4()
        info = rest.get_bucket(bucket_name)
        emptySpace = info.stats.ram - info.stats.memUsed
        log.info('emptySpace : {0} fill_ram_percentage : {1}'.format(emptySpace, fill_ram_percentage))
        fill_space = (emptySpace * fill_ram_percentage) / 100.0
        log.info("fill_space {0}".format(fill_space))
        # each packet can be 10 KB
        packetSize = int(10 * 1024)
        number_of_buckets = int(fill_space) / packetSize
        log.info('packetSize: {0}'.format(packetSize))
        log.info('memory usage before key insertion : {0}'.format(info.stats.memUsed))
        log.info('inserting {0} new keys to memcached @ {0}'.format(number_of_buckets, serverInfo.ip))
        keys = ["key_%s_%d" % (testuuid, i) for i in range(number_of_buckets)]
        inserted_keys = []
        for key in keys:
            vbucketId = crc32.crc32_hash(key) & 1023 # or & 0x3FF
            client.vbucketId = vbucketId
            try:
                client.set(key, 0, 0, key)
                inserted_keys.append(key)
            except mc_bin_client.MemcachedError as error:
                log.error(error)
                client.close()
                log.error("unable to push key : {0} to bucket : {1}".format(key, client.vbucketId))
                if test:
                    test.fail("unable to push key : {0} to bucket : {1}".format(key, client.vbucketId))
                else:
                    break
        client.close()
        return inserted_keys
