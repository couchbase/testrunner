import time
import logger
from membase.api.rest_client import RestConnection, RestHelper, vBucket

log = logger.Logger.get_logger()

class RebalanceHelper():

    @staticmethod
    def verify_maps(vbucket_map_before,vbucket_map_after):
        #for each bucket check the replicas
        vbucket = vBucket()
        for i in range(0,len(vbucket_map_before)):
            if not vbucket_map_before[i].master == vbucket_map_after[i].master:
                log.error(
                    'vbucket[{0}].master mismatch {1} vs {2}'.format(i,vbucket_map_before[i].master,vbucket_map_after[i].master))
                return False
            for j in range(0,len(vbucket_map_before[i].replica)):
                if not (vbucket_map_before[i].replica[j]) == (vbucket_map_after[i].replica[j]):
                    log.error('vbucket[{0}].replica[{1} mismatch {2} vs {3}'.format(i,j,
                        vbucket_map_before[i].replica[j],vbucket_map_after[i].replica[j] ))
                    return False
        return True

    @staticmethod
    def delete_all_buckets_or_assert(ips,test_case):
        log.info('deleting existing buckets on {0}'.format(ips))
        for ip in ips:
            rest = RestConnection(ip=ip,
                                  username='Administrator',
                                  password='password')
            buckets = rest.get_buckets()
            for bucket in buckets:
                print bucket.name
                rest.delete_bucket(bucket.name)
                log.info('deleted bucket : {0}'.format(bucket.name))
                msg = 'bucket "{0}" was not deleted even after waiting for two minutes'.format(bucket.name)
                test_case.assertTrue(BucketOperationHelper.wait_for_bucket_deletion(bucket.name, rest, 200)
                                , msg=msg)

    @staticmethod
    def wait_for_bucket_deletion(bucket,
                                 rest,
                                 timeout_in_seconds=120):
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
        log.info('waiting for bucket creation to complete....')
        start = time.time()
        helper = RestHelper(rest)
        while (time.time() - start) <= timeout_in_seconds:
            if helper.bucket_exists(bucket):
                return True
            else:
                time.sleep(2)
        return False