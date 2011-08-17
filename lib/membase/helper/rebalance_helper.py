import time
import logger
from membase.api.exception import StatsUnavailableException
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.bucket_helper import BucketOperationHelper
from memcached.helper.data_helper import MemcachedClientHelper
import threading

log = logger.Logger.get_logger()

class RebalanceHelper():
    @staticmethod
    #bucket is a json object that contains name,port,password
    def wait_for_stats(master, bucket, stat_key, stat_value, timeout_in_seconds=120, verbose=True):
        log.info("waiting for bucket {0} stat : {1} to match {2}".format(bucket, stat_key, stat_value))
        start = time.time()
        verified = False
        while (time.time() - start) <= timeout_in_seconds:
            rest = RestConnection(master)
            stats = rest.get_bucket_stats(bucket)
            if stats and stat_key in stats and stats[stat_key] == stat_value:
                log.info("{0} : {1}".format(stat_key, stats[stat_key]))
                verified = True
                break
            else:
                if stats and stat_key in stats:
                    if verbose:
                        log.info("{0} : {1}".format(stat_key, stats[stat_key]))
                if not verbose:
                    time.sleep(0.1)
                else:
                    time.sleep(2)
        return verified


    @staticmethod
    def wait_till_total_numbers_match(master,
                                      bucket,
                                      timeout_in_seconds=120):

        log.info('waiting for sum_of_curr_items == total_items....')
        start = time.time()
        verified = False
        while (time.time() - start) <= timeout_in_seconds:
            try:
                if RebalanceHelper.verify_items_count(master,bucket):
                    verified = True
                    break
                else:
                    time.sleep(2)
            except StatsUnavailableException:
                log.info("unable to retrieve stats for any node. returning true")
                verified = True
                break
        rest = RestConnection(master)
        RebalanceHelper.print_taps_from_all_nodes(rest, bucket)
        return verified

    @staticmethod
    #TODO: add password and port
    def print_taps_from_all_nodes(rest, bucket='default'):
        #get the port number from rest ?
        
        log = logger.Logger.get_logger()
        nodes_for_stats = rest.get_nodes()
        for node_for_stat in nodes_for_stats:
            try:
                client = MemcachedClientHelper.direct_client(node_for_stat, bucket)
                log.info("getting tap stats.. for {0}".format(node_for_stat.ip))
                tap_stats = client.stats('tap')
                if tap_stats:
                    RebalanceHelper.log_interesting_taps(node_for_stat, tap_stats, log)
                client.close()
            except Exception as ex:
                log.error("error {0} while getting stats...".format(ex))


    @staticmethod
    def log_interesting_taps(node, tap_stats, logger):
        interesting_stats = ['ack_log_size', 'ack_seqno', 'ack_window_full', 'has_item', 'has_queued_item',
                             'idle', 'paused', 'pending_backfill', 'pending_disk_backfill', 'recv_ack_seqno',
                             'ep_num_new_']
        for name in tap_stats:
            for interesting_stat in interesting_stats:
                if name.find(interesting_stat) != -1:
                    logger.info("TAP {0} :{1}   {2}".format(node.id, name, tap_stats[name]))
                    break


    @staticmethod
    def verify_items_count(master,bucket):
        #get the #of buckets from rest
        rest = RestConnection(master)
        bucket_info = rest.get_bucket(bucket)
        replica_factor = bucket_info.numReplicas
        #print out vb_pending_num,vb_active_num,vb_replica_num as well
        master_stats = rest.get_bucket_stats(bucket)
        vbucket_active_sum = 0
        vbucket_replica_sum = 0
        vbucket_pending_sum = 0
        all_server_stats = []
        stats_received = 0
        nodes = rest.get_nodes()
        for server in nodes:
            #get the stats
            server_stats = rest.get_bucket_stats_for_node(bucket, server.ip)
            if not server_stats:
                log.info("unable to get stats from {0}".format(server.ip))
            else:
                stats_received += 1
            all_server_stats.append((server, server_stats))
        if not stats_received:
            raise StatsUnavailableException()
        sum = 0
        for server, single_stats in all_server_stats:
            if not single_stats or "curr_items" not in single_stats:
                continue
            sum += single_stats["curr_items"]
            log.info("curr_items from {0} : {1}".format(server.ip, single_stats["curr_items"]))
            if 'vb_pending_num' in single_stats:
                vbucket_pending_sum += single_stats['vb_pending_num']
                log.info("vb_pending_num from {0} : {1}".format(server.ip, single_stats["vb_pending_num"]))
            if 'vb_active_num' in single_stats:
                vbucket_active_sum += single_stats['vb_active_num']
                log.info("vb_active_num from {0} : {1}".format(server.ip, single_stats["vb_active_num"]))
            if 'vb_replica_num' in single_stats:
                vbucket_replica_sum += single_stats['vb_replica_num']
                log.info("vb_replica_num from {0} : {1}".format(server.ip, single_stats["vb_replica_num"]))

        msg = "summation of vb_active_num : {0} vb_pending_num : {1} vb_replica_num : {2}"
        log.info(msg.format(vbucket_active_sum, vbucket_pending_sum, vbucket_replica_sum))
        msg = 'sum : {0} and sum * replica_factor ({1}) : {2}'
        log.info(msg.format(sum, replica_factor, (sum * (replica_factor + 1))))
        log.info('master_stats : {0}'.format(master_stats["curr_items_tot"]))
        return (sum * (replica_factor + 1)) == master_stats["curr_items_tot"]


    @staticmethod
    def verify_maps(vbucket_map_before, vbucket_map_after):
        #for each bucket check the replicas
        for i in range(0, len(vbucket_map_before)):
            if not vbucket_map_before[i].master == vbucket_map_after[i].master:
                log.error(
                    'vbucket[{0}].master mismatch {1} vs {2}'.format(i, vbucket_map_before[i].master,
                                                                     vbucket_map_after[i].master))
                return False
            for j in range(0, len(vbucket_map_before[i].replica)):
                if not (vbucket_map_before[i].replica[j]) == (vbucket_map_after[i].replica[j]):
                    log.error('vbucket[{0}].replica[{1} mismatch {2} vs {3}'.format(i, j,
                                                                                    vbucket_map_before[i].replica[j],
                                                                                    vbucket_map_after[i].replica[j]))
                    return False
        return True

    @staticmethod
    def delete_all_buckets_or_assert(ips, test_case):
        log.info('deleting existing buckets on {0}'.format(ips))
        for ip in ips:
            rest = RestConnection(ip=ip)
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

        # in this method

    #    @staticmethod
    #    def add_node_and_rebalance(rest,node_ip):
    #        pass
    #read the current nodes
    # if the node_ip already added then just
    #silently return
    #if its not added then let try to add this and then rebalance
    #we should alo try to get the bucket information from
    #rest api instead of passing it to the fucntions
