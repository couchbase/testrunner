from mc_bin_client import MemcachedClient
import logger
from couchbase.options import ClusterOptions
from couchbase.cluster import Cluster
from couchbase.bucket import Bucket
from couchbase.auth import PasswordAuthenticator
log = logger.Logger.get_logger()
import couchbase.subdocument as SD
import time
from couchbase.bucket import Bucket


class TestMemcachedClient():

    def connection(self, client_ip, bucket_name, user, password, port=11210, vbucket_count=1024):
        log.info(
            "Bucket name for connection is ---- {0}, username -- {1}, ----- password -- {2}".format(bucket_name, user,
                                                                                                    password))
        try:
            mc = MemcachedClient(host=client_ip, port=port)
            mc.vbucket_count = vbucket_count
            mc.sasl_auth_plain(user, password)
            mc.bucket_select(bucket_name)
            return mc, True
        except Exception as e:
            log.info("Exception is from connection function {0}".format(e))
            return False, False

    def write_data(self, mc):
        try:
            prefix = "test-"
            number_of_items = 10
            keys = ["{0}-{1}".format(prefix, i) for i in range(0, number_of_items)]
            for k in keys:
                mc.set(k, 0, 0, str(k + "body"))
            return True
        except Exception as e:
            log.info("Exception is from write_data function {0}".format(e))
            return False

    def read_data(self, client_ip, mc, bucket_name, vbucket_count=1024):
        try:
            mc_temp, status = self.connection(client_ip, bucket_name, 'Administrator', 'password',
                                              vbucket_count=vbucket_count)
            self.write_data(mc_temp)
            test = mc.get("test--0")
            return True
        except Exception as e:
            log.info("Exception is from read_data function {0}".format(e))
            return False

    def read_stats(self, mc):
        try:
            test = mc.stats('warmup')
            return True
        except Exception as e:
            log.info("Exception is {0}".format(e))
            return False

    def get_meta(self, client_ip, mc, bucket_name, vbucket_count=1024):
        try:
            mc_temp, status = self.connection(client_ip, bucket_name, 'Administrator', 'password',
                                              vbucket_count=vbucket_count)
            self.write_data(mc_temp)
            test = mc.getMeta("test--0")
            return True
        except Exception as e:
            log.info("Exception is from get_meata function {0}".format(e))
            return False

    def set_meta(self, client_ip, mc, bucket_name, vbucket_count=1024):
        try:
            mc_temp, status = self.connection(client_ip, bucket_name, 'Administrator', 'password',
                                              vbucket_count=vbucket_count)
            self.write_data(mc_temp)
            rc = mc_temp.getMeta("test--0")
            cas = rc[4] + 1
            rev_seqno = rc[3]
            set_with_meta_resp = mc.setWithMeta("test--0", '123456789', 0, 0, 123, cas)
            return True
        except Exception as e:
            log.info("Exception is from set_meta function {0}".format(e))
            return False


class TestSDK:
    def connection(self, client_ip, bucket_name, user, password):
        log.info(
            "Bucket name for connection is ---- {0}, username -- {1}, ----- password -- {2}".format(bucket_name, user,
                                                                                                    password))
        result = False
        connection_string = 'couchbase://' + client_ip + '/'
        log.info("Value of connection string is - {0}".format(connection_string))
        time.sleep(2)
        try:
            url = 'couchbase://{ip}/'.format(ip=client_ip)
            authenticator = PasswordAuthenticator(username=user, password=password)
            cluster_ops = ClusterOptions(authenticator)
            cluster = Cluster.connect(url, cluster_ops)
            bucket = Bucket(cluster, bucket_name)
            if bucket is not None:
                result = True
                return bucket, result
        except Exception as ex:
            log.info("Exception in creating an SDK connection {0}".format(ex))
            return result

    def set_xattr(self, sdk_conn):
        try:
            k = "sdk_1"
            sdk_conn.upsert(k, {})
            sdk_conn.mutate_in(k, [SD.upsert('my', {'value': 1}, xattr=True)])
            return True
        except Exception as e:
            log.info("Exception is from set_xattr function {0}".format(e))
            return False

    def get_xattr(self, client_ip, sdk_conn, bucket_name):
        try:
            temp_conn, result = self.connection(client_ip, bucket_name, 'Administrator', 'password')
            self.set_xattr(temp_conn)
            k = 'sdk_1'
            rv = sdk_conn.lookup_in(k, [SD.get('my', xattr=True)])
            return True
        except Exception as e:
            log.info("Exception is from get_xattr function {0}".format(e))
            return False
