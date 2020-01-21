from basetestcase import BaseTestCase
from couchbase_helper.cluster import Cluster
from mc_bin_client import MemcachedClient, MemcachedError
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection

AUTH_SUCCESS = ""
AUTH_FAILURE = "Auth error"

class SaslTest(BaseTestCase):

    def setUp(self):
        super(SaslTest, self).setUp()

    def tearDown(self):
        super(SaslTest, self).tearDown()

    def create_pwd_buckets(self, server, buckets):
        tasks = []
        for name in buckets:
            sasl_params = self._create_bucket_params(server=server, size=100, replicas=0)
            tasks.append(self.cluster.async_create_sasl_bucket(name=name, password=buckets[name], bucket_params=sasl_params))

        for task in tasks:
            task.result(self.wait_timeout * 10)

    def do_auth(self, bucket, password):
        """ default self.auth_mech is 'PLAIN' """
        self.log.info("Authenticate with {0} to {1}:{2}".format(self.auth_mech,
                                                                bucket,
                                                                password))
        ret = None
        nodes = RestConnection(self.master).get_nodes()
        for n in nodes:
            if n.ip == self.master.ip and n.port == self.master.port:
                node = n
        client = MemcachedClient(self.master.ip, node.memcached)
        try:
            if self.auth_mech == "PLAIN":
                ret = client.sasl_auth_plain(bucket, password)[2]
            else:
                self.fail("Invalid auth mechanism {0}".format(self.auth_mech))
        except MemcachedError as e:
            ret = e[0].split(' for vbucket')[0]
        client.close()
        return ret

    """ Make sure that list mechanisms works and the response is in order
        From 5.0.1, mechs will be:
            ['SCRAM-SHA512', 'SCRAM-SHA1', 'SCRAM-SHA256', 'PLAIN']
    """
    def test_list_mechs(self):
        nodes = RestConnection(self.master).get_nodes()
        for n in nodes:
            if n.ip == self.master.ip and n.port == self.master.port:
                node = n
        client = MemcachedClient(self.master.ip, node.memcached)
        mechs = list(client.sasl_mechanisms())
        self.log.info("Start check mech types")
        assert b"SCRAM-SHA1" in mechs
        assert b"SCRAM-SHA256" in mechs
        assert b"SCRAM-SHA512" in mechs
        assert b"PLAIN" in mechs
        assert len(list(mechs)) == 4

    """Tests basic sasl authentication on buckets that exist"""
    def test_basic_valid(self):
        buckets = { "bucket1" : "password",
                    "bucket2" : "password" }
        self.create_pwd_buckets(self.master, buckets)

        for bucket in buckets:
            assert self.do_auth("Administrator", buckets[bucket]) == AUTH_SUCCESS

    """Tests basic sasl authentication on non-existent buckets"""
    def test_basic_invalid(self):
        buckets = { "bucket1" : "password1",
                    "bucket2" : "password2" }
        for bucket in buckets:
            assert AUTH_FAILURE in self.do_auth(bucket, buckets[bucket])

    """Tests basic sasl authentication on incomplete passwords

    This test makes sure that all password characters are tested and that if the
    user provides a partial password then authenication will fail"""
    def test_auth_incomplete_password(self):
        buckets = { "bucket1" : "password1" }
        self.create_pwd_buckets(self.master, buckets)

        for bucket in buckets:
            assert AUTH_FAILURE in self.do_auth(bucket, "")
            assert AUTH_FAILURE in self.do_auth(bucket, "pass")
            assert AUTH_FAILURE in self.do_auth(bucket, "password")
            assert AUTH_FAILURE in self.do_auth(bucket, "password12")
            assert self.do_auth("Administrator", "password") == AUTH_SUCCESS

    """Tests basic sasl authentication on incomplete bucket names

    This test makes sure that all bucket characters are tested and that if the
    user provides a partial bucket name then authenication will fail"""
    def test_auth_incomplete_bucket(self):
        buckets = { "bucket1" : "password1" }
        self.create_pwd_buckets(self.master, buckets)

        assert AUTH_FAILURE in self.do_auth("", "password1")
        assert AUTH_FAILURE in self.do_auth("buck", "password1")
        assert AUTH_FAILURE in self.do_auth("bucket", "password1")
        assert AUTH_FAILURE in self.do_auth("bucket12", "password1")
        assert self.do_auth("Administrator", "password") == AUTH_SUCCESS

    """Test bucket names and passwords with null characters

    Null characters are used to seperate fields in some authentication
    mechanisms and this test chaeck some of those cases."""
    def test_auth_null_character_tests(self):
        buckets = { "bucket1" : "password1" }
        self.create_pwd_buckets(self.master, buckets)

        assert AUTH_FAILURE in self.do_auth("\0Administrator", "password1")
        assert AUTH_FAILURE in self.do_auth("\0\0\0Administrator", "password1")
        assert AUTH_FAILURE in self.do_auth("Administrator", "\0password1")
        assert AUTH_FAILURE in self.do_auth("Administrator", "\0\0\0password1")
        assert AUTH_FAILURE in self.do_auth("Administrator\0", "\0password1")
        assert AUTH_FAILURE in self.do_auth("Administrator", "\0password1")
        assert self.do_auth("Administrator", "password") == AUTH_SUCCESS

    """Test bucket names and passwords with spaces

    Space characters are used to seperate fields in some authentication
    mechanisms and this test chaeck some of those cases."""
    def test_auth_space_character_tests(self):
        pass

    """Test bucket names and passwords with special characters"""
    def test_auth_special_character_tests(self):
        pass

    """UTF-8 Sasl test cases

    Space characters are used to seperate fields in some authentication
    mechanisms and this test chaeck some of those cases."""
    def test_auth_utf8(self):
        pass

    """Test large usernames and passwords"""
    def test_auth_too_big(self):
        pass

    def test_password(self):
        buckets_num = self.input.param("buckets_to_check", 1)
        valid_password = self.input.param("valid_pass", "password")
        if isinstance(valid_password, int):
            valid_password = str(valid_password)
        valid_password = valid_password.replace('[space]', ' ')
        invalid_pass = self.input.param("invalid_pass", [])
        if invalid_pass:
            invalid_pass = invalid_pass.split(";")
        self._create_sasl_buckets(self.master, buckets_num, bucket_size=100,
                                                    password=valid_password)
        if self.input.param("include_restart", False):
            self.restart_server(self.servers[:self.nodes_init])
        for bucket in self.buckets:
            for password in invalid_pass:
                password = \
                    password.replace('[space]', ' ').replace('[tab]', '\t').encode('ascii')
                self.assertTrue(AUTH_FAILURE in self.do_auth(bucket.name, password),
                             "Bucket %s, valid pass %s, shouldn't authentificate with %s"\
                              %(bucket, bucket.saslPassword, password))
            self.assertEqual(self.do_auth("Administrator", bucket.saslPassword), AUTH_SUCCESS,
                             "Bucket %s, valid pass %s, authentification should success"\
                             %(bucket, bucket.saslPassword))

    def restart_server(self, servers):
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            shell.stop_couchbase()
            self.sleep(3, "Pause between start and stop")
            shell.start_couchbase()
            shell.disconnect()
        self.sleep(10, "Pause for starting servers")