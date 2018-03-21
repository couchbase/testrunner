from membase.api.rest_client import RestConnection
from basetestcase import BaseTestCase
from lib.remote.remote_util import RemoteMachineShellConnection
from couchbase_helper.documentgenerator import BlobGenerator
from lib.couchbase_helper.cluster import Cluster


class CsvDataTest(BaseTestCase):

    def setUp(self):
        super(CsvDataTest, self).setUp()
        self.username_arg = " -u " + self.input.param('username', 'Administrator')
        self.password_arg = " -p " + self.input.param('password', 'password')

    def __load_data(self):
        self.gen_load = BlobGenerator('couch', 'cb-', self.value_size, end=self.num_items)
        bucket_to_load = None
        for bucket in self.buckets:
            if bucket.name == 'default':
                bucket_to_load = bucket
                break
        self.assertNotEqual(bucket_to_load, None, msg="Could not find default bucket on node {0}".format(self.master.ip))
        self.cluster.load_gen_docs(self.master, bucket_to_load.name, self.gen_load, bucket_to_load.kvs[1], 'create',
                                   compression=self.sdk_compression)

    '''The below function loads some data to bucket and then creates a csv
       file of that data on localhost. Then it restore that csv file to another
       bucket and verify whether the number of items in both the buckets are same.'''

    def create_and_restore_csv(self):
        try:
            self.__load_data()
            shell_obj = RemoteMachineShellConnection(self.master)
            self.log.info("Removing backup folder if already present")
            info = shell_obj.extract_remote_info()
            path = "/tmp/backup/"
            if info.type.lower() == "windows":
                path = "/cygdrive/c" + path
            #TODO : Check for mac also
            shell_obj.delete_files(path)
            create_dir = "mkdir " + path
            data_type = "csv:"
            destination = path + "data.csv"
            shell_obj.execute_command(create_dir)
            source = "http://localhost:8091"
            options = "-b default" + self.username_arg + self.password_arg
            shell_obj.execute_cbtransfer(source, data_type + destination, options)
            self.log.info("Created csv file @ %s" % destination)
            source, destination = destination, source
            options = "-B standard_bucket0" + self.username_arg + self.password_arg
            self.log.info("Restoring data....!")
            shell_obj.execute_cbtransfer(source, destination, options)
            self.sleep(10)
            self.log.info("Checking whether number of items loaded match with the number of items restored.")
            rest = RestConnection(self.master)
            itemCount = rest.get_bucket_json('standard_bucket0')['basicStats']['itemCount']
            self.assertEqual(itemCount, self.num_items, msg="Number of items loaded do no match\
            with the number of items restored. Number of items loaded is {0} \
            but number of items restored is {1}".format(self.num_items, itemCount))
            self.log.info("Number of items loaded = Number of items restored. Pass!!")
        finally:
            shell_obj.disconnect()
