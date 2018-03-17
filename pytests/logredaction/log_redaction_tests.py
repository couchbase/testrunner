from logredaction.log_redaction_base import LogRedactionBase
from couchbase_helper.documentgenerator import BlobGenerator


class LogRedactionTests(LogRedactionBase):
    def setUp(self):
        super(LogRedactionTests, self).setUp()

    def tearDown(self):
        super(LogRedactionTests, self).tearDown()

    def test_enabling_redaction(self):
        self.set_redaction_level()

    def test_cbcollect_with_redaction_enabled(self):
        self.set_redaction_level()
        self.start_logs_collection()
        result = self.monitor_logs_collection()
        logs_path = result["perNode"]["ns_1@127.0.0.1"]["path"]
        redactFileName = logs_path.split('/')[-1]
        nonredactFileName = logs_path.split('/')[-1].replace('-redacted', '')
        remotepath = logs_path[0:logs_path.rfind('/')+1]
        self.verify_log_files_exist(remotepath=remotepath,
                                    redactFileName=redactFileName,
                                    nonredactFileName=nonredactFileName)

    def test_cbcollect_with_redaction_disabled(self):
        self.set_redaction_level()
        self.start_logs_collection()
        result = self.monitor_logs_collection()
        logs_path = result["perNode"]["ns_1@127.0.0.1"]["path"]
        nonredactFileName = logs_path.split('/')[-1]
        remotepath = logs_path[0:logs_path.rfind('/')+1]
        self.verify_log_files_exist(remotepath=remotepath,
                                    nonredactFileName=nonredactFileName)

    def test_ns_server_with_redaction_enabled(self):
        #load bucket and do some ops
        gen_create = BlobGenerator('logredac', 'logredac-', self.value_size, end=self.num_items)
        self._load_all_buckets(self.master, gen_create, "create", 0)

        gen_delete = BlobGenerator('logredac', 'logredac-', self.value_size, start=self.num_items / 2, end=self.num_items)
        gen_update = BlobGenerator('logredac', 'logredac-', self.value_size, start=self.num_items + 1,
                                   end=self.num_items * 3 / 2)

        self._load_all_buckets(self.master, gen_delete, "create", 0)
        self._load_all_buckets(self.master, gen_update, "create", 0)

        #set log redaction level, collect logs, verify log files exist and verify them for redaction
        self.set_redaction_level()
        self.start_logs_collection()
        result = self.monitor_logs_collection()
        logs_path = result["perNode"]["ns_1@127.0.0.1"]["path"]
        redactFileName = logs_path.split('/')[-1]
        nonredactFileName = logs_path.split('/')[-1].replace('-redacted', '')
        remotepath = logs_path[0:logs_path.rfind('/') + 1]
        self.verify_log_files_exist(remotepath=remotepath,
                                    redactFileName=redactFileName,
                                    nonredactFileName=nonredactFileName)
        self.verify_log_redaction(remotepath=remotepath,
                                  redactFileName=redactFileName,
                                  nonredactFileName=nonredactFileName,
                                  logFileName="ns_server.debug.log")


