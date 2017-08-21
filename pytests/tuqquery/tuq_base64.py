import copy
import base64
import testconstants
from tuqquery.tuq import QueryTests
from remote.remote_util import RemoteMachineShellConnection
from couchbase_helper.documentgenerator import JSONNonDocGenerator
from membase.api.exception import CBQError

class Base64Tests(QueryTests):
    def setUp(self):
        self.skip_generation = True
        super(Base64Tests, self).setUp()
        self.gens_load = self.generate_docs()
        for bucket in self.buckets:
            self.cluster.bucket_flush(self.master, bucket=bucket,
                                  timeout=self.wait_timeout * 5)
        self.load(self.gens_load)

    def suite_setUp(self):
        super(Base64Tests, self).suite_setUp()

    def tearDown(self):
        super(Base64Tests, self).tearDown()

    def suite_tearDown(self):
        super(Base64Tests, self).suite_tearDown()

    def test_simple_query(self):
        for bucket in self.buckets:
            try:
                self.query = "select BASE64(%s) from %s" % (bucket.name, bucket.name)
                self.run_cbq_query()
                self.sleep(3)
                actual_result = self.run_cbq_query()
                actual_result = [doc["$1"] for doc in actual_result['results']]
                expected_result = self._generate_full_docs_list(self.gens_load)
                expected_result = [base64.b64encode(doc) for doc in expected_result]
                self._verify_results(actual_result, expected_result)
            except Exception, ex:
                self.query = "SELECT %s FROM %s" % (bucket.name, bucket.name)
                self.run_cbq_query()
                raise ex

    def test_negative_value(self):
        # tuq should not crash after error
        for bucket in self.buckets:
            self.query = "select BASE64() from %s" % (bucket.name)
            try:
                self.run_cbq_query()
            except CBQError:
                shell = RemoteMachineShellConnection(self.master)
                output = shell.execute_command('ps -aef | grep cbq')
                if str(output).find('cbq-engine') == -1:
                    os = self.shell.extract_remote_info().type.lower()
                    if os != 'windows':
                        self.fail('Cbq-engine is crashed')
                self.log.info('Error appeared as expected')
            else:
                self.fail('Error expected but not appeared')

    def generate_docs(self, name="tuq", start=0, end=0):
        if end==0:
            end = self.num_items
        values = ["Engineer", "Sales", "Support"]
        generators = [JSONNonDocGenerator(name, values, start=start,end=end)]
        return generators

    def _generate_full_docs_list(self, gens_load):
        all_docs_list = []
        for gen_load in gens_load:
            doc_gen = copy.deepcopy(gen_load)
            while doc_gen.has_next():
                _, val = doc_gen.next()
                all_docs_list.append(val)
        return all_docs_list

    def _verify_results(self, actual_result, expected_result):
        self.assertEquals(len(actual_result), len(expected_result),
                          "Results are incorrect.Actual num %s. Expected num: %s.\n" % (
                                            len(actual_result), len(expected_result)))
        msg = "Results are incorrect.\n Actual first and last 100:  %s.\n ... \n %s" +\
        "Expected first and last 100: %s.\n  ... \n %s"
        self.assertTrue(sorted(actual_result) == sorted(expected_result),
                          msg % (actual_result[:100],actual_result[-100:],
                                 expected_result[:100],expected_result[-100:]))