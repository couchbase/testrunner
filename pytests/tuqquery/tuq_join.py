import uuid
import copy
from tuqquery.tuq import QueryTests
from couchbase.documentgenerator import DocumentGenerator


JOIN_INNER = "inner"
JOIN_LEFT = "left"
JOIN_RIGHT = "right"

class JoinTests(QueryTests):
    def setUp(self):
        super(JoinTests, self).setUp()
        self.gens_tasks = self.generate_docs_tasks()

    def suite_setUp(self):
        super(JoinTests, self).suite_setUp()
        self.load(self.gens_tasks, start_items=self.num_items)

    def tearDown(self):
        super(JoinTests, self).tearDown()

    def suite_tearDown(self):
        super(JoinTests, self).suite_tearDown()

    def test_simple_join_keys(self):
        for bucket in self.buckets:
            self.query = "SELECT employee.name, employee.tasks_ids, new_project " +\
            "FROM %s as employee JOIN default.project as new_project " % bucket.name +\
            "KEYS employee.tasks_ids"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['resultset'], key=lambda doc:(
                                                               doc['name'], doc['new_project']))
            expected_result = self._generate_full_joined_docs_list()
            expected_result = [{"name" : doc['name'], "tasks_ids" : doc['tasks_ids'],
                                "new_project" : doc['project']}
                               for doc in expected_result]
            expected_result = sorted(expected_result, key=lambda doc:(
                                                          doc['name'], doc['new_project']))
            self._verify_results(actual_result, expected_result)

    def test_join_several_keys(self):
        for bucket in self.buckets:
            self.query = "SELECT employee.name, employee.tasks_ids, new_task.project, new_task.task_name " +\
            "FROM %s as employee JOIN default as new_task " % bucket.name +\
            "KEYS employee.tasks_ids"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['resultset'], key=lambda doc:(
                                                               doc['name'],
                                                               doc['new_project'], doc["task_name"]))
            expected_result = self._generate_full_joined_docs_list()
            expected_result = [{"name" : doc['name'], "tasks_ids" : doc['tasks_ids'],
                                "project" : doc['project'], "task_name" : doc['task_name']}
                               for doc in expected_result]
            expected_result = sorted(expected_result, key=lambda doc:(
                                                          doc['name'], doc['new_project'],
                                                          doc["task_name"]))
            self._verify_results(actual_result, expected_result)

    def test_simple_join_key(self):
        for bucket in self.buckets:
            self.query = "SELECT employee.name, employee.tasks_ids, new_project " +\
            "FROM %s as employee JOIN default.project as new_project " % bucket.name +\
            "KEY employee.tasks_ids[0]"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['resultset'], key=lambda doc:(
                                                               doc['name'], doc['new_project']))
            expected_result = self._generate_full_joined_docs_list(particular_key=0)
            expected_result = [{"name" : doc['name'], "tasks_ids" : doc['tasks_ids'],
                                "new_project" : doc['project']}
                               for doc in expected_result]
            expected_result = sorted(expected_result, key=lambda doc:(
                                                          doc['name'], doc['new_project']))
            self._verify_results(actual_result, expected_result)

    def test_join_several_key(self):
        for bucket in self.buckets:
            self.query = "SELECT employee.name, employee.tasks_ids, new_task.project, new_task.task_name " +\
            "FROM %s as employee JOIN default as new_task " % bucket.name +\
            "KEY employee.tasks_ids[1]"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['resultset'], key=lambda doc:(
                                                               doc['name'],
                                                               doc['new_project'], doc["task_name"]))
            expected_result = self._generate_full_joined_docs_list(particular_key=1)
            expected_result = [{"name" : doc['name'], "tasks_ids" : doc['tasks_ids'],
                                "project" : doc['project'], "task_name" : doc['task_name']}
                               for doc in expected_result]
            expected_result = sorted(expected_result, key=lambda doc:(
                                                          doc['name'], doc['new_project'],
                                                          doc["task_name"]))
            self._verify_results(actual_result, expected_result)

    def test_where_join_keys(self):
        for bucket in self.buckets:
            self.query = "SELECT employee.name, employee.tasks_ids, new_project " +\
            "FROM %s as employee JOIN default.project as new_project " % bucket.name +\
            "KEYS employee.tasks_ids WHERE new_project == 'IT'"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['resultset'], key=lambda doc:(
                                                               doc['name'], doc['new_project']))
            expected_result = self._generate_full_joined_docs_list()
            expected_result = [{"name" : doc['name'], "tasks_ids" : doc['tasks_ids'],
                                "new_project" : doc['project']}
                               for doc in expected_result if doc['project'] == 'IT']
            expected_result = sorted(expected_result, key=lambda doc:(
                                                          doc['name'], doc['new_project']))
            self._verify_results(actual_result, expected_result)

    def generate_docs(self, docs_per_day, start=0):
        generators = []
        types = ['Engineer', 'Sales', 'Support']
        join_yr = [2010, 2011]
        join_mo = xrange(1, 12 + 1)
        join_day = xrange(1, 28 + 1)
        template = '{{ "name":"{0}", "join_yr":{1}, "join_mo":{2}, "join_day":{3},'
        template += ' "job_title":"{4}", "tasks_ids":{5}}}'
        for info in types:
            for year in join_yr:
                for month in join_mo:
                    for day in join_day:
                        prefix = str(uuid.uuid4())[:7]
                        name = ["employee-%s" % (str(day))]
                        tasks_ids = ["test_task-%s" % day, "test_task-%s" % (day + 1)]
                        generators.append(DocumentGenerator("query-test" + prefix,
                                               template,
                                               name, [year], [month], [day],
                                               [info], [tasks_ids],
                                               start=start, end=docs_per_day))
        return generators

    def generate_docs_tasks(self):
        generators = []
        start, end = 0, (28 + 1)
        template = '{{ "task_name":"{0}", "project":{1}}}'
        generators.append(DocumentGenerator("test_task", template,
                                            ["test_task_%s" % i for i in xrange(0,10)],
                                            ["CB"],
                                            start=start, end=10))
        generators.append(DocumentGenerator("test_task", template,
                                            ["test_task_%s" % i for i in xrange(10,20)],
                                            ["MB"],
                                            start=10, end=20))
        generators.append(DocumentGenerator("test_task", template,
                                            ["test_task_%s" % i for i in xrange(20,end)],
                                            ["IT"],
                                            start=20, end=end))
        return generators

    def _generate_full_joined_docs_list(self, join_type=JOIN_INNER,
                                        particular_key=None):
        joined_list = []
        all_docs_list = self._generate_full_docs_list(self.gens_load)
        if join_type == JOIN_INNER:
            for item in all_docs_list:
                keys = item["tasks_ids"]
                if particular_key:
                    keys=[item["tasks_ids"][particular_key]]
                tasks_items = self._generate_full_docs_list(self.gens_tasks, keys=keys)
                for tasks_item in tasks_items:
                    item_to_add = copy.deepcopy(item)
                    item_to_add.update(tasks_item)
                    joined_list.append(item_to_add)
        elif join_type == JOIN_LEFT:
            raise Exception("Not implemented yet")
        elif join_type == JOIN_RIGHT:
            raise Exception("Not implemented yet")
        else:
            raise Exception("Unknown type of join")
        return joined_list