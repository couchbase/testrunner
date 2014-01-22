import uuid
import copy
from tuqquery.tuq import QueryTests
from couchbase.documentgenerator import DocumentGenerator


JOIN_INNER = "INNER"
JOIN_LEFT = "LEFT"
JOIN_RIGHT = "RIGHT"

class JoinTests(QueryTests):
    def setUp(self):
        super(JoinTests, self).setUp()
        self.gens_tasks = self.generate_docs_tasks()
        self.type_join = self.input.param("type_join", JOIN_INNER)

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
            "FROM %s as employee %s JOIN default.project as new_project " % (bucket.name, self.type_join) +\
            "KEYS employee.tasks_ids"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['resultset'])
            full_list = self._generate_full_joined_docs_list(join_type=self.type_join)
            expected_result = [doc for doc in full_list if not doc]
            expected_result.extend([{"name" : doc['name'], "tasks_ids" : doc['tasks_ids'],
                                     "new_project" : doc['project']}
                                    for doc in full_list if doc and 'project' in doc])
            expected_result.extend([{"name" : doc['name'], "tasks_ids" : doc['tasks_ids']}
                                    for doc in full_list if doc and not 'project' in doc])
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_join_several_keys(self):
        for bucket in self.buckets:
            self.query = "SELECT employee.name, employee.tasks_ids, new_task.project, new_task.task_name " +\
            "FROM %s as employee %s JOIN default as new_task " % (bucket.name, self.type_join) +\
            "KEYS employee.tasks_ids"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['resultset'])
            full_list = self._generate_full_joined_docs_list(join_type=self.type_join)
            expected_result = [doc for doc in full_list if not doc]
            expected_result.extend([{"name" : doc['name'], "tasks_ids" : doc['tasks_ids'],
                                     "project" : doc['project'], "task_name" : doc['task_name']}
                                    for doc in full_list if doc and 'project' in doc])
            expected_result.extend([{"name" : doc['name'], "tasks_ids" : doc['tasks_ids']}
                                    for doc in full_list if doc and not 'project' in doc])
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_simple_join_key(self):
        for bucket in self.buckets:
            self.query = "SELECT employee.name, employee.tasks_ids, new_project " +\
            "FROM %s as employee %s JOIN default.project as new_project " % (bucket.name, self.type_join) +\
            "KEY employee.tasks_ids[0]"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['resultset'])
            full_list = self._generate_full_joined_docs_list(particular_key=0,
                                                             join_type=self.type_join)
            expected_result = [doc for doc in full_list if not doc]
            expected_result.extend([{"name" : doc['name'], "tasks_ids" : doc['tasks_ids'],
                                     "new_project" : doc['project']}
                                    for doc in full_list if doc and 'project' in doc])
            expected_result.extend([{"name" : doc['name'], "tasks_ids" : doc['tasks_ids']}
                                    for doc in full_list if doc and not 'project' in doc])
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_join_several_key(self):
        for bucket in self.buckets:
            self.query = "SELECT employee.name, employee.tasks_ids, new_task.project, new_task.task_name " +\
            "FROM %s as employee %s JOIN default as new_task " % (bucket.name, self.type_join) +\
            "KEY employee.tasks_ids[1]"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['resultset'])
            full_list = self._generate_full_joined_docs_list(particular_key=1,
                                                                   join_type=self.type_join)
            expected_result = [doc for doc in full_list if not doc]
            expected_result.extend([{"name" : doc['name'], "tasks_ids" : doc['tasks_ids'],
                                     "project" : doc['project'], "task_name" : doc['task_name']}
                                    for doc in full_list if doc and 'project' in doc])
            expected_result.extend([{"name" : doc['name'], "tasks_ids" : doc['tasks_ids']}
                                    for doc in full_list if doc and not 'project' in doc])
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_where_join_keys(self):
        for bucket in self.buckets:
            self.query = "SELECT employee.name, employee.tasks_ids, new_project " +\
            "FROM %s as employee %s JOIN default.project as new_project " % (bucket.name, self.type_join) +\
            "KEYS employee.tasks_ids WHERE new_project == 'IT'"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['resultset'])
            expected_result = self._generate_full_joined_docs_list(join_type=self.type_join)
            expected_result = [{"name" : doc['name'], "tasks_ids" : doc['tasks_ids'],
                                "new_project" : doc['project']}
                               for doc in expected_result if doc and 'project' in doc and\
                               doc['project'] == 'IT']
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_join_unnest_alias(self):
        for bucket in self.buckets:
            self.query = "SELECT task2 FROM %s emp1 JOIN %s" % (bucket.name, bucket.name) +\
            " task KEYS emp1.tasks_ids UNNEST emp1.tasks_ids as task2"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['resultset'], key=lambda doc:(
                                                               doc['task2']))
            expected_result = self._generate_full_joined_docs_list()
            expected_result = [{"task2" : task} for doc in expected_result
                               for task in doc['tasks_ids']]
            expected_result = sorted(expected_result, key=lambda doc:(
                                                          doc['task2']))
            self._verify_results(actual_result, expected_result)

    def test_unnest(self):
        for bucket in self.buckets:
            self.query = "SELECT emp.name, task FROM %s emp UNNEST emp.tasks_ids task" % bucket.name
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['resultset'], key=lambda doc:(
                                                               doc['name'], doc['task']))
            expected_result = self._generate_full_joined_docs_list(join_type=self.type_join)
            expected_result = [{"task" : task, "name" : doc["name"]} for doc in expected_result
                               for task in doc['tasks_ids'] if doc]
            expected_result = sorted(expected_result, key=lambda doc:(
                                                          doc['name'], doc['task']))
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
        template = '{{ "task_name":"{0}", "project": "{1}"}}'
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
        if join_type.upper() == JOIN_INNER:
            for item in all_docs_list:
                keys = item["tasks_ids"]
                if particular_key is not None:
                    keys=[item["tasks_ids"][particular_key]]
                tasks_items = self._generate_full_docs_list(self.gens_tasks, keys=keys)
                for tasks_item in tasks_items:
                    item_to_add = copy.deepcopy(item)
                    item_to_add.update(tasks_item)
                    joined_list.append(item_to_add)
        elif join_type.upper() == JOIN_LEFT:
            for item in all_docs_list:
                keys = item["tasks_ids"]
                if particular_key:
                    keys=[item["tasks_ids"][particular_key]]
                tasks_items = self._generate_full_docs_list(self.gens_tasks, keys=keys)
                for key in keys:
                    item_to_add = copy.deepcopy(item)
                    if key in [doc["_id"] for doc in tasks_items]:
                        item_to_add.update([doc for doc in tasks_items if key == doc['_id']][0])
                    joined_list.append(item_to_add)
            joined_list.extend([{}] * self.gens_tasks[-1].end)
        elif join_type.upper() == JOIN_RIGHT:
            raise Exception("RIGHT JOIN doen't exists in corrunt implementation")
        else:
            raise Exception("Unknown type of join")
        return joined_list