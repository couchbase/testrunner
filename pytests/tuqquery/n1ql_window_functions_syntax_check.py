from .tuq import QueryTests
import random
import string
from random import randint
from membase.api.exception import CBQError
import threading
import copy

class WindowFunctionsSyntaxTest(QueryTests):

    def setUp(self):
        super(WindowFunctionsSyntaxTest, self).setUp()
        self.log_config_info()
        self.log.info("==============  WindowFunctionsSyntaxTest setup has started ==============")


        self.primary_idx = {'name': '#primary', 'bucket': 'test_bucket', 'fields': (), 'state': 'online', 'using': self.index_type.lower(), 'is_primary': True}
        self.idx_1 = {'name': 'ix_char', 'bucket': 'test_bucket', 'fields': [('char_field', 0)], 'state': 'online', 'using': self.index_type.lower(), 'is_primary': False}
        self.idx_2 = {'name': 'ix_decimal', 'bucket': 'test_bucket', 'fields': [('decimal_field', 0)], 'state': 'online', 'using': self.index_type.lower(), 'is_primary': False}
        self.idx_3 = {'name': 'ix_int', 'bucket': 'test_bucket', 'fields': [('int_field', 0)], 'state': 'online', 'using': self.index_type.lower(), 'is_primary': False}

        self.indexes = [self.primary_idx, self.idx_1, self.idx_2, self.idx_3]

        self.log.info("==============  WindowFunctionsTest setup has completed ==============")

    def tearDown(self):
        self.log_config_info()
        self.log.info("==============  WindowFunctionsSyntaxTest tearDown has started ==============")
        super(WindowFunctionsSyntaxTest, self).tearDown()
        self.log.info("==============  WindowFunctionsSyntaxTest tearDown has completed ==============")

    def suite_setUp(self):
        super(WindowFunctionsSyntaxTest, self).suite_setUp()
        self.init_nodes()
        self.load_test_data("test_bucket")
        self.create_primary_index('test_bucket')
        self.create_secondary_indexes('test_bucket')
        self.adopt_test_data("test_bucket")
        self.log_config_info()
        self.log.info("==============  WindowFunctionsSyntaxTest suite_setup has started ==============")
        self.log.info("==============  WindowFunctionsSyntaxTest suite_setup has completed ==============")

    def suite_tearDown(self):
        self.log_config_info()
        self.log.info("==============  WindowFunctionsSyntaxTest suite_tearDown has started ==============")
        super(WindowFunctionsSyntaxTest, self).suite_tearDown()
        self.log.info("==============  WindowFunctionsSyntaxTest suite_tearDown has completed ==============")




    def run_all(self):
        self.test_from_select_batches()
        self.test_select_from_batches()

    def generate_from_select_queries(self):
        result = []
        counter = 0
        window_function_values = [' LAST_VALUE(t1.decimal_field) OVER (PARTITION BY t1.char_field ORDER BY t1.decimal_field RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) ']
        alias_values = [' wf ']
        bucket_alias_values = [' ', ' as ']
        let_where_values = [' ', ' where t1.int_field > 1000 ', ' let int_val=1000 where t1.int_field > int_val ']
        group_by_values = [' ', ' group by t1.char_field, t1.decimal_field ']
        letting_having_values = [' ', ' having t1.char_field="E" ', ' letting char_val="E" having t1.char_field=char_val ']
        order_by_values = [' ', ' order by t1.char_field ']
        asc_desc_values = [' ', ' asc ', ' desc ']
        limit_values = [' ', ' limit 100 ']
        offset_values = [' ', ' offset 10 ']
        join_values = [' ', ' inner join ', ' left join ', ' left outer join ', ' inner nest ', ' left nest ']
        union_values = [' ', ' union ', ' union all ']
        namespace_values = ['', 'default:']
        use_keys_values = [' ', ' use primary keys[\'test\'] ', ' use keys[\'test\'] ', ' use index(`ix_char`) ', ' use index(`ix_char`, `ix_decimal`) ', ' use index(`ix_char` using gsi) ']
        join_predicate_values = [' on t1.primary_key=t2.primary_key ', ' on primary keys[\'test\']', ' on keys t1.char_field ', ' on key t2.char_field for t1 ', ' on primary key t2.primary_key for t1 ']
        unnest_flatten_values = [' unnest ', ' left unnest ', ' flatten ', ' left flatten ']

        for window_function_value in window_function_values:
            for alias_value in alias_values:
                for let_where_value in let_where_values:
                    for group_by_value in group_by_values:
                        for letting_having_value in letting_having_values:
                            if group_by_value == ' ':
                                letting_having_value = ' '
                            for order_by_value in order_by_values:
                                for asc_desc_value in asc_desc_values:
                                    if order_by_value == ' ':
                                        asc_desc_value = ' '
                                    for limit_value in limit_values:
                                        for offset_value in offset_values:
                                            for bucket_alias_value in bucket_alias_values:
                                                for namespace_value in namespace_values:
                                                    for use_keys_value in use_keys_values:
                                                        for unnest_flatten_value in unnest_flatten_values:
                                                            for join_value in join_values:
                                                                for join_predicate_value in join_predicate_values:
                                                                    join_expression = ' '
                                                                    if join_value!=' ':
                                                                        join_expression = join_value+' '+namespace_value+'test_bucket '+bucket_alias_value+' t2 '+use_keys_value+join_predicate_value
                                                                    else:
                                                                        join_expression = unnest_flatten_value+' t1.char_field '
                                                                    for union_value in union_values:
                                                                        union_left_parenthesis = ''
                                                                        union_right_parenthesis = ''
                                                                        right_union_expression = ''
                                                                        if union_value!=' ':
                                                                            union_left_parenthesis='('
                                                                            union_right_parenthesis=')'
                                                                            right_union_expression = union_left_parenthesis+"select t1.char_field, t1.decimal_field, "+window_function_value+alias_value+" " \
                                                                            "from "+namespace_value+"test_bucket "+bucket_alias_value+" t1 "+use_keys_value+join_expression+let_where_value+group_by_value+letting_having_value+ \
                                                                            order_by_value+asc_desc_value+limit_value+offset_value+union_right_parenthesis

                                                                        query = "from ("+union_left_parenthesis+"select t1.char_field, t1.decimal_field, "+window_function_value+alias_value+" " \
                                                                            "from "+namespace_value+"test_bucket "+bucket_alias_value+" t1 "+use_keys_value+join_expression+let_where_value+group_by_value+letting_having_value+ \
                                                                            order_by_value+asc_desc_value+limit_value+offset_value+union_right_parenthesis+union_value+right_union_expression+") a select a.wf"
                                                                        result.append(query)
                                                                        counter+=1
        return result

    def generate_select_from_queries(self):
        result = []
        counter = 0
        window_function_values = [' LAST_VALUE(t1.decimal_field) OVER (PARTITION BY t1.char_field ORDER BY t1.decimal_field RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) ']
        alias_values = [' wf ']
        bucket_alias_values = [' ', ' as ']
        let_where_values = [' ', ' where t1.int_field > 1000 ', ' let int_val=1000 where t1.int_field > int_val ']
        group_by_values = [' ', ' group by t1.char_field, t1.decimal_field ']
        letting_having_values = [' ', ' having t1.char_field="E" ', ' letting char_val="E" having t1.char_field=char_val ']
        order_by_values = [' ', ' order by t1.char_field ']
        asc_desc_values = [' ', ' asc ', ' desc ']
        limit_values = [' ', ' limit 100 ']
        offset_values = [' ', ' offset 10 ']
        join_values = [' ', ' inner join ', ' left join ', ' left outer join ', ' inner nest ', ' left nest ']
        union_values = [' ', ' union ', ' union all ']
        namespace_values = ['', 'default:']
        use_keys_values = [' ', ' use primary keys[\'test\'] ', ' use keys[\'test\'] ', ' use index(`ix_char`) ', ' use index(`ix_char`, `ix_decimal`) ', ' use index(`ix_char` using gsi) ']
        join_predicate_values = [' on t1.primary_key=t2.primary_key ', ' on primary keys[\'test\']', ' on keys t1.char_field ', ' on key t2.char_field for t1 ', ' on primary key t2.primary_key for t1 ']
        unnest_flatten_values = [' unnest ', ' left unnest ', ' flatten ', ' left flatten ']

        for window_function_value in window_function_values:
            for alias_value in alias_values:
                for let_where_value in let_where_values:
                    for group_by_value in group_by_values:
                        for letting_having_value in letting_having_values:
                            if group_by_value == ' ':
                                letting_having_value = ' '
                            for order_by_value in order_by_values:
                                for asc_desc_value in asc_desc_values:
                                    if order_by_value == ' ':
                                        asc_desc_value = ' '
                                    for limit_value in limit_values:
                                        for offset_value in offset_values:
                                            for bucket_alias_value in bucket_alias_values:
                                                for namespace_value in namespace_values:
                                                    for use_keys_value in use_keys_values:
                                                        for unnest_flatten_value in unnest_flatten_values:
                                                            for join_value in join_values:
                                                                for join_predicate_value in join_predicate_values:
                                                                    join_expression = ' '
                                                                    if join_value!=' ':
                                                                        join_expression = join_value+' '+namespace_value+'test_bucket '+bucket_alias_value+' t2 '+use_keys_value+join_predicate_value
                                                                    else:
                                                                        join_expression = unnest_flatten_value+' t1.char_field '
                                                                    for union_value in union_values:
                                                                        union_left_parenthesis = ''
                                                                        union_right_parenthesis = ''
                                                                        right_union_expression = ''
                                                                        if union_value!=' ':
                                                                            union_left_parenthesis='('
                                                                            union_right_parenthesis=')'
                                                                            right_union_expression = union_left_parenthesis+"select t1.char_field, t1.decimal_field, "+window_function_value+alias_value+" " \
                                                                            "from "+namespace_value+"test_bucket "+bucket_alias_value+" t1 "+use_keys_value+join_expression+let_where_value+group_by_value+letting_having_value+ \
                                                                            order_by_value+asc_desc_value+limit_value+offset_value+union_right_parenthesis

                                                                        query = union_left_parenthesis+"select t1.char_field, t1.decimal_field, "+window_function_value+alias_value+\
                                                                                " from "+namespace_value+"test_bucket "+bucket_alias_value+" t1 "+use_keys_value+join_expression+let_where_value+group_by_value+\
                                                                            letting_having_value+order_by_value+asc_desc_value+limit_value+offset_value+union_right_parenthesis+union_value+right_union_expression
                                                                        result.append(query)
                                                                        counter+=1

        return result

    def test_from_select_batches(self):
        queries = self.generate_from_select_queries()
        batches = self.produce_batches(queries, 4)
        for batch in batches:
            threads = []
            for b in batch:
                t = threading.Thread(target=self._run_test, args=(b,))
                t.daemon = True
                threads.append(t)
                t.start()
            for th in threads:
                th.join()
                threads.remove(th)

    def _run_test(self, query):
        try:
            self.run_cbq_query(query)
        except CBQError as e:
            self.assertEqual('True', 'False', 'Wrong query - '+str(query))

    def test_select_from_batches(self):
        queries = self.generate_select_from_queries()
        batches = self.produce_batches(queries, 4)

        for batch in batches:
            threads = []
            for b in batch:
                t = threading.Thread(target=self._run_test, args=(b,))
                t.daemon = True
                threads.append(t)
                t.start()
            for th in threads:
                th.join()
                threads.remove(th)


    def produce_batches(self, queries, batch_size):
        result = []
        counter = 0
        arr = []
        for query in queries:
            if counter<batch_size:
                arr.append(query)
                counter+=1
            else:
                add = copy.copy(arr)
                result.append(add)
                arr = []
                arr.append(query)
                counter = 1
        return result

    def init_nodes(self):
        test_bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket("test_bucket", 11222, test_bucket_params)

    def load_test_data(self, bucket_name='test_bucket'):
        for i in range(0, 1, 1):
            initial_statement = (" INSERT INTO {0} (KEY, VALUE) VALUES ('primary_key_"+str(i)+"',").format(bucket_name)
            initial_statement += "{"
            initial_statement += "'primary_key':'primary_key_"+str(i) + "','char_field':'" + random.choice(string.ascii_uppercase) + \
                                  "','decimal_field':"+str(round(10000*random.random(), 0))+",'int_field':"+str(randint(0, 100000000))+"})"
            self.run_cbq_query(initial_statement)

    def adopt_test_data(self, bucket_name='test_bucket'):
        self.run_cbq_query("update {0} set decimal_field=null where char_field='A'".format(bucket_name))
        self.run_cbq_query("update {0} set decimal_field=missing where char_field='B'".format(bucket_name))

        self.run_cbq_query("update {0} set decimal_field=null where char_field='C' and decimal_field%2=0".format(bucket_name))
        self.run_cbq_query("update {0} set decimal_field=missing where char_field='C' and decimal_field%3=0".format(bucket_name))

        self.run_cbq_query("update {0} set decimal_field=2 where char_field='D' and decimal_field%2=0".format(bucket_name))
        self.run_cbq_query("update {0} set decimal_field=1 where char_field='E'".format(bucket_name))

    def create_primary_index(self, bucket_name='test_bucket'):
        self.run_cbq_query("CREATE PRIMARY INDEX `#primary` ON `{0}`".format(bucket_name))

    def create_secondary_indexes(self, bucket_name='test_bucket'):
        self.run_cbq_query('CREATE INDEX ix_char ON {0}(char_field);'.format(bucket_name))
        self.run_cbq_query('CREATE INDEX ix_decimal ON {0}(decimal_field);'.format(bucket_name))
        self.run_cbq_query('CREATE INDEX ix_int ON {0}(int_field);'.format(bucket_name))
        self.run_cbq_query('CREATE INDEX ix_primary ON {0}(primary_key);'.format(bucket_name))