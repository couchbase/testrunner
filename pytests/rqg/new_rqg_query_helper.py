import random
import copy
import string
import pprint
from .base_query_helper import BaseRQGQueryHelper


'''
N1QL PARSE ORDER
1.  with
2.  from
3.  let
4.  where
5.  group
6.  letting
7.  having
8.  select
9.  order by
10. offset
11. limit

RQG RESERVED KEYWORD LIST

*CLAUSE KEYWORD*
WITH_START
WITH_END
FROM_START
FROM_END
LET_START
LET_END
WHERE_START
WHERE_END
GROUPBY_START
GROUPBY_END
LETTING_START
LETTING_END
HAVING_START
HAVING_END
SELECT_START
SELECT_END
ORDERBY_START
ORDERBY_END
OFFSET_START
OFFSET_END
LIMIT_START
LIMIT_END

*INTERNAL KEYWORDS*
CTE_START
CTE_END
WITH_CLAUSE_SUBQUERY
NESTED_WITH_CLAUSE_SUBQUERY
CHAINED_WITH_CLAUSE_SUBQUERY
WITH_CLAUSE_CONSTANT
CTE_ALIAS
WITH_CLAUSE_ALIAS
FIELDS
FIELDS_CONDITION
FROM_FIELD
FROM_CLAUSE
LEFT OUTER JOIN
RIGHT OUTER JOIN
INNER JOIN
WITH_TEMPLATE
WITH_EXPRESSION_TEMPLATES
WITH_EXPRESSION_ORDER
WITH_EXPRESSIONS
WITH_FIELDS
WITH_CLAUSE
FROM_TEMPLATE
BUCKET_NAME
TABLE_AND_CTE_JOIN
TABLE_CTE
TABLE_TABLE
CTE_CTE
CTE_TABLE
WHERE_CLAUSE
'''


class RQGQueryHelperNew(BaseRQGQueryHelper):

    ''' Dispatcher function. Uses test_name parameter to identify the way how templates
        will be transformed into SQL and N1QL queries.
        from let where group by letting haVING SELECT ORDER BY'''
    def _get_conversion_func(self, test_name):
        if test_name == 'group_by_alias':
            return self._convert_sql_template_for_group_by_aliases
        elif test_name == 'skip_range_key_scan':
            return self._convert_sql_template_for_skip_range_scan
        elif test_name == 'common_table_expression':
            return self._convert_sql_template_for_common_table_expression
        elif test_name == 'ansi_joins':
            return self._convert_sql_template_for_ansi_join
        elif test_name == 'window_functions':
            return self._convert_sql_template_for_window_functions
        else:
            print("Unknown test name")
            exit(1)

    def log_info(self, object):
        if not self.debug_logging:
            return
        pprint.pprint(object)

    def _convert_sql_template_for_common_table_expression(self, query_template, conversion_map):
        table_map = conversion_map.get("table_map", {})
        template_map = self._extract_clauses(query_template)
        template_map = self._convert_with_clause_template_n1ql(conversion_map, template_map)
        template_map = self._convert_from_clause_template_n1ql(conversion_map, template_map)
        template_map = self._convert_where_clause_template_n1ql(conversion_map, template_map)
        template_map = self._convert_select_clause_template_n1ql(conversion_map, template_map)

        n1ql_template_map = copy.deepcopy(template_map)
        sql_template_map = copy.deepcopy(template_map)

        n1ql_template_map = self._convert_select_subquery_for_n1ql(n1ql_template_map)
        n1ql_template_map["N1QL"] = self._combine_converted_clauses(n1ql_template_map)

        sql_template_map = self._convert_on_clause_for_sql(sql_template_map)
        sql_template_map["SQL"] = self._combine_converted_clauses(sql_template_map)
        sql_template_map["SQL"] = sql_template_map["SQL"].replace(" RAW ", " ")

        indexes = {}
        indexes = self.create_join_index(conversion_map, template_map, indexes)
        query_map = {"n1ql": n1ql_template_map['N1QL'],  "sql": sql_template_map['SQL'],
                     "bucket": str(",".join(list(table_map.keys()))),
                     "expected_result": None, "indexes": indexes,
                     "tests": ["BASIC"]}
        query_map = self.convert_table_name(query_map, conversion_map)
        return query_map

    def _convert_on_clause_for_sql(self, template_map):
        from_map = template_map['FROM_FIELD']
        from_type = from_map['type']
        if from_type == "joins":
            from_clause = template_map['FROM_CLAUSE']
            on_clause = from_clause.split(" ON ")[1].strip("(").strip(")").replace("==", "=")
            from_clause = from_clause.split(" ON ")[0] + " ON " + on_clause
            template_map['FROM_CLAUSE'] = from_clause
        return template_map

    def _convert_select_subquery_for_n1ql(self, template_map):
        select_subqueries = self.get_from_dict(template_map, ['SELECT_FIELDS', 'SUBQUERIES'])
        if select_subqueries == []:
            return template_map
        subquery_aliases = list(select_subqueries.keys())
        for alias in subquery_aliases:
            subquery_select = select_subqueries[alias]["SELECT_CLAUSE"]
            new_select_clause = "SELECT RAW " + subquery_select.split("SELECT")[1]
            select_subqueries[alias]["SELECT_CLAUSE"] = new_select_clause

        self.set_in_dict(template_map, ['SELECT_FIELDS', 'SUBQUERIES'], select_subqueries)

        select_fields = self.get_from_dict(template_map, ['SELECT_FIELDS', 'FIELDS'])
        final_select_fields = []
        for field_info in select_fields:
            if field_info['ALIAS'] != '':
                field = field_info['FIELD'] + " AS " + field_info['ALIAS']
            else:
                field = field_info['FIELD']
            final_select_fields.append(field)

        subquery_dict = self.get_from_dict(template_map, ['SELECT_FIELDS', 'SUBQUERIES'])
        for subquery_alias in list(subquery_dict.keys()):
            converted_subquery_map = self.get_from_dict(template_map, ['SELECT_FIELDS', 'SUBQUERIES', subquery_alias])
            combined_subquery = "(" + self._combine_converted_clauses(converted_subquery_map) + ")[0] AS " + subquery_alias
            final_select_fields.append(combined_subquery)

        select_clause = "SELECT"
        for field in final_select_fields:
            select_clause += " " + field + ","
        select_clause = self._remove_trailing_substring(select_clause.strip(), ",")
        self.set_in_dict(template_map, ['SELECT_CLAUSE'], select_clause)
        return template_map

    def create_join_index(self, conversion_map, template_map, indexes={}):
        table_name = conversion_map.get("table_name", "simple_table")
        from_map = template_map['FROM_FIELD']
        from_type = from_map['type']
        if from_type == "joins":
            join_type = from_map['join_type']
            random_index_name = "join_index_" + str(self._random_int())
            if join_type == "LEFT OUTER JOIN":
                statement = "create index " + random_index_name + " on " + table_name + "(" + from_map['right_on_field'] + ")"
                indexes[random_index_name] = {"name": random_index_name, "type": "GSI", "definition": statement}
            elif join_type == "RIGHT OUTER JOIN" or join_type == "INNER JOIN":
                statement = "create index " + random_index_name + " on " + table_name + "(" + from_map['left_on_field'] + ")"
                indexes[random_index_name] = {"name": random_index_name, "type": "GSI", "definition": statement}
            else:
                pass
        return indexes

    def _extract_clauses(self, query_template):
        with_sep = ("WITH_TEMPLATE", "WITH_START", "WITH_END")
        from_sep = ("FROM_TEMPLATE", "FROM_START", "FROM_END")
        let_sep = ("LET_TEMPLATE", "LET_START", "LET_END")
        where_sep = ("WHERE_TEMPLATE", "WHERE_START", "WHERE_END")
        groupby_sep = ("GROUPBY_TEMPLATE", "GROUPBY_START", "GROUPBY_END")
        letting_sep = ("LETTING_TEMPLATE", "LETTING_START", "LETTING_END")
        having_sep = ("HAVING_TEMPLATE", "HAVING_START", "HAVING_END")
        select_sep = ("SELECT_TEMPLATE", "SELECT_START", "SELECT_END")
        orderby_sep = ("ORDERBY_TEMPLATE", "ORDERBY_START", "ORDERBY_END")
        offset_sep = ("OFFSET_TEMPLATE", "OFFSET_START", "OFFSET_END")
        limit_sep = ("LIMIT_TEMPLATE", "LIMIT_START", "LIMIT_END")

        clause_seperators = [with_sep, from_sep, let_sep, where_sep, groupby_sep,
                             letting_sep, having_sep, select_sep, orderby_sep, offset_sep, limit_sep]
        parsed_clauses = dict()
        parsed_clauses['RAW_QUERY_TEMPLATE'] = query_template
        for clause_seperator in clause_seperators:
            clause = clause_seperator[0]
            start_sep = clause_seperator[1]
            end_sep = clause_seperator[2]
            result = []
            tmp = query_template.split(start_sep)
            for substring in tmp:
                if end_sep in substring:
                    result.append(substring.split(end_sep)[0].strip())
            parsed_clauses[clause] = result
        return parsed_clauses

    def _convert_with_clause_template_n1ql(self, conversion_map, template_map, key_path=[]):
        with_template = self.get_from_dict(template_map, key_path+['WITH_TEMPLATE', 0])
        self.set_in_dict(template_map, key_path+['WITH_EXPRESSION_TEMPLATES'], {})
        self.set_in_dict(template_map, key_path+['WITH_EXPRESSION_ORDER'], {})
        start_sep = "CTE_START"
        end_sep = "CTE_END"
        tmp = with_template.split(start_sep)
        i = 0
        for substring in tmp:
            if end_sep in substring:
                random_alias = 'CTE_' + ''.join(random.choice(string.ascii_uppercase) for _ in range(5))
                clause_template = substring.split(end_sep)[0].strip()
                self.set_in_dict(template_map, key_path+['WITH_EXPRESSION_TEMPLATES', random_alias], clause_template)
                self.set_in_dict(template_map, key_path+['WITH_EXPRESSION_ORDER', random_alias], i)
                i += 1
        with_ordering = []
        with_expression_ordering_map = self.get_from_dict(template_map, key_path+['WITH_EXPRESSION_ORDER'])
        for with_alias in list(with_expression_ordering_map.keys()):
            with_ordering.append((with_alias, self.get_from_dict(template_map, key_path+['WITH_EXPRESSION_ORDER', with_alias])))

        with_ordering.sort(key=lambda x: x[1])

        self.set_in_dict(template_map, key_path+['WITH_EXPRESSIONS'], {})
        self.set_in_dict(template_map, key_path+['WITH_FIELDS'], {})

        for with_order in with_ordering:
            with_expression_alias = with_order[0]
            template_map = self._convert_with_expression(with_expression_alias, template_map, conversion_map, key_path=key_path)
        converted_with_clause = "WITH"
        for with_order in with_ordering:
            converted_with_expression_alias = with_order[0]
            expression = self.get_from_dict(template_map, key_path+['WITH_EXPRESSIONS', converted_with_expression_alias])
            converted_with_clause += " " + converted_with_expression_alias + " as " + "(" + expression + "),"

        with_clause = str(self._remove_trailing_substring(converted_with_clause.strip(), ","))
        self.set_in_dict(template_map, key_path+['WITH_CLAUSE'], with_clause)
        return template_map

    def _convert_with_expression(self, alias, template_map, conversion_map, key_path=[]):
        expression = self.get_from_dict(template_map, key_path+['WITH_EXPRESSION_TEMPLATES', alias])
        if expression == "WITH_CLAUSE_SUBQUERY":
            query_template = "SELECT_START SELECT FIELDS SELECT_END FROM_START FROM BUCKET_NAME_WITH_ALIAS FROM_END WHERE_START WHERE FIELDS_CONDITION WHERE_END"
            self.set_in_dict(template_map, key_path+['WITH_EXPRESSION_TEMPLATES', alias], self._extract_clauses(query_template))
            template_map = self._convert_from_clause_template_n1ql(conversion_map, template_map, key_path=key_path+['WITH_EXPRESSION_TEMPLATES', alias])
            template_map = self._convert_where_clause_template_n1ql(conversion_map, template_map, key_path=key_path+['WITH_EXPRESSION_TEMPLATES', alias])
            template_map = self._convert_select_clause_template_n1ql(conversion_map, template_map, key_path=key_path+['WITH_EXPRESSION_TEMPLATES', alias])
            with_expression = self._combine_converted_clauses(template_map, key_path=key_path+['WITH_EXPRESSION_TEMPLATES', alias])
            self.set_in_dict(template_map, key_path+['WITH_EXPRESSIONS', alias], with_expression)
            self.set_in_dict(template_map, key_path+['WITH_FIELDS', alias], self._extract_fields_from_clause("SELECT", template_map, conversion_map, key_path=key_path+['WITH_EXPRESSION_TEMPLATES', alias]))
        elif expression == "NESTED_WITH_CLAUSE_SUBQUERY":
            query_template = "WITH_START WITH CTE_START WITH_CLAUSE_SUBQUERY CTE_END WITH_END SELECT_START SELECT FIELDS SELECT_END FROM_START FROM WITH_CLAUSE_ALIAS FROM_END WHERE_START WHERE FIELDS_CONDITION WHERE_END"
            self.set_in_dict(template_map, key_path+['WITH_EXPRESSION_TEMPLATES', alias], self._extract_clauses(query_template))
            template_map = self._convert_with_clause_template_n1ql(conversion_map, template_map, key_path=key_path+['WITH_EXPRESSION_TEMPLATES', alias])
            template_map = self._convert_from_clause_template_n1ql(conversion_map, template_map, key_path=key_path+['WITH_EXPRESSION_TEMPLATES', alias])
            template_map = self._convert_where_clause_template_n1ql(conversion_map, template_map, key_path=key_path+['WITH_EXPRESSION_TEMPLATES', alias])
            template_map = self._convert_select_clause_template_n1ql(conversion_map, template_map, key_path=key_path+['WITH_EXPRESSION_TEMPLATES', alias])
            with_expression = self._combine_converted_clauses(template_map, key_path=key_path+['WITH_EXPRESSION_TEMPLATES', alias])
            self.set_in_dict(template_map, key_path+['WITH_EXPRESSIONS', alias], with_expression)
            self.set_in_dict(template_map, key_path+['WITH_FIELDS', alias], self._extract_fields_from_clause("SELECT", template_map, conversion_map, key_path=key_path+['WITH_EXPRESSION_TEMPLATES', alias]))
        elif expression == "CHAINED_WITH_CLAUSE_SUBQUERY":
            query_template = "SELECT_START SELECT FIELDS SELECT_END FROM_START FROM CTE_ALIAS FROM_END WHERE_START WHERE CTE_FIELDS_CONDITION WHERE_END"
            self.set_in_dict(template_map, key_path+['WITH_EXPRESSION_TEMPLATES', alias], self._extract_clauses(query_template))
            template_map = self._convert_from_clause_template_n1ql(conversion_map, template_map, key_path=key_path+['WITH_EXPRESSION_TEMPLATES', alias])
            template_map = self._convert_where_clause_template_n1ql(conversion_map, template_map, key_path=key_path+['WITH_EXPRESSION_TEMPLATES', alias])
            template_map = self._convert_select_clause_template_n1ql(conversion_map, template_map, key_path=key_path+['WITH_EXPRESSION_TEMPLATES', alias])
            with_expression = self._combine_converted_clauses(template_map, key_path=key_path+['WITH_EXPRESSION_TEMPLATES', alias])
            self.set_in_dict(template_map, key_path+['WITH_EXPRESSIONS', alias], with_expression)
            self.set_in_dict(template_map, key_path+['WITH_FIELDS', alias], self._extract_fields_from_clause("SELECT", template_map, conversion_map, key_path=key_path+['WITH_EXPRESSION_TEMPLATES', alias]))
        else:
            print("Unknown with expression template")
            exit(1)
        return template_map

    def _convert_from_clause_template_n1ql(self, conversion_map, template_map, key_path=[]):
        from_template = self.get_from_dict(template_map, key_path+['FROM_TEMPLATE', 0])
        from_expression = from_template.split("FROM ")[1].strip()
        from_clause = "FROM"
        table_name = conversion_map.get("table_name", "simple_table")
        if from_expression == "BUCKET_NAME":
            # any select is from the default table/bucket
            from_clause += " " + table_name
            from_map = {"left_table": table_name, "class": "BUCKET_NAME",
                        'type': "basic"}
        elif from_expression == "BUCKET_NAME_WITH_ALIAS":
            # any select is from the default table/bucket
            bucket_alias = 'BUCKET_' + ''.join(random.choice(string.ascii_uppercase) for _ in range(5))
            from_clause += " " + table_name + " AS " + bucket_alias
            from_map = {"left_table": table_name, "left_table_alias": bucket_alias, "class": "BUCKET_NAME_WITH_ALIAS",
                        'type': "basic"}
        elif from_expression == "WITH_CLAUSE_ALIAS":
            # outer most select is from a with clause alias
            with_clause_aliases = self.get_from_dict(template_map, key_path+['WITH_EXPRESSIONS'])
            with_clause_aliases = list(with_clause_aliases.keys())
            with_alias = random.choice(with_clause_aliases)
            from_clause += " " + with_alias
            from_map = {"left_table": with_alias, "class": "WITH_ALIAS", "type": "basic"}
        elif from_expression == "FROM_SUBQUERY_CTE":
            from_subquery = self._get_raw_subquery_template("FROM_SUBQUERY_WITH_CTE")
            subquery_template = self._extract_clauses(from_subquery)
            subquery_template = self._convert_subquery(conversion_map, subquery_template, key_path=[])
            subquery_alias = 'SUBQUERY_' + ''.join(random.choice(string.ascii_uppercase) for _ in range(5))
            combined_subquery = "(" + self._combine_converted_clauses(subquery_template) + ") AS " + subquery_alias
            from_map = {"subquery_alias": subquery_alias, "subquery_template": subquery_template, "subquery": combined_subquery,
                        "class": "SUBQUERY_CTE", "type": "subquery"}
            from_clause += " " + combined_subquery
        elif from_expression == "CTE_ALIAS":
            # select in a cte is from a previous cte
            last_index = 0
            i = 0
            for part in key_path:
                if part == 'WITH_EXPRESSION_TEMPLATES':
                    last_index = i
                i += 1
            with_expression_order_key_path = key_path[:last_index]
            with_expression_ordering = self.get_from_dict(template_map, with_expression_order_key_path + ['WITH_EXPRESSION_ORDER'])
            target_alias = key_path[-1]
            target_alias_order = with_expression_ordering[target_alias]
            source_aliases = []
            for source_alias in list(with_expression_ordering.keys()):
                if with_expression_ordering[source_alias] < target_alias_order:
                    source_aliases.append(source_alias)
            if len(source_aliases) == 0:
                print("No with clause to chain to")
                exit(1)
            source_alias = random.choice(source_aliases)
            from_clause += " " + source_alias + " " + "AS" + " " + source_alias+source_alias
            from_map = {"left_table": source_alias, "left_table_alias": source_alias+source_alias,
                        "class": "CTE_ALIAS", "type": "basic"}
        elif from_expression == "TABLE_AND_CTE_JOIN":
            with_clause_aliases = self.get_from_dict(template_map, key_path+['WITH_EXPRESSIONS'])
            with_clause_aliases = list(with_clause_aliases.keys())
            if len(with_clause_aliases) < 2:
                join_order = random.choice(['TABLE_CTE', 'CTE_TABLE', 'TABLE_TABLE'])
            else:
                with_clause_aliases = self.get_from_dict(template_map, key_path+['WITH_EXPRESSIONS'])
                with_clause_aliases = list(with_clause_aliases.keys())
                left_table = random.choice(with_clause_aliases)
                with_clause_aliases.remove(left_table)
                right_table = random.choice(with_clause_aliases)

                left_with_alias_fields = self.get_from_dict(template_map, key_path+['WITH_FIELDS', left_table])
                left_with_alias_fields = [tuple[0] for tuple in left_with_alias_fields]

                right_with_alias_fields = self.get_from_dict(template_map, key_path+['WITH_FIELDS', right_table])
                right_with_alias_fields = [tuple[0] for tuple in right_with_alias_fields]

                common_fields = [field for field in left_with_alias_fields if field in right_with_alias_fields]
                if len(common_fields) == 0:
                    join_order = random.choice(['TABLE_CTE', 'TABLE_TABLE', 'CTE_TABLE'])
                else:
                    join_order = random.choice(['CTE_CTE', 'TABLE_CTE', 'TABLE_TABLE', 'CTE_CTE'])

            if join_order == "TABLE_CTE" or join_order == "CTE_TABLE":
                # select is from a table/bucket joined to a cte
                with_clause_aliases = self.get_from_dict(template_map, key_path+['WITH_EXPRESSIONS'])
                with_clause_aliases = list(with_clause_aliases.keys())
                with_alias = random.choice(with_clause_aliases)
                with_alias_fields = self.get_from_dict(template_map, key_path+['WITH_FIELDS', with_alias])
                with_alias_fields = [tuple[0] for tuple in with_alias_fields]
                join_field = random.choice(with_alias_fields)
                join_type = random.choice(["LEFT OUTER JOIN", "RIGHT OUTER JOIN", "INNER JOIN"])

                if join_order == "CTE_TABLE":
                    left_table = with_alias
                    right_table = table_name

                if join_order == "TABLE_CTE":
                    left_table = table_name
                    right_table = with_alias

                from_clause += " " + left_table + " " + join_type + " " + right_table + " " + "ON" + " " + "(" + left_table + "." + join_field + " " + "==" + " " + right_table + "." + join_field + ")"
                from_map = {"left_table": left_table, "class": "TABLE_AND_CTE_JOIN", "type": "joins",
                            "right_table": right_table, "left_on_field": join_field, "right_on_field": join_field,
                            "join_type": join_type}
            elif join_order == "TABLE_TABLE":
                table_map = conversion_map.get("table_map", {})
                table_fields = list(table_map[table_name]["fields"].keys())
                join_field = random.choice(table_fields)
                join_type = random.choice(["LEFT OUTER JOIN", "RIGHT OUTER JOIN", "INNER JOIN"])
                left_table = table_name
                left_table_alias = left_table+"_ALIAS_LEFT"
                right_table = table_name
                right_table_alias = right_table+"_ALIAS_RIGHT"
                from_clause += " " + left_table + " AS " + left_table_alias + " " + join_type + " " + right_table + " AS " + right_table_alias + " " + "ON" + " " + "(" + left_table_alias + "." + join_field + " " + "==" + " " + right_table_alias + "." + join_field + ")"
                from_map = {"left_table": left_table, "left_table_alias": left_table_alias, "class": "TABLE_AND_CTE_JOIN", "type": "joins",
                        "right_table": right_table, "right_table_alias": right_table_alias, "left_on_field": join_field, "right_on_field": join_field,
                        "join_type": join_type}
            elif join_order == "CTE_CTE":
                left_table_alias = left_table + "_ALIAS_LEFT"
                right_table_alias = right_table + "_ALIAS_RIGHT"

                join_field = random.choice(common_fields)

                join_type = random.choice(["LEFT OUTER JOIN", "RIGHT OUTER JOIN", "INNER JOIN"])
                from_clause += " " + left_table + " AS " + left_table_alias + " " + join_type + " " + right_table + " AS " + right_table_alias + " " + "ON" + " " + "(" + left_table_alias + "." + join_field + " " + "==" + " " + right_table_alias + "." + join_field + ")"
                from_map = {"left_table": left_table, "left_table_alias": left_table_alias, "class": "TABLE_AND_CTE_JOIN", "type": "joins",
                            "right_table": right_table, "right_table_alias": right_table_alias, "left_on_field": join_field, "right_on_field": join_field,
                            "join_type": join_type}
        else:
            print(("Unknown from clause type: " + from_expression))
            exit(1)
        self.set_in_dict(template_map, key_path+['FROM_FIELD'], from_map)
        self.set_in_dict(template_map, key_path+['FROM_CLAUSE'], str(from_clause))
        return template_map

    def _convert_where_clause_template_n1ql(self, conversion_map, template_map, key_path=[]):
        where_clause = "WHERE"
        from_map = self.get_from_dict(template_map, key_path+['FROM_FIELD'])
        from_class = from_map["class"]
        table_map = conversion_map.get("table_map", {})

        where_template = self.get_from_dict(template_map, key_path+['WHERE_TEMPLATE', 0])
        where_expression = where_template.split("WHERE ")[1].strip()

        num_where_comparisons = random.randint(0, 4)
        if num_where_comparisons == 0:
            where_clause = ""
            self.set_in_dict(template_map, key_path+['WHERE_CLAUSE'], where_clause)
            return template_map

        if from_class == "BUCKET_NAME":
            # need to add random field selection from bucket
            from_table = from_map["left_table"]
            all_fields = list(table_map[from_table]["fields"].keys())
            if where_expression == "FIELDS_CONDITION":
                for i in range(0, num_where_comparisons):
                    random_field = random.choice(all_fields)
                    random_constant = self._random_constant(random_field)
                    comparator = random.choice(['<', '>', '=', '!='])
                    conjunction = random.choice(['AND', 'OR'])
                    where_clause += " " + random_field + " " + comparator + " " + str(random_constant) + " " + conjunction
                where_clause = where_clause.rsplit(' ', 1)[0]
            elif where_expression == "FIELDS_SUBQUERY_CONDITION":
                random_field = random.choice(all_fields)
                random_field_no_alias = random_field.split(".")[-1]
                subquery_select_field_no_alias = ''
                while subquery_select_field_no_alias != random_field_no_alias:
                    self.log_info("Attempting to generate proper subquery...")
                    where_subquery = self._get_raw_subquery_template("WHERE_SUBQUERY_WITH_CTE")
                    where_subquery_template = self._extract_clauses(where_subquery)
                    where_subquery_template = self._convert_subquery(conversion_map, where_subquery_template, key_path=[])
                    where_subquery_template['SELECT_CLAUSE'] = where_subquery_template['SELECT_CLAUSE'].replace("SELECT", "SELECT RAW")
                    subquery_select_field_no_alias = where_subquery_template['SELECT_FIELDS']['FIELDS'][0]['FIELD'].split(".")[-1]
                where_subquery = self._combine_converted_clauses(where_subquery_template)
                where_clause += " " + random_field + " IN " + "(" + where_subquery + ")"
                self.set_in_dict(template_map, key_path+['WHERE_SUBQUERIES'], where_subquery_template)

            else:
                print(("WHERE CLAUSE CONVERSION - Unknown where expression type: " + str(where_expression)))
                exit(1)

        elif from_class == "BUCKET_NAME_WITH_ALIAS":
            from_table = from_map["left_table"]
            all_fields = list(table_map[from_table]["fields"].keys())

            if where_expression == "FIELDS_CONDITION":
                for i in range(0, num_where_comparisons):
                    random_field = random.choice(all_fields)
                    random_field = from_map['left_table_alias'] + "." + random_field
                    random_constant = self._random_constant(random_field)
                    comparator = random.choice(['<', '>', '=', '!='])
                    conjunction = random.choice(['AND', 'OR'])
                    where_clause += " " + random_field + " " + comparator + " " + str(random_constant) + " " + conjunction
                where_clause = where_clause.rsplit(' ', 1)[0]
            elif where_expression == "FIELDS_SUBQUERY_CONDITION":
                random_field = random.choice(all_fields)
                random_field_no_alias = random_field.split(".")[-1]
                subquery_select_field_no_alias = ''
                while subquery_select_field_no_alias != random_field_no_alias:
                    self.log_info("Attempting to generate proper subquery...")
                    where_subquery = self._get_raw_subquery_template("WHERE_SUBQUERY_WITH_CTE")
                    where_subquery_template = self._extract_clauses(where_subquery)
                    where_subquery_template = self._convert_subquery(conversion_map, where_subquery_template, key_path=[])
                    where_subquery_template['SELECT_CLAUSE'] = where_subquery_template['SELECT_CLAUSE'].replace("SELECT", "SELECT RAW")
                    subquery_select_field_no_alias = where_subquery_template['SELECT_FIELDS']['FIELDS'][0]['FIELD'].split(".")[-1]
                where_subquery = self._combine_converted_clauses(where_subquery_template)
                random_field = from_map['left_table_alias'] + "." + random_field
                where_clause += " " + random_field + " IN " + "(" + where_subquery + ")"
                self.set_in_dict(template_map, key_path+['WHERE_SUBQUERIES'], where_subquery_template)
            else:
                print(("WHERE CLAUSE CONVERSION - Unknown where expression type: " + str(where_expression)))
                exit(1)

        elif from_class == "SUBQUERY_CTE":
            subquery_alias = from_map["subquery_alias"]
            subquery_select_fields = self.get_from_dict(template_map, key_path+['FROM_FIELD', 'subquery_template', 'SELECT_FIELDS', 'FIELDS'])
            all_fields = [field_tuple['FIELD'] for field_tuple in subquery_select_fields]

            if where_expression == "FIELDS_CONDITION":
                for i in range(0, num_where_comparisons):
                    random_field = random.choice(all_fields)
                    random_field = subquery_alias + "." + random_field
                    random_constant = self._random_constant(random_field)
                    comparator = random.choice(['<', '>', '=', '!='])
                    conjunction = random.choice(['AND', 'OR'])
                    where_clause += " " + random_field + " " + comparator + " " + str(random_constant) + " " + conjunction
                where_clause = where_clause.rsplit(' ', 1)[0]

            elif where_expression == "FIELDS_SUBQUERY_CONDITION":
                random_field = random.choice(all_fields)
                random_field_no_alias = random_field.split(".")[-1]
                subquery_select_field_no_alias = ''
                while subquery_select_field_no_alias != random_field_no_alias:
                    self.log_info("Attempting to generate proper subquery...")
                    where_subquery = self._get_raw_subquery_template("WHERE_SUBQUERY_WITH_CTE")
                    where_subquery_template = self._extract_clauses(where_subquery)
                    where_subquery_template = self._convert_subquery(conversion_map, where_subquery_template, key_path=[])
                    where_subquery_template['SELECT_CLAUSE'] = where_subquery_template['SELECT_CLAUSE'].replace("SELECT", "SELECT RAW")
                    subquery_select_field_no_alias = where_subquery_template['SELECT_FIELDS']['FIELDS'][0]['FIELD'].split(".")[-1]
                where_subquery = self._combine_converted_clauses(where_subquery_template)
                random_field = subquery_alias + "." + random_field
                where_clause += " " + random_field + " IN " + "(" + where_subquery + ")"
                self.set_in_dict(template_map, key_path+['WHERE_SUBQUERIES'], where_subquery_template)
            else:
                print(("WHERE CLAUSE CONVERSION - Unknown where expression type: " + str(where_expression)))
                exit(1)

        elif from_class == "WITH_ALIAS":
            from_table = from_map["left_table"]
            with_alias_fields = self.get_from_dict(template_map, key_path+['WITH_FIELDS', from_table])

            if where_expression == "FIELDS_CONDITION":
                for i in range(0, num_where_comparisons):
                    random_with_field_info = random.choice(with_alias_fields)
                    random_with_field = random_with_field_info[0]
                    random_constant = self._random_constant(random_with_field)
                    comparator = random.choice(['<', '>', '=', '!='])
                    conjunction = random.choice(['AND', 'OR'])
                    where_clause += " " + from_table + "." + random_with_field + " " + comparator + " " + str(random_constant) + " " + conjunction
                where_clause = where_clause.rsplit(' ', 1)[0]
            elif where_expression == "FIELDS_SUBQUERY_CONDITION":
                random_field = random.choice(with_alias_fields)
                random_field = random_field[0]
                random_field_no_alias = random_field.split(".")[-1]
                subquery_select_field_no_alias = ''
                while subquery_select_field_no_alias != random_field_no_alias:
                    self.log_info("Attempting to generate proper subquery...")
                    where_subquery = self._get_raw_subquery_template("WHERE_SUBQUERY_WITH_CTE")
                    where_subquery_template = self._extract_clauses(where_subquery)
                    where_subquery_template = self._convert_subquery(conversion_map, where_subquery_template, key_path=[])
                    where_subquery_template['SELECT_CLAUSE'] = where_subquery_template['SELECT_CLAUSE'].replace("SELECT", "SELECT RAW")
                    subquery_select_field_no_alias = where_subquery_template['SELECT_FIELDS']['FIELDS'][0]['FIELD'].split(".")[-1]
                where_subquery = self._combine_converted_clauses(where_subquery_template)
                random_field = from_table + "." + random_field
                where_clause += " " + random_field + " IN " + "(" + where_subquery + ")"
                self.set_in_dict(template_map, key_path+['WHERE_SUBQUERIES'], where_subquery_template)
            else:
                print(("WHERE CLAUSE CONVERSION - Unknown where expression type: " + str(where_expression)))
                exit(1)

        elif from_class == "CTE_ALIAS":
            from_table = from_map["left_table"]
            from_table_alias = from_map["left_table_alias"]
            last_index = 0
            i = 0
            for part in key_path:
                if part == 'WITH_EXPRESSION_TEMPLATES':
                    last_index = i
                i += 1
            with_fields_key_path = key_path[:last_index]

            with_fields = self.get_from_dict(template_map, with_fields_key_path + ['WITH_FIELDS', from_table])
            source_fields = [field_tuple[0] for field_tuple in with_fields]

            if where_expression == "CTE_FIELDS_CONDITION":
                for i in range(0, num_where_comparisons):
                    where_field = random.choice(source_fields)
                    comparator = random.choice(['<', '>', '=', '!='])
                    conjunction = random.choice(['AND', 'OR'])
                    random_constant = self._random_constant(where_field)
                    where_clause += " " + from_table_alias + "." + where_field + " " + comparator + " " + str(random_constant) + " " + conjunction
                where_clause = where_clause.rsplit(' ', 1)[0]
            elif where_expression == "FIELDS_SUBQUERY_CONDITION":
                random_field = random.choice(source_fields)
                random_field_no_alias = random_field.split(".")[-1]
                subquery_select_field_no_alias = ''
                while subquery_select_field_no_alias != random_field_no_alias:
                    self.log_info("Attempting to generate proper subquery...")
                    where_subquery = self._get_raw_subquery_template("WHERE_SUBQUERY_WITH_CTE")
                    where_subquery_template = self._extract_clauses(where_subquery)
                    where_subquery_template = self._convert_subquery(conversion_map, where_subquery_template, key_path=[])
                    where_subquery_template['SELECT_CLAUSE'] = where_subquery_template['SELECT_CLAUSE'].replace("SELECT", "SELECT RAW")
                    subquery_select_field_no_alias = where_subquery_template['SELECT_FIELDS']['FIELDS'][0]['FIELD'].split(".")[-1]
                where_subquery = self._combine_converted_clauses(where_subquery_template)
                random_field = from_table_alias + "." + random_field
                where_clause += " " + random_field + " IN " + "(" + where_subquery + ")"
                self.set_in_dict(template_map, key_path+['WHERE_SUBQUERIES'], where_subquery_template)
            else:
                print(("WHERE CLAUSE CONVERSION - Unknown where expression type: " + str(where_expression)))
                exit(1)
        elif from_class == "TABLE_AND_CTE_JOIN":
            from_map = self.get_from_dict(template_map, key_path + ['FROM_FIELD'])

            left_table = from_map['left_table']
            left_table_alias = from_map.get('left_table_alias', "NO_ALIAS")
            if left_table_alias == "NO_ALIAS":
                left_table_alias = left_table

            if left_table.startswith("CTE"):
                left_table_fields = self.get_from_dict(template_map, key_path + ['WITH_FIELDS', left_table])
                left_table_fields = [tuple[0] for tuple in left_table_fields]
            else:
                left_table_fields = list(table_map[left_table]["fields"].keys())

            right_table = from_map['right_table']
            right_table_alias = from_map.get('right_table_alias', "NO_ALIAS")
            if right_table_alias == "NO_ALIAS":
                right_table_alias = right_table

            if right_table.startswith("CTE"):
                right_table_fields = self.get_from_dict(template_map, key_path + ['WITH_FIELDS', right_table])
                right_table_fields = [tuple[0] for tuple in right_table_fields]
            else:
                right_table_fields = list(table_map[right_table]["fields"].keys())

            if where_expression == "FIELDS_CONDITION":
                for i in range(0, num_where_comparisons):
                    comparator = random.choice(['<', '>', '=', "!="])
                    conjunction = random.choice(['AND', 'OR'])
                    if random.choice(["LEFT_TABLE", "RIGHT_TABLE"]) == "LEFT_TABLE":
                        where_field = random.choice(left_table_fields)
                        where_table = left_table_alias
                    else:
                        where_field = random.choice(right_table_fields)
                        where_table = right_table_alias
                    random_constant = self._random_constant(where_field)
                    where_clause += " " + where_table + "." + where_field + " " + comparator + " " + str(random_constant) + " " + conjunction
                where_clause = where_clause.rsplit(' ', 1)[0]
            elif where_expression == "FIELDS_SUBQUERY_CONDITION":
                if random.choice(["LEFT_TABLE", "RIGHT_TABLE"]) == "LEFT_TABLE":
                    where_field = random.choice(left_table_fields)
                    where_table = left_table_alias
                else:
                    where_field = random.choice(right_table_fields)
                    where_table = right_table_alias

                random_field = where_field
                random_field_no_alias = random_field.split(".")[-1]
                subquery_select_field_no_alias = ''
                while subquery_select_field_no_alias != random_field_no_alias:
                    self.log_info("Attempting to generate proper subquery...")
                    where_subquery = self._get_raw_subquery_template("WHERE_SUBQUERY_WITH_CTE")
                    where_subquery_template = self._extract_clauses(where_subquery)
                    where_subquery_template = self._convert_subquery(conversion_map, where_subquery_template, key_path=[])
                    where_subquery_template['SELECT_CLAUSE'] = where_subquery_template['SELECT_CLAUSE'].replace("SELECT", "SELECT RAW")
                    subquery_select_field_no_alias = where_subquery_template['SELECT_FIELDS']['FIELDS'][0]['FIELD'].split(".")[-1]
                where_subquery = self._combine_converted_clauses(where_subquery_template)
                random_field = where_table + "." + where_field
                where_clause += " " + random_field + " IN " + "(" + where_subquery + ")"
                self.set_in_dict(template_map, key_path+['WHERE_SUBQUERIES'], where_subquery_template)
            else:
                print(("WHERE CLAUSE CONVERSION - Unknown where expression type: " + str(where_expression)))
                exit(1)

        else:
            print(("WHERE CLAUSE CONVERSION - Unknown from expression type: " + str(from_class)))
            exit(1)

        self.set_in_dict(template_map, key_path+['WHERE_CLAUSE'], where_clause)
        return template_map

    def _convert_select_clause_template_n1ql(self, conversion_map, template_map, key_path=[]):
        select_template = self.get_from_dict(template_map, key_path+['SELECT_TEMPLATE', 0])
        select_expression = select_template.split("SELECT")[1].strip()
        select_clause = "SELECT"
        from_map = self.get_from_dict(template_map, key_path+['FROM_FIELD'])

        if select_expression == "FIELDS":
            random_select_fields = self._get_random_select_fields(from_map, conversion_map, template_map, key_path=key_path)
            select_fields = {'FIELDS': [{'FIELD': field, 'ALIAS': ''} for field in random_select_fields], 'SUBQUERIES': []}
            self.set_in_dict(template_map, key_path+['SELECT_FIELDS'], select_fields)
            for field in random_select_fields:
                select_clause += " " + field + ","

        elif select_expression == "SINGLE_FIELD":
            random_select_fields = self._get_random_select_fields(from_map, conversion_map, template_map, key_path=key_path)
            select_fields = {'FIELDS': [{'FIELD': random_select_fields[0], 'ALIAS': ''}],
                             'SUBQUERIES': []}
            self.set_in_dict(template_map, key_path+['SELECT_FIELDS'], select_fields)
            select_clause += " " + random_select_fields[0] + ","

        elif select_expression == "SINGLE_AGGREGATE_FIELD":
            random_select_fields = self._get_random_select_fields(from_map, conversion_map, template_map, key_path=key_path)
            single_select_field = random_select_fields[0]
            aggregate = random.choice(['AVG', 'MIN', 'MAX', 'COUNT'])
            agg_alias = aggregate + '_' + ''.join(random.choice(string.ascii_uppercase) for _ in range(5))

            select_fields = {'FIELDS': {'FIELD': single_select_field, 'ALIAS': agg_alias, 'AGGREGATE': aggregate}, 'SUBQUERIES': []}
            self.set_in_dict(template_map, key_path+['SELECT_FIELDS'], select_fields)
            select_clause += " " + aggregate + "(" + single_select_field + ")" + " AS " + agg_alias + ","

        elif select_expression == "FIELDS_OR_SUBQUERY":
            random_select_fields = self._get_random_select_fields(from_map, conversion_map, template_map, key_path=key_path)

            num_subqueries = random.choice([1])
            subquery_dict = {}
            for _ in range(0, num_subqueries):
                subquery = self._get_raw_subquery_template(type="SUBQUERY_WITH_CTE")
                subquery_template = self._extract_clauses(subquery)
                subquery_alias = 'SUBQUERY_' + ''.join(random.choice(string.ascii_uppercase) for _ in range(5))
                subquery_dict[subquery_alias] = subquery_template

            select_fields = {'FIELDS': [{'FIELD': field, 'ALIAS': ''} for field in random_select_fields], 'SUBQUERIES': subquery_dict}
            self.set_in_dict(template_map, key_path+['SELECT_FIELDS'], select_fields)
            converted_subqueries = []

            subquery_dict = self.get_from_dict(template_map, key_path+['SELECT_FIELDS', 'SUBQUERIES'])

            for subquery_alias in list(subquery_dict.keys()):
                template_map = self._convert_subquery(conversion_map, template_map, key_path=key_path+['SELECT_FIELDS', 'SUBQUERIES', subquery_alias])
                converted_subquery_map = self.get_from_dict(template_map, key_path+['SELECT_FIELDS', 'SUBQUERIES', subquery_alias])
                combined_subquery = "(" + self._combine_converted_clauses(converted_subquery_map) + ") AS " + subquery_alias
                converted_subqueries.append(combined_subquery)

            for field in random_select_fields+converted_subqueries:
                select_clause += " " + field + ","
        else:
            print("Unknown select type")
            exit(1)

        select_clause = self._remove_trailing_substring(select_clause.strip(), ",")
        self.set_in_dict(template_map, key_path+['SELECT_CLAUSE'], select_clause)
        return template_map

    def _get_raw_subquery_template(self, type="SIMPLE"):
        if type == 'SIMPLE':
            subquery = "SELECT_START SELECT FIELDS SELECT_END FROM_START FROM BUCKET_NAME FROM_END WHERE_START WHERE FIELDS_CONDITION WHERE_END"
        elif type == "FROM_SUBQUERY_WITH_CTE":
            subquery = "WITH_START WITH CTE_START WITH_CLAUSE_SUBQUERY CTE_END WITH_END SELECT_START SELECT FIELDS SELECT_END FROM_START FROM BUCKET_NAME FROM_END WHERE_START WHERE FIELDS_CONDITION WHERE_END"
        elif type == "SUBQUERY_WITH_CTE":
            subquery = "WITH_START WITH CTE_START WITH_CLAUSE_SUBQUERY CTE_END WITH_END SELECT_START SELECT SINGLE_AGGREGATE_FIELD SELECT_END FROM_START FROM WITH_CLAUSE_ALIAS FROM_END WHERE_START WHERE FIELDS_CONDITION WHERE_END"
        elif type == "SUBQUERY_FOR_CHAINED_CTE":
            subquery = "SELECT_START SELECT FIELDS SELECT_END FROM_START FROM CTE_ALIAS FROM_END WHERE_START WHERE CTE_FIELDS_CONDITION WHERE_END"
        elif type == "WHERE_SUBQUERY_WITH_CTE":
            subquery = "WITH_START WITH CTE_START WITH_CLAUSE_SUBQUERY CTE_END WITH_END SELECT_START SELECT SINGLE_FIELD SELECT_END FROM_START FROM BUCKET_NAME_WITH_ALIAS FROM_END WHERE_START WHERE FIELDS_CONDITION WHERE_END"
        else:
            print("Unknown subquery type")
            exit(1)
        return subquery

    def _convert_query(self, conversion_map, template_map, key_path=[]):
        query_template_map = self.get_from_dict(template_map, key_path)
        query_template_keys = list(query_template_map.keys())
        if "WITH_TEMPLATE" in query_template_keys:
            template_map = self._convert_with_clause_template_n1ql(conversion_map, template_map, key_path)

        if "FROM_TEMPLATE" in query_template_keys:
            template_map = self._convert_from_clause_template_n1ql(conversion_map, template_map, key_path)

        #if "LET_TEMPLATE" in query_template_keys:
        #    template_map = self._convert_let_clause_template_n1ql(conversion_map, template_map, key_path)

        if "WHERE_TEMPLATE" in query_template_keys:
            template_map = self._convert_where_clause_template_n1ql(conversion_map, template_map, key_path)

        #if "GROUPBY_TEMPLATE" in query_template_keys:
        #    template_map = self._convert_groupby_clause_template_n1ql(conversion_map, template_map, key_path)

        #if "LETTING_TEMPLATE" in query_template_keys:
        #    template_map = self._convert_letting_clause_template_n1ql(conversion_map, template_map, key_path)

        #if "HAVING_TEMPLATE" in query_template_keys:
        #    template_map = self._convert_having_clause_template_n1ql(conversion_map, template_map, key_path)

        #if "ORDERBY_TEMPLATE" in query_template_keys:
        #    template_map = self._convert_orderby_clause_template_n1ql(conversion_map, template_map, key_path)

        #if "OFFSET_TEMPLATE" in query_template_keys:
        #    template_map = self._convert_offset_clause_template_n1ql(conversion_map, template_map, key_path)

        #if "LIMIT_TEMPLATE" in query_template_keys:
        #    template_map = self._convert_limit_clause_template_n1ql(conversion_map, template_map, key_path)

        if "SELECT_TEMPLATE" in query_template_keys:
            template_map = self._convert_select_clause_template_n1ql(conversion_map, template_map, key_path)

        return template_map

    def _convert_subquery(self, conversion_map, template_map, key_path=[]):
        subquery_template = self._convert_query(conversion_map, template_map, key_path)
        return subquery_template

    def _combine_converted_clauses(self, template_map, key_path=[]):
        clause_order = ["WITH_CLAUSE", "SELECT_CLAUSE", "FROM_CLAUSE", "LET_CLAUSE", "WHERE_CLAUSE", "GROUPBY_CLAUSE", "LETTING_CLAUSE",
                        "HAVING_CLAUSE", "ORDERBY_CLAUSE", "OFFSET_CLAUSE", "LIMIT_CLAUSE"]
        query = ""
        for clause in clause_order:
            converted_clause = self.get_from_dict(template_map, key_path).get(clause, "")
            if converted_clause != "":
                query += converted_clause + " "
        return query

    """this function takes in a dictionary and a list of keys 
    and will return the value after traversing all the keys in the list
    https://stackoverflow.com/questions/14692690/access-nested-dictionary-items-via-a-list-of-keys"""
    def get_from_dict(self, data_dict, key_list):
        for key in key_list:
            data_dict = data_dict[key]
        return data_dict

    """this function take in a dictionary, a list of keys, and a value
    and will set the value after traversing the list of keys
    https://stackoverflow.com/questions/14692690/access-nested-dictionary-items-via-a-list-of-keys"""
    def set_in_dict(self, data_dict, key_list, value):
        for key in key_list[:-1]:
            data_dict = data_dict.setdefault(key, {})
        data_dict[key_list[-1]] = value

    def _get_random_select_fields(self, from_map, conversion_map, template_map, key_path=[]):
        from_class = from_map['class']
        table_map = conversion_map.get("table_map", {})
        random_fields = []
        if from_class == "BUCKET_NAME":
            from_table = from_map['left_table']
            all_fields = list(table_map[from_table]["fields"].keys())
            random_fields = self._random_sample(all_fields)
        elif from_class == "BUCKET_NAME_WITH_ALIAS":
            from_table = from_map['left_table']
            all_fields = list(table_map[from_table]["fields"].keys())
            random_fields = self._random_sample(all_fields)
            random_fields = [from_map['left_table_alias'] + "." + random_field for random_field in random_fields]
        elif from_class == "WITH_ALIAS":
            from_table = from_map['left_table']
            all_fields = self.get_from_dict(template_map, key_path + ['WITH_FIELDS', from_table])
            all_fields = [field_tuple[0] for field_tuple in all_fields]
            if len(all_fields) == 0:
                random_fields = [from_table]
            else:
                random_fields = self._random_sample(all_fields)
                random_fields = [from_table + "." + field for field in random_fields]

        elif from_class == "CTE_ALIAS":
            from_table = from_map['left_table']
            from_table_alias = from_map['left_table_alias']
            last_index = 0
            i = 0
            for part in key_path:
                if part == 'WITH_EXPRESSION_TEMPLATES':
                    last_index = i
                i += 1
            with_fields_key_path = key_path[:last_index]
            with_fields = self.get_from_dict(template_map, with_fields_key_path + ['WITH_FIELDS'])
            target_fields = with_fields[from_table]
            all_fields = [field_tuple[0] for field_tuple in target_fields]
            random_fields = self._random_sample(all_fields)
            random_fields = [from_table_alias + "." + field for field in random_fields]

        elif from_class == "SUBQUERY_CTE":
            subquery_alias = from_map['subquery_alias']
            subquery_fields = from_map['subquery_template']['SELECT_FIELDS']['FIELDS']
            all_fields = [field_tuple['FIELD'] for field_tuple in subquery_fields]
            random_fields = self._random_sample(all_fields)
            random_fields = [subquery_alias + "." + field for field in random_fields]

        elif from_class == "TABLE_AND_CTE_JOIN":
            left_table = from_map['left_table']
            left_table_alias = from_map.get('left_table_alias', "NO_ALIAS")
            if left_table_alias == "NO_ALIAS":
                left_table_alias = left_table

            if left_table.startswith("CTE"):
                left_table_fields = self.get_from_dict(template_map, key_path + ['WITH_FIELDS', left_table])
                left_table_fields = [(left_table_alias, tuple[0]) for tuple in left_table_fields]
            else:
                left_table_fields = list(table_map[left_table]["fields"].keys())
                left_table_fields = [(left_table_alias, field) for field in left_table_fields]

            right_table = from_map['right_table']
            right_table_alias = from_map.get('right_table_alias', "NO_ALIAS")
            if right_table_alias == "NO_ALIAS":
                right_table_alias = right_table

            if right_table.startswith("CTE"):
                right_table_fields = self.get_from_dict(template_map, key_path + ['WITH_FIELDS', right_table])
                right_table_fields = [(right_table_alias, tuple[0]) for tuple in right_table_fields]
            else:
                right_table_fields = list(table_map[right_table]["fields"].keys())
                right_table_fields = [(right_table_alias, field) for field in right_table_fields]

            all_fields = []
            seen_fields = []
            check_fields = left_table_fields + right_table_fields
            random.shuffle(check_fields)
            for tuple in check_fields:
                if tuple[1] not in seen_fields:
                    seen_fields.append(tuple[1])
                    all_fields.append(tuple)
            random_fields = self._random_sample(all_fields)
            random_fields = [field[0] + "." + field[1] for field in random_fields]

        else:
            print(("Unknown from type for select clause conversion: " + str(from_class)))
            exit(1)

        return random_fields

    def convert_table_name(self, query_map, conversion_map):
        database = conversion_map['database_name']
        query_map["n1ql"] = query_map['n1ql'].replace("simple_table", database + "_" + "simple_table")
        for key in list(query_map['indexes'].keys()):
            if 'definition' in query_map['indexes'][key]:
                query_map['indexes'][key]['definition'] = query_map['indexes'][key]['definition'].replace("simple_table", database + "_" + "simple_table")
        return query_map

    def _random_sample(self, list):
        size_of_sample = random.choice(list(range(1, len(list) + 1)))
        random_sample = [list[i] for i in random.sample(range(len(list)), size_of_sample)]
        return random_sample

    def _random_constant(self, field=None):
        if field:
            if "int_field1" in field:
                random_constant = random.randrange(36787, 99912344, 1000000)
            elif "bool_field1" in field:
                random_constant = random.choice([True, False])
            elif "varchar_field1" in field:
                random_constant = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase) for _ in range(5))
                random_constant = "'%s'" %random_constant
            elif "char_field1" in field:
                random_constant = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase) for _ in range(1))
                random_constant = "'%s'" %random_constant
            elif "datetime_field1" in field:
                random_constant = "'%s'" % self._random_datetime()
            elif "decimal_field1" in field:
                random_constant = random.randrange(16, 9971, 10)
            elif "primary_key_id" in field:
                random_constant = "'%s'" % random.randrange(1, 9999, 10)
            else:
                print(("Unknown field type: " + str(field)))
                exit(1)
        else:
            constant_type = random.choice(["STRING", "INTEGER", "DECIMAL", "BOOLEAN"])
            if constant_type == "STRING":
                random_constant = ''.join(random.choice(string.ascii_uppercase) for _ in range(5))
                random_constant = "'%s'" %random_constant
            elif constant_type == "INTEGER":
                random_constant = random.randrange(36787, 99912344, 1000000)
            elif constant_type == "DECIMAL":
                random_constant = float((random.randrange(700, 997400))/100)
            elif constant_type == "BOOLEAN":
                random_constant = random.choice([True, False])
            else:
                print("Unknown constant type")
                exit(1)
        return random_constant

    def _remove_trailing_substring(self, string, ending):
        if string.endswith(ending):
            return string[:-len(ending)]
        else:
            return string

    def _extract_fields_from_clause(self, clause_type, template_map, conversion_map, key_path=[]):
        table_map = conversion_map.get("table_map", {})
        table_name = conversion_map.get("table_name", "simple_table")
        table_fields = list(table_map[table_name]["fields"].keys())
        expression = self.get_from_dict(template_map, key_path+[clause_type + "_CLAUSE"])
        if expression.find(clause_type) == -1:
            return []
        if clause_type == "SELECT":
            expression_fields_string = expression.split("SELECT")[1].split("FROM")[0].strip()
        else:
            print("Unknown clause type")
            exit(1)

        return self._extract_raw_fields_from_string(table_fields, expression_fields_string)

    def _extract_raw_fields_from_string(self, raw_field_list, string):
        raw_fields = []
        for raw_field in raw_field_list:
            if raw_field.find('char') == 0:
                idx = self.find_char_field(string)
                if idx > -1:
                    raw_fields.append((str(raw_field), idx))
            else:
                idx = string.find(raw_field)
                if idx > -1:
                    raw_fields.append((str(raw_field), idx))
        return raw_fields

    def _convert_sql_template_for_ansi_join(self, query_template, conversion_map):
        table_map = conversion_map.get("table_map", {})
        table_name = conversion_map.get("table_name", "simple_table")
        new_template = self._extract_subquery(query_template)
        subqueries = []
        aliases= []
        alias = ''
        subquery_index = 0

        # Convert the whole query without the subquery clause inside of it
        sql, table_map = self._convert_sql_template_to_value(sql=new_template['STRIPPED_QUERY'], table_map=table_map, table_name=table_name, ansi_joins=True)

        # Go through subquery templates and convert them to real queries
        for template in new_template['SUBQUERY_TEMPLATE']:
            subquery, table_map = self._convert_sql_template_to_value(sql=template, table_map=table_map, table_name=table_name, ansi_joins=True)
            # Strip out the alias name so the on clause can be fixed later
            alias_name = subquery.split("SELECT")[1].split('.')[0].strip()
            # Avoid duplicate subquery error
            subquery = subquery.replace(alias_name, "s_" + str(subquery_index))
            # Give the whole subquery an alias so it can be referred to in the on_clause
            subquery = subquery.replace("ALIAS", "subquery" + str(subquery_index) + "_s_" + str(subquery_index))
            subqueries.append((subquery, subquery_index, "s_" + str(subquery_index)))
            subquery_index += 1

        subquery_index = 0
        # An index to add special logic for the first loop through a for loop
        index = 0

        # Go through each subquery and put it back into the overall query
        for subquery in subqueries:
            # Put the subquery back into its proper place
            sql = sql.replace("STUB_" + str(subquery_index), subquery[0])
            # The on_clause of the query needs to be fixed so that the correct fields are being referenced, extract it
            old_on_clause = sql.split("ON")[1].strip().split(')')[0].strip()
            old_on_clause = old_on_clause.replace('(', '')
            # Loop through the on clause to replace the alias's that need to be changed
            for character in old_on_clause.split(" "):
                # An alias needs to be referencing a bucket that is in the join, make sure this is the case
                if "t_" in character and 'subquery' not in character:
                    alias = character.split('.')[0].strip()
                    if alias != '':
                        # We don't want to change all t_ terms to subquery, only one of them
                        if index > 0 and alias in aliases:
                            aliases.append(alias)
                        elif index == 0:
                            aliases.append(alias)
                            index += 1
                        else:
                            continue

            # Replace the aliases that need to be changed inside of the on_clause
            for alias in aliases:
                new_on_clause = old_on_clause.replace(alias, 'subquery' + str(subquery[1]) + "_" + subquery[2])
                if 'subquery' not in alias:
                    sql = sql.replace(alias, 'subquery' + str(subquery[1]) + "_" + subquery[2])
            # Put the new on clause into the original query
            sql = sql.replace(old_on_clause, new_on_clause)

            subquery_index += 1
            aliases = []
            index = 0

        n1ql = self._gen_sql_to_nql(sql, ansi_joins=True)
        sql = self._convert_condition_template_to_value_datetime(sql, table_map, sql_type="sql")
        n1ql = self._convert_condition_template_to_value_datetime(n1ql, table_map, sql_type="n1ql")

        # Handle a special difference between sql and n1ql, is missing is not a concept in sql
        if "IS MISSING" in sql:
            sql = sql.replace("IS MISSING", "IS NULL")

        map = {"n1ql": n1ql,
               "sql": sql,
               "bucket": str(",".join(list(table_map.keys()))),
               "expected_result": None,
               "indexes": {}
               }

        table_name = random.choice(list(table_map.keys()))
        map["bucket"] = table_name

        return map

    ''' This method will go through my templates and extract the subqueries out of the template. It will replace those extracted
        subqueries with the keyword STUB_ followed by the number of subquery that it is. THis is so it can be re-inserted
        in the correct spot later on.'''
    def _extract_subquery(self, query_template):
        subquery_sep = ("SUBQUERY_TEMPLATE", "SUBQUERY_START", "SUBQUERY_END")

        clause_seperators = [subquery_sep]
        parsed_clauses = dict()
        parsed_clauses['RAW_QUERY_TEMPLATE'] = query_template
        parsed_clauses['STRIPPED_QUERY'] = ''
        index = 0
        for clause_seperator in clause_seperators:
            clause = clause_seperator[0]
            start_sep = clause_seperator[1]
            end_sep = clause_seperator[2]
            result = []
            tmp = query_template.split(start_sep)
            for substring in tmp:
                if end_sep in substring:
                    result.append(substring.split(end_sep)[0].strip())
                    parsed_clauses['STRIPPED_QUERY'] = parsed_clauses['STRIPPED_QUERY'] + "STUB_" + str(index) + " " + substring.split(end_sep)[1].strip()
                    index += 1
                else:
                    parsed_clauses['STRIPPED_QUERY'] = parsed_clauses['STRIPPED_QUERY'] + substring
            parsed_clauses[clause] = result

        return parsed_clauses

    def _convert_sql_template_for_skip_range_scan(self, n1ql_template, conversion_map):
        table_map = conversion_map.get("table_map", {})
        table_name = conversion_map.get("table_name", "simple_table")
        aggregate_pushdown = "secondary"
        sql, table_map = self._convert_sql_template_to_value(sql=n1ql_template, table_map=table_map, table_name=table_name, aggregate_pushdown=aggregate_pushdown, ansi_joins=False)
        n1ql = self._gen_sql_to_nql(sql, ansi_joins=False)
        sql = self._convert_condition_template_to_value_datetime(sql, table_map, sql_type="sql")
        n1ql = self._convert_condition_template_to_value_datetime(n1ql, table_map, sql_type="n1ql")
        sql_map = self._divide_sql(n1ql)

        if "IS MISSING" in sql:
            sql = sql.replace("IS MISSING", "IS NULL")

        map = {"n1ql": n1ql,
               "sql": sql,
               "bucket": str(",".join(list(table_map.keys()))),
               "expected_result": None,
               "indexes": {}
               }

        table_name = random.choice(list(table_map.keys()))
        map["bucket"] = table_name
        table_fields = list(table_map[table_name]["fields"].keys())

        aggregate_pushdown_index_name, create_aggregate_pushdown_index_statement = self._create_skip_range_key_scan_index(table_name, table_fields, sql_map)
        map = self.aggregate_special_convert(map)
        map["indexes"][aggregate_pushdown_index_name] = {"name": aggregate_pushdown_index_name,
                                                         "type": "GSI",
                                                         "definition": create_aggregate_pushdown_index_statement
                                                         }
        return map

    def _create_skip_range_key_scan_index(self, table_name, table_fields, sql_map):
        where_condition = sql_map["where_condition"]
        select_from = sql_map["select_from"]
        group_by = sql_map["group_by"]
        select_from_fields = []
        where_condition_fields = []
        groupby_fields = []
        aggregate_pushdown_fields_in_order = []
        skip_range_scan_index_fields_in_order = []

        for field in table_fields:
            if field.find('char') == 0:
                if select_from:
                    idx = self.find_char_field(select_from)
                    if idx > -1:
                        select_from_fields.append((idx, field))
                if where_condition:
                    idx = self.find_char_field(where_condition)
                    if idx > -1:
                        where_condition_fields.append((idx, field))
                if group_by:
                    idx = self.find_char_field(group_by)
                    if idx > -1:
                        groupby_fields.append((idx, field))
            else:
                if select_from:
                    idx = select_from.find(field)
                    if idx > -1:
                        select_from_fields.append((idx, field))
                if where_condition:
                    idx = where_condition.find(field)
                    if idx > -1:
                        where_condition_fields.append((idx, field))
                if group_by:
                    idx = group_by.find(field)
                    if idx > -1:
                        groupby_fields.append((idx, field))

        select_from_fields.sort(key=lambda tup: tup[0])
        where_condition_fields.sort(key=lambda tup: tup[0])
        groupby_fields.sort(key=lambda tup: tup[0])

        leading_key = random.choice(where_condition_fields)
        skip_range_scan_index_fields_in_order.append(leading_key[1])
        where_condition_fields.remove(leading_key)
        all_fields = select_from_fields + where_condition_fields + groupby_fields

        for i in range(0, len(all_fields)):
            num_random_fields = random.choice([1, 2, 3])
            for j in range(0, num_random_fields):
                random_field = ''.join(random.choice(string.ascii_uppercase) for _ in range(10))
                skip_range_scan_index_fields_in_order.append(random_field)
            next_field = random.choice(all_fields)
            all_fields.remove(next_field)
            skip_range_scan_index_fields_in_order.append(next_field[1])

        aggregate_pushdown_index_name = "{0}_aggregate_pushdown_index_{1}".format(table_name, self._random_int())

        create_aggregate_pushdown_index = \
                        "CREATE INDEX {0} ON {1}({2}) USING GSI".format(aggregate_pushdown_index_name, table_name, self._convert_list(skip_range_scan_index_fields_in_order, "numeric"))
        return aggregate_pushdown_index_name, create_aggregate_pushdown_index

    def _convert_sql_template_for_window_functions(self, query_template, conversion_map):
        table_map = conversion_map.get("table_map", {})
        sql, n1ql = self._sql_template_to_value_for_window_functions(query_template)

        if "IS MISSING" in sql:
            sql = sql.replace("IS MISSING", "IS NULL")

        map = {"n1ql": n1ql,
                "sql": sql,
                "bucket": str(",".join(list(table_map.keys()))),
                "expected_result": None,
                "indexes": {}
              }
        return map

    def _sql_template_to_value_for_window_functions(self, sql):
        sql_map = {}
        sql_map['window_function_name'] = ''
        sql_map['window_function_arguments'] = ''
        sql_map['nthval_from'] = ''
        sql_map['nulls_treatment'] = ''
        sql_map['window_partition'] = ''
        sql_map['window_order'] = ''
        sql_map['window_frame'] = ''
        sql_map['window_frame_exclusion'] = ''

        n1ql_map = {}
        n1ql_map['window_function_name'] = ''
        n1ql_map['window_function_arguments'] = ''
        n1ql_map['nthval_from'] = ''
        n1ql_map['nulls_treatment'] = ''
        n1ql_map['window_partition'] = ''
        n1ql_map['window_order'] = ''
        n1ql_map['window_frame'] = ''
        n1ql_map['window_frame_exclusion'] = ''

        if 'WINDOW_FUNCTION_NAME_START' in sql:
            sql_map['window_function_name'] = sql[sql.find('WINDOW_FUNCTION_NAME_START')+len('WINDOW_FUNCTION_NAME_START'):sql.find('WINDOW_FUNCTION_NAME_END')]
            n1ql_map['window_function_name'] = sql[sql.find('WINDOW_FUNCTION_NAME_START')+len('WINDOW_FUNCTION_NAME_START'):sql.find('WINDOW_FUNCTION_NAME_END')]


        if 'WINDOW_FUNCTION_ARGUMENTS_START' in sql:
            if sql_map['window_function_name'].strip() in ['PERCENT_RANK', 'RANK', 'DENSE_RANK', 'CUME_DIST', 'ROW_NUMBER']:
                sql_map['window_function_arguments'] = ''
            elif sql_map['window_function_name'].strip() in ['NTH_VALUE']:
                sql_map['window_function_arguments'] = ' decimal_field1, 2 '
            elif sql_map['window_function_name'].strip() in ['NTILE']:
                sql_map['window_function_arguments'] = ' 10 '
            else:
                sql_map['window_function_arguments'] = sql[sql.find('WINDOW_FUNCTION_ARGUMENTS_START')+len('WINDOW_FUNCTION_ARGUMENTS_START'):sql.find('WINDOW_FUNCTION_ARGUMENTS_END')]

            if n1ql_map['window_function_name'].strip() in ['PERCENT_RANK', 'RANK', 'DENSE_RANK', 'CUME_DIST', 'ROW_NUMBER']:
                n1ql_map['window_function_arguments'] = ''
            elif n1ql_map['window_function_name'].strip() in ['NTH_VALUE']:
                n1ql_map['window_function_arguments'] = ' decimal_field1, 2 '
            elif n1ql_map['window_function_name'].strip() in ['NTILE']:
                n1ql_map['window_function_arguments'] = ' 10 '
            else:
                n1ql_map['window_function_arguments'] = sql[sql.find('WINDOW_FUNCTION_ARGUMENTS_START') + len(
                    'WINDOW_FUNCTION_ARGUMENTS_START'):sql.find('WINDOW_FUNCTION_ARGUMENTS_END')]

        if 'NTHVAL_FROM_START' in sql:
            sql_map['nthval_from'] = sql[sql.find('NTHVAL_FROM_START')+len('NTHVAL_FROM_START'):sql.find('NTHVAL_FROM_END')]
            if sql_map['nthval_from'].strip() == 'FROM FIRST':
                sql_map['window_order'] = 'ORDER BY DECIMAL_FIELD1 ASC'
            if sql_map['nthval_from'].strip() == 'FROM LAST':
                sql_map['window_order'] = 'ORDER BY DECIMAL_FIELD1 DESC'
            sql_map['nthval_from'] = ''

            # works only for NTH_VALUE
            n1ql_map['nthval_from'] = sql[sql.find('NTHVAL_FROM_START')+len('NTHVAL_FROM_START'):sql.find('NTHVAL_FROM_END')]
            if n1ql_map['window_function_name'].strip() in ['LEAD', 'NTILE', 'LAG', 'LAST_VALUE', 'COUNTN', 'STDDEV_SAMP', 'CUME_DIST', 'ROW_NUMBER', 'COUNT',
                                                            'DENSE_RANK', 'FIRST_VALUE', 'MIN', 'VARIANCE', 'AVG', 'STDDEV', 'RANK', 'MAX', 'PERCENT_RANK',
                                                            'STDDEV_POP', 'SUM', 'ARRAY_AGG', 'VAR_SAMP', 'VAR_POP', 'MEAN']:
                n1ql_map['nthval_from'] = ''

        if 'NULLS_TREATMENT_START' in sql:
            sql_map['nulls_treatment'] = ''

            # works only for nth_value
            if n1ql_map['window_function_name'].strip() in ['PERCENT_RANK', 'LEAD', 'FIRST_VALUE', 'NTILE', 'ROW_NUMBER', 'LAG', 'LAST_VALUE', 'AVG',
                                                            'ARRAY_AGG', 'CUME_DIST', 'RANK', 'COUNT', 'STDDEV_SAMP', 'SUM', 'MIN', 'STDDEV', 'MAX', 'VAR_SAMP',
                                                            'COUNTN', 'VARIANCE', 'DENSE_RANK', 'STDDEV_POP', 'MEAN', 'VAR_POP']:
                n1ql_map['nulls_treatment'] = ''
            else:
                n1ql_map['nulls_treatment'] = sql[sql.find('NULLS_TREATMENT_START') + len('NULLS_TREATMENT_START'):sql.find(
                    'NULLS_TREATMENT_END')]

        if 'WINDOW_PARTITION_START' in sql:
            sql_map['window_partition'] = sql[sql.find('WINDOW_PARTITION_START')+len('WINDOW_PARTITION_START'):sql.find('WINDOW_PARTITION_END')]

            n1ql_map['window_partition'] = sql[sql.find('WINDOW_PARTITION_START') + len('WINDOW_PARTITION_START'):sql.find(
                'WINDOW_PARTITION_END')]

        if 'WINDOW_ORDER_START' in sql:
            sql_map['window_order'] = sql[sql.find('WINDOW_ORDER_START')+len('WINDOW_ORDER_START'):sql.find('WINDOW_ORDER_END')]
            if sql_map['window_function_name'].strip() in ['RANK', 'DENSE_RANK', 'PERCENT_RANK', 'CUME_DIST', 'LAG', 'NTILE', 'LEAD'] and sql_map['window_order'].strip() == '':
                sql_map['window_order'] = ' ORDER BY DECIMAL_FIELD1 '

            n1ql_map['window_order'] = sql[sql.find('WINDOW_ORDER_START') + len('WINDOW_ORDER_START'):sql.find(
                'WINDOW_ORDER_END')]
            if n1ql_map['window_function_name'].strip() in ['RANK', 'DENSE_RANK', 'PERCENT_RANK', 'CUME_DIST', 'LAG',
                                                           'NTILE', 'LEAD'] and n1ql_map['window_order'].strip() == '':
                n1ql_map['window_order'] = ' ORDER BY DECIMAL_FIELD1 '

        if 'WINDOW_FRAME_START' in sql:
            sql_map['window_frame'] = sql[sql.find('WINDOW_FRAME_START')+len('WINDOW_FRAME_START'):sql.find('WINDOW_FRAME_END')]
            if sql_map['window_function_name'].strip() in ['ROW_NUMBER', 'LEAD', 'CUME_DIST', 'LAG', 'PERCENT_RANK', 'DENSE_RANK', 'NTILE', 'RANK']:
                sql_map['window_frame'] = ''
            if sql_map['window_function_name'].strip() in ['FIRST_VALUE', 'LAST_VALUE', 'NTH_VALUE', 'ARRAY_AGG', 'VAR_POP', 'VAR_SAMP', 'AVG', 'STDDEV_POP', 'MIN',
                                                           'STDDEV_SAMP', 'COUNT', 'COUNTN', 'STDDEV', 'MAX', 'VARIANCE', 'MEAN', 'SUM'] and sql_map['window_order'].strip() == '':
                sql_map['window_order'] = ' ORDER BY DECIMAL_FIELD1 '

            n1ql_map['window_frame'] = sql[sql.find('WINDOW_FRAME_START')+len('WINDOW_FRAME_START'):sql.find('WINDOW_FRAME_END')]
            if n1ql_map['window_function_name'].strip() in ['ROW_NUMBER', 'LEAD', 'CUME_DIST', 'LAG', 'PERCENT_RANK', 'DENSE_RANK', 'NTILE', 'RANK']:
                n1ql_map['window_frame'] = ''
            if n1ql_map['window_function_name'].strip() in ['FIRST_VALUE', 'LAST_VALUE', 'NTH_VALUE', 'ARRAY_AGG', 'VAR_POP', 'VAR_SAMP', 'AVG', 'STDDEV_POP', 'MIN',
                                                            'STDDEV_SAMP', 'COUNT', 'COUNTN', 'STDDEV', 'MAX', 'VARIANCE', 'MEAN', 'SUM'] and n1ql_map['window_order'].strip() == '':
                n1ql_map['window_order'] = ' ORDER BY DECIMAL_FIELD1 '

        if 'WINDOW_FRAME_EXCLUSION_START' in sql:
            sql_map['window_frame_exclusion'] = sql[sql.find('WINDOW_FRAME_EXCLUSION_START')+len('WINDOW_FRAME_EXCLUSION_START'):sql.find('WINDOW_FRAME_EXCLUSION_END')]

            if sql_map['window_frame'].strip() == '':
                sql_map['window_frame_exclusion'] = ''

            n1ql_map['window_frame_exclusion'] = sql[sql.find('WINDOW_FRAME_EXCLUSION_START')+len('WINDOW_FRAME_EXCLUSION_START'):sql.find('WINDOW_FRAME_EXCLUSION_END')]
            if n1ql_map['window_frame'].strip() == '':
                n1ql_map['window_frame_exclusion'] = ''

            # PostgreSQL bug
            if sql_map['window_function_name'].strip() in ['FIRST_VALUE'] and sql_map['window_frame_exclusion'].strip() == 'EXCLUDE TIES':
                sql_map['window_frame_exclusion'] = ''
                n1ql_map['window_frame_exclusion'] = ''

        converted_sql = 'SELECT char_field1, decimal_field1, '+sql_map['window_function_name']+'('+sql_map['window_function_arguments']+')'+' '+sql_map['nthval_from']+' '+sql_map['nulls_treatment']+\
                        ' OVER('+sql_map['window_partition']+' '+sql_map['window_order']+' '+sql_map['window_frame']+' '+sql_map['window_frame_exclusion']+') as wf FROM simple_table'

        converted_n1ql = 'SELECT char_field1, decimal_field1, '+n1ql_map['window_function_name']+'('+n1ql_map['window_function_arguments']+')'+' '+n1ql_map['nthval_from']+' '+n1ql_map['nulls_treatment']+\
                        ' OVER('+n1ql_map['window_partition']+' '+n1ql_map['window_order']+' '+n1ql_map['window_frame']+' '+n1ql_map['window_frame_exclusion']+') as wf FROM simple_table'

        return converted_sql.lower(), converted_n1ql.lower()

    ''' Main function to convert templates into SQL and N1QL queries for GROUP BY clause field aliases '''
    def _convert_sql_template_for_group_by_aliases(self, query_template, conversion_map):
        table_map = conversion_map.get("table_map", {})
        table_name = conversion_map.get("table_name", "simple_table")
        n1ql_template_map = self._divide_sql(query_template)

        sql_template_map = copy.copy(n1ql_template_map)
        sql_template_map = self._add_field_aliases_to_sql_select_clause(sql_template_map)
        sql_template_map = self._cleanup_field_aliases_from_sql_clause(sql_template_map, 'where_condition')
        sql_template_map = self._cleanup_field_aliases_from_sql_clause(sql_template_map, 'order_by')
        sql_template_map = self._cleanup_field_aliases_from_sql_clause(sql_template_map, 'group_by')
        sql_template_map = self._cleanup_field_aliases_from_sql_clause(sql_template_map, 'having')

        sql_table, table_map = self._gen_select_tables_info(sql_template_map["from_fields"], table_map, False)
        converted_sql_map, converted_n1ql_map = self._apply_group_by_aliases(sql_template_map, n1ql_template_map, table_map)
        n1ql, sql, table_map = self._convert_sql_n1ql_templates_to_queries(converted_n1ql_map=converted_n1ql_map,
                                                                                         converted_sql_map=converted_sql_map,
                                                                                         table_map=table_map,
                                                                                         table_name=table_name)
        if "IS MISSING" in sql:
            sql = sql.replace("IS MISSING", "IS NULL")

        map = { "n1ql": n1ql,
                "sql": sql,
                "bucket": str(",".join(list(table_map.keys()))),
                "expected_result": None,
                "indexes": {} }
        return map

    ''' Function builds whole SQL and N1QL queries after applying field aliases to all available query clauses. '''
    def _convert_sql_n1ql_templates_to_queries(self, converted_n1ql_map={}, converted_sql_map={}, table_map={}, table_name="simple_table"):

        new_sql = "SELECT " + converted_sql_map['select_from'] + " FROM " + table_name
        if converted_sql_map['where_condition']:
            new_sql += " WHERE " + converted_sql_map['where_condition']
        if converted_sql_map['group_by']:
            new_sql += " GROUP BY " + converted_sql_map['group_by']
        if converted_sql_map['having']:
            new_sql += " HAVING " + converted_sql_map['having']
        if converted_sql_map['order_by']:
            new_sql += " ORDER BY " + converted_sql_map['order_by']

        new_n1ql = "SELECT " + converted_n1ql_map['select_from'] + " FROM " + table_name
        if converted_n1ql_map['where_condition']:
            new_n1ql += " WHERE " + converted_n1ql_map['where_condition']
        if converted_n1ql_map['group_by']:
            new_n1ql += " GROUP BY " + converted_n1ql_map['group_by']
        if converted_n1ql_map['having']:
            new_n1ql += " HAVING " + converted_n1ql_map['having']
        if converted_n1ql_map['order_by']:
            new_n1ql += " ORDER BY " + converted_n1ql_map['order_by']

        return new_n1ql, new_sql, table_map

    ''' Function constructs temp dictionaries for SQL and N1QL query clauses and passes them to 
        transform function. '''
    def _apply_group_by_aliases(self, sql_template_map={}, n1ql_template_map={}, table_map={}):

        string_field_names = self._search_fields_of_given_type(["varchar", "text", "tinytext", "char", "character"], table_map)
        numeric_field_names = self._search_fields_of_given_type(["int", "mediumint", "double", "float", "decimal", "numeric"], table_map)
        datetime_field_names = self._search_fields_of_given_type(["datetime", "timestamp without time zone"], table_map)
        bool_field_names = self._search_fields_of_given_type(["tinyint", "boolean"], table_map)

        converted_sql_map = copy.deepcopy(sql_template_map)
        converted_n1ql_map = copy.deepcopy(n1ql_template_map)

        if "BOOL_FIELD" in n1ql_template_map['group_by']:
            converted_n1ql_map, converted_sql_map = self.normalize_field_aliases(bool_field_names,
                                                                                    "BOOL_ALIAS",
                                                                                    "BOOL_FIELD",
                                                                                    n1ql_template_map,
                                                                                    converted_n1ql_map,
                                                                                    converted_sql_map)
        if "STRING_FIELD" in n1ql_template_map['group_by']:
            converted_n1ql_map, converted_sql_map = self.normalize_field_aliases(string_field_names,
                                                                                 "STRING_ALIAS",
                                                                                 "STRING_FIELD",
                                                                                 n1ql_template_map,
                                                                                 converted_n1ql_map,
                                                                                 converted_sql_map)
        if "NUMERIC_FIELD" in n1ql_template_map['group_by']:
            converted_n1ql_map, converted_sql_map = self.normalize_field_aliases(numeric_field_names,
                                                                                 "NUMERIC_ALIAS",
                                                                                 "NUMERIC_FIELD",
                                                                                 n1ql_template_map,
                                                                                 converted_n1ql_map,
                                                                                 converted_sql_map)
        if "DATETIME_FIELD" in n1ql_template_map['group_by']:
            converted_n1ql_map, converted_sql_map = self.normalize_field_aliases(datetime_field_names,
                                                                                 "DATETIME_ALIAS",
                                                                                 "DATETIME_FIELD",
                                                                                 n1ql_template_map,
                                                                                 converted_n1ql_map,
                                                                                 converted_sql_map)

        return converted_sql_map, converted_n1ql_map

    def _add_field_aliases_to_sql_select_clause(self, sql_map):
        select_from = sql_map['select_from']

        select_from = self._add_field_alias(select_from, "NUMERIC_FIELD", "NUMERIC_ALIAS")
        select_from = self._add_field_alias(select_from, "VARCHAR_FIELD", "VARCHAR_ALIAS")
        select_from = self._add_field_alias(select_from, "BOOL_FIELD", "BOOL_ALIAS")
        select_from = self._add_field_alias(select_from, "DATETIME_FIELD", "DATETIME_ALIAS")

        sql_map['select_from'] = select_from
        return sql_map

    def _add_field_alias(self, select_from, field_token, alias_token):
        if not (field_token + " " + alias_token) in select_from:
            if alias_token in select_from:
                select_from = select_from.replace(alias_token, field_token + " " + alias_token)
            else:
                select_from = select_from.replace(field_token, field_token + " " + alias_token)
        return select_from

    def _cleanup_field_aliases_from_sql_clause(self, sql_map={}, clause_name=''):
        if clause_name in list(sql_map.keys()):
            clause_str = sql_map[clause_name]

            clause_str = clause_str.replace("NUMERIC_FIELD NUMERIC_ALIAS", "NUMERIC_FIELD").\
                replace("BOOL_FIELD BOOL_ALIAS", "BOOL_FIELD").replace("STRING_FIELD STRING_ALIAS", "STRING_FIELD").\
                replace("DATETIME_FIELD DATETIME_ALIAS", "DATETIME_FIELD")

            clause_str = clause_str.replace("NUMERIC_ALIAS", "NUMERIC_FIELD").replace("BOOL_ALIAS", "BOOL_FIELD").\
                replace("STRING_ALIAS", "STRING_FIELD").replace("DATETIME_ALIAS", "DATETIME_FIELD")

            sql_map[clause_name] = clause_str

        return sql_map


    ''' Function substitutes XXX_FIELD and XXX_ALIAS placeholders with real field names and their aliases.
        Additional transformations to stay compatible with common SQL and N1QL syntax.
        Additional transformations to produce the same fields aliases usage for SELECT clause in SQL and N1QL queries.'''
    def normalize_field_aliases(self, field_names, alias_token, field_token, n1ql_map, converted_n1ql_map, converted_sql_map):
        field_name = random.choice(field_names)

        group_by_alias = alias_token in n1ql_map["group_by"]
        select_alias = (field_token+" "+alias_token) in n1ql_map["select_from"]

        if alias_token in n1ql_map["group_by"]:
            converted_n1ql_map['group_by'] = converted_n1ql_map['group_by'].replace(alias_token, field_name[:-1])

        if (field_token+" "+alias_token) in n1ql_map["select_from"]:
            converted_n1ql_map['select_from'] = converted_n1ql_map['select_from'].replace(field_token+" "+alias_token, (field_name + " " + field_name[:-1]))
            converted_sql_map['select_from'] = converted_sql_map['select_from'].replace(field_token+" "+alias_token, (field_name + " " + field_name[:-1]))
        elif field_token in n1ql_map["select_from"]:
            converted_n1ql_map['select_from'] = converted_n1ql_map['select_from'].replace(field_token, field_name)
            converted_sql_map['select_from'] = converted_sql_map['select_from'].replace(field_token+" "+alias_token, field_name)
        else: #BOOL_ALIAS
            if group_by_alias:
                converted_n1ql_map['select_from'] = converted_n1ql_map['select_from'].replace(alias_token, field_name[:-1])
                converted_sql_map['select_from'] = converted_sql_map['select_from'].replace(field_token+" "+alias_token, (field_name + " " + field_name[:-1]))
            else:
                converted_n1ql_map['select_from'] = converted_n1ql_map['select_from'].replace(alias_token, field_name)
                converted_sql_map['select_from'] = converted_sql_map['select_from'].replace(field_token+" "+alias_token, field_name)

        if alias_token in n1ql_map['order_by']:
            if group_by_alias or select_alias:
                converted_n1ql_map['order_by'] = converted_n1ql_map['order_by'].replace(alias_token, field_name[:-1])
            else:
                converted_n1ql_map['order_by'] = converted_n1ql_map['order_by'].replace(alias_token, field_name)

        if alias_token in n1ql_map['having']:
            if group_by_alias:
                converted_n1ql_map['having'] = converted_n1ql_map['having'].replace(alias_token, field_name[:-1])
            else:
                converted_n1ql_map['having'] = converted_n1ql_map['having'].replace(alias_token, field_name)

        converted_n1ql_map['group_by'] = converted_n1ql_map['group_by'].replace(field_token, field_name)
        converted_n1ql_map['select_from'] = converted_n1ql_map['select_from'].replace(field_token, field_name)
        converted_n1ql_map['where_condition'] = converted_n1ql_map['where_condition'].replace(field_token, field_name)
        converted_n1ql_map['order_by'] = converted_n1ql_map['order_by'].replace(field_token, field_name)
        converted_n1ql_map['having'] = converted_n1ql_map['having'].replace(field_token, field_name)

        converted_sql_map['group_by'] = converted_sql_map['group_by'].replace(field_token, field_name)
        converted_sql_map['select_from'] = converted_sql_map['select_from'].replace(field_token, field_name)
        converted_sql_map['where_condition'] = converted_sql_map['where_condition'].replace(field_token, field_name)
        converted_sql_map['order_by'] = converted_sql_map['order_by'].replace(field_token, field_name)
        converted_sql_map['having'] = converted_sql_map['having'].replace(field_token, field_name)

        converted_sql_map['select_from'] = converted_sql_map['select_from'].replace(alias_token, field_name[:-1])

        return converted_n1ql_map, converted_sql_map

