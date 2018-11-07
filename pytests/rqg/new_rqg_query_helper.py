import random
import copy
from base_query_helper import BaseRQGQueryHelper

class RQGQueryHelperNew(BaseRQGQueryHelper):

    ''' Dispatcher function. Uses test_name parameter to identify the way how templates
        will be transformed into SQL and N1QL queries.
        from let where group by letting haVING SELECT ORDER BY'''
    def _convert_sql_template_to_value(self, query_template="", table_map={}, table_name="simple_table",
                                       define_gsi_index=False, partitioned_indexes=False, test_name=""):

        if test_name == 'group_by_alias':
            return self._convert_sql_template_for_group_by_aliases(query_template, table_map, table_name)
        else:
            print("Unknown test name")
            exit(1)

    ''' Main function to convert templates into SQL and N1QL queries for GROUP BY clause field aliases '''
    def _convert_sql_template_for_group_by_aliases(self, query_template="", table_map={}, table_name="simple_table"):
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
                "bucket": str(",".join(table_map.keys())),
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

        string_field_names = self._search_fields_of_given_type(["varchar", "text", "tinytext", "char"], table_map)
        numeric_field_names = self._search_fields_of_given_type(["int", "mediumint", "double", "float", "decimal"], table_map)
        datetime_field_names = self._search_fields_of_given_type(["datetime"], table_map)
        bool_field_names = self._search_fields_of_given_type(["tinyint"], table_map)

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
        if clause_name in sql_map.keys():
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

