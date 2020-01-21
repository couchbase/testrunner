import random
import re
import string
import json
from random import randrange
from random import randint
from datetime import datetime
from functools import reduce

class BaseRQGQueryHelper(object):

    def _find_hints(self, n1ql_query):
        map = self._divide_sql(n1ql_query)
        select_from = map["select_from"]
        table_name = map["from_fields"].strip().split("USE INDEX")[0]
        if self._check_function(select_from):
            return "FUN"
        if select_from.strip() == "*":
            return [str(table_name)]
        if ".*" in select_from:
            return [select_from.split(".")[0]]
        hints = []
        for select_from in n1ql_query.split("SELECT"):
            data_point = select_from.split("FROM")[0]
            if ".*" in data_point:
                hints.append(data_point.split(".*").replace(" ", ""))
        return hints

    def _divide_sql(self, sql, ansi_joins=False):
        sql = sql.replace(";", "")
        sql = sql.replace("\n", "")
        group_by_text = None
        where_condition_text = None
        order_by_text = None
        having_text = None
        select_text = self._find_string_type(sql, ["SELECT", "Select", "select"])
        from_text = self._find_string_type(sql, ["FROM", "from", "From"])
        where_text = self._find_string_type(sql, ["WHERE", "where"])
        order_by = self._find_string_type(sql, ["ORDER BY", "order by"])
        group_by = self._find_string_type(sql, ["GROUP BY", "group by"])
        having = self._find_string_type(sql, ["HAVING", "having"])

        if where_text:
            from_field_text = sql.split(from_text)[1].split(where_text)[0]
        else:
            if "SUBTABLE" in sql:
                from_field_text = sql.split(from_text)[2]
            else:
                from_field_text = sql.split(from_text)[1]
        if "SUBTABLE" in sql:
            select_from_text = sql.split(' ')[1]+' ' + sql.split(' ')[2]
        else:
            select_from_text = sql.split(select_text)[1].split(from_text)[0].strip()
        if where_text:
            if "SUBTABLE" in sql:
                where_condition_text = sql.split(from_text)[1].split(where_text)[1].split(order_by)[0]
            else:
                where_condition_text = sql.split(where_text)[1]
        if group_by:
            group_by_text = sql.split(group_by)[1]
            where_condition_text = where_condition_text.split(group_by)[0]
            if having:
                having_text = group_by_text.split(having)[1]
                group_by_text = group_by_text.split(having)[0]
        if order_by:
            order_by_text = sql.split(order_by)[1]
            if group_by_text and not having:
                group_by_text = group_by_text.split(order_by)[0]
            if having:
                having_text = having_text.split(order_by)[0]
            where_condition_text = where_condition_text.split(order_by)[0]

        map = {
                "from_fields": from_field_text,
                "where_condition": where_condition_text,
                "select_from": select_from_text,
                "group_by": group_by_text,
                "order_by": order_by_text,
                "having": having_text,
                }

        return map

    def extract_select_clause(self, sql):
        select_text = self._find_string_type(sql, ["SELECT", "Select", "select"])
        from_text = self._find_string_type(sql, ["FROM", "from", "From"])
        if "SUBTABLE" in sql:
            select_from_text = sql.split(' ')[1]+' ' + sql.split(' ')[2]
        else:
            select_from_text = sql.split(select_text)[1].split(from_text)[0].strip()
        return select_from_text

    def _gen_query_with_subqueryenhancement(self, sql="", table_map={}, count1=0):
        outer_table_map = {}
        outer_table_maps = {}
        new_sql = ""
        new_n1ql = ""
        space = " "
        start_query = False
        not_seen_end = True
        sql_template = ""
        inner_subquery_in_field = ""
        inner_subquery_fields = ""
        end_map = {}
        start_count = 0
        end_count = 0
        outer_table_alias = "CRAP"
        for token in sql.split(" "):
            if not_seen_end and start_query and ("START" in token or "END" in token):
                start_query = False
                table_map_new = {}
                for key in list(set(table_map.keys()) - set(outer_table_maps.keys())):
                    table_map_new[key] = table_map[key]
                if len(table_map_new) == 0:
                    table_map_new = outer_table_map
                sql_template, new_table_map = self._convert_sql_template_to_value(sql_template, table_map_new)
                if "SUBTABLE" in sql_template or "OUTERBUCKET" in sql_template:
                    sql_template = sql_template[1:]
                    sql_template = sql_template.replace("SUBTABLE", "simple_table_2 t_1")

                print("sql_template after convert to value is {0} ".format(sql_template))

                if "SUBTABLE" in sql_template:
                    n1ql_template = self._gen_sqlsubquery_to_nqlsubquery(sql_template)
                else:
                    n1ql_template = self._gen_sql_to_nql(sql_template)
                print("n1ql template is {0}".format(n1ql_template))
                sql_template = sql_template.replace("SUBTABLE", "simple_table_2 t_1")
                print(sql_template)

                table_name = random.choice(list(new_table_map.keys()))
                inner_table_alias = new_table_map[table_name]["alias_name"]
                print("inner_table_alias is %s" % inner_table_alias)

                sql_template = sql_template.replace("OUTERBUCKET.primary_key_id", "t_5.primary_key_id")
                sql_template =  sql_template.replace("OUTERBUCKET.*", "t_5.*")
                sql_template = sql_template.replace("OUTERBUCKET", "simple_table_1 t_5")
                n1ql_template = n1ql_template.replace("OUTERBUCKET.primary_key_id", "t_5.primary_key_id")
                n1ql_template =  n1ql_template.replace("OUTERBUCKET.*", "t_5.*")
                n1ql_template = n1ql_template.replace("OUTERBUCKET", "simple_table_1 t_5")
                n1ql_template = n1ql_template.replace("simple_table_2 t_1", "t_5.simple_table_2 t_1")

                print("outer_table_alias is %s" % outer_table_alias)
                if "USE KEYS" in sql_template:
                    sql_template = sql_template.replace("USE KEYS", "")
                    if "OUTER_PRIMARY_KEY" in sql_template:
                        table_name = random.choice(list(outer_table_map.keys()))
                        outer_table_alias = alias_name
                        print("outer_table_alias 1 is {0}".format(outer_table_alias))
                        primary_key_field = outer_table_map[table_name]["primary_key_field"]
                        sql_template = sql_template.replace("OUTER_PRIMARY_KEY", "")
                        n1ql_template = n1ql_template.replace("OUTER_PRIMARY_KEY", alias_name+"."+primary_key_field)
                    elif "INNER_PRIMARY_KEYS" in sql_template:
                        primary_key_field = table_map[table_name]["primary_key_field"]
                        keys = table_map[table_name]["fields"][primary_key_field]["distinct_values"][0:10]
                        keys = self._convert_list(keys, "string")
                        sql_template = sql_template.replace("[INNER_PRIMARY_KEYS]", "")
                        n1ql_template = n1ql_template.replace("INNER_PRIMARY_KEYS", keys)
                    elif "OUTER_BUCKET_ALIAS" in sql_template:
                        table_name = random.choice(list(outer_table_map.keys()))
                        outer_table_alias = outer_table_map[table_name]["alias_name"]
                        sql_template = sql_template.replace("META(OUTER_BUCKET_ALIAS).id", "")
                        n1ql_template = n1ql_template.replace("OUTER_BUCKET_ALIAS", alias_name)
                outer_table_maps.update(new_table_map)
                if outer_table_map == {}:
                    outer_table_map.update(new_table_map)
                    table_name_1 = random.choice(list(outer_table_map.keys()))
                    alias_name = outer_table_map[table_name_1]["alias_name"]
                else:
                    table_name_1 = random.choice(list(outer_table_map.keys()))
                    outer_table_alias = outer_table_map[table_name_1]["alias_name"]
                    outer_table_alias = "t_5"
                    outer_table_map = {}
                    outer_table_map.update(new_table_map)
                if "AND_OUTER_INNER_TABLE_PRIMARY_KEY_COMPARISON" in sql_template:
                    value = "  {0}.primary_key_id = {1}.primary_key_id   AND  ".format(inner_table_alias,
                                                                                       outer_table_alias)
                    sql_template = sql_template.replace("AND_OUTER_INNER_TABLE_PRIMARY_KEY_COMPARISON", value)
                    if inner_table_alias != "t_2":
                        n1ql_template = n1ql_template.replace("WHERE  AND_OUTER_INNER_TABLE_PRIMARY_KEY_COMPARISON",
                                                              "USE KEYS [t_5.primary_key_id] WHERE")

                if "MYSQL_OPEN_PAR" in sql_template:
                    sql_template = sql_template.replace("MYSQL_OPEN_PAR", "(")
                    n1ql_template = n1ql_template.replace("MYSQL_OPEN_PAR", " ")
                if "MYSQL_CLOSED_PAR" in sql_template:
                    sql_template = sql_template.replace("MYSQL_CLOSED_PAR", ")")
                    n1ql_template = n1ql_template.replace("MYSQL_CLOSED_PAR", " ")
                if "OUTER_SUBQUERY_FIELDS" in sql_template:
                    sql_template = sql_template.replace("OUTER_SUBQUERY_FIELDS", inner_subquery_fields)
                    n1ql_template = n1ql_template.replace("OUTER_SUBQUERY_FIELDS", inner_subquery_fields)
                if "INNER_SUBQUERY_FIELDS" in sql_template:
                    inner_subquery_fields = self._search_fields_of_given_type(["int", "mediumint", "double", "float", "decimal"],
                                                                              new_table_map)
                    new_list = " "
                    inner_subquery_fields = self._convert_list(inner_subquery_fields, "numeric")
                    field_map = {}
                    for field in inner_subquery_fields.split(","):
                        field_map["\""+field.split(".")[1].strip()+"\""] = field.split(".")[1].strip()
                        new_list += " "+field.split(".")[1] + " ,"
                    sql_template = sql_template.replace("INNER_SUBQUERY_FIELDS", "( "+new_list[:len(new_list)-1]+" )")
                    n1ql_template = n1ql_template.replace("INNER_SUBQUERY_FIELDS",
                                                          "TO_ARRAY(" + str(field_map).replace("'", "")+")")
                    inner_subquery_fields = new_list[:len(new_list)-1]
                if "OUTER_SUBQUERY_IN_FIELD" in sql_template:
                    sql_template = sql_template.replace("OUTER_SUBQUERY_IN_FIELD", inner_subquery_in_field)
                    n1ql_template = n1ql_template.replace("OUTER_SUBQUERY_IN_FIELD", inner_subquery_in_field)
                if "INNER_SUBQUERY_IN_FIELD" in sql_template:
                    inner_subquery_in_field = self._search_fields_of_given_type(["int", "mediumint", "double", "float", "decimal"],
                                                                                new_table_map)[0]
                    inner_subquery_in_field = inner_subquery_in_field.split(".")[1]
                    sql_template = sql_template.replace("INNER_SUBQUERY_IN_FIELD", inner_subquery_in_field)
                    n1ql_template = n1ql_template.replace("INNER_SUBQUERY_IN_FIELD", inner_subquery_in_field)+" "
                if "OUTER_SUBQUERY_AGG_FIELD" in sql_template:
                    sql_template = sql_template.replace("OUTER_SUBQUERY_AGG_FIELD", inner_subquery_agg_field)
                    n1ql_template = n1ql_template.replace("OUTER_SUBQUERY_AGG_FIELD", inner_subquery_agg_field)
                if "INNER_SUBQUERY_AGG_FIELD" in sql_template:
                    inner_subquery_agg_field = self._search_fields_of_given_type(["int", "mediumint", "double", "float", "decimal"],
                                                                                 new_table_map)[0]
                    inner_subquery_agg_field = inner_subquery_agg_field.split(".")[1]
                    sql_template = sql_template.replace("INNER_SUBQUERY_AGG_FIELD", inner_subquery_agg_field)
                    n1ql_template = n1ql_template.replace("INNER_SUBQUERY_AGG_FIELD", inner_subquery_agg_field)
                if "END_" not in token:
                    new_sql += sql_template+space
                    new_n1ql += n1ql_template+space
                    start_count += 1
                else:
                    new_sql += sql_template+space
                    new_n1ql += n1ql_template+space
                if "END_" in token:
                    start_count -= 2
                    not_seen_end = False
                    if (start_count-end_count) in list(end_map.keys()):
                        new_n1ql += end_map[start_count-end_count]
                    end_count += 1
                else:
                    start_query = True
                    sql_template = ""
            elif not_seen_end and start_query:
                sql_template += token+space
            elif not_seen_end and "START" in token:
                start_count += 1
                start_query = True
                sql_template = ""
                new_sql += space
                new_n1ql += space
            else:
                if "END_" not in token:
                    new_sql += token+space
                    new_n1ql += token+space
                if "END_" in token:
                    if (start_count-end_count) in list(end_map.keys()):
                        new_n1ql = new_n1ql+end_map[start_count-end_count]
                    end_count += 1
        if "MYSQL_CLOSED_PAR" in new_sql:
            new_sql = new_sql.replace("MYSQL_CLOSED_PAR", ")")
            new_n1ql = new_n1ql.replace("MYSQL_CLOSED_PAR", " ")
        for x in range(0, randint(0, 5)):
            alias_name = "tb_"+self._random_char() + str(count1)
            print("alias name is {0}".format(alias_name))
            new_n1ql = "SELECT {0}.* FROM ({1}) {0}".format(alias_name, new_n1ql)
        new_sql = new_sql.replace("NOT_EQUALS", " NOT IN ")
        new_sql = new_sql.replace("EQUALS", " = ")
        new_n1ql = new_n1ql.replace("NOT_EQUALS", " NOT IN ")
        new_n1ql = new_n1ql.replace("EQUALS", " IN ")
        new_sql = new_sql.replace("RAW", "")
        new_n1ql = new_n1ql.replace("AND_OUTER_INNER_TABLE_PRIMARY_KEY_COMPARISON", "")
        print("new n1ql is %s" % (new_n1ql))
        print("new sql is %s" % (new_sql))
        return {"sql": new_sql, "n1ql": new_n1ql}, outer_table_map

    def _gen_query_with_subquery(self, sql="", table_map={}, count1=0):
        outer_table_map = {}
        outer_table_maps = {}
        new_sql = ""
        new_n1ql = ""
        space = " "
        start_query = False
        not_seen_end = True
        sql_template = ""
        inner_subquery_in_field = ""
        inner_subquery_fields = ""
        end_map = {}
        start_count = 0
        end_count = 0
        outer_table_alias = "CRAP"
        for token in sql.split(" "):
            if not_seen_end and start_query and ("START" in token or "END" in token):
                start_query = False
                table_map_new = {}
                for key in list(set(table_map.keys()) - set(outer_table_maps.keys())):
                    table_map_new[key] = table_map[key]
                if len(table_map_new) == 0:
                    table_map_new = outer_table_map
                sql_template, new_table_map = self._convert_sql_template_to_value(sql_template, table_map_new)
                n1ql_template = self._gen_sql_to_nql(sql_template)
                table_name = random.choice(list(new_table_map.keys()))
                inner_table_alias = new_table_map[table_name]["alias_name"]
                if "USE KEYS" in sql_template:
                    sql_template = sql_template.replace("USE KEYS", "")
                    if "OUTER_PRIMARY_KEY" in sql_template:
                        table_name = random.choice(list(outer_table_map.keys()))
                        outer_table_alias = alias_name
                        primary_key_field = outer_table_map[table_name]["primary_key_field"]
                        sql_template = sql_template.replace("OUTER_PRIMARY_KEY", "")
                        n1ql_template = n1ql_template.replace("OUTER_PRIMARY_KEY", alias_name+"."+primary_key_field)
                    elif "INNER_PRIMARY_KEYS" in sql_template:
                        primary_key_field = table_map[table_name]["primary_key_field"]
                        keys = table_map[table_name]["fields"][primary_key_field]["distinct_values"][0:10]
                        keys = self._convert_list(keys, "string")
                        sql_template = sql_template.replace("[INNER_PRIMARY_KEYS]", "")
                        n1ql_template = n1ql_template.replace("INNER_PRIMARY_KEYS", keys)
                    elif "OUTER_BUCKET_ALIAS" in sql_template:
                        table_name = random.choice(list(outer_table_map.keys()))
                        outer_table_alias = outer_table_map[table_name]["alias_name"]
                        sql_template = sql_template.replace("META(OUTER_BUCKET_ALIAS).id", "")
                        n1ql_template = n1ql_template.replace("OUTER_BUCKET_ALIAS", alias_name)
                outer_table_maps.update(new_table_map)
                if outer_table_map == {}:
                    outer_table_map.update(new_table_map)
                    table_name_1 = random.choice(list(outer_table_map.keys()))
                    alias_name = outer_table_map[table_name_1]["alias_name"]
                else:
                    table_name_1 = random.choice(list(outer_table_map.keys()))
                    outer_table_alias = outer_table_map[table_name_1]["alias_name"]
                    outer_table_map = {}
                    outer_table_map.update(new_table_map)
                if "AND_OUTER_INNER_TABLE_PRIMARY_KEY_COMPARISON" in sql_template :
                    value = "  {0}.primary_key_id = {1}.primary_key_id   AND  ".format(inner_table_alias, outer_table_alias)
                    sql_template = sql_template.replace("AND_OUTER_INNER_TABLE_PRIMARY_KEY_COMPARISON", value)
                if "MYSQL_OPEN_PAR" in sql_template:
                    sql_template = sql_template.replace("MYSQL_OPEN_PAR", "(")
                    n1ql_template = n1ql_template.replace("MYSQL_OPEN_PAR", " ")
                if "MYSQL_CLOSED_PAR" in sql_template:
                    sql_template = sql_template.replace("MYSQL_CLOSED_PAR", ")")
                    n1ql_template = n1ql_template.replace("MYSQL_CLOSED_PAR", " ")
                if "OUTER_SUBQUERY_FIELDS" in sql_template:
                    sql_template = sql_template.replace("OUTER_SUBQUERY_FIELDS", inner_subquery_fields)
                    n1ql_template = n1ql_template.replace("OUTER_SUBQUERY_FIELDS", inner_subquery_fields)
                if "INNER_SUBQUERY_FIELDS" in sql_template:
                    inner_subquery_fields = self._search_fields_of_given_type(["int", "mediumint", "double", "float", "decimal"], new_table_map)
                    new_list =" "
                    inner_subquery_fields = self._convert_list(inner_subquery_fields, "numeric")
                    field_map = {}
                    for field in inner_subquery_fields.split(","):
                        field_map["\""+field.split(".")[1].strip()+"\""] = field.split(".")[1].strip()
                        new_list += " "+field.split(".")[1] + " ,"
                    sql_template = sql_template.replace("INNER_SUBQUERY_FIELDS", "( "+new_list[:len(new_list)-1]+" )")
                    n1ql_template = n1ql_template.replace("INNER_SUBQUERY_FIELDS", "TO_ARRAY(" + str(field_map).replace("'", "")+")")
                    inner_subquery_fields = new_list[:len(new_list)-1]
                if "OUTER_SUBQUERY_IN_FIELD" in sql_template:
                    sql_template = sql_template.replace("OUTER_SUBQUERY_IN_FIELD", inner_subquery_in_field)
                    n1ql_template = n1ql_template.replace("OUTER_SUBQUERY_IN_FIELD", inner_subquery_in_field)
                if "INNER_SUBQUERY_IN_FIELD" in sql_template:
                    inner_subquery_in_field = self._search_fields_of_given_type(["int", "mediumint", "double", "float", "decimal"], new_table_map)[0]
                    inner_subquery_in_field = inner_subquery_in_field.split(".")[1]
                    sql_template = sql_template.replace("INNER_SUBQUERY_IN_FIELD", inner_subquery_in_field)
                    n1ql_template = n1ql_template.replace("INNER_SUBQUERY_IN_FIELD", inner_subquery_in_field)+" "
                if "OUTER_SUBQUERY_AGG_FIELD" in sql_template:
                    sql_template = sql_template.replace("OUTER_SUBQUERY_AGG_FIELD", inner_subquery_agg_field)
                    n1ql_template = n1ql_template.replace("OUTER_SUBQUERY_AGG_FIELD", inner_subquery_agg_field)
                if "INNER_SUBQUERY_AGG_FIELD" in sql_template:
                    inner_subquery_agg_field = self._search_fields_of_given_type(["int", "mediumint", "double", "float", "decimal"], new_table_map)[0]
                    inner_subquery_agg_field = inner_subquery_agg_field.split(".")[1]
                    sql_template = sql_template.replace("INNER_SUBQUERY_AGG_FIELD", inner_subquery_agg_field)
                    n1ql_template = n1ql_template.replace("INNER_SUBQUERY_AGG_FIELD", inner_subquery_agg_field)
                if "END_" not in token:
                    new_sql += sql_template+space
                    new_n1ql += n1ql_template+space
                    start_count += 1
                else:
                    new_sql += sql_template+space
                    new_n1ql += n1ql_template+space
                if "END_" in token:
                    start_count -= 2
                    not_seen_end = False
                    if (start_count-end_count) in list(end_map.keys()):
                        new_n1ql += end_map[start_count-end_count]
                    end_count += 1
                else:
                    start_query = True
                    sql_template = ""
            elif not_seen_end and start_query:
                sql_template += token+space
            elif not_seen_end and "START" in token:
                start_count += 1
                start_query = True
                sql_template = ""
                new_sql += space
                new_n1ql += space
            else:
                if "END_" not in token:
                    new_sql += token+space
                    new_n1ql += token+space
                if "END_" in token:
                    if (start_count-end_count) in list(end_map.keys()):
                        new_n1ql= new_n1ql+end_map[start_count-end_count]
                    end_count += 1
        if "MYSQL_CLOSED_PAR" in new_sql:
                new_sql = new_sql.replace("MYSQL_CLOSED_PAR", ")")
                new_n1ql = new_n1ql.replace("MYSQL_CLOSED_PAR", " ")
        for _ in range(0, randint(0, 5)):
            alias_name = "tb_"+self._random_char() + str(count1)
            new_n1ql = "SELECT {0}.* FROM ({1}) {0}".format(alias_name, new_n1ql)
        new_sql = new_sql.replace("NOT_EQUALS", " NOT IN ")
        new_sql = new_sql.replace("EQUALS", " = ")
        new_n1ql = new_n1ql.replace("NOT_EQUALS", " NOT IN ")
        new_n1ql = new_n1ql.replace("EQUALS", " IN ")
        new_sql = new_sql.replace("RAW", "")
        new_n1ql = new_n1ql.replace("AND_OUTER_INNER_TABLE_PRIMARY_KEY_COMPARISON", "")
        return {"sql": new_sql, "n1ql": new_n1ql}, outer_table_map

    def _gen_select_tables_info(self, sql="", table_map={}, ansi_joins=False):
        table_name_list = list(table_map.keys())
        # For ansi_joins there is an edge case where there is only 1 table name available to select and
        # this creates a join of a table against itself, very expensive and not useful at all
        if ansi_joins:
            persistent_table_name_list = ['simple_table_1', 'simple_table_2', 'simple_table_3', 'simple_table_4',
                                          'simple_table_10']
        prev_table_list = []
        standard_tokens = ["INNER JOIN", "LEFT JOIN"]
        new_sub_query = ""
        rewrite_table_name_alias = False
        sql_token_list = self._gen_select_after_analysis(sql, standard_tokens=standard_tokens)
        if len(sql_token_list) == 1:
            table_name = random.choice(table_name_list)
            table_name_alias = ""
            if "alias_name" in list(table_map[table_name].keys()):
                table_name_alias = table_map[table_name]["alias_name"]
            bucket_string = table_name
            if table_name_alias != "":
                table_name_alias = table_map[table_name]["alias_name"]
                bucket_string = table_name+"  "+table_name_alias
            return sql.replace("BUCKET_NAME", bucket_string), {table_name: table_map[table_name]}
        for token in sql_token_list:
            use_table_entry = False
            if token.strip() not in standard_tokens:
                choice_list = list(set(table_name_list) - set(prev_table_list))
                if len(choice_list) > 0:
                    table_name = random.choice(choice_list)
                    table_name_alias = table_map[table_name]["alias_name"]
                else:
                    table_name = table_name_list[0]
                    table_name_alias = table_map[table_name]["alias_name"]+self._random_alphabet_string()
                    # If there is only one table this will cause a join to use the same table on the left and right, we
                    # want to avoid this at all costs
                    if ansi_joins:
                        join_list = re.split('INNER JOIN|LEFT JOIN', new_sub_query)
                        join_bucket = join_list[0].strip().split(" ")[0]
                        if table_name == join_bucket:
                            new_table_name_list =list(set(persistent_table_name_list) - {table_name})
                            table_name = random.choice(new_table_name_list)
                            table_name_alias = table_map[table_name_list[0]]["alias_name"]+self._random_alphabet_string()
                            use_table_entry = True
                data = token
                data = data.replace("BUCKET_NAME", (table_name+" "+table_name_alias))
                if "ALIAS" in token:
                    data = data.replace("ALIAS", "query")
                    rewrite_table_name_alias = True
                if "PREVIOUS_TABLE" in token:
                    previous_table_name = random.choice(prev_table_list)
                    if use_table_entry:
                        previous_table_name_alias = table_map[table_name_list[0]]["alias_name"]
                    else:
                        previous_table_name_alias = table_map[previous_table_name]["alias_name"]
                    if "BOOL_FIELD" in token:
                        field_name, values = self._search_field(["tinyint"], table_map)
                        table_field = field_name.split(".")[1]
                        if rewrite_table_name_alias:
                            data = data.replace("PREVIOUS_TABLE.BOOL_FIELD",
                                                ("query" + "." + table_field)) + " "
                        data = data.replace("CURRENT_TABLE.BOOL_FIELD", (table_name_alias + "." + table_field)) + " "
                        data = data.replace("PREVIOUS_TABLE.BOOL_FIELD", (previous_table_name_alias+"."+table_field))
                    if "STRING_FIELD" in token:
                        if ansi_joins:
                            field_name, values = self._search_field(["text", "tinytext", "char"], table_map)
                        else:
                            field_name, values = self._search_field(["varchar", "text", "tinytext", "char"], table_map)

                        table_field = field_name.split(".")[1]
                        if rewrite_table_name_alias:
                            data = data.replace("PREVIOUS_TABLE.STRING_FIELD",
                                                ("query" + "." + table_field)) + " "
                        data = data.replace("CURRENT_TABLE.STRING_FIELD", (table_name_alias + "." + table_field)) + " "
                        data = data.replace("PREVIOUS_TABLE.STRING_FIELD", (previous_table_name_alias+"."+ table_field))
                    if "NUMERIC_FIELD" in token:
                        field_name, values = self._search_field(
                            ["int", "mediumint", "double", "float", "decimal"], table_map)
                        table_field = field_name.split(".")[1]
                        if rewrite_table_name_alias:
                            data = data.replace("PREVIOUS_TABLE.NUMERIC_FIELD",
                                                ("query" + "." + table_field)) + " "
                        data = data.replace("CURRENT_TABLE.NUMERIC_FIELD", (table_name_alias + "." + table_field)) + " "
                        data = data.replace("PREVIOUS_TABLE.NUMERIC_FIELD", (previous_table_name_alias+"." + table_field))
                    if use_table_entry:
                        data = data.replace("PREVIOUS_TABLE.FIELD", (
                                previous_table_name_alias + "." + "primary_key_id"))

                        data = data.replace("CURRENT_TABLE.FIELD",
                                            (table_name_alias + "." + table_map[table_name_list[0]]["primary_key_field"]))
                    else:
                        data = data.replace("PREVIOUS_TABLE.FIELD", (
                                previous_table_name_alias + "." + table_map[previous_table_name]["primary_key_field"]))
                        data = data.replace("CURRENT_TABLE.FIELD", (table_name_alias+"."+table_map[table_name]["primary_key_field"]))
                    rewrite_table_name_alias = False

                if "CURRENT_TABLE" in token:
                    if "BOOL_FIELD" in token:
                        field_name, values = self._search_field(["tinyint"], table_map)
                        table_field = field_name.split(".")[1]
                        data = data.replace("CURRENT_TABLE.BOOL_FIELD", (table_name_alias + "." + table_field)) + " "
                    if "STRING_FIELD" in token:
                        if ansi_joins:
                            field_name, values = self._search_field(["text", "tinytext", "char"], table_map)
                        else:
                            field_name, values = self._search_field(["varchar", "text", "tinytext", "char"], table_map)
                        table_field = field_name.split(".")[1]
                        data = data.replace("CURRENT_TABLE.STRING_FIELD", (table_name_alias + "." + table_field)) + " "
                    if "NUMERIC_FIELD" in token:
                        field_name, values = self._search_field(
                            ["int", "mediumint", "double", "float", "decimal"], table_map)
                        table_field = field_name.split(".")[1]
                        data = data.replace("CURRENT_TABLE.NUMERIC_FIELD", (table_name_alias + "." + table_field)) + " "
                new_sub_query += data + " "
                prev_table_list.append(table_name)
            else:
                new_sub_query += token+" "
        new_map = {}
        for key in list(table_map.keys()):
            if key in prev_table_list:
                new_map[key] = table_map[key]
        return new_sub_query, new_map

    def _check_deeper_query_condition(self, query):
        standard_tokens = ["UNION ALL", "INTERSECT ALL", "EXCEPT ALL", "UNION", "INTERSECT", "EXCEPT"]
        for token in standard_tokens:
            if token in query:
                return True
        return False

    def _gen_sql_with_deep_selects(self, sql ="", table_map = {}, table_name= "simple_table"):
        standard_tokens = ["UNION ALL", "INTERSECT ALL", "EXCEPT ALL", "UNION", "INTERSECT", "EXCEPT"]
        query_list = []
        new_sql = ""
        for token in standard_tokens:
            if token in sql:
                new_sql = " "
                sql_token_list = self._gen_select_after_analysis(sql, standard_tokens = standard_tokens)
                for sql_token in sql_token_list:
                    if sql_token in standard_tokens:
                        new_sql += sql_token +" "
                    else:
                        new_query, table_map = self._convert_sql_template_to_value(sql = sql_token, table_map = table_map, table_name=table_name)
                        query_list.append(new_query)
                        new_sql += new_query
                return new_sql, query_list, table_map
        new_sql, table_map = self._convert_sql_template_to_value(sql =sql, table_map = table_map, table_name=table_name)
        return new_sql, query_list, table_map

    def _gen_select_after_analysis(self, query, standard_tokens = None):
        sql_delimiter_list = [query]
        if standard_tokens is None:
            standard_tokens = ["UNION ALL", "INTERSECT ALL", "EXCEPT ALL", "UNION", "INTERSECT", "EXCEPT"]
        for token in standard_tokens:
            if token in query:
                sql_delimiter_list = self._gen_select_delimiter_list(sql_delimiter_list, token)
        return sql_delimiter_list

    def _gen_select_delimiter_list(self, query_token_list, delimit, standard_tokens = None):
        sql_delimiter_list = []
        if standard_tokens is None:
            standard_tokens = ["UNION ALL", "INTERSECT ALL", "EXCEPT ALL", "UNION", "INTERSECT", "EXCEPT"]
        for query in query_token_list:
            if query.strip() not in standard_tokens:
                tokens = query.split(delimit)
                count = 0
                while count < len(tokens):
                    sql_delimiter_list.append(tokens[count])
                    count += 1
                    if count < len(tokens):
                        sql_delimiter_list.append(delimit)
                        sql_delimiter_list.append(tokens[count])
                        count += 1
                        if count < len(tokens):
                            sql_delimiter_list.append(delimit)
            else:
                sql_delimiter_list.append(query)
        return sql_delimiter_list

    def _insert_statements_n1ql(self, bucket_name, map):
        list = []
        for key in list(map.keys()):
            list.append(self._insert_statement_n1ql(bucket_name, key, map[key]))

    def _upsert_statements_n1ql(self, bucket_name, map):
        list = []
        for key in list(map.keys()):
            list.append(self._upsert_statement_n1ql(bucket_name, key, map[key]))

    def _insert_statement_n1ql(self, bucket_name, key, value):
        sql_template = 'INSERT INTO {0} (KEY, VALUE) VALUES ({1},{2})'
        return sql_template.format(bucket_name, key, value)

    def _builk_insert_statement_n1ql(self, bucket_name, map):
        sql_template = 'INSERT INTO {0} (KEY, VALUE) VALUES {1}'
        temp = ""
        for key in list(map.keys()):
            temp += "({0},{1}),".format("\"" + key + "\"", json.dumps(map[key]))
        temp = temp[0:len(temp)-1]
        return sql_template.format(bucket_name, temp)

    def _builk_upsert_statement_n1ql(self, bucket_name, map):
        sql_template = 'UPSERT INTO {0} (KEY, VALUE) VALUES {1}'
        temp = ""
        for key in list(map.keys()):
            temp += "({0},{1}),".format("\""+key+"\"", json.dumps(map[key]))
        temp = temp[0:len(temp)-1]
        return sql_template.format(bucket_name, temp)

    def _upsert_statement_n1ql(self, bucket_name, key, value):
        sql_template = 'INSERT INTO {0} (KEY, VALUE) VALUES ({1},{2})'
        return sql_template.format(bucket_name, key, value)

    def _add_explain_with_hints(self, sql, index_hint):
        sql_map = self._divide_sql(sql)
        select_from = sql_map["select_from"]
        from_fields = sql_map["from_fields"]
        where_condition = sql_map["where_condition"]
        order_by = sql_map["order_by"]
        group_by = sql_map["group_by"]
        having = sql_map["having"]
        new_sql = "EXPLAIN SELECT "
        if select_from:
            new_sql += select_from + " FROM "
        if from_fields:
            new_sql += from_fields + " "
            new_sql += index_hint + " "
        if where_condition:
            new_sql += " WHERE " + where_condition + " "
        if group_by:
            new_sql += " GROUP BY " + group_by +" "
        if order_by:
            new_sql += " ORDER BY " + order_by +" "
        if having:
            new_sql += " HAVING " + having +" "
        return new_sql

    def check_groupby_orderby(self, sql, list_of_fields):
        sql_map = self._divide_sql(sql)
        order_by = sql_map["order_by"]
        group_by = sql_map["group_by"]
        having = sql_map["having"]
        where_condition = sql_map["where_condition"]
        returnparam = False
        if where_condition:
            if sorted(where_condition) == sorted(list_of_fields):
                returnparam = False
            else:
                returnparam = True
                return returnparam
        if group_by:
            if sorted(group_by) == sorted(list_of_fields):
                returnparam = False
            else:
                returnparam = True
                return returnparam
        if order_by:
            if sorted(order_by) == sorted(list_of_fields):
                returnparam = False
            else:
                returnparam = True
                return returnparam
        if having:
            if sorted(having) == sorted(list_of_fields):
                returnparam = False
            else:
                return returnparam
                returnparam = True
        return returnparam

    # sql or n1ql
    def _add_index_hints_to_query(self, sql, index_list=[]):
        sql_map = self._divide_sql(sql)
        select_from = sql_map["select_from"]
        from_fields = sql_map["from_fields"]
        where_condition = sql_map["where_condition"]
        order_by = sql_map["order_by"]
        group_by = sql_map["group_by"]
        having = sql_map["having"]
        new_sql = "SELECT "
        new_index_list = [index["name"]+" USING "+index["type"] for index in index_list]
        index_hint =" USE INDEX({0})".format(str(",".join(new_index_list)))
        if select_from:
            new_sql += select_from + " FROM "
        if from_fields:
            if " LET " in from_fields:
                actual_from_fields = from_fields.split(" LET ")[0]
                let_fields = " LET " + from_fields.split(" LET ")[1]
                new_sql += actual_from_fields + " "
                new_sql += index_hint + " "
                new_sql += let_fields + " "
            else:
                new_sql += from_fields + " "
                new_sql += index_hint + " "
        if where_condition:
            new_sql += " WHERE " + where_condition + " "
        if group_by:
            new_sql += " GROUP BY " + group_by + " "
        if order_by:
            new_sql += " ORDER BY " + order_by + " "
        if having:
            new_sql += " HAVING " + having + " "
        return new_sql

    def _add_limit_to_query(self, sql, limit):
        sql_map = self._divide_sql(sql)
        select_from = sql_map["select_from"]
        from_fields = sql_map["from_fields"]
        where_condition = sql_map["where_condition"]
        order_by = sql_map["order_by"]
        new_sql = "SELECT "
        if select_from:
            new_sql += select_from +" FROM "
        if from_fields:
            new_sql += from_fields + " "
        if where_condition:
            new_sql += " WHERE " + where_condition + " "
        if order_by:
            new_sql += " ORDER BY " + order_by + " "
        new_sql += " limit " + str(limit) + " "
        return new_sql

    def _check_function(self, sql):
        func_list = ["min", "max", "count", "sum", "avg", "stddev", "variance", "stddev_samp", "stddev_pop",
                     "variance_pop", "variance_samp", "mean", "var_pop", "var_samp"]
        for func in func_list:
            if func in sql.lower():
                return True
        return False

    def _find_string_type(self, n1ql_query, hints=[]):
        for hint in hints:
            if hint in n1ql_query:
                return hint

    def _gen_json_from_results_with_primary_key(self, columns, rows, primary_key=""):
        primary_key_index = 0
        count = 0
        dict = {}
        # Trace_index_of_primary_key
        for column in columns:
            if column == primary_key:
                primary_key_index = count
            count += 1
        # Convert to JSON and capture in a dictionary
        for row in rows:
            index = 0
            map = {}
            for column in columns:
                map[column] = row[index]
                index += 1
            dict[row[primary_key_index]] = map
        return dict

    def _gen_json_from_results(self, columns, rows):
        data = []
        # Convert to JSON and capture in a dictionary
        for row in rows:
            index = 0
            map = {}
            for column in columns:
                map[column] = row[index]
                index += 1
            data.append(map)
        return data

    def _search_field(self, types, map):
        list_types =[]
        table_name = random.choice(list(map.keys()))
        table_name_alias = None
        if "alias_name" in list(map[table_name].keys()):
            table_name_alias = map[table_name]["alias_name"]
        for key in list(map[table_name]["fields"].keys()):
            if self._search_presence_of_type(map[table_name]["fields"][key]["type"], types):
                key_name = key
                if table_name_alias:
                    key_name = table_name_alias+"."+key
                list_types.append(key_name)
        if len(list_types) > 0:
            key =random.choice(list_types)
        key_name = key
        if "." in key:
            key_name = key.split(".")[1]
        return key, map[table_name]["fields"][key_name]["distinct_values"]

    def _search_fields_of_given_type(self, types, map):
        list_types =[]
        table_name = random.choice(list(map.keys()))
        table_name_alias = None
        if "alias_name" in list(map[table_name].keys()):
            table_name_alias = map[table_name]["alias_name"]
        for key in list(map[table_name]["fields"].keys()):
            if self._search_presence_of_type(map[table_name]["fields"][key]["type"], types):
                key_name = key
                if table_name_alias:
                    key_name = table_name_alias+"."+key
                list_types.append(key_name)
        return list_types

    def _search_presence_of_type(self, type, list):
        for key in list:
            if key == type.split("(")[0]:
                return True
        return False

    def _generate_random_range(self, list):
        num_to_gen = randrange(1, len(list)+1)
        rand_sample = [list[i] for i in sorted(random.sample(range(len(list)), num_to_gen))]
        return rand_sample

    def _random_alphanumeric(self, limit=10):
        # ascii alphabet of all alphanumerals
        r = (list(range(48, 58)) + list(range(65, 91)) + list(range(97, 123)))
        random.shuffle(r)
        return reduce(lambda i, s: i + chr(s), r[:random.randint(0, len(r))], "")

    def _random_char(self):
        return random.choice(string.ascii_uppercase)

    def _random_tiny_int(self):
        return randint(0, 1)

    def _random_int(self):
        return randint(0, 100000000)

    def _random_float(self):
        return round(10000*random.random(), 0)

    def _random_double(self):
        return round(10000*random.random(), 0)

    def _random_datetime(self, start=1999, end=2015):
        year = random.choice(list(range(start, end)))
        month = random.choice(list(range(1, 13)))
        day = random.choice(list(range(1, 29)))
        return datetime(year, month, day)

    def _generate_insert_statement_from_data(self, table_name="TABLE_NAME", data_map={}):
        intial_statement = " INSERT INTO {0} ".format(table_name)
        column_names = "( "+",".join(list(data_map.keys()))+" ) "
        values_string = ""
        for value in list(data_map.values()):
            if str(value) == "True":
                value = 1
            if str(value) == "False":
                value = 0
            values_string += "\'"+str(value)+"\',"
        values = "( "+values_string[0:len(values_string)-1]+" ) "
        return intial_statement+column_names+" VALUES "+values

    def _generate_bulk_insert_statement_from_data(self, table_name="TABLE_NAME", data_map={}):
        intial_statement = " INSERT INTO {0} ".format(table_name)
        column_names = "( "+",".join(list(data_map.keys()))+" ) "
        values_string = ""
        values = ""
        for key in list(data_map.keys()):
            for value in list(data_map[key].values()):
                if str(value) == "True":
                    value = 1
                if str(value) == "False":
                    value = 0
                values_string += "\'"+str(value)+"\',"
            values += "( "+values_string[0:len(values_string)-1]+" ),"
        values = values[0:len(values)-1]
        return intial_statement+column_names+" VALUES "+values

    def _generate_insert_statement(self, table_name="TABLE_NAME", table_map={}, primary_key=""):
        intial_statement = ""
        intial_statement += " INSERT INTO {0} ".format(table_name)
        column_names = "( "+",".join(list(table_map.keys()))+" ) "
        values = ""
        for field_name in list(table_map.keys()):
            type = table_map[field_name]["type"]
            if "primary" in field_name:
                values += primary_key+","
            elif "tinyint" in type:
                values += str(self._random_tiny_int()) + ","
            elif "mediumint" in type:
                values += str(self._random_int() % 100)+","
            elif "int" in type:
                values += str(self._random_int()) + ","
            elif "decimal" in type:
                values += str(self._random_float()) + ","
            elif "float" in type:
                values += str(self._random_float())+","
            elif "double" in type:
                values += str(self._random_double())+","
            elif "varchar" in type:
                values += "\"" + self._random_alphabet_string() + "\","
            elif "char" in type:
                values += "\'" + self._random_char() + "\',"
            elif "tinytext" in type:
                values += "\'"+self._random_alphabet_string(limit=1)+"\',"
            elif "mediumtext" in type:
                values += "\'"+self._random_alphabet_string(limit=5)+"\',"
            elif "text" in type:
                values += "\'"+self._random_alphabet_string(limit=5)+"\',"
            elif "datetime" in type:
                values += "\'"+str(self._random_datetime())+"\',"
        return intial_statement+column_names+" VALUES ( "+values[0:len(values)-1]+" )"

    def _random_alphabet_string(self, limit=10):
        uppercase = sorted(string.ascii_uppercase)
        lowercase = sorted(string.ascii_lowercase)
        value = []
        for _ in range(0, limit//2):
            value.append(random.choice(uppercase))
            value.append(random.choice(lowercase))
        random.shuffle(value)
        return "".join(value)

    def _covert_field_template_for_update(self, sql="", table_map={}, alias=None):
        string_field_names = self._search_fields_of_given_type(["varchar", "text", "tinytext", "char"], table_map)
        numeric_field_names = self._search_fields_of_given_type(["int", "mediumint", "double", "float", "decimal"], table_map)
        datetime_field_names = self._search_fields_of_given_type(["datetime"], table_map)
        bool_field_names = self._search_fields_of_given_type(["tinyint"], table_map)
        primary_key_field = "primary_key"
        for field in string_field_names:
            if "primary" in field:
                primary_key_field = field
        string_field_names.remove(primary_key_field)
        new_sql = sql
        value = None
        field_name = "None"
        if "BOOL_FIELD" in sql:
            field_name = new_sql.replace("BOOL_FIELD", random.choice(bool_field_names))
            value = "True"
        if "STRING_FIELD" in sql:
            field_name = new_sql.replace("STRING_FIELD", random.choice(string_field_names))
            value = self._random_char()
        if "NUMERIC_FIELD" in sql:
            field_name = new_sql.replace("NUMERIC_FIELD", random.choice(numeric_field_names))
            value = str(self._random_int())
        if "DATETIME_FIELD" in sql:
            field_name = new_sql.replace("DATETIME_FIELD", random.choice(datetime_field_names))
            value = self._random_datetime()
        if alias is None:
            return "{0} = '{1}'".format(field_name, value)
        return "{0} = '{1}'".format(alias+"."+field_name, value)

    def _covert_fields_template_to_value(self, sql="", table_map={}, sql_map=None):
        string_field_names = self._search_fields_of_given_type(["varchar", "text", "tinytext", "char"], table_map)
        numeric_field_names = self._search_fields_of_given_type(["int", "mediumint", "double", "float", "decimal"], table_map)
        datetime_field_names = self._search_fields_of_given_type(["datetime"], table_map)
        bool_field_names = self._search_fields_of_given_type(["tinyint"], table_map)
        all_field_names = string_field_names + numeric_field_names + datetime_field_names + bool_field_names
        new_sql = sql
        if "PRIMARY_KEY_VAL" in sql:
            new_sql = new_sql.replace("PRIMARY_KEY_VAL", "primary_key_id")
        if "BOOL_FIELD_LIST" in sql:
            new_list = self._generate_random_range(bool_field_names)
            new_sql = new_sql.replace("BOOL_FIELD_LIST", self._convert_list(new_list, "numeric"))
        if "DATETIME_FIELD_LIST" in sql:
            new_list = self._generate_random_range(datetime_field_names)
            new_sql = new_sql.replace("DATETIME_FIELD_LIST", self._convert_list(new_list, "numeric"))
        if "STRING_FIELD_LIST" in sql:
            new_list = self._generate_random_range(string_field_names)
            new_sql = new_sql.replace("STRING_FIELD_LIST", self._convert_list(new_list, "numeric"))
        if "NUMERIC_FIELD_LIST" in sql:
            new_list = self._generate_random_range(numeric_field_names)
            new_sql = new_sql.replace("NUMERIC_FIELD_LIST", self._convert_list(new_list, "numeric"))
        if "BOOL_FIELD" in sql:
            new_sql = new_sql.replace("BOOL_FIELD", random.choice(bool_field_names))
        if "STRING_FIELD" in sql:
            new_sql = new_sql.replace("STRING_FIELD", random.choice(string_field_names))
        if "NUMERIC_FIELD" in sql:
            new_sql = new_sql.replace("NUMERIC_FIELD", random.choice(numeric_field_names))
        if "DATETIME_FIELD" in sql:
            new_sql = new_sql.replace("DATETIME_FIELD", random.choice(datetime_field_names))
        if "OUTER_BUCKET_NAME.*" in new_sql:
            projection = " "+table_map[list(table_map.keys())[0]]["alias_name"]+".* "
            new_sql = new_sql.replace("OUTER_BUCKET_NAME.*", projection)
        if "ORDER_BY_SEL_VAL" in sql:
            select_field_names_list = self.extract_field_names(sql_map['select_from'], all_field_names)
            new_sql = new_sql.replace("ORDER_BY_SEL_VAL", self._convert_list(select_field_names_list, "numeric"))

        return new_sql

    def _convert_sql_template_to_value_nested_subqueries(self, n1ql_template=""):
        table_alias_list = []
        space = " "
        new_sql = ""
        alias_count = 0
        for token in n1ql_template.split(" "):
            if "TABLE_ALIAS" in token:
                if "TABLE_ALIAS.*" in token:
                    alias_name = "sb_t_"+self._random_char()
                    table_alias_list.append(alias_name)
                    new_sql += token.replace("TABLE_ALIAS", alias_name)+space
                    alias_count += 1
                else:
                    new_sql += token.replace("TABLE_ALIAS", table_alias_list[alias_count-1])+space
                    alias_count -= 1
            else:
                new_sql += token+space
        return new_sql

    def _convert_sql_template_to_value_with_subqueries(self, n1ql_template="", table_map={}, table_name="simple_table", define_gsi_index=False):
        count = self._random_int()
        map, table_map = self._gen_query_with_subquery(sql=n1ql_template, table_map=table_map, count1=count+1)
        sql = map["sql"]
        n1ql = map["n1ql"]
        map = {
                "n1ql": n1ql,
                "sql": sql,
                "bucket": str(",".join(list(table_map.keys()))),
                "expected_result": None,
                "indexes": {}
                    }
        return map

    def _convert_sql_template_to_value_with_subqueryenhancements(self, n1ql_template="", table_map={}, table_name="simple_table", define_gsi_index=False):
        count = self._random_int()
        map, table_map = self._gen_query_with_subqueryenhancement(sql=n1ql_template, table_map=table_map, count1=count+1)
        sql = map["sql"]
        n1ql = map["n1ql"]
        map = {
                "n1ql": n1ql,
                "sql": sql,
                "bucket": str(",".join(list(table_map.keys()))),
                "expected_result": None,
                "indexes": {}
                    }
        return map

    def find_all(self, a_str, sub):
        start = 0
        while True:
            start = a_str.find(sub, start)
            if start == -1:
                return
            yield start
            start += len(sub)

    def find_char_field(self, a_str):
        char_occurences = list(self.find_all(a_str, "char"))
        rchar_occurences = list(self.find_all(a_str, "rchar"))
        for i in range(len(char_occurences)):
            if i + 1 > len(rchar_occurences) or char_occurences[i] != rchar_occurences[i] + 1:
                return char_occurences[i]
        return -1

    def remove_aggregate_func(self, field_string):
        for agg_func in ["MIN", "MAX", "AVG", "SUM", "COUNTN", "COUNT"]:
            field_string = field_string.replace(agg_func, "")

        field_string = str("".join(field_string.rsplit(")", 1)))
        field_string = str("".join(field_string.split("(", 1)))
        field_string = field_string.strip()
        return field_string

    def create_aggregate_pushdown_index(self, table_name, table_fields, sql_map, aggregate_pushdown, partitioned_indexes=False):
        where_condition = sql_map["where_condition"]
        select_from = sql_map["select_from"]
        #from_fields = sql_map["from_fields"]
        order_by = sql_map["order_by"]
        group_by = sql_map["group_by"]
        select_from_fields = []
        where_condition_fields = []
        groupby_fields = []
        aggregate_pushdown_fields_in_order = []

        if aggregate_pushdown == "secondary" or aggregate_pushdown == "partial":
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

            for item in where_condition_fields:
                if item[1] not in aggregate_pushdown_fields_in_order:
                    aggregate_pushdown_fields_in_order.append(item[1])
            for item in select_from_fields:
                if item[1] not in aggregate_pushdown_fields_in_order:
                    aggregate_pushdown_fields_in_order.append(item[1])
            for item in groupby_fields:
                if item[1] not in aggregate_pushdown_fields_in_order:
                    aggregate_pushdown_fields_in_order.append(item[1])

            if aggregate_pushdown_fields_in_order and aggregate_pushdown == "secondary":
                aggregate_pushdown_index_name = "{0}_aggregate_pushdown_index_{1}".format(table_name, self._random_int())

                if not partitioned_indexes :
                    create_aggregate_pushdown_index = \
                        "CREATE INDEX {0} ON {1}({2}) USING GSI".format(aggregate_pushdown_index_name, table_name, self._convert_list(aggregate_pushdown_fields_in_order, "numeric"))
                else:
                    create_aggregate_pushdown_index = \
                        "CREATE INDEX {0} ON {1}({2}) PARTITION BY HASH(meta().id) USING GSI".format(
                            aggregate_pushdown_index_name, table_name,
                            self._convert_list(
                                aggregate_pushdown_fields_in_order, "numeric"))
                return aggregate_pushdown_index_name, create_aggregate_pushdown_index
            if aggregate_pushdown_fields_in_order and aggregate_pushdown == "partial":
                aggregate_pushdown_index_name = "{0}_aggregate_pushdown_index_{1}".format(table_name, self._random_int())

                if not partitioned_indexes:
                    create_aggregate_pushdown_index = \
                        "CREATE INDEX {0} ON {1}({2}) WHERE {3} USING GSI".format(aggregate_pushdown_index_name,
                                                                              table_name,
                                                                              self._convert_list(aggregate_pushdown_fields_in_order, "numeric"),
                                                                              where_condition)
                else:
                    create_aggregate_pushdown_index = \
                        "CREATE INDEX {0} ON {1}({2}) PARTITION BY HASH(meta().id) WHERE {3} USING GSI".format(
                            aggregate_pushdown_index_name,
                            table_name,
                            self._convert_list(
                                aggregate_pushdown_fields_in_order, "numeric"),
                            where_condition)

                return aggregate_pushdown_index_name, create_aggregate_pushdown_index
        elif aggregate_pushdown == "functional":
            select_from_fields = [self.remove_aggregate_func(expr).replace("AS A", "").replace("AS B", "").replace("?", ",").replace("&", ",") for expr in select_from.split(",")]
            select_from_fields = [x for x in select_from_fields if x != "1"]
            for field in table_fields:
                if field.find('char') == 0:
                    if where_condition:
                        idx = self.find_char_field(where_condition)
                        if idx > -1:
                            where_condition_fields.append((idx, field))
                    if group_by:
                        idx = self.find_char_field(group_by)
                        if idx > -1:
                            groupby_fields.append((idx, field))
                else:
                    if where_condition:
                        idx = where_condition.find(field)
                        if idx > -1:
                            where_condition_fields.append((idx, field))
                    if group_by:
                        idx = group_by.find(field)
                        if idx > -1:
                            groupby_fields.append((idx, field))

            where_condition_fields.sort(key=lambda tup: tup[0])
            groupby_fields.sort(key=lambda tup: tup[0])

            for item in where_condition_fields:
                if item[1] not in aggregate_pushdown_fields_in_order:
                    aggregate_pushdown_fields_in_order.append(item[1])
            for item in select_from_fields:
                if item not in aggregate_pushdown_fields_in_order:
                    aggregate_pushdown_fields_in_order.append(item)
            for item in groupby_fields:
                if item[1] not in aggregate_pushdown_fields_in_order:
                    aggregate_pushdown_fields_in_order.append(item[1])

            if aggregate_pushdown_fields_in_order and aggregate_pushdown == "functional":
                aggregate_pushdown_index_name = "{0}_aggregate_pushdown_index_{1}".format(table_name, self._random_int())
                create_aggregate_pushdown_index = \
                    "CREATE INDEX {0} ON {1}({2}) USING GSI".format(aggregate_pushdown_index_name, table_name, self._convert_list(aggregate_pushdown_fields_in_order, "numeric"))
                return aggregate_pushdown_index_name, create_aggregate_pushdown_index
        else:
            return None, None

    def convert_sql_position_func(self, temp_sql):
        while "?" in temp_sql:
            sql_list = temp_sql.split("?", 1)
            left = sql_list[0]
            right = sql_list[1]
            head_of_query = left.rsplit("POSITION(", 1)[0]
            target_string = left.rsplit("POSITION(", 1)[1]
            sub_string = right.split(")", 1)[0]
            end_of_query = right.split(")", 1)[1]
            if "(" in sub_string:
                sub_string = sub_string + " )"
            else:
                end_of_query = " ) " + end_of_query
            temp_sql = str(head_of_query + "(POSITION( " + sub_string + " IN " + target_string + ") -1" + end_of_query)
        return temp_sql

    def convert_sql_datetime_func(self, temp_sql):
        while "&" in temp_sql:
            sql_list = temp_sql.split("&", 1)
            left = sql_list[0]
            right = sql_list[1]
            head_of_query = left.rsplit("DATE_PART_STR(", 1)[0]
            datetime_field = left.rsplit("DATE_PART_STR(", 1)[1]
            time_selector = right.split(")", 1)[0].replace('"', '')
            end_of_query = right.split(")", 1)[1].lstrip()
            temp_sql = str(head_of_query + time_selector + "( " + datetime_field + " ) " + end_of_query)
        return temp_sql

    def convert_sql_log_func(self, temp_sql):
        sql_list = temp_sql.split("LOG(")
        temp_sql = "LOG( 10, ".join(sql_list)
        return temp_sql

    def aggregate_special_convert(self, map):
        map["n1ql"] = map["n1ql"].replace("?", ",").replace("&", ",").replace(" COMMA ", " , ")
        map["n1ql"] = map["n1ql"].replace("SUBSTR", "SUBSTR1")
        map["sql"] = map["sql"].replace(" COMMA ", " , ")
        map["sql"] = self.convert_sql_position_func(str(map["sql"]))
        map["sql"] = self.convert_sql_datetime_func(str(map["sql"]))
        map["sql"] = self.convert_sql_log_func(str(map["sql"]))
        return map

    def _add_let_and_letting_statements(self, sql_map, table_map):
        select_from = sql_map['select_from']
        from_fields = sql_map["from_fields"]
        where_condition = sql_map['where_condition']
        group_by = sql_map['group_by']
        order_by = sql_map["order_by"]
        having = sql_map["having"]

        all_field_names, string_field_names, numeric_field_names, datetime_field_names, bool_field_names = self.get_all_field_names(table_map)

        query_fields_let = []
        valid_clauses = []
        select_from_fields = self.extract_field_names(select_from, all_field_names)
        query_fields_let += select_from_fields
        valid_clauses += [select_from]
        if where_condition:
            where_condition_fields = self.extract_field_names(where_condition, all_field_names)
            query_fields_let += where_condition_fields
            valid_clauses += [where_condition]
        if group_by:
            groupby_fields = self.extract_field_names(group_by, all_field_names)
            query_fields_let += groupby_fields
            valid_clauses += [group_by]

        query_fields_let = list(set(query_fields_let))

        let_map = self._create_let_map(query_fields_let)
        let_statement = self._create_let_statement(let_map)

        new_sql = "SELECT "
        if select_from:
            for field in list(let_map.keys()):
                let_var_name = let_map[field]
                select_from = select_from.replace(field, let_var_name)
            new_sql += select_from + " FROM "
        if from_fields:
            new_sql += from_fields + " "
        if let_statement:
            new_sql += " " + let_statement
        if where_condition:
            for field in list(let_map.keys()):
                let_var_name = let_map[field]
                where_condition = where_condition.replace(field, let_var_name)
            new_sql += " WHERE " + where_condition + " "
        if group_by:
            for field in list(let_map.keys()):
                let_var_name = let_map[field]
                group_by = group_by.replace(field, let_var_name)
            new_sql += " GROUP BY " + group_by +" "

            group_fields = group_by.split(",")
            group_fields = [field.strip() for field in group_fields]
            letting_map = self._create_letting_map(group_fields)
            letting_statement = self._create_letting_statement(letting_map)
            new_sql += " " + letting_statement

            if having or order_by:
                letting_map = self._update_letting_map_with_let_vars(letting_map, let_map)
                for field in list(letting_map.keys()):
                    letting_var_name = letting_map[field]
                    if having:
                        having = having.replace(field, letting_var_name)
                    if order_by:
                        order_by = order_by.replace(field, letting_var_name)

        if order_by:
            new_sql += " ORDER BY " + order_by +" "
        if having:
            new_sql += " HAVING " + having +" "
        return new_sql

    def _update_letting_map_with_let_vars(self, letting_map, let_map):
        for letting_field in list(letting_map.keys()):
            for let_field in list(let_map.keys()):
                if letting_field == let_map[let_field]:
                    letting_map[let_field] = letting_map.pop(letting_field)
        return letting_map

    def _add_alias_to_select_fields(self, sql):
        select_clause = self.extract_select_clause(sql)
        rest_of_sql = sql.split("FROM")[1]
        select_fields = [field.split("AS ")[0] for field in select_clause.split(",")]
        num_fields = len(select_fields)
        new_select_clause = "SELECT"
        alias = "z"
        for i in range(0, num_fields):
            new_select_clause += " " + select_fields[i] + " as " + alias + ","
            alias += "z"

        new_select_clause = new_select_clause[:-1] + " "
        return new_select_clause + "FROM " + rest_of_sql

    def _create_let_map(self, fields):
        let_map = dict()
        index = 1
        for field in fields:
            let_map[field] = "let_var_"+str(index)
            index += 1
        return let_map

    def _create_letting_map(self, fields):
        letting_map = dict()
        index = 1
        for field in fields:
            letting_map[field] = "letting_var_"+str(index)
            index += 1
        return letting_map

    def _create_let_statement(self, let_map):
        let_stm = "LET "
        for field in list(let_map.keys()):
            var_name = let_map[field]
            let_stm += var_name+"="+field+","
        let_stm = let_stm[:-1]
        let_stm += " "
        return let_stm

    def _create_letting_statement(self, letting_map):
        letting_stm = "LETTING "
        for field in list(letting_map.keys()):
            var_name = letting_map[field]
            letting_stm += var_name+"="+field+","
        letting_stm = letting_stm[:-1]
        letting_stm += " "
        return letting_stm

    def _convert_sql_template_to_value_for_secondary_indexes(self, n1ql_template="", table_map={}, table_name="simple_table", define_gsi_index=False, ansi_joins=False, aggregate_pushdown=False, partitioned_indexes=False, with_let=False):
        index_name_with_occur_fields_where = None
        index_name_with_expression = None
        index_name_fields_only = None
        aggregate_pushdown_index_name = None
        sql, table_map = self._convert_sql_template_to_value(sql=n1ql_template, table_map=table_map, table_name=table_name, aggregate_pushdown=aggregate_pushdown, ansi_joins=ansi_joins)
        n1ql = self._gen_sql_to_nql(sql, ansi_joins)
        sql = self._convert_condition_template_to_value_datetime(sql, table_map, sql_type="sql")
        n1ql = self._convert_condition_template_to_value_datetime(n1ql, table_map, sql_type="n1ql")
        sql_map = self._divide_sql(n1ql)

        if "IS MISSING" in sql:
            sql = sql.replace("IS MISSING", "IS NULL")

        if with_let:
            n1ql_map = self._divide_sql(n1ql)
            n1ql = self._add_let_and_letting_statements(n1ql_map, table_map)
            n1ql = self._add_alias_to_select_fields(n1ql)
            sql = self._add_alias_to_select_fields(sql)

        map = { "n1ql": n1ql,
                "sql": sql,
                "bucket": str(",".join(list(table_map.keys()))),
                "expected_result": None,
                "indexes": {} }

        if not define_gsi_index:
            if aggregate_pushdown == "primary":
                map["n1ql"] = map["n1ql"].replace("primary_key_id", "meta().id ")
                map = self.aggregate_special_convert(map)
            return map

        where_condition = sql_map["where_condition"]
        select_from = sql_map["select_from"]
        from_fields = sql_map["from_fields"]
        table_name = random.choice(list(table_map.keys()))
        map["bucket"] = table_name
        table_fields = list(table_map[table_name]["fields"].keys())

        field_that_occur = []

        if aggregate_pushdown:
            aggregate_pushdown_index_name, create_aggregate_pushdown_index_statement = self.create_aggregate_pushdown_index(table_name, table_fields, sql_map, aggregate_pushdown, partitioned_indexes)
            map = self.aggregate_special_convert(map)
        else:
            if where_condition and ("OR" not in where_condition):
                for field in table_fields:
                    if (field in where_condition) or (field in from_fields):
                        field_that_occur.append(field)

                index_name_with_occur_fields_where = "{0}_where_based_fields_occur_{1}".format(table_name, self._random_int())
                index_name_fields_only = "{0}_index_name_fields_only_{1}_{2}".format(table_name, "_".join(field_that_occur), self._random_int())
                # need to modify index on expression. index should be created on fields and functions on fields in where
                # clause and not on whole where clause
                # index_name_with_expression = "{0}_expression_based_{1}".format(table_name, self._random_int())
                if not partitioned_indexes:
                    create_index_fields_occur_with_where = \
                    "CREATE INDEX {0} ON {1}({2}) WHERE {3} USING GSI".format(index_name_with_occur_fields_where,
                                                                              table_name,
                                                                              self._convert_list(field_that_occur, "numeric"),
                                                                              where_condition)
                else:
                    create_index_fields_occur_with_where = \
                        "CREATE INDEX {0} ON {1}({2}) PARTITION BY HASH(meta().id) WHERE {3} USING GSI".format(
                            index_name_with_occur_fields_where,
                            table_name,
                            self._convert_list(field_that_occur, "numeric"),
                            where_condition)

                if not partitioned_indexes:
                    create_index_name_fields_only = \
                        "CREATE INDEX {0} ON {1}({2}) USING GSI".format(index_name_fields_only,
                                                                        table_name,
                                                                        self._convert_list(field_that_occur, "numeric"))
                else:
                    create_index_name_fields_only = \
                        "CREATE INDEX {0} ON {1}({2}) PARTITION BY HASH(meta().id) USING GSI".format(
                            index_name_fields_only,
                            table_name,
                            self._convert_list(field_that_occur, "numeric"))

                if not partitioned_indexes:
                    create_index_name_with_expression = "CREATE INDEX {0} ON {1}({2}) USING GSI".format(index_name_with_expression,
                                                                                                        table_name,
                                                                                                        where_condition)
                else:
                    create_index_name_with_expression = "CREATE INDEX {0} ON {1}({2}) PARTITION BY HASH(meta().id) USING GSI".format(
                        index_name_with_expression,
                        table_name,
                        where_condition)

        if aggregate_pushdown_index_name:
            map["indexes"][aggregate_pushdown_index_name] = \
                {
                    "name": aggregate_pushdown_index_name,
                    "type": "GSI",
                    "definition": create_aggregate_pushdown_index_statement
                }

        if index_name_with_occur_fields_where:
            map["indexes"][index_name_with_occur_fields_where] = \
                            {
                                "name": index_name_with_occur_fields_where,
                                "type": "GSI",
                                "definition": create_index_fields_occur_with_where
                            }

        if index_name_with_expression:
            map["indexes"][index_name_with_expression] = \
                            {
                                "name": index_name_with_expression,
                                "type": "GSI",
                                "definition": create_index_name_with_expression
                            }

        if index_name_fields_only:
            map["indexes"][index_name_fields_only] = \
                            {
                                "name": index_name_fields_only,
                                "type": "GSI",
                                "definition": create_index_name_fields_only
                            }
        return map

    def _convert_sql_template_to_value_for_secondary_indexes_sub_queries(self, n1ql_template="", table_map={}, table_name="simple_table", define_gsi_index=True, partitioned_indexes=False):
        sql, query_list, table_map = self._gen_sql_with_deep_selects(sql=n1ql_template, table_map=table_map, table_name=table_name)
        n1ql = self._gen_sql_to_nql(sql)
        sql = self._convert_condition_template_to_value_datetime(sql, table_map, sql_type="sql")
        n1ql = self._convert_condition_template_to_value_datetime(n1ql, table_map, sql_type="n1ql")
        table_name = list(table_map.keys())[0]
        map = {
                "n1ql": n1ql,
                "sql": sql,
                "expected_result": None,
                "bucket": table_name,
                "indexes": {}
             }
        if not define_gsi_index:
            return map
        for n1ql in query_list:
            sql_map = self._divide_sql(n1ql)
            where_condition = sql_map["where_condition"]
            fields = list(table_map[table_name]["fields"].keys())
            field_that_occur = []
            if where_condition and ("OR" not in where_condition):
                for field in fields:
                    if field in where_condition:
                        field_that_occur.append(field)
            if where_condition and ("OR" not in where_condition):
                index_name_fields_only = "{0}_index_name_fields_only_{1}".format(table_name, self._random_alphanumeric(4))

                if not partitioned_indexes:
                    create_index_name_fields_only = \
                        "CREATE INDEX {0} ON {1}({2}) USING GSI".format(index_name_fields_only, table_name,
                                                                        self._convert_list(field_that_occur, "numeric"))
                else:
                    create_index_name_fields_only = \
                        "CREATE INDEX {0} ON {1}({2}) PARTITION BY HASH(meta().id) USING GSI".format(
                            index_name_fields_only, table_name,
                            self._convert_list(field_that_occur, "numeric"))
                map["indexes"][index_name_fields_only] = \
                    {
                        "name":index_name_fields_only,
                        "type":"GSI",
                        "definition":create_index_name_fields_only
                    }
        return map

    def _convert_delete_sql_template_to_values(self, sql="", table_map={}):
        tokens = sql.split("WHERE")
        new_sql = "DELETE FROM {0} WHERE ".format(list(table_map.keys())[0])
        new_sql += self._convert_condition_template_to_value(tokens[1], table_map)
        return new_sql

    def _delete_sql_template_to_values_with_merge(self, source_table="", target_table="", sql="", table_map={}):
        new_map ={}
        new_map[source_table] = table_map[target_table]
        tokens = sql.split("WHERE")
        new_map[source_table]["alias_name"] = target_table
        where_condition = self._convert_condition_template_to_value(tokens[1], new_map)
        merge_sql = "MERGE INTO {0} USING {1} ON KEY copy_simple_table.primary_key_id".format(target_table,  source_table)
        merge_sql +=" WHEN MATCHED THEN"
        new_sql = " DELETE FROM {0} ".format(target_table)
        new_sql += " WHERE "+where_condition
        merge_sql += " DELETE WHERE "+self._gen_sql_to_nql(where_condition)
        return {"sql_query": new_sql, "n1ql_query": merge_sql.replace(";", "")}

    def _update_sql_template_to_values_with_merge(self, source_table="", target_table="", sql="", table_map={}):
        new_map ={}
        new_map[source_table] = table_map[target_table]
        new_map[source_table]["alias_name"] = target_table
        tokens = sql.split("WHERE")
        where_condition = self._convert_condition_template_to_value(tokens[1], new_map)
        tokens = sql.split("WHERE")
        merge_sql = "MERGE INTO {0} USING {1} ON KEY copy_simple_table.primary_key_id".format(target_table,  source_table)
        merge_sql +=" WHEN MATCHED THEN"
        new_sql = " UPDATE {0} ".format(target_table)
        list = []
        for field in tokens[0].split("SET")[1].split(","):
            list.append(self._covert_field_template_for_update(field, new_map))
        new_sql += " SET " + ",".join(list).replace("target_table.", "")
        new_sql += " WHERE "+where_condition
        merge_sql += " UPDATE SET "+",".join(list)+" WHERE "+self._gen_sql_to_nql(where_condition)
        return {"sql_query": new_sql, "n1ql_query": merge_sql.replace(";", "")}

    def _insert_sql_template_to_values_with_merge(self, source_table="", target_table="", sql="", table_map={}):
        table_map[source_table]["alias_name"] = "source_table"
        table_map[target_table]["alias_name"] = "target_table"
        where_condition = self._convert_condition_template_to_value(tokens[1], table_map)
        tokens = sql.split("WHERE")
        #  merge_sql = "MERGE {0} target_table USING {1} source_table ON KEY source_table.primary_key_id".format(source_table, target_table)
        merge_sql += " WHEN NOT MATCHED THEN"
        new_sql = " INSERT INTO {0} ".format(target_table)
        field_list = []
        value_list = []
        for field in target_table[target_table]["fields"]:
            field_list.append(field)
            value_list.append(source_table+"."+field)
        merge_sql += " (KEY, VALUE) VALUES ({0}, {1})".format("source_table.primary_key_id", )
        new_sql +=  "({0}) SELECT {0} FROM {1}".format(",".join(field_list), source_table)
        return {"sql_query": new_sql, "n1ql_query": self._gen_sql_to_nql(merge_sql)}

    def _update_sql_template_to_values(self, target_table="simple_table", source_table="copy_simple_table", sql="", table_map={}):
        tokens = sql.split("WHERE")
        new_sql = " UPDATE {0} ".format(list(table_map.keys())[0])
        list = []
        for field in tokens[0].split("SET")[1].split(","):
            list.append(self._covert_field_template_for_update(field, table_map))
        new_sql += " SET " + ",".join(list)
        new_sql += " WHERE "+self._convert_condition_template_to_value(tokens[1], table_map)
        return {"sql_query": new_sql, "n1ql_query": self._gen_sql_to_nql(new_sql)}

    def _delete_sql_template_to_values(self, sql="", table_map={}):
        tokens = sql.split("WHERE")
        new_sql = " DELETE FROM {0} ".format(list(table_map.keys())[0])
        new_sql += " WHERE "+self._convert_condition_template_to_value(tokens[1], table_map)
        return {"sql_query": new_sql, "n1ql_query": self._gen_sql_to_nql(new_sql)}

    def _convert_sql_template_to_value(self, sql="", table_map={}, table_name="simple_table", aggregate_pushdown=False, ansi_joins=False):
        aggregate_function_list = []
        sql_map = self._divide_sql(sql, ansi_joins)
        select_from = sql_map["select_from"]
        from_fields = sql_map["from_fields"]
        where_condition = sql_map["where_condition"]
        order_by = sql_map["order_by"]
        group_by = sql_map["group_by"]
        having = sql_map["having"]
        converted = dict()

        if isinstance(select_from, (list)):
            i = 0
            for fields in select_from:
                select_from[i] = self._covert_fields_template_to_value(fields, table_map)
                i += 1
            i = 0
            for fields in from_fields:
                from_fields[i], table_map = self._gen_select_tables_info(fields, table_map, ansi_joins)
                i += 1
            i = 0
            for fields in where_condition:
                where_condition[i] = self._convert_condition_template_to_value(fields, table_map)
                i += 1

            # Make sure that the inner select uses the table that the rest of the subquery uses (the conversion above does not enforce the correct alias name)
            correct_alias = from_fields[1].split(" ")[3]
            outer_select_alias = select_from[1].split(".")[0].strip()

            if correct_alias not in select_from[2]:
                select_clause = select_from[2].split(",")
                old_alias = select_clause[0].split(".")[0].strip()
                select_from[2] = select_from[2].replace(old_alias, correct_alias)

            # Make sure that outer select uses a table that is contained inside the query
            if outer_select_alias not in from_fields[0]:
                select_from[1] = select_from[1].replace(outer_select_alias, table_map[list(table_map.keys())[0]]['alias_name'])

            new_sql = "SELECT " + select_from[1] + "FROM (SELECT" + select_from[2] + "FROM " + from_fields[1] + "WHERE " \
                      + where_condition[0] + from_fields[0] + "WHERE " + where_condition[1]
        else:
            from_fields, table_map = self._gen_select_tables_info(from_fields, table_map, ansi_joins)
            aggregate_groupby_orderby_fields = None
            new_sql = "SELECT "
            if "(SELECT" in sql or "( SELECT" in sql:
                new_sql = "(SELECT "
            if select_from:
                if group_by and having:
                    groupby_fields = self._covert_fields_template_to_value(group_by, table_map).split(",")
                    if "AGGREGATE_FIELD" not in select_from:
                        new_sql += ",".join(groupby_fields) + " FROM "
                    else:
                        select_sql, aggregate_function_list = self._gen_aggregate_method_subsitution(select_from, groupby_fields)
                        new_sql += select_sql + " FROM "
                else:
                    if aggregate_pushdown:
                        if "SAME_FIELD" in select_from:
                            sql_map["where_condition"] = self._convert_condition_template_to_value(where_condition, table_map)
                            where_condition = sql_map["where_condition"]
                        if group_by:
                            sql_map["group_by"] = self._covert_fields_template_to_random_value("group_by", sql_map, table_map)
                            group_by = sql_map["group_by"]
                        new_sql += self._covert_fields_template_to_random_value("select_from", sql_map, table_map) + " FROM "
                    else:
                        converted['select_from'] = self._covert_fields_template_to_value(select_from, table_map)
                        new_sql += converted['select_from'] + " FROM "
            if from_fields:
                new_sql += from_fields + " "
            if where_condition:
                new_sql += " WHERE "+self._convert_condition_template_to_value(where_condition, table_map)+ " "
            if group_by:
                if group_by and having and not aggregate_pushdown:
                    new_sql += " GROUP BY "+(",".join(groupby_fields))+" "
                elif group_by and order_by and aggregate_pushdown:
                    aggregate_groupby_orderby_fields = self._covert_fields_template_to_value(group_by, table_map)
                    new_sql += " GROUP BY "+aggregate_groupby_orderby_fields+" "
                else:
                    new_sql += " GROUP BY "+self._covert_fields_template_to_value(group_by, table_map)+" "
            if having:
                groupby_table_map = self._filter_table_map_based_on_fields(groupby_fields, table_map)
                if "AGGREGATE_FIELD" not in sql:
                    new_sql += " HAVING "+self._convert_condition_template_to_value(having, groupby_table_map)+" "
                else:
                    new_sql += " HAVING "+self._convert_condition_template_to_value_with_aggregate_method(having, groupby_table_map, aggregate_function_list)
            if order_by:
                if aggregate_pushdown:
                    new_sql += " ORDER BY "+aggregate_groupby_orderby_fields+" "
                else:
                    new_sql += " ORDER BY "+self._covert_fields_template_to_value(order_by, table_map, converted)+" "
        return new_sql, table_map

    def random_field_choice(self, field_names):
        return random.choice(field_names)

    def random_field_sample(self, field_names):
        new_list = self._generate_random_range(field_names)
        return self._convert_list(new_list, "numeric")

    def extract_groupby_field_names(self, sql_string):
        fields = sql_string.split(",")
        return fields

    def extract_field_names(self, sql_string, field_list):
        present_fields = []
        for field in field_list:
            if field.find('char') == 0:
                idx = self.find_char_field(sql_string)
                if idx > -1:
                    present_fields.append(field)
            else:
                idx = sql_string.find(field)
                if idx > -1:
                    present_fields.append(field)
        return list(set(present_fields))

    def get_all_field_names(self, table_map):
        string_field_names = self._search_fields_of_given_type(["varchar", "text", "tinytext", "char"], table_map)
        numeric_field_names = self._search_fields_of_given_type(["int", "mediumint", "double", "float", "decimal"], table_map)
        datetime_field_names = self._search_fields_of_given_type(["datetime"], table_map)
        bool_field_names = self._search_fields_of_given_type(["tinyint"], table_map)
        all_field_names = string_field_names + numeric_field_names + datetime_field_names + bool_field_names
        return all_field_names, string_field_names, numeric_field_names, datetime_field_names, bool_field_names

    def _covert_fields_template_to_random_value(self, field_key, sql_map, table_map={}):
        sql = sql_map[field_key]
        all_field_names, string_field_names, numeric_field_names, datetime_field_names, bool_field_names = self.get_all_field_names(table_map)
        new_sql = sql
        if "PRIMARY_KEY_VAL" in sql:
            new_sql = new_sql.replace("PRIMARY_KEY_VAL", "primary_key_id ")
        if "SAME_FIELD" in sql:
            where_field_names_list = self.extract_field_names(sql_map['where_condition'], all_field_names)
            new_sql = re.sub(r'SAME_FIELD', self.random_field_choice(where_field_names_list), new_sql)
        if "GROUPBY_FIELD" in sql:
            groupby_field_names_list = self.extract_field_names(sql_map['group_by'], all_field_names)
            new_sql = re.sub(r'GROUPBY_FIELD', self.random_field_choice(groupby_field_names_list), new_sql)
        if "BOOL_FIELD_LIST" in sql:
            new_sql = re.sub(r'BOOL_FIELD_LIST', self.random_field_sample(bool_field_names), new_sql)
        if "DATETIME_FIELD_LIST" in sql:
            new_sql = re.sub(r'DATETIME_FIELD_LIST', self.random_field_sample(datetime_field_names), new_sql)
        if "STRING_FIELD_LIST" in sql:
            new_sql = re.sub(r'STRING_FIELD_LIST', self.random_field_sample(string_field_names), new_sql)
        if "NUMERIC_FIELD_LIST" in sql:
            new_sql = re.sub(r'NUMERIC_FIELD_LIST', self.random_field_sample(numeric_field_names), new_sql)
        if "BOOL_FIELD" in sql:
            new_sql = re.sub(r'BOOL_FIELD', self.random_field_choice(bool_field_names), new_sql)
        if "STRING_FIELD" in sql:
            new_sql = re.sub(r'STRING_FIELD', self.random_field_choice(string_field_names), new_sql)
        if "NUMERIC_FIELD" in sql:
            new_sql = re.sub(r'NUMERIC_FIELD', self.random_field_choice(numeric_field_names), new_sql)
        if "DATETIME_FIELD" in sql:
            new_sql = re.sub(r'DATETIME_FIELD', self.random_field_choice(datetime_field_names), new_sql)
        if "OUTER_BUCKET_NAME.*" in new_sql:
            projection = " "+table_map[list(table_map.keys())[0]]["alias_name"]+".* "
            new_sql = new_sql.replace("OUTER_BUCKET_NAME.*", projection)
        return new_sql

    def _gen_aggregate_method_subsitution(self, sql, fields):
        new_sql = ""
        aggregate_function_list = []
        count_star = 0
        token_count = 1
        for token in sql.split(","):
            function_without_alias = token.replace("AS AGGREGATE_FIELD", "")
            function_with_alias = token.replace("AS AGGREGATE_FIELD", " AS ALIAS_"+str(token_count))
            token_count += 1
            if "*" in token:
                if count_star == 0:
                    count_star = 1
                    new_sql += " "+function_with_alias+" ,"
                    aggregate_function_list.append(function_without_alias)
            else:
                subsitution_str = function_with_alias.replace("FIELD", random.choice(fields))
                if subsitution_str not in aggregate_function_list:
                    new_sql += " "+subsitution_str+" ,"
                    aggregate_function_list.append(function_without_alias)
        return new_sql[0:len(new_sql)-1], aggregate_function_list

    def _filter_table_map_based_on_fields(self, fields=[], table_map={}):
        map = {}
        alias_map = {}
        for table_name in table_map:
            if "alias_name" in list(table_map[table_name].keys()):
                alias_map[table_map[table_name]["alias_name"]] = table_name
        for field in fields:
            field = field.strip()
            if len(alias_map) > 0:
                tokens = field.split(".")
                alias_name = tokens[0]
                table_name = alias_map[tokens[0]]
                field_name = tokens[1]
            else:
                field_name = field
                table_name = list(table_map.keys())[0]
            if table_name not in list(map.keys()):
                map[table_name] = {}
                map[table_name]["fields"] = {}
                if len(alias_map) > 0:
                    map[table_name]["alias_name"] = alias_name
            map[table_name]["fields"][field_name] = table_map[table_name]["fields"][field_name]
        return map

    def _convert_condition_template_to_value(self, sql="", table_map={}):
        if "NULL_STR_FIELD" in sql:
            field_name, values = self._search_field(["text", "tinytext", "char"], table_map)
            sql = sql.replace("NULL_STR_FIELD", field_name)
        if "NULL_NUM_FIELD" in sql:
            field_name, values = self._search_field(["int", "mediumint", "double", "float", "decimal"], table_map)
            sql = sql.replace("NULL_NUM_FIELD", field_name)
        tokens = sql.split(" ")
        string_check = False
        numeric_check = False
        bool_check = False
        datetime_check = False
        add_token = True
        new_sql = ""
        space = " "
        values = ["DEFAULT"]
        for token in tokens:
            check = string_check or numeric_check or bool_check or datetime_check
            if not check:
                if "BOOL_FIELD" in token:
                    add_token = False
                    field_name, values = self._search_field(["tinyint"], table_map)
                    new_sql += token.replace("BOOL_FIELD", field_name)+space
                elif "STRING_FIELD" in token:
                    string_check = True
                    add_token = False
                    field_name, values = self._search_field(["varchar", "text", "tinytext", "char"], table_map)
                    new_sql += token.replace("STRING_FIELD", field_name)+space
                elif "NUMERIC_FIELD" in token:
                    add_token = False
                    field_name, values = self._search_field(["int", "mediumint", "double", "float", "decimal"], table_map)
                    new_sql += token.replace("NUMERIC_FIELD", field_name)+space
                    numeric_check = True
                elif "PRIMARY_KEY_VAL" in token:
                    string_check = True
                    add_token = False
                    field_name, values = self._search_field(["varchar", "text", "tinytext", "char"], table_map)
                    new_sql = new_sql + token.replace("PRIMARY_KEY_VAL", "primary_key_id ")
            else:
                if string_check:
                    if token in ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']:
                        add_token = False
                        new_sql += token+space
                        string_check = False
                    elif token == "IS":
                        string_check = False
                        add_token = True
                    elif "LIST" in token:
                        string_check = False
                        add_token = False
                        max = 20
                        if len(values) < 20:
                            max = len(values)
                        list = self._convert_list(values[0:max], type="string")
                        new_sql += token.replace("LIST", list)+space
                    elif "STRING_VALUES" in token:
                        mid_value_index = len(values)//2
                        if "%" in token:
                            value = token.replace("STRING_VALUES", str(values[mid_value_index]))
                            new_sql += value+space
                        else:
                            new_sql+=token.replace("STRING_VALUES", "\""+str(values[mid_value_index])+"\"")+space
                        string_check = False
                        add_token = False
                    elif "UPPER_BOUND_VALUE" in token:
                        string_check = False
                        add_token = False
                        new_sql += token.replace("UPPER_BOUND_VALUE", "\"" + str(values[len(values) - 1]) + "\"") + space
                    elif "LOWER_BOUND_VALUE" in token:
                        add_token = False
                        new_sql += token.replace("LOWER_BOUND_VALUE", "\"" + str(values[0]) + "\"") + space
                    else:
                        add_token = False
                        new_sql += token+space
                elif numeric_check:
                    if token == "IS":
                        numeric_check = False
                        add_token = True
                    elif "LIST" in token:
                        numeric_check = False
                        add_token = False
                        max = 20
                        if len(values) < 20:
                            max = len(values)
                        list = self._convert_list(values[0:max], type="numeric")
                        new_sql += token.replace("LIST", list)+space
                    elif "NUMERIC_VALUE" in token:
                        mid_value_index = len(values)//2
                        numeric_check = False
                        add_token = False
                        new_sql += token.replace("NUMERIC_VALUE", str(values[mid_value_index]))+space
                    elif "UPPER_BOUND_VALUE" in token:
                        numeric_check = False
                        add_token = False
                        new_sql += token.replace("UPPER_BOUND_VALUE", str(values[len(values) - 1])) + space
                    elif "LOWER_BOUND_VALUE" in token:
                        add_token = False
                        new_sql += token.replace("LOWER_BOUND_VALUE", str(values[0])) + space
                    else:
                        add_token = False
                        new_sql += token+space
                elif datetime_check:
                    if token == "IS":
                        datetime_check = False
                        add_token = True
                    elif "LIST" in token:
                        datetime_check = False
                        add_token = False
                        max = 20
                        if len(values) < 20:
                            max = len(values)
                        list = self._convert_list(values[0:max], type="datetime")
                        new_sql += token.replace("LIST", list)+space
                    elif "UPPER_BOUND_VALUE" in token:
                        datetime_check = False
                        add_token = False
                        new_sql += token.replace("UPPER_BOUND_VALUE", "\'" + str(values[len(values) - 1]) + "\'") + space
                    elif "LOWER_BOUND_VALUE" in token:
                        add_token = False
                        new_sql += token.replace("LOWER_BOUND_VALUE", "\'" + str(values[0]) + "\'") + space
                    else:
                        add_token = False
                        new_sql += token+space
                else:
                    new_sql += token+space
            if add_token:
                new_sql += token+space
            else:
                add_token = True
        return new_sql

    def _convert_condition_template_to_value_with_aggregate_method(self, sql="", table_map={}, aggregate_function_list_list=[]):
        tokens = sql.split(" ")
        string_check = False
        numeric_check = False
        bool_check = False
        datetime_check = False
        add_token = True
        new_sql = ""
        space = " "
        values = ["DEFAULT"]
        for token in tokens:
            check = string_check or numeric_check or bool_check or datetime_check
            aggregate_function = random.choice(aggregate_function_list_list)
            aggregate_function_str = aggregate_function.split("(")[0]+"( FIELD )"
            if not check:
                if "BOOL_FIELD" in token:
                    add_token = False
                    field_name, values = self._search_field(["tinyint"], table_map)
                    new_sql+=token.replace("BOOL_FIELD", aggregate_function_str.replace("FIELD", field_name))+space
                elif "STRING_FIELD" in token:
                    string_check = True
                    add_token = False
                    field_name, values = self._search_field(["varchar", "text", "tinytext", "char"], table_map)
                    new_sql += token.replace("STRING_FIELD", aggregate_function_str.replace("FIELD", field_name))+space
                elif "NUMERIC_FIELD" in token:
                    add_token = False
                    field_name, values = self._search_field(["int", "mediumint", "double", "float", "decimal"], table_map)
                    new_sql += token.replace("NUMERIC_FIELD", aggregate_function_str.replace("FIELD", field_name))+space
                    numeric_check = True
            else:
                if string_check:
                    if token == "IS":
                        string_check = False
                        add_token = True
                    elif "LIST" in token:
                        string_check = False
                        add_token = False
                        max = 20
                        if len(values) < 20:
                            max = len(values)
                        list = self._convert_list(values[0:max], type="string")
                        new_sql += token.replace("LIST", list)+space
                    elif "STRING_VALUES" in token:
                        mid_value_index = len(values)//2
                        if "%" in token:
                            value = token.replace("STRING_VALUES", str(values[mid_value_index]))
                            new_sql += value+space
                        else:
                            new_sql += token.replace("STRING_VALUES", "\""+str(values[mid_value_index])+"\"")+space
                        string_check = False
                        add_token = False
                    elif "UPPER_BOUND_VALUE" in token:
                        string_check = False
                        add_token = False
                        new_sql += token.replace("UPPER_BOUND_VALUE", "\"" + str(values[len(values) - 1]) + "\"") + space
                    elif "LOWER_BOUND_VALUE" in token:
                        add_token = False
                        new_sql += token.replace("LOWER_BOUND_VALUE", "\"" + str(values[0]) + "\"") + space
                    else:
                        add_token = False
                        new_sql += token+space
                elif numeric_check:
                    if token == "IS":
                        numeric_check = False
                        add_token = True
                    elif "LIST" in token:
                        numeric_check = False
                        add_token = False
                        max = 20
                        if len(values) < 20:
                            max = len(values)
                        list = self._convert_list(values[0:max], type="numeric")
                        new_sql += token.replace("LIST", list)+space
                    elif "NUMERIC_VALUE" in token:
                        mid_value_index = len(values)//2
                        numeric_check = False
                        add_token = False
                        new_sql += token.replace("NUMERIC_VALUE", str(values[mid_value_index]))+space
                    elif "UPPER_BOUND_VALUE" in token:
                        numeric_check = False
                        add_token = False
                        new_sql += token.replace("UPPER_BOUND_VALUE", str(values[len(values) - 1])) + space
                    elif "LOWER_BOUND_VALUE" in token:
                        add_token = False
                        new_sql += token.replace("LOWER_BOUND_VALUE", str(values[0])) + space
                    else:
                        add_token = False
                        new_sql += token+space
                elif datetime_check:
                    if token == "IS":
                        datetime_check = False
                        add_token = True
                    elif "LIST" in token:
                        datetime_check = False
                        add_token = False
                        max = 20
                        if len(values) < 20:
                            max = len(values)
                        list = self._convert_list(values[0:max], type="datetime")
                        new_sql += token.replace("LIST", list)+space
                    elif "UPPER_BOUND_VALUE" in token:
                        datetime_check = False
                        add_token = False
                        new_sql += token.replace("UPPER_BOUND_VALUE", "\'" + str(values[len(values) - 1]) + "\'") + space
                    elif "LOWER_BOUND_VALUE" in token:
                        add_token = False
                        new_sql += token.replace("LOWER_BOUND_VALUE", "\'" + str(values[0]) + "\'") + space
                    else:
                        add_token = False
                        new_sql += token+space
                else:
                    new_sql += token+space
            if add_token:
                new_sql += token+space
            else:
                add_token = True
        return new_sql

    def _convert_condition_template_to_value_datetime(self, sql="", table_map={}, sql_type="sql"):
        datetime_function_list = [["MILLIS"], ["MILLIS", "MILLIS_TO_STR", "MILLIS"], ["STR_TO_UTC", "MILLIS"], []]
        function_list = random.choice(datetime_function_list)
        tokens = sql.split(" ")
        string_check = False
        numeric_check = False
        bool_check = False
        datetime_check = False
        add_token = True
        new_sql = ""
        space = " "
        values = ["DEFAULT"]
        for token in tokens:
            check = string_check or numeric_check or bool_check or datetime_check
            if not check:
                if "DATETIME_FIELD" in token:
                    add_token = False
                    field_name, values = self._search_field(["datetime"], table_map)
                    if sql_type == "n1ql":
                        new_sql += token.replace("DATETIME_FIELD", self._apply_functions_to_params(function_list, field_name))+space
                    else:
                        new_sql += token.replace("DATETIME_FIELD", field_name)+space
                    datetime_check = True
            else:
                if datetime_check:
                    if token == "IS":
                        datetime_check = False
                        add_token = True
                    elif "DATETIME_LIST" in token:
                        datetime_check = False
                        add_token = False
                        max = 20
                        if len(values) < 20:
                            max = len(values)
                        list = self._convert_list(values[0:max], type="datetime")
                        new_list = self._convert_list_datetime(values[0:max], function_list)
                        if sql_type == "n1ql":
                            new_sql += token.replace("DATETIME_LIST", new_list)+space
                        else:
                            new_sql += token.replace("DATETIME_LIST", list)+space
                    elif "DATETIME_VALUE" in token:
                        mid_value_index = len(values)//2
                        datetime_check = False
                        add_token = False
                        if sql_type == "n1ql":
                            value = self.gen_datetime_delta(str(values[mid_value_index]), token, "n1ql")
                            new_sql += token.replace(token, self._apply_functions_to_params(function_list, value))+space
                        else:
                            value = self.gen_datetime_delta(str(values[mid_value_index]), token, "sql")
                            new_sql += token.replace(token, value)+space
                    elif "UPPER_BOUND_VALUE" in token:
                        datetime_check = False
                        add_token = False
                        if sql_type == "n1ql":
                            new_sql += token.replace("UPPER_BOUND_VALUE", self._apply_functions_to_params(function_list, "\'" + str(values[len(values) - 1]) + "\'")) + space
                        else:
                            new_sql += token.replace("UPPER_BOUND_VALUE", "\'" + str(values[len(values) - 1]) + "\'") + space
                    elif "LOWER_BOUND_VALUE" in token:
                        add_token = False
                        if sql_type == "n1ql":
                            new_sql += token.replace("LOWER_BOUND_VALUE", self._apply_functions_to_params(function_list, "\'" + str(values[0]) + "\'")) + space
                        else:
                            new_sql += token.replace("LOWER_BOUND_VALUE", "\'" + str(values[0]) + "\'") + space
                    else:
                        add_token = False
                        new_sql += token+space
                else:
                    new_sql += token+space
            if add_token:
                new_sql += token+space
            else:
                add_token = True
        if sql_type == "sql":
            new_sql = new_sql.replace("SUBSTR_INDEX", "1")
        else:
            new_sql = new_sql.replace("SUBSTR_INDEX", "0")
        return new_sql

    def gen_datetime_delta(self, datetime_value, sql, type):
        if "ADD" in sql:
            factor = sql.split("DATETIME_VALUE_ADD_")[1]
            sign = "+"
            if type == "n1ql":
                sign = ""
        elif "SUB" in sql:
            factor = sql.split("DATETIME_VALUE_SUB_")[1]
            sign = "-"
        else:
            return "\'"+datetime_value + "\' "
        if type == "sql":
            return "\'"+datetime_value + "\' "+ sign + " INTERVAL 1 " + factor.lower()
        else:
            return "DATE_ADD_STR(\'"+datetime_value+"\',"+sign+"1,\'"+factor.lower()+"\')"

    def find_matching_keywords(self, sql="", keyword_list=[]):
        list = []
        sql = sql.upper()
        for keyword in keyword_list:
            if keyword in sql:
                list.append(keyword)
        return sorted(list)

    def _apply_functions_to_params(self, function_list=[], param="default"):
        sql = param
        for function in function_list:
            sql = function + "( " + sql + " )"
        return sql

    def _gen_n1ql_to_sql(self, n1ql):
        check_keys = False
        space = " "
        new_sql = ""
        for token in n1ql.split(" "):
            if (not check_keys) and token in ["IN", "in"]:
                check_keys = True
                new_sql += " "
            elif not check_keys:
                new_sql += token+space
            if check_keys:
                if "[" in token:
                    val = token.replace("[", "(")
                    if "]" in token:
                        val = val.replace("]", ")")
                        check_keys = False
                    new_sql += val+space
                elif "]" in token:
                    val = token.replace("]", ")")
                    check_keys = False
                    new_sql += val+space
                else:
                    new_sql += token+space
        return new_sql

    def _gen_sql_to_n1ql_braces(self, n1ql):
        check_keys = False
        space = " "
        new_sql = ""
        for token in n1ql.split(" "):
            if (not check_keys) and token in ["IN", "in"]:
                check_keys = True
                new_sql += " "
            elif not check_keys:
                new_sql += token+space
            if check_keys:
                if "(" in token:
                    val = token.replace("(", "[")
                    if ")" in token:
                        val = val.replace(")", "]")
                        check_keys = False
                    new_sql += val+space
                elif ")" in token:
                    val = ""
                    count = 0
                    for vals in token:
                        if count == 0 and vals == ")":
                            val += "]"
                        else:
                            val += vals
                        count += 1
                    check_keys = False
                    new_sql += val+space
                else:
                    new_sql += token+space
        return new_sql

    def _gen_sql_to_nql(self, sql, ansi_joins=False):
        check_keys = False
        check_first_paran = False
        space = " "
        new_sql = ""
        value = ""
        for token in sql.split(" "):
            if (not check_keys) and (token in ["ON", "USING"]) and (not ansi_joins):
                check_keys= True
                if "FOR" not in sql.split(" "):
                    new_sql += " ON KEYS "
                else:
                    new_sql += " ON KEY "
            elif not check_keys:
                new_sql += token+space
            if check_keys:
                if (not check_first_paran) and "(" in token:
                    check_first_paran = True
                    if ")" in token:
                        check_first_paran = False
                        check_keys = False
                        new_sql += token.replace("(", "[ ").split("=")[0]+" ]"+space
                    elif token != "(":
                        value = token.replace("(", "")
                elif check_first_paran and ")" not in token:
                    value += token
                elif check_first_paran and ")" in token:
                    if token != ")":
                        value += token.replace(")", "")+space
                    new_sql += "["+space+value.split("=")[0]+space+"]"+space
                    check_keys = False
                    check_first_paran = False
                    value = ""
        new_sql = new_sql.replace("TRUNCATE", "TRUNC")
        return self._gen_sql_to_n1ql_braces(new_sql)

    def _gen_sqlsubquery_to_nqlsubquery(self, sql):
        check_keys = False
        check_first_paran = False
        space = " "
        new_sql = ""
        value = ""
        sql = sql.replace("SUBTABLE", "simple_table_2")
        for token in sql.split(" "):
            if (not check_keys) and (token in ["ON", "USING"]):
                check_keys = True
                if "FOR" not in sql.split(" "):
                    new_sql += " ON KEYS "
                else:
                    new_sql += " ON KEY "
            elif not check_keys:
                new_sql += token+space
            if check_keys:
                if (not check_first_paran) and "(" in token:
                    check_first_paran = True
                    if ")" in token:
                        check_first_paran = False
                        check_keys = False
                        new_sql += token.replace("(", "[ ").split("=")[0]+" ]"+space
                    elif token != "(":
                        value = token.replace("(", "")
                elif check_first_paran and ")" not in token:
                    value += token
                elif check_first_paran and ")" in token:
                    if token != ")":
                        value += token.replace(")", "")+space
                    new_sql += "["+space+value.split("=")[0]+space+"]"+space
                    check_keys = False
                    check_first_paran = False
                    value = ""
        new_sql = new_sql.replace("TRUNCATE", "TRUNC")
        return self._gen_sql_to_n1ql_braces(new_sql)

    def _read_from_file(self, file_path):
        with open(file_path) as f:
            content = f.readlines()
        return content

    def _read_keywords_from_file(self, file_path):
        content = self._read_from_file(file_path)
        list = []
        for keyword in content:
            list.append(keyword.replace("\n", ""))
        return list

    def _read_from_file_and_convert_queries(self, file_path):
        content = self._read_from_file(file_path)
        return self._convert_sql_list_to_n1ql(content)

    def _convert_n1ql_list_to_sql(self, file_path):
        f = open(file_path+".convert", 'w')
        n1ql_queries = self._read_from_file(file_path)
        for n1ql_query in n1ql_queries:
            sql_query = self._gen_n1ql_to_sql(n1ql_query)
            f.write(sql_query)
        f.close()

    def _convert_template_query_info_with_gsi(self, file_path, gsi_index_file_path=None, table_map={}, table_name="simple_table", define_gsi_index=True):
        f = open(gsi_index_file_path, 'w')
        n1ql_queries = self._read_from_file(file_path)
        for n1ql_query in n1ql_queries:
            map = self._convert_sql_template_to_value_for_secondary_indexes(
                n1ql_query, table_map=table_map, table_name=table_name, define_gsi_index=define_gsi_index)
            f.write(json.dumps(map)+"\n")
        f.close()

    def _convert_sql_list_to_n1ql(self, sql_list):
        n1ql_list = []
        for query in sql_list:
            n1ql_list.append(self._gen_sql_to_nql(query))
        return n1ql_list

    def _dump_queries_into_file(self, queries, file_path):
        f = open(file_path, 'w')
        for query in queries:
            f.write(query)
        f.close()

    def _convert_sql_to_nql_dump_in_file(self, file_path):
        queries = self._read_from_file_and_convert_queries(file_path)
        file_path += ".convert"
        self._dump_queries_into_file(queries, file_path)

    def _convert_list(self, list, type):
        temp_list = ""
        if type == "numeric":
            for num in list:
                temp_list += " " +str(num) + " ,"
        if type == "string":
            for num in list:
                temp_list += " \"" +str(num) + "\" ,"
        if type == "datetime":
            for num in list:
                temp_list += " \'" +str(num) + "\' ,"
        return temp_list[0:len(temp_list)-1]

    def _convert_list_datetime(self, list, function_list):
        temp_list = ""
        for num in list:
            value = " \'"+str(num)+"\'"
            value = self._apply_functions_to_params(function_list, value)
            temp_list += value+" ,"
        return temp_list[0:len(temp_list)-1]

if __name__ == "__main__":
    helper = BaseRQGQueryHelper()
    print(helper._convert_sql_template_to_value_nested_subqueries("CRAP1 TABLE_ALIAS.* CRAP2 TABLE_ALIAS.* FROM TABLE_ALIAS TABLE_ALIAS"))
