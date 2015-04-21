import random
import string
from random import randrange
from random import randint
from datetime import datetime

class QueryHelper(object):
    def _find_hints(self, n1ql_query):
        select_text  =  self._find_string_type(n1ql_query, ["SELECT", "Select", "select"])
        from_text = self._find_string_type(n1ql_query, ["FROM", "from", "From"])
        result_text = n1ql_query.split(select_text)[1].split(from_text)[0].strip()
        if self._check_function(result_text):
            return "FUN"
        if result_text == "*":
            return []
        if ".*" in result_text:
            return [result_text.split(".")[0]]

    def _divide_sql(self, sql):
        sql = sql.replace(";","")
        sql = sql.replace("\n","")
        group_by_text = None
        where_condition_text = None
        order_by_text = None
        select_from_text = None
        select_text  =  self._find_string_type(sql, ["SELECT", "Select", "select"])
        from_text = self._find_string_type(sql, ["FROM", "from", "From"])
        where_text = self._find_string_type(sql, ["WHERE", "where",])
        order_by = self._find_string_type(sql, ["ORDER BY", "order by"])
        group_by = self._find_string_type(sql, ["GROUP BY", "group by"])
        from_field_text = sql.split(from_text)[1].split(where_text)[0]
        select_from_text = sql.split(select_text)[1].split(from_text)[0].strip()
        where_condition_text = sql.split(where_text)[1]
        if group_by:
            group_by_text = sql.split(group_by)[1]
            where_condition_text = where_condition_text.split(group_by)[0]
        if order_by:
            order_by_text = sql.split(order_by)[1]
            if group_by_text:
                group_by_text = group_by_text.split(order_by)[0]
            where_condition_text = where_condition_text.split(order_by)[0]
        map = {
                "from_fields": from_field_text,
                "where_condition":where_condition_text,
                "select_from":select_from_text,
                "group_by": group_by_text,
                "order_by" : order_by_text
                }
        return map

    def _check_function(self, sql):
        func_list = ["MIN", "min", "MAX", "max" ,"COUNT","SUM","sum","AVG","avg"]
        for func in func_list:
            if func in sql:
                return True
        return False

    def _find_string_type(self, n1ql_query, hints = []):
        for hint in hints:
            if hint in n1ql_query:
                return hint

    def _gen_json_from_results_with_primary_key(self, columns, rows, primary_key = ""):
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
        for key in map.keys():
            if self._search_presence_of_type(map[key]["type"],types):
                list_types.append(key)
        key =random.choice(list_types)
        return key, map[key]["distinct_values"]

    def _search_fields_of_given_type(self, types, map):
        list_types =[]
        for key in map.keys():
            if self._search_presence_of_type(map[key]["type"],types):
                list_types.append(key)
        return list_types

    def _search_presence_of_type(self, type, list):
        for key in list:
            if key in type:
                return True
        return False

    def _generate_random_range(self, list):
        val = randrange(0,len(list))
        if val == 0:
            val = len(list)
        return list[0:val]

    def _random_alphanumeric(self, limit = 10):
        #ascii alphabet of all alphanumerals
        r = (range(48, 58) + range(65, 91) + range(97, 123))
        random.shuffle(r)
        return reduce(lambda i, s: i + chr(s), r[:random.randint(0, len(r))], "")

    def _random_char(self):
        return random.choice(string.ascii_uppercase)

    def _random_int(self):
        return randint(0,10000)

    def _random_float(self):
        return 10000*random.random()

    def _random_double(self):
        return 10000*random.random()

    def _random_datetime(self, start = 1999, end = 2015):
        year = random.choice(range(start, end))
        month = random.choice(range(1, 13))
        day = random.choice(range(1, 29))
        return datetime(year, month, day)

    def _generate_insert_statement(self, table_name ="TABLE_NAME", table_map ={}):
        values = ""
        intial_statement = ""
        intial_statement += " INSERT INTO {0} ".format(table_name)
        column_names = "( "+",".join(table_map.keys())+" ) "
        values = ""
        for field_name in table_map.keys():
            type = table_map[field_name]["type"]
            if "tinyint" in type:
                values +=  str(self._random_int()%10)+","
            elif "mediumint" in type:
                values +=  str(self._random_int()%100)+","
            elif "int" in type:
                values +=  str(self._random_int())+","
            elif "decimal" in type:
                values +=  str(self._random_float())+","
            elif "float" in type:
                values +=  str(self._random_float())+","
            elif "double" in type:
                values +=  str(self._random_double())+","
            elif "varchar" in type:
                values +=  "\""+self._random_alphabet_string()+"\","
            elif "char" in type:
                values +=  "\'"+self._random_char()+"\',"
            elif "tinytext" in type:
                values +=  "\'"+self._random_alphabet_string(limit = 1)+"\',"
            elif "mediumtext" in type:
                values +=  "\'"+self._random_alphabet_string(limit = 5)+"\',"
            elif "text" in type:
                values +=  "\'"+self._random_alphabet_string(limit = 5)+"\',"
            elif "datetime" in type:
                values +=  "\'"+str(self._random_datetime())+"\',"
            elif "bool" in type:
                values +=  "\'True\',"
        return intial_statement+column_names+" VALUES ( "+values[0:len(values)-1]+" )"

    def _random_alphabet_string(self, limit =10):
        uppercase = sorted(string.ascii_uppercase)
        lowercase = sorted(string.ascii_lowercase)
        value = []
        for x in range(0,limit/2):
            value.append(random.choice(uppercase))
            value.append(random.choice(lowercase))
        random.shuffle(value)
        return "".join(value)

    def _covert_fields_template_to_value(self, sql = "", table_map = {}):
        string_field_names = self._search_fields_of_given_type(["varchar","text","tinytext","char"], table_map)
        numeric_field_names = self._search_fields_of_given_type(["int","mediumint","tinyint","double", "float", "decimal"], table_map)
        datetime_field_names = self._search_fields_of_given_type(["datetime"], table_map)
        new_sql = sql
        if "DATETIME_FIELD_LIST" in sql:
            new_list = self._generate_random_range(datetime_field_names)
            new_sql = new_sql.replace("DATETIME_FIELD_LIST", self._convert_list(new_list,"numeric"))
        if "STRING_FIELD_LIST" in sql:
            new_list = self._generate_random_range(string_field_names)
            new_sql = new_sql.replace("STRING_FIELD_LIST", self._convert_list(new_list,"numeric"))
        if "NUMERIC_FIELD_LIST" in sql:
            new_list = self._generate_random_range(numeric_field_names)
            new_sql = new_sql.replace("NUMERIC_FIELD_LIST", self._convert_list(new_list,"numeric"))
        if "STRING_FIELD" in sql:
            new_sql = new_sql.replace("STRING_FIELD", random.choice(string_field_names))
        if "NUMERIC_FIELD"  in sql:
            new_sql = new_sql.replace("NUMERIC_FIELD", random.choice(numeric_field_names))
        if "DATETIME_FIELD"  in sql:
            new_sql = new_sql.replace("DATETIME_FIELD", random.choice(datetime_field_names))
        return new_sql

    def _convert_sql_template_to_value(self, sql ="", table_map = {}, table_name= "BUCKET_NAME"):
        sql_map = self._divide_sql(sql)
        select_from = sql_map["select_from"]
        from_fields = sql_map["from_fields"]
        where_condition = sql_map["where_condition"]
        order_by = sql_map["order_by"]
        group_by = sql_map["group_by"]
        new_sql = "SELECT "
        if select_from:
            new_sql += self._covert_fields_template_to_value(select_from, table_map)+" FROM "
        if from_fields:
            new_sql += from_fields.replace("BUCKET_NAME",table_name)+ " "
        if where_condition:
            new_sql += " WHERE "+self._convert_condition_template_to_value(where_condition, table_map)+ " "
        if group_by:
            new_sql += " GROUP BY "+self._covert_fields_template_to_value(group_by, table_map)+" "
        if order_by:
            new_sql += " ORDER BY "+self._covert_fields_template_to_value(order_by, table_map)+" "
        return new_sql

    def _convert_condition_template_to_value(self, sql ="", table_map = {}):
        tokens = sql.split(" ")
        check = False
        string_check = False
        boolean_check = False
        numeric_check = False
        bool_check = False
        datetime_check = False
        add_token = True
        new_sql = ""
        space = " "
        field_name = ""
        values = ["DEFAULT"]
        for token in tokens:
            check = string_check or numeric_check or bool_check or datetime_check
            if not check:
                if "STRING_FIELD" in token:
                    string_check = True
                    add_token = False
                    field_name, values = self._search_field(["varchar","text","tinytext","char"], table_map)
                    new_sql+=token.replace("STRING_FIELD",field_name)+space
                elif "NUMERIC_FIELD" in token:
                    add_token = False
                    field_name, values = self._search_field(["int","mediumint","tinyint","double", "float", "decimal"], table_map)
                    new_sql+=token.replace("NUMERIC_FIELD",field_name)+space
                    numeric_check = True
                elif "DATETIME_FIELD" in token:
                    add_token = False
                    field_name, values = self._search_field(["datetime"], table_map)
                    new_sql+=token.replace("DATETIME_FIELD",field_name)+space
                    datetime_check = True
                elif "BOOL_FIELD" in token:
                    new_sql+=token.replace("BOOL_FIELD",field_name)+space
                    field_name, values = self._search_field(["bool"], table_map)
                    add_token = False
                    book_check = False
            else:
                if string_check:
                    if token == "IS":
                        string_check = False
                        add_token = True
                    elif "LIST" in token:
                        string_check = False
                        add_token = False
                        max = 5
                        if len(values) < 5:
                            max = len(values)
                        list = self._convert_list(values[0:max], type="string")
                        new_sql+=token.replace("LIST",list)+space
                    elif "STRING_VALUES" in token:
                        mid_value_index = len(values)/2
                        if "%" in token:
                            value = token.replace("STRING_VALUES",str(values[mid_value_index]))
                            new_sql+=value+space
                        else:
                            new_sql+=token.replace("STRING_VALUES","\""+str(values[mid_value_index])+"\"")+space
                        string_check = False
                        add_token = False
                    elif "UPPER_BOUND_VALUE" in token:
                        string_check = False
                        add_token = False
                        new_sql+=token.replace("UPPER_BOUND_VALUE","\""+str(values[len(values) -1])+"\"")+space
                    elif "LOWER_BOUND_VALUE" in token:
                        add_token = False
                        new_sql+=token.replace("LOWER_BOUND_VALUE","\""+str(values[0])+"\"")+space
                    else:
                        add_token = False
                        new_sql+=token+space
                elif numeric_check:
                    if token == "IS":
                        numeric_check = False
                        add_token = True
                    elif "LIST" in token:
                        numeric_check = False
                        add_token = False
                        max = 5
                        if len(values) < 5:
                            max = len(values)
                        list = self._convert_list(values[0:max], type="numeric")
                        new_sql+=token.replace("LIST",list)+space
                    elif "NUMERIC_VALUE" in token:
                        mid_value_index = len(values)/2
                        numeric_check = False
                        add_token = False
                        new_sql+=token.replace("NUMERIC_VALUE",str(values[mid_value_index]))+space
                    elif "UPPER_BOUND_VALUE" in token:
                        numeric_check = False
                        add_token = False
                        new_sql+=token.replace("UPPER_BOUND_VALUE",str(values[len(values) -1]))+space
                    elif "LOWER_BOUND_VALUE" in token:
                        add_token = False
                        new_sql+=token.replace("LOWER_BOUND_VALUE",str(values[0]))+space
                    else:
                        add_token = False
                        new_sql+=token+space
                elif datetime_check:
                    if token == "IS":
                        datetime_check = False
                        add_token = True
                    elif "LIST" in token:
                        datetime_check = False
                        add_token = False
                        max = 5
                        if len(values) < 5:
                            max = len(values)
                        list = self._convert_list(values[0:max], type="datetime")
                        new_sql+=token.replace("LIST",list)+space
                    elif "DATETIME_VALUES" in token:
                        mid_value_index = len(values)/2
                        datetime_check = False
                        add_token = False
                        new_sql+=token.replace("DATETIME_VALUES","\'"+str(values[mid_value_index])+"\'")+space
                    elif "UPPER_BOUND_VALUE" in token:
                        datetime_check = False
                        add_token = False
                        new_sql+=token.replace("UPPER_BOUND_VALUE","\'"+str(values[len(values) -1])+"\'")+space
                    elif "LOWER_BOUND_VALUE" in token:
                        add_token = False
                        new_sql+=token.replace("LOWER_BOUND_VALUE","\'"+str(values[0])+"\'")+space
                    else:
                        add_token = False
                        new_sql+=token+space
                else:
                    new_sql+=token+space
            if add_token:
                new_sql+=token+space
            else:
                add_token = True
        return new_sql

    def _gen_n1ql_to_sql(self, n1ql):
        check_keys=False
        check_first_paran = False
        space = " "
        new_sql = ""
        value = ""
        #print "Analyzing for : %s" % sql
        for token in n1ql.split(" "):
            if (not check_keys) and (token == "IN" or token == "in"):
                check_keys= True
                new_sql += " ON KEYS "
            elif not check_keys:
                new_sql += token+space
            if check_keys:
                if "[" in token:
                    val = token.replace("[","(")
                    if "]" in token:
                        val = val.replace("]",")")
                        check_keys = False
                    new_sql += val+space
                elif "]" in token:
                    val = token.replace("]",")")
                    check_keys = False
                    new_sql += val+space
                else:
                    new_sql += token+space
        return new_sql

    def _gen_sql_to_nql(self, sql):
        check_keys=False
        check_first_paran = False
        space = " "
        new_sql = ""
        value = ""
        #print "Analyzing for : %s" % sql
        for token in sql.split(" "):
            if (not check_keys) and (token == "ON" or token == "USING"):
                check_keys= True
                new_sql += " ON KEYS "
            elif not check_keys:
                new_sql += token+space
            if check_keys:
                if (not check_first_paran) and "(" in token:
                    check_first_paran = True
                    if ")" in token:
                        check_first_paran = False
                        check_keys = False
                        new_sql += token.replace("(","[ ").split("=")[0]+" ]"+space
                    elif token != "(":
                        value = token.replace("(","")
                elif check_first_paran and ")" not in token:
                    value+=token
                elif check_first_paran and ")" in token:
                    if token != ")":
                        value += token.replace(")","")+space
                    new_sql += "["+space+value.split("=")[0]+space+"]"+space
                    check_keys = False
                    check_first_paran = False
                    value = ""
        return new_sql

    def  _read_from_file(self, file_path):
        with open(file_path) as f:
            content = f.readlines()
        return content

    def _read_from_file_and_convert_queries(self, file_path):
        content = self._read_from_file(file_path)
        return self._convert_sql_list_to_n1ql(content)

    def _convert_n1ql_list_to_sql(self, file_path):
        f = open(file_path+".convert",'w')
        n1ql_queries = self._read_from_file(file_path)
        for n1ql_query in n1ql_queries:
            sql_query=self._gen_n1ql_to_sql(n1ql_query)
            f.write(sql_query)
        f.close()

    def _convert_sql_list_to_n1ql(self, sql_list):
        n1ql_list = []
        for query in sql_list:
            n1ql_list.append(self._gen_sql_to_nql(query))
        return n1ql_list

    def _dump_queries_into_file(self, queries, file_path):
        f = open(file_path,'w')
        for query in queries:
            f.write(query)
        f.close()

    def _convert_sql_to_nql_dump_in_file(self, file_path):
        queries = self._read_from_file_and_convert_queries(file_path)
        file_path+=".convert"
        self._dump_queries_into_file(queries, file_path)

    def _convert_list(self, list, type):
        temp_list = ""
        if type == "numeric":
            for num in list:
                temp_list +=" "+str(num)+" ,"
        if type == "string":
            for num in list:
                temp_list +=" \""+num+"\" ,"
        if type == "datetime":
            for num in list:
                temp_list +=" \'"+str(num)+"\' ,"
        return temp_list[0:len(temp_list)-1]

if __name__=="__main__":
    helper = QueryHelper()
    helper._convert_n1ql_list_to_sql("/Users/parag/fix_testrunner/testrunner/b/resources/rqg/simple_table/query_examples/n1ql_10000_queries_for_simple_table.txt")
    #helper._convert_sql_to_nql_dump_in_file("/Users/parag/fix_testrunner/testrunner/b/resources/flightstats_mysql/inner_join_flightstats_n1ql_queries.txt")
    #print helper._gen_sql_to_nql("SELECT SUM(  a1.distance) FROM `ontime_mysiam`  AS a1 INNER JOIN `aircraft`  AS a2 ON ( a2 .`tail_num` = a1 .`tail_num` ) INNER JOIN `airports`  AS a3 ON ( a1 . `origin` = a3 .`code` ) ")
    #print helper._gen_sql_to_nql("SELECT a1.* FROM ON (a.key1 = a.key2)")
    #print helper._gen_sql_to_nql("SELECT a1.* FROM ON (a.key1= a.key2)")
    #print helper._gen_sql_to_nql("SELECT a1.* FROM ON (a.key1 =a.key2)")
    #print helper._gen_sql_to_nql("SELECT a1.* FROM USING (a.key1)")
    #print helper._gen_sql_to_nql("SELECT a1.* FROM USING ( a.key1 )")
    #print helper._gen_sql_to_nql("SELECT a1.* FROM USING (a.key1 )")
    #print helper._gen_sql_to_nql("SELECT a1.* FROM USING ( a.key1)")
    #print helper._gen_sql_to_nql("SELECT a1.* FROM ON ( a.key1 = a.key2 ) ON ( a.key1 = a.key2 )")
    #print helper._gen_sql_to_nql("SELECT a1.* FROM USING (a.key1)  ON (a.key1=a.key2)  USING ( a.key1 )")
    #print helper._gen_sql_to_nql("SELECT a1.* FROM USING (a.key1=a.key2)  ON (a.key1=a.key2)  USING ( a.key1 )")
    #print helper._gen_sql_to_nql("SELECT a1.* FROM ON (a.key1=a.key2)  ON (a.key1=a.key2)  ON ( a.key1 = a.key2 )")
    #path = "/Users/parag/fix_testrunner/testrunner/query.txt"
    #queries = helper._read_from_file_and_convert_queries(path)
    #helper. _dump_queries_into_file(queries, "/Users/parag/fix_testrunner/testrunner/n1ql_query.txt")
    #for query in queries:
     #   print query