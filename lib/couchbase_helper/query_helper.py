import random

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

    def _check_function(self, sql):
        func_list = ["MIN", "min", "MAX", "max" "DISTINCT","COUNT","SUM","sum"]
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

    def _search_presence_of_type(self, type, list):
        for key in list:
            if key in type:
                return True
        return False

    def _convert_sql_template_to_value(self, sql ="", table_map = {}):
        tokens = sql.split(" ")
        check = False
        string_check = False
        boolean_check = False
        numeric_check = False
        bool_check = False
        add_token = True
        new_sql = ""
        space = " "
        field_name = ""
        values = ["DEFAULT"]
        for token in tokens:
            check = string_check or numeric_check or bool_check
            if not check:
                if "STRING_FIELD" in token:
                    string_check = True
                    add_token = False
                    field_name, values = self._search_field(["varchar"], table_map)
                    new_sql+=token.replace("STRING_FIELD",field_name)+space
                elif "NUMERIC_FIELD" in token:
                    add_token = False
                    field_name, values = self._search_field(["int","double", "float", "decimal"], table_map)
                    new_sql+=token.replace("NUMERIC_FIELD",field_name)+space
                    numeric_check = True
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
                else:
                    new_sql+=token+space
            if add_token:
                new_sql+=token+space
            else:
                add_token = True
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
        return temp_list[0:len(temp_list)-1]



if __name__=="__main__":
    helper = QueryHelper()
    print "running now"
    print helper._find_hints("SELECT MIN(A.*) from check")
    sql = "SELECT * FROM TABLE WHERE ( NUMERIC_FIELD COMPARATOR NUMERIC_VALUE ) and (  NUMERIC_FIELD COMPARATOR LOWER_BOUND_VALUE OR UPPER_BOUND_VALUE ) "
    new_sql = helper._convert_sql_template_to_value(sql = sql)
    print sql
    print new_sql
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