class QueryHelper(object):
    def _find_hints(self, n1ql_query):
        select_text  =  self._find_string_type(n1ql_query, ["SELECT", "Select", "select"])
        from_text = self._find_string_type(n1ql_query, ["FROM", "from", "From"])
        result_text = n1ql_query.split(select_text)[1].split(from_text)[0].strip()
        if result_text == "*":
            return []
        if ".*" in result_text:
            return [result_text.split(".")[0]]

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


if __name__=="__main__":
    helper = QueryHelper()
    print "running now"
    helper._convert_sql_to_nql_dump_in_file("/Users/parag/fix_testrunner/testrunner/b/resources/flightstats_mysql/inner_join_flightstats_n1ql_queries.txt")
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