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

if __name__=="__main__":
    helper = QueryHelper()
    print "running now"
    print helper._find_hints("SELECT * FROM")
    print helper._find_hints("SELECT a1.* FROM")