import random
import sys
sys.path.append('/Users/apiravi/testrunner/')
from lib.couchbase_helper.data import FIRST_NAMES, LAST_NAMES, LANGUAGES, DEPT

class DATASET:
    EMP_FIELDS = {'str': ["name", "dept", "manager_reports",
                               "languages_known", "email"],
                  'float': ["salary"],
                  'int': ["empid", "mutated", "manages_teamsize"],
                  'bool': ["is_manager"],
                  'date': ["join_date"],
                  'text': ["name", "manager_reports"]}

class QUERY_TYPE:
    VALUES = ["match", "phrase", "bool", "match_phrase",
              "prefix", "fuzzy", "conjunction", "disjunction"
              "wildcard", "regexp",  "query_string",
              "numeric_range", "date_range", "match_all", "match_none"]


class FTSQueryGenerator:

    def __init__(self, num_queries=1, query_type=None, seed=0):
        """
        FTS query generator for employee dataset (JsonDocGenerator in
        couchbase_helper/documentgenerator.py)

        """
        random.seed(seed)
        self.queries_to_generate = num_queries
        self.iterator = 0
        self.queries = []
        self.query_types = query_type
        self.construct_queries()

    def construct_queries(self):
        while self.iterator < self.queries_to_generate:
            fieldname = self.get_random_value(self.query_types)
            query = eval("self.construct_%s_query" % fieldname + "()")
            self.queries.append(query)
            self.iterator += 1

    def construct_match_query(self, ret_list=False):
        """
        Returns a single match query or a list containing upto 3 match queries
        """
        match_query = {}
        fieldname = self.get_random_value(DATASET.EMP_FIELDS['str'])
        match_query["field"] = fieldname
        match_query["match"] = \
            eval("self.get_queryable_%s" % fieldname + "()")
        if not ret_list:
            return match_query
        match_query_count = random.randint(2, 3)
        match_query_list = [match_query]
        while len(match_query_list) < match_query_count:
            match_query = {}
            fieldname = self.get_random_value(DATASET.EMP_FIELDS['str'])
            match_query["field"] = fieldname
            match_query["match"] = \
                eval("self.get_queryable_%s" % fieldname + "()")
            match_query_list.append(match_query)
        return match_query_list

    def construct_bool_query(self):
        bool_query = {}
        if bool(random.getrandbits(1)):
            bool_query['must'] = \
                {"conjuncts": self.construct_match_query(
                    ret_list=bool(random.getrandbits(1)))}
        if bool(random.getrandbits(1)):
            bool_query['must_not'] = \
                {"disjuncts": self.construct_match_query(
                    ret_list=bool(random.getrandbits(1)))}
        bool_query['should'] = \
                {"disjuncts": self.construct_match_query(
                    ret_list=bool(random.getrandbits(1)))}
        return bool_query

    def construct_conjunction_query(self, mixed=True):
        if mixed:
            return {"conjuncts": self.construct_match_query(ret_list=True)}
        else:
            return {"conjuncts": self.construct_compound_query()}

    def construct_disjunction_query(self):
        return {"disjuncts": self.construct_match_query(ret_list=True)}

    def construct_match_phrase_query(self):
        match_phrase_query = {}
        fieldname = self.get_random_value(DATASET.EMP_FIELDS['text'])
        match_phrase_query["field"] = fieldname
        match_phrase_query["match_phrase"] = \
            eval("self.get_queryable_%s" % fieldname + "(full=True)")
        return match_phrase_query

    def construct_phrase_query(self):
        phrase_query = self.construct_match_phrase_query()
        flat_query = json.dumps(phrase_query).replace("match_phrase", "phrase")
        return json.loads(flat_query)

    def construct_prefix_query(self):
        prefix_query = {}
        match_query = self.construct_match_query()
        prefix_query["prefix"] = match_query["match"][:random.randint(1, 4)]
        prefix_query["field"] = match_query["field"]
        return prefix_query

    def construct_date_range_query(self):
        date_query = {}
        fieldname = self.get_random_value(DATASET.EMP_FIELDS['date'])
        date_query['field'] = fieldname
        date_query['start'] = eval("self.get_queryable_%s" % fieldname + "()")
        date_query['end'] = eval("self.get_queryable_%s" % fieldname + "()")
        return date_query

    def construct_numeric_range_query(self):
        numeric_query = {}
        fieldname = self.get_random_value(DATASET.EMP_FIELDS['int'] +
                                          DATASET.EMP_FIELDS['float'])
        numeric_query['field'] = fieldname
        numeric_query['min'] = eval("self.get_queryable_%s" % fieldname + "()")
        numeric_query['max'] = numeric_query['min'] + random.randint(2, 10000)
        return numeric_query

    def construct_query_string_query(self):
        #TODO
        pass

    def construct_wildcard_query(self):
        # TODO
        wildcard_query = {"wildcard": {}}
        pass

    def construct_regexp_query(self):
        #TODO
        pass

    def construct_compound_query(self):
        """
        This is used to consolidate more than one type of query
        say - return a list of match, phrase, match-phrase queries
        * to be enclosed by 'conjuncts' or 'disjuncts' query
        """
        compound_query = []
        if bool(random.getrandbits(1)):
            compound_query.append(self.construct_match_query())
        if bool(random.getrandbits(1)):
            compound_query.append(self.construct_phrase_query())
        if bool(random.getrandbits(1)):
            compound_query.append(self.construct_match_phrase_query())
        if bool(random.getrandbits(1)):
            compound_query.append(self.construct_date_range_query())
        if bool(random.getrandbits(1)):
            compound_query.append(self.construct_numeric_range_query())
        return compound_query

    def get_random_value(self, list):
        return list[random.randint(0, len(list)-1)]

    def get_queryable_name(self, full=False):
        """
        Returns a first or last name OR
        a combination of both
        """
        if full:
            return self.get_queryable_full_name()

        if bool(random.getrandbits(1)):
            name_list = FIRST_NAMES + LAST_NAMES
            return self.get_random_value(name_list)
        else:
            return self.get_queryable_full_name()


    def get_queryable_full_name(self):
        return "%s %s" %(self.get_random_value(FIRST_NAMES),
                         self.get_random_value(LAST_NAMES))

    def get_queryable_dept(self):
        """
        Returns a valid dept to be queried
        """
        return self.get_random_value(DEPT)

    def get_queryable_join_date(self):
        import datetime
        year = random.randint(1950, 2016)
        month = random.randint(1, 12)
        day = random.randint(1, 28)
        hour = random.randint(0, 23)
        min = random.randint(0, 59)
        return str(datetime.datetime(year, month, day, hour, min))

    def get_queryable_languages_known(self):
        """
        Returns one or two languages
        """
        if bool(random.getrandbits(1)):
            return self.get_random_value(LANGUAGES)
        else:
            return "%s %s" % (self.get_random_value(LANGUAGES),
                             self.get_random_value(list(reversed(LANGUAGES))))

    def get_queryable_email(self):
        return "%s@mcdiabetes.com" % self.get_random_value(FIRST_NAMES)

    def get_queryable_empid(self):
        return random.randint(10000000, 10001000)

    def get_queryable_salary(self):
        return random.random() *100000 + 50000

    def get_queryable_manager_team_size(self):
        return random.randint(5, 10)

    def get_queryable_mutated(self):
        return random.randint(0, 10)

    def get_queryable_manager_reports(self, full=False):
        """
        Returns first names of 1-4 reports
        """
        num_reports = random.randint(1, 4)
        reports = []
        if not full:
            while len(reports) < num_reports:
                if num_reports == 1:
                    return self.get_random_value(FIRST_NAMES)
                reports.append(self.get_random_value(FIRST_NAMES))
            return ' '.join(reports)
        else:
            while len(reports) < num_reports:
                if num_reports == 1:
                    return self.get_queryable_full_name()

                reports.append(self.get_queryable_full_name())
            return ' '.join(reports)

if __name__ == "__main__":
    import json
    #query_type=["match", "match_phrase","bool", "conjunction", "disjunction",
    # "prefix"]
    query_type=["prefix"]
    query_gen = FTSQueryGenerator(10, query_type=query_type)
    for query in query_gen.queries:
        print json.dumps(query, indent=3).replace("manager_", "manager.")
        print "------------"