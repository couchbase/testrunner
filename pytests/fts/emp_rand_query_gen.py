import random
import json
import sys
sys.path.append("/Users/apiravi/testrunner")
from lib.couchbase_helper.data import FIRST_NAMES, LAST_NAMES, LANGUAGES, DEPT

class DATASET:
    EMP_FIELDS = {'str': ["name", "dept", "manages_reports",
                               "languages_known", "email"],
                  'float': ["salary"],
                  'int': ["empid", "mutated", "manages_team_size"],
                  'bool': ["is_manager"],
                  'date': ["join_date"],
                  'text': ["name", "manages_reports"]}

class QUERY_TYPE:
    VALUES = ["match", "phrase", "bool", "match_phrase",
              "prefix", "fuzzy", "conjunction", "disjunction"
              "wildcard", "regexp",  "query_string",
              "numeric_range", "date_range", "match_all", "match_none"]


class FTSESQueryGenerator:

    def __init__(self, num_queries=1, query_type=None, seed=0):
        """
        FTS(Bleve) and equivalent ES(Lucene) query generator for employee dataset
        (JsonDocGenerator in couchbase_helper/documentgenerator.py)

        """
        random.seed(seed)
        self.queries_to_generate = num_queries
        self.iterator = 0
        self.fts_queries = []
        self.es_queries = []
        self.query_types = query_type
        self.construct_queries()

    def construct_queries(self):
        while self.iterator < self.queries_to_generate:
            fieldname = self.get_random_value(self.query_types)
            fts_query, es_query = eval("self.construct_%s_query" % fieldname + "()")
            self.fts_queries.append(fts_query)
            self.es_queries.append(es_query)
            self.iterator += 1

    def construct_match_query(self, ret_list=False):
        """
        Returns a single match query or a list containing upto 3 match queries
        """
        match_query_count = random.randint(2, 3)
        fts_match_query_list = []
        es_match_query_list = []

        while len(fts_match_query_list) < match_query_count:
            fts_match_query = {}
            es_match_query = {'match': {}}

            fieldname = self.get_random_value(DATASET.EMP_FIELDS['str'])
            match_str = eval("self.get_queryable_%s" % fieldname + "()")

            fts_match_query["field"] = fieldname
            fts_match_query["match"] = match_str

            es_match_query['match'][fieldname] = match_str

            if not ret_list:
                return fts_match_query, es_match_query

            fts_match_query_list.append(fts_match_query)
            es_match_query_list.append(es_match_query)

        return fts_match_query_list, es_match_query_list

    def construct_bool_query(self):
        """
        Constructs a bool query with must, must_not and should clauses
        """
        fts_bool_query = {}
        es_bool_query = {'bool': {}}

        if bool(random.getrandbits(1)):
            must_fts_query, must_es_query = self.construct_match_query(
                ret_list=bool(random.getrandbits(1)))
            fts_bool_query['must'] = {"conjuncts": must_fts_query}
            es_bool_query['bool']['must'] = must_es_query

        if bool(random.getrandbits(1)):
            must_not_fts, must_not_es = self.construct_match_query(
                ret_list=bool(random.getrandbits(1)))
            fts_bool_query['must_not'] = {"disjuncts": must_not_fts}
            es_bool_query['bool']['must_not'] = must_not_es

        should_fts, should_es = self.construct_match_query(
            ret_list=bool(random.getrandbits(1)))
        fts_bool_query['should'] = {"disjuncts": should_fts}
        es_bool_query['bool']['should'] = should_es

        return fts_bool_query, es_bool_query

    def construct_conjunction_query(self, mixed=True):
        """
        Returns an fts and es query with queries to be ANDed
        """
        if not mixed:
            fts_query, es_query = self.construct_match_query(ret_list=True)
        else:
            fts_query, es_query = self.construct_compound_query()
        fts_conj_query = {"conjuncts": fts_query}
        es_conj_query = {'bool': {}}
        es_conj_query['bool']['must'] = es_query
        return fts_conj_query, es_conj_query

    def construct_disjunction_query(self):
        """
        Returns an fts and es query with queries to be ORed
        """
        fts_query, es_query = self.construct_match_query(ret_list=True)
        fts_disj_query = {"disjuncts": fts_query}
        es_disj_query = {'bool': {}}
        es_disj_query['bool']['should'] = es_query
        return fts_disj_query, es_disj_query

    def construct_match_phrase_query(self):
        """
         An exact phrase search with analysis on search phrase
        """
        fts_match_phrase_query = {}
        es_match_phrase_query = {'match_phrase': {}}
        fieldname = self.get_random_value(DATASET.EMP_FIELDS['text'])
        match_str = eval("self.get_queryable_%s" % fieldname + "(full=True)")
        fts_match_phrase_query["field"] = fieldname
        fts_match_phrase_query["match_phrase"] = match_str
        es_match_phrase_query['match_phrase'][fieldname] = match_str
        return fts_match_phrase_query, es_match_phrase_query

    def construct_phrase_query(self):
        """
        Same as match_phrase query minus the analysis
        An equivalent query in ES is not known
        """
        fts_match_phrase_query, _ = self.construct_match_phrase_query()
        flat_query = json.dumps(fts_match_phrase_query).replace("match_phrase",
                                                                "phrase")
        return json.loads(flat_query), {}

    def construct_prefix_query(self):
        fts_prefix_query = {}
        es_prefix_query = {'prefix':{}}
        fts_match_query, _ = self.construct_match_query()
        prefix_search = fts_match_query["match"][:random.randint(1, 4)]
        fts_prefix_query["prefix"] =  prefix_search
        fts_prefix_query["field"] = fts_match_query["field"]
        es_prefix_query['prefix'][fts_match_query["field"]] = prefix_search
        return fts_prefix_query, es_prefix_query

    def construct_date_range_query(self):
        """
        Generates a fts and es date range query
        """
        fts_date_query = {}
        es_date_query = self.construct_es_empty_filter_query()

        fieldname = self.get_random_value(DATASET.EMP_FIELDS['date'])
        start = eval("self.get_queryable_%s" % fieldname + "()")
        end = eval("self.get_queryable_%s" % fieldname + "(now=True)")
        fts_date_query['field'] = fieldname
        fts_date_query['start'] = start
        fts_date_query['end'] = end

        es_date_query['filtered']['filter']['range'] = {fieldname: {}}

        if bool(random.getrandbits(1)):
            fts_date_query['inclusive_start'] = True
            fts_date_query['inclusive_end'] = True
            es_date_query['filtered']['filter']['range'][fieldname]['gte'] = start
            es_date_query['filtered']['filter']['range'][fieldname]['lte'] = end
        else:
            fts_date_query['inclusive_start'] = False
            fts_date_query['inclusive_end'] = False
            es_date_query['filtered']['filter']['range'][fieldname]['gt'] = start
            es_date_query['filtered']['filter']['range'][fieldname]['lt'] = end

        return fts_date_query, es_date_query

    def construct_numeric_range_query(self):
        """
        Generates a fts and es numeric range query
        """
        fts_numeric_query = {}
        es_numeric_query = self.construct_es_empty_filter_query()

        fieldname = self.get_random_value(DATASET.EMP_FIELDS['int'] +
                                          DATASET.EMP_FIELDS['float'])
        low = eval("self.get_queryable_%s" % fieldname + "()")
        high = low + random.randint(2, 10000)

        fts_numeric_query['field'] = fieldname
        fts_numeric_query['min'] = low
        fts_numeric_query['max'] = high

        es_numeric_query['filtered']['filter']['range'] = {fieldname: {}}

        if bool(random.getrandbits(1)):
            fts_numeric_query['inclusive_min'] = True
            fts_numeric_query['inclusive_max'] = True
            es_numeric_query['filtered']['filter']['range'][fieldname]['gte'] = \
                low
            es_numeric_query['filtered']['filter']['range'][fieldname]['lte'] =\
                high
        else:
            fts_numeric_query['inclusive_min'] = False
            fts_numeric_query['inclusive_max'] = False
            es_numeric_query['filtered']['filter']['range'][fieldname]['gt'] = \
                low
            es_numeric_query['filtered']['filter']['range'][fieldname]['lt'] =\
                high
        return fts_numeric_query, es_numeric_query

    def construct_es_empty_filter_query(self):
        return {'filtered': {'filter': {}}}

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
        fts_compound_query = []
        es_compound_query = []
        if bool(random.getrandbits(1)):
            fts, es = self.construct_match_query()
            fts_compound_query.append(fts)
            es_compound_query.append(es)
        if bool(random.getrandbits(1)):
            fts, es = self.construct_prefix_query()
            fts_compound_query.append(fts)
            es_compound_query.append(es)
        if bool(random.getrandbits(1)):
            fts, es = self.construct_match_phrase_query()
            fts_compound_query.append(fts)
            es_compound_query.append(es)
        if bool(random.getrandbits(1)):
            fts, es = self.construct_date_range_query()
            fts_compound_query.append(fts)
            es_compound_query.append(es)
        if bool(random.getrandbits(1)):
            fts, es = self.construct_numeric_range_query()
            fts_compound_query.append(fts)
            es_compound_query.append(es)
        return fts_compound_query, es_compound_query

    def get_random_value(self, list):
        return list[random.randint(0, len(list)-1)]

    def get_queryable_name(self, full=False):
        """
        Returns a first or last name OR
        a combination of both
        """
        if full:
            return self.return_unicode(self.get_queryable_full_name())

        if bool(random.getrandbits(1)):
            name_list = FIRST_NAMES + LAST_NAMES
            return self.return_unicode(self.get_random_value(name_list))
        else:
            return self.return_unicode(self.get_queryable_full_name())

    def return_unicode(self, text):
        try:
            text = unicode(text, 'utf-8')
            return text
        except TypeError:
            return text

    def get_queryable_full_name(self):
        return "%s %s" %(self.get_random_value(FIRST_NAMES),
                         self.get_random_value(LAST_NAMES))

    def get_queryable_dept(self):
        """
        Returns a valid dept to be queried
        """
        return self.get_random_value(DEPT)

    def get_queryable_join_date(self, now=False):
        import datetime
        if now:
            return datetime.datetime.now().isoformat()
        year = random.randint(1950, 2016)
        month = random.randint(1, 12)
        day = random.randint(1, 28)
        hour = random.randint(0, 23)
        min = random.randint(0, 59)
        return datetime.datetime(year, month, day, hour, min).isoformat()

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
        return "%s@mcdiabetes.com" % self.get_random_value(FIRST_NAMES).lower()

    def get_queryable_empid(self):
        return random.randint(10000000, 10001000)

    def get_queryable_salary(self):
        return round(random.random(), 2) *100000 + 50000

    def get_queryable_manages_team_size(self):
        return random.randint(5, 10)

    def get_queryable_mutated(self):
        return random.randint(0, 5)

    def get_queryable_manages_reports(self, full=False):
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
    #query_type=["match", "match_phrase","bool", "conjunction", "disjunction",
    # "prefix"]
    query_type = ['match_phrase']
    query_gen = FTSESQueryGenerator(2, query_type=query_type)
    for index, query in enumerate(query_gen.fts_queries):
        print json.dumps(query, ensure_ascii=False, indent=3).replace("manages_", "manages.",)
        print json.dumps(query_gen.es_queries[index], ensure_ascii=False, indent=3).\
            replace("manages_", "manages.")
        print "------------"