import random
import json
import sys
sys.path.append("/Users/apiravi/testrunner")
from emp_querables import EmployeeQuerables
from wiki_queryables import WikiQuerables

class DATASET:
    FIELDS = {'emp': {'str': ["name", "dept", "manages_reports",
                               "languages_known", "email", "_type"],
                      'num': ["empid", "mutated", "manages_team_size", "salary"],
                      'bool': ["is_manager"],
                      'date': ["join_date"],
                      'text': ["name", "manages_reports"]},
              'wiki': {'str': ["title", "revision_text_text", "_type"],
                       'text': ["title", "revision_text_text"],
                       'num': ["id", "mutated", "revision_contributor_id", "revision_contributor_id"],
                       'bool': [],
                       'date': ["revision_timestamp"]}}

class QUERY_TYPE:
    VALUES = ["match", "phrase", "bool", "match_phrase",
              "prefix", "fuzzy", "conjunction", "disjunction"
              "wildcard", "regexp",  "query_string",
              "numeric_range", "date_range", "match_all", "match_none"]


class FTSESQueryGenerator(EmployeeQuerables, WikiQuerables):

    def __init__(self, num_queries=1, query_type=None, seed=0, dataset="emp"):
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
        self.dataset = dataset
        self.fields = DATASET.FIELDS['emp']
        self.construct_fields()
        self.construct_queries()

    def construct_fields(self):
        if self.dataset == "emp":
            self.fields = DATASET.FIELDS['emp']
        elif self.dataset == "wiki":
            self.fields = DATASET.FIELDS['wiki']
        elif self.dataset == "all":
            for _, fields in DATASET.FIELDS.iteritems():
                self.fields['str'] += fields['str']
                self.fields['date'] += fields['date']
                self.fields['num'] += fields['num']
                self.fields['text'] += fields['text']
                self.fields['bool'] += fields['bool']

    def replace_underscores(self, query):
        replace_dict = {
            "manages_": "manages.",
            "revision_text_text":  "revision.text.#text",
            "revision_contributor_username": "revision.contributor.username",
            "revision_contributor_id": "revision.contributor.id",
            "revision_date": "revision.date"
        }
        query_str = json.dumps(query, ensure_ascii=False)
        for key, val in replace_dict.iteritems():
            query_str = query_str.replace(key, val)
        return json.loads(query_str, encoding='utf-8')

    def construct_queries(self):
        while self.iterator < self.queries_to_generate:
            fieldname = self.get_random_value(self.query_types)
            fts_query, es_query = eval("self.construct_%s_query" % fieldname + "()")
            if not fts_query:
                # if there are no queryable fields in a dataset for a
                # particular data type
                continue
            fts_query = self.replace_underscores(fts_query)
            es_query = self.replace_underscores(es_query)
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

            fieldname = self.get_random_value(self.fields['str'])
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
            if isinstance(must_fts_query, list):
                fts_bool_query['must'] = {"conjuncts": must_fts_query}
            else:
                fts_bool_query['must'] = {"conjuncts": [must_fts_query]}
            es_bool_query['bool']['must'] = must_es_query

        if bool(random.getrandbits(1)):
            must_not_fts, must_not_es = self.construct_match_query(
                ret_list=bool(random.getrandbits(1)))
            if isinstance(must_not_fts, list):
                fts_bool_query['must_not'] = {"disjuncts": must_not_fts}
            else:
                fts_bool_query['must_not'] = {"disjuncts": [must_not_fts]}
            es_bool_query['bool']['must_not'] = must_not_es

        should_fts, should_es = self.construct_match_query(
            ret_list=bool(random.getrandbits(1)))
        if isinstance(should_fts, list):
            fts_bool_query['should'] = {"disjuncts": should_fts}
        else:
            fts_bool_query['should'] = {"disjuncts": [should_fts]}
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
        fieldname = self.get_random_value(self.fields['text'])
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

        fieldname = self.get_random_value(self.fields['date'])
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

        fieldname = self.get_random_value(self.fields['num'])
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

    def construct_match_all_query(self):
        query = {'match_all': {}}
        return query, query

    def construct_compound_query(self):
        """
        This is used to consolidate more than one type of query
        say - return a list of match, phrase, match-phrase queries
        * to be enclosed by 'conjuncts' or 'disjuncts' query
        """
        fts_compound_query = []
        es_compound_query = []
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

    def get_queryable__type(self):
        doc_types = DATASET.FIELDS.keys()
        return self.get_random_value(doc_types)

if __name__ == "__main__":
    #query_type=["match", "match_phrase","bool", "conjunction", "disjunction",
    # "prefix"]
    query_type = ['match_phrase', 'match', 'date_range', 'numeric_range', 'bool', 'conjunction', 'disjunction', 'prefix']
    query_gen = FTSESQueryGenerator(100, query_type=query_type, dataset='wiki')
    for index, query in enumerate(query_gen.fts_queries):
        print json.dumps(query, ensure_ascii=False, indent=3)
        print json.dumps(query_gen.es_queries[index], ensure_ascii=False, indent=3)
        print "------------"