import random
import json
#import sys
#sys.path.append("/Users/apiravi/testrunner")
from .emp_querables import EmployeeQuerables
from .wiki_queryables import WikiQuerables
import Geohash

class DATASET:
    FIELDS = {'emp': {'str': ["name", "dept", "manages_reports",
                               "languages_known", "email", "type"],
                      'text': ["name", "manages_reports"],
                      'num': ["mutated", "manages_team_size", "salary"],
                      'bool': ["is_manager"],
                      'date': ["join_date"]
                      },

              'wiki': {'str': ["title", "revision_text_text", "type", "revision_contributor_username"],
                       'text': ["title", "revision_text_text"],
                       'num': ["mutated"],
                       'bool': [],
                       'date': ["revision_timestamp"]}
              }

class QUERY_TYPE:
    VALUES = ["match", "bool", "match_phrase",
              "prefix", "fuzzy", "conjunction", "disjunction"
              "wildcard", "regexp",  "query_string",
              "numeric_range", "date_range", "term_range",
              "match_all", "match_none"]

    # to know what type of queries to generate for fields
    # returned by custom map_generator (only for custom map indexes)
    CUSTOM_QUERY_TYPES = {
        'text': ["match", "bool", "match_phrase",
                 "prefix", "wildcard", "query_string",
                 "conjunction", "disjunction", "term_range"],
        'str': ["match", "bool", "match_phrase",
                 "prefix", "wildcard", "query_string",
                 "conjunction", "disjunction", "term_range"],
        'num': ["numeric_range"],
        'date': ["date_range"]
    }

class FTSESQueryGenerator(EmployeeQuerables, WikiQuerables):

    def __init__(self, num_queries=1, query_type=None, seed=0, dataset="emp",
                 fields=None):
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
        self.smart_queries = False
        if fields:
            # Smart query generation
            self.fields = {}
            self.make_fields_compatible(fields)
            self.query_types = self.get_custom_query_types()
            self.smart_queries = True
        else:
            self.fields = self.construct_fields()
            self.query_types = query_type
        if self.query_types:
            self.construct_queries()
        else:
            print("No string/number/date fields indexed for smart" \
                  " query generation ")


    def construct_fields(self):
        all_fields = {}
        if self.dataset == "emp":
            all_fields = DATASET.FIELDS['emp']
        elif self.dataset == "wiki":
            all_fields = DATASET.FIELDS['wiki']
        elif self.dataset == "all":
            fields_set = set()
            for _, fields in DATASET.FIELDS.items():
                fields_set |= set(fields.keys())
            for v in fields_set:
                all_fields[v] = []
            for _, fields in DATASET.FIELDS.items():
                all_fields['str'] += fields['str']
                all_fields['date'] += fields['date']
                all_fields['num'] += fields['num']
                all_fields['text'] += fields['text']
                all_fields['bool'] += fields['bool']
        return all_fields

    def make_fields_compatible(self, fields):
        """
        Passed field types could be specified as  "num"/"number"/"integer".
        Standardize it to work with RQG
        """
        for field_type, field_list in fields.items():
            if field_type == "str" or field_type == "text":
                self.fields["str"] = field_list
                self.fields["text"] = field_list
            if field_type == "number" or field_type == "integer":
                self.fields["num"] = field_list
            if field_type == "datetime":
                self.fields["date"] = field_list
        print("Smart queries will be generated on fields: %s" % self.fields)

    def get_custom_query_types(self):
        query_types = []
        for field_type in list(self.fields.keys()):
            query_types += QUERY_TYPE.CUSTOM_QUERY_TYPES[field_type]
        return list(set(query_types))

    def replace_underscores(self, query):
        replace_dict = {
            "manages_": "manages.",
            "revision_text_text":  "revision.text.#text",
            "revision_contributor_username": "revision.contributor.username",
            "revision_contributor_id": "revision.contributor.id",
            "revision_date": "revision.date"
        }
        query_str = json.dumps(query, ensure_ascii=False)
        for key, val in replace_dict.items():
            query_str = query_str.replace(key, val)
        return json.loads(query_str, encoding='utf-8')

    def construct_queries(self):
        while self.iterator < self.queries_to_generate:
            fieldname = self.get_random_value(self.query_types)
            fts_query, es_query = eval("self.construct_%s_query()" % fieldname)
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
            match_str = eval("self.get_queryable_%s()" % fieldname)

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
        if fieldname == "name":
            match_str = eval("self.get_queryable_%s" % fieldname + "(full=True)")
        else:
            match_str = eval("self.get_queryable_%s()" % fieldname)
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
        Generates an fts and es numeric range query
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

    def get_term(self, fieldname=None):
        """
        Returns a queryable term for a given field
        :param fieldname: the field we get a term for
        :return:
        """
        if not fieldname:
            fieldname = self.get_random_value(self.fields['str'] +
                                          self.fields['text'])
        str = eval("self.get_queryable_%s" % fieldname + "()")
        terms = str.split(' ')
        return terms[0]

    def construct_term_range_query(self):
        """
        Generates an fts and es term range query
        """
        fts_term_range_query = {}
        es_term_range_query = self.construct_es_empty_filter_query()

        fieldname = self.get_random_value(self.fields['str'] +
                                          self.fields['text'])
        str1 = self.get_term(fieldname)
        str2 = self.get_term(fieldname)

        fts_term_range_query['field'] = fieldname
        fts_term_range_query['min'] = str1
        fts_term_range_query['max'] = str2

        es_term_range_query['filtered']['filter']['range'] = {fieldname: {}}

        if bool(random.getrandbits(1)):
            fts_term_range_query['inclusive_min'] = True
            fts_term_range_query['inclusive_max'] = True
            es_term_range_query['filtered']['filter']['range'][fieldname]['gte'] = \
                str1
            es_term_range_query['filtered']['filter']['range'][fieldname]['lte'] = \
                str2
        else:
            fts_term_range_query['inclusive_min'] = False
            fts_term_range_query['inclusive_max'] = False
            es_term_range_query['filtered']['filter']['range'][fieldname]['gt'] = \
                str1
            es_term_range_query['filtered']['filter']['range'][fieldname]['lt'] = \
                str2
        return fts_term_range_query, es_term_range_query

    def construct_es_empty_filter_query(self):
        return {'filtered': {'filter': {}}}

    def construct_terms_query_string_query(self):
        """
        Generates disjunction, boolean query string queries
        """

        if bool(random.getrandbits(1)):
            # text/str terms
            fieldname = self.get_random_value(self.fields['str'] +
                                              self.fields['text'])
            match_str = eval("self.get_queryable_%s" % fieldname + "()")
            if ':' or ' ' in match_str:
                match_str = '\"' + match_str + '\"'
            if bool(random.getrandbits(1)) and not self.smart_queries:
                return match_str
            else:
                return fieldname+':'+ match_str
        else:
            # numeric range
            operators = ['>', '>=', '<', '<=']
            fieldname = self.get_random_value(self.fields['num'])
            val = eval("self.get_queryable_%s" % fieldname + "()")
            if bool(random.getrandbits(1)):
                # upper or lower bound specified
                end_point = fieldname + ':'+ \
                            self.get_random_value(operators) + str(val)
                return end_point
            else:
                # both upper and lower bounds specified
                # +age:>=10 +age:<20
                high_val = val + random.randint(2, 10000)
                range_str = fieldname + ':'+ \
                             self.get_random_value(operators[:1]) + str(val) +\
                            ' +' +fieldname + ':'+ \
                             self.get_random_value(operators[2:]) +\
                             str(high_val)
                return range_str

    def construct_query_string_query(self):
        """
        Returns an fts and es query string query
        See: http://www.blevesearch.com/docs/Query-String-Query/
        """
        fts_query = {'query': ""}
        es_query = {"query_string": {'query': ""}}
        connectors = [' ', ' +', ' -']
        match_str = ""

        try:
            # search term
            term = self.construct_terms_query_string_query()
            connector = self.get_random_value(connectors)
            match_str += connector + term

            if bool(random.getrandbits(1)):
                # another term
                term = self.construct_terms_query_string_query()
                connector = self.get_random_value(connectors)
                match_str += connector + term

                # another term
                term = self.construct_terms_query_string_query()
                connector = self.get_random_value(connectors)
                match_str += connector + term

            fts_query['query'] = match_str.lstrip()
            es_query['query_string']['query'] = match_str.lstrip()

            return fts_query, es_query
        except KeyError:
            # if there are no sufficient num or str/text fields passed
            return {}, {}


    def construct_wildcard_query(self):
        """
        Wildcards supported:
         * - any char sequence (even empty)
         ? - any single char
        Sample FTS query:
        {
            "field":  "user",
            "wildcard": "ki*y"
        }
        """
        fts_query = {}
        es_query = {"wildcard": {}}
        fieldname = self.get_random_value(self.fields['str'] +
                                              self.fields['text'])
        match_str = eval("self.get_queryable_%s" % fieldname + "()")
        if bool(random.getrandbits(1)):
            # '*' query
            pos = random.randint(0, len(match_str)-1)
            match_str = match_str[:pos] + '*'
        else:
            # '?' query
            pos = random.randint(0, len(match_str)-1)
            match_str = match_str.replace(match_str[pos], '?')

        fts_query['field'] = fieldname
        fts_query['wildcard'] = match_str

        es_query['wildcard'][fieldname] = match_str

        return fts_query, es_query

    def construct_regexp_query(self):
        """
        All regexp queries are not generated but defined by the dataset
        queryables
        """
        fts_query = {}
        es_query = {'regexp': {}}
        fieldname = self.get_random_value(self.fields['text'])
        match_str = eval("self.get_queryable_regex_%s" % fieldname + "()")
        fts_query['field'] = fieldname
        fts_query['regexp'] = match_str

        es_query['regexp'][fieldname] = match_str

        return fts_query, es_query

    def construct_fuzzy_query(self):
        """
        fuzziness: edit distance (0: exact search to 1: fuzziness,
        2: more fuzziness and so on
        In FTS, fuzzy queries are performed on match and term queries
        """
        fts_query = {}
        es_query = {'fuzzy': {}}
        fuzziness = random.randint(0, 2)
        fieldname = self.get_random_value(self.fields['str'] +
                                          self.fields['text'])
        match_str = eval("self.get_queryable_%s" % fieldname + "()")
        if bool(random.getrandbits(1)):
            match_str = match_str[1:]
        else:
            match_str = match_str[:len(match_str)-2]

        fts_query['field'] = fieldname
        fts_query['match'] = match_str
        fts_query['fuzziness'] = fuzziness

        es_query['fuzzy'][fieldname] = {}
        es_query['fuzzy'][fieldname]['value'] = match_str
        es_query['fuzzy'][fieldname]['fuzziness'] = fuzziness

        return fts_query, es_query

    @staticmethod
    def construct_geo_location_query(lon=None, lat=None,
                                     distance=None, dist_unit=None):
        """
        Returns a geo location query for Couchbase and Elastic search
        """
        from lib.couchbase_helper.data import LON_LAT
        if not lon:
            lon_lat = random.choice(LON_LAT)
            lon = lon_lat[0]
            lat = lon_lat[1]
            distance = random.choice([10, 100, 500, 1000, 10000])
            dist_unit = random.choice(["km", "mi"])

        fts_query = {
            "location": {
                "lon": lon,
                "lat": lat
            },
            "distance": str(distance)+dist_unit,
            "field": "geo"
        }

        es_query= {
            "query": {
                "match_all" : {}
            },
            "filter" : {
                "geo_distance" : {
                     "distance" : str(distance)+dist_unit,
                     "geo" : {
                         "lat" : lat,
                         "lon" : lon
                     }
                }
            }
        }

        case = random.randint(0, 3)

        # Geo Location as array
        if case == 1:
            fts_query['location'] = [lon, lat]
            es_query['filter']['geo_distance']['geo'] = [lon, lat]

        # Geo Location as string
        if case == 2:
            fts_query['location'] = "{0},{1}".format(lat, lon)
            es_query['filter']['geo_distance']['geo'] = "{0},{1}".format(lat, lon)

        # Geo Location as Geohash
        if case == 3:
            geohash = Geohash.encode(lat, lon, precision=random.randint(3, 8))
            fts_query['location'] = geohash
            es_query['filter']['geo_distance']['geo'] = geohash

        # Geo Location as an object of lat and lon if case == 0
        return fts_query, es_query

    @staticmethod
    def construct_geo_bounding_box_query(lon1=None, lat1=None,
                                     lon2=None, lat2=None):
        """
        Returns a geo bounding box query for Couchbase and Elastic search
        """
        from lib.couchbase_helper.data import LON_LAT
        if not lon1:
            lon_lat1 = random.choice(LON_LAT)
            lon_lat2 = random.choice(LON_LAT)
            lon1 = lon_lat1[0]
            lat1 = lon_lat1[1]
            lon2 = lon_lat2[0]
            lat2 = lon_lat2[1]

        fts_query = {
            "top_left": {
                "lon": lon1,
                "lat": lat1
            },
            "bottom_right": {
                "lon": lon2,
                "lat": lat2
            },
            "field": "geo"
        }

        es_query = {
            "query" : {
                "match_all" : {}
            },
            "filter" : {
                "geo_bounding_box" : {
                    "geo" : {
                        "top_left" : {
                            "lat" : lat1,
                            "lon" : lon1
                        },
                        "bottom_right" : {
                            "lat" : lat2,
                            "lon" : lon2
                        }
                    }
                }
            }
        }


        if bool(random.getrandbits(1)):
            fts_query['top_left'] = [lon1, lat1]
            fts_query['bottom_right'] = [lon2, lat2]
            es_query['filter']['geo_bounding_box']['geo']['top_left'] = \
                [lon1, lat1]
            es_query['filter']['geo_bounding_box']['geo']['bottom_right'] = \
                [lon2, lat2]

        return fts_query, es_query

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
        if bool(random.getrandbits(1))and 'date_range' in self.query_types:
            fts, es = self.construct_date_range_query()
            fts_compound_query.append(fts)
            es_compound_query.append(es)
        if bool(random.getrandbits(1)) and 'numeric_range' in self.query_types:
            fts, es = self.construct_numeric_range_query()
            fts_compound_query.append(fts)
            es_compound_query.append(es)
        return fts_compound_query, es_compound_query

    def get_queryable_type(self):
        doc_types = list(DATASET.FIELDS.keys())
        return self.get_random_value(doc_types)

if __name__ == "__main__":
    #query_type=['match_phrase', 'match', 'date_range', 'numeric_range', 'bool',
    #              'conjunction', 'disjunction', 'prefix']
    query_type = ['term_range']
    query_gen = FTSESQueryGenerator(100, query_type=query_type, dataset='all')
    for index, query in enumerate(query_gen.fts_queries):
        print(json.dumps(query, ensure_ascii=False, indent=3))
        print(json.dumps(query_gen.es_queries[index], ensure_ascii=False, indent=3))
        print("------------")