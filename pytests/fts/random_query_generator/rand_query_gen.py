import random
import json
# import sys
# sys.path.append("/Users/apiravi/testrunner")
from .emp_querables import EmployeeQuerables
from .wiki_queryables import WikiQuerables
import Geohash
import math
import random
import copy

from itertools import (accumulate,
                       chain,
                       repeat)

from ground.base import get_context
from hypothesis_geometry.core.factories import (to_convex_vertices_sequence,
                                                to_multicontour,
                                                to_polygon)

context = get_context()
Point = context.point_cls

class DATASET:
    FIELDS = {'emp': {'str': ["name", "dept", "manages_reports",
                              "languages_known", "email", "type"],
                      'text': ["name", "manages_reports"],
                      'num': ["mutated", "manages_team_size", "salary"],
                      'bool': ["is_manager"],
                      'date': ["join_date"],
                      'array': ["languages_known", "manages_reports"],
                      'vector': ["l_vector"]
                      },

              'wiki': {'str': ["title", "revision_text_text", "type", "revision_contributor_username"],
                       'text': ["title", "revision_text_text"],
                       'num': ["mutated"],
                       'bool': [],
                       'date': ["revision_timestamp"],
                       'array': []}
              }
    CONSOLIDATED_FIELDS = ["name", "dept", "manages_reports", "languages_known", "email", "mutated",
                           "manages_team_size", "salary", "is_manager", "join_date", "title", "revision_text_text",
                           "revision_contributor_username", "mutated", "revision_timestamp", "l_vector"]


class QUERY_TYPE:
    VALUES = ["match", "bool", "match_phrase",
              "prefix", "fuzzy", "conjunction", "disjunction"
                                                "wildcard", "regexp", "query_string",
              "numeric_range", "date_range", "term_range",
              "match_all", "match_none", "vector"]

    # to know what type of queries to generate for fields
    # returned by custom map_generator (only for custom map indexes)
    CUSTOM_QUERY_TYPES = {
        'text': ["match", "bool", "match_phrase",
                 "prefix", "wildcard", "query_string",
                 "conjunction", "disjunction", "term_range"],
        'str': ["match", "bool", "match_phrase",
                "prefix", "wildcard", "query_string",
                "conjunction", "disjunction", "term_range"],
        'bool': [],
        'num': ["numeric_range"],
        'date': ["date_range"],
        "vector": ["vector_type"]
    }

    N1QL_QUERY_TYPES = {
        'text': ["term_equal", "term_range",
                 "term_like", "term_between", "conjunction_disjunction"],
        'str': ["term_equal", "term_range",
                "term_between", "term_like", "conjunction_disjunction"],
        'num': ["num_equal", "num_range", "num_between"],
        'date': ["date_equal", "date_range", "date_between"],
        'bool': ["boolean"],
        'array': ["array_any"]
    }


class FTSESQueryGenerator(EmployeeQuerables, WikiQuerables):

    def __init__(self, num_queries=1, query_type=None, seed=0, dataset="emp",
                 fields=None,doc_map_count=1):
        """
        FTS(Bleve) and equivalent ES(Lucene) query generator for employee dataset
        (JsonDocGenerator in couchbase_helper/documentgenerator.py)

        """
        random.seed(seed)
        self.queries_to_generate = num_queries
        self.iterator = 0
        self.fts_queries = []
        self.vector_queries = []
        self.es_queries = []
        self.query_types = query_type
        self.dataset = dataset
        self.smart_queries = False
        self.doc_map_count = doc_map_count
        if fields and query_type == ['N1QL_MATCH_PHRASE']:
            self.fields = {}
            add_only_match = False
            self.make_fields_compatible(fields)
            if 'text' in self.fields.keys():
                temp_text = self.fields['text']
                temp_str = self.fields['str']
                del self.fields['text']
                del self.fields['str']
                add_only_match = True
            self.query_types = self.get_custom_query_types()
            if add_only_match:
                self.fields['str'] = temp_str
                self.fields['text'] = temp_text
                self.query_types.append("match")
                self.query_types.append("match_phrase")
                print("query_types: %s" % self.query_types)
            self.smart_queries = True
        elif fields:
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
            all_fields = copy.deepcopy(DATASET.FIELDS['emp'])
        elif self.dataset == "wiki":
            all_fields = copy.deepcopy(DATASET.FIELDS['wiki'])
        elif self.dataset == "all" or self.dataset == "default":
            fields_set = set()
            for _, fields in DATASET.FIELDS.items():
                fields_set |= set(fields.keys())
            for v in fields_set:
                all_fields[v] = []
            all_dataset = copy.deepcopy(DATASET.FIELDS)
            for _, fields in all_dataset.items():
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
            if field_type == "boolean":
                self.fields["bool"] = field_list
            if field_type == "vector":
                self.fields["vector"] = field_list
        print("Smart queries will be generated on fields: %s" % self.fields)

    def get_custom_query_types(self):
        query_types = []
        for field_type in list(self.fields.keys()):
            query_types += QUERY_TYPE.CUSTOM_QUERY_TYPES[field_type]
        return list(set(query_types))

    def replace_underscores(self, query):
        replace_dict = {
            "manages_": "manages.",
            "revision_text_text": "revision.text.`#text`",
            "revision_contributor_username": "revision.contributor.username",
            "revision_contributor_id": "revision.contributor.id",
            "revision_date": "revision.date",
            "revision_timestamp": "revision.timestamp"
        }
        query_str = json.dumps(query, ensure_ascii=False)
        for key, val in replace_dict.items():
            query_str = query_str.replace(key, val)
        return json.loads(query_str)

    def inject_type_filter(self, query_dict, type_value):
        if "bool" in query_dict:
            filters = query_dict["bool"].get("filter", [])
            
            # If filter is a dict, convert it into a list
            if isinstance(filters, dict):
                filters = [filters]
            
            # If filter is missing, start with an empty list
            if not isinstance(filters, list):
                filters = []

            # Add the type filter
            filters.append({"term": {"type": type_value}})
            
            # Update back the filters
            query_dict["bool"]["filter"] = filters

        else:
            # If not a bool query, wrap it
            original_query = query_dict.copy()
            query_dict.clear()
            query_dict["bool"] = {
                "must": original_query,
                "filter": [
                    {"term": {"type": type_value}}
                ]
            }

        return query_dict


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
            if fieldname == "vector_type":
                self.vector_queries.append(fts_query)
            self.iterator += 1

    def construct_vector_type_query(self):
        vector_query = {}
        fieldname = self.get_random_value(self.fields['vector'])
        vector_to_query = eval("self.get_queryable_%s()" % fieldname)

        vector_query["field"] = fieldname
        vector_query["vector"] = vector_to_query
        vector_query["k"] = random.randint(2, 50)
        return vector_query, None



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

            field_key = 'match'

            es_match_query = {field_key: {}}
            es_match_query[field_key][fieldname] = match_str

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
        es_disj_query['bool']['minimum_should_match'] = 1
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
        es_prefix_query = {'prefix': {}}
        fts_match_query, _ = self.construct_match_query()
        prefix_search = fts_match_query["match"][:random.randint(1, 4)]
        fts_prefix_query["prefix"] = prefix_search
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

        es_date_query['bool']['filter']['range'] = {fieldname: {}}

        if bool(random.getrandbits(1)):
            fts_date_query['inclusive_start'] = True
            fts_date_query['inclusive_end'] = True
            es_date_query['bool']['filter']['range'][fieldname]['gte'] = start
            es_date_query['bool']['filter']['range'][fieldname]['lte'] = end
        else:
            fts_date_query['inclusive_start'] = False
            fts_date_query['inclusive_end'] = False
            es_date_query['bool']['filter']['range'][fieldname]['gt'] = start
            es_date_query['bool']['filter']['range'][fieldname]['lt'] = end

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

        es_numeric_query['bool']['filter']['range'] = {fieldname: {}}

        if bool(random.getrandbits(1)):
            fts_numeric_query['inclusive_min'] = True
            fts_numeric_query['inclusive_max'] = True
            es_numeric_query['bool']['filter']['range'][fieldname]['gte'] = \
                low
            es_numeric_query['bool']['filter']['range'][fieldname]['lte'] = \
                high
        else:
            fts_numeric_query['inclusive_min'] = False
            fts_numeric_query['inclusive_max'] = False
            es_numeric_query['bool']['filter']['range'][fieldname]['gt'] = \
                low
            es_numeric_query['bool']['filter']['range'][fieldname]['lt'] = \
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

        es_term_range_query['bool']['filter']['range'] = {fieldname: {}}

        if bool(random.getrandbits(1)):
            fts_term_range_query['inclusive_min'] = True
            fts_term_range_query['inclusive_max'] = True
            es_term_range_query['bool']['filter']['range'][fieldname]['gte'] = \
                str1
            es_term_range_query['bool']['filter']['range'][fieldname]['lte'] = \
                str2
        else:
            fts_term_range_query['inclusive_min'] = False
            fts_term_range_query['inclusive_max'] = False
            es_term_range_query['bool']['filter']['range'][fieldname]['gt'] = \
                str1
            es_term_range_query['bool']['filter']['range'][fieldname]['lt'] = \
                str2
        return fts_term_range_query, es_term_range_query

    def construct_es_empty_filter_query(self):
        return {'bool': {'filter': {}}}

    def construct_terms_query_string_query(self):
        """
        Generates disjunction, boolean query string queries.
        Returns a tuple: (with_keyword_suffix, without_keyword_suffix)
        """

        if bool(random.getrandbits(1)):
            # text/str terms
            fieldname = self.get_random_value(self.fields['str'] + self.fields['text'])
            match_str = eval(f"self.get_queryable_{fieldname}()")
            if ':' in match_str or ' ' in match_str:
                match_str = f'"{match_str}"'

            term_string_query = f"{fieldname}:{match_str}"

            if bool(random.getrandbits(1)) and not self.smart_queries:
                # Return just the match string (used in some fuzzy or free-text modes)
                return match_str
            else:
                return term_string_query

        else:
            # numeric range
            operators = ['>', '>=', '<', '<=']
            fieldname = self.get_random_value(self.fields['num'])
            val = eval(f"self.get_queryable_{fieldname}()")

            if bool(random.getrandbits(1)):
                # single range condition
                query = f"{fieldname}:{self.get_random_value(operators)}{val}"
                return query
            else:
                # compound range condition (e.g. +age:>=10 +age:<20)
                high_val = val + random.randint(2, 10000)
                lower = f"{fieldname}:{self.get_random_value(operators[:1])}{val}"
                upper = f"+{fieldname}:{self.get_random_value(operators[2:])}{high_val}"
                query = f"{lower} {upper}"
                return query


    def construct_query_string_query(self):
        """
        Returns an fts and es query string query
        See: http://www.blevesearch.com/docs/Query-String-Query/
        """
        fts_query = {'query': ""}
        es_query = {"query_string": {'query': ""}}
        connectors = [' ', ' +', ' -']

        try:
            # search term
            term = self.construct_terms_query_string_query()

            connector = self.get_random_value(connectors)
            
            match_str = connector + term

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
            pos = random.randint(0, len(match_str) - 1)
            match_str = match_str[:pos] + '*'
        else:
            # '?' query
            pos = random.randint(0, len(match_str) - 1)
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
            match_str = match_str[:len(match_str) - 2]

        fts_query['field'] = fieldname
        fts_query['match'] = match_str
        fts_query['fuzziness'] = fuzziness

        es_query['fuzzy'][fieldname] = {}
        es_query['fuzzy'][fieldname]['value'] = match_str
        es_query['fuzzy'][fieldname]['fuzziness'] = fuzziness

        return fts_query, es_query

    def generate_random_point():
        lat = -90 + random.random()*180
        long = -90 + random.random()*180
        return lat,long

    def random_polygon(vertices_count: int):
        return FTSESQueryGenerator._random_polygon(FTSESQueryGenerator.unique_random_points(vertices_count),
         vertices_count)

    def update_shapes_with_polygon():
        v = random.randint(4,10)
        p = FTSESQueryGenerator.random_polygon(v)
        coord = []
        temp = []
        for i in range(len(p.border.vertices)):
            temp.append([p.border.vertices[i].x,p.border.vertices[i].y])
        temp.append(temp[0])
        coord.append(temp)
        temp = []
        for i in range(len(p.holes)):
            for j in range(len(p.holes[i].vertices)): 
                hole = p.holes[i].vertices[j]
                temp.append([hole.x,hole.y])
        if len(temp) > 0:
            temp.append(temp[0])
            coord.append(temp)
        return coord

    def random_point(x_min=0, x_max=90,y_min=0, y_max=60):
        return Point(random.uniform(x_min, x_max), random.uniform(y_min, y_max))

    def unique_random_points(count: int):
        x_min = random.randint(0,58)
        x_max = x_min + random.random()/4
        y_min = random.randint(0,88)
        y_max = y_min + random.random()/4 
        result = {FTSESQueryGenerator.random_point(x_min = x_min,x_max = x_max,y_min = y_min,y_max = y_max) for _ in repeat(None, count)}
        while len(result) < count:
            result.add(FTSESQueryGenerator.random_point())
        return list(result)

    def _random_polygon(points, vertices_count):
        border_size = random.randint(len(context.points_convex_hull(points)),
                                 vertices_count)
        holes_vertices_count = vertices_count - border_size
        holes_count = random.randint(0, holes_vertices_count // 3)
        extra_holes_vertices_count = holes_vertices_count - holes_count * 3
        holes_sizes = []
        for _ in repeat(None, holes_count):
            hole_size = random.randint(3, extra_holes_vertices_count + 3)
            extra_holes_vertices_count -= hole_size - 3
            holes_sizes.append(hole_size)
        return to_polygon(points, border_size, holes_sizes, random.choice, context)

    def generate_random_polygon():
        coord = FTSESQueryGenerator.update_shapes_with_polygon()
        return coord

    def generate_random_linestring():
        coord = []
        n = random.randint(2,10)
        for v in range(1,n+1):
            lat1,lon1 = FTSESQueryGenerator.generate_random_point() 
            coord.append([lat1,lon1])
        return coord

    def generate_random_multipoint():
        coord = []
        n = random.randint(4,10)
        for v in range(1,n+1):
                lat,lon = FTSESQueryGenerator.generate_random_point() 
                coord.append([lat,lon])
        return coord
    
    def generate_random_multilinestring():
        coord = []
        n = random.randint(4,10)
        for v in range(1,n+1):
                line = FTSESQueryGenerator.generate_random_linestring() 
                coord.append(line) 
        return coord

    def generate_random_multipolygon():
         coord = []
         n = random.randint(4,10)
         for v in range(1,n+1):
             coord.append(FTSESQueryGenerator.generate_random_polygon())
         return coord

    def generate_random_envelope():
        lat1 = -80 + random.random()*160
        lon1 = -80 + random.random()*160
        diff = random.random() 
        lat2 = lat1 + diff
        lon2 = lon1 - diff
        coord = [[lat1,lon1],[lat2,lon2]]
        return coord

    def generate_random_circle():
        lat,lon = FTSESQueryGenerator.generate_random_point() 
        coord = [lat,lon]
        distance_units = ["m","km","mi"] 
        radius = str(random.randint(5,100)) + random.choice(distance_units)
        return coord,radius

    def construct_geo_shape_queries_helper(shape,relation,compare_es,fts_queries,es_queries,\
        relation_query_count):
        for relation,count in relation_query_count.items():
            for i in range(0,count):
                fts_query,es_query = FTSESQueryGenerator.construct_geo_shape_query(shape=shape,relation=relation)
                fts_queries.append(fts_query)
                if compare_es:
                    es_queries.append(es_query)
    
    @staticmethod
    def construct_geo_shape_queries(shape="",num_queries=9,compare_es=True):
       num_queries_per_shape = num_queries
       if shape == "":
            shapes = ['point','linestring','polygon','multipoint','multilinestring','multipolygon', 'circle',
        'envelope','geometrycollection']
            shape = random.choice(shapes)
            num_queries_per_shape = num_queries_per_shape//len(shapes)

       es_queries = []
       fts_queries = []

       relations = ['intersects','within','contains']
       if shape == 'linestring' or shape == 'multilinestring':
            relations = ['intersects','contains']
            
       # relation-query count map
       relation_query_count = dict()
       query_count = 0
       for relation in relations:
            if relation not in relation_query_count.keys():
                    relation_query_count[relation] = 0
            relation_query_count[relation] += 1
            query_count+=1
        
       while query_count < num_queries_per_shape:
            relation = random.choice(relations)
            relation_query_count[relation] += 1
            query_count += 1

       FTSESQueryGenerator.construct_geo_shape_queries_helper(shape=shape,relation=relation,
            compare_es=compare_es,fts_queries=fts_queries,es_queries=es_queries,\
            relation_query_count=relation_query_count) 

       return fts_queries,es_queries

    @staticmethod
    def construct_geo_shape_query(shape="point",relation = "intersects"):    
        fts_query = dict()
        es_query = dict()
        coord = []
        coll_shapes = []
        shapes = ['point','linestring','polygon','multipoint','multilinestring','multipolygon','circle',
        'envelope','geometrycollection']

        if shape == 'point':
            lat,lon = FTSESQueryGenerator.generate_random_point() 
            coord = [lat,lon]
        
        if shape == 'multipoint':
            coord = FTSESQueryGenerator.generate_random_multipoint()

        elif shape == 'linestring':
            coord = FTSESQueryGenerator.generate_random_linestring()

        elif shape == 'multilinestring':
            coord = FTSESQueryGenerator.generate_random_multilinestring()

        elif shape == 'polygon':
           coord = FTSESQueryGenerator.generate_random_polygon()

        elif shape == 'multipolygon':
           coord = FTSESQueryGenerator.generate_random_multipolygon() 

        elif shape == "envelope":
           coord = FTSESQueryGenerator.generate_random_envelope() 

        elif shape == "circle":
            coord,radius = FTSESQueryGenerator.generate_random_circle() 

        elif shape == "geometrycollection":
            contains_linestring = False
            n = random.randint(1,5) 
            for i in range(n):
                coll_shape = random.choice(shapes[:-1])
                temp = dict()
                temp['type'] = coll_shape
                if (coll_shape == "linestring" or coll_shape == "multilinestring"):
                   contains_linestring = True
                   if relation == "within":
                        relation = random.choice(["intersects","contains"])
                if coll_shape == "point":
                    lat,lon = FTSESQueryGenerator.generate_random_point() 
                    point_coord = [lat,lon]
                    temp['coordinates'] = point_coord
                if coll_shape == "multipoint":
                    temp['coordinates'] = FTSESQueryGenerator.generate_random_multipoint()
                if coll_shape == "linestring":
                    temp['coordinates'] = FTSESQueryGenerator.generate_random_linestring()
                if coll_shape == "multilinestring":
                    temp['coordinates'] = FTSESQueryGenerator.generate_random_multilinestring()
                if coll_shape == "polygon":
                    temp['coordinates'] = FTSESQueryGenerator.generate_random_polygon()
                if coll_shape == "multipolygon":
                    temp['coordinates'] = FTSESQueryGenerator.generate_random_multipolygon()
                if coll_shape == "circle":
                    temp['coordinates'],temp['radius'] = FTSESQueryGenerator.generate_random_circle()
                if coll_shape == "envelope":
                    temp['coordinates'] = FTSESQueryGenerator.generate_random_envelope()
                coll_shapes.append(temp)

        shape_dict = dict()
        shape_dict = {
                    "type": shape
        }
        if shape != "geometrycollection":
                shape_dict["coordinates"] = coord
        else:
                shape_dict["geometries"] = coll_shapes
        if shape == "circle":
                shape_dict["radius"] = radius

        fts_query =  {'field': 'location','geometry': {'shape': shape_dict,'relation': relation}}

        es_query = {
            "from": 0,
            "size": 10000,
            "query": {
                "bool": {
                    "must": [ 
                        {"match_all": {}}
                    ],
                    "filter": [ 
                        {
                            "geo_shape": {
                                "location": {
                                    "shape": shape_dict,
                                    "relation": relation
                                }
                            }
                        }
                    ]
                }
            }
        }

        return fts_query,es_query

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
            "distance": str(distance) + dist_unit,
            "field": "geo"
        }

        es_query = {
            "query":{
                "bool":{
                    "filter":{
                        "geo_distance":{
                            "distance": str(distance) + dist_unit,
                            "geo":{
                                "lat": lat,
                                "lon": lon
                            },
                            "distance_type": "arc"
                        }
                    }
                }
            }
        }

        case = random.randint(0, 3)

        # Geo Location as array
        if case == 1:
            fts_query['location'] = [lon, lat]
            es_query['query']['bool']['filter']['geo_distance']['geo'] = [lon, lat]

        # Geo Location as string
        if case == 2:
            fts_query['location'] = "{0},{1}".format(lat, lon)
            es_query['query']['bool']['filter']['geo_distance']['geo'] = "{0},{1}".format(lat, lon)

        # Geo Location as Geohash
        if case == 3:
            geohash = Geohash.encode(lat, lon, precision=random.randint(3, 8))
            fts_query['location'] = geohash
            es_query['query']['bool']['filter']['geo_distance']['geo'] = geohash

        # Geo Location as an object of lat and lon if case == 0
        return fts_query, es_query

    @staticmethod
    def generate_polygon(longitude, latitude, ave_radius, irregularity, spikeyness, num_vertices):
        """Start with the centre of the polygon at latitude, longitude,
            then creates the polygon by sampling points on a circle around the centre.
            Random noise is added by varying the angular spacing between sequential points,
            and by varying the radial distance of each point from the centre.

            Params:
            latitude, longitude - coordinates of the "centre" of the polygon
            ave_radius - in px, the average radius of this polygon, this roughly controls how large the polygon is, really only useful for order of magnitude.
            irregularity - [0,1] indicating how much variance there is in the angular spacing of vertices. [0,1] will map to [0, 2pi/numberOfVerts]
            spikeyness - [0,1] indicating how much variance there is in each vertex from the circle of radius ave_radius. [0,1] will map to [0, ave_radius]
            num_vertices - self-explanatory

            Returns a list of vertices, in CCW order.
            """

        irregularity = FTSESQueryGenerator.clip(irregularity, 0, 1) * 2 * math.pi / num_vertices
        spikeyness = FTSESQueryGenerator.clip(spikeyness, 0, 1) * ave_radius

        # generate n angle steps
        angle_steps = []
        lower = (2 * math.pi / num_vertices) - irregularity
        upper = (2 * math.pi / num_vertices) + irregularity
        sum = 0.0
        for i in range(num_vertices):
            tmp = random.uniform(lower, upper)
            angle_steps.append(tmp)
            sum = sum + tmp

        # normalize the steps so that point 0 and point n+1 are the same
        k = sum / (2 * math.pi)
        for i in range(num_vertices):
            angle_steps[i] = angle_steps[i] / k

        # now generate the points
        points = []
        angle = random.uniform(0, 2 * math.pi)
        for i in range(num_vertices):
            r_i = FTSESQueryGenerator.clip(random.gauss(ave_radius, spikeyness), 0, 2 * ave_radius)
            x = latitude + r_i * math.cos(angle)
            y = longitude + r_i * math.sin(angle)
            if x > 90: x = (-1 * x) + 90
            if x < -90: x = abs(x) - 90
            if y > 180: y = (-1 * y) + 180
            if y < -180: y = abs(y) - 180
            points.append((float(x), float(y)))

            angle = angle + angle_steps[i]

        return points

    @staticmethod
    def clip(x, min1, max1):
        if min1 > max1:
            return x
        elif x < min1:
            return min1
        elif x > max1:
            return max1
        else:
            return x

    @staticmethod
    def get_self_intersect_vertices(verts):
        mod_verts = []
        mid_vert = int((len(verts) - 1) / 2)

        mod_verts.append(verts[0])
        mod_verts.append(verts[mid_vert])

        x = 1

        while (mid_vert + x) < (len(verts) - 1):
            mod_verts.append(verts[mid_vert + x])
            mod_verts.append(verts[mid_vert - x])
            x += 1

        mod_verts.append(verts[len(verts) - 1])

        return mod_verts

    @staticmethod
    def construct_geo_polygon_query(center=None, polygon_feature="regular", num_vertices=None):
        """
        Returns a geo polygon query for Couchbase and Elastic search
        """

        if polygon_feature == "irregular":
            irregularity = 0.8
            spikeyness = 0.5
        else:
            irregularity = 0
            spikeyness = 0

        if polygon_feature == "self-intersect":
            num_vertices = random.randrange(5, 20, 2)

        ave_radius = random.randint(5, 50)

        if not num_vertices:
            num_vertices = random.randint(3, 20)

        verts = FTSESQueryGenerator.generate_polygon(center[0], center[1], ave_radius, irregularity,
                                                     spikeyness, num_vertices)

        if polygon_feature == "self-intersect":
            verts = FTSESQueryGenerator.get_self_intersect_vertices(verts)

        fts_query = {
            "polygon_points": [],
            "field": "geo"
        }

        es_query = {
            "query":{
                "bool":{
                    "filter":{
                        "geo_polygon":{
                            "geo":{
                                "points": []
                            }
                        }
                    }
                }
            }
        }

        case = random.randint(0, 4)
        format = None

        # Geo Location as map
        if case == 0:
            format = "map"
            verts_map_list = []
            for vert in verts:
                vert_map = {"lat": vert[0], "lon": vert[1]}
                verts_map_list.append(vert_map)

            fts_query['polygon_points'] = verts_map_list
            es_query['query']['bool']['filter']['geo_polygon']['geo']['points'] = verts_map_list

        # Geo Location as array
        if case == 1:
            format = "array"
            verts_list = []
            for vert in verts:
                verts_list.append([vert[1], vert[0]])
            fts_query['polygon_points'] = verts_list
            es_query['query']['bool']['filter']['geo_polygon']['geo']['points'] = verts_list

        # Geo Location as string
        if case == 2:
            format = "string"
            verts_list = []
            for vert in verts:
                verts_list.append(str(vert[0]) + "," + str(vert[1]))

            fts_query['polygon_points'] = verts_list
            es_query['query']['bool']['filter']['geo_polygon']['geo']['points'] = verts_list

        # Geo Location as Geohash
        if case == 3:
            format = "Geohash"
            verts_list = []
            precision = random.randint(3, 8)
            for vert in verts:
                verts_list.append(Geohash.encode(vert[0], vert[1], precision))

            fts_query['polygon_points'] = verts_list
            es_query['query']['bool']['filter']['geo_polygon']['geo']['points'] = verts_list

        # Geo Location as mixed
        if case == 4:
            format = "Mixed"
            verts_list = []
            for vert in verts:
                mixed_case = random.randint(0, 3)
                if mixed_case == 0:
                    # Geo Location as map
                    mixed_vert = {"lat": vert[0], "lon": vert[1]}
                if mixed_case == 1:
                    # Geo Location as array
                    mixed_vert = [vert[1], vert[0]]
                if mixed_case == 2:
                    # Geo Location as string
                    mixed_vert = str(vert[0]) + "," + str(vert[1])
                if mixed_case == 3:
                    # Geo Location as Geohash
                    precision = random.randint(3, 8)
                    mixed_vert = Geohash.encode(vert[0], vert[1], precision)

                verts_list.append(mixed_vert)

            fts_query['polygon_points'] = verts_list
            es_query['query']['bool']['filter']['geo_polygon']['geo']['points'] = verts_list

        # Geo Location as an object of lat and lon if case == 0
        return fts_query, es_query, ave_radius, num_vertices, format

    @staticmethod
    def construct_geo_bounding_box_query(lon1=None, lat1=None,
                                         lon2=None, lat2=None):
        """
        Returns a geo bounding box query for Couchbase and Elastic search
        """
        from lib.couchbase_helper.data import LON_LAT,LON_LAT_MAP
        if not lon1:
            lon_lat_ref = LON_LAT_MAP[random.randint(0, 7)]
            lon_lat1 = lon_lat_ref[0]
            lon_lat2  = lon_lat_ref[random.randint(1,len(lon_lat_ref)-1)]
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
            "query":{
                "bool":{
                    "filter":{
                        "geo_bounding_box":{
                            "geo":{
                                "top_left": {
                                "lat": lat1,
                                "lon": lon1
                                },
                                "bottom_right": {
                                    "lat": lat2,
                                    "lon": lon2
                                }
                            }
                        }
                    }
                }
            }
        }

        if bool(random.getrandbits(1)):
            fts_query['top_left'] = [lon1, lat1]
            fts_query['bottom_right'] = [lon2, lat2]
            es_query['query']['bool']['filter']['geo_bounding_box']['geo']['top_left'] = \
                [lon1, lat1]
            es_query['query']['bool']['filter']['geo_bounding_box']['geo']['bottom_right'] = \
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
        if bool(random.getrandbits(1)) and 'date_range' in self.query_types:
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


class FTSFlexQueryGenerator(FTSESQueryGenerator):

    def __init__(self, num_queries=1, query_type=None, seed=0, dataset="emp", fields=None):
        super().__init__(num_queries, query_type=None, fields=None, dataset=dataset)
        self.queries_to_generate = num_queries
        self.iterator = 0
        self.fts_flex_queries = []
        self.gsi_flex_queries = []
        self.fts_gsi_flex_queries = []
        self.gsi_queries = []
        self.fts_flex_query_template = "select meta().id from default USE INDEX " \
                                       "({{flex_hint}}) where {0} {1}"
        self.gsi_query_template = "select meta().id from default where {0} {1}"
        if fields:
            # Smart query generation
            self.fields = {}
            self.make_fields_compatible(fields)
            self.check_for_array_fields_remove_type()
            self.query_types = self.get_custom_n1ql_query_types()
            self.smart_queries = True
        else:
            self.fields = self.construct_fields()
            self.check_for_array_fields_remove_type()
            self.query_types = self.get_custom_n1ql_query_types()
        if self.query_types:
            self.construct_flex_queries()
        else:
            print("No string/number/date fields indexed for smart" \
                  " query generation ")
        self.expected_fts_index_field = {}

    def check_for_array_fields_remove_type(self):
        if 'text' in self.fields.keys():
            del self.fields['text']

        if "str" in self.fields.keys():
            self.fields['str'] = list(set(self.fields['str']))
            temp_fields = copy.deepcopy(self.fields)
            for text_field in temp_fields['str']:
                if text_field in DATASET.FIELDS['emp']["array"]:
                    if "array" not in self.fields.keys():
                        self.fields["array"] = [text_field]
                    elif text_field not in self.fields["array"]:
                        self.fields["array"].append(text_field)
                    self.fields['str'].remove(text_field)

            if "type" in self.fields['str']:
                self.fields['str'].remove("type")

        print("Smart queries will be generated on fields: %s" % self.fields)

    def get_custom_n1ql_query_types(self):
        query_types = []
        for field_type in list(self.fields.keys()):
            if field_type == 'vector':
                continue
            if self.fields[field_type].__len__() != 0:
                query_types += QUERY_TYPE.N1QL_QUERY_TYPES[field_type]
        return list(set(query_types))

    def check_for_dataset_in_predicate(self, predicate, dataset):
        for k, field_list in DATASET.FIELDS[dataset].items():
            for field in field_list:
                if field in predicate:
                    return True
        return False

    def construct_flex_num_queries(self):
            while self.iterator < self.queries_to_generate:
                fieldname = self.get_random_value(self.query_types)
                flex_query_predicate_list = eval("self.construct_flex_%s_query()" % fieldname)
                if not flex_query_predicate_list:
                    # if there are no queryable fields in a dataset for a
                    # particular data type
                    continue
                for predicate in flex_query_predicate_list:
                    for type in self.get_type_mapping(predicate):
                        self.fts_flex_queries.append(self.replace_underscores(self.fts_flex_query_template.format(type, predicate)))
                        self.gsi_queries.append(self.replace_underscores(self.gsi_query_template.format(type, predicate)))
                self.iterator += len(flex_query_predicate_list)

    def get_type_mapping(self, predicate):
        if self.dataset == "default":
            return [""]
        if self.dataset == "all":
            type_map_list = []
            #commenting due to bug
            #type_map_list = ['(type = \"emp\" or type = \"wiki\") and ', 'type = \"emp\" or type = \"wiki\" and ']
            #bug: MB-39517
            if "mutated" in predicate and not (predicate.count("OR") >= 1):
                type_map_list.append('(type = "emp" or type = "wiki") and ')
            if self.check_for_dataset_in_predicate(predicate, "emp") \
                    and not (predicate.count("OR") >= 1 and self.check_for_dataset_in_predicate(predicate, "wiki")):
                type_map_list.append('type = "emp" and ')

            if self.check_for_dataset_in_predicate(predicate, "wiki") \
                    and not (predicate.count("OR") >= 1 and self.check_for_dataset_in_predicate(predicate, "emp")):
                type_map_list.append('type = "wiki" and ')

            return type_map_list
        else:
            return ['type = \"{0}\" and '.format(self.dataset)]

    def construct_flex_queries(self):
        for fieldname in self.query_types:
            flex_query_predicate_list = eval("self.construct_flex_%s_query()" % fieldname)
            if not flex_query_predicate_list:
                # if there are no queryable fields in a dataset for a
                # particular data type
                continue
            for predicate in flex_query_predicate_list:
                for type_map in self.get_type_mapping(predicate):
                    self.fts_flex_queries.append(self.replace_underscores(self.fts_flex_query_template.format(type_map, predicate)))
                    self.gsi_queries.append(self.replace_underscores(self.gsi_query_template.format(type_map, predicate)))

    def construct_flex_term_range_query(self):
        flex_query_predicate_list = []

        fieldname = self.get_random_value(self.fields['str'])
        str1 = eval("self.get_queryable_%s_range" % fieldname + "()")
        str2 = eval("self.get_queryable_%s_range" % fieldname + "(min=False)")

        flex_query_predicate = "( {0} > \"{1}\" and {0} < \"{2}\")".format(fieldname, str1, str2)
        flex_query_predicate_list.append(flex_query_predicate)

        fieldname = self.get_random_value(self.fields['str'])
        str1 = eval("self.get_queryable_%s_range" % fieldname + "()")
        str2 = eval("self.get_queryable_%s_range" % fieldname + "(min=False)")

        flex_query_predicate = "( {0} >= \"{1}\" and {0} <= \"{2}\")".format(fieldname, str1, str2)
        flex_query_predicate_list.append(flex_query_predicate)

        fieldname = self.get_random_value(self.fields['str'])
        str1 = eval("self.get_queryable_%s_range" % fieldname + "()")
        str2 = eval("self.get_queryable_%s_range" % fieldname + "(min=False)")

        flex_query_predicate = "( {0} >= \"{1}\" and {0} < \"{2}\")".format(fieldname, str1, str2)
        flex_query_predicate_list.append(flex_query_predicate)

        fieldname = self.get_random_value(self.fields['str'])
        str1 = eval("self.get_queryable_%s_range" % fieldname + "()")
        str2 = eval("self.get_queryable_%s_range" % fieldname + "(min=False)")

        flex_query_predicate = "( {0} > \"{1}\" and {0} <= \"{2}\")".format(fieldname, str1, str2)
        flex_query_predicate_list.append(flex_query_predicate)

        return flex_query_predicate_list

    def construct_flex_term_equal_query(self):
        flex_query_predicate_list = []
        for x in range(5):
            fieldname = self.get_random_value(self.fields['str'])
            match_str = eval("self.get_queryable_%s()" % fieldname)
            flex_query_predicate = "( {0} = \"{1}\")".format(fieldname, match_str)
            flex_query_predicate_list.append(flex_query_predicate)

        return flex_query_predicate_list

    def construct_flex_term_between_query(self):
        flex_query_predicate_list = []

        for x in range(5):
            fieldname = self.get_random_value(self.fields['str'])
            str1 = eval("self.get_queryable_%s_range" % fieldname + "()")
            str2 = eval("self.get_queryable_%s_range" % fieldname + "(min=False)")

            flex_query_predicate = "( {0} between \"{1}\" and \"{2}\")".format(fieldname, str1, str2)
            flex_query_predicate_list.append(flex_query_predicate)

        return flex_query_predicate_list

    def construct_flex_term_like_query(self):
        flex_query_predicate_list = []

        for x in range(5):
            fieldname = self.get_random_value(self.fields['str'])
            match_str = "(text)"
            meta_list = ['(', '.']
            # due to bug# MB-38690
            while any(x in match_str for x in meta_list):
                match_str = eval("self.get_queryable_%s()" % fieldname)
                # due to bug# MB-38690
                if fieldname == "email":
                    pos = random.randint(1, len(match_str) - 5)
                else:
                    pos = random.randint(1, len(match_str) - 1)
                match_str = match_str[:pos]
            match_str = match_str + '%'
            flex_query_predicate = "( {0} like \"{1}\")".format(fieldname, match_str)
            flex_query_predicate_list.append(flex_query_predicate)

        return flex_query_predicate_list

    def construct_flex_num_equal_query(self):
        flex_query_predicate_list = []
        for x in range(5):
            fieldname = self.get_random_value(self.fields['num'])
            match_str = eval("self.get_queryable_%s()" % fieldname)
            flex_query_predicate = "( {0} = {1})".format(fieldname, match_str)
            flex_query_predicate_list.append(flex_query_predicate)

        return flex_query_predicate_list

    def construct_flex_num_range_query(self):

        flex_query_predicate_list = []

        fieldname = self.get_random_value(self.fields['num'])
        low = eval("self.get_queryable_%s" % fieldname + "()")
        high = low + random.randint(2, 10000)

        flex_query_predicate = "( {0} > {1} and {0} < {2})".format(fieldname, low, high)
        flex_query_predicate_list.append(flex_query_predicate)

        fieldname = self.get_random_value(self.fields['num'])
        low = eval("self.get_queryable_%s" % fieldname + "()")
        high = low + random.randint(2, 10000)

        flex_query_predicate = "({0} > {1} and {0} < {2})".format(fieldname, low, high)
        flex_query_predicate_list.append(flex_query_predicate)

        fieldname = self.get_random_value(self.fields['num'])
        low = eval("self.get_queryable_%s" % fieldname + "()")
        high = low + random.randint(2, 10000)

        flex_query_predicate = "( {0} >= {1} and {0} <= {2})".format(fieldname, low, high)
        flex_query_predicate_list.append(flex_query_predicate)

        fieldname = self.get_random_value(self.fields['num'])
        low = eval("self.get_queryable_%s" % fieldname + "()")
        high = low + random.randint(2, 10000)

        flex_query_predicate = "( {0} >= {1} and {0} < {2})".format(fieldname, low, high)
        flex_query_predicate_list.append(flex_query_predicate)

        fieldname = self.get_random_value(self.fields['num'])
        low = eval("self.get_queryable_%s" % fieldname + "()")
        high = low + random.randint(2, 10000)

        flex_query_predicate = "( {0} > {1} and {0} <= {2})".format(fieldname, low, high)
        flex_query_predicate_list.append(flex_query_predicate)

        return flex_query_predicate_list

    def construct_flex_num_between_query(self):
        flex_query_predicate_list = []

        for x in range(5):
            fieldname = self.get_random_value(self.fields['num'])
            low = eval("self.get_queryable_%s" % fieldname + "()")
            high = low + random.randint(2, 10000)

            flex_query_predicate = "( {0} between {1} and {2})".format(fieldname, low, high)
            flex_query_predicate_list.append(flex_query_predicate)

        return flex_query_predicate_list

    def construct_flex_date_equal_query(self):

        flex_query_predicate_list = []
        for x in range(5):
            fieldname = self.get_random_value(self.fields['date'])
            match_str = eval("self.get_queryable_%s" % fieldname + "()")
            flex_query_predicate = "( {0} = \"{1}\")".format(fieldname, match_str)
            flex_query_predicate_list.append(flex_query_predicate)

        return flex_query_predicate_list

    def construct_flex_date_range_query(self):

        flex_query_predicate_list = []

        fieldname = self.get_random_value(self.fields['date'])
        start = eval("self.get_queryable_%s" % fieldname + "()")
        end = eval("self.get_queryable_%s" % fieldname + "(now=True)")

        flex_query_predicate = "( {0} > \"{1}\" and {0} < \"{2}\")".format(fieldname, start, end)
        flex_query_predicate_list.append(flex_query_predicate)

        fieldname = self.get_random_value(self.fields['date'])
        start = eval("self.get_queryable_%s" % fieldname + "()")
        end = eval("self.get_queryable_%s" % fieldname + "(now=True)")

        flex_query_predicate = "( {0} > \"{1}\" and {0} < \"{2}\")".format(fieldname, start, end)
        flex_query_predicate_list.append(flex_query_predicate)

        fieldname = self.get_random_value(self.fields['date'])
        start = eval("self.get_queryable_%s" % fieldname + "()")
        end = eval("self.get_queryable_%s" % fieldname + "(now=True)")

        flex_query_predicate = "( {0} >= \"{1}\" and {0} <= \"{2}\")".format(fieldname, start, end)
        flex_query_predicate_list.append(flex_query_predicate)

        fieldname = self.get_random_value(self.fields['date'])
        start = eval("self.get_queryable_%s" % fieldname + "()")
        end = eval("self.get_queryable_%s" % fieldname + "(now=True)")

        flex_query_predicate = "( {0} >= \"{1}\" and {0} < \"{2}\")".format(fieldname, start, end)
        flex_query_predicate_list.append(flex_query_predicate)

        fieldname = self.get_random_value(self.fields['date'])
        start = eval("self.get_queryable_%s" % fieldname + "()")
        end = eval("self.get_queryable_%s" % fieldname + "(now=True)")

        flex_query_predicate = "( {0} > \"{1}\" and {0} <= \"{2}\")".format(fieldname, start, end)
        flex_query_predicate_list.append(flex_query_predicate)

        return flex_query_predicate_list

    def construct_flex_date_between_query(self):
        flex_query_predicate_list = []

        for x in range(5):
            fieldname = self.get_random_value(self.fields['date'])
            start = eval("self.get_queryable_%s" % fieldname + "()")
            end = eval("self.get_queryable_%s" % fieldname + "(now=True)")

            flex_query_predicate = "( {0} between \"{1}\" and \"{2}\")".format(fieldname, start, end)
            flex_query_predicate_list.append(flex_query_predicate)

        return flex_query_predicate_list

    def construct_flex_boolean_query(self):
        flex_query_predicate_list = []
        fieldname = self.get_random_value(self.fields['bool'])
        flex_query_predicate_list.append("{0} = True".format(fieldname))
        flex_query_predicate_list.append("{0} = False".format(fieldname))
        # commenting for MB-38815
        # flex_query_predicate_list.append("{0}".format(fieldname))

        return flex_query_predicate_list

    def construct_flex_array_in_query(self):
        flex_query_predicate_list = []
        for x in range(5):
            fieldname = self.get_random_value(self.fields['array'])
            match_str = self.get_term(fieldname)
            flex_query_predicate_list.append("\"{0}\" in {1}".format(match_str, fieldname))

        return flex_query_predicate_list

    def construct_flex_array_any_query(self):
        flex_query_predicate_list = []
        for x in range(5):
            fieldname = self.get_random_value(self.fields['array'])
            match_str = self.get_term(fieldname)
            flex_query_predicate_list.append("( ANY v IN {0} SATISFIES v = \"{1}\" END)".format(fieldname, match_str))

        for x in range(5):
            fieldname = self.get_random_value(self.fields['array'])
            match_str = self.get_term(fieldname)
            pos = random.randint(1, len(match_str) - 1)
            match_str = match_str[:pos] + '%'
            flex_query_predicate_list.append("( ANY v IN {0} SATISFIES v like \"{1}\" END)".format(fieldname, match_str))

        for x in range(5):
            fieldname = self.get_random_value(self.fields['array'])
            match_str = self.get_term(fieldname)
            flex_query_predicate_list.append("( SOME v IN {0} SATISFIES v = \"{1}\" END)".format(fieldname, match_str))

        for x in range(5):
            fieldname = self.get_random_value(self.fields['array'])
            match_str = self.get_term(fieldname)
            flex_query_predicate_list.append("( ANY v IN {0} SATISFIES v IN [\"{1}\", \"aaaaa\",\"bbbb\" ] END)".format(fieldname, match_str))

        for x in range(5):
            fieldname = self.get_random_value(self.fields['array'])
            match_str = self.get_term(fieldname)
            pos = random.randint(1, len(match_str) - 1)
            match_str = match_str[:pos] + '%'
            flex_query_predicate_list.append("( SOME v IN {0} SATISFIES v like \"{1}\" END)".format(fieldname, match_str))

        for x in range(5):
            fieldname = self.get_random_value(self.fields['array'])
            match_str = self.get_term(fieldname)
            flex_query_predicate_list.append("( ANY AND EVERY v IN {0} SATISFIES v = \"{1}\" END)".format(fieldname, match_str))

        for x in range(5):
            fieldname = self.get_random_value(self.fields['array'])
            match_str = self.get_term(fieldname)
            pos = random.randint(1, len(match_str) - 1)
            match_str = match_str[:pos] + '%'
            flex_query_predicate_list.append("( ANY AND EVERY v IN {0} SATISFIES v like \"{1}\" END)".format(fieldname, match_str))

        return flex_query_predicate_list

    def construct_flex_conjunction_disjunction_query(self):
        """
        Returns an fts and es query with queries to be ANDed
        """
        flex_query_predicate_list = []
        for x in range(10):
            mixed = bool(random.getrandbits(1))
            logical_operator = "AND" if bool(random.getrandbits(1)) else "OR"
            query_predicate = ""
            num_of_predicates = random.randint(2,5)
            for x in range(num_of_predicates):
                fieldname = self.get_random_value(self.query_types)
                while fieldname is "conjunction_disjunction": fieldname = self.get_random_value(self.query_types)
                query_predicate_list = eval("self.construct_flex_%s_query()" % fieldname)
                if query_predicate == "":
                    query_predicate = self.get_random_value(query_predicate_list)
                    continue
                if mixed:
                    logical_operator = "AND" if bool(random.getrandbits(1)) else "OR"
                query_predicate = "( " + query_predicate + " " + logical_operator + " " + self.get_random_value(query_predicate_list) + ")"
                #query_predicate = query_predicate + " " + logical_operator + " " + self.get_random_value(query_predicate_list)
            flex_query_predicate_list.append(query_predicate)
        return flex_query_predicate_list


if __name__ == "__main__":
    # query_type=['match_phrase', 'match', 'date_range', 'numeric_range', 'bool',
    #              'conjunction', 'disjunction', 'prefix']
    # query_type = ['term_range']
    # query_gen = FTSESQueryGenerator(100, query_type=query_type, dataset='all')
    # for index, query in enumerate(query_gen.fts_queries):
    #    print json.dumps(query, ensure_ascii=False, indent=3)
    #    print json.dumps(query_gen.es_queries[index], ensure_ascii=False, indent=3)
    #    print "------------"

    # fts_query, es_query = FTSESQueryGenerator.construct_geo_polygon_query([-118.77, 34.243], "regular", None)
    # print(fts_query)
    # print(es_query)

    query_gen = FTSFlexQueryGenerator(num_queries=1, query_type="term_range",
                                      seed=0, dataset="emp",
                                      fields={})
    #query_gen.check_for_array_fields(fields={'datetime': ['join_date'], 'text': ['manages_reports', 'email']})
    print(query_gen.fts_flex_queries)
    #print(query_gen.gsi_flex_queries)

    #query_gen = FTSFlexQueryGenerator(num_queries=1, query_type=["term_range"],
    #                                  seed=0, dataset="emp",
    #                                  fields=None)

    #print(query_gen.fts_flex_queries)
    #print(query_gen.gsi_flex_queries)
