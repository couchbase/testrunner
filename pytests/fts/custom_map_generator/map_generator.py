#  Sample emp doc and a custom map on it -
#  https://gist.github.com/arunapiravi/044d6547b7853ad6c14a
import random
import copy
from TestInput import TestInputSingleton

EMP_FIELDS = {
    'text': ["name", "dept", "languages_known", "email", "type"],
    'number': ["mutated", "salary"],
    'boolean': ["is_manager"],
    'datetime': ["join_date"],
    'object': ["manages"]  # denote nested fields
}

TOTAL_EMP_FIELDS = 9

EMP_NESTED_FIELDS = {
    'manages': {
        'text': ["reports"],
        'number': ["team_size"]
    }
}

# Need to work on double nested field "revision_text_text"

WIKI_FIELDS = {
    'text': ["title", "type"],
    'number': ["mutated"],
    #'object': ["revision", "text", "contributor"]
    'object': ["revision"]
}

TOTAL_WIKI_FIELDS = 6

WIKI_NESTED_FIELDS = {
    'revision': {
        'datetime': ["timestamp"]
    },
    #'text': {
    #    'text': ["#text"]
    #},
    #'contributor': {
    #    'text': ["username"]
    #}
}

FULL_FIELD_NAMES = {
    'reports': 'manages_reports',
    'team_size': 'manages_team_size',
    'timestamp': 'revision_timestamp',
    #'#text': 'revision_text_text',
    #'username': 'revision_contributor_username'
}

CUSTOM_ANALYZER_TEMPLATE = {
    "analyzers": {},
    "token_filters": {
        "back_edge_ngram": {
            "back":True,
            "max": 5,
            "min": 3,
            "type": "edge_ngram"
        },
        "dict_compound_en": {
            "dict_token_map": "stop_en",
            "type": "dict_compound"
        },
        "dict_compound_fr": {
            "dict_token_map": "articles_fr",
            "type": "dict_compound"
        },
        "front_edge_ngram": {
            "back":False,
            "max": 5,
            "min": 3,
            "type": "edge_ngram"
        },
        "keyword_marker": {
            "keywords_token_map": "stopwords",
            "type": "keyword_marker"
        },
        "stopwords": {
            "stop_token_map": "stopwords",
            "type": "stop_tokens"
        },
        "length": {
            "max": 5,
            "min": 3,
            "type": "length"
        },
        "ngram": {
            "max": 5,
            "min": 3,
            "type": "ngram"
        },
        "shingle": {
            "filler": "",
            "max": 5,
            "min": 2,
            "output_original": True,
            "output_unigrams": True,
            "separator": "",
            "type": "shingle"
        },
        "truncate": {
            "length": 10,
            "type": "truncate_token"
        }
    },
    "token_maps": {
        "stopwords": {
            "tokens": ['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves',
                 'you', 'your', 'yours', 'yourself', 'yourselves', 'he', 'him',
                 'his', 'himself', 'she', 'her', 'hers', 'herself', 'it', 'its',
                 'itself', 'they', 'them', 'their', 'theirs', 'themselves',
                 'what', 'which', 'who', 'whom', 'this', 'that', 'these',
                 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been',
                 'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did',
                 'doing', 'would', 'should', 'could', 'ought', "i'm", "you're",
                 "he's", "she's", "it's", "we're", "they're", "i've", "you've",
                 "we've", "they've", "i'd", "you'd", "he'd", "she'd", "we'd",
                 "they'd", "i'll", "you'll", "he'll", "she'll", "we'll",
                 "they'll", "isn't", "aren't", "wasn't", "weren't", "hasn't",
                 "haven't", "hadn't", "doesn't", "don't", "didn't", "won't",
                 "wouldn't", "shan't", "shouldn't", "can't", 'cannot',
                 "couldn't", "mustn't", "let's", "that's", "who's", "what's",
                 "here's", "there's", "when's", "where's", "why's", "how's",
                 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as',
                 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about',
                 'against', 'between', 'into', 'through', 'during', 'before',
                 'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in',
                 'out', 'on', 'off', 'over', 'under', 'again', 'further',
                 'then', 'once', 'here', 'there', 'when', 'where', 'why',
                 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most',
                 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own',
                 'same', 'so', 'than', 'too', 'very'],
            "type": "custom"
        }
    },
    "char_filters": {
        "mapping": {
            "regexp": "[f]",
            "replace": "ph",
            "type": "regexp"
        }
    },
    "tokenizers": {
        "alphanumeric": {
            "regexp": "[0-9a-zA-Z_]*",
            "type": "regexp"
        }
    }
}

ANALYZERS = ["standard", "simple", "keyword", "en"]

LANG_ANALYZERS = ["ar", "cjk", "fr", "fa", "hi", "it", "pt", "en", "web"]

CHAR_FILTERS = ["html", "mapping"]

TOKENIZERS = ["letter", "single", "unicode", "web", "whitespace", "alphanumeric"]

TOKEN_FILTERS = ["apostrophe", "elision_fr", "to_lower", "ngram",
                 "front_edge_ngram", "back_edge_ngram", "shingle",
                 "truncate", "stemmer_porter", "length", "keyword_marker",
                 "stopwords", "cjk_bigram", "stemmer_it_light",
                 "stemmer_fr_light", "stemmer_fr_min", "stemmer_pt_light"]

class CustomMapGenerator:
    """
    # Generates an FTS and equivalent ES custom map for emp/wiki datasets
    """
    def __init__(self, seed=0, dataset="emp", num_custom_analyzers=0,multiple_filters=False,
                 custom_map_add_non_indexed_fields=True, text_analyzer=None, type_mapping=None, collection_index=False):
        random.seed(seed)
        self.fts_map = {"types": {}}
        self.es_map = {}
        self.num_field_maps = random.randint(1, 10)
        self.queryable_fields = {}
        self.num_custom_analyzers = num_custom_analyzers
        self.text_analyzer = text_analyzer
        self.custom_map_add_non_indexed_fields = custom_map_add_non_indexed_fields
        # Holds the list of custom analyzers created by
        # build_custom_analyzer method
        self.custom_analyzers=[]
        self.multiple_filters = multiple_filters
        self.vector_search = TestInputSingleton.input.param("vector_search", False)
        self.llm_model = TestInputSingleton.input.param("llm_model", "all-MiniLM-L6-v2")
        self.vector_similarity = TestInputSingleton.input.param("vector_similarity", "l2_norm")
        if self.vector_search:
            EMP_FIELDS['vector'] = ["l_vector"]

        for n in range(0, self.num_custom_analyzers, 1):
            self.custom_analyzers.append("customAnalyzer"+str(n+1))

        if dataset == "emp":
            self.fields = EMP_FIELDS
            self.nested_fields = EMP_NESTED_FIELDS
            self.max_fields = TOTAL_EMP_FIELDS
            if self.vector_search:
                self.max_fields = self.max_fields + 1
            type_mapping_val = type_mapping + "." + dataset if collection_index else dataset
            self.fts_map['types'][type_mapping_val] = {
                                        "dynamic": False,
                                        "enabled": True,
                                        "fields": [],
                                        "properties": {}
                                    }
            self.es_map = {
                        "dynamic": False,
                        "properties": {}
                    }
            if collection_index:
                self.build_custom_map(dataset, type_mapping)
            else:
                self.build_custom_map(dataset)
        elif dataset == "wiki":
            self.fields = WIKI_FIELDS
            self.nested_fields = WIKI_NESTED_FIELDS
            self.max_fields = TOTAL_WIKI_FIELDS
            type_mapping_val = type_mapping + "." + dataset if collection_index else dataset
            self.fts_map['types'][type_mapping_val] = {
                                        "dynamic": False,
                                        "enabled": True,
                                        "fields": [],
                                        "properties": {}
                                    }
            self.es_map = {
                        "dynamic": False,
                        "properties": {}
                    }
            if collection_index:
                self.build_custom_map(dataset, type_mapping)
            else:
                self.build_custom_map(dataset)
        elif dataset == "all":
            self.fields = EMP_FIELDS
            self.nested_fields = EMP_NESTED_FIELDS
            self.max_fields = TOTAL_EMP_FIELDS
            type_mapping_val = type_mapping+".emp" if collection_index else 'emp'
            self.fts_map['types'][type_mapping_val] = {
                                        "dynamic": False,
                                        "enabled": True,
                                        "fields": [],
                                        "properties": {}
                                    }
            self.es_map = {
                        "dynamic": False,
                        "properties": {}
                    }
            if collection_index:
                self.build_custom_map("emp", type_mapping)
            else:
                self.build_custom_map("emp")
            if int(TestInputSingleton.input.param("doc_maps", 1)) > 1:
                self.fields = WIKI_FIELDS
                self.nested_fields = WIKI_NESTED_FIELDS
                self.max_fields = TOTAL_WIKI_FIELDS
                type_mapping_val = type_mapping+".wiki" if collection_index else 'wiki'
                self.fts_map['types'][type_mapping_val] = {
                                        "dynamic": False,
                                        "enabled": True,
                                        "fields": [],
                                        "properties": {}
                                    }
                if collection_index:
                    self.build_custom_map("wiki", type_mapping)
                else:
                    self.build_custom_map("wiki")

    def get_random_value(self, list):
        return list[random.randint(0, len(list)-1)]

    def get_map(self):
        return self.fts_map, self.es_map

    def get_smart_query_fields(self):
        """
        Smart querying refers to generating queries on
        fields referenced in the custom map
        """
        return self.queryable_fields

    def add_to_queryable_fields(self, field, field_type):
        if field in FULL_FIELD_NAMES:
            # if nested field, then fully qualify the field name
            field = FULL_FIELD_NAMES[field]
        if field_type not in list(self.queryable_fields.keys()):
            self.queryable_fields[field_type] = []
        if field not in self.queryable_fields[field_type]:
            self.queryable_fields[field_type].append(field)

    def build_custom_map(self, dataset, collection_type=None):
        field_map = self.field_map(self.fields)
        for field, _type in field_map.items():
            type_val = collection_type + "." + dataset if collection_type else dataset
            if field not in iter(list(self.nested_fields.keys())):
                fts_child, es_child = self.get_child_field(field, _type)
            else:
                fts_child, es_child = self.get_child_map(field, type_val, dataset)
            self.fts_map['types'][type_val]['properties'][field] = fts_child
            self.es_map['properties'][field] = es_child

        if self.custom_map_add_non_indexed_fields:
            self.add_non_indexed_field_to_query()

    def add_non_indexed_field_to_query(self):
        """
        Add 1 or 2 non-indexed fields(negative test for custom mapping)
        Query on non-indexed fields to see if 0 results are returned
        """
        count = 0
        if self.num_field_maps < self.max_fields:
            while count < self.max_fields:
                count += 1
                field, field_type = self.get_random_field_name_and_type(
                    self.fields)
                if field_type != 'object' and \
                   field_type not in list(self.queryable_fields.keys()):
                    print(("Adding an extra non-indexed field '%s' to" \
                          " list of queryable fields" % field))
                    self.queryable_fields[field_type] = [field]
                    break
                if field_type != 'object' and \
                   field not in self.queryable_fields[field_type]:
                    print(("Adding an extra non-indexed field '%s' to" \
                          " list of queryable fields" % field))
                    self.queryable_fields[field_type].append(field)
                    break
            else:
                print(("Unable to add a non-indexed field after %s retries" \
                      % self.max_fields))

    def get_child_map(self, field, dataset, es_dataset):
        """
        Child maps are for nested json structures i.e, any higher level field
        having another nested structure as its value
        """
        current_prop = self.fts_map['types'][dataset]['properties']
        if field not in iter(current_prop.keys()):
            fts_child_map = {}
            fts_child_map['dynamic'] = False
            fts_child_map['enabled'] = True
            fts_child_map['fields'] = []
            fts_child_map['properties'] = {}

            es_child_map = {}
            es_child_map['dynamic'] = False
            es_child_map['enabled'] = True
            es_child_map['type'] = "object"
            es_child_map['properties'] = {}
        else:
            fts_child_map = self.fts_map['types'][dataset]['properties'][field]
            es_child_map = self.es_map['properties'][field]

        field, type = self.get_nested_child_field(field)
        fts_child, es_child = self.get_child_field(field, type)
        fts_child_map['properties'][field] = fts_child
        es_child_map['properties'][field] = es_child

        return fts_child_map, es_child_map

    def get_child_field(self, field, type):
        """
        Encapsulate the field map with 'dynamic', 'enabled', 'properties'
        and fields
        """
        fts_child = {}
        fts_child['dynamic'] = False
        fts_child['enabled'] = True
        fts_child['properties'] = {}
        fts_child['fields'] = []

        fts_field, es_field = self.get_field_map(field, type)
        fts_child['fields'].append(fts_field)

        return fts_child, es_field

    def get_field_map(self, field, field_type):
        """
        Set Index properties for any field in json and return the field map
        """
        is_indexed = bool(random.getrandbits(1))
        fts_field_map = {}
        if field_type != "vector":
            fts_field_map['include_in_all'] = True
            fts_field_map['include_term_vectors'] = True
        fts_field_map['index'] = True
        fts_field_map['name'] = field
        fts_field_map['store'] = False
        fts_field_map['type'] = field_type
        if self.num_custom_analyzers:
            analyzer = self.get_random_value(self.custom_analyzers)
        elif self.text_analyzer:
            analyzer = self.text_analyzer
        else:
            analyzer = self.get_random_value(ANALYZERS)
        if field_type == "text":
            fts_field_map['analyzer'] = analyzer
        else:
            fts_field_map['analyzer'] = ""

        if field_type == "vector":
            from sentence_transformers import SentenceTransformer
            encoder = SentenceTransformer(self.llm_model)
            fts_field_map['dims'] = encoder.get_sentence_embedding_dimension()
            fts_field_map['similarity'] = self.vector_similarity

        es_field_map = {}
        es_field_map['type'] = field_type
        
        # Update numeric and vector types
        if field_type in ["number", "vector"]:
            es_field_map['type'] = "float"

        # Update date type
        if field_type == "datetime":
            es_field_map['type'] = "date"

        # Boolean fields should use keyword for exact matching
        if field_type == "boolean":
            es_field_map['type'] = "boolean"

        # Default Elasticsearch 8.x behavior: "store" should be False unless needed
        es_field_map['store'] = False

        # Handle text fields correctly
        if field_type == "text":
            es_field_map['type'] = "text"  # Use "text" instead of "string"
            es_field_map['term_vector'] = "with_positions_offsets"  # Update term vector syntax

            # Handle analyzer settings properly
            if analyzer == "en":
                es_field_map['analyzer'] = "english"
            elif analyzer == "standard":
                es_field_map['analyzer'] = "standard"
            else:
                es_field_map['analyzer'] = analyzer  # Keep custom analyzers unchanged

        # If exact matching is required, use "keyword"
        if field_type == "keyword":
            es_field_map['type'] = "keyword"

        # add to list of queryable fields
        self.add_to_queryable_fields(field, field_type)

        return fts_field_map, es_field_map

    def get_random_field_name_and_type(self, fields):
        type = self.get_random_value(list(fields.keys()))
        field = self.get_random_value(fields[type])
        return field, type

    def field_map(self, fields):
        map = {}
        for k,v in fields.items():
            for j in v:
                map[j] = k
        return map

    def get_nested_child_field(self, nested_field):
        if nested_field in iter(list(self.nested_fields.keys())):
            return self.get_random_field_name_and_type(
                self.nested_fields[nested_field])

    def build_custom_analyzer(self):
        analyzer_map = {}
        if self.multiple_filters:
            num_token_filters = random.randint(1, min(3, len(TOKEN_FILTERS)))
            num_char_filters = random.randint(1, min(3, len(CHAR_FILTERS)))
        else:
            num_token_filters = 1
            num_char_filters = 1

        for custom_analyzer in self.custom_analyzers:
            analyzer_map[custom_analyzer] = {}
            analyzer_map[custom_analyzer]["char_filters"] = []
            analyzer_map[custom_analyzer]["token_filters"] = []
            analyzer_map[custom_analyzer]["tokenizer"] = ""

            for num in range(0, num_char_filters, 1):
                char_filter = self.get_random_value(CHAR_FILTERS)
                if not analyzer_map[custom_analyzer]["char_filters"].count(char_filter):
                    analyzer_map[custom_analyzer]["char_filters"].append(char_filter)

            tokenizer = self.get_random_value(TOKENIZERS)
            analyzer_map[custom_analyzer]["tokenizer"] = tokenizer

            for num in range(0, num_token_filters, 1):
                token_filter = self.get_random_value(TOKEN_FILTERS)
                if not analyzer_map[custom_analyzer]["token_filters"].count(token_filter):
                    analyzer_map[custom_analyzer]["token_filters"].append(token_filter)
            analyzer_map[custom_analyzer]["type"] = "custom"

        analyzer = CUSTOM_ANALYZER_TEMPLATE
        analyzer["analyzers"]=analyzer_map
        return analyzer

if __name__ == "__main__":
    import json
    custom_map = CustomMapGenerator(seed=1).get_map()
    print((json.dumps(custom_map, indent=3)))