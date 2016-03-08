#  Sample emp doc and a custom map on it -
#  https://gist.github.com/arunapiravi/044d6547b7853ad6c14a
import random
import copy
from TestInput import TestInputSingleton

EMP_FIELDS = {
    'text': ["name", "dept", "languages_known", "email"],
    'number': ["mutated", "salary"],
    #'boolean': ["is_manager"],
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

WIKI_FIELDS = {
    'text': ["title", "id", "type"],
    'number': ["mutated"],
    'object': ["revision", "text", "contributor"]
}

TOTAL_WIKI_FIELDS = 7

WIKI_NESTED_FIELDS = {
    'revision': {
        'datetime': ["timestamp"]
    },
    'text': {
        'text': ["#text"]
    },
    'contributor': {
        'text': ["username", "id"]
    }
}

FULL_FIELD_NAMES = {
    'reports': 'manages_reports',
    'team_size': 'manages_team_size',
    'timestamp': 'revision_timestamp',
    '#text': 'revision_text_text',
    'username': 'revision_contributor_username'
}

ANALYZERS = ["standard", "simple", "keyword", "en"]

LANG_ANALYZERS = ["ar", "cjk", "fr", "fa", "hi", "it", "pt", "en", "web"]

class CustomMapGenerator:
    """
    # Generates an FTS and equivalent ES custom map for emp/wiki datasets
    """
    def __init__(self, seed=0, dataset="emp"):
        random.seed(seed)
        self.fts_map = {"types": {}}
        self.es_map = {}
        self.num_field_maps = random.randint(1, 10)
        self.queryable_fields = {}
        if dataset == "emp":
            self.fields = EMP_FIELDS
            self.nested_fields = EMP_NESTED_FIELDS
            self.max_fields = TOTAL_EMP_FIELDS
            self.fts_map['types'][dataset] = {
                                        "dynamic": False,
                                        "enabled": True,
                                        "fields": [],
                                        "properties": {}
                                    }
            self.es_map[dataset] = {
                        "dynamic": False,
                        "properties": {}
                    }
            self.build_custom_map(dataset)
        elif dataset == "wiki":
            self.fields = WIKI_FIELDS
            self.nested_fields = WIKI_NESTED_FIELDS
            self.max_fields = TOTAL_WIKI_FIELDS
            self.fts_map['types'][dataset] = {
                                        "dynamic": False,
                                        "enabled": True,
                                        "fields": [],
                                        "properties": {}
                                    }
            self.es_map[dataset] = {
                        "dynamic": False,
                        "properties": {}
                    }
            self.build_custom_map(dataset)
        elif dataset == "all":
            self.fields = EMP_FIELDS
            self.nested_fields = EMP_NESTED_FIELDS
            self.max_fields = TOTAL_EMP_FIELDS
            self.fts_map['types']['emp'] = {
                                        "dynamic": False,
                                        "enabled": True,
                                        "fields": [],
                                        "properties": {}
                                    }
            self.es_map['emp'] = {
                        "dynamic": False,
                        "properties": {}
                    }
            self.build_custom_map('emp')
            if int(TestInputSingleton.input.param("doc_maps", 1)) > 1:
                self.fields = WIKI_FIELDS
                self.nested_fields = WIKI_NESTED_FIELDS
                self.max_fields = TOTAL_WIKI_FIELDS
                self.fts_map['types']['wiki'] = {
                                        "dynamic": False,
                                        "enabled": True,
                                        "fields": [],
                                        "properties": {}
                                    }
                self.es_map['wiki'] = {
                        "dynamic": False,
                        "properties": {}
                    }
                self.build_custom_map('wiki')
            else:
                if not TestInputSingleton.input.param("default_map", False):
                    # if doc_maps=1 and default map is disabled, force single
                    # map on ES by disabling wiki map
                    self.es_map['wiki'] = {
                            "dynamic": False,
                            "properties": {}
                        }

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
        if field_type not in self.queryable_fields.keys():
            self.queryable_fields[field_type] = []
        if field not in self.queryable_fields[field_type]:
            self.queryable_fields[field_type].append(field)

    def build_custom_map(self, dataset):
        for x in xrange(0, self.num_field_maps):
            field, type = self.get_random_field_name_and_type(self.fields)
            if field not in self.nested_fields.iterkeys():
                fts_child, es_child = self.get_child_field(field, type)
            else:
                fts_child, es_child = self.get_child_map(field)
            self.fts_map['types'][dataset]['properties'][field] = fts_child
            self.es_map[dataset]['properties'][field] = es_child
        self.add_non_indexed_field_to_query()

    def add_non_indexed_field_to_query(self):
        """
        Add 1 or 2 non-indexed fields(negative test for custom mapping)
        Query on non-indexed fields to see if 0 results are returned
        """
        if self.num_field_maps < self.max_fields:
            while(True):
                field, field_type = self.get_random_field_name_and_type(
                    self.fields)
                if field_type != 'object' and \
                   field_type not in self.queryable_fields.keys():
                    print "Adding an extra non-indexed field '%s' to" \
                          " list of queryable fields" % field
                    self.queryable_fields[field_type] = [field]
                    break
                if field_type != 'object' and \
                   field not in self.queryable_fields[field_type]:
                    print "Adding an extra non-indexed field '%s' to" \
                          " list of queryable fields" % field
                    self.queryable_fields[field_type].append(field)
                    break

    def get_child_map(self, field):
        """
        Child maps are for nested json structures i.e, any higher level field
        having another nested structure as its value
        """
        fts_child_map = {}
        fts_child_map['dynamic'] = False
        fts_child_map['enabled'] = True
        fts_child_map['fields'] = []
        field, type = self.get_nested_child_field(field)
        fts_child, es_child = self.get_child_field(field, type)
        fts_child_map['properties'] = {field: fts_child}

        es_child_map = {}
        es_child_map['dynamic'] = False
        es_child_map['enabled'] = True
        es_child_map['type'] = "object"
        es_child_map['properties'] = {field: es_child}

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
        fts_field_map['include_in_all'] = True
        fts_field_map['include_term_vectors'] = True
        fts_field_map['index'] = True
        fts_field_map['name'] = field
        fts_field_map['store'] = False
        fts_field_map['type'] = field_type
        analyzer = self.get_random_value(ANALYZERS)
        if field_type == "text":
            fts_field_map['analyzer'] = analyzer
        else:
            fts_field_map['analyzer'] = ""

        es_field_map = {}
        es_field_map['type'] = field_type
        if field_type == "number":
            es_field_map['type'] = "float"
        if field_type == "datetime":
            es_field_map['type'] = "date"
        es_field_map['store'] = False
        #if is_indexed:
        es_field_map['index'] = 'analyzed'
        #else:
        #    es_field_map['index'] = 'no'
        if field_type == "text":
            es_field_map['type'] = "string"
            es_field_map['term_vector'] = "yes"
            es_field_map['analyzer'] = analyzer
            if analyzer == "en":
                es_field_map['analyzer'] = "english"

        # add to list of queryable fields
        self.add_to_queryable_fields(field, field_type)

        return fts_field_map, es_field_map

    def get_random_field_name_and_type(self, fields):
        type = self.get_random_value(fields.keys())
        field = self.get_random_value(fields[type])
        return field, type

    def get_nested_child_field(self, nested_field):
        if nested_field in self.nested_fields.iterkeys():
            return self.get_random_field_name_and_type(
                self.nested_fields[nested_field])

if __name__ == "__main__":
    import json
    custom_map = CustomMapGenerator(seed=1).get_map()
    print json.dumps(custom_map, indent=3)