import string
import random


class RepeatableGenerator(object):

    def __init__(self, iterable):
        self.reset()
        self.iterable = iterable

    def __next__(self):
        self.counter += 1
        if self.counter == len(self.iterable):
            self.counter = 0
        return self.iterable[self.counter]

    def reset(self):
        self.counter = -1


class ViewGen(object):

    ddocs = RepeatableGenerator(tuple(string.ascii_uppercase))

    views = RepeatableGenerator((
        "id_by_city",
        "name_and_email_by_category_and_and_coins",
        "id_by_realm_and_coins",
        "name_and_email_by_city",
        "name_by_category_and_and_coins",
        "experts_id_by_realm_and_coins",
        "id_by_realm",
        "achievements_by_category_and_and_coins",
        "name_and_email_by_realm_and_coins",
        "experts_coins_by_name"
    ))

    maps = {
        "id_by_city":
            """
            function(doc, meta) {
                emit(doc.city, null);
            }
            """,
        "name_and_email_by_city":
            """
            function(doc, meta) {
                emit(doc.city, [doc.name, doc.email]);
            }
            """,
        "id_by_realm":
            """
            function(doc, meta) {
                emit(doc.realm, null);
            }
            """,
        "experts_coins_by_name":
            """
            function(doc, meta) {
                if (doc.category === 2) {
                    emit(doc.name, doc.coins);
                }
            }
            """,
        "name_by_category_and_and_coins":
            """
            function(doc, meta) {
                emit([doc.category, doc.coins], doc.name);
            }
            """,
        "name_and_email_by_category_and_and_coins":
            """
            function(doc, meta) {
                emit([doc.category, doc.coins], [doc.name, doc.email]);
            }
            """,
        "achievements_by_category_and_and_coins":
            """
            function(doc, meta) {
                emit([doc.category, doc.coins], doc.achievements);
            }
            """,
        "id_by_realm_and_coins":
            """
            function(doc, meta) {
                emit([doc.realm, doc.coins], null)
            }
            """,
        "name_and_email_by_realm_and_coins":
            """
            function(doc, meta) {
                emit([doc.realm, doc.coins], [doc.name, doc.email]);
            }
            """,
        "experts_id_by_realm_and_coins":
            """
            function(doc, meta) {
                if (doc.category === 2) {
                    emit([doc.realm, doc.coins], null);
                }
            }
            """
    }

    baseq = '/{bucket}/_design/{ddoc}/_view/{view}'

    queries = {
        "id_by_city":
            baseq + '?key="{{city}}"&limit=30',
        "name_and_email_by_city":
            baseq + '?key="{{city}}"&limit=30',
        "id_by_realm":
            baseq + '?startkey="{{realm}}"&limit=30',
        "experts_coins_by_name":
            baseq + '?startkey="{{name}}"&descending=true&limit=30',
        "name_by_category_and_and_coins":
            baseq + '?startkey=[{{category}},0]&endkey=[{{category}},{{coins}}]&limit=30',
        "name_and_email_by_category_and_and_coins":
            baseq + '?startkey=[{{category}},0]&endkey=[{{category}},{{coins}}]&limit=30',
        "achievements_by_category_and_and_coins":
            baseq + '?startkey=[{{category}},0]&endkey=[{{category}},{{coins}}]&limit=30',
        "id_by_realm_and_coins":
            baseq + '?startkey=["{{realm}}",{{coins}}]&endkey=["{{realm}}",10000]&limit=30',
        "name_and_email_by_realm_and_coins":
            baseq + '?startkey=["{{realm}}",{{coins}}]&endkey=["{{realm}}",10000]&limit=30',
        "experts_id_by_realm_and_coins":
            baseq + '?startkey=["{{realm}}",{{coins}}]&endkey=["{{realm}}",10000]&limit=30'
    }

    queries_by_type = {  # 45%/30%/25%
        "id_by_city": 9,
        "name_and_email_by_category_and_and_coins": 6,
        "id_by_realm_and_coins": 5,
        "name_and_email_by_city": 9,
        "name_by_category_and_and_coins": 6,
        "experts_id_by_realm_and_coins": 5,
        "id_by_realm": 9,
        "achievements_by_category_and_and_coins": 6,
        "name_and_email_by_realm_and_coins": 5,
        "experts_coins_by_name": 9
    }

    def generate_ddocs(self, pattern=None, options=None):
        """Generate dictionary with design documents and views.
        Pattern looks like:
            [8, 8, 8] -- 8 ddocs (8 views, 8 views, 8 views)
            [2, 2, 4] -- 3 ddocs (2 views, 2 views, 4 views)
            [8] -- 1 ddoc (8 views)
            [1, 1, 1, 1] -- 4 ddocs (1 view per ddoc)
        """
        if [v for v in pattern if v > 10]:
            raise Exception("Maximum 10 views per ddoc allowed")
        if len(pattern) > 10:
            raise Exception("Maximum 10 design documents allowed")

        ddocs = dict()
        for number_of_views in pattern:
            ddoc_name = next(self.ddocs)
            ddocs[ddoc_name] = {'views': {}}
            for index_of_view in range(number_of_views):
                view_name = next(self.views)
                map = self.maps[view_name]
                ddocs[ddoc_name]['views'][view_name] = {'map': map}
            if options:
                ddocs[ddoc_name]["options"] = options
        self.ddocs.reset()
        self.views.reset()
        return ddocs

    def _get_query(self, bucket, ddoc, view, stale):
        """Generate template-based query"""
        template = self.queries[view]
        query = template.format(bucket=bucket, ddoc=ddoc, view=view)
        if stale == "ok":
            query += "&stale=ok"
        elif stale == "false":
            query += "&stale=false"
        return (query, ) * self.queries_by_type[view]

    def generate_queries(self, ddocs, bucket='default', stale="update_after"):
        """Generate string from permuted queries"""
        # Generate list of queries
        queries = (self._get_query(bucket, ddoc, view, stale)
                   for ddoc, ddoc_definition in ddocs.items()
                   for view in ddoc_definition["views"])
        queries = [query for query_group in queries for query in query_group]

        # Deterministic shuffle
        random.seed("".join(queries))
        random.shuffle(queries)

        # Joined queries
        return ';'.join(queries)
