from .base_query_helper import BaseRQGQueryHelper


class RQGQueryHelper(BaseRQGQueryHelper):

    def _find_hints(self, n1ql_query):
        return super(RQGQueryHelper, self)._find_hints(n1ql_query)

if __name__ == "__main__":
    helper = RQGQueryHelper()
    print(helper._convert_sql_template_to_value_nested_subqueries("CRAP1 TABLE_ALIAS.* CRAP2 TABLE_ALIAS.* FROM TABLE_ALIAS TABLE_ALIAS"))
