generator_methods = {
    # primitives
    "bool": "rand_bool",
    "null": "rand_null",
    "int": "rand_int",
    "int-negative": "rand_int",
    "float": "rand_float",
    "float-negative": "rand_float",
    "string-short": "rand_string",
    "string-medium": "rand_string",
    "string-long": "rand_string",
    "string-vlong": "rand_string",
    "string-empty": "empty",
    "string-number": "string_num",
    # arrays
    "array-strings": "array_strings",
    "array-numbers": "array_numbers",
    "array-empty": "empty",
    # doc
    "doc-sub": "sub_doc",
    "doc-empty": "empty",
    # misc
    "string-reserved": "rand_reserved",
    "date": "date_time",
    "tabs": "tabs",
    "name": "rand_name"
}

niql_reserved_keywords = [
            'ALL', 'ALTER', 'ANALYZE', 'AND', 'ANY', 'ARRAY', 'AS', 'ASC', \
            'BEGIN', 'BETWEEN', 'BINARY', 'BOOLEAN', 'BREAK', 'BUCKET', 'BUILD', 'BY', \
            'CALL', 'CASE', 'CAST', 'CLUSTER', 'COLLATE', 'COLLECTION', 'COMMIT', 'CONNECT', 'CONTINUE', 'CORRELATE',
            'COVER', \
            'CREATE', \
            'DATABASE', 'DATASET', 'DATASTORE', 'DECLARE', 'DECREMENT', 'DELETE', 'DERIVED', 'DESC', 'DESCRIBE',
            'DISTINCT', \
            'DO', 'DROP', \
            'EACH', 'ELEMENT', 'ELSE', 'END', 'EVERY', 'EXCEPT', 'EXCLUDE', 'EXECUTE', 'EXISTS', 'EXPLAIN', \
            'FALSE', 'FETCH', 'FIRST', 'FLATTEN', 'FOR', 'FORCE', 'FROM', 'FUNCTION', \
            'GRANT', 'GROUP', 'GSI', \
            'HAVING', \
            'IF', 'IGNORE', 'ILIKE', 'IN', 'INCLUDE', 'INCREMENT', 'INDEX', 'INFER', 'INLINE', 'INNER', 'INSERT',
            'INTERSECT', \
            'INTO', 'IS', \
            'JOIN', \
            'KEY', 'KEYS', 'KEYSPACE', 'KNOWN', \
            'LAST', 'LEFT', 'LET', 'LETTING', 'LIKE', 'LIMIT', 'LSM', \
            'MAP', 'MAPPING', 'MATCHED', 'MATERIALIZED', 'MERGE', 'MINUS', 'MISSING', \
            'NAMESPACE', 'NEST', 'NOT', 'NULL', 'NUMBER', \
            'OBJECT', 'OFFSET', 'ON', 'OPTION', 'OR', 'ORDER', 'OUTER', 'OVER', 'PARSE', 'PARTITION', 'PASSWORD',
            'PATH',
            'POOL', \
            'PREPARE', 'PRIMARY', 'PRIVATE', 'PRIVILEGE', 'PROCEDURE', 'PUBLIC', \
            'RAW', 'REALM', 'REDUCE', 'RENAME', 'RETURN', 'RETURNING', 'REVOKE', 'RIGHT', 'ROLE', 'ROLLBACK', \
            'SATISFIES', 'SCHEMA', 'SELECT', 'SELF', 'SEMI', 'SET', 'SHOW', 'SOME', 'START', 'STATISTICS', 'STRING',
            'SYSTEM', \
            'THEN', 'TO', 'TRANSACTION', 'TRIGGER', 'TRUE', 'TRUNCATE', \
            'UNDER', 'UNION', 'UNIQUE', 'UNKNOWN', 'UNNEST', 'UNSET', 'UPDATE', 'UPSERT', 'USE', 'USER', 'USING', \
            'VALIDATE', 'VALUE', 'VALUED', 'VALUES', 'VIA', 'VIEW', \
            'WHEN', 'WHERE', 'WHILE', 'WITH', 'WITHIN', \
            'WORK', \
            'XOR'
        ]

empty ={
    "string": "",
    "array": [],
    "doc": {}
}