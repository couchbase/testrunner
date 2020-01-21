class Constants:

    generator_methods = {
      "array-dates": "array_dates",
      "array_empty": "empty",
      "array-literals": "array_literals",
      "array-mix": "array_mix",
      "array-numbers": "array_numbers",
      "array-strings": "array_strings",
      "bool": "rand_bool",
      "date": "date_time",
      "doc-empty": "empty",
      "doc-sub": "sub_doc",
      "float": "rand_float",
      "float-negative": "rand_float",
      "int": "rand_int",
      "int-negative": "rand_int",
      "null": "rand_null",
      "string-empty": "empty",
      "string-long": "rand_string",
      "string-medium": "rand_string",
      "string-short": "rand_string",
      "string-number": "string_num",
      "string-reserved": "rand_reserved",
      "string-vlong": "rand_string"
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