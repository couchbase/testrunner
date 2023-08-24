from pytests.sdk_workload.client_sdk import SDKClient

indexes = [
    'CREATE PRIMARY INDEX ON `{}`.`{}`.`{}`;',
    'CREATE INDEX ix_name ON `{}`.`{}`.`{}` (name);',
    'CREATE INDEX ix_age ON `{}`.`{}`.`{}` (age);',
    'CREATE INDEX ix_name_age ON `{}`.`{}`.`{}` (name, age);',
    'CREATE INDEX ix_age_over_age on `{}`.`{}`.`{}`(age) where age between 0 and 5;',
    'BUILD INDEX ON `{}`.`{}`.`{}`(`#primary`, `ix_name`, `ix_email`, `ix_age_over_age`)"'
]

queries = [
    'select x.* from `{}`.`{}`.`{}` x limit 100;',
    'select meta().id, name, age from `{}`.`{}`.`{}` where age between 0 and 5 limit 100;',
    'select x.* from `{}`.`{}`.`{}` where age between 0 and 5 x limit 100;',
    'select age, count(*) from `{}`.`{}`.`{}` group by age order by age limit 100;'
]


class RunQueryWorkload(object):
    def __init__(self):
        self.sdk_client = SDKClient()

    def build_indexes(self, cluster, bucket, scope, collection, counter_obj):
        for index in indexes:
            index_query = index.format(bucket, scope, collection)
            self.sdk_client.run_query(cluster, index_query, counter_obj)

    def run_query(self, cluster, bucket, scope, collection, counter_obj):
        for q in queries:
            query = q.format(bucket, scope, collection)
            self.sdk_client.run_query(cluster, query, counter_obj)
