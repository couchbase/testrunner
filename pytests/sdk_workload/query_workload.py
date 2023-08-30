import logging
import time
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
    'select x.* from `{}`.`{}`.`{}` x limit 3000;',
    'select meta().id, name, age from `{}`.`{}`.`{}` where age between 0 and 5 limit 3000;',
    'select x.* from `{}`.`{}`.`{}` where age between 0 and 5 x limit 3000;',
    'select age, count(*) from `{}`.`{}`.`{}` group by age order by age limit 3000;',
    'select x.* from `{}`.`{}`.`{}` x limit 3000;',
    'select meta().id, name, age from `{}`.`{}`.`{}` where age between 0 and 5 limit 3000;',
    'select x.* from `{}`.`{}`.`{}` where age between 0 and 5 x limit 3000;',
    'select age, count(*) from `{}`.`{}`.`{}` group by age order by age limit 3000;',
    'select x.* from `{}`.`{}`.`{}` where age between 0 and 5 x limit 3000;',
    'select age, count(*) from `{}`.`{}`.`{}` group by age order by age limit 3000;'
]


class RunQueryWorkload(object):
    def __init__(self):
        self.sdk_client = SDKClient()

    def build_indexes(self, cluster, bucket, scope, collection, counter_obj, retries):
        for index in indexes:
            index_query = index.format(bucket, scope, collection)
            self.sdk_client.run_query(cluster, index_query, counter_obj, retries)

    def run_query(self, cluster, bucket, scope, collection, duration, counter_obj, retries):
        t_end = time.time() + int(duration)
        while time.time() < t_end:
            logging.info("Running queries on {}-{}-{}".format(bucket, scope, collection))
            for q in queries:
                query = q.format(bucket, scope, collection)
                result = self.sdk_client.run_query(cluster, query, counter_obj, retries)
                if not result:
                    logging.CRITICAL("Query failed - {}".format(q))
            time.sleep(5)
