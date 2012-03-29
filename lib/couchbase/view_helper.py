
import logger
import time

class ViewHelper():

    @staticmethod
    def queryViewUntilNotEmpty(rest, design_doc_name, view_name, bucket, query, spatial=False,
                               interval=10, tries=40):
        log = logger.Logger.get_logger()
        for i in range(0, tries):
            try:
                start = time.time()
                query["connectionTimeout"] = 60000
                results = rest.query_view(design_doc_name, view_name, bucket, query)
                if results.get(u'errors', []):
                    assert False, "Query error {0} due to {1}".format(view_name, results.get(u'errors'))

                duration = time.time() - start
                if results.get(u'rows', []) or results.get(u'total_rows', 0) > 0:
                    log.info("Query took {0} sec, {1} tries".format(duration, (i + 1)))
                    return results, duration
                else:
                    log.info("Query empty after {0} sec, {1} tries".format(duration, (i + 1)))
                    time.sleep(interval)
            except Exception as ex:
                log.error("View not ready, retry in {0} sec. Error: {1}".format(interval, ex))
                time.sleep(interval)
                continue
        assert False, "Query {0} failed {1} times. Test Failed".format(view_name, tries)

    @staticmethod
    def queryViewWaitForKeys(rest, design_doc_name, view_name, bucket, query, expected_keys,
                             spatial=False, interval=10, timeout=600):
        log = logger.Logger.get_logger()
        st = time.time()
        total_time = 0
        while (time.time() - st) < timeout:
            result, duration = ViewHelper.queryViewUntilNotEmpty(rest, view_name, view_name,
                                                                 bucket, query, spatial)
            keys = result.getKeys()
            if len(keys) >= expected_keys:
                return result, (time.time() - st)
            else:
                log.info("Query returned {0} items, expected {1}".format(len(keys), expected_keys))
                log.info("Retrying query in {0} seconds".format(interval))
                time.sleep(interval)
        assert False, "Query ({0}) timeout {1}. Got {2}/{3} keys".format(view_name, timeout,
                                                                         len(keys), expected_keys)
