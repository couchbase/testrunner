
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
                query.setConnectionTimeout(60000)
                if spatial == True:
                    results = rest.querySpatialView(design_doc_name, view_name, bucket, query)
                else:
                    results = rest.queryView(design_doc_name, view_name, bucket, query)
                if results.get(u'errors', []):
                    assert False, "Query error {0} due to {1}".format(view, results.get(u'errors'))

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
        assert False, "Query {0} failed {1} times. Test Failed".format(view, tries)
