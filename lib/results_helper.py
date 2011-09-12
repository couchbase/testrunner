import urlparse
import uuid
import time

import logger
import couchdb


log = logger.Logger.get_logger()
class ResultsHelper(object):
    def __init__(self, server):
        parsed = urlparse.urlparse(server,"http")
        self.scheme = parsed.scheme
        self.username = parsed.username
        self.password = parsed.password
        self.hostname = parsed.hostname
        if parsed.port:
            self.port = parsed.port
        else:
            self.port = "5984"
        self.dbname = parsed.path[1:]

        couch = couchdb.Server("http://{0}:{1}@{2}:{3}/".format(self.username, self.password, self.hostname, self.port))
        if not self.dbname in couch:
            couch.create(self.dbname)
            # TODO create views
        self.database = couch[self.dbname]


    def add_perf(self, data, log=None):
        if not "timestamp" in data:
            data["timestamp"] = time.time()
        if not "product" in data:
            data["product"] = "couchbase"
        data["type"] = "performance"
        docid, revid = self.database.save(data)
        if log:
            self.database.put_attachment(docid, log, filename="log", content_type="text/plain")
        return docid


    def compare_perf(self, data, limit, same_keys, different_keys, result_key):
        # returns a list with the percent different for key (ex: "average_ops")
        # between the passed in data and stored data
        # data -- new data that we are testing
        # limit -- limit number of previous results to compare
        # same_keys -- list of keys that must match between new and old
        # different_keys -- dictionary of keys:values that the old data must match
        # result_key -- key that will be compared between new and old

        constraints = "doc.type == 'performance'"
        for key in same_keys:
            if constraints:
                constraints += " && "
            value = data[key]
            if isinstance(value, bool):
                if value == True:
                    constraint = "doc."+key+" = true"
                else:
                    constraint = "doc."+key+" != true"
            elif isinstance(value, int) or isinstance(value, float):
                constraint = "doc."+key+" == " + str(value)
            elif isinstance(value, str):
                constraint = "doc."+key+" == '" + value + "'"
            elif not value:
                constraint = "doc."+key+" == null"
            else:
                constraint = "doc."+key+" == '" + str(value) + "'"
            constraints += constraint

        for key,value in different_keys.iteritems():
            if constraints:
                constraints += " && "
            if isinstance(value, bool):
                if value == True:
                    constraint = "doc."+key+" = true"
                else:
                    constraint = "doc."+key+" != true"
            elif isinstance(value, int) or isinstance(value, float):
                constraint = "doc."+key+" == " + str(value)
            elif isinstance(value, str):
                constraint = "doc."+key+" == '" + value + "'"
            elif not value:
                constraint = "doc."+key+" == null"
            else:
                constraint = "doc."+key+" == '" + str(value) + "'"
            constraints += constraint

        map_fun = '''function(doc) {
  if ('''+constraints+''') {
    emit(doc.timestamp, doc.'''+result_key+''');
  }
}'''
        log.info(map_fun)
        percent_change = []
        if limit:
            rows = self.database.query(map_fun, limit=limit, descending=True)
        else:
            rows = self.database.query(map_fun, descending=True)
        for row in rows:
            percent_change.append(float(data[result_key])/float(row.value))
            log.info("{0} (new / old): {1} / {2} -> {3}".format(result_key, data[result_key], row.value, float(data[result_key])/float(row.value)))

        return percent_change
