#!/usr/bin/python

# Archive pdf graphs to couchdb

import time
from os import path
from optparse import OptionParser

from couchdbkit import Server


class DocGenerator:

    def __init__(self):
        self._filename = "unknown"
        self._test = "unknown"
        self._base = "unknown"
        self._target = "unknown"
        self._run_id = "unknown"
        self._time = time.strftime('%b-%d-%G_%X')
        self._unix_time = time.time()
        self._category = "unknown"

    def create_doc(self, filename):

        if not filename or not self._locate_file(filename):
            print "unable to create doc, invalid filename: {0}".format(filename)
            return None

        self._filename = path.basename(filename)

        try:
            self._test, self._base, self._target, self._run_id, self._time \
                = self._filename.split("_", 4)
            self._time = self._time.split(".")[0]
            self._category = self._test.split("-", 1)[0]
        except Exception as e:
            print "cannot parse filename \"{0}\" : {1}".format(filename, str(e))

        print self.to_string()

        doc = {"filename": self._filename,
               "test": self._test,
               "create_time": self._time,
               "category": self._category,
               "base_build": self._base,
               "target_build": self._target,
               "run_id": self._run_id,
               "upload_time": self._unix_time}

        return doc

    def _locate_file(self, filename):
        return filename and path.exists(filename)

    def to_string(self):
        return "filename: {0}, test: {1}, base_build = {2}, " \
               "target_build = {3}, run_id = {4}, time = {5}, category = {6}" \
               .format(self._filename, self._test, self._base,
                       self._target, self._run_id, self._time, self._category)


class GraphPoster:

    def __init__(self, node, db):
        server = Server(node)
        self._db = server.get_or_create_db(db)

    def post_to_couchdb(self, filename):

        self.doc = DocGenerator().create_doc(filename)

        if self.doc:
            self.res = self._db.save_doc(self.doc, force_update=True)
            if self.res:
                print "posted pdf to couchdb: " + \
                    "filename = {0}, doc_id = {1}, rev = {2} " \
                    .format(filename, self.res["id"], self.res["rev"])

            with open(filename, 'r') as f:
                self._db.put_attachment(self.doc, f, filename,
                                        "application/pdf")


if __name__ == "__main__":

    parser = OptionParser()
    parser.add_option("-n", "--node", dest="node",
                      default="http://127.0.0.1:5984",
                      help="couchdb ip:port , defaults to 127.0.0.1:5984")
    parser.add_option("-d", "--database", dest="db",
                      default="eperf-graphs", help="db name in couchdb")
    parser.add_option("-f", "--filename", dest="filename",
                      default="test.pdf",
                      help="pdf file to be posted to couchdb")
    options, args = parser.parse_args()

    poster = GraphPoster(options.node, options.db)
    poster.post_to_couchdb(options.filename)
