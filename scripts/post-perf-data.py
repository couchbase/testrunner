import sys

sys.path.append('.')
sys.path.append('lib')
from optparse import OptionParser
from couchdbkit import Server
from logger import Logger
import json
from bz2 import BZ2Compressor


log = Logger.get_logger()

def compress(data):
    try:
        bc = BZ2Compressor()
        bc.compress(lg)
        attachment = bc.flush()
        return attachment
    except Exception as ex:
        log.error("unable to bzip data",ex)


if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-n", "--node", dest="node", default="http://127.0.0.1:5984",
                      help="couchdb ip , defaults to 127.0.0.1:5984")
    parser.add_option("-d", "--database",
                      dest="database", help="db name in couchdb",
                      default="tmp")
    parser.add_option("-i", "--input",
                      dest="input", help="json file to post to couchdb",
                      default="test.json")

    options, args = parser.parse_args()

    try:
        server = Server(options.node)
        db = server.get_or_create_db(options.database)
        input_json = json.loads(open(options.input).read())

        log.info("loaded file : {0} into a json document".format(options.input))

        attachments = {}


        if "ns_server_data" in input_json:
            attachments["ns_server_data"] = []
            ns_server_data = input_json["ns_server_data"]

            for item in ns_server_data:
                if "op" in item:
                    samples = item["op"]["samples"]
                    _time = 0
                    _new_samples = []
                    for i in range(0, 59):
                        _new_sample = {}
                        for sample in samples:
                            _new_sample[sample] = samples[sample][i]
                        attachments["ns_server_data"].append(_new_sample)
            del input_json["ns_server_data"]
        if "dispatcher" in input_json:
            attachments["dispatcher"] = input_json["dispatcher"]
            del input_json["dispatcher"]
        if "timings" in input_json:
            attachments["timings"] = input_json["timings"]
            del input_json["timings"]
        if "ops" in input_json:
            attachments["ops"] = input_json["ops"]
            del input_json["ops"]
        if "systemstats" in input_json:
            attachments["systemstats"] = input_json["systemstats"]
            del input_json["systemstats"]
        if "membasestats" in input_json:
            attachments["membasestats"] = input_json["membasestats"]
            del input_json["membasestats"]
        if "latency-get" in input_json:
            attachments["latency-get"] = input_json["latency-get"]
            del input_json["latency-get"]
        if "latency-set" in input_json:
            attachments["latency-set"] = input_json["latency-set"]
            del input_json["latency-set"]


        log.info("attachments has {0} objects".format(len(attachments)))
        res = db.save_doc(input_json,force_update=True)
        doc_id = res["id"]
        rev_id = res["rev"]
        msg = "inserted document with id : {0} and rev : {1}in database {2}"
        log.info(msg.format(doc_id, rev_id, options.database))
        for name in attachments:
            log.info("inserting attachment with name : {0}".format(name))
            db.put_attachment(input_json,attachments[name],name,"text/plain")


    except Exception as ex:
        print ex
        log.error("unable to connect to {0}".format(options.node))



