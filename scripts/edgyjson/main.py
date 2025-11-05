import argparse
import json
import logging
import os
import random
import sys
import traceback

from couchbase.cluster import Cluster

import couchbase.subdocument as SD

from couchbase.exceptions import CouchbaseException

from .constants import Constants as constants
from .ValueGenerator import ValueGenerator

# Example usage: python main.py -ip 192.168.56.111 -u Administrator -p password -b default -n 5
class JSONDoc(object):
    def __init__(self, server=None, username=None, password=None, bucket=None, startseqnum=1, randkey=False,
                 keyprefix="edgyjson-",
                 encoding="utf-8", num_docs=1, template="mix.json", xattrs=False, filter=False, load=True):
        self.startseqnum = startseqnum
        self.randkey = randkey
        self.keyprefix = keyprefix
        self.encoding = encoding
        self.num_docs = num_docs
        self.template_file = template
        self.xattrs = xattrs
        self.filter = filter
        logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
        local = False
        if not server or len(server) < 2:
            local = True
        else:
            self.server = server
            self.username = username
            self.password = password
            self.bucket = bucket
        self.createContent()
        if load:
            if local:
                logging.info("No valid server provided. Generating docs locally in ./output dir")
                self.printDoc()
            else:
                self.uploadDoc()

    def uploadDoc(self):
        # connect to cb cluster
        cb = None
        connection = "couchbase://" + self.server
        if "ip6" in self.server or self.server.startswith("["):
            connection = connection + "?ipv6=allow"
        try:
            from couchbase.auth import PasswordAuthenticator
            from couchbase.options import ClusterOptions
            cluster = Cluster(connection, ClusterOptions(
                PasswordAuthenticator(self.username, self.password)))
            cb = cluster.bucket(self.bucket).default_collection()
        except Exception:
            logging.error("Connection error\n" + traceback.format_exc())
        finally:
            if not cb:
                logging.error("Connection error: Failed to establish connection")
        json_docs = {}
        for i in range(self.startseqnum, self.startseqnum + self.num_docs):
            self.createContent()
            dockey = self.keyprefix + str(i)
            json_docs[dockey] = self.json_objs_dict

        BYTES_PER_BATCH = 1024 * 256  # 256K
        batches = []
        cur_batch = {}
        cur_size = 0
        batches.append(cur_batch)

        for key, value in list(json_docs.items()):
            cur_batch[key] = value
            cur_size += len(key) + len(value) + 24
            if cur_size > BYTES_PER_BATCH:
                cur_batch = {}
                batches.append(cur_batch)
                cur_size = 0

        num_completed = 0
        while batches:
            batch = batches[-1]
            try:
                cb.upsert_multi(batch)
                num_completed += len(batch)
                batches.pop()
            except CouchbaseException as e:
                logging.error(e)
                ok, fail = e.split_results()
                new_batch = {}
                for key in fail:
                    new_batch[key] = list(json_docs.items())[key]
                batches.pop()
                batches.append(new_batch)
                num_completed += len(ok)
                logging.info("Retrying {}/{} items".format(len(new_batch), len(ok)))
            logging.info("Completed {}/{} items".format(num_completed, len(json_docs)))
        if self.xattrs:
            logging.info("Upserting xattrs")
            self.add_xattrs(cb)
        if self.filter:
            logging.info("Upserting filter_expressions.json")
            cb.upsert("filter_expressions", self.filters_json_objs_dict)

    def add_xattrs(self, cb):
        num_xattr_docs = 10 if self.num_docs > 10 else self.num_docs
        for val in range(self.startseqnum, self.startseqnum + num_xattr_docs):
            dockey = self.keyprefix + str(val)
            cb.mutate_in(dockey, SD.upsert("xattr1", val, xattr=True, create_parents=True))
            cb.mutate_in(dockey,
                         SD.upsert("xattr2", {'field1': val, 'field2': val * val}, xattr=True, create_parents=True))
            cb.mutate_in(dockey, SD.upsert('xattr3', {'field1': {'sub_field1a': val, 'sub_field1b': val * val},
                                                      'field2': {'sub_field2a': 2 * val, 'sub_field2b': 2 * val * val}},
                                           xattr=True, create_parents=True))
        logging.info("Added xattrs to {0} docs".format(num_xattr_docs))

    def printDoc(self):
        for i in range(self.startseqnum, self.startseqnum + self.num_docs):
            logging.info("generating doc: " + str(i))
            self.createContent()
            try:
                current_dir = os.path.dirname(__file__)
                dockey = self.keyprefix + str(i) + ".json"
                output = os.path.join(current_dir, "output/", dockey)
                with open(output, 'w') as f:
                    f.write(json.dumps(self.json_objs_dict, indent=3).encode(self.encoding, "ignore"))
                logging.info("print: " + dockey)
            except Exception as e:
                logging.error("Print error\n" + traceback.format_exc())
            if self.filter:
                try:
                    current_dir = os.path.dirname(__file__)
                    output = os.path.join(current_dir, "output/", "filter_expressions.json")
                    with open(output, 'w') as f:
                        f.write(json.dumps(self.filters_json_objs_dict, indent=3).encode(self.encoding, "ignore"))
                    logging.info("print: filter_expressions.json")
                except Exception as e:
                    logging.error("Print error\n" + traceback.format_exc())

    def createContent(self):
        self.json_objs_dict = {}
        self.filters_json_objs_dict = {}
        if self.filter:
           self.filters_json_objs_dict["filter_expressions"] = []
        try:
            current_dir = os.path.dirname(__file__)
            template = open(os.path.join(current_dir, "templates/", self.template_file))
            content = json.load(template)
            template.close()
        except IOError:
            logging.error("Unable to find template file , data not loaded!")

        valuegen = ValueGenerator()
        for key, value in list(content.items()):
            if key in list(constants.generator_methods.keys()):
                if value:
                    argsdict = value
                else:
                    argsdict = {}
                val, filter_exp = getattr(valuegen, constants.generator_methods[key])(**argsdict)
                if self.randkey:
                    func_list = [func for func in dir(valuegen) if
                                 callable(getattr(valuegen, func)) and not func.startswith("__")]
                    key = getattr(globals()['ValueGenerator'](), random.choice(func_list))()
                    # Discard rand keys that are lists or dicts
                    while isinstance(key, list) or isinstance(key, dict):
                        key = getattr(globals()['ValueGenerator'](), random.choice(func_list))()
            self.json_objs_dict[key] = val
            if self.filter:
                self.filters_json_objs_dict[key.split('-')[0]] = filter_exp

    def parseargs(self, argsstr):
        argsindex = argsstr.split(',')
        argsdict = dict()
        for item in argsindex:
            # Separate the arg name and value
            argname, argvalue = item.split('=')
            # Add it to the dictionary
            argsdict.update({argname: argvalue})
        return argsdict


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--server', '-ip',
                        help='IP address of cb server to upload docs to. If not provided, docs will be generated locally at ./output dir',
                        type=str)
    parser.add_argument('--user', '-u', help='Server username', type=str)
    parser.add_argument('--password', '-p', help='Server password', type=str)
    parser.add_argument('--bucket', '-b', help='Bucket name', type=str)
    parser.add_argument('--startseqnum', '-s', help="Starting document ID number", type=int, default=1)
    parser.add_argument('--keyprefix', '-kp', help='Document ID prefix', type=str, default="edgyjson-")
    parser.add_argument('--randkey', '-rk', help="Randomize keys", type=bool, default=False)
    parser.add_argument('--encoding', '-e', help="JSON Encoding format : utf-8, utf-16, utf-32", type=str,
                        default="utf-8")
    parser.add_argument('--numdocs', '-n', help="Number of documents to generate", type=int, default=1)
    parser.add_argument('--template', '-t', help="JSON Template File. Should be placed in ./templates dir", type=str,
                        default="mix.json")
    parser.add_argument('--xattrs', '-x', help="Add xattrs?", type=bool, default=False)
    parser.add_argument('--filter', '-f', help="Generate filters", type=bool, default=False)
    args = parser.parse_args()
    if args.server and (args.user is None or args.password is None or args.bucket is None):
        logging.error("username, password and bucket name are required")
        sys.exit(0)
    JSONDoc(args.server, args.user, args.password, args.bucket, args.startseqnum, args.randkey, args.keyprefix,
            args.encoding, args.numdocs,
            args.template, args.xattrs)
