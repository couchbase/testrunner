import json
import string
import random
from random import choice
from string import ascii_uppercase
from string import ascii_lowercase
from string import digits
import gzip

from TestInput import TestInputSingleton
from testconstants import DEWIKI, ENWIKI, ESWIKI, FRWIKI, NAPADATASET, GEOJSONDATASET

from lib.Cb_constants.CBServer import CbServer
from .data import FIRST_NAMES, LAST_NAMES, DEPT, LANGUAGES


class KVGenerator(object):
    def __init__(self, name, start, end):
        self.name = name
        try:
            self.start = int(start)
        except ValueError:
            self.start = 0
            pass

        try:
            self.end = int(end)
        except ValueError:
            self.end = 0
            pass

        try:
            self.itr = int(start)
        except ValueError:
            self.itr = 0
            pass

    def has_next(self):
        try:
            self.itr = int(self.itr)
        except ValueError:
            self.itr = 0
            pass
        try:
            self.end = int(self.end)
        except ValueError:
            self.end = 0
            pass

        return self.itr < self.end

    def __next__(self):
        raise NotImplementedError

    def reset(self):
        self.itr = self.start

    def __iter__(self):
        return self

    def __len__(self):
        return self.end - self.start

    # is a python generator function
    def isGenerator(self):
        return True


class DocumentGenerator(KVGenerator):
    """ An idempotent document generator."""

    def __init__(self, name, template, *args, **kwargs):
        """Initializes the document generator

        Example:
        Creates 10 documents, but only iterates through the first 5.

        age = range(5)
        first = ['james', 'sharon']
        template = '{{ "age": {0}, "first_name": "{1}" }}'
        gen = DocumentGenerator('test_docs', template, age, first, start=0, end=5)

        Args:
            name: The key name prefix
            template: A formated string that can be used to generate documents
            *args: Each argument is list for the corresponding parameter in the template. In the above example age[2]
                   appears in the 3rd document
            *kwargs: Special constrains for the document generator, currently start and end are supported
        """
        self.args = args
        self.template = template

        size = 0
        if not len(self.args) == 0:
            size = 1
            for arg in self.args:
                size *= len(arg)

        KVGenerator.__init__(self, name, 0, size)

        if 'start' in kwargs:
            self.start = kwargs['start']
            self.itr = kwargs['start']

        if 'end' in kwargs:
            self.end = kwargs['end']

    """Creates the next generated document and increments the iterator.

    Returns:
        The document generated"""

    def __next__(self):
        if self.itr >= self.end:
            raise StopIteration
        seed = self.itr
        doc_args = []
        for arg in self.args:
            value = arg[int(seed % len(arg))]
            doc_args.append(value)
            seed //= len(arg)
        doc = self.template.format(*doc_args).replace('\'', '"').replace('True',
                                                                         'true').replace('False', 'false').replace('\\',
                                                                                                                   '\\\\')

        json_doc = json.loads(doc)

        if self.name == "random_keys":
            """ This will generate a random ascii key with 12 characters """
            json_doc['_id'] = ''.join(choice(ascii_uppercase + ascii_lowercase + digits) \
                                      for i in range(12))
        else:
            json_doc['_id'] = self.name + '-' + str(self.itr)
        self.itr += 1
        # print("-->json dumps#{}:{}:{}".format(self.itr, json_doc['_id'],jsondumps))
        return json_doc['_id'], json.dumps(json_doc)


class SubdocDocumentGenerator(KVGenerator):
    """ An idempotent document generator."""

    def __init__(self, name, template, *args, **kwargs):
        """Initializes the document generator

        Example:
        Creates 10 documents, but only iterates through the first 5.

        age = range(5)
        first = ['james', 'sharon']
        template = '{{ "age": {0}, "first_name": "{1}" }}'
        gen = DocumentGenerator('test_docs', template, age, first, start=0, end=5)

        Args:
            name: The key name prefix
            template: A formated string that can be used to generate documents
            *args: A list for each argument in the template
            *kwargs: Special constrains for the document generator
        """
        self.args = args
        self.template = template

        print(type(self.template))

        size = 0
        if not len(self.args) == 0:
            size = 1
            for arg in self.args:
                size *= len(arg)

        KVGenerator.__init__(self, name, 0, size)

        if 'start' in kwargs:
            self.start = kwargs['start']
            self.itr = kwargs['start']

        if 'end' in kwargs:
            self.end = kwargs['end']

    """Creates the next generated document and increments the iterator.

    Returns:
        The document generated"""

    def __next__(self):
        if self.itr >= self.end:
            raise StopIteration

        self.itr += 1
        return self.name + '-' + str(self.itr), json.dumps(self.template).encode("ascii", "ignore")


class BlobGenerator(KVGenerator):
    def __init__(self, name, seed, value_size, start=0, end=10000):
        KVGenerator.__init__(self, name, start, end)
        self.seed = seed
        self.value_size = value_size
        self.itr = self.start

    def __next__(self):
        if self.itr >= self.end:
            raise StopIteration

        if self.name == "random_keys":
            key = ''.join(choice(ascii_uppercase + ascii_lowercase + digits) for i in range(12))
        else:
            key = self.name + str(self.itr)
        if self.value_size == 1:
            value = random.choice(string.letters)
        else:
            value = self.seed + str(self.itr)
            extra = self.value_size - len(value)
            if extra > 0:
                value += 'a' * extra
        self.itr += 1
        return key, value


class BatchedDocumentGenerator(object):

    def __init__(self, document_generator, batch_size_int=100):
        self._doc_gen = document_generator
        self._batch_size = batch_size_int
        if self._batch_size <= 0:
            raise ValueError("Invalid Batch size {0}".format(self._batch_size))

    def has_next(self):
        return self._doc_gen.has_next()

    def next_batch(self):
        count = 0
        key_val = {}
        while count < self._batch_size and self.has_next():
            key, val = next(self._doc_gen)
            key_val[key] = val
            count += 1
        return key_val

    # is a python generator function
    def isGenerator(self):
        return True

class JSONNonDocGenerator(KVGenerator):
    """
    Values can be arrays, integers, strings
    """
    def __init__(self, name, values, start=0, end=10000):
        KVGenerator.__init__(self, name, start, end)
        self.values = values
        self.itr = self.start

    def __next__(self):
        if self.itr >= self.end:
            raise StopIteration

        key = self.name + str(self.itr)
        index = self.itr
        while index > len(self.values):
            index = index - len(self.values)
        value = json.dumps(self.values[index - 1])
        self.itr += 1
        return key, value

class Base64Generator(KVGenerator):
    def __init__(self, name, values, start=0, end=10000):
        KVGenerator.__init__(self, name, start, end)
        self.values = values
        self.itr = self.start

    def __next__(self):
        if self.itr >= self.end:
            raise StopIteration

        key = self.name + str(self.itr)
        index = self.itr
        while index > len(self.values):
            index = index - len(self.values)
        value = self.values[index - 1]
        self.itr += 1
        return key, value


class JsonDocGenerator(KVGenerator):

    def __init__(self, name, op_type="create", encoding="utf-8", *args, **kwargs):
        """Initializes the JSON document generator
        gen =  JsonDocGenerator(prefix, encoding="utf-8",start=0,end=num_items)

        Args:
            prefix: prefix for key
            encoding: utf-8/ascii/utf-16 encoding of JSON doc
            *args: A list for each argument in the template
            *kwargs: Special constrains for the document generator

        Sample doc:
                    {
                      "salary": 75891.68,
                      "name": "Safiya Morgan",
                      "dept": "Support",
                      "is_manager": true,
                      "mutated": 0,
                      "join_date": "1984-05-22 07:28:00",
        optional-->   "manages": {
                        "team_size": 6,
                        "reports": [
                          "Basha Taylor",
                          "Antonia Cox",
                          "Winta Campbell",
                          "Lilith Scott",
                          "Beryl Miller",
                          "Ambika Reed"
                        ]
                      },
                      "languages_known": [
                        "English",
                        "Spanish",
                        "German"
                      ],
                      "emp_id": 10000001,
                      "email": "safiya_1@mcdiabetes.com"
                    }
        """
        self.args = args
        self.name = name
        self.gen_docs = {}
        self.encoding = encoding
        self.vector_search = TestInputSingleton.input.param("vector_search", False)
        self.llm_model = TestInputSingleton.input.param("llm_model", "all-MiniLM-L6-v2")
        if self.vector_search:
            fileobj = open("lib/couchbase_helper/vector/vector_data")
            self.learnings = fileobj.readlines()
            self.learnings = [line.strip() for line in self.learnings]

        size = 0
        if not len(self.args) == 0:
            size = 1
            for arg in self.args:
                size *= len(arg)

        KVGenerator.__init__(self, name, 0, size)
        random.seed(0, 1)

        if 'start' in kwargs:
            self.start = int(kwargs['start'])
            self.itr = int(kwargs['start'])
        if 'end' in kwargs:
            self.end = int(kwargs['end'])

        if op_type == "create":
            for count in range(self.start + 1, self.end + 1, 1):
                emp_name = self.generate_name()
                doc_dict = {
                    'emp_id': str(10000000 + int(count)),
                    'name': emp_name,
                    'dept': self.generate_dept(),
                    'email': "%s@mcdiabetes.com" %
                             (emp_name.split(' ')[0].lower()),
                    'salary': self.generate_salary(),
                    'join_date': self.generate_join_date(),
                    'languages_known': self.generate_lang_known(),
                    'is_manager': bool(random.getrandbits(1)),
                    'mutated': 0,
                    'type': 'emp'
                }
                if self.vector_search:
                    lg, l_vector = self.generate_random_learnings()
                    doc_dict['learnings'] = lg
                    doc_dict['l_vector'] = l_vector
                if doc_dict["is_manager"]:
                    doc_dict['manages'] = {'team_size': random.randint(5, 10)}
                    doc_dict['manages']['reports'] = []
                    for _ in range(0, doc_dict['manages']['team_size']):
                        doc_dict['manages']['reports'].append(self.generate_name())

                self.gen_docs[count - 1] = doc_dict
        elif op_type == "delete":
            # for deletes, just keep/return empty docs with just type field
            for count in range(self.start, self.end):
                self.gen_docs[count] = {'type': 'emp'}

    def update(self, fields_to_update=None):
        """
            Updates the fields_to_update in the document.
            @param fields_to_update is usually a list of fields one wants to
                   regenerate in a doc during update. If this is 'None', by
                   default for this dataset, 'salary' field is regenerated.
        """
        random.seed(1, 1)
        for count in range(self.start, self.end):
            doc_dict = self.gen_docs[count]
            if fields_to_update is None:
                doc_dict['salary'] = self.generate_salary()
            else:
                if 'salary' in fields_to_update:
                    doc_dict['salary'] = self.generate_salary()
                if 'l_vector' in fields_to_update:
                    lg, l_vector = self.generate_random_learnings()
                    doc_dict['learnings'] = lg
                    doc_dict['l_vector'] = l_vector
                if 'dept' in fields_to_update:
                    doc_dict['dept'] = self.generate_dept()
                if 'is_manager' in fields_to_update:
                    doc_dict['is_manager'] = bool(random.getrandbits(1))
                    if doc_dict["is_manager"]:
                        doc_dict['manages'] = {'team_size': random.randint(5, 10)}
                        doc_dict['manages']['reports'] = []
                        for _ in range(0, doc_dict['manages']['team_size']):
                            doc_dict['manages']['reports'].append(self.generate_name())
                if 'languages_known' in fields_to_update:
                    doc_dict['languages_known'] = self.generate_lang_known()
                if 'email' in fields_to_update:
                    doc_dict['email'] = "%s_%s@mcdiabetes.com" % \
                                        (doc_dict['name'].split(' ')[0].lower(),
                                         str(random.randint(0, 99)))
                if 'manages.team_size' in fields_to_update or \
                        'manages.reports' in fields_to_update:
                    doc_dict['manages'] = {}
                    doc_dict['manages']['team_size'] = random.randint(5, 10)
                    doc_dict['manages']['reports'] = []
                    for _ in range(0, doc_dict['manages']['team_size']):
                        doc_dict['manages']['reports'].append(self.generate_name())
            self.gen_docs[count] = doc_dict

    def __next__(self):
        if self.itr >= self.end:
            raise StopIteration
        doc = self.gen_docs[self.itr]
        self.itr += 1
        return self.name + str(10000000 + self.itr), \
               json.dumps(doc).encode(self.encoding, "ignore")

    def generate_join_date(self):
        import datetime
        year = random.randint(1950, 2016)
        month = random.randint(1, 12)
        day = random.randint(1, 28)
        hour = random.randint(0, 23)
        min = random.randint(0, 59)
        return datetime.datetime(year, month, day, hour, min).isoformat()

    def generate_random_learnings(self):
        from sentence_transformers import SentenceTransformer
        l = self.learnings[random.randint(0, len(self.learnings) - 1)]
        encoder = SentenceTransformer(self.llm_model)
        l_vector = encoder.encode(l)
        return l, l_vector.tolist()

    def generate_dept(self):
        return DEPT[random.randint(0, len(DEPT) - 1)]

    def generate_salary(self):
        return round(random.random() * 100000 + 50000, 2)

    def generate_name(self):
        return "%s %s" % (FIRST_NAMES[random.randint(1, len(FIRST_NAMES) - 1)],
                          LAST_NAMES[random.randint(1, len(LAST_NAMES) - 1)])

    def generate_lang_known(self):
        count = 0
        lang = []
        while count < 3:
            lang.append(LANGUAGES[random.randint(0, len(LANGUAGES) - 1)])
            count += 1
        return lang


class WikiJSONGenerator(KVGenerator):

    def __init__(self, name, lang='EN', encoding="utf-8", op_type="create",
                 *args, **kwargs):

        """Wikipedia JSON document generator

        gen = WikiJSONGenerator(prefix, lang="DE","encoding="utf-8",
                                start=0,end=1000)
        Args:
            prefix: prefix for key
            encoding: utf-8/ascii/utf-16 encoding of JSON doc
            *args: A list for each argument in the template
            *kwargs: Special constrains for the document generator

        ** For EN, generates 20000 unique docs, and then duplicates docs **
        ** For ES, DE and FR, generates 5000 unique docs and then duplicates **


        Sample EN doc:

        {
           "revision": {
              "comment": "robot Modifying: [[bar:Apr\\u00fc]]",
              "timestamp": "2010-05-13T20:42:11Z",
              "text": {
                 "@xml:space": "preserve",
                 "#text": "'''April''' is the fourth month of the year with 30
                 days. The name April comes from that Latin word ''aperire''
                 which means \"to open\". This probably refers to growing plants
                 in spring. April begins on the same day of week as ''[[July]]''
                 in all years and also ''[[January]]'' in leap years.\n\nApril's
                flower is the Sweet Pea and ...<long text>
                },
              "contributor": {
                 "username": "Xqbot",
                 "id": "40158"
              },
              "id": "2196110",
              "minor": null
           },
           "id": "1",
           "title": "April"
           "mutated": 0,
        }


        """

        self.args = args
        self.name = name
        self.gen_docs = {}
        self.encoding = encoding
        self.lang = lang

        size = 0
        if not len(self.args) == 0:
            size = 1
            for arg in self.args:
                size *= len(arg)

        KVGenerator.__init__(self, name, 0, size)
        random.seed(0, 1)

        if 'start' in kwargs:
            self.start = int(kwargs['start'])
            self.itr = int(kwargs['start'])
        if 'end' in kwargs:
            self.end = int(kwargs['end'])

        if op_type == "create":
            self.read_from_wiki_dump()
        elif op_type == "delete":
            # for deletes, just keep/return empty docs with just type field
            for count in range(self.start, self.end):
                self.gen_docs[count] = {'type': 'wiki'}

    def read_from_wiki_dump(self):
        count = 0
        done = False
        while not done:
            try:
                with gzip.open("lib/couchbase_helper/wiki/{0}wiki.txt.gz".
                                       format(self.lang.lower()), "r") as f:
                    for doc in f:
                        self.gen_docs[count] = doc
                        if count >= self.end:
                            f.close()
                            done = True
                            break
                        count += 1
                    f.close()
            except IOError:
                lang = self.lang.lower()
                wiki = eval("{0}WIKI".format(self.lang))
                print(("Unable to find file lib/couchbase_helper/wiki/"
                       "{0}wiki.txt.gz. Downloading from {1}...".
                       format(lang, wiki)))
                import urllib.request, urllib.parse, urllib.error
                urllib.request.URLopener().retrieve(
                    wiki,
                    "lib/couchbase_helper/wiki/{0}wiki.txt.gz".format(lang))
                print("Download complete!")

    def __next__(self):
        if self.itr >= self.end:
            raise StopIteration
        doc = {}
        try:
            doc = eval(self.gen_docs[self.itr])
        except TypeError:
            # happens with 'delete' gen
            pass
        doc['mutated'] = 0
        doc['type'] = 'wiki'
        self.itr += 1
        return self.name + str(10000000 + self.itr), \
               json.dumps(doc, indent=3).encode(self.encoding, "ignore")


class GeoSpatialDataLoader(KVGenerator):

    def __init__(self, name, encoding="utf-8", op_type="create",
                 *args, **kwargs):

        """Geo Spatial doc loader

        gen = GeoSpatialDataLoader(prefix,"encoding="utf-8",
                                start=0,end=1000)
        Args:
            prefix: prefix for key
            encoding: utf-8/ascii/utf-16 encoding of JSON doc
            *args: A list for each argument in the template
            *kwargs: Special constrains for the document generator


        Sample earthquakes doc:

        {
            "Src": "ak",
            "geo": {
                "lat": 64.4679,
                "lon": -148.0767
            },
            "Region": "Central Alaska",
            "Datetime": "Wednesday, September 26, 2012 15:07:51 UTC",
            "Depth": 11.7,
            "Version": 1,
            "type": "earthquake",
            "Eqid": 10565543,
            "Magnitude": 1.3,
            "NST": 5
        }


        """

        self.args = args
        self.name = name
        self.gen_docs = {}
        self.encoding = encoding

        size = 0
        if not len(self.args) == 0:
            size = 1
            for arg in self.args:
                size *= len(arg)

        KVGenerator.__init__(self, name, 0, size)
        random.seed(0, 1)

        if 'start' in kwargs:
            self.start = int(kwargs['start'])
            self.itr = int(kwargs['start'])
        if 'end' in kwargs:
            self.end = int(kwargs['end'])

        if op_type == "create":
            if self.name == "earthquake":
                self.read_from_dump()
            elif self.name == "geojson":
                self.custom_read_from_dump(kwargs['filename'])
        elif op_type == "delete":
            # for deletes, just keep/return empty docs with just type field
            for count in range(self.start, self.end):
                self.gen_docs[count] = {'type': 'earthquake'}

    def read_from_dump(self):
        count = 0
        done = False
        try:
            with open("lib/couchbase_helper/geospatial/"
                      "earthquakes.json") as f:
                while not done:
                    for doc in json.load(f):
                        if bool(random.getrandbits(1)):
                            doc['geo'] = [doc['geo']['lon'], doc['geo']['lat']]
                        doc['Eqid'] = str(doc['Eqid'])
                        doc['Version'] = str(doc['Version'])
                        doc['mutated'] = 0
                        doc['type'] = 'earthquake'
                        self.gen_docs[count] = doc
                        if count >= self.end:
                            f.close()
                            done = True
                            break
                        count += 1
            f.close()
        except IOError:
            print("Unable to find file lib/couchbase_helper/"
                  "geospatial/earthquakes.json, data not loaded!")

    def custom_read_from_dump(self, filename="geoshape.json"):
        count = 0
        done = False
        while not done:
            try:
                import os
                filepath = os.path.join("lib", "couchbase_helper", "geospatial", filename)
                f = open(filepath, 'r')
                for doc in json.load(f):
                    doc['id'] = str(doc['id'])
                    doc['type'] = 'earthquake'
                    self.gen_docs[count] = doc
                    if count >= self.end:
                        done = True
                        f.close()
                        break
                    count += 1

            except IOError:
                print("Unable to find file " + filepath + ", data not loaded!")
                geoshape_s3_path = GEOJSONDATASET
                print(("Unable to find file " + filepath + "Downloading ..."))
                import urllib.request, urllib.parse, urllib.error
                urllib.request.URLopener().retrieve(
                    geoshape_s3_path, filepath)
                print("Download complete!")

    def __next__(self):
        if self.itr >= self.end:
            raise StopIteration
        doc = {}
        try:
            doc = self.gen_docs[self.itr]
        except TypeError:
            # happens with 'delete' gen
            pass
        self.itr += 1
        return self.name + str(self.itr), \
               json.dumps(doc, indent=3).encode(self.encoding, "ignore")


class SDKDataLoader(object):
    # type: (int, int, int, int, str, int, str, str, list, list, str, bool)
    def __init__(self, num_ops=0, percent_create=0, percent_update=0, percent_delete=0, workers=1,
                 load_pattern="uniform", start_seq_num=1, key_prefix="doc_", key_suffix="_", ops_rate=5000,
                 scope="_default", collection="_default", json_template="Person", doc_expiry=0, fields_to_update=None,
                 doc_size=500, get_sdk_logs=False, username="Administrator", password="password", timeout=1000,
                 start=0, end=0, op_type="create", all_collections=False, es_compare=False, es_host=None, es_port=None,
                 es_login=None, es_password=None, output=False, upd_del_shift=0, shuffle_docs=False, capella=False, base64=False, model="sentence-transformers/all-MiniLM-L6-v2"):
        self.num_ops = num_ops
        self.percent_create = percent_create
        self.percent_update = percent_update
        self.percent_delete = percent_delete
        self.json_template = json_template
        self.key_prefix = key_prefix
        self.start = start
        self.end = end if end > start else num_ops - 1
        self.load_pattern = load_pattern
        self.start_seq_num = start_seq_num
        self.key_prefix = key_prefix
        self.key_suffix = key_suffix
        self.scope = scope
        self.collection = collection
        self.json_template = json_template
        self.doc_expiry = doc_expiry
        self.doc_size = doc_size
        self.get_sdk_logs = get_sdk_logs
        self.username = username
        self.password = password
        self.timeout = timeout
        self.fields_to_update = fields_to_update
        self.op_type = op_type
        self.all_collections = all_collections
        self.results = None
        self.es_compare = es_compare
        self.es_host = es_host
        self.es_port = es_port
        self.es_login = es_login
        self.es_password = es_password
        self.output = output
        self.upd_del_shift = upd_del_shift
        self.shuffle_docs = shuffle_docs
        self.secure = CbServer.use_https
        self.capella = capella
        self.ops_rate = ops_rate
        self.workers = workers
        self.base64 = base64
        self.model = model


    def update(self, fields_to_update=None):
        self.start_seq_num = self.start + 1
        self.num_ops = self.end - self.start
        self.percent_create = 0
        self.percent_update = 100
        self.percent_delete = 0
        self.fields_to_update = fields_to_update
        self.op_type = "update"

    def delete(self):
        self.start_seq_num = self.start + 1
        self.num_ops = self.end - self.start
        self.percent_create = 0
        self.percent_update = 0
        self.percent_delete = 100
        self.op_type = "delete"

    def isGenerator(self):
        return False

    def get_num_ops(self):
        return self.num_ops

    def get_start_seq_num(self):
        return self.start_seq_num

    def set_num_ops(self, num_ops):
        self.num_ops = num_ops

    def set_start_seq_num(self, start_seq_num):
        self.start_seq_num = start_seq_num

    def get_sdk_results(self):
        return self.results

    def set_sdk_results(self, results):
        self.results = results


class NapaDataLoader(KVGenerator):

    def __init__(self, name, encoding="utf-8", op_type="create",
                 *args, **kwargs):

        """Napa doc loader

        gen = NapaDataLoader(prefix,"encoding="utf-8",
                                start=0,end=1000)
        Args:
            prefix: prefix for key
            encoding: utf-8/ascii/utf-16 encoding of JSON doc
            *args: A list for each argument in the template
            *kwargs: Special constrains for the document generator


        Sample napa doc:

        {
          "_id": "1019_ycsb",
          "doc_id": 1019,
          "gid": "f9d57c4b-b6f6-5ef6-9c77-e4e2a1e3e823",
          "index": 1,
          "routing_number": 15073,
          "company_name": "Wisozk, Casper and Moore",
          "company_code": "HOF44",
          "first_name": "Kaden",
          "middle_name": "Terry",
          "last_name": "Monahan",
          "age": 41,
          "ballance_current": "$8166.76",
          "ballance_average": "$2294.65",
          "picture": "https://www.gravatar.com/avatar/cc8a0fed5b51679b0f96044c6b41d0bd",
          "phone": "(748) 317-8602",
          "dob": "2020-02-15",
          "email": "Larue_Labadie@hotmail.com",
          "isActive": false,
          "address": {
            "street": "7826 Schinner Forges Port",
            "city": "West Merlinstad",
            "zip": "04380",
            "country": "Germany",
            "applicationId": 12,
            "deviceTypeId": 9,
            "deviceStatus": 0,
            "ecpdId": "3",
            "activationDate": "2020-04-18T18:36:40.752Z"
          },
          "devices": [
            "504062-023",
            "504798-457",
            "521716-476",
            "861229-804"
          ],
          "children": [
            {
              "first_name": "Titus",
              "gender": null,
              "age": 9
            },
            {
              "first_name": "Meta",
              "gender": "M",
              "age": 10
            },
            {
              "first_name": "Emmanuelle",
              "gender": "F",
              "age": 11
            },
            {
              "first_name": "Hope",
              "gender": "M",
              "age": 13
            },
            {
              "first_name": "Monserrat",
              "gender": "M",
              "age": 7
            }
          ]
        }


        """

        self.args = args
        self.name = name
        self.gen_docs = {}
        self.encoding = encoding

        size = 0
        if not len(self.args) == 0:
            size = 1
            for arg in self.args:
                size *= len(arg)

        KVGenerator.__init__(self, name, 0, size)
        random.seed(0, 1)

        if 'start' in kwargs:
            self.start = int(kwargs['start'])
            self.itr = int(kwargs['start'])
        if 'end' in kwargs:
            self.end = int(kwargs['end'])

        if op_type == "create":
            self.read_from_napa_dump()
        elif op_type == "delete":
            # for deletes, just keep/return empty docs with just type field
            for count in range(self.start, self.end):
                self.gen_docs[count] = {'type': 'napa'}

    def read_from_napa_dump(self):
        count = 0
        done = False
        while not done:
            try:
                with gzip.open("lib/couchbase_helper/napa/napa_dataset.txt.gz", "r") as f:
                    for doc in f:
                        self.gen_docs[count] = doc
                        if count >= self.end:
                            f.close()
                            done = True
                            break
                        count += 1
                    f.close()
            except IOError:
                print("Unable to find file lib/couchbase_helper/napa/"
                      "napa_dataset.txt.gz. Downloading from {0}...".format(NAPADATASET))
                import urllib.request, urllib.parse, urllib.error
                urllib.request.URLopener().retrieve(
                    NAPADATASET,
                    "lib/couchbase_helper/napa/napa_dataset.txt.gz")
                print("Download complete!")

    def __next__(self):
        if self.itr >= self.end:
            raise StopIteration
        doc = {}
        try:
            doc = eval(self.gen_docs[self.itr])
        except TypeError:
            # happens with 'delete' gen
            pass
        doc['type'] = 'napa'
        self.itr += 1
        return self.name + str(10000000 + self.itr), \
               json.dumps(doc, indent=3).encode(self.encoding, "ignore")
