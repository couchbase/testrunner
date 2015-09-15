import json
import string
import random
from couchbase_helper.data import FIRST_NAMES, LAST_NAMES

class KVGenerator(object):
    def __init__(self, name, start, end):
        self.name = name
        self.start = start
        self.end = end
        self.itr = start

    def has_next(self):
        return self.itr < self.end

    def next(self):
        raise NotImplementedError

    def reset(self):
        self.itr = self.start

    def __iter__(self):
        return self

    def __len__(self):
        return self.end - self.start

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
            *args: A list for each argument in the template
            *kwargs: Special constrains for the document generator
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
        The doucument generated"""
    def next(self):
        if self.itr >= self.end:
            raise StopIteration

        seed = self.itr
        doc_args = []
        for arg in self.args:
            value = arg[seed % len(arg)]
            doc_args.append(value)
            seed /= len(arg)

        json_doc = json.loads(self.template.format(*doc_args).replace('\'', '"').replace('True', 'true').replace('False', 'false'))
        json_doc['_id'] = self.name + '-' + str(self.itr)
        self.itr += 1
        return json_doc['_id'], json.dumps(json_doc).encode("ascii", "ignore")

class BlobGenerator(KVGenerator):
    def __init__(self, name, seed, value_size, start=0, end=10000):
        KVGenerator.__init__(self, name, start, end)
        self.seed = seed
        self.value_size = value_size
        self.itr = self.start

    def next(self):
        if self.itr >= self.end:
            raise StopIteration

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
        if self._batch_size <= 0 :
            raise ValueError("Invalid Batch size {0}".format(self._batch_size))

    def has_next(self):
        return self._doc_gen.has_next()

    def next_batch(self):
        count = 0
        key_val = {}
        while count < self._batch_size and self.has_next():
            key, val = self._doc_gen.next()
            key_val[key] = val
            count += 1
        return key_val

class JSONNonDocGenerator(KVGenerator):
    """
    Values can be arrays, integers, strings
    """
    def __init__(self, name, values, start=0, end=10000):
        KVGenerator.__init__(self, name, start, end)
        self.values = values
        self.itr = self.start

    def next(self):
        if self.itr >= self.end:
            raise StopIteration

        key = self.name + str(self.itr)
        index = self.itr
        while index > len(self.values):
            index = index - len(self.values)
        value = json.dumps(self.values[index-1])
        self.itr += 1
        return key, value

class Base64Generator(KVGenerator):
    def __init__(self, name, values, start=0, end=10000):
        KVGenerator.__init__(self, name, start, end)
        self.values = values
        self.itr = self.start

    def next(self):
        if self.itr >= self.end:
            raise StopIteration

        key = self.name + str(self.itr)
        index = self.itr
        while index > len(self.values):
            index = index - len(self.values)
        value = self.values[index-1]
        self.itr += 1
        return key, value

class JsonDocGenerator(KVGenerator):

    def __init__(self, name, encoding="utf-8", *args, **kwargs ):
        """Initializes the JSON document generator


        gen = DocumentGenerator('test_docs', template, age, first, start=0, end=5)

        Args:
            name: The key name prefix
            *args: A list for each argument in the template
            *kwargs: Special constrains for the document generator
        """
        self.args = args
        self.name = name
        self.gen_docs= []
        self.encoding = encoding

        size = 0
        if not len(self.args) == 0:
            size = 1
            for arg in self.args:
                size *= len(arg)

        KVGenerator.__init__(self, name, 0, size)

        if 'start' in kwargs:
            self.start = int(kwargs['start'])
            self.itr = int(kwargs['start'])

        if 'end' in kwargs:
            self.end = int(kwargs['end'])

        for count in xrange(self.start+1, self.end+1):
            emp_name = self.generate_name()
            doc_dict = {
                        'emp_id': 10000000+count,
                        'name': emp_name,
                        'dept': self.generate_dept(),
                        'email': "%s_%s@mcdiabetes.com" %
                                 (emp_name.split(' ')[0].lower(), str(count)),
                        'salary': self.generate_salary(),
                        'join_date': self.generate_join_date(),
                        'languages_known': self.generate_lang_known(),
                        'is_manager': bool(random.getrandbits(1))
                       }
            if doc_dict["is_manager"]:
                doc_dict['manages'] = {'team_size': random.randint(5,10)}
                doc_dict['manages']['reports'] = []
                for _ in xrange(0, doc_dict['manages']['team_size']):
                    doc_dict['manages']['reports'].append(self.generate_name())
            self.gen_docs.append(doc_dict)

    def next(self):
        if self.itr >= self.end:
            raise StopIteration
        doc = self.gen_docs[self.itr]
        self.itr += 1
        return self.name+str(doc['emp_id']),\
               json.dumps(doc).encode(self.encoding, "ignore")

    def generate_join_date(self):
        import datetime
        year = random.randint(1950, 2016)
        month = random.randint(1, 12)
        day = random.randint(1, 28)
        hour = random.randint(0,23)
        min = random.randint(0,59)
        return str(datetime.datetime(year, month, day, hour, min))

    def generate_dept(self):
        dept_list = ['Engineering', 'Sales', 'Support', 'Marketing', 'IT', 'Finance']
        return dept_list[random.randint(0,len(dept_list)-1)]

    def generate_salary(self):
        return round(random.random()*100000, 2)

    def generate_name(self):
        return "%s %s" %(FIRST_NAMES[random.randint(1, len(FIRST_NAMES)-1)],
                         LAST_NAMES[random.randint(1, len(LAST_NAMES)-1)])

    def generate_lang_known(self):
        lang_list = ['English', 'Spanish', 'German', 'Italian', 'French']
        return [lang_list[i] for i in xrange(1,random.randint(0,len(lang_list)-1))]

