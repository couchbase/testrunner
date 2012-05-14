import json

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

        json_doc = json.loads(self.template.format(*doc_args))
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
        value = self.seed + str(self.itr)
        extra = self.value_size - len(value)
        if extra > 0:
            value += 'a' * extra
        self.itr += 1
        return key, value
