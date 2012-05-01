import copy
import json

class DocumentGenerator(object):
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
        self.name = name
        self.template = template
        self.args = args

        if 'start' in kwargs:
            self.itr = kwargs['start']
        else:
            self.itr = 0

        if 'end' in kwargs:
            self.end = kwargs['end']
        else:
            self.end = len(self)

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

    """Whether or not the iterator is at the end of the list

    Return:
        True if we have more documents to generate, False otherwise."""
    def has_next(self):
        return self.itr < self.end

    """Resets the iterator back to the beginning of the list."""
    def reset(self):
        self.itr = 0

    def __iter__(self):
        return self

    def __len__(self):
        if len(self.args) == 0:
            return 0
        size = 1
        for arg in self.args:
            size = size * len(arg)
        return size
