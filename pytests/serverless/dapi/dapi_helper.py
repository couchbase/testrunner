import random

from lib.couchbase_helper.documentgenerator import DocumentGenerator

from string import ascii_uppercase, ascii_lowercase, digits
letters = ascii_uppercase + ascii_lowercase + digits

def doc_generator(key_prefix="key", key_size=8,
                value_size=10, number_of_docs=10,
                randomize_value=False, mixed_key=False):
    # key construction
    key_add_on = ""
    for _ in range(key_size - (len(key_prefix)+ len(str(number_of_docs)))):
        if mixed_key:
            key_add_on += random.choice(letters)
        else:
            key_add_on += "-"

    key_prefix = key_prefix + key_add_on

    # doc construction
    _l = len('"age     ": 5')
    template = ''
    if value_size >= _l:
        template = '{{ "age": {0}}}'
        value_size -= _l
    _l = len('"name      ": "james    "')
    if value_size >= _l:
        template = '{{ "age": {0}, "name": "{1}"}}'
        value_size -= _l
    _l = len('"about      ": "a     "')
    if value_size >= _l:
        template = '{{ "age": {0}, "name": "{1}", "about": "{2}"}}'
        value_size -= _l

    about_string = letters
    if randomize_value:
        about_string = letters * (value_size // len(letters))
        about_string = about_string + letters[:value_size % len(letters)]
    else:
        about_string = 'a' * value_size

    age = range(5)
    names = ["james", "Bella", "Ciera", "Delta", "David"]
    about = [about_string]
    gen = DocumentGenerator(key_prefix, template,
                            age, names, about, start=0, end=number_of_docs)
    return gen