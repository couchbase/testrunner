import io
import random
import string
import sys
import requests
from datetime import datetime
import pytz
import collections
from .constants import Constants as constants


class ValueGenerator(object):

    def array_dates(self, num_min=1, num_max=10):
        ret = []
        self.num_min = int(num_min)
        self.num_max = int(num_max)
        while self.num_min < self.num_max:
            ret.append(self.date_time())
            self.num_min += 1
        return ret

    def array_literals(self):
        return [0b1010,  # Binary
                100,  # Decimal
                0o310,  # Octal
                0x12c,  # Hex
                3.14e8,  # Exponential
                2147483648,  # Long int
                "\u00dcnic\u00f6de",  # Unicode
                True + 5,  # Boolean
                False - 5,
                None,  # Special
                """string""",  # Triple quotes
                'string1 "string2"!',  # Single quotes
                "string1' string2'.",  # Double quotes
                "\"string1?\" string2.",  # Escaped quotes
                '''This one string can go
                over several lines''',  # Triple quoted string, with single quotes
                r"raw \n string",  # Raw string
                r"\"string1!\" string2.",  # Raw string
                r"""string1 string2!""",  # Raw string
                "Line\t1\n\"Line 2\"\n\\Line 3\n"  # Escaped chars
                ]

    def array_mix(self):
        ret = []
        for val in list(self.sub_doc().values()):
            ret.append(list(val.values()))
        return ret

    def array_numbers(self, num_min=0, num_max=10):
        ret = []
        self.num_min = int(num_min)
        self.num_max = int(num_max)
        while self.num_min < self.num_max:
            ret.append(random.choice([self.rand_int(),
                                      self.rand_float(),
                                      self.rand_int(negative=True),
                                      self.rand_int(negative=True),
                                      ]))
            self.num_min += 1
        return ret

    def array_strings(self, len_min=0, len_max=10, num_min=0, num_max=10):
        ret = []
        self.num_min = int(num_min)
        self.num_max = int(num_max)
        while self.num_min < self.num_max:
            ret.append(self.rand_string(len_min=len_min, len_max=len_max))
            self.num_min += 1
        return ret

    def rand_bool(self):
        return random.choice([True, False])

    def date_time(self):
        d = datetime.now()  # naive
        tz = pytz.timezone(random.choice(pytz.all_timezones))
        dt = datetime.now(tz=tz)  # tz aware
        dates = ["2019-03-21T18:25:43-05:00",  # ISO 8601
                 "2019-03-21T18:25:43.511Z",
                 str(d.isoformat()),
                 str(dt.isoformat()),
                 str(d.ctime()),
                 str(datetime.fromtimestamp(1528797322)),
                 str(datetime.now(tz=pytz.UTC)),
                 str(d.time()),
                 str(dt.astimezone(tz=tz)),
                 str(d.date()),
                 str(d.dst())
                 ]
        return random.choice(dates)

    def sub_doc(self, max_nesting=10, max_items=20):
        try:
            items = random.randint(1, int(max_items))
            sub_doc_dict = collections.defaultdict(dict)
            func_list = [func for func in dir(self) if
                         callable(getattr(self, func)) and not func.startswith("__")
                         # Exclude array_mix()
                         and not func.endswith("mix")]
            # and not func == "sub_doc"]
            while items > 0:
                nesting = random.randint(0, int(max_nesting))
                val = getattr(globals()['ValueGenerator'](), random.choice(func_list))()
                sub_doc_dict[items][nesting] = val
                items -= 1
            return sub_doc_dict
        except:
            # Insurance against maximum recursion depth exceeded exception
            return {{{}}}

    def rand_float(self, negative=False):
        if bool(negative):
            return -1 * random.uniform(sys.float_info.min, sys.float_info.max)
        else:
            return random.uniform(sys.float_info.min, sys.float_info.max)

    def rand_int(self, negative=False):
        if bool(negative):
            return -1 * random.randint(1, sys.maxsize)
        else:
            return random.randint(0, sys.maxsize)

    def rand_null(self):
        return random.choice(["NULL", "null", "Null", None])

    def rand_string(self, len_min=0, len_max=10):
        self.len_min = int(len_min)
        self.len_max = int(len_max)
        return ''.join(random.choice(
            string.ascii_letters * 40 + \
            string.digits * 10 + \
            string.whitespace * 40 + \
            string.punctuation * 10) for _ in range(random.randint(self.len_min, self.len_max)))

    def string_num(self, len_min=1, len_max=10):
        self.len_min = int(len_min)
        self.len_max = int(len_max)
        return ''.join(random.choice(
            string.digits
        ) for _ in range(random.randint(self.len_min, self.len_max)))

    def rand_reserved(self):
        return random.choice(constants.niql_reserved_keywords)

    def empty(self, type="string"):
        return constants.empty[type]

    def tabs(self, len_min=1, len_max=10):
        return "\t" * random.randint(int(len_min), int(len_max))

    # def rand_name(self):
    #     name = requests.get('http://uinames.com/api')
    #     if not name:
    #         return "Chuck Norris"
    #     return name.json()
