import random
from lib.couchbase_helper.data import FIRST_NAMES, LAST_NAMES, LANGUAGES, DEPT

class EmployeeQuerables:

    def get_random_value(self, list):
        return list[random.randint(0, len(list)-1)]

    def get_queryable_name(self, full=False):
        """
        Returns a first or last name OR
        a combination of both
        """
        if full:
            return self.return_unicode(self.get_queryable_full_name())

        if bool(random.getrandbits(1)):
            name_list = FIRST_NAMES + LAST_NAMES
            return self.return_unicode(self.get_random_value(name_list))
        else:
            return self.return_unicode(self.get_queryable_full_name())

    def return_unicode(self, text):
        try:
            text = str(text, 'utf-8')
            return text
        except TypeError:
            return text

    def get_queryable_full_name(self):
        return "%s %s" %(self.get_random_value(FIRST_NAMES),
                         self.get_random_value(LAST_NAMES))

    def get_queryable_dept(self):
        """
        Returns a valid dept to be queried
        """
        return self.get_random_value(DEPT)

    def get_queryable_join_date(self, now=False):
        import datetime
        if now:
            return datetime.datetime.now().isoformat()
        year = random.randint(1950, 2016)
        month = random.randint(1, 12)
        day = random.randint(1, 28)
        hour = random.randint(0, 23)
        min = random.randint(0, 59)
        return datetime.datetime(year, month, day, hour, min).isoformat()

    def get_queryable_languages_known(self):
        """
        Returns one or two languages
        """
        return self.get_random_value(LANGUAGES)

    def get_queryable_email(self):
        return "%s@mcdiabetes.com" % self.get_random_value(FIRST_NAMES).lower()

    def get_queryable_empid(self):
        return random.randint(10000000, 10000100)

    def get_queryable_salary(self):
        return round(random.random(), 2) *100000 + 50000

    def get_queryable_manages_team_size(self):
        return random.randint(5, 10)

    def get_queryable_mutated(self):
        return random.randint(0, 5)

    def get_queryable_manages_reports(self, full=False):
        """
        Returns first names of 1-4 reports
        """
        num_reports = random.randint(1, 4)
        reports = []
        if not full:
            while len(reports) < num_reports:
                if num_reports == 1:
                    return self.get_random_value(FIRST_NAMES)
                reports.append(self.get_random_value(FIRST_NAMES))
            return ' '.join(reports)
        else:
            while len(reports) < num_reports:
                if num_reports == 1:
                    return self.return_unicode(self.get_queryable_full_name())

                reports.append(self.return_unicode(self.get_queryable_full_name()))
            return ' '.join(reports)

    def get_queryable_regex_name(self):
        list = ['Ad*', 'XI+', 'I+', 'Scot+', 'Phil+', 'An.*', 'Tr*', 'Adrian?e*',
                'Wel*[a-z]*', 'Rus+el+*', 'Ca[m-z]+', 'Kil{1}[a-z]+', '[^a-m][a-z]+',
                'I{1,2}']
        return self.get_random_value(list)

    def get_queryable_regex_manages_reports(self):
        return self.get_queryable_regex_name()