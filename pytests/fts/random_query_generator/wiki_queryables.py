import random
from lib.couchbase_helper.wiki.en_wiki_querables import TITLES, USERNAMES, USER_IDS

class WikiQuerables:

    def get_random_value(self, list):
        return list[random.randint(0, len(list)-1)]

    def get_queryable_title(self):
        """
        Returns a valid wiki doc title
        """
        return self.return_unicode(self.get_random_value(TITLES))

    def get_queryable_revision_text_text(self):
        """
        Returns a valid wiki search term
        """
        return self.return_unicode(self.get_random_value(TITLES))

    def get_queryable_revision_contributor_id(self):
        """
        Returns a valid wiki contributor id(number)
        """
        return self.get_random_value(USER_IDS)

    def get_queryable_revision_contributor_username(self):
        """
        Returns a valid wiki contributor username(str)
        """
        return self.return_unicode(self.get_random_value(USERNAMES))

    def get_queryable_id(self):
        """
        Returns a valid wiki id(number)
        """
        return self.get_random_value(USER_IDS)

    def get_queryable_revision_timestamp(self, now=False):
        """
        Returns a valid wiki revision timestamp
        """
        import datetime
        if now:
            return datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
        year = random.randint(1950, 2016)
        month = random.randint(1, 12)
        day = random.randint(1, 28)
        hour = random.randint(0, 23)
        min = random.randint(0, 59)
        secs = random.randint(0, 59)
        return datetime.datetime(year, month, day, hour, min, secs).strftime('%Y-%m-%dT%H:%M:%SZ')

    def return_unicode(self, text):
        try:
            text = str(text, 'utf-8')
            return text
        except TypeError:
            return text

    def get_queryable_regex_title(self):
        list = ['Au*', '[A-Ca-z ]+', '[A-Za-z]+\xe9*', 'Embas+y*',
                'Etc[,./]*', 'Edit*', 'Flesch[-:]Kincaid*',
                '[:{}()]', '[0-9]+', 'Kil+', '[E-M]ist[ ,]of*', 'm[m-t].']
        return self.return_unicode(self.get_random_value(list))

    def get_queryable_regex_revision_text_text(self):
        return self.get_queryable_regex_title()