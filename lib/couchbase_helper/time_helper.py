import re
import datetime

class TimeUtil:
    """ A class for time utility functions
    """

    @staticmethod
    def rfc3339nano_to_datetime(my_time, time_dash='_'):
        """ Converts a cbbackupmgr RFC3399Nano timestamp to a datetime object ignoring the nanoseconds
        """
        my_time = my_time.replace('_', ':') # Replace undercores with colons
        my_time = re.sub(r"\.\d*", "", my_time) # Strip nanoseconds
        return datetime.datetime.strptime(my_time, f"%Y-%m-%dT%H:%M:%S%z") # Parse string to datetime
