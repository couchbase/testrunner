
from memcached.helper.data_helper import MemcachedClientHelper

class StatsCommon():

    @staticmethod
    def get_stats(servers, bucket, stat_param, stat):
        """Gets stats for a specific key from a list of servers

        Parameters:
            servers - A list of servers to get a stat from. ([TestInputServer])
            bucket - The name of the bucket to get a stat from. (String)
            stat_param - The parameter for the stats call, eg. 'checkpoint'. (String)
            stat - The name of the stat to get. (String)

        Returns:
            Dictionary - The key is the test input server and the value is the
            result of the for the stat passed in as a parameter.
        """
        result = {}
        for server in servers:
            client = MemcachedClientHelper.direct_client(server, bucket)
            stats = client.stats(stat_param)
            result[server] = stats[stat]
        return result

    @staticmethod
    def build_stat_check(server, param, stat, comparison, value, struct=[]):
        """This is a utility function for the StatsWaitTask. The StatsWaitTask requires
        a complex structure in order to successfully wait for stats to reach a certain
        condition. An example of this structure is below.

        [{'server': TestInputServer, # The server for conditions listed below
            'stats': {
                checkpoint : { # The parameter for stats commands
                    ep_queue_size: { # The stat to compare against
                        'compare' : '==', # How to compare (==, <=, >=, <, >)
                        'value' : 0 # The value to compare to
                    }
                }
            }
        }] # Notice the dictionary is in a list so you can specif mutliple servers

        Parameter:
            server - The server to get a stat from
            param - The stats command parameter
            stat - The name of the stat to get
            comparison - How to compare the value recieved
            value - What to compare this value to
            struct - The current stats struct. This should be an empty list for the
            first call and then the result of the previous call for future invocations
            of this function

        Returns:
            List - The structure above.
        """
        for item in struct:
            if item['server'] == server:
                param_list = item['stats']
                if not param_list.has_key(param):
                    param_list[param] = {}
                stat_list = param_list[param]
                stat_list[stat] = {'compare': comparison, 'value': value}
                return struct
        item = {'server': server,
                   'stats': {
                       param: {
                           stat: {
                               'compare' : comparison,
                               'value' : value
                           }
                       }
                    }
                }
        struct.append(item)
        return struct
