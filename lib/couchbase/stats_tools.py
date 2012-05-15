
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
