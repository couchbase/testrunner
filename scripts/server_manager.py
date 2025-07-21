"""
Server Manager Module

This module provides functions for managing server allocation and deallocation
in the Couchbase server pool. Converted from the original JavaScript
server_manager.js.
"""

import json
import logging


class ServerManager:
    """Server Manager class for managing server allocation and deallocation."""

    def __init__(self, sdk_cluster_obj, bucket_name, logger=None):
        """Initialize the server manager with Couchbase connection."""
        self.bucket_name = bucket_name
        self.cluster = sdk_cluster_obj
        self.bucket = self.cluster.bucket(bucket_name)
        self.default_collection = self.bucket.default_collection()
        self.logger = logger or logging.getLogger(__name__)

    def get_dockers(self, username, count, pool_id='12hour'):
        """
        Get Docker servers from the server pool.

        Args:
            username: The username requesting the servers
            count: Number of servers requested
            pool_id: Pool ID (default: 12hour)

        Returns:
            str: IP address of allocated server
        """
        self.logger.info(f"get_dockers: username={username}, count={count}, "
                         f"pool_id={pool_id}")

        query_string = \
            f"SELECT ipaddr,availableServers,users FROM `QE-server-pool` " \
            f"WHERE serverType='docker' AND poolId='{pool_id}'"

        docker_ip_list = list()
        self.logger.debug(f"Query: {query_string}")

        try:
            results = self.cluster.query(query_string)

            self.logger.debug(f"Result: {results}")

            for result in results.rows():
                self.logger.debug(
                    f"Available servers: {result['availableServers']}")

                if result['availableServers'] >= count:
                    self.logger.debug(f"Incoming users are {result['users']}")

                    users = result['users'] if result['users'] else {}
                    users[username] = count

                    self.logger.info(f"get dockers, the user is {username}")
                    available_servers = result['availableServers'] - count

                    self.logger.debug(f"users are {json.dumps(users)}")

                    # Update the record
                    update_string = (
                        f"UPDATE `QE-server-pool`"
                        f" SET availableServers={available_servers},"
                        f" users='{json.dumps(users)}'"
                        f" WHERE ipaddr='{result['ipaddr']}'")

                    self.logger.debug(f"Query: {update_string}")
                    self.cluster.query(update_string)

                    docker_ip_list.append(result['ipaddr'])

            # No available Docker servers found
            self.logger.warning("The current number of servers requested "
                                "is not available")
        except Exception as e:
            self.logger.error(f"Error in get_dockers: {str(e)}")
        return docker_ip_list

    def release_dockers(self, username, ipaddr):
        """
        Release Docker servers back to the pool.

        Args:
            username: The username releasing the servers
            ipaddr: The IP address of the server to release

        Returns:
            bool: True if successful, False if server not found
        """
        self.logger.info(f"release_dockers: username={username}, "
                         f"ipaddr={ipaddr}")

        query_string = (
            f"SELECT ipaddr,availableServers,users FROM `QE-server-pool` "
            f"WHERE ipaddr = '{ipaddr}'")
        self.logger.debug(f"Query: {query_string}")

        try:
            results = self.cluster.query(query_string)
            self.logger.debug(f"Result: {results}")

            if len(results.rows()) > 0:
                result = results.rows()[0]
                users = result['users'] if result['users'] else {}

                if len(users) == 0 or username not in users:
                    # This is a double delete
                    self.logger.info(f"{username} already released dockers")
                    return True
                else:
                    new_count = result['availableServers'] + users[username]
                    self.logger.debug(f"deleting user {json.dumps(users)}")
                    del users[username]
                    self.logger.debug(f"users are {json.dumps(users)}")

                    # Update the record
                    update_string = (
                        f"UPDATE `QE-server-pool` SET "
                        f"availableServers={new_count}, "
                        f"users='{json.dumps(users)}' WHERE ipaddr='{ipaddr}'")

                    self.logger.debug(f"Query: {update_string}")
                    self.cluster.query(update_string)

                    self.logger.info(f"release_dockers: username={username}, "
                                     f"ipaddr={ipaddr}")
                    return True
            else:
                self.logger.error("Unknown server")
                return False

        except Exception as e:
            self.logger.error(f"Error in release_dockers: {str(e)}")
            return False

    def get_available_count(self, os_type, pool_id='12hour', docker=False):
        """
        Get available server count for a specific OS type.

        Args:
            os_type: OS type (e.g., 'centos', 'docker')
            pool_id: Pool ID (default: 12hour)

        Returns:
            int: Available server count
        """
        self.logger.info(f"os_type={os_type}, pool_id={pool_id}, "
                         f"docker={docker}")

        if not docker:
            # Regular server count
            query_string = (
                f"SELECT count(*) FROM `QE-server-pool` "
                f"WHERE state='available' AND os='{os_type}' "
                f"AND (poolId='{pool_id}' OR '{pool_id}' IN poolId)")
            self.logger.debug(f"Query: {query_string}")

            try:
                results = self.cluster.query(query_string)
                # Access the first row and get the count value
                for row in results.rows():
                    count = row['$1']
                    self.logger.info(f"Count: {count}")
                    return count
            except Exception as e:
                self.logger.error(f"Error in get_available_count: {str(e)}")
            return 0
        else:
            # Docker server count
            query_string = (
                f"SELECT ipaddr,availableServers,users FROM `QE-server-pool` "
                f"WHERE serverType='docker' AND os='{os_type}' AND "
                f"(poolId='{pool_id}' OR '{pool_id}' IN poolId)")
            self.logger.debug(f"Query: {query_string}")

            try:
                results = self.cluster.query(query_string)
                capacity_count = 0

                for result in results.rows():
                    available_servers = result['availableServers']
                    self.logger.debug(
                        f"Processing result: {result}, "
                        f"Available servers: {available_servers}")
                    if available_servers >= capacity_count:
                        capacity_count = available_servers

                return capacity_count
            except Exception as e:
                self.logger.error(f"Error in get_available_docker_count: "
                                  f"{str(e)}")
                return 0

    def add_server(self, ipaddr, os_type, version):
        """
        Add a new server to the pool.

        Args:
            ipaddr: IP address of the server
            os_type: OS type
            version: Server version
        """
        self.logger.info(f"add_server: ipaddr={ipaddr}, os={os_type}, "
                         f"version={version}")

        try:
            doc = {
                'ipaddr': ipaddr,
                'OS': os_type,
                'version': version,
                'state': 'available'
            }
            self.bucket.upsert(ipaddr, doc)
            self.logger.info(f"Server {ipaddr} added")

        except Exception as e:
            self.logger.error(f"Error in add_server: {str(e)}")

    def remove_server(self, ipaddr):
        """
        Remove a server from the pool.

        Args:
            ipaddr: IP address of the server to remove
        """
        self.logger.info(f"remove_server: ipaddr={ipaddr}")

        try:
            self.bucket.remove(ipaddr)
            self.logger.info(f"Server {ipaddr} removed")

        except Exception as e:
            self.logger.error(f"Error in remove_server: {str(e)}")

    def get_servers(self, username, count=1, os_type='centos', expires_in=1,
                    pool_id='12hour', dont_reserve=False):
        """
        Get servers from the pool using Couchbase transactions.

        Args:
            username: Username requesting servers
            count: Number of servers requested
            os_type: OS type
            expires_in: Expiration time in minutes
            pool_id: Pool ID
            dont_reserve: Whether to reserve servers

        Returns:
            list: List of server IP addresses
        """
        self.logger.info(f"get_servers: username={username}, count={count}, "
                         f"os={os_type}")
        server_list = list()

        try:
            # Use Couchbase transaction for atomic server allocation
            def allocate_servers(ctx):
                # Count available servers within transaction
                count_query = (
                    f"SELECT count(*) FROM `QE-server-pool` "
                    f"WHERE state='available' AND os='{os_type}' "
                    f"AND (poolId='{pool_id}' OR '{pool_id}' IN poolId)")

                count_result = ctx.query(count_query)
                available_count = 0
                for row in count_result.rows():
                    available_count = row['$1']
                    break

                self.logger.info(f"requested count: {count}, "
                                 f"available count: {available_count}")

                if count > available_count:
                    self.logger.warning("Not enough servers available")
                    return []

                # Get servers within transaction
                get_servers_query = (
                    f"SELECT *, meta().id FROM `QE-server-pool` "
                    f"WHERE state='available' AND os='{os_type}' "
                    f"AND (poolId='{pool_id}' OR '{pool_id}' IN poolId) "
                    f"ORDER BY to_number(memory) ASC LIMIT {count}")

                self.logger.debug(f"Query: {get_servers_query}")

                get_results = ctx.query(get_servers_query)

                # Process results properly
                for result in get_results.rows():
                    # Extract the document data from the result
                    doc_data = result['QE-server-pool']

                    # Update document state
                    doc_data['state'] = 'booked'
                    doc_data['prevUser'] = doc_data.get('username', '')
                    doc_data['username'] = username

                    # Replace document within transaction
                    target_id = ctx.get(self.default_collection, result['id'])
                    ctx.replace(target_id, doc_data)
                    server_list.append(doc_data['ipaddr'])

            # Execute the transaction
            _ = self.cluster.transactions.run(allocate_servers)
            self.logger.info(
                f"Allocated {len(server_list)} servers: {server_list}")
            return server_list
        except Exception as e:
            self.logger.error(f"Error in get_servers transaction: {str(e)}")
            return list()

    def release_ip(self, ipaddr, state='available'):
        """
        Release a specific IP address.

        Args:
            ipaddr: IP address to release
            state: New state for the server
        """
        self.logger.info(f"release_ip: ipaddr={ipaddr}, state={state}")

        update_string = (
            f"UPDATE `QE-server-pool` SET state='{state}'"
            f" WHERE ipaddr='{ipaddr}' AND state='booked'")

        self.logger.debug(f"Query: {update_string}")

        try:
            self.cluster.query(update_string)
            self.logger.info(f"IP {ipaddr} released")
        except Exception as e:
            self.logger.error(f"Error in release_ip: {str(e)}")

    def release_servers(self, username, state='available'):
        """
        Release all servers for a specific username using transactions.

        Args:
            username: Username whose servers should be released
            state: New state for the servers
        """
        self.logger.info(f"release_servers: username={username}, "
                         f"state={state}")

        try:
            # Use Couchbase transaction for atomic server release
            def release_servers_txn(ctx):
                # Query to find all servers for this username
                query = (
                    f"SELECT *, meta().id FROM `QE-server-pool` "
                    f"WHERE username='{username}' AND state='booked'")

                self.logger.debug(f"Query: {query}")

                results = ctx.query(query)
                released_count = 0

                # Process each server document
                for result in results.rows():
                    # Extract the document data from the result
                    doc_data = result['QE-server-pool']
                    meta_id = result['id']

                    # Update document state
                    doc_data['state'] = state
                    doc_data['prevUser'] = doc_data.get('username', '')
                    doc_data['username'] = ''

                    # Replace document within transaction
                    target_doc = ctx.get(self.default_collection, meta_id)
                    ctx.replace(target_doc, doc_data)
                    released_count += 1

                self.logger.info(
                    f"Released {released_count} servers for user {username}")
                return released_count

            # Execute the transaction
            result = self.cluster.transactions.run(release_servers_txn)
            self.logger.info(
                f"Successfully released {result} servers for user {username}")
            return result

        except Exception as e:
            self.logger.error(
                f"Error in release_servers transaction: {str(e)}")
            return 0

    def show_all(self):
        """
        Show all servers in the pool.

        Returns:
            list: List of all server documents
        """
        query_string = "SELECT * FROM `QE-server-pool`"

        try:
            results = self.cluster.query(query_string)
            server_list = [row for row in results.rows()]
            self.logger.info(f"show_all result: {len(server_list)} servers "
                             "found")
            return server_list

        except Exception as e:
            self.logger.error(f"Error in show_all: {str(e)}")
            return []
