import json
import random
import ipaddress

from .stable_topology_fts import StableTopFTS
from TestInput import TestInputSingleton
import fts.IP_Dataset.ip_data as ip_data

class StableTopExtendedFTS(StableTopFTS):

    def setUp(self):
        self.input = TestInputSingleton.input
        self.IPs = ip_data.IP
        self.valid_IPv4 = ip_data.valid_IPv4
        self.invalid_IPv4 = ip_data.invalid_IPv4
        self.valid_IPv6 = ip_data.valid_IPv6
        self.invalid_IPv6 = ip_data.invalid_IPv6
        self.ipVSexpectedHits = ip_data.ipVSexpectedHits
        self.query_run_type = self.input.param("query_run_type", "random")
        self.IP_range_queries = [
            {"field": "ip", "cidr": "192.168.0.0/16"},
            {"field": "ip", "cidr": "172.16.0.0/12"},
            {"field": "ip", "cidr": "2001:db8::/32"},
            {"field": "ip", "cidr": "2001:db8::/32"}
        ]
        self.run_n1ql_search_function = self.input.param("run_n1ql_search_function", False)
        self.validate_n1ql_and_FTS = self.input.param("validate_n1ql_and_FTS", False)
        super(StableTopExtendedFTS, self).setUp()

    def tearDown(self):
        super(StableTopExtendedFTS, self).tearDown()

    def is_valid_ip_address(self, ip_address_str):
        try:
            ipaddress.ip_address(ip_address_str)
            return True
        except ValueError:
            return False

    def run_valid_IP_queries(self):
        """
            Runs Valid IP Queries
            Runs IPv4 queries if "query_run_type" = valid_IPv4
            Runs IPv6 queries if "query_run_type" = valid_IPv6
            Runs random valid IP queries if nothing is specified
        """
        collection_index, type, index_scope, index_collections = self.define_index_parameters_collection_related()
        index = self.create_index(
            bucket=self._cb_cluster.get_bucket_by_name('default'),
            index_name="index", collection_index=collection_index, _type=type,
            scope=index_scope, collections=index_collections)
        index.add_child_field_to_default_collection_mapping(field_name="ip",
                                                            field_type="IP",
                                                            field_alias="ip",
                                                            scope=self.scope,
                                                            collection=self.collection)

        index.index_definition['uuid'] = index.get_uuid()
        index.update()
        self.sleep(5, "Waiting 5 seconds for index to update..")
        self.load_data_using_n1ql_fixed_key('default', index_scope, index_collections[0], "ip", self.IPs)
        self.wait_for_indexing_complete()

        fail_count = 0
        failed_queries = []
        validation_fail_count = 0
        validation_failed_queries = []
        for i in range(self.num_queries):
            if self.query_run_type == "valid IPv4":
                ip_address = self.valid_IPv4[random.randint(0, len(self.valid_IPv4) - 1)]
            elif self.query_run_type == "valid IPv6":
                ip_address = self.valid_IPv6[random.randint(0, len(self.valid_IPv6) - 1)]
            else:
                valid_set = self.valid_IPv4 + self.valid_IPv6
                ip_address = valid_set[random.randint(0, len(valid_set) - 1)]
            query = {"field": "ip", "cidr": f"{ip_address}"}
            if isinstance(query, str):
                query = json.loads(query)
            n1ql_hits = -1
            if self.run_n1ql_search_function:
                n1ql_query = f"SELECT COUNT(*) FROM `default`.{index_scope}.{index_collections[0]} AS t1 WHERE SEARCH(t1, {{\"field\": \"ip\", \"cidr\": \"{ip_address}\"}});"
                n1ql_hits = self._cb_cluster.run_n1ql_query(n1ql_query)['results'][0]['$1']
                if n1ql_hits == 0:
                    n1ql_hits = -1
                self.log.info("FTS Hits for N1QL query: %s" % n1ql_hits)

            hits, matches, time_taken, status = index.execute_query(query)
            self.log.info("FTS Hits for Search query: %s" % hits)

            if hits == self.ipVSexpectedHits[ip_address]:
                self.log.info(f"Query Passed! FTS returned {hits} hits for a VALID IP - {ip_address}")
            else:
                self.log.info(f"Query Failed! FTS returned {hits} hits for VALID IP - {ip_address} while expected were {self.ipVSexpectedHits[ip_address]}")
                fail_count += 1
                failed_queries.append({"query": query,
                                       "reason": f"FTS returned {hits} hits for VALID IP - {ip_address} while expected were {self.ipVSexpectedHits[ip_address]}"})

            if self.validate_n1ql_and_FTS:
                if n1ql_hits == hits:
                    self.log.info(
                        f"Validation for N1QL and FTS Passed! N1QL hits =  {n1ql_hits}, FTS hits = {hits}")
                else:
                    validation_fail_count += 1
                    validation_failed_queries.append(
                        {"query": query, "reason": f"N1QL hits =  {n1ql_hits}, FTS hits = {hits}"})

        if not self.validate_n1ql_and_FTS:
            if fail_count:
                raise Exception("%s out of %s queries failed! - %s" % (fail_count,
                                                                       self.num_queries,
                                                                       failed_queries))
            else:
                self.log.info(
                    "SUCCESS: %s out of %s queries passed" % (self.num_queries - fail_count, self.num_queries))

        if fail_count and validation_fail_count:
            raise Exception(f"{fail_count} out of {self.num_queries} queries failed! - {failed_queries}. \n {validation_fail_count} out of {self.num_queries} n1ql-fts validation failed! - {validation_failed_queries}")
        elif fail_count:
            raise Exception(f"{fail_count} out of {self.num_queries} queries failed! - {failed_queries}. \n"
                            f"SUCCESS {self.num_queries - validation_fail_count} out of {self.num_queries} n1ql-fts validation passed!")
        elif validation_fail_count:
            raise Exception(
                f"{validation_fail_count} out of {self.num_queries} n1ql-fts validation failed! - {validation_failed_queries}\n"
                f"SUCCESS: {self.num_queries - fail_count} out of {self.num_queries} queries passed.")
        else:
            self.log.info(f"SUCCESS: {self.num_queries - fail_count} out of {self.num_queries} queries passed")
            self.log.info(
                f"SUCCESS: {self.num_queries - validation_fail_count} out of {self.num_queries} n1ql-fts validation passed.")

    def run_invalid_IP_queries(self):
        """
            Runs Invalid IP Queries
            Runs invalid IPv4 queries if "query_run_type" = valid_IPv4
            Runs invalid IPv6 queries if "query_run_type" = valid_IPv6
            Runs random invalid IP queries if nothing is specified
        """
        collection_index, type, index_scope, index_collections = self.define_index_parameters_collection_related()
        index = self.create_index(
            bucket=self._cb_cluster.get_bucket_by_name('default'),
            index_name="index", collection_index=collection_index, _type=type,
            scope=index_scope, collections=index_collections)
        index.add_child_field_to_default_collection_mapping(field_name="ip",
                                                            field_type="IP",
                                                            field_alias="ip",
                                                            scope=self.scope,
                                                            collection=self.collection)
        index.index_definition['uuid'] = index.get_uuid()
        index.update()
        self.sleep(5, "Waiting 5 seconds for index to update..")
        self.load_data_using_n1ql_fixed_key('default', index_scope, index_collections[0], "ip", self.IPs)
        self.wait_for_indexing_complete()

        fail_count = 0
        failed_queries = []
        validation_fail_count = 0
        validation_failed_queries = []
        for i in range(self.num_queries):
            if self.query_run_type == "invalid IPv4":
                ip_address = self.valid_IPv4[random.randint(0, len(self.valid_IPv4) - 1)]
            elif self.query_run_type == "invalid IPv6":
                ip_address = self.valid_IPv6[random.randint(0, len(self.valid_IPv6) - 1)]
            else:
                invalid_set = self.invalid_IPv4 + self.invalid_IPv6
                ip_address = invalid_set[random.randint(0, len(invalid_set) - 1)]
            query = {"field": "ip", "cidr": f"{ip_address}"}
            if isinstance(query, str):
                query = json.loads(query)
            n1ql_hits = -1
            if self.run_n1ql_search_function:
                n1ql_query = f"SELECT COUNT(*) FROM `default`.{index_scope}.{index_collections[0]} AS t1 WHERE SEARCH(t1, {{\"field\": \"ip\", \"cidr\": \"{ip_address}\"}});"
                try:
                    n1ql_hits = self._cb_cluster.run_n1ql_query(n1ql_query)['results'][0]['$1']
                except:
                    n1ql_hits = -1
                if n1ql_hits == 0:
                    n1ql_hits = -1
                self.log.info("FTS Hits for N1QL query: %s" % n1ql_hits)

            hits, matches, time_taken, status = index.execute_query(query)
            self.log.info("FTS Hits for query: %s" % hits)

            if hits == self.ipVSexpectedHits[ip_address]:
                self.log.info(f"Query Passed! FTS returned {hits} hits for an INVALID IP - {ip_address}")
            else:
                self.log.info(f"Query Failed! FTS returned {hits} hits for INVALID IP - {ip_address} while expected were {self.ipVSexpectedHits[ip_address]}")
                fail_count += 1
                failed_queries.append({"query": query,
                                       "reason": f"FTS returned {hits} hits for IP - {ip_address} while expected were {self.ipVSexpectedHits[ip_address]}"})

            if self.validate_n1ql_and_FTS:
                if n1ql_hits == hits:
                    self.log.info(
                        f"Validation for N1QL and FTS Passed! N1QL hits =  {n1ql_hits}, FTS hits = {hits}")
                else:
                    validation_fail_count += 1
                    validation_failed_queries.append(
                        {"query": query, "reason": f"N1QL hits =  {n1ql_hits}, FTS hits = {hits}"})

        if not self.validate_n1ql_and_FTS:
            if fail_count:
                raise Exception("%s out of %s queries failed! - %s" % (fail_count,
                                                                       self.num_queries,
                                                                       failed_queries))
            else:
                self.log.info(
                    "SUCCESS: %s out of %s queries passed" % (self.num_queries - fail_count, self.num_queries))

        if fail_count and validation_fail_count:
            raise Exception(f"{fail_count} out of {self.num_queries} queries failed! - {failed_queries}. \n"
                            f"{validation_fail_count} out of {self.num_queries} n1ql-fts validation failed! - {validation_failed_queries}")
        elif fail_count:
            raise Exception(f"{fail_count} out of {self.num_queries} queries failed! - {failed_queries}. \n"
                            f"SUCCESS {self.num_queries - validation_fail_count} out of {self.num_queries} n1ql-fts validation passed!")
        elif validation_fail_count:
            raise Exception(
                f"{validation_fail_count} out of {self.num_queries} n1ql-fts validation failed! - {validation_failed_queries}\n"
                f"SUCCESS: {self.num_queries - fail_count} out of {self.num_queries} queries passed.")
        else:
            self.log.info(f"SUCCESS: {self.num_queries - fail_count} out of {self.num_queries} queries passed")
            self.log.info(
                f"SUCCESS: {self.num_queries - validation_fail_count} out of {self.num_queries} n1ql-fts validation passed.")

    def IP_Range_queries(self):
        """
            Runs Range Queries for IPv4 and IPv6 IP Addresses
        """
        collection_index, type, index_scope, index_collections = self.define_index_parameters_collection_related()
        index = self.create_index(
            bucket=self._cb_cluster.get_bucket_by_name('default'),
            index_name="index", collection_index=collection_index, _type=type,
            scope=index_scope, collections=index_collections)
        index.add_child_field_to_default_collection_mapping(field_name="ip",
                                                            field_type="IP",
                                                            field_alias="ip",
                                                            scope=self.scope,
                                                            collection=self.collection)

        index.index_definition['uuid'] = index.get_uuid()
        index.update()
        self.sleep(5, "Waiting 5 seconds for index to update..")
        self.load_data_using_n1ql_fixed_key('default', index_scope, index_collections[0], "ip", self.IPs)
        self.wait_for_indexing_complete()

        fail_count = 0
        failed_queries = []
        validation_fail_count = 0
        validation_failed_queries = []
        for range_query in self.IP_range_queries:
            query = range_query
            if isinstance(range_query, str):
                query = json.loads(range_query)
            n1ql_hits = -1
            if self.run_n1ql_search_function:
                n1ql_query = f"SELECT COUNT(*) FROM `default`.{index_scope}.{index_collections[0]} AS t1 WHERE SEARCH(t1, {query});"
                n1ql_hits = self._cb_cluster.run_n1ql_query(n1ql_query)['results'][0]['$1']
                if n1ql_hits == 0:
                    n1ql_hits = -1
                self.log.info("FTS Hits for N1QL query: %s" % n1ql_hits)

            hits, matches, time_taken, status = index.execute_query(query)
            self.log.info("FTS Hits for query: %s" % hits)

            expected_hits = 0
            expected_ip_hits = []
            for ip_1 in self.IPs:
                if isinstance(ip_1, list):
                    for ip_in_list in ip_1:
                        try:
                            if ipaddress.ip_address(ip_in_list) in ipaddress.ip_network(range_query['cidr']):
                                expected_hits += 1
                                expected_ip_hits.append(ip_in_list)
                                break
                        except:
                            pass
                else:
                    try:
                        if ipaddress.ip_address(ip_1) in ipaddress.ip_network(range_query['cidr']):
                            expected_hits += 1
                            expected_ip_hits.append(ip_1)
                    except:
                        pass

            if hits == expected_hits:
                self.log.info(f"Query Passed! FTS returned {hits} hits for range query - {query}")
            else:
                self.log.error(
                    f"Query Failed! FTS returned {hits} hits for a range query - {query} having VALID IP while expected hits were {expected_hits}")
                self.log.error(f"Expected IPs to be returned : {expected_ip_hits}")
                fail_count += 1
                failed_queries.append({"query": query,
                                       "reason": f"FTS returned {hits} hits for a range query - {query} having VALID IP while expected hits were {expected_hits}"})

            if self.validate_n1ql_and_FTS:
                if n1ql_hits == hits:
                    self.log.info("Validation for N1QL and FTS Passed!")
                else:
                    if fail_count:
                        raise Exception(f"{fail_count} out of {len(self.IP_range_queries)} queries failed! - {failed_queries}. \n"
                                        f"N1QL Validation Failed N1QL hits =  {n1ql_hits}, FTS hits = {hits}")
                    else:
                        raise Exception(
                            f"SUCCESS: {len(self.IP_range_queries) - fail_count} out of {len(self.IP_range_queries)} queries passed. \n"
                            f"N1QL Validation Failed N1QL hits =  {n1ql_hits}, FTS hits = {hits}")

            if self.validate_n1ql_and_FTS:
                if n1ql_hits == hits:
                    self.log.info(
                        f"Validation for N1QL and FTS Passed! N1QL hits =  {n1ql_hits}, FTS hits = {hits}")
                else:
                    self.log.info(
                        f"Validation for N1QL and FTS Failed! N1QL hits =  {n1ql_hits}, FTS hits = {hits}")
                    validation_fail_count += 1
                    validation_failed_queries.append(
                        {"query": query, "reason": f"N1QL hits =  {n1ql_hits}, FTS hits = {hits}"})

        if not self.validate_n1ql_and_FTS:
            if fail_count:
                raise Exception("%s out of %s queries failed! - %s" % (fail_count,
                                                                       len(self.IP_range_queries),
                                                                       failed_queries))
            else:
                self.log.info(
                    "SUCCESS: %s out of %s queries passed" % (len(self.IP_range_queries) - fail_count, len(self.IP_range_queries)))

        if fail_count and validation_fail_count:
            raise Exception(f"{fail_count} out of {len(self.IP_range_queries)} queries failed! - {failed_queries}. \n"
                            f"{validation_fail_count} out of {len(self.IP_range_queries)} n1ql-fts validation failed! - {validation_failed_queries}")
        elif fail_count:
            raise Exception(f"{fail_count} out of {len(self.IP_range_queries)} queries failed! - {failed_queries}. \n"
                            f"SUCCESS {len(self.IP_range_queries) - validation_fail_count} out of {len(self.IP_range_queries)} n1ql-fts validation passed!")
        elif validation_fail_count:
            raise Exception(
                f"{validation_fail_count} out of {len(self.IP_range_queries)} n1ql-fts validation failed! - {validation_failed_queries}\n"
                f"SUCCESS: {len(self.IP_range_queries) - fail_count} out of {len(self.IP_range_queries)} queries passed.")
        else:
            self.log.info(f"SUCCESS: {len(self.IP_range_queries) - fail_count} out of {len(self.IP_range_queries)} queries passed")
            self.log.info(
                f"SUCCESS: {len(self.IP_range_queries) - validation_fail_count} out of {len(self.IP_range_queries)} n1ql-fts validation passed.")
