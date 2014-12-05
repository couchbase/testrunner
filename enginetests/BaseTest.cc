#include "gtest/gtest.h"
#include "stdlib.h"
#include <limits>
#include <cstring>
#include <iostream>
#include <vector>
#include <sstream>
#include <libcouchbase/couchbase.h>
#include "dataclient.cc"

extern char *getvaluebuf;
extern long *getvaluesize;
extern lcb_datatype_t* getvaluedtype;
extern bool warmupdone;
extern std::vector<std::string> threadstate;
char **testargv;
int testargc;
int numReplicaget;
extern struct stats_generic genericstats;

class BaseTest : public testing::Test
{

protected:

	virtual void SetUp()
	{
		createBucket(true);
		memset(&create_options, 0, sizeof(create_options));
		if (testargc > 0)
		{
			create_options.v.v0.host = testargv[1];
		}
		if (testargc > 1)
		{
			create_options.v.v0.user = testargv[2];
			create_options.v.v0.bucket = testargv[2];
		}
		if (testargc > 2)
		{
			create_options.v.v0.passwd = testargv[3];
		}
		err = lcb_create(&instance, &create_options);
		assert(err == LCB_SUCCESS);
		(void) lcb_set_error_callback(instance, error_callback);
		err = lcb_connect(instance);
		assert(err == LCB_SUCCESS);
		fprintf(stderr, "\nconnect succeeded\n");
		(void) lcb_set_get_callback(instance, get_callback);
		(void) lcb_set_store_callback(instance, store_callback);
		(void) lcb_set_stat_callback(instance, stats_callback);

	}

	virtual void TearDown()
	{
		lcb_destroy(instance);
		resetboolFlags();
		genericstats.statsvec.clear();
		genericstats.refcount = 0;
	}

	void sendHello()
	{
		lcb_wait(instance);
		{
			err = lcb_hello(instance, NULL);
			assert(err == LCB_SUCCESS);
		}

	}

	void resetboolFlags()
	{
		warmupdone = false;
		gotwm_val_cn = false;
		threadstate.clear();
	}

	std::string exec(char* cmd)
	{
		FILE* pipe = popen(cmd, "r");
		if (!pipe)
			return "ERROR";
		char buffer[128];
		std::string result = "";
		while (!feof(pipe))
		{
			if (fgets(buffer, 128, pipe) != NULL)
				result += buffer;
		}
		pclose(pipe);
		return result;
	}

	void compareDocs(const lcb_store_cmd_t *commands)
	{
		EXPECT_EQ(*getvaluedtype, commands->v.v0.datatype);
		EXPECT_EQ(*getvaluesize, commands->v.v0.nbytes);
		for (long i = 0; i < *getvaluesize; i++)
		{
			EXPECT_EQ(*(getvaluebuf + i), *((char* )commands->v.v0.bytes + i));
		}
	}

	void createBucket(bool fullEvict)
	{
		char del[] = "/opt/couchbase/bin/couchbase-cli bucket-delete -c 127.0.0.1:8091 --bucket=default -u Administrator -p password";
		exec(del);
		sleep(45);
		if (!fullEvict)
		{
			char create[] =
			    "/opt/couchbase/bin/couchbase-cli bucket-create -c 127.0.0.1:8091 --bucket=default --bucket-type=couchbase --bucket-ramsize=200 --bucket-replica=1 -u Administrator -p password";
			exec(create);
			sleep(45);
		}
		else if (fullEvict)
		{
			char create[] =
			    "/opt/couchbase/bin/couchbase-cli bucket-create -c 127.0.0.1:8091 --bucket=default --bucket-type=couchbase --bucket-ramsize=200 --bucket-eviction-policy=fullEviction --bucket-replica=1 -u Administrator -p password";
			exec(create);
			sleep(45);

		}

	}

	uint64_t insertItems(uint64_t numItems)
	{
		uint64_t numinserted = 0;
		const char inflated[] = "abc123";
		size_t inflated_len = strlen(inflated);
		for (uint64_t i = 0; i < numItems; i++)
		{
			lcb_store_cmd_t cmd;
			char buf[20];
			const lcb_store_cmd_t *commands[1];
			commands[0] = &cmd;
			memset(&cmd, 0, sizeof(cmd));
			cmd.v.v0.operation = LCB_SET;
			cmd.v.v0.datatype = LCB_BINARY_RAW_BYTES;
			std::stringstream ss;
			ss << i;
			ss << "dexpire";
			std::string myString = ss.str();
			cmd.v.v0.key = myString.c_str();
			cmd.v.v0.nkey = myString.size();
			cmd.v.v0.bytes = inflated;
			cmd.v.v0.nbytes = inflated_len;
			cmd.v.v0.exptime = 0x9900;
			lcb_error_t err = lcb_store(instance, NULL, 1, &commands[0]);
			if (err == LCB_SUCCESS)
				numinserted++;
			//callget(&instance, commands[0]->v.v0.key, commands[0]->v.v0.nkey);
			lcb_wait(instance);
			if (i % 100000 == 0)
				sleep(1);
		}
		return numinserted;
	}

	void verifyReplication(uint64_t numItems)
	{
		uint64_t numinserted = 0;
		const char inflated[] = "replicationverification";
		size_t inflated_len = strlen(inflated);
		for (uint64_t i = 0; i < numItems; i++)
		{
			lcb_store_cmd_t cmd;
			char buf[20];
			const lcb_store_cmd_t *commands[1];
			commands[0] = &cmd;
			memset(&cmd, 0, sizeof(cmd));
			cmd.v.v0.operation = LCB_SET;
			cmd.v.v0.datatype = LCB_BINARY_RAW_BYTES;
			std::stringstream ss;
			ss << i;
			ss << "replica";
			std::string myString = ss.str();
			cmd.v.v0.key = myString.c_str();
			cmd.v.v0.nkey = myString.size();
			cmd.v.v0.bytes = inflated;
			cmd.v.v0.nbytes = inflated_len;
			cmd.v.v0.exptime = 0x9900;
			lcb_error_t err = lcb_store(instance, NULL, 1, &commands[0]);
			if (err == LCB_SUCCESS)
				numinserted++;
			//callget(&instance, commands[0]->v.v0.key, commands[0]->v.v0.nkey);
			lcb_wait(instance);
			sleep(1);
		}
		EXPECT_EQ(numItems, numinserted);
		for (uint64_t i = 0; i < numItems; i++)
		{
			lcb_get_replica_cmd_t *get = (lcb_get_replica_cmd_t*) calloc(1, sizeof(*get));
			get->version = 0;
			std::stringstream ss;
			ss << i;
			ss << "replica";
			std::string myString = ss.str();
			get->v.v0.key = myString.c_str();
			get->v.v0.nkey = myString.size();
			lcb_get_replica_cmd_st* commands[] =
			{ get };
			lcb_get_replica(instance, NULL, 1, commands);
			lcb_wait(instance);
		}
		EXPECT_GE(numReplicaget, (numItems * 9) / 10);
	}

	void insert_items_instances(uint64_t numItems, std::vector<lcb_t*> instanceVec)
	{

		uint64_t numinserted = 0;
		const char docvalue[] = "abc123";
		size_t doc_len = strlen(docvalue);
		for (uint64_t i = 0; i < numItems; i++)
		{
			lcb_store_cmd_t cmd;
			const lcb_store_cmd_t *commands[1];
			commands[0] = &cmd;
			memset(&cmd, 0, sizeof(cmd));
			cmd.v.v0.operation = LCB_SET;
			cmd.v.v0.datatype = LCB_BINARY_RAW_BYTES;
			std::stringstream ss;
			ss << i;
			ss << "key";
			std::string myString = ss.str();
			cmd.v.v0.key = myString.c_str();
			cmd.v.v0.nkey = myString.size();
			cmd.v.v0.bytes = docvalue;
			cmd.v.v0.nbytes = doc_len;
			for (std::vector<lcb_t*>::iterator itr = instanceVec.begin(); itr != instanceVec.end(); ++itr)
			{
				lcb_error_t err = lcb_store(**itr, NULL, 1, &commands[0]);
				lcb_wait(**itr);
			}
		}
	}

	StatsVector getgenericstats(std::string statsreq, lcb_t& instance)
	{

		lcb_server_stats_cmd_t stats;
		stats.version = 0;
		stats.v.v0.name = statsreq.c_str();
		stats.v.v0.nname = statsreq.length();
		const lcb_server_stats_cmd_t *commandstat[1];
		commandstat[0] = &stats;
		lcb_wait(instance);
		lcb_server_stats(instance, NULL, 1, &commandstat[0]);
		lcb_wait(instance);
		return genericstats.statsvec;
	}

	void DatatypeTester(const lcb_store_cmd_t *commands)
	{
		lcb_wait(instance);
		{
			err = lcb_store(instance, NULL, 1, &commands);
			assert(err == LCB_SUCCESS);
		}
		lcb_wait(instance);
		callget(&instance, commands->v.v0.key, commands->v.v0.nkey);
		lcb_wait(instance);
		fprintf(stderr, "\nInside DatatypeTester\n");
		compareDocs(commands);
	}
	lcb_uint32_t tmo;
	const lcb_store_cmd_t *commands[1];
	lcb_error_t err;
	lcb_t instance;
	struct lcb_create_st create_options;
};
