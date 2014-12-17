#include "BaseTest.cc"
#include <fstream>

extern char* wm_val_cn;
extern char* rep_val_cn;
extern char* ejected_val_cn;
extern char* resident_val_cn;
extern bool gotreplica_curr_items;
extern bool goteject_num_items;
extern bool got_active_resident;
extern struct stats_generic genericstats;

class WarmupTest : public BaseTest
{

protected:

	void TearDown()
	{
		lcb_destroy(instance);
		resetboolFlags();
	}

	bool verifyWarmup(uint64_t expectedItems)
	{
		while (!warmupdone || !gotwm_val_cn)
		{
			lcb_server_stats_cmd_t stats;
			stats.version = 0;
			stats.v.v0.name = "warmup";
			stats.v.v0.nname = 6;
			const lcb_server_stats_cmd_t *commandstat[1];
			commandstat[0] = &stats;
			lcb_server_stats(instance, NULL, 1, &commandstat[0]);
			lcb_wait(instance);

		}

		int wm_val_cn_i = atoi(wm_val_cn);
		fprintf(stderr, "\nnumber of warmed up elements:  %d \n", wm_val_cn_i);
		fprintf(stderr, "\nnumber of expected elements:  %lu \n", expectedItems);
		int resident_ratio = getActiveResidentRatio();
		int ejected_itm_cnt = getEjectedNumItems();
		fprintf(stderr, "\nActiveResidentRatio :  %d \n", resident_ratio);
		if (resident_ratio == 100)
		{
			return (wm_val_cn_i == expectedItems);
		}
		else
			return (((wm_val_cn_i - ejected_itm_cnt) * 100) / expectedItems == resident_ratio);
	}

	bool verifytransition(std::vector<std::string> const & uuphases, bool valueEvic)
	{
		std::vector<std::string>::const_iterator itr = uuphases.begin();
		EXPECT_FALSE((*itr).compare("initialize"));
		std::cout << "state from stats: " << *itr << '\n';
		itr++;
		EXPECT_FALSE((*itr).compare("creating vbuckets"));
		std::cout << "state from stats: " << *itr << '\n';
		itr++;
		EXPECT_FALSE((*itr).compare("estimating database item count"));
		std::cout << "state from stats: " << *itr << '\n';
		itr++;
		if (valueEvic)
		{
			EXPECT_FALSE((*itr).compare("loading keys"));
			std::cout << "state from stats: " << *itr << '\n';
			itr++;
			EXPECT_FALSE((*itr).compare("determine access log availability"));
			std::cout << "state from stats: " << *itr << '\n';
			itr++;
			EXPECT_FALSE((*itr).compare("loading data"));
			std::cout << "state from stats: " << *itr << '\n';
			itr++;
		}
		if (!valueEvic)
		{
			EXPECT_FALSE((*itr).compare("determine access log availability"));
			std::cout << "state from stats: " << *itr << '\n';
			itr++;
			EXPECT_FALSE((*itr).compare("loading k/v pairs"));
			std::cout << "state from stats: " << *itr << '\n';
			itr++;
		}
		EXPECT_FALSE((*itr).compare("done"));
		std::cout << "state from stats: " << *itr << '\n';
	}

	int getReplicaCount()
	{
		while (!gotreplica_curr_items)
		{
			lcb_server_stats_cmd_t stats;
			stats.version = 0;
			stats.v.v0.name = ""; //"all";
			stats.v.v0.nname = 0;
			const lcb_server_stats_cmd_t *commandstat[1];
			commandstat[0] = &stats;
			lcb_server_stats(instance, NULL, 1, &commandstat[0]);
			lcb_wait(instance);
		}

		int replicacount = atoi(rep_val_cn);
		return replicacount;
	}

	int getActiveResidentRatio()
	{

		while (!got_active_resident)
		{
			lcb_server_stats_cmd_t stats;
			stats.version = 0;
			stats.v.v0.name = ""; //"all";
			stats.v.v0.nname = 0;
			const lcb_server_stats_cmd_t *commandstat[1];
			commandstat[0] = &stats;
			lcb_server_stats(instance, NULL, 1, &commandstat[0]);
			lcb_wait(instance);
		}
		return atoi(resident_val_cn);
	}

	int getEjectedNumItems()
	{

		while (!goteject_num_items)
		{
			lcb_server_stats_cmd_t stats;
			stats.version = 0;
			stats.v.v0.name = ""; //"all";
			stats.v.v0.nname = 0;
			const lcb_server_stats_cmd_t *commandstat[1];
			commandstat[0] = &stats;
			lcb_server_stats(instance, NULL, 1, &commandstat[0]);
			lcb_wait(instance);
		}
		return atoi(ejected_val_cn);
	}
//reset all the boolean flags initialized in dataclient.c, BaseTest.cc and in current file
	void resetboolFlags()
	{
		gotwm_val_cn = false;
		warmupdone = false;
		gotreplica_curr_items = false;
		got_active_resident = false;
		goteject_num_items = false;
		threadstate.clear();
	}

	uint64_t numInserted;
};

TEST_F(WarmupTest, Shutdown_Stat_Test)
{
	bool fullEvict = true;
	createBucket(!fullEvict);
	uint64_t numItems = 1000;
	numInserted = insertItems(numItems);
	sleep(10);
	fprintf(stderr, "\nnumber of inserted elements:  %lu \n", numInserted);
	//stop server gracefully
	char stop[] = "sudo /etc/init.d/couchbase-server stop";
	exec(stop);
	sleep(30);
	std::ifstream file("/opt/couchbase/var/lib/couchbase/data/default/stats.json");
	std::string *result = new std::string;
	std::getline(file, *result);
	size_t valpos = result->find("ep_force_shutdown") + 21;
	std::string forceshutdown = result->substr(valpos, 5);
	std::cout << "forceshutdown: " << forceshutdown << "\n";
	EXPECT_EQ(forceshutdown, "false");
	char start[] = "sudo /etc/init.d/couchbase-server start";
	exec(start);
	sleep(30);
}

TEST_F(WarmupTest, WarmupActive_100DGM_ValueEvic_Test)
{
	bool fullEvict = true;
	createBucket(!fullEvict);
	uint64_t numItems = 100000;
	numInserted = insertItems(numItems);
	sleep(10);
	fprintf(stderr, "\nnumber of inserted elements:  %lu \n", numInserted);
	//restart server for setting warmup
	char restart2[] = "sudo /etc/init.d/couchbase-server restart";
	exec(restart2);
	sleep(40);
	EXPECT_TRUE(verifyWarmup(numInserted));
}

TEST_F(WarmupTest, WarmupActive_100DGM_FullEvic_Test)
{
	bool fullEvict = true;
	createBucket(fullEvict);
	uint64_t numItems = 100000;
	numInserted = insertItems(numItems);
	sleep(10);
	//restart server for setting warmup
	char restart2[] = "sudo /etc/init.d/couchbase-server restart";
	exec(restart2);
	sleep(40);
	EXPECT_TRUE(verifyWarmup(numInserted));
}

TEST_F(WarmupTest, WarmupActive_64DGM_ValueEvic_Test)
{
	bool fullEvict = true;
	createBucket(!fullEvict);
	uint64_t numItems = 1000000;
	numInserted = insertItems(numItems);
	sleep(10);
	//restart server for setting warmup
	char restart2[] = "sudo /etc/init.d/couchbase-server restart";
	exec(restart2);
	sleep(40);
	EXPECT_TRUE(verifyWarmup(numInserted));
}

TEST_F(WarmupTest, WarmupActive_74DGM_FullEvic_Test)
{
	bool fullEvict = true;
	createBucket(fullEvict);
	uint64_t numItems = 1500000;
	numInserted = insertItems(numItems);
	sleep(10);
	//restart server for setting warmup
	char restart2[] = "sudo /etc/init.d/couchbase-server restart";
	exec(restart2);
	sleep(40);
	EXPECT_TRUE(verifyWarmup(numInserted));
}

TEST_F(WarmupTest, WarmupActive_30DGM_ValueEvic_Test)
{
	//bool fullEvict = true;
	//createBucket(!fullEvict);
	uint64_t numItems = 1200000;
	numInserted = insertItems(numItems);
	sleep(10);
	//restart server for setting warmup
	char restart2[] = "sudo /etc/init.d/couchbase-server restart";
	exec(restart2);
	sleep(40);
	EXPECT_TRUE(verifyWarmup(numInserted));
	//char delBucket[] = "/root/ep-engine-tests/delBucket";
	//exec(delBucket);
	sleep(40);
}

TEST_F(WarmupTest, WarmupActive_55DGM_FullEvic_Test)
{
	bool fullEvict = true;
	createBucket(fullEvict);
	uint64_t numItems = 2000000;
	numInserted = insertItems(numItems);
	sleep(10);
	//restart server for setting warmup
	char restart2[] = "sudo /etc/init.d/couchbase-server restart";
	exec(restart2);
	sleep(40);
	EXPECT_TRUE(verifyWarmup(numInserted));
}

TEST_F(WarmupTest, WarmupActive_10DGM_ValueEvic_Test)
{
	bool fullEvict = true;
	createBucket(!fullEvict);
	uint64_t numItems = 1500000;
	numInserted = insertItems(numItems);
	sleep(10);
	//restart server for setting warmup
	char restart2[] = "sudo /etc/init.d/couchbase-server restart";
	exec(restart2);
	sleep(40);
	EXPECT_TRUE(verifyWarmup(numInserted));
}

TEST_F(WarmupTest, WarmupActive_30DGM_FullEvic_Test)
{
	bool fullEvict = true;
	createBucket(fullEvict);
	uint64_t numItems = 3500000;
	numInserted = insertItems(numItems);
	sleep(10);
	//restart server for setting warmup
	char restart2[] = "sudo /etc/init.d/couchbase-server restart";
	exec(restart2);
	sleep(40);
	EXPECT_TRUE(verifyWarmup(numInserted));
}

TEST_F(WarmupTest, WarmupActive_4DGM_ValueEvic_Test)
{
	bool fullEvict = true;
	createBucket(!fullEvict);
	uint64_t numItems = 1600000;
	numInserted = insertItems(numItems);
	sleep(10);
	//restart server for setting warmup
	char restart2[] = "sudo /etc/init.d/couchbase-server restart";
	exec(restart2);
	sleep(40);
	EXPECT_TRUE(verifyWarmup(numInserted));
}

TEST_F(WarmupTest, WarmupActive_18DGM_FullEvic_Test)
{
	bool fullEvict = true;
	createBucket(fullEvict);
	uint64_t numItems = 6000000;
	numInserted = insertItems(numItems);
	sleep(10);
	//restart server for setting warmup
	char restart2[] = "sudo /etc/init.d/couchbase-server restart";
	exec(restart2);
	sleep(40);
	EXPECT_TRUE(verifyWarmup(numInserted));
}

TEST_F(WarmupTest, PostWarmupReplication_Test)
{
	char addNode[] = "/root/ep-engine-tests/addNode";
	exec(addNode);
	sleep(40);
	bool fullEvict = true;
	createBucket(!fullEvict);
	uint64_t numItems = 100000;
	numInserted = insertItems(numItems);
	sleep(10);
	int repcount = getReplicaCount();
	fprintf(stderr, "\nnumber of replica  elements:  %d \n", repcount);
	//restart server for setting warmup
	char restart2[] = "sudo /etc/init.d/couchbase-server restart";
	exec(restart2);
	sleep(40);
	int warmuprepcount = getReplicaCount();
	fprintf(stderr, "\nnumber of replica  elements after restart:  %d \n", warmuprepcount);
	EXPECT_EQ(warmuprepcount, repcount);
	verifyReplication(10);
}

TEST_F(WarmupTest, WarmupReplica_100DGM_ValueEvic_Test)
{
	char addNode[] = "/root/ep-engine-tests/addNode";
	exec(addNode);
	sleep(40);
	bool fullEvict = true;
	createBucket(!fullEvict);
	uint64_t numItems = 100000;
	numInserted = insertItems(numItems);
	sleep(10);
	int repcount = getReplicaCount();
	fprintf(stderr, "\nnumber of replica  elements:  %d \n", repcount);
	//restart server for setting warmup
	char restart2[] = "sudo /etc/init.d/couchbase-server restart";
	exec(restart2);
	sleep(40);
	int warmuprepcount = getReplicaCount();
	fprintf(stderr, "\nnumber of replica  elements after restart:  %d \n", warmuprepcount);
	EXPECT_EQ(warmuprepcount, repcount);
	char removeNode[] = "/root/ep-engine-tests/removeNode";
	exec(removeNode);
}

TEST_F(WarmupTest, WarmupReplica_100DGM_FullEvic_Test)
{
	char addNode[] = "/root/ep-engine-tests/addNode";
	exec(addNode);
	sleep(40);
	bool fullEvict = true;
	createBucket(fullEvict);
	uint64_t numItems = 100000;
	numInserted = insertItems(numItems);
	sleep(10);
	int repcount = getReplicaCount();
	fprintf(stderr, "\nnumber of replica  elements:  %d \n", repcount);
	//restart server for setting warmup
	char restart2[] = "sudo /etc/init.d/couchbase-server restart";
	exec(restart2);
	sleep(40);
	int warmuprepcount = getReplicaCount();
	fprintf(stderr, "\nnumber of warmed up replica  elements:  %d \n", warmuprepcount);
	EXPECT_EQ(warmuprepcount, repcount);
	char removeNode[] = "/root/ep-engine-tests/removeNode";
	exec(removeNode);
}

TEST_F(WarmupTest, WarmupReplica_20DGM_ValueEvic_Test)
{
	char addNode[] = "/root/ep-engine-tests/addNode";
	exec(addNode);
	sleep(40);
	bool fullEvict = true;
	createBucket(!fullEvict);
	uint64_t numItems = 1500000;
	numInserted = insertItems(numItems);
	sleep(10);
	int repcount = getReplicaCount();
	fprintf(stderr, "\nnumber of replica  elements:  %d \n", repcount);
	//restart server for setting warmup
	char restart2[] = "sudo /etc/init.d/couchbase-server restart";
	exec(restart2);
	sleep(40);
	int warmuprepcount = getReplicaCount();
	fprintf(stderr, "\nnumber of replica  elements after restart:  %d \n", warmuprepcount);
	EXPECT_EQ(warmuprepcount, repcount);
	char removeNode[] = "/root/ep-engine-tests/removeNode";
	exec(removeNode);
}

TEST_F(WarmupTest, WarmupReplica_20DGM_FullEvic_Test)
{
	char addNode[] = "/root/ep-engine-tests/addNode";
	exec(addNode);
	sleep(40);
	bool fullEvict = true;
	createBucket(fullEvict);
	uint64_t numItems = 1500000;
	numInserted = insertItems(numItems);
	sleep(10);
	int repcount = getReplicaCount();
	fprintf(stderr, "\nnumber of replica  elements:  %d \n", repcount);
	//restart server for setting warmup
	char restart2[] = "sudo /etc/init.d/couchbase-server restart";
	exec(restart2);
	sleep(40);
	int warmuprepcount = getReplicaCount();
	fprintf(stderr, "\nnumber of warmed up replica  elements:  %d \n", warmuprepcount);
	EXPECT_EQ(warmuprepcount, repcount);
	char removeNode[] = "/root/ep-engine-tests/removeNode";
	exec(removeNode);
}

TEST_F(WarmupTest, WarmupStateTransValueTest)
{

	bool fullEvict = true;
	createBucket(!fullEvict);
	uint64_t numItems = 100000;
	numInserted = insertItems(numItems);
	sleep(10);
	fprintf(stderr, "\nnumber of inserted elements:  %lu \n", numInserted);
	//restart server for setting warmup
	char restart[] = "sudo /etc/init.d/couchbase-server restart";
	exec(restart);
	EXPECT_TRUE(verifyWarmup(numInserted));
	std::vector<std::string> uuphases;
	std::string prevstr = "";
	sleep(10);
	for (std::vector<std::string>::iterator i = threadstate.begin(); i != threadstate.end(); i++)
	{
		if (prevstr.compare(*i) != 0)
		{
			uuphases.push_back(*i);
		}
		prevstr = *i;
	}
	bool valueEvic = true;
	verifytransition(uuphases, valueEvic);
}

TEST_F(WarmupTest, WarmupStateTransFullTest)
{

	bool fullEvict = true;
	createBucket(fullEvict);
	uint64_t numItems = 100000;
	numInserted = insertItems(numItems);
	sleep(10);
	fprintf(stderr, "\nnumber of inserted elements:  %lu \n", numInserted);
	//restart server for setting warmup
	char restart[] = "sudo /etc/init.d/couchbase-server restart";
	exec(restart);
	EXPECT_TRUE(verifyWarmup(numInserted));
	std::vector<std::string> uuphases;
	std::string prevstr = "";
	sleep(10);
	for (std::vector<std::string>::iterator i = threadstate.begin(); i != threadstate.end(); i++)
	{
		if (prevstr.compare(*i) != 0)
		{
			uuphases.push_back(*i);
		}
		prevstr = *i;
	}
	bool valueEvic = false;
	verifytransition(uuphases, valueEvic);
}

TEST_F(WarmupTest, estimateDatabaseItemCountTest)
{
	std::vector<lcb_t*> instancevec;
	instancevec.push_back(&instance);
	insert_items_instances(100000, instancevec);
	sleep(10);
	char restart[] = "sudo /etc/init.d/couchbase-server restart";
	exec(restart);
	sleep(120);
	(void) lcb_set_stat_callback(instance, stats_generic_callback);
	err = lcb_connect(instance);
	assert(err == LCB_SUCCESS);
	std::string warmup("warmup");
	StatsVector warmupstats = getgenericstats(warmup, instance);
	std::cout << "vector length : " << warmupstats.size() << "\n";
	int wmp_key_est = 0;
	int wmp_value_est = 0;
	for (StatsVector::iterator itr = warmupstats.begin(); itr != warmupstats.end(); ++itr)
	{
		if (!itr->first.compare("ep_warmup_estimated_key_count"))
		{
			wmp_key_est = atoi(itr->second.c_str());
			std::cout << "ep_warmup_estimated_key_count: " << itr->second << "\n";
		}
		if (!itr->first.compare("ep_warmup_estimated_value_count"))
		{
			wmp_value_est = atoi(itr->second.c_str());
			std::cout << "ep_warmup_estimated_value_count: " << itr->second << "\n";
		}
	}

	int diff = 100000 - wmp_key_est;
	EXPECT_LT(diff, 10);
	diff = 100000 - wmp_value_est;
	EXPECT_LT(diff, 10);
}

TEST_F(WarmupTest, mem_low_watTest)
{
	std::vector<lcb_t*> instancevec;
	instancevec.push_back(&instance);
	insert_items_instances(100000, instancevec);
	sleep(10);
	char memlow[] = "curl -XPOST -u Administrator:password -d"
		"'ns_bucket:update_bucket_props(\"default\", [{extra_config_string,  "
		"\"mem_low_wat=40000000\"}]).' http://127.0.0.1:8091/diag/eval";
	exec(memlow);
	char restart[] = "sudo /etc/init.d/couchbase-server restart";
	exec(restart);
	sleep(60);
	(void) lcb_set_stat_callback(instance, stats_generic_callback);
	err = lcb_connect(instance);
	assert(err == LCB_SUCCESS);
	std::string warmup("warmup");
	StatsVector warmupstats = getgenericstats(warmup, instance);
	std::cout << "vector length : " << warmupstats.size() << "\n";
	int wmp_value = 0;
	for (StatsVector::iterator itr = warmupstats.begin(); itr != warmupstats.end(); ++itr)
	{
		if (!itr->first.compare("ep_warmup_value_count"))
		{
			wmp_value = atoi(itr->second.c_str());
			std::cout << "ep_warmup_value_count: " << itr->second << "\n";
		}
	}
	EXPECT_GT(wmp_value, 0);
	genericstats.refcount = 0;
	genericstats.statsvec.clear();
	std::string memory("memory");
	StatsVector memorystats = getgenericstats(memory, instance);
	std::cout << "vector length : " << memorystats.size() << "\n";
	int ep_mem_low_wat = 0;
	int mem_used = 0;
	for (StatsVector::iterator itr = memorystats.begin(); itr != memorystats.end(); ++itr)
	{
		if (!itr->first.compare("ep_mem_low_wat"))
		{
			ep_mem_low_wat = atoi(itr->second.c_str());
			std::cout << "ep_mem_low_wat" << itr->second << "\n";
		}
		if (!itr->first.compare("mem_used"))
		{
			mem_used = atoi(itr->second.c_str());
			ep_mem_low_wat = atoi(itr->second.c_str());
			std::cout << "ep_mem_low_wat" << itr->second << "\n";
		}

	}
	EXPECT_LT(abs(ep_mem_low_wat - mem_used), 40000);
}

TEST_F(WarmupTest, ep_warmup_min_items_thresholdTest)
{
	std::vector<lcb_t*> instancevec;
	instancevec.push_back(&instance);
	insert_items_instances(100000, instancevec);
	sleep(10);
	char memlow[] = "curl -XPOST -u Administrator:password -d"
		"'ns_bucket:update_bucket_props(\"default\", [{extra_config_string,  "
		"\"warmup_min_items_threshold=10\"}]).' http://127.0.0.1:8091/diag/eval";
	exec(memlow);
	char restart[] = "sudo /etc/init.d/couchbase-server restart";
	exec(restart);
	sleep(120);
	(void) lcb_set_stat_callback(instance, stats_generic_callback);
	err = lcb_connect(instance);
	assert(err == LCB_SUCCESS);
	std::string warmup("warmup");
	StatsVector warmupstats = getgenericstats(warmup, instance);
	std::cout << "vector length : " << warmupstats.size() << "\n";
	int wmp_value = 0;
	for (StatsVector::iterator itr = warmupstats.begin(); itr != warmupstats.end(); ++itr)
	{
		if (!itr->first.compare("ep_warmup_value_count"))
		{
			wmp_value = atoi(itr->second.c_str());
			std::cout << "ep_warmup_value_count: " << itr->second << "\n";
		}
	}
	EXPECT_LT(((wmp_value * 100) / 100000), 12);
	EXPECT_GT(((wmp_value * 100) / 100000), 8);
}

TEST_F(WarmupTest, warmup_min_memory_thresholdTest)
{
	std::vector<lcb_t*> instancevec;
	instancevec.push_back(&instance);
	insert_items_instances(100000, instancevec);
	sleep(10);
	char memlow[] = "curl -XPOST -u Administrator:password -d"
		"'ns_bucket:update_bucket_props(\"default\", [{extra_config_string,  "
		"\"warmup_min_memory_threshold=20\"}]).' http://127.0.0.1:8091/diag/eval";
	exec(memlow);
	char restart[] = "sudo /etc/init.d/couchbase-server restart";
	exec(restart);
	sleep(60);
	(void) lcb_set_stat_callback(instance, stats_generic_callback);
	err = lcb_connect(instance);
	assert(err == LCB_SUCCESS);
	std::string memory("memory");
	StatsVector memorystats = getgenericstats(memory, instance);
	std::cout << "vector length : " << memorystats.size() << "\n";
	uint64_t ep_max_size = 0;
	uint64_t mem_used = 0;
	for (StatsVector::iterator itr = memorystats.begin(); itr != memorystats.end(); ++itr)
	{
		if (!itr->first.compare("ep_max_size"))
		{
			ep_max_size = atoi(itr->second.c_str());
		}
		if (!itr->first.compare("mem_used"))
		{
			mem_used = atoi(itr->second.c_str());
		}

	}
	mem_used = mem_used * 100;
	std::cout << "mem_used*100: " << mem_used << "\n";
	std::cout << "ep_max_size: " << ep_max_size << "\n";
	std::cout << "(mem_used*100)/ep_max_size: " << mem_used / ep_max_size << "\n";
	EXPECT_LT(mem_used / ep_max_size, 22);
	EXPECT_GT(mem_used / ep_max_size, 18);
	genericstats.refcount = 0;
	genericstats.statsvec.clear();
	std::string warmup("warmup");
	StatsVector warmupstats = getgenericstats(warmup, instance);
	std::cout << "vector length : " << warmupstats.size() << "\n";
	int wmp_value = 0;
	for (StatsVector::iterator itr = warmupstats.begin(); itr != warmupstats.end(); ++itr)
	{
		if (!itr->first.compare("ep_warmup_value_count"))
		{
			wmp_value = atoi(itr->second.c_str());
			std::cout << "ep_warmup_value_count: " << itr->second << "\n";
		}
	}
	EXPECT_GT(wmp_value, 0);
}

int main(int argc, char **argv)
{
	::testing::InitGoogleTest(&argc, argv);
	testargv = argv;
	testargc = argc;
	return RUN_ALL_TESTS();
}
