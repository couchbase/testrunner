#include "BaseTest.cc"
#include <sys/stat.h>
#include <fstream>

class AccessLogTest : public BaseTest
{

protected:

	void generateAccessLogs()
	{

		//change access log task sleep interval
		char alog[] = "curl -XPOST -u Administrator:password -d  "
			"'ns_bucket:update_bucket_props(\"default\", [{extra_config_string,  "
			"\"alog_sleep_time=2\"}]).' http://127.0.0.1:8091/diag/eval";
		exec(alog);

		//restart server for setting to take effect
		char restart[] = "sudo /etc/init.d/couchbase-server restart";
		exec(restart);
	}

	void loadfromAccessLogs()
	{

		//restart server for setting warmup
		char restart2[] = "sudo /etc/init.d/couchbase-server restart";
		exec(restart2);
	}

	void createView()
	{

		char view[] = "curl -X PUT -H 'Content-Type: application/json'  "
			"http://Administrator:password@127.0.0.1:8092/default/_design/getmeta  "
			"-d @getmeta.ddoc";
		exec(view);
	}

	char getRevId()
	{

		char getview[] = "curl http://Administrator:password@127.0.0.1:8092/default"
			"/_design/getmeta/_view/getrev?limit=1 > viewfile1";
		std::string viewfetch = exec(getview);
		char rmnewline[] = "tr -d \"\n\r\" < viewfile1 >viewfile2";
		exec(rmnewline);
		//std::cout <<'\n'<<"fetch from view: "<<viewfetch<<'\n';
		std::ifstream file("viewfile2");
		std::string result;
		std::getline(file, result);
		size_t valpos = result.find("value");
		char revid = result.at(valpos + 8);
		return revid;
	}

	void getwarmupcount()
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
	}
};

TEST_F(AccessLogTest, AccessLogTest_LogExists)
{
	bool fullEvict = true;
	createBucket(!fullEvict);
	const char inflated[] = "abc123";
	size_t inflated_len = strlen(inflated);

	uint64_t numItems = 100000;
	(void) insertItems(numItems);

	for (uint64_t i = 1; i < 100000; i = i + 100)
	{
		std::stringstream ss;
		ss << i;
		ss << "dexpire";
		std::string myString = ss.str();
		for (int j = 0; j < 100; j++)
			callget(&instance, myString.c_str(), myString.size());
	}

	generateAccessLogs();
	//allowing extra time  eviction  to complete
	sleep(180);

	std::string log0 = "/opt/couchbase/var/lib/couchbase/data/default/access.log.0";
	std::string log1 = "/opt/couchbase/var/lib/couchbase/data/default/access.log.1";
	std::string log2 = "/opt/couchbase/var/lib/couchbase/data/default/access.log.2";
	std::string log3 = "/opt/couchbase/var/lib/couchbase/data/default/access.log.3";

	FILE *file0;
	FILE *file1;
	FILE *file2;
	FILE *file3;

	if ((file0 = fopen(log0.c_str(), "r")) && (file1 = fopen(log1.c_str(), "r")) && (file2 = fopen(log2.c_str(), "r"))
	    && (file3 = fopen(log3.c_str(), "r")))
	{
		fclose(file0);
		fclose(file1);
		fclose(file2);
		fclose(file3);
	}
	else
		EXPECT_TRUE(false);
	char delBucket[] = "/root/ep-engine-tests/delBucket";
	exec(delBucket);
	sleep(40);
}

TEST_F(AccessLogTest, AccessLogTest_RevId)
{
	bool fullEvict = true;
	createBucket(!fullEvict);
	createView();
	sleep(30);
	const char inflated[] = "abc123";
	size_t inflated_len = strlen(inflated);

	uint64_t numItems = 100000;
	(void) insertItems(numItems);
	(void) insertItems(numItems);
	(void) insertItems(numItems);

	generateAccessLogs();
	//allowing extra time  eviction  to complete
	sleep(180);

	std::string log0 = "/opt/couchbase/var/lib/couchbase/data/default/access.log.0";
	std::string log1 = "/opt/couchbase/var/lib/couchbase/data/default/access.log.1";

	FILE *file0;
	FILE *file1;

	if ((file0 = fopen(log0.c_str(), "r")) && (file1 = fopen(log1.c_str(), "r")))
	{
		fclose(file0);
		fclose(file1);
	}
	else
		EXPECT_TRUE(false);
	loadfromAccessLogs();
	sleep(40);
	createView();
	sleep(30);
	char RevId = getRevId();
	printf("RevId: %c", RevId);
	EXPECT_GT(RevId, '1');
	EXPECT_LE(RevId, '3');
	char delBucket[] = "/root/ep-engine-tests/delBucket";
	//exec(delBucket);
	sleep(40);
}

TEST_F(AccessLogTest, AccessLogTest_CorruptLog)
{
	bool fullEvict = true;
	createBucket(!fullEvict);
	const char inflated[] = "abc123";
	size_t inflated_len = strlen(inflated);

	uint64_t numItems = 100000;
	(void) insertItems(numItems);

	for (uint64_t i = 1; i < 100000; i = i + 100)
	{
		std::stringstream ss;
		ss << i;
		ss << "dexpire";
		std::string myString = ss.str();
		for (int j = 0; j < 100; j++)
			callget(&instance, myString.c_str(), myString.size());
	}

	generateAccessLogs();
	//allowing extra time  eviction  to complete
	sleep(180);

	//rewrite some access logs with intent to corrupt
	FILE * File1;
	FILE * File2;
	File1 = fopen("/opt/couchbase/var/lib/couchbase/data/default/access.log.0", "w");
	File2 = fopen("/opt/couchbase/var/lib/couchbase/data/default/access.log.1", "w");
	if (File1 != NULL && File2 != NULL)
	{
		fputs("this log is now corrupt", File1);
		fclose(File1);
		fputs("this log is now corrupt", File2);
		fclose(File2);
	}
	else
		EXPECT_TRUE(false);

	loadfromAccessLogs();
	getwarmupcount();

	int wm_val_cn_i = atoi(wm_val_cn);
	printf("wamup value count: %d", wm_val_cn_i);

	EXPECT_TRUE(wm_val_cn_i > 0);
	char delBucket[] = "/root/ep-engine-tests/delBucket";
	exec(delBucket);
	sleep(40);
}

TEST_F(AccessLogTest, LoadFromOldAccessLogTest)
{
	bool fullEvict = true;
	createBucket(!fullEvict);
	const char inflated[] = "abc123";
	size_t inflated_len = strlen(inflated);

	uint64_t numItems = 100000;
	(void) insertItems(numItems);

	for (uint64_t i = 1; i < 100000; i = i + 100)
	{
		std::stringstream ss;
		ss << i;
		ss << "dexpire";
		std::string myString = ss.str();
		for (int j = 0; j < 100; j++)
			callget(&instance, myString.c_str(), myString.size());
	}

	generateAccessLogs();
	//allowing extra time  eviction  to complete
	sleep(360);

	//rewrite some access logs with intent to corrupt
	FILE * File1;
	FILE * File2;
	File1 = fopen("/opt/couchbase/var/lib/couchbase/data/default/access.log.0", "w");
	if (File1 != NULL)
	{
		fputs("this log is now corrupt", File1);
		fclose(File1);
	}
	else
		EXPECT_TRUE(false);

	loadfromAccessLogs();
	getwarmupcount();

	int wm_val_cn_i = atoi(wm_val_cn);
	printf("wamup value count: %d", wm_val_cn_i);

	EXPECT_TRUE(wm_val_cn_i > 0);
	char delBucket[] = "/root/ep-engine-tests/delBucket";
	exec(delBucket);
	sleep(40);
}

TEST_F(AccessLogTest, AccessLogTest_OldLogDeletion)
{
	bool fullEvict = true;
	createBucket(!fullEvict);
	const char inflated[] = "abc123";
	size_t inflated_len = strlen(inflated);

	uint64_t numItems = 100000;
	(void) insertItems(numItems);

	for (uint64_t i = 1; i < 100000; i = i + 100)
	{
		std::stringstream ss;
		ss << i;
		ss << "dexpire";
		std::string myString = ss.str();
		for (int j = 0; j < 100; j++)
			callget(&instance, myString.c_str(), myString.size());
	}

	generateAccessLogs();
	//allowing extra time  eviction  to complete
	sleep(360);

	std::string log0 = "/opt/couchbase/var/lib/couchbase/data/default/access.log.0.old";
	std::string log1 = "/opt/couchbase/var/lib/couchbase/data/default/access.log.1.old";
	std::string log2 = "/opt/couchbase/var/lib/couchbase/data/default/access.log.2.old";
	std::string log3 = "/opt/couchbase/var/lib/couchbase/data/default/access.log.3.old";

	struct stat f0info;
	struct stat f1info;
	struct stat f2info;
	struct stat f3info;
	stat(log0.c_str(), &f0info);
	stat(log1.c_str(), &f1info);
	stat(log2.c_str(), &f2info);
	stat(log3.c_str(), &f3info);
	time_t log0oldmodtime = f0info.st_mtime;
	time_t log1oldmodtime = f1info.st_mtime;
	time_t log2oldmodtime = f2info.st_mtime;
	time_t log3oldmodtime = f3info.st_mtime;
	sleep(180);
	stat(log0.c_str(), &f0info);
	stat(log1.c_str(), &f1info);
	stat(log2.c_str(), &f2info);
	stat(log3.c_str(), &f3info);
	EXPECT_GT(f0info.st_mtime, log0oldmodtime);
	EXPECT_GT(f1info.st_mtime, log1oldmodtime);
	EXPECT_GT(f2info.st_mtime, log2oldmodtime);
	EXPECT_GT(f3info.st_mtime, log3oldmodtime);
}

TEST_F(AccessLogTest, AccessLogTest_100DGM_ValEvict)
{
	bool fullEvict = true;
	createBucket(!fullEvict);
	const char inflated[] = "abc123";
	size_t inflated_len = strlen(inflated);

	uint64_t numItems = 100000;
	(void) insertItems(numItems);

	for (uint64_t i = 1; i < 100000; i = i + 100)
	{
		std::stringstream ss;
		ss << i;
		ss << "dexpire";
		std::string myString = ss.str();
		for (int j = 0; j < 100; j++)
			callget(&instance, myString.c_str(), myString.size());
	}

	generateAccessLogs();
	//allowing extra time  eviction  to complete
	sleep(180);

	getwarmupcount();

	int wm_val_cn_i = atoi(wm_val_cn);
	printf("wamup value count: %d", wm_val_cn_i);

	char strings[] = "strings /opt/couchbase/var/lib/couchbase/data/default/access.log.*|wc -l";
	std::string wmp_cnt = exec(strings);
	int wmp_cnt_i = atoi(wmp_cnt.c_str());
	EXPECT_TRUE(wmp_cnt_i > 0);
	printf("warmup value count from access logs: %d", wmp_cnt_i);
	EXPECT_GE(wm_val_cn_i, wmp_cnt_i);
	char delBucket[] = "/root/ep-engine-tests/delBucket";
	exec(delBucket);
	sleep(40);
}

TEST_F(AccessLogTest, AccessLogTest_100DGM_fullEvict)
{
	bool fullEvict = true;
	createBucket(fullEvict);
	const char inflated[] = "abc123";
	size_t inflated_len = strlen(inflated);

	uint64_t numItems = 100000;
	(void) insertItems(numItems);

	for (uint64_t i = 1; i < 100000; i = i + 100)
	{
		std::stringstream ss;
		ss << i;
		ss << "dexpire";
		std::string myString = ss.str();
		for (int j = 0; j < 100; j++)
			callget(&instance, myString.c_str(), myString.size());
	}

	generateAccessLogs();
	//allowing extra time  eviction  to complete
	sleep(180);

	loadfromAccessLogs();

	getwarmupcount();

	int wm_val_cn_i = atoi(wm_val_cn);
	printf("wamup value count: %d", wm_val_cn_i);

	char strings[] = "strings /opt/couchbase/var/lib/couchbase/data/default/access.log.*|wc -l";
	std::string wmp_cnt = exec(strings);
	int wmp_cnt_i = atoi(wmp_cnt.c_str());
	EXPECT_TRUE(wmp_cnt_i > 0);
	printf("warmup value count from access logs: %d", wmp_cnt_i);
	EXPECT_GE(wm_val_cn_i, wmp_cnt_i);
	char delBucket[] = "/root/ep-engine-tests/delBucket";
	exec(delBucket);
	sleep(40);
}

TEST_F(AccessLogTest, AccessLogTest_64DGM_ValEvict)
{
	bool fullEvict = true;
	createBucket(!fullEvict);
	const char inflated[] = "abc123";
	size_t inflated_len = strlen(inflated);

	uint64_t numItems = 1000000;
	(void) insertItems(numItems);

	for (uint64_t i = 1; i < 1000000; i = i + 500)
	{
		std::stringstream ss;
		ss << i;
		ss << "dexpire";
		std::string myString = ss.str();
		for (int j = 0; j < 100; j++)
			callget(&instance, myString.c_str(), myString.size());
	}
	char delAcclogs[] = "rm /opt/couchbase/var/lib/couchbase/data/default/access.log.*";
	exec(delAcclogs);

	generateAccessLogs();
	//allowing extra time  eviction  to complete
	sleep(180);

	loadfromAccessLogs();
	getwarmupcount();

	int wm_val_cn_i = atoi(wm_val_cn);
	printf("wamup value count: %d", wm_val_cn_i);

	char strings[] = "strings /opt/couchbase/var/lib/couchbase/data/default/access.log.*|wc -l";
	std::string wmp_cnt = exec(strings);
	int wmp_cnt_i = atoi(wmp_cnt.c_str());
	EXPECT_TRUE(wmp_cnt_i > 0);
	printf("warmup value count from access logs: %d", wmp_cnt_i);
	EXPECT_GE(wm_val_cn_i, wmp_cnt_i);
	char delBucket[] = "/root/ep-engine-tests/delBucket";
	exec(delBucket);
	sleep(40);
}

TEST_F(AccessLogTest, AccessLogTest_64DGM_fullEvict)
{
	bool fullEvict = true;
	createBucket(fullEvict);
	const char inflated[] = "abc123";
	size_t inflated_len = strlen(inflated);

	uint64_t numItems = 1000000;
	(void) insertItems(numItems);

	for (uint64_t i = 1; i < 1000000; i = i + 500)
	{
		std::stringstream ss;
		ss << i;
		ss << "dexpire";
		std::string myString = ss.str();
		for (int j = 0; j < 100; j++)
			callget(&instance, myString.c_str(), myString.size());
	}

	generateAccessLogs();
	//allowing extra time  eviction  to complete
	sleep(180);

	loadfromAccessLogs();
	getwarmupcount();

	int wm_val_cn_i = atoi(wm_val_cn);
	printf("wamup value count: %d", wm_val_cn_i);

	char strings[] = "strings /opt/couchbase/var/lib/couchbase/data/default/access.log.*|wc -l";
	std::string wmp_cnt = exec(strings);
	int wmp_cnt_i = atoi(wmp_cnt.c_str());
	EXPECT_TRUE(wmp_cnt_i > 0);
	printf("warmup value count from access logs: %d", wmp_cnt_i);
	EXPECT_GE(wm_val_cn_i, wmp_cnt_i);
	char delBucket[] = "/root/ep-engine-tests/delBucket";
	exec(delBucket);
	sleep(40);
}

TEST_F(AccessLogTest, InsertItems_DGM_10_Test_ValEvict)
{
	bool fullEvict = true;
	createBucket(!fullEvict);
	const char inflated[] = "abc123";
	size_t inflated_len = strlen(inflated);

	uint64_t numItems = 1000000;
	(void) insertItems(numItems);

	for (uint64_t i = 1; i < 1500000; i = i + 500)
	{
		std::stringstream ss;
		ss << i;
		ss << "dexpire";
		std::string myString = ss.str();
		callget(&instance, myString.c_str(), myString.size());
		callget(&instance, myString.c_str(), myString.size());
		callget(&instance, myString.c_str(), myString.size());
	}

	generateAccessLogs();
	loadfromAccessLogs();

	getwarmupcount();

	int wm_val_cn_i = atoi(wm_val_cn);
	printf("wamup value count: %d", wm_val_cn_i);

	char strings[] = "strings /opt/couchbase/var/lib/couchbase/data/default/access.log.*|wc -l";
	std::string wmp_cnt = exec(strings);
	int wmp_cnt_i = atoi(wmp_cnt.c_str());
	printf("warmup value count from access logs: %d", atoi(wmp_cnt.c_str()));
	EXPECT_GE(wm_val_cn_i, wmp_cnt_i);
	char delBucket[] = "/root/ep-engine-tests/delBucket";
	exec(delBucket);
	sleep(40);

}

TEST_F(AccessLogTest, InsertItems_DGM_10_Test_fullEvict)
{
	bool fullEvict = true;
	createBucket(fullEvict);
	const char inflated[] = "abc123";
	size_t inflated_len = strlen(inflated);

	uint64_t numItems = 1000000;
	(void) insertItems(numItems);

	for (uint64_t i = 1; i < 1500000; i = i + 500)
	{
		std::stringstream ss;
		ss << i;
		ss << "dexpire";
		std::string myString = ss.str();
		callget(&instance, myString.c_str(), myString.size());
		callget(&instance, myString.c_str(), myString.size());
		callget(&instance, myString.c_str(), myString.size());
	}

	generateAccessLogs();
	loadfromAccessLogs();

	getwarmupcount();

	int wm_val_cn_i = atoi(wm_val_cn);
	printf("wamup value count: %d", wm_val_cn_i);

	char strings[] = "strings /opt/couchbase/var/lib/couchbase/data/default/access.log.*|wc -l";
	std::string wmp_cnt = exec(strings);
	int wmp_cnt_i = atoi(wmp_cnt.c_str());
	printf("warmup value count from access logs: %d", atoi(wmp_cnt.c_str()));
	EXPECT_GE(wm_val_cn_i, wmp_cnt_i);
	char delBucket[] = "/root/ep-engine-tests/delBucket";
	exec(delBucket);
	sleep(40);

}

int main(int argc, char **argv)
{
	::testing::InitGoogleTest(&argc, argv);
	testargv = argv;
	testargc = argc;
	return RUN_ALL_TESTS();
}
