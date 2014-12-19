#include "BaseTest.cc"
#include <sys/stat.h>
#include <fstream>

using namespace std;

class AccessLogTest : public BaseTest
{

protected:

	void generateAccessLogs(bool wait_for_eviction=false)
	{
		cout << "Generating Access Logs" << endl;
		//change access log task sleep interval
		char alog[] = "curl -XPOST -u Administrator:password -d  "
			"'ns_bucket:update_bucket_props(\"default\", [{extra_config_string,  "
			"\"alog_sleep_time=2\"}]).' http://127.0.0.1:8091/diag/eval";
		exec(alog);

		cout << "Restart server for setting to take effect" << endl;
		char restart[] = "sudo /etc/init.d/couchbase-server restart";
		exec(restart);

		if (wait_for_eviction)
		{
			cout << "Allowing extra time eviction to complete" << endl;
			sleep(180);
		}
	}

	void loadfromAccessLogs()
	{
		cout << "Loading From Access Logs" << endl;
		//restart server for setting warmup
		char restart2[] = "sudo /etc/init.d/couchbase-server restart";
		exec(restart2);
	}

	void createView()
	{
		cout << "Creating View from getmeta.doc" << endl;
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

	void get_warmp_up_count_from_server_stats()
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

	void file_exists(string const & filename)
	{
		  struct stat buffer;
		  bool exist =  (stat (filename.c_str(), &buffer) == 0);
		  EXPECT_TRUE(exist);
	}

	int get_warmp_up_count_from_access_logs()
	{
		char strings[] = "strings /opt/couchbase/var/lib/couchbase/data/default/access.log.*|wc -l";
		std::string wmp_cnt = exec(strings);
		int wmp_cnt_i = atoi(wmp_cnt.c_str());
		EXPECT_TRUE(wmp_cnt_i > 0);
		cout << "Warmup value count from access logs: " << wmp_cnt_i;
		return wmp_cnt_i;
	}

	void compare_warmup_counts()
	{
		this->get_warmp_up_count_from_server_stats();

		int wm_val_cn_i = atoi(wm_val_cn);
		cout << "Warmup value count from stats: " << wm_val_cn_i;

		int wmp_cnt_i = this->get_warmp_up_count_from_access_logs();
		EXPECT_GE(wm_val_cn_i, wmp_cnt_i);
	}

	void insert_and_get_items(int numItems, int jump=100)
	{
		(void) this->insertItems(numItems);

		for (uint64_t i = 1; i < numItems; i = i + jump)
		{
			std::stringstream ss;
			ss << i;
			ss << "dexpire";
			std::string myString = ss.str();
			for (int j = 0; j < 100; j++)
				callget(&instance, myString.c_str(), myString.size());
		}
	}
};

TEST_F(AccessLogTest, AccessLogTest_LogExists)
{
	bool fullEvict = true;
	this->createBucket(!fullEvict);

	this->insert_and_get_items(100000);

	this->generateAccessLogs(true);

	this->file_exists("/opt/couchbase/var/lib/couchbase/data/default/access.log.0");
	this->file_exists("/opt/couchbase/var/lib/couchbase/data/default/access.log.1");
	this->file_exists("/opt/couchbase/var/lib/couchbase/data/default/access.log.2");
	this->file_exists("/opt/couchbase/var/lib/couchbase/data/default/access.log.3");
}

TEST_F(AccessLogTest, AccessLogTest_RevId)
{
	bool fullEvict = true;
	this->createBucket(!fullEvict);
	this->createView();
	sleep(30);

	uint64_t numItems = 100000;
	(void) this->insertItems(numItems);
	(void) this->insertItems(numItems);
	(void) this->insertItems(numItems);

	this->generateAccessLogs(true);

	this->file_exists("/opt/couchbase/var/lib/couchbase/data/default/access.log.0");
	this->file_exists("/opt/couchbase/var/lib/couchbase/data/default/access.log.1");

	this->loadfromAccessLogs();
	sleep(40);
	this->createView();
	sleep(30);
	char RevId = this->getRevId();
	printf("RevId: %c", RevId);
	EXPECT_GT(RevId, '1');
	EXPECT_LE(RevId, '3');
}

TEST_F(AccessLogTest, AccessLogTest_CorruptLog)
{
	bool fullEvict = true;
	this->createBucket(!fullEvict);

	this->insert_and_get_items(100000);

	this->generateAccessLogs(true);

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

	this->loadfromAccessLogs();
	this->get_warmp_up_count_from_server_stats();

	int wm_val_cn_i = atoi(wm_val_cn);
	printf("wamup value count: %d", wm_val_cn_i);

	EXPECT_TRUE(wm_val_cn_i > 0);
}

TEST_F(AccessLogTest, LoadFromOldAccessLogTest)
{
	bool fullEvict = true;
	this->createBucket(!fullEvict);

	this->insert_and_get_items(100000);

	this->generateAccessLogs();
	//allowing extra time  eviction  to complete
	sleep(360);

	//rewrite some access logs with intent to corrupt
	FILE * File1;
	File1 = fopen("/opt/couchbase/var/lib/couchbase/data/default/access.log.0", "w");
	if (File1 != NULL)
	{
		fputs("this log is now corrupt", File1);
		fclose(File1);
	}
	else
		EXPECT_TRUE(false);

	this->loadfromAccessLogs();
	this->get_warmp_up_count_from_server_stats();

	int wm_val_cn_i = atoi(wm_val_cn);
	printf("wamup value count: %d", wm_val_cn_i);

	EXPECT_TRUE(wm_val_cn_i > 0);
}

TEST_F(AccessLogTest, AccessLogTest_OldLogDeletion)
{
	bool fullEvict = true;
	this->createBucket(!fullEvict);

	this->insert_and_get_items(100000);

	this->generateAccessLogs();
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
	this->createBucket(!fullEvict);

	this->insert_and_get_items(100000);

	this->generateAccessLogs();
	//allowing extra time  eviction  to complete
	sleep(180);

	this->compare_warmup_counts();
}

TEST_F(AccessLogTest, AccessLogTest_100DGM_fullEvict)
{
	bool fullEvict = true;
	this->createBucket(fullEvict);

	this->insert_and_get_items(100000);

	this->generateAccessLogs(true);
	this->compare_warmup_counts();
}

TEST_F(AccessLogTest, AccessLogTest_64DGM_ValEvict)
{
	bool fullEvict = true;
	this->createBucket(!fullEvict);

	this->insert_and_get_items(1000000, 500);

	char delAcclogs[] = "rm /opt/couchbase/var/lib/couchbase/data/default/access.log.*";
	exec(delAcclogs);

	this->generateAccessLogs(true);
	this->compare_warmup_counts();
}

TEST_F(AccessLogTest, AccessLogTest_64DGM_fullEvict)
{
	bool fullEvict = true;
	this->createBucket(fullEvict);

	this->insert_and_get_items(1000000, 500);

	this->generateAccessLogs(true);
	this->compare_warmup_counts();
}

TEST_F(AccessLogTest, InsertItems_DGM_10_Test_ValEvict)
{
	bool fullEvict = true;
	this->createBucket(!fullEvict);

	uint64_t numItems = 1000000;
	(void) this->insertItems(numItems);

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

	this->generateAccessLogs();
	sleep(180);
	this->compare_warmup_counts();
}

TEST_F(AccessLogTest, InsertItems_DGM_10_Test_fullEvict)
{
	bool fullEvict = true;
	this->createBucket(fullEvict);

	uint64_t numItems = 1000000;
	(void) this->insertItems(numItems);

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

	this->generateAccessLogs();
	sleep(180);
	this->compare_warmup_counts();
}

int main(int argc, char **argv)
{
	::testing::InitGoogleTest(&argc, argv);
	testargv = argv;
	testargc = argc;
	return RUN_ALL_TESTS();
}
