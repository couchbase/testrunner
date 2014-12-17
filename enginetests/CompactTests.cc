//the method definition for "checkNewRevNum" is taken 
//from ep-engine source code at ep-engine/src/couch-kvstore/couch-kvstore.cc 
#include "internal.h"
#include <string>
#include <unistd.h>
#include <netinet/in.h>
#include "libcouchstore/couch_db.h"
#include "libcouchstore/couch_common.h"
#include "libcouchstore/couch_index.h"
#include "libcouchstore/error.h"
#include <platform/dirutils.h>
#include <cJSON.h>
#include "BaseTest.cc"

using namespace std;

extern char *getvaluebuf;
extern long *getvaluesize;
typedef struct DbSize AggDbSize;
struct DbSize
{
	uint64_t fileSize;
	uint64_t dataSize;
};

class CompactionTest : public BaseTest
{

protected:
	uint64_t checkNewRevNum(string &dbFileName)
	{

		uint64_t newrev = 0;

		// extract out the file revision number first
		size_t secondDot = dbFileName.rfind(".");
		string nameKey = dbFileName.substr(0, secondDot);
		nameKey.append(".");
		const vector<string> files = CouchbaseDirectoryUtilities::findFilesWithPrefix(nameKey);
		// found file(s) whoes name has the same key name pair with different
		// revision number
		for (vector<string>::const_iterator iter = files.begin(); iter != files.end(); ++iter)
		{
			string const & filename = *iter;
			size_t secondDot = filename.rfind(".");
			char *ptr = NULL;
			uint64_t revnum = strtoull(filename.substr(secondDot + 1).c_str(), &ptr, 10);
			if (newrev < revnum)
			{
				newrev = revnum;
				dbFileName = filename;
			}
		}
		return newrev;
	}

	void sendcompact(uint16_t vbid, uint64_t purge_before_ts, uint64_t purge_before_seq, uint8_t drop_deletes)
	{

		lcb_wait(instance);
		{
			err = lcb_compact(instance, NULL, vbid, purge_before_ts, purge_before_seq, drop_deletes);
			assert(err == LCB_SUCCESS);
		}
	}

	void storekey(string const & myString)
	{
		lcb_store_cmd_t cmd;
		const lcb_store_cmd_t *commands[1];
		commands[0] = &cmd;
		memset(&cmd, 0, sizeof(cmd));
		cmd.v.v0.key = myString.c_str();
		cmd.v.v0.nkey = myString.size();
		const char inflated[] = "abc123";
		size_t inflated_len = strlen(inflated);
		cmd.v.v0.operation = LCB_SET;
		cmd.v.v0.datatype = LCB_BINARY_RAW_BYTES;
		cmd.v.v0.bytes = inflated;
		cmd.v.v0.nbytes = inflated_len;
		cmd.v.v0.exptime = 0x5;
		lcb_store(instance, NULL, 1, &commands[0]);
		lcb_wait(instance);
	}

	void print_dbinfo(DbInfo const * dbinfo)
	{

		fprintf(stderr, "\nDbinfo filename: %s", dbinfo->filename);
		fprintf(stderr, "\nDbinfo doc count:  %lu \n", dbinfo->doc_count);
		fprintf(stderr, "\nDbinfo file size used:  %lu bytes", dbinfo->file_size);
		fprintf(stderr, "\nDbinfo deleted count:  %lu", dbinfo->deleted_count);
		fprintf(stderr, "\nDbinfo last seq no:  %lu ", dbinfo->last_sequence);
		fprintf(stderr, "\nDbinfo purge seq number:  %lu ", dbinfo->purge_seq);
	}

	void print_docinfo(DocInfo const * docinfo)
	{

		fprintf(stderr, "\ndocument key: ");
		fwrite(docinfo->id.buf, sizeof(char), docinfo->id.size, stderr);
		fprintf(stderr, "\ndocinfo sequence number: %lu ", docinfo->db_seq);
		fprintf(stderr, "\ndocinfo rev id: %lu ", docinfo->rev_seq);
		fprintf(stderr, "\ndocinfo deleted: %d ", docinfo->deleted);
		fprintf(stderr, "\ndocinfo doc size: %lu ", docinfo->size);

	}

	void insert_items_vbucket(int vbucket, int numitems)
	{
		int num = 0;
		int key = 1;
		//Store items with short TTL in the same vBucket
		while (num < numitems)
		{

			stringstream ss;
			ss << key;
			string const myString = ss.str();
			int vb = getvbucketbykey(instance, (const void*) myString.c_str(), myString.size());
			if (vb == vbucket)
			{
				storekey(myString);
				num++;
			}
			key++;
		}
	}

	void insert_items_vbucket(int vbucket, int numitems, int keyhint)
	{
		int num = 0;
		int key = keyhint;
		//Store items with short TTL in the same vBucket
		while (num < numitems)
		{
			stringstream ss;
			ss << key;
			string const myString = ss.str();
			int vb = getvbucketbykey(instance, (const void*) myString.c_str(), myString.size());
			if (vb == vbucket)
			{
				storekey(myString);
				num++;
			}
			key++;
		}
	}

	static const string getJSONObjString(const cJSON *i)
	{

		if (i == NULL)
		{
			return "";
		}
		if (i->type != cJSON_String)
		{
			abort();
		}
		return i->valuestring;
	}

	vector<int> getvbcheckpoints()
	{

		couchstore_error_t error;
		//structs to hold vB file information
		Db* db = new Db();
		LocalDoc* localdoc = (LocalDoc*) malloc(sizeof(LocalDoc));
		int numVBuckets = 1024;
		const char* id = "_local/vbstate";
		vector<int> chkpointvec;

		for (int vbnum = 0; vbnum < numVBuckets; vbnum++)
		{
			string const base_str = "/opt/couchbase/var/lib/couchbase/data/default/";
			string const append = ".couch.1";
			ostringstream convert;
			convert << vbnum;
			string filename = base_str + convert.str() + append;
			(void) this->checkNewRevNum(filename);
			//open vB file
			cout << filename << endl;
			error = couchstore_open_db(filename.c_str(), COUCHSTORE_OPEN_FLAG_RDONLY, &db);
			EXPECT_EQ(0, error);
			couchstore_open_local_document(db, (void *) id, strlen(id), &localdoc);
			string statjson = string(localdoc->json.buf, localdoc->json.size);
			cJSON *parsedjson = cJSON_Parse(statjson.c_str());
			string checkpoint_id = getJSONObjString(cJSON_GetObjectItem(parsedjson, "checkpoint_id"));
			cout << "vbid: " << vbnum << " checkpoint: " << checkpoint_id << "\n";
			chkpointvec.push_back(atoi(checkpoint_id.c_str()));
		}
		return chkpointvec;
	}

	AggDbSize const getTotalDbSize()
	{

		couchstore_error_t error;
		//structs to hold vB file information
		Db* db = (Db*) malloc(sizeof(Db));
		DbInfo* dbinfo = (DbInfo*) malloc(sizeof(DbInfo));
		int numVBuckets = 1024;
		AggDbSize TotalDbSize;

		for (int vbnum = 0; vbnum < numVBuckets; vbnum++)
		{
			string base_str = "/opt/couchbase/var/lib/couchbase/data/default/";
			string append = ".couch.1";
			string vb_num;
			ostringstream convert;
			convert << vbnum;
			vb_num = convert.str();
			string filename = base_str + vb_num + append;
			(void) this->checkNewRevNum(filename);

			//open vB file
			error = couchstore_open_db(filename.c_str(), COUCHSTORE_OPEN_FLAG_RDONLY, &db);
			EXPECT_EQ(0, error);
			//get vB info 
			error = couchstore_db_info(db, dbinfo);
			EXPECT_EQ(0, error);
			TotalDbSize.fileSize = TotalDbSize.fileSize + dbinfo->file_size;
			TotalDbSize.dataSize = TotalDbSize.dataSize + dbinfo->space_used;
			//close db handles
			couchstore_close_db(db);
		}
		return TotalDbSize;
	}

	void run_compaction(int vbid, string const & compaction_arguments = "")
	{
		string compaction_cmd = "/opt/couchbase/bin/cbcompact localhost:11210 compact ";
		stringstream ss;
		ss << vbid;
		compaction_cmd = compaction_cmd + ss.str() + " " + compaction_arguments;
		this->exec(compaction_cmd.c_str());

	}

	DbInfo * get_db_info(string const & filename)
	{
		Db* db = (Db*) malloc(sizeof(Db));
		DbInfo* dbinfo = (DbInfo*) malloc(sizeof(DbInfo));
		couchstore_error_t error = couchstore_open_db(filename.c_str(), COUCHSTORE_OPEN_FLAG_RDONLY, &db);
		EXPECT_EQ(0, error);
		//get vB info
		error = couchstore_db_info(db, dbinfo);
		EXPECT_EQ(0, error);
		this->print_dbinfo(dbinfo);
		couchstore_close_db(db);
		return dbinfo;
	}

	void disable_auto_compaction()
	{
		char autocompact[] = "curl -X POST -u Administrator:password -d  "
			"autoCompactionDefined=false -d parallelDBAndViewCompaction=false  "
			"http://localhost:8091/controller/setAutoCompaction";
		this->exec(autocompact);
	}

	void TearDown()
	{
		string const start = "sudo /etc/init.d/couchbase-server start";
		this->exec(start.c_str());
		sleep(30);
		this->BaseTest::TearDown();
	}
};

TEST_F(CompactionTest, SizeReductionTest)
{
	this->sendHello();
	stringstream ss;
	ss << "fooaaa";
	string myString = ss.str();
	for (int i = 0; i < 100000; i++)
	{
		storekey(myString);
	}
	sleep(30);
	fprintf(stderr, "\nperformed 100000 mutations on vbucket 14\n");

	//parameters for LCB_COMPACT command
	uint16_t vbid = 14;
	uint64_t purge_before_ts = 0;
	uint64_t purge_before_seq = 0;
	uint8_t drop_deletes = 0;

	//file sizes before and after compaction
	uint64_t fsize_b4compact = 0;
	uint64_t fsize_a4trcompact = 0;

	//open vB file
	DbInfo* dbinfo = this->get_db_info("/opt/couchbase/var/lib/couchbase/data/default/14.couch.1");
	fsize_b4compact = dbinfo->file_size;
	//send lcb_compact command
	this->sendcompact(vbid, purge_before_ts, purge_before_seq, drop_deletes);
	lcb_wait(instance);

	//open vB file with new rev number
	DbInfo* dbinfo1 = this->get_db_info("/opt/couchbase/var/lib/couchbase/data/default/14.couch.2");
	fsize_a4trcompact = dbinfo1->file_size;
	//assert the expected file sizes
	EXPECT_LT(fsize_a4trcompact, 0.01 * fsize_b4compact);

	free(dbinfo);
	free(dbinfo1);
}

/*TEST_F(CompactionTest, DatatypeVerificationTest) {
 sendHello();
 stringstream ss;
 ss << "fooaaa";
 string myString = ss.str();

 couchstore_error_t error;
 //structs to hold vB file information
 Db* db = (Db*)malloc(sizeof(Db));
 DocInfo* docinfo = (DocInfo*)malloc(sizeof(DocInfo));

 const char *filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.2";
 //open vB file
 error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db);
 EXPECT_EQ(0,error);
 error = couchstore_docinfo_by_id(db,(void*)myString.c_str(),myString.size(),&docinfo);
 EXPECT_EQ(0,error);
 //close db handles
 fwrite(docinfo->rev_meta.buf, sizeof(char), docinfo->rev_meta.size, stderr);
 fprintf(stderr, "\nrevmeta size: %d ",docinfo->rev_meta.size);

 couchstore_close_db(db);
 }*/

TEST_F(CompactionTest, MetadataVerificationTest)
{
	this->sendHello();
	stringstream ss;
	ss << "fooaaa";
	string myString = ss.str();
	for (int i = 0; i < 100000; i++)
	{
		storekey(myString);
	}
	sleep(30);
	fprintf(stderr, "\nperformed 100000 mutations on vbucket 14\n");

	couchstore_error_t error;
	//structs to hold vB file information
	Db* db = (Db*) malloc(sizeof(Db));
	DocInfo* docinfo1 = (DocInfo*) malloc(sizeof(DocInfo));
	DocInfo* docinfo2 = (DocInfo*) malloc(sizeof(DocInfo));

	//parameters for LCB_COMPACT command
	uint16_t vbid = 14;
	uint64_t purge_before_ts = 0;
	uint64_t purge_before_seq = 0;
	uint8_t drop_deletes = 0;
	const char *filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.1";
	//open vB file
	error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db);
	EXPECT_EQ(0, error);
	error = couchstore_docinfo_by_id(db, (void*) myString.c_str(), myString.size(), &docinfo1);
	EXPECT_EQ(0, error);
	//close db handles
	couchstore_close_db(db);
	//send lcb_compact command
	this->sendcompact(vbid, purge_before_ts, purge_before_seq, drop_deletes);
	lcb_wait(instance);

	//open vB file with new rev number 
	filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.2";
	error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db);
	EXPECT_EQ(0, error);
	error = couchstore_docinfo_by_id(db, (void*) myString.c_str(), myString.size(), &docinfo2);
	EXPECT_EQ(0, error);
	//assert the expected file sizes
	EXPECT_EQ(docinfo1->db_seq, docinfo2->db_seq);
	EXPECT_EQ(docinfo1->rev_seq, docinfo2->rev_seq);
	EXPECT_EQ(docinfo1->deleted, docinfo2->deleted);
	EXPECT_EQ(docinfo1->size, docinfo2->size);
}

TEST_F(CompactionTest, FileStatVerifyTest)
{
	char autocompact[] = "curl -X POST -u Administrator:password -d  "
		"autoCompactionDefined=true -d parallelDBAndViewCompaction=false  "
		"-d databaseFragmentationThreshold[percentage]=95  "
		"http://localhost:8091/controller/setAutoCompaction";
	this->exec(autocompact);
	this->sendHello();
	int keyhint = 0;
	AggDbSize CurrDbSize =
	{ 0, 0 };
	uint64_t prevfileSize = 0;
	while (true)
	{
		this->insert_items_vbucket(14, 10000, keyhint);
		keyhint = keyhint + 10000;
		sleep(5);
		CurrDbSize = this->getTotalDbSize();
		if (CurrDbSize.fileSize < prevfileSize)
			break;
		prevfileSize = CurrDbSize.fileSize;
		fprintf(stderr, "\ncurrDataSize:  %lu bytes", CurrDbSize.dataSize);
	}
	sleep(60);
	uint64_t newRevNum = 1;
	string filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.1";
	newRevNum = this->checkNewRevNum(filename);
	EXPECT_GE(newRevNum, 2);
	uint64_t space_used = 0;
	uint64_t file_size = 0;
	(void) lcb_set_stat_callback(instance, stats_generic_callback);
	string diskinfo("diskinfo detail");
	StatsVector diskinfostats = this->getgenericstats(diskinfo, instance);
	cout << "vector length : " << diskinfostats.size() << "\n";
	for (StatsVector::iterator itr = diskinfostats.begin(); itr != diskinfostats.end(); ++itr)
	{
		cout << itr->first << "\n";
		if (!itr->first.compare("vb_14:data_size"))
		{
			cout << "vb_14:data_size: " << itr->second << "\n";
			space_used = atoi(itr->second.c_str());
		}
		if (!itr->first.compare("vb_14:file_size"))
		{
			cout << "vb_14:file_size: " << itr->second << "\n";
			file_size = atoi(itr->second.c_str());
		}
	}
	//structs to hold vB file information
	DbInfo* dbinfo = this->get_db_info(filename);
	EXPECT_EQ(space_used, dbinfo->space_used);
	EXPECT_EQ(file_size, dbinfo->file_size);
	genericstats.refcount = 0;
	genericstats.statsvec.clear();
	uint64_t high_seqno = 0;
	uint64_t purge_seqno = 0;
	string vbseqno("vbucket-seqno 14");
	StatsVector vbseqnostats = this->getgenericstats(vbseqno, instance);
	cout << "vector length : " << vbseqnostats.size() << "\n";
	for (StatsVector::iterator itr = vbseqnostats.begin(); itr != vbseqnostats.end(); ++itr)
	{
		cout << itr->first << "\n";
		if (!itr->first.compare("vb_14:high_seqno"))
		{
			cout << "vb_14:high_seqno: " << itr->second << "\n";
			high_seqno = atoi(itr->second.c_str());

		}
		if (!itr->first.compare("vb_14:purge_seqno"))
		{
			cout << "vb_14:purge_seqno: " << itr->second << "\n";
			purge_seqno = atoi(itr->second.c_str());
		}
	}
	EXPECT_EQ(high_seqno, dbinfo->last_sequence);
	EXPECT_EQ(purge_seqno, dbinfo->purge_seq);

	free(dbinfo);
}

TEST_F(CompactionTest, FragThresholdPercTest)
{
	char autocompact[] = "curl -X POST -u Administrator:password -d  "
		"autoCompactionDefined=true -d parallelDBAndViewCompaction=false  "
		"-d databaseFragmentationThreshold[percentage]=95  "
		"http://localhost:8091/controller/setAutoCompaction";
	this->exec(autocompact);
	this->sendHello();
	int FragPercThreshold = 95;
	int RealPercFrag = 0;
	int keyhint = 0;
	int numVbuckets = 1024;
	int MinFileSize = 131072 * numVbuckets;
	AggDbSize CurrDbSize =
	{ 0, 0 };
	uint64_t prevfileSize = 0;
	while (RealPercFrag < FragPercThreshold || CurrDbSize.fileSize < MinFileSize)
	{
		this->insert_items_vbucket(14, 10000, keyhint);
		keyhint = keyhint + 10000;
		sleep(5);
		CurrDbSize = this->getTotalDbSize();
		if (CurrDbSize.fileSize < prevfileSize)
			break;
		prevfileSize = CurrDbSize.fileSize;
		RealPercFrag = ((CurrDbSize.fileSize - CurrDbSize.dataSize) * 100) / CurrDbSize.fileSize;
		fprintf(stderr, "\ncurrFileSize:  %lu bytes", CurrDbSize.fileSize);
		fprintf(stderr, "\ncurrDataSize:  %lu bytes", CurrDbSize.dataSize);
		fprintf(stderr, "\nRealPercFrag:  %d", RealPercFrag);
	}
	sleep(60);
	uint64_t newRevNum = 1;
	string filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.1";
	newRevNum = this->checkNewRevNum(filename);
	EXPECT_GE(newRevNum, 2);
}

TEST_F(CompactionTest, FragThresholdSizeTest)
{
	char autocompact[] = "curl -X POST -u Administrator:password -d  "
		"autoCompactionDefined=true -d parallelDBAndViewCompaction=false  "
		"-d databaseFragmentationThreshold[size]=100000000  "
		"http://localhost:8091/controller/setAutoCompaction";
	this->exec(autocompact);
	this->sendHello();
	int FragSizeThreshold = 100000000;
	int RealSizeFrag = 0;
	int keyhint = 0;
	int numVbuckets = 1024;
	int MinFileSize = 131072 * numVbuckets;
	AggDbSize CurrDbSize =
	{ 0, 0 };
	uint64_t prevfileSize = 0;

	while (RealSizeFrag < FragSizeThreshold || CurrDbSize.fileSize < MinFileSize)
	{
		this->insert_items_vbucket(14, 10000, keyhint);
		keyhint = keyhint + 10000;
		sleep(5);
		CurrDbSize = this->getTotalDbSize();
		if (CurrDbSize.fileSize < prevfileSize)
			break;
		prevfileSize = CurrDbSize.fileSize;
		RealSizeFrag = CurrDbSize.fileSize - CurrDbSize.dataSize;
		fprintf(stderr, "\ncurrFileSize:  %lu bytes", CurrDbSize.fileSize);
		fprintf(stderr, "\ncurrDataSize:  %lu bytes", CurrDbSize.dataSize);
		fprintf(stderr, "\nRealSizeFrag:  %d", RealSizeFrag);
	}
	sleep(60);
	uint64_t newRevNum = 1;
	string filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.1";
	newRevNum = this->checkNewRevNum(filename);
	EXPECT_GE(newRevNum, 2);
}

TEST_F(CompactionTest, Expired10ItemPurgeTest)
{
	this->sendHello();
	this->disable_auto_compaction();
	int numitems = 10;
	int vbucket = 15;
	this->insert_items_vbucket(vbucket, numitems);
	sleep(30);

	DbInfo* dbinfo = this->get_db_info("/opt/couchbase/var/lib/couchbase/data/default/15.couch.1");

	//no documents should be deleted prior to compaction
	EXPECT_EQ(0, dbinfo->deleted_count);

	this->run_compaction(vbucket);

	sleep(120);

	DbInfo* dbinfo1 = this->get_db_info("/opt/couchbase/var/lib/couchbase/data/default/15.couch.2");
	//11 documents should be deleted post compaction
	EXPECT_EQ(10, dbinfo1->deleted_count);

	free(dbinfo);
	free(dbinfo1);
}

TEST_F(CompactionTest, Expired1KItemPurgeTest)
{
	this->sendHello();
	this->disable_auto_compaction();
	int numitems = 1000;
	int vbucket = 16;
	this->insert_items_vbucket(vbucket, numitems);
	sleep(30);

	DbInfo* dbinfo = this->get_db_info("/opt/couchbase/var/lib/couchbase/data/default/16.couch.1");

	//no documents should be deleted prior to compaction
	EXPECT_EQ(0, dbinfo->deleted_count);

	this->run_compaction(vbucket);

	sleep(120);
	//open vB file with new rev number 
	DbInfo* dbinfo1 = this->get_db_info("/opt/couchbase/var/lib/couchbase/data/default/16.couch.2");
	EXPECT_EQ(1000, dbinfo1->deleted_count);

	free(dbinfo);
	free(dbinfo1);
}

TEST_F(CompactionTest, Expired10KItemPurgeTest)
{
	this->sendHello();
	this->disable_auto_compaction();
	int numitems = 10000;
	int vbucket = 16;
	this->insert_items_vbucket(vbucket, numitems);
	sleep(30);

	DbInfo* dbinfo = this->get_db_info("/opt/couchbase/var/lib/couchbase/data/default/16.couch.1");
	//no documents should be deleted prior to compaction
	EXPECT_EQ(0, dbinfo->deleted_count);

	this->run_compaction(vbucket);

	sleep(120);
	//open vB file with new rev number
	DbInfo* dbinfo1 = this->get_db_info("/opt/couchbase/var/lib/couchbase/data/default/16.couch.2");
	EXPECT_EQ(10000, dbinfo1->deleted_count);

	free(dbinfo);
	free(dbinfo1);
}

TEST_F(CompactionTest, Expired100KItemPurgeTest)
{
	this->sendHello();
	this->disable_auto_compaction();
	int numitems = 100000;
	int vbucket = 18;
	this->insert_items_vbucket(vbucket, numitems);
	sleep(30);

	DbInfo* dbinfo = this->get_db_info("/opt/couchbase/var/lib/couchbase/data/default/18.couch.1");
	//no documents should be deleted prior to compaction
	EXPECT_EQ(0, dbinfo->deleted_count);

	this->run_compaction(vbucket);

	sleep(120);
	DbInfo* dbinfo1 = this->get_db_info("/opt/couchbase/var/lib/couchbase/data/default/18.couch.2");
	EXPECT_EQ(100000, dbinfo1->deleted_count);

	free(dbinfo);
	free(dbinfo1);
}

TEST_F(CompactionTest, DropDeletesFalseDocExptimeGreaterTest)
{

	this->sendHello();
	this->disable_auto_compaction();
	int numitems = 10;
	int vbucket = 14;
	this->insert_items_vbucket(vbucket, numitems);
	sleep(30);
	//set compact_cmd parameters 
	uint16_t vbid = 14;
	uint32_t exptime = 946759881;                   // harcoded to Jan 01 2000
	uint64_t purge_before_ts = exptime;
	uint64_t purge_before_seq = 1;
	uint8_t drop_deletes = 0;
	//run compaction once to mark expired items as deleted 
	this->sendcompact(vbid, purge_before_ts, purge_before_seq, drop_deletes);
	lcb_wait(instance);
	sleep(60);

	DbInfo* dbinfo1 = this->get_db_info("/opt/couchbase/var/lib/couchbase/data/default/14.couch.2");
	EXPECT_EQ(10, dbinfo1->deleted_count);

	this->run_compaction(vbid, "--purge-before=946759881 --purge-only-upto-seq=1");

	DbInfo* dbinfo2 = this->get_db_info("/opt/couchbase/var/lib/couchbase/data/default/14.couch.3");
	EXPECT_EQ(10, dbinfo2->deleted_count);

	free(dbinfo1);
	free(dbinfo2);
}

TEST_F(CompactionTest, DropDeletesFalseDocPurgeseqGreaterTest)
{
	this->sendHello();
	this->disable_auto_compaction();
	int numitems = 10;
	int vbucket = 14;
	this->insert_items_vbucket(vbucket, numitems);
	sleep(30);

	//run compaction once to mark expired items as deleted 
	this->run_compaction(vbucket);
	sleep(60);

	DbInfo* dbinfo1 = this->get_db_info( "/opt/couchbase/var/lib/couchbase/data/default/14.couch.2");
	EXPECT_EQ(10, dbinfo1->deleted_count);

	this->run_compaction(vbucket, "--purge-before=2946759881 --purge-only-upto-seq=1");

	DbInfo* dbinfo2 = this->get_db_info( "/opt/couchbase/var/lib/couchbase/data/default/14.couch.3");
	EXPECT_EQ(10, dbinfo2->deleted_count);

	free(dbinfo1);
	free(dbinfo2);
}

TEST_F(CompactionTest, DropDeletesFalsePurgeSeqZeroTest)
{
	this->sendHello();
	this->disable_auto_compaction();
	stringstream ss;
	ss << "fooaaa";
	uint16_t vbid = 14;
	string myString = ss.str();
	for (int i = 0; i < 1000; i++)
	{
		storekey(myString);
	}
	sleep(30);
	//run compaction once to mark expired items as deleted 
	this->run_compaction(vbid);
	sleep(60);
	couchstore_error_t error;
	Db* db1 = (Db*) malloc(sizeof(Db));
	Db* db2 = (Db*) malloc(sizeof(Db));
	DbInfo* dbinfo1 = (DbInfo*) malloc(sizeof(DbInfo));
	DbInfo* dbinfo2 = (DbInfo*) malloc(sizeof(DbInfo));
	DocInfo* docinfo1 = (DocInfo*) malloc(sizeof(DocInfo));
	DocInfo* docinfo2 = (DocInfo*) malloc(sizeof(DocInfo));
	//get the expiry time of a doc
	uint32_t exptime;
	const char* key = "fooaaa";
	uint64_t fsize_b4compact = 0;
	//uint64_t fsize_a4trcompact = 0;
	const char *filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.2";

	error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db1);
	EXPECT_EQ(0, error);
	error = couchstore_docinfo_by_id(db1, (void*) key, 6, &docinfo1);
	EXPECT_EQ(0, error);
	this->print_docinfo(docinfo1);

	//read the document expiry time and decrement it 
	memcpy(&exptime, docinfo1->rev_meta.buf + 8, 4);
	exptime = ntohl(exptime);
	exptime = exptime + 100;
	couchstore_free_docinfo(docinfo1);

	error = couchstore_db_info(db1, dbinfo1);
	EXPECT_EQ(0, error);
	this->print_dbinfo(dbinfo1);
	fsize_b4compact = dbinfo1->file_size;
	couchstore_close_db(db1);
	//set compact_cmd parameters 
	uint64_t purge_before_ts = exptime;
	uint64_t purge_before_seq = 0;
	uint8_t drop_deletes = 0;

	this->sendcompact(vbid, purge_before_ts, purge_before_seq, drop_deletes);
	lcb_wait(instance);
	sleep(5);
	filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.3";
	error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db2);
	EXPECT_EQ(0, error);
	error = couchstore_docinfo_by_id(db2, (void*) key, 6, &docinfo2);
	this->print_docinfo(docinfo2);
	EXPECT_EQ(-5, error);
	error = couchstore_db_info(db2, dbinfo2);
	EXPECT_EQ(0, error);
	this->print_dbinfo(dbinfo2);
	EXPECT_EQ(0, dbinfo2->deleted_count);
}

TEST_F(CompactionTest, DropDeletesMaxPurgedSeqnoTest)
{
	this->sendHello();
	stringstream ss;
	ss << "fooaaa";
	string myString = ss.str();
	for (int i = 0; i < 1000; i++)
	{
		storekey(myString);
	}
	sleep(30);
	couchstore_error_t error;
	Db* db1 = (Db*) malloc(sizeof(Db));
	Db* db2 = (Db*) malloc(sizeof(Db));
	DbInfo* dbinfo1 = (DbInfo*) malloc(sizeof(DbInfo));
	DbInfo* dbinfo2 = (DbInfo*) malloc(sizeof(DbInfo));
	DocInfo* docinfo1 = (DocInfo*) malloc(sizeof(DocInfo));
	DocInfo* docinfo2 = (DocInfo*) malloc(sizeof(DocInfo));
	//get the expiry time of a doc
	uint32_t exptime;
	const char* key = "fooaaa";
	uint16_t vbid = 14;
	uint64_t fsize_b4compact = 0;
	//uint64_t fsize_a4trcompact = 0;
	const char *filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.1";

	error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db1);
	EXPECT_EQ(0, error);
	error = couchstore_docinfo_by_id(db1, (void*) key, 6, &docinfo1);
	EXPECT_EQ(0, error);
	this->print_docinfo(docinfo1);
	uint64_t docrev_seqno = docinfo1->db_seq;
	//run compaction once to mark expired items as deleted 
	this->run_compaction(vbid);
	sleep(60);

	//read the document expiry time and decrement it 
	memcpy(&exptime, docinfo1->rev_meta.buf + 8, 4);
	exptime = ntohl(exptime);
	exptime = exptime + 100;
	couchstore_free_docinfo(docinfo1);

	error = couchstore_db_info(db1, dbinfo1);
	EXPECT_EQ(0, error);
	this->print_dbinfo(dbinfo1);
	fsize_b4compact = dbinfo1->file_size;
	couchstore_close_db(db1);
	//set compact_cmd parameters 
	uint64_t purge_before_ts = exptime;
	uint64_t purge_before_seq = 0;
	uint8_t drop_deletes = 1;

	this->sendcompact(vbid, purge_before_ts, purge_before_seq, drop_deletes);
	lcb_wait(instance);
	sleep(10);

	filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.3";
	error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db2);
	EXPECT_EQ(0, error);
	error = couchstore_docinfo_by_id(db2, (void*) key, 6, &docinfo2);
	this->print_docinfo(docinfo2);
	EXPECT_EQ(-5, error);
	error = couchstore_db_info(db2, dbinfo2);
	EXPECT_EQ(0, error);
	this->print_dbinfo(dbinfo2);
	EXPECT_EQ(docrev_seqno + 1, dbinfo2->purge_seq);
}

TEST_F(CompactionTest, DropDeletesTrue10StoneTest)
{

	this->sendHello();
	int numitems = 10;
	int vbucket = 14;
	this->insert_items_vbucket(vbucket, numitems);
	sleep(30);
	//set compact_cmd parameters 
	uint16_t vbid = 14;
	uint32_t exptime = 0;
	uint64_t purge_before_ts = exptime;
	uint64_t purge_before_seq = 1;
	uint8_t drop_deletes = 1;
	//run compaction once to mark expired items as deleted 
	this->sendcompact(vbid, purge_before_ts, purge_before_seq, drop_deletes);
	lcb_wait(instance);
	sleep(60);

	DbInfo* dbinfo1 = this->get_db_info("/opt/couchbase/var/lib/couchbase/data/default/14.couch.2");
	EXPECT_EQ(10, dbinfo1->deleted_count);

	this->run_compaction(vbid, "--purge-before=946759881 --purge-only-upto-seq=1 --dropdeletes");

	DbInfo* dbinfo2 = this->get_db_info("/opt/couchbase/var/lib/couchbase/data/default/14.couch.3");
	EXPECT_EQ(0, dbinfo2->deleted_count);

	free(dbinfo1);
	free(dbinfo2);
}

TEST_F(CompactionTest, DropDeletesTrue10KStoneTest)
{

	this->sendHello();
	int numitems = 10000;
	int vbucket = 14;
	this->insert_items_vbucket(vbucket, numitems);
	sleep(30);
	//set compact_cmd parameters 
	uint16_t vbid = 14;
	uint32_t exptime = 0;
	uint64_t purge_before_ts = exptime;
	uint64_t purge_before_seq = 1;
	uint8_t drop_deletes = 1;
	//run compaction once to mark expired items as deleted 
	this->sendcompact(vbid, purge_before_ts, purge_before_seq, drop_deletes);
	lcb_wait(instance);
	sleep(100);

	DbInfo* dbinfo1 = this->get_db_info("/opt/couchbase/var/lib/couchbase/data/default/14.couch.2");
	EXPECT_EQ(10000, dbinfo1->deleted_count);

	this->run_compaction(vbid, "--purge-before=946759881 --purge-only-upto-seq=1 --dropdeletes");

	DbInfo* dbinfo2 = this->get_db_info("/opt/couchbase/var/lib/couchbase/data/default/14.couch.3");
	EXPECT_EQ(0, dbinfo2->deleted_count);

	free(dbinfo1);
	free(dbinfo2);
}

TEST_F(CompactionTest, DropDeletesTrue100KStoneTest)
{

	this->sendHello();
	int numitems = 100000;
	int vbucket = 14;
	this->insert_items_vbucket(vbucket, numitems);
	sleep(30);
	//set compact_cmd parameters 
	uint16_t vbid = 14;
	uint32_t exptime = 0;
	uint64_t purge_before_ts = exptime;
	uint64_t purge_before_seq = 1;
	uint8_t drop_deletes = 1;
	//run compaction once to mark expired items as deleted 
	this->sendcompact(vbid, purge_before_ts, purge_before_seq, drop_deletes);
	lcb_wait(instance);
	sleep(240);

	DbInfo* dbinfo1 = this->get_db_info("/opt/couchbase/var/lib/couchbase/data/default/14.couch.2");
	EXPECT_EQ(100000, dbinfo1->deleted_count);

	this->run_compaction(vbid, "--purge-before=946759881 --purge-only-upto-seq=1 --dropdeletes");

	DbInfo* dbinfo2 = this->get_db_info("/opt/couchbase/var/lib/couchbase/data/default/14.couch.3");
	EXPECT_EQ(0, dbinfo2->deleted_count);

	free(dbinfo1);
	free(dbinfo2);
}

TEST_F(CompactionTest, cbcompactnodropsTest)
{
	this->sendHello();
	int numitems = 10000;
	int vbucket = 14;
	this->insert_items_vbucket(vbucket, numitems);
	sleep(30);
	this->run_compaction(vbucket);
	sleep(30);

	DbInfo* dbinfo1 = this->get_db_info("/opt/couchbase/var/lib/couchbase/data/default/14.couch.2");
	EXPECT_EQ(10000, dbinfo1->deleted_count);

	free(dbinfo1);
}

TEST_F(CompactionTest, cbcompactdropTrueTest)
{
	this->sendHello();
	this->disable_auto_compaction();
	int numitems = 10000;
	int vbucket = 14;
	this->insert_items_vbucket(vbucket, numitems);
	sleep(30);
	this->run_compaction(vbucket);
	sleep(30);
	this->run_compaction(vbucket, "--purge-before=0 --purge-only-upto-seq=0 --dropdeletes");
	sleep(30);

	DbInfo* dbinfo1 = this->get_db_info("/opt/couchbase/var/lib/couchbase/data/default/14.couch.3");
	EXPECT_EQ(0, dbinfo1->deleted_count);

	free(dbinfo1);
}

TEST_F(CompactionTest, cbcompactdropexpTest)
{
	this->sendHello();
	this->disable_auto_compaction();
	int numitems = 10000;
	int vbucket = 14;
	this->insert_items_vbucket(vbucket, numitems);
	sleep(30);
	this->run_compaction(vbucket);
	//we wont hit the epoch time below any time soon
	this->run_compaction(vbucket, "--purge-before=2000000000 --purge-only-upto-seq=20000");
	sleep(30);

	DbInfo* dbinfo1 = this->get_db_info("/opt/couchbase/var/lib/couchbase/data/default/14.couch.3");
	EXPECT_EQ(0, dbinfo1->deleted_count);

	free(dbinfo1);
}

TEST_F(CompactionTest, cbcompactdropseqnoTest)
{
	this->sendHello();
	this->disable_auto_compaction();
	int numitems = 10000;
	int vbucket = 14;
	this->insert_items_vbucket(vbucket, numitems);
	sleep(30);
	this->run_compaction(vbucket);
	//we wont hit the epoch time below any time soon
	this->run_compaction(vbucket, "--purge-before=2000000000 --purge-only-upto-seq=15000");
	sleep(30);

	DbInfo* dbinfo1 = this->get_db_info("/opt/couchbase/var/lib/couchbase/data/default/14.couch.3");
	EXPECT_EQ(5000, dbinfo1->deleted_count);

	free(dbinfo1);
}

TEST_F(CompactionTest, Expired1MItemPurgeTest)
{
	this->sendHello();
	this->disable_auto_compaction();
	int numitems = 1000000;
	int vbucket = 20;
	this->insert_items_vbucket(vbucket, numitems);
	sleep(30);

	uint64_t newRevNum = 1;

	string filename = "/opt/couchbase/var/lib/couchbase/data/default/20.couch.1";
	newRevNum = this->checkNewRevNum(filename);
	cout << "current filename: " << filename << "\n";
	DbInfo* dbinfo = this->get_db_info(filename);

	//no documents should be deleted prior to compaction
	EXPECT_EQ(0, dbinfo->deleted_count);

	this->run_compaction(vbucket);
	sleep(120);
	//open vB file with new rev number 
	newRevNum = this->checkNewRevNum(filename);
	cout << "new filename after compaction: " << filename << "\n";

	DbInfo* dbinfo1 = this->get_db_info(filename);
	EXPECT_EQ(1000000, dbinfo1->deleted_count);

	free(dbinfo);
	free(dbinfo1);
}

TEST_F(CompactionTest, DropDeletesTrue1MStoneTest)
{
	this->sendHello();
	this->disable_auto_compaction();
	int numitems = 1000000;
	int vbucket = 14;
	this->insert_items_vbucket(vbucket, numitems);
	sleep(30);
	//run compaction once to mark expired items as deleted
	this->run_compaction(vbucket);
	sleep(240);

	DbInfo* dbinfo1 = this->get_db_info("/opt/couchbase/var/lib/couchbase/data/default/14.couch.2");
	EXPECT_EQ(1000000, dbinfo1->deleted_count);

	this->run_compaction(vbucket, "--purge-before=0 --purge-only-upto-seq=0 --dropdeletes");

	DbInfo* dbinfo2 = this->get_db_info("/opt/couchbase/var/lib/couchbase/data/default/14.couch.3");
	EXPECT_EQ(0, dbinfo2->deleted_count);

	free(dbinfo1);
	free(dbinfo2);
}

//This test needs to be migrated to Warmup Tests
TEST_F(CompactionTest, ExpireAtWarmupTest)
{
	this->sendHello();
	this->disable_auto_compaction();
	stringstream ss;
	ss << "fooaaa";
	storekey(ss.str());
	fprintf(stderr, "\n1 mutation on vbucket 14\n");
	string const restart = "sudo /etc/init.d/couchbase-server restart";
	this->exec(restart.c_str());
	sleep(40);

	DbInfo* dbinfo = this->get_db_info("/opt/couchbase/var/lib/couchbase/data/default/14.couch.1");
	EXPECT_EQ(0, dbinfo->deleted_count);

	free(dbinfo);
}

//This test needs to be migrated to Warmup Tests
TEST_F(CompactionTest, CheckpointWarmupTest)
{
	this->sendHello();
	this->disable_auto_compaction();
	insertItems(10000);
	string const stop = "sudo /etc/init.d/couchbase-server stop";
	this->exec(stop.c_str());
	sleep(30);
	vector<int> vbchkvec = getvbcheckpoints();
	string const start = "sudo /etc/init.d/couchbase-server start";
	this->exec(start.c_str());
	sleep(30);
	(void) lcb_set_stat_callback(instance, stats_generic_callback);
	err = lcb_connect(instance);
	assert(err == LCB_SUCCESS);
	string const checkpoint("checkpoint");
	StatsVector const checkpointstats = this->getgenericstats(checkpoint, instance);
	cout << "vector length : " << checkpointstats.size() << "\n";
	string const base_str = "/opt/couchbase/var/lib/couchbase/data/default/";
	string const append = ".couch.1";

	for (StatsVector::const_iterator itr = checkpointstats.begin(); itr != checkpointstats.end(); ++itr)
	{
		if (!itr->first.compare("vb_999:persisted_checkpoint_id"))
			cout << "vb_999:persisted_checkpoint_id " << itr->second << "\n";
	}
}

int main(int argc, char **argv)
{
	::testing::InitGoogleTest(&argc, argv);
	testargv = argv;
	testargc = argc;
	return RUN_ALL_TESTS();

}
