//the method definition for "checkNewRevNum" is taken 
//from ep-engine source code at ep-engine/src/couch-kvstore/couch-kvstore.cc 
#include <algorithm>
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

extern char *getvaluebuf;
extern long *getvaluesize;
typedef struct DbSize AggDbSize;  
struct DbSize{
	uint64_t fileSize;
	uint64_t dataSize;
}; 

int populateAllKeys(Db *db, DocInfo *docinfo, void *ctx){
    std::vector<std::string>* keyvec = (std::vector<std::string>*)ctx;
    uint16_t keylen = docinfo->id.size;
    char *key = docinfo->id.buf;
    std::string keystr(key,keylen);
    keyvec->push_back(keystr);
    return 0;
}        

class CompactionTest : public BaseTest {


	protected:
	uint64_t checkNewRevNum(std::string &dbFileName){

		uint64_t newrev = 0;
		std::string nameKey;

		// extract out the file revision number first
		size_t secondDot = dbFileName.rfind(".");
		nameKey = dbFileName.substr(0, secondDot);
		nameKey.append(".");
		const std::vector<std::string> files = 
			CouchbaseDirectoryUtilities::findFilesWithPrefix(nameKey);
		std::vector<std::string>::const_iterator itor;
		// found file(s) whoes name has the same key name pair with different
		// revision number
		for (itor = files.begin(); itor != files.end(); ++itor) {
			const std::string &filename = *itor;
			size_t secondDot = filename.rfind(".");
			char *ptr = NULL;
			uint64_t revnum = strtoull(filename.substr(secondDot + 1).c_str(), &ptr, 10);
			if (newrev < revnum) {
				newrev = revnum;
				dbFileName = filename;
			}
		}
		return newrev;
	}

	void sendcompact(uint16_t vbid, uint64_t purge_before_ts, 
			uint64_t purge_before_seq, uint8_t drop_deletes){

		lcb_wait(instance);
		{
			err = lcb_compact(instance, NULL,vbid,purge_before_ts,
					purge_before_seq,drop_deletes);
			assert(err==LCB_SUCCESS);
		}
	}

	void storekey(std::string& myString,bool expire) {
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
                if(expire){
		    cmd.v.v0.exptime = 0x5;
                }
		lcb_store(instance, NULL, 1, &commands[0]);
		lcb_wait(instance);
	}

	void storekey(std::string& myString) {
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

	void print_dbinfo(DbInfo* dbinfo){

		fprintf(stderr, "\nDbinfo filename: %s", dbinfo->filename); 
		fprintf(stderr, "\nDbinfo doc count:  %lu \n", dbinfo->doc_count); 
		fprintf(stderr, "\nDbinfo file size used:  %lu bytes", dbinfo->file_size); 
		fprintf(stderr, "\nDbinfo deleted count:  %lu", dbinfo->deleted_count); 
		fprintf(stderr, "\nDbinfo last seq no:  %lu ", dbinfo->last_sequence); 
		fprintf(stderr, "\nDbinfo purge seq number:  %lu ", dbinfo->purge_seq); 
	} 

	void print_docinfo(DocInfo* docinfo){

		fprintf(stderr, "\ndocument key: ");
		fwrite(docinfo->id.buf, sizeof(char), docinfo->id.size, stderr); 
		fprintf(stderr, "\ndocinfo sequence number: %lu ",docinfo->db_seq); 
		fprintf(stderr, "\ndocinfo rev id: %lu ",docinfo->rev_seq); 
		fprintf(stderr, "\ndocinfo deleted: %d ",docinfo->deleted); 
		fprintf(stderr, "\ndocinfo doc size: %lu ",docinfo->size); 

	}

	int insert_items_vbucket(int vbucket, int numitems, bool expire){
		int num = 0; 
		int key =1;
		//Store items with short TTL in the same vBucket
		while(num<numitems) {

			std::stringstream ss;
			ss << key;
			std::string myString = ss.str();
			int vb =  getvbucketbykey(instance, (const void*) 
					myString.c_str(), myString.size()); 
			if(vb==vbucket) {
				if(expire){
                                    storekey(myString);
				    num++; 
                                }
                                else{
                                    storekey(myString,expire);
                                    num++;
                                }
			}
			key++;
		}
                return 1; 
	}

	int insert_items_vbucket(int vbucket, int numitems){
		int num = 0; 
		int key =1;
		//Store items with short TTL in the same vBucket
		while(num<numitems) {

			std::stringstream ss;
			ss << key;
			std::string myString = ss.str();
			int vb =  getvbucketbykey(instance, (const void*) 
					myString.c_str(), myString.size()); 
			if(vb==vbucket) {
				storekey(myString);
				num++; 
			}
			key++;
		}
                return 1; 
	}

	int insert_items_vbucket(int vbucket, int numitems, int keyhint){
		int num = 0; 
		int key = keyhint;
		//Store items with short TTL in the same vBucket
		while(num<numitems) {

			std::stringstream ss;
			ss << key;
			std::string myString = ss.str();
			int vb =  getvbucketbykey(instance, (const void*) 
					myString.c_str(), myString.size()); 
			if(vb==vbucket) {
				storekey(myString);
				num++; 
			}
			key++;
		} 
                return 1;
	}

        static const std::string getJSONObjString(const cJSON *i){
       
                 if (i == NULL) {
                        return "";
                 }
                 if (i->type != cJSON_String) {
                        abort();
                 }
                 return i->valuestring;
        }

        std::vector<int> getvbcheckpoints(){

                 
		couchstore_error_t error; 
		//structs to hold vB file information
		Db* db = (Db*)malloc(sizeof(Db));
		DbInfo* dbinfo = (DbInfo*)malloc(sizeof(DbInfo));
                LocalDoc* localdoc = (LocalDoc*)malloc(sizeof(LocalDoc));
		int numVBuckets = 1024;
                const char* id = "_local/vbstate";
                std::vector<int> chkpointvec;
 
		for (int vbnum = 0; vbnum<numVBuckets;vbnum++){
			std::string base_str = 
				"/opt/couchbase/var/lib/couchbase/data/default/";
			std::string append = ".couch.1";
			std::string vb_num;
			std::ostringstream convert;
			convert << vbnum;
			vb_num = convert.str();
			std::string filename = base_str+vb_num+append;
			(void)checkNewRevNum(filename);
			//open vB file
			error = couchstore_open_db(filename.c_str(), 
					COUCHSTORE_OPEN_FLAG_RDONLY, &db);
			EXPECT_EQ(0,error);
                        couchstore_open_local_document(db,(void *)id,strlen(id),&localdoc);
                        std::string statjson = std::string(localdoc->json.buf,localdoc->json.size);
                        cJSON *parsedjson = cJSON_Parse(statjson.c_str());
                        std::string checkpoint_id = getJSONObjString(
                                                    cJSON_GetObjectItem(parsedjson,"checkpoint_id"));
                        std::cout<<"vbid: "<<vbnum<<"checkpoint: "<<checkpoint_id<<"\n";
                        chkpointvec.push_back(atoi(checkpoint_id.c_str())); 
               }
               return chkpointvec;
        }
  
	AggDbSize getTotalDbSize(){

		couchstore_error_t error; 
		//structs to hold vB file information
		Db* db = (Db*)malloc(sizeof(Db));
		DbInfo* dbinfo = (DbInfo*)malloc(sizeof(DbInfo));
		int numVBuckets = 1024;
		AggDbSize TotalDbSize;
		char byte;

		for (int vbnum = 0; vbnum<numVBuckets;vbnum++){
			std::string base_str = 
				"/opt/couchbase/var/lib/couchbase/data/default/";
			std::string append = ".couch.1";
			std::string vb_num;
			std::ostringstream convert;
			convert << vbnum;
			vb_num = convert.str();
			std::string filename = base_str+vb_num+append;
			(void)checkNewRevNum(filename);

			//open vB file
			error = couchstore_open_db(filename.c_str(), 
					COUCHSTORE_OPEN_FLAG_RDONLY, &db);
			EXPECT_EQ(0,error);
			//get vB info 
			error = couchstore_db_info(db, dbinfo);
			EXPECT_EQ(0,error);
			TotalDbSize.fileSize = TotalDbSize.fileSize + dbinfo->file_size; 
			TotalDbSize.dataSize = TotalDbSize.dataSize + dbinfo->space_used; 
			//close db handles
			couchstore_close_db(db);
		}    
		return TotalDbSize;
	}

	std::vector<std::string> vb14keylist;
};


TEST_F(CompactionTest, SizeReductionTest) {
        
	sendHello();
	std::stringstream ss;
	ss << "fooaaa";
	std::string myString = ss.str();
	for(int  i = 0;i<100000;i++) {
		storekey(myString);
	}
	sleep(30);
	fprintf(stderr, "\nperformed 100000 mutations on vbucket 14\n"); 

	couchstore_error_t error; 
	//structs to hold vB file information
	Db* db = (Db*)malloc(sizeof(Db));
	DbInfo* dbinfo = (DbInfo*)malloc(sizeof(DbInfo));

	//parameters for LCB_COMPACT command
	uint16_t vbid = 14;
	uint64_t purge_before_ts = 0;  
	uint64_t purge_before_seq =0;
	uint8_t drop_deletes = 0;

	//file sizes before and after compaction
	uint64_t fsize_b4compact =0;
	uint64_t fsize_a4trcompact =0;

	const char *filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.1";

	//open vB file
	error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db);
	EXPECT_EQ(0,error);
	//get vB info 
	error = couchstore_db_info(db, dbinfo);
	EXPECT_EQ(0,error);
	print_dbinfo(dbinfo);
	fsize_b4compact = dbinfo->file_size;
	//close db handles
	couchstore_close_db(db);

	//send lcb_compact command
	sendcompact(vbid, purge_before_ts, purge_before_seq, drop_deletes);  
	lcb_wait(instance);

	//open vB file with new rev number 
	filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.2"; 
	error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db);
	EXPECT_EQ(0,error);
	error = couchstore_db_info(db, dbinfo);
	EXPECT_EQ(0,error);
	print_dbinfo(dbinfo);
	fsize_a4trcompact = dbinfo->file_size;
	//assert the expected file sizes
	EXPECT_LT(fsize_a4trcompact,0.01*fsize_b4compact);
}

TEST_F(CompactionTest, DocsVerificationTest) {
	sendHello();
        std::vector<std::string> keyvec1;
        std::vector<std::string> keyvec2;
        std::vector<Doc> docvec1;
        std::vector<Doc> docvec2;
        bool expire = false;
        insert_items_vbucket(14,10000,expire);
	sleep(30);
	fprintf(stderr, "\nperformed mutations on vbucket 14\n"); 

	couchstore_error_t error; 
	//structs to hold vB file information
	Db* db = (Db*)malloc(sizeof(Db));

	const char *filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.1";
	//open vB file
	error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db);
	EXPECT_EQ(0,error);
	error = couchstore_all_docs(db, NULL,0,populateAllKeys,
                                      static_cast<void *>(&keyvec1));
	EXPECT_EQ(0,error);
        for(std::vector<std::string>::iterator it1 = keyvec1.begin();it1!=keyvec1.end();++it1){
             Doc* doc = (Doc*)malloc(sizeof(Doc));
             couchstore_open_document(db, it1->c_str(), it1->size(),&doc,DECOMPRESS_DOC_BODIES);
             docvec1.push_back(*doc);
             //free(doc); 
        }
        //close db handles
	couchstore_close_db(db);
	//send lcb_compact command
	char compact[] = "/opt/couchbase/bin/cbcompact localhost:11210 compact 14";
	exec(compact);
        sleep(15);
	//open vB file with new rev number 
	filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.2"; 
	error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db);
	EXPECT_EQ(0,error);
	error = couchstore_all_docs(db, NULL,0,populateAllKeys,
                                    static_cast<void *>(&keyvec2));
	EXPECT_EQ(0,error);
        EXPECT_EQ(keyvec1.size(),keyvec2.size());
        
        for(std::vector<std::string>::iterator it1 = keyvec2.begin();it1!=keyvec2.end();++it1){
             Doc* doc = (Doc*)malloc(sizeof(Doc));
             couchstore_open_document(db, it1->c_str(), it1->size(),&doc,DECOMPRESS_DOC_BODIES);
             docvec2.push_back(*doc);
             //free(doc); 
        }
        EXPECT_EQ(docvec1.size(),docvec2.size());
        
        for(int i = 0;i<10000;i++){
             uint16_t keylen1 = docvec1[i].id.size;
             char *key1 = docvec1[i].id.buf;
             std::string keystr1(key1,keylen1);
             uint16_t vallen1 = docvec1[i].data.size;
             char *val1 = docvec1[i].data.buf;
             std::string valstr1(val1,vallen1);
             uint16_t keylen2 = docvec2[i].id.size;
             char *key2 = docvec2[i].id.buf;
             std::string keystr2(key2,keylen2);
             uint16_t vallen2 = docvec2[i].data.size;
             char *val2 = docvec2[i].data.buf;
             std::string valstr2(val2,vallen2);
             EXPECT_EQ(keystr1,keystr2);
             EXPECT_EQ(valstr1,valstr2);
        }
        
}

TEST_F(CompactionTest, MetadataVerificationTest) {
	sendHello();
	std::stringstream ss;
	ss << "fooaaa";
	std::string myString = ss.str();
	for(int  i = 0;i<100000;i++) {
		storekey(myString);
	}
	sleep(30);
	fprintf(stderr, "\nperformed 100000 mutations on vbucket 14\n"); 

	couchstore_error_t error; 
	//structs to hold vB file information
	Db* db = (Db*)malloc(sizeof(Db));
	DocInfo* docinfo1 = (DocInfo*)malloc(sizeof(DocInfo));
	DocInfo* docinfo2 = (DocInfo*)malloc(sizeof(DocInfo));

	//parameters for LCB_COMPACT command
	uint16_t vbid = 14;
	uint64_t purge_before_ts = 0;  
	uint64_t purge_before_seq =0;
	uint8_t drop_deletes = 0;
	const char *filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.1";
	//open vB file
	error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db);
	EXPECT_EQ(0,error);
	error = couchstore_docinfo_by_id(db,(void*)myString.c_str(),myString.size(),&docinfo1);
	EXPECT_EQ(0,error);
	//close db handles
	couchstore_close_db(db);
	//send lcb_compact command
	sendcompact(vbid, purge_before_ts, purge_before_seq, drop_deletes);  
	lcb_wait(instance);

	//open vB file with new rev number 
	filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.2"; 
	error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db);
	EXPECT_EQ(0,error);
	error = couchstore_docinfo_by_id(db,(void*)myString.c_str(),myString.size(),&docinfo2);
	EXPECT_EQ(0,error);
	//assert the expected file sizes
	EXPECT_EQ(docinfo1->db_seq,docinfo2->db_seq);
	EXPECT_EQ(docinfo1->rev_seq,docinfo2->rev_seq);
	EXPECT_EQ(docinfo1->deleted,docinfo2->deleted);
	EXPECT_EQ(docinfo1->size,docinfo2->size);
}

TEST_F(CompactionTest, FileStatVerifyTest) {
	char autocompact[] = "curl -X POST -u Administrator:password -d  "
		"autoCompactionDefined=true -d parallelDBAndViewCompaction=false  "
		"-d databaseFragmentationThreshold[percentage]=95  "
		"http://jackfruit-s12201.sc.couchbase.com:8091/controller/setAutoCompaction";
	exec(autocompact);   
	sendHello();
	int keyhint = 0;
	bool perc = true;
	int numVbuckets = 1024;
	AggDbSize CurrDbSize = {0,0};
	uint64_t prevfileSize = 0;
	while(true){
		insert_items_vbucket(14,10000,keyhint);
		keyhint = keyhint+10000;
		sleep(5);
		CurrDbSize = getTotalDbSize();
		if(CurrDbSize.fileSize<prevfileSize) break;
		prevfileSize = CurrDbSize.fileSize;
		fprintf(stderr, "\ncurrDataSize:  %lu bytes", CurrDbSize.dataSize); 
	}
	sleep(60);
	uint64_t newRevNum = 1;
	std::string filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.1";
	newRevNum = checkNewRevNum(filename);
	EXPECT_GE(newRevNum,2);
	//structs to hold vB file information
	Db* db = (Db*)malloc(sizeof(Db));
	DbInfo* dbinfo = (DbInfo*)malloc(sizeof(DbInfo));
	//open vB file
	couchstore_error_t error = couchstore_open_db(filename.c_str(), COUCHSTORE_OPEN_FLAG_RDONLY, &db);
	EXPECT_EQ(0,error);
	//get vB info 
	error = couchstore_db_info(db, dbinfo);
	EXPECT_EQ(0,error);
        uint64_t space_used = 0;
        uint64_t file_size = 0;
        (void)lcb_set_stat_callback(instance, stats_generic_callback);
        std::string diskinfo("diskinfo detail");
        StatsVector diskinfostats = getgenericstats(diskinfo,instance);
        std::cout<<"vector length : "<<diskinfostats.size()<<"\n"; 
        for(StatsVector::iterator itr = diskinfostats.begin(); itr!=diskinfostats.end();++itr){ 
             std::cout<<itr->first<<"\n";
             if(!itr->first.compare("vb_14:data_size")){
                  std::cout<<"vb_14:data_size: "<<itr->second<<"\n";
                  space_used = atoi(itr->second.c_str());
             } 
             if(!itr->first.compare("vb_14:file_size")){
                  std::cout<<"vb_14:file_size: "<<itr->second<<"\n";
                  file_size = atoi(itr->second.c_str());
             }
        }
        EXPECT_EQ(space_used,dbinfo->space_used);
        EXPECT_EQ(file_size,dbinfo->file_size);
        genericstats.refcount = 0;
        genericstats.statsvec.clear();
        uint64_t high_seqno = 0;
        uint64_t purge_seqno = 0;
        std::string vbseqno("vbucket-seqno 14");
        StatsVector vbseqnostats = getgenericstats(vbseqno,instance); 
        std::cout<<"vector length : "<<vbseqnostats.size()<<"\n"; 
        for(StatsVector::iterator itr = vbseqnostats.begin(); itr!=vbseqnostats.end();++itr){ 
             std::cout<<itr->first<<"\n";
             if(!itr->first.compare("vb_14:high_seqno")){
                  std::cout<<"vb_14:high_seqno: "<<itr->second<<"\n";
                  high_seqno = atoi(itr->second.c_str());
                 
             }
             if(!itr->first.compare("vb_14:purge_seqno")){
                  std::cout<<"vb_14:purge_seqno: "<<itr->second<<"\n";
                  purge_seqno = atoi(itr->second.c_str());
             }
        }
        EXPECT_EQ(high_seqno,dbinfo->last_sequence);
        EXPECT_EQ(purge_seqno,dbinfo->purge_seq);
       
}


TEST_F(CompactionTest, FragThresholdPercTest) {
	char autocompact[] = "curl -X POST -u Administrator:password -d  "
		"autoCompactionDefined=true -d parallelDBAndViewCompaction=false  "
		"-d databaseFragmentationThreshold[percentage]=95  "
		"http://localhost:8091/controller/setAutoCompaction";
	exec(autocompact);   
	sendHello();
	int FragPercThreshold = 95;
	int RealPercFrag = 0;
	int keyhint = 0;
	bool perc = true;
	int numVbuckets = 1024;
	int MinFileSize = 131072*numVbuckets;
	AggDbSize CurrDbSize = {0,0};
	uint64_t prevfileSize = 0;
	while(RealPercFrag<FragPercThreshold || CurrDbSize.fileSize<MinFileSize){
		insert_items_vbucket(14,10000,keyhint);
		keyhint = keyhint+10000;
		sleep(5);
		CurrDbSize = getTotalDbSize();
		if(CurrDbSize.fileSize<prevfileSize) break;
		prevfileSize = CurrDbSize.fileSize;
		RealPercFrag = ((CurrDbSize.fileSize-CurrDbSize.dataSize)*100)
			/CurrDbSize.fileSize;
		fprintf(stderr, "\ncurrFileSize:  %lu bytes", CurrDbSize.fileSize); 
		fprintf(stderr, "\ncurrDataSize:  %lu bytes", CurrDbSize.dataSize); 
		fprintf(stderr, "\nRealPercFrag:  %d", RealPercFrag); 
	}
	sleep(60);
	uint64_t newRevNum = 1;
	std::string filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.1";
	newRevNum = checkNewRevNum(filename);
	EXPECT_GE(newRevNum,2); 
}


TEST_F(CompactionTest, FragThresholdSizeTest) {
	char autocompact[] = "curl -X POST -u Administrator:password -d  "
		"autoCompactionDefined=true -d parallelDBAndViewCompaction=false  "
		"-d databaseFragmentationThreshold[size]=100000000  "
		"http://localhost:8091/controller/setAutoCompaction";
	exec(autocompact);   
	sendHello();
	int FragSizeThreshold = 100000000;
	int RealSizeFrag = 0;
	int keyhint = 0;
	bool perc = false;
	int numVbuckets = 1024;
	int MinFileSize = 131072*numVbuckets;
	AggDbSize CurrDbSize = {0,0};
	uint64_t prevfileSize = 0;

	while(RealSizeFrag<FragSizeThreshold || CurrDbSize.fileSize<MinFileSize){
		insert_items_vbucket(14,10000,keyhint);
		keyhint = keyhint+10000;
		sleep(5);
		CurrDbSize = getTotalDbSize();
		if(CurrDbSize.fileSize<prevfileSize) break;
		prevfileSize = CurrDbSize.fileSize;
		RealSizeFrag = CurrDbSize.fileSize-CurrDbSize.dataSize;
		fprintf(stderr, "\ncurrFileSize:  %lu bytes", CurrDbSize.fileSize);
		fprintf(stderr, "\ncurrDataSize:  %lu bytes", CurrDbSize.dataSize);
		fprintf(stderr, "\nRealSizeFrag:  %d", RealSizeFrag);
	}
	sleep(60);
	uint64_t newRevNum = 1;
	std::string filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.1";
	newRevNum = checkNewRevNum(filename);
	EXPECT_GE(newRevNum,2); 
}

TEST_F(CompactionTest, Expired10ItemPurgeTest){
	sendHello();
	couchstore_error_t error; 
	int numitems = 10;
	int vbucket = 15;
	insert_items_vbucket(vbucket, numitems);  
	sleep(30);
	//structs to hold vB file information
	Db* db = (Db*)malloc(sizeof(Db));
	DbInfo* dbinfo = (DbInfo*)malloc(sizeof(DbInfo));

	//parameters for LCB_COMPACT command
	uint16_t vbid = 15;
	uint64_t purge_before_ts = 0;  
	uint64_t purge_before_seq =0;
	uint8_t drop_deletes = 0;

	//file sizes before and after compaction
	uint64_t fsize_b4compact =0;
	uint64_t fsize_a4trcompact =0;

	const char *filename = "/opt/couchbase/var/lib/couchbase/data/default/15.couch.1";

	//open vB file
	error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db);
	EXPECT_EQ(0,error);
	//get vB info 
	error = couchstore_db_info(db, dbinfo);
	print_dbinfo(dbinfo);
	fsize_b4compact = dbinfo->file_size;

	//no documents should be deleted prior to compaction
	EXPECT_EQ(0,dbinfo->deleted_count);

	//close db handles
	couchstore_close_db(db);
	//send lcb_compact command
	//sendcompact(vbid, purge_before_ts, purge_before_seq, drop_deletes);  
	//lcb_wait(instance);
	char compact[] = "/opt/couchbase/bin/cbcompact localhost:11210 compact 15";
	exec(compact);

	sleep(120);
	//open vB file with new rev number 
	filename = "/opt/couchbase/var/lib/couchbase/data/default/15.couch.2"; 
	error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db);
	EXPECT_EQ(0,error);
	error = couchstore_db_info(db, dbinfo);
	EXPECT_EQ(0,error);
	print_dbinfo(dbinfo);
	fsize_a4trcompact = dbinfo->file_size;
	//11 documents should be deleted post compaction
	EXPECT_EQ(10,dbinfo->deleted_count);

}

TEST_F(CompactionTest, Expired1KItemPurgeTest){
	sendHello();
	couchstore_error_t error; 
	int numitems = 1000;
	int vbucket = 16;
	insert_items_vbucket(vbucket, numitems);  
	sleep(30);
	//structs to hold vB file information
	Db* db = (Db*)malloc(sizeof(Db));
	DbInfo* dbinfo = (DbInfo*)malloc(sizeof(DbInfo));

	//parameters for LCB_COMPACT command
	uint16_t vbid = 16;
	uint64_t purge_before_ts = 0;  
	uint64_t purge_before_seq =0;
	uint8_t drop_deletes = 0;

	//file sizes before and after compaction
	uint64_t fsize_b4compact =0;
	uint64_t fsize_a4trcompact =0;

	const char *filename = "/opt/couchbase/var/lib/couchbase/data/default/16.couch.1";

	//open vB file
	error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db);
	EXPECT_EQ(0,error);
	//get vB info 
	error = couchstore_db_info(db, dbinfo);
	print_dbinfo(dbinfo);
	fsize_b4compact = dbinfo->file_size;

	//no documents should be deleted prior to compaction
	EXPECT_EQ(0,dbinfo->deleted_count);

	//close db handles
	couchstore_close_db(db);
	//send lcb_compact command
	//sendcompact(vbid, purge_before_ts, purge_before_seq, drop_deletes);  
	//lcb_wait(instance);
	char compact[] = "/opt/couchbase/bin/cbcompact localhost:11210 compact 16";
	exec(compact);

	sleep(120);
	//open vB file with new rev number 
	filename = "/opt/couchbase/var/lib/couchbase/data/default/16.couch.2"; 
	error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db);
	EXPECT_EQ(0,error);
	error = couchstore_db_info(db, dbinfo);
	EXPECT_EQ(0,error);
	print_dbinfo(dbinfo);
	fsize_a4trcompact = dbinfo->file_size;
	//11 documents should be deleted post compaction
	EXPECT_EQ(1000,dbinfo->deleted_count);

}

TEST_F(CompactionTest, Expired10KItemPurgeTest){
	sendHello();
	couchstore_error_t error; 
	int numitems = 10000;
	int vbucket = 16;
	insert_items_vbucket(vbucket, numitems);  
	sleep(30);
	//structs to hold vB file information
	Db* db = (Db*)malloc(sizeof(Db));
	DbInfo* dbinfo = (DbInfo*)malloc(sizeof(DbInfo));

	//parameters for LCB_COMPACT command
	uint16_t vbid = 16;
	uint64_t purge_before_ts = 0;  
	uint64_t purge_before_seq =0;
	uint8_t drop_deletes = 0;

	//file sizes before and after compaction
	uint64_t fsize_b4compact =0;
	uint64_t fsize_a4trcompact =0;

	const char *filename = "/opt/couchbase/var/lib/couchbase/data/default/16.couch.1";

	//open vB file
	error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db);
	EXPECT_EQ(0,error);
	//get vB info 
	error = couchstore_db_info(db, dbinfo);
	print_dbinfo(dbinfo);
	fsize_b4compact = dbinfo->file_size;

	//no documents should be deleted prior to compaction
	EXPECT_EQ(0,dbinfo->deleted_count);

	//close db handles
	couchstore_close_db(db);
	//send lcb_compact command
	//sendcompact(vbid, purge_before_ts, purge_before_seq, drop_deletes);  
	//lcb_wait(instance);
	char compact[] = "/opt/couchbase/bin/cbcompact localhost:11210 compact 16";
	exec(compact);

	sleep(120);
	//open vB file with new rev number 
	filename = "/opt/couchbase/var/lib/couchbase/data/default/16.couch.2"; 
	error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db);
	EXPECT_EQ(0,error);
	error = couchstore_db_info(db, dbinfo);
	EXPECT_EQ(0,error);
	print_dbinfo(dbinfo);
	fsize_a4trcompact = dbinfo->file_size;
	//11 documents should be deleted post compaction
	EXPECT_EQ(10000,dbinfo->deleted_count);

}


TEST_F(CompactionTest, Expired100KItemPurgeTest){
	sendHello();
	couchstore_error_t error; 
	//structs to hold vB file information
	Db* db = (Db*)malloc(sizeof(Db));
	DbInfo* dbinfo = (DbInfo*)malloc(sizeof(DbInfo));
	//open vB file
	const char *filename = "/opt/couchbase/var/lib/couchbase/data/default/18.couch.1";
	error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db);
	EXPECT_EQ(0,error);
	int numitems = 100000;
	int vbucket = 18;
	insert_items_vbucket(vbucket, numitems);  
	sleep(30);

	//parameters for LCB_COMPACT command
	uint16_t vbid = 18;
	uint64_t purge_before_ts = 0;  
	uint64_t purge_before_seq =0;
	uint8_t drop_deletes = 0;

	//file sizes before and after compaction
	uint64_t fsize_b4compact =0;
	uint64_t fsize_a4trcompact =0;


	//get vB info 
	error = couchstore_db_info(db, dbinfo);
	print_dbinfo(dbinfo);
	fsize_b4compact = dbinfo->file_size;

	//no documents should be deleted prior to compaction
	EXPECT_EQ(0,dbinfo->deleted_count);

	//close db handles
	couchstore_close_db(db);
	//send lcb_compact command
	//sendcompact(vbid, purge_before_ts, purge_before_seq, drop_deletes);  
	//lcb_wait(instance);
	char compact[] = "/opt/couchbase/bin/cbcompact localhost:11210 compact 18";
	exec(compact);

	sleep(120);
	//open vB file with new rev number 
	filename = "/opt/couchbase/var/lib/couchbase/data/default/18.couch.2"; 
	error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db);
	EXPECT_EQ(0,error);
	error = couchstore_db_info(db, dbinfo);
	EXPECT_EQ(0,error);
	print_dbinfo(dbinfo);
	fsize_a4trcompact = dbinfo->file_size;
	//11 documents should be deleted post compaction
	EXPECT_EQ(100000,dbinfo->deleted_count);

}


TEST_F(CompactionTest, DropDeletesFalseDocExptimeGreaterTest) {

	sendHello(); 
	int numitems = 10;
	int vbucket = 14;
	insert_items_vbucket(vbucket, numitems);  
	sleep(30);
	//set compact_cmd parameters 
	uint16_t vbid = 14;
	uint32_t exptime = 946759881;                   // harcoded to Jan 01 2000
	uint64_t purge_before_ts = exptime;  
	uint64_t purge_before_seq =1;
	uint8_t drop_deletes = 0;
	//run compaction once to mark expired items as deleted 
	sendcompact(vbid, purge_before_ts, purge_before_seq, drop_deletes);  
	lcb_wait(instance);
	sleep(60);

	couchstore_error_t error; 
	Db* db1 = (Db*)malloc(sizeof(Db));
	Db* db2 = (Db*)malloc(sizeof(Db));
	DbInfo* dbinfo1 = (DbInfo*)malloc(sizeof(DbInfo));
	DbInfo* dbinfo2 = (DbInfo*)malloc(sizeof(DbInfo));
	DocInfo* docinfo1 = (DocInfo*)malloc(sizeof(DocInfo));
	DocInfo* docinfo2 = (DocInfo*)malloc(sizeof(DocInfo));
	//get the expiry time of a doc
	const char* key = "fooaaa"; 
	uint64_t fsize_b4compact =0;
	uint64_t fsize_a4trcompact =0;
	const char *filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.2"; 

	error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db1);
	EXPECT_EQ(0,error);

	error= couchstore_db_info(db1, dbinfo1);
	EXPECT_EQ(0,error);
	print_dbinfo(dbinfo1);
	EXPECT_EQ(10,dbinfo1->deleted_count);
	couchstore_close_db(db1);

	//send another compaction to drop the tombstones
	//sendcompact(vbid, purge_before_ts, purge_before_seq, drop_deletes);  
	//lcb_wait(instance);
	char compact2[] = "/opt/couchbase/bin/cbcompact localhost:11210 compact 14  "
		"--purge-before=946759881 --purge-only-upto-seq=1";
	exec(compact2);

	filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.3"; 
	error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db2);
	EXPECT_EQ(0,error);
	error = couchstore_db_info(db2, dbinfo2);
	EXPECT_EQ(0,error);
	print_dbinfo(dbinfo2);
	EXPECT_EQ(10,dbinfo2->deleted_count);

}

TEST_F(CompactionTest, DropDeletesFalseDocPurgeseqGreaterTest) {

	sendHello(); 
	int numitems = 10;
	int vbucket = 14;
	insert_items_vbucket(vbucket, numitems);  
	sleep(30);
	//set compact_cmd parameters 
	uint16_t vbid = 14;
	uint64_t exptime = 2946759881;                   //a very future date
	uint64_t purge_before_ts = exptime;  
	uint64_t purge_before_seq =1;
	uint8_t drop_deletes = 0;
	//run compaction once to mark expired items as deleted 
	char compact[] = "/opt/couchbase/bin/cbcompact localhost:11210 compact 14";
	exec(compact);
	sleep(60);

	couchstore_error_t error; 
	Db* db1 = (Db*)malloc(sizeof(Db));
	Db* db2 = (Db*)malloc(sizeof(Db));
	DbInfo* dbinfo1 = (DbInfo*)malloc(sizeof(DbInfo));
	DbInfo* dbinfo2 = (DbInfo*)malloc(sizeof(DbInfo));
	DocInfo* docinfo1 = (DocInfo*)malloc(sizeof(DocInfo));
	DocInfo* docinfo2 = (DocInfo*)malloc(sizeof(DocInfo));
	//get the expiry time of a doc
	const char* key = "fooaaa"; 
	uint64_t fsize_b4compact =0;
	uint64_t fsize_a4trcompact =0;
	const char *filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.2"; 

	error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db1);
	EXPECT_EQ(0,error);

	error= couchstore_db_info(db1, dbinfo1);
	EXPECT_EQ(0,error);
	print_dbinfo(dbinfo1);
	EXPECT_EQ(10,dbinfo1->deleted_count);
	couchstore_close_db(db1);

	//send another compaction to drop the tombstones
	//sendcompact(vbid, purge_before_ts, purge_before_seq, drop_deletes);  
	//lcb_wait(instance);
	char compact2[] = "/opt/couchbase/bin/cbcompact localhost:11210 compact 14  "
		"--purge-before=2946759881 --purge-only-upto-seq=1";
	exec(compact2);

	filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.3"; 
	error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db2);
	EXPECT_EQ(0,error);
	error = couchstore_db_info(db2, dbinfo2);
	EXPECT_EQ(0,error);
	print_dbinfo(dbinfo2);
	EXPECT_EQ(10,dbinfo2->deleted_count);

}

TEST_F(CompactionTest, DropDeletesFalsePurgeSeqZeroTest) {

	sendHello(); 
	std::stringstream ss;
	ss << "fooaaa";
	std::string myString = ss.str();
	for(int  i = 0;i<1000;i++) {
		storekey(myString);
	}
	sleep(30);
	//run compaction once to mark expired items as deleted 
	char compact[] = "/opt/couchbase/bin/cbcompact localhost:11210 compact 14";
	exec(compact);
	sleep(60);
	couchstore_error_t error; 
	Db* db1 = (Db*)malloc(sizeof(Db));
	Db* db2 = (Db*)malloc(sizeof(Db));
	DbInfo* dbinfo1 = (DbInfo*)malloc(sizeof(DbInfo));
	DbInfo* dbinfo2 = (DbInfo*)malloc(sizeof(DbInfo));
	DocInfo* docinfo1 = (DocInfo*)malloc(sizeof(DocInfo));
	DocInfo* docinfo2 = (DocInfo*)malloc(sizeof(DocInfo));
	//get the expiry time of a doc
	uint32_t exptime;
	const char* key = "fooaaa"; 
	uint16_t vbid = 14;
	uint64_t fsize_b4compact =0;
	uint64_t fsize_a4trcompact =0;
	const char *filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.2"; 

	error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db1);
	EXPECT_EQ(0,error);
	error = couchstore_docinfo_by_id(db1,(void*)key,6,&docinfo1);
	EXPECT_EQ(0,error);
	print_docinfo(docinfo1);

	//read the document expiry time and decrement it 
	memcpy(&exptime, docinfo1->rev_meta.buf + 8, 4);
	exptime = ntohl(exptime);
	exptime = exptime +100;
	couchstore_free_docinfo(docinfo1);

	error= couchstore_db_info(db1, dbinfo1);
	EXPECT_EQ(0,error);
	print_dbinfo(dbinfo1);
	fsize_b4compact = dbinfo1->file_size;
	couchstore_close_db(db1);
	//set compact_cmd parameters 
	uint64_t purge_before_ts = exptime;  
	uint64_t purge_before_seq =0;
	uint8_t drop_deletes = 0;

	sendcompact(vbid, purge_before_ts, purge_before_seq, drop_deletes);  
	lcb_wait(instance);
        sleep(5);
	filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.3"; 
	error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db2);
	EXPECT_EQ(0,error);
	error = couchstore_docinfo_by_id(db2, (void*)key,6,&docinfo2);
	print_docinfo(docinfo2);
	EXPECT_EQ(-5,error);
	error = couchstore_db_info(db2, dbinfo2);
	EXPECT_EQ(0,error);
	print_dbinfo(dbinfo2);
	EXPECT_EQ(0,dbinfo2->deleted_count);
}

TEST_F(CompactionTest, DropDeletesMaxPurgedSeqnoTest) {

	sendHello(); 
	std::stringstream ss;
	ss << "fooaaa";
	std::string myString = ss.str();
	for(int  i = 0;i<1000;i++) {
		storekey(myString);
	}
	sleep(30);
	couchstore_error_t error; 
	Db* db1 = (Db*)malloc(sizeof(Db));
	Db* db2 = (Db*)malloc(sizeof(Db));
	DbInfo* dbinfo1 = (DbInfo*)malloc(sizeof(DbInfo));
	DbInfo* dbinfo2 = (DbInfo*)malloc(sizeof(DbInfo));
	DocInfo* docinfo1 = (DocInfo*)malloc(sizeof(DocInfo));
	DocInfo* docinfo2 = (DocInfo*)malloc(sizeof(DocInfo));
	//get the expiry time of a doc
	uint32_t exptime;
	const char* key = "fooaaa"; 
	uint16_t vbid = 14;
	uint64_t fsize_b4compact =0;
	uint64_t fsize_a4trcompact =0;
	const char *filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.1"; 

	error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db1);
	EXPECT_EQ(0,error);
	error = couchstore_docinfo_by_id(db1,(void*)key,6,&docinfo1);
	EXPECT_EQ(0,error);
	print_docinfo(docinfo1);
	uint64_t docrev_seqno = docinfo1->db_seq;
	//run compaction once to mark expired items as deleted 
	char compact[] = "/opt/couchbase/bin/cbcompact localhost:11210 compact 14";
	exec(compact);
	sleep(60);

	//read the document expiry time and decrement it 
	memcpy(&exptime, docinfo1->rev_meta.buf + 8, 4);
	exptime = ntohl(exptime);
	exptime = exptime +100;
	couchstore_free_docinfo(docinfo1);

	error= couchstore_db_info(db1, dbinfo1);
	EXPECT_EQ(0,error);
	print_dbinfo(dbinfo1);
	fsize_b4compact = dbinfo1->file_size;
	couchstore_close_db(db1);
	//set compact_cmd parameters 
	uint64_t purge_before_ts = exptime;  
	uint64_t purge_before_seq =0;
	uint8_t drop_deletes = 1;

	sendcompact(vbid, purge_before_ts, purge_before_seq, drop_deletes);  
	lcb_wait(instance);
	sleep(10);

	filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.3"; 
	error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db2);
	EXPECT_EQ(0,error);
	error = couchstore_docinfo_by_id(db2, (void*)key,6,&docinfo2);
	print_docinfo(docinfo2);
	EXPECT_EQ(-5,error);
	error = couchstore_db_info(db2, dbinfo2);
	EXPECT_EQ(0,error);
	print_dbinfo(dbinfo2);
	EXPECT_EQ(docrev_seqno+1,dbinfo2->purge_seq);
}

TEST_F(CompactionTest, DropDeletesTrue10StoneTest) {

	sendHello(); 
	int numitems = 10;
	int vbucket = 14;
	insert_items_vbucket(vbucket, numitems);  
	sleep(30);
	//set compact_cmd parameters 
	uint16_t vbid = 14;
	uint32_t exptime = 0;
	uint64_t purge_before_ts = exptime;  
	uint64_t purge_before_seq =1;
	uint8_t drop_deletes = 1;
	//run compaction once to mark expired items as deleted 
	sendcompact(vbid, purge_before_ts, purge_before_seq, drop_deletes);  
	lcb_wait(instance);
	sleep(60);

	couchstore_error_t error; 
	Db* db1 = (Db*)malloc(sizeof(Db));
	Db* db2 = (Db*)malloc(sizeof(Db));
	DbInfo* dbinfo1 = (DbInfo*)malloc(sizeof(DbInfo));
	DbInfo* dbinfo2 = (DbInfo*)malloc(sizeof(DbInfo));
	DocInfo* docinfo1 = (DocInfo*)malloc(sizeof(DocInfo));
	DocInfo* docinfo2 = (DocInfo*)malloc(sizeof(DocInfo));
	//get the expiry time of a doc
	const char* key = "fooaaa"; 
	uint64_t fsize_b4compact =0;
	uint64_t fsize_a4trcompact =0;
	const char *filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.2"; 

	error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db1);
	EXPECT_EQ(0,error);

	error= couchstore_db_info(db1, dbinfo1);
	EXPECT_EQ(0,error);
	print_dbinfo(dbinfo1);
	EXPECT_EQ(10,dbinfo1->deleted_count);
	couchstore_close_db(db1);

	//send another compaction to drop the tombstones
	//sendcompact(vbid, purge_before_ts, purge_before_seq, drop_deletes);  
	//lcb_wait(instance);
	char compact2[] = "/opt/couchbase/bin/cbcompact localhost:11210 compact 14  "
		"--purge-before=946759881 --purge-only-upto-seq=1 --dropdeletes";
	exec(compact2);

	filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.3"; 
	error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db2);
	EXPECT_EQ(0,error);
	error = couchstore_db_info(db2, dbinfo2);
	EXPECT_EQ(0,error);
	print_dbinfo(dbinfo2);
	EXPECT_EQ(0,dbinfo2->deleted_count);

}

TEST_F(CompactionTest, DropDeletesTrue10KStoneTest) {

	sendHello(); 
	int numitems = 10000;
	int vbucket = 14;
	insert_items_vbucket(vbucket, numitems);  
	sleep(30);
	//set compact_cmd parameters 
	uint16_t vbid = 14;
	uint32_t exptime = 0;
	uint64_t purge_before_ts = exptime;  
	uint64_t purge_before_seq =1;
	uint8_t drop_deletes = 1;
	//run compaction once to mark expired items as deleted 
	sendcompact(vbid, purge_before_ts, purge_before_seq, drop_deletes);  
	lcb_wait(instance);
	sleep(100);

	couchstore_error_t error; 
	Db* db1 = (Db*)malloc(sizeof(Db));
	Db* db2 = (Db*)malloc(sizeof(Db));
	DbInfo* dbinfo1 = (DbInfo*)malloc(sizeof(DbInfo));
	DbInfo* dbinfo2 = (DbInfo*)malloc(sizeof(DbInfo));
	DocInfo* docinfo1 = (DocInfo*)malloc(sizeof(DocInfo));
	DocInfo* docinfo2 = (DocInfo*)malloc(sizeof(DocInfo));
	//get the expiry time of a doc
	const char* key = "fooaaa"; 
	uint64_t fsize_b4compact =0;
	uint64_t fsize_a4trcompact =0;
	const char *filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.2"; 

	error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db1);
	EXPECT_EQ(0,error);

	error= couchstore_db_info(db1, dbinfo1);
	EXPECT_EQ(0,error);
	print_dbinfo(dbinfo1);
	EXPECT_EQ(10000,dbinfo1->deleted_count);
	couchstore_close_db(db1);

	//send another compaction to drop the tombstones
	//sendcompact(vbid, purge_before_ts, purge_before_seq, drop_deletes);  
	//lcb_wait(instance);
	char compact2[] = "/opt/couchbase/bin/cbcompact localhost:11210 compact 14  "
		"--purge-before=946759881 --purge-only-upto-seq=1 --dropdeletes";
	exec(compact2);

	filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.3"; 
	error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db2);
	EXPECT_EQ(0,error);
	error = couchstore_db_info(db2, dbinfo2);
	EXPECT_EQ(0,error);
	print_dbinfo(dbinfo2);
	EXPECT_EQ(0,dbinfo2->deleted_count);

}

TEST_F(CompactionTest, DropDeletesTrue100KStoneTest) {

	sendHello(); 
	int numitems = 100000;
	int vbucket = 14;
	insert_items_vbucket(vbucket, numitems);  
	sleep(30);
	//set compact_cmd parameters 
	uint16_t vbid = 14;
	uint32_t exptime = 0;
	uint64_t purge_before_ts = exptime;  
	uint64_t purge_before_seq =1;
	uint8_t drop_deletes = 1;
	//run compaction once to mark expired items as deleted 
	sendcompact(vbid, purge_before_ts, purge_before_seq, drop_deletes);  
	lcb_wait(instance);
	sleep(240);

	couchstore_error_t error; 
	Db* db1 = (Db*)malloc(sizeof(Db));
	Db* db2 = (Db*)malloc(sizeof(Db));
	DbInfo* dbinfo1 = (DbInfo*)malloc(sizeof(DbInfo));
	DbInfo* dbinfo2 = (DbInfo*)malloc(sizeof(DbInfo));
	DocInfo* docinfo1 = (DocInfo*)malloc(sizeof(DocInfo));
	DocInfo* docinfo2 = (DocInfo*)malloc(sizeof(DocInfo));
	//get the expiry time of a doc
	const char* key = "fooaaa"; 
	uint64_t fsize_b4compact =0;
	uint64_t fsize_a4trcompact =0;
	const char *filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.2"; 

	error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db1);
	EXPECT_EQ(0,error);

	error= couchstore_db_info(db1, dbinfo1);
	EXPECT_EQ(0,error);
	print_dbinfo(dbinfo1);
	EXPECT_EQ(100000,dbinfo1->deleted_count);
	couchstore_close_db(db1);

	//send another compaction to drop the tombstones
	//sendcompact(vbid, purge_before_ts, purge_before_seq, drop_deletes);  
	//lcb_wait(instance);
	char compact2[] = "/opt/couchbase/bin/cbcompact localhost:11210 compact 14  "
		"--purge-before=946759881 --purge-only-upto-seq=1 --dropdeletes";
	exec(compact2);

	filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.3"; 
	error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db2);
	EXPECT_EQ(0,error);
	error = couchstore_db_info(db2, dbinfo2);
	EXPECT_EQ(0,error);
	print_dbinfo(dbinfo2);
	EXPECT_EQ(0,dbinfo2->deleted_count);

}

TEST_F(CompactionTest, cbcompactnodropsTest) {

	couchstore_error_t error; 
	Db* db1 = (Db*)malloc(sizeof(Db));
	DbInfo* dbinfo1 = (DbInfo*)malloc(sizeof(DbInfo));
	sendHello(); 
	int numitems = 10000;
	int vbucket = 14;
	insert_items_vbucket(vbucket, numitems);  
	sleep(30);
	char compact[] = "/opt/couchbase/bin/cbcompact localhost:11210 compact 14";
	exec(compact);
	sleep(30);
	const char *filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.2"; 
	error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db1);
	EXPECT_EQ(0,error);
	error = couchstore_db_info(db1, dbinfo1);
	EXPECT_EQ(0,error);
	EXPECT_EQ(10000,dbinfo1->deleted_count);

}

TEST_F(CompactionTest, cbcompactdropTrueTest) {

	couchstore_error_t error; 
	Db* db1 = (Db*)malloc(sizeof(Db));
	DbInfo* dbinfo1 = (DbInfo*)malloc(sizeof(DbInfo));
	sendHello(); 
	int numitems = 10000;
	int vbucket = 14;
	insert_items_vbucket(vbucket, numitems);  
	sleep(30);
	char compact[] = "/opt/couchbase/bin/cbcompact localhost:11210 compact 14";
	exec(compact);
	sleep(30);
	char compact2[] = "/opt/couchbase/bin/cbcompact localhost:11210 compact 14  "
		"--purge-before=0 --purge-only-upto-seq=0 --dropdeletes";
	exec(compact2);
	sleep(30);
	const char *filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.3"; 
	error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db1);
	EXPECT_EQ(0,error);
	error = couchstore_db_info(db1, dbinfo1);
	EXPECT_EQ(0,error);
	EXPECT_EQ(0,dbinfo1->deleted_count);

}

TEST_F(CompactionTest, cbcompactdropexpTest) {

	couchstore_error_t error; 
	Db* db1 = (Db*)malloc(sizeof(Db));
	DbInfo* dbinfo1 = (DbInfo*)malloc(sizeof(DbInfo));
	sendHello(); 
	int numitems = 10000;
	int vbucket = 14;
	insert_items_vbucket(vbucket, numitems);  
	sleep(30);
	char compact[] = "/opt/couchbase/bin/cbcompact localhost:11210 compact 14";
	exec(compact);
	//we wont hit the epoch time below any time soon
	char compact2[] = "/opt/couchbase/bin/cbcompact localhost:11210 compact 14  "
		"--purge-before=2000000000 --purge-only-upto-seq=20000";
	exec(compact2);
	sleep(30);
	const char *filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.3"; 
	error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db1);
	EXPECT_EQ(0,error);
	error = couchstore_db_info(db1, dbinfo1);
	EXPECT_EQ(0,error);
	EXPECT_EQ(0,dbinfo1->deleted_count);

}

TEST_F(CompactionTest, cbcompactdropseqnoTest) {

	couchstore_error_t error; 
	Db* db1 = (Db*)malloc(sizeof(Db));
	DbInfo* dbinfo1 = (DbInfo*)malloc(sizeof(DbInfo));
	sendHello(); 
	int numitems = 10000;
	int vbucket = 14;
	insert_items_vbucket(vbucket, numitems);  
	sleep(30);
	char compact[] = "/opt/couchbase/bin/cbcompact localhost:11210 compact 14";
	exec(compact);
	char compact2[] = "/opt/couchbase/bin/cbcompact localhost:11210 compact 14  "
		"--purge-before=2000000000 --purge-only-upto-seq=15000";
	exec(compact2);
	sleep(30);
	const char *filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.3"; 
	error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db1);
	EXPECT_EQ(0,error);
	error = couchstore_db_info(db1, dbinfo1);
	EXPECT_EQ(0,error);
	EXPECT_EQ(5000,dbinfo1->deleted_count);

}

TEST_F(CompactionTest, Expired1MItemPurgeTest){
	char autocompact[] = "curl -X POST -u Administrator:password -d autoCompactionDefined=false " 
		"-d parallelDBAndViewCompaction=false  "
		"http://tambran-s21705.sc.couchbase.com:8091/controller/setAutoCompaction";
	exec(autocompact);   
	sendHello();
	couchstore_error_t error; 
	int numitems = 1000000;
	int vbucket = 20;
	insert_items_vbucket(vbucket, numitems);  
	sleep(30);
	//structs to hold vB file information
	Db* db = (Db*)malloc(sizeof(Db));
	DbInfo* dbinfo = (DbInfo*)malloc(sizeof(DbInfo));

	//parameters for LCB_COMPACT command
	uint16_t vbid = 20;
	uint64_t purge_before_ts = 0;  
	uint64_t purge_before_seq =0;
	uint8_t drop_deletes = 0;

	//file sizes before and after compaction
	uint64_t fsize_b4compact =0;
	uint64_t fsize_a4trcompact =0;
	uint64_t newRevNum = 1;
	std::string filename = "/opt/couchbase/var/lib/couchbase/data/default/20.couch.1";
	newRevNum = checkNewRevNum(filename);
	std::cout<<"current filename: "<<filename<<"\n";
	//open vB file
	error = couchstore_open_db(filename.c_str(), COUCHSTORE_OPEN_FLAG_RDONLY, &db);
	EXPECT_EQ(0,error);
	//get vB info 
	error = couchstore_db_info(db, dbinfo);
	print_dbinfo(dbinfo);
	fsize_b4compact = dbinfo->file_size;

	//no documents should be deleted prior to compaction
	EXPECT_EQ(0,dbinfo->deleted_count);

	//close db handles
	couchstore_close_db(db);
	//send lcb_compact command
	//using cbcompact tool as lcb 2.3.0 has a bug here
	// sendcompact(vbid, purge_before_ts, purge_before_seq, drop_deletes);  
	//lcb_wait(instance);
	char compact[] = "/opt/couchbase/bin/cbcompact localhost:11210 compact 20";
	exec(compact);
	sleep(120);
	//open vB file with new rev number 
	newRevNum = checkNewRevNum(filename);
	std::cout<<"new filename after compaction: "<<filename<<"\n";
	error = couchstore_open_db(filename.c_str(), COUCHSTORE_OPEN_FLAG_RDONLY, &db);
	EXPECT_EQ(0,error);
	error = couchstore_db_info(db, dbinfo);
	EXPECT_EQ(0,error);
	print_dbinfo(dbinfo);
	fsize_a4trcompact = dbinfo->file_size;
	//11 documents should be deleted post compaction
	EXPECT_EQ(1000000,dbinfo->deleted_count);

}

TEST_F(CompactionTest, DropDeletesTrue1MStoneTest) {

	char autocompact[] = "curl -X POST -u Administrator:password -d  "
		"autoCompactionDefined=false -d parallelDBAndViewCompaction=false  "
		"http://tambran-s21705.sc.couchbase.com:8091/controller/setAutoCompaction";
	exec(autocompact);   
	sendHello(); 
	int numitems = 1000000;
	int vbucket = 14;
	insert_items_vbucket(vbucket, numitems);  
	sleep(30);
	//set compact_cmd parameters 
	uint16_t vbid = 14;
	uint32_t exptime = 0;
	uint64_t purge_before_ts = exptime;  
	uint64_t purge_before_seq =1;
	uint8_t drop_deletes = 1;
	//run compaction once to mark expired items as deleted 
	char compact[] = "/opt/couchbase/bin/cbcompact localhost:11210 compact 14";
	exec(compact);
	sleep(240);

	couchstore_error_t error; 
	Db* db1 = (Db*)malloc(sizeof(Db));
	Db* db2 = (Db*)malloc(sizeof(Db));
	DbInfo* dbinfo1 = (DbInfo*)malloc(sizeof(DbInfo));
	DbInfo* dbinfo2 = (DbInfo*)malloc(sizeof(DbInfo));
	DocInfo* docinfo1 = (DocInfo*)malloc(sizeof(DocInfo));
	DocInfo* docinfo2 = (DocInfo*)malloc(sizeof(DocInfo));
	//get the expiry time of a doc
	const char* key = "fooaaa"; 
	uint64_t fsize_b4compact =0;
	uint64_t fsize_a4trcompact =0;
	const char *filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.2"; 

	error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db1);
	EXPECT_EQ(0,error);

	error= couchstore_db_info(db1, dbinfo1);
	EXPECT_EQ(0,error);
	print_dbinfo(dbinfo1);
	EXPECT_EQ(1000000,dbinfo1->deleted_count);
	couchstore_close_db(db1);

	//send another compaction to drop the tombstones
	//sendcompact(vbid, purge_before_ts, purge_before_seq, drop_deletes);  
	//lcb_wait(instance);
	char compact2[] = "/opt/couchbase/bin/cbcompact localhost:11210 compact 14  "
		"--purge-before=0 --purge-only-upto-seq=0 --dropdeletes";
	exec(compact2);

	filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.3"; 
	error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db2);
	EXPECT_EQ(0,error);
	error = couchstore_db_info(db2, dbinfo2);
	EXPECT_EQ(0,error);
	print_dbinfo(dbinfo2);
	EXPECT_EQ(0,dbinfo2->deleted_count);

}

//This test needs to be migrated to Warmup Tests
TEST_F(CompactionTest, ExpireAtWarmupTest) {
	sendHello();
	std::stringstream ss;
	ss << "fooaaa";
	std::string myString = ss.str();
	storekey(myString);
	storekey(myString);
	storekey(myString);
	fprintf(stderr, "\n3 mutation on vbucket 14\n"); 
        char restart[] = "sudo /etc/init.d/couchbase-server restart";
        exec(restart);
        sleep(40);
	couchstore_error_t error; 
	//structs to hold vB file information
	Db* db = (Db*)malloc(sizeof(Db));
	DbInfo* dbinfo = (DbInfo*)malloc(sizeof(DbInfo));
	DocInfo* docinfo1 = (DocInfo*)malloc(sizeof(DocInfo));

	const char *filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.1";
	//open vB file
	error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db);
	EXPECT_EQ(0,error);
	//get vB info 
	error = couchstore_db_info(db, dbinfo);
	EXPECT_EQ(0,error);
	print_dbinfo(dbinfo);
	//close db handles
	EXPECT_EQ(0,dbinfo->deleted_count);
	error = couchstore_docinfo_by_id(db,(void*)myString.c_str(),myString.size(),&docinfo1);
	EXPECT_EQ(0,error);
        print_docinfo(docinfo1);
        couchstore_close_db(db);

}

//This test needs to be migrated to Warmup Tests
TEST_F(CompactionTest, CheckpointWarmupTest) {
	sendHello();
        insertItems(100000);
        char stop[] = "sudo /etc/init.d/couchbase-server stop";
        exec(stop);
        sleep(30);
        std::vector<int> vbchkvec = getvbcheckpoints();
        char start[] = "sudo /etc/init.d/couchbase-server start";
        exec(start);
        sleep(30);
        (void)lcb_set_stat_callback(instance, stats_generic_callback);
        err = lcb_connect(instance);
        assert(err==LCB_SUCCESS);
        std::string checkpoint("checkpoint");
        StatsVector checkpointstats = getgenericstats(checkpoint,instance);
        std::cout<<"vector length : "<<checkpointstats.size()<<"\n";
        std::string base_str = "/opt/couchbase/var/lib/couchbase/data/default/";
        std::string append = ".couch.1";
         
        for(StatsVector::iterator itr = checkpointstats.begin(); itr!=checkpointstats.end();++itr){
            if(!itr->first.compare("vb_999:persisted_checkpoint_id"))
                std::cout<<"vb_999:persisted_checkpoint_id "<<itr->second<<"\n"; 
        }
}


int main(int argc, char **argv) {
	::testing::InitGoogleTest(&argc, argv);
	testargv = argv;
	testargc = argc;
	return RUN_ALL_TESTS();

}
