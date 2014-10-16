//getNumCPU method code taken from /ep-engine/src/executorpool.cc in ep-engine source code
#include "BaseTest.cc"

extern struct stats_generic genericstats;

static const size_t EP_MIN_NUM_IO_THREADS = 4;
class PriorityTest : public BaseTest {
    protected:
   
    void createlcbinstance(lcb_t *instance, char* host, char* user, char* passwd, int& numargs){
        
        memset(&create_options, 0, sizeof(create_options));
        if (numargs > 0) {
            create_options.v.v0.host = host;
        }
        if (numargs > 1) {
            create_options.v.v0.user = user;
            create_options.v.v0.bucket = user;
        }
        if (numargs > 2) {
            create_options.v.v0.passwd = passwd;
        }
        err = lcb_create(instance, &create_options);
        assert(err==LCB_SUCCESS);
        (void)lcb_set_error_callback(*instance, error_callback);
        fprintf(stderr, "\nconnect succeeded\n");
        (void)lcb_set_get_callback(*instance, get_callback);
        (void)lcb_set_store_callback(*instance, store_callback);
    
   }

    virtual void SetUp(){    

        createBucket(true);
        createlcbinstance(&instance, testargv[1], testargv[2],testargv[3], testargc); 

    }
    
    void deleteBucket(std::string bucketname){
    
        std::string del = "/opt/couchbase/bin/couchbase-cli bucket-delete -c 127.0.0.1:8091 --bucket="
                          + bucketname + " -u Administrator -p password";
        exec((char*)del.c_str());
        sleep(60);
    }
    
    void createBucketwithPriority(bool highprio, std::string bucketname){
        if(!highprio){
            std::string create = "/opt/couchbase/bin/couchbase-cli bucket-create -c 127.0.0.1:8091 --bucket="
                                 + bucketname + " --bucket-type=couchbase --bucket-ramsize=200 --bucket-replica=1"+
                                 " --bucket-priority=low -u Administrator -p password";
            exec((char*)create.c_str());
        }
        else{           
            std::string create = "/opt/couchbase/bin/couchbase-cli bucket-create -c 127.0.0.1:8091 --bucket="
                                 + bucketname + " --bucket-type=couchbase --bucket-ramsize=200 --bucket-replica=1"+
                                 " --bucket-priority=high -u Administrator -p password";
            exec((char*)create.c_str());
        }
        sleep(60);
    }
    
    void verifywithintolerance(int num1, int num2){
        if(num1>num2){
          int percdiff = (int)(((num1-num2)*100)/num1); 
          EXPECT_LT(percdiff,10);
          std::cout<<"percdiff: "<<percdiff<<"\n";
        }
        else{
          int percdiff = (int)(((num2-num1)*100)/num2); 
          EXPECT_LT(percdiff,10);
          std::cout<<"percdiff: "<<percdiff<<"\n";
        }
    }
 
    size_t getNumCPU(void) {
        
        size_t numCPU;
    #ifdef WIN32
        SYSTEM_INFO sysinfo;
        GetSystemInfo(&sysinfo);
        numCPU = (size_t)sysinfo.dwNumberOfProcessors;
    #else
        numCPU = (size_t)sysconf(_SC_NPROCESSORS_ONLN);
    #endif
  
        return (numCPU < 256) ? numCPU : 0;
    }
    
    size_t getnumThreads(){
        
        size_t numCPU = getNumCPU();
        size_t numThreads = (size_t)((numCPU * 3)/4);
        numThreads = (numThreads < EP_MIN_NUM_IO_THREADS) ? EP_MIN_NUM_IO_THREADS : numThreads;
        return numThreads;
    }
    
    size_t getNumNonIO(){

         size_t numThreads = getnumThreads();  
         size_t count = numThreads / 10;
         if(count==0) return count+1;
         else if(numThreads%10==0) return count;
         else return count+1;
    }
    
    size_t getNumAuxIO(){

         size_t numThreads = getnumThreads();  
         size_t count = numThreads / 10;
         if(count==0) return count+1;
         else if(numThreads%10==0) return count;
         else return count+1;
    }
    
    size_t getNumWriters(){

         size_t numThreads = getnumThreads();  
         size_t count = numThreads - getNumAuxIO() - getNumNonIO();
         count = count/2;
         if(count==0) return count+1;
         else return count;
    }
    
    size_t getNumReaders(){

         size_t numThreads = getnumThreads();  
         size_t count = numThreads - getNumAuxIO() - getNumNonIO() - getNumWriters();
         return count;
    }
};

TEST_F(PriorityTest, CreateLowBucketPriorityTest) {
    (void)lcb_set_stat_callback(instance, stats_generic_callback);
    bool highprio = true;
    deleteBucket("default");
    createBucketwithPriority(!highprio, "default");
    err = lcb_connect(instance);
    assert(err==LCB_SUCCESS);
    std::string all =  "";
    StatsVector defaultstats = getgenericstats(all,instance);
    std::cout<<"vector length : "<<defaultstats.size()<<"\n";
    EXPECT_GT(defaultstats.size(),0);
    for(StatsVector::iterator itr = defaultstats.begin(); itr!=defaultstats.end();++itr){
        if(itr->first.compare("ep_bucket_priority")==0){
            EXPECT_TRUE(!itr->second.compare("LOW"));
            std::cout<<itr->second<<"\n";
        }
   }
}

TEST_F(PriorityTest, CreateHighBucketPriorityTest) {
    (void)lcb_set_stat_callback(instance, stats_generic_callback);
    bool highprio = true;
    deleteBucket("default");
    createBucketwithPriority(highprio,"default");
    err = lcb_connect(instance);
    assert(err==LCB_SUCCESS);
    std::string all = "";
    StatsVector defaultstats = getgenericstats(all,instance);
    std::cout<<"vector length : "<<defaultstats.size()<<"\n";
    EXPECT_GT(defaultstats.size(),0);
    for(StatsVector::iterator itr = defaultstats.begin(); itr!=defaultstats.end();++itr){
        if(itr->first.compare("ep_bucket_priority")==0){
            EXPECT_TRUE(!itr->second.compare("HIGH"));
            std::cout<<itr->second<<"\n";
        }
    }
}

TEST_F(PriorityTest, TotalSpawnedThreadsTest) {
    (void)lcb_set_stat_callback(instance, stats_generic_callback);
    err = lcb_connect(instance);
    assert(err==LCB_SUCCESS);
    std::string workload("workload");
    StatsVector workloadstats = getgenericstats(workload,instance);
    std::cout<<"vector length : "<<workloadstats.size()<<"\n";
    size_t numThreads =0;
    for(StatsVector::iterator itr = workloadstats.begin(); itr!=workloadstats.end();++itr){
        if(!itr->first.compare("ep_workload:num_auxio")||!itr->first.compare("ep_workload:num_nonio")
           ||!itr->first.compare("ep_workload:num_readers")||!itr->first.compare("ep_workload:num_writers")){
               numThreads = numThreads+atoi(itr->second.c_str()); 
        }
    }
    std::cout<<"calculated total num of threads: "<<numThreads<<"\n";
    size_t expectednumThreads = getnumThreads();
    EXPECT_EQ(expectednumThreads,numThreads); 
}

TEST_F(PriorityTest, NumAuxIOThreadsTest) {
    (void)lcb_set_stat_callback(instance, stats_generic_callback);
    err = lcb_connect(instance);
    assert(err==LCB_SUCCESS);
    std::string workload("workload");
    StatsVector workloadstats = getgenericstats(workload,instance);
    std::cout<<"vector length : "<<workloadstats.size()<<"\n";
    size_t numAuxIOThreads =0;
    for(StatsVector::iterator itr = workloadstats.begin(); itr!=workloadstats.end();++itr){
        if(!itr->first.compare("ep_workload:num_auxio")){
               numAuxIOThreads = atoi(itr->second.c_str()); 
        }
    }
    size_t expectednumAuxIOThreads = getNumAuxIO();
    std::cout<<"calculated num of AuxIO threads: "<<expectednumAuxIOThreads<<"\n";
    EXPECT_EQ(expectednumAuxIOThreads,numAuxIOThreads); 
}


TEST_F(PriorityTest, NumnonIOThreadsTest) {
    (void)lcb_set_stat_callback(instance, stats_generic_callback);
    err = lcb_connect(instance);
    assert(err==LCB_SUCCESS);
    std::string workload("workload");
    StatsVector workloadstats = getgenericstats(workload,instance);
    std::cout<<"vector length : "<<workloadstats.size()<<"\n";
    size_t numNonIOThreads =0;
    for(StatsVector::iterator itr = workloadstats.begin(); itr!=workloadstats.end();++itr){
        if(!itr->first.compare("ep_workload:num_nonio")){
               numNonIOThreads = atoi(itr->second.c_str()); 
        }
    }
    size_t expectednumNonIOThreads = getNumNonIO();
    std::cout<<"calculated total num of nonio threads: "<<expectednumNonIOThreads<<"\n";
    EXPECT_EQ(expectednumNonIOThreads,numNonIOThreads); 
}

TEST_F(PriorityTest, NumWriterThreadsTest) {
    (void)lcb_set_stat_callback(instance, stats_generic_callback);
    err = lcb_connect(instance);
    assert(err==LCB_SUCCESS);
    std::string workload("workload");
    StatsVector workloadstats = getgenericstats(workload,instance);
    std::cout<<"vector length : "<<workloadstats.size()<<"\n";
    size_t numWriterThreads =0;
    for(StatsVector::iterator itr = workloadstats.begin(); itr!=workloadstats.end();++itr){
        if(!itr->first.compare("ep_workload:num_writers")){
               numWriterThreads = atoi(itr->second.c_str()); 
        }
    }
    size_t expectednumWriterThreads = getNumWriters();
    std::cout<<"calculated num of writer threads: "<<expectednumWriterThreads<<"\n";
    EXPECT_EQ(expectednumWriterThreads,numWriterThreads); 
}

TEST_F(PriorityTest, NumReaderThreadsTest) {
    (void)lcb_set_stat_callback(instance, stats_generic_callback);
    err = lcb_connect(instance);
    assert(err==LCB_SUCCESS);
    std::string workload("workload");
    StatsVector workloadstats = getgenericstats(workload,instance);
    std::cout<<"vector length : "<<workloadstats.size()<<"\n";
    size_t numReaderThreads =0;
    for(StatsVector::iterator itr = workloadstats.begin(); itr!=workloadstats.end();++itr){
        if(!itr->first.compare("ep_workload:num_readers")){
               numReaderThreads = atoi(itr->second.c_str()); 
        }
    }
    size_t expectednumReaderThreads = getNumReaders();
    std::cout<<"calculated num of reader threads: "<<expectednumReaderThreads<<"\n";
    EXPECT_EQ(expectednumReaderThreads,numReaderThreads); 
}

TEST_F(PriorityTest, HighLowStorageTest) {
    (void)lcb_set_stat_callback(instance, stats_generic_callback);
    err = lcb_connect(instance);
    assert(err==LCB_SUCCESS);
    bool highprio = true;
    createBucketwithPriority(highprio,"highprio");
    lcb_t inst_second;       //second lcb instance to connect to second bucket
    int numargs = 2;
    const char* bucketname = "highprio";
    //create connection to bucket highprio
    createlcbinstance(&inst_second,testargv[1],(char*)bucketname,NULL,numargs); 
    (void)lcb_set_stat_callback(inst_second, stats_generic_callback);
    err = lcb_connect(inst_second);
    assert(err==LCB_SUCCESS);
    std::vector<lcb_t*> instancevec;
    instancevec.push_back(&instance);
    instancevec.push_back(&inst_second);
    insert_items_instances(100000,instancevec);
    std::string timings("timings");
    StatsVector timingstats = getgenericstats(timings,inst_second);
    std::cout<<"vector length : "<<timingstats.size()<<"\n";
    int highstoragebin1 = 0;
    int highstoragebin2 = 0;
    for(StatsVector::iterator itr = timingstats.begin(); itr!=timingstats.end();++itr){
        if(!itr->first.compare("storage_age_0,1000000")){
               std::cout<<"storage_age_0,1000000 : "<<itr->second<<"\n";
               highstoragebin1 = atoi(itr->second.c_str());
        }
        else if(!itr->first.compare("storage_age_1000000,2400000")){ 
               std::cout<<"storage_age_1000000,2400000 : "<<itr->second<<"\n";
               highstoragebin2 = atoi(itr->second.c_str());
        }
    }
    genericstats.refcount = 0;
    genericstats.statsvec.clear();
    int lowstoragebin1 = 0;
    int lowstoragebin2 = 0;
    StatsVector timingstatsbucket2 = getgenericstats(timings,instance);
    std::cout<<"vector length : "<<timingstatsbucket2.size()<<"\n";
    for(StatsVector::iterator itr = timingstatsbucket2.begin(); itr!=timingstatsbucket2.end();++itr){
        if(!itr->first.compare("storage_age_0,1000000")){
               std::cout<<"storage_age_0,1000000 : "<<itr->second<<"\n";
               lowstoragebin1 = atoi(itr->second.c_str());
        }
        else if(!itr->first.compare("storage_age_1000000,2400000")){ 
               lowstoragebin2 = atoi(itr->second.c_str());
               std::cout<<"storage_age_1000000,2400000 : "<<itr->second<<"\n";
        }
    }
    EXPECT_GT(highstoragebin1,lowstoragebin1);
    EXPECT_GT(highstoragebin2,lowstoragebin2);
}

TEST_F(PriorityTest, SingleBucketStorageAgeTest) {
    (void)lcb_set_stat_callback(instance, stats_generic_callback);
    err = lcb_connect(instance);
    assert(err==LCB_SUCCESS);
    std::vector<lcb_t*> instancevec;
    instancevec.push_back(&instance);
    insert_items_instances(100000,instancevec);
    std::string timings("timings");
    StatsVector timingstats = getgenericstats(timings,instance);
    std::cout<<"vector length : "<<timingstats.size()<<"\n";
    int lowstoragebin1 = 0;
    int lowstoragebin2 = 0;
    for(StatsVector::iterator itr = timingstats.begin(); itr!=timingstats.end();++itr){
        if(!itr->first.compare("storage_age_0,1000000")){
               std::cout<<"storage_age_0,1000000 : "<<itr->second<<"\n";
               lowstoragebin1 = atoi(itr->second.c_str());
        }
        else if(!itr->first.compare("storage_age_1000000,2400000")){ 
               std::cout<<"storage_age_1000000,2400000 : "<<itr->second<<"\n";
               lowstoragebin2 = atoi(itr->second.c_str());
        }
    }
    genericstats.refcount = 0;
    genericstats.statsvec.clear();
    deleteBucket("default");
    bool highprio = true;
    createBucketwithPriority(highprio,"highprio");
    lcb_t inst_second;       //second lcb instance to connect to second bucket
    int numargs = 2;
    const char* bucketname = "highprio";
    //create connection to bucket highprio
    createlcbinstance(&inst_second,testargv[1],(char*)bucketname,NULL,numargs); 
    (void)lcb_set_stat_callback(inst_second, stats_generic_callback);
    err = lcb_connect(inst_second);
    assert(err==LCB_SUCCESS);
    instancevec.clear();
    instancevec.push_back(&inst_second);
    insert_items_instances(100000,instancevec);
    StatsVector timingstatsbucket2 = getgenericstats(timings,inst_second);
    std::cout<<"vector length : "<<timingstatsbucket2.size()<<"\n";
    int highstoragebin1 = 0;
    int highstoragebin2 = 0;
    for(StatsVector::iterator itr = timingstatsbucket2.begin(); itr!=timingstatsbucket2.end();++itr){
        if(!itr->first.compare("storage_age_0,1000000")){
               std::cout<<"storage_age_0,1000000 : "<<itr->second<<"\n";
               highstoragebin1 = atoi(itr->second.c_str());
        }
        else if(!itr->first.compare("storage_age_1000000,2400000")){ 
               highstoragebin2 = atoi(itr->second.c_str());
               std::cout<<"storage_age_1000000,2400000 : "<<itr->second<<"\n";
        }
    }
    verifywithintolerance(highstoragebin1,lowstoragebin1);
    verifywithintolerance(highstoragebin2,lowstoragebin2);
}

int main(int argc, char *argv[]){
    ::testing::InitGoogleTest(&argc, argv);
    testargv = argv;
    testargc = argc;
    return RUN_ALL_TESTS();
}
