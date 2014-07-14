#include "BaseTest.cc"

extern char* wm_val_cn;
extern char* rep_val_cn;
extern char* ejected_val_cn;
extern char* resident_val_cn;
extern bool gotreplica_curr_items;
extern bool goteject_num_items;
extern bool got_active_resident;

class WarmupTest : public BaseTest {

protected:

    void TearDown(){
        lcb_destroy(instance);
        resetboolFlags();
    }

    bool verifyWarmup(uint64_t expectedItems){
        while(!warmupdone || !gotwm_val_cn){
            lcb_server_stats_cmd_t stats;
            stats.version = 0;
            stats.v.v0.name = "warmup";
            stats.v.v0.nname = 6;
            const lcb_server_stats_cmd_t *commandstat[1];
            commandstat[0] = &stats;
            lcb_server_stats(instance,NULL,1, &commandstat[0]);
            lcb_wait(instance);
        }

        int wm_val_cn_i = atoi(wm_val_cn);
        fprintf(stderr, "\nnumber of warmed up elements:  %d \n",wm_val_cn_i);
        fprintf(stderr, "\nnumber of expected elements:  %lu \n",expectedItems);
        int resident_ratio = getActiveResidentRatio();
        int ejected_itm_cnt = getEjectedNumItems();
        fprintf(stderr, "\nActiveResidentRatio :  %d \n",resident_ratio);
        if(resident_ratio==100){
            return (wm_val_cn_i == expectedItems);
        }
        else return (((wm_val_cn_i-ejected_itm_cnt)*100)/expectedItems==resident_ratio);
    }

    int getReplicaCount(){
        while(!gotreplica_curr_items){
            lcb_server_stats_cmd_t stats;
            stats.version = 0;
            stats.v.v0.name = ""; //"all";
            stats.v.v0.nname = 0;
            const lcb_server_stats_cmd_t *commandstat[1];
            commandstat[0] = &stats;
            lcb_server_stats(instance,NULL,1,&commandstat[0]);
            lcb_wait(instance);
        }

        int replicacount = atoi(rep_val_cn);
        return replicacount;
    }

    int getActiveResidentRatio(){

        while(!got_active_resident){
            lcb_server_stats_cmd_t stats;
            stats.version = 0;
            stats.v.v0.name = ""; //"all";
            stats.v.v0.nname = 0;
            const lcb_server_stats_cmd_t *commandstat[1];
            commandstat[0] = &stats;
            lcb_server_stats(instance,NULL,1,&commandstat[0]);
            lcb_wait(instance);
        }
        return atoi(resident_val_cn);
    }

    int getEjectedNumItems(){

        while(!goteject_num_items){
            lcb_server_stats_cmd_t stats;
            stats.version = 0;
            stats.v.v0.name = ""; //"all";
            stats.v.v0.nname = 0;
            const lcb_server_stats_cmd_t *commandstat[1];
            commandstat[0] = &stats;
            lcb_server_stats(instance,NULL,1,&commandstat[0]);
            lcb_wait(instance);
        }
        return atoi(ejected_val_cn);
    }
    //reset all the boolean flags initialized in dataclient.c, BaseTest.cc and in current file
    void resetboolFlags(){
        gotwm_val_cn = false;
        warmupdone = false;
        gotreplica_curr_items = false;
        got_active_resident = false;
        goteject_num_items = false;
        threadstate.clear();
    }

    uint64_t numInserted;
};

TEST_F(WarmupTest, WarmupActive_100DGM_ValueEvic_Test) {
    bool fullEvict = true;
    createBucket(!fullEvict);
    uint64_t numItems = 100000;
    numInserted = insertItems(numItems);
    sleep(10);
    fprintf(stderr, "\nnumber of inserted elements:  %lu \n",numInserted);
    //restart server for setting warmup
    char restart2[] = "sudo /etc/init.d/couchbase-server restart";
    exec(restart2);
    sleep(40);
    EXPECT_TRUE(verifyWarmup(numInserted));
    char delBucket[] = "/root/ep-engine-tests/delBucket";
    exec(delBucket);
    sleep(40);
}

TEST_F(WarmupTest, WarmupActive_100DGM_FullEvic_Test) {
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
    char delBucket[] = "/root/ep-engine-tests/delBucket";
    exec(delBucket);
    sleep(40);
}

TEST_F(WarmupTest, WarmupActive_64DGM_ValueEvic_Test) {
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
    char delBucket[] = "/root/ep-engine-tests/delBucket";
    exec(delBucket);
    sleep(40);
}

TEST_F(WarmupTest, WarmupActive_74DGM_FullEvic_Test) {
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
    char delBucket[] = "/root/ep-engine-tests/delBucket";
    exec(delBucket);
    sleep(40);
}

TEST_F(WarmupTest, WarmupActive_30DGM_ValueEvic_Test) {
    bool fullEvict = true;
    createBucket(!fullEvict);
    uint64_t numItems = 1200000;
    numInserted = insertItems(numItems);
    sleep(10);
    //restart server for setting warmup
    char restart2[] = "sudo /etc/init.d/couchbase-server restart";
    exec(restart2);
    sleep(40);
    EXPECT_TRUE(verifyWarmup(numInserted));
    char delBucket[] = "/root/ep-engine-tests/delBucket";
    exec(delBucket);
    sleep(40);
}


TEST_F(WarmupTest, WarmupActive_55DGM_FullEvic_Test) {
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
    char delBucket[] = "/root/ep-engine-tests/delBucket";
    exec(delBucket);
    sleep(40);
}

TEST_F(WarmupTest, WarmupActive_10DGM_ValueEvic_Test) {
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
    char delBucket[] = "/root/ep-engine-tests/delBucket";
    //exec(delBucket);
    sleep(40);
}

TEST_F(WarmupTest, WarmupActive_30DGM_FullEvic_Test) {
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
    char delBucket[] = "/root/ep-engine-tests/delBucket";
    exec(delBucket);
    sleep(40);
}

TEST_F(WarmupTest, WarmupActive_4DGM_ValueEvic_Test) {
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
    char delBucket[] = "/root/ep-engine-tests/delBucket";
    exec(delBucket);
    sleep(40);
}

TEST_F(WarmupTest, WarmupActive_18DGM_FullEvic_Test) {
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
    char delBucket[] = "/root/ep-engine-tests/delBucket";
    exec(delBucket);
    sleep(40);
}


TEST_F(WarmupTest, WarmupReplica_100DGM_ValueEvic_Test) {
    char addNode[] = "/root/ep-engine-tests/addNode";
    exec(addNode);
    sleep(40);
    bool fullEvict = true;
    createBucket(!fullEvict);
    uint64_t numItems = 100000;
    numInserted = insertItems(numItems);
    sleep(10);
    int repcount = getReplicaCount();
    fprintf(stderr, "\nnumber of replica  elements:  %d \n",repcount);
    //restart server for setting warmup
    char restart2[] = "sudo /etc/init.d/couchbase-server restart";
    exec(restart2);
    sleep(40);
    int warmuprepcount = getReplicaCount();
    fprintf(stderr, "\nnumber of replica  elements after restart:  %d \n",warmuprepcount);
    EXPECT_EQ(warmuprepcount,repcount);
    char removeNode[] = "/root/ep-engine-tests/removeNode";
    exec(removeNode);
}

TEST_F(WarmupTest, WarmupReplica_100DGM_FullEvic_Test) {
    char addNode[] = "/root/ep-engine-tests/addNode";
    exec(addNode);
    sleep(40);
    bool fullEvict = true;
    createBucket(fullEvict);
    uint64_t numItems = 100000;
    numInserted = insertItems(numItems);
    sleep(10);
    int repcount = getReplicaCount();
    fprintf(stderr, "\nnumber of replica  elements:  %d \n",repcount);
    //restart server for setting warmup
    char restart2[] = "sudo /etc/init.d/couchbase-server restart";
    exec(restart2);
    sleep(40);
    int warmuprepcount = getReplicaCount();
    fprintf(stderr, "\nnumber of warmed up replica  elements:  %d \n",warmuprepcount);
    EXPECT_EQ(warmuprepcount,repcount);
    char removeNode[] = "/root/ep-engine-tests/removeNode";
    exec(removeNode);
}

TEST_F(WarmupTest, WarmupReplica_20DGM_ValueEvic_Test) {
    char addNode[] = "/root/ep-engine-tests/addNode";
    exec(addNode);
    sleep(40);
    bool fullEvict = true;
    createBucket(!fullEvict);
    uint64_t numItems = 1500000;
    numInserted = insertItems(numItems);
    sleep(10);
    int repcount = getReplicaCount();
    fprintf(stderr, "\nnumber of replica  elements:  %d \n",repcount);
    //restart server for setting warmup
    char restart2[] = "sudo /etc/init.d/couchbase-server restart";
    exec(restart2);
    sleep(40);
    int warmuprepcount = getReplicaCount();
    fprintf(stderr, "\nnumber of replica  elements after restart:  %d \n",warmuprepcount);
    EXPECT_EQ(warmuprepcount,repcount);
    char removeNode[] = "/root/ep-engine-tests/removeNode";
    exec(removeNode);
}

TEST_F(WarmupTest, WarmupReplica_20DGM_FullEvic_Test) {
    char addNode[] = "/root/ep-engine-tests/addNode";
    exec(addNode);
    sleep(40);
    bool fullEvict = true;
    createBucket(fullEvict);
    uint64_t numItems = 1500000;
    numInserted = insertItems(numItems);
    sleep(10);
    int repcount = getReplicaCount();
    fprintf(stderr, "\nnumber of replica  elements:  %d \n",repcount);
    //restart server for setting warmup
    char restart2[] = "sudo /etc/init.d/couchbase-server restart";
    exec(restart2);
    sleep(40);
    int warmuprepcount = getReplicaCount();
    fprintf(stderr, "\nnumber of warmed up replica  elements:  %d \n",warmuprepcount);
    EXPECT_EQ(warmuprepcount,repcount);
    char removeNode[] = "/root/ep-engine-tests/removeNode";
    exec(removeNode);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    testargv = argv;
    testargc = argc;
    return RUN_ALL_TESTS();
}
