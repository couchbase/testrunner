#include "BaseTest.cc"

class DatatypeTest : public BaseTest {
};

TEST_F(DatatypeTest, rawData) {
  sendHello();
  const char inflated[] = "abc123";
  size_t inflated_len = strlen(inflated);
  lcb_store_cmd_t cmd;
  const lcb_store_cmd_t *commands[1];
  commands[0] = &cmd;
  memset(&cmd, 0, sizeof(cmd));
  cmd.v.v0.operation = LCB_SET;
  cmd.v.v0.datatype = LCB_BINARY_RAW_BYTES;
  cmd.v.v0.key = "fooaaa";
  cmd.v.v0.nkey = 6;
 // cmd.v.v0.cas = 527601;
  cmd.v.v0.bytes = inflated;
  cmd.v.v0.nbytes = inflated_len;
  cmd.v.v0.exptime = 0x2000; 
  DatatypeTester(commands[0]);
}

TEST_F(DatatypeTest, callGet) {
  sendHello();
  lcb_wait(instance);
  callget(&instance, "fooJSON",7);
  lcb_wait(instance);
  fprintf(stderr, "\ndatatype for key fooJSON is %d\n",*getvaluedtype); 
}

TEST_F(DatatypeTest, raw20M) {
  sendHello();
  tmo = 350000000; 
  lcb_cntl(instance, LCB_CNTL_SET, LCB_CNTL_OP_TIMEOUT, &tmo);
  FILE *file20MB;
  char *membuffer;
  long numbytes;
  file20MB = fopen("/root/Datatypetests/out20M.dat","r");
  fseek(file20MB,0L,SEEK_END);
  numbytes=ftell(file20MB);
  fseek(file20MB,0L,SEEK_SET);
  membuffer = (char*)calloc(numbytes,sizeof(char));
  if(membuffer==NULL) exit(EXIT_FAILURE);
  fread(membuffer, sizeof(char),numbytes,file20MB);
  fclose(file20MB); 
  lcb_store_cmd_t cmd;
  const lcb_store_cmd_t *commands[1];
  commands[0] = &cmd;
  memset(&cmd, 0, sizeof(cmd));
  cmd.v.v0.operation = LCB_SET;
  cmd.v.v0.datatype = LCB_BINARY_RAW_BYTES; //DATATYPE_COMPRESSED;
  cmd.v.v0.key = "foo20M";
  cmd.v.v0.nkey = 6;
  cmd.v.v0.bytes = membuffer;
  cmd.v.v0.nbytes = numbytes; 
  DatatypeTester(commands[0]);
}

TEST_F(DatatypeTest, rawCompressedData) {
  sendHello();
  const char inflated[] = "gggggggggghhhhhhhhhhhh";
  size_t inflated_len = strlen(inflated);
  char deflated[256];
  size_t deflated_len = 256;
  snappy_status status;
  status = snappy_compress(inflated, inflated_len,
                            deflated, &deflated_len);
  EXPECT_EQ(0,status);
  lcb_store_cmd_t cmd;
  const lcb_store_cmd_t *commands[1];
  commands[0] = &cmd;
  memset(&cmd, 0, sizeof(cmd));
  cmd.v.v0.operation = LCB_SET;
  cmd.v.v0.datatype = LCB_BINARY_DATATYPE_COMPRESSED;
  cmd.v.v0.key = "fooccc";
  cmd.v.v0.nkey = 6;
  cmd.v.v0.bytes = deflated;
  cmd.v.v0.nbytes = deflated_len; 
  DatatypeTester(commands[0]);
}

TEST_F(DatatypeTest, jsonData) {
  sendHello();
  FILE *fileJSON;
  char *membuffer;
  long numbytes;
  fileJSON = fopen("/root/Datatypetests/buzz.json","r");
  fseek(fileJSON,0L,SEEK_END);
  numbytes=ftell(fileJSON);
  fseek(fileJSON,0L,SEEK_SET);
  membuffer = (char*)calloc(numbytes,sizeof(char));
  if(membuffer==NULL) exit(EXIT_FAILURE);
  fread(membuffer, sizeof(char),numbytes,fileJSON);
  fclose(fileJSON); 
  //command to store
  lcb_store_cmd_t cmd;
  commands[0] = &cmd;
  memset(&cmd, 0, sizeof(cmd));
  cmd.v.v0.operation = LCB_SET;
  cmd.v.v0.datatype = LCB_BINARY_DATATYPE_JSON;
  cmd.v.v0.key = "fooJSON";
  cmd.v.v0.nkey = 7;
  cmd.v.v0.bytes = membuffer;
  cmd.v.v0.nbytes = numbytes;
  DatatypeTester(commands[0]);
}

TEST_F(DatatypeTest, jsonCompressedData) {
  sendHello();
  FILE *fileJSON;
  char *membuffer;
  long numbytes;
  char* deflated;
  size_t deflated_len = 2560;
  snappy_status status;
  fileJSON = fopen("/root/Datatypetests/buzz.json","r");
  fseek(fileJSON,0L,SEEK_END);
  numbytes=ftell(fileJSON);
  fseek(fileJSON,0L,SEEK_SET);
  membuffer = (char*)calloc(numbytes,sizeof(char));
  if(membuffer==NULL) exit(EXIT_FAILURE);
  fread(membuffer, sizeof(char),numbytes,fileJSON);
  fclose(fileJSON);
  deflated = (char*)calloc(numbytes,sizeof(char));
  status = snappy_compress(membuffer, numbytes,
                            deflated, &deflated_len);
  //command to store
  lcb_store_cmd_t cmd;
  commands[0] = &cmd;
  memset(&cmd, 0, sizeof(cmd));
  cmd.v.v0.operation = LCB_SET;
  cmd.v.v0.datatype = LCB_BINARY_DATATYPE_COMPRESSED_JSON;
  cmd.v.v0.key = "fooJSONC";
  cmd.v.v0.nkey = 8;
  cmd.v.v0.bytes = deflated;
  cmd.v.v0.nbytes = deflated_len;
  DatatypeTester(commands[0]);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  testargv = argv;
  testargc = argc;
  return RUN_ALL_TESTS();
}
