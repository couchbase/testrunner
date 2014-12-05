/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2012-2013 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#include <snappy-c.h>
#include <time.h>
#include <sys/time.h>
#include <stdio.h>
#include <vector>
#include <libcouchbase/couchbase.h>
#include <libcouchbase/durability.h>
#include <stdlib.h>
#include <string>
#include <cstring>
#include <stdint.h>
#ifdef _WIN32
#define PRIu64 "I64u"
#else
#include <inttypes.h>
#endif

char* getvaluebuf;
char* statkeybuf;
char* statvaluebuf;
lcb_size_t* statkeysize;
lcb_size_t* statvaluesize;
long* getvaluesize;
lcb_datatype_t* getvaluedtype;
std::vector<std::string> threadstate;
bool warmupdone = false;
bool gotwm_val_cn = false;
bool gotreplica_curr_items = false;
bool goteject_num_items = false;
bool got_active_resident = false;
char* wm_val_cn;
char* rep_val_cn;
char* ejected_val_cn;
char* resident_val_cn;
int* ep_warmup_value_count;
//extern double t1;
extern struct timeval tim;
struct timeval reptim;
extern double t1;
double t2;
std::vector<std::pair<std::string, double> > replatencies;
typedef std::vector<std::pair<std::string, std::string> > StatsVector;
struct stats_generic
{
	int refcount;
	StatsVector statsvec;
};
struct stats_generic genericstats =
{ 0 };
extern int numReplicaget;

static void error_callback(lcb_t instance, lcb_error_t error, const char *errinfo)
{
	fprintf(stderr, "ERROR: %s (0x%x), %s\n", lcb_strerror(instance, error), error, errinfo);
	//exit(EXIT_FAILURE);
}

static void durability_callback(lcb_t instance, const void *cookie, lcb_error_t error, const lcb_durability_resp_t *resp)
{
	if (error == LCB_SUCCESS)
	{
		/*gettimeofday(&reptim, NULL);
		 t2 = (double) reptim.tv_usec - t1;
		 std::string key1((char *)resp->v.v0.key,(size_t)resp->v.v0.nkey);
		 std::pair <std::string,double> keylatency(key1,(double) (reptim.tv_sec*1000000+reptim.tv_usec));
		 replatencies.push_back(keylatency);
		 fprintf(stderr, "key: ");
		 fwrite(resp->v.v0.key, sizeof(char), resp->v.v0.nkey, stderr);
		 fprintf(stderr, " replicated to number of nodes:");
		 fprintf(stderr, "%x",resp->v.v0.nreplicated);
		 fprintf(stderr, " reptim is:");
		 fprintf(stderr, "%.6lf",((double) reptim.tv_usec)/1000);
		 fprintf(stderr, " replication latency is:");
		 fprintf(stderr, "%.6lf",t2/1000);
		 fprintf(stderr, "\n");*/

	}
	else
	{
		fprintf(stderr, "ERROR: \n", lcb_strerror(instance, error), error);
		//exit(EXIT_FAILURE);
	}
}

static void store_callback(lcb_t instance, const void *cookie, lcb_storage_t operation, lcb_error_t error, const lcb_store_resp_t *item)
{
	if (error == LCB_SUCCESS)
	{
		//fprintf(stderr, "STORED \"");
		//fwrite(item->v.v0.key, sizeof(char), item->v.v0.nkey, stderr);
	}
	else
	{
		fprintf(stderr, "STORE ERROR: %s (0x%x)\n", lcb_strerror(instance, error), error);
		//exit(EXIT_FAILURE);
	}
	(void) cookie;
	(void) operation;
}

static void get_callback(lcb_t instance, const void *cookie, lcb_error_t error, const lcb_get_resp_t *item)
{
	if (error == LCB_SUCCESS)
	{

		std::string key;
		key.assign((const char *) item->v.v0.key, item->v.v0.nkey);
		if (key.compare(0, 7, "replica"))
			numReplicaget++;
		/*getvaluesize = (long *) calloc(1,sizeof(long));
		 getvaluedtype = (lcb_datatype_t*) calloc(1,sizeof(lcb_datatype_t));
		 getvaluebuf = (char *)item->v.v0.bytes;
		 *getvaluesize = (long)item->v.v0.nbytes;
		 *getvaluedtype = item->v.v0.datatype;

		 fprintf(stderr, "GOT size of doc: %ld \n",(long)item->v.v0.nbytes);
		 fwrite(item->v.v0.key, sizeof(char), item->v.v0.nkey, stderr);
		 fwrite(item->v.v0.bytes, sizeof(char), item->v.v0.nbytes, stderr);
		 fprintf(stderr, "\n");
		 fprintf(stderr, "GOT CAS value: %ld \n",(long)item->v.v0.cas);
		 if(item->v.v0.datatype==LCB_BINARY_DATATYPE_COMPRESSED_JSON||item->v.v0.datatype==LCB_BINARY_DATATYPE_COMPRESSED){
		 char uncompressed[2560];
		 size_t uncompressed_len = 2560;
		 snappy_status status;
		 status=snappy_uncompress((const char *)item->v.v0.bytes, item->v.v0.nbytes, uncompressed, &uncompressed_len);
		 fprintf(stderr, "uncompressed length %d\n",(int)uncompressed_len);
		 fwrite(uncompressed, sizeof(char), uncompressed_len, stderr);
		 fprintf(stderr,"\n");
		 }*/
	}
	else
	{
		fprintf(stderr, "GET ERROR: %s (0x%x)\n", lcb_strerror(instance, error), error);
	}
	(void) cookie;
}

bool isWarmupdone(const lcb_server_stat_resp_t *resp)
{

	char* keystr = (char *) calloc(resp->v.v0.nkey, sizeof(char));
	memcpy(keystr, resp->v.v0.key, resp->v.v0.nkey);
	std::string key = std::string(keystr);
	std::string keyref("ep_warmup_state");

	char* valstr = (char *) calloc(resp->v.v0.nbytes, sizeof(char));
	memcpy(valstr, resp->v.v0.bytes, resp->v.v0.nbytes);
	std::string val = std::string(valstr);
	std::string valref("done");

	if (keyref.compare(key) == 0 && valref.compare(val) == 0)
	{
		char* thstate = (char *) calloc(resp->v.v0.nbytes + 1, sizeof(char));
		memcpy(thstate, resp->v.v0.bytes, resp->v.v0.nbytes);
		thstate[resp->v.v0.nbytes] = '\0';
		std::string str = std::string(thstate);
		threadstate.push_back(str);
		return true;
	}

	else if (keyref.compare(key) == 0 && valref.compare(val) != 0)
	{
		char* thstate = (char *) calloc(resp->v.v0.nbytes + 1, sizeof(char));
		memcpy(thstate, resp->v.v0.bytes, resp->v.v0.nbytes);
		thstate[resp->v.v0.nbytes] = '\0';
		std::string str = std::string(thstate);
		threadstate.push_back(str);
		return false;
	}

	else
		return false;
}

bool isWarmupvalcount(const lcb_server_stat_resp_t *resp)
{

	char* keystr = (char *) calloc(resp->v.v0.nkey, sizeof(char));
	memcpy(keystr, resp->v.v0.key, resp->v.v0.nkey);
	std::string key = std::string(keystr);
	std::string keyref("ep_warmup_value_count");

	if (key.compare(keyref) == 0)
	{
		return true;
	}
	else
		return false;
}

bool isreplica_curr_items(const lcb_server_stat_resp_t *resp)
{

	char* keystr = (char *) calloc(resp->v.v0.nkey, sizeof(char));
	memcpy(keystr, resp->v.v0.key, resp->v.v0.nkey);
	std::string key = std::string(keystr);
	std::string keyref("vb_replica_curr_items");

	if (key.compare(keyref) == 0)
	{
		//char* valstr= (char *)calloc(resp->v.v0.nbytes,sizeof(char));
		//memcpy(valstr,resp->v.v0.bytes,resp->v.v0.nbytes);
		//std::string val = std::string(valstr);
		//fprintf(stderr, "\n%s: %s",key.c_str(),val.c_str());
		return true;
	}
	else
		return false;
}

bool isactive_resident(const lcb_server_stat_resp_t *resp)
{

	char* keystr = (char *) calloc(resp->v.v0.nkey, sizeof(char));
	memcpy(keystr, resp->v.v0.key, resp->v.v0.nkey);
	std::string key = std::string(keystr);
	std::string keyref("vb_active_perc_mem_resident");

	if (key.compare(keyref) == 0)
	{
		return true;
	}
	else
		return false;
}

bool isevicted_items(const lcb_server_stat_resp_t *resp)
{

	char* keystr = (char *) calloc(resp->v.v0.nkey, sizeof(char));
	memcpy(keystr, resp->v.v0.key, resp->v.v0.nkey);
	std::string key = std::string(keystr);
	std::string keyref("ep_num_value_ejects");

	if (key.compare(keyref) == 0)
	{
		return true;
	}
	else
		return false;
}

static void stats_generic_callback(lcb_t instance, const void *cookie, lcb_error_t error, const lcb_server_stat_resp_t *resp)
{
	genericstats.refcount++;
	if (error == LCB_SUCCESS)
	{
		if (resp->v.v0.nkey > 0)
		{
			std::string key;
			key.assign((const char *) resp->v.v0.key, resp->v.v0.nkey);
			std::string value;
			value.assign((const char *) resp->v.v0.bytes, resp->v.v0.nbytes);
			genericstats.statsvec.push_back(std::pair<std::string, std::string>(key, value));
		}
	}

	else
	{
		fprintf(stderr, "GET ERROR: %s (0x%x)\n", lcb_strerror(instance, error), error);
	}
	(void) cookie;
}

static void stats_callback(lcb_t instance, const void *cookie, lcb_error_t error, const lcb_server_stat_resp_t *resp)
{
	if (error == LCB_SUCCESS)
	{

		//check if ep_warmup_state is done and mark bool variable warmupdone
		if (isWarmupdone(resp))
			warmupdone = true;

		//read ep_warmup_value_count
		if (isWarmupvalcount(resp))
		{

			if (warmupdone && !gotwm_val_cn)
			{
				wm_val_cn = (char *) calloc(resp->v.v0.nbytes, sizeof(char));
				memcpy(wm_val_cn, resp->v.v0.bytes, resp->v.v0.nbytes);
				gotwm_val_cn = true;
				//*ep_warmup_value_count = atoi(num);
				//fprintf(stderr, "\n%d\n",*ep_warmup_value_count);
			}
		}
		if (isreplica_curr_items(resp))
		{
			if (!gotreplica_curr_items)
			{
				rep_val_cn = (char *) calloc(resp->v.v0.nbytes, sizeof(char));
				memcpy(rep_val_cn, resp->v.v0.bytes, resp->v.v0.nbytes);
				gotreplica_curr_items = true;
			}
		}
		if (isactive_resident(resp))
		{
			if (!got_active_resident)
			{
				resident_val_cn = (char *) calloc(resp->v.v0.nbytes, sizeof(char));
				memcpy(resident_val_cn, resp->v.v0.bytes, resp->v.v0.nbytes);
				got_active_resident = true;
			}
		}
		if (isevicted_items(resp))
		{
			if (!goteject_num_items)
			{
				ejected_val_cn = (char *) calloc(resp->v.v0.nbytes, sizeof(char));
				memcpy(ejected_val_cn, resp->v.v0.bytes, resp->v.v0.nbytes);
				goteject_num_items = true;
			}
		}
	}
	else
	{
		fprintf(stderr, "GET ERROR: %s (0x%x)\n", lcb_strerror(instance, error), error);
	}
	(void) cookie;
}

int callget(lcb_t* instance, const void* gkey, size_t gnkey)
{
	lcb_wait(*instance);
	{
		lcb_error_t err;
		lcb_get_cmd_t cmd;
		const lcb_get_cmd_t *commands[1];
		commands[0] = &cmd;
		memset(&cmd, 0, sizeof(cmd));
		cmd.v.v0.key = gkey;
		cmd.v.v0.nkey = gnkey;
		err = lcb_get(*instance, NULL, 1, commands);
		if (err != LCB_SUCCESS)
		{
			fprintf(stderr, "Failed to get: %s\n", lcb_strerror(NULL, err));
			return 1;
		}
	}
	lcb_wait(*instance);
	//lcb_destroy(*instance);
	return 0;
}
