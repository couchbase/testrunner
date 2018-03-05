function OnUpdate(doc, meta) {
    var curl=SELECT CURL("http://localhost:8091/pools/default/buckets",{"header":"authorization: Basic QWRtaW5pc3RyYXRvcjpwYXNzd29yZA==",
                 "request":"GET"});
    var res=curl.execQuery();
    log("result: ",res);
    if(res[0]["$1"][0]["bucketType"] === "membase"){
        dst_bucket["curl"]=res
    }
}