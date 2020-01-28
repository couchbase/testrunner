function OnUpdate(doc, meta) {
    try{
    var curl=SELECT CURL("http://localhost:8091/pools/default/buckets",{"header":"authorization: Basic QWRtaW5pc3RyYXRvcjpwYXNzd29yZA==",
                 "request":"GET"});
    for(var row of curl){
    var res=row;
    }
    log("result: ",res);
    if(res["$1"][0]["bucketType"] === "membase"){
        dst_bucket["curl"]=res
    }
    }catch(e){
        log(e);
    }
}