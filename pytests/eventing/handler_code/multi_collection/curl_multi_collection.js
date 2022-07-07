function OnUpdate(doc, meta) {
    var request = {
	path : 'job/test_suite_executor/api/json?tree=jobs[component]'
    }
    try {
    	var response = curl("GET", server, request);
    	var doc_meta = {"id":meta.id,
        "keyspace": {"scope_name":meta.keyspace.scope_name,
        "collection_name":meta.keyspace.collection_name}};
    	log('response body received from server:', response.body);
    	log('response headers received from server:', response.headers);
    	log('response status received from server:', response.status);
    	var res= new Uint8Array(response.body);
    	if(response.status == 200){
    	    var result= couchbase.insert(dst_bucket,doc_meta,response.body);
    	    log(result);
    	}
    	else{
    	    var result= couchbase.insert(dst_bucket,doc_meta,response.status);
    	    log(result);
    	}
    }
    catch (e) {
    	log('error:', e);
        }
}



function OnDelete(meta) {
    var request = {
	path : 'job/test_suite_executor/api/json?tree=jobs[component]'
    };
    try {
    	var response = curl("GET", server, request);
    	log('response body received from server:', response.body);
    	log('response headers received from server:', response.headers);
    	log('response status received from server:', response.status);
    	var res= new Uint8Array(response.body);
    	var doc_meta = {"id":meta.id,
        "keyspace": {"scope_name":meta.keyspace.scope_name,
        "collection_name":meta.keyspace.collection_name}};
        var result = couchbase.delete(dst_bucket,doc_meta);
        log(result);
    }
    catch (e) {
    	log('error:', e);
        }
}