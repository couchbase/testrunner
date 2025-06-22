function OnUpdate(doc, meta) {
    var request = {
	path : 'get?foo1=bar1&foo2=bar2'
    }
    try {
        var response = curl("GET", server, request);
    }
    catch (e) {
        log('error:', e);
        if(e["message"]=="Ability to make curl calls is disabled"){
            var result1= couchbase.insert(dst_bucket,meta,doc);
            log(result1);
        }
    }
}



function OnDelete(meta) {
    var request = {
	path : 'get?foo1=bar1&foo2=bar2'
    };
    try {
        var response = curl("GET", server, request);
        log('response body received from server:', response.body);
        log('response headers received from server:', response.headers);
        log('response status received from server:', response.status);
        var res= new Uint8Array(response.body);
        var doc_meta={"id":meta.id};
        var result = couchbase.delete(dst_bucket,doc_meta);
        log(result);
    }
    catch (e) {
        log('error:', e);
        }
}