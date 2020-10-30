function OnUpdate(doc, meta) {
    var expiry = new Date();
    expiry.setSeconds(expiry.getSeconds() + 150);

    var context = {docID : meta.id, random_text : "e6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh07Aumoe6cZZGHuh07Aumoe6cZZGHuh07Aumoe6"};
    createTimer(timerCallback,  expiry, meta.id, context);
}



function OnDelete(meta) {
    var expiry = new Date();
    expiry.setSeconds(expiry.getSeconds() + 150);

    var context = {docID : meta.id};
    createTimer(NDtimerCallback,  expiry, meta.id, context);
}

function NDtimerCallback(context) {
    var request = {
	path : 'job/test_suite_executor/api/json?tree=jobs[component]'
    };
    try {
    	var response = curl("GET", server, request);
    	log('response body received from server:', response.body);
    	log('response headers received from server:', response.headers);
    	log('response status received from server:', response.status);
    	var res= new Uint8Array(response.body);
        var meta={"id":context.docID};
        var result = couchbase.delete(dst_bucket,meta);
        log(result);
    }
    catch (e) {
    	log('error:', e);
        }
}
function timerCallback(context) {
    var request = {
	path : 'job/test_suite_executor/api/json?tree=jobs[component]'
    };
    try {
    	var response = curl("GET", server, request);
    	log('response body received from server:', response.body);
    	log('response headers received from server:', response.headers);
    	log('response status received from server:', response.status);
    	var res= new Uint8Array(response.body);
    	var meta={"id":context.docID};
    	if(response.status == 200){
    	    var result= couchbase.insert(dst_bucket,meta,response.body);
    	    log(result);
    	}
    	else{
    	    var result= couchbase.insert(dst_bucket,meta,response.status);
    	    log(result);
    	}
    }
    catch (e) {
    	log('error:', e);
        }
}