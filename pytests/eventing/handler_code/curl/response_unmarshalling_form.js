function OnUpdate(doc, meta) {
    var request = {
	path : '/response/urlencode'
    };
    try {
    	var response = curl("GET", server, request);
    	log('response body received from server:', response.body);
    	log('response headers received from server:', response.headers);
    	log('response status received from server:', response.status);
    	if(response.status == 200 && response.body.field1 == "value1" && response.body.field2 == "value2"){
    	    dst_bucket[meta.id]=response.body;
    	}
    }
    catch (e) {
    	log('error:', e);
    }
}

function OnDelete(meta) {
    var request = {
	path : '/response/urlencode'
    };
    try {
    	var response = curl("GET", server, request);
    	log('response body received from server:', response.body);
    	log('response headers received from server:', response.headers);
    	log('response status received from server:', response.status);
    	if(response.status == 200 && response.body.field1 == "value1" && response.body.field2 == "value2"){
    	    delete dst_bucket[meta.id];
    	}
    }
    catch (e) {
    	log('error:', e);
    }
}