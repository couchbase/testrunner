function OnUpdate(doc, meta) {
    var request = {
	path : '/headers',
	headers: {
    "sample-header": "test"
    }
    };
    while(true){
    try {
    	var response = curl("GET", server, request);
    	log('response body received from server:', response.body);
    	log('response headers received from server:', response.headers);
    	log('response status received from server:', response.status);
    	var res= new Uint8Array(response.body);
    	if(response.status == 200){
    	    dst_bucket[meta.id]=response.body;
    	    break;
    	}
    	else{
    	    dst_bucket[meta.id]=response.status;
    	    break;
    	}
    }
    catch (e) {
    	log('error:', e);
        }
    }
}



function OnDelete(meta) {
    var request = {
	path : '/headers',
	headers: {
    "sample-header": "test"
    }
    };
    while(true){
    try {
    	var response = curl("GET", server, request);
    	log('response body received from server:', response.body);
    	log('response headers received from server:', response.headers);
    	log('response status received from server:', response.status);
    	var res= new Uint8Array(response.body);
    	if(response.status == 200){
    	    delete dst_bucket[meta.id];
    	    break;
    	}
    	else{
    	    delete dst_bucket[meta.id];
    	    break;
    	}
    }
    catch (e) {
    	log('error:', e);
        }
    }
}