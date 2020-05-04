function OnUpdate(doc, meta) {
    var request = {
	path : '/'
    };
    try {
    	var response = curl("GET", server, request);
    	log('response body received from server:', response.body);
    	log('response headers received from server:', response.headers);
    	log('response status received from server:', response.status);
    	var res= new Uint8Array(response.body);
    }
    catch (e) {
    	log('error:', e);
    	if(e["message"]=="Unable to perform the request: SSL peer certificate or SSH remote key was not OK"){
    	    dst_bucket[meta.id]=e["message"];
    	}
    }
}


function OnDelete(meta) {
    var request = {
	path : '/'
    };
    try {
    	var response = curl("GET", server, request);
    	log('response body received from server:', response.body);
    	log('response headers received from server:', response.headers);
    	log('response status received from server:', response.status);
    	var res= new Uint8Array(response.body);
    	if(response.status == 200){
    	    delete dst_bucket[meta.id];
    	}
    }
    catch (e) {
    	log('error:', e);
    	if(e["message"]=="Unable to perform the request: SSL peer certificate or SSH remote key was not OK"){
    	    delete dst_bucket[meta.id];
    	}
    }
}