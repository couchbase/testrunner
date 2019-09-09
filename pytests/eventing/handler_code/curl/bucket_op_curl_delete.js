function OnUpdate(doc, meta) {
    var request = {
	path : 'delete?param=text',
	headers: {
    "cache-control": "no-cache",
    "Postman-Token": "a3e931fe-8fe2-413c-be82-546062d28377"
    }
    };
    while(true){
    try {
    	var response = curl("DELETE", server, request);
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
	path : 'delete?param=text',
	headers: {
    "cache-control": "no-cache",
    "Postman-Token": "a3e931fe-8fe2-413c-be82-546062d28377"
    }
    };
    while(true){
    try {
    	var response = curl("DELETE", server, request);
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