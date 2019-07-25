function OnUpdate(doc, meta) {
    var request = {
	path : 'put?param=text',
	headers: {
    "cache-control": "no-cache",
    "Postman-Token": "a3e931fe-8fe2-413c-be82-546062d28377"
    },
    body: "This is expected to be sent back as part of response body."
    };
    try {
    	var response = curl("PUT", server, request);
    	log('response body received from server:', response.body);
    	log('response headers received from server:', response.headers);
    	log('response status received from server:', response.status);
    	var res= new Uint8Array(response.body);
    	if(response.status == 200){
    	    dst_bucket[meta.id]=response.body;
    	}
    	else{
    	    dst_bucket[meta.id]=response.status;
    	}
    }
    catch (e) {
    	log('error:', e);
        }
}



function OnDelete(meta) {
    var request = {
	path : 'put?param=text',
	headers: {
    "cache-control": "no-cache",
    "Postman-Token": "a3e931fe-8fe2-413c-be82-546062d28377"
    },
    body: "This is expected to be sent back as part of response body."
    };
    try {
    	var response = curl("PUT", server, request);
    	log('response body received from server:', response.body);
    	log('response headers received from server:', response.headers);
    	log('response status received from server:', response.status);
    	var res= new Uint8Array(response.body);
    	if(response.status == 200){
    	    delete dst_bucket[meta.id];
    	}
    	else{
    	    delete dst_bucket[meta.id];
    	}
    }
    catch (e) {
    	log('error:', e);
        }
}