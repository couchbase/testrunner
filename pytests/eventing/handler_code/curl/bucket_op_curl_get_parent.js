function OnUpdate(doc, meta) {
    var request = {
	path : '../b',
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
    	if(e instanceof CurlError){
            dst_bucket[meta.id]="CurlError";
        }
    }
}



function OnDelete(meta) {
    var request = {
	path : '../b'
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
    	if(e instanceof CurlError){
            delete dst_bucket[meta.id];
            }
        }
}
