function OnUpdate(doc, meta) {
    var request = {
    encoding: 'TEXT',
	path : 'json',
	body: {"name":"vikas"}
    };
    try {
    	var response = curl("POST", server, request);
    	log('response body received from server:', response.body);
    	log('response headers received from server:', response.headers);
    	log('response status received from server:', response.status);
    	var res= new Uint8Array(response.body);
    }
    catch (e) {
    	log('error:', e);
    	if(e instanceof CurlError && e["message"]=="Unable to encode Object as TEXT","stack"){
    	    dst_bucket[meta.id]="Unable to encode Object as TEXT";
    	}
        }
}

function OnDelete(meta) {
    var request = {
    encoding: 'TEXT',
	path : 'json',
	body: {"name":"vikas"}
    };
    try {
    	var response = curl("POST", server, request);
    	log('response body received from server:', response.body);
    	log('response headers received from server:', response.headers);
    	log('response status received from server:', response.status);
    	var res= new Uint8Array(response.body);
    }
    catch (e) {
    	log('error:', e);
    	log('error:', e);
    	if(e instanceof CurlError && e["message"]=="Unable to encode Object as TEXT","stack"){
    	    delete dst_bucket[meta.id];
    	}
        }
}