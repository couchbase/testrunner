function OnUpdate(doc, meta) {
    var request = {
	path : 'text',
	body: "some_string"
    };
    try {
    	var response = curl("POST", server, request);
    	log('response body received from server:', response.body);
    	log('response headers received from server:', response.headers);
    	log('response status received from server:', response.status);
    	if(response.status == 200 && response.body.last_name == "chaudhary"){
    	    dst_bucket[meta.id]=response.body;
    	}
    }
    catch (e) {
    	log('error:', e);
        }
}

function OnDelete(meta) {
    var request = {
	path : 'text',
	body: "some_string"
    };
    try {
    	var response = curl("POST", server, request);
    	log('response body received from server:', response.body);
    	log('response headers received from server:', response.headers);
    	log('response status received from server:', response.status);
    	if(response.status == 200 && response.body.last_name == "chaudhary"){
    	    delete dst_bucket[meta.id];
    	}
    }
    catch (e) {
    	log('error:', e);
        }
}