function OnUpdate(doc, meta) {
    var request = {
	path : 'post?param=text',
	headers: {
    "cache-control": "no-cache",
    "Postman-Token": "a3e931fe-8fe2-413c-be82-546062d28377"
    },
    body: "This is expected to be sent back as part of response body."
    };
    try {
    	var response = curl("POST", server, request);
    	log('response body received from server:', response.body);
    	log('response headers received from server:', response.headers);
    	log('response status received from server:', response.status);
    	var res= new Uint8Array(response.body);
    	if(response.status == 200){
    	    var id=meta.id;
    	    var status= response.status;
    	    var query= UPSERT into dst_bucket (KEY, VALUE) VALUES ($id, $status);
    	}
    }
    catch (e) {
    	log('error:', e);
        }
}



function OnDelete(meta) {
    var request = {
	path : 'post?param=text',
	headers: {
    "cache-control": "no-cache",
    "Postman-Token": "a3e931fe-8fe2-413c-be82-546062d28377"
    },
    body: "This is expected to be sent back as part of response body."
    };
    try {
    	var response = curl("POST", server, request);
    	log('response body received from server:', response.body);
    	log('response headers received from server:', response.headers);
    	log('response status received from server:', response.status);
    	var res= new Uint8Array(response.body);
    	if(response.status == 200){
    	    var id=meta.id;
    	    delete from dst_bucket where META().id=$id;
    	}
    }
    catch (e) {
    	log('error:', e);
        }
}