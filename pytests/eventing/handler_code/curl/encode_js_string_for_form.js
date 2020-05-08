function OnUpdate(doc, meta) {
    var request = {
	path : 'form',
	encoding: "FORM",
	body: {"email":"joe.blogs@gmail.com"},
	headers: {
	    "Content-Type":"application/x-www-form-urlencoded"
	    }
    };
    try {
    	var response = curl("POST", server, request);
    	log('response body received from server:', response.body);
    	log('response headers received from server:', response.headers);
    	log('response status received from server:', response.status);
    	var res= new Uint8Array(response.body);
    	if(response.status == 200){
    	    dst_bucket[meta.id]=response.body;
    	}
    }
    catch (e) {
    	log('error:', e);
        }
}

function OnDelete(meta) {
    var request = {
	path : 'form',
	encoding: "FORM",
	body: {"email":"joe.blogs@gmail.com",
        "password":"secure_Password123"},
	headers: {
	    "Content-Type":"application/x-www-form-urlencoded"
	    }
    };
    try {
    	var response = curl("POST", server, request);
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
        }
}