function OnUpdate(doc, meta) {
    var request1 = {
	path : 'cookie-req'
    };

    var request = {
	path : 'cookie'
    };
    try {
        var response1 = curl("GET", server, request1);
        log('request1:',request1)
    	log('response body received from server:', response1);

    	var response = curl("GET", server, request);
    	log('response body received from server:', response.body);
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
    var request1 = {
	path : 'cookie-req'
    };

    var request = {
	path : 'cookie'
    };
    try {
        var response1 = curl("GET", server, request1);
        log('request1:',request1)
    	log('response body received from server:', response1);

    	var response = curl("GET", server, request);
    	log('response body received from server:', response.body);
    	var res= new Uint8Array(response.body);
    	if(response.status == 200){
    	    delete dst_bucket[meta.id];
    	}
    }
    catch (e) {
    	log('error:', e);
        }
}