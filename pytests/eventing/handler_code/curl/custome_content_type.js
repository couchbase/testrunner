function OnUpdate(doc, meta) {
    var request = {
	path : '/custom/encoding'
    };
    try {
    	var response = curl("GET", server, request);
    	log('response body received from server:', response.body);
    	log('response headers received from server:', response.headers);
    	log('response status received from server:', response.status);
    	var arr = new Uint8Array(response.body);
    	var f="field1";
    	var ex = Uint8Array.from(f, x => x.charCodeAt(0));
    // 	log("arr:",arr);
    // 	log("field1:",ex);
    	if(response.status == 200 && matchExpectedResponse(ex,arr)){
    	    dst_bucket[meta.id]=ex;
    	}
    }
    catch (e) {
    	log('error:', e);
    }
}

function OnDelete(meta) {
    var request = {
	path : '/custom/encoding'
    };
    try {
    	var response = curl("GET", server, request);
    	log('response body received from server:', response.body);
    	log('response headers received from server:', response.headers);
    	log('response status received from server:', response.status);
    	var arr = new Uint8Array(response.body);
    	var f="field1";
    	var ex = Uint8Array.from(f, x => x.charCodeAt(0));
    // 	log("arr:",arr);
    // 	log("field1:",ex);
    	if(response.status == 200 && matchExpectedResponse(ex,arr)){
    	    delete dst_bucket[meta.id];
    	}
    }
    catch (e) {
    	log('error:', e);
    }
}

function matchExpectedResponse(expected,actual){
    if (expected.Length != actual.Length) return false;
    for (var i = 0 ; i != expected.byteLength ; i++)
    {
        if (expected[i] !=actual[i]) return false;
    }
    return true;
}