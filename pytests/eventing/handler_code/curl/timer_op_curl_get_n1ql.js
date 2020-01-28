function OnUpdate(doc, meta) {
    var expiry = new Date();
    expiry.setSeconds(expiry.getSeconds() + 30);

    var context = {docID : meta.id, random_text : "e6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh0R7Aumoe6cZZGHuh07Aumoe6cZZGHuh07Aumoe6cZZGHuh07Aumoe6"};
    createTimer(timerCallback,  expiry, meta.id, context);
}



function OnDelete(meta) {
    var expiry = new Date();
    expiry.setSeconds(expiry.getSeconds() + 30);

    var context = {docID : meta.id};
    createTimer(NDtimerCallback,  expiry, meta.id, context);
}

function NDtimerCallback(context) {
    var request = {
	path : 'headers',
	headers: {
    "sample-header": "test"
    }
    };
    try {
    	var response = curl("GET", server, request);
    	log('response body received from server:', response.body);
    	log('response headers received from server:', response.headers);
    	log('response status received from server:', response.status);
    	var res= new Uint8Array(response.body);
    	if(response.status == 200){
    	    var id=context.docID;
    	    delete from dst_bucket where META().id=$id;
    	}
    	else{
    	    var id=context.docID;
    	    delete from dst_bucket where META().id=$id;
    	}
    }
    catch (e) {
    	log('error:', e);
        }
}
function timerCallback(context) {
    var request = {
	path : 'headers',
	headers: {
    "sample-header": "test"
    }
    };
    try {
    	var response = curl("GET", server, request);
    	log('response body received from server:', response.body);
    	log('response headers received from server:', response.headers);
    	log('response status received from server:', response.status);
    	var res= new Uint8Array(response.body);
    	if(response.status == 200){
    	    var id=context.docID;
    	    var status= response.status;
    	    var query= UPSERT into dst_bucket (KEY, VALUE) VALUES ($id, $status);
    	}
    	else{
    	    var id=context.docID;
    	    var status= response.status;
    	    var query= UPSERT into dst_bucket (KEY, VALUE) VALUES ($id, $status);
    	}
    }
    catch (e) {
    	log('error:', e);
        }
}