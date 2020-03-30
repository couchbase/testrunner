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
    	    delete dst_bucket[context.docID];
    	}
    	else{
    	    delete dst_bucket[context.docID];
    	}
    }
    catch (e) {
    	log('error:', e);
        }
}
function timerCallback(context) {
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
    	    dst_bucket[context.docID]=response.body;
    	}
    	else{
    	    dst_bucket[context.docID]=response.status;
    	}
    }
    catch (e) {
    	log('error:', e);
        }
}
