function OnUpdate(doc, meta) {
	var request = {
		path: 'job/test_suite_executor/api/json?tree=jobs[component]'
	}
	try {
		validateBucketGet(meta);
		var response = curl("GET", server, request);
		log('response body received from server:', response.body);
		log('response headers received from server:', response.headers);
		log('response status received from server:', response.status);
		var res = new Uint8Array(response.body);
		if (response.status == 200) {
			dst_bucket[meta.id] = response.body;
		}
		else {
			dst_bucket[meta.id] = response.status;
		}
	}
	catch (e) {
		log('error:', e);
	}
}


function OnDelete(meta, options) {
	var request = {
		path: 'job/test_suite_executor/api/json?tree=jobs[component]'
	};
	try {
		var response = curl("GET", server, request);
		log('response body received from server:', response.body);
		log('response headers received from server:', response.headers);
		log('response status received from server:', response.status);
		var res = new Uint8Array(response.body);
		delete dst_bucket[meta.id];
	}
	catch (e) {
		log('error:', e);
	}
}

function validateBucketGet(meta) {
	var val = src_bucket[meta.id];

	if (val === null || val === undefined) {
		throw new Error("GET failed: document not found for key " + meta.id);
	}

	log("Bucket GET successful for key:", meta.id);
	return val;
}