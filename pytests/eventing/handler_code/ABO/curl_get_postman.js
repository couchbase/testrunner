// Retries the curl call once when it throws (connect timeout, reset, DNS) or
// comes back with a transient status. postman-echo.com is a public endpoint, so
// the occasional failure is expected and should not cost us a document.
// There is no sleep primitive in the handler runtime, so the retry is immediate.
function curlWithRetry(method, request, attempts) {
    var outcome = {response: null, error: null};
    for (var i = 1; i <= attempts; i++) {
        try {
            var response = curl(method, server, request);
            outcome.response = response;
            outcome.error = null;
            log('curl attempt', i, 'returned status:', response.status);
            if (response.status == 200) {
                return outcome;
            }
            if (!(response.status == 429 || response.status >= 500)) {
                return outcome;
            }
            log('curl attempt', i, 'got transient status:', response.status);
        }
        catch (e) {
            outcome.response = null;
            outcome.error = e;
            log('curl attempt', i, 'failed with error:', e);
        }
    }
    return outcome;
}

function OnUpdate(doc, meta) {
    var request = {
	path : 'get',
	timeout : 30
    }
    var outcome = curlWithRetry("GET", request, 2);
    // A document is written on every path so the doc count stays comparable even
    // when postman-echo is flaky. Failures are only visible in the app logs.
    var value;
    if (outcome.error != null) {
        log('curl failed after all attempts:', outcome.error);
        value = {"curl_error": '' + outcome.error};
    }
    else if (outcome.response.status == 200) {
        log('response body received from server:', outcome.response.body);
        log('response headers received from server:', outcome.response.headers);
        value = outcome.response.body;
    }
    else {
        log('curl returned non-200 after all attempts:', outcome.response.status);
        value = outcome.response.status;
    }
    try {
        var result = couchbase.insert(dst_bucket, meta, value);
        log(result);
    }
    catch (e) {
        log('error:', e);
    }
}



function OnDelete(meta) {
    var request = {
	path : 'get',
	timeout : 30
    };
    var outcome = curlWithRetry("GET", request, 2);
    if (outcome.error != null) {
        log('curl failed after all attempts:', outcome.error);
    }
    else {
        log('response body received from server:', outcome.response.body);
        log('response status received from server:', outcome.response.status);
    }
    // The delete runs whatever the curl did, otherwise the post-delete doc count
    // check never reaches zero.
    try {
        var doc_meta = {"id": meta.id};
        var result = couchbase.delete(dst_bucket, doc_meta);
        log(result);
    }
    catch (e) {
        log('error:', e);
    }
}
