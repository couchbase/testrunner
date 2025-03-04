function OnUpdate(doc, meta) {
    var request = {
        path: '$url',
        timeout: 1  // Set a very small timeout to trigger timeout behavior
    };

    try {
        var response = curl("GET", server, request);

        log('response body received from server:', response.body);
        log('response headers received from server:', response.headers);
        log('response status received from server:', response.status);
    
        if (response.status === 408) {
            log('Request timed out as expected');
        } else if (response.status === 200) {
            dst_bucket[meta.id] = response.body;
        } else {
            dst_bucket[meta.id] = response.status;
        }
    } catch (e) {
        if (e.message && e.message.includes('timeout')) {
            log('Request timed out as expected');
        } else {
            log('error:', e);
            dst_bucket["curl_error"] = 'Error: ' + e;
        }
    }
}

function OnDelete(meta) {
    var request = {
        path: '$url',
        timeout: 1  // Set a very small timeout to trigger timeout behavior
    };

    try {
        var response = curl("GET", server, request);

        log('response body received from server:', response.body);
        log('response headers received from server:', response.headers);
        log('response status received from server:', response.status);

        if (response.status === 408) {
            log('Request timed out as expected');
        } else if (response.status === 200) {
            delete dst_bucket[meta.id];
        } else {
            log('Non-timeout error:', response.status);
        }
    } catch (e) {
        if (e.message && e.message.includes('timeout')) {
            log('Request timed out as expected');
        } else {
            log('error:', e);
            dst_bucket["curl_error"] = 'Error: ' + e;
        }
    }
}