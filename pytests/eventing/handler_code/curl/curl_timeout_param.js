function OnUpdate(doc, meta) {
    var request = {
        path: '$url',
        timeout: 20
    };

    try {
        var response = curl("GET", server, request);

        log('response body received from server:', response.body);
        log('response headers received from server:', response.headers);
        log('response status received from server:', response.status);

        var res = new Uint8Array(response.body);
        if (response.status == 200) {
            dst_bucket[meta.id] = response.body;
        } else {
            dst_bucket[meta.id] = response.status;
        }
    } catch (e) {
        log('error:', e);
        dst_bucket["curl_error"] = 'Error: ' + e;
    }
}

function OnDelete(meta) {
    var request = {
        path: '$url',
        timeout: 20
    };

    try {
        var response = curl("GET", server, request);

        log('response body received from server:', response.body);
        log('response headers received from server:', response.headers);
        log('response status received from server:', response.status);

        var res = new Uint8Array(response.body);
        delete dst_bucket[meta.id];
    } catch (e) {
        log('error:', e);
        dst_bucket["curl_error"] = 'Error: ' + e;
    }
}