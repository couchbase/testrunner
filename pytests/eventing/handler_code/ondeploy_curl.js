function OnUpdate(doc, meta) {
    log("Doc created/updated", meta.id);
}

function OnDelete(meta, options) {
    log("Doc deleted/expired", meta.id);
}

function OnDeploy(action) {
    let success = 0;

    // Curl Request Function
    function CurlRequest() {
        var success = 0;
        var meta = {"id": "1"};
        var request = { path : 'get?foo1=bar1&foo2=bar2'};

        try {
            var response = curl("GET", server, request);

            log('response body received from server:', response.body);
            log('response headers received from server:', response.headers);
            log('response status received from server:', response.status);

            var res = new Uint8Array(response.body);

            if (response.status == 200) {
                dst_bucket["deploy_response"] = response.body;
                dst_bucket["deploy_success"] = 'Success: HTTP request completed';
                success++;
            } else {
                dst_bucket["deploy_response"] = response.status;
                dst_bucket["deploy_error"] = 'Error: HTTP request failed';
            }
        } catch (e) {
            log('error:', e);
            dst_bucket["deploy_error"] = 'Error: ' + e;
        }
        return success;
    }

    // Timer Operation
    function Timer() {
        var success = 0;
        try {
            let fireAt = new Date();
            fireAt.setSeconds(fireAt.getSeconds() + 5);

            let context = { docID: "valid_doc" };
            createTimer(ValidTimerCallback, fireAt, "valid_timer", context);

            function ValidTimerCallback(context) {
                dst_bucket[context.docID] = 'From Timer Callback';
                dst_bucket["valid_timer_success"] = 'Success: Timer fired and document added';
                success++;
            }
        } catch (e) {
            log('error:', e);
            dst_bucket["valid_timer_error"] = 'Error: ' + e;
        }
        return success;
    }

    // Timeout Operation
    function Timeout() {
        var success = 0;
        try {
            // Simulating an incorrect timeout logic
            for (let i = 0; i < 10_000_000_000; i++) {}

            dst_bucket["timeout_success"] = 'Success: Timeout operation completed';
            success++;
        } catch (e) {
            log('Timeout error:', e);
            dst_bucket["timeout_error"] = 'Error: Timeout occurred';
        }
        return success;
    }

    // Perform all the operations
    success += CurlRequest();
    success += Timer();
    success += Timeout();

    // Log based on success count
    if (success === 3) {
        log("All operations completed successfully.");
    } else {
        log("Some operations failed. Success docs count:", success);
    }
}
