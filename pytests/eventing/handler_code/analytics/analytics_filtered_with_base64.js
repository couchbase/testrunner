function OnUpdate(doc, meta) {
    var step = "init";
    try {
        // --- Step 1: Analytics filtered query ---
        step = "analytics_filtered";
        log("Processing doc:", meta.id);

        var filteredIt = couchbase.analyticsQuery(
            "SELECT name, country FROM `travel-sample`.`inventory`.`airline` WHERE country = $country LIMIT 10;",
            { "country": "United States" }
        );
        var rows = [];
        for (let row of filteredIt) {
            rows.push(row);
        }

        log("Filtered query returned", rows.length, "rows");

        // --- Step 2: Base64 encode the analytics result ---
        step = "base64_encode";
        var originalPayload = {
            doc_id: meta.id,
            query_type: "filtered_where",
            row_count: rows.length,
            data: rows
        };

        var encoded = couchbase.base64Encode(originalPayload);
        log("Base64 encoded analytics result, length:", encoded.length);

        // --- Step 3: Base64 decode and verify round-trip ---
        step = "base64_decode";
        var decoded = couchbase.base64Decode(encoded);
        var decodedObj = JSON.parse(decoded);

        var roundTripPassed =
            JSON.stringify(decodedObj) === JSON.stringify(originalPayload);

        // --- Step 4: Write result ---
        step = "write_result";
        dst_bucket[meta.id] = {
            doc_id: meta.id,
            query_type: "filtered_with_base64",
            row_count: rows.length,
            data: rows,
            base64_encoded_length: encoded.length,
            round_trip_passed: roundTripPassed,
            coexistence: (rows.length > 0 && roundTripPassed),
            processed_at: new Date().toISOString()
        };

        log("Analytics Filtered + Base64 test", roundTripPassed ? "PASSED" : "FAILED",
            "for:", meta.id);

    } catch (e) {
        log("Error at step [" + step + "]:", e);
    }
}
