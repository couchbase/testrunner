function OnUpdate(doc, meta) {
    var step = "init";
    try {
        // --- Step 1: Analytics COUNT(DISTINCT) query ---
        step = "analytics_count_distinct";
        log("Processing doc:", meta.id);

        var countDistinctIt = couchbase.analyticsQuery(
            "SELECT COUNT(DISTINCT country) AS distinct_country_count FROM `travel-sample`.`inventory`.`airline`;"
        );
        var result = null;
        for (let row of countDistinctIt) {
            result = row;
        }

        var distinctCount = result ? result.distinct_country_count : 0;
        log("COUNT(DISTINCT) returned:", distinctCount);

        // --- Step 2: Base64 encode the analytics result ---
        step = "base64_encode";
        var originalPayload = {
            doc_id: meta.id,
            query_type: "count_distinct",
            distinct_country_count: distinctCount,
            data: result
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
            query_type: "count_distinct_with_base64",
            distinct_country_count: distinctCount,
            data: result,
            base64_encoded_length: encoded.length,
            round_trip_passed: roundTripPassed,
            coexistence: (distinctCount > 0 && roundTripPassed),
            processed_at: new Date().toISOString()
        };

        log("Analytics COUNT(DISTINCT) + Base64 test", roundTripPassed ? "PASSED" : "FAILED",
            "for:", meta.id);

    } catch (e) {
        log("Error at step [" + step + "]:", e);
    }
}
