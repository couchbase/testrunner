function OnUpdate(doc, meta) {
    try {
        log("Starting FTS + Base64 test for doc:", meta.id);
        var matchQuery = couchbase.SearchQuery.match("location hostel")
            .operator("and")
            .field("reviews.content");

        var it = couchbase.searchQuery(
            "travel_sample_test",
            matchQuery,
            { size: 5 }
        );

        var ftsIds = [];
        for (let row of it) {
            ftsIds.push(row.id);
        }

        log("FTS returned IDs:", ftsIds);

        var originalPayload = {
            source_doc: meta.id,
            fts_ids: ftsIds,
            hit_count: ftsIds.length
        };

        var encodedPayload = couchbase.base64Encode(originalPayload);

        var testDocId = "fts_base64_test::" + meta.id;

        couchbase.upsert(dst_bucket, { id: testDocId }, {
            encoded: encodedPayload
        });

        var stored = couchbase.get(dst_bucket, { id: testDocId });

        if (!stored.success) {
            throw "Failed to fetch encoded document";
        }

        var decodedPayload = couchbase.base64Decode(stored.doc.encoded);
        var decodedObject = JSON.parse(decodedPayload);

        var testPassed =
            JSON.stringify(decodedObject) === JSON.stringify(originalPayload);

        dst_bucket[testDocId + "::result"] = {
            test: "FTS + Base64 coexistence",
            status: testPassed ? "PASSED" : "FAILED",
            original: originalPayload,
            decoded: decodedObject,
            processed_at: new Date().toISOString()
        };

        log(
            "FTS + Base64 test",
            testPassed ? "PASSED" : "FAILED",
            "for doc:",
            meta.id
        );

    } catch (e) {
        log("ERROR in FTS + Base64 test:", e);
    }
}