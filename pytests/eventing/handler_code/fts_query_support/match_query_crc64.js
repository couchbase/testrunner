function OnUpdate(doc, meta) {
    try {
        log("Processing Doc: " + meta.id);

        var ftsResults = runFTSMatchQuery();
        log("FTS returned " + ftsResults.length + " hits");

        var resultsString = JSON.stringify(ftsResults);
        var crcChecksum = couchbase.crc_64_go_iso(resultsString);
        log("CRC64 checksum of FTS results: " + crcChecksum);

        var docChecksum = couchbase.crc_64_go_iso(JSON.stringify(doc));
        log("CRC64 checksum of source doc: " + docChecksum);

        dst_bucket[meta.id] = {
            source_doc_id: meta.id,
            source_doc_crc: docChecksum,
            fts_query: {
                hits_count: ftsResults.length,
                doc_ids: ftsResults,
                results_crc: crcChecksum
            },
            processed_at: new Date().toISOString(),
            test_status: "PASSED"
        };

        log("Successfully wrote FTS+CRC64 result for: " + meta.id);

    } catch (e) {
        log("Error processing doc " + meta.id + ": " + e);
        dst_bucket[meta.id + "_error"] = {
            error: e.toString(),
            test_status: "FAILED"
        };
    }
}

// Helper: Run FTS Match Query
function runFTSMatchQuery() {
    var matchQuery = couchbase.SearchQuery.match("location hostel")
        .operator("and")
        .field("reviews.content")
        .analyzer("standard");
    var options = { size: 10 };
    var it = couchbase.searchQuery("travel_sample_test", matchQuery, options);
    var ids = [];
    for (let row of it) {
        ids.push(row.id);
    }
    return ids;
}