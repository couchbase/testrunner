function OnUpdate(doc, meta) {
    try {
        var ftsHitCounter = 0;
        var matchQuery = couchbase.SearchQuery.match("location hostel")
            .operator("and")
            .field("reviews.content");

        var ftsIds = runQuery(matchQuery, {size: 10});

        ftsHitCounter += ftsIds.length;

        log("FTS hits:", ftsIds.length, "Total counter:", ftsHitCounter);

        dst_bucket[meta.id] = {
            source_doc: meta.id,
            fts_hits: ftsIds.length,
            counter_value: ftsHitCounter
        };

    } catch (e) {
        log("Error:", e);
    }
}

// Helper function to run a query and return matching doc IDs
function runQuery(query, options) {
    var it = couchbase.searchQuery("travel_sample_test", query, options);
    var ids = [];
    for (let row of it) {
        ids.push(row.id);
    }
    return ids;
}
