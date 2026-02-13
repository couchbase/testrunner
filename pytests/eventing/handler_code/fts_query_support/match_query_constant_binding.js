function OnUpdate(doc, meta) {
    try {
        log("Processing doc:", meta.id);

        var matchQuery = couchbase.SearchQuery.match(FTS_TERM)
            .operator("and")
            .field("reviews.content")
            .analyzer("standard")
            .fuzziness(2)
            .prefixLength(4);
        writeQueryResult("matchQuery", runQuery(matchQuery, { size: 5 }), meta);

        log("FTS completed for", meta.id);

    } catch (e) {
        log("Error running FTS:", e);
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
// Write query results to destination bucket
function writeQueryResult(queryName, ids, meta) {
    var destDocId = queryName + "_" + meta.id;
    dst_bucket[destDocId] = { query: queryName, ids: ids };
    log("Wrote results for " + queryName + " to " + destDocId);
}