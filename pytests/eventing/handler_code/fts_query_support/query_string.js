function OnUpdate(doc, meta) {
    try {
        log("Processing Doc: " + meta.id);
        log("Running FTS Query");
        log("Running Query String Query");
        var queryStringQuery = couchbase.SearchQuery.queryString("Cleanliness");
        writeQueryResult("queryStringQuery", runQuery(queryStringQuery, { size: 61 }), meta);
    } catch (e) {
        log("Error processing doc " + meta.id + ": " + e);
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