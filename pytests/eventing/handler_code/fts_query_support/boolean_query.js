function OnUpdate(doc, meta) {
    try {
        log("Processing Doc: " + meta.id);
        log("Running FTS Query");
        log("Running Boolean Query");
        var matchQueryForCompound = couchbase.SearchQuery.match("location")
            .field("reviews.content");
        var booleanQueryForCompound = couchbase.SearchQuery.booleanField(true)
            .field("free_breakfast");
        var mustQuery = couchbase.SearchQuery.conjuncts(matchQueryForCompound);
        var mustNotQuery = couchbase.SearchQuery.disjuncts(
            couchbase.SearchQuery.booleanField(false).field("free_breakfast")
        );
        var shouldQuery = couchbase.SearchQuery.disjuncts(booleanQueryForCompound);
        var booleanQuery = couchbase.SearchQuery.boolean()
            .must(mustQuery)
            .should(shouldQuery)
            .mustNot(mustNotQuery);
        writeQueryResult("booleanQuery", runQuery(booleanQuery, { size: 453 }), meta);
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