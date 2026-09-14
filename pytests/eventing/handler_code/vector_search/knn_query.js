function OnUpdate(doc, meta) {
    try {
        log("Starting Vector Search Query for doc: " + meta.id);

        var vectorQuery = couchbase.SearchQuery.vectorQuery(VECTOR_FIELD, QUERY_VECTOR)
            .numCandidates(K * 5);
        var vectorSearch = couchbase.SearchQuery.vectorSearch(vectorQuery);

        var ids = runQuery({ match_none: {} }, { size: K, vectorSearch: vectorSearch });
        writeQueryResult("knnQuery", ids, meta);

        log("All queries completed for doc: " + meta.id);
    } catch (e) {
        log("Error processing doc " + meta.id + ": " + e);
    }
}

// Helper function to run a query and return matching doc IDs
function runQuery(query, options) {
    var it = couchbase.searchQuery(INDEX_NAME, query, options);
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

function OnDelete(meta) {
    delete dst_bucket["knnQuery_" + meta.id];
}
