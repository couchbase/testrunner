function OnUpdate(doc, meta) {
    try {
        log("Starting Vector Search Query (Internal) for doc: " + meta.id);

        var rawQuery = {
            indexName: INDEX_NAME,
            query: { match_none: {} },
            knn: [
                {
                    field: VECTOR_FIELD,
                    vector: QUERY_VECTOR,
                    k: K
                }
            ],
            size: K
        };

        var it = couchbase.searchQueryInternal(JSON.stringify(rawQuery));
        var ids = [];
        for (let row of it) {
            ids.push(row.id);
        }

        writeQueryResult("knnQueryInternal", ids, meta);
        log("All queries completed for doc: " + meta.id);
    } catch (e) {
        log("Error processing doc " + meta.id + ": " + e);
    }
}

// Write query results to destination bucket
function writeQueryResult(queryName, ids, meta) {
    var destDocId = queryName + "_" + meta.id;
    dst_bucket[destDocId] = { query: queryName, ids: ids };
    log("Wrote results for " + queryName + " to " + destDocId);
}

function OnDelete(meta) {
    delete dst_bucket["knnQueryInternal_" + meta.id];
}
