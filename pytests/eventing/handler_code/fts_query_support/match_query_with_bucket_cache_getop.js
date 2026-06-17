function OnUpdate(doc, meta) {
    try {
        log("Processing doc:", meta.id);

        var matchQuery = couchbase.SearchQuery
            .match("location hostel")
            .operator("and")
            .field("reviews.content");

        var ftsIds = runQuery(matchQuery, { size: 5 });
        if (ftsIds.length === 0) {
            throw "FTS query returned 0 hits";
        }

        var readCount = 100;
        var srcMeta = { id: meta.id };

        var start_cached = new Date();
        var cachedDoc = null;
        for (var i = 0; i < readCount; i++) {
            var cachedRes = couchbase.get(src_bucket, srcMeta, { "cache": true });
            if (!cachedRes.success) {
                throw "Cached get failed for " + meta.id + ": " + JSON.stringify(cachedRes);
            }
            cachedDoc = cachedRes.doc;
        }
        var cached_time = new Date() - start_cached;

        var start_non_cached = new Date();
        var normalDoc = null;
        for (var j = 0; j < readCount; j++) {
            var normalRes = couchbase.get(src_bucket, srcMeta);
            if (!normalRes.success) {
                throw "Non-cached get failed for " + meta.id + ": " + JSON.stringify(normalRes);
            }
            normalDoc = normalRes.doc;
        }
        var non_cached_time = new Date() - start_non_cached;

        var docsMatch = JSON.stringify(cachedDoc) === JSON.stringify(normalDoc);
        if (!docsMatch) {
            throw "Cached and non-cached docs do not match for " + meta.id;
        }

        dst_bucket[meta.id] = {
            source_doc: meta.id,
            fts_hits: ftsIds.length,
            read_iterations: readCount,
            cached_read_time_ms: cached_time,
            non_cached_read_time_ms: non_cached_time,
            performance_difference_ms: non_cached_time - cached_time,
            documents_match: docsMatch,
            tested_at: new Date().toISOString()
        };

        log("FTS + KV cache test completed for:", meta.id,
            "cached:", cached_time, "ms, non-cached:", non_cached_time, "ms");
    } catch (e) {
        log("Error in FTS + KV cache test for:", meta.id, "error:", e);
        throw e;
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