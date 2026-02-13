function OnUpdate(doc, meta) {
    try {
        log("Processing Doc: " + meta.id);

        var matchQuery = couchbase.SearchQuery.match("location hostel")
            .operator("and")
            .field("reviews.content")
            .analyzer("standard")
            .fuzziness(2)
            .prefixLength(4);

        var ftsIds = runQuery(matchQuery, { size: 10 });
        writeQueryResult("matchQuery_with_curl", ftsIds, meta);
        log("FTS query returned " + ftsIds.length + " docs");

        var enrichedResults = [];
        for (var i = 0; i < Math.min(ftsIds.length, 3); i++) {
            var docId = ftsIds[i];

            var request = {
                path: '/post',
                headers: { "content-type": "application/json" },
                body: JSON.stringify({ doc_id: docId, request_type: "enrichment" })
            };

            try {
                var response = curl("POST", server, request);
                if (response.status === 200) {
                    enrichedResults.push({ doc_id: docId, enrichment: response.body });
                } else {
                    enrichedResults.push({ doc_id: docId, enrichment: { error: "HTTP " + response.status } });
                }
            } catch (e) {
                enrichedResults.push({ doc_id: docId, enrichment: { error: e.toString() } });
            }
        }

        dst_bucket[meta.id] = {
            source_doc: meta.id,
            fts_total_hits: ftsIds.length,
            enriched_results: enrichedResults,
            processed_at: new Date().toISOString()
        };

        log("Wrote enriched FTS+CURL result for: " + meta.id);

    } catch (e) {
        log("Error in OnUpdate: " + e);
    }
}

// --- Helper functions ---
function runQuery(query, options) {
    var it = couchbase.searchQuery("travel_sample_test", query, options);
    var ids = [];
    for (let row of it) {
        ids.push(row.id);
    }
    return ids;
}

function writeQueryResult(queryName, ids, meta) {
    var destDocId = queryName + "_" + meta.id;
    dst_bucket[destDocId] = { query: queryName, ids: ids };
    log("Wrote results for " + queryName + " to " + destDocId);
}
