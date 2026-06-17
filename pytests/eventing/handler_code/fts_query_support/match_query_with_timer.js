function ftsTimerCallback(context) {
        log("FTS timer(30s) fired for doc: " + context.docID);

        var matchQuery = couchbase.SearchQuery.match("location hostel")
            .operator("and")
            .field("reviews.content")
            .analyzer("standard")
            .fuzziness(2)
            .prefixLength(4);
        writeQueryResult("matchQuery_with_timer", runQuery(matchQuery, { size: 5 }), { id: context.docID });
}

function OnUpdate(doc, meta) {
    try {
        var expiry = new Date();
        expiry.setSeconds(expiry.getSeconds() + 30);

        var context = {
            docID: meta.id,
            queryType: "match_with_timer"
        };

        createTimer(ftsTimerCallback, expiry, meta.id, context);

        log("FTS timer (30s) scheduled for doc: " + meta.id);

    } catch (e) {
        log("Error scheduling timer: " + e);
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
