function OnUpdate(doc, meta) {
    try {
        log("Testing FTS + N1QL for doc: " + meta.id);

        /* 1. FTS query */
        var matchQuery = couchbase.SearchQuery.match("location hostel")
            .operator("and")
            .field("reviews.content")
            .analyzer("standard")
            .fuzziness(2)
            .prefixLength(4);

        var ids = runQuery(matchQuery, { size: 5 });
        var ftsCount = ids.length;
        log("FTS returned " + ftsCount + " hits");

        /* 2. N1QL SELECT from travel-sample (read-only, no recursion guard) */
        var n1qlCount = 0;
        var rows = [];
        var results = SELECT name, country FROM `travel-sample`.`_default`.`_default`
                      WHERE type = "airline" LIMIT 5;
        for (var row of results) {
            rows.push(row);
            n1qlCount++;
        }
        results.close();
        log("N1QL returned " + n1qlCount + " rows");

        /* 3. Write combined result via bucket binding */
        dst_bucket["fts_n1ql_" + meta.id] = {
            fts_hits: ftsCount,
            fts_ids: ids,
            n1ql_rows: n1qlCount,
            n1ql_data: rows,
            coexistence: (ftsCount > 0 && n1qlCount > 0),
            source_doc: meta.id
        };
        log("Wrote FTS+N1QL result for doc: " + meta.id);

    } catch (e) {
        log("Error testing FTS + N1QL: " + e);
    }
}
/* Helper function to run a query and return matching doc IDs */
function runQuery(query, options) {
    var it = couchbase.searchQuery("travel_sample_test", query, options);
    var ids = [];
    for (let row of it) {
        ids.push(row.id);
    }
    return ids;
}
