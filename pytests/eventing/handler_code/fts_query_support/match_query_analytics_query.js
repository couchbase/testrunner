function OnUpdate(doc, meta) {
    try {
        log("Processing doc:", meta.id);
        var matchQuery = couchbase.SearchQuery
            .match("location hostel")
            .operator("and")
            .field("reviews.content");

        var ftsCount = 0;
        var ftsIds = [];
        var ftsIt = couchbase.searchQuery(
            "travel_sample_test",
            matchQuery,
            { size: 10 }
        );

        for (let row of ftsIt) {
            ftsCount++;
            ftsIds.push(row.id);
        }

        log("FTS hits:", ftsCount);

        var limit = 4;
        var analyticsCount = 0;
        var analyticsIt = couchbase.analyticsQuery(
            "SELECT * FROM `travel-sample`.`inventory`.`airline` LIMIT $limit;",
            { "limit": limit }
        );

        for (let row of analyticsIt) {
            analyticsCount++;
        }

        log("Analytics rows:", analyticsCount);
        dst_bucket[meta.id] = {
            source_doc: meta.id,
            fts_hits: ftsCount,
            fts_ids_sample: ftsIds,
            analytics_rows: analyticsCount,
            coexistence_test: (ftsCount > 0 && analyticsCount === limit),
            processed_at: new Date().toISOString()
        };

        log("FTS + Analytics test complete for:", meta.id);

    } catch (e) {
        log("Error:", e);
    }
}
