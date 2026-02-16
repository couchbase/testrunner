function OnUpdate(doc, meta) {
    try {
        log("Processing doc:", meta.id);

        var limit = 5;
        var analyticsCount = 0;
        var sampleRows = [];

        var analyticsIt = couchbase.analyticsQuery(
            "SELECT name, country FROM `travel-sample`.`inventory`.`airline` LIMIT $limit;",
            { "limit": limit }
        );

        for (let row of analyticsIt) {
            analyticsCount++;
            sampleRows.push(row);
        }

        log("Analytics returned", analyticsCount, "rows");

        dst_bucket[meta.id] = {
            doc_id: meta.id,
            analytics_rows: analyticsCount,
            data: sampleRows,
            success: (analyticsCount === limit),
            processed_at: new Date().toISOString()
        };

        log("Analytics sanity test complete for:", meta.id);

    } catch (e) {
        log("Error: ", e);
    }
}
