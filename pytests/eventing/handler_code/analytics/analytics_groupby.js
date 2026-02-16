function OnUpdate(doc, meta) {
    try {
        log("Processing doc:", meta.id);

        var groupByIt = couchbase.analyticsQuery(
            "SELECT country, COUNT(*) AS cnt FROM `travel-sample`.`inventory`.`airline` GROUP BY country ORDER BY cnt DESC LIMIT 5;"
        );
        var rows = [];
        for (let row of groupByIt) {
            rows.push(row);
        }

        log("GROUP BY returned", rows.length, "rows");

        dst_bucket[meta.id] = {
            doc_id: meta.id,
            query_type: "group_by_aggregation",
            row_count: rows.length,
            data: rows,
            success: (rows.length > 0),
            processed_at: new Date().toISOString()
        };

        log("GROUP BY test complete for:", meta.id);

    } catch (e) {
        log("Error:", e);
    }
}