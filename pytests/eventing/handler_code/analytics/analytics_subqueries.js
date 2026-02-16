function OnUpdate(doc, meta) {
    try {
        log("Processing doc:", meta.id);

        var subqueryIt = couchbase.analyticsQuery(
            "SELECT name, country FROM `travel-sample`.`inventory`.`airline` " +
            "WHERE country IN (SELECT VALUE r.country FROM `travel-sample`.`inventory`.`airline` r " +
            "GROUP BY r.country HAVING COUNT(*) > 10);"
        );
        var rows = [];
        for (let row of subqueryIt) {
            rows.push(row);
        }

        log("Subquery returned", rows.length, "rows");

        dst_bucket[meta.id] = {
            doc_id: meta.id,
            query_type: "subquery",
            row_count: rows.length,
            data: rows,
            success: (rows.length > 0),
            processed_at: new Date().toISOString()
        };

        log("Subquery test complete for:", meta.id);

    } catch (e) {
        log("Error:", e);
    }
}