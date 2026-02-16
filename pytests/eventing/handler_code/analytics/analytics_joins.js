function OnUpdate(doc, meta) {
    try {
        log("Processing doc:", meta.id);

        var joinIt = couchbase.analyticsQuery(
            "SELECT r.airline, a.name AS airline_name, COUNT(*) AS route_count " +
            "FROM `travel-sample`.`inventory`.`route` r " +
            "JOIN `travel-sample`.`inventory`.`airline` a ON r.airline = a.iata " +
            "GROUP BY r.airline, a.name ORDER BY route_count DESC LIMIT 5;"
        );
        var rows = [];
        for (let row of joinIt) {
            rows.push(row);
        }

        log("JOIN query returned", rows.length, "rows");

        dst_bucket[meta.id] = {
            doc_id: meta.id,
            query_type: "join_query",
            row_count: rows.length,
            data: rows,
            success: (rows.length > 0),
            processed_at: new Date().toISOString()
        };

        log("JOIN query test complete for:", meta.id);

    } catch (e) {
        log("Error:", e);
    }
}