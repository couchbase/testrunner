function OnUpdate(doc, meta) {
    var step = "init";
    try {
        // --- Step 1: Analytics JOIN query ---
        step = "analytics_joins";
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

        // --- Step 2: N1QL query ---
        step = "n1ql_query";
        var n1qlCount = 0;
        var n1qlRows = [];

        var n1qlIt = N1QL(
            "SELECT COUNT(*) AS total_routes FROM `travel-sample`.`inventory`.`route`;"
        );

        for (let row of n1qlIt) {
            n1qlCount++;
            n1qlRows.push(row);
        }

        log("N1QL returned", n1qlCount, "rows");

        // --- Step 3: Write coexistence result ---
        step = "write_result";
        dst_bucket[meta.id] = {
            doc_id: meta.id,
            query_type: "joins_with_n1ql",
            analytics_row_count: rows.length,
            n1ql_rows: n1qlCount,
            n1ql_data: n1qlRows,
            data: rows,
            coexistence: (rows.length > 0 && n1qlCount > 0),
            processed_at: new Date().toISOString()
        };

        log("Analytics JOIN + N1QL coexistence test complete for:", meta.id);

    } catch (e) {
        log("Error at step [" + step + "]:", e);
    }
}
