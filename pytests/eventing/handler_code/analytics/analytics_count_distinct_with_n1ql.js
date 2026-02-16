function OnUpdate(doc, meta) {
    var step = "init";
    try {
        // --- Step 1: Analytics COUNT(DISTINCT) query ---
        step = "analytics_count_distinct";
        log("Processing doc:", meta.id);

        var countDistinctIt = couchbase.analyticsQuery(
            "SELECT COUNT(DISTINCT country) AS distinct_country_count FROM `travel-sample`.`inventory`.`airline`;"
        );
        var analyticsResult = null;
        for (let row of countDistinctIt) {
            analyticsResult = row;
        }

        var distinctCount = analyticsResult ? analyticsResult.distinct_country_count : 0;
        log("Analytics COUNT(DISTINCT) returned:", distinctCount);

        // --- Step 2: N1QL query ---
        step = "n1ql_query";
        var n1qlCount = 0;
        var n1qlRows = [];

        var n1qlIt = N1QL(
            "SELECT COUNT(*) AS total FROM `travel-sample`.`inventory`.`airline` WHERE country IS NOT NULL;"
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
            query_type: "count_distinct_with_n1ql",
            distinct_country_count: distinctCount,
            data: analyticsResult,
            n1ql_rows: n1qlCount,
            n1ql_data: n1qlRows,
            coexistence: (distinctCount > 0 && n1qlCount > 0),
            processed_at: new Date().toISOString()
        };

        log("Analytics COUNT(DISTINCT) + N1QL coexistence test complete for:", meta.id);

    } catch (e) {
        log("Error at step [" + step + "]:", e);
    }
}
