function OnUpdate(doc, meta) {
    var step = "init";
    try {
        // --- Step 1: Analytics subquery ---
        step = "analytics_subquery";
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

        // --- Step 2: CRC64 checksum of analytics results ---
        step = "crc64_checksum";
        var resultsString = JSON.stringify(rows);
        var resultsCrc = couchbase.crc_64_go_iso(resultsString);
        log("CRC64 of subquery results:", resultsCrc);

        // CRC64 of the source document
        var docCrc = couchbase.crc_64_go_iso(JSON.stringify(doc));
        log("CRC64 of source doc:", docCrc);

        // --- Step 3: Write result ---
        step = "write_result";
        dst_bucket[meta.id] = {
            doc_id: meta.id,
            query_type: "subqueries_with_crc",
            row_count: rows.length,
            analytics_crc64: resultsCrc,
            source_doc_crc64: docCrc,
            data: rows,
            coexistence: (rows.length > 0 && resultsCrc.length > 0),
            processed_at: new Date().toISOString()
        };

        log("Analytics Subquery + CRC64 test complete for:", meta.id);

    } catch (e) {
        log("Error at step [" + step + "]:", e);
    }
}
