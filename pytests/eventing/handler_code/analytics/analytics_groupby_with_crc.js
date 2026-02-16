function OnUpdate(doc, meta) {
    var step = "init";
    try {
        // --- Step 1: Analytics GROUP BY query ---
        step = "analytics_groupby";
        log("Processing doc:", meta.id);

        var groupByIt = couchbase.analyticsQuery(
            "SELECT country, COUNT(*) AS cnt FROM `travel-sample`.`inventory`.`airline` GROUP BY country ORDER BY cnt DESC LIMIT 5;"
        );
        var rows = [];
        for (let row of groupByIt) {
            rows.push(row);
        }

        log("GROUP BY returned", rows.length, "rows");

        // --- Step 2: CRC64 checksum of analytics results ---
        step = "crc64_checksum";
        var resultsString = JSON.stringify(rows);
        var resultsCrc = couchbase.crc_64_go_iso(resultsString);
        log("CRC64 of GROUP BY results:", resultsCrc);

        // CRC64 of the source document
        var docCrc = couchbase.crc_64_go_iso(JSON.stringify(doc));
        log("CRC64 of source doc:", docCrc);

        // --- Step 3: Write result ---
        step = "write_result";
        dst_bucket[meta.id] = {
            doc_id: meta.id,
            query_type: "groupby_with_crc",
            row_count: rows.length,
            analytics_crc64: resultsCrc,
            source_doc_crc64: docCrc,
            data: rows,
            coexistence: (rows.length > 0 && resultsCrc.length > 0),
            processed_at: new Date().toISOString()
        };

        log("Analytics GROUP BY + CRC64 test complete for:", meta.id);

    } catch (e) {
        log("Error at step [" + step + "]:", e);
    }
}
