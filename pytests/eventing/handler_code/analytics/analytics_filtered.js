function OnUpdate(doc, meta) {
    try {
        log("Processing doc:", meta.id);

        var filteredIt = couchbase.analyticsQuery(
            "SELECT name, country FROM `travel-sample`.`inventory`.`airline` WHERE country = $country LIMIT 10;",
            { "country": "United States" }
        );
        var rows = [];
        for (let row of filteredIt) {
            rows.push(row);
        }

        log("Filtered query returned", rows.length, "rows");

        dst_bucket[meta.id] = {
            doc_id: meta.id,
            query_type: "filtered_where",
            row_count: rows.length,
            data: rows,
            success: (rows.length > 0),
            processed_at: new Date().toISOString()
        };

        log("Filtered WHERE test complete for:", meta.id);

    } catch (e) {
        log("Error:", e);
    }
}