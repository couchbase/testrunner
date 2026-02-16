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

        // --- Step 2: Advanced bucket ops — upsert, get, delete, upsert ---
        step = "bucket_upsert";
        var tempDocId = "analytics_groupby_temp_" + meta.id;
        var tempMeta = { "id": tempDocId };

        // Upsert a temp document with analytics data
        couchbase.upsert(dst_bucket, tempMeta, {
            query_type: "groupby",
            row_count: rows.length,
            groupby_data: rows,
            created_at: new Date().toISOString()
        });

        // Read it back
        step = "bucket_get";
        var getResult = couchbase.get(dst_bucket, tempMeta);
        if (!getResult.success) {
            throw "Failed to get temp doc: " + tempDocId;
        }
        var storedCount = getResult.doc.row_count;
        log("Read back row_count:", storedCount);

        // Delete the temp doc
        step = "bucket_delete";
        couchbase.delete(dst_bucket, tempMeta);

        // --- Step 3: Write final result ---
        step = "write_result";
        dst_bucket[meta.id] = {
            doc_id: meta.id,
            query_type: "groupby_with_bucket_op",
            row_count: rows.length,
            bucket_ops_verified: (storedCount === rows.length),
            data: rows,
            coexistence: (rows.length > 0 && storedCount === rows.length),
            processed_at: new Date().toISOString()
        };

        log("Analytics GROUP BY + Bucket Ops test complete for:", meta.id);

    } catch (e) {
        log("Error at step [" + step + "]:", e);
    }
}
