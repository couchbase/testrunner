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

        // --- Step 2: Insert a base document into dst_bucket ---
        step = "insert_base_doc";
        var docMeta = { "id": meta.id };
        couchbase.insert(dst_bucket, docMeta, {
            source_doc: meta.id,
            analytics_hit_count: rows.length,
            data: rows
        });

        // --- Step 3: Write analytics metadata as xattrs ---
        step = "write_xattrs";
        var airlineNames = rows.map(function(r) { return r.name; });
        var countries = rows.map(function(r) { return r.country; });

        var mutateResult = couchbase.mutateIn(dst_bucket, docMeta, [
            couchbase.MutateInSpec.upsert("analytics_meta.row_count", rows.length,
                { "create_path": true, "xattrs": true }),
            couchbase.MutateInSpec.upsert("analytics_meta.airline_names", airlineNames,
                { "create_path": true, "xattrs": true }),
            couchbase.MutateInSpec.upsert("analytics_meta.countries", countries,
                { "create_path": true, "xattrs": true }),
            couchbase.MutateInSpec.upsert("analytics_meta.query_type", "subquery",
                { "create_path": true, "xattrs": true })
        ]);

        if (!mutateResult.success) {
            log("mutateIn failed for doc:", meta.id, mutateResult);
            return;
        }

        // --- Step 4: Read back xattrs to verify ---
        step = "read_xattrs";
        var lookupResult = couchbase.lookupIn(dst_bucket, docMeta, [
            couchbase.LookupInSpec.get("analytics_meta.row_count", { "xattrs": true }),
            couchbase.LookupInSpec.get("analytics_meta.airline_names", { "xattrs": true }),
            couchbase.LookupInSpec.get("analytics_meta.query_type", { "xattrs": true })
        ]);

        if (!lookupResult.success) {
            log("lookupIn failed for doc:", meta.id, lookupResult);
            return;
        }

        var storedCount = lookupResult.doc[0].value;
        var storedNames = lookupResult.doc[1].value;
        var storedType = lookupResult.doc[2].value;

        var countMatch = (storedCount === rows.length);
        var namesMatch = (JSON.stringify(storedNames) === JSON.stringify(airlineNames));
        var typeMatch = (storedType === "subquery");
        var allPassed = countMatch && namesMatch && typeMatch;

        // --- Step 5: Write final test result ---
        step = "write_result";
        dst_bucket[meta.id + "_xattr_result"] = {
            test: "Analytics Subquery + xattrs coexistence",
            analytics_rows: rows.length,
            xattr_write: mutateResult.success,
            xattr_read: lookupResult.success,
            count_match: countMatch,
            names_match: namesMatch,
            type_match: typeMatch,
            status: allPassed ? "PASSED" : "FAILED"
        };

        log("Analytics Subquery + xattrs test", allPassed ? "PASSED" : "FAILED",
            "for:", meta.id);

    } catch (e) {
        log("Error at step [" + step + "]:", e);
    }
}
