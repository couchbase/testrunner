function OnUpdate(doc, meta) {
    try {
        log("Starting FTS + xattrs test for doc:", meta.id);

        // Run FTS match query
        var matchQuery = couchbase.SearchQuery
            .match("location hostel")
            .operator("and")
            .field("reviews.content");

        var ftsIt = couchbase.searchQuery(
            "travel_sample_test",
            matchQuery,
            { size: 5 }
        );

        var ftsIds = [];
        for (let row of ftsIt) {
            ftsIds.push(row.id);
        }

        log("FTS returned", ftsIds.length, "hits");

        // Insert a base document into dst_bucket for xattr operations
        var docMeta = { "id": meta.id };
        couchbase.insert(dst_bucket, docMeta, {
            source_doc: meta.id,
            fts_hit_count: ftsIds.length
        });

        // Write FTS results as an xattr on the destination document
        var mutateResult = couchbase.mutateIn(dst_bucket, docMeta, [
            couchbase.MutateInSpec.upsert("fts_meta.hit_count", ftsIds.length, { "create_path": true, "xattrs": true }),
            couchbase.MutateInSpec.upsert("fts_meta.ids", ftsIds, { "create_path": true, "xattrs": true }),
            couchbase.MutateInSpec.upsert("fts_meta.query_term", "location hostel", { "create_path": true, "xattrs": true })
        ]);

        if (!mutateResult.success) {
            log("mutateIn failed for doc:", meta.id, mutateResult);
            return;
        }

        log("xattrs written successfully for doc:", meta.id);

        // Read back the xattrs using lookupIn to verify they exist
        var lookupResult = couchbase.lookupIn(dst_bucket, docMeta, [
            couchbase.LookupInSpec.get("fts_meta.hit_count", { "xattrs": true }),
            couchbase.LookupInSpec.get("fts_meta.ids", { "xattrs": true }),
            couchbase.LookupInSpec.get("fts_meta.query_term", { "xattrs": true })
        ]);

        if (!lookupResult.success) {
            log("lookupIn failed for doc:", meta.id, lookupResult);
            return;
        }

        // Validate that the xattr values match what we wrote
        var storedCount = lookupResult.doc[0].value;
        var storedIds = lookupResult.doc[1].value;
        var storedTerm = lookupResult.doc[2].value;

        var countMatch = (storedCount === ftsIds.length);
        var idsMatch = (JSON.stringify(storedIds) === JSON.stringify(ftsIds));
        var termMatch = (storedTerm === "location hostel");

        var allPassed = countMatch && idsMatch && termMatch
            && lookupResult.doc[0].success
            && lookupResult.doc[1].success
            && lookupResult.doc[2].success;

        // Write the final test result document
        dst_bucket[meta.id + "_xattr_result"] = {
            test: "FTS match_query + xattrs",
            fts_hits: ftsIds.length,
            xattr_write: mutateResult.success,
            xattr_read: lookupResult.success,
            count_match: countMatch,
            ids_match: idsMatch,
            term_match: termMatch,
            status: allPassed ? "PASSED" : "FAILED"
        };

        log("FTS + xattrs test", allPassed ? "PASSED" : "FAILED", "for doc:", meta.id);

    } catch (e) {
        log("Error in FTS + xattrs test:", e);
        dst_bucket["fts_xattr_error::" + meta.id] = {
            error: e.toString(),
            status: "FAILED"
        };
    }
}
