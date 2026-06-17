function OnUpdate(doc, meta) {
    try {

        var res = couchbase.lookupIn(
            src_col,
            { id: meta.id },
            [ couchbase.LookupInSpec.get("reviews.content") ]
        );

        if (!res.success) {
            log("No reviews.content field for doc:", meta.id);
            return;
        }

        var q = couchbase.SearchQuery
            .match("hostel")
            .field("reviews.content");

        var it = couchbase.searchQuery(
            "travel_sample_test",
            q,
            { size: 10 }
        );

        var hits = [];
        for (let row of it) {
            hits.push(row.id);
        }

        var dstMeta = { id: meta.id };

        couchbase.insert(dst_bucket, dstMeta, {
            original_doc: doc,
            fts_hits: hits.join(",")
        });

        couchbase.mutateIn(dst_bucket, dstMeta, [
            couchbase.MutateInSpec.insert("testField", "insert")
        ]);

        var updatedDoc = dst_bucket[meta.id];
        if (updatedDoc.testField !== "insert") {
            log("Insert validation failed:", meta.id);
            return;
        }

        couchbase.mutateIn(dst_bucket, dstMeta, [
            couchbase.MutateInSpec.replace("testField", "replace")
        ]);

        updatedDoc = dst_bucket[meta.id];
        if (updatedDoc.testField !== "replace") {
            log("Replace validation failed:", meta.id);
            return;
        }

        couchbase.mutateIn(dst_bucket, dstMeta, [
            couchbase.MutateInSpec.remove("testField")
        ]);

        updatedDoc = dst_bucket[meta.id];
        if ("testField" in updatedDoc) {
            log("Remove validation failed:", meta.id);
            return;
        }

        couchbase.mutateIn(dst_bucket, dstMeta, [
            couchbase.MutateInSpec.upsert("arrayTest", []),
            couchbase.MutateInSpec.arrayAppend("arrayTest", 2),
            couchbase.MutateInSpec.arrayPrepend("arrayTest", 1),
            couchbase.MutateInSpec.arrayInsert("arrayTest[0]", 0),
            couchbase.MutateInSpec.arrayAddUnique("arrayTest", 3)
        ]);

        updatedDoc = dst_bucket[meta.id];
        var array = updatedDoc.arrayTest;

        if (array.length !== 4) {
            log("Array length validation failed:", meta.id);
            return;
        }

        for (var i = 0; i < 4; i++) {
            if (array[i] !== i) {
                log("Array content validation failed:", meta.id);
                return;
            }
        }

        log(
            "FTS + mutation pipeline completed successfully",
            "doc:", meta.id,
            "fts hits:", hits.length
        );
    }
    catch (e) {
        log("Eventing error for doc:", meta.id, e);
    }
}