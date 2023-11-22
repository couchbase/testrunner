function OnUpdate(doc, meta) {
    meta = {"id": meta.id};
    couchbase.insert(dst_bucket, meta, doc);

    couchbase.mutateIn(dst_bucket, meta, [
        couchbase.MutateInSpec.insert("testField", "insert")
    ]);
    doc = dst_bucket[meta.id];
    if (doc.testField != "insert") {
        return;
    }

    couchbase.mutateIn(dst_bucket, meta, [
        couchbase.MutateInSpec.replace("testField", "replace")
    ]);
    doc = dst_bucket[meta.id];
    if (doc.testField != "replace") {
        return;
    }

    couchbase.mutateIn(dst_bucket, meta, [
        couchbase.MutateInSpec.remove("testField")
    ]);
    doc = dst_bucket[meta.id];
    if ("testField" in doc) {
        return;
    }

    couchbase.mutateIn(dst_bucket, meta, [
        couchbase.MutateInSpec.upsert("arrayTest", []),
        couchbase.MutateInSpec.arrayAppend("arrayTest", 2),
        couchbase.MutateInSpec.arrayPrepend("arrayTest", 1),
        couchbase.MutateInSpec.arrayInsert("arrayTest[0]", 0),
        couchbase.MutateInSpec.arrayAddUnique("arrayTest", 3)
    ]);
    doc = dst_bucket[meta.id];
    var array = doc["arrayTest"];
    if (array.length != 4) {
        return;
    }
    for(var i = 0; i < 4; i++) {
        if(array[i] != i) {
            return;
        }
    }
}

function OnDelete(meta) {
    delete dst_bucket[meta.id];
}