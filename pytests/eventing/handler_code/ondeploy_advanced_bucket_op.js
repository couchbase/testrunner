function OnUpdate(doc, meta) {
  log("Doc created/updated", meta.id);
}

function OnDelete(meta, options) {
  log("Doc deleted/expired", meta.id);
}

function OnDeploy(action) {
  var success = 0;

  // Basic Bucket Operations
  function basicBucketOperations() {
    var docId = "basic_doc";
    dst_bucket[docId] = 'hello world';

    var doc_read = dst_bucket[docId];
    if (doc_read == 'hello world') {
      success++;
    }
  }

  // Advanced Bucket Operations (GET)
  function advancedBucketOperations() {
    var meta = { "id": "advanced_doc" };
    dst_bucket[meta.id] = "Created doc";
    var res = couchbase.get(dst_bucket, meta);

    if (res.success) {
      success++;
    }
  }

  // Bucket Cache Operations
  function bucketCacheOperations() {
    var meta = { "id": "cache_doc" };
    dst_bucket[meta.id] = "Created doc";
    var start_long = new Date();
    for (var i = 0; i < 10000; i++) {
      couchbase.get(dst_bucket, meta);
    }
    var long = new Date() - start_long;
    var start_short = new Date();
    for (var i = 0; i < 10000; i++) {
      couchbase.get(dst_bucket, meta, { "cache": true });
    }

    var short = new Date() - start_short;
    log("Long", long, "Short", short, "Ratio", long / short);
    if (long / short > 2) {
      dst_bucket[meta.id] = 'success';
    }
    success++;
  }

  // Subdocument Operations
  function subDocOperations() {
    var meta = { "id": "subdoc" };
    dst_bucket[meta.id] = {};
    couchbase.mutateIn(dst_bucket, meta, [
      couchbase.MutateInSpec.insert("testField", "insert")
    ]);
    var doc = dst_bucket[meta.id];
    if (doc.testField !== "insert") {
      log(doc);
      return;
    }
    couchbase.mutateIn(dst_bucket, meta, [
      couchbase.MutateInSpec.replace("testField", "replace")
    ]);
    doc = dst_bucket[meta.id];
    if (doc.testField !== "replace") {
      log(doc);
      return;
    }

    // Remove field "testField"
    couchbase.mutateIn(dst_bucket, meta, [
      couchbase.MutateInSpec.remove("testField")
    ]);
    doc = dst_bucket[meta.id];
    if ("testField" in doc) {
      log(doc);
      return;
    }

    // Perform multiple array operations on "arrayTest"
    couchbase.mutateIn(dst_bucket, meta, [
      couchbase.MutateInSpec.upsert("arrayTest", []),
      couchbase.MutateInSpec.arrayAppend("arrayTest", 2),
      couchbase.MutateInSpec.arrayPrepend("arrayTest", 1),
      couchbase.MutateInSpec.arrayInsert("arrayTest[0]", 0),
      couchbase.MutateInSpec.arrayAddUnique("arrayTest", 3)
    ]);
    doc = dst_bucket[meta.id];
    var array = doc["arrayTest"];

    // Check if array operations are successful
    if (array.length !== 4) {
      log(doc);
      return;
    }

    // Verify array order and elements
    for (var i = 0; i < 4; i++) {
      if (array[i] !== i) {
        log(doc);
        return;
      }
    }
    success++;
  }

  // Perform all the operations
  basicBucketOperations();
  advancedBucketOperations();
  bucketCacheOperations();
  subDocOperations();

  // Log result based on success count
  if (success === 4) {
    log("All operations completed successfully.");
  } else {
    log("Some operations failed. Success count:", success);
  }
}
