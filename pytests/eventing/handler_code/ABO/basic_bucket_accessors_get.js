function OnUpdate(doc, meta) {
    try {
      // Bucket GET operation
      var val = src_bucket[meta.id];

      // Explicit validation for GET
      if (val === null || val === undefined) {
        throw new Error("GET failed: document not found for key " + meta.id);
      }
      log("GET operation successful for key:", meta.id);

      // Bucket SET operation
      dst_bucket[meta.id] = "get successful";

    } catch (e) {
      // Fail fast and make the failure visible
      log("Eventing function failed for key:", meta.id, "Error:", e.message);
      throw e;
    }
}

function OnDelete(meta, options) {
  delete dst_bucket[meta.id];
  log("Doc deleted/expired", meta.id);
}