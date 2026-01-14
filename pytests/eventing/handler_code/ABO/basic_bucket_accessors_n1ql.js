function OnUpdate(doc, meta, xattrs) {
    log("Executing N1QL Query");
       var val = src_bucket[meta.id];
        // Explicit validation for GET
       if (val === null || val === undefined) {
           throw new Error("GET failed: document not found for key " + meta.id);
       }
       log("GET operation successful for key:", meta.id);
        try {
            var docId = meta.id;
            var query_n1ql = INSERT INTO default.scope0.collection1 ( KEY, VALUE ) VALUES ( $docId ,'Executing N1QL Query');
        } catch (e) {
            log("Error executing N1QL query:", e);
        }
}

function OnDelete(meta, options) {
    delete dst_bucket[meta.id];
    log("Doc deleted/expired", meta.id);
}
