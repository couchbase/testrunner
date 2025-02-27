function OnUpdate(doc, meta, xattrs) {
  log("Doc created/updated", meta.id);
}

function OnDelete(meta, options) {
  log("Doc deleted/expired", meta.id);
}

function OnDeploy(action) {
    log("OnDeploy function run", action);
    log("Executing N1QL Query");
    var randomId = "doc_" + Math.random().toString()
    var meta = { id: randomId };  // Set the random id to meta.id
    try {
        var docId = meta.id;
        var query_n1ql = INSERT INTO dst_bucket ( KEY, VALUE ) VALUES ( $docId ,'Executing N1QL Query');
    } catch (e) {
        log("Error executing N1QL query:", e);
    }
}