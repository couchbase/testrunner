function OnUpdate(doc,meta) {
    var docId = meta.id;
    var query = INSERT INTO dst_bucket ( KEY, VALUE ) VALUES ( $docId ,'Hello World');
}

function OnDelete(meta) {
    var docId = meta.id;
    try {
        var query = INSERT INTO dst_bucket ( KEY, VALUE ) VALUES ( $docId ,'Hello World');
    } catch (e) {
        log(e);
        if(e["message"].includes("LCB_ERR_AUTHENTICATION_FAILURE")){
            var query = DELETE FROM dst_bucket where meta().id = $docId;
        }
    }
}