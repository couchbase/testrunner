function OnUpdate(doc, meta) {
    var docID= meta.id;
    // Adding these extra comments to validate MB-30240
    log('Before Inserting document', doc);
    log('Before Inserting document', doc);
    log('Before Inserting document', doc);
    try {
    var query = INSERT INTO dst_bucket ( KEY, VALUE ) VALUES ( $docID ,'N1QL op');
    }catch (e) {
        log(e);
    }
    // Adding these extra comments to validate MB-30240
    log('After Inserting document', doc);
    log('After Inserting document', doc);
    log('After Inserting document', doc);
}

function OnDelete(meta) {
    var docID = meta.id;
    // Adding these extra comments to validate MB-30240
    log('Before Deleting document', docID);
    try {
        var query = DELETE FROM dst_bucket where meta().id = $docID;
    } catch (e) {
        log(e);
        var obj = JSON.parse(e.message);
        var code=obj['errors'][0]['code'];
        log(code);
        if(code == 12011){
            log('retry');
            query = DELETE FROM dst_bucket where meta().id = $docID;
        }
    }

    // Adding these extra comments to validate MB-30240
    log('After Deleting document', docID);
}
