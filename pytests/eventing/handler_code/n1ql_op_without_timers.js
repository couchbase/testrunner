function OnUpdate(doc, meta) {
    var docID= meta.id;
    // Adding these extra comments to validate MB-30240
    log('Before Inserting document', doc);
    log('Before Inserting document', doc);
    log('Before Inserting document', doc);
    while (true) {
    try {
    var query = INSERT INTO dst_bucket ( KEY, VALUE ) VALUES ( $docID ,'N1QL op');
     break;
    } catch (e) {
        log(e);
        }
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
    while (true) {
    try {
        var query = DELETE FROM dst_bucket where meta().id = $docID;
        break;
    } catch (e) {
        log(e);
        }
    }
    // Adding these extra comments to validate MB-30240
    log('After Deleting document', docID);
}
