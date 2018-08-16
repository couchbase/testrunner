function OnUpdate(doc, meta) {
    var docID= meta.id;
    // Adding these extra comments to validate MB-30240
    log('Before Inserting document', doc);
    log('Before Inserting document', doc);
    log('Before Inserting document', doc);
    var query = INSERT INTO dst_bucket ( KEY, VALUE ) VALUES ( $docID ,'N1QL op');
    // Adding these extra comments to validate MB-30240
    log('After Inserting document', doc);
    log('After Inserting document', doc);
    log('After Inserting document', doc);
}

function OnDelete(meta) {
    var docID = meta.id;
    // Adding these extra comments to validate MB-30240
    log('Deleting document', docID);
    var query = DELETE FROM dst_bucket where meta().id = $docID;
    // Adding these extra comments to validate MB-30240
    log('Deleting document', docID);
}
