function OnUpdate(doc, meta) {
    var docID= meta.id;
    var query = INSERT INTO dst_bucket ( KEY, VALUE ) VALUES ( $docID ,'N1QL op');
}

function OnDelete(meta) {
    var docID = meta.id;
    var query = DELETE FROM dst_bucket where meta().id = $docID;
}
