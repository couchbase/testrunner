function OnUpdate(doc,meta) {
    var docId = meta.id;
    var query = INSERT INTO dst_bucket ( KEY, VALUE ) VALUES ( :docId ,'Hello World');
    query.execQuery();
}