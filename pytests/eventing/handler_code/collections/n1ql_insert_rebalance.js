function OnUpdate(doc,meta) {
    var docId = meta.id;
    var query = INSERT INTO dst_bucket.scope_1.coll_4 ( KEY, VALUE ) VALUES ( $docId ,'Hello World');
}

function OnDelete(meta,options){
    delete dst_bucket[meta.id];
}