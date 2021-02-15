function OnUpdate(doc,meta) {
    var docId = meta.id;
    var query = INSERT INTO dst_bucket.scope_1.coll_4 ( KEY, VALUE ) VALUES ( $docId ,'Hello World');
}

function OnDelete(meta,options){
    var id=meta.id;
    delete from dst_bucket.scope_1.coll_4 where META().id=$id;
}