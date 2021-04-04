function OnUpdate(doc,meta) {
    var docId = meta.id;
    var query = INSERT INTO n1ql_op_dst.event.coll_0 ( KEY, VALUE ) VALUES ( $docId ,'Hello World');
}

function OnDelete(meta,options){
    var id=meta.id;
    delete from n1ql_op_dst.event.coll_0 where META().id=$id;
}