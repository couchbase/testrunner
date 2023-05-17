function OnUpdate(doc,meta) {
    var docId = meta.id;
    var query = INSERT INTO default.scope0.collection1 ( KEY, VALUE ) VALUES ( $docId ,'Hello World');
}