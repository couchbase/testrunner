function OnUpdate(doc,meta)
{
    var docId = meta.id;
    var query = INSERT INTO dst_bucket._default._default ( KEY, VALUE ) VALUES ( $docId ,'Hello World');
}

function OnDelete(meta,options)
{
    var id = meta.id;
    DELETE from dst_bucket._default._default where META().id = $id;
}