function OnUpdate(doc, meta)
{
    var docid = meta.id;
    var query= INSERT into src_bucket._default._default (KEY, VALUE) VALUES ( $docid, 'Couchbase');
}

function onDelete(meta, options)
{
    var id = meta.id;
    DELETE from src_bucket._default._default where META().id = $id;
}