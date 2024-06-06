function OnUpdate(doc, meta, xattrs){
    try {
       dst_bucket[meta.id]=couchbase.base64Encode(doc)

    }
    catch (e) {
        log(e)
    }
}
function OnDelete(meta,options){
  var doc={"id":meta.id}
  var result = couchbase.delete(dst_bucket,doc);
}