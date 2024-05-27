function OnUpdate(doc, meta, xattrs){
    var failWholeTest=true
    try{
    var data={"data":couchbase.base64Encode(doc)}
    var result1= couchbase.insert(dst_bucket,meta,data);
    var result = couchbase.get(dst_bucket, meta);
    if (result.success) {
        var decodedResult=couchbase.base64Decode(result.doc.data)
        var cleanedJSON=decodedResult.replace(/\\/g, '')
        if (JSON.stringify(doc)==cleanedJSON){
            failWholeTest=false
            log("Documents matched")
        }
        else{

            log("Documents dont match",JSON.stringify(doc),cleanedJSON)
        }
    } else {
        log('Document FETCH FAILED', new_meta.id, 'result',result);
    }}
    catch (e) {
        log(e)
    }
    if (!failWholeTest){
        dst_bucket[meta.id+"98797"]="success"
    }
}
function OnDelete(doc,meta,xattrs){}