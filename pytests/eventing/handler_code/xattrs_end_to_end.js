function arraysAreEqual(arr1, arr2, tolerance = 1e-10) {
    return arr1.length === arr2.length && arr1.every((value, index) => Math.abs(value - arr2[index]) <= tolerance);
}

function OnUpdate(doc, meta,xattrs) {
    var meta = {"id": meta.id};
    couchbase.insert(dst_bucket, meta, doc);
    var request={
        body:{"texts":doc}
    };
    var response = curl("POST", server, request);
    if(response.status == 200){
        var encodedData=couchbase.base64Float64ArrayEncode(response.body)
        var result=couchbase.mutateIn(dst_bucket, meta, [couchbase.MutateInSpec.upsert("encodedData",encodedData , { "xattrs": true })]);
        if (result.success){
            result= couchbase.lookupIn(dst_bucket, meta, [couchbase.LookupInSpec.get("encodedData", { "xattrs": true })]);
            if(result.success&&result.doc[0].success){
                var decodedData=couchbase.base64Float64ArrayDecode(result.doc[0].value)
                if (arraysAreEqual(response.body,decodedData)){
                    dst_bucket[meta.id+"test"]="success"
                }
            }
        }

    }
}

function OnDelete(meta, options) {
    log("Doc deleted/expired", meta.id);
}