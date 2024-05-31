function OnUpdate(doc, meta) {
    let vectArr=doc.vector_data;
    var success=true;
    if (vectArr!=null) {
        var x = couchbase.base64Float64ArrayEncode(vectArr)
        var y = couchbase.base64Float64ArrayDecode(x)
        if (JSON.stringify(vectArr) != JSON.stringify(y)) {
            success = false;
        } else {
            var x = couchbase.base64Float32ArrayEncode(vectArr)
            var y = couchbase.base64Float32ArrayDecode(x)
            if (JSON.stringify(vectArr) != JSON.stringify(y)) {
                success = false;
            } else {
                var x = couchbase.base64Encode(vectArr)
                var y = couchbase.base64Decode(x)
                if (JSON.stringify(vectArr) != y) {
                    success = false;
                }
            }
        }
    }
    if (success){
        dst_bucket[meta.id]="success"
    }
}

function OnDelete(meta, options) {
    log("Doc deleted/expired", meta.id);
}