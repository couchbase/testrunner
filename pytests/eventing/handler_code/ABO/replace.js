function OnUpdate(doc, meta) {
    log("Doc created/updated", meta.id);
    var result = couchbase.get(src_bucket,meta);
    log(result);

    if(result.success && result.meta.cas != undefined &&
        JSON.stringify(result.doc) === JSON.stringify(doc)){
            var meta_data={"id": meta.id,"cas":result.meta.cas};
            var result1 = couchbase.replace(src_bucket,meta_data,doc);
            log(result1);
            if(result1.success){
                var result2= couchbase.insert(dst_bucket,meta_data,doc);
                log(result2);
        }
    }
}

function OnDelete(meta, options) {
    log("Doc deleted/expired", meta.id);
    var result = couchbase.get(dst_bucket,meta);
    log(result);
    if(result.success && result.meta.cas != undefined){
            var meta_data={"id": meta.id,"cas":result.meta.cas};
            var result1= couchbase.replace(dst_bucket,meta_data,doc);
            log(result1);
            if(result1.success){
                var doc={"id":meta.id};
                var result2= couchbase.delete(dst_bucket,doc);
                log(result2);
        }
    }
}
