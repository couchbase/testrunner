function OnUpdate(doc, meta) {
    log('document', doc);
    let doc_id = meta.id;
    try {
        UPDATE `dst_bucket` as b SET META(b).expiration = 10 WHERE META(b).id = $doc_id;
    }
    catch(e){
        log(e);
    }

}
