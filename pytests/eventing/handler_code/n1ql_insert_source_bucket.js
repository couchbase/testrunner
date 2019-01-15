function OnUpdate(doc, meta) {
    log('docId', meta.id);
    var query= INSERT into src_bucket (KEY, VALUE) VALUES ("key1", { "type" : "hotel", "name" : "new hotel" });
}
function OnDelete(meta) {
}