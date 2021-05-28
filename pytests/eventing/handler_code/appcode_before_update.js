function OnUpdate(doc, meta) {
    dst_bucket[meta.id] = doc;
}