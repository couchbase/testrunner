function OnUpdate(doc, meta) {
  log("Doc created/updated", meta.id);
}

function OnDelete(meta, options) {
  log("Doc deleted/expired", meta.id);
}

function OnDeploy(action) {
  var randomId = "doc_" + Math.random().toString()

  var meta = { id: randomId };  // Set the random id to meta.id

  dst_bucket[meta.id] = 'basic bucket op';
  var doc_read = dst_bucket[meta.id];

  log("Operation complete for doc with id:", meta.id);
}
