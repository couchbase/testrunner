function OnUpdate(doc, meta) {
  log("Doc created/updated", meta.id);
}

function OnDelete(meta, options) {
  log("Doc deleted/expired", meta.id);
}

function OnDeploy(action) {
  var randomId = "doc_" + Math.random().toString();
  var meta = { id: randomId };

  dst_bucket[meta.id] = 'basic bucket op';
  var doc_read = dst_bucket[meta.id]; 

  var startTime = Date.now();
  var duration = 50000;

  while (Date.now() - startTime < duration) {
  }

  log("Operation complete for doc with id:", meta.id);
}
