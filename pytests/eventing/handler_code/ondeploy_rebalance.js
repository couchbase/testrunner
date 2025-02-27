function OnUpdate(doc, meta) {
    log("Doc created/updated", meta.id);
  }

function OnDelete(meta, options) {
    log("Doc deleted/expired", meta.id);
  }

function OnDeploy(action) {
    for (var i = 0; i < 10000; i++) {
      var randomId = "doc_" + Math.random().toString(36)
      var meta = { id: randomId };
      dst_bucket[meta.id] = 'adding docs';
      if (i % 1000 === 0) {
        log("Operation complete for doc with id:", meta.id);
      }
    }
  }