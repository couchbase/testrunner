function OnUpdate(doc, meta) {
    log("Doc created/updated", meta.id);
  }

  function OnDelete(meta, options) {
    log("Doc deleted/expired", meta.id);
  }

function OnDeploy(action){
    var a = 5;
    // b is undefined so the function won't be deployed
    var result = a + b;
    log(result);
}
