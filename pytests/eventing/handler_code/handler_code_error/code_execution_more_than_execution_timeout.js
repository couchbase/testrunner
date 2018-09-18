function OnUpdate(doc,meta) {
  sleep(62000);
  dst_bucket[meta.id] = doc;
}

function sleep(milliseconds) {
  var start = new Date().getTime();
  for (var i = 0; i < 1e250; i++) {
    if ((new Date().getTime() - start) > milliseconds){
      break;
    }
  }
}