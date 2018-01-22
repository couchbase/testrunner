function OnUpdate(doc,meta) {
    try {
        var docId = new Date();
        sleep(1000);
        var insert_ = INSERT INTO `src_bucket` ( KEY, VALUE ) VALUES ( $docId , $docId);
        insert_.execQuery();
        dst_bucket[docId]=new Date();
    } catch (e) {
        log(e);
    }
}

function sleep(milliseconds) {
  var start = new Date().getTime();
  for (var i = 0; i < 1e7; i++) {
    if ((new Date().getTime() - start) > milliseconds){
      break;
    }
  }
}