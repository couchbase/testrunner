function OnUpdate(doc, meta) {
  log("Doc created/updated", meta.id);
}

function OnDelete(meta, options) {
  log("Doc deleted/expired", meta.id);
}

function OnDeploy(action) {

  // NIQL Query
  var docId = meta.id;
  var query_n1ql = INSERT INTO default.scope0.collection1 ( KEY, VALUE ) VALUES ( $docId ,'Hello World');

  //Analytics Query
  var count = 0;
  const limit = 2;
  let query_analytics = couchbase.analyticsQuery('SELECT * FROM default LIMIT $limit;', {"limit": limit});
  for (let row of query_analytics) {
     ++count;
  }

  if (count === limit) {
    dst_bucket[meta.id] = 'analytics_query_executed';
  }
}
