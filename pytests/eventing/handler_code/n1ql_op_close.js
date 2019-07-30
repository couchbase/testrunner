function OnUpdate(doc, meta) {
    log('docId', meta.id);
    var i=0;
    var query = SELECT name, (SELECT raw avg(s.ratings.Overall)
          FROM   t.reviews  as s)[0] AS overall_avg_rating
          FROM   `travel-sample` AS t
          WHERE type = "hotel"
          ORDER BY overall_avg_rating DESC;
    for(var q of query){
        dst_bucket[i] = q;
        i++;
        if(i==10){
            break;
        }
    }
    query.close();
    for(var q1 of query){
        dst_bucket[i] = q1;
        i++;
    }
    query.close();
}
function OnDelete(meta) {
    log('docId', meta.id);
    var i=0;
    var query = SELECT name, (SELECT raw avg(s.ratings.Overall)
          FROM   t.reviews  as s)[0] AS overall_avg_rating
          FROM   `travel-sample` AS t
          WHERE type = "hotel"
          ORDER BY overall_avg_rating DESC;
    for(var q of query){
        delete dst_bucket[i];
        i++;
        if(i==10){
            break;
        }
    }
    query.close();
    for(var q1 of query){
        delete dst_bucket[i];
        i++;
    }

}