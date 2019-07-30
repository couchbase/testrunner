function OnUpdate(doc, meta) {
    log('docId', meta.id);
    var query= SELECT t1.country, array_agg(t1.city), sum(t1.city_cnt) as apnum
      FROM (SELECT city, city_cnt, array_agg(airportname) as apnames, country
      FROM `travel-sample` WHERE type = "airport"
      GROUP BY city, country LETTING city_cnt = count(city) ) AS t1
      WHERE t1.city_cnt > 5
      GROUP BY t1.country;
    var i=0;
    for(var q of query){
        var query2 = SELECT name, (SELECT raw avg(s.ratings.Overall)
              FROM   t.reviews  as s)[0] AS overall_avg_rating
              FROM   `travel-sample` AS t
              WHERE type = "hotel"
              ORDER BY overall_avg_rating DESC
              LIMIT 3;
        for(var q of query2){
            dst_bucket[i] = q;
            i++;
        }
    }
}
function OnDelete(meta) {
    log('docId', meta.id);
    var query= SELECT t1.country, array_agg(t1.city), sum(t1.city_cnt) as apnum
      FROM (SELECT city, city_cnt, array_agg(airportname) as apnames, country
      FROM `travel-sample` WHERE type = "airport"
      GROUP BY city, country LETTING city_cnt = count(city) ) AS t1
      WHERE t1.city_cnt > 5
      GROUP BY t1.country;
    var i=0;
    for(var q of query){
        var query2 = SELECT name, (SELECT raw avg(s.ratings.Overall)
              FROM   t.reviews  as s)[0] AS overall_avg_rating
              FROM   `travel-sample` AS t
              WHERE type = "hotel"
              ORDER BY overall_avg_rating DESC
              LIMIT 3;
        for(var q of query2){
            delete dst_bucket[i];
            i++;
        }
    }
}