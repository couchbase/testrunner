function OnUpdate(doc, meta) {
    var curl=SELECT CURL("https://maps.googleapis.com/maps/api/geocode/json",
           {"data":"address=Half+Moon+Bay" , "request":"GET"} );
    dst_bucket["curl"]=curl.execQuery();
}