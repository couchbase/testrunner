function OnUpdate(doc, meta) {
    var query=GRANT Admin TO cbadminbucket;
    dst_bucket["grant"]=query.execQuery();

    var re=Revoke Admin FROM cbadminbucket;
    dst_bucket["revoke"]=re.execQuery();
}