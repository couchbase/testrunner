function OnUpdate(doc, meta) {
    var query=GRANT Admin TO cbadminbucket;
    dst_bucket["grant"]=query;

    var re=Revoke Admin FROM cbadminbucket;
    dst_bucket["revoke"]=re;
}