function OnUpdate(doc, meta) {
    var query=GRANT cluster_admin TO cbadminbucket;
    dst_bucket["grant"]=query;

    var re=Revoke cluster_admin FROM cbadminbucket;
    dst_bucket["revoke"]=re;
}