function OnUpdate(doc, meta) {
    try{
        var req = {
        path: '/diag/eval',
        body: 'os:cmd(\"touch /tmp/hi\")'
        }
        var response = curl("POST", server, req)
    }
    catch(e){
        log("error:",e);
        if(e["message"] == "The curl call tried accessing a restricted endpoint: /diag/eval")
        {
            dst_bucket[meta.id]= doc;
        }
    }
}

function OnDelete(meta) {
    delete dst_bucket[meta.id];
}