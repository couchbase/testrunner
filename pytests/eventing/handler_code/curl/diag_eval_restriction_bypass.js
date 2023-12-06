function OnUpdate(doc, meta) {
    var count = 0;
    var paths = ['///diag/eval', '/diag//eval', '/diag/eval/'];
    for(let i=0; i<3; i++) {
        try{
            var req = {
                path: paths[i],
                body: 'os:cmd(\"touch /tmp/hi\")'
            }
            var response = curl("POST", server, req);
        }
        catch(e){
            if(e["message"] == "The curl call tried accessing a restricted endpoint: /diag/eval")
            {
                count++;
            }
        }
    }
    if(count == 3) {
        dst_bucket[meta.id] = doc;
    }
}

function OnDelete(meta) {
    delete dst_bucket[meta.id];
}