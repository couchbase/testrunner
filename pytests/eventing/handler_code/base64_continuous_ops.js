function testEncodeDecode()
{
    var x=(Math.random()+1 ).toString(16).substring(2);
    var result1=couchbase.base64Encode(x);
    var result2=couchbase.base64Decode(result1);
    if(result2!=x){
        return false;
    }
  return true;

}
function OnUpdate(doc, meta, xattrs) {
    var context = {docID : meta.id,status: true};
    var failed=false;
     for(let i=0;i<1000;i++)
     {
        if (!testEncodeDecode()){
          failed=true;
        }
     }
    if (!failed){
      dst_bucket[meta.id]="success";
    }
}

function OnDelete(meta, options) {
    log("Doc deleted/expired", meta.id);
}