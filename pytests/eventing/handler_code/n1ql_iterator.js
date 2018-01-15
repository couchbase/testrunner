function OnUpdate(doc, meta){
    var sel=SELECT *  from src_bucket;
    var i=0;
    for (var item of sel){
        dst_bucket[i]=item;
        i++;
    }
}