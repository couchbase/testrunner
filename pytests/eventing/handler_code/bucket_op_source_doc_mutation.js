function OnUpdate(doc,meta) {
   var src_doc= doc;
   if(src_doc["updated_field"]==null){
    src_doc["updated_field"]=1;
   }
   else{
    src_doc["updated_field"]=src_doc["updated_field"]+1;
   }
   src_bucket[meta.id]=src_doc;
}

function OnDelete(meta){
    var doc={"doc_deleted":1};
    src_bucket[meta.id]=doc;

}