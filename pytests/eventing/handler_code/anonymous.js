function OnUpdate(doc, meta) {
    var anon = function () {
         var sel = SELECT  *  from src_bucket where mutated=0 limit 1;
         for(var row of sel){
         dst_bucket['select'] = row;
         }
   };
    anon();

     (function () {
         var sel = SELECT  *  from src_bucket where mutated=0 limit 1;
         for(var row of sel){
         dst_bucket['select2'] = row;
         }
   })();
}