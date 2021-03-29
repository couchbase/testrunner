function NestMe(query, depth) {
    depth += 1;
    if(depth != 4) {
         NestMe(query, depth);
    }
    for(let row of query) {
    }
}

function OnUpdate(doc, meta) {
    try{
       let query = SELECT * FROM `travel-sample` limit 20;
       NestMe(query, 0);
       for(let row of query) {
        }
       // iterate rest of the rows here
       query.close(); // Shouldn't throw error
       dst_bucket[meta.id]="No error";
    }catch(e){
        log(e);
    }
}


function OnDelete(meta){
    try{
       let query = SELECT * FROM `travel-sample` limit 20;
       NestMe(query, 0);
       for(let row of query) {
        }
       // iterate rest of the rows here
       query.close(); // Shouldn't throw error
       delete dst_bucket[meta.id];
    }catch(e){
        log(e);
    }
}