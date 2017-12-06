function OnUpdate(doc, meta) {
    var t=1;
    testRecursion(t);
}

function testRecursion(t){
    if(t==3)
        return;
    else{
        t++;
        dst_bucket[t]=t;
        testRecursion(t);
    }
}