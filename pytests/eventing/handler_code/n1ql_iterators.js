function OnUpdate(doc, meta) {
    log('document', doc);
    res1 = test_continue();
    res2 = test_break();
    res3 = test_labelled_continue();
    res4 = test_labelled_break();
    if (res1 && res2 && res3 && res4){
        dst_bucket[meta.id] = doc;
    }
}
function OnDelete(meta) {
}

function test_continue(){
    var nums = [1, 2, 3, 4, 5, 6, 7, 8, 9 , 10];
    var count = 0;
    for(var row of nums) {
      for(var row of nums) {
        count++;
        if(count == 5 || count == 7){
          continue;
        }
        ++count;
      }
    }
    var res1 = SELECT * FROM src_bucket LIMIT 10;
    var count1 = 0;
    for(var row of res1) {
      for(var row of res1) {
        count1++;
        if(count1 == 5 || count1 == 7){
          continue;
        }
        ++count1;
      }
    }
    log("count : ",count);
    log("count1 : ",count1);
    if (count === count1){
        return true;
    } else {
        return false;
    }
}

function test_break(){
    var nums = [1, 2, 3, 4, 5, 6, 7, 8, 9 , 10];
    var count = 0;
    for(var row of nums) {
        for(var row of nums) {
            count++;
            if(count == 5 || count == 7){
                break;
            }
            ++count;
        }
    }
    var res1 = SELECT * FROM src_bucket LIMIT 10;
    var count1 = 0;
    for(var row of res1) {
        for(var row of res1) {
            count1++;
            if(count1 == 5 || count1 == 7){
                break;
            }
            ++count1;
        }
    }
    if (count === count1){
        return true;
    } else {
        return false;
    }
}

function test_labelled_continue(){
    var nums = [1, 2, 3, 4, 5, 6, 7, 8, 9 , 10];
    var count = 0;
    list: for(var row of nums) {
      for(var row of nums) {
        count++;
        if(count == 5 || count == 7){
          continue list;
        }
        ++count;
      }
    }
    var res1 = SELECT * FROM src_bucket LIMIT 10;
    var count1 = 0;
    list: for(var row of res1) {
      for(var row of res1) {
        count1++;
        if(count1 == 5 || count1 == 7){
          continue list;
        }
        ++count1;
      }
    }
    log("count : ",count);
    log("count1 : ",count1);
    if (count === count1){
        return true;
    } else {
        return false;
    }
}

function test_labelled_break(){
    var nums = [1, 2, 3, 4, 5, 6, 7, 8, 9 , 10];
    var count = 0;
    list: for(var row of nums) {
        for(var row of nums) {
            count++;
            if(count == 5 || count == 7){
                break list;
            }
            ++count;
        }
    }
    var res1 = SELECT * FROM src_bucket LIMIT 10;
    var count1 = 0;
    list: for(var row of res1) {
        for(var row of res1) {
            count1++;
            if(count1 == 5 || count1 == 7){
                break list;
            }
            ++count1;
        }
    }
    if (count === count1){
        return true;
    } else {
        return false;
    }
}
