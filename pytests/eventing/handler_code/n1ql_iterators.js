function OnUpdate(doc, meta) {
    log('document : ', meta.id);
    var query_result = SELECT * FROM src_bucket LIMIT 10;
    res1 = test_continue(query_result);
    res2 = test_break(query_result);
    res3 = test_labelled_continue(query_result);
    res4 = test_labelled_break(query_result);
    if (res1 && res2 && res3 && res4){
        dst_bucket[meta.id] = doc;
    }
}
function OnDelete(meta) {
}

function test_continue(query_result){
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
    var count1 = 0;
    for(var row of query_result) {
      for(var row of query_result) {
        count1++;
        if(count1 == 5 || count1 == 7){
          continue;
        }
        ++count1;
      }
    }
    log("test_continue :: count : ",count);
    log("test_continue :: count1 : ",count1);
    if (count === count1){
        return true;
    } else {
        return false;
    }
}

function test_break(query_result){
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
    var count1 = 0;
    for(var row of query_result) {
        for(var row of query_result) {
            count1++;
            if(count1 == 5 || count1 == 7){
                break;
            }
            ++count1;
        }
    }
    log("test_break :: count : ",count);
    log("test_break :: count1 : ",count1);
    if (count === count1){
        return true;
    } else {
        return false;
    }
}

function test_labelled_continue(query_result){
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
    var count1 = 0;
    list: for(var row of query_result) {
      for(var row of query_result) {
        count1++;
        if(count1 == 5 || count1 == 7){
          continue list;
        }
        ++count1;
      }
    }
    log("test_labelled_continue :: count : ",count);
    log("test_labelled_continue :: count1 : ",count1);
    if (count === count1){
        return true;
    } else {
        return false;
    }
}

function test_labelled_break(query_result){
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
    var count1 = 0;
    list: for(var row of query_result) {
        for(var row of query_result) {
            count1++;
            if(count1 == 5 || count1 == 7){
                break list;
            }
            ++count1;
        }
    }
    log("test_labelled_break :: count : ",count);
    log("test_labelled_break :: count1 : ",count1);
    if (count === count1){
        return true;
    } else {
        return false;
    }
}
