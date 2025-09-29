function callerFunction(component,functionName){
    switch (functionName) {
        case 0:
            return couchbase.base64Encode(component)
        case 1:
            return couchbase.base64Decode(component)
        case 2:
            return couchbase.base64Float32ArrayEncode(component)
        case 3:
            return couchbase.base64Float32ArrayDecode(component)
        case 4:
            return couchbase.base64Float64ArrayEncode(component)
        case 5:
            return couchbase.base64Float64ArrayDecode(component)
    }
}

function OnUpdate(meta, options) {
    log("Doc updated", meta.id);
}

function OnDelete(meta, options) {
    log("Doc deleted/expired", meta.id);
}

function OnDeploy(action) {
    var meta = {"id" : "1"};
    const sampleInteger=-10678324;
    const sampleFloat= 45.894055;
    const sampleBool=true;
    var sampleASCIIString="";
    for (let i = 0; i < 128; i++) {
        sampleASCIIString += String.fromCharCode(i);
    }
    const sampleNonAsciiString = "ÇüéâäàåçêëèïîìÄÅÉæÆôöòûùÿÖÜø£Ø×ƒáíóúñÑªº¿®¬½¼¡«»";
    const sampleNull=null;
    const sampleUndefined = undefined;
    const sampleInfinity = Infinity
    const sampleFunction = function() { log("Function called"); }
    const sampleNaN= NaN
    const sampleSymbol = Symbol("Couchbase") ;
    const sampleIntegerArray = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    const sampleDate = new Date();
    const sampleJSONObject = {
        name: "John",
        age: 30,
        city: "New York"
    };
    const sampleFloatArray = [1.2, 3.5, -2.7, 0.8, 5.0, -6.3, 4.9, -0.1, 2.4, -3.6];
    const sampleStringArray =["stringArr1","stringArr2"]
    const sampleBigInt= BigInt("82739827138971298731298")

    const negative_base64_encode =[sampleBigInt];
    const positive_base64_encode = [sampleInteger,sampleFloat,sampleBool,sampleASCIIString,sampleNonAsciiString,sampleNull,
        sampleUndefined,sampleInfinity,sampleFunction,sampleNaN,sampleSymbol,sampleIntegerArray,sampleDate,sampleJSONObject,sampleFloatArray,
        sampleStringArray];
    const negative_base64_float_encode = [sampleInteger,sampleFloat,sampleBool,sampleNonAsciiString,sampleNull,
        sampleUndefined,sampleInfinity,sampleFunction,sampleNaN,sampleSymbol,sampleDate,sampleJSONObject,
        sampleStringArray,sampleBigInt]

    const negative_decode=[sampleInteger,sampleFloat,sampleBool,sampleNonAsciiString,sampleNull,
        sampleUndefined,sampleInfinity,sampleFunction,sampleNaN,sampleSymbol,sampleIntegerArray,sampleDate,sampleJSONObject,sampleFloatArray,
        sampleStringArray,sampleBigInt]

    const base64EncodedString = couchbase.base64Encode(sampleASCIIString);
    const positive_decode = [base64EncodedString];

    const positive_base64_float_encode =[sampleFloatArray,sampleIntegerArray]

    const positive_scenarios=[positive_base64_encode,positive_decode,positive_base64_float_encode,positive_decode,positive_base64_float_encode,positive_decode]
    const negative_scenarios=[negative_base64_encode,negative_decode,negative_base64_float_encode,negative_decode,negative_base64_float_encode,negative_decode]
    const functNames={
        0:"base64encode",1:"base64decode",2:"base64float32encode",3:"base64float32decode",4:"base64float64encode",5:"base64float64decode"}
    var failWholeTest=false
    for(let i=0;i<6;i++){
        for(let j=0;j<positive_scenarios[i].length;j++) {
            try {
                var result=callerFunction(positive_scenarios[i][j],i)
            }
            catch (e) {
                failWholeTest=true
                log(functNames[i],": TEST FAILED FOR ",positive_scenarios[i][j]," WAS EXPECTED TO PASS. REASON : ", e)
            }
        }
    }

    for(let i=0;i<6;i++){
        log("TESTING ",functNames[i])
        for(let j=0;j<negative_scenarios[i].length;j++) {
            var failed=false
            try {
                var result=callerFunction(negative_scenarios[i][j],i)
            }
            catch (e) {
                failed=true
                log(functNames[i],"TEST FAILED FOR INPUT ",negative_scenarios[i][j]," as EXPECTED")
            }
            if (!failed){
                log(functNames[i],"TEST PASSED FOR INPUT ",negative_scenarios[i][j]," WAS EXPECTED TO FAIL")
            }
        }
    }
    if (!failWholeTest){
        dst_bucket[meta.id]="success"
    }
}

