package main

import (
    "flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"
	"encoding/json"
)

// Usage example:
// cd /opt/gocode; go run /opt/gocode/simpleServe.go 8081 2>&1 | tee /opt/gocode/simpleServe.txt
// cd /opt/gocode; go run /opt/gocode/simpleServe.go 8082 2>&1 | tee /opt/gocode/simpleServe2.txt

func main() {
	counter := 0
	sum := 0.0
	var payloads [][]byte

    flag.Parse()  
    port := ":" + flag.Arg(0)  
    fmt.Println("SimpleServ is initialized with port " + port)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Printf("Error trying to read body: %s", err)
		}
		if len(body) > 0 {
			payloads = append(payloads, body)
			var payload map[string]interface{}
			json.Unmarshal(body, &payload)
			fValue, ok := payload["delay"].(float64)
			if ok {
			    var iValue int = int(fValue) * 1000
	            log.Printf("Start sleeping for %d ms", iValue)
		        time.Sleep(time.Duration(iValue) * time.Millisecond)
	            log.Printf("End sleep for %d ms", iValue)
			}
			log.Printf("%s", body)
		}
		if len(r.Form) > 0 {
			log.Printf("Handled request with form: %v", r.Form)
			floatValue, err := strconv.ParseFloat(r.Form.Get("value"), 64)
			if err == nil {
				sum = sum + floatValue
			}
		}
		fmt.Fprintf(w, "OK")
		counter++
	})

	http.ListenAndServe(port, nil)

}