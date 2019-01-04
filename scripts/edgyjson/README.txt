Why?
Use edgyjson to create json docs to test edge cases, boundary values and buggy keys/values

What?
The output of edgyjson is a set of json docs conforming to a user specified template, that can be analyzed locally or uploaded to a CB server 

How?
Local docs example: python main.py -n 5
Upload docs example: python main.py -ip 192.168.56.111 -u Administrator -p password -b default -n 5 -s 10 -e utf-8 -t mix.json
