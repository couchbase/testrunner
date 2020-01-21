import csv
import sys
import json
from random import randint

#EDIT THIS LIST WITH YOUR REQUIRED JSON KEY NAMES
fieldnames=["Year", "Quarter", "Month", "DayofMonth"]

def convert(filename):
  csv_filename = filename[0]
  print("Opening CSV file: ", csv_filename)
  with open(csv_filename) as f:
    content = f.readlines()
  fieldnames = content[0].replace("\n", "").split(",")
  f=open(csv_filename, 'r')
  csv_reader = csv.DictReader(f, fieldnames)
  json_filename = csv_filename.split(".")[0]+".json"
  print("Saving JSON to file: ", json_filename)
  jsonf = open(json_filename, 'w')
  for r in csv_reader:
    data = json.dumps(r)
    jsonf.write(data)
    jsonf.write("\n")
  f.close()
  jsonf.close()

def state_data_dump(filename):
 csv_filename = filename
 print("Opening CSV file: ", csv_filename)
 with open(csv_filename) as f:
    content = f.readlines()
 json_filename = csv_filename.split(".")[0]+".import.csv"
 print("Saving JSON to file: ", json_filename)
 csvf = open(json_filename, 'w')
 for r in content:
 	data = []
 	tokens= r.split(",")
 	print(tokens)
 	data.append(tokens[0])
 	data.append(tokens[0])
 	data.append(tokens[1].replace("\r\n", ""))
 	data.append(tokens[1].replace("\r\n", "")+"\n")
  	csvf.write(",".join(data))
 f.close()
 csvf.close()

def zip_data_dump(filename):
 csv_filename = filename
 print("Opening CSV file: ", csv_filename)
 with open(csv_filename) as f:
    content = f.readlines()
 json_filename = "zipcode.import.csv"
 print("Saving JSON to file: ", json_filename)
 csvf = open(json_filename, 'w')
 for r in content:
 	data = []
 	tokens= r.split(",")
 	print(tokens)
 	data.append(tokens[0])
 	data.append(tokens[0])
 	data.append(tokens[1].replace("\r\n", ""))
 	data.append(str(randint(1, 10000))+"\n")
  	csvf.write(",".join(data))
 f.close()
 csvf.close()

def carriers_data_dump(filename):
 csv_filename = filename
 states = ["CA", "TX", "IA", "IS", "NY"]
 print("Opening CSV file: ", csv_filename)
 json_filename = "carriers.import.csv"
 csvf = open(json_filename, 'w')
 carriers_count_map = {}
 map = {}
 count =0
 with open(filename, 'rb') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
    for r in spamreader:
      if count == 0:
        k = 0
        for key in r:
          map[key] = k
          k += 1
      else:
        tokens= r
        if tokens[map["Carrier"]] not in list(carriers_count_map.keys()):
          carriers_count_map[tokens[map["Carrier"]]] = 0
        else:
          carriers_count_map[tokens[map["Carrier"]]] += 1
        if tokens[map["OriginState"]] not in list(carriers_count_map.keys()):
          carriers_count_map[tokens[map["OriginState"]]] = 0
        else:
          carriers_count_map[tokens[map["OriginState"]]] += 1
        if carriers_count_map[tokens[map["Carrier"]]] < 1:
          data = []
          data.append(tokens[map["Carrier"]])
          data.append(tokens[map["Carrier"]])
          data.append(tokens[map["OriginState"]].split(",")[0])
          data.append(str(randint(1, 360))+"\n")
          csvf.write(",".join(data))
      count += 1
 f.close()
 csvf.close()

def airports_data_dump(filename):
 csv_filename = filename
 map = {"OriginAirportID":0,"OriginCityName":0,"OriginState":0}
 print("Opening CSV file: ", csv_filename)
 json_filename = "airports.import.csv"
 csvf = open(json_filename, 'w')
 count =0
 carriers_count_map = {}
 with open(filename, 'rb') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
    for r in spamreader:
      if count == 0:
        k = 0
        for key in r:
          map[key] = k
          k += 1
      else:
        tokens= r
        if tokens[map["Origin"]] not in list(carriers_count_map.keys()):
          carriers_count_map[tokens[map["Origin"]]] = 0
        else:
          carriers_count_map[tokens[map["Origin"]]] += 1
        if carriers_count_map[tokens[map["Origin"]]] < 1:
          data = []
          data.append(tokens[map["Origin"]])
          data.append(tokens[map["OriginState"]].split(",")[0])
          data.append(tokens[map["Origin"]].split(",")[0])
          data.append(str(randint(1, 360))+"\n")
          csvf.write(",".join(data))
      count += 1
 csvf.close()

def aircraft_data_dump(filename):
 csv_filename = filename
 map = {}
 states = ["CA", "TX", "IA", "IS", "NY"]
 print("Opening CSV file: ", csv_filename)
 json_filename = "aircraft.import.csv"
 csvf = open(json_filename, 'w')
 carriers_count_map = {}
 count =0
 with open(filename, 'rb') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
    for r in spamreader:
      if count == 0:
        k = 0
        for key in r:
          map[key] = k
          k += 1
      else:
        tokens= r
        if tokens[map["TailNum"]] not in list(carriers_count_map.keys()):
          carriers_count_map[tokens[map["TailNum"]]] = 0
        else:
          carriers_count_map[tokens[map["TailNum"]]] += 1
        if carriers_count_map[tokens[map["TailNum"]]] < 1:
          data = []
          data.append(tokens[map["TailNum"]])
          data.append(tokens[map["FlightNum"]].split(",")[0])
          data.append(tokens[map["FlightNum"]].split(",")[0])
          data.append(tokens[map["OriginState"]].split(",")[0]+"\n")
          csvf.write(",".join(data))
      count += 1
 csvf.close()

def aircraft_craft_engine_data_dump(filename):
 csv_filename = filename
 map = {}
 carriers_count_map = {}
 states = ["CA", "TX", "IA", "IS", "NY"]
 print("Opening CSV file: ", csv_filename)
 json_filename = "aircraft_engine_code.import.csv"
 csvf = open(json_filename, 'w')
 count =0
 with open(filename, 'rb') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
    for r in spamreader:
      if count == 0:
        k = 0
        for key in r:
          map[key] = k
          k += 1
      count += 1
      data = []
      tokens= r
      data.append(tokens[map["TailNum"]])
      data.append(tokens[map["TailNum"]])
      data.append(tokens[map["TailNum"]])
      data.append("NOT SURE :: JUNK VALUE FOR MANUFCATURER\n")
      csvf.write(",".join(data))
 csvf.close()


def online_myisam_data_dump(filename):
 csv_filename = filename
 map = {}
 states = ["CA", "TX", "IA", "IS", "NY"]
 carriers_count_map = {}
 print("Opening CSV file: ", csv_filename)
 json_filename = "online_myisam.import.csv"
 csvf = open(json_filename, 'w')
 count =0
 list = ['ORD', 'AKN', 'BIS', 'LIT', 'MSP']
 with open(filename, 'rb') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
    for r in spamreader:
      if count == 0:
        k = 0
        for key in r:
          map[key] = k
          k += 1
      else:
        tokens= r
        check = tokens[map["Origin"]] in list
        key = tokens[map["Carrier"]]+tokens[map["TailNum"]+map["Origin"]]
        if key not in list(carriers_count_map.keys()):
          carriers_count_map[key] = 0
        else:
          carriers_count_map[key] += 0
        if check and carriers_count_map[key] < 1:
          print(count, tokens[map["Carrier"]])
          data = []
          data.append(tokens[map["Carrier"]])
          data.append(tokens[map["Dest"]])
          data.append(str(count))
          data.append(str(count))
          data.append(tokens[map["TailNum"]])
          data.append(tokens[map["Origin"]])
          data.append(tokens[map["Dest"]])
          data.append(str(count))
          data.append(tokens[map["DepTime"]])
          data.append(tokens[map["Distance"]])
          data.append(tokens[map["FlightNum"]])
          data.append(tokens[map["DestAirportID"]]+"\n")
          csvf.write(",".join(data))
      count += 1
 csvf.close()

def aircraft_remarks_data_dump(filename):
 csv_filename = filename
 map = {}
 states = ["CA", "TX", "IA", "IS", "NY"]
 print("Opening CSV file: ", csv_filename)
 json_filename = "airport_remarks.import.csv"
 csvf = open(json_filename, 'w')
 count =0
 with open(filename, 'rb') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
    for r in spamreader:
      data = []
      tokens= r
      data.append(tokens[0])
      data.append(tokens[1])
      data.append(tokens[0])
      data.append(str(tokens[0])+"\n")
      csvf.write(",".join(data))
 csvf.close()


def distinct_airports_data_dump(filename):
 json_filename = "filtered_airports.import.csv"
 csvf = open(json_filename, 'w')
 count =0
 keys = []
 with open(filename, 'rb') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
    for r in spamreader:
      key = r[0]
      if key not in keys:
        keys.append(key)
        csvf.write(",".join(r)+"\n")
 csvf.close()

def distinct_aircrafts_data_dump(filename):
 json_filename = "filtered_aircrafts.import.csv"
 csvf = open(json_filename, 'w')
 count =0
 keys = []
 with open(filename, 'rb') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
    for r in spamreader:
      key = r[0]
      if key not in keys:
        keys.append(key)
        csvf.write(",".join(r)+"\n")
 csvf.close()

def distinct_aircrafts_engine_data_dump(filename):
 json_filename = "filtered_aircrafts_engine.import.csv"
 csvf = open(json_filename, 'w')
 count =0
 keys = []
 with open(filename, 'rb') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
    for r in spamreader:
      key = r[0]
      if key not in keys:
        keys.append(key)
        csvf.write(",".join(r)+"\n")
 csvf.close()


if __name__=="__main__":
  convert(sys.argv[1:])
	#state_data_dump("states.csv")
	#zip_data_dump("states.csv")
	#carriers_data_dump("On_Time_On_Time_Performance_2008_1.csv")
  #airports_data_dump("On_Time_On_Time_Performance_2008_1.csv")
  #distinct_airports_data_dump("airports.import.csv")
  #aircraft_data_dump("On_Time_On_Time_Performance_2008_1.csv")
  #aircraft_remarks_data_dump("airport_information.csv")
  #distinct_aircrafts_data_dump("aircraft.import.csv")
  #aircraft_craft_engine_data_dump("On_Time_On_Time_Performance_2008_1.csv")
  #distinct_aircrafts_engine_data_dump("aircraft_engine_code.import.csv")
  #online_myisam_data_dump("On_Time_On_Time_Performance_2008_1.csv")

