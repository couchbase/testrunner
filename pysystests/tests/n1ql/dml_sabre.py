#!/usr/bin/python
import json
import os
import requests
import time
import thread
import multiprocessing
import argparse
import random
import datetime
from datetime import timedelta

SELECT_QUERIES = {
    "sabre": {
        "findFlightsSpecificDates" : "SELECT AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureAirport.LocationCode as Depart_Airport, "
                               "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[1].ArrivalAirport.LocationCode as Arrival_Airport, "
                               "ARRAY_LENGTH(AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment) as Flight_Segment, "
                               "DATE_DIFF_STR(AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[1].ArrivalDateTime, "
                               "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureDateTime,\\\"hour\\\") as Flight_Time, "
                               "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureDateTime as Depart_Time, "
                               "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[1].ArrivalDateTime as Arrival_Time "
                               "FROM default "
                               "WHERE "
                               "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureAirport.LocationCode = $1 and "
                               "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[1].ArrivalAirport.LocationCode = $2 and "
                               "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureDateTime between $3 "
                               "and $4 "
                               "ORDER BY Flight_Time, Depart_Time, Arrival_Time "
                               "LIMIT 10"
        ,
        "findCheapestFlightFlexDates" : "SELECT AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureAirport.LocationCode as Depart_Airport, "
                               "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[1].ArrivalAirport.LocationCode as Arrival_Airport, "
                               "ARRAY_LENGTH(AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment) as Flight_Segment, "
                               "DATE_DIFF_STR(AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[1].ArrivalDateTime, "
                               "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureDateTime,\\\"hour\\\") as Flight_Time, "
                               "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureDateTime as Depart_Time, "
                               "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[1].ArrivalDateTime as Arrival_Time, "
                               "AirItineraryPricingInfo.PTC_FareBreakdowns.PTC_FareBreakdown.PassengerFare.TotalFare.Amount as Fare "
                               "FROM default "
                               "WHERE "
                               "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureAirport.LocationCode = $1 and "
                               "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[1].ArrivalAirport.LocationCode = $2 and "
                               "AirItineraryPricingInfo.PTC_FareBreakdowns.PTC_FareBreakdown.PassengerFare.TotalFare.Amount between $5 and $6 "
                               "ORDER BY Fare, Depart_Time, Arrival_Time "
                               "LIMIT 10"
        ,
        "findFlightsMinStopMinPrice" : "SELECT AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureAirport.LocationCode as Depart_Airport, "
                                        "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[1].ArrivalAirport.LocationCode as Arrival_Airport, "
                                        "ARRAY_LENGTH(AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment) as Flight_Segment, "
                                        "DATE_DIFF_STR(AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[1].ArrivalDateTime, "
                                        "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureDateTime,\\\"hour\\\") as Flight_Time, "
                                        "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureDateTime as Depart_Time, "
                                        "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[1].ArrivalDateTime as Arrival_Time, "
                                        "AirItineraryPricingInfo.PTC_FareBreakdowns.PTC_FareBreakdown.PassengerFare.TotalFare.Amount as Fare "
                                        "FROM default "
                                        "WHERE "
                                        "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureAirport.LocationCode = $1 and "
                                        "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[1].ArrivalAirport.LocationCode = $2 and "
                                        "AirItineraryPricingInfo.PTC_FareBreakdowns.PTC_FareBreakdown.PassengerFare.TotalFare.Amount between $5 and $6 and "
                                        "ARRAY_LENGTH(AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment) < 3 "
                                        "ORDER BY Fare, Flight_Segment, Depart_Time, Arrival_Time "
                                        "LIMIT 10"
        ,
        "findOneWayFlights" : "SELECT AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureAirport.LocationCode as Depart_Airport, "
                                     "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[1].ArrivalAirport.LocationCode as Arrival_Airport, "
                                     "ARRAY_LENGTH(AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment) as Flight_Segment, "
                                     "DATE_DIFF_STR(AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[1].ArrivalDateTime, "
                                     "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureDateTime,\\\"hour\\\") as Flight_Time, "
                                     "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureDateTime as Depart_Time, "
                                     "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[1].ArrivalDateTime as Arrival_Time, "
                                     "AirItinerary.DirectionInd as DirectionInd "
                                     "FROM default "
                                     "WHERE "
                                     "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureAirport.LocationCode = $1 and "
                                     "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[1].ArrivalAirport.LocationCode = $2 and "
                                     "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureDateTime between $3 "
                                     "and $4 and "
                                     "AirItinerary.DirectionInd = \\\"OneWay\\\" "
                                     "ORDER BY Flight_Time, Depart_Time, Arrival_Time "
                                     "LIMIT 10"
        }

}

UPDATE_QUERIES = {
    "sabre": {
        "updateTaxesforSelectKeys": "UPDATE default USE KEYS [$1,$2] "
                                    "SET AirItineraryPricingInfo.PTC_FareBreakdowns.PTC_FareBreakdown.PassengerFare.Taxes.TotalTax=$3, "
                                    "AirItineraryPricingInfo.PTC_FareBreakdowns.PTC_FareBreakdown.PassengerFare.TotalFare = "
                                    "AirItineraryPricingInfo.PTC_FareBreakdowns.PTC_FareBreakdown.PassengerFare.BaseFare + $3, "
                                    "AirItineraryPricingInfo.ItinTotalFare.TotalFare.Amount ="
                                    "AirItineraryPricingInfo.PTC_FareBreakdowns.PTC_FareBreakdown.PassengerFare.BaseFare + $3 "
                                    "RETURNING default"
        ,
        "updatePricesforSelectTaxCodes": "UPDATE default "
                                         "SET AirItineraryPricingInfo.PTC_FareBreakdowns.PTC_FareBreakdown.PassengerFare.Taxes.TotalTax=$1, "
                                         "AirItineraryPricingInfo.PTC_FareBreakdowns.PTC_FareBreakdown.PassengerFare.TotalFare = "
                                         "AirItineraryPricingInfo.PTC_FareBreakdowns.PTC_FareBreakdown.PassengerFare.BaseFare + $3, "
                                         "AirItineraryPricingInfo.ItinTotalFare.TotalFare.Amount ="
                                         "AirItineraryPricingInfo.PTC_FareBreakdowns.PTC_FareBreakdown.PassengerFare.BaseFare + $3 "
                                         "WHERE "
                                         "AirItineraryPricingInfo.PTC_FareBreakdowns.PTC_FareBreakdown.PassengerFare.Taxes.Tax[0].TaxCode=$4 AND "
                                         "SequenceNumber = $5 "
                                         "RETURNING default"
        ,
        "updateFlightNumberForRoute": "UPDATE default "
                                      "SET AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].FlightNumber = $6, "
                                      "AirItineraryPricingInfo.PTC_FareBreakdowns.PTC_FareBreakdown.PassengerFare.TotalFare = $3 "
                                      "WHERE "
                                      "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].FlightNumber=$7 AND "
                                      "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureAirport.LocationCode = $1 AND "
                                      "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[1].ArrivalAirport.LocationCode = $2 AND "
                                      "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureDateTime = $8 "
                                      "RETURNING default"
        ,

    }
}

DELETE_QUERIES = {
    "sabre": {
        "deleteRecordsforSelectKeys": "DELETE FROM default D "
                                      "USE KEYS [$1,$2] "
                                      "RETURNING D"
        ,
        "deleteSelectDepartDateLocation" : "DELETE FROM default S "
                                                "WHERE "
                                                "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureDateTime "
                                                "between $3 and $4 AND "
                                                "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[1].ArrivalAirport.LocationCode = $5 AND "
                                                "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureAirport.LocationCode = $6"
                                                "RETURNING S"
        ,
        "deleteforSelectFlightforDate": "DELETE FROM default "
                                        "WHERE "
                                        "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureDateTime "
                                        "between $3 and $4 AND "
                                        "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].FlightNumber = $7 AND "
                                        "AirItinerary.DirectionInd = \\\"OneWay\\\" "
                                        "RETURNING default"
    }
}

def runQueryOnce(query, param, server_ip):
    print param
    url = "http://" + server_ip + ":8093/query"
    print url

    stmt = '{"statement" : "' + str(query) + '"'
    stmt = stmt + '}'
    query = json.loads(stmt)
    print query

    r = requests.post(url, data=query, auth=('Administrator', 'password'))
    r123 = r.json()['results']
    x = list()
    for i in range(len(r123)):
        x.append(r123[i][param])
    return x

def runNQueryParam(query, param1, param2, server_ip):

    if "SELECT" in query:
        print " SELECT ****"
        param = random.sample(param1, 2)
        d1 = datetime.datetime(2014,1,10)
        d2 = d1 + timedelta(days=30)
        param.append(d1)
        param.append(d2)
        a1 = round(random.uniform(250,300))
        a2 = a1 + 300
        param.append(a1)
        param.append(a2)
    elif "UPDATE" in query:
        print " UPDATE ****"
        param = random.sample(param1, 2)
        a1 = round(random.uniform(10,30))
        param.append(a1)
        all_tax_codes = ["AY", "US1", "XF", "ZP"]
        t1=random.sample(all_tax_codes, 1)
        param.append(a1)
        s1=random.randint(10,1000)
        param.append(s1)
        all_flight_numbers = ["240","250", "260", "270","280", "1200" ,"1210", "1150", "1140" ]
        f=random.sample(all_flight_numbers, 2)
        param.append(f[0])
        param.append(f[1])
        d1 = datetime.datetime(2014,1,10)
        param.append(d1)
    elif "DELETE" in query:
        print "DELETE ****"
        param = random.sample(param1, 2)


    print "&" * 80
    print query
    print "&" * 80
    for p in param:
        print p
    print "&" * 80

    url = "http://" + server_ip + ":8093/query"
    print url

    stmt = '{"statement" : "' + str(query) + '"'
    if len(param) > 0:
        stmt = stmt + ', "args" : "['
    else:
        stmt = stmt + '}'
    i = 0
    myarg = ""
    for p in param:
        if isinstance(p, bool):
            myarg = myarg + str.lower(str(p))
        elif isinstance(p, (int, float, long)) and not isinstance(p, (bool)):
            myarg = myarg + str(p)
        else:
            myarg = myarg + '\\"' + str(p) + '\\"'
        i = i + 1
        if i < len(param):
            myarg = myarg + ","
    stmt = stmt + myarg
    stmt = stmt + ']" }'

    query = json.loads(stmt)
    print query

    r = requests.post(url, data=query, stream=False, headers={'Connection': 'close'},
        auth=('Administrator', 'password'))
    print r.json()
    r123 = r.json()['results']
    return r123

def calcSeqNumber():
    seq_num = [random.randint(1000, 10000) for r in xrange(50)]
    print "*" * 80
    print seq_num
    return seq_num

def calcAirports():
    airport_codes = ["ABR", "ABI", "ATL","BOS", "BUR", "CHI", "MDW", "DAL", "SFO", "SAN", "SJC", "LGA", "JFK", "MSP"]
    return airport_codes

def calcKeys():
    seed = [str(random.randint(1,5000)) + "_sabre_" + str(random.randint(1,100000)) for r in range(50)]
    print "*" * 80
    print seed
    print "*" * 80
    return seed

def calcAmount():
    #base_fare = round(random.uniform(250,600))
    #taxes = round(random.uniform(250,600))
    amount = [round(random.uniform(150,600))+round(random.uniform(10,40)) for r in range(50)]
    print "*" * 80
    print amount
    print "*" * 80
    return amount


parser = argparse.ArgumentParser(description='This script is used for sabre dataset')
parser.add_argument('-d', '--duration', help='Runtime duration', required=True)
parser.add_argument('-c', '--clients', help='Number of clients', required=True)
parser.add_argument('-q', '--queryNode', help='query node ip', required=True)
parser.add_argument('-o', '--ops', help='Select either (s)SELECTS | (u)UPDATES | (d) DELETEs', required=True)


args = vars(parser.parse_args())

## output duration and clients ##
print ("Duration: %s" % args['duration'] )
print ("Num clients: %s" % args['clients'])

total_clients = int(args['clients'])
queryNode = str(args['queryNode'])
op_select = str(args['ops'])

if op_select == "s":
    print "*" * 80
    print "Issue selects"
    q = SELECT_QUERIES["sabre"]
    len_query = len(q)

elif op_select == "u":
    print "*" * 80
    print "Issue updates and deletes"
    q = UPDATE_QUERIES["sabre"]
    len_query = len(q)

elif op_select == "d":
    print "*" * 80
    print "Issue deletes"
    q = DELETE_QUERIES["sabre"]
    len_query = len(q)

else:
    print " SELECT by default"

pool = multiprocessing.Pool(total_clients)

airport_codes = calcAirports()
result_seq = calcSeqNumber()
keys = calcKeys()
amount = calcAmount()

## Do not delete - Sample Single Client execution ##
for k in range(1):
    for j in range(len_query):
        print list(q)[j]
        k_qry = list(q)[j]
        if "Key" in k_qry:
            update_amt = random.randint(10, 100)
            r = runNQueryParam(q[k_qry], keys, update_amt, queryNode)

        elif "Amount" in k_qry:
            r = runNQueryParam(q[k_qry], amount, 0, queryNode)
        else:
            r = runNQueryParam(q[k_qry], airport_codes, 0, queryNode)

        print r
### End ###

#result = []
#print "----------------Start querying sabre with {0} clients for {1} duration -------------".format(total_clients, args['duration'])
#for k in range(int(args['duration'])):
#	for i in range(total_clients):
#		print "Starting thread {0}".format(i)
#		for j in range(len_query):
#         		print list(q)[j]
#         		k_qry = list(q)[j]
#			if "SequenceNumber" in k_qry:
#	 			r = pool.apply_async(runNQueryParam(q[k_qry],result_seq))
#	 			result.append(r)
#			elif "Key1" in k_qry:
#				r = pool.apply_async(runNQueryParam(q[k_qry],result_keys))
#				result.append(r)
#			elif "Amount" in k_qry:
#				r = pool.apply_async(runNQueryParam(q[k_qry],result_amount))
#				result.append(r)
#			else:
#				r = pool.apply_async(runNQueryParam(q[k_qry],result_code))
#				result.append(r)
#		pool.close()
#		print "Waiting for {0} threads to finish".format(j)
#		pool.join()

print "Done with querying"
