#!/usr/bin/python
import json
import os
import requests
import time
import multiprocessing
import argparse
import random
import datetime
from datetime import timedelta
import logging

#USAGE : python dml_sabre.py -d 2 -c 1 -q 127.0.0.1 -ops select -scan REQUEST_PLUS -m 16

#Fix 1 delete query
SELECT_QUERIES = {
    "default": {
        "findFlightsSpecificDates": "SELECT AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureAirport.LocationCode as Depart_Airport, "
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
        "findCheapestFlightFlexDates": "SELECT AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureAirport.LocationCode as Depart_Airport, "
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
        "findFlightsMinStopMinPrice": "SELECT AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureAirport.LocationCode as Depart_Airport, "
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
        "findOneWayFlights": "SELECT AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureAirport.LocationCode as Depart_Airport, "
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
    "default": {
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
    "default": {
        "deleteRecordsforSelectKeys": "DELETE FROM default D "
                                      "USE KEYS [$1,$2] "
                                      "RETURNING D"
        ,
        "deleteSelectDepartDateLocation": "DELETE FROM default S "
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
    url = "http://" + server_ip + ":8093/query"
    stmt = '{"statement" : "' + str(query) + '"'
    stmt = stmt + '}'
    query = json.loads(stmt)
    r = requests.post(url, data=query, auth=('Administrator', 'password'))
    r123 = r.json()['results']
    x = list()
    for i in range(len(r123)):
        x.append(r123[i][param])
    return x


def runNQueryParam(query, param1, scan_consistency, max_parallelism, server_ip):
    if "SELECT" in query:
        param = random.sample(param1, 2)
        d1 = datetime.datetime(2014, 1, 10)
        d2 = d1 + timedelta(days=30)
        param.append(d1)
        param.append(d2)
        a1 = round(random.uniform(250, 300))
        a2 = a1 + 300
        param.append(a1)
        param.append(a2)
    elif "UPDATE" in query:
        param = random.sample(param1, 2)
        a1 = round(random.uniform(10, 30))
        param.append(a1)
        all_tax_codes = ["AY", "US1", "XF", "ZP"]
        t1 = random.sample(all_tax_codes, 1)
        param.append(a1)
        s1 = random.randint(10, 1000)
        param.append(s1)
        all_flight_numbers = ["240", "250", "260", "270", "280", "1200", "1210", "1150", "1140"]
        f = random.sample(all_flight_numbers, 2)
        param.append(f[0])
        param.append(f[1])
        d1 = datetime.datetime(2014, 1, 10)
        param.append(d1)
    elif "DELETE" in query:
        param = random.sample(param1, 2)

    url = "http://" + server_ip + ":8093/query"
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
        elif isinstance(p, (int, float)) and not isinstance(p, (bool)):
            myarg = myarg + str(p)
        else:
            myarg = myarg + '\\"' + str(p) + '\\"'
        i = i + 1
        if i < len(param):
            myarg = myarg + ","
    stmt = stmt + myarg
    #stmt = stmt + ']" }'
    stmt = stmt + ']"'
    if "SELECT" in query:
        stmt = stmt + ', "scan_consistency": "' + scan_consistency + '"'
    stmt = stmt + ', "max_parallelism":"' + max_parallelism + '"'
    stmt = stmt + '}'
    query = json.loads(stmt)
    lgr.warn("Query:%s" % query)
    r = requests.post(url, data=query, stream=False, headers={'Connection': 'close'},
        auth=('Administrator', 'password'))
    lgr.warn("Query Output:%s" % r.json())
    r123 = r.json()['results']
    return r123


def calcSeqNumber():
    seq_num = [random.randint(1000, 10000) for r in range(50)]
    lgr.info("SequenceNumbers: %s" % seq_num)
    return seq_num


def calcAirports():
    airport_codes = ["ABR", "ABI", "ATL", "BOS", "BUR", "CHI", "MDW", "DAL", "SFO", "SAN", "SJC", "LGA", "JFK", "MSP"]
    return airport_codes


def calcKeys():
    seed = [str(random.randint(1, 5000)) + "_sabre_" + str(random.randint(1, 100000)) for r in range(50)]
    lgr.info("seed: %s" % seed)
    return seed


def calcAmount():
    #base_fare = round(random.uniform(250,600))
    #taxes = round(random.uniform(250,600))
    amount = [round(random.uniform(150, 600)) + round(random.uniform(10, 40)) for r in range(50)]
    lgr.info("Amount: %s" % amount)
    return amount

# create logger
lgr = logging.getLogger('dml')
lgr.setLevel(logging.DEBUG)
# add a file handler

fh = logging.FileHandler('dml_sabre.log')
fh.setLevel(logging.WARNING)

# create a formatter and set the formatter for the handler.
frmt = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(frmt)

# add the Handler to the logger
lgr.addHandler(fh)

parser = argparse.ArgumentParser(description='This script is used for default(sabre) dataset')
parser.add_argument('-d', '--duration', help='Runtime duration', default=60)
parser.add_argument('-c', '--clients', help='Number of clients', default=1)
parser.add_argument('-q', '--queryNode', help='query node ip', required=True)
parser.add_argument('-scan', '--scan_consistency', help='request_plus | statement_plus | not_bounded',
    default="NOT_BOUNDED")
parser.add_argument('-m', '--max_parallelism', help='Specify num of max parallelism, default = num of cores',
    default=8)
parser.add_argument('-ops', '--operations', help='select|update|delete', default="select")

args = vars(parser.parse_args())

duration = int(args['duration'])
total_clients = int(args['clients'])
queryNode = str(args['queryNode'])
ops = str(args['operations'])
scan_consistency = str(args['scan_consistency'])
max_parallelism = str(args['max_parallelism'])

## output run parameters ##
lgr.warn('======================================')
lgr.warn("Duration: %s" % duration)
lgr.warn("Num clients: %s" % total_clients)
lgr.warn("Query Node: %s" % queryNode)
lgr.warn("Operations: %s" % ops)
lgr.warn('======================================')

if ops == "update" or ops == "UPDATE":
    q = UPDATE_QUERIES["default"]
    len_query = len(q)

elif ops == "delete" or ops == "DELETE":
    q = DELETE_QUERIES["default"]
    len_query = len(q)
else:
    q = SELECT_QUERIES["default"]
    len_query = len(q)

airport_codes = calcAirports()
result_seq = calcSeqNumber()
keys = calcKeys()
amount = calcAmount()

pool = multiprocessing.Pool(total_clients)
k = os.system("ps aux | grep python")
lgr.warn('Number of client process running %s' % k)

start = time.time() # pick current time as program start time.

while (time.time() - start) <= duration:
    for j in range(total_clients):
        for j in range(len_query):
            k_qry = list(q)[j]
            print(k_qry)
            lgr.info("Query_Type: %s" % list(q)[j])
            if "Key" in k_qry:
                update_amt = random.randint(10, 100)
                r = runNQueryParam(q[k_qry], keys, scan_consistency, max_parallelism, queryNode)
            elif "Amount" in k_qry:
                r = runNQueryParam(q[k_qry], amount, scan_consistency, max_parallelism, queryNode)
            else:
                r = runNQueryParam(q[k_qry], airport_codes, scan_consistency, max_parallelism, queryNode)

#End of while
pool.close()
pool.join()

k = os.system("ps aux | grep python")
lgr.warn('======================================')
lgr.warn('Number of client process after the run %s' % k)
lgr.warn('End of DMLs')
lgr.warn('======================================')

