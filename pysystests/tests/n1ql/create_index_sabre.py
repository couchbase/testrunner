#!/usr/bin/python
import json
import requests
import time
import argparse

DROP_INDEX = {
    "sabre": {
        "outbound_flight": "DROP INDEX default.outbound_flight"
        ,
        "fare_idx": "DROP INDEX default.fare_idx"
        ,
        "min_price_min_stop": "DROP INDEX default.min_price_min_stop"
        ,
        "one_way_direction": "DROP INDEX default.one_way_direction"
        ,
        "tax_seq": "DROP INDEX default.tax_seq"
        ,
        "flight_direction": "DROP INDEX default.flight_direction"
        ,
        "flight_num_route": "DROP INDEX default.flight_num_route"
    }
}

CREATE_INDEX = {
    "sabre": {
        "outbound_flight": "CREATE INDEX outbound_flight "
                             "ON default( "
                             "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureAirport.LocationCode,"
                             "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[1].ArrivalAirport.LocationCode,"
                             "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureDateTime) "
        ,
        "fare_idx": "CREATE INDEX fare_idx "
                      "ON default"
                      "(AirItineraryPricingInfo.PTC_FareBreakdowns.PTC_FareBreakdown.PassengerFare.TotalFare.Amount) "
        ,
        "min_price_min_stop": "CREATE INDEX min_price_min_stop ON "
                                "default("
                                "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureAirport.LocationCode, "
                                "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[1].ArrivalAirport.LocationCode,"
                                "ARRAY_LENGTH(AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment),"
                                "AirItineraryPricingInfo.PTC_FareBreakdowns.PTC_FareBreakdown.PassengerFare.TotalFare.Amount)"
        ,
        "one_way_direction": "CREATE INDEX one_way_direction ON "
                               "default("
                               "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureAirport.LocationCode, "
                               "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[1].ArrivalAirport.LocationCode,"
                               "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureDateTime,"
                               "AirItinerary.DirectionInd) "
        ,
        "flight_direction": "CREATE INDEX flight_direction ON "
                              "default("
                              "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureDateTime,"
                              "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].FlightNumber,"
                              "AirItinerary.DirectionInd) "
        ,
        "tax_seq": "CREATE INDEX tax_seq ON "
                     "default("
                     "AirItineraryPricingInfo.PTC_FareBreakdowns.PTC_FareBreakdown.PassengerFare.Taxes.Tax[0].TaxCode,"
                     "SequenceNumber) "
        ,
        "flight_num_route": "CREATE INDEX flight_num_route ON "
                               "default("
                               "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureAirport.LocationCode,"
                               "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[1].ArrivalAirport.LocationCode, "
                               "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].DepartureDateTime, "
                               "AirItinerary.OriginDestinationOptions.OriginDestinationOption[0].FlightSegment[0].FlightNumber) "
        ,
    }
}

BUILD_INDEX = {
    "sabre": {
        "build_default_flight": "BUILD INDEX on default(outbound_flight) "
        ,
        "build_default_fare": "BUILD INDEX on default(fare_idx) "
        ,
        "build_default_price": "BUILD INDEX on default(min_price_min_stop ) "
        ,
        "build_default_direction": "BUILD INDEX on default(flight_direction ) "
        ,
        "build_default_one_direction": "BUILD INDEX on default(one_way_direction) "
        ,
        "build_default_tax": "BUILD INDEX on default(tax_seq) "
        ,
        "build_default_route": "BUILD INDEX on default(flight_num_route) "
    }
}

def runQueryOnce(query, param, server_ip, build=False):
    url = "http://" + server_ip + ":8093/query"
    print build
    print param

    stmt = '{"statement" : "' + str(query)
    if "DROP" in query:
        stmt = stmt + " USING " + param + '"'

    elif "CREATE" in query:
        stmt = stmt + " USING " + param
        if param == "gsi":
            stmt = stmt + " WITH {\'nodes\': [\'" + server_ip+ ":8091\'], \'defer_build\': true}" + '"'
        else:
            stmt = stmt + '"'

    elif "BUILD" in query:
        stmt = stmt + '"'

    stmt = stmt + '}'
    print "-" *80
    print stmt
    query = json.loads(stmt)
    print query

    if build:
        time.sleep(10)

    r = requests.post(url, data=query, auth=('Administrator', 'password'))
    r_json = r.json()['results']
    x = list()
    for i in range(len(r_json)):
        x.append(r_json[i][param])
    return x

parser = argparse.ArgumentParser(description='This script is used for sabre indexes')
parser.add_argument('-i', '--index_type', help='Index Type', required=True)
parser.add_argument('-q', '--query_node', help='query node ip', required=True)
parser.add_argument('-drop', '--drop_index', help='drop index true', required=False)

args = vars(parser.parse_args())

index_type = str(args['index_type'])
queryNode = str(args['query_node'])
drop_index = str(args['drop_index'])

if index_type == "gsi":
    print "GSI INDEX"

elif index_type == "view":
    print "VIEW INDEX"

else:
    print " Create GSI indexes by  default"

if drop_index == "true":
    d = DROP_INDEX["sabre"]
    len_query = len(d)
    for j in range(len_query):
        print list(d)[j]
        k_qry = list(d)[j]
        r = runQueryOnce(d[k_qry], index_type, queryNode)

q = CREATE_INDEX["sabre"]
len_query = len(q)
for j in range(len_query):
    print list(q)[j]
    k_qry = list(q)[j]
    r = runQueryOnce(q[k_qry], index_type, queryNode)

if index_type =="view":
    b = BUILD_INDEX["sabre"]
    len_query = len(b)
    for j in range(len_query):
        print list(b)[j]
        k_qry = list(b)[j]
        r = runQueryOnce(b[k_qry], index_type, queryNode, build=True)


