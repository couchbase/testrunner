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
    },
    "tpcc": {
        "id_item": "DROP INDEX ITEM.IT_ID "
        ,
        "id_customer": "DROP INDEX CUSTOMER.CU_ID_D_ID_W_ID"
        ,
        "last_id_customer": "DROP INDEX CUSTOMER.CU_W_ID_D_ID_LAST"
        ,
        "wid_orders": "DROP INDEX ORDERS.OR_O_ID_D_ID_W_ID"
        ,
        "cid_orders": "DROP INDEX ORDERS.OR_W_ID_D_ID_C_ID"
        ,
        "id_stock": "DROP INDEX STOCK.ST_W_ID_I_ID1"
        ,
        "wid_district": "DROP INDEX DISTRICT.DI_ID_W_ID"
        ,
        "id_warehouse": "DROP INDEX WAREHOUSE.WH_ID"
        ,
        "wid_order_line": "DROP INDEX ORDER_LINE.OL_O_ID_D_ID_W_ID"
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
    },
    "tpcc": {
        "id_customer": "CREATE INDEX CU_ID_D_ID_W_ID on "
                       "CUSTOMER(C_ID, C_D_ID, C_W_ID) "
        ,
        "last_id_customer": "CREATE INDEX CU_W_ID_D_ID_LAST "
                            "on CUSTOMER(C_W_ID, C_D_ID, C_LAST) "
        ,
        "wid_district": "CREATE INDEX DI_ID_W_ID "
                        "on DISTRICT(D_ID, D_W_ID)"
        ,
        "id_item": "CREATE INDEX IT_ID "
                   "on ITEM(I_ID) "
        ,
        "wid_new_order": "CREATE INDEX NO_D_ID_W_ID "
                         "on NEW_ORDER(NO_O_ID, NO_D_ID, NO_W_ID) "
        ,
        "wid_orders": "CREATE INDEX OR_O_ID_D_ID_W_ID "
                      "on ORDERS(O_ID, O_D_ID, O_W_ID, O_C_ID) "
        ,
        "cid_orders": "CREATE INDEX OR_W_ID_D_ID_C_ID "
                      "on ORDERS(O_W_ID, O_D_ID, O_C_ID) "
        ,
        "wid_order_line": "CREATE INDEX OL_O_ID_D_ID_W_ID "
                          "on ORDER_LINE(OL_O_ID, OL_D_ID, OL_W_ID) "
        ,
        "id_stock": "CREATE INDEX ST_W_ID_I_ID1 "
                    "on STOCK(S_W_ID, S_I_ID)"
        ,
        "id_warehouse": "CREATE INDEX WH_ID "
                        "on WAREHOUSE(W_ID) "
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
    },
    "tpcc": {
        "build_customer": "BUILD INDEX "
                          "on CUSTOMER(CU_ID_D_ID_W_ID, CU_W_ID_D_ID_LAST) ",
        "build_district": "BUILD INDEX "
                          "on DISTRICT(DI_ID_W_ID) ",
        "build_item": "BUILD INDEX "
                      "on ITEM(IT_ID) ",
        "build_new_order": "BUILD INDEX "
                           "on NEW_ORDER(NO_D_ID_W_ID) ",
        "build_orders": "BUILD INDEX "
                        "on ORDERS(OR_O_ID_D_ID_W_ID, OR_W_ID_D_ID_C_ID) ",
        "build_order_line": "BUILD INDEX "
                            "on ORDER_LINE(OL_O_ID_D_ID_W_ID) ",
        "build_stock": "BUILD INDEX "
                       "on STOCK(ST_W_ID_I_ID1) ",
        "build_warehouse": "BUILD INDEX "
                           "on WAREHOUSE(WH_ID) "
    }
}

def runQueryOnce(query, param, query_ip, server_ip, build=False):
    url = "http://" + query_ip + ":8093/query"
    print build
    print param

    stmt = '{"statement" : "' + str(query)
    if "DROP" in query:
        stmt = stmt + " USING " + param + '"'

    elif "CREATE" in query:
        stmt = stmt + " USING " + param
        if param == "gsi":
            stmt = stmt + " WITH {\'nodes\': [\'" + server_ip + ":8091\'], \'defer_build\': true}" + '"'
        else:
            stmt = stmt + '"'

    elif "BUILD" in query:
        stmt = stmt + '"'

    stmt = stmt + '}'
    query = json.loads(stmt)
    print query

    if build:
        time.sleep(20)

    r = requests.post(url, data=query, auth=('Administrator', 'password'))
    r_json = r.json()['results']
    x = list()
    for i in range(len(r_json)):
        x.append(r_json[i][param])
    return x

parser = argparse.ArgumentParser(description='This script is used for sabre indexes')
parser.add_argument('-i', '--index_type', help='Index Type', required=True)
parser.add_argument('-q', '--query_node', help='query node ip', required=True)
parser.add_argument('-idx', '--index_node', help='index node ip', required=True)
parser.add_argument('-drop', '--drop_index', help='drop index true', required=False)
parser.add_argument('-bucket', '--bucket_type', help='For sabre| tpcc', required=True)

args = vars(parser.parse_args())

index_type = str(args['index_type'])
query_node = str(args['query_node'])
index_node = str(args['index_node'])
drop_index = str(args['drop_index'])
bucket_type = str(args['bucket_type'])

if drop_index == "true":
    d = DROP_INDEX[bucket_type]
    len_query = len(d)
    for j in range(len_query):
        print list(d)[j]
        k_qry = list(d)[j]
        r = runQueryOnce(d[k_qry], index_type, query_node, index_node)

q = CREATE_INDEX[bucket_type]
len_query = len(q)
for j in range(len_query):
    print list(q)[j]
    k_qry = list(q)[j]
    r = runQueryOnce(q[k_qry], index_type, query_node, index_node)

if index_type == "gsi":
    b = BUILD_INDEX[bucket_type]
    len_query = len(b)
    for j in range(len_query):
        print list(b)[j]
        k_qry = list(b)[j]
        r = runQueryOnce(b[k_qry], index_type, query_node, index_node, build=True)


