#!/usr/bin/python
import argparse
import datetime
import json
import random
import requests
from random import randint

from lib.sdk_client3 import SDKClient

from couchbase.exceptions import TimeoutException

doc_template = "{\\\"AirItinerary\\\":" +\
                            "{\\\"DirectionInd\\\":$18," +\
                            "\\\"OriginDestinationOptions\\\":" +\
                                "{\\\"OriginDestinationOption\\\":" +\
                                    "[{\\\"ElapsedTime\\\":$10," +\
                                    "\\\"FlightSegment\\\":" +\
                                        "[{\\\"ArrivalAirport\\\":" +\
                                                "{\\\"LocationCode\\\":$3}," +\
                                                "\\\"ArrivalDateTime\\\":$6," +\
                                                "\\\"ArrivalTimeZone\\\":" +\
                                                    "{\\\"GMTOffset\\\":$12}," +\
                                                "\\\"DepartureAirport\\\":" +\
                                                    "{\\\"LocationCode\\\":$2}," +\
                                                "\\\"DepartureDateTime\\\":$5," +\
                                                "\\\"DepartureTimeZone\\\":" +\
                                                    "{\\\"GMTOffset\\\":$11}," +\
                                                "\\\"ElapsedTime\\\":$8," +\
                                                "\\\"Equipment\\\":" +\
                                                    "{\\\"AirEquipType\\\":$14}," +\
                                                "\\\"FlightNumber\\\":$16," +\
                                                "\\\"MarketingAirline\\\":" +\
                                                    "{\\\"Code\\\":$20}," +\
                                                "\\\"MarriageGrp\\\":\\\"O\\\"," +\
                                                "\\\"OperatingAirline\\\":" +\
                                                    "{\\\"Code\\\":\\\"DL\\\"," +\
                                                    "\\\"FlightNumber\\\":$16}," +\
                                                "\\\"ResBookDesigCode\\\":\\\"V\\\"," +\
                                                "\\\"StopQuantity\\\":0," +\
                                                "\\\"TPA_Extensions\\\":" +\
                                                    "{\\\"eTicket\\\":" +\
                                                        "{\\\"Ind\\\":$19}}}," +\
                                        "{\\\"ArrivalAirport\\\":" + \
                                                "{\\\"LocationCode\\\":$4}," +\
                                                "\\\"ArrivalDateTime\\\":$7," +\
                                                "\\\"ArrivalTimeZone\\\":" +\
                                                    "{\\\"GMTOffset\\\":$13}," +\
                                                "\\\"DepartureAirport\\\":" +\
                                                    "{\\\"LocationCode\\\":$3}," +\
                                                "\\\"DepartureDateTime\\\":$6," +\
                                                "\\\"DepartureTimeZone\\\":" +\
                                                    "{\\\"GMTOffset\\\":$12}," +\
                                                "\\\"ElapsedTime\\\":$9," +\
                                                "\\\"Equipment\\\":" +\
                                                    "{\\\"AirEquipType\\\":$15}," +\
                                                "\\\"FlightNumber\\\":$17," +\
                                                "\\\"MarketingAirline\\\":" +\
                                                    "{\\\"Code\\\":$20}," +\
                                                "\\\"MarriageGrp\\\":\\\"I\\\"," +\
                                                "\\\"OperatingAirline\\\":" +\
                                                    "{\\\"Code\\\":\\\"DL\\\"," +\
                                                    "\\\"FlightNumber\\\":$17}," +\
                                                "\\\"ResBookDesigCode\\\":\\\"V\\\"," +\
                                                "\\\"StopQuantity\\\":0," +\
                                                "\\\"TPA_Extensions\\\":" +\
                                                    "{\\\"eTicket\\\":" +\
                                                        "{\\\"Ind\\\":$19}}}" +\
                                        "]}," +\
                                    "{\\\"ElapsedTime\\\":$30," +\
                                    "\\\"FlightSegment\\\":" +\
                                        "[{\\\"ArrivalAirport\\\":" +\
                                                "{\\\"LocationCode\\\":$23}," +\
                                                "\\\"ArrivalDateTime\\\":$26," +\
                                                "\\\"ArrivalTimeZone\\\":" +\
                                                    "{\\\"GMTOffset\\\":$32}," +\
                                                "\\\"DepartureAirport\\\":" +\
                                                    "{\\\"LocationCode\\\":$22}," +\
                                                "\\\"DepartureDateTime\\\":$25," +\
                                                "\\\"DepartureTimeZone\\\":" +\
                                                    "{\\\"GMTOffset\\\":$31}," +\
                                                "\\\"ElapsedTime\\\":$28," +\
                                                "\\\"Equipment\\\":" +\
                                                    "{\\\"AirEquipType\\\":$34}," +\
                                                "\\\"FlightNumber\\\":$36," +\
                                                "\\\"MarketingAirline\\\":" +\
                                                    "{\\\"Code\\\":$20}," +\
                                                "\\\"MarriageGrp\\\":\\\"O\\\"," +\
                                                "\\\"OperatingAirline\\\":" +\
                                                    "{\\\"Code\\\":\\\"DL\\\"," +\
                                                    "\\\"FlightNumber\\\":$36}," +\
                                                "\\\"ResBookDesigCode\\\":\\\"X\\\"," +\
                                                "\\\"StopQuantity\\\":0," +\
                                                "\\\"TPA_Extensions\\\":" +\
                                                    "{\\\"eTicket\\\":{\\\"Ind\\\":$19}}}," +\
                                        "{\\\"ArrivalAirport\\\":" +\
                                                "{\\\"LocationCode\\\":$24}," +\
                                                "\\\"ArrivalDateTime\\\":$27," +\
                                                "\\\"ArrivalTimeZone\\\":" +\
                                                    "{\\\"GMTOffset\\\":$33}," +\
                                                "\\\"DepartureAirport\\\":" +\
                                                    "{\\\"LocationCode\\\":$23}," +\
                                                "\\\"DepartureDateTime\\\":$26," +\
                                                "\\\"DepartureTimeZone\\\"" +\
                                                    ":{\\\"GMTOffset\\\":$32}," +\
                                                "\\\"ElapsedTime\\\":$29," +\
                                                "\\\"Equipment\\\":" +\
                                                    "{\\\"AirEquipType\\\":$35}," +\
                                                "\\\"FlightNumber\\\":419," +\
                                                "\\\"MarketingAirline\\\"" +\
                                                    ":{\\\"Code\\\":$20}," +\
                                                "\\\"MarriageGrp\\\":\\\"I\\\"," +\
                                                "\\\"OperatingAirline\\\":" +\
                                                    "{\\\"Code\\\":\\\"DL\\\"," +\
                                                    "\\\"FlightNumber\\\":$37}," +\
                                                "\\\"ResBookDesigCode\\\":\\\"X\\\"," +\
                                                "\\\"TPA_Extensions\\\":" +\
                                                    "{\\\"eTicket\\\":{\\\"Ind\\\":$19}}}" +\
                                        "]}]}},"   +\
                        "\\\"AirItineraryPricingInfo\\\":" +\
                            "{\\\"ItinTotalFare\\\":" +\
                                    "{\\\"TotalFare\\\":"  +\
                                        "{\\\"Amount\\\":385," +\
                                        "\\\"CurrencyCode\\\":\\\"USD\\\"," +\
                                        "\\\"DecimalPlaces\\\":2}}," +\
                            "\\\"PTC_FareBreakdowns\\\":" +\
                                    "{\\\"PTC_FareBreakdown\\\":" +\
                                        "{\\\"FareBasisCodes\\\":" +\
                                            "{\\\"FareBasisCode\\\":"  +\
                                                "[" +\
                                                "{\\\"ArrivalAirportCode\\\":$3," +\
                                                    "\\\"BookingCode\\\":\\\"V\\\"," +\
                                                    "\\\"DepartureAirportCode\\\":$2," +\
                                                    "\\\"content\\\":\\\"VA07A0QZ\\\"}," +\
                                                "{\\\"ArrivalAirportCode\\\":$4," +\
                                                    "\\\"AvailabilityBreak\\\":true," +\
                                                    "\\\"BookingCode\\\":\\\"V\\\"," +\
                                                    "\\\"DepartureAirportCode\\\":$3," +\
                                                    "\\\"content\\\":\\\"VA07A0QZ\\\"}," +\
                                                "{\\\"ArrivalAirportCode\\\":$23," +\
                                                    "\\\"BookingCode\\\":\\\"X\\\"," +\
                                                    "\\\"DepartureAirportCode\\\":$22," +\
                                                    "\\\"content\\\":\\\"XA21A0NY\\\"}," +\
                                                "{\\\"ArrivalAirportCode\\\":$24," +\
                                                    "\\\"AvailabilityBreak\\\":true," +\
                                                    "\\\"BookingCode\\\":\\\"X\\\"," +\
                                                    "\\\"DepartureAirportCode\\\":$23," +\
                                                    "\\\"content\\\":\\\"XA21A0NY\\\"}]}," +\
                                            "\\\"PassengerFare\\\":" +\
                                                "{\\\"BaseFare\\\":" +\
                                                    "{\\\"Amount\\\":$43," +\
                                                    "\\\"CurrencyCode\\\":\\\"USD\\\"}," +\
                                                "\\\"EquivFare\\\":" +\
                                                    "{\\\"Amount\\\":$43," +\
                                                    "\\\"CurrencyCode\\\":\\\"USD\\\"," +\
                                                    "\\\"DecimalPlaces\\\":1}," +\
                                            "\\\"Taxes\\\":" +\
                                                "{\\\"Tax\\\":" +\
                                                    "[" +\
                                                    "{\\\"Amount\\\":$38," +\
                                                        "\\\"CurrencyCode\\\":\\\"USD\\\"," +\
                                                        "\\\"DecimalPlaces\\\":2," +\
                                                        "\\\"TaxCode\\\":\\\"AY\\\"}," +\
                                                    "{\\\"Amount\\\":$39," +\
                                                        "\\\"CurrencyCode\\\":\\\"USD\\\"," +\
                                                        "\\\"DecimalPlaces\\\":2," +\
                                                        "\\\"TaxCode\\\":\\\"US1\\\"}," +\
                                                    "{\\\"Amount\\\":$40," +\
                                                        "\\\"CurrencyCode\\\":\\\"USD\\\"," +\
                                                        "\\\"DecimalPlaces\\\":2," +\
                                                        "\\\"TaxCode\\\":\\\"XF\\\"}," +\
                                                    "{\\\"Amount\\\":$41," +\
                                                        "\\\"CurrencyCode\\\":\\\"USD\\\"," +\
                                                        "\\\"DecimalPlaces\\\":2," +\
                                                        "\\\"TaxCode\\\":\\\"ZP\\\"}" +\
                                                    "]," +\
                                                "\\\"TotalTax\\\":" +\
                                                    "{\\\"Amount\\\":$42," +\
                                                    "\\\"CurrencyCode\\\":\\\"USD\\\"," +\
                                                    "\\\"DecimalPlaces\\\":2}}," +\
                                            "\\\"TotalFare\\\":" +\
                                                "{\\\"Amount\\\":$44," +\
                                                "\\\"CurrencyCodev\\\":\\\"USD\\\"}}," +\
                                            "\\\"PassengerTypeQuantity\\\":" +\
                                                    "{\\\"Code\\\":\\\"ADT\\\"," +\
                                                    "\\\"Quantity\\\":1}}}}," +\
                        "\\\"SequenceNumber\\\":$45," +\
                        "\\\"TicketingInfo\\\":" +\
                            "{\\\"TicketType\\\":$46}}"

INSERT_QUERIES = \
{
"sabre": {
"createNewAirItinerary": "insert into default(key, value) values (TO_STRING($1), %s)" % doc_template
}
}

def runNQueryParam(query, param, server_ip):
    url = "http://" + server_ip + ":8093/query"
    #print url

    stmt = '{"statement" : "' + str(query) + '"'
    if (len(param) > 0):
        stmt = stmt + ', "args" : "['
    else:
        stmt = stmt + '}'
    i = 0
    myarg = ""
    for p in param:
        if isinstance(p, (bool)):
            myarg = myarg + str.lower(str(p))
        elif isinstance(p, (int, float)) and not isinstance(p, (bool)):
            myarg = myarg + str(p)
        else:
            myarg = myarg + '\\"' + str(p) + '\\"'
        i = i + 1
        if i < len(param):
            myarg = myarg + ","

    stmt = stmt + myarg
    stmt = stmt + ']" }'
    #print stmt

    query = json.loads(stmt)
    print(query)

    r = requests.post(url, data=query, stream=False, headers={'Connection': 'close'},
        auth=('Administrator', 'password'))
    print(r.json())
    r123 = r.json()['results']
    return r123

def runSDKQuery(keys, servers_ips, buckets):
    cbs = []
    srv = servers_ips
    if isinstance(servers_ips, list):
        srv = ','.join(servers_ips)
    if isinstance(buckets, str):
        cbs.append(SDKClient(hosts=[srv], bucket=buckets))
    else:
        for bucket in buckets:
            cbs.append(SDKClient(hosts=[srv], bucket=bucket))

    for cb in cbs:
        print('Inserting %s docs' % len(keys))
        try:
            out = cb.default_collection.insert_multi(keys)
        except TimeoutException as ex:
            out = str(ex)
            print('WARNING: Not all documents were inserted because of timeout error!! Please decrease batch size')
        cb.close()
    return out


def calcFlightSegmentDateTime():
    segments=[0, 1] # assume 2 segments
    x = dict((a, {}) for a in segments)

    year_range = [str(i) for i in range(2013, 2015)]
    month_range = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
    day_range = [str(i).zfill(2) for i in range(1, 28)]
    hour_range = [str(i).zfill(2) for i in range(0, 23)]
    min_range = [str(i).zfill(2) for i in range(0, 59)]

    year=int(random.choice(year_range))
    month = int(random.choice(month_range))
    day = int(random.choice(day_range))
    hour = int(random.choice(hour_range))
    minute = int(random.choice(min_range))

    for i in range(len(segments)):
        hour_delta = randint(0, 12)
        minute_delta = randint(0, 59)
        dep_1 = datetime.datetime(year, month, day, hour, minute) # Temp Assignment

        if i==1: # assuming 2 segments in all currently, to change for more flexibility
            if x[0]['arrv_2']: # add try catch exceptions here
                dep_1 = x[0]['arrv_2'] # Last Journey Location becomes input for the next Segment

        arrv_1 = dep_1 + datetime.timedelta(hours=hour_delta, minutes=minute_delta)
        arrv_2 = arrv_1 + datetime.timedelta(hours=randint(0, 12), minutes=randint(0, 59))
        elapsed_time_1 = int((arrv_1 - dep_1).total_seconds() / 60)
        elapsed_time_2 = int((arrv_2 - arrv_1).total_seconds() / 60)

        offset_dep_1 = randint(-12, 12) # pick any GMT, can be later offset w/ other Airport Codes
        offset_arrv_1 = randint(-1, 1) + offset_dep_1 # calculate this back to journey time
        offset_arrv_2 = randint(-1, 1) + offset_arrv_1 # calculate this back to journey time

        x[i]['dep_1']=dep_1
        x[i]['arrv_1']=arrv_1
        x[i]['arrv_2']=arrv_2
        x[i]['elapsed_time_1']=elapsed_time_1
        x[i]['elapsed_time_2']=elapsed_time_2
        x[i]['total_elapsed_time'] = elapsed_time_1+elapsed_time_2
        x[i]['offset_dep_1']=offset_dep_1
        x[i]['offset_arrv_1']=offset_arrv_1
        x[i]['offset_arrv_2']=offset_arrv_2

    return x

def calcAirportRouteInfo():
    segments=[0, 1] # assume 2 segments
    y = dict((a, {}) for a in segments)
    #y = {}
    all_airports = ["ABR", "ABI", "ATL", "BOS", "BUR", "CHI", "MDW", "DAL", "SFO", "SAN", "SJC", "LGA", "JFK", "MSP"]
    airport_codes = []
    # Improve this logic
    all_air_equip_type = ["757", "777", "787", "788", "M5", "A101", "A203", "M6"]
    air_equip_codes = []
     ### Improve this logic
    all_flight_numbers = ["240", "250", "260", "270", "280", "1200", "1210", "1150", "1140" ]
    flight_codes =[]


    for i in range(len(segments)):
        airport_codes = random.sample(all_airports, 3)
        air_equip_codes = random.sample(all_air_equip_type, 2)
        flight_codes = random.sample(all_flight_numbers, 2)
        y[0]['dep_airport'] = airport_codes[0]
        y[0]['arrv_airport_2']= airport_codes[2]

        if i==1: # assuming 2 segments in all currently, to change for more flexibility
            if y[0]['arrv_airport_2'] and y[0]['dep_airport']: # add exception handling here
                y[1]['dep_airport']= y[0]['arrv_airport_2']# Last Journey Location becomes input for the next Segment
                y[1]['arrv_airport_2']= y[0]['dep_airport']

        y[i]['arrv_airport_1']= airport_codes[1]
        y[i]['dep_flight_1'] = flight_codes[0]
        y[i]['dep_flight_2']=flight_codes[1]
        y[i]['dep_airequip_code_1']=air_equip_codes[0]
        y[i]['dep_airequip_code_2']=air_equip_codes[1]
    return y

def calcTaxes():
    all_tax_codes = ["AY", "US1", "XF", "ZP"] #check if there are more codes.
    t = dict((a, {}) for a in all_tax_codes)
    total_taxes=0
    for i in all_tax_codes:
        t[i]=round(random.uniform(10, 25))
        total_taxes = total_taxes+t[i]
    return t, total_taxes

def load_documents(insert_documents, seed, server_ip):
    #all_journey_direction = ["Return", "OneWay"]
    all_journey_direction = ["Return"]
    all_marketing_airlines = ["A", "B", "V", "SW", "B1"]

    q = INSERT_QUERIES["sabre"]
    len_query = len(q)

    for j in range(insert_documents):
        key = str(j)+ '_' +seed
        y=calcAirportRouteInfo()
        x=calcFlightSegmentDateTime()
        journey_direction = random.sample(all_journey_direction, 1)
        eTicketBool=random.choice([True, False])
        if eTicketBool:
            ticket_type="eTicket"
        ticket_type="paper"
        marketingAirline = random.sample(all_marketing_airlines, 1)
        dummyvar=0 #placeholder
        t, total_taxes = calcTaxes()
        base_fare = round(random.uniform(250, 600)) # later can add logic for varying with mileage/airports
        total_fare = base_fare+total_taxes
        sequence_number = j+1000 # for the lack of better understanding of sequencenUmber

        param = [key, #1
                 y[0]['dep_airport'], #2 Departure1
                 y[0]['arrv_airport_1'], #3 Arrival 1
                 y[0]['arrv_airport_2'],#4 Arrival 2
                 x[0]['dep_1'], #5
                 x[0]['arrv_1'], #6
                 x[0]['arrv_2'], #7
                 x[0]['elapsed_time_1'], #8
                 x[0]['elapsed_time_2'],#9
                 x[0]['total_elapsed_time'], #10
                 x[0]['offset_dep_1'], #11
                 x[0]['offset_arrv_1'], #12
                 x[0]['offset_arrv_2'],#13
                 y[0]['dep_airequip_code_1'],#14
                 y[0]['dep_airequip_code_2'],#15
                 y[0]['dep_flight_1'],#16
                 y[0]['dep_flight_2'],#17
                 journey_direction,#18
                 eTicketBool,#19
                 marketingAirline, #20
                 dummyvar,#21
                 y[1]['dep_airport'], #22 Departure
                 y[1]['arrv_airport_1'], #23 Arrival 1
                 y[1]['arrv_airport_2'],#24 Arrival 2
                 x[1]['dep_1'], #25
                 x[1]['arrv_1'], #26
                 x[1]['arrv_2'], #27
                 x[1]['elapsed_time_1'], #28
                 x[1]['elapsed_time_2'],#29
                 x[1]['total_elapsed_time'], #30
                 x[1]['offset_dep_1'], #31
                 x[1]['offset_arrv_1'], #32
                 x[1]['offset_arrv_2'],#33
                 y[1]['dep_airequip_code_1'],#34
                 y[1]['dep_airequip_code_2'],#35
                 y[1]['dep_flight_1'],#36
                 y[1]['dep_flight_2'],#37
                 t['ZP'],#38
                 t['XF'],#39
                 t['US1'],#40
                 t['AY'],#41
                 total_taxes,#42
                 base_fare, #43
                 total_fare, #44
                 sequence_number, #45
                 ticket_type #46
        ]
        print(list(q)[0])
        k_qry = list(q)[0]
        r = runNQueryParam(q[k_qry], param, server_ip)
        print(r)

        print("Done!")

def sdk_load_documents(insert_documents, seed, server_ip, batch_size, buckets):
    all_journey_direction = ["Return"]
    all_marketing_airlines = ["A", "B", "V", "SW", "B1"]

    q = INSERT_QUERIES["sabre"]

    j = 1
    while j <= insert_documents:
        keys = {}
        for doc_num in range(batch_size):
            key = str(j)+ '_' +seed
            y=calcAirportRouteInfo()
            x=calcFlightSegmentDateTime()
            journey_direction = random.sample(all_journey_direction, 1)
            eTicketBool=random.choice([True, False])
            if eTicketBool:
                ticket_type="eTicket"
            ticket_type="paper"
            marketingAirline = random.sample(all_marketing_airlines, 1)
            dummyvar=0 #placeholder
            t, total_taxes = calcTaxes()
            base_fare = round(random.uniform(250, 600)) # later can add logic for varying with mileage/airports
            total_fare = base_fare+total_taxes
            sequence_number = j+1000 # for the lack of better understanding of sequencenUmber

            param = [key, #1
                     y[0]['dep_airport'], #2 Departure1
                     y[0]['arrv_airport_1'], #3 Arrival 1
                     y[0]['arrv_airport_2'],#4 Arrival 2
                     x[0]['dep_1'], #5
                     x[0]['arrv_1'], #6
                     x[0]['arrv_2'], #7
                     x[0]['elapsed_time_1'], #8
                     x[0]['elapsed_time_2'],#9
                     x[0]['total_elapsed_time'], #10
                     x[0]['offset_dep_1'], #11
                     x[0]['offset_arrv_1'], #12
                     x[0]['offset_arrv_2'],#13
                     y[0]['dep_airequip_code_1'],#14
                     y[0]['dep_airequip_code_2'],#15
                     y[0]['dep_flight_1'],#16
                     y[0]['dep_flight_2'],#17
                     journey_direction,#18
                     eTicketBool,#19
                     marketingAirline, #20
                     dummyvar,#21
                     y[1]['dep_airport'], #22 Departure
                     y[1]['arrv_airport_1'], #23 Arrival 1
                     y[1]['arrv_airport_2'],#24 Arrival 2
                     x[1]['dep_1'], #25
                     x[1]['arrv_1'], #26
                     x[1]['arrv_2'], #27
                     x[1]['elapsed_time_1'], #28
                     x[1]['elapsed_time_2'],#29
                     x[1]['total_elapsed_time'], #30
                     x[1]['offset_dep_1'], #31
                     x[1]['offset_arrv_1'], #32
                     x[1]['offset_arrv_2'],#33
                     y[1]['dep_airequip_code_1'],#34
                     y[1]['dep_airequip_code_2'],#35
                     y[1]['dep_flight_1'],#36
                     y[1]['dep_flight_2'],#37
                     t['ZP'],#38
                     t['XF'],#39
                     t['US1'],#40
                     t['AY'],#41
                     total_taxes,#42
                     base_fare, #43
                     total_fare, #44
                     sequence_number, #45
                     ticket_type #46
            ]
            global doc_template
            template = doc_template
            template = template.replace('\\"', '"')
            for p in reversed(range(len(param))):
                if isinstance(param[p], (bool)):
                    template = template.replace('$%s' % (p + 1), str.lower(str(param[p])))
                elif isinstance(param[p], (int, float)) and not isinstance(param[p], (bool)):
                    template = template.replace('$%s' % (p + 1), str(param[p]))
                elif isinstance(param[p], list):
                    template = template.replace('$%s' % (p + 1), str(param[p]))
                else:
                    template = template.replace('$%s' % (p + 1), '"' + str(param[p]) + '"')
            template = template.replace("'", '"')
            keys[key] = json.loads(template)
            j += 1
        print(list(q)[0])
        r = runSDKQuery(keys, server_ip, buckets)
        print(r)

    print("Done!")



parser = argparse.ArgumentParser(description='This script is used load sabre dataset')
parser.add_argument('-doc', '--documents', help='Number of documents', required=True)
parser.add_argument('-q', '--queryNode', help='query node ip', required=True)
parser.add_argument('-s', '--seed', help='Seed for key generation', required=False)
parser.add_argument('-b', '--queryBuckets', help='Buckets to query', required=False)
parser.add_argument('-z', '--queryBatchSize', help='Batch size to insert', required=False)
args = vars(parser.parse_args())

## output duration and clients ##
print(("Documents: %s" % args['documents'] ))
print(("QueryNode: %s" % args['queryNode']))

insert_documents = int(args['documents'])
queryNode = str(args['queryNode'])
if queryNode.find(';') != -1:
    queryNode = queryNode.split(';')
queryBuckets = 'default'
if args['queryBuckets']:
    queryBuckets = str(args['queryBuckets']).split[';']
batchSize = 1
if 'queryBatchSize' in args:
    batchSize = int(args['queryBatchSize'])
print('Batch size: %s' % batchSize)
#Create random insert key
seed = "sabre_" + str(randint(1, 100000))

print("------------ Begin Inserts  ------------")
if isinstance(queryNode, list) or batchSize > 1 or isinstance(queryBuckets, list):
    l = sdk_load_documents(insert_documents, seed, queryNode, batchSize, queryBuckets)
else:
    l = load_documents(insert_documents, seed, queryNode)
print("------------ End of my insert program ------------")
