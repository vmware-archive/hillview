#!/usr/bin/env python3
import json
import itertools
import string

infile = 'short.schema'
states = [ "AK", "AL", "AR", "AZ", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "IA", "ID", "IL", "IN", "KS",
           "KY", "LA", "MA", "MD", "ME", "MI", "MN", "MO", "MS", "MT", "NC", "ND", "NE", "NH", "NJ", "NM",
           "NV", "NY", "OH", "OK", "OR", "PA", "PR", "RI", "SC", "SD", "TN", "TT", "TX", "UT", "VA", "VT",
           "WA", "WI", "WV", "WY" ]
carriers = ["9E", "AA", "AS", "B6", "CO", "DH", "DL", "EV", "F9", "FL", "G4", "GA", "HP", "KH", "MQ", "NK",
            "NW", "OH", "OO", "TW", "TZ", "UA", "US", "VX", "WN", "XE", "YV", "YX"]

def get_metadata(cn):
    if cn == "DayOfWeek":
        (g, gMin, gMax) = (1, 1, 7)
    elif cn == "DepTime" or cn == "ArrTime":
        (g, gMin, gMax) = (5, 0, 2400)
    elif cn == "DepDelay" or cn == "ArrDelay":
        (g, gMin, gMax) = (1, -100, 1000)
    elif cn == "Cancelled":
        (g, gMin, gMax) = (1, 0, 1)
    elif cn == "ActualElapsedTime":
        (g, gMin, gMax) = (1, 15, 800)
    elif cn == "Distance":
        (g, gMin, gMax) = (10, 0, 5100)
    elif cn == "FlightDate":
        # cluster values: (86400000, 852076800000, 1561852800000)
        # 2017 values: (86400000, 1483286400000, 1514736000000)
        # 2016 values, 2 months: (86400000, 1451635200000, 1456732800000)
        (g, gMin, gMax) = (86400000, 1451635200000, 1456732800000)
    else:
        raise Exception("Unexpected column " + cn)
    return {'type': "DoubleColumnQuantization",
            'granularity': g,
            'globalMin': gMin,
            'globalMax': gMax}

def get_string_metadata(col):
    letters = list(string.ascii_uppercase)
    if col == "OriginState" or col == "DestState":
        letters = states
    elif col == "UniqueCarrier":
        letters = carriers
    return {'type': "StringColumnQuantization",
            'globalMax': 'a',
            'leftBoundaries': letters }

def main():
    colnames = []
    with open(infile, 'r') as f:
        contents = "".join(line for line in f)
        schema = json.loads(contents)
        colnames = map(lambda s: s["name"], schema)

    length2 = itertools.combinations(colnames, 2)
    length2 = [sorted(x) for x in length2]

    with open('privacy_metadata.json', 'w') as f:
        quantization = {}
        defaultEpsilons = { "0": 1, "1": 1, "2": .1 }
        for col in schema:
            cn = col["name"]
            if col["kind"] == "String":
                quantization[cn] = get_string_metadata(cn)
            else:
                quantization[cn] = get_metadata(cn)
        output = {'epsilons': {}, 'defaultEpsilons': defaultEpsilons, 'quantization': { 'quantization': quantization } }
        f.write(json.dumps(output))

if __name__=='__main__':
    main()
