#!/usr/bin/env python3
import csv
import json
import itertools
import string

infile = 'short.schema'

def get_metadata(g, gMin, gMax):
    return {'type': "DoubleColumnQuantization",
            'granularity': g,
            'globalMin': gMin,
            'globalMax': gMax}

def get_string_metadata():
    letters = list(string.ascii_lowercase) + list(string.ascii_uppercase)
    return {'type': "StringColumnQuantization",
            'globalMax': 'z',
            'leftBoundaries': letters }

def concat_colnames(colnames):
    return '+'.join(colnames)

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
        epsilons = {}
        for col in schema:
            cn = col["name"]
            if col["kind"] == "String":
                quantization[cn] = get_string_metadata()
            else:
                quantization[cn] = get_metadata(5.0, -100.0, 100.0)
            epsilons[cn] = 1
        for cn in length2:
            concat_cn = concat_colnames(cn)
            epsilons[concat_cn] = 0.1
        output = {'quantization': { 'quantization': quantization }, 'epsilons': epsilons }
        f.write(json.dumps(output))

if __name__=='__main__':
    main()
