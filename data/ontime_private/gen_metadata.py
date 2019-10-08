#!/usr/bin/env python3
import csv
import json
import itertools

infile = '2016_1.csv'

def get_metadata(e, g, gMin, gMax):
    return {'type': "DoubleColumnPrivacyMetadata",
            'epsilon': e,
            'granularity': g,
            'globalMin': gMin,
            'globalMax': gMax}

def concat_colnames(colnames):
    return '+'.join(colnames)

def main():
    colnames = []
    with open(infile, 'r') as f:
        colnames = f.readline().strip().split(',')

    length2 = itertools.combinations(colnames, 2)
    length2 = [sorted(x) for x in length2]
    print(list(length2))

    with open('privacy_metadata.json', 'w') as f:
        metadata = {}
        for cn in colnames:
            metadata[cn] = get_metadata(0.1, 1.0, -100.0, 100.0)
        for cn in length2:
            concat_cn = concat_colnames(cn)
            metadata[concat_cn] = {'type':'ColumnPrivacyMetadata','epsilon':0.1}
        output = {'metadata':metadata}
        f.write(json.dumps(output))

if __name__=='__main__':
    main()
