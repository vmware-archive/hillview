#!/usr/bin/env python3
import csv
import json

import numpy as np

def get_quantization(g, gMin, gMax):
    return {'type': "DoubleColumnQuantization",
            'granularity': g,
            'globalMin': gMin,
            'globalMax': gMax}

def main():
    mean = 0.0
    stdev = 25.0
    nsamples = 100000
    values = np.random.normal(mean, stdev, nsamples)

    colname = "Values"
    with open('gaussian.csv', 'w') as f:
        f.write(colname + '\n')
        for v in values:
            f.write('%0.02f' % v + '\n')

    schema = [{'name':colname, 'kind':'Double'}]
    with open('gaussian.schema', 'w') as f:
        f.write(json.dumps(schema))

    quantization = {'quantization': {colname: get_quantization(1.0, -100.0, 100.0)}}
    metadata = {'quantization': quantization, 'epsilons': { colname: .1 } }
    with open('privacy_metadata.json', 'w') as f:
        f.write(json.dumps(metadata))

if __name__=='__main__':
    main()
