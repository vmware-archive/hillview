#!/bin/sh

wget http://archive.ics.uci.edu/ml/machine-learning-databases/image/segmentation.data
wget http://archive.ics.uci.edu/ml/machine-learning-databases/image/segmentation.test
wget http://archive.ics.uci.edu/ml/machine-learning-databases/image/segmentation.names

mv segmentation.names segmentation.readme
tail -n +6 segmentation.test > segmentation.csv
tail -n +6 segmentation.data >> segmentation.csv
rm segmentation.data segmentation.test
