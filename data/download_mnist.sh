#!/bin/bash

set -x

echo 'Downloading MNIST data...'
wget http://yann.lecun.com/exdb/mnist/train-images-idx3-ubyte.gz
gunzip train-images-idx3-ubyte.gz
wget http://yann.lecun.com/exdb/mnist/train-labels-idx1-ubyte.gz
gunzip train-labels-idx1-ubyte.gz
wget http://yann.lecun.com/exdb/mnist/t10k-images-idx3-ubyte.gz
gunzip t10k-images-idx3-ubyte.gz
wget http://yann.lecun.com/exdb/mnist/t10k-labels-idx1-ubyte.gz
gunzip t10k-labels-idx1-ubyte.gz
echo 'Done with downloading!'

echo 'Converting data to csv, and generating schema file...'
./convert_to_csv.py
rm train-images-idx3-ubyte
rm train-labels-idx1-ubyte
rm t10k-images-idx3-ubyte
rm t10k-labels-idx1-ubyte
echo 'Done!'

echo 'Concatenating test and train data to one file...'
tail -n +2 mnist_test.csv >> mnist_train.csv
rm mnist_test.csv
mv mnist_train.csv mnist.csv
echo 'Done!'
