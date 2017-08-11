#!/bin/sh
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